"""The **FileVaultStateStore** — a persistent, on-disk :class:`~concursus.statestore.StateStore`.

Concursus's default :class:`~concursus.statestore.InProcessStateStore` is pure in-memory (run
state vanishes on process exit) and its :class:`~concursus.statestore.MemoryStateStore` persists
opaque AgentCore Blob events — neither writes a durable, human-readable *note* to disk. This
backend closes that gap: it writes **one round-trip-exact markdown note per record event** under
``<vault>/runs/<session>/`` and reloads them to resume, so a run survives process exit and its log
is greppable/inspectable offline — without AWS. It is the offline / air-gapped / CI / debuggable
durability tier, opt-in behind the same 4-method :class:`StateStore` Protocol.

Design (FZ 35e1b1): it *reuses* the statestore marshalling seam rather than reinventing it —
:func:`~concursus.statestore._build_metadata` / :func:`~concursus.statestore._event_to_record`
(shared with :class:`MemoryStateStore`, so the file and AgentCore backends differ only in
transport), :func:`~concursus.statestore.content_hash`, and
:func:`~concursus.statestore._index_records`. The **authoritative payload is an embedded,
base64-wrapped JSON blob**, never the rendered YAML/body — so an arbitrary ``output`` dict
(newlines, quotes, ``---``, ``[](.md)`` link syntax, numeric-looking strings) round-trips
exactly; the frontmatter and body are lossy display/index copies never re-ingested. Writes are
atomic (temp + ``os.replace``); a reentrant lock + generation-token OCC over ``.lock`` / ``.gen``
sidecars keeps concurrent writers over one vault from clobbering. Pure-Python, stdlib only.

The on-disk notes stay the single source of truth; :mod:`concursus.rundb` builds a *derived,
rebuildable* SQLite graph/index over them (never a second source).
"""

from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set

from .statestore import (
    Record,
    _ADDR_SEP,
    _apply_meta,
    _build_metadata,
    _DEDUP_RECORD_TYPE,
    _event_to_record,
    _index_records,
    content_hash,
)

# The authoritative embedded-JSON marker: everything after it on the line is base64 of the JSON
# truth (mirrors HiveFleet's ``b64:`` embedded-note discipline — display frontmatter is lossy).
_BLOB_PREFIX = "b64:"

# Sidecars living beside the run dir for the cross-process write guard.
_LOCK_NAME = ".concursus.lock"
_GEN_NAME = ".concursus.gen"


def _slug(raw: str, *, maxlen: int = 80) -> str:
    """A collision-resistant, filesystem-safe slug of ``raw``.

    Keeps ``[A-Za-z0-9._-]`` verbatim, maps every other run of characters to ``-``, and appends a
    short content hash so distinct inputs that fold to the same safe stem never collide (the
    injective-slug *purpose* of HiveFleet's ``slug_component`` — without the Luhmann FZ form,
    since concursus addresses are already ``/``-materialized paths).
    """
    safe_chars = []
    for ch in raw:
        safe_chars.append(ch if (ch.isalnum() or ch in "._-") else "-")
    safe = "".join(safe_chars).strip("-") or "x"
    digest = content_hash({"_": raw})[:8]
    stem = safe[:maxlen]
    return f"{stem}__{digest}"


def _encode_blob(output: dict) -> str:
    """Base64 the canonical JSON of ``{node: output}``-style payloads for lossless embedding."""
    raw = json.dumps(output, sort_keys=True).encode("utf-8")
    return _BLOB_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_blob(token: str) -> dict:
    """Inverse of :func:`_encode_blob` (tolerates a missing prefix)."""
    if token.startswith(_BLOB_PREFIX):
        token = token[len(_BLOB_PREFIX) :]
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


def _record_to_note(record: Record) -> str:
    """Render a :class:`Record` as a round-trip-exact markdown note.

    Frontmatter = :func:`_build_metadata` (the all-string display/index copy) plus one
    authoritative ``payload`` line = ``b64:<base64(json({node: output}))>`` (byte-identical to
    :class:`MemoryStateStore`'s Blob). A human-readable body follows for greppability but is
    **never** re-ingested — :func:`_note_to_record` reads only the frontmatter + the blob.
    """
    meta = _build_metadata(record)
    lines = ["---"]
    for key in sorted(meta):
        lines.append(f"{key}: {json.dumps(meta[key])}")
    lines.append(f"payload: {_encode_blob({record.node: record.output})}")
    lines.append("---")
    lines.append("")
    lines.append(f"# {record.node} (attempt {record.attempt}, {record.status})")
    lines.append("")
    lines.append("> Derived display copy — the `payload` frontmatter blob is the source of truth.")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(record.output, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _note_to_record(text: str) -> Record:
    """Parse a note written by :func:`_record_to_note` back into an exact :class:`Record`.

    Reads ONLY the frontmatter (each ``key: <json>``) and the authoritative ``payload`` blob,
    reconstructs the AgentCore-shaped event dict, and defers to
    :func:`~concursus.statestore._event_to_record` — the same marshalling
    :class:`MemoryStateStore` uses, so the two backends never drift.
    """
    if not text.startswith("---"):
        raise ValueError("note missing frontmatter")
    _, _, rest = text.partition("---\n")
    fm_block, _, _ = rest.partition("\n---")
    meta: Dict[str, str] = {}
    payload_token = ""
    for line in fm_block.splitlines():
        if not line.strip() or ":" not in line:
            continue
        key, _, raw = line.partition(":")
        key = key.strip()
        raw = raw.strip()
        if key == "payload":
            payload_token = raw
            continue
        try:
            meta[key] = json.loads(raw)
        except json.JSONDecodeError:
            meta[key] = raw

    node = meta.get("node", "")
    blob = _decode_blob(payload_token) if payload_token else {node: {}}
    event = {
        "metadata": meta,
        "payload": [{"blob": json.dumps(blob)}],
        "eventId": meta.get("event_id"),
        "eventTimestamp": _coerce_int(meta.get("timestamp")),
    }
    return _event_to_record(event)


def _coerce_int(value: object) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


class FileVaultStateStore:
    """A persistent, on-disk :class:`StateStore` — durable markdown notes, resume by reload.

    Mirrors :class:`InProcessStateStore`'s ``put`` semantics (append-only log + a
    ``{node: latest validated output}`` projection, attempt auto-increment, content-hash dedup)
    and adds durability: each ``put`` writes one immutable note file (atomically), and a fresh
    store over an existing vault lazily reloads it before the first read (resume = replay with a
    filesystem transport). Concurrent writers over one vault are serialized by a reentrant lock
    plus a generation-token OCC read-fresh over ``.gen``.
    """

    def __init__(self, run_dir) -> None:
        self._dir = Path(run_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._records: List[Record] = []
        self._projection: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._clock: int = 0
        self._loaded = False
        self._lock = threading.RLock()
        self._depth = 0  # critical-section reentrancy depth (guarded by _lock)
        self._own_gen = -1

    @classmethod
    def from_config(cls, *, vault_path, session_id: str) -> "FileVaultStateStore":
        """Bind a run to ``<vault_path>/runs/<slug(session_id)>/`` (persistence-by-default posture).

        Callers that want ephemeral behaviour keep the bare :class:`InProcessStateStore` default;
        this is the explicit persistent choice (mirrors ``MemoryService.from_config``).
        """
        run_dir = Path(vault_path) / "runs" / _slug(session_id)
        return cls(run_dir)

    # -- write --------------------------------------------------------------
    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None:
        with self._critical(write=True):
            self._ensure_loaded_locked()
            self._clock += 1
            attempt = self._attempts.get(node, 0) + 1
            self._attempts[node] = attempt
            chash = content_hash(output)
            dedup = node in self._projection and content_hash(self._projection[node]) == chash

            record = Record(
                node=node,
                output=dict(output),
                attempt=attempt,
                content_hash=chash,
                timestamp=self._clock,
            )
            _apply_meta(record, meta)
            if dedup and record.record_type == "agent_output":
                record.record_type = _DEDUP_RECORD_TYPE

            self._write_note(record)
            self._records.append(record)
            if record.status == "validated":
                self._projection[node] = record.output

    # -- reads --------------------------------------------------------------
    def get(self, node: str) -> dict:
        with self._critical():
            self._ensure_loaded_locked()
            if node not in self._projection:
                raise KeyError(node)
            return self._projection[node]

    def completed(self) -> Set[str]:
        with self._critical():
            self._ensure_loaded_locked()
            latest_overall, _, _ = _index_records(self._records)
            return {n for n, r in latest_overall.items() if r.status == "validated"}

    def records(self) -> List[Record]:
        with self._critical():
            self._ensure_loaded_locked()
            return list(self._records)

    # -- persistence --------------------------------------------------------
    def _note_filename(self, record: Record) -> str:
        addr = record.address or record.node
        return f"{_slug(addr)}__a{record.attempt}.md"

    def _write_note(self, record: Record) -> None:
        self._atomic_write(self._dir / self._note_filename(record), _record_to_note(record))

    @staticmethod
    def _atomic_write(path: Path, text: str) -> None:
        """Write ``text`` to ``path`` atomically (temp file in the same dir + ``os.replace``)."""
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)

    def _load(self) -> None:
        """Rebuild the in-memory state from the note files (resume = replay over the filesystem).

        Reads every ``*.md`` under the run dir, parses each into a :class:`Record`, then runs the
        same tail as :meth:`MemoryStateStore.replay`: :func:`_index_records` → projection /
        attempts. Records are ordered by their monotonic ``timestamp`` so last-write-wins is stable.
        """
        records: List[Record] = []
        for note in sorted(self._dir.glob("*.md")):
            try:
                records.append(_note_to_record(note.read_text(encoding="utf-8")))
            except (ValueError, json.JSONDecodeError, OSError):
                continue  # skip a malformed / partial file rather than abort the whole reload
        records.sort(key=lambda r: (r.timestamp if r.timestamp is not None else 0))

        _, latest_validated, attempts = _index_records(records)
        self._records = records
        self._projection = {node: r.output for node, r in latest_validated.items()}
        self._attempts = attempts
        self._clock = max((r.timestamp or 0 for r in records), default=0)
        self._own_gen = self._read_gen()  # sync to committed generation (OCC baseline)
        self._loaded = True

    def _ensure_loaded_locked(self) -> None:
        if not self._loaded:
            self._load()

    # -- cross-process write guard (RLock + generation-token OCC) -----------
    def _critical(self, *, write: bool = False):
        return _Critical(self, write=write)

    def _read_gen(self) -> int:
        try:
            return int((self._dir / _GEN_NAME).read_text(encoding="utf-8").strip() or "0")
        except (OSError, ValueError):
            return 0

    def _bump_gen(self) -> None:
        self._own_gen = self._read_gen() + 1
        self._atomic_write(self._dir / _GEN_NAME, str(self._own_gen))


class _Critical:
    """Reentrant critical section: the in-process ``RLock`` plus a cross-process advisory lock and
    a generation-token OCC read-fresh. On the OUTERMOST entry it takes an exclusive ``fcntl``
    lock on ``.lock``, and if a peer advanced ``.gen`` since our last write it reloads before
    mutating (so a write always allocates over the current committed state); it bumps ``.gen`` on
    exit. Degrades to the RLock alone where ``fcntl`` is unavailable (non-POSIX)."""

    def __init__(self, store: "FileVaultStateStore", *, write: bool) -> None:
        self._store = store
        self._write = write
        self._fh = None
        self._outermost = False

    def __enter__(self) -> "_Critical":
        store = self._store
        store._lock.acquire()
        self._outermost = store._depth == 0
        store._depth += 1
        if not self._outermost:
            return self
        try:
            import fcntl  # POSIX only

            self._fh = open(store._dir / _LOCK_NAME, "w")
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            self._fh = None
        # OCC read-fresh: a peer advanced the on-disk generation → reload before we touch state.
        if store._loaded and store._read_gen() != store._own_gen:
            store._load()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        store = self._store
        try:
            if self._outermost:
                if self._write and exc_type is None:
                    store._bump_gen()
                if self._fh is not None:
                    try:
                        import fcntl

                        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                    finally:
                        self._fh.close()
                        self._fh = None
        finally:
            store._depth -= 1
            store._lock.release()
