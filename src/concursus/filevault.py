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

# The default vault posture: emit notes conformant to the Abuse SlipBox format (validate under
# ``check_note_format.py`` / ``validate_fz_trails.py``) so a run's on-disk notes are a genuine,
# indexer-ingestible slipbox trail — not merely durable markdown. Set ``slipbox_form=False`` for
# the leaner machine schema (see FZ 35e1b1: the authentic form is optional for ephemeral run-state).
_SLIPBOX_TOPICS = ["Multi-Agent Orchestration", "Concursus Run State"]

# A record's status maps onto the SlipBox status vocabulary (validated→active, failed→draft).
_SLIPBOX_STATUS = {"validated": "active", "failed": "draft", "superseded": "superseded"}


def _building_block_for(record: Record) -> str:
    """Derive the SlipBox ``building_block`` for a record from its kind (mirrors HiveFleet's
    ``building_block_of``): a failed record is a ``counter_argument`` (a refuted attempt), a
    content-hash dedup no-op is ``navigation`` (a structural marker, not new evidence), and any
    other validated agent output is an ``empirical_observation`` (a produced result). This is
    *derived*, never hardcoded, so the note's building_block reflects what the record actually is.
    """
    if record.status == "failed":
        return "counter_argument"
    if record.record_type == _DEDUP_RECORD_TYPE:
        return "navigation"
    return "empirical_observation"


# --------------------------------------------------------------------------- Luhmann FZ helpers
# A run's records form a per-run Folgezettel trail: the run root is FZ ``"1"`` and each record is a
# write-order child (``1a``, ``1b`` … bijective base-26 past 26), so notes carry a valid
# ``folgezettel:`` / ``lineage:`` the SlipBox tooling accepts. Concursus addresses are ``/``-paths
# (not dotted ordinals like HiveFleet), so FZ position is assigned by write order, not re-based.
def _int_to_letters(n: int) -> str:
    """Bijective base-26: ``1→a … 26→z, 27→aa`` (a total, reversible ordinal→letter map)."""
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("a") + rem) + out
    return out


def _fz_for(position: int) -> str:
    """The FZ string for the ``position``-th (1-based) record in a run: ``1a``, ``1b`` … under root ``1``."""
    return "1" + _int_to_letters(position)


def _trail_id(session_id: str) -> str:
    """A SlipBox ``lineage:`` path id / trail slug for a run — matches the grammar ``^[a-z][a-z0-9_]*``.

    Lowercases, folds every non-``[a-z0-9_]`` char to ``_``, and prefixes ``run_`` when the result
    does not start with a letter, so a run's notes share one valid, injective-enough trail id.
    """
    slug = "".join(ch if (ch.isascii() and (ch.isalnum() or ch == "_")) else "_" for ch in session_id.lower())
    if not slug or not slug[0].isalpha():
        slug = "run_" + slug
    return slug


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


def _record_to_note(
    record: Record,
    *,
    slipbox_form: bool = True,
    position: int = 1,
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> str:
    """Render a :class:`Record` as a round-trip-exact markdown note.

    Two forms share one authoritative ``payload`` line (``b64:<base64(json({node: output}))>``,
    byte-identical to :class:`MemoryStateStore`'s Blob) — :func:`_note_to_record` reads only that
    blob plus the machine frontmatter keys, never the display fields, so the round-trip is exact
    either way:

    * ``slipbox_form=True`` (default) emits a note conformant to the Abuse SlipBox format —
      P.A.R.A. ``tags`` / ``keywords`` / ``topics`` / ``building_block`` / valid ``status`` /
      ``folgezettel`` / ``lineage`` / ``access_control_group``, a typed H1, and a
      ``## Related Notes`` section — so it validates under ``check_note_format.py`` and reads as a
      genuine, indexer-ingestible slipbox trail;
    * ``slipbox_form=False`` emits the lean machine schema (``node`` / ``attempt`` / ``status`` /
      ``consumes`` / ``payload``) for a smaller, non-indexed durable log.
    """
    machine = _build_metadata(record)  # the authoritative, all-string run-state keys
    # Two authoritative, lossless lines the reader reconstructs from — the HiveFleet discipline:
    # ``payload`` = the output blob, ``meta`` = the record's metadata. Every display field below
    # (SlipBox frontmatter, H1, body) is a lossy copy that :func:`_note_to_record` never reads.
    blob_line = f"payload: {_encode_blob({record.node: record.output})}"
    meta_line = f"meta: {_encode_blob(machine)}"

    if not slipbox_form:
        lines = ["---"]
        for key in sorted(machine):
            lines.append(f"{key}: {json.dumps(machine[key])}")
        lines += [meta_line, blob_line, "---", "",
                  f"# {record.node} (attempt {record.attempt}, {record.status})",
                  "", "> Derived display copy — the `payload` frontmatter blob is the source of truth.",
                  "", "```json", json.dumps(record.output, indent=2, sort_keys=True), "```", ""]
        return "\n".join(lines)

    fz = _fz_for(position)
    status = _SLIPBOX_STATUS.get(record.status, "active")
    tags = ["resource", "concursus", "run_state", record.record_type]
    keywords = [
        f"node {record.node}",
        f"attempt {record.attempt}",
        record.schema or "agent output",
        f"status {record.status}",
    ]
    # Frontmatter: SlipBox display/index fields first, then the authoritative machine keys +
    # payload blob (kept so the round-trip stays exact). check_note_format ignores unknown keys.
    fm: Dict[str, object] = {
        "tags": tags,
        "keywords": keywords,
        "topics": _SLIPBOX_TOPICS,
        "language": "json",
        "date of note": date,
        "status": status,
        "building_block": _building_block_for(record),
        "folgezettel": fz,
        "lineage": [f"{trail_id}:{fz}"],
        "node": record.node,
        "attempt": str(record.attempt),
        "record_status": record.status,
        "record_type": record.record_type,
        "content_hash": record.content_hash or "",
    }
    if record.schema is not None:
        fm["schema"] = record.schema
    if record.producer is not None:
        fm["producer"] = record.producer
    if record.consumes:
        fm["consumes"] = list(record.consumes)
    if record.address is not None:
        fm["address"] = record.address
    fm["access_control_group"] = ["general"]

    lines = ["---"]
    for key, value in fm.items():
        if isinstance(value, list):
            if not value:
                lines.append(f"{key}: []")
            else:
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
        else:
            lines.append(f"{key}: {json.dumps(value)}")
    lines.append(meta_line)
    lines.append(blob_line)
    lines.append("---")
    lines.append("")
    lines.append(f"# Run State: {record.node} (attempt {record.attempt}, {record.status})")
    lines.append("")
    lines.append(
        f"The `{record.node}` node's output on this run (record type `{record.record_type}`). "
        "The authoritative value is the `payload` frontmatter blob; the JSON below is a derived, "
        "human-readable display copy."
    )
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(record.output, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    lines.append("## Related Notes")
    lines.append("")
    rel = list(related or [])
    if not rel:
        rel = ["[Run entry point](_run.md)"]  # never an orphan — always link the run entry
    for item in rel:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _note_to_record(text: str) -> Record:
    """Parse a note written by :func:`_record_to_note` back into an exact :class:`Record`.

    Reads ONLY the two authoritative frontmatter blobs — ``meta:`` (the record's metadata) and
    ``payload:`` (the output) — and ignores every SlipBox display field (tags/keywords/H1/body),
    so the round-trip is exact regardless of the on-disk *form*. Reconstructs the AgentCore-shaped
    event dict and defers to :func:`~concursus.statestore._event_to_record` — the same marshalling
    :class:`MemoryStateStore` uses, so the file and Memory backends never drift. Falls back to the
    flat machine keys for a legacy note written before the ``meta`` blob existed.
    """
    if not text.startswith("---"):
        raise ValueError("note missing frontmatter")
    _, _, rest = text.partition("---\n")
    fm_block, _, _ = rest.partition("\n---")

    meta_token = ""
    payload_token = ""
    flat: Dict[str, str] = {}
    for line in fm_block.splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped or stripped.startswith("- "):
            continue
        key, _, raw = stripped.partition(":")
        key, raw = key.strip(), raw.strip()
        if key == "meta":
            meta_token = raw
        elif key == "payload":
            payload_token = raw
        elif not raw:  # a YAML list header (``key:``) — skip its ``- item`` lines above
            continue
        else:
            try:
                flat[key] = json.loads(raw)
            except json.JSONDecodeError:
                flat[key] = raw

    meta = _decode_blob(meta_token) if meta_token else flat
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

    def __init__(self, run_dir, *, slipbox_form: bool = True, trail_id: str = "run", date: str = "") -> None:
        self._dir = Path(run_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._slipbox_form = slipbox_form
        self._trail_id = trail_id
        self._date = date
        self._records: List[Record] = []
        self._projection: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._clock: int = 0
        self._loaded = False
        self._lock = threading.RLock()
        self._depth = 0  # critical-section reentrancy depth (guarded by _lock)
        self._own_gen = -1

    @classmethod
    def from_config(
        cls, *, vault_path, session_id: str, slipbox_form: bool = True, date: str = ""
    ) -> "FileVaultStateStore":
        """Bind a run to ``<vault_path>/runs/<slug(session_id)>/`` (persistence-by-default posture).

        Emits SlipBox-conformant notes by default (``slipbox_form=True`` — validate under
        ``check_note_format.py``); pass ``slipbox_form=False`` for the lean machine schema. The
        run's ``trail_id`` (SlipBox lineage path id) is derived from ``session_id``. Callers that
        want ephemeral behaviour keep the bare :class:`InProcessStateStore` default; this is the
        explicit persistent choice (mirrors ``MemoryService.from_config``).
        """
        run_dir = Path(vault_path) / "runs" / _slug(session_id)
        return cls(run_dir, slipbox_form=slipbox_form, trail_id=_trail_id(session_id), date=date)

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
        position = len(self._records) + 1  # 1-based write order → the record's FZ position
        related = self._related_for(record)
        text = _record_to_note(
            record,
            slipbox_form=self._slipbox_form,
            position=position,
            trail_id=self._trail_id,
            date=self._date,
            related=related,
        )
        self._atomic_write(self._dir / self._note_filename(record), text)
        if self._slipbox_form:
            self._write_run_entry()

    def _related_for(self, record: Record) -> List[str]:
        """The ``## Related Notes`` links for a record's note: the run entry point plus each
        upstream producer it ``consumes`` (as a link to that producer's latest note on disk)."""
        links = ["[Run entry point](_run.md)"]
        for edge in record.consumes:
            producer = edge.partition(":")[0]
            latest = self._attempts.get(producer)
            if latest:
                fname = f"{_slug(producer)}__a{latest}.md"
                links.append(f"[consumes {producer}]({fname})")
        return links

    def _write_run_entry(self) -> None:
        """Regenerate the run's ``_run.md`` Folgezettel entry point — a SlipBox-conformant
        navigation note listing every record in the run (so no note is an orphan and the run
        reads as a genuine trail with a root)."""
        rows = [
            f"- [{r.node} a{r.attempt}]({self._note_filename(r)}) — {r.status}"
            for r in self._records
        ]
        fm = {
            "tags": ["resource", "concursus", "run_state", "entry_point"],
            "keywords": ["concursus run", "run state trail", "folgezettel entry point"],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": self._date,
            "status": "active",
            "building_block": "navigation",
            "folgezettel": "1",
            "lineage": [f"{self._trail_id}:1"],
            "access_control_group": ["general"],
        }
        lines = ["---"]
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {json.dumps(value)}")
        lines += ["---", "", "# Run State: trail entry point", "",
                  f"The Folgezettel root of this concursus run (trail `{self._trail_id}`). "
                  "Each record below is one node output, addressed as a child of this root.", ""]
        lines += rows if rows else ["- (no records yet)"]
        lines.append("")
        self._atomic_write(self._dir / "_run.md", "\n".join(lines))

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
            if note.name == "_run.md":
                continue  # the entry-point navigation note, not a record
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
