"""The **run DB** — a derived, rebuildable SQLite graph/index over a persisted run's notes.

Concursus's :class:`~concursus.rungraph.RunGraph` and :class:`~concursus.runindex.RunIndex` are
fast, in-process, rebuilt-on-demand structures — there is no queryable-*at-rest* store. This
module adds one, mirroring the slipbox's ``build_unified_db.py`` discipline: it reads a
:class:`~concursus.filevault.FileVaultStateStore`'s note files (the **single source of truth**)
and materializes a **gitignored, disposable** SQLite DB — a metadata-postings table, the
``consumes`` data-dependency edges, the Folgezettel execution-address tree, and a
latest-validated projection VIEW. The notes stay canonical; deleting the DB loses nothing
(``build_run_db`` rebuilds it). It exists for at-rest queries — a stopped run, a dashboard,
cross-process inspection — without booting the interpreter and reloading every note.

Pure stdlib (``sqlite3``). No AWS, no third-party deps.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from .filevault import _note_to_record, iter_note_versions
from .statestore import Record, _ADDR_SEP

_LOG = logging.getLogger(__name__)


# The ``records`` table is the incremental source (one row per note, carrying its file_path +
# mtime for change-detection and its raw ``consumes`` list). Everything below it is a PURE
# DERIVATION of ``records`` — so an incremental pass upserts only changed record rows and then
# rebuilds the derived tables, while a full rebuild rebuilds everything from scratch.
_RECORDS_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    event_key         TEXT PRIMARY KEY,   -- node#attempt (a run's stable per-event id)
    node              TEXT NOT NULL,
    address           TEXT NOT NULL,
    address_parent    TEXT,
    attempt           INTEGER NOT NULL,
    status            TEXT NOT NULL,
    record_type       TEXT NOT NULL,
    schema            TEXT,
    producer          TEXT,
    supersedes        TEXT,
    content_hash      TEXT,
    timestamp         INTEGER,
    output_json       TEXT NOT NULL,
    consumes_json     TEXT NOT NULL DEFAULT '[]',  -- the record's raw consumes list (for edge derivation)
    file_path         TEXT,               -- source note (incremental-rebuild bookkeeping)
    last_indexed_mtime REAL               -- st_mtime of the note when last ingested
);
CREATE INDEX IF NOT EXISTS ix_records_node        ON records(node);
CREATE INDEX IF NOT EXISTS ix_records_status      ON records(status);
CREATE INDEX IF NOT EXISTS ix_records_record_type ON records(record_type);
CREATE INDEX IF NOT EXISTS ix_records_schema      ON records(schema);
CREATE INDEX IF NOT EXISTS ix_records_producer    ON records(producer);
CREATE INDEX IF NOT EXISTS ix_records_file_path   ON records(file_path);
"""

# The derived read-models (rebuilt from ``records`` on every pass — never a source of truth).
_PROJECTION_SCHEMA = """
CREATE TABLE consumes_edges (
    consumer  TEXT NOT NULL,   -- the node whose record declared this AgentRef
    producer  TEXT NOT NULL,   -- the upstream node it consumes
    jsonpath  TEXT             -- the JSONPath into the producer's output
);
CREATE INDEX ix_edges_producer ON consumes_edges(producer);
CREATE INDEX ix_edges_consumer ON consumes_edges(consumer);

CREATE TABLE run_addresses (
    address         TEXT PRIMARY KEY,   -- an execution-tree address (+ every ancestor prefix)
    parent_address  TEXT,
    depth           INTEGER NOT NULL
);

-- Latest validated record per node — the read-model projection, NEVER a source-of-truth table.
-- Implemented without window functions (ROW_NUMBER OVER ...) so it runs on the older SQLite that
-- ships with some Python builds (window functions need SQLite >= 3.25): a row is "latest" when no
-- other validated row for the same node has a strictly greater (attempt, timestamp) key.
CREATE VIEW projection AS
SELECT r.node, r.output_json, r.attempt, r.timestamp
FROM records r
WHERE r.status = 'validated'
  AND NOT EXISTS (
      SELECT 1 FROM records r2
      WHERE r2.node = r.node
        AND r2.status = 'validated'
        AND ( r2.attempt > r.attempt
              OR ( r2.attempt = r.attempt
                   AND COALESCE(r2.timestamp, 0) > COALESCE(r.timestamp, 0) )
              OR ( r2.attempt = r.attempt
                   AND COALESCE(r2.timestamp, 0) = COALESCE(r.timestamp, 0)
                   AND r2.event_key > r.event_key ) )
  );
"""

# Optional full-text index over the run outputs (FTS5). Created only when the SQLite build
# ships FTS5; a run without it degrades gracefully (no records_fts table, everything else works).
_FTS_SCHEMA = """
CREATE VIRTUAL TABLE records_fts USING fts5(event_key, node, output_text);
"""

# The derived index over the OPT-IN append-only note version timeline. One row per
# snapshot under ``<run_dir>/versions/<note_stem>/vNNN.md`` — a pure DERIVATION of that sidecar
# tree, DROP+recreated on every build (never a source of truth; the version notes are canonical).
# A run that never opted into versioning has an empty ``note_versions`` table — the default path is
# unchanged. ``reverted_from`` is non-NULL for a forward-revert snapshot.
_NOTE_VERSIONS_SCHEMA = """
CREATE TABLE note_versions (
    note           TEXT NOT NULL,      -- the versioned note's stem (e.g. '_run' or 'a__a1')
    version        INTEGER NOT NULL,   -- 1-based, append-only (newest = MAX per note)
    when_stamp     TEXT,               -- the 'when' provenance stamp (may be empty)
    content_hash   TEXT,               -- hash of the snapshotted content
    reverted_from  INTEGER,            -- source version for a forward revert; NULL otherwise
    file_path      TEXT,               -- the snapshot note on disk
    PRIMARY KEY (note, version)
);
CREATE INDEX ix_note_versions_note ON note_versions(note);
"""


def _event_key(record: Record) -> str:
    return f"{record.node}#{record.attempt}"


def _record_note_paths(run_dir) -> List[Path]:
    """Every record note under ``run_dir`` (excludes the ``_run.md`` navigation entry point)."""
    return [p for p in sorted(Path(run_dir).glob("*.md")) if p.name != "_run.md"]


def load_records(run_dir) -> List[Record]:
    """Read every note file under ``run_dir`` into a timestamp-ordered list of records."""
    records: List[Record] = []
    for note in _record_note_paths(run_dir):
        try:
            records.append(_note_to_record(note.read_text(encoding="utf-8")))
        except Exception:
            continue
    records.sort(key=lambda r: (r.timestamp if r.timestamp is not None else 0))
    return records


def _load_records_with_provenance(run_dir):
    """Yield ``(record, file_path, mtime)`` for each readable record note under ``run_dir``."""
    out = []
    for note in _record_note_paths(run_dir):
        try:
            rec = _note_to_record(note.read_text(encoding="utf-8"))
        except Exception:
            continue
        out.append((rec, str(note), note.stat().st_mtime))
    out.sort(key=lambda t: (t[0].timestamp if t[0].timestamp is not None else 0))
    return out


def _fts5_available(con: sqlite3.Connection) -> bool:
    """True if this SQLite build has the FTS5 extension (optional, degrades gracefully)."""
    try:
        con.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        con.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


def _pragma_plan(con: sqlite3.Connection) -> None:
    """Apply the standard connection pragmas to a freshly-opened ``sqlite3`` connection.

    WAL journaling (readers stay unblocked during a write), ``synchronous=NORMAL`` (durable enough
    for a disposable projection — a lost tail is just re-derived from the note SSOT), and
    ``foreign_keys=ON``. Called right after every :func:`sqlite3.connect` in this module. It is a
    pure, side-effect-only-on-the-connection helper: idempotent, a no-op on a DB already in WAL,
    and it NEVER raises — a filesystem that rejects a pragma (e.g. some network mounts, or a
    corrupt file about to be self-healed) simply keeps that pragma's default, and the DB still
    builds correctly. Query results are unaffected, so the default code path is unchanged.
    """
    for pragma in ("journal_mode=WAL", "synchronous=NORMAL", "foreign_keys=ON"):
        try:
            con.execute(f"PRAGMA {pragma}")
        except sqlite3.DatabaseError:
            pass


def _quick_check_ok(con: sqlite3.Connection) -> bool:
    """True if ``PRAGMA quick_check`` reports the DB intact; False if SQLite reports it damaged.

    A brand-new/empty DB reports ``ok`` (nothing to check), so this returns False only for a
    genuinely corrupt file — the signal for :func:`build_run_db` to rebuild from the note SSOT
    rather than trust (and propagate) a poisoned projection.
    """
    try:
        rows = con.execute("PRAGMA quick_check").fetchall()
    except sqlite3.DatabaseError:
        return False
    return len(rows) == 1 and rows[0][0] == "ok"


def _discard_corrupt_db(db_path: str) -> None:
    """Delete a corrupt projection DB (and any WAL/SHM/journal sidecars) so it rebuilds cleanly.

    Safe because the DB is a disposable derivation — the note files are the untouched SSOT — so
    nothing is lost. Used by :func:`build_run_db`'s self-heal path.
    """
    base = Path(db_path)
    for p in (base, Path(f"{base}-wal"), Path(f"{base}-shm"), Path(f"{base}-journal")):
        p.unlink(missing_ok=True)


def _rebuild_note_versions(con: sqlite3.Connection, run_dir) -> None:
    """(Re)derive the ``note_versions`` index from the OPT-IN ``versions/`` timeline tree.

    Pure projection of the append-only version notes on disk (canonical). DROP+recreated every
    build. A run that never opted into versioning yields no rows, so the table is simply empty —
    the default (unversioned) code path is unaffected.
    """
    con.executescript("DROP TABLE IF EXISTS note_versions;")
    con.executescript(_NOTE_VERSIONS_SCHEMA)
    for note_stem, v in iter_note_versions(run_dir):
        con.execute(
            "INSERT OR REPLACE INTO note_versions VALUES (?,?,?,?,?,?)",
            (
                note_stem,
                int(v.get("version") or 0),
                v.get("when") or None,
                v.get("content_hash") or None,
                v.get("reverted_from"),
                v.get("file_path"),
            ),
        )


def _rebuild_derived_tables(con: sqlite3.Connection, run_dir=None) -> None:
    """(Re)derive consumes_edges, run_addresses, the projection VIEW, (if available) the FTS
    index, and — when ``run_dir`` is given — the opt-in ``note_versions`` timeline index; all pure
    projections OF the ``records`` table (or, for ``note_versions``, of the ``versions/`` tree).

    An incremental pass upserts only changed ``records`` rows, then calls this to rebuild the
    derived read-models; a full rebuild rebuilds ``records`` too. Either way the derived tables
    are a deterministic function of ``records``, so the result is identical.
    """
    con.executescript(
        "DROP VIEW IF EXISTS projection;"
        "DROP TABLE IF EXISTS consumes_edges;"
        "DROP TABLE IF EXISTS run_addresses;"
        "DROP TABLE IF EXISTS records_fts;"
    )
    con.executescript(_PROJECTION_SCHEMA)
    has_fts = _fts5_available(con)
    if has_fts:
        con.executescript(_FTS_SCHEMA)

    for event_key, node, addr, consumes_json, output_json in con.execute(
        "SELECT event_key, node, address, consumes_json, output_json FROM records"
    ).fetchall():
        # consumes edges — the AgentRef data-dependency graph at rest.
        try:
            consumes = json.loads(consumes_json)
        except Exception:
            consumes = []
        for edge in consumes:
            prod, _, path = str(edge).partition(":")
            con.execute(
                "INSERT INTO consumes_edges VALUES (?,?,?)", (node, prod, path or None)
            )
        # execution-tree addresses — the address and every ancestor prefix.
        parts = addr.split(_ADDR_SEP)
        for i in range(1, len(parts) + 1):
            a = _ADDR_SEP.join(parts[:i])
            p = a.rsplit(_ADDR_SEP, 1)[0] if _ADDR_SEP in a else None
            con.execute(
                "INSERT OR REPLACE INTO run_addresses VALUES (?,?,?)",
                (a, p, a.count(_ADDR_SEP)),
            )
        # optional full-text row over the output payload.
        if has_fts:
            con.execute(
                "INSERT INTO records_fts (event_key, node, output_text) VALUES (?,?,?)",
                (event_key, node, output_json),
            )
    # The opt-in append-only note version timeline index (empty for an unversioned run).
    if run_dir is not None:
        _rebuild_note_versions(con, run_dir)
    con.commit()


_PRECEDENT_SCHEMA = """
CREATE TABLE precedents (
    trail_id   TEXT PRIMARY KEY,   -- one row per distilled run/family
    status     TEXT NOT NULL,      -- completed | partial | failed (derived verdict)
    total      INTEGER NOT NULL,
    completed  INTEGER NOT NULL,
    n_failed   INTEGER NOT NULL,
    nodes_json TEXT NOT NULL,      -- the executed node set
    payload_json TEXT NOT NULL     -- the full precedent payload (source of truth is the note)
);
CREATE INDEX ix_precedents_status ON precedents(status);
"""


def build_precedent_db(vault_path, db_path: Optional[str] = None) -> str:
    """Rebuild the derived cross-run precedent DB from the precedent notes; return the DB path.

    The at-rest analogue of :func:`~concursus.distill.render_precedent_hub`: reads ONLY the notes
    under ``<vault>/precedents/`` (the source of truth), DROP+recreates the table (a pure
    projection), and writes ``<vault>/precedents/index/precedents.sqlite`` by default. It is a
    read-only *retrieval index* over finished runs — never a live router; deleting it loses
    nothing. Idempotent and disposable.
    """
    from .distill import load_precedents, precedents_dir

    vault_path = Path(vault_path)
    if db_path is None:
        index_dir = precedents_dir(vault_path) / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        db_path = str(index_dir / "precedents.sqlite")

    con = sqlite3.connect(db_path)
    _pragma_plan(con)
    try:
        con.executescript("DROP TABLE IF EXISTS precedents;")
        con.executescript(_PRECEDENT_SCHEMA)
        for r in load_precedents(vault_path):
            payload = r.output if isinstance(r.output, dict) else {}
            trail_id = str(payload.get("trail_id") or r.node)
            outcome = payload.get("outcome") or {}
            failed = outcome.get("failed") or {}
            con.execute(
                "INSERT OR REPLACE INTO precedents VALUES (?,?,?,?,?,?,?)",
                (
                    trail_id,
                    str(payload.get("status", "")),
                    int(outcome.get("total", 0) or 0),
                    int(outcome.get("completed", 0) or 0),
                    len(failed),
                    json.dumps(payload.get("nodes") or [], sort_keys=True),
                    json.dumps(payload, sort_keys=True),
                ),
            )
        con.commit()
    finally:
        con.close()
    return db_path


def _upsert_record_row(con: sqlite3.Connection, r: Record, file_path: str, mtime: float) -> None:
    addr = r.address or r.node
    parent = addr.rsplit(_ADDR_SEP, 1)[0] if _ADDR_SEP in addr else None
    con.execute(
        "INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            _event_key(r),
            r.node,
            addr,
            parent,
            r.attempt,
            r.status,
            r.record_type,
            r.schema,
            r.producer,
            r.supersedes,
            r.content_hash,
            r.timestamp,
            json.dumps(r.output, sort_keys=True),
            json.dumps(list(r.consumes or []), sort_keys=True),
            file_path,
            mtime,
        ),
    )


def build_run_db(
    run_dir, db_path: Optional[str] = None, *, incremental: bool = True
) -> str:
    """Build/refresh the derived SQLite DB for one run from its note files; return the DB path.

    Reads ONLY the notes (the source of truth) and writes ``<run_dir>/index/run.sqlite`` by
    default. The DB is a pure, disposable projection — deleting it loses nothing.

    - ``incremental=True`` (default): keep the existing ``records`` rows, re-ingest only the
      notes whose ``file_path`` is new or whose ``st_mtime`` changed (mirroring the vault's
      ``build_unified_db`` mtime-keyed discipline), drop rows for notes that vanished, then
      rebuild the derived read-models (``consumes_edges`` / ``run_addresses`` / ``projection`` /
      the optional ``records_fts``). The result is byte-for-byte identical to a full rebuild.
    - ``incremental=False``: DROP+recreate everything from scratch.

    **Self-healing:** before an incremental pass over a *pre-existing* DB file, the DB is probed
    with ``PRAGMA quick_check``; if SQLite reports corruption the poisoned file (and its WAL/SHM
    sidecars) is discarded and the run is rebuilt from scratch off the untouched note SSOT rather
    than raising or propagating bad rows. A healthy DB never takes this path, so its behaviour is
    unchanged.
    """
    run_dir = Path(run_dir)
    if db_path is None:
        index_dir = run_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        db_path = str(index_dir / "run.sqlite")

    # Self-heal: an incremental pass trusts the existing DB, so verify it is intact first. A
    # corrupt projection is disposable — drop it and fall back to a from-scratch rebuild off the
    # notes (the SSOT), losing nothing. (A full rebuild already starts empty, so it is exempt.)
    if incremental and Path(db_path).exists():
        probe = sqlite3.connect(db_path)
        _pragma_plan(probe)
        try:
            healthy = _quick_check_ok(probe)
        finally:
            probe.close()
        if not healthy:
            _discard_corrupt_db(db_path)
            incremental = False

    con = sqlite3.connect(db_path)
    _pragma_plan(con)
    try:
        if not incremental:
            con.executescript("DROP TABLE IF EXISTS records;")
        con.executescript(_RECORDS_SCHEMA)

        # Current on-disk mtimes vs. what the DB last indexed → the incremental delta.
        prior = {
            row[0]: (row[1], row[2])  # file_path -> (event_key, last_indexed_mtime)
            for row in con.execute(
                "SELECT file_path, event_key, last_indexed_mtime FROM records"
            ).fetchall()
            if row[0] is not None
        }
        seen_paths: set[str] = set()
        for r, file_path, mtime in _load_records_with_provenance(run_dir):
            seen_paths.add(file_path)
            was = prior.get(file_path)
            if incremental and was is not None and was[1] == mtime:
                continue  # unchanged note — leave its row in place
            _upsert_record_row(con, r, file_path, mtime)

        # Drop rows whose source note disappeared (incremental only; full rebuild started empty).
        if incremental:
            for stale in set(prior) - seen_paths:
                con.execute("DELETE FROM records WHERE file_path = ?", (stale,))
        con.commit()

        _rebuild_derived_tables(con, run_dir)
    finally:
        con.close()
    return db_path


# --------------------------------------------------------------------------- at-rest snapshot read
# ``get_run_snapshot`` is the at-rest, cross-process analogue of ``DirectorCockpit.snapshot()``: a
# single OFFLINE read over one run's note SSOT (via :func:`load_records`) that returns an ordered,
# JSON-serializable slice — optionally narrowed to one agent/node and/or a step window. It is a PURE
# read projection: it opens no live plan, drives no dispatch, mutates nothing, and pulls no
# boto3/langgraph. A run whose notes are absent yields an empty (but well-formed) snapshot.
#
# A "step" is the 1-based ORDINAL of a record in the run's canonical AT-REST order — the
# deterministic order :func:`load_records` returns (address then attempt; the append-only note
# metadata does NOT persist a global wall-clock sequence, so this is a stable structural index, not
# an execution clock). ``step=3`` is the third record in that order and ``step=(2, 4)`` is that
# inclusive window. Ordinals are assigned over the WHOLE run before the agent filter, so a step
# number names the same record regardless of which agent it is scoped to.
_REDACTED = "[REDACTED]"


def _normalize_step_window(step: Any) -> tuple:
    """Coerce the ``step`` arg into an inclusive ``(lo, hi)`` 1-based window (``None`` = unbounded).

    ``None`` → ``(None, None)`` (every step); an ``int`` → ``(step, step)`` (that single step); a
    2-element ``(lo, hi)`` tuple/list → that inclusive window, where either bound may be ``None`` for
    an open-ended side. Any other shape raises ``ValueError`` (a caller passing a malformed window
    should hear about it rather than silently get the whole log)."""
    if step is None:
        return (None, None)
    if isinstance(step, (tuple, list)):
        if len(step) != 2:
            raise ValueError(f"step window must be a (lo, hi) pair, got {step!r}")
        lo, hi = step
        return (None if lo is None else int(lo), None if hi is None else int(hi))
    return (int(step), int(step))


def _record_projection(record: Record, step: int) -> Dict[str, Any]:
    """Project one :class:`Record` to a JSON-serializable snapshot row (display copy, read-only).

    ``status``/``record_type`` are forced to plain ``str`` (they are ``str``-subclass enums) so the
    row serializes cleanly, and ``consumes`` is copied to a fresh list. ``output`` is the verbatim
    agent output already decoded from the note's authoritative blob."""
    return {
        "step": step,
        "node": record.node,
        "address": record.address or record.node,
        "attempt": record.attempt,
        "status": str(record.status),
        "record_type": str(record.record_type),
        "schema": record.schema,
        "producer": record.producer,
        "consumes": list(record.consumes or []),
        "content_hash": record.content_hash,
        "timestamp": record.timestamp,
        "output": record.output,
    }


def get_run_snapshot(run_id, *, agent: Optional[str] = None, step: Any = None) -> Dict[str, Any]:
    """Return one run's ordered, JSON-serializable snapshot — optionally filtered by agent/step.

    A single OFFLINE read over the run's note SSOT: ``run_id`` is the run directory (the
    ``FileVaultStateStore`` run dir whose ``*.md`` notes are the single source of truth). Records are
    loaded via :func:`load_records` (the canonical deterministic at-rest order) and each is assigned
    a 1-based ``step`` ordinal over the WHOLE run; the agent/node filter runs through the derived
    :class:`~concursus.state.runindex.RunIndex` metadata index, and the step window is applied on
    top. Returns::

        {"run_id", "agent", "step", "total", "count", "records": [<row>, ...]}

    where ``records`` are the selected rows in step order and ``total`` is the full log length. It is
    a PURE read projection (INV-5): it re-derives everything from the append-only notes on each call,
    opens no live plan, drives no dispatch, and mutates nothing — the at-rest, cross-process analogue
    of :meth:`DirectorCockpit.snapshot`. An absent/empty run dir yields an empty snapshot."""
    from .runindex import RunIndex  # lazy: keeps rundb import-light; runindex → statestore only

    records = load_records(run_id)  # timestamp-ordered read over the note SSOT (offline)
    step_of = {id(r): i for i, r in enumerate(records, start=1)}
    pool = RunIndex.from_records(records).query(node=agent) if agent is not None else records

    lo, hi = _normalize_step_window(step)
    rows = [
        _record_projection(r, step_of[id(r)])
        for r in pool
        if (lo is None or step_of[id(r)] >= lo) and (hi is None or step_of[id(r)] <= hi)
    ]
    rows.sort(key=lambda row: row["step"])
    return {
        "run_id": str(run_id),
        "agent": agent,
        "step": list(step) if isinstance(step, (tuple, list)) else step,
        "total": len(records),
        "count": len(rows),
        "records": rows,
    }


def redact_snapshot(snapshot: Any, pattern: Any) -> Any:
    """Return a deep copy of ``snapshot`` with every ``pattern`` match masked as ``[REDACTED]``.

    An OPTIONAL egress guard for :func:`get_run_snapshot` output (or any JSON-serializable value): a
    single compiled pattern is applied to every string in the structure (dict values, list items,
    nested), each match replaced with the ``[REDACTED]`` sentinel. ``pattern`` may be a ``str``
    (compiled here) or an already-compiled ``re.Pattern``. When at least one match is masked a WARN
    is logged (so an operator sees that egress carried a secret), and the count is included. Pure and
    read-only: it copies rather than mutating its input and touches no disk/network."""
    compiled = re.compile(pattern) if isinstance(pattern, (str, bytes)) else pattern
    masked = 0

    def _walk(value: Any) -> Any:
        nonlocal masked
        if isinstance(value, str):
            new, n = compiled.subn(_REDACTED, value)
            masked += n
            return new
        if isinstance(value, dict):
            return {k: _walk(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_walk(v) for v in value]
        return value

    redacted = _walk(snapshot)
    if masked:
        _LOG.warning(
            "redact_snapshot masked %d match(es) of pattern %r before egress",
            masked,
            getattr(compiled, "pattern", pattern),
        )
    return redacted
