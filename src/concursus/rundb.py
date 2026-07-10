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
import sqlite3
from pathlib import Path
from typing import List, Optional

from .filevault import _note_to_record
from .statestore import Record, _ADDR_SEP


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
CREATE VIEW projection AS
SELECT node, output_json, attempt, timestamp
FROM (
    SELECT node, output_json, attempt, timestamp,
           ROW_NUMBER() OVER (
               PARTITION BY node ORDER BY attempt DESC, timestamp DESC
           ) AS rn
    FROM records
    WHERE status = 'validated'
)
WHERE rn = 1;
"""

# Optional full-text index over the run outputs (FTS5). Created only when the SQLite build
# ships FTS5; a run without it degrades gracefully (no records_fts table, everything else works).
_FTS_SCHEMA = """
CREATE VIRTUAL TABLE records_fts USING fts5(event_key, node, output_text);
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


def _rebuild_derived_tables(con: sqlite3.Connection) -> None:
    """(Re)derive consumes_edges, run_addresses, the projection VIEW, and (if available) the FTS
    index — all pure projections OF the ``records`` table.

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
    """
    run_dir = Path(run_dir)
    if db_path is None:
        index_dir = run_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        db_path = str(index_dir / "run.sqlite")

    con = sqlite3.connect(db_path)
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

        _rebuild_derived_tables(con)
    finally:
        con.close()
    return db_path
