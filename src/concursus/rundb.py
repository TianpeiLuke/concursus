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


_SCHEMA = """
CREATE TABLE records (
    event_key       TEXT PRIMARY KEY,   -- node#attempt (a run's stable per-event id)
    node            TEXT NOT NULL,
    address         TEXT NOT NULL,
    address_parent  TEXT,
    attempt         INTEGER NOT NULL,
    status          TEXT NOT NULL,
    record_type     TEXT NOT NULL,
    schema          TEXT,
    producer        TEXT,
    supersedes      TEXT,
    content_hash    TEXT,
    timestamp       INTEGER,
    output_json     TEXT NOT NULL
);
CREATE INDEX ix_records_node        ON records(node);
CREATE INDEX ix_records_status      ON records(status);
CREATE INDEX ix_records_record_type ON records(record_type);
CREATE INDEX ix_records_schema      ON records(schema);
CREATE INDEX ix_records_producer    ON records(producer);

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


def _event_key(record: Record) -> str:
    return f"{record.node}#{record.attempt}"


def load_records(run_dir) -> List[Record]:
    """Read every note file under ``run_dir`` into a timestamp-ordered list of records."""
    records: List[Record] = []
    for note in sorted(Path(run_dir).glob("*.md")):
        try:
            records.append(_note_to_record(note.read_text(encoding="utf-8")))
        except Exception:
            continue
    records.sort(key=lambda r: (r.timestamp if r.timestamp is not None else 0))
    return records


def build_run_db(run_dir, db_path: Optional[str] = None) -> str:
    """Rebuild the derived SQLite DB for one run from its note files; return the DB path.

    Reads ONLY the notes (the source of truth), DROP+recreates every table (they are pure
    projections), and writes ``<run_dir>/index/run.sqlite`` by default. Idempotent and disposable.
    """
    run_dir = Path(run_dir)
    if db_path is None:
        index_dir = run_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        db_path = str(index_dir / "run.sqlite")

    records = load_records(run_dir)

    con = sqlite3.connect(db_path)
    try:
        con.executescript(
            "DROP VIEW IF EXISTS projection;"
            "DROP TABLE IF EXISTS records;"
            "DROP TABLE IF EXISTS consumes_edges;"
            "DROP TABLE IF EXISTS run_addresses;"
        )
        con.executescript(_SCHEMA)

        addresses: set[str] = set()
        for r in records:
            addr = r.address or r.node
            parent = addr.rsplit(_ADDR_SEP, 1)[0] if _ADDR_SEP in addr else None
            con.execute(
                "INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                ),
            )
            for edge in r.consumes:
                prod, _, path = edge.partition(":")
                con.execute(
                    "INSERT INTO consumes_edges VALUES (?,?,?)", (r.node, prod, path or None)
                )
            # Register the address and every ancestor prefix (mirrors RunIndex._addresses).
            parts = addr.split(_ADDR_SEP)
            for i in range(1, len(parts) + 1):
                a = _ADDR_SEP.join(parts[:i])
                p = a.rsplit(_ADDR_SEP, 1)[0] if _ADDR_SEP in a else None
                addresses.add(a)
                con.execute(
                    "INSERT OR REPLACE INTO run_addresses VALUES (?,?,?)",
                    (a, p, a.count(_ADDR_SEP)),
                )
        con.commit()
    finally:
        con.close()
    return db_path
