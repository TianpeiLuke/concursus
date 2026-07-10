"""Tests for the derived run DB (rundb) — AI-9: incremental rebuild + optional FTS.

The DB is a pure, disposable projection over a FileVault run's note files. These tests pin the
AI-9 contract: an incremental refresh re-ingests only changed notes, drops rows for vanished
notes, yields byte-for-byte the same query results as a full rebuild, and (when the SQLite build
ships FTS5) exposes a full-text index that degrades gracefully when it does not.
"""

from __future__ import annotations

import os
import sqlite3

from concursus.filevault import FileVaultStateStore
from concursus.rundb import _fts5_available, build_run_db


def _run(vault, session, *, slipbox_form=False):
    """Drive a tiny 2-node run to completion on a FileVault store; return the run dir."""
    store = FileVaultStateStore.from_config(
        vault_path=vault, session_id=session, slipbox_form=slipbox_form, date="2026-07-10"
    )
    store.put("ingest", {"document": "alpha beta"}, meta={"producer": "ingest"})
    store.put(
        "summarize",
        {"summary": "gamma delta"},
        meta={"producer": "summarize", "consumes": ["ingest:$.document"]},
    )
    return store._dir


def _snapshot(db_path):
    """A comparable snapshot of every derived read-model, order-independent."""
    con = sqlite3.connect(db_path)
    try:
        recs = sorted(
            con.execute(
                "SELECT event_key, node, address, attempt, status, record_type, "
                "schema, producer, output_json FROM records"
            ).fetchall()
        )
        edges = sorted(con.execute("SELECT consumer, producer, jsonpath FROM consumes_edges").fetchall())
        addrs = sorted(con.execute("SELECT address, parent_address, depth FROM run_addresses").fetchall())
        proj = sorted(con.execute("SELECT node, output_json FROM projection").fetchall())
    finally:
        con.close()
    return recs, edges, addrs, proj


def test_incremental_matches_full_rebuild(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "a" * 40)

    inc_db = build_run_db(run_dir, str(tmp_path / "inc.sqlite"), incremental=True)
    full_db = build_run_db(run_dir, str(tmp_path / "full.sqlite"), incremental=False)

    assert _snapshot(inc_db) == _snapshot(full_db)
    # Both captured the run: 2 records, the one consumes edge, the projection.
    recs, edges, _addrs, proj = _snapshot(inc_db)
    assert len(recs) == 2
    assert ("summarize", "ingest", "$.document") in edges
    assert len(proj) == 2


def test_incremental_reingest_only_changed_notes(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "b" * 40)
    db_path = str(tmp_path / "run.sqlite")

    build_run_db(run_dir, db_path, incremental=True)  # first full-ish pass

    # Bump one note's mtime into the future and record which rows the DB thinks it indexed.
    notes = [p for p in run_dir.glob("*.md") if p.name != "_run.md"]
    target = notes[0]
    future = target.stat().st_mtime + 1000
    os.utime(target, (future, future))

    con = sqlite3.connect(db_path)
    before = dict(
        con.execute("SELECT file_path, last_indexed_mtime FROM records WHERE file_path IS NOT NULL")
    )
    con.close()

    build_run_db(run_dir, db_path, incremental=True)  # second pass — only `target` changed

    con = sqlite3.connect(db_path)
    after = dict(
        con.execute("SELECT file_path, last_indexed_mtime FROM records WHERE file_path IS NOT NULL")
    )
    con.close()

    # The touched note's recorded mtime advanced; the untouched one did not.
    assert after[str(target)] == future
    for p, m in before.items():
        if p != str(target):
            assert after[p] == m


def test_incremental_drops_rows_for_vanished_notes(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "c" * 40)
    db_path = str(tmp_path / "run.sqlite")
    build_run_db(run_dir, db_path, incremental=True)

    con = sqlite3.connect(db_path)
    assert con.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 2
    con.close()

    # Delete one record note and refresh incrementally.
    notes = [p for p in run_dir.glob("*.md") if p.name != "_run.md"]
    notes[0].unlink()
    build_run_db(run_dir, db_path, incremental=True)

    con = sqlite3.connect(db_path)
    remaining = con.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    con.close()
    assert remaining == 1


def test_fts_search_finds_run_output_or_degrades(tmp_path):
    run_dir = _run(tmp_path, "concursus-" + "d" * 40)
    db_path = build_run_db(run_dir, str(tmp_path / "run.sqlite"), incremental=True)

    con = sqlite3.connect(db_path)
    try:
        if not _fts5_available(con):
            # Graceful degradation: no FTS table, but everything else built fine.
            names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
            assert "records_fts" not in names
            assert "records" in names and "consumes_edges" in names
            return
        hits = con.execute(
            "SELECT node FROM records_fts WHERE records_fts MATCH ?", ("gamma",)
        ).fetchall()
        assert ("summarize",) in hits
        # A term from the other node's output matches that node.
        hits2 = con.execute(
            "SELECT node FROM records_fts WHERE records_fts MATCH ?", ("alpha",)
        ).fetchall()
        assert ("ingest",) in hits2
    finally:
        con.close()
