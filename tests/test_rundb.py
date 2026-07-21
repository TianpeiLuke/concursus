"""Tests for the derived run DB (rundb) — AI-9: incremental rebuild + optional FTS.

The DB is a pure, disposable projection over a FileVault run's note files. These tests pin the
AI-9 contract: an incremental refresh re-ingests only changed notes, drops rows for vanished
notes, yields byte-for-byte the same query results as a full rebuild, and (when the SQLite build
ships FTS5) exposes a full-text index that degrades gracefully when it does not.
"""

from __future__ import annotations

import os
import sqlite3

from concursus.state.filevault import FileVaultStateStore
from concursus.state.rundb import (
    _fts5_available,
    _pragma_plan,
    _quick_check_ok,
    build_run_db,
)


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


# -- (2) _pragma_plan: WAL / synchronous=NORMAL / foreign_keys=ON, applied after connect --------
def test_pragma_plan_sets_wal_synchronous_and_foreign_keys(tmp_path):
    """The helper flips a fresh connection to WAL + NORMAL + foreign_keys, and is idempotent."""
    db_path = str(tmp_path / "pragma.sqlite")
    con = sqlite3.connect(db_path)
    try:
        _pragma_plan(con)
        assert con.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert con.execute("PRAGMA synchronous").fetchone()[0] == 1  # NORMAL
        assert con.execute("PRAGMA foreign_keys").fetchone()[0] == 1  # ON
        # Idempotent: re-applying on an already-WAL connection is a harmless no-op.
        _pragma_plan(con)
        assert con.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        con.close()


def test_build_run_db_applies_pragma_plan(tmp_path):
    """A DB built by build_run_db persists in WAL mode (the pragma plan was applied on connect)."""
    run_dir = _run(tmp_path, "concursus-" + "e" * 40)
    db_path = build_run_db(run_dir, str(tmp_path / "run.sqlite"), incremental=True)

    con = sqlite3.connect(db_path)
    try:
        # WAL is a persistent, on-disk property of the database file — a fresh reader sees it.
        assert con.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        con.close()


# -- (1) self-healing: a corrupt DB is dropped + rebuilt from the note SSOT, not raised ----------
def _corrupt(db_path):
    """Overwrite a SQLite file with garbage so PRAGMA quick_check reports damage."""
    with open(db_path, "wb") as fh:
        fh.write(b"this is not a valid sqlite database header at all " * 64)


def test_quick_check_flags_corruption(tmp_path):
    """_quick_check_ok is True for a healthy DB and False for a corrupt one."""
    run_dir = _run(tmp_path, "concursus-" + "f" * 40)
    db_path = build_run_db(run_dir, str(tmp_path / "run.sqlite"), incremental=True)

    con = sqlite3.connect(db_path)
    try:
        assert _quick_check_ok(con) is True
    finally:
        con.close()

    _corrupt(db_path)
    con = sqlite3.connect(db_path)
    try:
        assert _quick_check_ok(con) is False
    finally:
        con.close()


def test_incremental_self_heals_corrupt_db(tmp_path):
    """An incremental pass over a CORRUPT DB drops it and rebuilds from notes — no exception,
    and the result matches a from-scratch rebuild off the same (untouched) note SSOT."""
    run_dir = _run(tmp_path, "concursus-" + "g" * 40)
    db_path = str(tmp_path / "run.sqlite")
    build_run_db(run_dir, db_path, incremental=True)  # a healthy DB first

    _corrupt(db_path)  # simulate a torn write / bit-rot

    # Must NOT raise; must recover the full projection from the note files.
    healed = build_run_db(run_dir, db_path, incremental=True)
    assert healed == db_path

    # The rebuilt DB is intact and byte-for-byte equal to a fresh full rebuild.
    full_db = build_run_db(run_dir, str(tmp_path / "full.sqlite"), incremental=False)
    assert _snapshot(healed) == _snapshot(full_db)
    recs, edges, _addrs, proj = _snapshot(healed)
    assert len(recs) == 2
    assert ("summarize", "ingest", "$.document") in edges
    assert len(proj) == 2


def test_healthy_incremental_path_is_unchanged(tmp_path):
    """The self-heal probe leaves a HEALTHY DB's incremental behaviour identical: an unchanged
    note is not re-ingested (its recorded mtime is preserved across the second pass)."""
    run_dir = _run(tmp_path, "concursus-" + "h" * 40)
    db_path = str(tmp_path / "run.sqlite")
    build_run_db(run_dir, db_path, incremental=True)

    con = sqlite3.connect(db_path)
    before = dict(
        con.execute("SELECT file_path, last_indexed_mtime FROM records WHERE file_path IS NOT NULL")
    )
    con.close()

    # Nothing changed on disk → a second incremental pass must preserve every recorded mtime
    # (i.e. it took the normal incremental path, not a corruption-triggered full rebuild).
    build_run_db(run_dir, db_path, incremental=True)

    con = sqlite3.connect(db_path)
    after = dict(
        con.execute("SELECT file_path, last_indexed_mtime FROM records WHERE file_path IS NOT NULL")
    )
    con.close()
    assert after == before


# -- derived note_versions index over the opt-in timeline ---------
def test_note_versions_table_empty_for_unversioned_run(tmp_path):
    """A run that never opted into versioning has an EMPTY note_versions table (default path
    unchanged) — the table exists but the run's other read-models are unaffected."""
    run_dir = _run(tmp_path, "concursus-" + "i" * 40)  # default store, no versions/
    db_path = build_run_db(run_dir, str(tmp_path / "run.sqlite"), incremental=True)
    con = sqlite3.connect(db_path)
    try:
        assert con.execute("SELECT COUNT(*) FROM note_versions").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 2
    finally:
        con.close()


def test_note_versions_index_mirrors_the_timeline(tmp_path):
    """With a versioned store, the derived note_versions index has one row per append-only snapshot,
    carries the typed provenance, and a forward-revert row stamps ``reverted_from``."""
    from concursus.state.filevault import revert_note

    run_dir = tmp_path / "run"
    store = FileVaultStateStore(run_dir, versioned=True)
    store.put("a", {"x": 1})
    store.put("b", {"y": 2})  # _run.md now has 2 versions
    revert_note(run_dir, "_run.md", 1)  # forward revert -> a 3rd version stamped reverted_from=1

    db_path = build_run_db(run_dir, str(tmp_path / "run.sqlite"), incremental=True)
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT version, reverted_from FROM note_versions WHERE note = '_run' ORDER BY version"
        ).fetchall()
        assert rows == [(1, None), (2, None), (3, 1)]
        # every snapshot row points at an on-disk file
        paths = [r[0] for r in con.execute("SELECT file_path FROM note_versions")]
        assert all(p and os.path.exists(p) for p in paths)
    finally:
        con.close()
