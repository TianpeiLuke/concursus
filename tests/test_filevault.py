"""Tests for the FileVaultStateStore (persistent on-disk slipbox) + the derived run DB.

Exercises the durability gap that InProcessStateStore (in-memory) and MemoryStateStore (opaque
AgentCore Blob events) leave open: round-trip-exact note serialization of arbitrary outputs,
resume-by-reload across a fresh store, StateStore-Protocol parity with InProcessStateStore, and a
derived SQLite run DB rebuilt entirely from the notes.
"""

import json
import sqlite3

import pytest

from concursus.filevault import (
    FileVaultStateStore,
    _decode_blob,
    _encode_blob,
    _note_to_record,
    _record_to_note,
)
from concursus.rundb import build_run_db, load_records
from concursus.rungraph import RunGraph
from concursus.runindex import RunIndex
from concursus.statestore import InProcessStateStore, Record


# -- round-trip exactness ---------------------------------------------------
def test_blob_roundtrip_survives_yaml_hostile_content():
    payload = {
        "text": "line1\nline2: value --- with dashes",
        "quote": 'he said "hi"',
        "link": "[a note](../x.md)",
        "numeric_string": "007",
        "nested": {"a": [1, 2, {"b": None}]},
    }
    assert _decode_blob(_encode_blob(payload)) == payload


def test_note_roundtrip_is_exact():
    rec = Record(
        node="summarize",
        output={"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123"},
        attempt=2,
        status="validated",
        schema="summarize-agent",
        producer="summarize",
        consumes=["ingest:$.document", "translate:$.text"],
        content_hash="abc",
        timestamp=5,
        address="summarize",
    )
    back = _note_to_record(_record_to_note(rec))
    assert back.node == rec.node
    assert back.output == rec.output  # exact — the whole point
    assert back.attempt == rec.attempt
    assert back.status == rec.status
    assert back.schema == rec.schema
    assert back.producer == rec.producer
    assert back.consumes == rec.consumes
    assert back.address == rec.address


# -- StateStore parity with the in-process default --------------------------
def test_put_get_completed_match_inprocess(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    ip = InProcessStateStore()
    for store in (fv, ip):
        store.put("a", {"x": 1}, meta={"producer": "a"})
        store.put("b", {"y": 2}, meta={"producer": "b", "consumes": ["a:$.x"]})
    assert fv.get("a") == ip.get("a") == {"x": 1}
    assert fv.completed() == ip.completed() == {"a", "b"}
    assert len(fv.records()) == len(ip.records()) == 2


def test_missing_node_raises_keyerror(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    with pytest.raises(KeyError):
        fv.get("nope")


def test_dedup_marks_identical_reput(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 1})  # identical -> dedup
    recs = fv.records()
    assert len(recs) == 2
    assert recs[0].record_type == "agent_output"
    assert recs[1].record_type == "dedup"
    assert recs[1].attempt == 2


def test_failed_record_excluded_from_completed(tmp_path):
    fv = FileVaultStateStore(tmp_path / "run")
    fv.put("a", {"error": "boom"}, meta={"status": "failed"})
    assert fv.completed() == set()
    with pytest.raises(KeyError):
        fv.get("a")


# -- persistence + resume ---------------------------------------------------
def test_notes_written_to_disk(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    fv.put("b", {"y": 2})
    notes = sorted(run.glob("*.md"))
    assert len(notes) == 2
    # every note is readable markdown with an authoritative payload blob
    for n in notes:
        text = n.read_text()
        assert text.startswith("---")
        assert "\npayload: b64:" in text


def test_resume_by_reload_from_fresh_store(tmp_path):
    run = tmp_path / "run"
    first = FileVaultStateStore(run)
    first.put("a", {"x": 1}, meta={"producer": "a"})
    first.put("b", {"y": 2}, meta={"producer": "b", "consumes": ["a:$.x"]})

    # A brand-new store over the same dir reconstructs the prior run's frontier and outputs.
    resumed = FileVaultStateStore(run)
    assert resumed.completed() == {"a", "b"}
    assert resumed.get("a") == {"x": 1}
    assert resumed.get("b") == {"y": 2}
    # consumes edges survived, so the run graph rebuilds
    graph = RunGraph.from_records(resumed.records())
    assert "a" in graph.upstream("b")


def test_from_config_scopes_by_session(tmp_path):
    fv = FileVaultStateStore.from_config(vault_path=tmp_path, session_id="concursus-" + "z" * 33)
    fv.put("a", {"x": 1})
    runs = list((tmp_path / "runs").iterdir())
    assert len(runs) == 1 and runs[0].is_dir()


def test_reput_after_resume_increments_attempt(tmp_path):
    run = tmp_path / "run"
    FileVaultStateStore(run).put("a", {"x": 1})
    resumed = FileVaultStateStore(run)
    resumed.put("a", {"x": 2})  # new output, next attempt
    recs = [r for r in resumed.records() if r.node == "a"]
    assert sorted(r.attempt for r in recs) == [1, 2]
    assert resumed.get("a") == {"x": 2}


# -- derived run DB ---------------------------------------------------------
def test_run_db_mirrors_records_and_edges(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    fv.put("summarize", {"summary": "s"}, meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    db_path = build_run_db(run)
    con = sqlite3.connect(db_path)
    try:
        nodes = {r[0] for r in con.execute("SELECT node FROM records")}
        assert nodes == {"ingest", "summarize"}
        edges = con.execute("SELECT producer, consumer FROM consumes_edges").fetchall()
        assert ("ingest", "summarize") in edges
        proj = dict(con.execute("SELECT node, output_json FROM projection"))
        assert json.loads(proj["summarize"]) == {"summary": "s"}
        addrs = {r[0] for r in con.execute("SELECT address FROM run_addresses")}
        assert {"ingest", "summarize"} <= addrs
    finally:
        con.close()


def test_run_db_parity_with_runindex(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("a", {"x": 1})
    fv.put("a", {"x": 1})  # dedup, attempt 2
    fv.put("b", {"error": "x"}, meta={"status": "failed"})

    db_path = build_run_db(run)
    con = sqlite3.connect(db_path)
    try:
        failed = {r[0] for r in con.execute("SELECT node FROM records WHERE status='failed'")}
    finally:
        con.close()

    idx = RunIndex.from_records(load_records(run))
    assert failed == {r.node for r in idx.query(status="failed")}
