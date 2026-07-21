"""Tests for the memory loop — AI-15 post-run distillation + AI-16 cross-run precedent hub.

Both halves are PURE POST-RUN over the FileVault substrate: :func:`distill_run` writes one
precedent note per finished run (round-trips through the same marshalling as run records), and
:func:`render_precedent_hub` / :class:`PrecedentIndex` / :func:`build_precedent_db` are read-only
projections over the SET of precedent notes (one row per run, idempotent). Neither ever feeds back
into a running plan: precedents live under ``<vault>/precedents/`` (a sibling of ``runs/``), so
they are never globbed back as run records and can never leak into a resume/replay.
"""

import json
import sqlite3

from concursus.state.distill import (
    build_precedent_payload,
    distill_run,
    distill_store,
    load_precedents,
    precedents_dir,
    render_precedent_hub,
)
from concursus.state.filevault import FileVaultStateStore, _note_to_record
from concursus.state.rundb import build_precedent_db, build_run_db, load_records
from concursus.state.runindex import PrecedentIndex
from concursus.state.statestore import Record


def _finished_run(vault, session, *, slipbox_form=False):
    """Drive a tiny 2-node run to completion on a FileVault store and return the store."""
    store = FileVaultStateStore.from_config(
        vault_path=vault, session_id=session, slipbox_form=slipbox_form, date="2026-07-10"
    )
    store.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    store.put(
        "summarize",
        {"summary": "s"},
        meta={"producer": "summarize", "consumes": ["ingest:$.document"]},
    )
    return store


# -- AI-15: the write half --------------------------------------------------
def test_build_precedent_payload_captures_graph_and_outcome():
    records = [
        Record(node="ingest", output={"document": "d"}, producer="ingest"),
        Record(
            node="summarize",
            output={"summary": "s"},
            producer="summarize",
            consumes=["ingest:$.document"],
        ),
    ]
    result = {"ingest": {"document": "d"}, "summarize": {"summary": "s"}}
    payload = build_precedent_payload(result, records, trail_id="run_x")
    assert payload["trail_id"] == "run_x"
    assert payload["status"] == "completed"
    assert payload["outcome"]["total"] == 2 and payload["outcome"]["completed"] == 2
    assert ["summarize", "ingest", "$.document"] in payload["consumes"]
    assert payload["results"] == result


def test_partial_run_status_when_a_node_failed():
    records = [
        Record(node="a", output={"x": 1}),
        Record(node="b", output={}, status="failed", blocked_on="blocked on a"),
    ]
    payload = build_precedent_payload({"a": {"x": 1}}, records, trail_id="r")
    assert payload["status"] == "partial"
    assert payload["outcome"]["failed"] == {"b": "blocked on a"}


def test_distill_run_writes_a_precedent_note_that_roundtrips(tmp_path):
    store = _finished_run(tmp_path, "concursus-" + "a" * 40)
    result = {n: store.get(n) for n in sorted(store.completed())}

    path = distill_run(
        result,
        store.records(),
        vault_path=tmp_path,
        trail_id=store.trail_id,
        run_dir=store.run_dir,
    )
    # The precedent lands under <vault>/precedents/ — NEVER a run dir (can't be replayed).
    assert path.startswith(str(precedents_dir(tmp_path)))

    # It round-trips through the SAME marshalling as a run record — exact payload recovered.
    from pathlib import Path

    rec = _note_to_record(Path(path).read_text())
    assert rec.output["trail_id"] == store.trail_id
    assert rec.output["status"] == "completed"
    assert rec.output["results"] == result
    assert rec.schema == "run_precedent"


def test_distill_store_convenience_projects_the_frontier(tmp_path):
    store = _finished_run(tmp_path, "concursus-" + "c" * 40)
    path = distill_store(store)
    from pathlib import Path

    rec = _note_to_record(Path(path).read_text())
    assert rec.output["results"] == {"ingest": {"document": "d"}, "summarize": {"summary": "s"}}
    assert sorted(rec.output["nodes"]) == ["ingest", "summarize"]


def test_precedent_note_is_not_loaded_as_a_run_record(tmp_path):
    """A precedent note must never leak into a run's replay/resume — it is a sibling tree, so the
    run-record loaders never see it."""
    store = _finished_run(tmp_path, "concursus-" + "d" * 40)
    distill_store(store)
    # load_records over the RUN dir still sees only the 2 run records, not the precedent.
    run_records = load_records(store.run_dir)
    assert {r.node for r in run_records} == {"ingest", "summarize"}


def test_distill_slipbox_form_is_conformant(tmp_path):
    store = _finished_run(tmp_path, "concursus-" + "e" * 40, slipbox_form=True)
    path = distill_store(store)
    text = open(path).read()
    assert '\ntags:\n  - "resource"' in text
    assert '\nbuilding_block: "empirical_observation"' in text  # a produced summary artifact
    assert "\n## Related Notes\n" in text
    # still round-trips exactly regardless of form
    from pathlib import Path

    rec = _note_to_record(Path(path).read_text())
    assert rec.output["status"] == "completed"


# -- AI-16: the cross-run hub (read-only projection) ------------------------
def test_hub_renders_one_row_per_run_and_is_idempotent(tmp_path):
    # Two distinct finished runs, each distilled into one precedent note.
    for tag in ("a", "b"):
        store = _finished_run(tmp_path, "concursus-" + tag * 40)
        distill_store(store)

    path = render_precedent_hub(tmp_path)
    first = open(path).read()

    trails = sorted(r.output["trail_id"] for r in load_precedents(tmp_path))
    assert len(trails) == 2
    # Exactly one hub row per distilled run.
    for tid in trails:
        assert first.count(f"[{tid}](") == 1

    # Idempotent: same notes -> byte-identical hub (pure regenerable projection, no accumulation).
    second = open(render_precedent_hub(tmp_path)).read()
    assert first == second


def test_redistilling_one_run_keeps_one_row(tmp_path):
    store = _finished_run(tmp_path, "concursus-" + "f" * 40)
    distill_store(store)
    distill_store(store)  # re-distill the SAME run/family -> overwrites its single note
    assert len(load_precedents(tmp_path)) == 1


def test_precedent_index_queries_across_runs(tmp_path):
    # one completed run, one partial run (a failed node)
    good = _finished_run(tmp_path, "concursus-" + "g" * 40)
    distill_store(good)

    bad = FileVaultStateStore.from_config(vault_path=tmp_path, session_id="concursus-" + "h" * 40)
    bad.put("a", {"x": 1})
    bad.put("b", {}, meta={"status": "failed", "blocked_on": "blocked on a"})
    distill_store(bad)

    idx = PrecedentIndex.from_vault(tmp_path)
    assert len(idx.trails()) == 2
    assert [p["trail_id"] for p in idx.query(status="completed")] == [good.trail_id]
    assert [p["trail_id"] for p in idx.query(status="partial")] == [bad.trail_id]
    assert idx.get(good.trail_id)["outcome"]["completed"] == 2


def test_precedent_db_mirrors_notes_and_is_disposable(tmp_path):
    for tag in ("a", "b"):
        distill_store(_finished_run(tmp_path, "concursus-" + tag * 40))

    db_path = build_precedent_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute("SELECT trail_id, status, total, completed FROM precedents").fetchall()
        assert len(rows) == 2
        assert all(status == "completed" and total == 2 for _, status, total, _ in rows)
    finally:
        con.close()

    # Disposable: rebuilding from the same notes yields the same row set.
    build_precedent_db(tmp_path)
    con = sqlite3.connect(db_path)
    try:
        assert con.execute("SELECT COUNT(*) FROM precedents").fetchone()[0] == 2
    finally:
        con.close()


def test_hub_empty_vault_renders_placeholder(tmp_path):
    path = render_precedent_hub(tmp_path)
    assert "no runs distilled yet" in open(path).read()
