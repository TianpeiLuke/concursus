"""Tests for the AI-18 run-state ingestion capture renderers (filevault).

These exercise the four write-time / read-only NOTE PROJECTIONS over already-frozen run state:
``capture_run_plan_note`` (a durable ``_plan.md`` topology snapshot), ``capture_agent_response_note``
(round-trip-exact + Did→Observed→Outcome enrichment), ``capture_agent_log_note`` (the FAILURE-only
promotion policy), and ``capture_run_output_note`` (the record_type→renderer dispatch umbrella).

Identity invariants under test: none of the renderers mutates state or influences dispatch; the plan
snapshot is NOT a run record (never parsed back, never corrupts ``load_records``); the enriched
response note reloads byte-exact; the log→note promotion trigger is failure-only (never a verdict
path).
"""

import json

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble import OrchestrationAssembler
from concursus.filevault import (
    FileVaultStateStore,
    _note_to_record,
    _record_to_note,
    capture_agent_log_note,
    capture_agent_response_note,
    capture_run_output_note,
    capture_run_plan_note,
)
from concursus.rundb import build_run_db, load_records
from concursus.statestore import Record, _DEDUP_RECORD_TYPE


# -- fixtures ---------------------------------------------------------------
def _agent(name, inputs, outputs, depends_on=None):
    reg = {
        "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
        "protocol": "HTTP",
        "entry": f"agents.{name}:run",
        "role_arn": "arn:aws:iam::123456789012:role/agent",
    }
    data = {"name": name, "registry": reg, "contract": {"inputs": inputs, "outputs": outputs}}
    if depends_on is not None:
        data["spec"] = {"depends_on": depends_on}
    return AgentManifest.from_dict(data)


def _plan():
    """A frozen 3-node plan: ingest -> summarize -> critique."""
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")
    manifests = {
        "ingest": _agent("ingest", {"uri": {"type": "string"}}, {"document": {"type": "string"}}),
        "summarize": _agent(
            "summarize",
            {"document": {"type": "string"}},
            {"properties": {"summary": {"type": "string"}}},
            depends_on=[{"from": "ingest.document", "to": "document"}],
        ),
        "critique": _agent(
            "critique",
            {"summary": {"type": "string"}},
            {"critique": {"type": "string"}},
            depends_on=[{"from": "summarize.summary", "to": "summary"}],
        ),
    }
    return OrchestrationAssembler().assemble(dag, manifests)


# -- (i) capture_run_plan_note: Mermaid, non-record, does not corrupt load_records --------------
def test_plan_note_writes_mermaid_dag(tmp_path):
    plan = _plan()
    path = capture_run_plan_note(plan, tmp_path, trail_id="run_x", date="2026-07-10")
    assert path.endswith("_plan.md")
    text = open(path).read()
    # A Mermaid DAG of the frozen order + wiring is rendered in the body.
    assert "```mermaid" in text
    assert "graph TD" in text
    # producer->consumer edges rendered (ingest feeds summarize feeds critique)
    assert "-->" in text
    for node in ("ingest", "summarize", "critique"):
        assert node in text
    # It is stamped as a non-record note (never parsed back as a Record).
    assert "concursus_note_kind" in text


def test_plan_note_is_not_a_record_and_does_not_corrupt_load(tmp_path):
    run = tmp_path / "run"
    fv = FileVaultStateStore(run)
    fv.put("ingest", {"document": "d"}, meta={"producer": "ingest"})
    fv.put("summarize", {"summary": "s"},
           meta={"producer": "summarize", "consumes": ["ingest:$.document"]})

    # Drop a plan snapshot ALONGSIDE _run.md under the same run dir.
    plan = _plan()
    capture_run_plan_note(plan, run, trail_id="run_x")
    assert (run / "_plan.md").exists()

    # _note_to_record REFUSES to parse the plan note (it is stamped as a non-record).
    with pytest.raises(ValueError):
        _note_to_record((run / "_plan.md").read_text())

    # load_records (rundb) and a fresh store reload both skip it — the 2 real records survive intact.
    recs = load_records(run)
    assert {r.node for r in recs} == {"ingest", "summarize"}

    resumed = FileVaultStateStore(run)
    assert resumed.completed() == {"ingest", "summarize"}
    assert resumed.get("summarize") == {"summary": "s"}
    assert len(resumed.records()) == 2  # the plan note is NOT loaded as a record

    # The derived SQLite DB (built over the notes) is also uncorrupted by the plan note.
    import sqlite3
    db_path = build_run_db(run)
    con = sqlite3.connect(db_path)
    try:
        nodes = {r[0] for r in con.execute("SELECT node FROM records")}
    finally:
        con.close()
    assert nodes == {"ingest", "summarize"}


def test_plan_note_lean_form_also_non_record(tmp_path):
    run = tmp_path / "run"
    plan = _plan()
    path = capture_run_plan_note(plan, run, slipbox_form=False)
    text = open(path).read()
    assert "concursus_note_kind" in text
    assert "```mermaid" in text
    # No SlipBox scaffolding in the lean form.
    assert "building_block:" not in text
    with pytest.raises(ValueError):
        _note_to_record(text)


def test_plan_note_drops_bulky_deploy_payload(tmp_path):
    """The plan note keeps only a compact hosting digest — the full to_dict() deploy payload
    (wrapper/dockerfile/create_agent_runtime request) is DROPPED so a note is never megabytes."""
    plan = _plan()
    path = capture_run_plan_note(plan, tmp_path)
    text = open(path).read()
    # The compact summary carries hosting digests (build_mode/protocol), not the raw invoke request.
    assert "build_mode" in text
    # The bulky deploy payload is DROPPED from the embedded JSON summary block (the body prose may
    # still NAME what it drops, so we assert against the rendered summary, not the whole document).
    summary_json = text.split("```json", 1)[1].split("```", 1)[0]
    summary = json.loads(summary_json)
    assert "create_agent_runtime" not in summary_json  # no inlined invoke request
    for entry in summary["entries"].values():
        assert "has_dockerfile" in entry  # only the boolean flag survives
        # never the raw source blobs — only the compact has_* digests
        assert "wrapper" not in entry and "dockerfile" not in entry


# -- (ii) enriched response note still round-trips exactly ---------------------------------------
def test_enriched_response_note_roundtrips_byte_exact(tmp_path):
    rec = Record(
        node="summarize",
        output={"summary": "multi\nline\n---\ntext", "n": 3, "s": "0123",
                "root_cause": "timeout", "confidence": 0.9},
        attempt=2,
        status="validated",
        schema="summarize-agent",
        producer="summarize",
        consumes=["ingest:$.document"],
        content_hash="abc",
        timestamp=5,
        address="summarize",
    )
    text = capture_agent_response_note(rec, trail_id="run_x", date="2026-07-10")
    # The body carries the compact Did->Observed->Outcome digest + reflected machine findings...
    assert "Did → Observed → Outcome" in text
    assert "Machine Findings" in text
    assert "root_cause" in text and "confidence" in text
    # ...but the authoritative payload/meta blobs stay untouched, so the reload is byte-exact.
    back = _note_to_record(text)
    assert back.output == rec.output  # exact — the whole point
    assert back.node == rec.node
    assert back.attempt == rec.attempt
    assert back.status == rec.status
    assert back.consumes == rec.consumes
    assert back.address == rec.address


def test_response_note_without_findings_has_no_findings_block(tmp_path):
    rec = Record(node="a", output={"x": 1}, attempt=1, status="validated", timestamp=1)
    text = capture_agent_response_note(rec)
    assert "Did → Observed → Outcome" in text
    assert "Machine Findings" not in text  # the common case: no finding keys -> no block
    assert _note_to_record(text).output == {"x": 1}


def test_enrichment_is_display_only_matches_record_to_note(tmp_path):
    """capture_agent_response_note is a thin named seam over _record_to_note — same bytes."""
    rec = Record(node="a", output={"x": 1}, attempt=1, status="validated", timestamp=1)
    assert capture_agent_response_note(rec, trail_id="t") == _record_to_note(rec, trail_id="t")


# -- (iii) failure-only promotion policy ---------------------------------------------------------
def test_log_note_promoted_only_on_failure():
    failed = Record(node="bad", output={"error": "boom"}, attempt=1, status="failed", timestamp=1)
    ok = Record(node="ok", output={"x": 1}, attempt=1, status="validated", timestamp=2)
    superseded = Record(node="s", output={"x": 1}, attempt=1, status="superseded", timestamp=3)

    # Only a failed record is promoted to a durable note; every other status returns None.
    promoted = capture_agent_log_note(failed)
    assert promoted is not None
    assert capture_agent_log_note(ok) is None
    assert capture_agent_log_note(superseded) is None


def test_promoted_failure_log_is_counter_argument_and_roundtrips():
    failed = Record(node="bad", output={"error": "boom", "failure_mode": "oom"},
                    attempt=1, status="failed", timestamp=1)
    text = capture_agent_log_note(failed)
    assert text is not None
    assert '"counter_argument"' in text  # a failed record is a refuted attempt
    # It renders through the same round-trip-exact path as any response note.
    back = _note_to_record(text)
    assert back.output == {"error": "boom", "failure_mode": "oom"}
    assert back.status == "failed"


# -- capture_run_output_note: record_type -> renderer dispatch umbrella ---------------------------
def test_run_output_dispatch_routes_by_record_type():
    agent = Record(node="a", output={"x": 1}, attempt=1, status="validated",
                   record_type="agent_output", timestamp=1)
    dedup = Record(node="a", output={"x": 1}, attempt=2, status="validated",
                   record_type=_DEDUP_RECORD_TYPE, timestamp=2)
    text_agent = capture_run_output_note(agent)
    text_dedup = capture_run_output_note(dedup)
    # Both route through the response renderer (round-trip exact); dedup is a navigation marker.
    assert _note_to_record(text_agent).output == {"x": 1}
    assert _note_to_record(text_dedup).record_type == _DEDUP_RECORD_TYPE


def test_run_output_dispatch_unknown_type_falls_back_to_response():
    weird = Record(node="a", output={"x": 1}, attempt=1, status="validated",
                   record_type="totally_unknown_kind", timestamp=1)
    text = capture_run_output_note(weird)  # widen-and-render: no crash, still round-trips
    assert _note_to_record(text).output == {"x": 1}


def test_run_output_umbrella_renders_failed_record():
    """The umbrella renders EVERY record (the failure-only promotion policy lives in the log seam)."""
    failed = Record(node="bad", output={"error": "boom"}, attempt=1, status="failed", timestamp=1)
    text = capture_run_output_note(failed)  # not None — umbrella renders it
    assert _note_to_record(text).status == "failed"
