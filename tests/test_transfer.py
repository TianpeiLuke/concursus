"""Tests for the session-end knowledge-transfer connector (C1 + C4 + C2 + C3 + rollup).

C1 = the ``slipbox_transfer`` terminal-node manifest + acceptance gate; C4 = registering the
consolidation sub-agent so the router can bind the task; C2 = exporting a run's episodic notes to
the sub-agent's local ingestion inbox; C3 = the trigger that fires the export at the strictly-outer
moment (synthesize / reaper teardown); rollup = the session verdict (``overall_ok``) that is false
unless the transfer ran and was accepted. All builders are additive/opt-in: a run that does not call
them is byte-for-byte unchanged.

This is the parity-safe subset for the public mirror — the digest-view bundle and the S3 push
variant depend on subsystems not present here, so the export is the local-inbox raw-notes path.
"""

from __future__ import annotations

import types

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.core.manifest import ManifestError
from concursus.core.resolve import AlignmentError, check_alignment, resolve_edges
from concursus.build.ledger import DeployLedger
from concursus.execute.supervisor import SchemaError, Supervisor, check_acceptance
from concursus.governor import FanOutEventSink
from concursus.governor.registry import AgentRegistry
from concursus.state.filevault import FileVaultStateStore
from concursus.state.statestore import InProcessStateStore
from concursus.state.transfer import (
    CONSOLIDATOR_JOB_DICT_KEYS,
    SLIPBOX_FOUNDRY_CAPABILITIES,
    SLIPBOX_TRANSFER_NODE,
    TransferTriggerSink,
    build_slipbox_transfer_manifest,
    distill_export,
    export_run_log,
    mark_transferred,
    recover_trail_id,
    register_slipbox_foundry,
    run_needs_transfer,
    session_overall_ok,
    slipbox_transfer_acceptance_fn,
    sweep_untransferred_runs,
    transfer_node_ok,
    transfer_run,
    wire_slipbox_transfer_terminal,
)

_ARN = "arn:aws:bedrock-agentcore:us-east-1:0:runtime/slipbox"


class _RecordingAdmit:
    def __init__(self):
        self.calls = []

    def __call__(self, members, objective):
        self.calls.append((list(members), objective))
        return {"admitted": len(members), "objective": objective}


# -- C1: manifest -----------------------------------------------------------
def test_manifest_validates_on_arn_path():
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    assert m.protocol == "MCP" and m.side_effecting is True


def test_manifest_without_hosting_handle_fails_closed():
    with pytest.raises(ManifestError):
        build_slipbox_transfer_manifest()


def test_contract_outputs_subset_of_real_job_dict():
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    declared = set(m.output_schema["properties"])
    assert declared <= CONSOLIDATOR_JOB_DICT_KEYS
    assert {"state", "result_path"} <= declared


@pytest.mark.parametrize(
    "bad",
    [
        {"result_path": "/x"},
        {"state": "complete"},
        {"state": "running", "result_path": "/x"},
        {"state": "complete", "result_path": ""},
        {},
    ],
)
def test_acceptance_fail_closed(bad):
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    with pytest.raises(SchemaError):
        check_acceptance(bad, m.output_schema)


def test_acceptance_accepts_complete():
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    check_acceptance({"state": "complete", "result_path": "/vault/a.md"}, m.output_schema)


# -- C1: terminal wiring ----------------------------------------------------
def _producer(name, out_field):
    return AgentManifest.from_dict(
        {"name": name, "registry": {"container_uri": "x", "protocol": "HTTP"},
         "contract": {"outputs": {"properties": {out_field: {"type": "object"}}}}}
    )


def test_wire_makes_transfer_sole_sink_and_aligns():
    dag = AgentDAG()
    dag.add_node("ingest")
    dag.add_node("analyze")
    dag.add_edge("ingest", "analyze")
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    wire_slipbox_transfer_terminal(dag, m, producer_outputs={"analyze": "report"})
    assert dag.sinks() == [SLIPBOX_TRANSFER_NODE]
    manifests = {"ingest": _producer("ingest", "doc"), "analyze": _producer("analyze", "report"),
                 SLIPBOX_TRANSFER_NODE: m}
    check_alignment(dag, manifests, single_writer=True)


def test_wire_distinct_inputs_per_producer():
    dag = AgentDAG()
    dag.add_node("a")
    dag.add_node("b")  # both sinks
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    wire_slipbox_transfer_terminal(dag, m, producer_outputs={"a": "ra", "b": "rb"})
    tos = [e["to"] for e in m.depends_on]
    assert sorted(tos) == ["from_a", "from_b"] and len(set(tos)) == 2


def test_wire_empty_dag_raises():
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    with pytest.raises(ValueError):
        wire_slipbox_transfer_terminal(AgentDAG(), m)


# -- C1: end-to-end gate ----------------------------------------------------
def _transfer_session(job_output):
    dag = AgentDAG()
    dag.add_node(SLIPBOX_TRANSFER_NODE)
    m = build_slipbox_transfer_manifest(agent_runtime_arn=_ARN)
    manifests = {SLIPBOX_TRANSFER_NODE: m}
    plan = types.SimpleNamespace(order=dag.topological_sort(), wiring=resolve_edges(dag, manifests))
    store = InProcessStateStore()

    def invoke(arn, qualifier, session_id, payload_bytes):
        return dict(job_output)

    sup = Supervisor(
        plan, manifests, invoke_fn=invoke, arns={SLIPBOX_TRANSFER_NODE: _ARN},
        check_acceptance=True, acceptance_fn=slipbox_transfer_acceptance_fn,
        on_error="record", state_store=store,
    )
    sup.run({})
    return store


def test_gate_rejects_incomplete_and_accepts_complete():
    bad = _transfer_session({"state": "dead_letter", "result_path": "", "job_id": "j"})
    assert SLIPBOX_TRANSFER_NODE not in bad.completed()
    good = _transfer_session({"state": "complete", "result_path": "/vault/a.md", "job_id": "j"})
    assert SLIPBOX_TRANSFER_NODE in good.completed()


# -- C4: registry -----------------------------------------------------------
def _reg(tmp_path):
    ledger = DeployLedger(str(tmp_path / "l.json"))
    return AgentRegistry(ledger), ledger


def test_register_makes_task_dispatchable_with_arn(tmp_path):
    reg, ledger = _reg(tmp_path)
    register_slipbox_foundry(reg, ledger, arn=_ARN)
    v = reg.match_task(SLIPBOX_TRANSFER_NODE)
    assert v is not None and v.name == "SlipboxFoundry" and v.arn == _ARN


def test_no_ledger_row_not_dispatchable(tmp_path):
    reg, _ledger = _reg(tmp_path)
    reg.register_agent(types.SimpleNamespace(name="SlipboxFoundry"),
                       capabilities=set(SLIPBOX_FOUNDRY_CAPABILITIES))
    assert reg.match_task(SLIPBOX_TRANSFER_NODE) is None


# -- C2: export -------------------------------------------------------------
def _finished_run(tmp_path):
    run_dir = tmp_path / "vault" / "runs" / "sess1"
    store = FileVaultStateStore(str(run_dir), trail_id="sess1", slipbox_form=True)
    store.put("ingest", {"document": "DOC"})
    store.put("analyze", {"report": "R"})
    return store, run_dir


def test_export_copies_notes_byte_identical(tmp_path):
    _store, run_dir = _finished_run(tmp_path)
    res = export_run_log(str(run_dir), str(tmp_path / "inbox"), trail_id="sess1")
    assert res["members"] and res["admitted"] is None
    for m in res["members"]:
        name = m.split("/")[-1]
        assert (tmp_path / "inbox" / name).read_text() == (run_dir / name).read_text()


def test_export_admit_fn_called(tmp_path):
    _store, run_dir = _finished_run(tmp_path)
    admit = _RecordingAdmit()
    res = export_run_log(str(run_dir), str(tmp_path / "inbox"), admit_fn=admit, trail_id="sess1")
    assert len(admit.calls) == 1 and admit.calls[0][1] == "hive-session-sess1"
    assert set(admit.calls[0][0]) == set(res["members"])


def test_export_reexport_inode_stable(tmp_path):
    import os

    _store, run_dir = _finished_run(tmp_path)
    inbox = tmp_path / "inbox"
    r1 = export_run_log(str(run_dir), str(inbox), trail_id="sess1")
    i1 = {m.split("/")[-1]: os.stat(m).st_ino for m in r1["members"]}
    r2 = export_run_log(str(run_dir), str(inbox), trail_id="sess1")
    i2 = {m.split("/")[-1]: os.stat(m).st_ino for m in r2["members"]}
    assert i1 == i2


def test_distill_export_writes_precedent(tmp_path):
    store, _run_dir = _finished_run(tmp_path)
    path = distill_export(store)
    assert path.endswith(".md") and "/precedents/" in path.replace("\\", "/")


# -- C3: triggers -----------------------------------------------------------
def test_trigger_fires_only_on_decision_synthesize(tmp_path):
    _store, run_dir = _finished_run(tmp_path)
    admit = _RecordingAdmit()
    sink = TransferTriggerSink(str(run_dir), str(tmp_path / "inbox"), admit_fn=admit, trail_id="sess1")
    sink.emit({"type": "episode_end", "done": True})
    sink.emit({"type": "decision", "route": "planner"})
    sink.emit({"type": "episode_end", "route": "synthesize"})  # not a decision
    assert sink.last_result is None and admit.calls == []
    sink.emit({"type": "decision", "route": "synthesize"})
    assert sink.last_result["members"] and len(admit.calls) == 1
    assert run_needs_transfer(str(run_dir)) is False


def test_trigger_idempotent(tmp_path):
    _store, run_dir = _finished_run(tmp_path)
    admit = _RecordingAdmit()
    sink = TransferTriggerSink(str(run_dir), str(tmp_path / "inbox"), admit_fn=admit, trail_id="sess1")
    sink.emit({"type": "decision", "route": "synthesize"})
    sink.emit({"type": "decision", "route": "synthesize"})
    assert sink.last_result.get("skipped") is True and len(admit.calls) == 1


def test_fanout_composes_and_isolates(tmp_path):
    seen = []

    class _Ok:
        def emit(self, e):
            seen.append(e)

    class _Boom:
        def emit(self, e):
            raise RuntimeError("boom")

    _store, run_dir = _finished_run(tmp_path)
    trig = TransferTriggerSink(str(run_dir), str(tmp_path / "inbox"), trail_id="sess1")
    FanOutEventSink([_Boom(), _Ok(), trig]).emit({"type": "decision", "route": "synthesize"})
    assert len(seen) == 1  # bad child did not starve the others
    assert trig.last_result is not None


def test_fanout_empty_is_noop():
    FanOutEventSink([]).emit({"type": "decision", "route": "synthesize"})
    FanOutEventSink(None).emit({"type": "decision", "route": "synthesize"})


def test_sweep_transfers_only_untransferred(tmp_path):
    runs_root = tmp_path / "vault" / "runs"
    sx = FileVaultStateStore(str(runs_root / "sessX"), trail_id="sessX", slipbox_form=True)
    sx.put("a", {"v": 1})
    mark_transferred(str(runs_root / "sessX"))
    sy = FileVaultStateStore(str(runs_root / "sessY"), trail_id="sessY", slipbox_form=True)
    sy.put("b", {"v": 2})
    admit = _RecordingAdmit()
    swept = sweep_untransferred_runs(
        str(runs_root), lambda rd, tid: str(tmp_path / "inbox" / tid), admit_fn=admit
    )
    assert len(swept) == 1 and len(admit.calls) == 1
    assert admit.calls[0][1] == "hive-session-sessY"


def test_sweep_ignores_stray_files(tmp_path):
    runs_root = tmp_path / "vault" / "runs"
    sy = FileVaultStateStore(str(runs_root / "sessY"), trail_id="sessY", slipbox_form=True)
    sy.put("b", {"v": 2})
    (runs_root / "stray.txt").write_text("x")
    swept = sweep_untransferred_runs(str(runs_root), lambda rd, tid: str(tmp_path / "inbox" / tid))
    assert len(swept) == 1


def test_recover_trail_id_matches_real_trail_id(tmp_path):
    store = FileVaultStateStore.from_config(
        vault_path=str(tmp_path / "vault"), session_id="acme.run-9", slipbox_form=True
    )
    store.put("a", {"x": 1})
    assert store.run_dir.name != store.trail_id
    assert recover_trail_id(store.run_dir) == store.trail_id


def test_backstop_objective_matches_graceful_for_from_config_run(tmp_path):
    g = FileVaultStateStore.from_config(
        vault_path=str(tmp_path / "gvault"), session_id="acme.run-9", slipbox_form=True
    )
    g.put("a", {"x": 1})
    ga = _RecordingAdmit()
    TransferTriggerSink(str(g.run_dir), str(tmp_path / "gin"), admit_fn=ga, trail_id=g.trail_id).emit(
        {"type": "decision", "route": "synthesize"}
    )
    b = FileVaultStateStore.from_config(
        vault_path=str(tmp_path / "bvault"), session_id="acme.run-9", slipbox_form=True
    )
    b.put("a", {"x": 1})
    ba = _RecordingAdmit()
    sweep_untransferred_runs(
        str(tmp_path / "bvault" / "runs"), lambda rd, tid: str(tmp_path / "bin" / tid), admit_fn=ba
    )
    assert ga.calls[0][1] == ba.calls[0][1] == "hive-session-acme_run_9"


# -- rollup -----------------------------------------------------------------
def test_overall_ok_true_when_transfer_completes():
    store = _transfer_session({"state": "complete", "result_path": "/vault/a.md", "job_id": "j"})
    assert session_overall_ok(store)["overall_ok"] is True
    assert transfer_node_ok(store) is True


def test_overall_ok_false_when_dead_letter():
    store = _transfer_session({"state": "dead_letter", "result_path": "", "job_id": "j"})
    r = session_overall_ok(store)
    assert r["overall_ok"] is False and r["transfer_present"] is False


def test_overall_ok_fail_closed_without_transfer():
    assert session_overall_ok(InProcessStateStore())["overall_ok"] is False


def test_overall_ok_requires_all_work_when_plan_order_given():
    store = _transfer_session({"state": "complete", "result_path": "/vault/a.md", "job_id": "j"})
    r = session_overall_ok(store, plan_order=[SLIPBOX_TRANSFER_NODE, "unfinished"])
    assert r["overall_ok"] is False and r["work_complete"] is False
