"""Tests for the read-only director cockpit (S6-G5)."""
from __future__ import annotations

import types

from concursus import AgentDAG, AgentManifest
from concursus.core.resolve import resolve_edges
from concursus.execute.supervisor import Supervisor
from concursus.state.statestore import InProcessStateStore
from concursus.governor.cockpit import DirectorCockpit


def _manifests():
    ingest = AgentManifest.from_dict({
        "name": "ingest",
        "registry": {"container_uri": "x", "protocol": "HTTP"},
        "contract": {
            "inputs": {"uri": {"type": "string", "required": True}},
            "outputs": {"document": {"type": "string", "required": True}},
        },
        "spec": {"depends_on": []},
    })
    summarize = AgentManifest.from_dict({
        "name": "summarize",
        "registry": {"container_uri": "x", "protocol": "HTTP"},
        "contract": {
            "inputs": {"document": {"type": "string", "required": True}},
            "outputs": {"summary": {"type": "string", "required": True}},
        },
        "spec": {"depends_on": [{"from": "ingest.document", "to": "document"}]},
    })
    return {"ingest": ingest, "summarize": summarize}


def _dag():
    dag = AgentDAG()
    dag.add_node("ingest")
    dag.add_node("summarize")
    dag.add_edge("ingest", "summarize")
    return dag


def _fake_invoker(*args, **kwargs):
    """Return a valid ingest doc; blow up whenever summarize is invoked."""
    blob = " ".join([str(a) for a in args] + [str(v) for v in kwargs.values()])
    if "summarize" in blob:
        raise ValueError("boom summarize")
    return {"document": "doc-body"}


def _plan(dag, manifests):
    """A ProvisioningPlan-like duck-typed stand-in (order + wiring + revision)."""
    return types.SimpleNamespace(
        order=dag.topological_sort(),
        wiring=resolve_edges(dag, manifests),
        revision=0,
    )


def _run_with_failure():
    manifests = _manifests()
    dag = _dag()
    plan = _plan(dag, manifests)
    store = InProcessStateStore()
    sup = Supervisor(
        plan,
        manifests,
        invoke_fn=_fake_invoker,
        arns={"ingest": "arn:ingest", "summarize": "arn:summarize"},
        state_store=store,
        on_error="record",
        session_id="S" * 40,
    )
    sup.run({"uri": "s3://doc"})
    return sup, store, plan


def test_cockpit_is_read_only_projection(tmp_path):
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, vault_path=str(tmp_path), plan=plan)

    before = [repr(r) for r in store.records()]

    # Exercise all three read-only surfaces.
    cockpit.briefing()
    cockpit.exception_queue()
    cockpit.runs_monitor()

    after = [repr(r) for r in store.records()]

    assert before == after
    assert len(before) == len(after)


def test_exception_queue_matches_summary_failed():
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)

    summary_failed = sup.summary()["failed"]
    assert summary_failed, "fixture must produce at least one failed node"

    queue = cockpit.exception_queue()
    queue_nodes = {row["node"] for row in queue}

    assert queue_nodes == set(summary_failed.keys())
    for row in queue:
        assert row["reason"] == summary_failed[row["node"]]
