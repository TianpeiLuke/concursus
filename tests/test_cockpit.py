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


def test_cockpit_over_live_loop(tmp_path):
    """I-3: a DirectorCockpit built over a LIVE GovernorLoop run is a PURE read surface — its
    exception_queue equals the run's summary().failed and rendering it leaves store.records()
    byte-identical (no assemble, no dispatch, no put; INV-5)."""
    from concursus import AgentManifest, GovernorLoop
    from concursus.state.statestore import InProcessStateStore

    def _m(name, *, inputs=None, depends_on=None):
        return AgentManifest.from_dict({
            "name": name,
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
                "protocol": "HTTP",
                "entry": f"agents.{name}:run",
                "role_arn": "arn:aws:iam::123456789012:role/agent",
            },
            "contract": {
                "inputs": inputs or {},
                "outputs": {"doc": {"type": "string", "required": True}},
            },
            "spec": {"depends_on": depends_on or []},
        })

    manifests = {
        "ingest": _m("ingest"),
        "summarize": _m(
            "summarize",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }

    def _plan_model_fn(goal, precedents, directives):
        return {"nodes": ["ingest", "summarize"], "edges": [["ingest", "summarize"]]}

    class _StoreWritingSupervisor:
        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
            self._plan = plan
            self._store = store

        def run(self, inputs):
            already = set(self._store.completed())
            for node in self._plan.order:
                if node not in already:
                    self._store.put(node, {"doc": f"{node}-out"})
            return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}

    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        supervisor_factory=lambda **kw: _StoreWritingSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.done is True

    cockpit = loop.cockpit(vault_path=str(tmp_path))

    before = [repr(r) for r in store.records()]

    # The cockpit's exception queue is exactly the run's failed rows (here: none — a clean run).
    summary = cockpit.briefing()["summary"]
    queue = cockpit.exception_queue()
    assert {row["node"] for row in queue} == set(summary["failed"].keys())
    # A clean run has no failures; the completed set is the live run's.
    assert set(summary["completed_nodes"]) == {"ingest", "summarize"}
    monitor = cockpit.runs_monitor()
    assert monitor["revision"] == result.state.plan_history[-1].revision

    after = [repr(r) for r in store.records()]
    # Read-only: rendering every cockpit surface left the append-only log byte-identical (INV-5).
    assert before == after
