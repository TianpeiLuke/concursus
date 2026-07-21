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


def test_exception_queue_surfaces_escalations_and_unmatched():
    """J-2: a cockpit handed escalated + unmatched sets APPENDS a distinct row for each,
    with reasons 'escalated' / 'unmatched', on top of the summary's failed rows."""
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(
        supervisor=sup, plan=plan, escalated=["x"], unmatched=["y"]
    )

    summary_failed = sup.summary()["failed"]
    assert summary_failed, "fixture must produce at least one failed node"

    queue = cockpit.exception_queue()
    by_reason = {}
    for row in queue:
        by_reason.setdefault(row["reason"], []).append(row["node"])

    # Every failed row is still present with its summary reason.
    for node, reason in summary_failed.items():
        assert node in by_reason.get(reason, [])
    # The escalated + unmatched sets appear as distinct governance rows.
    assert by_reason["escalated"] == ["x"]
    assert by_reason["unmatched"] == ["y"]
    # All three reason kinds are represented.
    assert {"escalated", "unmatched"} <= set(by_reason)


def test_cockpit_still_read_only():
    """J-2/INV-5: rendering the governance-augmented exception queue leaves the
    append-only log byte-identical — the escalated/unmatched sets are pure values."""
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(
        supervisor=sup, plan=plan, escalated=["x"], unmatched=["y"]
    )

    before = [repr(r) for r in store.records()]
    cockpit.exception_queue()
    after = [repr(r) for r in store.records()]

    assert before == after
    assert len(before) == len(after)


def test_exception_queue_default_is_failed_only():
    """J-2 opt-in rule: with no governance sets, the queue is byte-for-byte today's
    failed-only queue — no escalated/unmatched rows."""
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)

    summary_failed = sup.summary()["failed"]
    queue = cockpit.exception_queue()

    assert {row["node"] for row in queue} == set(summary_failed.keys())
    assert all(row["reason"] not in ("escalated", "unmatched") for row in queue)


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


# ---- P5: snapshot-then-follow, family-tree, event-bus ----

def test_snapshot_orders_by_seq_and_reports_offset():
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)

    snap = cockpit.snapshot()
    seqs = [r.seq for r in snap["records"] if r.seq is not None]
    assert seqs == sorted(seqs), "snapshot records must be ordered by monotonic seq"
    assert snap["count"] == len(snap["records"])
    if seqs:
        assert snap["offset"] == max(seqs)


def test_follow_returns_only_newer_slice_no_reconcile():
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)

    snap = cockpit.snapshot()
    # Following from the current offset yields nothing new (loss-free, no full reconcile).
    tail = cockpit.follow(snap["offset"])
    assert tail["count"] == 0
    assert tail["offset"] == snap["offset"]

    # Following from 0 replays the whole ordered slice (late-attach = replay-from-offset).
    full = cockpit.follow(0)
    assert full["count"] == snap["count"]
    assert [r.node for r in full["records"]] == [r.node for r in snap["records"]]


def test_follow_is_read_only(tmp_path):
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)
    before = [repr(r) for r in store.records()]
    cockpit.snapshot()
    cockpit.follow(0)
    cockpit.family_tree()
    assert [repr(r) for r in store.records()] == before


def test_family_tree_annotates_live_status_over_frozen_dag():
    sup, store, plan = _run_with_failure()
    cockpit = DirectorCockpit(supervisor=sup, plan=plan)

    tree = cockpit.family_tree()
    by_node = {n["node"]: n for n in tree["nodes"]}

    # Full frozen topology is present up-front (compile-time lineage, not runtime-reconstructed).
    assert set(by_node) == set(sup.summary()["order"])
    # ingest completed; summarize failed (per the fixture).
    assert by_node["ingest"]["status"] == "done"
    assert by_node["summarize"]["status"] == "failed"
    # Edges come from the frozen plan.wiring.
    assert "ingest" in by_node["summarize"]["producers"]
    assert tree["counts"]["done"] >= 1
    assert tree["counts"]["failed"] >= 1


def test_node_event_bus_fans_out_per_node():
    from concursus.governor.cockpit import NodeEventBus

    bus = NodeEventBus()
    got_a, got_b = [], []
    unsub_a = bus.subscribe("A", lambda nid, chunk: got_a.append((nid, chunk)))
    bus.subscribe("B", lambda nid, chunk: got_b.append((nid, chunk)))

    bus.emit("A", "hello")
    bus.emit("B", "world")
    bus.emit("C", "orphan")  # no listener -> no-op, no error

    assert got_a == [("A", "hello")]
    assert got_b == [("B", "world")]

    unsub_a()
    bus.emit("A", "again")
    assert got_a == [("A", "hello")], "unsubscribed listener must not receive further chunks"
