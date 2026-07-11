"""Tests for the governor's fixed cyclic outer loop (:class:`GovernorLoop`).

The governor is a NEW OUTER layer *around* the compiler.  These tests assert the identity
invariants that keep the compiler a compiler:

* One :meth:`Supervisor.run` per round — a single static forward pass, never a cycle inside the
  supervisor (INV-1).
* Each round forms a DISTINCT frozen :class:`ProvisioningPlan` VALUE with a strictly-increasing
  revision (INV-3/INV-4); the prior plan object is byte-identical afterwards.
* ``collect`` folds outputs into the append-only :class:`StateStore` log, never a mutated plan
  (INV-5).
* The loop TERMINATES on frontier-exhaustion (not the hard step cap), and the langgraph-absent
  pure-Python path runs.

Everything is offline: a fake Supervisor + an :class:`InProcessStateStore`, no AWS touched.
"""

import copy
import importlib.util

import pytest

from concursus import (
    AgentDAG,
    AgentManifest,
    GovernorLoop,
    GovernorLoopError,
    GovernorResult,
    OrchestrationAssembler,
    plan_from_goal,
)
from concursus.governor import GovernorLoop as GovernorLoopFromSubpkg
from concursus.state.statestore import InProcessStateStore, MemoryStateStore

#: Whether langgraph is importable here. GovernorLoop(backend="auto") upgrades to the langgraph
#: StateGraph when present and falls back to the pure-Python driver when absent — so an
#: auto-backend run reports "python" only in the zero-dependency environment.
LANGGRAPH_INSTALLED = importlib.util.find_spec("langgraph") is not None

# pytest inserts the tests/ dir onto sys.path, so the shipped fake AgentCore data-plane client
# (create_event / list_events) can be reused here — no boto3, no AWS.
from test_statestore import FakeMemoryClient

# Exported from both the top-level package and the subpackage.
assert GovernorLoop is GovernorLoopFromSubpkg


def _manifest(name, *, inputs=None, depends_on=None):
    return AgentManifest.from_dict(
        {
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
        }
    )


def _plan_model_fn(goal, precedents, directives):
    """Inject a two-node topology so the manifests' depends_on edge type-aligns."""
    return {"nodes": ["ingest", "summarize"], "edges": [["ingest", "summarize"]]}


def _two_node_manifests():
    return {
        "ingest": _manifest("ingest"),
        "summarize": _manifest(
            "summarize",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }


class _FakeSupervisor:
    """A fake episode supervisor: records the plan it ran and writes canned outputs to the store.

    Mirrors the real seam: constructed per round over ONE frozen plan and ``run`` ONCE.  It writes
    every plan node's output into the shared store (so the frontier exhausts) and records the exact
    plan object it saw, so the test can assert one distinct frozen plan per round.
    """

    seen_plans = []  # class-level ledger of the plan object each episode ran over
    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store
        type(self).seen_plans.append(plan)

    def run(self, inputs):
        type(self).run_count += 1
        outputs = {}
        for node in self._plan.order:
            out = {"doc": f"{node}-out"}
            outputs[node] = out
        return outputs


def _fresh_fake():
    _FakeSupervisor.seen_plans = []
    _FakeSupervisor.run_count = 0
    return _FakeSupervisor


def test_outer_loop_runs_bounded_freeze_episodes():
    """One Supervisor.run per round, a distinct frozen plan per round, terminates on frontier-exhaust."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    assert isinstance(result, GovernorResult)
    # The fake writes every node → the frontier exhausts on the first episode.
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert result.frontier == []
    # It stopped on the natural bound, NOT the hard step cap.
    assert result.terminated_by != "step_cap"
    assert result.rounds < loop._step_cap()

    # Exactly one Supervisor.run per round (INV-1: a single static pass per episode).
    assert fake.run_count == result.rounds == result.supervisor_runs

    # One DISTINCT frozen plan object per round (INV-4).
    assert len(fake.seen_plans) == result.rounds
    ids = [id(p) for p in fake.seen_plans]
    assert len(set(ids)) == len(ids)
    # Strictly increasing revisions across the plan-value sequence (INV-4).
    revs = [p.revision for p in result.state.plan_history]
    assert revs == sorted(revs) and len(set(revs)) == len(revs)
    assert revs[0] == 0

    # The completed prefix is re-derived from the append-only log.
    assert set(result.completed) == {"ingest", "summarize"}


def test_collect_writes_log_not_plan():
    """COLLECT folds outputs into the append-only log; the frozen plan object is unchanged (INV-5)."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    manifests = _two_node_manifests()

    # Build the exact same first-round plan the loop will form, and snapshot it.
    dag = plan_from_goal("summarize the document", plan_model_fn=_plan_model_fn)
    expected_plan = OrchestrationAssembler().assemble(dag, manifests)
    snapshot = copy.deepcopy(expected_plan.to_dict())

    loop = GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The log grew (outputs were folded in via StateStore.put), not the plan.
    records = store.records()
    assert len(records) >= 2
    logged_nodes = {r.node for r in records}
    assert {"ingest", "summarize"} <= logged_nodes

    # The first frozen plan VALUE is byte-identical to a plan assembled independently — COLLECT
    # never edited it in place.
    first_plan = result.state.plan_history[0]
    assert first_plan.to_dict() == snapshot
    # And the plan the FIRST episode ran over is that same untouched object.
    assert fake.seen_plans[0] is first_plan


def test_langgraph_absent_python_backend_runs():
    """With langgraph absent, backend='python' drives the loop to a bounded termination."""
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.backend == "python"  # explicit backend='python' always uses the pure driver
    assert result.done is True
    assert fake.run_count >= 1
    # 'auto' falls back to python when langgraph is absent, and upgrades to the StateGraph when
    # present — either way the episode drives to a bounded, done termination.
    fake2 = _fresh_fake()
    loop2 = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake2(**kw),
        plan_model_fn=_plan_model_fn,
        backend="auto",
    )
    result2 = loop2.run({"uri": "s3://doc"})
    assert result2.backend == ("langgraph" if LANGGRAPH_INSTALLED else "python")
    assert result2.done is True


@pytest.mark.skipif(
    not LANGGRAPH_INSTALLED,
    reason="exercises the langgraph StateGraph backend; requires the optional 'reasoning' extra",
)
def test_langgraph_present_backend_runs_bounded_episode():
    """With langgraph installed, backend='langgraph' drives the SAME outer loop via the StateGraph
    to a bounded, done termination — the optional backend is equivalent to the pure-Python driver."""
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="langgraph",
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.backend == "langgraph"
    assert result.done is True
    assert fake.run_count >= 1


class _PartialSupervisor:
    """A fake that completes exactly ONE additional plan node per round (partial progress).

    This forces the loop to run >=2 rounds so the ``recompile`` planner branch fires with a
    NON-EMPTY ``completed`` set — exercising the monotonic executed-prefix PINNING that the
    single-round fake never reaches.  It does NOT write to the store (COLLECT persists), mirroring
    a decoupled supervisor.
    """

    seen_plans = []  # class-level ledger of the plan object each episode ran over
    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store
        type(self).seen_plans.append(plan)

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        outputs = {}
        for node in self._plan.order:  # complete exactly one NEW node this round, in dispatch order
            outputs[node] = {"doc": f"{node}-out"}
            if node not in already:
                break
        return outputs


def _fresh_partial():
    _PartialSupervisor.seen_plans = []
    _PartialSupervisor.run_count = 0
    return _PartialSupervisor


def test_partial_progress_multi_round_bumps_revision_and_pins_prefix():
    """N-round progress => N DISTINCT frozen plans with STRICTLY increasing revisions, executed
    prefix PINNED by ``recompile`` (INV-4) — the non-vacuous multi-round assertion."""
    fake = _fresh_partial()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # One new node per round over a 2-node plan => exactly two rounds, ending on frontier-exhaust
    # (NOT the stall bound or the step cap).
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert result.rounds >= 2
    assert fake.run_count == result.rounds == result.supervisor_runs

    # One DISTINCT frozen plan object per round (INV-4) — no aliasing across rounds.
    assert len(fake.seen_plans) == result.rounds
    ids = [id(p) for p in fake.seen_plans]
    assert len(set(ids)) == len(ids)

    # STRICTLY increasing revisions 0,1,... — a real monotonic bump past 0 (not a vacuous [0]).
    revs = [p.revision for p in result.state.plan_history]
    assert len(revs) == result.rounds
    assert revs == list(range(result.rounds))  # [0, 1, ...] — strictly increasing, contiguous
    assert revs[0] == 0 and revs[-1] >= 1

    # Round-2's recompile PINNED round-1's executed node: its prior entry/wiring survive verbatim
    # (the same object), and no MonotonicityError was raised (the run completed).
    plan0, plan1 = result.state.plan_history[0], result.state.plan_history[1]
    assert "ingest" in store.completed()  # ingest completed in round 1
    assert plan1.revision == plan0.revision + 1
    assert plan1.entries["ingest"] is plan0.entries["ingest"]
    assert plan1.wiring["ingest"] == plan0.wiring["ingest"]
    # The prior plan VALUE is untouched after the recompile swap (INV-3).
    assert plan0.revision == 0

    assert set(result.completed) == {"ingest", "summarize"}


class _StoreWritingSupervisor:
    """A fake that writes THROUGH to the shared store during ``run`` (like the real Supervisor's
    ``_dispatch``) and returns the store's view.

    Covers the default store-bound seam (the real integration path): COLLECT must treat the store
    as the SINGLE persistence point and NOT re-``put`` outputs the supervisor already wrote, else
    the append-only log accrues redundant ``dedup`` records O(rounds x nodes).
    """

    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        for node in self._plan.order:  # mirror Supervisor._dispatch: persist each validated node
            if node not in already:
                self._store.put(node, {"doc": f"{node}-out"})
        return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}


def test_shared_store_supervisor_writes_log_once_no_redundant_dedup():
    """With a store-bound supervisor (the default seam), COLLECT re-persists nothing — the log has
    exactly one record per node and NO redundant dedup records."""
    _StoreWritingSupervisor.run_count = 0
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: _StoreWritingSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    assert result.done is True
    assert set(result.completed) == {"ingest", "summarize"}
    records = store.records()
    # The supervisor persisted each node exactly once; COLLECT added no second (dedup) write.
    assert len(records) == 2
    assert sorted(r.node for r in records) == ["ingest", "summarize"]
    assert all(r.record_type != "dedup" for r in records)


def test_stall_terminates_on_no_progress_bound():
    """A supervisor that writes nothing must terminate on the no_progress bound, not run away."""

    class _NoopSupervisor:
        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
            self._plan = plan

        def run(self, inputs):
            return {}  # writes nothing → no forward progress

    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: _NoopSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=20,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.terminated_by == "no_progress"
    assert result.done is False
    assert result.rounds < loop._step_cap()


class _FailThenRecoverSupervisor:
    """A store-bound fake that FAILS one node on round 1 then recovers it on round 2.

    Round 1: ``ingest`` validates, ``summarize`` is written as a ``status="failed"`` record (so it
    stays OUT of ``completed()``), surfacing the ``"failure"`` replan signal COLLECT must detect.
    Round 2: over the recompiled frozen plan (``ingest`` PINNED, ``summarize`` re-planned), the
    still-open node validates and the frontier exhausts.  Mirrors the real store-bound seam: it
    writes THROUGH to the shared store during ``run``.
    """

    seen_plans = []
    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store
        type(self).seen_plans.append(plan)

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        for node in self._plan.order:
            if node in already:
                continue
            if node == "summarize" and type(self).run_count == 1:
                # Terminal failure on the first episode: recorded as a failed (non-validated) node.
                self._store.put(node, {"ok": False, "error": "boom"}, meta={"status": "failed"})
            else:
                self._store.put(node, {"doc": f"{node}-out"})
        return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}


def _fresh_fail_then_recover():
    _FailThenRecoverSupervisor.seen_plans = []
    _FailThenRecoverSupervisor.run_count = 0
    return _FailThenRecoverSupervisor


def test_route_after_collect_routes_failure_signal_to_planner():
    """A collect result carrying a replan signal routes to PLANNER (not synthesize), even when the
    frontier looks exhausted — the signal overrides frontier-exhaustion but respects the hard bounds."""
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
    )
    base = {
        "done": True,          # frontier LOOKS exhausted ...
        "replan_reason": "failure",  # ... but a failure signal fired
        "round": 1,
        "no_progress": 0,
    }
    assert loop._route_after_collect(dict(base)) == "planner"

    # With no signal, the same exhausted frontier terminates as before.
    assert loop._route_after_collect({**base, "replan_reason": None}) == "synthesize"

    # A replan signal still yields to the hard round/stall bounds — it can never run away.
    assert loop._route_after_collect({**base, "round": 8}) == "synthesize"
    assert loop._route_after_collect({**base, "no_progress": 2}) == "synthesize"


def test_replan_reason_routes_to_recompile():
    """A failed node sets ``replan_reason``, routes back to the planner, which calls the EXISTING
    ``recompile`` to form a revision+1 plan re-planning the failed node while PINNING the executed
    prefix — no MonotonicityError for a valid superset."""
    fake = _fresh_fail_then_recover()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The failure signal forced a SECOND round via recompile; the run finished cleanly on the
    # recovered frontier (no MonotonicityError, no stall/round-cap).
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert result.rounds >= 2
    assert fake.run_count == result.rounds == result.supervisor_runs

    # Round 1's collect saw the failure and stamped the replan reason onto the swapped-in plan.
    assert result.state.replan_reason == "failure"

    # Round 2's recompile bumped the revision and PINNED the already-executed node verbatim, while
    # the failed node was re-planned back into the frontier.
    plan0, plan1 = result.state.plan_history[0], result.state.plan_history[1]
    assert plan1.revision == plan0.revision + 1
    assert plan0.revision == 0  # the prior plan VALUE is untouched after the swap (INV-3)
    assert "ingest" in store.completed()
    assert plan1.entries["ingest"] is plan0.entries["ingest"]  # pinned, same object
    assert plan1.wiring["ingest"] == plan0.wiring["ingest"]
    assert "summarize" in plan1.entries  # the failed node re-planned into the new frozen plan

    # The frontier ultimately exhausts — the recovered node validated.
    assert set(result.completed) == {"ingest", "summarize"}


def _detect_loop():
    """A bare GovernorLoop (empty store) for unit-testing ``_detect_replan_reason`` in isolation."""
    return GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        plan_model_fn=_plan_model_fn,
        confidence_threshold=0.5,
    )


def test_detect_replan_reason_contradiction():
    """Two nodes emitting DISTINCT non-null values under the same verdict key => 'contradiction'.

    Directly exercises the contradiction branch (loop.py: the ('verdict','decision','label') key
    tuple + ``len(seen) > 1``), which no end-to-end fake ever reaches (every other fake emits only
    ``doc``/``ok``/``error`` outputs).
    """
    loop = _detect_loop()
    # Disagreement under 'verdict'.
    ctx = {"outputs": {"a": {"verdict": "allow"}, "b": {"verdict": "block"}}}
    assert loop._detect_replan_reason(ctx, set()) == "contradiction"
    # Disagreement under 'decision' (a non-first key in the tuple).
    ctx = {"outputs": {"a": {"decision": "approve"}, "b": {"decision": "deny"}}}
    assert loop._detect_replan_reason(ctx, set()) == "contradiction"
    # Disagreement under 'label' (the last key in the tuple).
    ctx = {"outputs": {"a": {"label": 1}, "b": {"label": 2}}}
    assert loop._detect_replan_reason(ctx, set()) == "contradiction"

    # AGREEMENT under the same key (identical verdicts) is NOT a contradiction — guards against a
    # regression that flips ``len(seen) > 1`` to ``>= 1`` / ``> 0``.
    ctx = {"outputs": {"a": {"verdict": "allow"}, "b": {"verdict": "allow"}}}
    assert loop._detect_replan_reason(ctx, set()) is None
    # A single node with a lone verdict is not a contradiction either.
    ctx = {"outputs": {"a": {"verdict": "allow"}}}
    assert loop._detect_replan_reason(ctx, set()) is None
    # A None verdict is ignored (only distinct NON-null values count as disagreement).
    ctx = {"outputs": {"a": {"verdict": "allow"}, "b": {"verdict": None}}}
    assert loop._detect_replan_reason(ctx, set()) is None


def test_detect_replan_reason_low_confidence():
    """A node whose numeric ``confidence`` is below the threshold => 'low_confidence'.

    Directly exercises the low-confidence branch (loop.py: ``float(out['confidence']) <
    self._confidence_threshold``), which no end-to-end fake ever reaches.
    """
    loop = _detect_loop()  # confidence_threshold=0.5
    # Below threshold => low_confidence.
    ctx = {"outputs": {"a": {"confidence": 0.2}}}
    assert loop._detect_replan_reason(ctx, set()) == "low_confidence"
    # int below threshold is handled by the (int, float) guard.
    ctx = {"outputs": {"a": {"confidence": 0}}}
    assert loop._detect_replan_reason(ctx, set()) == "low_confidence"

    # At/above threshold => clean (guards against a regression that flips ``<`` to ``<=``).
    ctx = {"outputs": {"a": {"confidence": 0.5}}}
    assert loop._detect_replan_reason(ctx, set()) is None
    ctx = {"outputs": {"a": {"confidence": 0.9}}}
    assert loop._detect_replan_reason(ctx, set()) is None
    # A non-numeric confidence is ignored, not coerced.
    ctx = {"outputs": {"a": {"confidence": "high"}}}
    assert loop._detect_replan_reason(ctx, set()) is None


def test_detect_replan_reason_priority_and_clean():
    """Signal priority is failure > contradiction > low_confidence; a clean episode returns None."""
    loop = _detect_loop()
    # A clean episode (only benign output keys) yields no replan.
    ctx = {"outputs": {"a": {"doc": "x"}, "b": {"doc": "y"}}}
    assert loop._detect_replan_reason(ctx, set()) is None

    # failure outranks a co-occurring contradiction.
    ctx = {
        "outputs": {
            "a": {"verdict": "allow", "ok": False},
            "b": {"verdict": "block"},
        }
    }
    assert loop._detect_replan_reason(ctx, set()) == "failure"

    # contradiction outranks a co-occurring low-confidence signal.
    ctx = {
        "outputs": {
            "a": {"verdict": "allow", "confidence": 0.1},
            "b": {"verdict": "block"},
        }
    }
    assert loop._detect_replan_reason(ctx, set()) == "contradiction"


class _ContradictorySupervisor:
    """A store-bound fake whose two nodes emit DISAGREEING verdicts, surfacing the 'contradiction'
    replan signal COLLECT must detect end-to-end.

    Round 1 writes both nodes THROUGH to the shared store (so the frontier exhausts) with distinct
    verdict values; the contradiction signal then OVERRIDES frontier-exhaustion and forces the loop
    back to the planner every round until a hard bound (the ``no_progress`` stall) terminates it.
    """

    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store

    def run(self, inputs):
        type(self).run_count += 1
        verdicts = {"ingest": "allow", "summarize": "block"}
        already = set(self._store.completed())
        for node in self._plan.order:
            if node not in already:
                self._store.put(node, {"verdict": verdicts.get(node, node)})
        return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}


def test_contradiction_signal_drives_replan_end_to_end():
    """An episode with disagreeing verdicts sets replan_reason='contradiction' and loops back through
    recompile (a real 2nd round), yielding to the hard stall bound rather than running away."""
    _ContradictorySupervisor.run_count = 0
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: _ContradictorySupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=20,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The contradiction overrode frontier-exhaustion (all nodes completed) and forced replans until
    # the stall bound tripped — a real multi-round loop through recompile, never a runaway.
    assert result.state.replan_reason == "contradiction"
    assert result.rounds >= 2
    assert result.terminated_by == "no_progress"
    assert result.rounds < loop._step_cap()
    # The nodes did complete (the signal, not the frontier, drove the replans).
    assert set(result.completed) == {"ingest", "summarize"}


# == (C-2) durable MemoryStateStore backend + G-4 dual-resume =================
def test_loop_selects_memory_backend_keyed_by_session_id():
    """Passing ``memory_id``/``actor_id`` (no explicit ``store``) selects the SHIPPED
    :class:`MemoryStateStore` behind the Protocol, pinned to the runtime ``session_id`` as the
    durable ``sessionId`` — WIRING only, no new writer, no AWS (a fake AgentCore client)."""
    client = FakeMemoryClient()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        memory_id="mem-1",
        actor_id="team-1",
        session_id="S" * 40,
        memory_client=client,
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    store = loop._store
    assert isinstance(store, MemoryStateStore)
    # sessionId is pinned to the runtime session id (the durable resume key).
    assert store._session_id == "S" * 40
    assert store._memory_id == "mem-1"
    assert store._actor_id == "team-1"
    # An explicit store still wins verbatim (offline default path is untouched).
    explicit = InProcessStateStore()
    loop2 = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=explicit,
        memory_id="mem-1",
        actor_id="team-1",
        session_id="S" * 40,
        memory_client=client,
        plan_model_fn=_plan_model_fn,
    )
    assert loop2._store is explicit


def test_memory_backend_requires_session_and_actor():
    """A Memory backend (``memory_id`` set) MUST carry a durable ``sessionId`` + ``actor_id``, else
    a fresh store could not resume the same run — a config error, raised at construction."""
    import pytest

    with pytest.raises(GovernorLoopError, match="session_id"):
        GovernorLoop(
            "summarize the document",
            _two_node_manifests(),
            memory_id="mem-1",
            actor_id="team-1",
            plan_model_fn=_plan_model_fn,
        )
    with pytest.raises(GovernorLoopError, match="actor_id"):
        GovernorLoop(
            "summarize the document",
            _two_node_manifests(),
            memory_id="mem-1",
            session_id="S" * 40,
            plan_model_fn=_plan_model_fn,
        )


def test_loop_resumes_via_memory_backend():
    """G-4 dual-resume against a DURABLE log: run an episode over a fake-client
    :class:`MemoryStateStore`, then a FRESH store over the SAME
    ``(memory_id, actor_id, session_id)`` reconstructs ``completed()`` by replaying the surviving
    append-only event log (the log, not the loop, is the sole structural anchor; INV-5)."""
    # A single fake AgentCore client stands in for the durable Memory backend; both the first run's
    # store and the resume store speak to it, so the event log survives the first loop's teardown.
    client = FakeMemoryClient()
    fake = _fresh_fake()  # store-writing fake writes every plan node THROUGH to the shared store
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        memory_id="mem-1",
        actor_id="team-1",
        session_id="S" * 40,
        memory_client=client,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The episode drove to frontier-exhaustion and the outputs were folded into the durable log.
    assert result.done is True
    assert set(result.completed) == {"ingest", "summarize"}
    assert isinstance(loop._store, MemoryStateStore)
    # create_event fired against the fake AgentCore client — the log is genuinely durable, not
    # an in-process dict.
    assert len(client.created) >= 2

    # RESUME: a brand-new MemoryStateStore over the SAME (memory_id, actor_id, session_id) and the
    # SAME backing client replays the surviving event log and reconstructs the executed prefix —
    # no new writes, no loop state carried over.
    resumed = MemoryStateStore(
        memory_id="mem-1", session_id="S" * 40, actor_id="team-1", client=client
    )
    resumed.replay()
    assert resumed.completed() == {"ingest", "summarize"}
    assert resumed.get("ingest") == {"doc": "ingest-out"}
    assert resumed.get("summarize") == {"doc": "summarize-out"}
