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
    EventSink,
    GovernorLoop,
    GovernorLoopError,
    GovernorResult,
    NullEventSink,
    OrchestrationAssembler,
    plan_from_goal,
)
from concursus.governor import GovernorLoop as GovernorLoopFromSubpkg
from concursus.governor import EventSink as EventSinkFromSubpkg
from concursus.governor import NullEventSink as NullEventSinkFromSubpkg
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


# == (I-0) OPT-IN pre-freeze deliberation planner ============================
def _accepting_investigator(h):
    """A deterministic stub investigator that ACCEPTs every hypothesis (no LLM/langgraph)."""
    return {"verdict": "ACCEPT", "evidence": {"reason": "stub accept"}}


def _manifests_for_dag(dag):
    """Build a trivial single-output manifest per node of a (edge-free) deliberated DAG."""
    return {name: _manifest(name) for name in dag.nodes}


def test_deliberate_planner_adjusts_then_freezes(tmp_path):
    """With deliberate=True the round-1 DAG comes from a CONVERGED form_plan deliberation: the debate
    signs off (no ThreadNotResolved), the DAG is frozen/assembled, and the episode runs to
    frontier-exhaustion — the dynamic adjustment happened STRICTLY BEFORE assemble."""
    from concursus.reasoning.deliberate import form_plan
    from concursus.reasoning.trailstore import HypothesisTrail

    goal = "summarize the document"
    # Reproduce the deterministic converged DAG the loop will author, to size the manifests to it.
    expected_dag = form_plan(
        HypothesisTrail(tmp_path / "probe"), goal, investigator=_accepting_investigator
    )
    assert expected_dag.nodes  # the debate converged and lowered a non-empty frozen DAG

    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        goal,
        _manifests_for_dag(expected_dag),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        deliberate=True,
        trail_factory=lambda: HypothesisTrail(tmp_path / "gov_run"),
        investigator=_accepting_investigator,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The deliberated DAG was frozen + assembled into a plan and one static episode ran over it.
    assert result.done is True
    assert result.terminated_by == "frontier_exhaust"
    assert fake.run_count == result.rounds == result.supervisor_runs
    # The frozen round-1 plan is a real ProvisioningPlan whose order equals the converged DAG nodes.
    first_plan = result.state.plan_history[0]
    assert first_plan.revision == 0
    assert set(first_plan.order) == set(expected_dag.nodes)
    assert set(result.completed) == set(expected_dag.nodes)


def _decomposing_investigator(h):
    """A deterministic stub that DECOMPOSES the seed approach into two sharper children, then ACCEPTs.

    Unlike :func:`_accepting_investigator` (which closes the single seed hypothesis, yielding a
    one-node edge-free DAG), this exercises real pre-freeze ADJUSTMENT: the seed ``Approach: <goal>``
    root fans out into two child hypotheses, each accepted, so ``form_plan`` lowers a MULTI-node DAG
    with parent->child edges — proving the deliberation actually restructured the plan before freeze.
    """
    text = (h.text or "")
    if text.startswith("Approach:"):
        return [
            {"text": "extract text", "confidence": 0.9},
            {"text": "rank sentences", "confidence": 0.9},
        ]
    return {"verdict": "ACCEPT", "evidence": {"reason": "stub accept"}}


def test_deliberate_planner_multi_node_decomposition_flows_through_episode(tmp_path):
    """A DECOMPOSING investigator makes the pre-freeze debate fan the seed into multiple children:
    the frozen round-1 plan carries a multi-node order (with the parent->child sub-tree), and the
    episode runs over that adjusted plan to frontier-exhaustion — the dynamic adjustment is real,
    not a single-node passthrough."""
    from concursus.reasoning.deliberate import form_plan
    from concursus.reasoning.trailstore import HypothesisTrail

    goal = "summarize the document"
    expected_dag = form_plan(
        HypothesisTrail(tmp_path / "probe"), goal, investigator=_decomposing_investigator
    )
    # The debate decomposed the seed: strictly more than one node, with real parent->child edges.
    assert len(expected_dag.nodes) >= 3
    assert expected_dag.edges

    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        goal,
        _manifests_for_dag(expected_dag),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        deliberate=True,
        trail_factory=lambda: HypothesisTrail(tmp_path / "gov_run"),
        investigator=_decomposing_investigator,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    assert result.done is True
    assert result.terminated_by == "frontier_exhaust"
    first_plan = result.state.plan_history[0]
    assert first_plan.revision == 0
    # The frozen plan reflects the DECOMPOSED sub-tree, not the single seed hypothesis.
    assert set(first_plan.order) == set(expected_dag.nodes)
    assert len(first_plan.order) >= 3
    assert set(result.completed) == set(expected_dag.nodes)


def test_deliberate_resume_persistent_trail_pins_prefix_no_monotonicity_error(tmp_path):
    """G-4 resume UNDER deliberate=True with a PERSISTENT trail_factory: the re-authored round-0 DAG
    must reproduce the SAME node ids the first process committed, so ``recompile`` pins the executed
    prefix instead of raising MonotonicityError.

    Regression for the non-idempotent re-author defect: re-running ``form_plan`` on the SAME durable
    HypothesisTrail dir advances its monotonic seq counter (h1 -> h4), which would shift node names
    and drop the already-committed node on resume. The fix re-authors from a FRESH empty trail on
    resume, so the round-0 order matches the surviving log and growth stays monotonic."""
    from concursus.governor.loop import InProcessCheckpointStore
    from concursus.reasoning.deliberate import form_plan
    from concursus.reasoning.trailstore import HypothesisTrail

    goal = "summarize the document"
    # A multi-node deliberated DAG so the resume actually replays recompile (iteration >= 1); a
    # single-node DAG would exhaust in round 1 and never exercise the re-author-then-recompile path.
    expected_dag = form_plan(
        HypothesisTrail(tmp_path / "probe"), goal, investigator=_decomposing_investigator
    )
    assert len(expected_dag.nodes) >= 3
    manifests = _manifests_for_dag(expected_dag)

    # The DURABLE trail dir a production deliberation would reuse — the exact config that broke.
    gov_trail_dir = tmp_path / "gov_run"

    class _PartialSupervisor:
        """Completes exactly ONE new node per round; INNER-replays committed nodes (never re-invokes)."""

        invoked = []

        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
            self._plan = plan
            self._store = store

        def run(self, inputs):
            already = set(self._store.completed())
            for node in self._plan.order:
                if node in already:
                    continue
                type(self).invoked.append(node)
                self._store.put(node, {"doc": f"{node}-out"})
                break
            return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}

    store = InProcessStateStore()          # the sole structural anchor, survives the crash
    checkpointer = InProcessCheckpointStore()

    # --- process 1: run two rounds then "crash" (round_cap) — one node completes per round.
    _PartialSupervisor.invoked = []
    loop1 = GovernorLoop(
        goal,
        manifests,
        store=store,
        checkpointer=checkpointer,
        supervisor_factory=lambda **kw: _PartialSupervisor(**kw),
        deliberate=True,
        trail_factory=lambda: HypothesisTrail(gov_trail_dir),
        investigator=_decomposing_investigator,
        max_rounds=2,
        no_progress_n=5,
        run_id="delib-run",
        backend="python",
    )
    r1 = loop1.run({"uri": "s3://doc"})
    assert r1.terminated_by == "round_cap"
    assert r1.done is False
    committed_after_crash = set(store.completed())
    assert len(committed_after_crash) == 2  # two nodes committed before the crash
    ckpt = checkpointer.load("delib-run")
    assert ckpt["iteration"] == 1  # one recompile survived — resume MUST replay it

    # --- process 2: a BRAND-NEW loop over the SAME store + checkpointer + the SAME persistent trail
    # dir resumes. Before the fix this raised MonotonicityError because the re-authored DAG's node
    # ids had shifted (h1 -> h4) against the surviving log.
    _PartialSupervisor.invoked = []
    loop2 = GovernorLoop(
        goal,
        manifests,
        store=store,
        checkpointer=checkpointer,
        supervisor_factory=lambda **kw: _PartialSupervisor(**kw),
        deliberate=True,
        trail_factory=lambda: HypothesisTrail(gov_trail_dir),
        investigator=_decomposing_investigator,
        max_rounds=8,
        no_progress_n=5,
        run_id="delib-run",
        backend="python",
    )
    r2 = loop2.run({"uri": "s3://doc"})

    # Resume completed the journey; the committed prefix was PINNED (never re-invoked, never dropped).
    assert r2.done is True
    assert r2.terminated_by == "frontier_exhaust"
    assert set(r2.completed) == set(expected_dag.nodes)
    # Only the still-open node did fresh work in the resumed process — committed nodes were skipped.
    assert set(_PartialSupervisor.invoked) == set(expected_dag.nodes) - committed_after_crash
    # Monotonic revision growth: 0 (assemble) -> 1 -> 2, prior order surviving as a subsequence.
    assert [p.revision for p in r2.state.plan_history] == [0, 1, 2]


def test_no_deliberate_planner_is_single_shot_default():
    """With deliberate=False (the default) round-1 authoring is IDENTICAL to today's single-shot
    plan_from_goal path — the frozen plan matches one assembled independently, byte for byte."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    manifests = _two_node_manifests()

    # The single-shot DAG + plan the default path must reproduce exactly.
    dag = plan_from_goal("summarize the document", plan_model_fn=_plan_model_fn)
    expected_plan = OrchestrationAssembler().assemble(dag, manifests)

    loop = GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    assert loop._deliberate is False  # opt-in switch defaults OFF
    result = loop.run({"uri": "s3://doc"})

    first_plan = result.state.plan_history[0]
    assert first_plan.to_dict() == expected_plan.to_dict()
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


# == (I-1) OPT-IN Trust-Ladder ROUTER frontier gate ==========================
def _sched_manifest(name, *, side_effecting=False, trust_seed=None):
    """A manifest for the scheduler's process table (capabilities default to the agent name)."""
    data = {
        "name": name,
        "registry": {"container_uri": "img", "protocol": "HTTP"},
        "contract": {"inputs": {}, "outputs": {"doc": {"type": "string", "required": True}}},
        "side_effecting": side_effecting,
    }
    if trust_seed is not None:
        data["trust_seed"] = trust_seed
    return AgentManifest.from_dict(data)


class _HeldTrackingSupervisor:
    """A store-bound fake that honors the ROUTER's ``held`` set: it invokes (and writes) every
    still-open, NON-held plan node and NEVER touches a held node.

    Records which nodes it actually invoked (class-level) so a test can assert a below-bar node's
    agent was never invoked that round while a cleared node was.
    """

    invoked = []       # class-level ledger of nodes actually dispatched (across all rounds)
    run_count = 0
    seen_held = []     # the held-set the router handed the factory each round

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id, held=None):
        self._plan = plan
        self._store = store
        self._held = set(held or ())
        type(self).seen_held.append(set(self._held))

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        for node in self._plan.order:
            if node in already or node in self._held:
                continue  # skip completed AND held (escalated/unmatched) nodes — no invoke
            type(self).invoked.append(node)
            self._store.put(node, {"doc": f"{node}-out"})
        return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}


def _fresh_held_tracking():
    _HeldTrackingSupervisor.invoked = []
    _HeldTrackingSupervisor.run_count = 0
    _HeldTrackingSupervisor.seen_held = []
    return _HeldTrackingSupervisor


def _trust_scheduler(tmp_path):
    """A TrustLadderScheduler over a populated registry: ``summarize`` is side-effecting at a
    below-bar trust seed (→ ESCALATE), ``ingest`` is non-side-effecting (→ DISPATCH)."""
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    ledger = DeployLedger(tmp_path / "ledger.json")
    ledger.record(name="ingest", fingerprint="fp1", arn="arn:ingest", deployed_at="2026-07-01")
    ledger.record(name="summarize", fingerprint="fp2", arn="arn:summarize", deployed_at="2026-07-01")
    m_ingest = _sched_manifest("ingest", side_effecting=False)
    m_sum = _sched_manifest("summarize", side_effecting=True, trust_seed=TrustGrade.L1_CANARY)
    registry = AgentRegistry(ledger)
    registry.register_agent(m_ingest)
    registry.register_agent(m_sum)
    return TrustLadderScheduler(
        registry,
        manifests={"ingest": m_ingest, "summarize": m_sum},
        min_autonomy=TrustGrade.L2_GUARDED,          # bar above summarize's earned L1
        escalation_grade=TrustGrade.L3_AUTONOMOUS,
    )


def test_router_gates_frontier_by_trust(tmp_path):
    """With an OPT-IN scheduler wired, the ROUTER holds a below-bar node: its agent is NOT invoked
    that round (cleared nodes ARE invoked), the frozen plan is never mutated, the held node is
    surfaced on ``GovernorResult.escalated``, and the loop terminates on a hard bound."""
    fake = _fresh_held_tracking()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=_trust_scheduler(tmp_path),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # The cleared node dispatched and completed; the below-bar node was HELD — never invoked.
    assert "ingest" in fake.invoked
    assert "summarize" not in fake.invoked
    assert "ingest" in store.completed()
    assert "summarize" not in store.completed()

    # The router held 'summarize' every round (it was escalated, not dispatched).
    assert all("summarize" in held for held in fake.seen_held)

    # The escalation is surfaced for the cockpit exception queue.
    assert result.escalated == ["summarize"]

    # The frozen plan STILL carries the held node in its order (INV-3: never dropped — dropping it
    # would raise MonotonicityError on recompile). Holding is by non-dispatch, not plan mutation.
    first_plan = result.state.plan_history[0]
    assert set(first_plan.order) == {"ingest", "summarize"}

    # A held-forever node can never exhaust the frontier, so the loop terminates on the hard stall
    # bound — bounded, never a runaway.
    assert result.terminated_by == "no_progress"
    assert result.done is False
    assert result.rounds < loop._step_cap()


def _arn_two_node_manifests():
    """The two-node topology, but each manifest carries an embedded ``agent_runtime_arn`` — the
    normal production shape (a deployed agent is dispatchable because its manifest pins its ARN).

    This is the shape that DEFEATS an ARN-stripping hold: the real Supervisor re-derives the ARN
    from ``registry.agent_runtime_arn`` when the supplied ``arns`` dict omits it, so dropping the
    supplied dict would still dispatch a held node. The correct hold is the Supervisor's non-dispatch
    skip param.
    """
    def _m(name, **kw):
        m = _manifest(name, **kw)
        m.registry["agent_runtime_arn"] = f"arn:aws:bedrock-agentcore:us-east-1:1:runtime/{name}"
        return m

    return {
        "ingest": _m("ingest"),
        "summarize": _m(
            "summarize",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }


def test_router_holds_via_real_default_factory_no_invoke_no_raise(tmp_path):
    """END-TO-END through the REAL ``_default_supervisor_factory`` (no cooperative fake), over
    manifests carrying ``agent_runtime_arn``: a below-bar node is HELD via the Supervisor's
    non-dispatch skip — its invoke_fn is NEVER called, the run does NOT raise (no placeholder
    ARN-integrity crash), the cleared node dispatches, and the held node stays in ``plan.order``
    yet never completes. Locks the real seam that the ARN-strip mechanism silently bypassed/crashed.
    """
    invoked = []

    def _stub_invoke(arn, qualifier, session_id, payload):
        invoked.append(arn)
        return {"doc": "ok"}  # satisfies the manifest output schema

    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _arn_two_node_manifests(),
        store=store,
        scheduler=_trust_scheduler(tmp_path),
        invoke_fn=_stub_invoke,           # NO supervisor_factory override → real default factory
        plan_model_fn=_plan_model_fn,
        max_rounds=6,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})  # must NOT raise

    # The cleared node dispatched through the real Supervisor + stub invoke; the held (below-bar,
    # side-effecting) node was NEVER invoked — the trust gate held on the real seam.
    ingest_arn = "arn:aws:bedrock-agentcore:us-east-1:1:runtime/ingest"
    summarize_arn = "arn:aws:bedrock-agentcore:us-east-1:1:runtime/summarize"
    assert ingest_arn in invoked
    assert summarize_arn not in invoked
    assert "ingest" in store.completed()
    assert "summarize" not in store.completed()

    # Held node stays in the frozen plan.order (INV-3) and is surfaced for the cockpit.
    first_plan = result.state.plan_history[0]
    assert set(first_plan.order) == {"ingest", "summarize"}
    assert result.escalated == ["summarize"]
    # No failed record was written for the held node — a pure non-dispatch, not a failure.
    assert all(r.node != "summarize" for r in store.records())
    # Held forever → bounded stall termination, never a runaway.
    assert result.terminated_by == "no_progress"
    assert result.done is False


def test_router_surfaces_unmatched_held_node(tmp_path):
    """An UNMATCHED held node (no standing agent) is surfaced on ``GovernorResult.unmatched``, so a
    frontier stall it causes is observable to the cockpit exception queue rather than invisible."""
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    # Registry knows only 'ingest' → 'summarize' has NO standing agent (UNMATCHED, not escalated).
    ledger = DeployLedger(tmp_path / "ledger.json")
    ledger.record(name="ingest", fingerprint="fp1", arn="arn:ingest", deployed_at="2026-07-01")
    m_ingest = _sched_manifest("ingest", side_effecting=False)
    registry = AgentRegistry(ledger)
    registry.register_agent(m_ingest)
    scheduler = TrustLadderScheduler(
        registry,
        manifests={"ingest": m_ingest},
        min_autonomy=TrustGrade.L1_CANARY,
    )

    fake = _fresh_held_tracking()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=scheduler,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # 'summarize' had no standing agent → held as UNMATCHED, surfaced distinctly from escalated.
    assert "summarize" not in fake.invoked
    assert result.unmatched == ["summarize"]
    assert "summarize" not in result.escalated
    # The unmatched node stalls the frontier → bounded no_progress termination.
    assert result.terminated_by == "no_progress"
    assert result.done is False


def test_no_scheduler_router_is_passthrough():
    """With no scheduler (the default), the ROUTER is BYTE-FOR-BYTE today's pass-through: nothing is
    held or escalated, and the loop behaves exactly as before — both nodes complete on the first
    frontier-exhausting episode."""
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
    assert loop._scheduler is None  # opt-in switch defaults OFF

    result = loop.run({"uri": "s3://doc"})

    # Existing behavior unchanged: the frontier exhausts on the first episode, nothing was held.
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert result.escalated == []
    assert set(result.completed) == {"ingest", "summarize"}
    assert fake.run_count == result.rounds == result.supervisor_runs


def test_collect_reearns_trust_gov_side(tmp_path, monkeypatch):
    """With an OPT-IN scheduler wired, COLLECT re-earns GOV-side trust for each node that completed
    THIS round (I-2): after a clean episode the scheduler's earned_grade for a completed node is
    promoted by update_trust. The re-earn lives GOV-side in collect and NEVER calls the create-time
    ``evaluate_deploy_gate`` per-invocation — the gate is consulted only for the create-time seed (at
    most once per agent), so its call count does NOT scale with the number of episodes."""
    from concursus import TrustGrade
    import concursus.governor.scheduler as sched_mod

    # Spy on the create-time gate: it must be consulted only for seeding, never per-invocation.
    calls = {"n": 0}
    real_gate = sched_mod.evaluate_deploy_gate

    def _spy_gate(*args, **kwargs):
        calls["n"] += 1
        return real_gate(*args, **kwargs)

    monkeypatch.setattr(sched_mod, "evaluate_deploy_gate", _spy_gate)

    scheduler = _trust_scheduler(tmp_path)
    fake = _fresh_held_tracking()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=scheduler,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )

    # Seed 'ingest' lazily (one create-time gate consult) so we can compare before/after.
    before = scheduler.earned_grade("ingest")

    result = loop.run({"uri": "s3://doc"})

    after = scheduler.earned_grade("ingest")

    # 'ingest' (non-side-effecting → dispatched) completed and re-earned trust GOV-side: the clean
    # outcome promoted its earned grade one rung via update_trust.
    assert "ingest" in store.completed()
    assert int(after) > int(before)
    assert after == TrustGrade.L1_CANARY

    # 'summarize' (below-bar, side-effecting) was HELD → never completed → never re-earned.
    assert "summarize" not in store.completed()

    # The loop ran multiple episodes (summarize held forever → bounded no_progress), yet the
    # create-time gate was consulted at most ONCE PER AGENT — update_trust in collect never calls it.
    assert result.rounds >= 2
    assert calls["n"] <= len(_two_node_manifests())


def test_collect_reearns_trust_by_agent_name_not_task_label(tmp_path):
    """COLLECT must re-earn trust keyed by the AGENT NAME that served a node, not the plan NODE/task
    label. When an agent's capability label differs from its own name, keying on the raw node id
    would write a junk ladder key the next round's decide() never reads, so the earned grade would
    never move and an escalated below-bar agent could never clear the bar. Regression: the re-earn
    must move ``earned_grade(agent_name)``."""
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    # The plan node/task label is 'ingest'; the standing agent that SERVES it is named 'worker'
    # (a decoupled capability label != agent name — a fully supported registry configuration).
    ledger = DeployLedger(tmp_path / "ledger.json")
    ledger.record(name="worker", fingerprint="fp1", arn="arn:worker", deployed_at="2026-07-01")
    ledger.record(name="scribe", fingerprint="fp2", arn="arn:scribe", deployed_at="2026-07-01")
    m_worker = _sched_manifest("worker", side_effecting=False)
    m_scribe = _sched_manifest("scribe", side_effecting=False)
    registry = AgentRegistry(ledger)
    # 'worker' serves task 'ingest'; 'scribe' serves task 'summarize' — labels differ from names.
    registry.register_agent(m_worker, capabilities={"ingest"})
    registry.register_agent(m_scribe, capabilities={"summarize"})
    scheduler = TrustLadderScheduler(
        registry,
        manifests={"worker": m_worker, "scribe": m_scribe},
        min_autonomy=TrustGrade.L1_CANARY,
    )

    fake = _fresh_held_tracking()  # accepts the ROUTER's held= kwarg (a scheduler is wired)
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=scheduler,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )

    before_worker = scheduler.earned_grade("worker")   # seed the agent-name key
    result = loop.run({"uri": "s3://doc"})
    after_worker = scheduler.earned_grade("worker")

    # The 'ingest' node completed and re-earned trust under the AGENT NAME 'worker' (not 'ingest').
    assert "ingest" in store.completed()
    assert int(after_worker) > int(before_worker)
    # The task label is NOT a ladder key — the re-earn did not leak under the node id.
    assert "ingest" not in scheduler._earned
    # And 'scribe' likewise moved for the 'summarize' node it served.
    assert int(scheduler.earned_grade("scribe")) > int(TrustGrade.L0_SHADOW)
    assert "summarize" not in scheduler._earned


def test_resume_does_not_reearn_surviving_prefix_trust(tmp_path):
    """On checkpoint resume, the surviving executed prefix must NOT re-earn trust a second time.
    _maybe_restore seeds ctx['completed'] from the surviving log so the first post-resume collect
    treats the prefix as already-earned. Regression for the double-count that would spuriously
    promote a node's earned grade across a crash."""
    from concursus.governor.loop import InProcessCheckpointStore
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    def _make_scheduler():
        ledger = DeployLedger(tmp_path / "ledger.json")
        ledger.record(name="ingest", fingerprint="fp1", arn="arn:ingest", deployed_at="2026-07-01")
        ledger.record(name="summarize", fingerprint="fp2", arn="arn:s", deployed_at="2026-07-01")
        m_ingest = _sched_manifest("ingest", side_effecting=False)
        m_sum = _sched_manifest("summarize", side_effecting=False)
        registry = AgentRegistry(ledger)
        registry.register_agent(m_ingest)
        registry.register_agent(m_sum)
        return TrustLadderScheduler(
            registry,
            manifests={"ingest": m_ingest, "summarize": m_sum},
            min_autonomy=TrustGrade.L1_CANARY,
        )

    class _PartialHeldSupervisor:
        """Completes exactly ONE new NON-held node per round (accepts the scheduler's held= kwarg)."""

        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id, held=None):
            self._plan = plan
            self._store = store
            self._held = set(held or ())

        def run(self, inputs):
            already = set(self._store.completed())
            for node in self._plan.order:
                if node in already or node in self._held:
                    continue
                self._store.put(node, {"doc": f"{node}-out"})
                break
            return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}

    store = InProcessStateStore()          # the sole structural anchor, survives the crash
    checkpointer = InProcessCheckpointStore()

    # --- process 1: complete exactly ONE node (ingest) then "crash" on round_cap.
    sched1 = _make_scheduler()
    loop1 = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        checkpointer=checkpointer,
        scheduler=sched1,
        supervisor_factory=lambda **kw: _PartialHeldSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=1,
        no_progress_n=5,
        run_id="resume-run",
        backend="python",
    )
    r1 = loop1.run({"uri": "s3://doc"})
    assert r1.terminated_by == "round_cap"
    assert "ingest" in store.completed()

    # --- process 2: a fresh loop + FRESH scheduler resumes over the SAME store + checkpointer.
    sched2 = _make_scheduler()
    grade_before_resume = sched2.earned_grade("ingest")  # freshly seeded, pre-resume
    loop2 = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        checkpointer=checkpointer,
        scheduler=sched2,
        supervisor_factory=lambda **kw: _PartialHeldSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=5,
        run_id="resume-run",
        backend="python",
    )
    r2 = loop2.run({"uri": "s3://doc"})

    # 'ingest' survived from process 1; the resumed loop must NOT re-earn it — its earned grade is
    # UNCHANGED across the resume (equal to the fresh seed), NOT a spurious extra promotion.
    assert r2.done is True
    assert sched2.earned_grade("ingest") == grade_before_resume
    # 'summarize' finished in the resumed rounds → it DID re-earn (moved above its seed).
    assert int(sched2.earned_grade("summarize")) > int(grade_before_resume)


def test_no_scheduler_collect_unchanged():
    """With no scheduler (the default), COLLECT is BYTE-FOR-BYTE today's path: update_trust is never
    touched, nothing is escalated/unmatched, and both nodes complete on the first frontier-exhausting
    episode exactly as before."""
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
    assert loop._scheduler is None  # opt-in switch defaults OFF

    result = loop.run({"uri": "s3://doc"})

    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert set(result.completed) == {"ingest", "summarize"}
    assert result.escalated == []
    assert result.unmatched == []
    # One write per node in the append-only log — no extra trust bookkeeping records.
    assert {r.node for r in store.records()} == {"ingest", "summarize"}


def test_unmatched_stall_is_labeled(tmp_path):
    """When a scheduler's registry matches NOTHING for a frontier node, EVERY node is held UNMATCHED
    so the frontier never advances at all: the loop stalls and is labeled ``unmatched_stall`` (a
    mis-registered agent is legible), not the generic ``no_progress``."""
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    # A registry with NO agents at all → both 'ingest' and 'summarize' are UNMATCHED (no standing
    # agent) → held every round → nothing ever completes → the frontier never advances.
    ledger = DeployLedger(tmp_path / "ledger.json")
    registry = AgentRegistry(ledger)
    scheduler = TrustLadderScheduler(
        registry,
        manifests={},
        min_autonomy=TrustGrade.L1_CANARY,
    )

    fake = _fresh_held_tracking()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=scheduler,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=2,
        backend="python",
    )
    result = loop.run({"uri": "s3://doc"})

    # No standing agent matched any node → all held UNMATCHED, none dispatched, none completed.
    assert fake.invoked == []
    assert result.completed == []
    assert result.done is False
    assert set(result.unmatched) == {"ingest", "summarize"}
    # The mis-registration stall is legible instead of an indistinguishable no_progress.
    assert result.terminated_by == "unmatched_stall"


def test_normal_no_progress_not_mislabeled():
    """A no-progress stall with NO unmatched node (no scheduler → unmatched always empty) keeps the
    generic ``no_progress`` label — the new ``unmatched_stall`` branch can never fire on it."""

    class _NoopSupervisor:
        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
            self._plan = plan

        def run(self, inputs):
            return {}  # writes nothing → no forward progress, but nothing is unmatched either

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

    assert loop._scheduler is None  # no scheduler → unmatched is always empty
    assert result.unmatched == []
    assert result.terminated_by == "no_progress"
    assert result.done is False


# -- A4: wire the router's cleared frontier -> recompile(compile_next=) ----------

class _PartialHeldSupervisor:
    """A partial-progress supervisor that ALSO accepts the router's ``held=`` kwarg (needed when a
    scheduler is wired). Completes exactly one new, non-held plan node per round (via COLLECT, like
    ``_PartialSupervisor``), so a multi-node plan takes multiple rounds and the recompile branch
    fires with a non-empty ``completed`` set."""

    seen_plans = []
    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id, held=None):
        self._plan = plan
        self._store = store
        self._held = set(held or ())
        type(self).seen_plans.append(plan)

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        outputs = {}
        for node in self._plan.order:  # one NEW non-held node this round, in dispatch order
            if node in self._held:
                continue
            outputs[node] = {"doc": f"{node}-out"}
            if node not in already:
                break
        return outputs


def _fresh_partial_held():
    _PartialHeldSupervisor.seen_plans = []
    _PartialHeldSupervisor.run_count = 0
    return _PartialHeldSupervisor


def _all_dispatch_scheduler(tmp_path):
    """A TrustLadderScheduler where BOTH plan nodes clear the bar (→ DISPATCH), so the router's
    ``compile_next`` is non-empty and the loop makes progress across rounds (unlike the held-forever
    fixtures). Both agents are non-side-effecting at/above the autonomy bar."""
    from concursus import DeployLedger, TrustGrade
    from concursus.governor.registry import AgentRegistry
    from concursus.governor.scheduler import TrustLadderScheduler

    ledger = DeployLedger(tmp_path / "ledger.json")
    ledger.record(name="ingest", fingerprint="fp1", arn="arn:ingest", deployed_at="2026-07-01")
    ledger.record(name="summarize", fingerprint="fp2", arn="arn:summarize", deployed_at="2026-07-01")
    m_ingest = _sched_manifest("ingest", side_effecting=False)
    m_sum = _sched_manifest("summarize", side_effecting=False)
    registry = AgentRegistry(ledger)
    registry.register_agent(m_ingest)
    registry.register_agent(m_sum)
    return TrustLadderScheduler(
        registry,
        manifests={"ingest": m_ingest, "summarize": m_sum},
        min_autonomy=TrustGrade.L1_CANARY,   # both clear it → both DISPATCH → non-empty compile_next
    )


def test_record_frontier_threads_compile_next_into_recompile(tmp_path):
    """A4: with ``record_frontier=True`` + a scheduler, round-1's ROUTER cleared frontier
    (``FrontierProposal.compile_next``) is threaded into round-2's ``recompile(compile_next=)`` and
    RECORDED on the fresh frozen plan's read-only ``frontier`` field — closing the previously-dead
    scheduler→compiler channel WITHOUT changing order/entries/wiring (INV-3/4)."""
    fake = _fresh_partial_held()  # one new non-held node per round → forces a 2nd round (a recompile fires)
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=_all_dispatch_scheduler(tmp_path),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        backend="python",
        record_frontier=True,
    )
    result = loop.run({"uri": "s3://doc"})

    # Both nodes dispatch → the loop progresses one node/round and exhausts the frontier.
    assert result.terminated_by == "frontier_exhaust"
    assert result.rounds >= 2
    history = result.state.plan_history

    # Round-1 (revision 0) recorded NO frontier (no prior router ran before the first assemble).
    assert history[0].revision == 0
    assert history[0].frontier == []

    # Round-2's plan (a recompile) RECORDED round-1's cleared frontier on its read-only field —
    # filtered to topology nodes, a subset of plan.order, and it did NOT change order/entries/wiring.
    plan1 = history[1]
    assert plan1.revision == 1
    assert plan1.frontier, "expected the cleared frontier recorded on the recompiled plan"
    assert set(plan1.frontier) <= set(plan1.order)
    # 'ingest' cleared to dispatch in round 1, so it is in round-2's recorded frontier.
    assert "ingest" in plan1.frontier
    # INV-3/4: the recorded frontier is advisory only — order/entries/wiring match round-1's plan
    # for the pinned executed node.
    assert plan1.entries["ingest"] is history[0].entries["ingest"]
    assert plan1.wiring["ingest"] == history[0].wiring["ingest"]
    # And it surfaces in the plan preview only when non-empty.
    assert plan1.to_dict().get("frontier") == list(plan1.frontier)


def test_record_frontier_default_off_leaves_frontier_empty(tmp_path):
    """Back-compat: the DEFAULT (record_frontier=False) never records a frontier, even with a
    scheduler + multi-round recompiles — every plan's ``frontier`` stays empty and ``to_dict`` omits
    it (byte-for-byte unchanged)."""
    fake = _fresh_partial_held()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        scheduler=_all_dispatch_scheduler(tmp_path),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        backend="python",
        # record_frontier defaults False
    )
    result = loop.run({"uri": "s3://doc"})

    assert result.rounds >= 2
    for plan in result.state.plan_history:
        assert plan.frontier == []
        assert "frontier" not in plan.to_dict()  # emitted only when non-empty


# -- A2: decompose-mode authoring (loop calls staff_capability_dag) --------------
def test_decompose_mode_authors_and_runs_a_capability_plan():
    """A2: GovernorLoop(decompose=True) authors a MULTI-NODE capability DAG, STAFFS it into an
    assemblable manifest set with NO caller manifests, and runs it end-to-end (cold start)."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "investigate the checkout latency regression",
        {},  # ZERO caller manifests — the loop staffs the capability DAG itself
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        max_rounds=8,
        no_progress_n=2,
        backend="python",
        decompose=True,
    )
    result = loop.run({"uri": "s3://doc"})

    # A real multi-node capability plan was authored + frozen (not the 1-node fallback).
    first_plan = result.state.plan_history[0]
    assert len(first_plan.order) > 1
    assert all("__" in n for n in first_plan.order)          # agent-agnostic capability labels
    # It ran to frontier-exhaustion over the staffed manifests.
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert set(result.completed) == set(first_plan.order)


def test_decompose_mode_binds_via_bind_fn():
    """A2: bind_fn lets the loop bind capability nodes to standing agents (recorded as bound_agent)."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "investigate root cause",
        {},
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        backend="python",
        decompose=True,
        bind_fn=lambda node: "veteran" if "scope" in node else None,  # bind the scope stage
    )
    result = loop.run({})
    # The staffed manifest for the scope node records the binding; others were authored.
    staffed = loop._staffed_manifests
    scope_nodes = [n for n in staffed if "scope" in n]
    assert scope_nodes and staffed[scope_nodes[0]].registry.get("bound_agent") == "veteran"
    assert result.done is True


def test_decompose_mode_default_off_unchanged():
    """Back-compat: without decompose=, authoring is byte-for-byte the single-shot manifest path."""
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        # decompose defaults False
    )
    result = loop.run({"uri": "s3://doc"})
    assert loop._staffed_manifests is None            # no staffing happened
    assert set(result.completed) == {"ingest", "summarize"}  # the caller's manifest topology


def test_decompose_mode_resume_reproduces_staffed_manifests(tmp_path):
    """A2 + resume: re-authoring on resume re-derives the IDENTICAL staffed manifest set + DAG order,
    so the executed prefix stays pinned (INV-4)."""
    from concursus.governor.loop import InProcessCheckpointStore

    goal = "build a detection model"
    ckpt = InProcessCheckpointStore()
    store = InProcessStateStore()
    loop1 = GovernorLoop(goal, {}, store=store, checkpointer=ckpt,
                         supervisor_factory=lambda **kw: _fresh_partial()(**kw),
                         backend="python", decompose=True, max_rounds=1, no_progress_n=5)
    loop1.run({})
    staffed1 = dict(loop1._staffed_manifests)

    # A fresh loop over the SAME goal + surviving log/checkpoint re-authors identically.
    loop2 = GovernorLoop(goal, {}, store=store, checkpointer=ckpt,
                         supervisor_factory=lambda **kw: _fresh_partial()(**kw),
                         backend="python", decompose=True, max_rounds=8, no_progress_n=5)
    result2 = loop2.run({})
    staffed2 = dict(loop2._staffed_manifests)

    # Same capability nodes + same authored order => deterministic re-derivation (INV-4).
    assert set(staffed1) == set(staffed2)
    assert [m.name for m in staffed1.values()] == [m.name for m in staffed2.values()]
    assert result2.done is True


# ============================================================ OPT-IN episode-boundary gate + sink
# Two default-OFF, opt-in seams on GovernorLoop, both scoped to EPISODE BOUNDARIES (never
# mid-episode): (a) an ``episode_gate`` callback that can pause/approve/abort BETWEEN episodes, and
# (b) an injected ``event_sink`` (Protocol emit(event)) that receives episode_start/episode_end/
# decision boundary events. The default path (both unset) must be byte-for-byte unchanged.


class _RecordingSink:
    """A test :class:`EventSink`: appends every emitted VALUE event to a list."""

    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(dict(event))


def test_event_sink_is_a_protocol_and_reexported():
    """EventSink/NullEventSink are exported from both the package and the subpackage."""
    assert EventSink is EventSinkFromSubpkg
    assert NullEventSink is NullEventSinkFromSubpkg
    # NullEventSink is a concrete no-op that satisfies the Protocol.
    assert isinstance(NullEventSink(), EventSink)
    NullEventSink().emit({"type": "episode_start"})  # never raises


def test_null_sink_run_is_byte_identical_to_default():
    """A run with event_sink=NullEventSink() returns a GovernorResult byte-identical to the default
    (no-sink) run, and leaves the append-only log identical — emitting never mutates the run."""
    def _run(sink):
        fake = _fresh_fake()
        store = InProcessStateStore()
        loop = GovernorLoop(
            "summarize the document",
            _two_node_manifests(),
            store=store,
            supervisor_factory=lambda **kw: fake(**kw),
            plan_model_fn=_plan_model_fn,
            backend="python",
            event_sink=sink,
        )
        result = loop.run({"uri": "s3://doc"})
        log = [(r.node, r.status, r.content_hash) for r in store.records()]
        return result, log

    baseline, baseline_log = _run(None)
    with_null, null_log = _run(NullEventSink())

    # The observable result is identical field-for-field.
    assert with_null.terminated_by == baseline.terminated_by == "frontier_exhaust"
    assert with_null.rounds == baseline.rounds
    assert with_null.done == baseline.done
    assert with_null.completed == baseline.completed
    assert with_null.frontier == baseline.frontier
    assert with_null.trace == baseline.trace
    assert with_null.supervisor_runs == baseline.supervisor_runs
    # And the append-only log is byte-identical: the no-op sink wrote nothing.
    assert null_log == baseline_log


def test_event_sink_emits_episode_boundary_events():
    """A recording sink receives episode_start/episode_end/decision events, each a plain-dict VALUE
    carrying the boundary (round/completed/frontier) — never a live plan/ctx handle."""
    fake = _fresh_fake()
    sink = _RecordingSink()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        event_sink=sink,
    )
    result = loop.run({"uri": "s3://doc"})

    types = [e["type"] for e in sink.events]
    # The single frontier-exhausting round emits exactly one of each boundary event, in order.
    assert types == ["episode_start", "episode_end", "decision"]
    # Every event is a pure VALUE dict (no plan/ctx object leaked through).
    for e in sink.events:
        assert e["run_id"] == "governor"
        assert isinstance(e["completed"], list)
        assert isinstance(e["frontier"], list)
        assert "plan" not in e and "state" not in e
    end = next(e for e in sink.events if e["type"] == "episode_end")
    assert set(end["completed"]) == {"ingest", "summarize"}
    assert end["done"] is True
    decision = next(e for e in sink.events if e["type"] == "decision")
    assert decision["route"] == "synthesize"
    assert decision["terminated_by"] == "frontier_exhaust"
    assert result.terminated_by == "frontier_exhaust"


def test_misbehaving_sink_never_breaks_the_loop():
    """A sink whose emit() raises must not break a live episode — emission is swallowed."""
    class _BoomSink:
        def emit(self, event):
            raise RuntimeError("observer blew up")

    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        event_sink=_BoomSink(),
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True


def test_gate_abort_stops_between_episodes_no_episode_runs():
    """An episode_gate returning 'abort' stops the loop BETWEEN episodes: no Supervisor is dispatched,
    the frontier stays open, and termination is labeled 'aborted' (INV-1: never mid-episode)."""
    fake = _fresh_fake()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        episode_gate=lambda boundary: "abort",
    )
    result = loop.run({"uri": "s3://doc"})

    assert result.terminated_by == "aborted"
    assert result.done is False
    # NO episode ran — the gate stopped it before any dispatch (INV-1).
    assert fake.run_count == 0
    assert result.supervisor_runs == 0
    # Nothing was written to the append-only log (no node was invoked).
    assert store.records() == []
    assert result.completed == []
    # The frozen plan's frontier is still fully open.
    assert set(result.frontier) == {"ingest", "summarize"}
    # The bounded chain still visited router+run_episode+collect before finalizing at synthesize.
    assert result.trace[-1] == "synthesize"


def test_gate_abort_after_one_round_stops_at_boundary():
    """A gate that approves round 1 then aborts stops at the NEXT episode boundary — the first
    episode ran to completion (single static pass), the second was never dispatched."""
    fake = _fresh_partial()  # completes exactly one node per round -> forces a 2nd round
    store = InProcessStateStore()
    calls = {"n": 0}

    def gate(boundary):
        calls["n"] += 1
        # Approve the first boundary (round 0 -> episode 1), abort the second.
        return "abort" if calls["n"] >= 2 else "approve"

    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        max_rounds=8,
        no_progress_n=5,
        episode_gate=gate,
    )
    result = loop.run({"uri": "s3://doc"})

    assert result.terminated_by == "aborted"
    # Exactly ONE episode ran (round 1); the gate stopped the loop at the 2nd boundary.
    assert fake.run_count == 1
    assert result.supervisor_runs == 1
    assert result.rounds == 1
    # One node completed in that single episode; the frontier is NOT exhausted.
    assert len(result.completed) == 1
    assert result.done is False


def test_gate_pause_labels_paused_and_runs_no_episode():
    """A gate returning 'pause' stops between episodes labeled 'paused' (a warm-resumable boundary)."""
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        episode_gate=lambda boundary: "pause",
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.terminated_by == "paused"
    assert result.done is False
    assert fake.run_count == 0


def test_gate_approve_is_byte_identical_to_default():
    """A gate that always approves ('continue') yields a run identical to the ungated default."""
    def _run(gate):
        fake = _fresh_fake()
        store = InProcessStateStore()
        loop = GovernorLoop(
            "summarize the document",
            _two_node_manifests(),
            store=store,
            supervisor_factory=lambda **kw: fake(**kw),
            plan_model_fn=_plan_model_fn,
            backend="python",
            episode_gate=gate,
        )
        result = loop.run({"uri": "s3://doc"})
        return result, [(r.node, r.status) for r in store.records()]

    baseline, baseline_log = _run(None)
    approved, approved_log = _run(lambda boundary: "continue")
    assert approved.terminated_by == baseline.terminated_by == "frontier_exhaust"
    assert approved.completed == baseline.completed
    assert approved.rounds == baseline.rounds
    assert approved.trace == baseline.trace
    assert approved_log == baseline_log


def test_gate_receives_readonly_boundary_value():
    """The gate is handed a plain-dict boundary VALUE (round/completed/frontier), never a live plan."""
    seen = []
    fake = _fresh_fake()
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        episode_gate=lambda boundary: seen.append(dict(boundary)) or "approve",
    )
    loop.run({"uri": "s3://doc"})
    assert seen  # consulted at least once
    b = seen[0]
    assert b["type"] == "episode_boundary"
    assert b["run_id"] == "governor"
    assert b["round"] == 0
    assert b["completed"] == []
    assert set(b["frontier"]) == {"ingest", "summarize"}
    assert "plan" not in b and "state" not in b


def test_buggy_gate_fails_open_to_default():
    """A gate that raises must fail OPEN (the loop runs the ungated bounded default), never crash."""
    fake = _fresh_fake()
    def gate(boundary):
        raise RuntimeError("gate blew up")
    loop = GovernorLoop(
        "summarize the document",
        _two_node_manifests(),
        store=InProcessStateStore(),
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
        episode_gate=gate,
    )
    result = loop.run({"uri": "s3://doc"})
    # Fail-open: the episode still ran and the frontier exhausted as in the default path.
    assert result.terminated_by == "frontier_exhaust"
    assert result.done is True
    assert fake.run_count == 1
