"""Full-stack end-to-end integration for the governed dynamic-governor hybrid.

The other governor test files exercise each seam IN ISOLATION (often with a *fake* per-episode
supervisor). This file composes the WHOLE stack against the REAL
:class:`~concursus.execute.supervisor.Supervisor` and asserts the pieces actually fit together:

* **Horizon 1** — ``deliberate=True`` authors the round-1 DAG via the ``form_plan`` deliberation
  tier, reconciled to the manifests' ground-truth topology before ``assemble`` (regression for the
  deliberate-DAG/manifest ``AlignmentError`` an isolated ``form_plan`` test could not surface).
* **Horizon 2** — the Trust-Ladder ROUTER holds a below-bar side-effecting node by NON-DISPATCH in
  the real ``Supervisor.run`` (the held node is never invoked, stays in the frozen plan).
* **Governance surfaces** — the escalation reaches ``GovernorResult.escalated`` and the read-only
  director cockpit's exception queue.
* **Durability** — a real ``MemoryStateStore`` (fake AgentCore client) run + ``checkpoint()`` +
  warm resume reconstructs the completed set; a governed loop with a checkpointer still escalates.

Everything is offline: a fake ``invoke_fn`` + a fake AgentCore Memory client; no boto3, no AWS,
no LLM (the deliberation runs on its deterministic stub). Runs identically with langgraph absent.
"""

import tempfile
from pathlib import Path

import pytest

from concursus import AgentManifest, DeployLedger, TrustGrade
from concursus.governor import (
    AgentRegistry,
    GovernorLoop,
    GovernorLoopError,
    InProcessCheckpointStore,
    InProcessEventQueue,
    KTLODaemon,
    TrustLadderScheduler,
)
from concursus.governor.cockpit import DirectorCockpit
from concursus.state.statestore import MemoryStateStore

# The shipped filter-honoring fake AgentCore Memory client lives in the statestore tests; pytest
# puts tests/ on sys.path so a real MemoryStateStore can be driven offline (no boto3, no AWS).
from test_statestore import FilteringFakeMemoryClient

_ARNS = {"ingest": "arn:ingest", "summarize": "arn:summarize"}


def _manifest(name, *, side_effecting=False, trust_seed=None, depends_on=None):
    """A COMPLETE manifest (registry.entry/role_arn present, so a real assemble synthesizes)."""
    data = {
        "name": name,
        "registry": {
            "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
            "protocol": "HTTP",
            "entry": f"agents.{name}:run",
            "role_arn": "arn:aws:iam::123456789012:role/agent",
        },
        "contract": {
            "inputs": {"document": {"type": "string"}} if depends_on else {},
            "outputs": {"doc": {"type": "string", "required": True}},
        },
        "side_effecting": side_effecting,
    }
    if trust_seed is not None:
        data["trust_seed"] = trust_seed
    if depends_on:
        data["spec"] = {"depends_on": depends_on}
    return AgentManifest.from_dict(data)


def _manifests():
    """A realistic 2-node team: read-only ``ingest`` -> SIDE-EFFECTING ``summarize`` (seed L1)."""
    return {
        "ingest": _manifest("ingest"),
        "summarize": _manifest(
            "summarize",
            side_effecting=True,
            trust_seed=TrustGrade.L1_CANARY,
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }


class _RecordingInvoke:
    """A fake :data:`InvokeFn`: records which agents were actually invoked, returns a canned doc."""

    def __init__(self):
        self.calls = []

    def __call__(self, arn, qualifier, session_id, payload_bytes):
        node = arn.split(":")[-1]
        self.calls.append(node)
        return {"doc": f"{node}-out"}


def _plan_model_fn(goal, precedents, directives):
    """Single-shot topology matching the manifests (the ``deliberate=False`` author seam)."""
    return {"nodes": ["ingest", "summarize"], "edges": [["ingest", "summarize"]]}


def _scheduler(tmp_path, *, min_autonomy=TrustGrade.L2_GUARDED):
    """A Trust-Ladder scheduler over a populated registry; ``summarize`` is side-effecting at seed
    L1, below an L2 bar => ESCALATE (held); ``ingest`` is read-only => DISPATCH."""
    manifests = _manifests()
    ledger = DeployLedger(tmp_path / "ledger.json")
    ledger.record(name="ingest", fingerprint="f1", arn="arn:ingest", deployed_at="2026-07-01")
    ledger.record(name="summarize", fingerprint="f2", arn="arn:summarize", deployed_at="2026-07-01")
    registry = AgentRegistry(ledger)
    for m in manifests.values():
        registry.register_agent(m)
    return TrustLadderScheduler(registry, manifests=manifests, min_autonomy=min_autonomy)


# == Horizon 1: deliberate-before-signoff assembles against real manifests ===================
@pytest.mark.parametrize("deliberate", [False, True])
def test_e2e_loop_completes_over_real_supervisor(deliberate):
    """A GovernorLoop drives a REAL Supervisor to completion — for BOTH the single-shot planner and
    the deliberate-before-signoff planner (regression: the deliberated DAG must reconcile to the
    manifest topology, or assemble would raise AlignmentError / AssemblyError)."""
    invoke = _RecordingInvoke()
    kwargs = dict(invoke_fn=invoke, arns=_ARNS, backend="python")
    if not deliberate:
        kwargs["plan_model_fn"] = _plan_model_fn
    loop = GovernorLoop("summarize the document", _manifests(), deliberate=deliberate, **kwargs)
    result = loop.run({"uri": "s3://doc"})

    assert set(result.completed) == {"ingest", "summarize"}
    assert result.state.current_frozen_plan.order == ["ingest", "summarize"]
    assert result.terminated_by == "frontier_exhaust"
    assert invoke.calls == ["ingest", "summarize"]  # real Supervisor invoked both, in order


# == Horizon 2: real Supervisor honors the Trust-Ladder hold (below-bar side-effecting node) ==
@pytest.mark.parametrize("deliberate", [False, True])
def test_e2e_below_bar_node_is_escalated_and_never_invoked(deliberate, tmp_path):
    """A below-bar SIDE-EFFECTING node is ESCALATED by the router and NEVER invoked by the real
    Supervisor (held by non-dispatch); the read-only agent still runs. Holds for both planners."""
    invoke = _RecordingInvoke()
    kwargs = dict(scheduler=_scheduler(tmp_path), invoke_fn=invoke, arns=_ARNS, backend="python")
    if not deliberate:
        kwargs["plan_model_fn"] = _plan_model_fn
    loop = GovernorLoop("summarize the document", _manifests(), deliberate=deliberate, **kwargs)
    result = loop.run({"uri": "s3://doc"})

    assert result.escalated == ["summarize"]           # surfaced for the director
    assert "summarize" not in invoke.calls             # the real Supervisor NEVER invoked it
    assert invoke.calls == ["ingest"]                  # only the cleared read-only node ran
    assert "ingest" in set(result.completed)
    assert "summarize" not in set(result.completed)
    # The frozen plan still carries summarize (held by non-dispatch, NOT dropped — INV-3/INV-4).
    assert set(result.state.current_frozen_plan.order) == {"ingest", "summarize"}


def test_e2e_cockpit_surfaces_escalation_as_judgment_row(tmp_path):
    """The read-only director cockpit's exception queue surfaces the trust escalation distinctly."""
    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        scheduler=_scheduler(tmp_path), invoke_fn=invoke, arns=_ARNS, backend="python",
        plan_model_fn=_plan_model_fn,
    )
    loop.run({"uri": "s3://doc"})
    cockpit = loop.cockpit()
    reasons = {row["node"]: row["reason"] for row in cockpit.exception_queue()}
    assert reasons.get("summarize") == "escalated"


def test_e2e_no_scheduler_default_dispatches_everything(tmp_path):
    """Opt-in OFF (no scheduler): the real Supervisor invokes every node — byte-for-byte today."""
    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        invoke_fn=invoke, arns=_ARNS, backend="python", plan_model_fn=_plan_model_fn,
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.escalated == []
    assert invoke.calls == ["ingest", "summarize"]
    assert set(result.completed) == {"ingest", "summarize"}


# == Durability: real MemoryStateStore + checkpoint + warm resume over a real episode ========
def test_e2e_memory_backed_episode_checkpoints_and_warm_resumes():
    """A REAL Supervisor episode writing through a real MemoryStateStore (fake client) can be
    checkpoint-compacted, and a fresh store warm-resumes the completed set from the durable log."""
    client = FilteringFakeMemoryClient()
    store = MemoryStateStore(memory_id="m", session_id="S" * 40, actor_id="team-1", client=client)
    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        store=store, invoke_fn=invoke, arns=_ARNS, backend="python", plan_model_fn=_plan_model_fn,
    )
    result = loop.run({"uri": "s3://doc"})
    assert set(result.completed) == {"ingest", "summarize"}

    store.checkpoint()  # compact + rotate epoch

    # Warm resume in a FRESH store over the SAME durable log == full replay.
    warm = MemoryStateStore(
        memory_id="m", session_id="S" * 40, actor_id="team-1",
        client=FilteringFakeMemoryClient(events=list(client._events)),
    )
    warm.replay()
    assert warm.completed() == {"ingest", "summarize"}
    full = MemoryStateStore(
        memory_id="m", session_id="S" * 40, actor_id="team-1",
        client=FilteringFakeMemoryClient(events=list(client._events)),
    )
    full.replay(force_full=True)
    assert warm.completed() == full.completed()
    for node in full.completed():
        assert warm.get(node) == full.get(node)


def test_e2e_governed_loop_with_checkpointer_still_escalates(tmp_path):
    """A governed loop wired with an outer InProcessCheckpointStore still escalates the below-bar
    node (the Phase-5 seams compose with the Phase-1 dual-resume checkpointer)."""
    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        scheduler=_scheduler(tmp_path), invoke_fn=invoke, arns=_ARNS, backend="python",
        checkpointer=InProcessCheckpointStore(), run_id="e2e-run", plan_model_fn=_plan_model_fn,
    )
    result = loop.run({"uri": "s3://doc"})
    assert result.escalated == ["summarize"]
    assert "ingest" in set(result.completed)


# == Governed KTLO standing crew: multi-ticket, real Supervisor, deliberate + trust-gated =====
def test_e2e_governed_ktlo_standing_crew_over_real_supervisor(tmp_path):
    """The FULL stack: a governed KTLO daemon drains a multi-ticket source, spawning one bounded
    freeze episode per ticket over the REAL Supervisor, each deliberating its plan (Horizon 1) and
    holding the below-bar node by trust (Horizon 2) — N tickets => N independent governed episodes."""
    invoke = _RecordingInvoke()
    tickets = [
        {"id": "t1", "goal": "summarize doc t1", "inputs": {"uri": "s3://d1"}},
        {"id": "t2", "goal": "summarize doc t2", "inputs": {"uri": "s3://d2"}},
        {"id": "t3", "goal": "summarize doc t3", "inputs": {"uri": "s3://d3"}},
    ]
    daemon = KTLODaemon(
        _manifests(),
        source=InProcessEventQueue(tickets, closed=True),
        mode="ktlo",
        scheduler=_scheduler(tmp_path),
        deliberate=True,                 # Horizon 1 pre-signoff planning on each episode
        invoke_fn=invoke,
        arns=_ARNS,
        backend="python",
    )
    result = daemon.run()

    assert result.terminated_by == "source_drained"
    assert len(result.episodes) == 3
    for episode in result.episodes:
        # Each episode was GOVERNED: below-bar summarize held, read-only ingest completed.
        assert episode.escalated == ["summarize"]
        assert "ingest" in set(episode.completed)
        assert "summarize" not in set(episode.completed)
    # Each episode is a FRESH bounded frozen plan (INV-4): three distinct plan objects, revision 0.
    assert len({id(p) for p in result.episode_plans}) == 3
    assert all(p.revision == 0 for p in result.episode_plans)
    # summarize was NEVER invoked across the whole standing run (held every episode).
    assert invoke.calls == ["ingest", "ingest", "ingest"]


# == C-4 auto-checkpoint cadence (optimization: realize the bounded warm resume automatically) ==
class _SpyCheckpointMemory(MemoryStateStore):
    """A MemoryStateStore that counts checkpoint() calls, to prove the loop's auto-cadence fires."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.checkpoint_calls = 0

    def checkpoint(self):
        self.checkpoint_calls += 1
        return super().checkpoint()


def test_e2e_auto_checkpoint_fires_at_cadence_over_memory_store():
    """checkpoint_every=1 makes the loop auto-compact the Memory log each round — so a long-running
    loop stays warm-resumable WITHOUT the caller remembering to checkpoint (the C-4 gap 35e9g flagged)."""
    invoke = _RecordingInvoke()
    client = FilteringFakeMemoryClient()
    store = _SpyCheckpointMemory(memory_id="m", session_id="S" * 40, actor_id="team-1", client=client)
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        store=store, invoke_fn=invoke, arns=_ARNS, backend="python",
        plan_model_fn=_plan_model_fn, checkpoint_every=1,
    )
    result = loop.run({"uri": "s3://doc"})
    assert set(result.completed) == {"ingest", "summarize"}
    # The auto-cadence fired at the round boundary and wrote a real checkpoint event to the log.
    assert store.checkpoint_calls >= 1
    checkpoint_events = [e for e in client._events if e["metadata"].get("record_type") == "checkpoint"]
    assert len(checkpoint_events) >= 1
    # And a warm resume still reconstructs the projection identically (compaction is INV-5-safe).
    warm = MemoryStateStore(
        memory_id="m", session_id="S" * 40, actor_id="team-1",
        client=FilteringFakeMemoryClient(events=list(client._events)),
    )
    warm.replay()
    assert warm.completed() == {"ingest", "summarize"}


def test_e2e_auto_checkpoint_default_off_is_byte_for_byte_unchanged():
    """checkpoint_every defaults to 0 => the loop NEVER auto-checkpoints (today's behavior)."""
    invoke = _RecordingInvoke()
    client = FilteringFakeMemoryClient()
    store = _SpyCheckpointMemory(memory_id="m", session_id="S" * 40, actor_id="team-1", client=client)
    GovernorLoop(
        "summarize the document", _manifests(),
        store=store, invoke_fn=invoke, arns=_ARNS, backend="python", plan_model_fn=_plan_model_fn,
    ).run({"uri": "s3://doc"})
    assert store.checkpoint_calls == 0
    assert not [e for e in client._events if e["metadata"].get("record_type") == "checkpoint"]


def test_e2e_auto_checkpoint_noop_on_store_without_checkpoint(tmp_path):
    """A store with no checkpoint() (the in-process default) silently no-ops the cadence — the loop
    still completes; nothing raises."""
    from concursus.state.statestore import InProcessStateStore

    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        store=InProcessStateStore(), invoke_fn=invoke, arns=_ARNS, backend="python",
        plan_model_fn=_plan_model_fn, checkpoint_every=1,
    )
    result = loop.run({"uri": "s3://doc"})  # must not raise on the missing checkpoint()
    assert set(result.completed) == {"ingest", "summarize"}


def test_e2e_negative_checkpoint_every_rejected():
    """checkpoint_every must be >= 0."""
    with pytest.raises(GovernorLoopError):
        GovernorLoop("g", _manifests(), checkpoint_every=-1)


# == resume + deliberate reconcile (close the coverage gap: the 35e9f fix on the resume path) ==
def test_e2e_deliberate_resume_reconciles_and_completes(tmp_path):
    """deliberate=True composed with the outer checkpointer/resume path still reconciles the
    deliberated DAG to the manifest topology and runs to completion — the 35e9f fix holds on resume
    (which re-authors the round-0 DAG via _author_first_dag(resume=True) -> _reconcile_dag_with_manifests)."""
    invoke = _RecordingInvoke()
    loop = GovernorLoop(
        "summarize the document", _manifests(),
        deliberate=True, invoke_fn=invoke, arns=_ARNS, backend="python",
        checkpointer=InProcessCheckpointStore(), run_id="delib-resume",
    )
    result = loop.run({"uri": "s3://doc"})
    # The deliberated DAG reconciled to the manifest topology (no AlignmentError/AssemblyError).
    assert set(result.completed) == {"ingest", "summarize"}
    assert result.state.current_frozen_plan.order == ["ingest", "summarize"]


# == FZ 35e2b3b C1: cold-start north-star — a NOVEL goal launches end-to-end, zero bench =======
class _ResultInvoke:
    """A fake InvokeFn for staffed capability roles (their output schema is ``{result}``)."""

    def __init__(self):
        self.calls = []

    def __call__(self, arn, qualifier, session_id, payload_bytes):
        self.calls.append(arn.split(":")[-1])
        return {"result": f"{arn.split(':')[-1]}-out"}


def test_e2e_cold_start_novel_goal_launches_with_zero_manifests():
    """THE NORTH-STAR (plan P0.2): a goal with NO precedent, NO matching agents, and NO caller
    manifests launches end-to-end via decompose -> staff -> assemble through the REAL Supervisor.
    Before this line of work it produced a 1-node plan / needed hand-authored manifests; now the
    loop decomposes into a capability chain, authors a role per capability, and runs it."""
    from concursus.assemble.planner import plan_from_goal

    goal = "investigate the checkout latency regression"
    # A deploy step would provision each authored role; simulate that by supplying an ARN per node.
    nodes = plan_from_goal(goal, decompose=True).nodes
    arns = {n: f"arn:{n}" for n in nodes}
    invoke = _ResultInvoke()

    loop = GovernorLoop(
        goal, {},                       # ZERO caller manifests — the cold-start premise
        invoke_fn=invoke, arns=arns, backend="python", decompose=True,
    )
    result = loop.run({"uri": "s3://doc"})

    # A real multi-node capability plan was authored, staffed, frozen, and RUN to completion.
    order = result.state.current_frozen_plan.order
    assert len(order) > 1 and all("__" in n for n in order)     # agent-agnostic capability roles
    assert result.terminated_by == "frontier_exhaust"
    assert set(result.completed) == set(order)
    assert invoke.calls == order                                # real Supervisor invoked each, in topo order


def test_e2e_cold_start_unprovisioned_roles_cannot_dispatch():
    """The freshly-authored roles are UNPROVISIONED (placeholder container_uri, no ARN), so the real
    Supervisor's binding-integrity gate refuses to dispatch them until a deploy supplies ARNs — a
    freshly-created role must be provisioned before it can run (deploy is a separate, gated step)."""
    loop = GovernorLoop(
        "investigate the checkout latency regression", {},
        invoke_fn=_ResultInvoke(), backend="python", decompose=True,  # NO arns => not provisioned
    )
    with pytest.raises(RuntimeError, match="no provisioned runtime ARN"):
        loop.run({"uri": "s3://doc"})
