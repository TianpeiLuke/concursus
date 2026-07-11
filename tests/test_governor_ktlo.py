"""Tests for the standing KTLO daemon (:class:`KTLODaemon`).

The KTLO daemon is a strictly-OUTER layer above the G-2 :class:`GovernorLoop`: a continuous
``monitor -> triage -> escalate -> (replan | close)`` loop that stays up, wakes on event arrival,
does drift detection, and — per triggered investigation — dispatches ONE fresh bounded
:class:`GovernorLoop` episode.

These tests assert the identity invariants that keep the compiler a compiler:

* Each woken investigation is a FRESH, INDEPENDENT, BOUNDED, terminating episode over its own store
  (INV-4): N events => N distinct frozen plans, N terminated GovernorResults.
* The daemon only ENQUEUES episodes; it never lengthens a single ``Supervisor.run`` into an
  unbounded in-episode loop (INV-1).
* The standing daemon SURVIVES between episodes (empty ticks) and drift triggers a fresh episode.

Everything is offline: a fake Supervisor + :class:`InProcessStateStore` per episode, no AWS.
"""

from concursus import (
    AgentManifest,
    GovernorResult,
    InProcessEventQueue,
    KTLODaemon,
    KTLODaemonError,
    KTLOResult,
    ScriptedEventSource,
    TRIAGE_CLOSE,
)
from concursus.governor import KTLODaemon as KTLODaemonFromSubpkg
from concursus.state.statestore import InProcessStateStore

# Exported from both the top-level package and the subpackage.
assert KTLODaemon is KTLODaemonFromSubpkg


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
    """A fake episode supervisor: writes every plan node's output to its store so the frontier
    exhausts on the first episode (each episode thus terminates on ``frontier_exhaust``)."""

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store

    def run(self, inputs):
        outputs = {}
        for node in self._plan.order:
            outputs[node] = {"doc": f"{node}-out"}
        return outputs


def _daemon(source, *, mode="ktlo", drift_detector=None, triage_fn=None, max_ticks=64):
    """Build an offline KTLO daemon over a fake per-episode supervisor + fresh in-process stores."""
    return KTLODaemon(
        _two_node_manifests(),
        source=source,
        mode=mode,
        drift_detector=drift_detector,
        triage_fn=triage_fn,
        store_factory=InProcessStateStore,
        supervisor_factory=lambda **kw: _FakeSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_ticks=max_ticks,
        episode_max_rounds=8,
        episode_no_progress_n=2,
        backend="python",
    )


# == config guards =============================================================
def test_daemon_config_guards():
    """Invalid config raises at construction: bad mode, missing source, non-positive tick cap."""
    import pytest

    with pytest.raises(KTLODaemonError, match="mode"):
        KTLODaemon(_two_node_manifests(), source=InProcessEventQueue([]), mode="bogus")
    with pytest.raises(KTLODaemonError, match="EventSource"):
        KTLODaemon(_two_node_manifests(), source=None)
    with pytest.raises(KTLODaemonError, match="max_ticks"):
        KTLODaemon(_two_node_manifests(), source=InProcessEventQueue([]), max_ticks=0)
    with pytest.raises(KTLODaemonError, match="manifests"):
        KTLODaemon({}, source=InProcessEventQueue([]))


# == the three required tests ==================================================
def test_standing_loop_spawns_bounded_episodes_per_event():
    """Feed 3 synthetic tickets => 3 independent, bounded, terminating episodes + 3 distinct frozen
    plans (INV-1/INV-4)."""
    tickets = [
        {"id": "t1", "goal": "summarize doc t1"},
        {"id": "t2", "goal": "summarize doc t2"},
        {"id": "t3", "goal": "summarize doc t3"},
    ]
    # A closed queue holding all three: one monitor tick drains them, then the source is drained.
    source = InProcessEventQueue(tickets, closed=True)
    daemon = _daemon(source, mode="ktlo")
    result = daemon.run()

    assert isinstance(result, KTLOResult)
    assert result.mode == "ktlo"
    assert result.terminated_by == "source_drained"
    assert result.alive is False

    # Three tickets => three dispatched episodes, each a complete bounded GovernorResult.
    assert result.events_seen == 3
    assert result.events_investigated == 3
    assert result.events_closed == 0
    assert len(result.episodes) == 3
    assert all(isinstance(e, GovernorResult) for e in result.episodes)
    # Each episode TERMINATED on a bound (not the hard step cap) — INV-1: a bounded episode, never
    # an unbounded in-episode loop.
    for e in result.episodes:
        assert e.done is True
        assert e.terminated_by == "frontier_exhaust"
        assert e.terminated_by != "step_cap"
        assert e.supervisor_runs == e.rounds  # one Supervisor.run per round
        assert set(e.completed) == {"ingest", "summarize"}

    # Three DISTINCT frozen plan objects — no plan shared across episodes (INV-4).
    assert len(result.episode_plans) == 3
    ids = [id(p) for p in result.episode_plans]
    assert len(set(ids)) == 3
    # Each fresh episode's first plan is revision 0 (a brand-new frozen plan, not a carried-over
    # mutation).
    assert all(p.revision == 0 for p in result.episode_plans)
    assert result.errors == []


def test_daemon_survives_between_episodes():
    """The standing daemon SURVIVES empty ticks between episodes: an arrival, then a quiet tick,
    then another arrival — two episodes, and the daemon does not stop on the empty tick."""
    # Scripted batches: ticket 1, THEN nothing (an empty tick the daemon must survive), THEN
    # ticket 2. After the last scripted batch the source is drained.
    source = ScriptedEventSource(
        [
            [{"id": "t1", "goal": "summarize doc t1"}],
            [],  # a quiet tick — the daemon wakes, sees nothing, and KEEPS STANDING
            [{"id": "t2", "goal": "summarize doc t2"}],
        ]
    )
    daemon = _daemon(source, mode="ktlo")
    result = daemon.run()

    assert result.terminated_by == "source_drained"
    # It ran at least the three scripted ticks (survived the empty middle one) plus a final drain
    # check — the empty tick did NOT terminate the loop.
    assert result.ticks >= 3
    # Two real tickets => two independent episodes; the quiet tick contributed none.
    assert result.events_seen == 2
    assert result.events_investigated == 2
    assert len(result.episodes) == 2
    assert len(result.episode_plans) == 2
    assert id(result.episode_plans[0]) != id(result.episode_plans[1])
    for e in result.episodes:
        assert e.done is True
        assert e.terminated_by == "frontier_exhaust"


def test_drift_triggers_new_frozen_episode():
    """Scheduled drift detection surfaces a synthetic signal that spawns a FRESH bounded frozen
    episode — even with an empty event queue."""
    # No queued tickets; the queue is closed so only drift can produce work.
    source = InProcessEventQueue([], closed=True)

    # A drift detector that fires exactly ONCE (first poll), then goes quiet — so the standing loop
    # spawns exactly one drift episode and then terminates on drained + quiet.
    fired = {"n": 0}

    def drift_detector():
        if fired["n"] == 0:
            fired["n"] += 1
            return [{"id": "drift-1", "goal": "investigate model drift", "kind": "drift"}]
        return []

    daemon = _daemon(source, mode="ktlo", drift_detector=drift_detector)
    result = daemon.run()

    assert result.terminated_by == "source_drained"
    assert result.drift_triggered == 1
    assert result.events_seen == 1
    assert result.events_investigated == 1
    # The drift signal spawned exactly ONE fresh, bounded, terminating frozen episode (INV-4).
    assert len(result.episodes) == 1
    assert len(result.episode_plans) == 1
    episode = result.episodes[0]
    assert episode.done is True
    assert episode.terminated_by == "frontier_exhaust"
    assert episode.terminated_by != "step_cap"
    # A brand-new frozen plan (revision 0), not a mutation of any prior plan.
    assert result.episode_plans[0].revision == 0
    assert set(episode.completed) == {"ingest", "summarize"}


# == launch vs ktlo: same machinery, one config ===============================
def test_launch_mode_is_one_shot_drain_once():
    """``mode='launch'`` runs a single drain-once tick then stops (``launch_complete``), spawning
    an episode per live signal — the SAME machinery as ktlo, differing only by mode."""
    tickets = [{"id": "t1", "goal": "g1"}, {"id": "t2", "goal": "g2"}]
    # An OPEN (not-yet-closed) queue: a standing ktlo loop would keep waiting, but launch drains
    # once and stops regardless.
    source = InProcessEventQueue(tickets, closed=False)
    daemon = _daemon(source, mode="launch")
    result = daemon.run()

    assert result.mode == "launch"
    assert result.terminated_by == "launch_complete"
    assert result.ticks == 1  # exactly one drain-once monitor tick
    assert result.events_investigated == 2
    assert len(result.episodes) == 2


# == triage: noise is closed, no episode formed ================================
def test_triage_closes_noise_without_forming_episode():
    """A signal triaged as noise (``close``) is dropped — no episode is dispatched for it."""
    tickets = [
        {"id": "t1", "goal": "real work"},
        {"id": "t2", "noise": True, "goal": "spam"},
    ]
    source = InProcessEventQueue(tickets, closed=True)
    daemon = _daemon(source, mode="ktlo")  # default triage: noise=True => close
    result = daemon.run()

    assert result.events_seen == 2
    assert result.events_closed == 1
    assert result.events_investigated == 1
    assert len(result.episodes) == 1


# == escalate: high severity flagged and still investigated ====================
def test_escalate_flags_high_severity_and_dispatches():
    """A high-severity signal is triaged as ``escalate`` — flagged AND dispatched as an episode."""
    tickets = [{"id": "t1", "severity": "sev2", "goal": "urgent"}]
    source = InProcessEventQueue(tickets, closed=True)
    daemon = _daemon(source, mode="ktlo")
    result = daemon.run()

    assert result.escalations == 1
    assert result.events_investigated == 1
    assert len(result.episodes) == 1


# == the daemon survives a raising episode =====================================
def test_daemon_survives_failing_episode():
    """An episode whose supervisor raises is recorded in ``errors``; the daemon SURVIVES and keeps
    processing subsequent signals."""

    # Build a daemon whose supervisor raises only for the "bad" ticket.
    class _SelectiveSupervisor:
        def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
            self._plan = plan

        def run(self, inputs):
            if inputs.get("signal", {}).get("id") == "bad":
                raise RuntimeError("kaboom")
            return {node: {"doc": f"{node}-out"} for node in self._plan.order}

    tickets = [
        {"id": "good1", "goal": "ok1"},
        {"id": "bad", "goal": "will raise"},
        {"id": "good2", "goal": "ok2"},
    ]
    source = InProcessEventQueue(tickets, closed=True)
    daemon = KTLODaemon(
        _two_node_manifests(),
        source=source,
        mode="ktlo",
        store_factory=InProcessStateStore,
        supervisor_factory=lambda **kw: _SelectiveSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        backend="python",
    )
    result = daemon.run()

    # All three were triaged as investigate; the bad one raised but the daemon SURVIVED.
    assert result.events_investigated == 3
    assert len(result.episodes) == 2  # two good episodes completed
    assert len(result.errors) == 1
    assert "kaboom" in result.errors[0]
    assert result.terminated_by == "source_drained"


# == tick cap: a never-draining source can never run away ======================
def test_tick_cap_bounds_a_never_draining_source():
    """A source that never declares itself drained (open, always empty) terminates on the hard
    ``max_ticks`` cap — the standing loop can NEVER run away."""
    # Open queue, empty forever: drained() is always False, so only the tick cap can stop it.
    source = InProcessEventQueue([], closed=False)
    daemon = _daemon(source, mode="ktlo", max_ticks=5)
    result = daemon.run()

    assert result.terminated_by == "tick_cap"
    assert result.ticks == 5
    assert result.events_seen == 0
    assert result.episodes == []


# == (J-1) OPT-IN governance forwarded per-episode into each fresh loop =========
def _sched_manifest(name, *, side_effecting=False, trust_seed=None):
    """A manifest for the scheduler's process table (mirrors the loop-test idiom)."""
    from concursus import AgentManifest as _AM

    data = {
        "name": name,
        "registry": {"container_uri": "img", "protocol": "HTTP"},
        "contract": {"inputs": {}, "outputs": {"doc": {"type": "string", "required": True}}},
        "side_effecting": side_effecting,
    }
    if trust_seed is not None:
        data["trust_seed"] = trust_seed
    return _AM.from_dict(data)


class _HeldTrackingSupervisor:
    """A store-bound fake episode supervisor that HONORS the ROUTER's ``held`` set: it invokes (and
    writes) every still-open, NON-held plan node and NEVER touches a held (escalated) node — so a
    below-bar node stays uncompleted and is surfaced on ``GovernorResult.escalated``."""

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id, held=None):
        self._plan = plan
        self._store = store
        self._held = set(held or ())

    def run(self, inputs):
        already = set(self._store.completed())
        for node in self._plan.order:
            if node in already or node in self._held:
                continue  # skip completed AND held (escalated) nodes — no invoke
            self._store.put(node, {"doc": f"{node}-out"})
        return {
            n: self._store.get(n)
            for n in self._plan.order
            if n in self._store.completed()
        }


def _trust_scheduler(tmp_path):
    """A TrustLadderScheduler over a populated registry: ``summarize`` is side-effecting at a
    below-bar trust seed (=> ESCALATE, held), ``ingest`` is non-side-effecting (=> DISPATCH)."""
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


def _governed_daemon(source, scheduler, *, mode="ktlo", max_ticks=64):
    """A governed variant of :func:`_daemon`: wires an OPT-IN Trust-Ladder scheduler so each spawned
    episode is GOVERNED (holds a below-bar node), plus a held-honoring per-episode supervisor."""
    return KTLODaemon(
        _two_node_manifests(),
        source=source,
        mode=mode,
        store_factory=InProcessStateStore,
        supervisor_factory=lambda **kw: _HeldTrackingSupervisor(**kw),
        plan_model_fn=_plan_model_fn,
        max_ticks=max_ticks,
        episode_max_rounds=8,
        episode_no_progress_n=2,
        backend="python",
        scheduler=scheduler,
    )


def test_ktlo_episodes_are_trust_governed_when_scheduler_passed(tmp_path):
    """A daemon built WITH an OPT-IN Trust-Ladder scheduler spawns GOVERNED episodes: the below-bar
    ``summarize`` node is HELD (escalated) by the per-episode loop's ROUTER — surfaced on the
    spawned episode's ``GovernorResult.escalated`` — while the daemon still only ENQUEUES a fresh,
    bounded GovernorLoop over a fresh store (INV-1/INV-4)."""
    tickets = [
        {"id": "t1", "goal": "summarize doc t1"},
        {"id": "t2", "goal": "summarize doc t2"},
    ]
    source = InProcessEventQueue(tickets, closed=True)
    daemon = _governed_daemon(source, _trust_scheduler(tmp_path), mode="ktlo")
    result = daemon.run()

    assert result.terminated_by == "source_drained"
    assert result.events_investigated == 2
    assert len(result.episodes) == 2
    # Every spawned episode was GOVERNED: the below-bar node was escalated (held), never completed.
    for e in result.episodes:
        assert e.escalated == ["summarize"]
        assert "summarize" not in set(e.completed)
        assert "ingest" in set(e.completed)
    # Still a fresh bounded episode per signal (INV-4): distinct frozen plans, each revision 0.
    assert len(result.episode_plans) == 2
    assert id(result.episode_plans[0]) != id(result.episode_plans[1])
    assert all(p.revision == 0 for p in result.episode_plans)


def test_ktlo_default_is_ungoverned_unchanged():
    """Default (no scheduler, ``deliberate`` false) => every episode is byte-for-byte today's
    ungoverned behavior: nothing escalated, both nodes complete, and the daemon defaults are the
    Phase-5 opt-in switches OFF."""
    tickets = [{"id": "t1", "goal": "summarize doc t1"}]
    source = InProcessEventQueue(tickets, closed=True)
    daemon = _daemon(source, mode="ktlo")

    # Opt-in switches default OFF on the daemon.
    assert daemon._scheduler is None
    assert daemon._deliberate is False

    result = daemon.run()
    assert result.terminated_by == "source_drained"
    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.escalated == []
    assert episode.terminated_by == "frontier_exhaust"
    assert set(episode.completed) == {"ingest", "summarize"}
