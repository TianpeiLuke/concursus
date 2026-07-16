"""Tests for the per-decision Trust-Ladder scheduler (S10-G6).

The scheduler is the governor ROUTER's agent matcher: at dispatch it re-reads the READ-ONLY
:class:`AgentRegistry` process table (G-7), reads each standing agent's authoritative EARNED
trust off a GOV-side ladder, and either clears a ready step to dispatch or escalates a below-bar
decision (L1->L3).  It PROPOSES a frontier (a VALUE that is INPUT to the next recompile) and NEVER
mutates a frozen plan; ``update_trust`` lives GOV-side ONLY and the create-time
``evaluate_deploy_gate`` is NEVER called per-invocation.
"""

from __future__ import annotations

import types

import concursus.build.trust as trust_mod
from concursus import AgentManifest, DeployLedger, TrustGrade
from concursus.governor.registry import AgentRegistry
from concursus.governor.scheduler import (
    DISPATCH,
    ESCALATE,
    UNMATCHED,
    Binding,
    FrontierProposal,
    ScheduleDecision,
    TrustLadderScheduler,
)


def _manifest(name, *, capabilities=None, side_effecting=False, trust_seed=None):
    reg = {"container_uri": "img", "protocol": "HTTP"}
    if capabilities is not None:
        reg["capabilities"] = list(capabilities)
    data = {
        "name": name,
        "registry": reg,
        "contract": {"inputs": {}, "outputs": {"doc": {"type": "string", "required": True}}},
        "side_effecting": side_effecting,
    }
    if trust_seed is not None:
        data["trust_seed"] = trust_seed
    return AgentManifest.from_dict(data)


def _registry_with(ledger, *manifests):
    reg = AgentRegistry(ledger)
    for m in manifests:
        reg.register_agent(m)
    return reg


def _plan(order):
    """A duck-typed frozen plan value: just an order attribute (the scheduler only reads order)."""
    return types.SimpleNamespace(order=list(order), revision=0)


def test_below_bar_escalates_not_dispatches(tmp_path):
    """A ready step whose matched agent's earned trust is below the required bar is ESCALATED
    (L1->L3), NOT dispatched — it must not appear in compile_next."""
    ledger = DeployLedger(tmp_path / "l.json")
    # A side-effecting agent standing at only L1_CANARY earned trust.
    ledger.record(name="refund", fingerprint="fp1", arn="arn:refund", deployed_at="2026-07-01")
    m = _manifest("refund", capabilities={"issue-refund"}, side_effecting=True,
                  trust_seed=TrustGrade.L1_CANARY)
    registry = _registry_with(ledger, m)
    sched = TrustLadderScheduler(
        registry,
        manifests={"refund": m},
        min_autonomy=TrustGrade.L2_GUARDED,          # bar above the earned L1
        escalation_grade=TrustGrade.L3_AUTONOMOUS,
    )

    decision = sched.decide("issue-refund")
    assert decision.action == ESCALATE
    assert decision.grade == TrustGrade.L1_CANARY
    assert decision.bar == TrustGrade.L2_GUARDED
    assert decision.escalated_to == TrustGrade.L3_AUTONOMOUS

    proposal = sched.propose_frontier(_plan(["issue-refund"]), completed=[])
    assert isinstance(proposal, FrontierProposal)
    assert "issue-refund" not in proposal.compile_next   # NOT dispatched
    assert "issue-refund" in proposal.escalated


def test_cleared_agent_dispatches(tmp_path):
    """An agent whose earned trust meets the bar is cleared to dispatch (in compile_next)."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="triage", fingerprint="fp1", arn="arn:triage", deployed_at="2026-07-01")
    m = _manifest("triage", capabilities={"classify"}, side_effecting=True,
                  trust_seed=TrustGrade.L3_AUTONOMOUS)
    registry = _registry_with(ledger, m)
    sched = TrustLadderScheduler(registry, manifests={"triage": m},
                                 min_autonomy=TrustGrade.L2_GUARDED)

    decision = sched.decide("classify")
    assert decision.action == DISPATCH
    proposal = sched.propose_frontier(_plan(["classify"]), completed=[])
    assert "classify" in proposal.compile_next
    assert "classify" not in proposal.escalated


def test_update_trust_is_gov_side_only(tmp_path, monkeypatch):
    """``evaluate_deploy_gate`` is the CREATE-TIME seed only — it must NOT be called per-invocation
    (per decide) nor by update_trust. update_trust re-earns trust GOV-side."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="triage", fingerprint="fp1", arn="arn:triage", deployed_at="2026-07-01")
    m = _manifest("triage", capabilities={"classify"}, side_effecting=True,
                  trust_seed=TrustGrade.L1_CANARY)
    registry = _registry_with(ledger, m)
    sched = TrustLadderScheduler(registry, manifests={"triage": m},
                                 min_autonomy=TrustGrade.L1_CANARY)

    # Count calls to the CREATE-TIME gate.
    calls = {"n": 0}
    real_gate = trust_mod.evaluate_deploy_gate

    def _counting_gate(**kwargs):
        calls["n"] += 1
        return real_gate(**kwargs)

    monkeypatch.setattr("concursus.governor.scheduler.evaluate_deploy_gate", _counting_gate)

    # Prime the earned grade (may consult the seed ONCE), then reset the counter.
    sched.seed_grade("triage")
    calls["n"] = 0

    # Many per-invocation decisions must NOT re-consult the create-time gate.
    for _ in range(5):
        sched.decide("classify")
    assert calls["n"] == 0, "evaluate_deploy_gate must NOT be called per-invocation"

    # update_trust re-earns GOV-side and must not consult the create-time gate either.
    before = sched.earned_grade("triage")
    after = sched.update_trust("triage", {"ok": True})
    assert calls["n"] == 0, "update_trust must be GOV-side, not the create-time gate"
    assert after >= before  # a clean outcome does not lower trust


def test_binding_is_input_to_recompile_not_mutation(tmp_path):
    """propose_frontier returns a VALUE (input to the next recompile); it never mutates the plan."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="triage", fingerprint="fp1", arn="arn:triage", deployed_at="2026-07-01")
    m = _manifest("triage", capabilities={"classify"}, side_effecting=False)
    registry = _registry_with(ledger, m)
    sched = TrustLadderScheduler(registry, manifests={"triage": m})

    plan = _plan(["classify", "unknown-task"])
    before_order = list(plan.order)
    proposal = sched.propose_frontier(plan, completed=["classify"])

    # The plan value is byte-identical: propose_frontier read it, never mutated it.
    assert plan.order == before_order
    # A completed node is not re-proposed; an unmatched frontier node is surfaced, not dispatched.
    assert "classify" not in proposal.compile_next
    assert "unknown-task" in proposal.unmatched
    assert "unknown-task" not in proposal.compile_next

    # The proposal is a plain-dict-able VALUE ready to hand to the next recompile.
    d = proposal.to_dict()
    assert set(d) >= {"compile_next", "escalated", "unmatched"}


# -- FZ 35e2b3 Phase 3a: auto-Create wires UNMATCHED -> spawn (opt-in) -------

def test_auto_create_off_by_default_leaves_unmatched_held(tmp_path):
    """Back-compat: without auto_create, an UNMATCHED role stays held (no spawn attempted)."""
    from concursus.governor.loop import GovernorLoop

    ledger = DeployLedger(tmp_path / "l.json")
    sched = TrustLadderScheduler(_registry_with(ledger), manifests={})
    loop = GovernorLoop(goal="g", manifests={}, scheduler=sched)  # auto_create defaults False
    ctx = {"plan": _plan(["needs_agent"]), "trace": []}
    out = loop._router(ctx)
    assert "needs_agent" in out["held"]
    assert not out.get("created")


def test_auto_create_spawns_for_unmatched_role(tmp_path):
    """P3a: with auto_create + a fake create_fn, an UNMATCHED role triggers an on-demand spawn."""
    from concursus.governor.loop import GovernorLoop

    ledger = DeployLedger(tmp_path / "l.json")
    sched = TrustLadderScheduler(_registry_with(ledger), manifests={})
    spawned = []

    def fake_create(task):
        spawned.append(task)          # a FAKE provisioner — no boto3, no CreateAgentRuntime
        return True                    # report the agent is now standing

    loop = GovernorLoop(goal="g", manifests={}, scheduler=sched,
                        auto_create=True, create_fn=fake_create)
    ctx = {"plan": _plan(["needs_agent"]), "trace": []}
    out = loop._router(ctx)
    assert spawned == ["needs_agent"]           # the spawn seam fired for the unmatched role
    assert out["created"] == ["needs_agent"]    # surfaced for the cockpit


def test_auto_create_failed_spawn_leaves_node_held(tmp_path):
    """A spawn that fails/does-not-confirm leaves the node held (safe degradation)."""
    from concursus.governor.loop import GovernorLoop

    ledger = DeployLedger(tmp_path / "l.json")
    sched = TrustLadderScheduler(_registry_with(ledger), manifests={})
    loop = GovernorLoop(goal="g", manifests={}, scheduler=sched,
                        auto_create=True, create_fn=lambda task: False)  # spawn "fails"
    ctx = {"plan": _plan(["needs_agent"]), "trace": []}
    out = loop._router(ctx)
    assert not out.get("created")
    assert "needs_agent" in out["held"]         # still held; loop degrades safely


# -- FZ 35e2b3 Phase 2: the BINDER (candidate set × trust-priority × availability) --

def test_decide_ranked_picks_best_trust_from_candidate_set(tmp_path):
    """P2.1/P2.2: among two capable agents, bind the higher-earned-trust one (not first-match)."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="low", fingerprint="f1", arn="arn:low", deployed_at="2026-07-01")
    ledger.record(name="high", fingerprint="f2", arn="arn:high", deployed_at="2026-07-01")
    m_low = _manifest("low", capabilities={"triage"}, trust_seed=TrustGrade.L1_CANARY)
    m_high = _manifest("high", capabilities={"triage"}, trust_seed=TrustGrade.L3_AUTONOMOUS)
    reg = _registry_with(ledger, m_low, m_high)
    sched = TrustLadderScheduler(reg, manifests={"low": m_low, "high": m_high})

    b = sched.decide_ranked("triage")
    assert isinstance(b, Binding)
    assert b.action == DISPATCH
    assert b.agent == "high"                      # best trust wins, not first-registered
    assert set(b.candidates) == {"low", "high"}   # full candidate set considered (P2.1)


def test_decide_ranked_unmatched_when_no_agent(tmp_path):
    """UNMATCHED when no standing agent serves the task (the Create arrow's trigger)."""
    ledger = DeployLedger(tmp_path / "l.json")
    reg = _registry_with(ledger)
    sched = TrustLadderScheduler(reg, manifests={})
    assert sched.decide_ranked("no_such_task").action == UNMATCHED


def test_decide_ranked_availability_breaks_trust_ties(tmp_path):
    """P2.3: among equal-trust candidates, prefer the least-loaded via load_fn."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="busy", fingerprint="f1", arn="arn:busy", deployed_at="2026-07-01")
    ledger.record(name="free", fingerprint="f2", arn="arn:free", deployed_at="2026-07-01")
    m_busy = _manifest("busy", capabilities={"triage"}, trust_seed=TrustGrade.L2_GUARDED)
    m_free = _manifest("free", capabilities={"triage"}, trust_seed=TrustGrade.L2_GUARDED)
    reg = _registry_with(ledger, m_busy, m_free)
    load = {"busy": 5, "free": 0}
    sched = TrustLadderScheduler(reg, manifests={"busy": m_busy, "free": m_free},
                                 load_fn=lambda n: load.get(n, 0))
    b = sched.decide_ranked("triage")
    assert b.agent == "free" and b.load == 0


def test_propose_bindings_covers_the_frontier(tmp_path):
    """P2.4: propose_bindings returns {node: Binding} over the ready frontier, skipping completed."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="t", fingerprint="f1", arn="arn:t", deployed_at="2026-07-01")
    m = _manifest("t", capabilities={"a", "b"}, trust_seed=TrustGrade.L2_GUARDED)
    reg = _registry_with(ledger, m)
    sched = TrustLadderScheduler(reg, manifests={"t": m})
    plan = _plan(["a", "b", "c"])
    bindings = sched.propose_bindings(plan, completed={"a"})
    assert set(bindings) == {"b", "c"}            # 'a' completed => skipped
    assert bindings["b"].action == DISPATCH
    assert bindings["c"].action == UNMATCHED      # no agent serves 'c'
