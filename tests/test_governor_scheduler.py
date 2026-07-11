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
