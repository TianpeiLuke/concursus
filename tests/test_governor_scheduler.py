"""Tests for the per-decision Trust-Ladder scheduler (S10-G6).

The scheduler is the governor ROUTER's agent matcher: at dispatch it re-reads the READ-ONLY
:class:`AgentRegistry` process table (G-7), reads each standing agent's authoritative EARNED
trust off a GOV-side ladder, and either clears a ready step to dispatch or escalates a below-bar
decision (L1->L3).  It PROPOSES a frontier (a VALUE that is INPUT to the next recompile) and NEVER
mutates a frozen plan; ``update_trust`` lives GOV-side ONLY and the create-time
``evaluate_deploy_gate`` is NEVER called per-invocation.
"""

from __future__ import annotations

import json
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
    Tier,
    TrustLadderScheduler,
    make_payload_tier,
    make_trust_strictness,
    project_context,
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


# -- Phase 3a: auto-Create wires UNMATCHED -> spawn (opt-in) -------

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


# -- Phase 2: the BINDER (candidate set × trust-priority × availability) --

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


# -- B4: the adaptive-strictness dial (make_trust_strictness) --------------------
def test_make_trust_strictness_weak_strict_strong_lean(tmp_path):
    """A below-bar (WEAK) agent => strict; an at/above-bar (STRONG) agent => lean; unknown => strict."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="weak", fingerprint="f1", arn="arn:weak", deployed_at="2026-07-01")
    ledger.record(name="strong", fingerprint="f2", arn="arn:strong", deployed_at="2026-07-01")
    m_weak = _manifest("weak", trust_seed=TrustGrade.L1_CANARY)      # below L2 bar
    m_strong = _manifest("strong", trust_seed=TrustGrade.L3_AUTONOMOUS)  # above L2 bar
    reg = _registry_with(ledger, m_weak, m_strong)
    sched = TrustLadderScheduler(reg, manifests={"weak": m_weak, "strong": m_strong})

    is_strict = make_trust_strictness(sched, strict_below=TrustGrade.L2_GUARDED)
    assert is_strict("weak") is True        # L1 < L2 => strict contract
    assert is_strict("strong") is False     # L3 >= L2 => lean path
    assert is_strict("never_seen") is True  # unknown/unproven => conservative strict


def test_make_trust_strictness_threshold_is_configurable(tmp_path):
    """The strict_below bar is tunable: raising it pulls more agents into the strict set."""
    ledger = DeployLedger(tmp_path / "l.json")
    ledger.record(name="mid", fingerprint="f1", arn="arn:mid", deployed_at="2026-07-01")
    m_mid = _manifest("mid", trust_seed=TrustGrade.L2_GUARDED)
    reg = _registry_with(ledger, m_mid)
    sched = TrustLadderScheduler(reg, manifests={"mid": m_mid})

    # At bar L2: L2 is NOT below L2 => lean.
    assert make_trust_strictness(sched, strict_below=TrustGrade.L2_GUARDED)("mid") is False
    # Raise the bar to L3: now L2 IS below => strict.
    assert make_trust_strictness(sched, strict_below=TrustGrade.L3_AUTONOMOUS)("mid") is True


def test_trust_dial_end_to_end_with_assembler(tmp_path):
    """CAPSTONE: the same type-mismatched plan is REJECTED when its consumer node is a WEAK agent
    (dial => strict) but PASSES when it is a STRONG agent (dial => lean) — strictness ∝ 1/trust,
    read off the Trust Ladder and wired straight into the compiler's deep gate."""
    from concursus import AgentDAG, AgentManifest, OrchestrationAssembler
    from concursus.core.resolve import AlignmentError

    def _plan_manifest(name, inputs, outputs, depends_on=None):
        data = {"name": name,
                "registry": {"container_uri": "img", "protocol": "HTTP", "entry": f"a.{name}:run"},
                "contract": {"inputs": inputs, "outputs": outputs}}
        if depends_on is not None:
            data["spec"] = {"depends_on": depends_on}
        return AgentManifest.from_dict(data)

    # A plan whose 'summarize' consumes an integer into a string input (a deep-gate mismatch).
    dag = AgentDAG()
    dag.add_node("ingest").add_node("summarize").add_edge("ingest", "summarize")
    plan_manifests = {
        "ingest": _plan_manifest("ingest", {"uri": {"type": "string"}},
                                 {"document": {"type": "integer"}}),
        "summarize": _plan_manifest("summarize", {"document": {"type": "string"}},
                                    {"summary": {"type": "string"}},
                                    depends_on=[{"from": "ingest.document", "to": "document"}]),
    }

    def _dial_for(summarize_seed):
        ledger = DeployLedger(tmp_path / f"l_{summarize_seed.name}.json")
        ledger.record(name="summarize", fingerprint="fp", arn="arn:s", deployed_at="2026-07-01")
        m = _manifest("summarize", trust_seed=summarize_seed)
        reg = _registry_with(ledger, m)
        sched = TrustLadderScheduler(reg, manifests={"summarize": m})
        return make_trust_strictness(sched, strict_below=TrustGrade.L2_GUARDED)

    # WEAK summarize (L1) => strict => the mismatch is caught.
    weak_dial = _dial_for(TrustGrade.L1_CANARY)
    with __import__("pytest").raises(AlignmentError, match="type-INCOMPATIBLE"):
        OrchestrationAssembler(strict_types=True, strict_fn=weak_dial).assemble(dag, plan_manifests)

    # STRONG summarize (L3) => lean => the same plan assembles.
    strong_dial = _dial_for(TrustGrade.L3_AUTONOMOUS)
    plan = OrchestrationAssembler(strict_types=True, strict_fn=strong_dial).assemble(dag, plan_manifests)
    assert plan.order == ["ingest", "summarize"]


# -- SPIKE B: the trust-tiered payload dial (make_payload_tier / project_context) --
def _tier_sched(tmp_path, **name_to_seed):
    ledger = DeployLedger(tmp_path / "lt.json")
    manifests = {}
    for name, seed in name_to_seed.items():
        ledger.record(name=name, fingerprint=f"f_{name}", arn=f"arn:{name}", deployed_at="2026-07-01")
        manifests[name] = _manifest(name, trust_seed=seed)
    reg = _registry_with(ledger, *manifests.values())
    return TrustLadderScheduler(reg, manifests=manifests)


def test_make_payload_tier_maps_grade_to_tier(tmp_path):
    """L3 -> HIGH (lean), L2 -> GUARDED, L0/L1 -> LOW (full), unknown -> LOW (conservative)."""
    sched = _tier_sched(
        tmp_path,
        strong=TrustGrade.L3_AUTONOMOUS,
        mid=TrustGrade.L2_GUARDED,
        weak=TrustGrade.L1_CANARY,
        shadow=TrustGrade.L0_SHADOW,
    )
    tier = make_payload_tier(sched, strict_below=TrustGrade.L2_GUARDED)
    assert tier("strong") is Tier.HIGH
    assert tier("mid") is Tier.GUARDED
    assert tier("weak") is Tier.LOW
    assert tier("shadow") is Tier.LOW
    assert tier("never_seen") is Tier.LOW  # unknown/unproven => conservative LOW


def test_make_payload_tier_programmatic_is_orthogonal_to_trust(tmp_path):
    """A script-like agent is PROGRAMMATIC regardless of its earned grade (matched, not coached)."""
    sched = _tier_sched(tmp_path, prog=TrustGrade.L3_AUTONOMOUS)  # even a high grade
    tier = make_payload_tier(sched, is_programmatic=lambda n: n == "prog")
    assert tier("prog") is Tier.PROGRAMMATIC


def test_project_context_is_a_monotone_lattice():
    """dim-2/3 context shrinks as trust rises: LOW keeps all, GUARDED only guardrails, HIGH none,
    PROGRAMMATIC only tool_calls. A higher tier's kept-set is a subset of a lower tier's."""
    ctx = {
        "sop": ["1. read", "2. extract"],
        "tools_available": ["doc_reader"],
        "guardrails": ["do not fabricate ids"],
        "examples": ["ex1"],
        "tool_calls": [{"tool": "doc_reader", "args": {}}],
    }
    assert project_context(ctx, Tier.LOW) == ctx                       # full coaching
    assert project_context(ctx, Tier.GUARDED) == {"guardrails": ctx["guardrails"]}
    assert project_context(ctx, Tier.HIGH) == {}                        # lean
    assert project_context(ctx, Tier.PROGRAMMATIC) == {"tool_calls": ctx["tool_calls"]}
    # monotone: HIGH ⊆ GUARDED ⊆ LOW (by kept keys)
    assert set(project_context(ctx, Tier.HIGH)) <= set(project_context(ctx, Tier.GUARDED))
    assert set(project_context(ctx, Tier.GUARDED)) <= set(project_context(ctx, Tier.LOW))
    # empty/absent context => {}
    assert project_context({}, Tier.LOW) == {}


def test_supervisor_payload_overlay_tiers_by_trust(tmp_path):
    """SPIKE B end-to-end: the SAME role gets a FULL context payload for a WEAK agent and a LEAN
    one for a STRONG agent — the invoke payload differs only by the tiered coaching context."""
    from concursus import AgentDAG, AgentManifest, OrchestrationAssembler
    from concursus.execute.supervisor import Supervisor
    from concursus.state.statestore import InProcessStateStore

    context = {"sop": ["read", "summarize"], "guardrails": ["cite sources"], "examples": ["ex"]}
    m = AgentManifest.from_dict({
        "name": "summarize",
        "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.summarize:run"},
        "contract": {
            "inputs": {"doc": {"type": "string"}},
            "outputs": {"summary": {"type": "string", "required": True}},
            "context": context,
        },
    })
    dag = AgentDAG()
    dag.add_node("summarize")
    manifests = {"summarize": m}
    plan = OrchestrationAssembler().assemble(dag, manifests)
    arns = {"summarize": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/x"}

    seen = {}

    def _spy(arn, qualifier, session_id, payload_bytes):
        seen.clear()
        seen.update(json.loads(payload_bytes))
        return {"summary": "ok"}

    def _run(tier_fn):
        sup = Supervisor(
            plan, manifests, invoke_fn=_spy, arns=arns,
            state_store=InProcessStateStore(), payload_tier_fn=tier_fn,
        )
        sup.run({"summarize": {"doc": "d"}})
        return dict(seen)

    weak_payload = _run(lambda node: Tier.LOW)
    strong_payload = _run(lambda node: Tier.HIGH)
    # dimension 1 (the declared input) is invariant across tiers.
    assert weak_payload["doc"] == "d" and strong_payload["doc"] == "d"
    # dimension 2 (coaching context) is present for the WEAK tier, absent for the STRONG one.
    assert weak_payload.get("sop") == context["sop"]
    assert weak_payload.get("guardrails") == context["guardrails"]
    assert "sop" not in strong_payload and "guardrails" not in strong_payload
    # default (no tier_fn) is byte-for-byte unchanged: no context overlaid.
    sup0 = Supervisor(plan, manifests, invoke_fn=_spy, arns=arns, state_store=InProcessStateStore())
    sup0.run({"summarize": {"doc": "d"}})
    assert "sop" not in seen and "guardrails" not in seen
