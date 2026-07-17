"""Tests for FZ 35e4a3a1b Phase 2 — the payload full contract (F1-F5).

F1 ProvisioningPlan.payload_contract + assemble authoring (emit-when-non-empty);
F2 check_alignment opt-in full-input-cover; F3 Supervisor reads the frozen contract;
F4 registry.programmatic -> PROGRAMMATIC tier; F5 re-tiering at recompile pins executed nodes.
All opt-in — the default (no payload_tier_fn / full_input_cover off) is byte-for-byte unchanged.
"""

from __future__ import annotations

import json

from concursus import AgentDAG, AgentManifest, DeployLedger, TrustGrade
from concursus.assemble.assemble import OrchestrationAssembler
from concursus.core.resolve import AlignmentError, check_alignment
from concursus.execute.supervisor import Supervisor
from concursus.governor.registry import AgentRegistry
from concursus.governor.scheduler import (
    Tier,
    TrustLadderScheduler,
    make_payload_tier,
    manifest_is_programmatic,
)
from concursus.state.statestore import InProcessStateStore


def _agent(name, inputs, outputs, *, context=None, depends_on=None, programmatic=False,
           trust_seed=None):
    reg = {"container_uri": "img", "protocol": "HTTP", "entry": f"a.{name}:run"}
    if programmatic:
        reg["programmatic"] = True
    contract = {"inputs": inputs, "outputs": outputs}
    if context is not None:
        contract["context"] = context
    data = {"name": name, "registry": reg, "contract": contract}
    if depends_on is not None:
        data["spec"] = {"depends_on": depends_on}
    if trust_seed is not None:
        data["trust_seed"] = trust_seed
    return AgentManifest.from_dict(data)


def _tier_sched(tmp_path, **name_to_seed):
    ledger = DeployLedger(tmp_path / "lt.json")
    manifests = {}
    for name, seed in name_to_seed.items():
        ledger.record(name=name, fingerprint=f"f_{name}", arn=f"arn:{name}", deployed_at="2026-07-01")
        manifests[name] = _agent(name, {}, {"doc": {"type": "string", "required": True}}, trust_seed=seed)
    reg = AgentRegistry(ledger)
    for m in manifests.values():
        reg.register_agent(m)
    return TrustLadderScheduler(reg, manifests=manifests)


# -- F1: payload_contract authored + emit-when-non-empty -------------------
def test_no_tier_fn_leaves_plan_byte_for_byte_unchanged():
    dag = AgentDAG()
    dag.add_node("summarize")
    m = _agent("summarize", {"doc": {"type": "string"}},
               {"summary": {"type": "string", "required": True}},
               context={"sop": ["read"], "guardrails": ["cite"]})
    plan = OrchestrationAssembler().assemble(dag, {"summarize": m})
    assert plan.payload_contract == {}
    assert "payload_contract" not in plan.to_dict()  # emit-when-non-empty


def test_assemble_authors_tiered_payload_contract(tmp_path):
    dag = AgentDAG()
    dag.add_node("summarize")
    m = _agent("summarize", {"doc": {"type": "string"}},
               {"summary": {"type": "string", "required": True}},
               context={"sop": ["read"], "guardrails": ["cite"]}, trust_seed=TrustGrade.L1_CANARY)
    sched = _tier_sched(tmp_path, summarize=TrustGrade.L1_CANARY)
    tier_fn = make_payload_tier(sched)
    plan = OrchestrationAssembler(payload_tier_fn=tier_fn).assemble(dag, {"summarize": m})
    pc = plan.payload_contract["summarize"]
    assert pc["trust_tier"] == "LOW"  # L1 => LOW => full context
    assert pc["static_context"] == {"sop": ["read"], "guardrails": ["cite"]}
    assert "payload_contract" in plan.to_dict()  # now emitted


# -- F2: full-input-cover gate ---------------------------------------------
def test_full_input_cover_flags_an_unsupplied_input():
    dag = AgentDAG()
    dag.add_node("solo")
    # declares an input with NO depends_on edge and NO context key of that name.
    m = _agent("solo", {"needle": {"type": "string"}},
               {"out": {"type": "string", "required": True}})
    # default: passes (name-level gate only).
    check_alignment(dag, {"solo": m})
    # opt-in cover: raises.
    try:
        check_alignment(dag, {"solo": m}, full_input_cover=True)
        assert False, "expected AlignmentError"
    except AlignmentError as e:
        assert "full-input-cover" in str(e)


def test_full_input_cover_passes_when_context_supplies_the_input():
    dag = AgentDAG()
    dag.add_node("solo")
    m = _agent("solo", {"needle": {"type": "string"}},
               {"out": {"type": "string", "required": True}},
               context={"needle": "a static value"})
    check_alignment(dag, {"solo": m}, full_input_cover=True)  # covered by context => ok


# -- F3: Supervisor reads the frozen payload_contract ----------------------
def test_supervisor_reads_frozen_payload_contract(tmp_path):
    dag = AgentDAG()
    dag.add_node("summarize")
    m = _agent("summarize", {"doc": {"type": "string"}},
               {"summary": {"type": "string", "required": True}},
               context={"sop": ["read"], "guardrails": ["cite"]})
    sched = _tier_sched(tmp_path, summarize=TrustGrade.L1_CANARY)
    plan = OrchestrationAssembler(payload_tier_fn=make_payload_tier(sched)).assemble(dag, {"summarize": m})

    seen = {}

    def _spy(arn, qualifier, session_id, payload_bytes):
        seen.update(json.loads(payload_bytes))
        return {"summary": "ok"}

    # NO payload_tier_fn on the Supervisor — it reads the FROZEN contract from the plan (F3).
    sup = Supervisor(plan, {"summarize": m}, invoke_fn=_spy,
                     arns={"summarize": "arn:aws:x:1:runtime/y"}, state_store=InProcessStateStore())
    sup.run({"summarize": {"doc": "d"}})
    assert seen["doc"] == "d"                       # dimension 1 invariant
    assert seen.get("sop") == ["read"]              # frozen tiered context overlaid
    assert seen.get("guardrails") == ["cite"]


# -- F4: registry.programmatic -> PROGRAMMATIC tier ------------------------
def test_programmatic_flag_forces_programmatic_tier(tmp_path):
    m_prog = _agent("tool", {}, {"doc": {"type": "string", "required": True}},
                    programmatic=True, trust_seed=TrustGrade.L3_AUTONOMOUS)
    sched = _tier_sched(tmp_path, tool=TrustGrade.L3_AUTONOMOUS)
    tier = make_payload_tier(sched, manifest_is_programmatic({"tool": m_prog}))
    assert tier("tool") is Tier.PROGRAMMATIC  # even at L3, programmatic wins


# -- F5: re-tiering at recompile pins an executed node ---------------------
def test_recompile_pins_executed_node_contract(tmp_path):
    dag = AgentDAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_edge("a", "b")
    ma = _agent("a", {}, {"document": {"type": "string", "required": True}},
                context={"sop": ["a-sop"]})
    mb = _agent("b", {"document": {"type": "string"}},
                {"summary": {"type": "string", "required": True}},
                context={"sop": ["b-sop"]},
                depends_on=[{"from": "a.document", "to": "document"}])
    manifests = {"a": ma, "b": mb}

    # round 1: 'a' is WEAK (L1 => LOW, full sop).
    sched = _tier_sched(tmp_path, a=TrustGrade.L1_CANARY, b=TrustGrade.L1_CANARY)
    asm = OrchestrationAssembler(payload_tier_fn=make_payload_tier(sched))
    plan1 = asm.assemble(dag, manifests)
    assert plan1.payload_contract["a"]["trust_tier"] == "LOW"

    # 'a' is promoted to L3 (HIGH) before recompile — but it already EXECUTED, so its contract
    # must stay pinned to what it ran with (LOW), while 'b' (not executed) re-tiers freshly.
    sched._earned["a"] = TrustGrade.L3_AUTONOMOUS
    plan2 = asm.recompile(plan1, completed={"a"}, dag=dag, manifests=manifests)
    assert plan2.payload_contract["a"]["trust_tier"] == "LOW"   # executed => pinned
    assert plan2.revision == 1
