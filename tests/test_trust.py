"""Tests for the create-time deploy gate (concursus.trust) — pure, no AWS."""

import pytest

from concursus.trust import (
    DEFAULT_QUALIFIER,
    HOLD,
    LIVE,
    SHADOW,
    SHADOW_QUALIFIER,
    GateDecision,
    TrustGrade,
    evaluate_deploy_gate,
)


# -- TrustGrade ordering + parsing ------------------------------------------
def test_trust_grade_is_ordered():
    assert TrustGrade.L0_SHADOW < TrustGrade.L1_CANARY < TrustGrade.L2_GUARDED
    assert TrustGrade.L2_GUARDED < TrustGrade.L3_AUTONOMOUS
    assert int(TrustGrade.L0_SHADOW) == 0 and int(TrustGrade.L3_AUTONOMOUS) == 3


def test_trust_grade_parse_forms():
    assert TrustGrade.parse(TrustGrade.L2_GUARDED) is TrustGrade.L2_GUARDED
    assert TrustGrade.parse(2) is TrustGrade.L2_GUARDED
    assert TrustGrade.parse("L2_GUARDED") is TrustGrade.L2_GUARDED
    assert TrustGrade.parse("l2") is TrustGrade.L2_GUARDED  # short alias, case-insensitive
    assert TrustGrade.parse("GUARDED") is TrustGrade.L2_GUARDED  # long alias
    assert TrustGrade.parse("3") is TrustGrade.L3_AUTONOMOUS


@pytest.mark.parametrize("bad", [4, -1, "L9", "nonsense", True, 1.5])
def test_trust_grade_parse_rejects_junk(bad):
    with pytest.raises(ValueError):
        TrustGrade.parse(bad)


# -- gate: no policy / non-side-effecting = LIVE (today's behavior) ---------
def test_gate_no_policy_is_live():
    d = evaluate_deploy_gate(side_effecting=True, trust_seed=TrustGrade.L0_SHADOW)
    assert d == GateDecision(LIVE, DEFAULT_QUALIFIER)


def test_gate_non_side_effecting_never_gated_even_with_floor():
    d = evaluate_deploy_gate(
        side_effecting=False,
        trust_seed=TrustGrade.L0_SHADOW,
        min_autonomy=TrustGrade.L3_AUTONOMOUS,
    )
    assert d.mode == LIVE and d.qualifier == DEFAULT_QUALIFIER


# -- gate: escalation (hold) ------------------------------------------------
def test_gate_below_floor_holds():
    d = evaluate_deploy_gate(
        side_effecting=True,
        trust_seed=TrustGrade.L0_SHADOW,
        min_autonomy=TrustGrade.L2_GUARDED,
    )
    assert d.mode == HOLD and d.qualifier is None and "min_autonomy" in d.reason


def test_gate_require_approval_holds_regardless_of_grade():
    d = evaluate_deploy_gate(
        side_effecting=True, trust_seed=TrustGrade.L3_AUTONOMOUS, require_approval=True
    )
    assert d.mode == HOLD and d.qualifier is None


# -- gate: shadow (cleared but not live) ------------------------------------
def test_gate_cleared_l0_deploys_shadow():
    d = evaluate_deploy_gate(
        side_effecting=True,
        trust_seed=TrustGrade.L0_SHADOW,
        min_autonomy=TrustGrade.L0_SHADOW,
    )
    assert d.mode == SHADOW and d.qualifier == SHADOW_QUALIFIER
    assert SHADOW_QUALIFIER != DEFAULT_QUALIFIER


# -- gate: live -------------------------------------------------------------
def test_gate_high_grade_meets_floor_is_live():
    d = evaluate_deploy_gate(
        side_effecting=True,
        trust_seed=TrustGrade.L3_AUTONOMOUS,
        min_autonomy=TrustGrade.L1_CANARY,
    )
    assert d.mode == LIVE and d.qualifier == DEFAULT_QUALIFIER


def test_gate_exactly_at_floor_above_l0_is_live():
    d = evaluate_deploy_gate(
        side_effecting=True,
        trust_seed=TrustGrade.L2_GUARDED,
        min_autonomy=TrustGrade.L2_GUARDED,
    )
    assert d.mode == LIVE and d.qualifier == DEFAULT_QUALIFIER


# -- manifest integration ---------------------------------------------------
def test_manifest_parses_trust_fields():
    from concursus.manifest import AgentManifest

    m = AgentManifest.from_dict(
        {
            "name": "writer",
            "registry": {"container_uri": "x"},
            "contract": {"outputs": {"out": {"type": "string", "required": True}}},
            "trust_seed": "L2_GUARDED",
            "side_effecting": True,
            "escalate_boundary": "oncall-sec",
        }
    ).validate()
    assert m.trust_seed is TrustGrade.L2_GUARDED
    assert m.side_effecting is True
    assert m.escalate_boundary == "oncall-sec"


def test_manifest_defaults_are_ungated_l0():
    from concursus.manifest import AgentManifest

    m = AgentManifest.from_dict(
        {
            "name": "reader",
            "registry": {"container_uri": "x"},
            "contract": {"outputs": {"out": {"type": "string", "required": True}}},
        }
    )
    assert m.trust_seed is TrustGrade.L0_SHADOW
    assert m.side_effecting is False and m.escalate_boundary == ""


def test_manifest_rejects_malformed_trust_seed():
    from concursus.manifest import AgentManifest, ManifestError

    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            {
                "name": "writer",
                "registry": {"container_uri": "x"},
                "contract": {"outputs": {"out": {"type": "string"}}},
                "trust_seed": "nonsense",
            }
        )
