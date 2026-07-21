"""Tests for the PURE ``state -> Decision`` scheduling core (:func:`compute_schedule`).

Unlike the impure :meth:`TrustLadderScheduler.propose_frontier` (which reads the live registry +
GOV-side trust ladder), :func:`compute_schedule` is a TOTAL, DETERMINISTIC function of an
already-resolved ``state`` VALUE with NO I/O. Every gate result (deps/trust/budget) is precomputed
by the caller; this core only PARTITIONS the frontier into ``dispatch`` vs ``declined``, where every
non-dispatch carries exactly one FIRST-CLASS structured reason. These tests exercise the decision
logic directly with plain mappings (proving purity: no registry, no ledger, no plan) and with
duck-typed objects.
"""

from __future__ import annotations

import types

import pytest

from concursus.governor.scheduler import (
    DECLINE_BUDGET_EXHAUSTED,
    DECLINE_DEPS_UNMET,
    DECLINE_TRUST_GATE_FAILED,
    Decision,
    DeclinedNode,
    SchedulerError,
    compute_schedule,
)


def _state(*nodes):
    return {"nodes": list(nodes)}


def test_all_gates_pass_dispatches():
    """A node clearing deps + trust + budget is dispatched, with no declines."""
    d = compute_schedule(_state({"node": "triage", "deps_met": True, "trust_ok": True, "budget_ok": True}))
    assert isinstance(d, Decision)
    assert d.dispatch == ("triage",)
    assert d.declined == ()


def test_defaults_are_permissive_so_a_bare_node_dispatches():
    """Absent gate flags default to True (permissive) — a bare {'node': x} dispatches (pure default)."""
    d = compute_schedule(_state({"node": "a"}, {"node": "b"}))
    assert d.dispatch == ("a", "b")
    assert d.declined == ()


def test_deps_unmet_is_first_class_decline():
    d = compute_schedule(_state({"node": "refund", "deps_met": False}))
    assert d.dispatch == ()
    assert len(d.declined) == 1
    dn = d.declined[0]
    assert isinstance(dn, DeclinedNode)
    assert dn.node == "refund"
    assert dn.reason == DECLINE_DEPS_UNMET


def test_trust_gate_failed_is_first_class_decline():
    d = compute_schedule(_state({"node": "refund", "trust_ok": False}))
    assert d.dispatch == ()
    assert d.declined[0].reason == DECLINE_TRUST_GATE_FAILED


def test_budget_exhausted_is_first_class_decline():
    d = compute_schedule(_state({"node": "refund", "budget_ok": False}))
    assert d.dispatch == ()
    assert d.declined[0].reason == DECLINE_BUDGET_EXHAUSTED


def test_decline_precedence_deps_beats_trust_and_budget():
    """When several gates fail, the EARLIEST (most fundamental) blocker is reported: deps first."""
    d = compute_schedule(_state({"node": "x", "deps_met": False, "trust_ok": False, "budget_ok": False}))
    assert d.declined[0].reason == DECLINE_DEPS_UNMET


def test_decline_precedence_trust_beats_budget():
    d = compute_schedule(_state({"node": "x", "trust_ok": False, "budget_ok": False}))
    assert d.declined[0].reason == DECLINE_TRUST_GATE_FAILED


def test_mixed_frontier_partitions_and_preserves_order():
    """A mixed frontier splits cleanly; input order is preserved in both dispatch and declined."""
    d = compute_schedule(_state(
        {"node": "a", "deps_met": True, "trust_ok": True, "budget_ok": True},
        {"node": "b", "deps_met": False},
        {"node": "c", "trust_ok": False},
        {"node": "d"},
        {"node": "e", "budget_ok": False},
    ))
    assert d.dispatch == ("a", "d")                       # order preserved
    assert [dn.node for dn in d.declined] == ["b", "c", "e"]  # order preserved
    assert d.declined_by(DECLINE_DEPS_UNMET) == ("b",)
    assert d.declined_by(DECLINE_TRUST_GATE_FAILED) == ("c",)
    assert d.declined_by(DECLINE_BUDGET_EXHAUSTED) == ("e",)


def test_custom_detail_is_carried_but_not_load_bearing():
    """A caller-supplied detail overrides the default template; the reason is still structured."""
    d = compute_schedule(_state({"node": "x", "trust_ok": False, "detail": "L1 below L2 bar"}))
    dn = d.declined[0]
    assert dn.reason == DECLINE_TRUST_GATE_FAILED       # machine-checkable cause
    assert dn.detail == "L1 below L2 bar"               # human elaboration


def test_default_detail_when_none_supplied():
    d = compute_schedule(_state({"node": "x", "budget_ok": False}))
    assert d.declined[0].detail  # non-empty default template


def test_missing_nodes_yields_empty_decision():
    assert compute_schedule({}) == Decision()
    assert compute_schedule({"nodes": []}) == Decision()


def test_missing_node_label_raises():
    with pytest.raises(SchedulerError, match="missing a 'node' label"):
        compute_schedule(_state({"deps_met": True}))


def test_duck_typed_state_and_entries_work():
    """State and entries may be objects (attrs), not just mappings — proves the read seam is generic."""
    entry = types.SimpleNamespace(node="x", deps_met=True, trust_ok=False, budget_ok=True)
    state = types.SimpleNamespace(nodes=[entry])
    d = compute_schedule(state)
    assert d.dispatch == ()
    assert d.declined[0].node == "x"
    assert d.declined[0].reason == DECLINE_TRUST_GATE_FAILED


def test_to_dict_is_a_plain_value():
    """Decision.to_dict is a plain, log-safe dict (append-only StateStore payload shape)."""
    d = compute_schedule(_state(
        {"node": "a"},
        {"node": "b", "deps_met": False},
    ))
    out = d.to_dict()
    assert out["dispatch"] == ["a"]
    assert out["declined"] == [{"node": "b", "reason": DECLINE_DEPS_UNMET, "detail": out["declined"][0]["detail"]}]
    assert out["declined"][0]["reason"] == "deps_unmet"


def test_is_pure_no_io_no_registry_needed():
    """The whole point: NO registry / ledger / plan is constructed — a plain dict is enough, and the
    same input always yields the same Decision (determinism)."""
    state = _state(
        {"node": "a"},
        {"node": "b", "trust_ok": False},
    )
    first = compute_schedule(state)
    second = compute_schedule(state)
    assert first == second                       # deterministic (frozen dataclass equality)
    assert state == _state(                        # input untouched (no mutation)
        {"node": "a"},
        {"node": "b", "trust_ok": False},
    )
