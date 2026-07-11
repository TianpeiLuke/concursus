"""Tests for the plan-formation driver (deliberate — AI-28/30/31).

These pin the Phase-5 crux: SEED starts an episode from a goal, LOWER may run only over a
CONVERGED debate and yields an IMMUTABLE AgentDAG the existing assembler freezes, and form_plan
runs the bounded SEED -> ... -> LOWER loop to a frozen plan with NEITHER langgraph NOR any LLM —
never touching Supervisor.run.
"""

from __future__ import annotations

import importlib.util

import pytest

from concursus.assemble.assemble import OrchestrationAssembler
from concursus.core.dag import AgentDAG
from concursus.reasoning.deliberate import form_plan, lower_to_dag, seed
from concursus.reasoning.trailstore import HypothesisTrail, ThreadNotResolved


def _trail(tmp_path):
    return HypothesisTrail(tmp_path / "run")


# -- AI-28: SEED ------------------------------------------------------------
def test_seed_from_goal_creates_root_hypotheses(tmp_path):
    trail = _trail(tmp_path)
    roots = seed(trail, "summarize a document then critique it")
    assert roots  # at least the goal-derived root
    model = trail.hypotheses(roots[0])
    assert roots[0] in model


def test_seed_requires_a_goal(tmp_path):
    trail = _trail(tmp_path)
    with pytest.raises(ValueError):
        seed(trail, "")


class _ReuseRetriever:
    """A stub retriever whose top hit is a strong precedent carrying a 3-step decomposition."""

    def retrieve(self, text, *, limit=3):
        class _RP:
            trail_id = "prior_run_x"
            score = 1.0
            payload = {"trail_id": "prior_run_x", "nodes": ["step_a", "step_b", "step_c"]}
        return [_RP()]


class _WeakRetriever:
    """A stub retriever whose only hit is BELOW the reuse threshold (must be ignored → cold start)."""

    def retrieve(self, text, *, limit=3):
        class _RP:
            trail_id = "weak"
            score = 0.1
            payload = {"trail_id": "weak", "nodes": ["x"]}
        return [_RP()]


def test_seed_reuses_strong_precedent_decomposition_leaving_frontier_empty(tmp_path):
    # A strong precedent is REUSED, not appended: one goal root pre-decomposed into confident
    # children, so the open frontier is empty — no re-investigation (the prune-and-replace fix).
    trail = _trail(tmp_path)
    roots = seed(trail, "goal", retriever=_ReuseRetriever())
    assert len(roots) == 1  # ONE goal root (not goal + an extra sibling precedent root)
    model = trail.hypotheses(roots[0])
    # the 3 prior steps were fanned out as confident children (>= confidence_floor)
    children = [h for hid, h in model.items() if h.parent == roots[0]]
    assert len(children) == 3
    assert all(c.confidence >= 0.6 for c in children)
    # reused steps are confident → excluded from the frontier → nothing to re-investigate
    assert trail.open_frontier(roots[0]) == []


def test_seed_weak_precedent_falls_back_to_cold_start(tmp_path):
    # A precedent below the reuse threshold is ignored → the byte-for-byte cold-start behavior.
    trail = _trail(tmp_path)
    roots_cold = seed(HypothesisTrail(tmp_path / "cold"), "goal")
    roots_weak = seed(trail, "goal", retriever=_WeakRetriever())
    assert len(roots_weak) == len(roots_cold) == 1
    # a cold start's single root is an open frontier of exactly one (the approach to decompose)
    assert len(trail.open_frontier(roots_weak[0])) == 1


# -- AI-30: LOWER -----------------------------------------------------------
def test_lower_raises_on_open_frontier(tmp_path):
    trail = _trail(tmp_path)
    roots = seed(trail, "goal")  # seeded, nothing resolved yet
    with pytest.raises(ThreadNotResolved):
        lower_to_dag(trail, roots[0])


def test_lower_returns_assemblable_dag_once_resolved(tmp_path):
    trail = _trail(tmp_path)
    roots = seed(trail, "goal")
    root = roots[0]
    # Resolve every open hypothesis ACCEPT so it labels IN.
    for hid in trail.open_frontier(root):
        trail.write_verdict(hid, "ACCEPT", {"reason": "test"})
    dag = lower_to_dag(trail, root)
    assert isinstance(dag, AgentDAG)
    assert dag.nodes  # accepted hypotheses became nodes
    # topological_sort works => acyclic/valid, i.e. an assembler could freeze it.
    assert dag.topological_sort()


def test_lower_drops_rejected_hypotheses(tmp_path):
    trail = _trail(tmp_path)
    roots = seed(trail, "goal")
    root = roots[0]
    frontier = trail.open_frontier(root)
    # ACCEPT the first, REJECT the rest.
    for i, hid in enumerate(frontier):
        trail.write_verdict(hid, "ACCEPT" if i == 0 else "REJECT", {"reason": "test"})
    dag = lower_to_dag(trail, root)
    # Only the accepted (IN) hypothesis survives as a node.
    assert len(dag.nodes) == 1


# -- AI-31: the bounded driver ----------------------------------------------
def _accepting_investigator(h):
    """A deterministic stub investigator that ACCEPTs every hypothesis (no LLM)."""
    return {"verdict": "ACCEPT", "evidence": {"reason": "stub accept"}}


def test_form_plan_end_to_end_to_frozen_dag(tmp_path):
    trail = _trail(tmp_path)
    dag = form_plan(trail, "summarize then critique", investigator=_accepting_investigator)
    assert isinstance(dag, AgentDAG)
    assert dag.nodes
    # The frozen DAG is consumable by the existing compiler front (topo-sortable, acyclic).
    order = dag.topological_sort()
    assert set(order) == set(dag.nodes)


def test_form_plan_is_bounded(tmp_path):
    trail = _trail(tmp_path)

    def _fan_forever(h):
        # A pathological investigator that always fans children instead of resolving.
        return [{"text": f"child of {h.id}", "confidence": 0.0}]

    # Must terminate (bounded by max_rounds/depth_cap) rather than hang; either it lowers a
    # (possibly empty) DAG once the depth cap closes the frontier, or it raises ThreadNotResolved —
    # both are bounded outcomes, neither is an infinite loop.
    try:
        dag = form_plan(
            trail, "goal", investigator=_fan_forever, max_rounds=3, depth_cap=2
        )
        assert isinstance(dag, AgentDAG)
    except ThreadNotResolved:
        pass  # bounded termination with an unresolved frontier is acceptable


def test_form_plan_needs_no_langgraph_or_llm(tmp_path):
    # langgraph must not be required for the driver to run end-to-end.
    assert importlib.util.find_spec("langgraph") is None
    trail = _trail(tmp_path)
    dag = form_plan(trail, "goal", investigator=_accepting_investigator)
    assert isinstance(dag, AgentDAG)


def test_form_plan_default_investigator_terminates(tmp_path):
    # The default (UNDEC) investigator closes everything immediately => bounded, no hang.
    trail = _trail(tmp_path)
    dag = form_plan(trail, "goal")  # no investigator => deterministic UNDEC stub
    assert isinstance(dag, AgentDAG)  # UNDEC => nothing IN => empty-but-valid DAG
