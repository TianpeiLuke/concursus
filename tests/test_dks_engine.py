"""Tests for the DKS engine — the Phase-5 deliberation state machine (AI-24 + AI-27 + AI-32).

Exercises the cyclic ``observe -> ... -> compile -> re-observe`` deliberation engine WITHOUT any
LLM/LangGraph installed: the pure-Python fallback driver runs the SAME node functions + routing,
the loop is BOUNDED (a pathological stub cannot exceed ``max_rounds``), the AI-27 confidence gate
returns the right band per score, an injected AI-32 ``policy=`` overrides routing, and constructing
the engine needs neither langgraph nor a model. Nothing here touches ``Supervisor.run``.
"""

import sys

import pytest

from concursus.dks_engine import (
    BAND_ARGUE_COUNTER,
    BAND_AUTO_ACCEPT,
    BAND_ESCALATE,
    CCSWeights,
    DKSEngine,
    DKSEngineError,
    DKSState,
    compute_ccs,
    route_by_confidence,
)
from concursus.trailstore import HypothesisTrail, ThreadNotResolved, require_resolved


# -- (i) runs to termination on a stub investigator with NO langgraph -------
def test_engine_runs_to_termination_pure_python_fallback(tmp_path):
    """With langgraph absent, the pure-Python fallback drives the cycle to a resolved frontier."""
    assert "langgraph" not in sys.modules  # sanity: no langgraph in this suite
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("why did the deploy fail?", ["h"])[0]

    def investigator(h):
        # Fan once at the shallow root, then accept everything → guaranteed convergence.
        if h.depth < 1:
            return ["sharper-1", "sharper-2"]
        return {"verdict": "ACCEPT", "evidence": {"seen": h.text}}

    engine = DKSEngine(trail, investigator=investigator, max_rounds=8)
    result = engine.run(root)

    assert result.backend == "python"  # fell back — langgraph not installed
    assert result.converged is True
    assert result.resolved is True
    assert result.frontier == []
    assert trail.open_frontier(root) == []
    require_resolved(trail, root)  # converged → no raise
    engine.lower_guard(root)  # AI-30 hand-off guard passes on a converged debate
    # The cyclic trace visited every deliberation node at least once.
    for node in ("observe", "name", "structure", "operationalize", "test",
                 "challenge", "improve", "compile"):
        assert node in result.trace
    # The MDP state pointer is populated.
    assert result.state.node_count >= 1
    assert set(result.state.label_fractions) == {"in", "out", "undec"}


def test_engine_default_investigator_terminates_immediately(tmp_path):
    """The default deterministic stub (no LLM) closes the frontier UNDEC in one round."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["a", "b"])[0]
    engine = DKSEngine(trail)  # default investigator, default heuristic policy, no counter-arg
    result = engine.run(root)
    assert result.converged is True
    assert trail.hypotheses(root)[root].verdict == "UNDEC"


# -- (ii) bounded — a pathological stub cannot exceed max_rounds ------------
def test_engine_is_bounded_by_max_rounds_with_pathological_stub(tmp_path):
    """An investigator that never resolves is hard-capped by max_rounds (no runaway expansion)."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["h"])[0]

    def never_resolves(h):
        return ["more"]  # always fans a child, never a verdict

    engine = DKSEngine(trail, investigator=never_resolves, max_rounds=3, depth_cap=1000)
    result = engine.run(root)
    assert result.rounds == 3  # hard budget hit
    assert result.converged is False  # frontier never emptied, but the loop still terminated
    assert result.frontier != []
    # The termination guard correctly reports the debate has NOT converged.
    with pytest.raises(ThreadNotResolved):
        engine.lower_guard(root)


def test_engine_rejects_bad_config():
    """Guard rails: unknown backend and a non-positive round budget are rejected."""
    with pytest.raises(DKSEngineError):
        DKSEngine(_FakeTrail(), backend="bogus")
    with pytest.raises(DKSEngineError):
        DKSEngine(_FakeTrail(), max_rounds=0)


def test_engine_langgraph_backend_raises_when_missing(tmp_path):
    """Explicitly requesting the langgraph backend when it is absent raises (never a hard dep)."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["h"])[0]
    engine = DKSEngine(trail, backend="langgraph")
    with pytest.raises(DKSEngineError):
        engine.run(root)


# -- (iii) route_by_confidence bands for 0.9 / 0.7 / 0.3 --------------------
def test_route_by_confidence_bands():
    assert route_by_confidence(0.9) == BAND_AUTO_ACCEPT
    assert route_by_confidence(0.7) == BAND_ARGUE_COUNTER
    assert route_by_confidence(0.3) == BAND_ESCALATE
    # Boundaries: >= 0.85 auto-accept, >= 0.50 argue, else escalate.
    assert route_by_confidence(0.85) == BAND_AUTO_ACCEPT
    assert route_by_confidence(0.50) == BAND_ARGUE_COUNTER
    assert route_by_confidence(0.4999) == BAND_ESCALATE


def test_compute_ccs_is_convex_and_clamped():
    w = CCSWeights(alpha=0.5, beta=0.25, gamma=0.25)
    assert compute_ccs(1.0, 1.0, 1.0, w) == pytest.approx(1.0)
    assert compute_ccs(0.0, 0.0, 0.0, w) == pytest.approx(0.0)
    assert compute_ccs(0.8, 0.4, 0.4, w) == pytest.approx(0.5 * 0.8 + 0.25 * 0.4 + 0.25 * 0.4)
    # Out-of-range inputs are clamped into [0, 1].
    assert compute_ccs(2.0, -1.0, 5.0, w) == pytest.approx(0.5 * 1.0 + 0.25 * 0.0 + 0.25 * 1.0)


# -- (iv) an injected policy= overrides routing -----------------------------
def test_injected_policy_overrides_heuristic_routing():
    """A learned AI-32 policy fully overrides the heuristic bands."""
    calls = {"n": 0, "seen_state": None}

    def always_escalate(score, state):
        calls["n"] += 1
        calls["seen_state"] = state
        return BAND_ESCALATE

    # A high score that the heuristic would auto-accept is escalated by the policy.
    assert route_by_confidence(0.99, policy=always_escalate) == BAND_ESCALATE
    assert calls["n"] == 1

    # The policy also sees the DKS-MDP state pointer (the AI-32 observation).
    st = DKSState(node_count=3)
    route_by_confidence(0.5, state=st, policy=always_escalate)
    assert calls["seen_state"] is st


def test_injected_policy_returning_unknown_band_is_rejected():
    """A rogue policy cannot inject an unknown route."""
    with pytest.raises(DKSEngineError):
        route_by_confidence(0.5, policy=lambda score, state: "teleport")


def test_engine_uses_injected_policy_on_challenge_step(tmp_path):
    """The engine threads policy= into route_by_confidence at the CHALLENGE step."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", [{"text": "h", "confidence": 0.1}])[0]

    seen = {"bands": 0}

    def policy(score, state):
        seen["bands"] += 1
        return BAND_ARGUE_COUNTER

    def counter(h, tr):
        # MOOG seam: emit one counter-argument the first time only (bounded by depth_cap anyway).
        if h.depth == 0:
            return ["counter: maybe not"]
        return None

    engine = DKSEngine(
        trail,
        investigator=lambda h: {"verdict": "UNDEC"},  # close leaves so it terminates
        policy=policy,
        counter_argument_fn=counter,
        max_rounds=4,
    )
    result = engine.run(root)
    assert seen["bands"] >= 1  # the challenge step consulted the injected policy
    assert result.converged is True


# -- (v) importing concursus + constructing the engine needs no langgraph ---
def test_import_and_construct_need_no_langgraph_or_llm(tmp_path):
    assert "langgraph" not in sys.modules
    import concursus  # top-level import must not pull langgraph/LLM

    assert hasattr(concursus, "DKSEngine")
    assert concursus.route_by_confidence(0.9) == BAND_AUTO_ACCEPT
    trail = HypothesisTrail(tmp_path / "run")
    engine = concursus.DKSEngine(trail)  # construction is model-free
    assert isinstance(engine, concursus.DKSEngine)
    assert "langgraph" not in sys.modules  # still not imported after constructing


class _FakeTrail:
    """A do-nothing trail stand-in for pure config-validation tests (never .run())."""

    def hypotheses(self, root=None):  # pragma: no cover - not reached in config tests
        return {}
