"""Tests for the inner graph — parallel hypothesis-investigator dispatch + DIGEST write-back (AI-25 + AI-29).

Exercises the FAR-HORIZON reasoning tier WITHOUT any LLM/LangGraph installed:

* AI-25 — :func:`partition_frontier` respects the concurrency ceiling; :func:`dispatch_frontier`
  runs ONE stub investigator per open hypothesis and merges the results ORDER-INSENSITIVELY; a
  failing investigator yields an ``ok=False`` result, never a raised exception.
* AI-29 — the ``.2`` worker-log digest appends an ACTION marker + a slipbox-card RESULT (raw
  offloaded to a ``log_ref`` file, never inlined); a retried digest with the same ``dedup_key`` is
  an idempotent no-op that survives a fresh digest over the same lane (reload path).

Identity guards: the inner graph is a FRESH disposable per-round projection (it holds no reference
to the durable trail or the committed plan), it is confined to the ``.2`` lane, and it NEVER writes
a ``.3`` verdict. Nothing here touches ``Supervisor.run``. Import needs no langgraph/LLM.
"""

import importlib.util
import sys
import time

import pytest

#: The invariant is that concursus never HARD-imports langgraph. The "not in sys.modules" sanity
#: checks below only hold when langgraph is absent (the zero-dependency system-python run enforces
#: them); when it is installed a prior test may have imported it, so those assertions are gated.
LANGGRAPH_INSTALLED = importlib.util.find_spec("langgraph") is not None

from concursus.reasoning import inner_graph as ig
from concursus.reasoning.inner_graph import (
    MAX_FANOUT_CAP,
    InnerGraph,
    InnerGraphDigest,
    InnerGraphError,
    InvestigationResult,
    compile_inner_graph,
    dispatch_frontier,
    partition_frontier,
    resolve_ceiling,
)
from concursus.reasoning.trailstore import HypothesisTrail


def _trail_with_open_frontier(tmp_path, n=5):
    """A trail whose root has ``n`` open (low-confidence, unresolved leaf) child hypotheses."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("why did the deploy fail?", ["root-hyp"])[0]
    trail.fanout_hypotheses(root, [f"cand-{i}" for i in range(n)])
    return trail, root


# -- (i) partition_frontier respects the ceiling ---------------------------
def test_partition_frontier_respects_ceiling():
    frontier = [f"h{i}" for i in range(10)]
    batches = partition_frontier(frontier, 4)
    assert [len(b) for b in batches] == [4, 4, 2]  # no batch exceeds the ceiling
    assert all(len(b) <= 4 for b in batches)
    # Order-preserving and lossless: concatenation recovers the frontier.
    assert [hid for b in batches for hid in b] == frontier
    # An empty frontier yields no batches; a ceiling >= size yields a single batch.
    assert partition_frontier([], 4) == []
    assert partition_frontier(["a", "b"], 8) == [["a", "b"]]


def test_partition_frontier_rejects_nonpositive_ceiling():
    with pytest.raises(InnerGraphError):
        partition_frontier(["a"], 0)
    with pytest.raises(InnerGraphError):
        partition_frontier(["a"], -1)


# -- resolve_ceiling: the min(pref, cap) clamp SHAPE ------------------------
def test_resolve_ceiling_clamps_pref_by_cap():
    # A soft preference can only TIGHTEN below the capacity, never rise above it.
    assert resolve_ceiling(2, 8) == 2  # pref under cap → pref wins
    assert resolve_ceiling(100, 8) == 8  # pref over cap → clamped to cap
    assert resolve_ceiling(8, 8) == 8  # equal → unchanged
    # The default ceiling of 4 is preserved on any host with >= 4 usable cores (cap >= 4).
    assert resolve_ceiling(4, 4) == 4
    assert resolve_ceiling(4, 14) == 4


def test_resolve_ceiling_floors_at_one():
    # max(1, ...) keeps the fan-out bounded and making progress for degenerate pref/cap.
    assert resolve_ceiling(0, 8) == 1
    assert resolve_ceiling(-5, 8) == 1
    assert resolve_ceiling(4, 0) == 1


def test_max_fanout_cap_is_the_hard_preference_independent_ceiling():
    # The hard cap sits above the default ceiling, so the default (4) is never touched by it,
    # yet it is the absolute upper bound that a soft config can only tighten below.
    assert MAX_FANOUT_CAP > ig._DEFAULT_CEILING
    # A CPU-derived capacity is itself floored at 1 and hard-capped by MAX_FANOUT_CAP.
    assert 1 <= ig._cpu_capacity() <= MAX_FANOUT_CAP


def test_cpu_capacity_hard_caps_a_many_core_host(monkeypatch):
    # Even a host reporting far more cores than the hard cap is bounded to MAX_FANOUT_CAP.
    monkeypatch.setattr(ig.os, "cpu_count", lambda: 100_000)
    assert ig._cpu_capacity() == MAX_FANOUT_CAP
    # A None cpu_count (unknowable) degrades safely to 1, never 0 or a crash.
    monkeypatch.setattr(ig.os, "cpu_count", lambda: None)
    assert ig._cpu_capacity() == 1


def test_compile_inner_graph_clamps_ceiling_by_cpu_capacity(tmp_path, monkeypatch):
    # A caller ceiling ABOVE the host capacity is tightened to the capacity (never explodes).
    monkeypatch.setattr(ig, "_cpu_capacity", lambda: 2)
    trail, root = _trail_with_open_frontier(tmp_path, n=6)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=100)
    assert graph.ceiling == 2  # clamped by capacity, not the caller's soft request
    assert all(len(b) <= 2 for b in graph.batches)
    assert len(graph) == 6  # still one per open hypothesis — the frontier is unchanged


def test_compile_inner_graph_default_ceiling_unchanged_under_capacity(tmp_path, monkeypatch):
    # DEFAULT PATH byte-for-byte: with the default ceiling 4 and a host with >= 4 cores, the
    # effective ceiling stays exactly 4 (today's behavior).
    monkeypatch.setattr(ig, "_cpu_capacity", lambda: 14)
    trail, root = _trail_with_open_frontier(tmp_path, n=5)
    graph = compile_inner_graph(trail, root)  # default concurrency_ceiling=4
    assert graph.ceiling == 4
    assert [len(b) for b in graph.batches] == [4, 1]


def test_compile_inner_graph_partitions_open_frontier(tmp_path):
    trail, root = _trail_with_open_frontier(tmp_path, n=5)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=2)
    assert isinstance(graph, InnerGraph)
    assert graph.ceiling == 2
    assert all(len(b) <= 2 for b in graph.batches)
    assert len(graph) == 5  # one per open hypothesis
    # It is a read-only snapshot of the frontier — every id has a captured Hypothesis.
    assert set(graph.projection) == set(graph.frontier)


# -- (ii) dispatch runs one stub investigator per open hypothesis + merges order-insensitively --
def test_dispatch_runs_one_investigator_per_open_hypothesis(tmp_path):
    trail, root = _trail_with_open_frontier(tmp_path, n=5)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=3)

    calls = []
    lock_ids = set()

    def investigator(h):
        calls.append(h.id)
        return {"verdict": "UNDEC", "evidence": {"seen": h.text}}

    merged = dispatch_frontier(graph, investigator)
    # Exactly one investigation per open hypothesis, keyed by hypothesis id.
    assert set(merged) == set(graph.frontier)
    assert len(merged) == 5
    assert sorted(calls) == sorted(graph.frontier)
    assert all(r.ok for r in merged.values())
    assert all(r.outcome == {"verdict": "UNDEC", "evidence": {"seen": trail.hypotheses(root)[hid].text}}
               for hid, r in merged.items())


def test_dispatch_merge_is_order_insensitive(tmp_path):
    """Workers finishing in a scrambled order still merge to the same id-keyed result set."""
    trail, root = _trail_with_open_frontier(tmp_path, n=4)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=4)

    # Make later-listed hypotheses finish FIRST (inverted sleep) — completion order != frontier order.
    order_index = {hid: i for i, hid in enumerate(graph.frontier)}

    def investigator(h):
        time.sleep(0.01 * (len(order_index) - order_index[h.id]))
        return {"verdict": "ACCEPT", "evidence": {"rank": order_index[h.id]}}

    merged = dispatch_frontier(graph, investigator)
    assert set(merged) == set(graph.frontier)  # keyed by id → completion order is irrelevant
    for hid in graph.frontier:
        assert merged[hid].outcome["evidence"]["rank"] == order_index[hid]


def test_dispatch_default_investigator_needs_no_llm(tmp_path):
    """The default deterministic stub investigates every open leaf with no model installed."""
    if not LANGGRAPH_INSTALLED:
        assert "langgraph" not in sys.modules
    trail, root = _trail_with_open_frontier(tmp_path, n=3)
    graph = compile_inner_graph(trail, root)
    merged = dispatch_frontier(graph)  # no investigator → deterministic stub
    assert len(merged) == 3
    assert all(r.ok and r.outcome["verdict"] == "UNDEC" for r in merged.values())


# -- (iii) a failing investigator yields RESULT(ok=False), not an exception -
def test_failing_investigator_yields_ok_false_not_exception(tmp_path):
    trail, root = _trail_with_open_frontier(tmp_path, n=3)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=3)
    frontier = graph.frontier
    boom = frontier[1]

    def investigator(h):
        if h.id == boom:
            raise RuntimeError("investigator blew up")
        return {"verdict": "ACCEPT"}

    merged = dispatch_frontier(graph, investigator)  # must NOT raise
    assert set(merged) == set(frontier)
    # The failing worker is a first-class ok=False result carrying the error string.
    assert merged[boom].ok is False
    assert merged[boom].outcome is None
    assert "RuntimeError" in merged[boom].error
    # Its siblings still succeeded — one worker's crash never aborted the fan-out.
    for hid in frontier:
        if hid != boom:
            assert merged[hid].ok is True


# -- (iv) a retried digest with the same dedup_key is a no-op ---------------
def test_retried_digest_same_dedup_key_is_noop(tmp_path):
    digest = InnerGraphDigest(tmp_path / "run")
    res = InvestigationResult(hypothesis_id=".3/h1/c2", ok=True, outcome={"verdict": "UNDEC"})
    key = res.key()

    first = digest.write_back(res)
    assert first.digested is True
    assert first.log_ref and first.card_ref
    # The card exists and the RAW payload is OFFLOADED to the log_ref (not inlined into the card).
    card_text = (digest.lane_dir / first.card_ref).read_text()
    assert first.log_ref in card_text
    assert '"verdict": "UNDEC"' not in card_text  # raw is offloaded, not inlined
    assert (digest.lane_dir / first.log_ref).exists()
    markers_after_first = digest.markers()
    assert len(markers_after_first) == 1

    # A retry with the SAME dedup_key is an idempotent no-op — no second marker/card.
    retry = InvestigationResult(hypothesis_id=".3/h1/c2", ok=True, outcome={"verdict": "ACCEPT"},
                                dedup_key=key)
    second = digest.write_back(retry)
    assert second.digested is False
    assert digest.markers() == markers_after_first  # unchanged
    assert digest.seen_keys() == [key]


def test_dedup_survives_restart_over_same_lane(tmp_path):
    """A fresh digest over the same ``.2`` lane reloads dedup keys → the retry is still a no-op."""
    run_dir = tmp_path / "run"
    d1 = InnerGraphDigest(run_dir)
    res = InvestigationResult(hypothesis_id=".3/h9", action="investigate", outcome={"v": 1})
    d1.write_back(res)
    key = res.key()

    # Simulate a process restart: a brand-new digest object over the same on-disk lane.
    d2 = InnerGraphDigest(run_dir)
    assert key in d2.seen_keys()  # reloaded from the existing lane log
    again = d2.write_back(InvestigationResult(hypothesis_id=".3/h9", action="investigate",
                                              outcome={"v": 2}))
    assert again.digested is False  # idempotent across the restart
    assert len(d2.markers()) == 1


# -- write-back through dispatch confines to .2 and never writes .3 ---------
def test_dispatch_with_digest_writes_only_the_2_lane(tmp_path):
    trail, root = _trail_with_open_frontier(tmp_path, n=3)
    graph = compile_inner_graph(trail, root, concurrency_ceiling=2)
    digest = InnerGraphDigest(trail.branch_dir.parent)  # run_dir/.2 lane

    merged = dispatch_frontier(graph, digest=digest)
    assert all(r.digested for r in merged.values())
    assert len(digest.markers()) == 3

    # AI-29 identity guard: the digest touched ONLY the ``.2`` lane, never the ``.3`` branch.
    assert digest.lane_dir.name == ".2"
    # The ``.3`` trail is unchanged — no verdicts were written (no RESOLVED markers).
    model = trail.hypotheses(root)
    assert all(not h.resolved and h.verdict is None for h in model.values())
    assert trail.open_frontier(root) != []  # still open — the inner graph resolves nothing


def test_inner_graph_is_a_fresh_disposable_projection(tmp_path):
    """Re-compiling after the frontier shrinks yields a NEW, smaller projection (not cyclic state)."""
    trail, root = _trail_with_open_frontier(tmp_path, n=4)
    g1 = compile_inner_graph(trail, root)
    assert len(g1) == 4
    # The engine (not the inner graph) closes a leaf; a re-compile reflects the shrunken frontier.
    trail.write_verdict(g1.frontier[0], "ACCEPT")
    g2 = compile_inner_graph(trail, root)
    assert len(g2) == 3
    assert g1 is not g2  # disposable — a fresh projection each round


def test_import_needs_no_langgraph_or_llm(tmp_path):
    if not LANGGRAPH_INSTALLED:
        assert "langgraph" not in sys.modules
    import concursus

    assert hasattr(concursus, "compile_inner_graph")
    assert hasattr(concursus, "InnerGraphDigest")
    if not LANGGRAPH_INSTALLED:
        assert "langgraph" not in sys.modules
