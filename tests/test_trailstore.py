"""Tests for the HypothesisTrail — the durable ``.3`` reasoning-branch store (AI-23 + AI-26).

Exercises the Phase-5 plan-formation substrate WITHOUT any LLM/LangGraph installed (a pure stdlib
trail + stub investigator callables): hypothesis fan-out under the ``.3`` branch, the depth/
confidence-bounded open frontier, the atomic verdict+resolved close, durable resume-by-replay, and
the Dung grounded-semantics labels. Every driver here is bounded and terminates; nothing touches
``Supervisor.run``.
"""

import pytest

from concursus.reasoning.trailstore import (
    HypothesisTrail,
    ThreadNotResolved,
    TrailStoreError,
    drive_deliberation,
    require_resolved,
)


# -- AI-23: fan-out + bounded open frontier ---------------------------------
def test_fanout_and_open_frontier_respects_depth_cap_and_excludes_resolved(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    roots = trail.fanout_root_hypotheses("why did the deploy fail?", ["net", "iam", "quota"])
    assert len(roots) == 3
    # All three roots are open (unresolved leaves under the caps).
    assert set(trail.open_frontier(roots[0], depth_cap=5)) == {roots[0]}
    # Fan children under one root; the parent is no longer a leaf, children become the frontier.
    kids = trail.fanout_hypotheses(roots[0], ["net.dns", "net.tls"])
    frontier = trail.open_frontier(roots[0], depth_cap=5)
    assert set(frontier) == set(kids)
    assert roots[0] not in frontier  # internal node (has children) is not open

    # depth_cap=0 closes everything below the root; the root itself (depth 0) stays if a leaf.
    fresh = HypothesisTrail(tmp_path / "run2")
    r = fresh.fanout_root_hypotheses("g", ["only"])
    deep = fresh.fanout_hypotheses(r[0], ["child"])  # depth 1
    assert fresh.open_frontier(r[0], depth_cap=0) == []  # child at depth 1 > cap 0 → excluded
    assert set(fresh.open_frontier(r[0], depth_cap=1)) == set(deep)

    # A resolved leaf is excluded from the frontier.
    trail.write_verdict(kids[0], "ACCEPT", {"log": "dns ok"})
    assert set(trail.open_frontier(roots[0], depth_cap=5)) == {kids[1]}


def test_confidence_floor_closes_confident_leaves(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    roots = trail.fanout_root_hypotheses(
        "g", [{"text": "sure", "confidence": 0.9}, {"text": "unsure", "confidence": 0.1}]
    )
    frontier = trail.open_frontier(roots[0], confidence_floor=0.6)
    # roots[0] is confident (0.9 ≥ floor) → closed; roots[1] queried separately below.
    assert frontier == []
    assert trail.open_frontier(roots[1], confidence_floor=0.6) == [roots[1]]


# -- AI-23: atomic verdict + resolved flip ----------------------------------
def test_write_verdict_atomically_appends_verdict_and_flips_resolved(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    roots = trail.fanout_root_hypotheses("g", ["h"])
    hid = roots[0]
    assert trail.hypotheses()[hid].resolved is False

    vid = trail.write_verdict(hid, "reject", {"evidence": "counterexample"})
    h = trail.hypotheses()[hid]
    assert h.resolved is True
    assert h.verdict == "REJECT"  # upper-cased
    assert h.evidence == {"evidence": "counterexample"}
    assert h.verdict_id == vid

    # A FRESH trail reloading the SAME .3 log never sees a verdict without its resolved marker.
    reloaded = HypothesisTrail(tmp_path / "run")
    rh = reloaded.hypotheses()[hid]
    assert rh.resolved is True and rh.verdict == "REJECT"
    # Both the verdict record and the resolved marker are present after one atomic flush.
    log_text = (tmp_path / "run" / ".3" / "trail.jsonl").read_text()
    assert '"kind": "verdict"' in log_text and '"kind": "resolved"' in log_text


def test_write_verdict_rejects_bad_verdict_and_unknown_id(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    roots = trail.fanout_root_hypotheses("g", ["h"])
    with pytest.raises(TrailStoreError):
        trail.write_verdict(roots[0], "MAYBE")
    with pytest.raises(TrailStoreError):
        trail.write_verdict(".3/nope", "ACCEPT")


# -- AI-26: Dung grounded semantics -----------------------------------------
def test_grounded_extension_chain_a_attacks_b_attacks_c(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    a, b, c = trail.fanout_root_hypotheses("g", ["A", "B", "C"])
    trail.attack(a, b)
    trail.attack(b, c)
    # Roots are separate trees; to put A, B, C in ONE argumentation framework, nest them under a
    # shared root subtree (attacks cross the tree freely). A unattacked → in; B attacked by in-A →
    # out; C attacked only by out-B → in.
    trail2 = HypothesisTrail(tmp_path / "run2")
    root = trail2.fanout_root_hypotheses("g", ["root"])[0]
    a2 = trail2.fanout_hypotheses(root, ["A"])[0]
    b2 = trail2.fanout_hypotheses(root, ["B"])[0]
    c2 = trail2.fanout_hypotheses(root, ["C"])[0]
    trail2.attack(a2, b2)
    trail2.attack(b2, c2)
    labels = trail2.compute_grounded_extension(root)
    assert labels[a2] == "in"
    assert labels[b2] == "out"
    assert labels[c2] == "in"
    assert labels[root] == "in"  # unattacked
    assert trail2.arg_label(b2) == "out"


def test_unattacked_is_in_and_mutual_pair_is_undec(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["root"])[0]
    lone = trail.fanout_hypotheses(root, ["lone"])[0]
    x = trail.fanout_hypotheses(root, ["X"])[0]
    y = trail.fanout_hypotheses(root, ["Y"])[0]
    trail.attack(x, y)
    trail.attack(y, x)  # mutual attack → neither can be grounded-in
    labels = trail.compute_grounded_extension(root)
    assert labels[lone] == "in"  # unattacked
    assert labels[x] == "undec"
    assert labels[y] == "undec"
    assert labels[root] == "in"


def test_self_attack_rejected(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    a = trail.fanout_root_hypotheses("g", ["A"])[0]
    with pytest.raises(TrailStoreError):
        trail.attack(a, a)


# -- termination guard + bounded driver (identity contract) -----------------
def test_require_resolved_raises_on_open_frontier_then_passes(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["h"])[0]
    with pytest.raises(ThreadNotResolved):
        require_resolved(trail, root)
    trail.write_verdict(root, "ACCEPT")
    require_resolved(trail, root)  # converged → no raise
    assert trail.open_frontier(root) == []


def test_drive_deliberation_is_bounded_and_terminates_with_stub_investigator(tmp_path):
    """A stub investigator (no LLM/LangGraph) drives the bounded loop to convergence."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["h"])[0]

    calls = {"n": 0}

    def investigator(h):
        calls["n"] += 1
        # Fan once under a shallow node, then accept everything → guaranteed termination.
        if h.depth < 1:
            return ["sharper-1", "sharper-2"]
        return {"verdict": "ACCEPT", "evidence": {"seen": h.text}}

    rounds = drive_deliberation(trail, root, investigator, max_rounds=8)
    assert rounds >= 1
    assert trail.open_frontier(root) == []  # converged within the budget
    require_resolved(trail, root)


def test_drive_deliberation_respects_max_rounds_budget(tmp_path):
    """An investigator that never resolves is capped by max_rounds (no unbounded expansion)."""
    trail = HypothesisTrail(tmp_path / "run")
    root = trail.fanout_root_hypotheses("g", ["h"])[0]

    def never_resolves(h):
        return ["more"]  # always fans a child, never a verdict

    rounds = drive_deliberation(trail, root, never_resolves, max_rounds=3, depth_cap=100)
    assert rounds == 3  # hard budget hit, loop terminated


# -- durability: resume by replay -------------------------------------------
def test_resume_by_replay_across_fresh_trail(tmp_path):
    trail = HypothesisTrail(tmp_path / "run")
    roots = trail.fanout_root_hypotheses("goal", ["a", "b"])
    kids = trail.fanout_hypotheses(roots[0], ["a1"])
    trail.attack(roots[1], roots[0])
    trail.write_verdict(kids[0], "UNDEC")

    fresh = HypothesisTrail(tmp_path / "run")
    model = fresh.hypotheses()
    assert set(model) == set(roots) | set(kids)
    assert model[roots[0]].children == kids
    assert roots[0] in model[roots[1]].attacks
    assert model[kids[0]].resolved and model[kids[0]].verdict == "UNDEC"


def test_from_config_binds_same_run_dir_as_filevault(tmp_path):
    from concursus.state.filevault import FileVaultStateStore

    store = FileVaultStateStore.from_config(vault_path=tmp_path, session_id="ticket-42")
    trail = HypothesisTrail.from_config(vault_path=tmp_path, session_id="ticket-42")
    assert trail.branch_dir.parent == store.run_dir
    assert trail.branch_dir.name == ".3"
