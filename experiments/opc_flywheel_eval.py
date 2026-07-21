"""OPC flywheel evaluation — does concursus's precedent loop *compound*?

The OPC gap review found that concursus builds every link of the One-Person-Company
compounding flywheel — run -> distill precedent -> retrieve at next plan-formation -> cheaper /
better next plan — but that the loop had never been *spun and measured* on a real corpus. This
harness spins it and measures it, honestly separating two claims:

  LAYER A (MEASURED, no LLM, no modelling): as the precedent corpus grows, does the real
    ``PrecedentRetriever`` (the shipped StructuredKey->Lexical ladder) increasingly surface a
    *same-family* prior for a new program-formation goal? This is a pure function of the shipped
    retriever over a growing store — zero assumptions.

  LAYER B (MEASURED, no LLM): given the retrieval result, does plan-formation cost (investigator
    invocations, an LLM-call proxy) fall — with the SAME plain investigator cold and warm? After
    the prune-and-replace ``deliberate.seed()`` fix, a strong retrieved precedent seeds the goal
    root PRE-DECOMPOSED (its prior steps become confident children the frontier excludes), so the
    investigator is simply never asked about the reused steps. The warm-vs-cold drop is therefore
    STRUCTURAL (seed prunes the frontier), not a smarter investigator — so both arms run the same
    ``blind_investigator`` and the difference is entirely the flywheel.

History: the first run of this harness found that the OLD
``seed()`` *appended* retrieved precedents as extra root hypotheses rather than pruning the search,
so a warm start cost MORE than a cold one (+1 call vs cold; only 38.5% cheaper than an ignoring
investigator). That was the named gap. The fix made reuse STRUCTURAL at seed time; this harness now
measures the resolved loop — warm should drop BELOW cold once a same-family precedent exists.

Pure stdlib + concursus. Deterministic (no RNG). Writes results JSON next to this file and prints
a summary table. Run: PYTHONPATH=src python3.11 experiments/opc_flywheel_eval.py
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from concursus import PrecedentRetriever, distill_run, form_plan
from concursus.reasoning.trailstore import HypothesisTrail
from concursus.state.statestore import Record

# --------------------------------------------------------------------------- the corpus
# Five abuse-prevention-flavoured program families, each with a DISTINCT canonical decomposition + vocabulary,
# so a same-family prior is lexically distinguishable from a cross-family one. Each family is
# launched across the same marketplaces (the "instances"): a realistic 1:N program portfolio.
FAMILIES = {
    "dnr": {
        "domain": "delivered not received claim abuse",
        "steps": ["ingest_dnr_claims", "detect_dnr_pattern", "triage_dnr_case",
                  "escalate_dnr_ring", "calibrate_dnr_threshold"],
    },
    "pda": {
        "domain": "policy driven abuse concession leakage",
        "steps": ["ingest_pda_events", "detect_pda_signal", "triage_pda_concession",
                  "escalate_pda_repeat", "calibrate_pda_precision"],
    },
    "mdr": {
        "domain": "material damaged received return fraud",
        "steps": ["ingest_mdr_returns", "detect_mdr_damage", "triage_mdr_claim",
                  "escalate_mdr_seller", "calibrate_mdr_recall"],
    },
    "flr": {
        "domain": "first loss reimbursement friction tuning",
        "steps": ["ingest_flr_reimbursements", "detect_flr_anomaly", "triage_flr_queue",
                  "escalate_flr_bulk", "calibrate_flr_friction"],
    },
    "ato": {
        "domain": "account takeover identity compromise",
        "steps": ["ingest_ato_signals", "detect_ato_login", "triage_ato_session",
                  "escalate_ato_fleet", "calibrate_ato_risk"],
    },
}
MARKETPLACES = ["us", "uk", "de", "jp", "in", "ca"]  # 6 instances per family


def goal_text(family: str, mkt: str) -> str:
    """The program-formation goal a director hands the planner — carries the family vocabulary."""
    f = FAMILIES[family]
    steps = " ".join(f["steps"])
    return (
        f"Launch a {family.upper()} ({f['domain']}) abuse-prevention program for marketplace "
        f"{mkt.upper()}: {steps.replace('_', ' ')}."
    )


def synth_records(family: str) -> tuple[dict, list]:
    """A finished run's (result, records) for one family instance — the family's canonical steps,
    chained producer->consumer, so the distilled precedent is retrievable by the family vocabulary."""
    steps = FAMILIES[family]["steps"]
    records, result = [], {}
    for i, step in enumerate(steps):
        consumes = [f"{steps[i-1]}:$.out"] if i > 0 else []
        records.append(
            Record(node=step, output={"out": f"{step}_ok"}, attempt=1, status="validated",
                   producer=step, consumes=consumes)
        )
        result[step] = {"out": f"{step}_ok"}
    return result, records


# --------------------------------------------------------------------------- investigator policies
def _counting(policy):
    """Wrap an investigator so we can count invocations (the LLM-call proxy cost)."""
    box = {"n": 0}

    def wrapped(h):
        box["n"] += 1
        return policy(h)

    return wrapped, box


def blind_investigator(h):
    """BLIND: ignores precedents. A fresh goal root is decomposed from scratch (fan out the family
    canonical steps); a precedent root is merely accepted; leaves are accepted."""
    text = h.text or ""
    if text.startswith("Approach:"):
        # Re-derive the decomposition from scratch — the family steps as children.
        fam = _family_of_goal(text)
        return [{"text": s.replace("_", " "), "confidence": 0.0} for s in FAMILIES[fam]["steps"]]
    return {"verdict": "ACCEPT", "evidence": {"reason": "blind: accept leaf/precedent"}}


def _family_of_goal(text: str) -> str:
    up = text.upper()
    for fam in FAMILIES:
        if f" {fam.upper()} " in up or f"({fam.upper()}" in up:
            return fam
    return "dnr"


# --------------------------------------------------------------------------- one plan-formation
def form_and_count(vault: Path, goal: str, *, retriever, policy_factory, has_precedent=False):
    """Run form_plan for one goal with a counting investigator; return (calls, plan_node_count).

    The SAME plain ``blind_investigator`` is used cold and warm: with the prune-and-replace seed()
    fix, a strong retrieved precedent seeds the goal pre-decomposed (confident children excluded
    from the frontier), so the investigator is simply never asked about the reused steps — the
    warm-vs-cold drop is structural, not a smarter investigator."""
    run_dir = Path(tempfile.mkdtemp(prefix="fp_", dir=vault))
    trail = HypothesisTrail(run_dir)
    inv, box = _counting(policy_factory)
    dag = form_plan(trail, goal, retriever=retriever, investigator=inv, max_rounds=6, depth_cap=4)
    shutil.rmtree(run_dir, ignore_errors=True)
    return box["n"], len(dag.nodes)


# --------------------------------------------------------------------------- the experiment
def run():
    vault = Path(tempfile.mkdtemp(prefix="opc_flywheel_"))
    retriever = PrecedentRetriever(vault_path=vault, limit=3)
    per_goal = []

    # Marketplace-major rounds: at round r, every family already has exactly r same-family priors
    # in the store (rounds 0..r-1). Corpus grows uniformly; same-family availability = r.
    for r, mkt in enumerate(MARKETPLACES):
        for family in FAMILIES:
            goal = goal_text(family, mkt)

            # -- LAYER A (measured): does retrieval surface a same-family prior? ------------------
            hits = retriever.retrieve(goal, limit=3)
            top1_family = _prefix_family(hits[0].trail_id) if hits else None
            top3_families = [_prefix_family(h.trail_id) for h in hits]
            top1_same = top1_family == family
            top3_same = family in top3_families
            corpus_size = len(hits) and _corpus_size(vault)

            # -- LAYER B (measured): plan-formation cost, cold vs warm, SAME plain investigator ----
            # After the prune-and-replace seed() fix, reuse is STRUCTURAL — a strong precedent seeds
            # the goal pre-decomposed, so warm needs no special investigator. warm_reuse uses the
            # SAME blind investigator as cold; the drop is entirely from seed() pruning the frontier.
            cold_calls, cold_nodes = form_and_count(
                vault, goal, retriever=None, policy_factory=blind_investigator, has_precedent=False)
            warm_reuse_calls, wr_nodes = form_and_count(
                vault, goal, retriever=retriever, policy_factory=blind_investigator, has_precedent=False)

            per_goal.append({
                "round": r, "family": family, "mkt": mkt,
                "same_family_priors": r,
                "corpus_size": _corpus_size(vault),
                "retrieval_top1_same_family": top1_same,
                "retrieval_top3_same_family": top3_same,
                "cold_calls": cold_calls,
                "warm_reuse_calls": warm_reuse_calls,
                "cold_nodes": cold_nodes, "warm_nodes": wr_nodes,
            })

            # -- grow the corpus: distill THIS goal's finished run into the precedent store --------
            result, records = synth_records(family)
            distill_run(result, records, vault_path=vault, trail_id=f"{family}_{mkt}")

    summary = _summarize(per_goal)
    out = {"families": list(FAMILIES), "marketplaces": MARKETPLACES,
           "n_goals": len(per_goal), "per_round": summary, "per_goal": per_goal}
    dest = Path(__file__).with_name("opc_flywheel_results.json")
    dest.write_text(json.dumps(out, indent=2))
    shutil.rmtree(vault, ignore_errors=True)
    _print(summary, dest)
    return out


def _prefix_family(trail_id: str) -> str:
    return (trail_id or "").split("_", 1)[0]


def _corpus_size(vault: Path) -> int:
    d = vault / "precedents"
    return len([p for p in d.glob("*.md")]) if d.exists() else 0


def _summarize(rows):
    by_round = {}
    for row in rows:
        by_round.setdefault(row["round"], []).append(row)
    out = []
    for r in sorted(by_round):
        rs = by_round[r]
        n = len(rs)
        out.append({
            "round": r,
            "same_family_priors": r,
            "n_goals": n,
            "retrieval_top1_hit_rate": round(sum(x["retrieval_top1_same_family"] for x in rs) / n, 3),
            "retrieval_top3_hit_rate": round(sum(x["retrieval_top3_same_family"] for x in rs) / n, 3),
            "mean_cold_calls": round(sum(x["cold_calls"] for x in rs) / n, 2),
            "mean_warm_reuse_calls": round(sum(x["warm_reuse_calls"] for x in rs) / n, 2),
        })
    return out


def _print(summary, dest):
    print("\n=== OPC FLYWHEEL EVALUATION (post prune-and-replace seed fix) ===\n")
    print("LAYER A (measured) — retrieval hit-rate vs corpus growth:")
    print(f"{'round':>5} {'priors':>7} {'top1_hit':>9} {'top3_hit':>9}")
    for s in summary:
        print(f"{s['round']:>5} {s['same_family_priors']:>7} "
              f"{s['retrieval_top1_hit_rate']:>9} {s['retrieval_top3_hit_rate']:>9}")
    print("\nLAYER B (measured) — plan-formation cost, SAME plain investigator (LLM-call proxy):")
    print(f"{'round':>5} {'cold':>6} {'warm_reuse':>11} {'delta':>8}")
    for s in summary:
        delta = round(s['mean_warm_reuse_calls'] - s['mean_cold_calls'], 2)
        print(f"{s['round']:>5} {s['mean_cold_calls']:>6} "
              f"{s['mean_warm_reuse_calls']:>11} {delta:>+8}")
    print(f"\nresults -> {dest}\n")


if __name__ == "__main__":
    run()
