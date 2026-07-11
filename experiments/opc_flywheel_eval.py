"""OPC flywheel evaluation — does concursus's precedent loop *compound*?

FZ 35e8 (the OPC gap review) found that concursus builds every link of the One-Person-Company
compounding flywheel — run -> distill precedent -> retrieve at next plan-formation -> cheaper /
better next plan — but that the loop had never been *spun and measured* on a real corpus. This
harness spins it and measures it, honestly separating two claims:

  LAYER A (MEASURED, no LLM, no modelling): as the precedent corpus grows, does the real
    ``PrecedentRetriever`` (the shipped StructuredKey->Lexical ladder) increasingly surface a
    *same-family* prior for a new program-formation goal? This is a pure function of the shipped
    retriever over a growing store — zero assumptions.

  LAYER B (MODELLED, explicit investigator policy): given the retrieval result, does plan-formation
    cost (investigator invocations, an LLM-call proxy) fall? This depends on the INJECTED
    investigator's policy — which concursus does NOT itself provide (it is the LLM's job). We run
    two explicit, self-contained policies to bound the answer:
      * BLIND   — ignores precedents: every fresh goal is decomposed from scratch; a seeded
                  precedent root is merely accepted. (Models an investigator that does not reuse.)
      * EXPLOIT — reuses a *correct* (same-family) retrieved prior: accepts the goal directly from
                  the prior decomposition instead of re-deriving it.
    The realism of EXPLOIT is gated by LAYER A: a precedent only helps on goals where retrieval
    actually surfaced a same-family one.

The honest structural finding this makes visible: ``deliberate.seed()`` *appends* retrieved
precedents as extra root hypotheses rather than pruning the search — so under the BLIND policy a
warm start costs MORE than a cold one (extra roots, no work removed). Compounding is therefore
CONTINGENT on an EXPLOIT-style investigator, not automatic from the wiring.

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
# Five BAP-flavoured program families, each with a DISTINCT canonical decomposition + vocabulary,
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


def make_exploit_investigator(has_correct_precedent: bool):
    """EXPLOIT: reuses a *correct* same-family prior. When retrieval surfaced a same-family
    precedent (``has_correct_precedent``), the goal root is ACCEPTED directly (reuse the prior
    decomposition, 1 call) instead of being re-derived; otherwise it falls back to BLIND
    decomposition. A precedent root is accepted; leaves are accepted."""

    def policy(h):
        text = h.text or ""
        if text.startswith("Approach:"):
            if has_correct_precedent:
                return {"verdict": "ACCEPT", "evidence": {"reason": "exploit: reuse prior decomposition"}}
            fam = _family_of_goal(text)
            return [{"text": s.replace("_", " "), "confidence": 0.0} for s in FAMILIES[fam]["steps"]]
        return {"verdict": "ACCEPT", "evidence": {"reason": "exploit: accept leaf/precedent"}}

    return policy


def _family_of_goal(text: str) -> str:
    up = text.upper()
    for fam in FAMILIES:
        if f" {fam.upper()} " in up or f"({fam.upper()}" in up:
            return fam
    return "dnr"


# --------------------------------------------------------------------------- one plan-formation
def form_and_count(vault: Path, goal: str, *, retriever, policy_factory, has_precedent):
    """Run form_plan for one goal with a counting investigator; return (calls, plan_node_count)."""
    run_dir = Path(tempfile.mkdtemp(prefix="fp_", dir=vault))
    trail = HypothesisTrail(run_dir)
    policy = policy_factory(has_precedent) if policy_factory is make_exploit_investigator else policy_factory
    inv, box = _counting(policy)
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

            # -- LAYER B (modelled): plan-formation cost cold vs warm-blind vs warm-exploit --------
            cold_calls, cold_nodes = form_and_count(
                vault, goal, retriever=None, policy_factory=blind_investigator, has_precedent=False)
            warm_blind_calls, wb_nodes = form_and_count(
                vault, goal, retriever=retriever, policy_factory=blind_investigator, has_precedent=False)
            warm_exploit_calls, we_nodes = form_and_count(
                vault, goal, retriever=retriever, policy_factory=make_exploit_investigator,
                has_precedent=top1_same)  # exploit only helps when retrieval was CORRECT

            per_goal.append({
                "round": r, "family": family, "mkt": mkt,
                "same_family_priors": r,
                "corpus_size": _corpus_size(vault),
                "retrieval_top1_same_family": top1_same,
                "retrieval_top3_same_family": top3_same,
                "cold_calls": cold_calls,
                "warm_blind_calls": warm_blind_calls,
                "warm_exploit_calls": warm_exploit_calls,
                "cold_nodes": cold_nodes, "exploit_nodes": we_nodes,
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
            "mean_warm_blind_calls": round(sum(x["warm_blind_calls"] for x in rs) / n, 2),
            "mean_warm_exploit_calls": round(sum(x["warm_exploit_calls"] for x in rs) / n, 2),
        })
    return out


def _print(summary, dest):
    print("\n=== OPC FLYWHEEL EVALUATION ===\n")
    print("LAYER A (measured) — retrieval hit-rate vs corpus growth:")
    print(f"{'round':>5} {'priors':>7} {'top1_hit':>9} {'top3_hit':>9}")
    for s in summary:
        print(f"{s['round']:>5} {s['same_family_priors']:>7} "
              f"{s['retrieval_top1_hit_rate']:>9} {s['retrieval_top3_hit_rate']:>9}")
    print("\nLAYER B (modelled) — plan-formation cost (investigator calls; LLM-call proxy):")
    print(f"{'round':>5} {'cold':>6} {'warm_blind':>11} {'warm_exploit':>13}")
    for s in summary:
        print(f"{s['round']:>5} {s['mean_cold_calls']:>6} "
              f"{s['mean_warm_blind_calls']:>11} {s['mean_warm_exploit_calls']:>13}")
    print(f"\nresults -> {dest}\n")


if __name__ == "__main__":
    run()
