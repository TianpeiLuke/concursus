# concursus/experiments

Reproducible evaluations of concursus's *designed* claims — the ones that unit tests can't settle
because they are about emergent behavior over a corpus, not per-function correctness. These are
**not** part of the test suite (they live outside `tests/` and are not collected by pytest); each
is a standalone script that prints a summary and writes a `*_results.json` record.

## `opc_flywheel_eval.py` — does the precedent loop *compound*?

Spins concursus's One-Person-Company flywheel (run → `distill` precedent → `retrieve` at next
`form_plan` → cheaper/better next plan) over a family-structured corpus of program-formation goals
and measures two layers:

- **Layer A (measured, no LLM):** as the precedent corpus grows, does the shipped
  `PrecedentRetriever` surface a *same-family* prior for a new goal? — top-1 recall jumps **0 → 1.0**
  the moment one same-family prior exists.
- **Layer B (modelled, explicit investigator policy):** does that reduce plan-formation cost
  (investigator calls = LLM-call proxy)? Compares **cold** (empty store) vs **warm-blind** (ignores
  precedents) vs **warm-exploit** (reuses a correct same-family prior). Finding: exploit is **38.5%
  cheaper than blind** but **+1 call vs cold**, because `deliberate.seed()` *appends* a precedent
  root rather than *pruning* the from-scratch goal root — a fixable wiring gap, not a fundamental one.

Run:

```bash
PYTHONPATH=src python3.11 experiments/opc_flywheel_eval.py
```

Deterministic (no RNG). Full analysis: the Abuse SlipBox note **FZ 35e8a**
(`thought_concursus_opc_flywheel_experiment`).
