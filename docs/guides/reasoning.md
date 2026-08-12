# Guide: The Reasoning Tier (DKS Deliberation)

*Form a plan by bounded deliberation, then lower it to a frozen `AgentDAG` — strictly before compile.*

The reasoning tier is Concursus's opt-in plan-formation **front**. Rather than hand-code the
`AgentDAG` you feed the compiler, you can *deliberate* one: seed hypotheses, fan out investigators,
argue them to a verdict, and lower the surviving conclusions into an immutable DAG. Everything in
this tier is pure stdlib — it runs end-to-end with **neither LangGraph nor any LLM installed** —
and it is entirely optional: the compiler and its full test suite never require it.

The load-bearing rule this tier respects: **Concursus is a compiler, not a runtime governor.** A
run is `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run`, a single forward pass over
an immutable plan. The reasoning tier lives **strictly before `assemble`**. It is never wired into
`Supervisor.run`, every one of its loops is bounded and must terminate (an empty hypothesis
frontier) before its result may be lowered, and lowering produces a fresh frozen DAG — never a
mutation of a live plan. Re-opening a debate is a *new* formation episode, not a mid-flight re-plan.

```text
goal / ticket
     │
     ▼   reasoning tier  (this guide — bounded, terminates)
  seed ──▶ form_plan loop ──▶ lower_to_dag
     │        (DKS + inner graph over the .3 trail)
     ▼
 frozen AgentDAG ──▶ assemble ──▶ ProvisioningPlan ──▶ Supervisor.run
                     └──────────── the compiler (never re-plans) ────────────┘
```

## The four modules

The tier stacks bottom-up. Each module is a source file under
[`../../src/concursus/reasoning/`](../../src/concursus/reasoning/); every symbol in the *Key symbols*
column below is re-exported from the top-level `concursus` package, so examples import from there.

| Module | Role | Key symbols |
|---|---|---|
| [`trailstore.py`](../../src/concursus/reasoning/trailstore.py) | The durable `.3` reasoning-branch store — a replayable tree of hypotheses closed by verdicts, plus Dung grounded-semantics labels. | `HypothesisTrail`, `Hypothesis`, `require_resolved`, `drive_deliberation`, `TrailStoreError`, `ThreadNotResolved` |
| [`dks_engine.py`](../../src/concursus/reasoning/dks_engine.py) | The bounded deliberation state machine — drives one trail to convergence, scoring/routing via the Confidence-Coherence Score. | `DKSEngine`, `DKSState`, `DKSResult`, `compute_ccs`, `CCSWeights`, `route_by_confidence`, `DKSEngineError` |
| [`inner_graph.py`](../../src/concursus/reasoning/inner_graph.py) | Parallel per-round fan-out — one injected investigator per open hypothesis, digested to the `.2` worker lane. | `compile_inner_graph`, `dispatch_frontier`, `partition_frontier`, `InnerGraph`, `InvestigationResult`, `InnerGraphDigest`, `InnerGraphError` |
| [`deliberate.py`](../../src/concursus/reasoning/deliberate.py) | The top-level driver — `seed → form_plan → lower_to_dag`. | `seed`, `form_plan`, `lower_to_dag` |

The full symbol catalog is in the [reasoning API reference](../reference/reasoning.md).

---

## HypothesisTrail — the durable `.3` substrate

A [`HypothesisTrail`](../../src/concursus/reasoning/trailstore.py) is a durable, replayable
deliberation over a run's `.3` reasoning branch. It records a *tree of hypotheses* — each closed by
a verdict — into an append-only JSONL log, and computes Dung grounded-semantics labels over an
attack graph. It dispatches nothing; it is pure plan-formation.

It binds to the same run directory the durable [`StateStore`](durable-state.md) uses, so a run's
`.1`/`.2` state notes and its `.3` reasoning branch live under one directory:

```python
from concursus import HypothesisTrail

# Bind to <vault>/runs/<slug(session_id)>/.3/ — the same run dir a FileVaultStateStore uses.
trail = HypothesisTrail.from_config(vault_path="/vault", session_id="ticket-42")
# Or construct directly over a run dir:
trail = HypothesisTrail("/vault/runs/ticket_42")          # branch defaults to ".3"
```

### The `Hypothesis` node

`trail.hypotheses(root=None)` rebuilds and returns the tree as `{id: Hypothesis}` (a `root` id
returns just that subtree, inclusive). Verdicts are **attributes on the hypothesis**, not separate
nodes:

| Field | Meaning |
|---|---|
| `id` | Materialized-path address (`.3/h1`, `.3/h1/c4`); the parent is the id minus its last segment. |
| `parent` | Parent hypothesis id (`None` for a root). |
| `text` | The hypothesis statement. |
| `confidence` | `[0, 1]` self-confidence; at/above `confidence_floor` a leaf is treated as closed. |
| `depth` | Distance from the root (roots are `0`). |
| `goal` | The seeding goal (roots only). |
| `resolved` / `verdict` / `evidence` / `verdict_id` | Populated once a verdict closes the hypothesis (`verdict` is `ACCEPT` \| `REJECT` \| `UNDEC`). |
| `children` / `attacks` | Ids of fanned children, and ids this hypothesis attacks (Dung edges out). |

`Hypothesis` is a plain (non-frozen) dataclass rebuilt on every read — never hold a reference
expecting it to reflect later mutations; re-read via `hypotheses()`.

### Building and closing a deliberation

```python
from concursus import HypothesisTrail, require_resolved

trail = HypothesisTrail.from_config(vault_path="/vault", session_id="ticket-42")

# SEED root hypotheses — one per candidate (bare text or {"text", "confidence"}).
roots = trail.fanout_root_hypotheses(
    "fix outage",
    [{"text": "rollback", "confidence": 0.3}, "scale up"],
)

# Fan sharper child hypotheses under a root (bounded: depth_cap caps depth, callers cap breadth).
kids = trail.fanout_hypotheses(roots[0], ["revert deploy", "flush cache"])

# Record a directed contradiction (a Dung attack edge; `contradicts` is an alias).
trail.attack(kids[0], kids[1])                       # kids[0] contradicts kids[1]

# Close a hypothesis: appends a VERDICT child AND flips its RESOLVED marker in ONE atomic
# file replace, so a concurrent scan never sees a verdict without its resolved marker.
vid = trail.write_verdict(kids[0], "ACCEPT", {"log": "reverted cleanly"})

# The open frontier: un-resolved leaves within the caps. [] means the debate has CONVERGED.
open_leaves = trail.open_frontier(roots[0], depth_cap=5, confidence_floor=0.6)

# The Dung grounded labels {id -> in|out|undec} over the root's subtree.
labels = trail.compute_grounded_extension(roots[0])  # {'.3/h1/c1': 'in', ...}

# The termination guard: raises ThreadNotResolved if any frontier leaf is still open.
require_resolved(trail, roots[0])
```

Method signatures at a glance:

| Method | Signature | Notes |
|---|---|---|
| `fanout_root_hypotheses` | `(goal, candidates) -> List[str]` | Roots at depth 0, parent `None`; SEED is a new-goal action, never a retrieval query. |
| `fanout_hypotheses` | `(parent_id, children) -> List[str]` | Children at `parent.depth + 1`; unknown parent raises `TrailStoreError`. |
| `open_frontier` | `(root, *, depth_cap=5, confidence_floor=0.6) -> List[str]` | A leaf is **closed** if resolved, has children, `depth > depth_cap`, **or** `confidence >= confidence_floor`. |
| `write_verdict` | `(id, verdict, evidence=None) -> str` | `verdict` is upper-cased then validated against `ACCEPT` \| `REJECT` \| `UNDEC`; returns the verdict-child id. |
| `hypotheses` | `(root=None) -> Dict[str, Hypothesis]` | Full tree or one subtree; rebuilt by replay each call. |
| `attack` / `contradicts` | `(attacker_id, target_id) -> None` | Directed; idempotent; self-attack raises `TrailStoreError`. |
| `compute_grounded_extension` | `(root) -> Dict[str, str]` | Least fixed point: `in` = all attackers `out` (vacuous when unattacked), `out` = attacked by an `in`, else `undec`. |
| `arg_label` | `(id) -> str` | The grounded label of one hypothesis. |
| `branch_dir` | property `-> Path` | The on-disk `.3` directory (`run_dir/branch`). |

> **Convergence and the confidence floor.** `open_frontier` closes a leaf whose `confidence >=
> confidence_floor` **even without a verdict**. This is intentional: high-confidence seeding (as in
> `seed`'s precedent reuse) empties the frontier immediately, skipping re-investigation.

### Bounded, pure-Python driving: `drive_deliberation` and `require_resolved`

`require_resolved(trail, root, *, depth_cap=5, confidence_floor=0.6)` raises
[`ThreadNotResolved`](../../src/concursus/reasoning/trailstore.py) (a subclass of
`TrailStoreError`, itself a `ValueError`) when the open frontier is non-empty — the guard a later
LOWER step must call before distilling a debate into a DAG.

`drive_deliberation` is a leaner sibling of the DKS engine's loop — a bounded, pure-Python driver
over an injected `investigator` seam, useful for tests and stub-driven flows:

```python
from concursus import drive_deliberation

def investigator(h):
    # Return a verdict spec (closes the hypothesis) ...
    return {"verdict": "ACCEPT", "evidence": {"reason": "looks right"}}
    # ... or a truthy list of child candidates (fans sharper children).

rounds = drive_deliberation(trail, roots[0], investigator, max_rounds=8)
# Each round resolves every open-frontier leaf until the frontier empties or max_rounds is spent.
# A falsy/empty investigator result closes the leaf UNDEC so the loop always progresses.
```

Every mutation appends a record and rewrites `<run_dir>/.3/trail.jsonl` atomically (temp file +
`os.replace`), so a verdict and its resolved marker land in a single indivisible swap. A fresh
trail over an existing `.3` branch reloads by replay — a deliberation survives process exit.

---

## DKSEngine — bounded deliberation to convergence

[`DKSEngine`](../../src/concursus/reasoning/dks_engine.py) is the cyclic state machine that
drives one `HypothesisTrail` to convergence. It walks the eight-step chain

```python
DKS_NODES = ("observe", "name", "structure", "operationalize",
             "test", "challenge", "improve", "compile")
```

with a confidence-gated loop-back from `compile` to `observe`. The loop is **bounded**
(`max_rounds` / `depth_cap` / `confidence_floor`) and **terminates** when the trail's open frontier
empties or the round budget is spent. It writes `.3` verdicts (via `write_verdict`) and attack
edges (via `attack`, for MOOG counters) but never dispatches an agent and is never wired into
`Supervisor.run`.

```python
from concursus import DKSEngine

# All heavy work is injected; unsupplied seams default to a deterministic stub / heuristic / no-op.
engine = DKSEngine(trail, max_rounds=8, depth_cap=5, confidence_floor=0.6)

result = engine.run(roots[0])     # -> DKSResult
result.converged                  # True once the open frontier is empty
result.resolved                   # alias for .converged
result.backend                    # 'langgraph' or 'python'
result.trace                      # ordered list of node names executed
result.state                      # the final DKSState snapshot

engine.lower_guard(roots[0])      # raises ThreadNotResolved if the debate is still open
```

Constructor:

```python
DKSEngine(
    trail,
    *,
    investigator=None,          # Callable[[Hypothesis], object]; defaults to a deterministic UNDEC stub
    policy=None,                # Callable[[float, DKSState|None], str]; overrides the heuristic gate
    counter_argument_fn=None,   # MOOG counter seam on CHALLENGE; defaults to a no-op
    weights=CCSWeights(),
    max_rounds=8,               # must be >= 1
    depth_cap=5,
    confidence_floor=0.6,
    backend="auto",             # "auto" | "python" | "langgraph"
)
```

> **The default investigator closes everything `UNDEC`,** so a stock `DKSEngine.run` converges in a
> single round. Real reasoning **requires** injecting an `investigator` — a callable that, given a
> `Hypothesis`, returns either a verdict spec `{"verdict": ..., "evidence": {...}}` (which closes it)
> or a list of child candidates (which fans sharper children). This is the same seam shape the inner
> graph and `form_plan` use.

`DKSState` is a compact (~1 KB) serializable snapshot a routing/RL policy can observe — node count,
Dung label fractions, calibration, per-verdict rule quality, plus round/frontier bookkeeping. The
durable deliberation lives in the trail; `DKSState` is just a pointer. `DKSEngineError` (a
`ValueError`) is raised on an invalid backend name, `max_rounds < 1`, a `backend="langgraph"`
request with LangGraph missing, or an injected policy returning an unknown band.

### The Confidence-Coherence Score (CCS) and the routing gate

`compute_ccs` scores a hypothesis as a convex combination; `route_by_confidence` maps that score to
a band. Both are pure, planning-time functions — they never re-route a committed plan.

```python
from concursus import compute_ccs, route_by_confidence, CCSWeights

# CCS = alpha*llm_conf + beta*homophily + gamma*coherence; inputs clamped to [0, 1].
score = compute_ccs(0.9, 0.5, 0.8)              # weights=CCSWeights() by default
band = route_by_confidence(score)               # -> one of the three bands below
```

`CCSWeights` is a frozen dataclass with defaults `alpha=0.5` (self/LLM confidence, weighted most
heavily), `beta=0.25` (homophily — agreement with same-label grounded neighbours), and `gamma=0.25`
(coherence — how few `undec` labels remain).

| Band constant | Value | CCS range | Meaning |
|---|---|---|---|
| `BAND_AUTO_ACCEPT` | `"auto_accept"` | `>= 0.85` | single-agent auto-accept |
| `BAND_ARGUE_COUNTER` | `"argue_counter"` | `0.50 <= CCS < 0.85` | two-agent argue + counter — the only band that invokes the MOOG `counter_argument_fn` on `challenge` |
| `BAND_ESCALATE` | `"escalate"` | `< 0.50` | human escalation |

An injected `policy=` fully overrides the heuristic, but its returned band is validated against
these three known bands — a rogue policy raises `DKSEngineError` rather than injecting an unknown
route.

### LangGraph is an optional, injected driver

LangGraph is an **optional, lazily-imported** backend, built inside the engine only when
`backend` is `"auto"` or `"langgraph"`. Importing `concursus` never requires it. With
`backend="auto"` (the default), the engine tries LangGraph and **silently falls back** to the pure
Python driver if it is not importable (or if a LangGraph runtime invocation fails); `"python"`
forces the fallback; `"langgraph"` is strict and raises `DKSEngineError` when the extra is missing.
Both backends run the *same* node functions and the *same* routing. LangGraph ships in the optional
`[reasoning]` extra.

---

## InnerGraph — parallel investigator fan-out, digested to `.2`

[`inner_graph.py`](../../src/concursus/reasoning/inner_graph.py) is the parallel-dispatch
layer used inside a deliberation round. It is a **fresh, disposable per-round projection** of the
open frontier: it fans one injected investigator per open hypothesis through a bounded thread pool,
merges results order-insensitively, and digests each result to the `.2` worker-log lane. It never
writes a `.3` verdict — closing a hypothesis is the engine's job.

```python
from concursus import (
    compile_inner_graph,
    dispatch_frontier,
    partition_frontier,
    InnerGraphDigest,
)

# Snapshot the CURRENT open frontier into a fresh, disposable InnerGraph.
graph = compile_inner_graph(trail, roots[0], concurrency_ceiling=4)
graph.frontier          # the flat open frontier (all batches concatenated)
len(graph)              # total hypotheses across all batches

# The digest writes under run_dir/.2 — pass the RUN dir; it appends the lane (".2") itself.
digest = InnerGraphDigest(trail.branch_dir.parent)

# Run one investigator per open hypothesis, clamped to graph.ceiling, merged by hypothesis id.
results = dispatch_frontier(graph, investigator, digest=digest)   # {id: InvestigationResult}

# The bounded fan-out is just deterministic chunking:
partition_frontier([".3/h1", ".3/h2", ".3/h3"], 2)   # -> [['.3/h1', '.3/h2'], ['.3/h3']]
```

Key contracts:

- **Fan-out is bounded.** `partition_frontier` splits the frontier into batches each `<=` the
  ceiling; each batch runs in a `ThreadPoolExecutor` sized `min(ceiling, len(batch))`. A non-positive
  ceiling raises `InnerGraphError`. The ceiling itself is first clamped to the host's capacity — see
  [The fan-out ceiling clamp](#the-fan-out-ceiling-clamp-opt-in-tightening) below.
- **A worker failure is data, not control flow.** An investigator crash becomes an
  `InvestigationResult(ok=False, error="<ExcType>: <msg>")` — never a raised exception — so one
  crash never aborts the fan-out or the merge. `InvestigationResult.key()` is the idempotency key
  (explicit `dedup_key`, else `"<hypothesis_id>:<action>"`).
- **The merge is order-insensitive**, keyed by hypothesis id — which worker finishes first is
  irrelevant.
- **The `InnerGraph` is disposable** — rebuilt each round from the pre-commit mutable hypothesis set
  and discarded; it holds no reference to the durable trail or any committed plan, so it can never
  ossify into a cyclic executor.

`InnerGraphDigest` is confined to `.2`. Per new result it appends an append-only ACTION marker to
`.2/log_<k>.jsonl`, offloads the raw payload to `.2/raw/<slug>.json`, and writes a lean, greppable
slipbox-card RESULT at `.2/cards/<slug>.md` that references the offloaded payload (never inlined).
A retried `write_back` with the same dedup key is an idempotent no-op that survives process restart
(dedup keys are reloaded from the lane logs). Inspect it via `digest.markers()` and
`digest.seen_keys()`.

### The fan-out ceiling clamp (opt-in tightening)

*(Opt-in and default-off, additive — the default path is byte-for-byte unchanged.)* The
`concurrency_ceiling` you pass `compile_inner_graph` is a **soft preference**. Before it becomes the
partition width, `compile_inner_graph` clamps it against the host's capacity:

```python
# inner_graph.py — inside compile_inner_graph
ceiling = resolve_ceiling(concurrency_ceiling, _cpu_capacity())   # max(1, min(pref, cap))
```

Two constants set the shape of that clamp:

- `MAX_FANOUT_CAP = 64` — the **hard, preference-independent** upper bound on concurrent
  investigators. A caller's soft config can only *tighten* the fan-out below it; nothing can raise
  the fan-out above it.
- `_cpu_capacity()` = `max(1, min(os.cpu_count() or 1, MAX_FANOUT_CAP))` — the host's usable
  capacity (the CPU count, itself hard-capped by `MAX_FANOUT_CAP`). It is the `cap` argument to the
  clamp. (Module-private — a `None` `os.cpu_count()` degrades to `1`.)

`resolve_ceiling(pref, cap)` is just `max(1, min(pref, cap))`: the `min` lets a soft `pref` only
lower the effective width, the `max(1, …)` floor keeps the fan-out making progress even for a
degenerate `pref`/`cap`. Because both live in `inner_graph.py` and are not re-exported from the
top-level package, import them from the submodule:

```python
from concursus.reasoning.inner_graph import resolve_ceiling, MAX_FANOUT_CAP

resolve_ceiling(4, 8)                 # 4   — default ceiling on an 8-core cap, unchanged
resolve_ceiling(100, 8)               # 8   — a soft ask of 100 is TIGHTENED to the 8-core cap
resolve_ceiling(100, MAX_FANOUT_CAP)  # 64  — never exceeds the hard cap
resolve_ceiling(0, 8)                 # 1   — the max(1, …) floor keeps fan-out making progress
```

**Why it is opt-in / default-off.** The default ceiling is still `4` (`_DEFAULT_CEILING`), well
below the cap, so on any host with `>= 4` usable cores `resolve_ceiling(4, cap) == 4` — the default
dispatch path is **byte-for-byte unchanged**. The clamp only bites when a caller *opts into* a
larger `concurrency_ceiling` (or runs on a host with fewer than 4 usable cores). It is a
*compile-time* clamp on the partition width, not a runtime governor: `dispatch_frontier` still runs
one bounded, disposable pass — the [compiler-not-governor rule](#guide-the-reasoning-tier-dks-deliberation)
holds, this is just a safe upper bound on how wide one deliberation round may fan out.

For statically-bounding the fan-out of a *frozen plan's* topology (rather than a deliberation
round's dispatch width), see [`unroll_static_fanout`](#unroll_static_fanout--compile-time-static-fan-out-unroll)
below — the DAG-topology sibling of this per-round clamp.

---

## deliberate — seed → form_plan → lower_to_dag

[`deliberate.py`](../../src/concursus/reasoning/deliberate.py) ties the trail, the DKS
engine, the inner graph, and an optional compile-time precedent retriever into one driver that
**forms** a plan by deliberation and then **lowers** the converged conclusion into a frozen
[`AgentDAG`](../../src/concursus/core/dag.py). Its public surface is `seed`, `form_plan`,
`lower_to_dag`, and the opt-in
[`unroll_static_fanout`](#unroll_static_fanout--compile-time-static-fan-out-unroll) compile pass.

### `seed` — start a formation episode (reusing a precedent by prune-and-replace)

```python
seed(trail, goal, *, retriever=None, limit=3, reuse_threshold=0.6, confidence_floor=0.6) -> List[str]
```

`seed` starts a new plan-formation episode from a goal — **triggered by a goal/ticket only, never by
a retrieval query** (the retriever is a priming read, never itself the write trigger). It has two
modes:

- **Cold / weak precedent** (no retriever, or none clearing `reuse_threshold`): seed a single
  `{"text": "Approach: <goal>", "confidence": 0.0}` root for the investigator to decompose.
- **Warm reuse** (a retrieved precedent scores `>= reuse_threshold` and carries a decomposition):
  seed one goal root, then **fan the prior's steps as already-confident children** (confidence
  `max(confidence_floor, 0.6)`), so `open_frontier` immediately excludes them. This is
  **prune-and-replace, not append** — reusing the prior structure rather than re-deriving it, so a
  warm start is cheaper than a cold one.

```python
from concursus import seed

roots = seed(trail, "fix outage")                          # cold: one "Approach: fix outage" root
roots = seed(trail, "fix outage", retriever=precedent_ix)  # warm: reuse a >=0.6-scoring precedent
```

The retriever is duck-typed (a `.retrieve(goal, limit=...)` method returning hits with `.score` /
`.payload` / `.trail_id`) — the compile-time [`PrecedentRetriever`](durable-state.md) fits, but any
best-effort object does; a missing or misbehaving retriever simply yields a cold start.

### `form_plan` — the bounded driver loop

```python
form_plan(trail, goal, *, retriever=None, engine=None, investigator=None,
          max_rounds=8, depth_cap=5, confidence_floor=0.6,
          concurrency_ceiling=4, digest=None) -> AgentDAG
```

For each seeded root, `form_plan` runs the bounded **SEED → READ FRONTIER → DISPATCH (inner graph) →
DIGEST → VERDICT (DKS engine) → RE-READ** loop until the frontier empties or `max_rounds` is spent,
then folds every converged root into one DAG. All model/agent work enters through injected seams —
if no `engine` is supplied, one is built over `trail` with the given `investigator` and caps — so it
runs correct-but-inert with neither LangGraph nor any LLM.

```python
from concursus import HypothesisTrail, form_plan

trail = HypothesisTrail.from_config(vault_path="/vault", session_id="triage-42")

def investigator(h):
    # Resolve one open hypothesis: a verdict spec (closes it) ...
    return {"verdict": "ACCEPT", "evidence": {"reason": "accepted"}}
    # ... or a list of child candidates (fans sharper children).

dag = form_plan(trail, "resolve ticket triage-42", investigator=investigator)
# `dag` is a validated, immutable AgentDAG — hand it to OrchestrationAssembler.assemble.
```

If any root fails to converge within `max_rounds`, the closing lower step raises `ThreadNotResolved`
(via `require_resolved`) rather than silently emitting a partial plan. Passing your own `engine`
uses it for the verdict phase, so the `investigator`/`max_rounds`/`depth_cap`/`confidence_floor`
args then drive only `seed` / `open_frontier` / the inner-graph fan-out.

### `lower_to_dag` — the deterministic lowering

```python
lower_to_dag(trail, root, *, require_resolved_first=True,
             depth_cap=5, confidence_floor=0.6) -> AgentDAG
```

`lower_to_dag` is a **pure, deterministic, no-LLM fold** over a *converged* debate. The surviving
**`in`-labelled** hypotheses (from the Dung grounded extension) become the task decomposition — one
DAG node per accepted hypothesis, edged parent → child along the accepted sub-tree — while `out`
hypotheses are dropped as dead-ends. Accepted ids are sorted (materialized paths sort parents before
children) for deterministic node order, node names are derived from the sanitized hypothesis text
plus its address suffix, and the result is validated (acyclic) before it is returned.

```python
from concursus import lower_to_dag

dag = lower_to_dag(trail, roots[0])                         # raises ThreadNotResolved if still open
dag = lower_to_dag(trail, roots[0], require_resolved_first=False)  # fold whatever IN labels exist now
```

`require_resolved_first=True` (the default) is the only thing enforcing convergence — with it
`False`, lowering a live debate can silently produce an empty or partial DAG. A degenerate debate
that accepted nothing still yields a valid empty DAG.

### `unroll_static_fanout` — compile-time static fan-out unroll

*(Opt-in and default-off, additive.)* A lowered `AgentDAG` sometimes wants *N* identical
branches of one node — a map-style fan-out. `unroll_static_fanout` is a **compile-time** rewrite of
the DAG *before* `assemble` freezes it: given a **declared, data-independent** count `N`, it clones
the named base node into `N` frozen parallel branches plus a synthetic gather join.

```python
unroll_static_fanout(dag, unroll=None) -> AgentDAG
```

```python
from concursus.reasoning.deliberate import unroll_static_fanout

dag = unroll_static_fanout(dag)                 # no spec -> the SAME object, byte-for-byte unchanged
wide = unroll_static_fanout(dag, {"probe": 3})  # 3 frozen branches probe__fe0..2 + a probe__gather join
```

Given `unroll = {base_node: N}`, each named `base` is expanded (in this one compile pass) into `N`
clones under namespaced ids `f"{base}__fe{i}"` (`i` in `0..N-1`), plus a **scatter** (every upstream
producer of `base` fans its shared input to all `N` clones) and a **gather** join node
`f"{base}__gather"` onto which every original downstream consumer of `base` is re-pointed. The result
is a fresh, validated (acyclic) frozen `AgentDAG`, so the static `Supervisor` runs the `N` branches +
the gather in one pass over the frozen `plan.order` — **no runtime graph mutation, no dynamic split.**

It stays firmly opt-in and inside the compiler-not-governor rule:

- An **absent / empty** `unroll` returns the input `dag` **unchanged (the same object)** — a caller
  that never asks for unrolling gets a byte-identical plan.
- Only `N >= 2` unrolls (`N == 1` is a degenerate no-op that leaves the base in place). A base id not
  in the DAG, or a non-int / `N < 1` count, raises `DAGError` at compile — unbounded /
  data-dependent fan-out is out of scope; `N` must be a **declared static bound**.

This is the DAG-topology sibling of the inner graph's per-round
[fan-out ceiling clamp](#the-fan-out-ceiling-clamp-opt-in-tightening): the clamp bounds how many
investigators a *deliberation round* dispatches at once, while `unroll_static_fanout` bounds how wide
a *frozen plan's* declared fan-out can be — both are static, opt-in bounds that keep the
compiler-not-governor invariant intact. The [Compiling & Running a Team](compiling-and-running.md)
guide covers where the unrolled plan goes next.

---

## Where to go next

- [Guide: Compiling & Running a Team](compiling-and-running.md) — where the lowered `AgentDAG` goes
  next: `resolve → assemble → freeze → supervise`.
- [Guide: Durable Run State](durable-state.md) — the `StateStore` seam the trail binds to, the
  append-only log, and the precedent retriever that primes `seed`.
- [Guide: The Governor (Runtime Governance)](governor.md) — the strictly-*outer* standing loop, the
  runtime complement to this strictly-*before* deliberation front.
- [API Reference: reasoning](../reference/reasoning.md) — the full symbol catalog for this tier.
- [Core Concepts](../concepts.md) and the [Overview](../overview.md) for the mental model, or the
  [documentation index](../README.md).
