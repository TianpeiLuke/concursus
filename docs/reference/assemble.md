# API Reference: `assemble`

*`OrchestrationAssembler`, `ProvisioningPlan`, and `plan_from_goal` — the offline compiler core that turns an `AgentDAG` + manifests into a frozen, JSON-serializable plan.*

The `assemble` tier is the **convergence point of the compiler**. It has two halves:

- **`planner.py`** — the compiler's generative *front*. [`plan_from_goal`](#plan_from_goal) authors a pure `AgentDAG` topology from a free-text goal *exactly once* at compile time (optionally via an injected LLM callable), then hands it downstream. It dispatches nothing and mutates no running plan.
- **`assemble.py`** — the compiler *back*. [`OrchestrationAssembler.assemble`](#orchestrationassemblerassemble) takes that DAG plus per-node manifests, validates topology + manifests, type-gates and resolves `depends_on` edges into wiring, synthesizes one build/deploy entry per node, topologically orders them, and freezes everything into a [`ProvisioningPlan`](#provisioningplan).

The flow is strictly **emit → validate → freeze → replay**. Recall the load-bearing invariant: *Concursus is a compiler, not a runtime governor.* The planner never mutates a running plan; `assemble` never touches AWS. The **only** sanctioned plan mutation is [`recompile`](#orchestrationassemblerrecompile), and the feedback edge it serves lives strictly *around* the compiler (run → distill → precedent → next compile), never inside a running `Supervisor`.

For the end-to-end pipeline narrative, see the guide [Compiling & Running a Team](../guides/compiling-and-running.md). For the reasoning tier that can produce the DAG before compile, see [Guide: Reasoning](../guides/reasoning.md) and [API Reference: reasoning](reasoning.md).

**Source:** [`assemble/assemble.py`](../../src/concursus/assemble/assemble.py) · [`assemble/planner.py`](../../src/concursus/assemble/planner.py)

The public compiler symbols are re-exported at the package root:

```python
from concursus import (
    OrchestrationAssembler,
    ProvisioningPlan,
    AssemblyError,
    MonotonicityError,
    plan_from_goal,
    PlanAuthorError,
)
```

---

## Symbol catalog

| Symbol                                                                                     | Kind         | Summary                                                                                                                                                   |
| ------------------------------------------------------------------------------------------ | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`plan_from_goal`](#plan_from_goal)                                                        | function     | Author an `AgentDAG` from a free-text goal (the compiler's generative front).                                                                             |
| [`PlanAuthorError`](#planauthorerror)                                                      | exception    | A goal could not be authored into a valid `AgentDAG`.                                                                                                     |
| [`unroll_static_fanout`](#unroll_static_fanout)                                            | function     | Opt-in compile-time unroll of a declared static bound `N` into `N` frozen branches + a gather (lives in `reasoning.deliberate`, participates in compile). |
| [`OrchestrationAssembler`](#orchestrationassembler)                                        | class        | Pure, offline compiler: `AgentDAG` + manifests → `ProvisioningPlan`.                                                                                      |
| [`OrchestrationAssembler.assemble`](#orchestrationassemblerassemble)                       | method       | Validate, align, wire, synthesize, order, freeze.                                                                                                         |
| [`OrchestrationAssembler.recompile`](#orchestrationassemblerrecompile)                     | method       | The only sanctioned plan mutation: a bounded, monotonic-superset re-compile.                                                                              |
| [`OrchestrationAssembler.retry_budget`](#orchestrationassemblerretry_budget)               | staticmethod | The bounded re-drive count: `max_retries` clamped to `[0, max_revisions]`.                                                                                |
| [`OrchestrationAssembler.redrive_until_valid`](#orchestrationassemblerredrive_until_valid) | method       | Opt-in, bounded validate-and-retry hook to re-drive one node; touches no plan/StateStore/AWS.                                                             |
| [`ProvisioningPlan`](#provisioningplan)                                                    | dataclass    | The compiled plan: order, entries, wiring, precedents, revision, frontier.                                                                               |
| [`ProvisioningPlan.to_dict`](#provisioningplanto_dict)                                     | method       | Full JSON-serializable preview (inlines deploy payloads).                                                                                                 |
| [`ProvisioningPlan.to_summary_dict`](#provisioningplanto_summary_dict)                     | method       | Compact hosting-digest projection for a durable plan note.                                                                                                |
| [`AssemblyError`](#assemblyerror)                                                          | exception    | A DAG/manifest set cannot be compiled into a plan.                                                                                                        |
| [`MonotonicityError`](#monotonicityerror)                                                  | exception    | A re-compile broke the monotonic-superset contract or the revision cap.                                                                                   |
| [`DEFAULT_MAX_REVISIONS`](#default_max_revisions)                                          | constant     | Default ceiling on monotonic re-compiles (`16`).                                                                                                          |
| [`DEFAULT_MAX_NODES`](#default_max_nodes)                                                  | constant     | Default node cap for the `decompose=True` complexity contract (`12`).                                                                                     |
| [`DEFAULT_MAX_DEPTH`](#default_max_depth)                                                  | constant     | Default longest-path-depth cap for the `decompose=True` complexity contract (`6`).                                                                        |
| [`DEFAULT_MAX_FANOUT`](#default_max_fanout)                                                | constant     | Default fan-out cap for the `decompose=True` complexity contract (`6`).                                                                                   |

> `DEFAULT_MAX_REVISIONS` is a module-level constant of [`assemble/assemble.py`](../../src/concursus/assemble/assemble.py) (not re-exported at the package root); `DEFAULT_MAX_NODES` / `DEFAULT_MAX_DEPTH` / `DEFAULT_MAX_FANOUT` live in [`assemble/planner.py`](../../src/concursus/assemble/planner.py). Import them from their modules when you need the raw values.

---

## `plan_from_goal`

```python
def plan_from_goal(
    goal: str,
    *,
    precedents: Optional[Sequence[Mapping[str, object]]] = None,
    operator_directives: Optional[Mapping[str, object]] = None,
    plan_model_fn: Optional[PlanModelFn] = None,
    decompose: bool = False,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_fanout: int = DEFAULT_MAX_FANOUT,
) -> AgentDAG
```

Author an [`AgentDAG`](core.md#agentdag) for `goal` — the compiler's generative **front** (the *emit* step of emit → validate → freeze → replay). It produces a topology **once**, at compile time; the returned DAG is meant to be handed straight to [`OrchestrationAssembler.assemble`](#orchestrationassemblerassemble), which is the sole authority that validates, type-gates, and freezes it. The planner itself dispatches nothing and mutates no running plan.

**Parameters** (keyword-only after `goal`):

| Param | Type | Default | Meaning |
|---|---|---|---|
| `goal` | `str` | — | Free-text description of the team's objective. Must be non-empty/non-blank. |
| `precedents` | `Optional[Sequence[Mapping[str, object]]]` | `None` | Read-only prior-run context (AI-17 `RetrievedPrecedent.to_dict()` payloads, or a plan's `precedents` list). Passed to `plan_model_fn` as context; never executed. In the offline `decompose=True` path they also seed the **C3 precedent-shape** tier — borrowed only when the goal matches no keyword shape (see Behavior step 4). |
| `operator_directives` | `Optional[Mapping[str, object]]` | `None` | Read-only operator constraints/preferences (e.g. required nodes, budget hints). Passed to `plan_model_fn` as context. |
| `plan_model_fn` | `Optional[PlanModelFn]` | `None` | The injected plan-author callable (the LLM seam). When `None`, a trivial **deterministic** single-node template is used, so the package imports and runs with **no** model present. |
| `decompose` | `bool` | `False` | **Opt-in, default OFF.** When `False` (the default), behavior is byte-identical to before — the single-node fallback. When `True` **and no `plan_model_fn` is injected**, the deterministic OFFLINE path emits a multi-node **capability chain** instead of one opaque node. |
| `max_nodes` | `int` | [`DEFAULT_MAX_NODES`](#default_max_nodes) (`12`) | Per-sub-task **complexity contract**: max total nodes in the authored DAG. Exceeding it raises [`PlanAuthorError`](#planauthorerror) at author time. |
| `max_depth` | `int` | [`DEFAULT_MAX_DEPTH`](#default_max_depth) (`6`) | Complexity contract: max longest-path depth. Exceeding it raises `PlanAuthorError` at author time. |
| `max_fanout` | `int` | [`DEFAULT_MAX_FANOUT`](#default_max_fanout) (`6`) | Complexity contract: max fan-out from any single node. Exceeding it raises `PlanAuthorError` at author time. |

**Returns:** a validated (acyclic, non-empty) [`AgentDAG`](core.md#agentdag) ready to `assemble`.

**Raises:** [`PlanAuthorError`](#planauthorerror) if `goal` is empty/blank, or if `plan_model_fn` returns an invalid spec (an unrecognized type, an invalid mapping, an empty/no-nodes plan, or a non-acyclic DAG).

**Behavior:**

1. Raises `PlanAuthorError` **immediately** if `goal` is empty/blank — before `plan_model_fn` is ever consulted.
2. Normalizes context to `list(precedents or [])` and `dict(operator_directives or {})`.
3. If `plan_model_fn is None` **and `decompose=False`** (the default), returns the deterministic single-node fallback template (a single-source DAG whose node id derives from the goal) — byte-identical to before. No LLM is called.
4. If `plan_model_fn is None` **and `decompose=True`**, emits a deterministic **OFFLINE multi-node capability chain**. Node ids are agent-agnostic *capability* labels of the form `"<goalslug>__<stage>"`, where `<goalslug>` is the goal `_slug`'d (lowercased, non-alphanumerics collapsed to a single `_`), **truncated to 24 chars**, then `rstrip("_")`-ed so a truncation ending in `_` yields a clean `"__<stage>"` boundary (`"…checkout__scope"`, not a spurious `"…checkout___scope"` / `"_scope"` stage; falls back to `"task"` if the slug is empty) — they always contain `"__"` and **never** an agent/manifest name. The stage chain is chosen in a strict **priority order** (C3):

   1. **Keyword match** — if the goal matches `_SHAPE_KEYWORDS`, use that domain shape (below).
   2. **Precedent shape** — else, when `precedents` are supplied, **borrow the capability-stage shape from the most-relevant (first) precedent**: read its executed `nodes`, strip each `<prefix>__<stage>` down to `<stage>`, and re-use that ordered, de-duplicated stage tuple (re-prefixed to the new goal). This warm-starts a *new* domain from a structurally-adjacent prior run. An unusable precedent (no multi-stage capability shape — e.g. a single opaque node) is ignored and the planner falls through.
   3. **Generic fallback** — else the generic `ingest -> analyze -> synthesize -> format` chain.

   Keyword-routed shapes:

   | Goal keyword(s) | Stage chain |
   |---|---|
   | `investigate` / `diagnos` / `root cause` | `scope -> gather_evidence -> hypothesize -> verify` |
   | `model` / `detect` | `scope_data -> build_model -> calibrate -> evaluate` |
   | `launch` / `program` | `scope -> design -> review -> rollout` |
   | `migrat` | `audit_source -> transform -> validate_parity` |
   | `report` / `summar` | `gather -> analyze -> draft` |
   | *(generic fallback)* | `ingest -> analyze -> synthesize -> format` |

   All of this is deterministic and offline. The chain is **LINEAR** (fan-out 1, bounded depth). Before returning, the authored DAG is checked against the complexity contract; if it exceeds `max_nodes`, `max_depth` (longest path), or `max_fanout`, [`PlanAuthorError`](#planauthorerror) is raised **at author time**.
5. Otherwise (a `plan_model_fn` is injected) calls `plan_model_fn(goal, ctx_precedents, ctx_directives)` **positionally** (with the normalized list/dict context, not the raw `None`-able args) and lowers the returned spec into an `AgentDAG`. An injected `plan_model_fn` **always overrides** the template — even when `decompose=True`. (When `decompose=True`, the complexity contract is applied to the injected model's output too; a default model-injected call is byte-identical to before.)

> **Note:** The planner's internal validation is only a cheap acyclicity + non-empty check. Alignment and wiring type-gating happen **later**, in `assemble`. A DAG that passes `plan_from_goal` can still fail `assemble`.

### `PlanModelFn`

```python
PlanModelFn = Callable[
    [str, Sequence[Mapping[str, object]], Mapping[str, object]], object
]
```

The injected plan-author seam: `(goal, precedents, operator_directives) -> plan spec`. A "plan spec" is either an already-built `AgentDAG` (returned as-is) or a plain mapping the planner lowers into an `AgentDAG`:

```python
{"nodes": ["triage", "fix"], "edges": [["triage", "fix"]]}
```

This is where an LLM would live; it is **never** imported or constructed by Concursus — the caller injects it, so the package depends on no model.

### Examples

Deterministic fallback (no model — the default, zero-LLM path):

```python
from concursus import plan_from_goal

dag = plan_from_goal("resolve billing ticket")
# -> single-node AgentDAG, node id derived from the goal ("resolve_billing_ticket")
```

Injected planner returning a `{"nodes", "edges"}` mapping:

```python
from concursus import plan_from_goal, OrchestrationAssembler

def my_llm_planner(goal, precedents, directives):
    # ... call your model here ...
    return {"nodes": ["triage", "fix"], "edges": [["triage", "fix"]]}

dag = plan_from_goal(
    "triage then remediate the incident",
    operator_directives={"required_nodes": ["triage"]},
    plan_model_fn=my_llm_planner,
)
plan = OrchestrationAssembler().assemble(dag, manifests)
```

Feeding a prior plan's precedents back in as read-only context (the outer feedback loop):

```python
dag = plan_from_goal(
    goal,
    precedents=prior_plan.precedents,  # read-only advisory context
    plan_model_fn=my_llm_planner,
)
```

Opt-in offline decomposition (no LLM) — a deterministic capability chain:

```python
from concursus import plan_from_goal

# decompose=True, no plan_model_fn -> deterministic OFFLINE multi-node capability chain.
# "investigate" routes to the scope -> gather_evidence -> hypothesize -> verify shape.
dag = plan_from_goal("investigate the checkout latency regression", decompose=True)
# -> node ids are capability labels, goal-slug (truncated to 24 chars) + "__" + stage:
#    "investigate_the_checkout__scope", "investigate_the_checkout__gather_evidence",
#    "investigate_the_checkout__hypothesize", "investigate_the_checkout__verify"
#    (each contains "__"; never an agent/manifest name). The chain is linear.

# Tighten the complexity contract; exceeding it raises PlanAuthorError at author time.
# The "investigate" shape authors 4 nodes, so max_nodes=3 rejects it (len(nodes) > max_nodes).
plan_from_goal("investigate the checkout latency regression", decompose=True, max_nodes=3)
# -> raises PlanAuthorError: "authored plan has 4 nodes; exceeds max_nodes=3"
```

Cross-domain precedent priming (C3) — a goal with **no** keyword match borrows the adjacent precedent's stage shape (priority: keyword > precedent > generic):

```python
# A prior run over a different goal executed a 3-stage capability shape:
prior = [{"nodes": ["refund_flow__triage", "refund_flow__assess", "refund_flow__resolve"]}]

# The new goal matches no _SHAPE_KEYWORDS, so the planner borrows the precedent's
# stages (triage -> assess -> resolve), re-prefixed to the new goal.
dag = plan_from_goal("handle the chargeback backlog", precedents=prior, decompose=True)
# -> "handle_the_chargeback_ba__triage", "..._assess", "..._resolve"
#    (deterministic/offline; an unusable single-node precedent would be ignored)
```

---

## `PlanAuthorError`

```python
class PlanAuthorError(ValueError)
```

Raised when a goal cannot be authored into a valid `AgentDAG`. Subclass of `ValueError`. Covers: an empty/blank goal, an unrecognized `plan_model_fn` return type, an invalid plan spec (`DAGError`/`KeyError`/`TypeError`/`IndexError` are caught and re-wrapped), an empty (no-nodes) authored plan, a non-acyclic DAG, or an authored plan that violates the complexity contract.

> A `plan_model_fn` that returns a cyclic or malformed mapping raises `PlanAuthorError` — **not** the underlying `DAGError`. Do not catch `DAGError` at the `plan_from_goal` call site; catch `PlanAuthorError`.

---

## `unroll_static_fanout`

```python
def unroll_static_fanout(
    dag: "AgentDAG",
    unroll: Optional[Mapping[str, int]] = None,
) -> AgentDAG
```

> **Lives in [`reasoning/deliberate.py`](../../src/concursus/reasoning/deliberate.py), not `assemble.py`** — it is exported from `concursus.reasoning.deliberate` (see [API Reference: reasoning](reasoning.md#reasoningdeliberate)). It is covered here because it **participates in compile**: it is a pure compile-time rewrite of the `AgentDAG` topology applied *before* [`OrchestrationAssembler.assemble`](#orchestrationassemblerassemble) freezes it, in the same *emit → validate → freeze → replay* pipeline.

**Opt-in, default OFF — compile-time static fan-out virtualization** (one of the opt-in additions completed in v0.6.0). Given `unroll = {base_node: N}` where `N` is a **declared, data-INDEPENDENT** static bound, each named `base` node is expanded, in this one compile pass, into `N` frozen parallel branches — the sub-node is cloned under namespaced ids `f"{base}__fe{i}"` (`i` in `0..N-1`) — plus:

- a **scatter**: every upstream producer of `base` fans its (shared) input to all `N` clones (a static shared-input scatter, not a runtime split); and
- a **gather**: a synthetic join node `f"{base}__gather"` that collects the `N` clone outputs, onto which every original downstream consumer of `base` is re-pointed.

The result is a **new** frozen [`AgentDAG`](core.md#agentdag) whose `validate()` passes, so the static [`Supervisor`](execute.md) runs the `N` branches + the gather in **one** static pass over the frozen `plan.order` — **no** runtime graph mutation, **no** dynamic split. This keeps the load-bearing invariant intact: all dynamism is bounded at *compile* time; `Supervisor.run` remains a single static pass over `plan.order`.

**Parameters:**

| Param | Type | Default | Meaning |
|---|---|---|---|
| `dag` | `AgentDAG` | — | The topology to rewrite (before `assemble`). |
| `unroll` | `Optional[Mapping[str, int]]` | `None` | **Opt-in, default OFF.** `{base_node: N}` — the declared static fan-out count per base node. Absent/empty ⇒ the input `dag` is returned **unchanged (same object)**. |

**Returns:** a new frozen, validated `AgentDAG` when a non-degenerate `unroll` spec is supplied; **the same `dag` object** when `unroll` is absent/empty.

**Raises:** [`DAGError`](core.md) (from [`core/dag.py`](../../src/concursus/core/dag.py)) if a base id is not in `dag`, or a count is a non-int / `bool` / `< 1` — a spec error caught at compile, never a silent mis-compile. Unbounded / data-dependent fan-out is out of scope.

**Gating (default path byte-for-byte unchanged):**

- `unroll` absent or empty ⇒ the input `dag` is returned **unchanged (the same object)**, so a caller that never asks for unrolling gets a byte-identical plan.
- Only `N >= 2` unrolls; `N == 1` is a degenerate **no-op** (the base node is left in place).
- A bad declared bound (unknown base id, non-int/`bool`/`N < 1`) **fails closed** with `DAGError` *before* any rewrite.

#### Example

```python
from concursus.reasoning.deliberate import unroll_static_fanout
from concursus import OrchestrationAssembler, AgentDAG

dag = AgentDAG()
dag.add_node("ingest")
dag.add_node("shard")       # a base node we want to fan out statically
dag.add_node("reduce")
dag.add_edge("ingest", "shard")
dag.add_edge("shard", "reduce")

# Default OFF: no spec => byte-identical (same object).
assert unroll_static_fanout(dag) is dag

# Opt-in: unroll "shard" into 3 frozen branches + a synthetic gather, at compile time.
unrolled = unroll_static_fanout(dag, {"shard": 3})
# nodes: ingest, shard__fe0, shard__fe1, shard__fe2, shard__gather, reduce
#   ingest -> each shard__fe{i}   (scatter: shared input fanned to all clones)
#   shard__fe{i} -> shard__gather (the synthetic join)
#   shard__gather -> reduce       (gather feeds the original downstream consumer)

plan = OrchestrationAssembler().assemble(unrolled, manifests)
# -> the Supervisor runs all 3 branches + the gather in ONE static pass over plan.order.
```

---

## `OrchestrationAssembler`

```python
class OrchestrationAssembler:
    def __init__(
        self,
        *,
        account: Optional[str] = None,
        region: Optional[str] = None,
        precedent_retriever: Optional["PrecedentRetriever"] = None,
        strict_types: bool = False,
        single_writer: bool = False,
        strict_fn: Optional[Callable[[str], bool]] = None,
        payload_tier_fn: Optional[Callable[[str], "Tier"]] = None,
        full_input_cover: bool = False,
    ) -> None
```

Pure, offline compiler that turns an [`AgentDAG`](core.md#agentdag) + per-node [`AgentManifest`](core.md#agentmanifest)s into a [`ProvisioningPlan`](#provisioningplan). It validates everything, resolves the wiring, and synthesizes the build/deploy entries — it **never imports boto3 or calls AWS**.

**Constructor parameters** (all keyword-only):

| Param | Type | Default | Meaning |
|---|---|---|---|
| `account` | `Optional[str]` | `None` | AWS account id threaded into the synthesized IAM roles, so the plan is previewable ahead of a real deploy. |
| `region` | `Optional[str]` | `None` | AWS region threaded into synthesized roles for preview. |
| `precedent_retriever` | `Optional[PrecedentRetriever]` | `None` | Optional compile-time, **read-only** retriever (AI-17). When supplied, `assemble` retrieves relevant prior resolved runs *before* freezing and attaches them as advisory context. It **never** changes the compiled topology and never touches AWS or a run log. Default `None` keeps `assemble` byte-for-byte unchanged. See [`PrecedentRetriever`](../../src/concursus/state/precedent.py) in [API Reference: state](state.md). |
| `strict_types` | `bool` | `False` | **Opt-in, default OFF.** The **deep type gate** (B2). When `True`, `check_alignment` additionally enforces that each `depends_on` edge's producer-output declared JSON-Schema `type` is *compatible* with the consumer-input declared `type`; a concrete mismatch raises [`AlignmentError`](core.md). Conservative: unknown/absent types on either side pass, and union types (e.g. `["string","null"]`) match by set-overlap, so an un-annotated manifest is never rejected. Default `False` leaves the name-level gate byte-for-byte unchanged. Compile-time only (INV-2). |
| `single_writer` | `bool` | `False` | **Opt-in, default OFF.** The **non-overlap gate** (B1). When `True`, `check_alignment` additionally rejects a plan where any consumer input is fed by more than one `depends_on` edge (two edges to the same `input_name` = a silent last-wins `payload[input_name]` overwrite at run time), raising a `"single-writer violation"` `AlignmentError`. Composable with `strict_types`. Default `False` = byte-for-byte unchanged. Compile-time only (INV-2). |
| `strict_fn` | `Optional[Callable[[str], bool]]` | `None` | **Opt-in, default OFF.** The per-node **adaptive-strictness dial** (B4). `None` (default) applies the enabled deep gates to *every* node. When set, it is a `node -> bool` predicate that **narrows** `strict_types`/`single_writer` to the nodes it returns truthy for; it never relaxes the name-level gate. Wire [`make_trust_strictness`](governor.md#make_trust_strictness) so WEAK/low-trust agents get the strict contract and STRONG/high-trust ones get the lean path. Author/compile-time only (INV-2). |
| `payload_tier_fn` | `Optional[Callable[[str], Tier]]` | `None` | **Opt-in, default OFF.** The **payload-contract author dial** (F1/F4). `None` (default) leaves [`ProvisioningPlan.payload_contract`](#provisioningplan) empty and the compile byte-for-byte unchanged. When set, it is a `node -> `[`Tier`](governor.md#tier) selector; `assemble` then **authors** `payload_contract[node]` for each node with a declared `contract.context`, stamping the node's `trust_tier` and a [`project_context`](governor.md#project_context)-projected `static_context`. Wire [`make_payload_tier`](governor.md#make_payload_tier) (optionally with [`manifest_is_programmatic`](governor.md#manifest_is_programmatic)) to drive detail from trust. Author/compile-time only — it never changes `order`/`entries`/`wiring` (INV-2). |
| `full_input_cover` | `bool` | `False` | **Opt-in, default OFF.** Threads through to [`check_alignment`](core.md) (F2). When `True`, every declared consumer input must have a compile-visible supplier — a `depends_on` edge **or** a static `contract.context` key of the same name — else [`AlignmentError`](core.md) (`full-input-cover` in the message). The b2 dimension-1 completeness quantifier. Default `False` leaves the name+edge gate byte-for-byte unchanged. Compile-time only (INV-2). |

> **All the deep gates thread through both [`assemble`](#orchestrationassemblerassemble) *and* [`recompile`](#orchestrationassemblerrecompile)** (recompile re-compiles via a fresh `assemble`), and all default OFF — so a default-constructed assembler is byte-for-byte identical to the pre-opt-in compile. `payload_tier_fn`/`full_input_cover` follow the same rule: default OFF ⇒ byte-for-byte unchanged, and a re-tiered `recompile` still pins already-executed nodes to their prior contract (INV-3, see [`recompile`](#orchestrationassemblerrecompile)).

### `OrchestrationAssembler.assemble`

```python
def assemble(
    self, dag: "AgentDAG", manifests: Dict[str, "AgentManifest"]
) -> ProvisioningPlan
```

Validate, align, wire, and synthesize — returning the full provisioning plan.

**Parameters:**

| Param | Type | Meaning |
|---|---|---|
| `dag` | `AgentDAG` | The topology to compile (one node per agent id). |
| `manifests` | `Dict[str, AgentManifest]` | `{node_id: manifest}` — every DAG node must have one. |

**Returns:** a [`ProvisioningPlan`](#provisioningplan) with `revision=0`.

**Raises:**

- [`AssemblyError`](#assemblyerror) — if a DAG node has **no** manifest to provision.
- It also **propagates** DAG validation, manifest validation, and alignment errors unchanged from the underlying steps: `DAGError` (from [`dag.validate()`](../../src/concursus/core/dag.py) / topological sort), `ManifestError` (from `manifest.validate()`), and `AlignmentError` (from [`check_alignment` / `resolve_edges`](../../src/concursus/core/resolve.py)).

**Steps, in order:**

1. `dag.validate()` — assert the topology is acyclic.
2. `manifest.validate()` for each manifest in `manifests.values()`.
3. `resolve.check_alignment(dag, manifests, ...)` — type-gate the declared `depends_on` edges (with the assembler's opt-in gates threaded through).
4. `wiring = resolve.resolve_edges(dag, manifests)` — compile edges into [`AgentRef`](core.md) wiring.
5. For each node in `dag.nodes`, look up its manifest (raise `AssemblyError` if `None`) and synthesize `entries[node]` via `RuntimeBuilderFactory.synthesize(manifest, account=self.account, region=self.region)`.
6. `order = dag.topological_sort()`.
7. If a `precedent_retriever` is set, `precedents = [p.to_dict() for p in retriever.retrieve(nodes=order)]` — computed **after** topology resolution and **not** influencing `order` / `entries` / `wiring`. Default is `[]`.
8. If a `payload_tier_fn` is set, author `payload_contract` (one entry per node with a declared `contract.context`); default is `{}`.

> **Purity guarantee:** `order`, `entries`, and `wiring` are computed **identically** whether or not a `precedent_retriever` or `payload_tier_fn` is supplied. Precedents and the payload contract are pure advisory/additive context; they never participate in the compiled topology.

#### Example

```python
from concursus import OrchestrationAssembler, AgentDAG, AgentManifest

dag = AgentDAG()
dag.add_node("summarize")

manifests = {
    "summarize": AgentManifest(
        name="summarize",
        registry={"container_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/summarize:latest",
                  "protocol": "HTTP",
                  "entry": "app:handler"},   # module:function the serving wrapper imports
        contract={"inputs": {"text": {"type": "string"}},
                  "outputs": {"properties": {"summary": {"type": "string"}}}},
    ),
}

asm = OrchestrationAssembler(account="123456789012", region="us-east-1")
plan = asm.assemble(dag, manifests)
preview = plan.to_dict()   # JSON-serializable `concursus plan` preview
```

With a compile-time precedent retriever (topology unchanged, `plan.precedents` populated):

```python
asm = OrchestrationAssembler(precedent_retriever=retriever)
plan = asm.assemble(dag, manifests)
```

Authoring the payload contract from trust (F1/F2/F4) — the assembler projects each node's tiered `static_context` and (opt-in) enforces full input cover:

```python
from concursus.governor import make_payload_tier, manifest_is_programmatic

# Derive a node -> Tier selector from the scheduler's trust grades
# (L3->HIGH, L2->GUARDED, L0/L1/unknown->LOW; programmatic nodes -> PROGRAMMATIC).
payload_tier_fn = make_payload_tier(
    scheduler,
    is_programmatic=manifest_is_programmatic(manifests),
)

asm = OrchestrationAssembler(
    payload_tier_fn=payload_tier_fn,   # F1: authors plan.payload_contract per node
    full_input_cover=True,             # F2: every consumer input needs an edge or a context key
)
plan = asm.assemble(dag, manifests)
plan.payload_contract["summarize"]     # {"trust_tier": "GUARDED", "static_context": {...}}
```

### `OrchestrationAssembler.recompile`

```python
def recompile(
    self,
    prior_plan: ProvisioningPlan,
    *,
    completed: Set[str],
    content_hashes: Optional[Mapping[str, str]] = None,
    dag: Optional["AgentDAG"] = None,
    manifests: Optional[Dict[str, "AgentManifest"]] = None,
    max_revisions: int = DEFAULT_MAX_REVISIONS,
    compile_next: Optional[Iterable[str]] = None,
) -> ProvisioningPlan
```

Emit a **fresh, frozen, monotonic-superset** plan superseding `prior_plan` (AI-20) — the **only sanctioned plan mutation**. It re-compiles `dag` + `manifests` into a brand-new plan (a fresh `assemble`), pins already-executed nodes to their prior entries/wiring, and guards monotonicity. This feedback edge lives strictly *around* the compiler — never inside a running `Supervisor`, and `prior_plan` (and any supervisor over it) is never mutated.

**Parameters** (keyword-only except `prior_plan`):

| Param | Type | Default | Meaning |
|---|---|---|---|
| `prior_plan` | `ProvisioningPlan` | — | The frozen plan the current run replayed; its `order`/`entries`/`wiring` are the monotonic floor. |
| `completed` | `Set[str]` | — | The already-executed node ids (e.g. `state_store.completed()`) — these are pinned. |
| `content_hashes` | `Optional[Mapping[str, str]]` | `None` | Optional `{node: content_hash}` provenance of executed outputs. Accepted as **read-only provenance only**; it does **not** relax the guard or change the returned plan. |
| `dag` | `Optional[AgentDAG]` | `None` | The (possibly extended) topology to re-compile. **Effectively required** (see note). |
| `manifests` | `Optional[Dict[str, AgentManifest]]` | `None` | The manifests to re-compile. **Effectively required** (see note). |
| `max_revisions` | `int` | [`DEFAULT_MAX_REVISIONS`](#default_max_revisions) (`16`) | Revision ceiling; the outer plan-generation loop is bounded. |
| `compile_next` | `Optional[Iterable[str]]` | `None` | **Opt-in, default OFF.** The scheduler's cleared-frontier node ids (a `TrustLadderScheduler` `propose_bindings` DISPATCH set). When supplied, they are recorded onto the fresh plan's read-only [`ProvisioningPlan.frontier`](#provisioningplan) field, filtered to topology nodes. It **never** changes `order`/`entries`/`wiring` (the monotonic superset is preserved) — it only closes the previously-dead scheduler→compiler channel. Default `None` keeps the returned plan byte-for-byte unchanged. |

**Returns:** a **new** `ProvisioningPlan` with `revision = prior_plan.revision + 1`, `order`/`precedents` from the fresh compile, and executed-node `entries`/`wiring` pinned to the prior plan. When a `payload_tier_fn` is set, `payload_contract` is re-authored from the live tier dial for not-yet-executed nodes but **pinned** to the prior contract for every node in `completed` (F5 re-tiering, INV-3).

**Raises:**

- [`MonotonicityError`](#monotonicityerror) — on a non-monotonic edit/removal/reorder of an already-executed or already-planned node, or once `revision` would exceed `max_revisions`.
- [`AssemblyError`](#assemblyerror) — if `dag` / `manifests` are missing, or the fresh compile fails.

**Order of operations:**

1. `revision = int(prior_plan.revision) + 1`; if `revision > max_revisions`, raise `MonotonicityError`.
2. If `dag is None` or `manifests is None`, raise `AssemblyError`.
3. `fresh = self.assemble(dag, manifests)`.
4. Check monotonicity of `fresh` against `prior_plan` for the `completed` set (raises `MonotonicityError` on violation).
5. Copy `fresh.entries` / `fresh.wiring`, then for each `node in completed` overwrite with `prior_plan`'s entry/wiring (list-copied) **if present**. Newly-added nodes keep their fresh entry/wiring.
6. If `compile_next` was supplied, record it on the new plan's read-only `frontier` field, filtered to nodes actually in `fresh.order`. This changes no `order`/`entries`/`wiring`.
7. **Re-tiering (F5).** The fresh compile re-authors `payload_contract` from the *live* `payload_tier_fn` (so a turned tier dial re-projects each node's `static_context`), but for each `node in completed` the prior plan's `payload_contract[node]` is **pinned** — the executed node keeps the exact contract it ran under (INV-3). Only not-yet-executed and newly-added nodes pick up the re-tiered contract. If `payload_tier_fn` is unset, `payload_contract` stays empty and this step is a no-op.
8. Return the new plan.

> **Gotchas.**
> - `dag=` and `manifests=` are typed `Optional` but are **effectively required** — passing `None` raises `AssemblyError`.
> - The **revision-cap check happens before** the `dag`/`manifests` `None`-check, so exceeding `max_revisions` raises `MonotonicityError` even if `dag`/`manifests` were also missing.
> - `precedents` in the recompiled plan come from the **fresh** compile (`fresh.precedents`), not from `prior_plan`.

**The two enforced monotonicity invariants:**

1. **Prior order survives as a subsequence.** No already-planned node may be dropped, and the prior nodes keep their exact relative order (new nodes may be interleaved). A missing prior node raises a *drops already-planned node(s)* error; a surviving-but-reordered set raises a *reorders already-planned nodes* error.
2. **Executed nodes are frozen.** For every node in `completed`, it must still be present (else *removes already-executed node*), its `BuildPlanEntry` must be byte-identical (else *edits already-executed node*), and its wiring list must be unchanged (else *rewires already-executed node*). This is what keeps resume a faithful replay.

#### Example

```python
next_plan = asm.recompile(
    prior_plan,
    completed=state_store.completed(),   # already-executed nodes, pinned
    dag=extended_dag,                    # e.g. prior nodes + newly-appended ones
    manifests=manifests,
)
assert next_plan.revision == prior_plan.revision + 1
```

To route a *change* to an already-executed node, you must add a **new** node — you may never edit or rewire a completed one. See [Compiling & Running a Team](../guides/compiling-and-running.md) for how recompile fits the run → distill → recompile loop, and [Core Concepts](../concepts.md) for the monotonicity contract in prose.

### `OrchestrationAssembler.retry_budget`

```python
@staticmethod
def retry_budget(
    max_retries: int, *, max_revisions: int = DEFAULT_MAX_REVISIONS
) -> int
```

The bounded re-drive count: a requested `max_retries` **clamped to `[0, max_revisions]`**. The validate-and-retry loop ([`redrive_until_valid`](#orchestrationassemblerredrive_until_valid)) shares the **same ceiling** as the monotonic re-compile ([`DEFAULT_MAX_REVISIONS`](#default_max_revisions) = `16`), so **both** of the compiler's plan-author feedback edges — re-compile a plan / re-drive a node — are bounded by one dial. A negative request floors at `0` (no retry); a request above the ceiling is capped, so a caller can never open an unbounded loop.

**Parameters:**

| Param | Type | Default | Meaning |
|---|---|---|---|
| `max_retries` | `int` | — | Requested re-drives after the first attempt. `< 0` floors to `0`. |
| `max_revisions` | `int` | [`DEFAULT_MAX_REVISIONS`](#default_max_revisions) (`16`) | The shared re-drive/re-compile ceiling; `max_retries` is capped to it. |

**Returns:** `min(max(max_retries, 0), max_revisions)` — the clamped retry count used by `redrive_until_valid`.

### `OrchestrationAssembler.redrive_until_valid`

```python
def redrive_until_valid(
    self,
    node: str,
    manifest: "AgentManifest",
    drive_fn: Callable[[Dict[str, Any]], Any],
    *,
    base_context: Optional[Mapping[str, Any]] = None,
    max_retries: int = 1,
    max_revisions: int = DEFAULT_MAX_REVISIONS,
    error_key: str = "validation_error",
) -> Any
```

**Opt-in, bounded validate-and-retry hook** (default-off) a caller may use to re-drive **one** node whose output failed its manifest-declared output schema (`contract["outputs"]`). It is a helper for a plan-author **driver AROUND the compiler** — it is **NOT** wired into [`assemble`](#orchestrationassemblerassemble) / [`recompile`](#orchestrationassemblerrecompile) (whose default output is byte-for-byte unchanged) and it is **NEVER** inside [`Supervisor.run`](execute.md), which stays a single static pass over a frozen `plan.order`. It touches **no plan, no `StateStore`, and no AWS** — a purely functional loop the caller owns.

Each attempt calls `drive_fn(context)` and shape-checks the result against `manifest.output_schema` via [`validate_output`](execute.md); on a `SchemaError` the failing reason is fed back into the **next** attempt's `context` under `error_key` and the node is re-driven. The first schema-valid output is returned; the last `SchemaError` is re-raised once the budget is exhausted.

The loop is **bounded, not an unbounded in-node loop**: the retry count is clamped by [`retry_budget`](#orchestrationassemblerretry_budget) to the recompile/revision budget (`max_revisions`), so a total of **at most `1 + retry_budget(max_retries)`** drives run (1 initial drive + up to `retry_budget` bounded retries).

**Parameters** (keyword-only after `drive_fn`):

| Param | Type | Default | Meaning |
|---|---|---|---|
| `node` | `str` | — | The node id being re-driven (for error context / caller bookkeeping). |
| `manifest` | `AgentManifest` | — | The node's manifest; its `output_schema` is the gate. |
| `drive_fn` | `Callable[[Dict[str, Any]], Any]` | — | `context -> output` — invokes the node with the (error-augmented) context. |
| `base_context` | `Optional[Mapping[str, Any]]` | `None` | Optional seed context for the **first** attempt (copied, never mutated). |
| `max_retries` | `int` | `1` | Requested re-drives after the first attempt (clamped by `max_revisions` via `retry_budget`). |
| `max_revisions` | `int` | [`DEFAULT_MAX_REVISIONS`](#default_max_revisions) (`16`) | The shared ceiling. |
| `error_key` | `str` | `"validation_error"` | The `context` key under which the prior attempt's validation error is fed back. |

**Returns:** the first schema-valid `drive_fn` output.

**Raises:** `SchemaError` — the last validation failure, once the bounded budget is exhausted.

> **Opt-in, default-off framing.** This is a hook a caller *chooses* to invoke; it changes **nothing** about a default compile or run. `assemble`/`recompile` output and `Supervisor.run`'s single static pass are byte-for-byte unchanged whether or not a driver uses it. It is the *node-level* sibling of `recompile`'s *plan-level* bound — both feedback edges live strictly around the compiler and share the `DEFAULT_MAX_REVISIONS` ceiling.

#### Example

```python
# Re-drive one node up to `retry_budget(2)` times if its output fails the manifest schema.
# Offline: drive_fn is a plain callable — no boto3/langgraph needed.
attempts = []

def drive_fn(context):
    # First call returns an invalid shape; the second reads the fed-back error and fixes it.
    attempts.append(context.get("validation_error"))
    if len(attempts) < 2:
        return {"wrong_field": 1}          # fails manifest.output_schema
    return {"summary": "ok"}               # schema-valid

result = asm.redrive_until_valid(
    "summarize",
    manifests["summarize"],
    drive_fn,
    max_retries=2,                         # clamped to [0, max_revisions]
)
# -> {"summary": "ok"}; at most 1 + retry_budget(2) drives ran. No plan/StateStore/AWS touched.
```

---

## `ProvisioningPlan`

```python
@dataclass
class ProvisioningPlan:
    order: List[str] = field(default_factory=list)
    entries: Dict[str, BuildPlanEntry] = field(default_factory=dict)
    wiring: Dict[str, List[AgentRef]] = field(default_factory=dict)
    precedents: List[dict] = field(default_factory=list)
    revision: int = 0
    frontier: List[str] = field(default_factory=list)
    payload_contract: Dict[str, dict] = field(default_factory=dict)
```

The compiled orchestration plan for one agent team.

**Fields:**

| Field | Type | Meaning |
|---|---|---|
| `order` | `List[str]` | A valid dispatch order (topological sort of the DAG). |
| `entries` | `Dict[str, BuildPlanEntry]` | `{node_id: BuildPlanEntry}` — packaging + `create_agent_runtime` params per agent. See [`BuildPlanEntry`](build.md#buildplanentry). |
| `wiring` | `Dict[str, List[AgentRef]]` | `{node_id: [AgentRef, ...]}` — resolved producer→consumer data edges. See [`AgentRef`](core.md). |
| `precedents` | `List[dict]` | Read-only cross-run precedent context (AI-17). Empty by default; **never** affects topology. |
| `revision` | `int` | Monotonic re-compile counter. `0` for a first `assemble`; incremented by each `recompile`. |
| `frontier` | `List[str]` | Read-only **advisory** record of the scheduler's cleared frontier for this revision (the [`recompile`](#orchestrationassemblerrecompile) `compile_next` set), filtered to topology nodes. Empty by default; **never** affects `order`/`entries`/`wiring`. Emitted in `to_dict()` only when non-empty (same pattern as `revision`). |
| `payload_contract` | `Dict[str, dict]` | The compiler-authored **payload contract** (F1). `{node: {"trust_tier": <TierName>, "static_context": <projected context>}}` — one entry per node with a declared `contract.context`, populated only when the assembler was built with a [`payload_tier_fn`](#orchestrationassembler). The frozen, self-contained tiered context a [`Supervisor`](execute.md) prefers over a live scheduler (F3). Empty `{}` by default; **never** affects `order`/`entries`/`wiring`. Emitted in `to_dict()`/`to_summary_dict()` only when non-empty. |

> **`ProvisioningPlan` is a plain (non-frozen) dataclass.** The "frozen" guarantee — that `order`/`entries`/`wiring` are treated as immutable and never mutated in place — is by convention/contract, not enforced by the type. Treat it as an immutable preview.

### `ProvisioningPlan.to_dict`

```python
def to_dict(self) -> dict
```

Render the plan as a JSON-serializable dict for a `concursus plan` preview.

- **Always** includes `"order"` (a list copy), `"entries"` (`{name: entry.to_dict()}`), and `"wiring"` (`{node: [asdict(ref), ...]}`).
- Adds `"precedents"` (`[dict(p) for p in self.precedents]`) **only when `self.precedents` is non-empty**.
- Adds `"revision"` **only when `self.revision` is non-zero**.
- Adds `"frontier"` (a list copy) **only when `self.frontier` is non-empty** — the same emit-when-set pattern as `"revision"`.
- Adds `"payload_contract"` (a per-node dict copy) **only when `self.payload_contract` is non-empty** — same emit-when-set pattern. So a plan compiled without a `payload_tier_fn` never carries the key.

So a first-compile plan with no retriever, no scheduler frontier, and no payload tiering is **byte-for-byte unchanged**. This inlines each `BuildPlanEntry`'s full payload (wrapper source, dockerfile, `create_agent_runtime` request) — potentially megabytes.

> Because `"precedents"`, `"revision"`, `"frontier"`, and `"payload_contract"` are omitted when empty/zero, do **not** assume those keys are always present. Use `.get(...)`.

### `ProvisioningPlan.to_summary_dict`

```python
def to_summary_dict(self) -> dict
```

A **compact**, navigable projection of the plan for a durable plan note (AI-18) — a read-only projection that influences no dispatch and mutates nothing. It **drops** the bulky deploy payloads that `to_dict` inlines, while preserving the compiled topology so a note can render the DAG. Returns:

- `"order"` — a list copy.
- `"wiring"` — `{node: [{"producer", "path", "input_name"}, ...]}` from each `AgentRef`.
- `"entries"` — a per-node **hosting digest**: `{"build_mode", "protocol", "port", "fingerprint", "ecr_repo", "has_wrapper", "has_dockerfile"}`, where `protocol`/`port` come from the entry's `invoke` dict and `has_wrapper`/`has_dockerfile` report whether those artifacts were synthesized.
- `"payload_contract"` — the per-node contract (`{node: {"trust_tier", "static_context"}}`), included **only when non-empty** (same emit-when-set rule as [`to_dict`](#provisioningplanto_dict)). This is the one bulky-adjacent field the summary preserves, since the frozen tiered context is what makes a plan note self-contained; a plan compiled without a `payload_tier_fn` omits the key.

```python
note = plan.to_summary_dict()   # compact hosting digest for a durable plan note
```

---

## `AssemblyError`

```python
class AssemblyError(ValueError)
```

Raised when a DAG/manifest set cannot be compiled into a provisioning plan. Subclass of `ValueError`. Raised by [`assemble`](#orchestrationassemblerassemble) when a DAG node has no manifest, and by [`recompile`](#orchestrationassemblerrecompile) when `dag`/`manifests` are missing or the fresh compile fails.

> `assemble` raises `AssemblyError` **only** for the missing-manifest case. Other failures (topology, manifest, alignment) propagate as their own error types (`DAGError`, `ManifestError`, `AlignmentError`) unchanged.

## `MonotonicityError`

```python
class MonotonicityError(AssemblyError)
```

Raised when a re-compile would edit, remove, or reorder an already-executed node — or drop/reorder any already-planned node — or when the revision cap is exceeded. Subclass of `AssemblyError` (hence also `ValueError`). Enforces the adaptive-compiler contract (AI-20): every plan mutation must be a bounded, monotonic superset that pins already-executed nodes so resume stays a faithful replay.

## `DEFAULT_MAX_REVISIONS`

```python
DEFAULT_MAX_REVISIONS = 16
```

Module-level `int` in [`assemble.py`](../../src/concursus/assemble/assemble.py). The default ceiling on monotonic re-compiles, making the outer plan-generation feedback loop **bounded** so a mis-behaving planner can never re-compile without end. Used as the default for `recompile`'s `max_revisions` parameter.

## `DEFAULT_MAX_NODES`

```python
DEFAULT_MAX_NODES = 12
```

Module-level `int` in [`planner.py`](../../src/concursus/assemble/planner.py). The default node cap for the `decompose=True` complexity contract; the default for [`plan_from_goal`](#plan_from_goal)'s `max_nodes` parameter. A DAG with more nodes raises [`PlanAuthorError`](#planauthorerror) at author time.

## `DEFAULT_MAX_DEPTH`

```python
DEFAULT_MAX_DEPTH = 6
```

Module-level `int` in [`planner.py`](../../src/concursus/assemble/planner.py). The default longest-path-depth cap for the `decompose=True` complexity contract; the default for `plan_from_goal`'s `max_depth` parameter. A deeper DAG raises `PlanAuthorError` at author time.

## `DEFAULT_MAX_FANOUT`

```python
DEFAULT_MAX_FANOUT = 6
```

Module-level `int` in [`planner.py`](../../src/concursus/assemble/planner.py). The default fan-out cap for the `decompose=True` complexity contract; the default for `plan_from_goal`'s `max_fanout` parameter. A wider fan-out raises `PlanAuthorError` at author time. (The offline template always emits a linear chain, so fan-out is 1 there.)

---

## See also

- [Guide: Compiling & Running a Team](../guides/compiling-and-running.md) — the full `resolve → assemble → freeze → supervise` pipeline, plus `recompile` and `plan_from_goal`.
- [Guide: Reasoning](../guides/reasoning.md) — forming a plan by deliberation and lowering it to a DAG *before* compile.
- [Core Concepts](../concepts.md) — DAG, manifest, plan, and the monotonicity invariant in prose.
- [API Reference: reasoning](reasoning.md#reasoningdeliberate) — the deliberation tier that exports `unroll_static_fanout`, the compile-time static fan-out rewrite applied before `assemble`.
- [API Reference: core](core.md) — `AgentDAG`, `AgentManifest`, `AgentRef`, and the resolver `assemble` calls.
- [API Reference: build](build.md) — `BuildPlanEntry` and `RuntimeBuilderFactory.synthesize`, which populate a plan's `entries`.
- [API Reference: execute](execute.md) — the `Supervisor` that consumes a frozen plan.
- [API Reference: state](state.md) — `PrecedentRetriever` and `StateStore.completed()`.
- [Documentation index](../README.md)
