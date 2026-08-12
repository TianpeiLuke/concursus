# Guide: Compiling & Running a Team

*The compile pipeline — resolve → assemble → freeze → supervise — plus the one sanctioned re-compile and authoring a plan from a goal.*

This guide walks the core path of Concursus: how a declared `AgentDAG` plus per-agent
manifests become a frozen, replayable `ProvisioningPlan`, and how the `Supervisor` drives that
plan to completion in a single forward pass. It also covers the two authoring seams that sit at
the *front* of the compiler — `plan_from_goal` (generate the topology) and `OrchestrationAssembler.recompile`
(the only sanctioned way to mutate a plan).

> **The load-bearing invariant.** *Concursus is a compiler, not a runtime governor.* A run is
> `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run` — a single static forward pass
> over an immutable plan. Every generative or mutating step (reasoning, plan authoring, a governor
> round) happens **strictly before** `assemble`. Resume is replay of an append-only log, never a
> re-plan mid-flight; the governor loop is strictly **outer** and never reaches inside a running
> `Supervisor` nor edits a frozen plan.

---

## The shape of a run

```python
from concursus import (
    AgentDAG,
    AgentManifest,
    OrchestrationAssembler,
    Supervisor,
)

# 1. Declare the topology (a -> b means b depends on a).
dag = AgentDAG()
for n in ["ingest", "summarize", "critique"]:
    dag.add_node(n)
dag.add_edge("ingest", "summarize").add_edge("summarize", "critique")

# 2. Load the per-node manifests, keyed by node id.
manifests = {
    "ingest": AgentManifest.from_yaml("agents/ingest.agent.yaml"),
    "summarize": AgentManifest.from_yaml("agents/summarize.agent.yaml"),
    "critique": AgentManifest.from_yaml("agents/critique.agent.yaml"),
}

# 3. Compile — pure, offline; returns a frozen ProvisioningPlan.
asm = OrchestrationAssembler(account="123456789012", region="us-east-1")
plan = asm.assemble(dag, manifests)

# 4. Run — one forward pass over the frozen plan.
sup = Supervisor(plan, manifests, invoke_fn=my_invoke)
outputs = sup.run({"source_url": "s3://bucket/doc"})   # -> {node_id: output_dict}
```

Steps 1–3 are the **compiler** (a value; no AWS, no side effects). Step 4 is the **runtime**.
The boundary between them is the frozen `ProvisioningPlan`.

Declaring the topology and writing manifests is covered in
[Authoring Agents](authoring-agents.md); this guide picks up at `assemble`.

---

## The compile pipeline: what each stage guarantees

`OrchestrationAssembler.assemble(dag, manifests)` is the offline convergence point. It runs four
stages in order and freezes the result. Each stage is a distinct guarantee — if any fails, no plan
is produced.

| Stage | What runs | Guarantee | Raises |
|---|---|---|---|
| **validate** | `dag.validate()`, then `manifest.validate()` for every manifest | Topology is acyclic; every manifest has a non-empty name, a `container_uri`/`agent_runtime_arn`, an `HTTP`/`MCP`/`A2A` protocol, and a non-empty output schema | `DAGError`, `ManifestError` |
| **resolve** | `check_alignment(dag, manifests)` | Every `depends_on` edge type-aligns: known producer, a declared producer output field, a declared consumer input, **and** a matching `producer -> consumer` edge in the DAG | `AlignmentError` |
| **build** | `RuntimeBuilderFactory.synthesize(...)` per node | Each node has a `BuildPlanEntry` (packaging + `create_agent_runtime` params); a node with no manifest is rejected | `AssemblyError` |
| **assemble** | `resolve_edges(...)` + `dag.topological_sort()` | Wiring is compiled into `AgentRef`s; nodes are placed in a deterministic dispatch order; everything is frozen into a `ProvisioningPlan` | — |

Two details worth internalizing:

- **`assemble` never touches AWS.** `account` / `region` are threaded into synthesized IAM roles
  *only* so the plan is previewable ahead of a real deploy. The plan is a value you can render,
  diff, and store before a single API call.
- **The `depends_on` type gate is real.** A wire only aligns when all four conditions hold. A
  producer field the schema does not declare, or a `depends_on` with no corresponding
  `add_edge` in the DAG, fails `assemble` — resolution is meaningless without the mandatory
  output schema that serves as the type gate.

See [API Reference: assemble](../reference/assemble.md) for the full contract, and the source at
[`assemble/assemble.py`](../../src/concursus/assemble/assemble.py). The alignment logic
lives in [`core/resolve.py`](../../src/concursus/core/resolve.py) and the topology in
[`core/dag.py`](../../src/concursus/core/dag.py); build synthesis is covered in
[API Reference: build](../reference/build.md).

### Opt-in contract gates: deeper resolve without touching the default

The default resolve stage is a **name-level** gate: it checks that each `depends_on` edge names a
known producer, a declared producer output field, a declared consumer input, and a matching DAG
edge. `OrchestrationAssembler` accepts two opt-in gates (both default **off**) that deepen this
same stage; they thread into `check_alignment` on both `assemble` and `recompile`, and are
compile-time only — no AWS, no runtime effect. Left off, resolve is byte-for-byte unchanged.

```python
from concursus import OrchestrationAssembler

# Default off: name-level gate only (unchanged).
plan = OrchestrationAssembler().assemble(dag, manifests)

# Opt in to the deeper gates.
asm = OrchestrationAssembler(strict_types=True, single_writer=True)
plan = asm.assemble(dag, manifests)   # AlignmentError if a wire's types clash or an input is double-fed
```

- **`strict_types` (default `False`)** — a **deep type-align** gate. Beyond checking that the
  producer output field *exists*, it checks that the field's declared JSON-Schema `type` is
  *compatible* with the consumer input's declared `type`; a concrete mismatch raises
  `AlignmentError`. It is deliberately conservative: an unknown or absent `type` on either side
  passes, and union types (e.g. `["string", "null"]`) align by set-overlap. Off = the name-level
  gate byte-for-byte.
- **`single_writer` (default `False`)** — a **non-overlap** gate. It rejects any consumer input fed
  by more than one `depends_on` edge (two edges into the same `input_name` are silent last-wins at
  run time — `payload[input_name]` is simply overwritten), raising a `single-writer violation`.
  Composable with `strict_types`.

Because a rejected wire carries the offending consumer and producer on the error, `AlignmentError`
exposes `.node` and `.producer` attributes (both `None` when the failure is not edge-specific),
so a re-binder can target the clash without parsing the message. The positional-message constructor
is unchanged.

#### `strict_fn`: dial the deep gates to a subset of nodes

Both deep gates can be **narrowed** to selected nodes via `strict_fn` — an
`Optional[Callable[[str], bool]]` (default `None`). When set, an enabled `strict_types` /
`single_writer` check is applied to a node only when `strict_fn(node)` is truthy; `None` applies the
enabled checks to *every* node. It never relaxes the name-level gate — an unaligned edge always
fails regardless of the dial.

```python
# Enforce the deep gates only on nodes an author flags as untrusted.
asm = OrchestrationAssembler(
    strict_types=True,
    strict_fn=lambda node: node.startswith("thirdparty_"),
)
```

The governor supplies a ready-made predicate for this seam — `make_trust_strictness(scheduler)`
returns a node→bool that is strict for weak/unproven agents and lean for proven ones, realizing
"strictness ∝ 1/strength off the same Trust Ladder that governs autonomy." See
[Guide: The Governor](governor.md).

---

## `ProvisioningPlan`: the frozen, replayable artifact

`assemble` returns a `ProvisioningPlan` — the compiled orchestration plan for one team. It is
treated as an immutable frozen preview (the "frozen" guarantee is by contract; the dataclass
itself is plain).

| Field | Type | What it holds |
|---|---|---|
| `order` | `List[str]` | Topological dispatch order (deterministic; lexicographic among equally-ready nodes) |
| `entries` | `Dict[str, BuildPlanEntry]` | Per-node packaging + `create_agent_runtime` params |
| `wiring` | `Dict[str, List[AgentRef]]` | Resolved producer→consumer data edges (`producer`, `path`, `input_name`) |
| `precedents` | `List[dict]` | Read-only cross-run advisory context; **never** affects `order`/`entries`/`wiring` |
| `revision` | `int` | Monotonic re-compile counter; `0` on a first `assemble` |
| `frontier` | `List[str]` | Read-only **advisory** frontier (default empty); the scheduler's cleared ready-set recorded by a `recompile(..., compile_next=...)`. **Never** affects `order`/`entries`/`wiring` |

Two renderings project the plan without mutating it:

- **`plan.to_dict()`** — the full, JSON-serializable preview (inlines each entry's wrapper source,
  dockerfile, and `create_agent_runtime` request; potentially megabytes). `precedents` is emitted
  only when non-empty, `revision` only when non-zero, and `frontier` only when non-empty, so a
  first-compile plan with no retriever is byte-for-byte stable.
- **`plan.to_summary_dict()`** — a compact, navigable projection for a durable plan note: the
  topology (`order` + `wiring`) plus a per-node hosting digest (`build_mode`, `protocol`, `port`,
  `fingerprint`, `ecr_repo`, `has_wrapper`, `has_dockerfile`), dropping the bulky deploy payloads.

Because the plan is frozen and its `order`/`wiring` are fixed, a run over it is fully replayable —
which is exactly what resume relies on (see [Durable Run State](durable-state.md)).

---

## `plan_from_goal`: authoring the topology (the compiler's front)

The `AgentDAG` does not have to be hand-built. `plan_from_goal` is the compiler's generative
**front** — it authors a topology *once*, at compile time, and hands it straight to `assemble`. It
never dispatches, never emits mid-run, and never mutates a running plan.

```python
from concursus import plan_from_goal, OrchestrationAssembler

# Default: no model injected, decompose off -> a deterministic single-node template.
# Concursus imports and runs with ZERO LLM present.
dag = plan_from_goal("resolve billing ticket")

# Generative: inject a plan-author callable (goal, precedents, directives) -> plan spec.
def my_planner(goal, precedents, directives):
    return {"nodes": ["triage", "fix"], "edges": [["triage", "fix"]]}

dag = plan_from_goal(
    "resolve billing ticket",
    plan_model_fn=my_planner,
    operator_directives={"required_nodes": ["triage"]},
)
plan = OrchestrationAssembler().assemble(dag, manifests)
```

Key facts:

- The LLM is an **injected, optional** callable (the `plan_model_fn` seam) — never imported or
  constructed here, so the package depends on no model. When `plan_model_fn=None` (and `decompose`
  is off), a trivial deterministic single-node template is used.
- `plan_model_fn` may return an already-built `AgentDAG` (returned as-is) or a plain mapping
  `{"nodes": [...], "edges": [[from, to], ...]}` (lowered via `AgentDAG.from_dict`).
- `precedents` and `operator_directives` are **read-only context** passed to `plan_model_fn`; they
  never run a topology.
- The planner only runs a cheap acyclicity/non-empty check. Alignment, wiring, and the type gate
  are `assemble`'s job — a DAG that passes `plan_from_goal` can still fail `assemble`.
- `PlanAuthorError` is raised for an empty goal (before `plan_model_fn` is ever consulted), an
  unrecognized return type, an invalid or empty spec, or a non-acyclic DAG.

### `decompose=True`: the offline capability decomposer

The single-node default is deliberately minimal — one opaque node standing in for the whole goal.
The opt-in `decompose=True` mode (default **off**) instead emits a deterministic, **offline**
multi-node **capability** chain, still with no LLM present. It is a third path alongside the
single-node default and the injected-model seam:

```python
# Opt-in: deterministic multi-node capability chain, still zero LLM.
dag = plan_from_goal("investigate elevated checkout latency", decompose=True)
# nodes: investigate_elevated_che__scope
#        investigate_elevated_che__gather_evidence
#        investigate_elevated_che__hypothesize
#        investigate_elevated_che__verify
# the goal is slug'd (non-alphanumerics -> "_") and truncated to 24 chars, then "__<stage>";
# a LINEAR chain (fan-out 1), each edge stage[i] -> stage[i+1]
```

What `decompose=True` guarantees:

- **Node ids are agent-agnostic capability labels**, shaped `<goalslug>__<stage>` — where
  `<goalslug>` is the goal lowercased, non-alphanumerics collapsed to `_`, truncated to 24 chars,
  and then **right-stripped of any trailing `_`** so the `__<stage>` boundary is always a clean
  double-underscore (a goal whose 24-char truncation ended in `_` yields `…checkout__scope`, not a
  spurious `…checkout___scope` with a dangling `_` on the stage). They always contain `__` and
  **never** name an agent or a manifest. Binding a capability to a concrete agent is the
  governor/scheduler's job, downstream of the compiler.
- **The shape is chosen by a three-tier priority**: a keyword match against the goal wins first;
  else a supplied precedent's stage shape (C3, below); else the generic fallback
  `ingest -> analyze -> synthesize -> format`. The keyword table:

  | Goal keyword | Capability stages |
  |---|---|
  | `investigate` / `diagnos` / `root cause` | `scope`, `gather_evidence`, `hypothesize`, `verify` |
  | `model` / `detect` | `scope_data`, `build_model`, `calibrate`, `evaluate` |
  | `launch` / `program` | `scope`, `design`, `review`, `rollout` |
  | `migrat` | `audit_source`, `transform`, `validate_parity` |
  | `report` / `summar` | `gather`, `analyze`, `draft` |

  Every emitted chain is **linear** (fan-out 1, bounded depth).
- **Precedent priming (C3) warm-starts a novel goal from an adjacent prior run.** When the goal
  matches no keyword *and* `precedents=` is supplied, the decomposer borrows the capability-stage
  shape of the most-relevant precedent: it reads that precedent's executed `nodes`, strips each
  `<prefix>__<stage>` down to its `<stage>` suffix, and re-prefixes the ordered, de-duplicated
  stages onto the new goal's slug. So a precedent that ran `refund_probe__scope`,
  `refund_probe__gather_evidence`, `refund_probe__verify` lends a novel goal the shape
  `<goalslug>__scope -> …__gather_evidence -> …__verify`. It stays fully deterministic and offline
  (precedents are read-only context; no LLM). A precedent with no usable multi-stage capability
  shape (a single opaque node) is ignored, and the generic fallback is used. Keyword always beats
  precedent, so an explicitly-recognized goal never defers to a borrowed shape.
- **An injected `plan_model_fn` always wins.** Passing `plan_model_fn` overrides the template even
  when `decompose=True`, so the decomposer is strictly the no-model multi-node path.
- **The per-sub-task complexity contract is enforced at author time.** `max_nodes` (default
  `12`), `max_depth` (default `6`, the longest path), and `max_fanout` (default `6`) bound the
  authored DAG; exceeding any of them raises `PlanAuthorError` before the plan ever reaches
  `assemble`. (Module constants `DEFAULT_MAX_NODES` / `DEFAULT_MAX_DEPTH` / `DEFAULT_MAX_FANOUT`
  supply these defaults; when `decompose=True` the same bounds are also checked against a DAG an
  injected model returns.)

Source: [`assemble/planner.py`](../../src/concursus/assemble/planner.py). For forming a
plan by bounded deliberation *before* this step, see [The Reasoning Tier](reasoning.md).

---

## `unroll_static_fanout`: compile-time static fan-out unrolling

Some topologies want *N* identical branches of one node — a map-style fan-out. `unroll_static_fanout`
is the **compile-time** way to get them: it rewrites an `AgentDAG` into a wider, still-frozen `AgentDAG`
*before* `assemble` freezes it, so the resulting plan runs its branches in the same single static pass.
It is opt-in and **default-off**; called with no spec it returns the input DAG **unchanged (the same
object)**, so a caller that never asks for unrolling gets a byte-identical plan.

```python
from concursus import AgentDAG, OrchestrationAssembler
from concursus.reasoning.deliberate import unroll_static_fanout

dag = AgentDAG()
for n in ["fetch", "shard", "reduce"]:
    dag.add_node(n)
dag.add_edge("fetch", "shard").add_edge("shard", "reduce")

# Opt in: expand `shard` into 3 frozen parallel branches at compile time.
wide = unroll_static_fanout(dag, {"shard": 3})
# nodes: fetch, shard__fe0, shard__fe1, shard__fe2, shard__gather, reduce
# fetch scatters to every shard__feN; each shard__feN feeds shard__gather; gather -> reduce.
plan = OrchestrationAssembler().assemble(wide, manifests)
```

What it guarantees:

- **`unroll = {base_node: N}` is a DECLARED, data-INDEPENDENT bound.** Each named `base` is cloned into
  `N` namespaced branches `f"{base}__fe{i}"` (`i` in `0..N-1`); every upstream producer of `base`
  **scatters** its shared input to all `N` clones; and a synthetic **gather** node `f"{base}__gather"`
  collects the clone outputs, onto which every original downstream consumer of `base` is re-pointed.
- **Only `N >= 2` unrolls.** `N == 1` is a degenerate no-op (the base is left in place). A base id absent
  from the DAG, or a non-int / `N < 1` count, raises `DAGError` at compile — a spec error caught up
  front, never a silent mis-compile. Unbounded / data-dependent fan-out is deliberately out of scope.
- **It is purely a topology rewrite before `assemble`.** The result is a new frozen `AgentDAG` whose
  `validate()` passes, so `Supervisor` runs the `N` branches + the gather in ONE pass over the frozen
  `plan.order` — no runtime graph mutation, no dynamic split. The default path (no `unroll`) is
  byte-for-byte unchanged.

Source: [`reasoning/deliberate.py`](../../src/concursus/reasoning/deliberate.py). For forming
the DAG this widens by bounded deliberation, see [The Reasoning Tier](reasoning.md).

---

## `recompile`: the only sanctioned plan mutation

A frozen plan is never edited in place. When a run needs to grow — new nodes discovered, the goal
extended — the sanctioned path is `OrchestrationAssembler.recompile`, which emits a **fresh, frozen,
monotonic-superset** plan that supersedes the prior one. The feedback edge lives *around* the
compiler (run → distill → precedent → next compile), never inside `Supervisor.run`.

```python
from concursus import InProcessStateStore, OrchestrationAssembler, Supervisor

store = InProcessStateStore()
sup = Supervisor(plan, manifests, invoke_fn=my_invoke, state_store=store)
sup.run(inputs)

# Re-author the topology (extended_dag is a superset of the prior dag), then re-compile.
next_plan = asm.recompile(
    plan,
    completed=store.completed(),   # already-executed node ids are pinned
    dag=extended_dag,
    manifests=manifests,
)
# next_plan.revision == plan.revision + 1; `plan` is never mutated.
```

`recompile` re-runs a full `assemble`, then guards monotonicity:

| Guard | Rule | On violation |
|---|---|---|
| **Bounded** | `revision` must not exceed `max_revisions` (default `16`) — checked *first* | `MonotonicityError` |
| **Order preserved** | The prior `order` must survive as a subsequence of the new `order` (new nodes may be interleaved; none may be dropped or reordered) | `MonotonicityError` |
| **Executed nodes frozen** | Every node in `completed` must still be present, with a byte-identical `BuildPlanEntry` and unchanged wiring | `MonotonicityError` |

Executed nodes are then **pinned** to their prior `entries`/`wiring`, so a resumed run replays them
byte-identically; newly-added nodes take the freshly-compiled entry/wiring. `dag=` and `manifests=`
are typed `Optional` but effectively required — passing `None` raises `AssemblyError`. `content_hashes`
is accepted as read-only provenance and does not relax the guard. `MonotonicityError` is a subclass
of `AssemblyError` (hence also `ValueError`).

The optional `compile_next=` kwarg (default off) closes the previously-dead scheduler→compiler
channel: it records the scheduler's cleared ready-frontier onto the fresh plan's read-only
`ProvisioningPlan.frontier` field (filtered to topology nodes). It is purely **advisory** — it
never changes `order`, `entries`, or `wiring`, so the monotonic superset above is preserved exactly.

```python
# Feed the governor's cleared frontier forward as advisory context on the next plan.
next_plan = asm.recompile(
    plan,
    completed=store.completed(),
    dag=extended_dag,
    manifests=manifests,
    compile_next=["triage", "fix"],   # -> next_plan.frontier; order/wiring unchanged
)
```

This is what keeps resume honest: a re-compile can only *add* — it can never rewrite what the
supervisor already ran.

### `redrive_until_valid`: the bounded validate-and-retry budget

Re-compiling a plan is one of the OS's two plan-author feedback edges; re-driving a single node whose
output failed its schema is the other. `OrchestrationAssembler.redrive_until_valid` is that second edge —
an **opt-in, bounded** helper a plan-author driver *around* the compiler may call. It is **not** wired
into `assemble` / `recompile` (whose default output is byte-for-byte unchanged) and is **never** inside
`Supervisor.run` (which stays a single static pass over a frozen `plan.order`).

```python
asm = OrchestrationAssembler(account="123456789012", region="us-east-1")

def drive(context):
    # caller-owned: invoke the node with the (error-augmented) context, return its output dict.
    return call_my_agent(manifest, context)

# Re-drive one node until its output satisfies the manifest schema, or the bounded budget is spent.
output = asm.redrive_until_valid(
    "summarize",
    manifest=manifests["summarize"],
    drive_fn=drive,
    base_context={"source_url": "s3://bucket/doc"},
    max_retries=2,          # clamped to [0, max_revisions]
)
```

- Each attempt calls `drive_fn(context)` and shape-checks the result against `manifest.output_schema`
  via `validate_output`. On a `SchemaError` the failing reason is fed back into the **next** attempt's
  `context` under `error_key` (default `"validation_error"`) and the node is re-driven; the first
  schema-valid output is returned. Once the budget is exhausted the last `SchemaError` re-raises.
- **The retry count shares the recompile ceiling.** `retry_budget(max_retries, max_revisions=…)` clamps
  a requested `max_retries` to `[0, max_revisions]` (`DEFAULT_MAX_REVISIONS` is `16`), so BOTH plan-author
  edges — re-compile / re-drive — are bounded by one dial. A negative request floors at `0` (no retry);
  a request above the ceiling is capped, so a caller can never open an unbounded loop. At most
  `1 + retry_budget(max_retries)` drives run.
- It touches **no plan, no `StateStore`, and no AWS** — a purely functional loop the caller owns. It
  never mutates a frozen plan and adds no compiler loop; the default compile path is unchanged.

---

## `Supervisor.run`: topological dispatch over the frozen plan

The `Supervisor` is the runtime half. It walks `plan.order` exactly once and, for each pending
node, assembles a payload, invokes the agent, validates the output, and threads it forward through
a resumable `StateStore`.

```python
sup = Supervisor(plan, manifests, invoke_fn=my_invoke)
outputs = sup.run({"source_url": "s3://bucket/doc"})   # {node_id: output_dict}
# run(inputs, *, parallel=1): parallel is opt-in; the default 1 is the serial pass below.
```

What happens per node, in order:

1. **Resume-by-skip.** If the node is already in `store.completed()`, it is skipped — its validated
   output was recorded on a prior run. This is how resume works: a re-run over the same store picks
   up exactly where it left off, no re-plan.
2. **Block check.** If any producer this node consumes has not completed, a `failed` record with a
   `blocked_on` reason is written and the node is skipped, so `extract` never hits a
   missing-producer `KeyError`.
3. **Payload assembly.** The payload starts from the node's external inputs, then each wiring
   `AgentRef` overlays `payload[ref.input_name] = extract(store.get(ref.producer), ref.path)` — the
   resolved upstream output the resolver promised.
4. **Invoke.** The injected `invoke_fn(arn, qualifier, session_id, payload_bytes)` is called.
5. **Validate.** The result is shape-checked against the manifest's output schema; a missing
   required field raises `SchemaError`.
6. **Record.** The validated output is written to the store (with its `producer` / `consumes` /
   `schema` metadata) and becomes available to dependents.

The return is `{node: store.get(node) for node in plan.order if node in store.completed()}` — only
completed nodes appear; failed, blocked, and held nodes are omitted from the return but visible via
`sup.summary()` and `sup.index()`.

### External inputs vs. wired inputs

`run(inputs)` takes the external run-input mapping. Resolution per node:

- If `inputs[node]` is a dict, that block is the node's external inputs.
- Otherwise, for a **source** node (no inbound wiring), the whole top-level `inputs` mapping is used.
- Wired inputs are always overlaid on top, per the `AgentRef` wiring.

So `run({"source_url": ...})` feeds the source node, while `run({"ingest": {"source_url": ...}})`
targets a specific node's block.

### One session id → warm microVMs

A single stable `runtimeSessionId` (≥ 33 chars) spans every invoke in a run, exposed as
`sup.session_id`. This gives session affinity across the AgentCore data plane — invocations sharing
a session land on warm microVMs and shared session memory.

The invoke transport is injectable via `invoke_fn`. The default lazily binds boto3's
`bedrock-agentcore` client, so importing the module needs no AWS SDK; without the `[agentcore]`
extra a real invoke raises `RuntimeError` at call time (not import time). Deploying real runtimes is
covered in [Deploying to AWS Bedrock AgentCore](deploying-to-agentcore.md).

### Fail-fast vs. resilient

Default construction (`on_error='raise'`, `max_attempts=1`) is a byte-for-byte fail-fast single
forward pass: any invoke/validate/integrity exception propagates unchanged. With `on_error='record'`
a terminal failure is recorded (not raised) and retried up to `max_attempts`; the pass continues so
a failure prunes only its dependent subtree while independent branches still return. `sup.summary_line()`
renders the outcome, e.g. `completed 4/6; node summarize failed; node critique blocked on summarize`.

Structural validation (dangling `AgentRef` producer, or a cycle in the wiring) happens **once** at
construction — `run()` contains no structural re-check loop, so it stays a static single pass.

### `run(parallel=N)`: bounded within-node parallelism

`run(inputs, *, parallel=1)` takes an opt-in `parallel` bound. At the default `parallel=1` this is
**exactly** the serial single pass above — byte-for-byte unchanged. At `parallel > 1` the pass instead
dispatches each dispatchable **antichain** — every still-open node whose `plan.wiring` producers are all
completed — concurrently on a bounded `ThreadPoolExecutor(max_workers=parallel)`, waits for the wave,
then recomputes the next antichain until every node is completed or none is dispatchable.

```python
# Opt in to an antichain-parallel wave; default parallel=1 is the serial pass.
outputs = sup.run({"source_url": "s3://bucket/doc"}, parallel=4)
```

This is **not** a new execution model — it is still a single static pass over the **frozen** `plan.order`,
never mutated, never replanned:

- **Results are byte-identical to the serial run.** A node is dispatched ONLY after all its producers
  completed, so its resolved inputs are identical regardless of intra-wave completion order. Per-node
  outputs, statuses, `consumes` edges, and content hashes match `parallel=1` exactly; only the
  store-local `seq` / `timestamp` reflect physical put order. The run is order-independent — the same
  store contents for any `parallel`.
- **`on_error` semantics are unchanged.** `'raise'` surfaces the first wave failure (fail-fast);
  `'record'` writes one failed record per node and lets the pass continue (a failure prunes only its
  dependent subtree). A held node is never dispatched, and a node whose producer failed/was held is
  recorded `blocked_on` exactly as the serial pass does. Resume=replay is likewise unchanged.
- **The request is CLAMPED by host CPU capacity.** `parallel` is passed through
  `resolve_ceiling(parallel, cpu_capacity)` — the same `max(1, min(pref, cap))` shape the inner graph's
  fan-out uses (capacity hard-capped by `MAX_FANOUT_CAP = 64`). A soft `parallel` request can only
  *tighten* the pool below the host's capacity, never spawn more workers than the host can serve. It
  shrinks only the worker-pool **width**, never the set of nodes dispatched — so determinism is untouched.

### Resume plan-identity guard (`verify_plan_identity`)

Resume is replay of the append-only log against a **frozen** plan. If the plan handed to `run` on resume
is *not* the same plan the log was recorded under, silently skipping the recorded `completed()` nodes
would mis-replay — a node id could now carry different wiring/entry. The opt-in `verify_plan_identity`
constructor flag (default `False`) turns that latent, silent hazard into a loud, legible error.

```python
sup = Supervisor(plan, manifests, invoke_fn=my_invoke,
                 state_store=store, verify_plan_identity=True)
sup.run(inputs)   # first pass: records plan_fingerprint(plan) under a reserved store id
# ... later, resuming under the SAME store:
Supervisor(divergent_plan, manifests, invoke_fn=my_invoke,
           state_store=store, verify_plan_identity=True).run(inputs)
# -> PlanIdentityError: the persisted hash != this plan's hash
```

- With the default `verify_plan_identity=False`, resume is byte-for-byte unchanged: a node in
  `completed()` is skipped with no identity check.
- When `True`, the first pass under a store persists this frozen plan's content-hash — `plan_fingerprint(plan)`,
  a stable hash of `order` + `wiring` + per-node entry `fingerprint` — under a reserved store id (**not** a
  DAG node, so `run`'s `{node: output}` return is unchanged). On any later resume it ASSERTS the persisted
  hash equals the current plan's hash **before** skipping/replaying any completed node, raising
  `PlanIdentityError` on mismatch instead of mis-replaying under a divergent plan.
- It is a **verification, never a rebind**: it never mutates `plan.order` and adds no compiler loop. The
  remedy on mismatch is to re-compile monotonically (`recompile`), not to resume a divergent plan.

### Custom node kinds (`NODE_EXECUTORS`, `node_executors=` / `node_kind_fn=`)

Node dispatch has exactly one uniform path today. `NODE_EXECUTORS` is the opt-in Strategy/Registry seam
that generalizes it without changing the default. A node executor is a uniform
`(supervisor, node, inputs, wiring) -> None` handler; the shipped `NODE_EXECUTORS` registry carries only
the default kind (`"default"`), which delegates verbatim to `Supervisor._dispatch`.

```python
from concursus.execute.supervisor import Supervisor, NODE_EXECUTORS

def my_kind(supervisor, node, inputs, wiring):
    # a custom handler shares the same store / on_error / retry path as the default.
    supervisor._dispatch(node, inputs, wiring)

sup = Supervisor(
    plan, manifests, invoke_fn=my_invoke,
    node_executors={"custom": my_kind},              # layered atop the shipped default kind
    node_kind_fn=lambda node: "custom" if node.startswith("x_") else "default",
)
```

- With **neither** `node_executors=` nor `node_kind_fn=` (the default), every node routes to the default
  kind → `_dispatch`, so the run is byte-for-byte unchanged.
- A caller registers custom kinds via `node_executors=` (copied atop the shipped `NODE_EXECUTORS`, so no
  test mutates shared global state) and selects a kind per node via `node_kind_fn=`. A selected kind with
  no registered handler falls back to the default handler.
- Every handler shares the same uniform interface, so a custom kind rides the identical store / `on_error`
  path; it never mutates the frozen `plan.order` and adds no loop.

Source: [`execute/supervisor.py`](../../src/concursus/execute/supervisor.py); full contract
in [API Reference: execute](../reference/execute.md).

---

## Recap: the invariant, restated

- **Compile is a value.** `assemble` is pure and offline; it produces a frozen `ProvisioningPlan`
  with a fixed `order`/`wiring`.
- **Run is one forward pass.** `Supervisor.run` walks that frozen order once, never mutating it.
- **Mutation is out-of-band.** The only sanctioned way to change a plan is `recompile` — a bounded,
  monotonic superset that pins already-executed nodes. Everything generative (`plan_from_goal`,
  reasoning, a governor round) happens strictly before `assemble`.
- **Resume is replay.** A re-run over the same `StateStore` skips completed nodes and continues —
  never a re-plan mid-flight.

*Concursus is a compiler, not a runtime governor.*

The opt-in seams above — the deeper resolve gates, `unroll_static_fanout`, `redrive_until_valid`,
antichain parallelism, `verify_plan_identity`, and the node-kind registry — are the flexibility &
robustness layer completed in v0.6.0. Every one of them is **opt-in, default-off**: wire none of
them and the `assemble → run` path is byte-for-byte the original.

---

## See also

- [API Reference: assemble](../reference/assemble.md) — `OrchestrationAssembler`, `ProvisioningPlan`, `plan_from_goal`.
- [API Reference: execute](../reference/execute.md) — the `Supervisor` in full.
- [Guide: Durable Run State](durable-state.md) — the `StateStore` seam, backends, and replay-resume.
- [Guide: Authoring Agents](authoring-agents.md) — writing the manifests `assemble` consumes.
- [Guide: The Governor](governor.md) — the strictly-outer loop that schedules and re-compiles around the compiler.
- [Guide: The Reasoning Tier](reasoning.md) — form a plan by bounded deliberation before compile.
- [Core Concepts](../concepts.md) · [Getting Started](../getting-started.md) · [Documentation Index](../README.md)
