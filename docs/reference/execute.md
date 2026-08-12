# API Reference: `execute`

*The `Supervisor` — topological dispatch over a frozen plan.*

The `execute` tier is the **runtime half** of Concursus. It takes a frozen `ProvisioningPlan` (produced by the `assemble` tier) and drives its agents to completion: walk `plan.order`, build each agent's invoke payload from external run inputs overlaid with resolved upstream outputs, call an injectable transport, shape-check the result against the manifest's output schema, and thread every validated output forward through a resumable `StateStore`. A single module — `execute.supervisor` — carries the whole tier.

> **The load-bearing invariant.** *Concursus is a compiler, not a runtime governor.* `Supervisor.run` is a single static forward pass over an immutable plan; it never re-plans, never mutates `plan.order` / `plan.wiring`, and contains no structural re-check loop. Structural validation of the plan happens **once**, at construction. Resume is replay of an append-only log, never a re-plan mid-flight.

Source: [`../../src/concursus/execute/supervisor.py`](../../src/concursus/execute/supervisor.py)

| Symbol | Kind | Summary |
|---|---|---|
| [`InvokeFn`](#invokefn) | type alias | The injectable invoke transport: `(arn, qualifier, session_id, payload_bytes) -> dict`. |
| [`SchemaError`](#schemaerror) | exception | Raised when an agent's output fails its declared output schema. |
| [`PlanIdentityError`](#planidentityerror) | exception | Raised when a resume replays against a plan whose `plan_fingerprint` differs from the persisted run. Opt-in guard, default off. |
| [`plan_fingerprint`](#plan_fingerprint) | function | Stable content-hash of a plan's compiled identity (`order` + `wiring` + `entries`) — the resume=replay identity guard. |
| [`validate_output`](#validate_output) | function | Minimal (no-`jsonschema`) shape check: `obj` is a dict and every required property is present. |
| [`check_hive_contract`](#check_hive_contract) | function | Opt-in storability gate: the output must be JSON-serializable (storable by the OS log). Default off. |
| [`check_acceptance`](#check_acceptance) | function | Opt-in post-run QA gate: each output field's declared `acceptance` contract must hold. Default off. |
| [`NodeExecutor`](#nodeexecutor--node_executors) | type alias | A uniform `(supervisor, node, inputs, wiring) -> None` node-kind handler (opt-in Strategy/Registry dispatch seam). |
| [`NODE_EXECUTORS`](#nodeexecutor--node_executors) | registry | The shipped node-kind registry, seeded with the single `"default"` kind → `_dispatch`. Instances copy it. |
| [`Supervisor`](#supervisor) | class | Drives a `ProvisioningPlan` to completion in topological order. |
| [`Supervisor.session_id`](#supervisorsession_id) | property | The stable per-run `runtimeSessionId` shared across every invoke. |
| [`Supervisor.run`](#supervisorrun) | method | One forward pass over `plan.order` (opt-in bounded `parallel=N` antichain wave); returns `{node_id: output_dict}` for completed nodes. |
| [`Supervisor.context`](#supervisorcontext) | method | Transitive upstream context for a node, rebuilt from recorded `consumes` edges. |
| [`Supervisor.index`](#supervisorindex) | method | A `RunIndex` over the run's log for tree traversal and metadata queries. |
| [`Supervisor.summary`](#supervisorsummary) | method | Read-only partial-run summary derived purely from the store log (incl. `failure_classes`: per-class terminal-failure counts). |
| [`Supervisor.summary_line`](#supervisorsummary_line) | method | One-line human rendering of `summary()` for the CLI failure path. |

There is **no `__all__`** in the module; the public API is the non-underscore symbols above. Four are re-exported from the package root:

```python
from concursus import Supervisor, SchemaError, PlanIdentityError, plan_fingerprint
```

`InvokeFn`, `validate_output`, `check_hive_contract`, `check_acceptance`, `NodeExecutor`, and `NODE_EXECUTORS` are public but *not* re-exported at the root — import them from the module:

```python
from concursus.execute.supervisor import (
    InvokeFn, validate_output, NodeExecutor, NODE_EXECUTORS,
)
```

The `Supervisor` walks a plan produced by the [`assemble` tier](../guides/compiling-and-running.md) and writes through the [`StateStore` seam](../guides/durable-state.md). For where it sits in the compile-then-run pipeline, see [Compiling & Running a Team](../guides/compiling-and-running.md).

---

## `InvokeFn`

```python
InvokeFn = Callable[[str, str, str, bytes], dict]
```

The type alias for the injectable **invoke transport**. A callable taking four positional arguments and returning the parsed output dict:

| Position | Param | Meaning |
|---|---|---|
| 1 | `arn: str` | The AgentCore runtime ARN to invoke. |
| 2 | `qualifier: str` | The runtime qualifier (defaults to `"DEFAULT"`, or `manifest.registry["qualifier"]`). |
| 3 | `session_id: str` | The stable per-run `runtimeSessionId` (see [`session_id`](#supervisorsession_id)). |
| 4 | `payload: bytes` | The assembled invoke payload, JSON-encoded. |

Passed as `invoke_fn=` to the [`Supervisor`](#supervisor). When `invoke_fn=None`, the constructor falls back to a default transport (`_default_invoke_fn`) that lazily binds boto3's `bedrock-agentcore` data-plane client — so importing this module needs no AWS SDK, and a unit test that injects a fake transport imports fine without the `[agentcore]` extra. Without that extra, a *real* invoke raises `RuntimeError` at call time (not import time), pointing you at `pip install concursus[agentcore]`. See [Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md).

```python
def fake_invoke(arn: str, qualifier: str, session_id: str, payload: bytes) -> dict:
    return {"summary": "..."}   # a parsed output dict

sup = Supervisor(plan, manifests, invoke_fn=fake_invoke)
```

---

## `SchemaError`

```python
class SchemaError(ValueError)
```

Raised when an agent's output fails to satisfy its declared output schema. Subclasses `ValueError`, so callers may catch either. Raised by [`validate_output`](#validate_output) when the output is not a dict or is missing a required field. Under the default `on_error='raise'` it propagates unchanged (fail-fast); under `on_error='record'` it is caught and written as a `failed` record instead.

---

## `PlanIdentityError`

```python
class PlanIdentityError(ValueError)
```

Raised when a **resume replays against a divergent plan** — the plan handed to [`Supervisor.run`](#supervisorrun) on resume hashes ([`plan_fingerprint`](#plan_fingerprint)) differently from the plan the store's log was originally recorded under. Subclasses `ValueError`. This is the opt-in **resume=replay identity guard**: the append-only log is the single source of truth, and resume is replay of that log against a *frozen* plan — so silently skipping the recorded `completed()` nodes under a plan whose wiring/entry changed would mis-replay. The guard turns that latent, silent hazard into a loud, legible error, steering you to re-compile monotonically (see the `assemble` tier's `recompile`) rather than resume against a divergent plan.

**Default off** — this check runs only when the [`Supervisor`](#supervisor) is constructed with `verify_plan_identity=True`. With the default `verify_plan_identity=False`, resume is byte-for-byte the original completed-node skip and this error is never raised. See [Resuming by skipping](#resuming-by-skipping).

---

## `plan_fingerprint`

```python
def plan_fingerprint(plan: "ProvisioningPlan") -> str
```

A stable **content-hash of a plan's compiled identity** — the basis of the resume=replay guard above. Pure and duck-typed; reads only three attributes (missing attributes default to empty, so a duck-typed `.order`/`.wiring` stand-in with no `entries` is fully supported):

- `plan.order` — the topological node list.
- `plan.wiring` — the resolved `AgentRef` producer→consumer edges, projected to `[producer, path, input_name]` triples in their **exact list order** (reordering an edge is a real difference, so order is preserved, not sorted).
- `plan.entries` — each `BuildPlanEntry`'s deploy-identity `fingerprint` (the stable per-node hosting hash, not the bulky wrapper/dockerfile body).

Two plans with the same compiled identity hash identically; any change to a node's order, wiring, or hosting identity changes the hash. It is a *pure query* — it never mutates the plan and adds no compiler loop.

```python
from concursus import plan_fingerprint

fp = plan_fingerprint(plan)   # a stable content-hash str; reordering a wire changes it
```

---

## `validate_output`

```python
def validate_output(obj: Any, schema: Dict[str, Any]) -> None
```

A minimal shape check with **no `jsonschema` dependency**: assert `obj` is a dict, and that every *required* property declared by `schema` is present. Returns `None` on success.

- **Parameters:**
  - `obj` — the agent's parsed output; must be a dict.
  - `schema` — the manifest's output schema (JSON-Schema-*ish*).
- **Returns:** `None`.
- **Raises:** [`SchemaError`](#schemaerror) — if `obj` is not a dict, or lists the sorted missing required field(s) alongside the sorted present keys.

How requiredness and properties are read:

- If `schema` is **not a dict or is empty/falsy**, only the "`obj` must be a dict" check applies, then it returns (a node with no manifest is validated against `{}`, so only that check runs).
- **Properties source:** `schema["properties"]` if it is a dict; otherwise all keys of `schema` except `"required"` (the flat-map shape).
- **Required set:** the entries of `schema["required"]` (when it is a list), unioned with any property whose subschema dict has `"required": True`.

> This gate does **not** check value types, nested shapes, or reject extra keys — it only enforces that the output is a dict and that the declared required fields are present.

```python
from concursus.execute.supervisor import validate_output, SchemaError

# nested {"properties": {...}} shape
validate_output({"summary": "hi"}, {"properties": {"summary": {}}, "required": ["summary"]})  # ok

try:
    validate_output({}, {"required": ["summary"]})
except SchemaError as e:
    ...   # missing required field(s): ['summary'] (present: [])
```

---

## `check_hive_contract`

```python
def check_hive_contract(obj: Any) -> None
```

The **agent↔OS-layer storability gate**. An agent output must conform to what the OS layer routes, stores, and content-addresses — i.e. it must be **JSON-serializable** (`json.dumps(obj, sort_keys=True)` succeeds). Returns `None` on success. **Default off** — this gate runs only when the `Supervisor` is constructed with `check_acceptance=True` (see below).

- **Parameters:** `obj` — the agent's parsed output.
- **Returns:** `None`.
- **Raises:** [`SchemaError`](#schemaerror) — if `obj` is not JSON-serializable (wraps the underlying `TypeError` / `ValueError`).

[`validate_output`](#validate_output) checks dict-ness + required-key presence, but a dict carrying a non-JSON value (a `set`, a bespoke object, …) **passes** it and then **crashes** the append-only log write at `content_hash` (`json.dumps`). This gate turns that late, opaque log-write crash into an early, legible `SchemaError` **at dispatch**, so it rides the same retry/record path — a present-but-unstorable output does not complete and earns no trust.

```python
from concursus.execute.supervisor import check_hive_contract, SchemaError

check_hive_contract({"summary": "hi"})   # ok — JSON-serializable

try:
    check_hive_contract({"tags": {"a", "b"}})   # a set is not JSON-serializable
except SchemaError as e:
    ...   # the OS log/dedup cannot store it — not JSON-serializable
```

---

## `check_acceptance`

```python
def check_acceptance(obj: Any, schema: Dict[str, Any]) -> None
```

A **post-run QA gate** that is *deeper* than [`validate_output`](#validate_output): where `validate_output` only checks required-key **presence**, `check_acceptance` verifies each output field's **value** against a declared, machine-checkable `acceptance` contract — the definition of "a good output" the Trust Ladder needs (a present-but-wrong output fails here and so earns no trust). Returns `None` on success. **Default off** — runs only when the `Supervisor` is constructed with `check_acceptance=True`.

- **Parameters:**
  - `obj` — the agent's parsed output.
  - `schema` — the manifest's output schema (the same JSON-Schema-*ish* schema `validate_output` reads).
- **Returns:** `None`.
- **Raises:** [`SchemaError`](#schemaerror) — naming the offending field and the violated rule on any acceptance-contract violation.

**Conservative by default.** If `obj` is not a dict, or `schema` is not a dict / is empty, it returns immediately; a field with **no `acceptance` mapping is unconstrained**, so a manifest that declares none is never newly rejected.

**Properties source** is the same as `validate_output`: `schema["properties"]` if it is a dict, otherwise all keys of `schema` except `"required"`. Per field, its `acceptance` mapping (when present, a dict) is enforced. The rule set is **declarative and deterministic — no code eval**:

| Rule | Type | Meaning |
|---|---|---|
| `non_empty` | `bool` | When `true`, the value must be non-empty (a str/list/dict/tuple must be truthy; `None` fails). |
| `min_length` | `int` | `len(value) >= N`. |
| `max_length` | `int` | `len(value) <= N`. |
| `enum` | `list` | `value` must be one of the listed values. |
| `pattern` | `str` (regex) | a str value must **fully** match the regex (`re.fullmatch`). |

```python
from concursus.execute.supervisor import check_acceptance, SchemaError

schema = {"properties": {"summary": {"acceptance": {"non_empty": True, "min_length": 3}}}}
check_acceptance({"summary": "hello"}, schema)   # ok

try:
    check_acceptance({"summary": ""}, schema)
except SchemaError as e:
    ...   # output field 'summary' fails its acceptance contract: must be non-empty
```

---

## `NodeExecutor` / `NODE_EXECUTORS`

```python
NodeExecutor = Callable[["Supervisor", str, Dict[str, Any], List["AgentRef"]], None]

NODE_EXECUTORS: Dict[str, NodeExecutor] = {"default": _default_node_executor}
```

The opt-in **Strategy/Registry dispatch seam**. A *node executor* is a uniform `(supervisor, node, inputs, wiring) -> None` handler — the Strategy generalization of today's single, uniform node dispatch (there is exactly one kind today). `NODE_EXECUTORS` is the shipped registry, seeded only with the `"default"` kind, which maps to `_default_node_executor` — a handler that delegates **verbatim** to [`Supervisor._dispatch`](#supervisorrun). So with **no** custom kind selected (the default), a run routes every node to `"default"` → `_dispatch` and behaves **byte-for-byte as before**.

A caller registers custom node-kinds through the [`Supervisor`](#supervisor) constructor and selects them per node:

- `node_executors=` — a `{kind: NodeExecutor}` mapping layered on top of the shipped registry. Each `Supervisor` instance **copies** `NODE_EXECUTORS` at construction (`dict(NODE_EXECUTORS)`) and updates it with the supplied kinds, so no instance mutates shared global state.
- `node_kind_fn=` — a `node -> kind` selector. When `None` (default), every node uses the `"default"` kind.

A selected kind with **no registered handler falls back to the default handler** (`self._node_executors.get(kind, _default_node_executor)`). Every handler shares the same uniform signature and so rides the identical store / `on_error` / retry path; a custom kind never mutates the frozen `plan.order` and adds no compiler loop — it only changes *how* a single node is invoked, never the topology or the single-pass walk.

```python
from concursus import Supervisor

# a custom node-kind handler: same uniform signature as the default
def echo_executor(sup, node, inputs, wiring):
    # ... a specialized dispatch for this kind; may still delegate to sup._dispatch(node, inputs, wiring)
    sup._dispatch(node, inputs, wiring)

sup = Supervisor(
    plan, manifests, invoke_fn=fake_invoke,
    node_executors={"echo": echo_executor},
    node_kind_fn=lambda node: "echo" if node == "special" else "default",
)
outputs = sup.run({"topic": "x"})   # 'special' -> echo_executor; every other node -> _dispatch (unchanged)
```

> Both `node_executors` and `node_kind_fn` are opt-in. With neither supplied, dispatch is byte-for-byte the original single path.

---

## `Supervisor`

```python
class Supervisor:
    def __init__(
        self,
        plan: "ProvisioningPlan",
        manifests: Dict[str, "AgentManifest"],
        *,
        invoke_fn: Optional[InvokeFn] = None,
        session_id: Optional[str] = None,
        arns: Optional[Dict[str, str]] = None,
        state_store: Optional[StateStore] = None,
        on_error: str = "raise",
        max_attempts: int = 1,
        arn_resolver: Optional[Callable[[str, "AgentManifest"], str]] = None,
        held: Optional[Set[str]] = None,
        check_acceptance: bool = False,
        acceptance_fn: Optional[Callable[[str], bool]] = None,
        payload_tier_fn: Optional[Callable[[str], Any]] = None,  # a node -> Tier selector
        verify_plan_identity: bool = False,
        node_executors: Optional[Dict[str, NodeExecutor]] = None,
        node_kind_fn: Optional[Callable[[str], str]] = None,
    ) -> None
```

Drives a [`ProvisioningPlan`](../guides/compiling-and-running.md) to completion — offline (with an injected transport) or live (against AgentCore). It walks `plan.order`, assembles each node's payload from external inputs overlaid with resolved upstream outputs (the `plan.wiring` `AgentRef`s), invokes the injected [`InvokeFn`](#invokefn), validates the result against the manifest's output schema, and threads it forward through a resumable [`StateStore`](../guides/durable-state.md).

The `plan` is **duck-typed** on `.order` (the topological node-id sequence) and `.wiring` (`Dict[node -> List[AgentRef]]`) only.

**Parameters** (everything after `plan`/`manifests` is keyword-only):

| Param | Default | Meaning |
|---|---|---|
| `plan` | — | The frozen `ProvisioningPlan` to drive; duck-typed on `.order` + `.wiring`. |
| `manifests` | — | `{node_id: AgentManifest}`; copied via `dict(manifests)`. |
| `invoke_fn` | `None` → `_default_invoke_fn` | The [`InvokeFn`](#invokefn) transport (lazy boto3 default). |
| `session_id` | `None` → fresh ≥33-char id | The stable per-run `runtimeSessionId`. |
| `arns` | `None` | Per-node ARN overrides (see precedence below). |
| `state_store` | `None` → `InProcessStateStore()` | The [`StateStore`](../guides/durable-state.md) run state is written through. |
| `on_error` | `"raise"` | `"raise"` (fail-fast) or `"record"` (record-and-continue). |
| `max_attempts` | `1` | Per-node retry budget; retries only take effect under `on_error='record'`. |
| `arn_resolver` | `None` | Opt-in dispatch-time ARN integrity assertion (never a rebind). |
| `held` | `None` | Opt-in governance HOLD set of node ids never dispatched this episode. |
| `check_acceptance` | `False` | Opt-in post-run QA gate. When `True`, after `validate_output` the supervisor runs [`check_hive_contract`](#check_hive_contract) then [`check_acceptance`](#check_acceptance) on the invoke result. Default `False` = run byte-for-byte unchanged. |
| `acceptance_fn` | `None` | QA dial: `node -> bool` predicate narrowing the QA gate to selected nodes. `None` = every node. No effect when `check_acceptance=False`. |
| `payload_tier_fn` | `None` | Opt-in `node -> Tier` selector. When set, `_external_inputs` overlays a tiered static-context projection **under** the external inputs — caller/wired inputs win on collision. `None` = no overlay, payload byte-for-byte unchanged. |
| `verify_plan_identity` | `False` | Opt-in resume=replay identity guard. When `True`, `run()` persists this frozen plan's [`plan_fingerprint`](#plan_fingerprint) on the first pass and asserts it still matches on any resume, raising [`PlanIdentityError`](#planidentityerror) on mismatch. `False` = resume is byte-for-byte the original completed-node skip. |
| `node_executors` | `None` → copy of [`NODE_EXECUTORS`](#nodeexecutor--node_executors) | Opt-in `{kind: NodeExecutor}` custom node-kind handlers, layered atop the shipped registry (copied per instance). `None` = only the `"default"` kind. |
| `node_kind_fn` | `None` | Opt-in `node -> kind` selector for the dispatch registry. `None` = every node routes to the `"default"` kind → `_dispatch`, byte-for-byte unchanged. |

- **Raises:**
  - `ValueError` — if `on_error` is not `"raise"` / `"record"`, or `max_attempts < 1`.
  - `RunGraphError` — from the one-time construction-time structural check (`_validate_plan_structure`) on a dangling `AgentRef` producer (a wire naming a producer absent from `plan.order`) or a cycle in the wiring. (`RunGraphError` subclasses `ValueError`; see [`state/rungraph.py`](../../src/concursus/state/rungraph.py).)

**ARN resolution precedence** (per node): supplied `arns[node]` → `manifest.registry["agent_runtime_arn"]` → the placeholder `"<agent-runtime-arn>"`. Supplied `arns` for nodes lacking a manifest are added too. An unresolved placeholder ARN fails the integrity check ("deploy first") just before invoke — see [Fail-fast vs. resilient](#fail-fast-vs-resilient) and the `arn_resolver` note below.

**Default is byte-for-byte the original pass.** Construction with `on_error='raise'`, `max_attempts=1`, no `held` set, no `arn_resolver`, `check_acceptance=False`, `payload_tier_fn=None`, `verify_plan_identity=False`, no `node_executors`, and `node_kind_fn=None` — and calling `run(inputs)` with the default `parallel=1` — is the original fail-fast single forward pass. The `held` / `arn_resolver` / `check_acceptance` / `payload_tier_fn` / `verify_plan_identity` / `node_executors` / `run(parallel=N)` extensions each layer strictly on top of that path and are individually default-off.

**Opt-in output-QA gate (`check_acceptance` / `acceptance_fn`).** When `check_acceptance=True`, at dispatch — after the shape-level `validate_output` succeeds — the supervisor runs [`check_hive_contract`](#check_hive_contract) **first** (the storability boundary: the output must be JSON-serializable / storable by the OS log) and then [`check_acceptance`](#check_acceptance) (each output field's declared `acceptance` contract). Both raise [`SchemaError`](#schemaerror) on a violation, riding the **same** retry/record path as any invoke/validate failure: it propagates under `on_error='raise'`, and is recorded as `failed` (subject to `max_attempts`) under `on_error='record'`. A QA/storability miss is thus **not admitted and does not complete**, so it earns no trust. `acceptance_fn` is an `Optional[Callable[[str], bool]]` that narrows the gate to the nodes it returns truthy for (wire a trust-derived predicate so a weak agent is QA-checked while a proven one runs lean); `None` = every node. Both default off — with `check_acceptance=False` the QA gate never runs and `acceptance_fn` has no effect.

```python
from concursus import Supervisor

sup = Supervisor(plan, manifests, invoke_fn=fake_invoke)
outputs = sup.run({"topic": "x"})   # -> {node_id: output_dict}
```

**Opt-in tiered static-context overlay (`payload_tier_fn`).** When constructed with `payload_tier_fn` (a `node -> Tier` selector, e.g. from [`make_payload_tier`](governor.md)), the supervisor overlays a **tiered static-context projection** into each node's payload *under* the external inputs — the caller/wired inputs still win on any key collision, so the overlay only supplies context keys the external layer doesn't already set. The tier controls how much of the manifest's free-form `context` (SOP / tools / guardrails / examples / tool-calls) survives the projection; payload detail is proportional to `1/trust`. This is the runtime read side of the payload contract the [`assemble` tier](assemble.md) authors — the `execute` half consumes the tier, it never re-tiers.

The overlay resolves via the internal `_overlay_tiered_context`, which **prefers the frozen `plan.payload_contract[node]["static_context"]`** when present — a self-contained projection baked into the plan at author-time, so no live scheduler is needed at run-time — and **falls back to the live `payload_tier_fn`** (calling [`project_context(manifest.context, tier(node))`](governor.md)) only when the plan carries no frozen contract for that node. With **neither** a frozen contract nor a `payload_tier_fn`, no overlay is applied and the payload is **byte-for-byte unchanged**. Preferring the frozen contract keeps a re-tiered plan (see the `assemble` tier's `recompile`, which pins an already-executed node to its prior contract) authoritative over any drift in the live tier dial.

```python
from concursus import Supervisor
from concursus.governor import make_payload_tier, manifest_is_programmatic

tier_fn = make_payload_tier(scheduler, is_programmatic=manifest_is_programmatic(manifests))
sup = Supervisor(plan, manifests, invoke_fn=fake_invoke, payload_tier_fn=tier_fn)
outputs = sup.run({"topic": "x"})
# per node: payload = tiered static context, then external inputs overlaid on top (external wins),
# then wired upstream outputs. A frozen plan.payload_contract[node] short-circuits tier_fn.
```

### `Supervisor.session_id`

```python
@property
def session_id(self) -> str
```

The stable per-run `runtimeSessionId` shared across **every** invoke in the run. Set from the `session_id` constructor argument, or a freshly generated ≥33-char id (AgentCore requires ≥ 33). One session id per run gives session affinity across the AgentCore data plane — invocations sharing a session land on warm microVMs and shared session memory.

- **Returns:** the session id (`str`).

### `Supervisor.run`

```python
def run(self, inputs: Dict[str, Any], *, parallel: int = 1) -> Dict[str, Dict]
```

A single forward pass over the frozen `plan.order`. Invokes each pending node and returns `{node_id: output_dict}` for the nodes that completed.

- **Parameters:**
  - `inputs` — the external run-input mapping (see [Threading inputs and upstream outputs](#threading-inputs-and-upstream-outputs)).
  - `parallel` (keyword-only, default `1`) — the worker-pool width for the opt-in bounded antichain-parallel wave. At `1` this is exactly the serial single pass (byte-for-byte unchanged). At `> 1`, see [Opt-in bounded antichain-parallel wave](#opt-in-bounded-antichain-parallel-wave-runparalleln) below.
- **Returns:** `{node: store.get(node) for node in plan.order if node in store.completed()}` — **only completed nodes**. Failed, blocked, and held nodes are omitted from the return (but visible via [`summary()`](#supervisorsummary) / [`index()`](#supervisorindex)).
- **Raises:**
  - `ValueError` — if `parallel < 1`.
  - [`PlanIdentityError`](#planidentityerror) — only when the supervisor was built with `verify_plan_identity=True` and this resume's plan hash diverges from the persisted run (checked *before* any completed-node skip).
  - Under the default `on_error='raise'`, any invoke / validate / integrity exception propagates unchanged; under `on_error='record'`, terminal failures are recorded rather than raised.

Per node, in `plan.order`:

1. **Resume-by-skip.** If the node is already in `store.completed()`, skip it — its validated output was recorded on a prior run.
2. **Held-skip.** If the node is in the `held` set, it is a pure **non-dispatch**: never invoked, and **nothing** is written to the log (unlike a blocked-skip), so it leaves no failed record and stays in the open frontier for a later round.
3. **Block-skip.** If any producer this node consumes has not completed, a `failed` record with a `"blocked on <producers>"` reason is written and the node is skipped, so [`extract`](../../src/concursus/core/resolve.py) never hits a missing-producer `KeyError`.
4. **Dispatch.** Otherwise, hand the node to the internal dispatch step (payload assembly → invoke → validate → record).

The plan topology (`order` / `wiring`) is never mutated at runtime; a failure or block prunes only *within* `plan.wiring`, never rewrites topology.

#### Opt-in bounded antichain-parallel wave (`run(parallel=N)`)

`parallel` is **opt-in and defaults to `1`** — at `1`, `run` is the exact serial single pass described above, byte-for-byte unchanged. At `> 1`, `run` delegates to the internal `_run_parallel`, which is **not a new execution model**: it is still a single static pass over the *frozen* `plan.order` — never mutated, never replanned (Concursus is a compiler, not a runtime governor). The only difference is *when* independent nodes are dispatched. Each round computes the current **dispatchable antichain** — every still-open node whose `plan.wiring` producers are **all** in `store.completed()` — and submits that whole wave concurrently to a bounded `ThreadPoolExecutor(max_workers=parallel)`, waits for the wave with `wait(futures)`, then recomputes the next antichain. It loops until every node is completed or no node is dispatchable; any still-open node with an uncompleted producer is then recorded `blocked_on` exactly as the serial pass does.

- **Determinism / order-independence.** A node is dispatched **only** after all its producers have completed, so its resolved inputs are identical to the serial run regardless of intra-wave completion order. Results are keyed by node id in the store, so the per-node outputs, statuses, `consumes` edges, and content hashes are **byte-for-byte identical** to `parallel=1` — only the store-local `seq` / `timestamp` reflect physical put order. The store contents are the same for any `parallel`.
- **CPU clamp.** The requested `parallel` is clamped by the host CPU capacity via the same `max(1, min(pref, cap))` shape the inner graph's fan-out uses (`resolve_ceiling`): a soft request can only *tighten* the pool below the host's capacity (hard-capped by `MAX_FANOUT_CAP`), never spawn more workers than the host can serve. The clamp only shrinks the *pool width*, never the set of nodes dispatched — so the store stays byte-for-byte identical to the serial pass.
- **Unchanged semantics.** `on_error` is unchanged: each node's `_dispatch` runs in the worker exactly as serial, so `'raise'` surfaces the first wave failure (fail-fast — once the wave is joined, the failures are inspected across the wave's futures and the first is re-raised) and `'record'` writes one failed record per node and lets the pass continue (a failure prunes only its dependent subtree). Resume=replay, held/blocked handling, and the identity guard all behave identically.

```python
from concursus import Supervisor

sup = Supervisor(plan, manifests, invoke_fn=fake_invoke)
outputs = sup.run({"topic": "x"}, parallel=4)   # independent nodes run concurrently per wave
# store contents (outputs, statuses, hashes) are byte-for-byte identical to parallel=1
```

### `Supervisor.context`

```python
def context(self, node: str) -> Dict[str, dict]
```

Returns the transitive **upstream context** for `node` as `{producer: latest validated output}`, rebuilt from the store's recorded `consumes` edges (not from `plan.wiring`).

- **Parameters:** `node` — the node id to gather upstream context for.
- **Returns:** `{producer: store.get(producer)}` for each producer in `graph.context_order(node)` (producers, nearest-first, bounded), where the graph is `RunGraph.from_records(store.records())`.

This is graph-aware shared upstream state *as a query*, distinct from the point-to-point `AgentRef` wiring the payload is built from. See [`state/rungraph.py`](../../src/concursus/state/rungraph.py).

### `Supervisor.index`

```python
def index(self) -> RunIndex
```

Returns a [`RunIndex`](../../src/concursus/state/runindex.py) over the run's log (`RunIndex.from_store(self._store)`) — for Folgezettel-tree traversal (retries / fan-out / branches) and metadata queries (`status` / `schema` / `record_type` / `producer`) without scanning payloads.

- **Returns:** a `RunIndex`.

### `Supervisor.summary`

```python
def summary(self) -> Dict[str, Any]
```

A read-only, operator-legible partial-run summary derived **purely from the store log** — no side effects, no change to `run()`'s `{node: output}` return contract.

- **Returns:** a dict with:

| Key | Type | Meaning |
|---|---|---|
| `total` | `int` | `len(plan.order)`. |
| `completed` | `int` | Count of `store.completed()`. |
| `completed_nodes` | `List[str]` | Sorted completed node ids. |
| `failed` | `Dict[str, str]` | Per-node reason: the failed record's `blocked_on` meta, or `""` for a genuine failure. Nodes that *later* completed are excluded, so a validated retry is not counted as a failure. |
| `failure_classes` | `Dict[str, int]` | A `{"crash": N, "hold": M}` count over the terminal failed nodes (see the classes below). Both keys are always present, possibly zero. |
| `order` | `List[str]` | `list(plan.order)`. |

Computed from `RunIndex.query(status="failed")` + `store.completed()`; the latest failed record per node wins. (The opt-in resume=replay identity record under the reserved `__plan_identity__` id is bookkeeping, not a DAG node, so it is dropped from `completed` and never inflates `completed`/`completed_nodes`.)

**Failure classification.** `failure_classes` distinguishes two kinds of terminal failure, so an operator can tell a node's *own* fault from collateral pruning:

- **`crash`** (`_FAILURE_CRASH`) — this node's own invoke / validate / ARN-integrity raised: a genuine, self-inflicted failure whose dependent subtree is what gets pruned.
- **`hold`** (`_FAILURE_HOLD`) — the node was **never invoked** because a producer it consumes failed or was itself held/blocked: a pruned-subtree skip, not this node's fault. (Written as the `failure_class` on every `blocked_on` record by `run` / `_run_parallel`.)

The class is read from the failed record's `failure_class` field; a legacy record with no `failure_class` — or one carrying an unrecognized value — is derived from `blocked_on` presence, so the count is stable across store backends and older logs. Note this is a *count* over failed nodes — a held node (governance HOLD) writes **no** record at all and so contributes to no class.

### `Supervisor.summary_line`

```python
def summary_line(self) -> str
```

A one-line human rendering of [`summary()`](#supervisorsummary) for the CLI failure path.

- **Returns:** e.g. `"completed 4/6; node summarize failed; node critique blocked on summarize"`. Iterates `s["order"]`; a node absent from `s["failed"]` is skipped; a node with an empty reason renders `node <id> failed`, otherwise `node <id> <reason>`. Parts are joined with `"; "`.

---

## Threading inputs and upstream outputs

`run(inputs)` takes the external run-input mapping. Per node, the payload is assembled in two layers.

**External inputs** resolve per node:

- If `inputs[node]` is a dict, that block is the node's external inputs.
- Otherwise, for a **source** node (no inbound wiring), the whole top-level `inputs` mapping is used.
- Otherwise (a non-source node with no explicit block) the external layer is empty.

So `run({"topic": "x"})` feeds a source node the whole mapping, while `run({"ingest": {"topic": "x"}})` targets a specific node's block.

**Tiered static context (opt-in).** When the `Supervisor` was constructed with `payload_tier_fn` (or the frozen `plan.payload_contract` carries a node), `_external_inputs` first lays down the node's tiered static-context projection, then overlays the resolved external inputs **on top** — so external inputs win on any key collision and the tiered context only fills keys they leave open. The projection prefers the frozen `plan.payload_contract[node]["static_context"]` else the live `payload_tier_fn`; with neither, this layer is absent and the payload is byte-for-byte unchanged. See [Opt-in tiered static-context overlay](#supervisor) above.

**Wired inputs** are then overlaid on top. For each `AgentRef` in `plan.wiring[node]`:

```python
payload[ref.input_name] = extract(store.get(ref.producer), ref.path)
```

— the upstream output field the resolver promised (see [`extract`](../../src/concursus/core/resolve.py) and the [`core` reference](core.md)). The assembled dict is JSON-encoded to `payload_bytes` and handed to the [`InvokeFn`](#invokefn), keyed by the same `session_id`.

---

## The transport seam — A2A / MCP / HTTPS

The payload assembly above is *what* flows; the transport is *how*. Concursus speaks the three AgentCore serving protocols, one per agent, fixed at build time. The [`build` tier](build.md) generates a serving wrapper (`app.py`) per protocol so that an agent author writes a plain callable and the wrapper adapts it to the wire:

| `protocol` | AgentCore serving contract | Port | Wrapper the build tier emits |
|---|---|---|---|
| `HTTP` (HTTPS on the wire) | `POST /invocations` + `GET /ping` | `8080` | [`HttpAgentTemplate`](build.md) — `BedrockAgentCoreApp` with an `@app.entrypoint handler(payload, context)`. |
| `MCP` | streamable-http at `/mcp` | `8000` | [`McpAgentTemplate`](build.md) — `FastMCP` with an `@mcp.tool()` named after the entry function. |
| `A2A` | JSON-RPC 2.0 at `/` | `9000` | [`A2AAgentTemplate`](build.md) — `BedrockAgentCoreApp` served as JSON-RPC 2.0. |

The port-per-protocol contract is fixed in [`PORTS`](build.md). The compiler records the chosen transport in [`BuildPlanEntry.invoke`](build.md) (`{"protocol", "qualifier", "port"}`), and the `Supervisor` dispatches through the injectable [`InvokeFn`](#invokefn):

```python
InvokeFn = Callable[[str, str, str, bytes], dict]
#                    arn  qualifier session_id payload_bytes -> parsed output dict
```

This is the single choke point where the runtime meets AgentCore's data plane. The default `InvokeFn` lazily binds boto3's `bedrock-agentcore` client; a test injects a fake and the whole team runs offline with no AWS. **The abstraction that matters:** the `Supervisor` builds the same payload dict and calls the same `InvokeFn` signature regardless of whether the target agent serves HTTP, MCP, or A2A — the protocol difference is absorbed by the generated wrapper, not leaked to the caller. Every invoke in a run shares one stable ≥33-char [`session_id`](#supervisorsession_id) (`runtimeSessionId`), giving session affinity (warm microVMs, shared session memory) across the whole team.

> **"Agent-to-agent" here means compiler-wired, Supervisor-actuated — not agents dialing each other.** A2A is one of three *serving* transports an agent can expose; it is **not** a side channel one agent uses to call another on its own. Producer→consumer data flow is always mediated: agent A returns its output, the `Supervisor` records it, and when agent B is dispatched the `Supervisor` reads `plan.wiring[B]` and lays A's value into B's payload. Keeping A2A a *served protocol* (not peer-initiated calls) is what preserves the single-forward-pass, replayable invariant — there is no hidden edge the plan doesn't know about.

---

## Resuming by skipping

Resume is not a re-plan — it is a re-run over the **same** `StateStore`. Because `run()` skips any node already in `store.completed()` (step 1 above), a fresh `Supervisor` over the same store picks up exactly where the last pass left off:

```python
from concursus import InProcessStateStore, Supervisor

store = InProcessStateStore()
Supervisor(plan, manifests, invoke_fn=fn, state_store=store).run(inputs)   # first pass
# ... microVM teardown / interrupt / later round ...
Supervisor(plan, manifests, invoke_fn=fn, state_store=store).run(inputs)   # resumes: completed nodes skipped
```

Writes to the store are append-only and a node's `attempt` auto-increments inside `store.put()`. For the backends behind the seam (in-process, AgentCore Memory, on-disk file vault) and how replay-resume works, see [Durable Run State](../guides/durable-state.md) and [`state/statestore.py`](../../src/concursus/state/statestore.py).

**Opt-in resume identity guard (`verify_plan_identity`).** Resume=replay is only safe if the resumed plan is the **same frozen plan** the log was recorded under: a completed node id skipped on resume would otherwise replay under divergent wiring/entry. Constructing the `Supervisor` with `verify_plan_identity=True` guards this — on the first pass `run` persists this frozen plan's [`plan_fingerprint`](#plan_fingerprint) under a reserved store id (`__plan_identity__`, not a DAG node, so it never appears in `run`'s return), and on any later resume it **asserts** the persisted hash equals the current plan's hash *before* skipping/replaying any completed node, raising [`PlanIdentityError`](#planidentityerror) on mismatch. It is a verification, never a rebind: it never mutates `plan.order` and adds no compiler loop. A second `run` on the *same* supervisor finds a matching identity and re-writes nothing (idempotent).

```python
from concursus import Supervisor, PlanIdentityError

# first pass persists the plan's fingerprint
Supervisor(plan, manifests, invoke_fn=fn, state_store=store, verify_plan_identity=True).run(inputs)
try:
    # a resume under a DIFFERENT (divergent) plan is rejected loudly, not mis-replayed
    Supervisor(divergent_plan, manifests, invoke_fn=fn, state_store=store, verify_plan_identity=True).run(inputs)
except PlanIdentityError:
    ...   # re-compile monotonically instead of resuming a divergent plan
```

With the default `verify_plan_identity=False`, resume is byte-for-byte the original completed-node skip and no fingerprint is written or checked.

## Fail-fast vs. resilient

| Mode | Construction | Behavior |
|---|---|---|
| **Fail-fast (default)** | `on_error='raise'`, `max_attempts=1` | Any invoke / validate / integrity exception propagates unchanged. Byte-for-byte the original single forward pass. |
| **Resilient** | `on_error='record'`, `max_attempts=N` | A terminal invoke/validate failure is recorded (not raised) and retried up to `max_attempts` on the **same** pinned node id; the pass continues, so a failure prunes only its dependent subtree while independent branches still return. |

```python
# record failures instead of raising; retry each node up to 3x
sup = Supervisor(plan, manifests, invoke_fn=fn, on_error="record", max_attempts=3)
sup.run(inputs)
print(sup.summary_line())
# 'completed 4/6; node summarize failed; node critique blocked on summarize'
```

> Retries via `max_attempts` **only** take effect under `on_error='record'`; under the default `on_error='raise'` the first exception propagates immediately.

**Governance HOLD.** The `held` set withholds nodes this episode — they are never invoked and leave *no* record, so a held node produces no spurious replan signal and simply stays in the open frontier for a later round:

```python
sup = Supervisor(plan, manifests, invoke_fn=fn, held={"risky_node"})
```

**ARN integrity (`arn_resolver`).** When supplied, `arn_resolver(node, manifest)` is consulted once at dispatch to **assert** the compiled ARN is still authoritative — it never rebinds the invoke to a re-fetched ARN. On mismatch it errors (forcing a re-compile) rather than silently swapping the value; an unresolved placeholder ARN fails the same integrity check with a "deploy first" error. This keeps a frozen binding frozen in-run.

---

## Invariants at a glance

- **`run()` is a single static forward pass.** It walks `plan.order` once; `plan.order` / `plan.wiring` are never mutated at runtime. A failure or block prunes only within `plan.wiring`, never rewrites topology. `run(parallel=N)` is the same static pass — it dispatches independent antichains concurrently but never re-plans; the store contents are byte-for-byte identical to the serial pass.
- **Structural validation happens exactly once.** Dangling `AgentRef` producers and wiring cycles are rejected at construction (`_validate_plan_structure`); `run()` has no structural re-check loop.
- **Resume by skipping.** A node already in `store.completed()` is skipped; a re-run over the same `StateStore` continues where it left off — never a re-plan.
- **Resume identity is opt-in.** `verify_plan_identity=True` persists [`plan_fingerprint`](#plan_fingerprint) and asserts it on resume, raising [`PlanIdentityError`](#planidentityerror) on a divergent plan; the default `False` is the original byte-for-byte completed-node skip.
- **One session id per run.** A stable ≥33-char `runtimeSessionId` spans every invoke, exposed as [`session_id`](#supervisorsession_id).
- **Held is a non-dispatch, blocked is a record.** A held node writes nothing; a blocked or genuinely-failed node writes a `failed` record (blocked ones carry a `blocked_on` reason; genuine failures carry an `error` / `error_type` payload). Each failed record also carries a `failure_class` — `hold` (never invoked; a producer failed/was blocked) or `crash` (this node's own invoke/validate/ARN check raised) — surfaced as `summary()["failure_classes"]`.
- **The default path is byte-for-byte the original.** `on_error='raise'`, `max_attempts=1`, no `held`, no `arn_resolver` preserves the original fail-fast pass.
- **Minimal output validation.** [`validate_output`](#validate_output) only checks "is a dict" + required-field presence — no type, nested-shape, or extra-key checks; an empty/non-dict schema (or a node with no manifest) applies only the dict check.
- **Opt-in QA is default-off.** [`check_hive_contract`](#check_hive_contract) and [`check_acceptance`](#check_acceptance) run only under `check_acceptance=True`; with the default `check_acceptance=False` the dispatch path is byte-for-byte the original. When on, a QA/storability miss raises `SchemaError` on the existing retry/record path — the output is not admitted and earns no trust.
- **Tiered static context is opt-in and read-only.** `payload_tier_fn` overlays a tiered static-context projection *under* the external inputs (external inputs win); it prefers the frozen `plan.payload_contract[node]["static_context"]` else the live tier fn. With neither, the payload is byte-for-byte unchanged. `execute` consumes the tier — it never authors or re-tiers the contract (that is the `assemble` tier's job).
- **One transport seam for three protocols.** HTTP/MCP/A2A differences are absorbed by the generated serving wrapper; the `Supervisor` builds one payload and calls one [`InvokeFn`](#invokefn) signature for all three. A2A is a *served* protocol, not a peer-initiated call — producer→consumer flow is always Supervisor-mediated through the wiring.
- **Node dispatch is a pluggable-but-default seam.** [`NODE_EXECUTORS`](#nodeexecutor--node_executors) maps a node-kind key to a uniform `(supervisor, node, inputs, wiring) -> None` handler; with no `node_executors` / `node_kind_fn`, every node routes to the `"default"` kind → `_dispatch`, byte-for-byte unchanged. A custom kind rides the identical store / `on_error` path and never mutates the frozen `plan.order` or adds a loop — it changes only how a single node is invoked.

## See also

- [Guide: Compiling & Running a Team](../guides/compiling-and-running.md) — where `resolve → assemble → freeze → supervise` fit together, and `Supervisor.run` in context.
- [Guide: Durable Run State](../guides/durable-state.md) — the `StateStore` seam, its backends, and replay-resume.
- [Guide: Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md) — turning a frozen plan into live runtimes the default `InvokeFn` calls.
- [Guide: The Governor](../guides/governor.md) — the strictly-outer loop that schedules and re-compiles around the compiler (and supplies the `held` set).
- [API Reference: core](core.md) — `AgentDAG`, `AgentManifest`, `AgentRef`, and `extract`, the wiring the payload is built from.
- [API Reference: assemble](assemble.md) — where `wiring`, `entries`, and `payload_contract` are frozen.
- [API Reference: build](build.md) — the HTTP/MCP/A2A serving templates, `PORTS`, and `BuildPlanEntry.invoke`.
- [API Reference: governor](governor.md) — `project_context` / `make_payload_tier` / `Tier` that tier the static context.
- [Core Concepts](../concepts.md) · [Overview](../overview.md) · [Documentation Index](../README.md)
- Source: [`execute/supervisor.py`](../../src/concursus/execute/supervisor.py), [`state/statestore.py`](../../src/concursus/state/statestore.py), [`state/rungraph.py`](../../src/concursus/state/rungraph.py), [`state/runindex.py`](../../src/concursus/state/runindex.py), [`core/resolve.py`](../../src/concursus/core/resolve.py).
