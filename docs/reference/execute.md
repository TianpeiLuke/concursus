# API Reference: `execute`

*The runtime tier — the `Supervisor`'s topological dispatch over a frozen plan, plus the batteries-included stack that invokes real leaf agents inside that dispatch.*

The `execute` tier is the **runtime half** of Concursus. It takes a frozen `ProvisioningPlan` (produced by the `assemble` tier) and drives its agents to completion. Two registers cohere here:

- The **`Supervisor`** — the dispatcher. It walks `plan.order`, builds each agent's invoke payload from external run inputs overlaid with resolved upstream outputs, calls an injectable transport, shape-checks the result against the manifest's output schema, and threads every validated output forward through a resumable `StateStore`. On its own it is an abstract dispatcher over an injected `invoke_fn` — the offline, AWS-free path these docs lead with.
- The **runtime stack** — plugged *into* the Supervisor's `NodeExecutor` seam. An `AgentInvoker` invokes a *real* leaf agent by `manifest.runtime.backend`; an `ExecutionMonitor` reads the invoke's log stream for rule-based per-node health and can preempt it; an `AgentHarness` wraps each node with I/O, contract enforcement, monitor wiring, and bounded corrective retry; a `FileStore` / `S3Store` holds the artifacts; and futility cancellation stops in-flight work whose output can no longer be consumed. It is **entirely opt-in**: a run that wires no harness factory routes every node to the default dispatch and is **byte-for-byte unchanged**.

> **The load-bearing invariant.** *Concursus is a compiler, not a runtime governor.* `Supervisor.run` is a single static forward pass over an immutable plan; it never re-plans, never mutates `plan.order` / `plan.wiring`, and contains no structural re-check loop. Structural validation of the plan happens **once**, at construction. Resume is replay of an append-only log, never a re-plan mid-flight. The whole runtime stack respects this: it runs **inside** the governed `(supervisor, node, inputs, wiring) -> None` dispatch seam — reading the frozen plan, invoking an agent, writing the same append-only log any node writes — and never mutates a plan, re-plans, or reaches inside another node's execution.

Eight modules make up the tier:

| Module | Source | Owns |
|---|---|---|
| `execute.supervisor` | [`../../src/concursus/execute/supervisor.py`](../../src/concursus/execute/supervisor.py) | The `Supervisor` (topological dispatch, resume=replay, the `NodeExecutor` registry seam, the opt-in antichain-parallel wave + `cancel_futile`, the 4-class failure taxonomy), the `InvokeFn` transport alias, the output gates (`validate_output` / `check_hive_contract` / `check_acceptance`), the shared `record_failure` writer, and the `plan_fingerprint` resume-identity guard. |
| `execute.invoker` | [`../../src/concursus/execute/invoker.py`](../../src/concursus/execute/invoker.py) | `AgentInvoker` — the unified wire-level dispatcher that invokes a leaf agent by `manifest.runtime.backend` (`callable` / `agentcore` / `http` / `strands`; `api` is a declared stub), and `invoke_with_tap`, which returns the response plus a live `LogEvent` stream. |
| `execute.monitor` | [`../../src/concursus/execute/monitor.py`](../../src/concursus/execute/monitor.py) | `ExecutionMonitor` (four rule-based per-node health strategies), `MonitorConfig`, `DefaultMonitorFactory`, and `remediation_for` (a corrective-retry prompt amendment derived from a terminating verdict). |
| `execute.harness` | [`../../src/concursus/execute/harness.py`](../../src/concursus/execute/harness.py) | `AgentHarness` — the per-node wrapper (input deref, prompt serialization, invoke + log forwarding, artifact write, contract enforcement), plus the `ObjectStore` / `ExecutionMonitor` Protocols it consumes. |
| `execute.harness_factory` | [`../../src/concursus/execute/harness_factory.py`](../../src/concursus/execute/harness_factory.py) | `HarnessFactory` (builds harnesses and the `NodeExecutor` + `node_kind_fn` that plug them into the Supervisor) and `make_harness_supervisor_factory` (the GovernorLoop glue). |
| `execute.object_store` | [`../../src/concursus/execute/object_store.py`](../../src/concursus/execute/object_store.py) | `FileStore` (local / test, `file://`) and `S3Store` (production, `s3://`, lazy boto3) — the concrete `ObjectStore` backends. |
| `execute.futility` | [`../../src/concursus/execute/futility.py`](../../src/concursus/execute/futility.py) | The machinery behind the opt-in `cancel_futile` seam: pure graph math (`invert_wiring` / `descendants` / `futility_closure`) plus the `CancelTokenRegistry` + `run_registered` that reach inside a running harness task to cancel it. |
| `execute.types` | [`../../src/concursus/execute/types.py`](../../src/concursus/execute/types.py) | The shared value types: `LogEvent` / `LogEventType` / `LogSeverity`, `HealthSignal` / `HealthStatus`, `PreemptiveTermination`, and `InvokeResult`. |

Everything here is pure-stdlib at import time; `boto3` (the `agentcore` backend + `S3Store`), `aiohttp` (the `http` backend), and `strands` (the `strands` backend) are all lazily imported, so the pure core and the full test suite run with none of them installed.

Four symbols are re-exported from the package root:

```python
from concursus import Supervisor, SchemaError, PlanIdentityError, plan_fingerprint
```

The runtime stack is re-exported from the **subpackage** (`concursus.execute.__all__`):

```python
from concursus.execute import (
    AgentInvoker, AgentHarness, HarnessFactory, make_harness_supervisor_factory,
    ExecutionMonitor, MonitorConfig, DefaultMonitorFactory,
    FileStore, S3Store,
    LogEvent, LogEventType, LogSeverity, HealthSignal, HealthStatus,
    InvokeResult, PreemptiveTermination,
)
```

The remaining public symbols are imported from their module — the Supervisor's transport/gate helpers, the dispatch seam, and the shared failure writer:

```python
from concursus.execute.supervisor import (
    InvokeFn, validate_output, check_hive_contract, check_acceptance,
    NodeExecutor, NODE_EXECUTORS, record_failure,
)
from concursus.execute.monitor import remediation_for
from concursus.execute.futility import (
    futility_closure, invert_wiring, descendants, CancelTokenRegistry, run_registered,
)
```

The `Supervisor` walks a plan produced by the [`assemble` tier](../guides/compiling-and-running.md) and writes through the [`StateStore` seam](../guides/durable-state.md). For where it sits in the compile-then-run pipeline, see [Compiling & Running a Team](../guides/compiling-and-running.md); for the runtime stack in prose, see [Running Agents](../guides/running-agents.md).

---

## `execute.supervisor`

Source: [`../../src/concursus/execute/supervisor.py`](../../src/concursus/execute/supervisor.py)

> The Supervisor is the whole tier's spine. Every other module plugs into its `NodeExecutor` seam or feeds its log; on its own (no `node_executors` / `node_kind_fn`) it is the abstract dispatcher over an injected `invoke_fn` — byte-for-byte the original single forward pass.

There is **no `__all__`** in the module; the public API is the non-underscore symbols below. Four are re-exported from the package root (`Supervisor`, `SchemaError`, `PlanIdentityError`, `plan_fingerprint`); the rest import from `concursus.execute.supervisor`.

| Symbol | Kind | Summary |
|---|---|---|
| [`InvokeFn`](#invokefn) | type alias | The injectable invoke transport: `(arn, qualifier, session_id, payload_bytes) -> dict`. |
| [`SchemaError`](#schemaerror) | exception | Raised when an agent's output fails its declared output schema. |
| [`PlanIdentityError`](#planidentityerror) | exception | Raised when a resume replays against a plan whose `plan_fingerprint` differs from the persisted run. Opt-in guard, default off. |
| [`plan_fingerprint`](#plan_fingerprint) | function | Stable content-hash of a plan's compiled identity (`order` + `wiring` + `entries`) — the resume=replay identity guard. |
| [`record_failure`](#record_failure) | function | The single shared writer for a node's `failed` record — used by both the default dispatch and the harness node executor. |
| [`validate_output`](#validate_output) | function | Minimal (no-`jsonschema`) shape check: `obj` is a dict and every required property is present. |
| [`check_hive_contract`](#check_hive_contract) | function | Opt-in storability gate: the output must be JSON-serializable (storable by the OS log). Default off. |
| [`check_acceptance`](#check_acceptance) | function | Opt-in post-run QA gate: each output field's declared `acceptance` contract must hold. Default off. |
| [`NodeExecutor`](#nodeexecutor--node_executors) | type alias | A uniform `(supervisor, node, inputs, wiring) -> None` node-kind handler (opt-in Strategy/Registry dispatch seam). |
| [`NODE_EXECUTORS`](#nodeexecutor--node_executors) | registry | The shipped node-kind registry, seeded with the single `"default"` kind → `_default_node_executor`. Instances copy it. |
| [`Supervisor`](#supervisor) | class | Drives a `ProvisioningPlan` to completion in topological order. |
| [`Supervisor.session_id`](#supervisorsession_id) | property | The stable per-run `runtimeSessionId` shared across every invoke. |
| [`Supervisor.run`](#supervisorrun) | method | One forward pass over `plan.order` (opt-in bounded `parallel=N` antichain wave); returns `{node_id: output_dict}` for completed nodes. |
| [`Supervisor.recorded_payloads`](#supervisorrecorded_payloads) | method | The real per-node invoke payloads captured during the run (empty unless `capture_agent_binding=True`). |
| [`Supervisor.context`](#supervisorcontext) | method | Transitive upstream context for a node, rebuilt from recorded `consumes` edges. |
| [`Supervisor.index`](#supervisorindex) | method | A `RunIndex` over the run's log for tree traversal and metadata queries. |
| [`Supervisor.summary`](#supervisorsummary) | method | Read-only partial-run summary derived purely from the store log (incl. `failure_classes`: per-class terminal-failure counts). |
| [`Supervisor.summary_line`](#supervisorsummary_line) | method | One-line human rendering of `summary()` for the CLI failure path. |

### `InvokeFn`

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

> `InvokeFn` is the Supervisor's *own* transport seam — the abstract-dispatcher path. It is distinct from the [`AgentInvoker`](#executeinvoker): `InvokeFn` is a bare `(arn, qualifier, session_id, bytes) -> dict` callable you inject, whereas `AgentInvoker` is the batteries-included dispatcher the [harness](#executeharness) drives through the `NodeExecutor` seam. A run uses one or the other per node, never both.

```python
def fake_invoke(arn: str, qualifier: str, session_id: str, payload: bytes) -> dict:
    return {"summary": "..."}   # a parsed output dict

sup = Supervisor(plan, manifests, invoke_fn=fake_invoke)
```

### `SchemaError`

```python
class SchemaError(ValueError)
```

Raised when an agent's output fails to satisfy its declared output schema. Subclasses `ValueError`, so callers may catch either. Raised by [`validate_output`](#validate_output) when the output is not a dict or is missing a required field. Under the default `on_error='raise'` it propagates unchanged (fail-fast); under `on_error='record'` it is caught and written as a `failed` record instead.

### `PlanIdentityError`

```python
class PlanIdentityError(ValueError)
```

Raised when a **resume replays against a divergent plan** — the plan handed to [`Supervisor.run`](#supervisorrun) on resume hashes ([`plan_fingerprint`](#plan_fingerprint)) differently from the plan the store's log was originally recorded under. Subclasses `ValueError`. This is the opt-in **resume=replay identity guard**: the append-only log is the single source of truth, and resume is replay of that log against a *frozen* plan — so silently skipping the recorded `completed()` nodes under a plan whose wiring/entry changed would mis-replay. The guard turns that latent, silent hazard into a loud, legible error, steering you to re-compile monotonically (see the `assemble` tier's `recompile`) rather than resume against a divergent plan.

**Default off** — this check runs only when the [`Supervisor`](#supervisor) is constructed with `verify_plan_identity=True`. With the default `verify_plan_identity=False`, resume is byte-for-byte the original completed-node skip and this error is never raised. See [Resuming by skipping](#resuming-by-skipping).

### `plan_fingerprint`

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

### `record_failure`

```python
def record_failure(
    supervisor: "Supervisor",
    node: str,
    *,
    failure_class: str,
    error: str,
    error_type: str,
    consumes: Optional[List[str]] = None,
    schema: Optional[str] = None,
    blocked_on: Optional[str] = None,
    address: Optional[str] = None,
) -> None
```

Writes **the** `failed` record for `node` — the single writer both node-kind branches share ([`_dispatch`](#supervisorrun) and the [harness node executor](#executeharness_factory)). It puts `{"error": error, "error_type": error_type}` with `status="failed"`, `producer=node`, the given `failure_class`, and — when supplied — the `consumes` edge list, the `schema` tag, and a `blocked_on` reason.

Keeping the failure-record shape in one place is the point: before it existed, the two branches hand-rolled their own `store.put` for failures and drifted twice — the harness path had no generic `except Exception` (so a contract violation aborted an `on_error='record'` run instead of pruning one subtree), and its failure writes dropped the `consumes` / `schema` provenance the success write included. Both are classes of bug a shared writer makes unrepresentable.

- **`failure_class`** is one of the [four failure classes](#the-four-class-failure-taxonomy).
- **`address`** defaults to `node` but is overridable, because the retry path encodes the attempt (`f"{node}/{attempt}"`) so successive attempts stay distinguishable in the log.
- It is **caller-agnostic on purpose**: it does *not* consult `on_error`. Deciding whether to record or re-raise stays with the caller, because only the caller knows whether the exception it holds is recoverable in its context.

Public but not re-exported at the root — `from concursus.execute.supervisor import record_failure`.

### `validate_output`

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

### `check_hive_contract`

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

### `check_acceptance`

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

### `NodeExecutor` / `NODE_EXECUTORS`

```python
NodeExecutor = Callable[["Supervisor", str, Dict[str, Any], List["AgentRef"]], None]

NODE_EXECUTORS: Dict[str, NodeExecutor] = {"default": _default_node_executor}
```

The opt-in **Strategy/Registry dispatch seam** — and the exact slot the whole runtime stack plugs into. A *node executor* is a uniform `(supervisor, node, inputs, wiring) -> None` handler — the Strategy generalization of today's single, uniform node dispatch (there is exactly one *default* kind). `NODE_EXECUTORS` is the shipped registry, seeded only with the `"default"` kind, which maps to `_default_node_executor` — a handler that delegates **verbatim** to [`Supervisor._dispatch`](#supervisorrun). So with **no** custom kind selected (the default), a run routes every node to `"default"` → `_dispatch` and behaves **byte-for-byte as before**.

A caller registers custom node-kinds through the [`Supervisor`](#supervisor) constructor and selects them per node:

- `node_executors=` — a `{kind: NodeExecutor}` mapping layered on top of the shipped registry. Each `Supervisor` instance **copies** `NODE_EXECUTORS` at construction (`dict(NODE_EXECUTORS)`) and updates it with the supplied kinds, so no instance mutates shared global state.
- `node_kind_fn=` — a `node -> kind` selector. When `None` (default), every node uses the `"default"` kind.

A selected kind with **no registered handler falls back to the default handler** (`self._node_executors.get(kind, _default_node_executor)`). Every handler shares the same uniform signature and so rides the identical store / `on_error` / retry path; a custom kind never mutates the frozen `plan.order` and adds no compiler loop — it only changes *how* a single node is invoked, never the topology or the single-pass walk.

The [`HarnessFactory`](#executeharness_factory) is the shipped consumer of this seam: it registers the `"harness"` kind and a `node_kind_fn` that routes any node whose manifest declares a `runtime:` block to it.

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

### `Supervisor`

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
        cancel_futile: bool = False,
        capture_agent_binding: bool = False,
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
| `cancel_futile` | `False` | Opt-in **futility cancellation** for the antichain-parallel wave. When `True`, a node that fails mid-wave cancels in-flight siblings whose every consumer just became unreachable (see [`execute.futility`](#executefutility) and [`run(parallel=N)`](#opt-in-bounded-antichain-parallel-wave-runparalleln)). `False` (and every serial run) = no registry is built and no closure ever runs — `run` behaves exactly as today. |
| `capture_agent_binding` | `False` | Opt-in provenance capture. When `True`, each validated `put` records the bound `agent_name` + `arn` in the record meta, and the real per-node invoke payload is retained for [`recorded_payloads()`](#supervisorrecorded_payloads). `False` = the put meta and payloads are byte-identical to before. |

- **Raises:**
  - `ValueError` — if `on_error` is not `"raise"` / `"record"`, or `max_attempts < 1`.
  - `RunGraphError` — from the one-time construction-time structural check (`_validate_plan_structure`) on a dangling `AgentRef` producer (a wire naming a producer absent from `plan.order`) or a cycle in the wiring. (`RunGraphError` subclasses `ValueError`; see [`state/rungraph.py`](../../src/concursus/state/rungraph.py).)

**ARN resolution precedence** (per node): supplied `arns[node]` → `manifest.registry["agent_runtime_arn"]` → the placeholder `"<agent-runtime-arn>"`. Supplied `arns` for nodes lacking a manifest are added too. An unresolved placeholder ARN fails the integrity check ("deploy first") just before invoke — see [Fail-fast vs. resilient](#fail-fast-vs-resilient) and the `arn_resolver` note below.

**Default is byte-for-byte the original pass.** Construction with `on_error='raise'`, `max_attempts=1`, no `held` set, no `arn_resolver`, `check_acceptance=False`, `payload_tier_fn=None`, `verify_plan_identity=False`, no `node_executors`, `node_kind_fn=None`, `cancel_futile=False`, and `capture_agent_binding=False` — and calling `run(inputs)` with the default `parallel=1` — is the original fail-fast single forward pass. Each of the `held` / `arn_resolver` / `check_acceptance` / `payload_tier_fn` / `verify_plan_identity` / `node_executors` / `cancel_futile` / `capture_agent_binding` / `run(parallel=N)` extensions layers strictly on top of that path and is individually default-off.

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

**Opt-in provenance capture (`capture_agent_binding`).** With `capture_agent_binding=True`, two extra, default-off records are kept. Each validated `put`'s meta gains `agent_name` (the bound `manifest.name`) and `arn` (the resolved runtime ARN) **only when present** — a node with no bound manifest / no real ARN adds nothing, so the put meta stays byte-identical. And the *real* per-node invoke payload the supervisor built in `_dispatch` is retained for [`recorded_payloads()`](#supervisorrecorded_payloads), so a post-run capture persists what was actually asked of each agent rather than only the compiler-authored static context. With the default `False`, no binding meta is written and `recorded_payloads()` is empty.

**Opt-in futility cancellation (`cancel_futile`).** Meaningful only for the parallel wave (`run(parallel>1)`). With the default `False`, no [`CancelTokenRegistry`](#executefutility) is built and no closure is ever computed, so the wave behaves exactly as today. With `True`, when a node fails mid-wave the supervisor condemns in-flight siblings whose every consumer just became unreachable — the in-flight twin of the blocked-skip. It **prunes only**: it never reroutes and never mutates the frozen plan. See [`run(parallel=N)`](#opt-in-bounded-antichain-parallel-wave-runparalleln) and [`execute.futility`](#executefutility).

#### `Supervisor.session_id`

```python
@property
def session_id(self) -> str
```

The stable per-run `runtimeSessionId` shared across **every** invoke in the run. Set from the `session_id` constructor argument, or a freshly generated ≥33-char id (AgentCore requires ≥ 33). One session id per run gives session affinity across the AgentCore data plane — invocations sharing a session land on warm microVMs and shared session memory.

- **Returns:** the session id (`str`).

#### `Supervisor.run`

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
3. **Block-skip.** If any producer this node consumes has not completed, a `failed` record with a `"blocked on <producers>"` reason and `failure_class="hold"` is written and the node is skipped, so [`extract`](../../src/concursus/core/resolve.py) never hits a missing-producer `KeyError`.
4. **Dispatch.** Otherwise, hand the node to `_route_dispatch` → its node-kind handler ([`NODE_EXECUTORS`](#nodeexecutor--node_executors) seam), defaulting to the internal `_dispatch` step (payload assembly → invoke → validate → record).

The plan topology (`order` / `wiring`) is never mutated at runtime; a failure or block prunes only *within* `plan.wiring`, never rewrites topology.

##### Opt-in bounded antichain-parallel wave (`run(parallel=N)`)

`parallel` is **opt-in and defaults to `1`** — at `1`, `run` is the exact serial single pass described above, byte-for-byte unchanged. At `> 1`, `run` delegates to the internal `_run_parallel`, which is **not a new execution model**: it is still a single static pass over the *frozen* `plan.order` — never mutated, never replanned (Concursus is a compiler, not a runtime governor). The only difference is *when* independent nodes are dispatched. Each round computes the current **dispatchable antichain** — every still-open node whose `plan.wiring` producers are **all** in `store.completed()` — and submits that whole wave concurrently to a bounded `ThreadPoolExecutor(max_workers=parallel)`, drains it with `as_completed`, then recomputes the next antichain. It loops until every node is completed or no node is dispatchable; any still-open node with an uncompleted producer is then recorded `blocked_on` exactly as the serial pass does.

- **Determinism / order-independence.** A node is dispatched **only** after all its producers have completed, so its resolved inputs are identical to the serial run regardless of intra-wave completion order. Results are keyed by node id in the store, so the per-node outputs, statuses, `consumes` edges, and content hashes are **byte-for-byte identical** to `parallel=1` — only the store-local `seq` / `timestamp` reflect physical put order. The store contents are the same for any `parallel`.
- **CPU clamp.** The requested `parallel` is clamped by the host CPU capacity via the same `max(1, min(pref, cap))` shape the inner graph's fan-out uses (`resolve_ceiling`): a soft request can only *tighten* the pool below the host's capacity (hard-capped by `MAX_FANOUT_CAP`), never spawn more workers than the host can serve. The clamp only shrinks the *pool width*, never the set of nodes dispatched — so the store stays byte-for-byte identical to the serial pass.
- **Unchanged semantics.** `on_error` is unchanged: each node's dispatch runs in the worker exactly as serial, so `'raise'` surfaces the first wave failure (fail-fast) and `'record'` writes one failed record per node and lets the pass continue (a failure prunes only its dependent subtree). Because `as_completed` yields in completion order while a barrier would inspect in futures-list order, escaped failures are **collected** and the earliest in `plan.order` is re-raised after the pool drains — so a caller still sees the same exception it would have seen before. Resume=replay, held/blocked handling, and the identity guard all behave identically.
- **Futility cancellation (opt-in, `cancel_futile`).** Draining with `as_completed` is what makes the seam possible: the moment a node resolves, its outcome is seen. When `cancel_futile=True` and a wave member fails (or the run is about to fail-fast), the supervisor condemns the in-flight siblings whose every consumer just became unreachable via [`futility_closure`](#executefutility) + [`CancelTokenRegistry`](#executefutility). With the flag off (the default), no registry exists and no closure is computed — the only difference from a barrier-based wave is *when* futures are inspected.

```python
from concursus import Supervisor

sup = Supervisor(plan, manifests, invoke_fn=fake_invoke)
outputs = sup.run({"topic": "x"}, parallel=4)   # independent nodes run concurrently per wave
# store contents (outputs, statuses, hashes) are byte-for-byte identical to parallel=1
```

#### `Supervisor.recorded_payloads`

```python
def recorded_payloads(self) -> Dict[str, Any]
```

The **real per-node invoke payloads** captured during the run, as `{node: payload_dict}`. Empty unless the supervisor was built with `capture_agent_binding=True`. Pass to a post-run capture (`capture_run(payloads=sup.recorded_payloads())`) so the persisted payload notes carry what was actually asked of each agent (redacted at capture time), rather than only the compiler-authored static context. Returns a copy.

#### `Supervisor.context`

```python
def context(self, node: str) -> Dict[str, dict]
```

Returns the transitive **upstream context** for `node` as `{producer: latest validated output}`, rebuilt from the store's recorded `consumes` edges (not from `plan.wiring`).

- **Parameters:** `node` — the node id to gather upstream context for.
- **Returns:** `{producer: store.get(producer)}` for each producer in `graph.context_order(node)` (producers, nearest-first, bounded), where the graph is `RunGraph.from_records(store.records())`.

This is graph-aware shared upstream state *as a query*, distinct from the point-to-point `AgentRef` wiring the payload is built from. See [`state/rungraph.py`](../../src/concursus/state/rungraph.py).

#### `Supervisor.index`

```python
def index(self) -> RunIndex
```

Returns a [`RunIndex`](../../src/concursus/state/runindex.py) over the run's log (`RunIndex.from_store(self._store)`) — for Folgezettel-tree traversal (retries / fan-out / branches) and metadata queries (`status` / `schema` / `record_type` / `producer`) without scanning payloads.

- **Returns:** a `RunIndex`.

#### `Supervisor.summary`

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
| `failure_classes` | `Dict[str, int]` | A `{"crash": N, "hold": M, "preemptive_termination": P, "futility_cancelled": F}` count over the terminal failed nodes (see the classes below). All four keys are always present, possibly zero. |
| `order` | `List[str]` | `list(plan.order)`. |

Computed from `RunIndex.query(status="failed")` + `store.completed()`; the latest failed record per node wins. (The opt-in resume=replay identity record under the reserved `__plan_identity__` id is bookkeeping, not a DAG node, so it is dropped from `completed` and never inflates `completed`/`completed_nodes`.)

##### The four-class failure taxonomy

`failure_classes` distinguishes four kinds of terminal failure, so an operator can tell a node's *own* fault from collateral pruning. The class is read from the failed record's `failure_class` field; a legacy record with no `failure_class` — or one carrying an unrecognized value — is derived from `blocked_on` presence, so the count is stable across store backends and older logs.

| Class (`failure_class`) | Written by | Meaning |
|---|---|---|
| **`crash`** (`_FAILURE_CRASH`) | `_dispatch` / harness executor | This node's own invoke / validate / ARN-integrity / contract check raised — a genuine, self-inflicted failure whose dependent subtree is what gets pruned. |
| **`hold`** (`_FAILURE_HOLD`) | `run` / `_run_parallel` | The node was **never invoked** because a producer it consumes failed or was itself held/blocked — a pruned-subtree skip, not this node's fault. (Written as the `failure_class` on every `blocked_on` record.) |
| **`preemptive_termination`** (`_FAILURE_PREEMPTIVE`) | harness executor | The node's per-node [`ExecutionMonitor`](#executemonitor) judged the run unhealthy and terminated it mid-flight ([`PreemptiveTermination`](#executetypes)), after any corrective retries were exhausted. |
| **`futility_cancelled`** (`_FAILURE_FUTILITY`) | harness executor | The supervisor cancelled this node mid-flight because every consumer of its output had become unreachable ([futility closure](#executefutility)). Morally a `hold`, but detected *during* dispatch, so it earns its own class. |

> Widening this from two classes to four is not cosmetic: `preemptive_termination` records were previously written by the harness executor but not recognized here, so a monitor-initiated termination silently bucketed as a self-inflicted `crash`. Note this is a *count* over failed nodes — a held node (governance HOLD) writes **no** record at all and so contributes to no class.

#### `Supervisor.summary_line`

```python
def summary_line(self) -> str
```

A one-line human rendering of [`summary()`](#supervisorsummary) for the CLI failure path.

- **Returns:** e.g. `"completed 4/6; node summarize failed; node critique blocked on summarize"`. Iterates `s["order"]`; a node absent from `s["failed"]` is skipped; a node with an empty reason renders `node <id> failed`, otherwise `node <id> <reason>`. Parts are joined with `"; "`.

### Threading inputs and upstream outputs

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

> The [harness node executor](#executeharness_factory) assembles the *same* two layers (external + wired), then wraps them — plus the plan's `task` / `io` / `static_context` — into a structured **envelope** the [`AgentHarness`](#executeharness) consumes, rather than a flat payload dict. The wiring resolution is identical; only the shape handed downstream differs.

### The transport seam — A2A / MCP / HTTPS

The payload assembly above is *what* flows; the transport is *how*. When the Supervisor drives its own `InvokeFn` (the abstract-dispatcher path), Concursus speaks the three AgentCore serving protocols, one per agent, fixed at build time. The [`build` tier](build.md) generates a serving wrapper (`app.py`) per protocol so that an agent author writes a plain callable and the wrapper adapts it to the wire:

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

This is the single choke point where the Supervisor's own path meets AgentCore's data plane. The default `InvokeFn` lazily binds boto3's `bedrock-agentcore` client; a test injects a fake and the whole team runs offline with no AWS. **The abstraction that matters:** the `Supervisor` builds the same payload dict and calls the same `InvokeFn` signature regardless of whether the target agent serves HTTP, MCP, or A2A — the protocol difference is absorbed by the generated wrapper, not leaked to the caller. Every invoke in a run shares one stable ≥33-char [`session_id`](#supervisorsession_id) (`runtimeSessionId`), giving session affinity (warm microVMs, shared session memory) across the whole team.

> The harness path uses a *different* addressing register: the [`AgentInvoker`](#executeinvoker) dispatches by `manifest.runtime.backend` and, for the `agentcore` backend, calls the Bedrock Agents `invoke_agent` API by `runtime.agent_id`. The `InvokeFn`/`BuildPlanEntry.invoke` transport table above governs the Supervisor's *own* dispatch, not the invoker's.

> **"Agent-to-agent" here means compiler-wired, Supervisor-actuated — not agents dialing each other.** A2A is one of three *serving* transports an agent can expose; it is **not** a side channel one agent uses to call another on its own. Producer→consumer data flow is always mediated: agent A returns its output, the `Supervisor` records it, and when agent B is dispatched the `Supervisor` reads `plan.wiring[B]` and lays A's value into B's payload. Keeping A2A a *served protocol* (not peer-initiated calls) is what preserves the single-forward-pass, replayable invariant — there is no hidden edge the plan doesn't know about.

### Resuming by skipping

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

### Fail-fast vs. resilient

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

## `execute.invoker`

Source: [`../../src/concursus/execute/invoker.py`](../../src/concursus/execute/invoker.py)

> The `AgentInvoker` is wire-level dispatch only — it never touches object storage (S3), never assembles prompts, and never enforces contracts (all that is the [harness](#executeharness)'s job). It reads `manifest.runtime.backend` and routes to one backend method. Every heavy dependency (`boto3`, `aiohttp`, `strands`) is lazily imported inside its backend, so importing the module needs none of them.

| Symbol | Kind | Summary |
|---|---|---|
| [`AgentInvoker`](#agentinvoker) | class | Unified invocation; dispatches by `manifest.runtime.backend`. |
| [`AgentInvoker.invoke`](#agentinvokerinvoke) | method (async) | Fire-and-forget invoke; returns the raw dict, no log stream. |
| [`AgentInvoker.invoke_with_tap`](#agentinvokerinvoke_with_tap) | method (async) | Invoke with an in-band log tap; returns `(result, AsyncIterator[LogEvent])`. |
| [`AgentInvoker.invoke_sync`](#agentinvokerinvoke_sync) | method | Synchronous wrapper around `invoke()`. |
| [`InvokerError`](#invokererror) | exception | Raised when an invocation fails at the transport level. |

### `AgentInvoker`

```python
class AgentInvoker:
    def __init__(self, manifest: Dict[str, Any], clients: Optional[Dict[str, Any]] = None)
```

Unified agent invocation. Reads `runtime = manifest.get("runtime", {})` and `backend = runtime.get("backend", "callable")` at construction, and routes each invoke to the matching backend method through an internal dispatch table. `clients` is an optional pre-built client bundle for connection reuse (a live callable, a `bedrock-agent-runtime` client, a `strands.Agent`); when `None`, clients are created per-invocation and cached on the instance.

**Backends** (`backend` key → method):

| `backend` | Addressing (`runtime` keys) | Transport | Log tap |
|---|---|---|---|
| `callable` | `client` (a key into the `clients` bundle) **or** `entry: "module:function"` | In-process Python call (sync or async; sync runs in an executor). | Only when the callable returns `(dict, AsyncIterator[LogEvent])`; a bare `dict` yields an empty stream. |
| `agentcore` | `agent_id` (required), `alias_id` (default `"TSTALIASID"`), `region` (default `"us-west-2"`), `timeout_s` (default `120`) | Bedrock Agents `invoke_agent` via a lazily-bound boto3 `bedrock-agent-runtime` client; the `completion` event stream is drained (chunks → output text, JSON-parsed if possible). | `trace` events → `LogEvent`s (classified to `TOOL_CALL` / `REASONING` / `OUTPUT_CHUNK` / `ERROR` / `PROGRESS`) when `stream=True`. |
| `http` | `endpoint` (required), `method` (default `"POST"`), `headers`, `timeout_s` (default `30`) | `aiohttp` if installed, else a `urllib` fallback; posts `{"prompt", "inputs", "context"}` and expects a JSON object. | SSE (`text/event-stream`) parsed for `LogEvent`s when `stream=True` and the response is event-stream. |
| `strands` | a pre-built `strands_agent` in `clients`, else built from `model_id` (default `"anthropic.claude-sonnet-4-20250514"`) + `system_prompt` | Strands `Agent.__call__` (synchronous), run in an executor; the string response is JSON-parsed if possible. | A streaming callback maps strands events to `LogEvent`s when `stream=True`. |
| `api` | — | **Declared stub** — raises [`InvokerError`](#invokererror) directing you to `http` or `agentcore`. Registered so a manifest may name it, but not yet implemented. |

An unknown `backend` raises [`InvokerError`](#invokererror) at dispatch. (A manifest typo is caught earlier, at compile time: `RUNTIME_BACKENDS` in [`core.manifest`](#coremanifest-the-harness-path-additions) fails closed on anything outside `("callable", "agentcore", "http", "strands", "api")`.)

For every backend, the parsed result must be a `dict`, else `InvokerError` is raised; the `callable` backend additionally accepts the `(dict, log_stream)` tuple form.

### `AgentInvoker.invoke`

```python
async def invoke(self, prompt: str, inputs: Dict[str, Any],
                 context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]
```

Fire-and-forget invoke — no log streaming. Dispatches with `stream=False` and returns just the raw dict from the leaf agent (any produced log stream is discarded).

### `AgentInvoker.invoke_with_tap`

```python
async def invoke_with_tap(self, prompt: str, inputs: Dict[str, Any],
                          context: Optional[Dict[str, Any]] = None
                          ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]
```

Invoke with an **in-band log tap**. Dispatches with `stream=True` and returns `(raw result dict, async iterator of [`LogEvent`](#executetypes)s)`. Backends that don't support streaming (or a `callable` returning a bare dict) return an **empty** iterator. This tuple is what lets an in-process agent feed the real [`ExecutionMonitor`](#executemonitor): the harness awaits the result while concurrently consuming the stream. (See also [`InvokeResult`](#executetypes), the equivalent named-tuple type.)

### `AgentInvoker.invoke_sync`

```python
def invoke_sync(self, prompt: str, inputs: Dict[str, Any],
                context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]
```

Synchronous wrapper around [`invoke()`](#agentinvokerinvoke). Runs the event loop if none is running; if called from *inside* a running loop, it runs the coroutine on a one-worker thread pool so it does not deadlock the caller's loop.

### `InvokerError`

```python
class InvokerError(RuntimeError):
    def __init__(self, backend: str, message: str, cause: Optional[Exception] = None)
```

Raised when an invocation fails at the transport level (unknown/stub backend, a missing required `runtime` key, an import-missing dependency, an HTTP ≥ 400, a non-dict response, …). Carries the `backend` tag and the underlying `cause`, and renders as `"[<backend>] <message>"`. Under the harness's `on_error='record'` path it is caught and recorded as a `crash` failure like any other exception.

---

## `execute.monitor`

Source: [`../../src/concursus/execute/monitor.py`](../../src/concursus/execute/monitor.py)

> The monitor **observes** — it never alters plans, injects into agent execution, or touches object storage. It consumes the `AsyncIterator[LogEvent]` from [`invoke_with_tap`](#agentinvokerinvoke_with_tap) and emits a [`HealthSignal`](#executetypes); a terminating signal tells the harness to raise [`PreemptiveTermination`](#executetypes). The Supervisor retains termination authority via that path.

| Symbol | Kind | Summary |
|---|---|---|
| [`ExecutionMonitor`](#executionmonitor) | class | Rule-based health monitor for a single node invocation. |
| [`ExecutionMonitor.watch`](#executionmonitorwatch) | method (async) | Consume the stream, assess per-event, return the final `HealthSignal`. |
| [`MonitorConfig`](#monitorconfig) | dataclass | Per-node monitor thresholds, parsed from the manifest's `monitor` block. |
| [`DefaultMonitorFactory`](#defaultmonitorfactory) | class | `MonitorFactory` for the `HarnessFactory`: builds one `ExecutionMonitor` per node. |
| [`remediation_for`](#remediation_for) | function | A corrective-retry prompt amendment derived from a terminating signal. |
| `FAILURE_IDLE_TIMEOUT` / `FAILURE_ERROR_THRESHOLD` / `FAILURE_TOOL_LOOP` / `FAILURE_TOKEN_BUDGET` | constants | The stable `failure_mode` tags one per strategy — a stable verdict vocabulary a future semantic assessor can also emit. |

### `ExecutionMonitor`

```python
class ExecutionMonitor:
    def __init__(
        self,
        node_id: str,
        config: Optional[MonitorConfig] = None,
        event_sink: Optional[Any] = None,
    )
```

A rule-based health monitor for a single node invocation. Consumes the log stream concurrently with the invoke and assesses each event against four strategies, returning a [`HealthSignal`](#executetypes) when the stream ends (`COMPLETED`) or a strategy fires (`TERMINATE`). `event_sink` is an optional callable receiving each `LogEvent` (UI forwarding / CloudWatch / run-log); a failing sink is swallowed and never breaks monitoring.

The four rule-based strategies, each tagged with a stable `failure_mode`:

| Strategy | `failure_mode` tag | Fires when |
|---|---|---|
| Idle timeout | `idle_timeout` | No log event for `config.idle_timeout_s` (applied *between* events). |
| Error accumulation | `error_threshold` | `config.error_threshold` events of `ERROR` severity or `ERROR` event-type accumulate. |
| Loop detection | `tool_loop` | The last `config.loop_detection_window` `TOOL_CALL` events share one signature (tool name + args). |
| Token budget | `token_budget` | Estimated tokens (~4 chars/token over event content) exceed `config.token_budget` (`0` = unlimited). |

A terminating signal populates `failure_mode` + `evidence` (structured, machine-readable — the raw material [`remediation_for`](#remediation_for) turns into a corrective retry); a healthy/completed signal leaves both empty.

### `ExecutionMonitor.watch`

```python
async def watch(self, log_stream: AsyncIterator[LogEvent]) -> HealthSignal
```

Consume the stream, assess health per-event, and return the final [`HealthSignal`](#executetypes). The idle timeout applies **between** events (via `asyncio.wait_for` on the next event). An empty stream — a backend without streaming — completes immediately (`COMPLETED`, `events_consumed=0`), so timeout-only monitoring for those backends is enforced by the invoke timeout itself. When `config.enabled` is `False`, the stream is drained without assessment (so the invoke is not back-pressured) and a `COMPLETED` signal is returned.

### `MonitorConfig`

```python
@dataclass
class MonitorConfig:
    enabled: bool = True
    idle_timeout_s: float = 300.0
    error_threshold: int = 3
    loop_detection_window: int = 5
    token_budget: int = 0            # 0 = unlimited

    @classmethod
    def from_manifest(cls, manifest: Dict[str, Any]) -> "MonitorConfig"
```

Per-node monitor thresholds. A node with no `monitor:` block gets these defaults (timeout-only monitoring). `from_manifest` reads the manifest's optional `monitor` dict; a non-dict block yields the defaults.

### `DefaultMonitorFactory`

```python
class DefaultMonitorFactory:
    def __init__(self, event_sink: Optional[Any] = None)
    def create(self, node_id: str, manifest: Dict[str, Any]) -> Optional[ExecutionMonitor]
```

The `MonitorFactory` the [`HarnessFactory`](#executeharness_factory) uses to build one `ExecutionMonitor` per node from each node's `monitor` block. Returns `None` for a node that explicitly disables monitoring (`monitor: {enabled: false}`) — which makes the harness fall back to a plain [`invoke()`](#agentinvokerinvoke) with zero streaming overhead.

### `remediation_for`

```python
def remediation_for(signal: "HealthSignal") -> Optional[str]
```

A **prompt amendment** derived from a *terminating* signal, for a corrective retry. It turns the monitor's structured `failure_mode` + `evidence` into supplementary text the harness injects into the retry's envelope as `remediation_context` — an overlay on the frozen compiler-vended task, never a plan mutation. Returns `None` when there is nothing useful to say (so the caller falls back to a plain retry).

This is the **rule tier's** amendment, not a diagnosis: every message states an observed fact plus a generic corrective (e.g. "you called `search` 5 times in a row with identical arguments — vary the arguments, use a different tool, or conclude") and never attributes a cause. It is strictly more than a blind retry and strictly less than a semantic judge. `from concursus.execute.monitor import remediation_for`.

---

## `execute.harness`

Source: [`../../src/concursus/execute/harness.py`](../../src/concursus/execute/harness.py)

> The `AgentHarness` is the per-node **edge machinery** between the Supervisor and a leaf agent. It creates an [`AgentInvoker`](#executeinvoker) in `__init__` from the manifest and owns everything else: input deref (object store → materialized data), prompt serialization, invoke + log forwarding to a monitor, artifact write (data → `ArtifactRef`), and contract enforcement. The Supervisor never reads or writes the object store — the harness does — which is what keeps resume-by-replay holding: the log holds *pointers*.

| Symbol | Kind | Summary |
|---|---|---|
| [`AgentHarness`](#agentharness) | class | Per-node wrapper: creates the invoker, manages I/O, enforces contracts. |
| [`AgentHarness.run`](#agentharnessrun) | method (async) | Execute the full harness lifecycle over one envelope; return the output-refs envelope. |
| [`ObjectStore`](#objectstore--executionmonitor-protocols) | Protocol | The `get_object` / `put_object` artifact-store interface (see [`execute.object_store`](#executeobject_store)). |
| [`ExecutionMonitor`](#objectstore--executionmonitor-protocols) | Protocol | The optional `watch(log_stream) -> HealthSignal` monitor interface the Supervisor injects. |

### `AgentHarness`

```python
class AgentHarness:
    def __init__(
        self,
        manifest: Dict[str, Any],
        store: ObjectStore,
        clients: Optional[Dict[str, Any]] = None,
        monitor: Optional[ExecutionMonitor] = None,
        output_prefix: str = "",
    )
```

Per-node wrapper. Constructs its own [`AgentInvoker`](#executeinvoker) from `manifest`, and extracts the contract for validation: `input_schema` / `output_schema` (each read via a `_schema_properties` helper that accepts **both** the nested `{"properties": {...}}` shape and a flat `{field: schema}` map — the same logic [`validate_output`](#validate_output) uses, so one contract declaration means the same thing to both readers) and the `output_mapping` (`response_key → contract_field`). `store` is the [`ObjectStore`](#objectstore--executionmonitor-protocols) for artifact read/write (inject a [`FileStore`](#executeobject_store) for tests); `monitor` is the optional [`ExecutionMonitor`](#executemonitor); `output_prefix` is the compiler-vended prefix for this node's output artifacts.

### `AgentHarness.run`

```python
async def run(self, envelope: Dict[str, Any]) -> Dict[str, Any]
```

Execute the full harness lifecycle and return the **output-refs envelope** (`{field: ArtifactRef-or-scalar}`). The `envelope` is the structured request the [harness node executor](#executeharness_factory) builds:

| Envelope key | Meaning |
|---|---|
| `task` | The compiler-vended task description (frozen). |
| `io` | The plan node's authored I/O declaration (`{"inputs": ..., "outputs": ...}`); when present it **outranks** the manifest's contract (the manifest is a per-agent fallback). |
| `inputs` | The merged external + wired inputs (each value a scalar, or an `ArtifactRef` dict to deref). |
| `context` | Invoke context — carries the `session_id`. |
| `static_context` | The already-projected tiered static context (a string, or JSON-encoded dict). |
| `remediation_context` | *(retry only)* The [`remediation_for`](#remediation_for) overlay, appended after the task — absent on a first attempt, so a normal run's prompt is byte-for-byte unchanged. |

The lifecycle, in order:

1. **Deref inputs.** For each input declared `type: artifact` with a `uri`, fetch the bytes from the object store, verify `content_hash` (raising `ValueError` on mismatch), and deserialize by `content_type`; scalars pass inline.
2. **Serialize prompt.** Assemble `static_context` + `Task:` + the rendered **output contract** (an advisory instruction block describing each declared output's format/sections/keys) + any `remediation_context`.
3. **Invoke (+ monitor).** With a `monitor` present, call [`invoke_with_tap`](#agentinvokerinvoke_with_tap) and `await monitor.watch(log_stream)`; a `should_terminate` signal raises [`PreemptiveTermination`](#executetypes). Without a monitor, call plain [`invoke`](#agentinvokerinvoke).
4. **Write outputs.** Apply the `output_mapping`, then for each declared `type: artifact` output serialize the value (`application/json` / `text/csv` / `text/*` / bytes) and `put_object` it, returning a full `ArtifactRef` (`uri`, `content_type`, `content_hash`, `bytes`); scalars pass inline. Undeclared fields are dropped.
5. **Enforce contract.** A required output field missing from the refs raises `ValueError`; a field declared `artifact` but not a `{uri: ...}` dict raises `ValueError`.

> **Gotcha:** `text/csv` is deliberately **asymmetric** — the write side accepts structured rows and emits real CSV, but the read side (deref) returns CSV **text**, honoring the convention that `application/json` yields data while `text/*` yields text. A consumer wanting rows can `csv.DictReader` the text itself.

### `ObjectStore` / `ExecutionMonitor` Protocols

```python
class ObjectStore(Protocol):
    async def get_object(self, uri: str) -> bytes: ...
    async def put_object(self, uri: str, data: bytes, content_type: str) -> str: ...

class ExecutionMonitor(Protocol):
    async def watch(self, log_stream: AsyncIterator[LogEvent]) -> HealthSignal: ...
```

The two injectable seams the harness depends on structurally (not by concrete type). `ObjectStore` is implemented by [`FileStore`](#executeobject_store) / [`S3Store`](#executeobject_store); the `ExecutionMonitor` Protocol is satisfied by [`execute.monitor.ExecutionMonitor`](#executemonitor). Declaring them as Protocols is what lets a test inject a fake store or monitor with no inheritance.

---

## `execute.harness_factory`

Source: [`../../src/concursus/execute/harness_factory.py`](../../src/concursus/execute/harness_factory.py)

> This module is the **bridge**: it turns a set of raw manifests + an `ObjectStore` into a [`NodeExecutor`](#nodeexecutor--node_executors) + `node_kind_fn` the [`Supervisor`](#supervisor) plugs in, so the [harness](#executeharness) runs a node *inside* the governed dispatch seam. Nodes without a `runtime:` block keep the legacy `_dispatch` path byte-for-byte.

| Symbol | Kind | Summary |
|---|---|---|
| [`HarnessFactory`](#harnessfactory) | class | Builds `AgentHarness` instances and the Supervisor integration hooks. |
| [`HarnessFactory.create_harness`](#harnessfactorycreate_harness) | method | Build an `AgentHarness` for one node invocation. |
| [`HarnessFactory.make_executor`](#harnessfactorymake_executor) | method | Return a `NodeExecutor` that runs a node through the harness. |
| [`HarnessFactory.make_kind_fn`](#harnessfactorymake_kind_fn) | method | Return a `node_kind_fn` routing `runtime`-bearing nodes to the harness kind. |
| [`make_harness_supervisor_factory`](#make_harness_supervisor_factory) | function | Wrap a `HarnessFactory` into a GovernorLoop-compatible `supervisor_factory`. |
| `HARNESS_NODE_KIND` | constant | The node-kind key the harness registers (`"harness"`). |
| `MonitorFactory` | Protocol | The `create(node_id, manifest) -> Optional[ExecutionMonitor]` interface (e.g. [`DefaultMonitorFactory`](#defaultmonitorfactory)). |

### `HarnessFactory`

```python
class HarnessFactory:
    def __init__(
        self,
        manifests: Dict[str, Dict[str, Any]],
        store: ObjectStore,
        monitor_factory: Optional[MonitorFactory] = None,
        output_prefix_root: str = "",
        clients: Optional[Dict[str, Any]] = None,
    )
```

Constructs [`AgentHarness`](#agentharness) instances and provides the two Supervisor integration hooks. `manifests` are **raw** manifest dicts (not `AgentManifest` objects — see [`to_harness_dict`](#coremanifest-the-harness-path-additions)); `store` is the shared [`ObjectStore`](#executeobject_store); `monitor_factory` (e.g. [`DefaultMonitorFactory`](#defaultmonitorfactory)) builds per-node monitors; `output_prefix_root` roots the per-node artifact prefix; `clients` is a shared client bundle passed to each harness's invoker.

### `HarnessFactory.create_harness`

```python
def create_harness(self, node_id: str, session_id: str) -> AgentHarness
```

Build an `AgentHarness` for one node invocation: looks up the raw manifest, builds a monitor via the `monitor_factory` (if any), and computes the node's `output_prefix` as `f"{output_prefix_root}/{session_id}/{node_id}"` (or `""` when no root). The harness node executor calls this **fresh per attempt**, because an `ExecutionMonitor` accumulates error counts and tool signatures — reusing one would re-trip its thresholds on the first event of a retry.

### `HarnessFactory.make_executor`

```python
def make_executor(self)   # -> NodeExecutor: (supervisor, node, inputs, wiring) -> None
```

Return a [`NodeExecutor`](#nodeexecutor--node_executors) that runs a node through the harness. It (1) resolves the node's static context, wired upstream outputs, and external inputs into the structured **envelope** [`AgentHarness.run`](#agentharnessrun) consumes; (2) runs the harness with a **bounded corrective retry**; (3) applies the same manifest-level output gates the default `_dispatch` runs; and (4) stores the result via the Supervisor's `StateStore` — reusing the Supervisor's [`record_failure`](#record_failure) / [`validate_output`](#validate_output) / [`check_hive_contract`](#check_hive_contract) / [`check_acceptance`](#check_acceptance) so the two node-kind branches cannot drift.

Its retry / failure handling maps each outcome onto the [four failure classes](#the-four-class-failure-taxonomy):

- **`PreemptiveTermination`** (monitor verdict) is **retryable**: if attempts remain and this failure mode has not already been remediated, the executor overlays the [`remediation_for`](#remediation_for) amendment onto the envelope and retries; otherwise it records a `preemptive_termination` failure. A mode that recurs *after* its remediation was applied escalates rather than looping on advice already given.
- **`asyncio.CancelledError`** (futility cancellation) is **never retried** — the work is provably unconsumable; it records a `futility_cancelled` failure with the registry's retained reason (as `blocked_on`, so `summary_line()` stays legible).
- **Any other exception** — most importantly a **contract violation** from the harness — records a `crash` failure under `on_error='record'` (or re-raises under `'raise'`), after exhausting retries.

Two constraints shape the retry budget: a **side-effecting** node (per its manifest) gets exactly one attempt regardless of `max_attempts` (retrying a partial real-world effect could double it), and futility is never retried. For an `agentcore`-backed node it also reuses the Supervisor's `_check_arn_integrity` before invoke, so an unprovisioned/stale plan binding is caught before the AWS call rather than surfacing as an opaque mid-run error.

### `HarnessFactory.make_kind_fn`

```python
def make_kind_fn(self) -> Callable[[str], str]
```

Return a `node_kind_fn` that routes any node whose raw manifest declares a `runtime` block to `HARNESS_NODE_KIND` (`"harness"`), and every other node to `"default"` (the legacy `_dispatch` path). This is the switch that keeps the stack opt-in per node: no `runtime:` block ⇒ unchanged dispatch.

### `make_harness_supervisor_factory`

```python
def make_harness_supervisor_factory(harness_factory: "HarnessFactory")
    # -> supervisor_factory(*, plan, manifests, store, invoke_fn, arns, session_id, held=None) -> Supervisor
```

Build a **GovernorLoop-compatible** `supervisor_factory` with the harness seam wired in. The `GovernorLoop` constructs one `Supervisor` per episode via its `supervisor_factory` seam; the default factory does **not** pass `node_executors` / `node_kind_fn`, so loop-driven runs never reach the harness. This wrapper preserves the default factory's exact contract (including the held-set semantics) and additionally injects the `NODE_EXECUTORS` registry plus the harness executor under `HARNESS_NODE_KIND`, and the `make_kind_fn()` selector — so loop episodes route `runtime`-bearing nodes through the harness while every other node keeps the legacy path byte-for-byte.

```python
from concursus.execute import HarnessFactory, make_harness_supervisor_factory, S3Store
from concursus.governor import GovernorLoop

factory = HarnessFactory(manifests=raw_manifests, store=S3Store())
loop = GovernorLoop(
    goal, agent_manifests,
    supervisor_factory=make_harness_supervisor_factory(factory),
    # ...
)
```

---

## `execute.object_store`

Source: [`../../src/concursus/execute/object_store.py`](../../src/concursus/execute/object_store.py)

> The two concrete backends behind the harness's [`ObjectStore`](#objectstore--executionmonitor-protocols) Protocol. Both are `async` and offload the blocking I/O to an executor, so they never block the event loop the invoke runs on.

| Symbol | Kind | Summary |
|---|---|---|
| [`FileStore`](#filestore) | class | Local-filesystem `ObjectStore` (`file://` and bare paths) — test / offline. |
| [`S3Store`](#s3store) | class | AWS S3 `ObjectStore` (`s3://`, lazy boto3) — production. |

### `FileStore`

```python
class FileStore:
    def __init__(self, root: Optional[str] = None)
    async def get_object(self, uri: str) -> bytes
    async def put_object(self, uri: str, data: bytes, content_type: str) -> str
```

Local-filesystem `ObjectStore`, suitable for testing, offline development, and local runs. Maps `file:///path` URIs to disk paths, handles bare/absolute paths, and — for convenience in tests — maps `s3://bucket/key` URIs onto `root/bucket/key`. When `root` is set, relative paths resolve under it. `put_object` creates parent directories; `get_object` raises `FileNotFoundError` if the object is absent. Nothing here touches AWS.

### `S3Store`

```python
class S3Store:
    def __init__(self, client=None, region: str = "us-west-2")
    async def get_object(self, uri: str) -> bytes
    async def put_object(self, uri: str, data: bytes, content_type: str) -> str
```

Production `ObjectStore` over `s3://bucket/key` URIs. Lazily imports `boto3` and builds an `s3` client on first use (or takes a pre-built `client`), raising a clear `RuntimeError` pointing at `pip install concursus[agentcore]` if boto3 is missing. A malformed URI (not `s3://`, or missing bucket/key) raises `ValueError`.

---

## `execute.futility`

Source: [`../../src/concursus/execute/futility.py`](../../src/concursus/execute/futility.py)

> The machinery behind the opt-in [`cancel_futile`](#supervisor) seam of [`_run_parallel`](#opt-in-bounded-antichain-parallel-wave-runparalleln). It is policy *computation* and *handles* only: it never chooses what to dispatch (the Supervisor's job), never mutates the plan (the compiler's), never judges health (the monitor's), and never writes to the store — a condemned worker writes its own failed record on the existing path. `__all__` = `CancelTokenRegistry`, `descendants`, `futility_closure`, `invert_wiring`, `run_registered`.

| Symbol | Kind | Summary |
|---|---|---|
| [`invert_wiring`](#invert_wiring) | function | Invert `node -> [AgentRef(producer)]` into `producer -> {consumers}`. |
| [`descendants`](#descendants) | function | Transitive consumers of a node — the doomed region once it has failed. |
| [`futility_closure`](#futility_closure) | function | The in-flight nodes whose output now feeds *only* the doomed region. |
| [`CancelTokenRegistry`](#canceltokenregistry) | class | Thread-safe `(node, attempt) -> (loop, task)` map for cancelling in-flight work. |
| [`run_registered`](#run_registered) | function (async) | Await a coroutine with its cancel token registered for the duration. |

### `invert_wiring`

```python
def invert_wiring(wiring_by_node: Mapping[str, Iterable["AgentRef"]]) -> Dict[str, Set[str]]
```

Invert `node -> [AgentRef(producer=...)]` into `producer -> {consumers}`. Computed **once** at wave-loop entry: `plan.wiring` is frozen for the run, so the consumer graph cannot go stale and every later futility question is a set-containment test.

### `descendants`

```python
def descendants(node: str, consumers: Mapping[str, Set[str]]) -> Set[str]
```

The transitive consumers of `node` — the **doomed region** once `node` has failed (excludes `node` itself). Iterative and visited-guarded, so it is cycle-safe even though a compiled plan is acyclic. The result is downward-closed by construction, which is what makes the cheap direct-consumer test in [`futility_closure`](#futility_closure) equivalent to checking every downstream path.

### `futility_closure`

```python
def futility_closure(
    consumers: Mapping[str, Set[str]],
    failed: str,
    in_flight: Iterable[str],
) -> Set[str]
```

The in-flight nodes whose output now feeds **only** the doomed region. A node `B` is futile iff `consumers(B)` is **non-empty** (a sink node is a run deliverable, never futile) and **wholly inside** `descendants(failed)` (if `B` also feeds a node outside the doomed region, that consumer still needs `B`, so `B` survives — this is what makes cancellation discriminating rather than a blunt cancel-the-whole-wave). `failed` is excluded; returns an empty set when the failed node was itself a sink.

### `CancelTokenRegistry`

```python
class CancelTokenRegistry:
    def register(self, node: str, attempt: int, loop, task) -> bool
    def revoke(self, node: str, attempt: int) -> None
    def condemn(self, node: str, reason: str) -> bool
    def condemn_many(self, nodes: Iterable[str], reason: str) -> Set[str]
    def reason_for(self, node: str) -> Optional[str]
    @property
    def decisions(self) -> Dict[str, str]
```

A thread-safe map of `(node, attempt) -> (event loop, task)` for cancellable in-flight work. `asyncio.run` builds its event loop internally and returns no handle, and `concurrent.futures.Future.cancel` returns `False` once a callable has started — so the only way to abort a running invoke from another thread is for the worker to **self-register** its `(loop, task)` (via [`run_registered`](#run_registered)); the Supervisor then condemns from its own thread through `loop.call_soon_threadsafe(task.cancel)`.

Two races are made benign: a condemnation issued *before* its token registers is **remembered** and fires at registration (closing the submit-then-register window), and a condemnation arriving *after* revocation is a silent no-op (the node already finished; its store record is authoritative). Keying on `attempt` keeps a retry's fresh token from being revoked by its predecessor's cleanup. `condemn_many` condemns a whole set and returns the subset whose live token was reached; `reason_for` / `decisions` expose the retained reasons (`decisions` is the read-out the GovernorLoop harvests at the episode boundary).

### `run_registered`

```python
async def run_registered(
    registry: CancelTokenRegistry,
    node: str,
    attempt: int,
    coro: Awaitable[Any],
) -> Any
```

Await `coro` with its cancel token registered for the duration, then revoke it in a `finally`. Wrap the harness coroutine in this and pass the wrapper to `asyncio.run`, so the task self-registers the moment its event loop exists; if `node` was condemned before registration, the cancellation is delivered at the first suspension point inside `coro`. The [harness node executor](#harnessfactorymake_executor) wraps its coroutine this way **only** while an opt-in `cancel_futile` wave is running (the registry is `None` on every serial run, leaving the coroutine unwrapped).

---

## `execute.types`

Source: [`../../src/concursus/execute/types.py`](../../src/concursus/execute/types.py)

> The shared value types the invoker, monitor, and harness pass between one another. All are re-exported from `concursus.execute`.

| Symbol | Kind | Summary |
|---|---|---|
| [`LogEvent`](#logevent) | dataclass | A single observable event from a leaf agent's execution. |
| `LogEventType` | str Enum | `TOOL_CALL` / `REASONING` / `ERROR` / `PROGRESS` / `OUTPUT_CHUNK`. |
| `LogSeverity` | str Enum | `INFO` / `WARNING` / `ERROR`. |
| [`HealthSignal`](#healthsignal) | dataclass | The assessment the `ExecutionMonitor` emits. |
| `HealthStatus` | str Enum | `HEALTHY` / `DEGRADED` / `TERMINATE` / `COMPLETED`. |
| [`PreemptiveTermination`](#preemptivetermination) | exception | Raised by the harness when the monitor signals early termination. |
| [`InvokeResult`](#invokeresult) | dataclass | The `(result, log_stream)` pair from `invoke_with_tap`. |

### `LogEvent`

```python
@dataclass
class LogEvent:
    timestamp: datetime
    node_id: str
    event_type: LogEventType
    content: str
    severity: LogSeverity = LogSeverity.INFO
    metadata: Dict[str, Any] = field(default_factory=dict)
```

A single observable event from a leaf agent's execution — produced by the [`AgentInvoker`](#executeinvoker)'s log tap and consumed by the [`ExecutionMonitor`](#executemonitor). The monitor's loop detector, for instance, reads `metadata` for the tool name + args to compute a stable tool signature.

### `HealthSignal`

```python
@dataclass
class HealthSignal:
    status: HealthStatus
    reason: str = ""
    should_terminate: bool = False
    events_consumed: int = 0
    failure_mode: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
```

The assessment [`ExecutionMonitor.watch`](#executionmonitorwatch) returns. `reason` is prose for a human; `failure_mode` (one of the stable [strategy tags](#executemonitor)) and `evidence` are structured for a machine — the raw material [`remediation_for`](#remediation_for) turns into a corrective retry. Both are empty for a healthy or completed signal; only a terminating assessment populates them.

### `PreemptiveTermination`

```python
class PreemptiveTermination(Exception):
    def __init__(self, reason: str, health_signal: Optional[HealthSignal] = None)
```

Raised by the [harness](#executeharness) when the monitor's signal has `should_terminate=True`. Carries the `reason` and the originating `health_signal`, so the [harness node executor](#harnessfactorymake_executor) can derive a corrective-retry amendment (and, on a terminal miss, record a [`preemptive_termination`](#the-four-class-failure-taxonomy) failure).

### `InvokeResult`

```python
@dataclass
class InvokeResult:
    result: Dict[str, Any]
    log_stream: AsyncIterator[LogEvent]
```

The final response plus the live log stream — the named-dataclass equivalent of the tuple [`invoke_with_tap`](#agentinvokerinvoke_with_tap) returns. The harness awaits `result` while concurrently consuming `log_stream`.

---

## `core.manifest`: the harness-path additions

The typed [`AgentManifest`](core.md#coremanifest) gained the blocks the harness stack reads, so a caller can hold **one** typed manifest instead of hand-maintaining a second raw representation.

- **`AgentRuntime`** — the OPTIONAL, frozen declaration of *how* to invoke the agent (the harness path's addressing). `backend` is typed and validated (fail-closed against `RUNTIME_BACKENDS` = `("callable", "agentcore", "http", "strands", "api")`); the rest is an open `config` mapping (the invoker validates each backend's keys per-backend at dispatch). The empty default is **falsy**, so a manifest with **no** `runtime:` block behaves exactly as before — routing to the legacy `_dispatch` path via [`HarnessFactory.make_kind_fn`](#harnessfactorymake_kind_fn).
- **`monitor`** — the optional per-node monitor block [`MonitorConfig.from_manifest`](#monitorconfig) reads.
- **`output_mapping`** — the `response_key → contract_field` remap the [harness](#executeharness) applies before writing outputs.
- **`to_harness_dict()`** — derives the RAW dict shape [`HarnessFactory`](#executeharness_factory) consumes from the typed manifest (`name`, `contract`, and any non-empty `runtime` / `monitor` / `output_mapping` / `side_effecting`), so the raw view is a derivation of the typed source rather than a second hand-authored copy.

`AgentRuntime.validate` fails closed on an unknown backend, so a manifest typo is a compile-time error rather than a mid-run `Unsupported backend` from the invoker. A manifest whose `runtime.backend` is not AgentCore-hosted is exempt from the `container_uri` / `agent_runtime_arn` registry requirement (there is no AgentCore runtime to name). See the [`core` reference](core.md#coremanifest).

---

## Invariants at a glance

- **`run()` is a single static forward pass.** It walks `plan.order` once; `plan.order` / `plan.wiring` are never mutated at runtime. A failure or block prunes only within `plan.wiring`, never rewrites topology. `run(parallel=N)` is the same static pass — it dispatches independent antichains concurrently but never re-plans; the store contents are byte-for-byte identical to the serial pass.
- **The whole runtime stack runs inside the dispatch seam.** The harness executor is a `NodeExecutor` plugged into `NODE_EXECUTORS`; it reads the frozen plan, invokes an agent, and writes the same append-only log any node writes. It never mutates `plan.order`, re-plans, or reaches inside another node's execution. Wire no harness factory and none of it runs — the default `invoke_fn` pass is unchanged.
- **Structural validation happens exactly once.** Dangling `AgentRef` producers and wiring cycles are rejected at construction (`_validate_plan_structure`); `run()` has no structural re-check loop.
- **Resume by skipping.** A node already in `store.completed()` is skipped; a re-run over the same `StateStore` continues where it left off — never a re-plan.
- **Resume identity is opt-in.** `verify_plan_identity=True` persists [`plan_fingerprint`](#plan_fingerprint) and asserts it on resume, raising [`PlanIdentityError`](#planidentityerror) on a divergent plan; the default `False` is the original byte-for-byte completed-node skip.
- **One session id per run.** A stable ≥33-char `runtimeSessionId` spans every invoke, exposed as [`session_id`](#supervisorsession_id).
- **Four failure classes, one shared writer.** Every `failed` record is written by [`record_failure`](#record_failure) and tagged `crash` / `hold` / `preemptive_termination` / `futility_cancelled` — surfaced as `summary()["failure_classes"]` (see the [taxonomy](#the-four-class-failure-taxonomy)). A held node writes *nothing* and contributes to no class.
- **The monitor observes; the Supervisor terminates.** [`ExecutionMonitor`](#executemonitor) only emits a [`HealthSignal`](#healthsignal); the [harness](#executeharness) turns a terminating signal into [`PreemptiveTermination`](#preemptivetermination) and a bounded [`remediation_for`](#remediation_for) corrective retry. The monitor never alters a plan or reaches into agent execution.
- **Futility cancellation prunes, never reroutes.** With `cancel_futile=True`, a mid-wave failure condemns only the in-flight siblings whose every consumer is provably unreachable ([`futility_closure`](#futility_closure)); a condemned node records `futility_cancelled` on the existing path. It never touches the frozen plan.
- **Opt-in QA is default-off.** [`check_hive_contract`](#check_hive_contract) and [`check_acceptance`](#check_acceptance) run only under `check_acceptance=True`; when on, a QA/storability miss raises `SchemaError` on the existing retry/record path — the output is not admitted and earns no trust.
- **Tiered static context is opt-in and read-only.** `payload_tier_fn` overlays a tiered static-context projection *under* the external inputs (external inputs win); it prefers the frozen `plan.payload_contract[node]["static_context"]` else the live tier fn. With neither, the payload is byte-for-byte unchanged. `execute` consumes the tier — it never authors or re-tiers the contract.
- **The invoker is wire-level only.** [`AgentInvoker`](#executeinvoker) dispatches by `manifest.runtime.backend` and touches no object store, prompt, or contract; the [harness](#executeharness) owns those. `boto3` / `aiohttp` / `strands` are lazily imported per-backend.
- **One transport seam for three protocols (Supervisor path).** HTTP/MCP/A2A differences are absorbed by the generated serving wrapper; the `Supervisor`'s own dispatch builds one payload and calls one [`InvokeFn`](#invokefn) signature for all three. A2A is a *served* protocol, not a peer-initiated call — producer→consumer flow is always Supervisor-mediated through the wiring.
- **Node dispatch is a pluggable-but-default seam.** [`NODE_EXECUTORS`](#nodeexecutor--node_executors) maps a node-kind key to a uniform `(supervisor, node, inputs, wiring) -> None` handler; with no `node_executors` / `node_kind_fn`, every node routes to the `"default"` kind → `_dispatch`, byte-for-byte unchanged.

## See also

- [Guide: Running Agents](../guides/running-agents.md) — the runtime stack in prose: the four backends, per-node monitoring, corrective retry, artifact I/O, and futility cancellation.
- [Guide: Compiling & Running a Team](../guides/compiling-and-running.md) — where `resolve → assemble → freeze → supervise` fit together, and `Supervisor.run` in context.
- [Guide: Durable Run State](../guides/durable-state.md) — the `StateStore` seam, its backends, and replay-resume.
- [Guide: Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md) — turning a frozen plan into live runtimes the default `InvokeFn` calls.
- [Guide: The Governor](../guides/governor.md) — the strictly-outer loop that schedules and re-compiles around the compiler (and supplies the `held` set and the harness `supervisor_factory`).
- [API Reference: core](core.md) — `AgentDAG`, `AgentManifest` (incl. `AgentRuntime` / `to_harness_dict`), `AgentRef`, and `extract`.
- [API Reference: assemble](assemble.md) — where `wiring`, `entries`, and `payload_contract` are frozen.
- [API Reference: build](build.md) — the HTTP/MCP/A2A serving templates, `PORTS`, and `BuildPlanEntry.invoke`.
- [API Reference: governor](governor.md) — `project_context` / `make_payload_tier` / `Tier` that tier the static context.
- [Core Concepts](../concepts.md) · [Overview](../overview.md) · [Documentation Index](../README.md)
- Source: [`execute/supervisor.py`](../../src/concursus/execute/supervisor.py), [`execute/invoker.py`](../../src/concursus/execute/invoker.py), [`execute/monitor.py`](../../src/concursus/execute/monitor.py), [`execute/harness.py`](../../src/concursus/execute/harness.py), [`execute/harness_factory.py`](../../src/concursus/execute/harness_factory.py), [`execute/object_store.py`](../../src/concursus/execute/object_store.py), [`execute/futility.py`](../../src/concursus/execute/futility.py), [`execute/types.py`](../../src/concursus/execute/types.py).
