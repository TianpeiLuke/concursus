# Guide: Running Agents

*Invoke real leaf agents through the harness — four backends, per-node health monitoring, corrective retry, artifact I/O, and futility cancellation — all inside the governed dispatch seam.*

The [compile-and-run guide](compiling-and-running.md) documents the `Supervisor` as an
abstract dispatcher: it walks a frozen `plan.order` and, for each node, hands an assembled
payload to an injected `invoke_fn(arn, qualifier, session_id, payload_bytes)`. That
`invoke_fn` is the whole story only when *you* supply the transport. This guide covers the
batteries-included path Concursus now ships: the **`concursus.execute`** runtime stack that
invokes a real leaf agent, monitors its health as it runs, enforces its I/O contract, writes
its artifacts, and — when a whole subtree becomes unreachable mid-wave — cancels work that
can no longer be consumed.

Read the load-bearing invariant first, because the stack was built to preserve it:
**Concursus is a compiler, not a runtime governor.** Everything in this guide runs *inside*
the node-dispatch seam — the same `(supervisor, node, inputs, wiring) -> None` `NodeExecutor`
contract the [custom-node-kinds section](compiling-and-running.md#custom-node-kinds-node_executors-node_executors--node_kind_fn)
documents. The harness executor reads the **frozen** plan, invokes an agent, and writes the
same append-only log any other node writes. It **never** mutates `plan.order`, never
re-plans, and never reaches inside another node's execution. The monitor *observes* a log
stream and can ask the Supervisor to terminate a node early; it cannot alter a plan or inject
into an agent. Corrective retry is an **overlay** on the retry envelope, never an edit of the
compiler-vended task.

> **The whole stack is opt-in and default-off.** It reaches a run **only** through the
> `NodeExecutor` seam. A `Supervisor` constructed without a harness factory — the default —
> takes the legacy `invoke_fn` path for every node and is **byte-for-byte unchanged**. You
> turn the stack on per node by declaring a `runtime:` block in the manifest and wiring
> `make_harness_supervisor_factory` (or `node_executors=` / `node_kind_fn=`) into the
> Supervisor. Declare no `runtime:` block on a node and it keeps the `_dispatch` path.

All symbols in this guide are exported from `concursus.execute` (see
[`execute/__init__.py`](../../src/concursus/execute/__init__.py)); the manifest field that
opts a node in lives in `concursus.core.manifest`. For the terse symbol catalog see the
[execute API reference](../reference/execute.md).

---

## The mental model: a harness per node, inside the seam

One run still walks the frozen `plan.order` exactly once. When a node's manifest declares a
`runtime:` block, `HarnessFactory.make_kind_fn()` routes it to the **harness node executor**
instead of the legacy `_dispatch`. That executor builds a fresh [`AgentHarness`](../../src/concursus/execute/harness.py)
for the node and runs one lifecycle:

```
resolve wired inputs  ->  AgentHarness.run(envelope)
                              │
        deref inputs (ObjectStore) ─┐
        serialize prompt (+ contract, + remediation overlay)
        AgentInvoker.invoke_with_tap(prompt, inputs, context)  ──► leaf agent
        ExecutionMonitor.watch(log_stream)  ──► HealthSignal
        write outputs (ObjectStore)  ──► ArtifactRefs
        enforce contract
                              │
                          refs ─┘  ->  Supervisor output gates  ->  StateStore.put
```

The pieces, and the file each lives in:

| Component | Role | Source |
|---|---|---|
| [`AgentInvoker`](../../src/concursus/execute/invoker.py) | Wire-level dispatch to a real leaf agent by `runtime.backend`; returns the response + a live `LogEvent` stream | `execute/invoker.py` |
| [`ExecutionMonitor`](../../src/concursus/execute/monitor.py) | Rule-based per-node health over that stream → `HealthSignal`; a terminating verdict → `PreemptiveTermination` | `execute/monitor.py` |
| [`AgentHarness`](../../src/concursus/execute/harness.py) | Per-node wrapper: input deref, prompt, invoke, monitor wiring, output write, contract enforcement | `execute/harness.py` |
| [`HarnessFactory`](../../src/concursus/execute/harness_factory.py) | Builds a harness per node and provides the `NodeExecutor` + `node_kind_fn` seam hooks | `execute/harness_factory.py` |
| [`FileStore` / `S3Store`](../../src/concursus/execute/object_store.py) | The `ObjectStore` artifact backends (boto3 lazy) | `execute/object_store.py` |
| [`futility`](../../src/concursus/execute/futility.py) | Cancels in-flight work whose every consumer just became unreachable | `execute/futility.py` |

The invoker never touches storage; the harness owns all I/O. The monitor only observes. The
Supervisor keeps termination authority (via the harness's `PreemptiveTermination` path) and
retry policy. Each organ does one thing.

---

## Authoring `runtime:` on a manifest — the opt-in switch

A node opts into the harness path by declaring a `runtime:` block. The block is modelled by
[`AgentRuntime`](../../src/concursus/core/manifest.py) on the typed `AgentManifest`, and its
one required key is `backend` — one of `callable`, `agentcore`, `http`, `strands`, or `api`.
The remaining keys are backend-specific and pass through as an open `config` mapping; the
invoker validates them per-backend at dispatch.

```yaml
# summarize.agent.yaml
name: summarize
runtime:
  backend: http
  endpoint: "https://svc.internal.example.com/invoke"
  timeout_s: 30
contract:
  inputs:
    source_data: { type: artifact, content_type: text/csv }
  outputs:
    summary: { type: artifact, content_type: text/markdown, required: true }
monitor:                 # optional — omit for timeout-only monitoring
  idle_timeout_s: 120
  error_threshold: 3
output_mapping:          # optional — response_key -> contract_field
  result_text: summary
```

`backend` is validated **at compile time**: `AgentManifest.validate()` fails closed on any
value outside `RUNTIME_BACKENDS`, so a typo is a compile error rather than a mid-run
`Unsupported backend` from the invoker. Declaring a non-AgentCore backend also relaxes the
registry gate — an in-process `callable` or a standalone `http` service genuinely has no
AgentCore runtime to name, so `validate()` no longer forces a `container_uri` /
`agent_runtime_arn` on those nodes (it still requires one for a node with no `runtime:` block
or an explicitly `agentcore` one).

> **Gotcha:** the `runtime:` block is **falsy when empty**. A manifest with no `runtime:` (or
> an empty one) behaves exactly as before — `make_kind_fn` routes it to the legacy
> `_dispatch` path. You only leave the default path by declaring a real `backend`.

`AgentManifest.to_harness_dict()` derives the raw dict shape the `HarnessFactory` consumes
(`name`, `runtime`, `contract`, `monitor`, `output_mapping`, `side_effecting`) from the one
typed manifest, so you hold a single source of truth instead of hand-maintaining two.

---

## The four invoker backends (+ one stub)

[`AgentInvoker`](../../src/concursus/execute/invoker.py) is constructed by the harness from
the manifest and an optional pre-built `clients` bundle for connection reuse. It exposes
three entry points; the harness uses `invoke_with_tap`:

```python
from concursus.execute import AgentInvoker

invoker = AgentInvoker(manifest, clients=None)
result = await invoker.invoke(prompt, inputs, context)                 # fire-and-forget, no stream
result, log_stream = await invoker.invoke_with_tap(prompt, inputs, context)  # + live LogEvent stream
result = invoker.invoke_sync(prompt, inputs, context)                  # sync wrapper
```

`invoke_with_tap` returns a `Tuple[dict, AsyncIterator[LogEvent]]` — the final response plus
the stream the monitor consumes concurrently. `_dispatch` routes on `runtime.backend`:

**`callable` — in-process Python function.** The default backend. Names the function one of
two ways: `runtime.client: "<key>"` resolves a *live* callable from the injected `clients`
bundle (the only way to hand this backend a closure, a bound method, or a `Mock`), or
`runtime.entry: "module:function"` imports and caches it. The callable may be sync or async
and returns **either** a bare `dict` (result, no log stream) **or** a `(dict,
AsyncIterator[LogEvent])` pair. The tuple form is what lets an in-process agent feed the real
monitor — a bare `dict` reports `COMPLETED` immediately.

```python
# entry-point form: runtime.entry: "my_agents.summarize:run"
def run(prompt: str, inputs: dict, context: dict) -> dict:
    return {"summary": summarize(inputs["source_data"])}

# injected-callable form: runtime.client: "summarize_fn", clients={"summarize_fn": run}
```

**`agentcore` — AWS Bedrock AgentCore.** Calls `invoke_agent(agentId, agentAliasId,
sessionId, inputText)` on a lazily-created `bedrock-agent-runtime` client (reused from
`clients["bedrock_agent"]` if present). Requires `runtime.agent_id`; `alias_id` defaults to
`"TSTALIASID"` and `region` to `us-west-2`. The response `completion` stream is drained into
the result text plus a `LogEvent` per **trace** event — `orchestrationTrace` maps to
`TOOL_CALL` / `REASONING` / `OUTPUT_CHUNK`, a `failureTrace` to `ERROR`. `boto3` is imported
lazily; a real invoke without the `[agentcore]` extra raises `InvokerError` at call time.

**`http` — standalone HTTPS service.** POSTs `{"prompt", "inputs", "context"}` to
`runtime.endpoint` (method/headers/`timeout_s` configurable). Uses `aiohttp` when available
and falls back to `urllib` when it is not — no hard dependency. When the response
`content_type` is `text/event-stream`, it parses **SSE**: each `data:` line whose JSON
`type` is not `"result"` becomes a `PROGRESS` `LogEvent`, and the `result`-typed event
carries the final payload.

**`strands` — AWS Strands Agent SDK.** Runs a pre-built `strands.Agent` (from
`clients["strands_agent"]`, or built from `runtime.model_id` + `runtime.system_prompt`).
Strands agents are synchronous, so the invoker runs them in an executor; a streaming callback
maps Strands event types (`tool_use`, `thinking`, `error`, `text`) to `LogEventType`. The
`strands` package is imported lazily.

**`api` — declared stub.** Reserved for a future REST backend whose auth/payload/response
contract differs from `http`. It is a recognized `RUNTIME_BACKENDS` literal (so `validate()`
accepts it) but raises `InvokerError` at dispatch today. Use `http` for standard REST
services.

> **Gotcha:** only `callable` (tuple form), `agentcore` (traces), `http` (SSE), and
> `strands` (callback) ever emit `LogEvent`s. A backend that does not stream returns an empty
> iterator, and the monitor completes immediately — for those, the effective health bound is
> the invoke timeout itself, not the idle timeout.

---

## `ExecutionMonitor`: rule-based per-node health

[`ExecutionMonitor`](../../src/concursus/execute/monitor.py) consumes the `LogEvent` stream
concurrently with the invoke and assesses each event against four rules. Its `watch()`
returns a `HealthSignal` when the stream ends (`COMPLETED`) or a rule fires (`TERMINATE`):

| Rule | Fires when | `failure_mode` tag |
|---|---|---|
| Idle timeout | No log event for `idle_timeout_s` (applied *between* events) | `idle_timeout` |
| Error threshold | `error_threshold` `ERROR`-severity/type events accumulate | `error_threshold` |
| Tool loop | The same tool signature repeats `loop_detection_window` times in a row | `tool_loop` |
| Token budget | Estimated output tokens (~4 chars/token) exceed `token_budget` (0 = unlimited) | `token_budget` |

Thresholds come from [`MonitorConfig`](../../src/concursus/execute/monitor.py), built per node
from the manifest's optional `monitor:` block via `MonitorConfig.from_manifest`. The defaults
give **timeout-only** monitoring: `idle_timeout_s=300`, `error_threshold=3`,
`loop_detection_window=5`, `token_budget=0`.

A terminating `HealthSignal` carries a **structured** `failure_mode` + `evidence` (not just
prose in `reason`), which is what makes the next retry *corrective* — the loop detector, for
instance, records *which* tool the agent looped on. The harness turns a terminating signal
into a `PreemptiveTermination`:

```python
health = await self.monitor.watch(log_stream)
if health.should_terminate:
    raise PreemptiveTermination(health.reason, health)
```

The monitor never alters a plan, injects into agent execution, or touches storage — the
Supervisor retains termination authority via that exception path.

### Preemptive termination → corrective retry

When the harness executor catches a `PreemptiveTermination`, [`remediation_for`](../../src/concursus/execute/monitor.py)
turns the signal's `failure_mode` + `evidence` into a plain-text amendment — e.g. *"you
called the tool `search` 5 times in a row with identical arguments and made no progress; do
not repeat an identical tool call."* The executor appends that as `remediation_context` on
the **retry envelope**:

```python
attempt_envelope = {**envelope, "remediation_context": amendment}
```

This is an **overlay, never a mutation**: the harness's `_serialize_prompt` appends the
correction *after* the frozen, compiler-vended task, and it is absent on a first attempt — so
a normal run's prompt is byte-for-byte unchanged. The retry ladder is bounded: a failure mode
that recurs *after* its own correction was applied escalates to a terminal
`preemptive_termination` record rather than being prescribed the same advice twice, and a
**side-effecting** node (`side_effecting: true`) always gets exactly one attempt — being
wrong toward wasting a node beats re-performing an external effect. Each retry builds a
**fresh** harness, because the monitor accumulates error counts and tool signatures that
would otherwise re-trip on the first event of the retry.

`remediation_for` is deliberately the *rule tier's* amendment: it states an observed fact
plus a generic corrective and never attributes a cause. Diagnosing *why* is reserved for a
future semantic assessor. It returns `None` when there is nothing useful to say, so the
caller falls back to a plain retry.

---

## The `ObjectStore` artifact path

The harness materializes inputs and writes outputs through the `ObjectStore` protocol
(`get_object` / `put_object`), so artifacts never travel inline through the invoke payload.
Two implementations ship, both in [`object_store.py`](../../src/concursus/execute/object_store.py):

```python
from concursus.execute import FileStore, S3Store

store = FileStore(root="/tmp/run-artifacts")   # local/test: file:// (and s3:// mapped under root)
store = S3Store(region="us-west-2")            # production: s3://; boto3 lazy, created on first use
```

On the read side, an input declared `type: artifact` whose value is an `ArtifactRef`
(`{uri, content_hash, content_type}`) is fetched, **hash-verified** (`sha256:`), and
deserialized by `content_type` (`application/json` → data, `text/*` → text). A scalar passes
through inline. On the write side, `_write_outputs` iterates the node's **declared** outputs,
applies `output_mapping` (`response_key → contract_field`), serializes each artifact field by
its `content_type` (JSON, real CSV, or text), writes it under the compiler-vended
`output_prefix`, and returns an `ArtifactRef` with a content hash and byte count. Anything not
in the declared output schema is dropped.

> **Gotcha:** CSV handling is deliberately **asymmetric**. The write side accepts structured
> rows (list-of-dicts or list-of-rows) and emits real CSV with a header; the read side returns
> the CSV as **text**, following the convention that `application/json` yields data while
> `text/*` yields text. A consumer that wants rows can `csv.DictReader` the text itself.

The prompt the agent sees also **renders the declared output contract** ("return `summary` as
Markdown"), so the agent is told the shape the harness is about to serialize and hash. `keys`
/ `sections` hints are advisory and never enforced.

---

## Wiring the harness into the Supervisor

The harness stack reaches a run **only** through the `NodeExecutor` seam. The one-call path
for a loop-driven run is [`make_harness_supervisor_factory`](../../src/concursus/execute/harness_factory.py),
which returns a `GovernorLoop`-compatible `supervisor_factory` with the seam pre-wired:

```python
from concursus.execute import HarnessFactory, S3Store, DefaultMonitorFactory
from concursus.execute import make_harness_supervisor_factory
from concursus.governor import GovernorLoop

factory = HarnessFactory(
    manifests=raw_manifests,                 # {node_id: raw manifest dict}
    store=S3Store(region="us-west-2"),
    monitor_factory=DefaultMonitorFactory(), # per-node ExecutionMonitors from the `monitor:` block
    output_prefix_root="s3://my-bucket/runs",
)

loop = GovernorLoop(
    goal, agent_manifests,
    supervisor_factory=make_harness_supervisor_factory(factory),
)
```

The wrapper preserves the default factory's exact contract (including the Trust-Ladder
`held`-set semantics) and additionally injects `node_executors` (the shipped `NODE_EXECUTORS`
plus the harness executor) and `node_kind_fn` (routes any node whose raw manifest declares a
`runtime:` block to the harness; every other node keeps the legacy `_dispatch` path).

For a single `Supervisor` you can wire the same two hooks directly:

```python
from concursus import Supervisor
from concursus.execute.supervisor import NODE_EXECUTORS
from concursus.execute.harness_factory import HARNESS_NODE_KIND

executors = {**NODE_EXECUTORS, HARNESS_NODE_KIND: factory.make_executor()}
sup = Supervisor(
    plan, manifests, invoke_fn=my_invoke,   # legacy transport for non-runtime nodes
    node_executors=executors,
    node_kind_fn=factory.make_kind_fn(),
)
```

The harness executor assembles a **structured envelope** — it resolves wired inputs from
completed upstream producers (passing an `ArtifactRef` through untouched for the harness to
deref), pulls the node's `task` / `io` declaration off the plan's frozen `payload_contract`,
projects the tiered `static_context`, and runs the harness. After the harness's own
`_check_contract` (required-field presence), the executor runs the **same** Supervisor output
gates the default path runs — `validate_output`, and under `check_acceptance=True`,
`check_hive_contract` + `check_acceptance` — then `store.put`s the refs. Same store, same
`on_error` path, same failure classes.

> **Gotcha:** for an `agentcore`-backed node the executor also runs the Supervisor's
> `_check_arn_integrity` **before** invoke, so an unprovisioned or stale plan binding surfaces
> as a legible failure at dispatch rather than an opaque AWS error mid-run. A `callable` /
> `http` node legitimately has no runtime ARN and is not gated this way.

---

## Futility cancellation

In an antichain-parallel wave (`Supervisor.run(parallel=N)`), a node can fail while its
siblings are still in flight. If every consumer of an in-flight sibling lives inside the
failed node's doomed downstream region, that sibling's output can no longer be consumed —
spending more on it is pure waste. The opt-in **`cancel_futile`** seam condemns exactly those
nodes:

```python
sup = Supervisor(plan, manifests, invoke_fn=my_invoke, cancel_futile=True)
outputs = sup.run(inputs, parallel=4)
```

The graph math lives in [`futility.py`](../../src/concursus/execute/futility.py) and runs over
the **frozen** `plan.wiring`: `invert_wiring` builds `producer → {consumers}` once,
`descendants` computes the doomed region of a failed node, and `futility_closure` returns the
in-flight nodes whose consumers are **non-empty and wholly inside** that region. A **sink**
node (nothing consumes it) is never futile — its output is a deliverable — and a node that
also feeds a consumer *outside* the doomed region survives, so cancellation is discriminating,
not a blunt cancel-the-wave.

Reaching inside a running harness is the one irreducible bit of machinery.
`asyncio.run` builds its event loop internally and returns no handle, so a running task can
only be cancelled if it **self-registers** its `(loop, task)` in a `CancelTokenRegistry` from
inside the coroutine (`run_registered`); the Supervisor then condemns from its own thread via
`loop.call_soon_threadsafe(task.cancel)`. A condemnation issued before its token registers is
remembered and fires at registration; one arriving after the node finished is a silent no-op.
The condemned worker catches the `CancelledError` and writes its own `futility_cancelled`
failed record with the retained reason — it is never retried, because the work is provably
unconsumable.

With `cancel_futile=False` (the default, and *every* serial run) no registry is built and no
closure is ever computed — `_run_parallel` behaves exactly as before. Futility cancellation
**prunes only**: it never reroutes and never mutates the frozen plan.

---

## The four-class failure taxonomy

Once nodes actually run agents, "a node failed" is no longer one thing. `Supervisor.summary()`
buckets every terminal failed record into a `failure_classes` count, and each class names a
distinct cause:

| Class | Meaning |
|---|---|
| `crash` | This node's own invoke / validate / ARN-integrity raised |
| `hold` | Never invoked because a producer it consumes failed or was blocked |
| `preemptive_termination` | The node's own `ExecutionMonitor` judged the run unhealthy and terminated it |
| `futility_cancelled` | The Supervisor cancelled it mid-flight because its output became unconsumable |

The class is read from the failed record's own `failure_class` field, falling back to
`blocked_on` presence for a legacy record that predates the field — so the count is stable
across store backends and older logs. Widening the taxonomy from two classes to four is what
stops a monitor-initiated termination (`preemptive_termination`) from being silently counted
as a self-inflicted `crash`.

An adjacent opt-in, `capture_agent_binding=True`, records the bound agent name + ARN in each
validated `put`'s metadata, and *separately* captures the real per-node invoke payload —
surfaced via `Supervisor.recorded_payloads()` for `capture_run(payloads=…)` and redacted at
capture time — so a post-run capture persists the actual bytes asked of the agent rather than
only the compiler-authored static context. Off by default — the `put` metadata is
byte-identical to before.

---

## Recap: the invariant, restated

- **Opt-in through one seam.** The stack reaches a run only via the `NodeExecutor` seam. Wire
  no harness factory and the default `invoke_fn` path is byte-for-byte unchanged.
- **The harness runs inside the governed dispatch.** It reads the frozen plan, invokes an
  agent, and writes the same append-only log — it never mutates `plan.order` or re-plans.
- **The monitor observes; the Supervisor governs.** A rule-based verdict can ask for early
  termination via `PreemptiveTermination`; it cannot alter a plan or an agent.
- **Corrective retry is an overlay.** `remediation_context` is appended after the frozen task,
  never merged into it, and is absent on a first attempt.
- **Futility prunes, never reroutes.** It cancels provably-unconsumable work over the frozen
  wiring; the plan is untouched.

*Concursus is a compiler, not a runtime governor* — the runtime stack lives inside the
governed dispatch, not above it.

---

## See also

- [API Reference: execute](../reference/execute.md) — every symbol in the execute tier.
- [Guide: Compiling & Running a Team](compiling-and-running.md) — the compile pipeline and the
  `NodeExecutor` seam this stack plugs into.
- [Guide: Authoring Agents](authoring-agents.md) — the manifests, and where `runtime:` /
  `monitor:` / `output_mapping` sit.
- [Guide: Durable Run State](durable-state.md) — the `StateStore` the harness writes through.
- [Guide: The Governor](governor.md) — the strictly-outer loop that constructs one Supervisor
  per episode via `supervisor_factory`.
- [Guide: Deploying to AWS Bedrock AgentCore](deploying-to-agentcore.md) — provisioning the
  runtimes an `agentcore`-backed node addresses.
- [Core Concepts](../concepts.md) and the [documentation index](../README.md).
