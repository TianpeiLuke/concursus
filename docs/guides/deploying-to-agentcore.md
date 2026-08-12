# Guide: Deploying to AWS Bedrock AgentCore

*From a frozen plan to live runtimes: offline build artifacts, the create-time trust gate, the append-only deploy ledger, and the one AWS + Docker actuator.*

Deploying is the step that turns a compiled, frozen `ProvisioningPlan` into per-agent
AgentCore runtimes on AWS. It is deliberately split into two halves so that everything
generative and previewable happens *before* any AWS call:

- an **offline builder** ([`build/build.py`](../../src/concursus/build/build.py)) that
  synthesizes packaging artifacts and `CreateAgentRuntime` parameter dicts — pure stdlib, no
  `boto3`, no Docker; and
- a **deploy-time actuator** ([`build/provision.py`](../../src/concursus/build/provision.py))
  that consumes those artifacts and actually talks to AWS + Docker.

Two guards sit between build and provision: the **create-time trust gate**
([`build/trust.py`](../../src/concursus/build/trust.py)) and the append-only **deploy
ledger** ([`build/ledger.py`](../../src/concursus/build/ledger.py)). Both fire at deploy
time, exactly once per node — never per invocation.

> **The load-bearing invariant.** Concursus is a compiler, not a runtime governor. A run is
> `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run` — a single forward pass over an
> immutable plan. Deploy is a strictly *before-run* step: it stands up the runtimes the frozen plan
> names, and the trust gate is a compile-time decision made once. It never re-earns trust from a run
> outcome and never chooses among competing agents — that live, per-decision grading is the
> [governor's](governor.md) job.

Everything on this path lives behind the optional `[agentcore]` extra (`boto3` +
`bedrock-agentcore`), and `boto3` is imported **lazily** — the pure core, the builder, the trust
gate, the ledger, and every dry-run all work with neither extra installed.

```bash
pip install "concursus[agentcore]"
```

---

## 1. Offline artifact synthesis (the builder)

Given an `AgentManifest`, the builder compiles a single `BuildPlanEntry` per agent node. This is
pure and offline: it renders artifacts and parameter dicts only, and never imports `boto3` or calls
AWS.

### `RuntimeBuilderFactory.synthesize`

```python
from concursus.build.build import RuntimeBuilderFactory

entry = RuntimeBuilderFactory.synthesize(
    manifest,                 # an AgentManifest
    account="123456789012",   # optional; falls back to the literal 'ACCOUNT_ID' placeholder
    region="us-east-1",       # optional; falls back to the literal 'REGION' placeholder
)
```

`synthesize` is a `@staticmethod` and its `account` / `region` are keyword-only. It dispatches the
manifest to the right per-protocol template or to the prebuilt registrar and returns a
`BuildPlanEntry`:

| Field | Meaning |
|---|---|
| `name` | Agent / node id. |
| `build_mode` | `"container"`, `"codezip"`, or `"prebuilt"`. |
| `wrapper` | The generated `app.py` serving-wrapper source (`None` for prebuilt / ARN-reuse). |
| `dockerfile` | The container `Dockerfile` (`None` unless `build_mode == "container"`). |
| `execution_role` | `{"policy": ..., "trust": ...}`, or `None` when `registry.role_arn` is supplied. |
| `create_agent_runtime` | The `create_agent_runtime` param dict (or an ARN-reuse marker `{"agentRuntimeArn": ...}`). |
| `invoke` | `{"protocol": ..., "qualifier": ..., "port": ...}` — what the [Supervisor](../reference/execute.md) needs to dispatch. |
| `ecr_repo` | Target ECR repo when configured. |
| `fingerprint` | The hosting-identity content fingerprint (see [§4](#4-the-deploy-ledger)). |

`BuildPlanEntry.to_dict()` returns it as a plain dict (via `dataclasses.asdict`).

### One template per serving protocol

The serving contract fixes exactly one port per protocol, held in the module-level `PORTS` dict:

| Protocol | Port | Serves |
|---|---|---|
| `HTTP` | `8080` | `POST /invocations` + `GET /ping` |
| `MCP` | `8000` | `/mcp` (streamable-http) |
| `A2A` | `9000` | JSON-RPC 2.0 at the root |

`synthesize` picks `HttpAgentTemplate`, `McpAgentTemplate`, or `A2AAgentTemplate` by
`manifest.protocol` and raises `BuildError` (a `ValueError` subclass) if the protocol is not one of
`HTTP` / `MCP` / `A2A`. An existing `registry.agent_runtime_arn` — or a `registry.container_uri`
with `build_mode == "prebuilt"` — routes to `PreBuiltRegistrar` instead, which emits an entry with
`wrapper=None`, `dockerfile=None`, `execution_role=None` and (for an ARN) the reuse marker.

> `registry.entry` must be `"module:function"` for the template path to render a wrapper —
> a missing or malformed value raises `BuildError`. Prebuilt / ARN-reuse skip wrapper rendering,
> so they don't need it.

> **Opt-in, default-off: the per-runtime-kind builder seam.** `synthesize` routes through a small
> Strategy/Registry seam (`RUNTIME_BUILDERS`, keyed by `registry.runtime_kind`). With no
> `runtime_kind` declared and no custom `runtime_builders` supplied — the default — this is *exactly*
> the single built-in compile path (`_default_runtime_builder`), byte-for-byte. A manifest may opt in
> to a registered custom kind; an absent or unregistered kind falls back to the default builder.

### The IAM execution role

`render_execution_role` synthesizes the role document the plan carries:

```python
from concursus.build.build import render_execution_role

role = render_execution_role(manifest, "123456789012", "us-east-1", container=True)
# -> {"policy": <IAM policy document>, "trust": <assume-role policy document>}
```

The policy always grants CloudWatch Logs, X-Ray, CloudWatch `PutMetricData` (namespaced to
`bedrock-agentcore`), and Bedrock `InvokeModel` (+ `InvokeModelWithResponseStream`). With
`container=True` it adds ECR pull (`BatchGetImage` / `GetDownloadUrlForLayer`), ECR
`GetAuthorizationToken`, and the `bedrock-agentcore` workload-access-token statements. Unknown
`account` / `region` fall back to the literal placeholders `ACCOUNT_ID` / `REGION` so the plan stays
previewable offline. The trust policy lets `bedrock-agentcore.amazonaws.com` assume the role,
conditioned on `aws:SourceAccount` and an `ArnLike` `aws:SourceArn`. When `registry.role_arn` is set,
the entry's `execution_role` is `None` (nothing to synthesize) and the ARN flows straight through.

Artifacts carry two placeholders that **deploy** fills in later — `<image-uri>` and
`<execution-role-arn>` — so a plan is fully renderable and reviewable before any AWS credential is
touched.

---

## 2. The build plan is per-plan, in topological order

The [`plan` CLI verb](cli.md) assembles a whole team's manifests + DAG into a frozen
`ProvisioningPlan` whose `.order` is a topological node sequence and whose `.entries[node]` are the
`BuildPlanEntry` objects above. See [reference/assemble.md](../reference/assemble.md) for how the
plan is compiled and [reference/build.md](../reference/build.md) for the full builder API. The
actuator below walks exactly this order.

---

## 3. The trust gate (create-time, once per node)

Before it stands up a *side-effecting* agent, the actuator consults a pure gate that decides
**live | shadow | hold** for that node — exactly once per deploy. This is the compile-time face of
graduated trust; the runtime, per-decision face lives in the
[governor's `TrustLadderScheduler`](governor.md).

### `TrustGrade` — the author-declared seed

`TrustGrade` is an ordered `IntEnum` the manifest author declares (`manifest.trust_seed`):

| Grade | Value | Meaning |
|---|---|---|
| `L0_SHADOW` | 0 | Cleared to run, but only in shadow. |
| `L1_CANARY` | 1 | Canary autonomy. |
| `L2_GUARDED` | 2 | Guarded autonomy. |
| `L3_AUTONOMOUS` | 3 | Full live autonomy. |

Because it's an `IntEnum`, grades compare with `<` / `<=` directly. `TrustGrade.parse` coerces a
grade, an int `0-3`, or a name (`"L2_GUARDED"`, `"L2"`, `"GUARDED"`, case-insensitive) into a
`TrustGrade` (and rejects `bool` and out-of-range ints with `ValueError`). `clamp_trust_grade`
clamps a *requested* grade down to a compiled ceiling — the effective grade is `min(compiled,
requested)`, a dial that only ever loosens toward caution.

### `evaluate_deploy_gate → GateDecision`

```python
from concursus.build.trust import evaluate_deploy_gate, TrustGrade

decision = evaluate_deploy_gate(
    side_effecting=True,
    trust_seed=TrustGrade.L0_SHADOW,
    min_autonomy=TrustGrade.L0_SHADOW,   # optional caller floor
    require_approval=False,              # optional
)
# -> GateDecision(mode="shadow", qualifier="SHADOW", reason="...cleared but not live...")
```

All arguments are keyword-only. The function is pure — no AWS, no state — and returns a frozen
`GateDecision(mode, qualifier, reason)`. The decision rules, evaluated top to bottom:

| Condition | Decision | Qualifier |
|---|---|---|
| Not side-effecting | `LIVE` | `DEFAULT` |
| No caller policy (`min_autonomy is None` **and** not `require_approval`) | `LIVE` (deploy byte-for-byte unchanged) | `DEFAULT` |
| `require_approval` set | `HOLD` (escalate — nothing deployed) | `None` |
| `trust_seed < min_autonomy` | `HOLD` (below floor) | `None` |
| Clears floor but only `L0_SHADOW` | `SHADOW` ("cleared, but not live") | `SHADOW` |
| Otherwise | `LIVE` | `DEFAULT` |

The mode constants are `LIVE = "live"`, `SHADOW = "shadow"`, `HOLD = "hold"`; the qualifiers are
`DEFAULT_QUALIFIER = "DEFAULT"` and `SHADOW_QUALIFIER = "SHADOW"`.

Discipline: this gate fires **once per node per deploy**. It is never a per-invocation check, never
re-earns trust from a run outcome, and never chooses among competing agents. With no caller policy
it is a no-op, so today's deploy stays byte-for-byte unchanged.

---

## 4. The deploy ledger

The deploy ledger answers one create-time question across separate CLI invocations: *"Have I
already stood up this exact content?"* If yes, deploy skips both the image build and
`CreateAgentRuntime` and reports `action="reused"`.

### Content identity: the hosting fingerprint

The key is `(name, fingerprint)`, where `fingerprint` is a SHA-256 of the agent's **hosting
identity** only — image/source, serving protocol, entrypoint, network configuration, execution-role
identity, sorted declared input keys, and output schema:

```python
from concursus.build.build import fingerprint

fp = fingerprint(manifest, account="123456789012", region="us-east-1")  # hex sha-256
```

It deliberately does **not** fold in agent-behavior inputs (model / prompt / SOPs): it is deploy
dedup metadata, not a trust re-earning check, and it must **never** select among versions at
dispatch time. Identical hosting inputs produce an identical fingerprint; any hosting change (a new
`container_uri`, protocol, output schema, etc.) changes it. The reuse key itself is single-sourced
through `deploy_identity(name, fingerprint)` so the confirmation lookup and any reconcile query can
never drift apart.

### `DeployLedger` / `DeployRow`

```python
from concursus.build.ledger import DeployLedger

led = DeployLedger(".concursus/deploy_ledger.json")

if led.has("planner", fp):          # already stood up this exact content?
    ...                             # deploy will report action="reused"

row = led.record(                   # append one outcome (keyword-only; ledger never reads the clock)
    name="planner",
    fingerprint=fp,
    deployed_at="2026-07-15T00:00:00+00:00",
    arn="arn:aws:...",
    action="created",
)

for r in led.rows():                # oldest-first audit history
    print(r.name, r.fingerprint, r.action)
```

The ledger is JSON-backed, **append-only**, and persistence-only:

- `lookup(name, fingerprint)` returns the newest matching `DeployRow` or `None` — the only
  create-time query it answers.
- `has(name, fingerprint)` is the boolean form.
- `record(...)` appends one outcome and flushes atomically (temp file + `os.replace`). It does
  **not** dedup — calling it twice for the same key appends two audit rows, and `lookup` returns
  the newest. `deployed_at` is a required keyword arg; the ledger never calls the clock itself.
- `rows()` returns the full history, oldest first.

The file on disk is the source of truth: every read re-loads it, so two instances/processes see each
other's writes. It is rebuildable / disposable — deleting it loses no canonical state (an
unreadable, corrupt, or missing file is treated as an empty ledger), and it is never consulted at
dispatch time.

> **Opt-in ledger capabilities (default-off).** Two additive, append-only projections extend the
> ledger without touching its default byte-for-byte format on disk:
>
> - **Typed rejections + desired-vs-confirmed reconcile.** `record_rejection(node, code, ...)`
>   logs a structured `{node, code, reason, confirmed_at}` entry (`code` ∈ `unsupported | invalid
>   | timeout | actuator_error`) for a node that was *not* stood up. `reconcile({node:
>   fingerprint})` then reports which desired nodes are `confirmed`, which `diverged`, and *why*
>   (the newest rejection for the node). Read-only projection — never a second authoritative copy
>   of run state.
> - **Content-reuse policy.** `lookup` / `has` accept an opt-in `context_mode`; the explicit
>   literal `"isolation"` refuses reuse (forcing a re-provision), while the empty default and
>   `"reuse"` permit it — so an existing caller that passes no policy is unchanged.

---

## 5. The actuator: `provision_plan` / `provision_agent`

[`provision.py`](../../src/concursus/build/provision.py) is the **only** module that talks
to AWS and Docker. Every AWS client and the shell runner is injectable, so the whole orchestration
is unit-testable with fakes — no AWS account, no Docker daemon.

### Clients

```python
from concursus.build.provision import Clients

clients = Clients.default(region="us-east-1")   # binds real boto3 clients (lazy import)
```

`Clients` bundles the three clients provisioning needs — `iam` (global), `ecr`, and
`control` (`bedrock-agentcore-control`, regional). `Clients.default(region=None)` imports `boto3`
lazily and raises `ProvisionError` if it isn't installed (install the `[agentcore]` extra). Inject
fakes for offline tests.

### Provision a whole plan

```python
from concursus.build.provision import provision_plan
from concursus.build.ledger import DeployLedger
from concursus.build.trust import TrustGrade

results = provision_plan(
    plan,
    region="us-east-1",
    source_dirs={"summarize": "./svc"},   # per-node build context; falls back to default_source_dir
    tag="v3",
    manifests=manifests,                  # enables the create-time trust gate
    min_autonomy=TrustGrade.L2_GUARDED,   # caller floor
    ledger=DeployLedger(".concursus/deploy_ledger.json"),
    now="2026-07-15T00:00:00+00:00",      # caller-supplied ledger timestamp
    halt_on_error=False,
)
# -> one result dict per node, in plan.order
```

`provision_plan` walks `plan.order` and calls `provision_agent` per node. Each call is guarded: on a
`ProvisionError` or a raw AWS/botocore error it records a `{"node", "action": "failed", "error"}`
result; any other exception is re-raised as a genuine bug. With `halt_on_error=True` (the default)
the walk stops after the first failing node, and with `False` it continues — either way the
accumulated results are always returned, so already-provisioned ARNs are preserved. `clients`
defaults to `Clients.default(region)`; passing no `known_fingerprints` / `ledger` / `manifests` /
`min_autonomy` keeps today's unconditional `created` behavior.

### Provision one agent

`provision_agent` provisions a single node and returns a result dict
(`{"node", "arn", "action", "role_arn", "image_uri", ...}`). Its order of operations:

1. **Reuse an existing `agentRuntimeArn` outright** → `action="reused"` (nothing created).
2. **Ledger reuse-by-content** (if a `ledger` is passed and the fingerprint matches a prior row)
   → `action="reused"`, skipping both build and create. *Checked before in-memory reuse.*
3. **In-memory reuse-by-content** (a matching `known_fingerprints[name]`) → `action="reused"`; a
   differing fingerprint marks `action="updated"`.
4. **The create-time trust gate** (if a `manifest` is passed): `HOLD` →
   `action="escalated"` with a `reason` and **no** create and **no** ARN; `SHADOW` → the result
   carries a non-`DEFAULT` `qualifier` and `reason`, but the `CreateAgentRuntime` request itself
   stays clean (the shadow endpoint is a separate downstream concern).
5. **Ensure the IAM execution role** (idempotent — `EntityAlreadyExists` refreshes the trust policy
   instead of failing). If the plan still carries the `<execution-role-arn>` placeholder and no role
   was synthesized, it raises `ProvisionError`.
6. **Build + push the container image** when the plan still carries `<image-uri>`: ensure the ECR
   repo (idempotent), assemble a temp build context (a copy of `source_dir` with the generated
   `app.py` + `Dockerfile` dropped in — the user's project is never mutated), `docker login`, build
   for **`linux/arm64`** explicitly, push, and substitute the real URI.
7. **`CreateAgentRuntime`**, then **poll `GetAgentRuntime` to `READY`** (`CreateAgentRuntime` is
   asynchronous; `CREATE_FAILED` / `UPDATE_FAILED` / a readiness timeout raise `ProvisionError`). A
   fresh runtime is `action="created"`.
8. **Append the outcome to the ledger** (when one is passed).

```python
from concursus.build.provision import provision_agent

res = provision_agent(
    entry,
    clients=clients,
    manifest=manifest,
    min_autonomy=TrustGrade.L2_GUARDED,
    require_approval=True,
)
# -> {"node": ..., "action": "escalated", "reason": "...held...", "arn": None}
```

The `action` values, at a glance:

| `action` | Meaning |
|---|---|
| `reused` | ARN-reuse marker, ledger hit, or a matching in-memory fingerprint — no create. |
| `escalated` | Trust gate returned `HOLD` — nothing created, no ARN; carries `reason`. |
| `updated` | In-memory fingerprint changed — the runtime was updated. |
| `created` | A fresh runtime, polled to `READY`. |
| `failed` | Provisioning raised — carries `error` (only via `provision_plan`'s guard). |

> Because `CreateAgentRuntime` is asynchronous, a node is only recorded as usable after it polls to
> a terminal `READY` — so a later `CREATE_FAILED` is never dedup-cached as created. Callers must not
> treat an `escalated` (held) node as deployed: it has no ARN.

Idempotency and safety are structural: IAM role and ECR repo creation branch on
`EntityAlreadyExists` / `RepositoryAlreadyExists`; images are always built for `linux/arm64`
(AgentCore Runtime only launches arm64); the temp build context is always cleaned up; and
`deployed_at` timestamps are caller-injected or a call-time value, never read at import.

### Opt-in: two-phase crash-safe actuation

Passing `two_phase=True` **with** a `ledger` makes the create crash-safe. Before `CreateAgentRuntime`
the actuator appends a `status="reserving"` reservation (durable intent, keyed by `(name,
fingerprint)` and carrying the deterministic runtime name); after the create + readiness wait
succeed it supersedes that with a `status="confirmed"` entry carrying the real ARN. A crash between
those phases leaves a dangling `reserving` entry. On the *next* deploy, `provision_plan` first calls
`reconcile_reservations(ledger, ...)`: for each dangling reservation it either **adopts** a runtime
already created under the deterministic name (append `confirmed`, reuse it) or **compensates** it
(append `compensated`, so the node is re-provisioned cleanly). With `two_phase=False` (the default)
or no ledger this is a no-op and the deploy is byte-for-byte unchanged.

---

## 6. From the command line

Both side-effecting paths are opt-in behind an explicit `--execute` flag; without it, `deploy` and
`run` are dry-runs that never import `boto3` or Docker. See the full [CLI guide](cli.md) and
[`cli.py`](../../src/concursus/cli.py).

### `deploy`

```bash
# Dry-run (default): print what deploy WOULD do, per agent, in topological order — no AWS.
concursus deploy *.agent.yaml --account 123456789012 --region us-east-1

# For real: ensure roles, build + push images, CreateAgentRuntime.
concursus deploy *.agent.yaml \
    --execute \
    --source-dir . --source-dir summarize=./svc \
    --tag v2 \
    --min-autonomy L2_GUARDED \
    --require-approval
```

`deploy --execute` runs `provision_plan` with `halt_on_error=False`: a failing node is reported
`FAILED` and the rest are still attempted. `escalated` (trust-gate held) and `failed` nodes both set
a non-zero exit code and print to stderr. `--min-autonomy` takes a `TrustGrade` name or `0-3`
(omit it to disable the gate); `--require-approval` holds every side-effecting agent regardless of
`trust_seed`.

### `run`

```bash
# Dry-run: explain the topological dispatch, no invocation.
concursus run *.agent.yaml --inputs '@inputs.json'

# Execute live, gated by a between-phases plan-approval prompt, durable via AgentCore Memory.
concursus run *.agent.yaml \
    --inputs '@inputs.json' \
    --execute \
    --memory-id my-memory-store --actor-id run \
    --approve --yes
```

Deploy stands the runtimes up; `run --execute` invokes them through the
[Supervisor](../reference/execute.md) — a single forward pass over the frozen plan. Durable run
state is opt-in behind `--memory-id` (an AgentCore Memory `StateStore`) or `--vault` (on-disk
markdown notes + a derived SQLite run DB; `--lean-form` emits a smaller machine form); `--vault`
takes precedence when both are given. The optional `--approve` / `--plan-approval` gate previews the
frozen plan and pauses before any billed `InvokeAgentRuntime` (`--yes` auto-approves; a non-TTY
without `--yes` aborts with exit 0). See the [durable-state guide](durable-state.md) for the
backends and replay-resume semantics.

---

## 7. Durable state placement on AWS

Where durable run state *lives* when hosted on AgentCore is a placement decision documented
separately in **[AI-19 — AgentCore-aligned durable placement](../agentcore_placement.md)**. In
brief: AgentCore **Memory** is the canonical append-only log (source of truth; `replay()` rebuilds
the projection after micro-VM teardown), and an on-disk **EFS vault** (round-trip-exact notes + a
disposable SQLite `rundb`) is a *derived, rebuildable* read-model reached only through the
`StateStore` seam. That note also carries the AgentCore hosting **alignment checklist** — session
scoping to `runtimeSessionId`, and the `networkMode: VPC` + EFS mount IAM + **NFS over TCP 2049**
requirements without which the mount silently fails.

---

## See also

- [Guide: Command-Line Interface](cli.md) — the full `deploy` / `run` reference.
- [API Reference: build](../reference/build.md) — builders, actuator, trust gate, deploy ledger.
- [API Reference: execute](../reference/execute.md) — the Supervisor's forward pass.
- [API Reference: assemble](../reference/assemble.md) — how the frozen plan is compiled.
- [Guide: Durable Run State](durable-state.md) — the `StateStore` seam and its backends.
- [Guide: The Governor](governor.md) — the runtime, per-decision face of the trust ladder.
- [AI-19 — AgentCore durable placement](../agentcore_placement.md) — Memory + EFS vault, VPC/2049 checklist.
- [Core Concepts](../concepts.md) · [Getting Started](../getting-started.md) · [Documentation index](../README.md)
