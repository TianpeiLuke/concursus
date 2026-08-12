# API Reference: `build`

*Runtime builders, the provisioning actuator, the create-time Trust Ladder, and the deploy ledger — the compile-then-deploy tier that stands up per-agent AgentCore runtimes.*

The `build` tier is the compile-then-deploy pipeline of `concursus`. It splits cleanly into a **pure, offline builder** (synthesize artifacts + parameter dicts, never touch AWS) and a **single deploy-time actuator** (the one module that talks to AWS and Docker), with a pure gate and a persisted ledger threaded through both. Four modules cohere:

| Module | Source | Owns |
|---|---|---|
| `build.build` | [`../../src/concursus/build/build.py`](../../src/concursus/build/build.py) | The offline builder: compile an `AgentManifest` into a `BuildPlanEntry` (serving wrapper `app.py`, `Dockerfile`, IAM execution role, `create_agent_runtime` params, and a hosting `fingerprint`). One template per serving protocol; prebuilt images / existing ARNs registered as-is. It also exposes an opt-in per-runtime-kind builder registry (`RUNTIME_BUILDERS`); the default kind is byte-for-byte today's compile. |
| `build.provision` | [`../../src/concursus/build/provision.py`](../../src/concursus/build/provision.py) | The deploy actuator: consume a `ProvisioningPlan`, and per node in topo order ensure the IAM role, build + push the ECR image, call `CreateAgentRuntime`, and poll to `READY`. Opt-in crash-safety wraps each create in a two-phase reserve→confirm with a start-of-deploy stale-reservation reconciler. The only module that talks to AWS + Docker. |
| `build.trust` | [`../../src/concursus/build/trust.py`](../../src/concursus/build/trust.py) | The create-time deploy gate: a pure `evaluate_deploy_gate` grading an author-declared `TrustGrade` seed against caller policy into a `GateDecision` of `live` / `shadow` / `hold`. |
| `build.ledger` | [`../../src/concursus/build/ledger.py`](../../src/concursus/build/ledger.py) | The deploy ledger: a JSON-backed, append-only record keyed by `(name, fingerprint)` that answers the create-time reuse-by-content question across CLI invocations. It also — all opt-in, default-off — records typed rejections and two-phase reservations, and answers a desired-vs-confirmed `reconcile`; the on-disk format is byte-for-byte unchanged until one is first used. |

> Reminder: **Concursus is a compiler, not a runtime governor.** A run is `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run`. The `build` tier lives entirely in the deploy leg of that pipeline: `build.build` renders the plan offline *before* anything is provisioned, and `build.provision` is a single forward pass over the frozen plan. The **create-time** trust gate here fires exactly once per node per deploy — it is not the runtime, per-decision Trust Ladder (that is [`governor.TrustLadderScheduler`](governor.md)).

To drive this tier end to end — from a frozen plan to live runtimes — see [Guide: Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md) and the [`concursus deploy`](../guides/cli.md) command. For the stage that produces the `ProvisioningPlan` these builders populate, see [`assemble`](assemble.md); for the manifests they consume, see [`core`](core.md) and [Authoring Agents](../guides/authoring-agents.md).

## What is re-exported at the root

These `build` symbols are re-exported from the package root:

```python
from concursus import (
    BuildPlanEntry, RuntimeBuilderFactory,          # build.build
    Clients, ProvisionError, provision_agent, provision_plan,  # build.provision
    TrustGrade, GateDecision, evaluate_deploy_gate,  # build.trust
    DeployLedger, DeployRow,                         # build.ledger
)
```

The rest are public but *not* re-exported — import them from their module:

```python
from concursus.build.build import (
    fingerprint, render_execution_role, BuildError, PreBuiltRegistrar, PORTS,
    RUNTIME_BUILDERS, RuntimeBuilder,          # per-runtime-kind registry (opt-in)
)
from concursus.build.provision import (
    role_name, repo_name, ensure_execution_role, ensure_ecr_repo, build_and_push_image,
    reconcile_reservations,                    # two-phase stale-reservation reconciler
)
from concursus.build.trust import LIVE, SHADOW, HOLD, DEFAULT_QUALIFIER, SHADOW_QUALIFIER
from concursus.build.ledger import (
    DeployRejection, DeployReservation, Reconciliation,  # typed rejections + two-phase + reconcile
    deploy_identity, content_reuse_allowed,
    REJECTION_CODES, RESERVATION_STATUSES,
)
```

> **The opt-in additions are default-off.** Everything flagged below as opt-in — the two-phase reserve/actuate/confirm actuation and its reconciler, the ledger's typed rejections and desired-vs-confirmed `reconcile`, and the `RUNTIME_BUILDERS` per-runtime-kind registry — is part of the flexibility & robustness layer completed in v0.6.0, and is disabled unless a caller explicitly turns it on (`two_phase=True`, a `record_rejection`/`reconcile` call, or a `registry.runtime_kind`). With none of them, the default compile and deploy are **byte-for-byte unchanged** — same `BuildPlanEntry`, same `CreateAgentRuntime` calls, same on-disk ledger format. None of these opt-in symbols are re-exported at the package root; import them from their module.

> **boto3 and Docker are lazy.** Nothing in this tier imports the AWS SDK or shells out to Docker at import time. `build.build`, `build.trust`, and `build.ledger` are pure stdlib and never touch AWS at all. `build.provision` imports only stdlib at the top: it binds `boto3` lazily inside [`Clients.default`](#clientsdefault) (raising [`ProvisionError`](#provisionerror) if the `[agentcore]` extra is not installed) and imports `subprocess` lazily inside the default shell runner. So the pure core and the full test suite run with neither installed.

---

## `build.build`

Source: [`../../src/concursus/build/build.py`](../../src/concursus/build/build.py)

Pure, offline runtime builder. Given an [`AgentManifest`](core.md#agentmanifest), it synthesizes a [`BuildPlanEntry`](#buildplanentry): the serving wrapper source, the container `Dockerfile`, the IAM execution-role document, the `create_agent_runtime` request dict, and a hosting-identity `fingerprint`. One template per serving protocol (HTTP / MCP / A2A); an already-built image or an existing runtime ARN is registered as-is. This layer renders artifacts and parameter dicts only — it **never imports boto3 or calls AWS**.

| Symbol | Kind | Summary |
|---|---|---|
| [`PORTS`](#ports) | constant | Serving-protocol → fixed serving port. |
| [`BuildError`](#builderror) | exception | An agent cannot be compiled into a build plan. |
| [`render_execution_role`](#render_execution_role) | function | Render the agent's AgentCore IAM execution role (`{"policy", "trust"}`). |
| [`fingerprint`](#fingerprint) | function | SHA-256 content fingerprint of an agent's hosting identity. |
| [`BuildPlanEntry`](#buildplanentry) | dataclass | The compiled build/deploy artifacts + params for one node. |
| [`RuntimeTemplate`](#runtimetemplate) | protocol | Structural contract for a per-protocol runtime template. |
| [`HttpAgentTemplate`](#the-protocol-templates) | class | HTTP template (`/invocations` + `/ping` on 8080). |
| [`McpAgentTemplate`](#the-protocol-templates) | class | MCP template (`/mcp`, streamable-http, on 8000). |
| [`A2AAgentTemplate`](#the-protocol-templates) | class | A2A template (JSON-RPC 2.0 at `/` on 9000). |
| [`PreBuiltRegistrar`](#prebuiltregistrar) | class | Register an already-built image / reuse an existing ARN. |
| [`RuntimeBuilder`](#runtime_builders--the-per-runtime-kind-registry) | type alias | The uniform `(m, *, account, region) -> BuildPlanEntry` builder signature. |
| [`RUNTIME_BUILDERS`](#runtime_builders--the-per-runtime-kind-registry) | constant | Per-runtime-kind builder registry (opt-in), seeded with the default kind. |
| [`RuntimeBuilderFactory`](#runtimebuilderfactory) | class | Dispatch a manifest to the right template/registrar (via the registry). |

### `PORTS`

```python
PORTS = {"HTTP": 8080, "MCP": 8000, "A2A": 9000}
```

The serving contract fixes exactly one port per protocol: HTTP serves `POST /invocations` + `GET /ping` on 8080, MCP serves `/mcp` on 8000, and A2A serves the JSON-RPC 2.0 root on 9000. Templates read `.port` from this; [`PreBuiltRegistrar`](#prebuiltregistrar) reads it as `PORTS.get(protocol)` (so an unknown protocol yields a `port` of `None` rather than raising).

### `BuildError`

```python
class BuildError(ValueError)
```

Raised when an agent cannot be compiled into a runtime build plan. Subclasses `ValueError`. Raised by the internal entry splitter on a bad or absent `registry.entry` (it must be `"module:function"` with a non-empty module and function), and by [`RuntimeBuilderFactory.synthesize`](#runtimebuilderfactory) on an unsupported protocol. Prebuilt / arn-reuse paths skip wrapper rendering, so they never need `registry.entry`.

### `render_execution_role`

```python
def render_execution_role(
    m: "AgentManifest",
    account: Optional[str],
    region: Optional[str],
    *,
    container: bool,
) -> dict
```

Render the agent's AgentCore execution role as `{"policy": ..., "trust": ...}` (each an IAM policy document, `Version` `2012-10-17`).

- **Always emitted:** CloudWatch Logs, X-Ray, CloudWatch `PutMetricData` (conditioned to the `bedrock-agentcore` namespace), and Bedrock `InvokeModel` / `InvokeModelWithResponseStream` statements.
- **`container=True` only:** adds ECR pull (`ecr:BatchGetImage` / `ecr:GetDownloadUrlForLayer`), `ecr:GetAuthorizationToken`, and the `bedrock-agentcore` workload-access-token statements a container runtime needs. A codezip/direct role omits these.
- **Parameters:** `account`, `region` — unknown values fall back to the literal placeholders `"ACCOUNT_ID"` / `"REGION"` so the plan stays previewable; `container` is keyword-only.
- **Returns:** `{"policy": <IAM policy doc>, "trust": <assume-role doc>}`. The trust policy allows `bedrock-agentcore.amazonaws.com` to `sts:AssumeRole`, conditioned on `aws:SourceAccount == account` and `aws:SourceArn` `ArnLike` the regional `bedrock-agentcore` ARN. The role name is sanitized from `m.name`.

```python
from concursus.build.build import render_execution_role

role = render_execution_role(manifest, "123456789012", "us-east-1", container=True)
# {"policy": {...}, "trust": {...}}
```

### `fingerprint`

```python
def fingerprint(
    m: "AgentManifest",
    *,
    account: Optional[str] = None,
    region: Optional[str] = None,
) -> str
```

SHA-256 content fingerprint of an agent's **hosting identity** (the DEPLOY-IDENTITY inputs), returned as a hex digest. It hashes only what changes *how the runtime is deployed*:

- the image/source (`registry.container_uri` or `registry.source_digest`),
- serving `protocol`, `entry`, and network mode,
- the execution-role identity — an explicit `registry.role_arn` as `{"role_arn": ...}`, else `{"policy_hash": <hash of the synthesized policy>}` (the `build_mode`, default `"container"`, decides the container flag for that synthesis),
- the sorted list of declared input keys, and
- the output schema.

- **Returns:** the hex SHA-256 digest. Identical hosting inputs produce an identical fingerprint; any hosting-input change (new `container_uri`, protocol, output schema, etc.) changes it.

> The fingerprint does **not** fold in agent-behavior inputs (model / prompt / SOPs). It is deploy-dedup metadata only — the shared key that threads [`provision_agent`](#provision_agent) reuse and the [`DeployLedger`](#deployledger) together. It must **never** be used to select among versions at dispatch time; that is the [governor](governor.md)'s job. The canonical hash uses `json.dumps(sort_keys=True, separators=(",", ":"))`, so equal inputs hash identically regardless of key order or whitespace.

```python
from concursus.build.build import fingerprint

fp = fingerprint(manifest)                              # hex sha-256 of the hosting identity
fp_scoped = fingerprint(manifest, account="123456789012", region="us-east-1")
```

### `BuildPlanEntry`

```python
@dataclass
class BuildPlanEntry:
    name: str
    build_mode: str
    wrapper: Optional[str]
    dockerfile: Optional[str]
    execution_role: Optional[dict]
    create_agent_runtime: dict
    invoke: dict
    ecr_repo: Optional[str]
    fingerprint: str = ""
```

The compiled build/deploy artifacts + parameters for one agent node. A mutable dataclass.

| Field | Meaning |
|---|---|
| `name` | The agent/node id. |
| `build_mode` | `"container"` \| `"codezip"` \| `"prebuilt"`. |
| `wrapper` | `app.py` source hosting the agent (`None` for prebuilt / arn-reuse). |
| `dockerfile` | Container `Dockerfile` (`None` for codezip / prebuilt). |
| `execution_role` | `{"policy": ..., "trust": ...}`, or `None` when `registry.role_arn` is given. |
| `create_agent_runtime` | The `create_agent_runtime` param dict, or an arn-reuse marker `{"agentRuntimeArn": ...}`. |
| `invoke` | `{"protocol": ..., "qualifier": ..., "port": ...}` for the supervisor. |
| `ecr_repo` | Target ECR repository for the image, when configured. |
| `fingerprint` | Hosting-identity fingerprint (see [`fingerprint`](#fingerprint)) — used at deploy for reuse-by-content (matching ⇒ `reused`, changed ⇒ `updated`), never for dispatch-time version selection. |

> The `wrapper` / `create_agent_runtime` artifacts carry placeholders (`<image-uri>` for the container URI, `<execution-role-arn>` for the role) that [`provision_agent`](#provision_agent) fills in at deploy time. These string literals are shared between `build.build` and `build.provision` and must stay in sync.

#### `BuildPlanEntry.to_dict`

```python
def to_dict(self) -> dict
```

Return the entry as a plain dict via `dataclasses.asdict` (deep-copies the nested dicts).

### `RuntimeTemplate`

```python
class RuntimeTemplate(Protocol):
    def render_wrapper(self, m: "AgentManifest") -> str: ...
    def render_packaging(self, m: "AgentManifest") -> str: ...
    def create_runtime_request(self, m: "AgentManifest", image_uri: Optional[str]) -> dict: ...
```

A structural (`typing.Protocol`) contract for a per-protocol AgentCore runtime template: render the serving wrapper source, render the `Dockerfile`, and assemble the `create_agent_runtime` request. The three concrete templates below satisfy it.

### The protocol templates

`HttpAgentTemplate`, `McpAgentTemplate`, and `A2AAgentTemplate` each fix one `protocol` and one serving harness. You rarely instantiate these directly — [`RuntimeBuilderFactory`](#runtimebuilderfactory) selects one for you — but the mapping is fixed:

| Template | `protocol` | Serving wrapper it emits |
|---|---|---|
| `HttpAgentTemplate` | `"HTTP"` | A `BedrockAgentCoreApp` with an `@app.entrypoint` `handler(payload, context)` that pulls the declared `contract.inputs` from the payload and calls the imported agent callable; serves `/invocations` + `/ping` on 8080. |
| `McpAgentTemplate` | `"MCP"` | A `FastMCP(name, host="0.0.0.0", port=8000)` with an `@mcp.tool()`-decorated function **named after the entry function** (not `handler`), taking `payload: dict`; served over streamable-http at `/mcp`. |
| `A2AAgentTemplate` | `"A2A"` | A `BedrockAgentCoreApp` with an `@app.entrypoint` `handler(payload, context)` served as JSON-RPC 2.0 at the root; `app.run(port=9000)`. |

All three render the `Dockerfile` (via the shared base — installs `requirements.txt`, `EXPOSE`s the protocol port, `CMD ["python", "app.py"]`) and the `create_agent_runtime` request identically; they differ only in the wrapper.

> Because the MCP template names its generated tool function after the entry function, a `registry.entry` whose function name is not a valid Python identifier would emit invalid wrapper source.

### `PreBuiltRegistrar`

```python
class PreBuiltRegistrar:
    def synthesize(
        self, m: "AgentManifest", *, account: Optional[str] = None, region: Optional[str] = None
    ) -> BuildPlanEntry
```

Register an already-built image or reuse an existing runtime ARN. Produces a [`BuildPlanEntry`](#buildplanentry) with `build_mode="prebuilt"`, `wrapper=None`, `dockerfile=None`, `execution_role=None`.

- If `registry.agent_runtime_arn` is set, `create_agent_runtime` is the arn-reuse marker `{"agentRuntimeArn": arn}` — nothing is created.
- Otherwise it builds a container create request from `registry.container_uri`.
- `invoke` is `{"protocol": m.protocol, "qualifier": registry.qualifier or "DEFAULT", "port": PORTS.get(protocol)}`; `ecr_repo` comes from the registry; `fingerprint` is computed via [`fingerprint`](#fingerprint).

### `RUNTIME_BUILDERS` — the per-runtime-kind registry

*Opt-in; default-off. The default kind is byte-for-byte today's compile.*

```python
RuntimeBuilder = Callable[..., BuildPlanEntry]  # (m, *, account=None, region=None) -> BuildPlanEntry

_DEFAULT_RUNTIME_KIND = "default"               # module-private; the one-and-only stock compile path

RUNTIME_BUILDERS: Dict[str, RuntimeBuilder] = {_DEFAULT_RUNTIME_KIND: _default_runtime_builder}
```

A **Strategy/Registry seam** generalizing today's single compile path into a lookup keyed by *runtime kind*. A runtime builder is a uniform `(m, *, account=None, region=None) -> BuildPlanEntry` callable; `RUNTIME_BUILDERS` maps a kind name to such a callable and ships seeded with exactly one entry — `"default"` → `_default_runtime_builder`, whose body is **today's exact `synthesize` logic** (prebuilt-registrar-or-protocol-template dispatch, unchanged).

- A manifest **opts in** to a custom builder via `registry.runtime_kind`. An **absent** `runtime_kind` (the default) resolves to `_DEFAULT_RUNTIME_KIND`, and an **unregistered** kind falls back to `_default_runtime_builder` — so a manifest that declares nothing, or declares an unknown kind, compiles byte-for-byte as it does today.
- The registry is a plain module-level dict seeded with the default kind. [`RuntimeBuilderFactory.synthesize`](#runtimebuilderfactory) **copies** it per call and **layers** any caller-supplied `runtime_builders` atop the copy, so no shared global state is mutated (a caller's kinds never leak into another call, and the shipped `RUNTIME_BUILDERS` is never modified in place).
- This is a pure, offline compile-time seam — it renders artifacts and param dicts only, exactly like the rest of `build.build`; it never touches AWS.

### `RuntimeBuilderFactory`

```python
class RuntimeBuilderFactory:
    @staticmethod
    def synthesize(
        m: "AgentManifest",
        *,
        account: Optional[str] = None,
        region: Optional[str] = None,
        runtime_builders: Optional[Dict[str, RuntimeBuilder]] = None,
    ) -> BuildPlanEntry
```

Dispatch a manifest to the right builder and compile it into a [`BuildPlanEntry`](#buildplanentry). A static method. It routes through the [`RUNTIME_BUILDERS`](#runtime_builders--the-per-runtime-kind-registry) registry (the Strategy/Registry seam).

- **Registry routing:** the kind is `registry.runtime_kind` (opt-in; absent ⇒ `_DEFAULT_RUNTIME_KIND`). The effective registry is `dict(RUNTIME_BUILDERS)` with any caller-supplied `runtime_builders` layered on top; the builder is `registry.get(kind, _default_runtime_builder)`. **With no `runtime_kind` declared and no `runtime_builders` supplied — the default — this is exactly `_default_runtime_builder`: byte-for-byte today's compile path.** A manifest declaring a *registered* custom kind routes to that builder; an unregistered kind falls back to the default.
- **Default-builder routing:** inside `_default_runtime_builder`, an existing `registry.agent_runtime_arn`, or a `registry.container_uri` with `build_mode == "prebuilt"`, routes to [`PreBuiltRegistrar`](#prebuiltregistrar). Otherwise `m.protocol` selects an HTTP/MCP/A2A template and `build_mode` (default `"container"`) decides whether a `Dockerfile` is emitted.
- **Template path details:** the wrapper is always rendered; the `dockerfile` is rendered only when `build_mode == "container"` (else `None`); the create request is built with `image_uri=None` (leaving the `<image-uri>` placeholder); `invoke` uses the template's `protocol`/`port` and `registry.qualifier` (default `"DEFAULT"`); `execution_role` is `None` when `registry.role_arn` is set, else synthesized via [`render_execution_role`](#render_execution_role) with `container=(build_mode == "container")`. The `fingerprint` is always computed.
- **Parameters:** `account`, `region`, and `runtime_builders` are keyword-only; `account`/`region` flow into role synthesis and the fingerprint.
- **Raises:** `BuildError` — when `m.protocol` is not `HTTP`, `MCP`, or `A2A` (raised inside the default builder).

```python
from concursus import RuntimeBuilderFactory

entry = RuntimeBuilderFactory.synthesize(manifest, account="123456789012", region="us-east-1")
entry.build_mode          # 'container'
entry.invoke["port"]      # 8080 for an HTTP agent
entry.fingerprint         # the hosting-identity hash

# Opt-in custom kind: a manifest declaring registry.runtime_kind == "mykind" routes here;
# an absent/unknown kind falls back to the default builder (today's compile, unchanged).
def _my_builder(m, *, account=None, region=None):
    return RuntimeBuilderFactory.synthesize(m, account=account, region=region)  # e.g. wrap the default

entry = RuntimeBuilderFactory.synthesize(manifest, runtime_builders={"mykind": _my_builder})
```

> `registry.container_uri` short-circuits to `PreBuiltRegistrar` **only** when `build_mode == "prebuilt"`; a `container_uri` under any other mode still goes through the template path.
>
> The layering is copy-then-update, so passing `runtime_builders` never mutates the shipped `RUNTIME_BUILDERS`, and one call's custom kinds never bleed into the next.

---

## `build.provision`

Source: [`../../src/concursus/build/provision.py`](../../src/concursus/build/provision.py)

The deploy-time **actuator** — turn a [`ProvisioningPlan`](assemble.md#provisioningplan) into live AgentCore runtimes. For each node in topological order it ensures the IAM execution role (idempotent), builds + pushes the container image to ECR when the plan still carries the `<image-uri>` placeholder, substitutes the real `roleArn` / `containerUri`, calls `CreateAgentRuntime`, and polls to `READY`.

This is the **only** module that talks to AWS and Docker. Every AWS client ([`Clients`](#clients)) and the shell runner (`run`) is injectable, so the whole orchestration is unit-testable offline with fakes — no AWS account, no Docker daemon. The module imports only stdlib at the top: `boto3` is bound lazily in [`Clients.default`](#clientsdefault), and `subprocess` is imported lazily in the default runner.

> **Transport.** The runtimes this actuator stands up are AgentCore's — the transport (A2A), tool discovery (Gateway), microVM isolation, identity, and HTTPS ingress are all provided by AgentCore itself. Concursus emits one serving harness per protocol (HTTP `/invocations`, MCP `/mcp` over streamable-http, or A2A JSON-RPC 2.0), and provision hands the built image to `CreateAgentRuntime`; the runtime's protocol and endpoint qualifier travel in the [`BuildPlanEntry.invoke`](#buildplanentry) block that the [`Supervisor`](execute.md) reads when it dispatches.

| Symbol | Kind | Summary |
|---|---|---|
| [`RunFn`](#runfn) | type alias | The injectable shell runner signature. |
| [`ProvisionError`](#provisionerror) | exception | A provisioning failure the code can explain. |
| [`Clients`](#clients) | dataclass | The three AWS clients provisioning needs. |
| [`role_name`](#role_name--repo_name) | function | The IAM role name for an entry. |
| [`repo_name`](#role_name--repo_name) | function | The ECR repository for an entry. |
| [`ensure_execution_role`](#ensure_execution_role) | function | Create/update the IAM role; return its ARN (idempotent). |
| [`ensure_ecr_repo`](#ensure_ecr_repo) | function | Create/look up the ECR repo; return its URI (idempotent). |
| [`build_and_push_image`](#build_and_push_image) | function | Build + push the container image; return its URI. |
| [`provision_agent`](#provision_agent) | function | Provision one agent; return a result dict. |
| [`reconcile_reservations`](#reconcile_reservations) | function | Recover crash-dangling two-phase reservations before a deploy (opt-in). |
| [`provision_plan`](#provision_plan) | function | Provision every node in the plan; return per-node results. |

### `RunFn`

```python
RunFn = Callable[..., None]
```

Type alias for the injectable shell runner: `(cmd, input=?, cwd=?) -> None`, raising on a non-zero exit. The default implementation shells out via `subprocess.run(..., check=True, text=True)` (imported lazily). Inject a fake to capture the docker commands in tests.

### `ProvisionError`

```python
class ProvisionError(RuntimeError)
```

Raised when provisioning a plan against AWS fails in a way the code can explain: missing boto3, a missing execution role, a runtime `CREATE_FAILED` / `UPDATE_FAILED`, or a readiness timeout. Subclasses `RuntimeError`. In [`provision_plan`](#provision_plan) both `ProvisionError` and raw botocore AWS errors are converted to per-node `"failed"` results; any other exception is re-raised as a genuine bug.

### `Clients`

```python
@dataclass
class Clients:
    iam: Any
    ecr: Any
    control: Any
```

The three AWS clients provisioning needs — IAM, ECR, and `bedrock-agentcore-control`. Inject fakes in tests.

#### `Clients.default`

```python
@classmethod
def default(cls, region: Optional[str] = None) -> "Clients"
```

Bind real boto3 clients: IAM (global), plus ECR and `bedrock-agentcore-control` (regional when `region` is given).

- **Raises:** `ProvisionError` — if boto3 is not importable. boto3 is imported **lazily here**, so the module has no hard SDK dependency at import; install the `[agentcore]` extra (`pip install concursus[agentcore]`) to use a real deploy.

### `role_name` / `repo_name`

```python
def role_name(entry: "BuildPlanEntry") -> str
def repo_name(entry: "BuildPlanEntry") -> str
```

- `role_name` — the IAM role name for an agent's execution role: `"concursus-{sanitized name}-exec"`, truncated to ≤ 64 chars (AgentCore-valid).
- `repo_name` — the ECR repository for an agent's image: `entry.ecr_repo` if set, else the derived default `"concursus/{sanitized-lowercased name}"`.

Both sanitize the name to alphanumerics plus `-`/`_` (other characters become `-`; leading/trailing `-_` stripped).

### `ensure_execution_role`

```python
def ensure_execution_role(role: dict, name: str, iam: Any) -> str
```

Create (or update) the execution role, attach its inline policy, and return the role ARN. **Idempotent** — on `EntityAlreadyExists` it refreshes the assume-role (trust) policy and looks up the ARN instead of failing. It always puts the inline policy `concursus-exec`.

- **Parameters:** `role` is the `{"policy": ..., "trust": ...}` document from the build plan (the trust doc drives `create_role` / `update_assume_role_policy`; the policy doc drives `put_role_policy`); `name` from [`role_name`](#role_name--repo_name); `iam` the IAM client.
- **Raises:** re-raises the `create_role` exception if its AWS error code is not `EntityAlreadyExists`.

### `ensure_ecr_repo`

```python
def ensure_ecr_repo(name: str, ecr: Any) -> str
```

Create (or look up) the ECR repository and return its `repositoryUri`. **Idempotent** — on `RepositoryAlreadyExists` it describes the repo and returns the existing URI.

- **Raises:** re-raises the `create_repository` exception if its AWS error code is not `RepositoryAlreadyExists`.

### `build_and_push_image`

```python
def build_and_push_image(
    entry: "BuildPlanEntry",
    repo_uri: str,
    *,
    source_dir: str,
    tag: str,
    ecr: Any,
    run: RunFn,
) -> str
```

Assemble a temp build context (a copy of `source_dir` with the plan's generated `app.py` + `Dockerfile` dropped in), `docker login` to ECR, `docker build` for `linux/arm64`, `docker push`; return the pushed image URI `"{repo_uri}:{tag}"`.

- The user's project is **never mutated** — the context is a temp copy (`shutil.copytree(dirs_exist_ok=True)`), always cleaned up in a `finally`.
- A missing `requirements.txt` is seeded with `bedrock-agentcore`.
- Images are built `--platform linux/arm64` **explicitly**, so a build on an x86/CI host does not push a green-but-unlaunchable amd64 image (AgentCore Runtime only launches arm64).

### `provision_agent`

```python
def provision_agent(
    entry: "BuildPlanEntry",
    *,
    clients: Clients,
    source_dir: str = ".",
    tag: str = "latest",
    run: Optional[RunFn] = None,
    known_fingerprints: Optional[Dict[str, str]] = None,
    manifest: Optional["AgentManifest"] = None,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
    ledger: Optional["DeployLedger"] = None,
    now: Optional[Union[str, int, float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
    two_phase: bool = False,
) -> Dict[str, Any]
```

Provision one agent and return a result dict `{"node", "arn", "action", "role_arn", "image_uri", ...}`. The order of operations:

1. **arn-reuse** — if the create request is an `{"agentRuntimeArn": ...}` marker, return it outright with `action="reused"` (nothing created).
2. **Persisted reuse-by-content** (opt-in) — if a `ledger` is passed and a row already exists for this `(name, fingerprint)`, it is a no-op `action="reused"` (build + create skipped), *even across separate CLI invocations*. Checked **before** in-memory reuse.
3. **In-memory reuse-by-content** (opt-in) — a `known_fingerprints[node]` equal to `entry.fingerprint` is `action="reused"`; a changed one flags the create as `action="updated"`.
4. **Create-time trust gate** (opt-in) — if a `manifest` is supplied, [`evaluate_deploy_gate`](#evaluate_deploy_gate) fires **exactly once** here; a `HOLD` returns `action="escalated"` with a `reason` and no create, a `SHADOW` sets `result["qualifier"]` (non-`DEFAULT`) + `reason` but leaves the create request clean.
5. Ensure the IAM role, build + push the image when the URI is still the placeholder. **Two-phase RESERVE** (opt-in) — if `two_phase=True` *with* a `ledger` and a `fingerprint`, append a `status="reserving"` entry (keyed by `(name, fingerprint)`, carrying the deterministic `agentRuntimeName`) **before** the AWS call.
6. Substitute both placeholders, call `CreateAgentRuntime`, poll to `READY`. **Two-phase CONFIRM** (opt-in) — on success, supersede the reservation with a `status="confirmed"` entry carrying the real ARN. Finally, optionally append the outcome to the ledger.

- **`action` values:** `"reused"` (arn-reuse marker, ledger hit, or matching in-memory fingerprint), `"escalated"` (trust-gate `HOLD` — nothing created, includes `reason`), `"updated"` (in-memory fingerprint changed), `"created"` (fresh).
- **Parameters:** `clients` (required, keyword-only) — the AWS clients; `source_dir` — the build-context directory; `tag` — the image tag; `run` — the shell runner (defaults to the subprocess runner); `known_fingerprints` — node → previously-deployed fingerprint for in-memory reuse; `manifest`, `min_autonomy`, `require_approval` — feed the trust gate; `ledger` — the persisted [`DeployLedger`](#deployledger); `now` — the ledger `deployed_at`/reservation `at` stamp (falls back to a call-time UTC timestamp, never an import-time clock read); `sleep` — the poll sleeper (defaults to `time.sleep`); `two_phase` (default `False`) — wrap the create in the reserve→confirm actuation described above (a **no-op** unless a `ledger` and a `fingerprint` are also present).
- **Returns:** the result dict.
- **Raises:** `ProvisionError` — a missing execution role (the `roleArn` is still the placeholder and no role was synthesized), or a runtime `CREATE_FAILED` / `UPDATE_FAILED` / readiness timeout. When `two_phase` is on, such a failure between RESERVE and CONFIRM deliberately leaves the `reserving` entry **dangling** for [`reconcile_reservations`](#reconcile_reservations) to recover on the next deploy.

> Reuse-by-content, the trust gate, and two-phase actuation are all opt-in: with no `known_fingerprints`, `ledger`, `manifest`/policy, or `two_phase=True`, behavior is byte-for-byte unchanged — every provisioned runtime is `"created"` and no reservation entries are written. `CreateAgentRuntime` is asynchronous (returns while `CREATING`), so a node is only recorded as usable after polling to a terminal `READY`; a later `CREATE_FAILED` is therefore never dedup-cached as created — and, under two-phase, the CONFIRM only fires *after* the readiness wait, so a runtime that later `CREATE_FAILED` is never confirmed.

```python
from concursus import provision_agent, Clients, TrustGrade

# Offline unit test with fakes:
res = provision_agent(entry, clients=Clients(iam=fake_iam, ecr=fake_ecr, control=fake_control),
                      run=fake_run)
res["action"]   # 'created'

# A side-effecting node held for approval:
res = provision_agent(entry, clients=clients, manifest=m,
                      min_autonomy=TrustGrade.L2_GUARDED, require_approval=True)
res["action"]   # 'escalated'  (no ARN, carries a 'reason')
```

> A `SHADOW` decision surfaces `result["qualifier"]` (non-`DEFAULT`) but does **not** alter the `CreateAgentRuntime` request — the shadow endpoint is a separate downstream concern. An `"escalated"` (`HOLD`) node has no ARN; callers must not treat it as deployed.

```python
# Two-phase crash-safe actuation (opt-in) — reserve → actuate → confirm, offline-testable:
from concursus import provision_agent, Clients, DeployLedger

led = DeployLedger(".concursus/ledger.json")
res = provision_agent(entry, clients=Clients(iam=fake_iam, ecr=fake_ecr, control=fake_control),
                     run=fake_run, ledger=led, two_phase=True, now="2026-07-21T00:00:00+00:00")
res["action"]              # 'created'
led.pending_reservations() # []  — the create succeeded, so the reservation was confirmed, not left dangling
```

### `reconcile_reservations`

*The stale-resource reconciler for two-phase actuation. Opt-in; the default deploy path never calls it.*

```python
def reconcile_reservations(
    ledger: "DeployLedger",
    *,
    clients: Optional[Clients] = None,
    control: Any = None,
    find_runtime: Optional[Callable[[str], Optional[str]]] = None,
    now: Optional[Union[str, int, float]] = None,
) -> List[Dict[str, Any]]
```

Recover the dangling two-phase reservations a crashed deploy left behind; return one result dict per reservation. Called at the **start** of a deploy (before provisioning) — [`provision_plan`](#provision_plan) invokes it automatically when `two_phase=True` and a `ledger` is present. For each still-`reserving` entry the ledger surfaces via [`DeployLedger.pending_reservations`](#deployledgerreservations--pending_reservations) (the newest entry for a `(node, fingerprint)` key is `reserving` — no `confirmed`/`compensated` ever followed), it decides:

- **adopt** — a runtime already exists under the reservation's deterministic `runtime_name` (the pre-crash `CreateAgentRuntime` *did* land). Its ARN is discovered and [`ledger.confirm_reservation`](#deployledgerreserve--confirm_reservation--compensate_reservation) is appended, resolving the reservation so the runtime is **reused, not re-created**. The result is `{"node", "action": "adopted", "arn"}`.
- **compensate** — no such runtime is found (the crash was *before* the create landed). [`ledger.compensate_reservation`](#deployledgerreserve--confirm_reservation--compensate_reservation) is appended, clearing the dangling reservation so the next [`provision_agent`](#provision_agent) re-provisions the node cleanly. The result is `{"node", "action": "compensated", "arn": None}`.

- **Adoption probe (injectable):** pass `find_runtime(runtime_name) -> arn | None` directly (a fake in tests), or a `control` / `clients` control plane whose `list_agent_runtimes` pages are walked by the internal `_find_runtime_by_name`. **With none of those, adoption is impossible and every dangling reservation is compensated** — the safe direction (the reconciler never re-adopts a runtime it cannot prove exists). This makes the whole recovery path offline + unit-testable: no AWS is needed when `find_runtime` is injected.
- **Parameters:** `ledger` (required) — the persisted [`DeployLedger`](#deployledger); `clients` / `control` — the AWS control plane for the default probe; `find_runtime` — an injected probe overriding the control-plane walk; `now` — the appended entry's `at` stamp (falls back to a call-time UTC timestamp).
- **Returns:** one result dict per reconciled reservation (empty list when nothing is pending — an empty/`None` ledger is a no-op).

```python
from concursus.build.provision import reconcile_reservations
from concursus import DeployLedger

led = DeployLedger(".concursus/ledger.json")
# Offline recovery with an injected adoption probe (no AWS):
outcomes = reconcile_reservations(led, find_runtime=lambda name: fake_arns.get(name),
                                 now="2026-07-21T00:00:00+00:00")
for o in outcomes:
    print(o["node"], o["action"], o.get("arn"))   # 'adopted' / 'compensated'
```

> This runs **before** provisioning, so a fresh deploy starts from a clean reservation ledger: dangling reservations are either adopted (reused) or compensated (cleared) first, and only then does the plan walk begin. It is a no-op unless two-phase actuation was previously used, so today's deploy is byte-for-byte unchanged.

### `provision_plan`

```python
def provision_plan(
    plan: "ProvisioningPlan",
    *,
    region: Optional[str] = None,
    source_dirs: Optional[Dict[str, str]] = None,
    default_source_dir: str = ".",
    tag: str = "latest",
    clients: Optional[Clients] = None,
    run: Optional[RunFn] = None,
    known_fingerprints: Optional[Dict[str, str]] = None,
    halt_on_error: bool = True,
    manifests: Optional[Dict[str, "AgentManifest"]] = None,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
    ledger: Optional["DeployLedger"] = None,
    now: Optional[Union[str, int, float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
    two_phase: bool = False,
) -> List[Dict[str, Any]]
```

Provision every agent in `plan.order` and return one result dict per node, in order. Each [`provision_agent`](#provision_agent) call is guarded: on a `ProvisionError` or a raw AWS error a `{"node", "action": "failed", "error"}` result is recorded; with `halt_on_error=True` (the default, preserving today's fail-fast deploy) the walk stops after the failure, and with `False` it continues. **Either way the accumulated results are always returned**, so already-provisioned nodes' ARNs are preserved.

- **Parameters:** `clients` defaults to [`Clients.default(region)`](#clientsdefault); `run` to the subprocess runner; `source_dirs` maps a node to its build-context dir (falling back to `default_source_dir`); the reuse/gate/ledger parameters mirror [`provision_agent`](#provision_agent) and all default to no-ops (keeping today's unconditional `"created"` behavior); `two_phase` (default `False`) — enable crash-safe two-phase actuation via the `ledger` (see below). Reads `plan.order` and `plan.entries[node]`.
- **Two-phase recovery (opt-in):** when `two_phase=True` **and** a `ledger` is supplied, [`reconcile_reservations`](#reconcile_reservations) runs **once, before** the plan walk — adopting or compensating any dangling reservation a previously-crashed deploy left behind — and then each [`provision_agent`](#provision_agent) call reserves-then-confirms its own create. With `two_phase=False` (the default) or no ledger this is a no-op and the deploy is byte-for-byte unchanged.
- **Returns:** `List[Dict[str, Any]]` — one result per node.
- **Raises:** re-raises any exception that is neither a `ProvisionError` nor an AWS error — treated as a genuine bug.

```python
from concursus import provision_plan, DeployLedger

results = provision_plan(
    plan,
    ledger=DeployLedger(".concursus/ledger.json"),
    now="2026-07-08T00:00:00+00:00",
    halt_on_error=False,
)
for r in results:
    print(r["node"], r["action"], r.get("arn"))
```

---

## `build.trust`

Source: [`../../src/concursus/build/trust.py`](../../src/concursus/build/trust.py)

The **create-time deploy gate** — decide `live` | `shadow` | `hold` for one agent node. Pure stdlib: no AWS, no state. It defines the author-declared trust vocabulary ([`TrustGrade`](#trustgrade)) and the pure gating function ([`evaluate_deploy_gate`](#evaluate_deploy_gate)) that [`provision_agent`](#provision_agent) consults **exactly once per node per deploy**.

> This is **not** the runtime, per-decision Trust Ladder — that live grading (choosing among competing agents, re-earning trust from run outcomes) is [`governor.TrustLadderScheduler`](governor.md). This gate is the compile-time seed the governor grades against: it fires once at provision time, never per-invocation, and never picks among agents. Keeping it out of the running Supervisor is what makes each deploy a pure, replayable, auditable decision.

| Symbol | Kind | Summary |
|---|---|---|
| [`LIVE` / `SHADOW` / `HOLD`](#the-mode-and-qualifier-constants) | constants | The three deploy modes. |
| [`DEFAULT_QUALIFIER` / `SHADOW_QUALIFIER`](#the-mode-and-qualifier-constants) | constants | The endpoint qualifiers. |
| [`TrustGrade`](#trustgrade) | enum | The author-declared autonomy grade (`IntEnum`). |
| [`GateDecision`](#gatedecision) | dataclass (frozen) | The immutable outcome of the gate. |
| [`evaluate_deploy_gate`](#evaluate_deploy_gate) | function | The pure gating function. |
| [`clamp_trust_grade`](#clamp_trust_grade) | function | Clamp a requested grade down to the compiled ceiling. |

### The mode and qualifier constants

```python
LIVE   = "live"    # provision to the live DEFAULT endpoint (today's behavior)
SHADOW = "shadow"  # provision, but to a non-DEFAULT (shadow) endpoint — observed, not promoted
HOLD   = "hold"    # do not provision; escalate for approval

DEFAULT_QUALIFIER = "DEFAULT"  # endpoint qualifier for a LIVE deploy
SHADOW_QUALIFIER  = "SHADOW"   # endpoint qualifier for a SHADOW deploy
```

These are the values that flow into [`GateDecision.mode`](#gatedecision) and `GateDecision.qualifier`.

### `TrustGrade`

```python
class TrustGrade(IntEnum):
    L0_SHADOW = 0
    L1_CANARY = 1
    L2_GUARDED = 2
    L3_AUTONOMOUS = 3
```

The autonomy a manifest author declares (or an operator floor requires) for a node. An `IntEnum` ordered `L0 < L1 < L2 < L3`, so grades compare with `<` / `<=` directly. `L0_SHADOW` means "cleared to run, but only in shadow"; `L3_AUTONOMOUS` is full live autonomy. This is the default type of [`AgentManifest.trust_seed`](core.md#agentmanifest).

#### `TrustGrade.parse`

```python
@classmethod
def parse(cls, value: Union["TrustGrade", int, str]) -> "TrustGrade"
```

Coerce a `TrustGrade`, an int `0-3`, or a name string into a `TrustGrade`. Accepts full names (`"L0_SHADOW"`), the short prefix (`"L0"`), or the long suffix (`"SHADOW"`), case-insensitive, plus digit strings.

- **Raises:** `ValueError` — on a `bool` (explicitly rejected, even though `bool` is an `int` subclass), an out-of-range int, or any unrecognized value.

```python
from concursus import TrustGrade

TrustGrade.parse("L2")        # <TrustGrade.L2_GUARDED: 2>
TrustGrade.parse("SHADOW")    # <TrustGrade.L0_SHADOW: 0>
TrustGrade.parse(3)           # <TrustGrade.L3_AUTONOMOUS: 3>
```

### `GateDecision`

```python
@dataclass(frozen=True)
class GateDecision:
    mode: str
    qualifier: Optional[str]
    reason: str = ""
```

The immutable outcome of the create-time deploy gate for one node. **Frozen.**

| Field | Meaning |
|---|---|
| `mode` | [`LIVE`](#the-mode-and-qualifier-constants), `SHADOW`, or `HOLD`. |
| `qualifier` | `"DEFAULT"` for live, `"SHADOW"` for shadow, `None` for a held node (nothing deployed). |
| `reason` | A human-legible explanation (empty for a plain live deploy). |

### `evaluate_deploy_gate`

```python
def evaluate_deploy_gate(
    *,
    side_effecting: bool,
    trust_seed: TrustGrade,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
) -> GateDecision
```

A pure function that decides `live` | `shadow` | `hold` for one node at create time. All arguments are keyword-only. The rules, evaluated once per deploy (never per invocation), in order:

| # | Condition | Result |
|---|---|---|
| 1 | not `side_effecting` | `LIVE` (a read-only agent is never gated) |
| 2 | `min_autonomy is None` **and** not `require_approval` (no caller policy) | `LIVE` (deploy byte-for-byte unchanged) |
| 3 | `require_approval` | `HOLD` — held for explicit approval |
| 4 | `trust_seed < min_autonomy` | `HOLD` — below the required floor |
| 5 | cleared the floor but `trust_seed <= L0_SHADOW` | `SHADOW` — "cleared, but not live" |
| 6 | otherwise | `LIVE` |

- **Returns:** a [`GateDecision`](#gatedecision). `HOLD` decisions carry a `reason` naming the cause (require-approval, or the failing `min_autonomy`).
- **Note:** `require_approval` takes precedence over the `min_autonomy` comparison (checked first among the policy branches).

```python
from concursus import evaluate_deploy_gate, TrustGrade

evaluate_deploy_gate(side_effecting=False, trust_seed=TrustGrade.L0_SHADOW)
# GateDecision(mode='live', qualifier='DEFAULT', reason='')

evaluate_deploy_gate(side_effecting=True, trust_seed=TrustGrade.L0_SHADOW,
                     min_autonomy=TrustGrade.L0_SHADOW)
# GateDecision(mode='shadow', qualifier='SHADOW', reason='...cleared but not live...')

evaluate_deploy_gate(side_effecting=True, trust_seed=TrustGrade.L1_CANARY,
                     require_approval=True)
# GateDecision(mode='hold', qualifier=None, reason='...--require-approval set...')
```

### `clamp_trust_grade`

```python
def clamp_trust_grade(
    compiled: Union["TrustGrade", int, str],
    requested: Union["TrustGrade", int, str],
) -> "TrustGrade"
```

Clamp a *requested* autonomy grade **down** to the `compiled` ceiling — never above it. The compile-time [`TrustGrade`](#trustgrade) a manifest author declared (or an operator floor pinned) is a monotonic ceiling: at runtime an agent-facing control surface may voluntarily opt *down* to a more cautious grade (e.g. run an `L3_AUTONOMOUS` node in `L1_CANARY` mode for one session), but it can never escalate above the compiled grade. The effective grade is `min(compiled, requested)` — a pure, replayable dial that only ever loosens toward caution.

- Both arguments are coerced via [`TrustGrade.parse`](#trustgradeparse) (a grade, int `0-3`, or name/alias).
- **Returns:** the lower of the two as a `TrustGrade`; a `requested` at or above `compiled` yields `compiled` unchanged (the escalation is silently clamped, not honored).

---

## `build.ledger`

Source: [`../../src/concursus/build/ledger.py`](../../src/concursus/build/ledger.py)

The **deploy ledger** — a persisted, JSON-backed, append-only record keyed by `(name, hosting fingerprint)`. It answers exactly one create-time question: *"have I already stood up this exact content?"* If yes, deploy skips the build + `CreateAgentRuntime` and reports `action="reused"`, even across separate CLI invocations, because the answer lives on disk. Pure stdlib with atomic writes; no AWS.

The same file gains three **additive, opt-in, default-off** projections alongside the confirmation `rows`: **typed rejections** (a plan node that was *not* stood up, and why), **two-phase reservations** (the durable-intent log behind [`reconcile_reservations`](#reconcile_reservations)), and a **desired-vs-confirmed [`reconcile`](#deployledgerreconcile)** query over the two. All keep the same append-only, atomic, rebuildable-convenience discipline as `rows`, and each is written to disk **only once first used** — so a ledger that never records one is byte-for-byte identical to the confirmation-only format.

> **Persistence-only.** The ledger deliberately drops any dispatch-time queries (`lookup(capability)` / `resolve(consumer, rights)` / `get_trust`) — it never answers "which standing agent can do task X?". It only answers the create-time content-identity question (now extended with the rejection/reservation projections above). It is rebuildable/disposable: deleting the file loses no canonical state, and an unreadable/corrupt/missing file is treated as an empty ledger rather than raising. The rejections and reservations are audit/projection too — never a second authoritative copy of run state.

| Symbol | Kind | Summary |
|---|---|---|
| [`DeployRow`](#deployrow) | dataclass | One append-only ledger row (a `CreateAgentRuntime` confirmation). |
| [`DeployRejection`](#deployrejection) | dataclass | One append-only typed-rejection entry (opt-in). |
| [`DeployReservation`](#deployreservation) | dataclass | One append-only two-phase-actuation phase entry (opt-in). |
| [`Reconciliation`](#reconciliation) | dataclass (frozen) | The result of a desired-vs-confirmed [`reconcile`](#deployledgerreconcile). |
| [`deploy_identity`](#deploy_identity--content_reuse_allowed) | function | The single canonical `(name, fingerprint)` reuse key. |
| [`content_reuse_allowed`](#deploy_identity--content_reuse_allowed) | function | Whether a resolved content-reuse policy permits reuse. |
| [`REJECTION_CODES`](#deployrejection) / [`RESERVATION_STATUSES`](#deployreservation) | constants | The closed sets of rejection codes / reservation statuses. |
| [`DeployLedger`](#deployledger) | class | The persisted, fingerprint-keyed ledger. |

### `DeployRow`

```python
@dataclass
class DeployRow:
    name: str
    fingerprint: str
    arn: Optional[str] = None
    image_uri: Optional[str] = None
    role_arn: Optional[str] = None
    deployed_at: Optional[Union[str, int, float]] = None
    action: Optional[str] = None
```

One append-only ledger row — a single `CreateAgentRuntime` outcome, keyed by content. `(name, fingerprint)` is the identity: a later deploy of the same name with the same hosting fingerprint is the *same content* and can be reused. `deployed_at` is caller-supplied (ISO string or epoch) — the ledger never reads the clock.

#### `DeployRow.to_dict` / `DeployRow.from_dict`

```python
def to_dict(self) -> Dict[str, Any]

@classmethod
def from_dict(cls, data: Dict[str, Any]) -> "DeployRow"
```

`to_dict` returns the row as a plain dict (`dataclasses.asdict`). `from_dict` builds a `DeployRow` from a dict, taking only the known row fields (`name`, `fingerprint`, `arn`, `image_uri`, `role_arn`, `deployed_at`, `action`) and defaulting missing keys to `None` — tolerant of partial or older rows, ignoring unknown keys.

### `DeployRejection`

*Opt-in; only written once a rejection is first recorded.*

```python
@dataclass
class DeployRejection:
    node: str
    code: str
    reason: Optional[str] = None
    confirmed_at: Optional[Union[str, int, float]] = None

REJECTION_CODES = ("unsupported", "invalid", "timeout", "actuator_error")
```

One append-only typed-rejection entry — a plan `node` that was **not** stood up, and *why*. A mutable dataclass with a `to_dict` / `from_dict` pair (same shape as [`DeployRow`](#deployrow); `from_dict` takes only `node`, `code`, `reason`, `confirmed_at` and defaults missing keys to `None`).

| Field | Meaning |
|---|---|
| `node` | The plan node id the rejection is keyed to. |
| `code` | One of the four `REJECTION_CODES`: `"unsupported"`, `"invalid"`, `"timeout"`, `"actuator_error"`. |
| `reason` | A free-text explanation (optional). |
| `confirmed_at` | A caller-supplied timestamp (ISO string or epoch) — the ledger never reads the clock. |

- `__post_init__` **coerces** any `code` outside `REJECTION_CODES` to `"actuator_error"` (the catch-all), so a projection over the ledger can rely on a closed set and never crashes on a typo.
- A node may be rejected more than once (retries, changed inputs); every entry is retained for audit, and the newest wins on [`DeployLedger.why_rejected`](#deployledgerrecord_rejection--rejections--why_rejected).

### `DeployReservation`

*The durable-intent log behind two-phase actuation. Opt-in; only written once a reservation is first recorded.*

```python
@dataclass
class DeployReservation:
    node: str
    fingerprint: str
    runtime_name: Optional[str] = None
    status: str = "reserving"
    arn: Optional[str] = None
    at: Optional[Union[str, int, float]] = None

RESERVATION_STATUSES = ("reserving", "confirmed", "compensated")
```

One append-only two-phase-actuation entry — a single phase transition for a `(node, fingerprint)` key. A mutable dataclass with `to_dict` / `from_dict` (taking only `node`, `fingerprint`, `runtime_name`, `status`, `arn`, `at`).

| Field | Meaning |
|---|---|
| `node` | The plan node id. |
| `fingerprint` | The hosting fingerprint — `(node, fingerprint)` is the reservation key. |
| `runtime_name` | The deterministic `agentRuntimeName` the actuator would use, so the reconciler can look up (adopt) a runtime a pre-crash actuator may already have created. |
| `status` | One of `RESERVATION_STATUSES`: `"reserving"` (written **before** the AWS call), `"confirmed"` (after it returns, carrying the real `arn`), or `"compensated"` (a reconciler cleared a dangling reservation). |
| `arn` | The real runtime ARN, set on a `"confirmed"` entry. |
| `at` | A caller-supplied timestamp of *this* entry (ISO string or epoch) — never a clock read. |

- `__post_init__` **coerces** any `status` outside `RESERVATION_STATUSES` to `"reserving"`, so a projection relies on a closed set.
- `"reserving"` is the only **non-terminal** status: a crash between reserve and confirm leaves it as the newest entry for its key — a *dangling reservation* that [`DeployLedger.pending_reservations`](#deployledgerreservations--pending_reservations) surfaces and [`reconcile_reservations`](#reconcile_reservations) resolves by adopting (`confirmed`) or clearing (`compensated`).

### `Reconciliation`

*The result of [`DeployLedger.reconcile`](#deployledgerreconcile).*

```python
@dataclass(frozen=True)
class Reconciliation:
    confirmed: Dict[str, str]                        # node -> confirmed fingerprint
    diverged: Dict[str, Optional[DeployRejection]]   # node -> newest rejection, or None
```

The immutable outcome of a desired-vs-confirmed reconcile. **Frozen** (empty-dict defaults are set via `object.__setattr__` in `__post_init__`). A pure projection — read-only over the ledger, allocates nothing on disk.

| Member | Meaning |
|---|---|
| `confirmed` | Maps a node to the fingerprint the ledger has a matching confirmation row for. |
| `diverged` | Maps a node **not** confirmed to *why*: the newest typed [`DeployRejection`](#deployrejection) recorded for it, or `None` when it was simply never stood up and never rejected. |
| `all_confirmed` | Property — `True` iff every desired node is confirmed (nothing diverged). |

### `deploy_identity` / `content_reuse_allowed`

```python
def deploy_identity(name: str, fingerprint: str) -> Tuple[str, str]
def content_reuse_allowed(context_mode: str = "") -> bool
```

Two pure module-level helpers underpinning the ledger's reuse logic.

- `deploy_identity` — the **single canonical** `(name, fingerprint)` reuse key, computed one way so the confirmation [`lookup`](#deployledgerlookup) and the [`reconcile`](#deployledgerreconcile) query can never drift on how a node's identity is derived. Content only — it never folds in a clock, an ARN, or any dispatch-time selector.
- `content_reuse_allowed` — whether a resolved content-reuse policy *permits* reusing an already-stood-up node. Only the explicit literal `"isolation"` refuses reuse (forcing a re-provision); every other value — including the empty default `""` (no policy) and `"reuse"` — permits it. The empty default is intentional: an existing caller that passes no policy is byte-for-byte unchanged (a matching row is still reused). The resolved policy typically comes from `resolve_context_mode` over the manifest's [`context_mode`](core.md#agentmanifest).

### `DeployLedger`

```python
class DeployLedger:
    def __init__(self, path: Union[str, Path]) -> None
```

A persisted, fingerprint-keyed deploy ledger. Rows load from `path` on construction and re-load before each read, so two instances over the same file see each other's writes (the file is the source of truth). Writes are atomic (a temp file in the same directory + `os.replace`) and append-only — an existing `(name, fingerprint)` row is retained for audit; the newest wins on lookup. The same instance also carries the opt-in **rejections** and **reservations** logs (each written to disk only once first used, keeping the default on-disk format unchanged).

#### `DeployLedger.lookup`

```python
def lookup(self, name: str, fingerprint: str, *, context_mode: str = "") -> Optional[DeployRow]
```

Return the newest row for `(name, fingerprint)`, or `None` if never deployed. The only create-time content-identity query the ledger answers. It re-reads the file first (so a row written by another process/instance is visible), then matches on the single canonical [`deploy_identity`](#deploy_identity--content_reuse_allowed) key (so it cannot drift from [`reconcile`](#deployledgerreconcile)), newest-first.

- **`context_mode`** (opt-in, default `""`) — the caller's *resolved* content-reuse policy for this node. When it is the explicit literal `"isolation"` the node is refused content-reuse (this returns `None` even when a matching row exists, forcing a re-provision); every other value — including the empty default and `"reuse"` — permits reuse via [`content_reuse_allowed`](#deploy_identity--content_reuse_allowed). So an existing caller that passes no policy is byte-for-byte unchanged.

#### `DeployLedger.has`

```python
def has(self, name: str, fingerprint: str, *, context_mode: str = "") -> bool
```

`True` iff this exact content `(name, fingerprint)` has already been stood up — a thin wrapper over `lookup(...) is not None`. Honors the same opt-in `context_mode` gate: an explicit `"isolation"` returns `False` even when a matching row exists.

#### `DeployLedger.record`

```python
def record(
    self,
    *,
    name: str,
    fingerprint: str,
    deployed_at: Union[str, int, float],
    arn: Optional[str] = None,
    image_uri: Optional[str] = None,
    role_arn: Optional[str] = None,
    action: Optional[str] = None,
) -> DeployRow
```

Append one deploy outcome and persist atomically; return the stored row. All arguments are keyword-only; `deployed_at` is **required** (the ledger never calls the clock itself — the call-time fallback lives in `provision`). It re-loads before appending to fold in concurrent writes, then flushes.

> `record` does **not** dedup — calling it twice for the same `(name, fingerprint)` appends two audit rows, and `lookup` returns the newest.

#### `DeployLedger.rows`

```python
def rows(self) -> List[DeployRow]
```

Return all rows in the ledger, oldest first (append-only audit history). It re-loads from disk first, then returns a copy of the list.

#### `DeployLedger.record_rejection` / `rejections` / `why_rejected`

*Opt-in typed rejections.*

```python
def record_rejection(
    self, *, node: str, code: str, confirmed_at: Union[str, int, float],
    reason: Optional[str] = None,
) -> DeployRejection

def rejections(self) -> List[DeployRejection]
def why_rejected(self, node: str) -> Optional[DeployRejection]
```

Append and query typed rejections — the audit trail of plan nodes that were **not** stood up.

- `record_rejection` — append one [`DeployRejection`](#deployrejection) and persist atomically; return it. `code` must be one of [`REJECTION_CODES`](#deployrejection) (`unsupported | invalid | timeout | actuator_error`); an unrecognized code is coerced to `"actuator_error"`. Append-only and audit-first (like [`record`](#deployledgerrecord)): a node may be rejected more than once, every entry retained. `confirmed_at` is required and caller-supplied — the ledger never reads the clock.
- `rejections` — all typed rejections, oldest first (re-loads first).
- `why_rejected` — the **newest** typed rejection recorded for `node`, or `None` if it was never rejected (re-loads first).

#### `DeployLedger.reserve` / `confirm_reservation` / `compensate_reservation`

*The three append points of two-phase actuation.*

```python
def reserve(self, *, node: str, fingerprint: str,
            runtime_name: Optional[str], at: Union[str, int, float]) -> DeployReservation
def confirm_reservation(self, *, node: str, fingerprint: str, arn: Optional[str],
                        at: Union[str, int, float], runtime_name: Optional[str] = None) -> DeployReservation
def compensate_reservation(self, *, node: str, fingerprint: str, at: Union[str, int, float],
                           runtime_name: Optional[str] = None, arn: Optional[str] = None) -> DeployReservation
```

Append the three [`DeployReservation`](#deployreservation) phase transitions; each persists atomically and returns the stored entry. All keyword-only; `at` is required and caller-supplied.

- `reserve` — **PHASE 1**: append a `status="reserving"` entry **before** the actuator is called (durable intent, carrying the deterministic `runtime_name`). Written by [`provision_agent`](#provision_agent) under `two_phase=True`.
- `confirm_reservation` — **PHASE 3**: append a `status="confirmed"` entry (carrying the real `arn`) after the create + readiness wait succeed. Supersedes the earlier `reserving` entry for the same `(node, fingerprint)` key so the key is no longer pending (newest-status-per-key wins); the `reserving` entry is retained for audit.
- `compensate_reservation` — **recovery**: append a `status="compensated"` entry clearing a dangling reservation that could not be adopted. Written by [`reconcile_reservations`](#reconcile_reservations).

#### `DeployLedger.reservations` / `pending_reservations`

```python
def reservations(self) -> List[DeployReservation]
def pending_reservations(self) -> List[DeployReservation]
```

- `reservations` — all reservation entries, oldest first (append-only audit history; re-loads first).
- `pending_reservations` — the still-`reserving` reservations, one per `(node, fingerprint)` key, oldest first. A key is *pending* iff its **newest** entry is `reserving` (a later `confirmed`/`compensated` resolves it). These are exactly the dangling reservations a crash left behind — what [`reconcile_reservations`](#reconcile_reservations) must adopt or compensate. A pure projection; re-loads first.

#### `DeployLedger.reconcile`

*Desired-vs-confirmed reconcile.*

```python
def reconcile(self, desired: Dict[str, str]) -> Reconciliation
```

Reconcile a plan's desired `{node: fingerprint}` against what the ledger confirms; return a [`Reconciliation`](#reconciliation). For each desired node, a confirmation `row` for its exact `(node, fingerprint)` content (via the single canonical [`deploy_identity`](#deploy_identity--content_reuse_allowed) key — so this can never disagree with [`lookup`](#deployledgerlookup)) lands it in `confirmed`. A node with no matching confirmation is *diverged*: it maps to the newest typed [`DeployRejection`](#deployrejection) recorded for it, or `None` when it was simply never stood up and never rejected. A pure projection over the append-only log — read-only, allocates nothing on disk.

```python
from concursus import DeployLedger

led = DeployLedger(".concursus/deploy_ledger.json")
if led.has("planner", fp):
    ...  # skip re-deploy
row = led.record(name="planner", fingerprint=fp,
                 deployed_at="2026-07-08T00:00:00+00:00", arn=arn, action="created")
for r in led.rows():
    print(r.name, r.fingerprint, r.action)

# Opt-in projections — typed rejections + desired-vs-confirmed reconcile:
led.record_rejection(node="risky", code="timeout",
                     confirmed_at="2026-07-21T00:00:00+00:00", reason="readiness timeout")
rec = led.reconcile({"planner": fp, "risky": fp2})
rec.all_confirmed              # False
rec.confirmed                  # {'planner': <fp>}
rec.diverged["risky"].code     # 'timeout'
```

---

## Invariants at a glance

- **Pure builder, single actuator.** `build.build`, `build.trust`, and `build.ledger` never import boto3 or call AWS; `build.provision` is the one module that talks to AWS + Docker, and it binds both lazily (boto3 in [`Clients.default`](#clientsdefault), `subprocess` in the default runner). The pure core and the full test suite run with neither the `[agentcore]` extra nor Docker installed.
- **Placeholders bridge build → deploy.** The build plan carries `<image-uri>` and `<execution-role-arn>` placeholders that `provision` substitutes; those literals are shared and must stay in sync between the two modules.
- **The fingerprint is deploy-dedup only.** It covers hosting identity, not agent behavior, and must never select a version at dispatch time.
- **Idempotent AWS side effects.** IAM role and ECR repo creation branch on `EntityAlreadyExists` / `RepositoryAlreadyExists`; images build for `linux/arm64` explicitly.
- **Async create is awaited.** `CreateAgentRuntime` returns while `CREATING`; a node is only recorded usable after polling to a terminal `READY`, so a later failure is never dedup-cached as created.
- **Partial-result guarantee.** `provision_plan` converts `ProvisionError` and raw AWS errors to per-node `"failed"` results and always returns the accumulated list; anything else re-raises as a bug.
- **The gate fires once per node per deploy.** `evaluate_deploy_gate` is pure and consulted exactly once by `provision_agent`; it never re-earns trust from a run outcome and never chooses among agents — that is the [governor](governor.md)'s runtime job.
- **The ledger is persistence-only, append-only, disposable.** The file is the source of truth (every read re-loads it); old rows are retained for audit; a corrupt/missing file is an empty ledger; `deployed_at` is always caller-supplied.
- **Every opt-in addition is default-off.** Two-phase actuation (`two_phase=True` + a ledger), the ledger's typed rejections and desired-vs-confirmed `reconcile`, and the `RUNTIME_BUILDERS` per-runtime-kind registry (`registry.runtime_kind`) are all disabled by default. With none of them the default compile is byte-for-byte the same `BuildPlanEntry`, deploy is the same unconditional `"created"`, and the ledger's on-disk format is unchanged (the `rejections`/`reservations` keys are written only once first used). None of the opt-in symbols are re-exported at the package root.
- **Two-phase actuation is crash-safe and reconciled first.** Under `two_phase=True`, `provision_agent` appends a `reserving` entry *before* `CreateAgentRuntime` and a `confirmed` entry *after* the readiness wait; a crash in between leaves a dangling reservation that `reconcile_reservations` adopts (a runtime exists under the deterministic name) or compensates (it does not) — running once at the *start* of the next deploy. Adoption is injectable, so the whole recovery path is offline-testable; with no probe, every dangling reservation is compensated (the safe direction).

## See also

- [Guide: Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md) — from a frozen plan to live runtimes: build artifacts, the trust gate, the ledger, and the AWS actuator.
- [Guide: Command-Line Interface](../guides/cli.md) — the `concursus deploy` command that drives this tier.
- [`assemble` reference](assemble.md) — how the `ProvisioningPlan` these builders populate is produced.
- [`core` reference](core.md) — the `AgentManifest` (and its `trust_seed`) that `build.build` compiles.
- [`execute` reference](execute.md) — the `Supervisor` that walks the frozen plan the runtimes back.
- [`governor` reference](governor.md) — the runtime, per-decision `TrustLadderScheduler` this create-time gate is distinct from.
- [Core Concepts](../concepts.md) — trust, fingerprints, and the compiler-not-governor invariant.
- [Documentation index](../README.md) — the full doc set and reading order.
