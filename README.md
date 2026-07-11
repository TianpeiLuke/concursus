# Concursus

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

**Compile a declarative DAG of subagents into a deployed, orchestrated team on AWS Bedrock AgentCore.**

Concursus (Latin *"a running-together / convergence"*) is [cursus](https://github.com/TianpeiLuke/cursus)'s agent-orchestration sibling. Where cursus compiles a pipeline DAG + configs into a SageMaker pipeline, Concursus compiles an **AgentDAG** + per-agent `.agent.yaml` manifests into (1) an AgentCore **provisioning plan** — one `CreateAgentRuntime` per agent — and (2) a **supervisor** that dispatches the agents in topological order, wires each agent's declared output into its dependents' input, and routes shared state through AgentCore **Memory**.

It is the **coordinator AgentCore deliberately doesn't ship**: AgentCore gives you transport (A2A), tool discovery (Gateway), microVM isolation, identity, memory, and hosting — but no scheduler, dependency graph, or supervisor. You declare a DAG of agents; Concursus provisions them and runs them.

> **Status: alpha.** This release ships the **declarative core** (`AgentDAG` + `AgentManifest`) **and the offline compiler**: the dependency resolver, the runtime builder, the `OrchestrationAssembler` (DAG + manifests → a `ProvisioningPlan`), and the topological `Supervisor` — plus the `plan` / `deploy` / `run` CLI verbs. The compiler is pure-Python; boto3 stays behind the `[agentcore]` extra and is imported lazily only when `deploy --execute` / `run --execute` actually calls AWS.

---

## Installation

```bash
pip install concursus                 # declarative core (pure Python)
pip install "concursus[agentcore]"    # + the AWS Bedrock AgentCore runtime binding (roadmap)
```

Requires **Python 3.10+**.

## Quick start

Declare a team as an `AgentDAG` (nodes = agents, edges = data dependencies):

```python
from concursus import AgentDAG

dag = AgentDAG()
for agent in ["ingest", "summarize", "critique", "format"]:
    dag.add_node(agent)
dag.add_edge("ingest", "summarize")
dag.add_edge("summarize", "critique")
dag.add_edge("critique", "format")

dag.topological_sort()   # ['ingest', 'summarize', 'critique', 'format']  <- dispatch order
dag.validate()           # raises if the topology has a cycle
```

Describe each agent with an `.agent.yaml` manifest — its AgentCore binding + typed interface:

```yaml
# summarize.agent.yaml
registry:
  container_uri: 111122223333.dkr.ecr.us-east-1.amazonaws.com/summarize-agent:latest
  role_arn: arn:aws:iam::111122223333:role/ConcursusAgentRuntimeRole
  network_mode: PUBLIC       # or VPC
  protocol: HTTP             # HTTP (/invocations) | MCP (/mcp) | A2A (/)
  qualifier: DEFAULT
  # ...or reuse an already-deployed agent:
  # agent_runtime_arn: arn:aws:bedrock-agentcore:us-east-1:111122223333:runtime/summarize-xyz
contract:
  inputs:
    document: {type: string}
  outputs:                   # required — the dependency resolver's type gate
    summary: {type: string}
    key_points: {type: array, items: {type: string}}
spec:
  depends_on:
    - {from: ingest.document, to: document}
```

```python
from concursus import AgentManifest

m = AgentManifest.from_yaml("summarize.agent.yaml").validate()
m.protocol        # 'HTTP'
m.output_schema   # {'summary': {...}, 'key_points': {...}}
```

Or from the CLI:

```bash
concursus info                        # overview
concursus validate *.agent.yaml       # validate manifests
concursus --version
```

## Compile a plan (`plan` → `deploy` → `run`)

Point the compiler at your manifests. Edges are inferred from each manifest's `depends_on`
(or pass `--dag ingest->summarize` to set them explicitly). `plan` prints a JSON
`ProvisioningPlan` — a topological `order`, one `create_agent_runtime` entry per agent, and
the resolved producer→consumer `wiring` — without touching AWS:

```bash
concursus plan *.agent.yaml
```

```python
from concursus import AgentDAG, AgentManifest, OrchestrationAssembler, Supervisor

manifests = {m.name: m for m in map(AgentManifest.from_yaml, paths)}
dag = AgentDAG()
for name in manifests:
    dag.add_node(name)
dag.add_edge("ingest", "summarize").add_edge("summarize", "critique")

plan = OrchestrationAssembler(account="111122223333", region="us-east-1").assemble(dag, manifests)
plan.order           # ['ingest', 'summarize', 'critique']  <- dispatch order
plan.to_dict()       # JSON-serializable preview (what `concursus plan` prints)
```

`deploy` dry-runs what *would* be created (nothing imported); `--execute` provisions each agent
end-to-end — ensure its IAM execution role, build + push its image to ECR (when the plan carries a
placeholder URI), then `CreateAgentRuntime` — reusing an existing image or runtime ARN as-is. `run`
dry-runs the topological dispatch; `--execute` invokes the live runtimes, threading each output
into its dependents:

```bash
concursus deploy *.agent.yaml                          # dry-run: the role/image/create steps
concursus deploy *.agent.yaml --execute --source-dir . # + boto3 + docker: role → ECR image → create
concursus run    *.agent.yaml --inputs '{"uri": "s3://doc"}'            # dry-run the dispatch
concursus run    *.agent.yaml --inputs @inputs.json --execute          # live InvokeAgentRuntime
```

```python
outputs = Supervisor(plan, manifests).run({"uri": "s3://doc"})   # {node_id: output_dict}
```

## How it works (the compile target)

Concursus compiles `AgentDAG + manifests` through `validate → resolve → provision → assemble`, mapping cursus concepts onto AgentCore primitives:

| cursus | Concursus | AgentCore primitive |
|---|---|---|
| `PipelineDAG` | `AgentDAG` | dispatch order (topological) |
| `.step.yaml` | `.agent.yaml` manifest | container image + `roleArn` + protocol |
| `DependencyType` enum | output **JSON Schema** (mandatory) | the resolver's type gate |
| `PropertyReference` (deferred) | `AgentRef` (eager JSONPath) | `InvokeAgentRuntime` response |
| step registration | agent registration | `CreateAgentRuntime` → ARN + V1 + `DEFAULT` endpoint |
| `PipelineAssembler` → `Pipeline` | `OrchestrationAssembler` → supervisor + plan | `BedrockAgentCoreApp` supervisor |
| S3 artifact channels | shared run state | **AgentCore Memory** |

The supervisor dispatches agents in topological order, invokes each with `InvokeAgentRuntime` under one `runtimeSessionId` (session affinity → warm microVMs), extracts each producer's output by JSONPath and injects it into its consumers, and persists outputs to Memory so state survives the ephemeral microVMs.

## Durable run state (the `StateStore` seam)

The supervisor threads every output through a **`StateStore`** — an append-only log of validated
outputs plus a derived `{node: output}` projection (the slipbox's single-source-of-truth /
derived-DB discipline). Three backends share one Protocol:

- **`InProcessStateStore`** — the zero-dependency, offline default. Nothing new to install.
- **`MemoryStateStore`** — opt-in, AgentCore **Memory**-backed. Each validated output is one Blob
  event; a run **resumes by replaying** its event log, so it survives micro-VM teardown / mid-run
  crashes — the supervisor skips any node already `completed()`. boto3 is imported lazily (the
  `[agentcore]` extra); pass `run --memory-id <id> [--actor-id <id>] --execute`.
- **`FileVaultStateStore`** — opt-in, **persistent on-disk** (no AWS). Each record is written as a
  **round-trip-exact markdown note** under `<vault>/runs/<session>/` (two authoritative base64 JSON
  blobs — `meta` + `payload` — are the source of truth; everything else is a greppable display
  copy), so a run is durable and inspectable offline and **resumes by reloading** the notes. Notes
  are **Abuse-SlipBox-conformant** by default (P.A.R.A. tags, a derived `building_block`,
  `folgezettel`/`lineage` forming a per-run trail, a typed H1, a `## Related Notes` section) — they
  validate under `check_note_format.py` and read as a genuine slipbox trail; `slipbox_form=False`
  gives a lean machine schema. `concursus.build_run_db`
  materializes a **derived, gitignored SQLite** graph/index over the notes (metadata postings,
  `consumes` edges, the execution-address tree, a latest-validated projection view) — the notes
  stay canonical, the DB is disposable. Pure stdlib; pass `run --vault <dir> --execute`.

Each record also persists its resolved `AgentRef` edges (`consumes`), turning the log into a
**queryable run graph** (`RunGraph`: `upstream`/`downstream`, a structural `validate`, bounded
`context_order`). `Supervisor.context(node)` returns a node's transitive upstream outputs — shared
context as a query, not point-to-point wiring:

```python
from concursus import Supervisor, InProcessStateStore

sup = Supervisor(plan, manifests, state_store=InProcessStateStore())
outputs = sup.run({"uri": "s3://doc"})   # {node_id: output_dict}
sup.context("critique")                  # {producer: output} for its transitive upstream
```

## The governor (opt-in dynamic outer loop)

The compiler and supervisor are **static**: `assemble` freezes one `ProvisioningPlan` and
`Supervisor.run` executes it in a single forward pass. When you want a *dynamic* control loop —
standing "keep-the-lights-on" monitoring, replan-on-signal, program/portfolio rollups — the
**`governor`** subpackage wraps a **bounded cycle around** the compiler. The design is a **dynamic
outer loop hosting freeze inner episodes**: each round the governor forms a *fresh frozen plan* at
the compiler front and dispatches *one* new bounded `Supervisor.run` episode. It never reaches
inside a running supervisor, never mutates a frozen plan, and never turns the compiler into a
runtime governor — the compiler-not-runtime-governor identity holds.

**It is entirely opt-in.** The zero-config path stays static `assemble` → `Supervisor.run`; you
reach for the governor only when you want the cyclic driver. Like the reasoning tier, **LangGraph
stays optional** — the loop mirrors the `DKSEngine` template (lazy import, pure-Python fallback), so
everything imports and runs with no langgraph installed.

```python
from concursus import GovernorLoop

# A bounded cycle: planner (assemble/recompile) -> router -> run_episode (one Supervisor.run)
# -> collect -> {replan | synthesize}. Terminates on frontier-exhaustion / stall / max_rounds
# / a hard step_cap. backend="auto" uses LangGraph if present, else the pure-Python driver.
loop = GovernorLoop(goal="triage-abuse-signal", manifests=manifests, max_rounds=8)
result = loop.run({"uri": "s3://signal"})   # GovernorResult: plan sequence + folded episode log
```

The subpackage is layered strictly outside the compiler (identity invariants INV-1..INV-5):

- **`GovernorState`** — persistent outer-loop state: the *sequence* of frozen plan VALUEs by
  `plan_version` + a pointer to the append-only `StateStore` log (the sole executed-prefix anchor),
  never a mutable compiler plan.
- **`GovernorLoop`** — the fixed cyclic driver. `planner` forms a fresh frozen plan each round
  (first via `plan_from_goal` + `assemble`, later via monotonic `recompile`); `run_episode` calls
  `Supervisor.run` once; `collect` folds outputs into the log and re-derives the executed prefix
  from `store.completed()`. Durable dual resume (outer `plan_version` checkpoint + inner log replay)
  when backed by a `MemoryStateStore`.
- **`TrustLadderScheduler`** — the `router`'s per-decision matcher: matches each ready step to a
  standing agent (read-only `AgentRegistry`), reads its *earned* trust, and proposes a frontier
  (`DISPATCH` / `ESCALATE` L1→L3 / `UNMATCHED`) that feeds the *next* `recompile` — never mutating a
  frozen plan.
- **`AgentRegistry`** — the governor's process table: a read-only versioned view over the shipped
  `DeployLedger` answering *"which standing agent, at which version, can do task X?"* (the ledger
  answers content-identity only). Spawn/fork delegate to the shipped `provision_agent`.
- **`DirectorCockpit`** — a read-only director view composing a briefing, an exception queue, and a
  runs monitor purely out of `render_precedent_hub` + `Supervisor.summary()`/`.index()`.
- **`KTLODaemon`** — a standing monitor above the loop (`monitor → triage → escalate → replan |
  close`) that wakes on a live `EventSource` + drift and dispatches one fresh bounded episode per
  investigation. `LAUNCH` (one-shot drain) vs `KTLO` (standing, `max_ticks`-bounded) is a config,
  not two code paths.
- **`scope`** — the `org → portfolio → program → task` layer above the single run: a `ScopeAddress`
  stack, a cross-program programs index (the program-grain analogue of the runs-grain precedent
  hub), and a 1:N `director_leverage_view` — all read-only projections over the per-run precedent
  notes.

Two shipped-but-idle core seams are now wired into the dispatch path (**C-3**, identity-preserving):
the `Supervisor` constructor runs the shipped `RunGraph.validate()` **once** as a pre-dispatch
structural gate (a dangling `AgentRef` or cycle is rejected before the first invoke; `run()` stays a
single static pass), and `MemoryStateStore.replay()` is documented as a full cold rebuild — the
INV-5-correct choice, since AgentCore's `nextToken` is an opaque pagination cursor, not an
events-after filter.

## Roadmap

- [x] Declarative core: `AgentDAG` + `AgentManifest` (`.agent.yaml`) + validation + CLI
- [x] Dependency resolver over declared output JSON Schemas (`AgentRef` wiring + type-gating)
- [x] `OrchestrationAssembler`: emit an AgentCore provisioning plan (`CreateAgentRuntime` per agent + synthesized IAM roles + endpoints)
- [x] The supervisor: topological dispatch over `InvokeAgentRuntime` with `AgentRef` wiring + one stable `runtimeSessionId`
- [x] `plan` / `deploy` / `run` CLI verbs (deploy/run `--execute` bind boto3 lazily)
- [x] Memory-backed shared run state (the `StateStore` seam: in-process default / AgentCore Memory opt-in, replay-resume, the AgentRef link graph + `context(node)`)
- [x] The governor: an opt-in dynamic outer loop (`GovernorLoop` / `TrustLadderScheduler` / `AgentRegistry` / `DirectorCockpit` / `KTLODaemon` / `scope`) that drives the freeze compiler as bounded episodes (LangGraph optional)
- [ ] Gateway/A2A node types; a data-driven catalog + recommender of team topologies

## License

MIT © Tianpei Xie
