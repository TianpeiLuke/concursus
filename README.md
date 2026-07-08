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
derived-DB discipline). Two backends share one Protocol:

- **`InProcessStateStore`** — the zero-dependency, offline default. Nothing new to install.
- **`MemoryStateStore`** — opt-in, AgentCore **Memory**-backed. Each validated output is one Blob
  event; a run **resumes by replaying** its event log, so it survives micro-VM teardown / mid-run
  crashes — the supervisor skips any node already `completed()`. boto3 is imported lazily (the
  `[agentcore]` extra); pass `run --memory-id <id> [--actor-id <id>] --execute`.

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

## Roadmap

- [x] Declarative core: `AgentDAG` + `AgentManifest` (`.agent.yaml`) + validation + CLI
- [x] Dependency resolver over declared output JSON Schemas (`AgentRef` wiring + type-gating)
- [x] `OrchestrationAssembler`: emit an AgentCore provisioning plan (`CreateAgentRuntime` per agent + synthesized IAM roles + endpoints)
- [x] The supervisor: topological dispatch over `InvokeAgentRuntime` with `AgentRef` wiring + one stable `runtimeSessionId`
- [x] `plan` / `deploy` / `run` CLI verbs (deploy/run `--execute` bind boto3 lazily)
- [x] Memory-backed shared run state (the `StateStore` seam: in-process default / AgentCore Memory opt-in, replay-resume, the AgentRef link graph + `context(node)`)
- [ ] Gateway/A2A node types; a data-driven catalog + recommender of team topologies

## License

MIT © Tianpei Xie
