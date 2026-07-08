# Concursus

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

**Compile a declarative DAG of subagents into a deployed, orchestrated team on AWS Bedrock AgentCore.**

Concursus (Latin *"a running-together / convergence"*) is [cursus](https://github.com/TianpeiLuke/cursus)'s agent-orchestration sibling. Where cursus compiles a pipeline DAG + configs into a SageMaker pipeline, Concursus compiles an **AgentDAG** + per-agent `.agent.yaml` manifests into (1) an AgentCore **provisioning plan** — one `CreateAgentRuntime` per agent — and (2) a **supervisor** that dispatches the agents in topological order, wires each agent's declared output into its dependents' input, and routes shared state through AgentCore **Memory**.

It is the **coordinator AgentCore deliberately doesn't ship**: AgentCore gives you transport (A2A), tool discovery (Gateway), microVM isolation, identity, memory, and hosting — but no scheduler, dependency graph, or supervisor. You declare a DAG of agents; Concursus provisions them and runs them.

> **Status: alpha.** This release ships the **declarative core** — the backend-agnostic `AgentDAG` and the `AgentManifest` (`.agent.yaml`) model, with validation. The AgentCore provisioning plan + supervisor (the `OrchestrationAssembler`) are on the [roadmap](#roadmap).

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

## Roadmap

- [x] Declarative core: `AgentDAG` + `AgentManifest` (`.agent.yaml`) + validation + CLI
- [ ] Advisory dependency resolver over declared output JSON Schemas
- [ ] `OrchestrationAssembler`: emit an AgentCore provisioning plan (`CreateAgentRuntime` per agent + IAM roles + endpoints)
- [ ] The supervisor: topological dispatch over `InvokeAgentRuntime` with `AgentRef` wiring + Memory-backed state
- [ ] Gateway/A2A node types; a data-driven catalog + recommender of team topologies

## License

MIT © Tianpei Xie
