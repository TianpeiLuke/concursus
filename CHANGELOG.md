# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-07

The offline compiler — `AgentDAG` + manifests now compile into a provisioning plan and a
topological supervisor, all pure-Python (boto3 stays behind the `[agentcore]` extra, imported
lazily only when a verb actually talks to AWS).

### Added

- **`resolve`** — the dependency resolver: `extract` (a minimal JSONPath over invoke
  outputs), `resolve_edges` (compile each manifest's `depends_on` into `AgentRef` wiring),
  and `check_alignment` (type-gate every edge's producer, output field, consumer input, and
  DAG edge; raises `AlignmentError`).
- **`build`** — the runtime builder: `RuntimeBuilderFactory` dispatches a manifest to an
  HTTP/MCP/A2A template (or the `PreBuiltRegistrar` for a prebuilt image / reused runtime ARN)
  and emits a `BuildPlanEntry` — the serving `app.py`, `Dockerfile`, synthesized IAM execution
  role, and `create_agent_runtime` params. `PORTS = {HTTP: 8080, MCP: 8000, A2A: 9000}`.
- **`assemble`** — `OrchestrationAssembler` compiles an `AgentDAG` + manifests into a
  JSON-serializable `ProvisioningPlan` (validate → align → wire → synthesize → order); pure and
  offline.
- **`supervisor`** — `Supervisor` dispatches a plan in topological order, threads each
  producer's output into its dependents via the `AgentRef` wiring, shape-checks results with
  `validate_output` (raises `SchemaError`), and shares one `runtimeSessionId` across the run.
  The invoke transport is injectable; the default binds boto3's `bedrock-agentcore` data plane
  lazily.
- **CLI** — three compiler verbs alongside `info`/`validate`: `plan` (print the provisioning
  plan as indented JSON), `deploy` (dry-run what would be created, or `--execute`
  `CreateAgentRuntime` on the control plane), and `run` (`--inputs` JSON; dry-run the topo
  dispatch, or `--execute` the live `InvokeAgentRuntime` loop). `--dag FROM->TO` overrides the
  edges inferred from `depends_on`.
- **Public API** — `AgentRef`, `AlignmentError`, `resolve_edges`, `check_alignment`,
  `RuntimeBuilderFactory`, `BuildPlanEntry`, `OrchestrationAssembler`, `ProvisioningPlan`,
  `Supervisor`, `SchemaError` are now exported from `concursus`.

## [0.1.0] - 2026-07-07

Initial alpha — the declarative core.

### Added

- **`AgentDAG`** — a pure, backend-agnostic directed acyclic graph of agents/tasks:
  `add_node`/`add_edge`, `get_dependencies`/`get_dependents`, `sources`/`sinks`,
  `topological_sort` (Kahn's, raises on a cycle), `validate`, and `to_dict`/`from_dict`.
- **`AgentManifest`** — the `.agent.yaml` model (registry + contract + spec) with
  `from_yaml`/`from_dict` and `validate` (requires a hosting binding and a mandatory output
  JSON Schema — the dependency resolver's type gate).
- **`concursus` CLI** — `info`, `validate <manifest.yaml>...`, `--version`.
- Packaging: PyPI-ready (`pyproject.toml`, dynamic version from `VERSION`, `src/` layout,
  `py.typed`, `concursus[agentcore]` / `concursus[dev]` extras).
