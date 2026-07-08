# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-07

### Added

- **`statestore`** — a `StateStore` seam for durable, addressable run state (the slipbox's
  single-source-of-truth log + derived-projection discipline). Two backends share one Protocol:
  `InProcessStateStore` (the zero-dependency, offline default — an append-only `Record` log plus a
  `{node: latest validated output}` projection, with per-node attempt auto-increment and
  `content_hash` no-op dedup) and `MemoryStateStore` (opt-in, AgentCore Memory-backed — one Blob
  event per validated output plus typed metadata; **replay-resume** rebuilds the projection from
  the event log via paginated `list_events`, so a run survives micro-VM teardown). boto3 is
  imported lazily only in the Memory backend; every test injects a fake client (no AWS). Exposes
  `StateStore` / `InProcessStateStore` / `MemoryStateStore` / `Record` / `content_hash`.
- **`rungraph`** — the AgentRef link graph: each `Record` persists its resolved `consumes` edges
  (`"producer:$.path"`), so the log projects into a queryable `RunGraph` (`from_records` /
  `from_edges`) with transitive `upstream`/`downstream`, a structural `validate` (raises
  `RunGraphError` on a cycle or a dangling AgentRef), and a bounded nearest-first `context_order`.
  Pure Python — no networkx.
- **`runindex`** — a dual index over the run log, exposing BOTH ways to read state: a **metadata
  query** surface (inverted postings over `node`/`status`/`record_type`/`schema`/`producer` —
  `query(status="failed")` is a lookup, not a payload scan, the local analogue of `list_events`
  filters) and a **Folgezettel-tree traversal** over each `Record`'s new materialized-path
  `address` (default the node name; a retry/fan-out/branch appends a `/` segment). The parent is
  prefix-derivable, so `ancestors`/`descendants`/`children`/`siblings`/`traverse` reconstruct the
  execution tree — the run-state analogue of `slipbox-traverse-folgezettel`. A sub-address maps to
  an AgentCore `branch{name, rootEventId}` in `MemoryStateStore`, so retries/fan-outs land as
  branches in the Memory log. `Supervisor.index()` returns it. Pure Python.
- **`Supervisor`** — now threads outputs through the `StateStore` seam (new `state_store=` keyword,
  defaulting to `InProcessStateStore`): a node already in `completed()` is skipped (resume), and
  each validated output is `put` with its `producer` / `consumes` / `schema` metadata. New
  `Supervisor.context(node)` returns the transitive upstream outputs (`{producer: output}`) via the
  run graph — shared upstream state as a query, not point-to-point wiring.
- **CLI** — `run --memory-id ID [--actor-id ID]` backs a `run --execute` with a durable, resumable
  `MemoryStateStore` sharing the supervisor's `runtimeSessionId` (default actor `run`); boto3 is
  used only under `--execute`, and the dry-run path still imports nothing.
- **Public API** — `StateStore`, `InProcessStateStore`, `MemoryStateStore`, `Record`,
  `content_hash`, `RunGraph`, `RunGraphError`, `RunIndex` are now exported from `concursus`.

## [0.3.0] - 2026-07-07

### Added

- **`provision`** — the deploy-time actuator behind `deploy --execute`: for each agent (in
  topological order) it ensures the IAM execution role (`create_role` + attach policy,
  idempotent), builds and pushes the container image to ECR when the plan carries a placeholder
  URI (`docker login`/`build`/`push` over a non-destructive temp build context), substitutes the
  real `roleArn` + `containerUri` into the request, and calls `CreateAgentRuntime`; an already
  built image or an existing runtime ARN is reused as-is. Every AWS client (`Clients`) and the
  shell runner are injectable, so the orchestration is unit-tested with fakes — no AWS, no Docker.
  Exposed as `provision_plan` / `Clients` / `ProvisionError`.
- **CLI** — `deploy --execute` now runs that full role→image→`CreateAgentRuntime` flow (previously
  it only called `CreateAgentRuntime` with placeholder role/image); new `--source-dir DIR|NODE=DIR`
  (build context, default `.`) and `--tag` (image tag, default `latest`). The dry-run now lists
  the role/image/create steps per agent. boto3 + the `docker` CLI are used only under `--execute`.

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
