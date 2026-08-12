# Concursus Documentation

*Documentation index and recommended reading order.*

This is the entry point to the `concursus` docs. If you are new, follow the **Start here** order below; otherwise jump straight to the guide or reference page you need from the tables that follow.

## What is Concursus?

**Concursus** is an agent-orchestration engine that compiles a declarative `AgentDAG` plus per-agent `.agent.yaml` manifests into (1) an AWS Bedrock AgentCore **provisioning plan** — one `CreateAgentRuntime` per agent — and (2) a **`Supervisor`** that dispatches those agents in topological order, wires each agent's declared output into its dependents' input, and threads shared run state through a pluggable `StateStore`. It is the coordinator AgentCore deliberately doesn't ship. The load-bearing rule the whole design hangs off is stated once and holds everywhere: **Concursus is a compiler, not a runtime governor** — a run is `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run`, a single forward pass over an immutable plan; every generative or mutating step (reasoning, a governor round) happens *strictly before* `assemble`, and resume is a faithful replay of an append-only log, never a re-plan mid-flight. The pure-Python core (plus PyYAML) installs and tests with no AWS and no LLM; `boto3` sits behind the `[agentcore]` extra and `langgraph` behind the `[reasoning]` extra, both imported lazily.

**The opt-in layer.** Over that same compiler, Concursus layers an opt-in, **default-off** flexibility & robustness surface — a deeper `assemble`-time contract (per-edge `strict_types` / `single_writer` / `full_input_cover`), `Supervisor.run(parallel=N)` antichain waves and the `unroll_static_fanout` compile pass, an append-only note version timeline (`FileVaultStateStore(versioned=True)`) and cross-node coordination notices, governor-side seams (`ControlSurface`, `IdleRuntimeCuller`, `GovernorLoop(episode_gate=…, event_sink=…)`), and the session-end **knowledge-transfer** connector (`state.transfer`). Turn none of them on and the default `AgentDAG → assemble → frozen plan → Supervisor.run` pass — and its resume-by-replay — is **byte-for-byte unchanged**; each seam only *widens* what a caller can ask for or *hardens* a failure path, and Concursus stays a **compiler, not a runtime governor**.

## Start here

Read these three in order to build the mental model before diving into a specific guide or reference page:

1. [Overview](overview.md) — what Concursus is, the problem it solves, and its mental model.
2. [Getting Started](getting-started.md) — install, declare your first team, compile it, and run it (Python API + CLI).
3. [Core Concepts](concepts.md) — the vocabulary and invariants: DAG, manifest, plan, state, trust, governor, reasoning.

Then reach for a **Guide** when you have a task in hand, or the **API reference** when you need an exact signature. The re-exported public surface all lives at the package root — `concursus/__init__.py` — so `from concursus import ...` is always the import path.

## Getting oriented

| Doc | What it covers |
|---|---|
| [Overview](overview.md) | What Concursus is, the problem it solves, and its mental model. |
| [Getting Started](getting-started.md) | Install, declare your first team, compile it, and run it — Python API and CLI. |
| [Core Concepts](concepts.md) | The vocabulary and invariants: DAG, manifest, plan, state, trust, governor, reasoning. |

## Guides

| Guide | What it covers |
|---|---|
| [Authoring Agents (`.agent.yaml`)](guides/authoring-agents.md) | Write agent manifests, declare dependencies, and satisfy the output-schema type gate. |
| [Compiling & Running a Team](guides/compiling-and-running.md) | The compile pipeline: resolve → assemble → freeze → supervise, plus recompile and `plan_from_goal`. |
| [Running Agents](guides/running-agents.md) | The `execute` runtime stack: the four invoker backends, `runtime:` on a manifest, the harness + `ExecutionMonitor` health checks and corrective retry, the `ObjectStore` artifact path, and futility cancellation — all opt-in through the `NodeExecutor` seam. |
| [Durable Run State](guides/durable-state.md) | The `StateStore` seam, three backends, replay-resume, and disposable projections. |
| [Session-End Knowledge Transfer](guides/knowledge-transfer.md) | The `slipbox_transfer` terminal node + acceptance gate, the episodic-log export, the strictly-outer transfer trigger, and the transfer-inclusive session rollup. |
| [The Governor (Runtime Governance)](guides/governor.md) | The strictly-outer standing loop: schedule, match by trust, run bounded episodes, escalate. |
| [The Reasoning Tier (DKS Deliberation)](guides/reasoning.md) | Form a plan by bounded deliberation, then lower it to a frozen `AgentDAG` — before compile. |
| [Deploying to AWS Bedrock AgentCore](guides/deploying-to-agentcore.md) | From frozen plan to live runtimes: build artifacts, the trust gate, ledger, and the AWS actuator. |
| [Command-Line Interface](guides/cli.md) | Full reference for `concursus`: `info`, `validate`, `plan`, `deploy`, `run`. |

## API reference

| Module | Symbols | Source |
|---|---|---|
| [`core`](reference/core.md) | `AgentDAG`, `AgentManifest`, and the dependency resolver. *(opt-in: `AgentCapabilities`, `contract_version`, `context_mode` + `resolve_context_mode`, `AgentDAG.classify_cycle_edges`.)* | [`core/`](../src/concursus/core/) |
| [`assemble`](reference/assemble.md) | `OrchestrationAssembler`, `ProvisioningPlan`, and `plan_from_goal`. *(opt-in: `strict_types` / `single_writer` / `full_input_cover` gates, `redrive_until_valid`, `unroll_static_fanout`.)* | [`assemble/`](../src/concursus/assemble/) |
| [`build`](reference/build.md) | Runtime builders, provisioning actuator, the Trust Ladder, and the deploy ledger. *(opt-in: `RUNTIME_BUILDERS` registry, two-phase reserve→confirm + `reconcile_reservations`.)* | [`build/`](../src/concursus/build/) |
| [`execute`](reference/execute.md) | The `Supervisor` — topological dispatch over a frozen plan — plus the runtime stack that invokes real leaf agents: `AgentInvoker` (dispatch by `manifest.runtime.backend` — `callable` / `agentcore` / `http` / `strands`, `api` a stub), the rule-based per-node `ExecutionMonitor`, and the `AgentHarness` wrapper (I/O, contract enforcement, monitor wiring, bounded retry). *(opt-in: `Supervisor.run(parallel=N)` antichain wave, the `NODE_EXECUTORS` dispatch seam wired via `make_harness_supervisor_factory`, `check_acceptance`, `cancel_futile`.)* | [`execute/`](../src/concursus/execute/) |
| [`state`](reference/state.md) | `StateStore` backends and the disposable projections over the append-only log. *(opt-in: `FileVaultStateStore(versioned=True)` timeline + `revert_note`, coordination notices, `RunEvent` contract, `get_run_snapshot` / `redact_snapshot`, the `state.transfer` session-end knowledge-transfer connector — `build_slipbox_transfer_manifest` / `wire_slipbox_transfer_terminal`, `export_run_log`, `TransferTriggerSink`, `session_overall_ok`.)* | [`state/`](../src/concursus/state/) |
| [`reasoning`](reference/reasoning.md) | `HypothesisTrail`, `DKSEngine` + CCS, `InnerGraph`, and the `deliberate` module (`form_plan`, `lower_to_dag`, `seed`). *(additive: `resolve_ceiling` / `MAX_FANOUT_CAP` fan-out clamp.)* | [`reasoning/`](../src/concursus/reasoning/) |
| [`governor`](reference/governor.md) | `GovernorLoop`, `TrustLadderScheduler`, `AgentRegistry`, `ScopeAddress`, `KTLODaemon`, `DirectorCockpit`. *(opt-in: `ControlSurface`, `IdleRuntimeCuller`, `GovernorLoop(episode_gate=…, event_sink=…)` + `GOV_EVENT_KINDS`, the `FanOutEventSink` EventSink.)* | [`governor/`](../src/concursus/governor/) |

Every addition above is **opt-in and default-off** — leaving the new keyword args at their `False` / `None` / empty defaults keeps the default compile-and-run path **byte-for-byte unchanged**. The `concursus` console script is defined in [`../src/concursus/cli.py`](../src/concursus/cli.py); see the [CLI guide](guides/cli.md) for its verbs.

## Design notes

| Note | What it covers |
|---|---|
| [AgentCore-aligned durable placement](agentcore_placement.md) | Where Concursus's durable state should live when a run is hosted on AWS Bedrock AgentCore, and the alignment checklist for that hosting. |

## See also

- **[Package README](../README.md)** — the product overview, installation extras, quick start, and architecture map.
- **[CHANGELOG](../CHANGELOG.md)** — the release history.

When code and docs disagree, the **code is truth** — open an issue or a PR against the doc.
