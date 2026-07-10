# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Reasoning-substrate tier (Phase 5, opt-in, LLM/LangGraph-free by default)** — the plan is
  FORMED by bounded deliberation strictly BEFORE `assemble`, then LOWERED to a frozen `AgentDAG`;
  `Supervisor.run` is untouched. Every model/agent seam is injected with deterministic-stub
  defaults, so `import concursus` and the full test suite run with **neither langgraph nor any LLM**
  installed (a new optional `reasoning` extra pins langgraph for the accelerated backend).
  - **`trailstore.HypothesisTrail`** (AI-23/26) — the durable `.3` hypothesis-branch API:
    `fanout_root_hypotheses` / `fanout_hypotheses` / `open_frontier(depth_cap, confidence_floor)` /
    `write_verdict` (append VERDICT + flip RESOLVED atomically) / `hypotheses`, plus a Dung
    grounded-semantics layer (`attack`, `compute_grounded_extension` -> IN/OUT/UNDEC, `arg_label`)
    and a `require_resolved` / `ThreadNotResolved` convergence guard.
  - **`dks_engine.DKSEngine`** (AI-24/27/32) — a BOUNDED cyclic deliberation state machine
    (observe -> … -> re-observe) carrying MDP-ish state `s_t=(n,r,c,f)`; LangGraph backend when
    available, else a pure-Python fallback driver running the same nodes/routing. `route_by_confidence`
    + CCS scoring (>=0.85 auto / 0.50–0.85 argue / <0.50 escalate) with injected `policy=` (RL seam)
    and `counter_argument_fn=` (MOOG seam). Bounded by `max_rounds`/`depth_cap`/`confidence_floor`.
  - **`inner_graph`** (AI-25/29) — `partition_frontier` / `compile_inner_graph` / `dispatch_frontier`
    run one investigator per open hypothesis (a fresh disposable per-round projection, clamped to a
    concurrency ceiling, merged order-insensitively; a worker failure is `InvestigationResult(ok=False)`,
    never an exception) with `InnerGraphDigest` write-back to the `.2` worker-log lane (idempotent
    on a dedup key) — never writing `.3` verdicts.
  - **`deliberate`** (AI-28/30/31) — `seed(goal, retriever=)` (goal-triggered episode, precedent
    priming), `lower_to_dag(root, require_resolved=)` (a pure deterministic fold of the IN-labelled
    hypotheses into an immutable `AgentDAG` the existing assembler freezes; raises on an open
    frontier), and `form_plan(...)` — the bounded SEED -> READ -> DISPATCH -> DIGEST -> VERDICT ->
    RE-READ driver that terminates in a frozen plan and hands it to the compiler.
- **Adaptive-compiler tier (opt-in, identity-preserving)** — the plan-generation feedback edge is
  now expressible AROUND the compiler without ever entering `Supervisor.run`:
  - **AI-22 `planner.plan_from_goal(goal, *, precedents=, operator_directives=, plan_model_fn=)`** —
    the generative FRONT of the compiler (emit -> validate -> freeze -> replay). It authors an
    `AgentDAG` that `OrchestrationAssembler.assemble` then validates + freezes + lowers; it
    dispatches nothing and emits no plan mid-run. The LLM is an **injected, optional**
    `plan_model_fn` seam — default `None` falls back to a trivial DETERMINISTIC template, so
    importing/using concursus needs **no** model. Retrieved precedents (AI-17) + operator
    directives are read-only context to the seam.
  - **AI-20 `OrchestrationAssembler.recompile(prior_plan, *, completed, content_hashes=, dag=,
    manifests=, max_revisions=)`** — the ONLY sanctioned plan mutation: a **bounded, monotonic**
    re-compile that emits a FRESH FROZEN SUPERSET plan **pinning** already-executed nodes
    (`completed()`/`content_hash`) to their prior entry/wiring, with a `_check_monotonic` guard
    that RAISES `MonotonicityError` on any edit/removal/reorder of an executed (or already-planned)
    node, and a `max_revisions` cap. A `ProvisioningPlan.revision` field (default `0`, surfaced in
    `to_dict()` only when non-zero) tracks the re-compile count. The prior/running plan is never
    mutated; resume-as-replay survives.
  - **AI-21 `concursus run --approve/--plan-approval` (+ `--yes`)** — an opt-in between-phases gate
    that previews the FROZEN `ProvisioningPlan.to_dict()` and PAUSES for confirmation BEFORE any
    billed `InvokeAgentRuntime` (safe precisely because the plan is frozen). Interactive by
    default; a non-TTY requires `--yes` or aborts. **Off by default** — today's `run --execute`
    path is byte-for-byte unchanged.
  - **AI-19 `docs/agentcore_placement.md`** — a design note (no runtime code, no boto3) for
    AgentCore-aligned durable placement: AgentCore Memory as the canonical append-only log + a
    derived on-disk `FileVault` vault (notes + `rundb`) on a BYO EFS mount via
    `filesystemConfigurations`, kept EXTERNAL/opt-in behind the `StateStore` seam, plus the
    session-scoped-writes / `VPC`+2049 / `HealthyBusy` / EFS-advisory-lock alignment checklist.
- **Deterministic `MemoryStateStore` ordering** — a store-local strict-monotonic sequence
  (`Record.seq`, mirroring `InProcessStateStore._clock`) is now the primary tie-break in
  `_is_newer`, replacing reliance on the ambiguous AgentCore `eventTimestamp` (kept for display).
  Concurrent branch/retry writes resolve deterministically on replay.
- **`RunIndex.validate()`** — an opt-in structural layout guard (`RunIndexError`) asserting the
  honest-tree invariants over the materialized-path addresses (every non-root address's
  parent-prefix is a real record; every root segment names a known node; optional contiguous
  attempts). Never mutates or re-addresses.
- **Incremental run DB + optional FTS** — `build_run_db(run_dir, incremental=True)` (default) now
  re-ingests only notes whose `st_mtime` changed (mtime/`content_hash`-keyed, mirroring the
  vault's `build_unified_db` discipline), drops rows for vanished notes, and rebuilds only the
  derived read-models — byte-for-byte identical to a full rebuild (`incremental=False`). An
  optional `records_fts` FTS5 table indexes run outputs for full-text search, degrading gracefully
  when the SQLite build lacks FTS5. Still a derived, gitignored, disposable projection.
- **ARN binding-integrity assertion (opt-in)** — `Supervisor(arn_resolver=…)` verifies, just
  before invoke, that a node's compiled ARN is provisioned (not the `<agent-runtime-arn>`
  placeholder) and — when a resolver is supplied — matches the authoritative ARN; a mismatch
  fails/records ("re-compile") rather than **silently rebinding** a frozen binding. It is an
  integrity *assertion*, never a runtime rebind and never a dispatch-time agent chooser.
- **Deploy governance (opt-in)** — `provision_plan(halt_on_error=…)` always returns partial
  results with a `failed` verb so one bad node no longer discards an in-progress deploy; a new
  `trust.py` (`TrustGrade`, pure `evaluate_deploy_gate`) + three declarative `AgentManifest` fields
  (`trust_seed` / `side_effecting` / `escalate_boundary`) add a **create-time** live|shadow|hold
  gate that fires once per deploy (never per-invocation, never earns/updates trust, never selects
  among agents); and a new `ledger.py` (`DeployLedger`) is a persistence-only, fingerprint-keyed,
  atomically-written deploy history enabling reuse-by-content across CLI invocations. New
  `deploy --min-autonomy/--require-approval` CLI flags. All defaults preserve today's deploy path.
- **Failure-tolerant `Supervisor` (opt-in)** — `Supervisor(on_error='record', max_attempts=N)`
  turns the static topo executor into a *fault-tolerant* one **without making it dynamic**: a
  failed node is recorded (`status='failed'`) and the run continues, transitively-blocked
  downstream nodes are skipped with a `blocked_on` reason, and `run()` returns the partial
  `{node: output}` of everything that completed. Bounded retry re-invokes the **same**
  manifest-pinned node id up to `max_attempts` (never branches/replans the topology). A new
  read-only `summary()` / `summary_line()` folds the partial outcome purely from the store, and
  the CLI prints it on failure. **Defaults are byte-for-byte fail-fast** (`on_error='raise'`,
  `max_attempts=1`) — the tested schema-error contract is unchanged.
- **Typed, self-validating `Record` fields** — `RecordStatus` / `RecordType` (`str`-subclass
  enums) + `Record.__post_init__` + `StateStoreError`: an unknown `status` now fails loudly at
  construction instead of silently dropping a node from `completed()`; unknown `record_type`
  widens-and-warns. Str-subclass enums keep every `== 'validated'` comparison and the on-disk
  form byte-identical.
- **Reentrant-lock guard** on `InProcessStateStore` / `MemoryStateStore` (`threading.RLock`
  around read-then-write bodies) so a future concurrent-dispatch supervisor cannot lose-update
  internal state. RLock only — the in-memory stores don't take on `FileVaultStateStore`'s
  fcntl+OCC.
- **Content fingerprint + reuse-by-content on deploy** — `build.fingerprint(manifest)` hashes an
  agent's **hosting identity** (container/protocol/entry/network/role/input-keys/output-schema —
  *not* model/prompt/behavior) with the same sha256-canonical-JSON discipline as
  `content_hash`, stamped onto `BuildPlanEntry.fingerprint`. `provision_agent(..., known_fingerprints=)`
  (opt-in) then reports `action='reused'` on a matching fingerprint and `'updated'` on a changed
  one — deploy dedup + real-change detection, never a dispatch-time version chooser.
- **`distill` — post-run precedent notes + a cross-run hub** — `distill.distill_run(store)` folds
  a finished run's `{node: output}` + recorded `consumes` graph + outcome into one compact
  precedent note under `<vault>/precedents/` (a sibling of `runs/`, deliberately isolated so a
  precedent is never reloaded as a run record). `distill.render_precedent_hub` is a pure,
  idempotent `entry_folgezettel_trails`-style projection over the set of precedent notes (one row
  per run), with a `runindex.PrecedentIndex` cross-run query surface and a disposable
  `rundb.build_precedent_db`. All read-only / post-run — the compiler identity is untouched.
- **`concursus run --vault --lean-form`** — the on-disk `StateStore` emits authentic
  Abuse-SlipBox notes by default (`slipbox_form=True` — indexer-ingestible, with a `_run.md`
  entry point); the new **`--lean-form`** CLI flag (or `slipbox_form=False`) opts into the lean
  machine schema (`node`/`attempt`/`status`/`consumes`/`payload`) for a smaller, non-indexed
  round-trip-exact durable log.
- **`FileVaultStateStore`** — a persistent, on-disk `StateStore` backend (no AWS), closing the
  gap that the in-memory `InProcessStateStore` (state lost on exit) and the opaque-Blob
  `MemoryStateStore` left open. Each record is written as a **round-trip-exact markdown note**
  under `<vault>/runs/<session>/`: two authoritative embedded base64 JSON blobs (`meta` + the
  output `payload`) are the source of truth (arbitrary outputs — newlines, quotes, `---`, link
  syntax, numeric-looking strings — survive exactly), while everything else is a greppable display
  copy never re-ingested. **Notes conform to the Abuse SlipBox format** by default
  (`slipbox_form=True`) — P.A.R.A. `tags` / `keywords` / `topics` / a **derived** `building_block`
  (validated→`empirical_observation`, failed→`counter_argument`, dedup→`navigation`) / valid
  `status` / `folgezettel` + `lineage` (a per-run Folgezettel trail rooted at `1`, records as
  write-order children `1a`, `1b`, …) / `access_control_group`, a typed H1, and a `## Related
  Notes` section (run entry + `consumes` producers) — so they validate under `check_note_format.py`
  and read as a genuine, indexer-ingestible slipbox trail (a `_run.md` entry point roots the trail
  so no note is an orphan). Pass `slipbox_form=False` for the lean machine schema.
  It **reuses the existing marshalling seam** (`_build_metadata` / `_event_to_record` /
  `content_hash` / `_index_records`), so it shares `MemoryStateStore`'s Record↔dict contract and
  differs only in transport. Writes are atomic (temp + `os.replace`); a reentrant lock plus a
  generation-token OCC over `.lock` / `.gen` sidecars serialize concurrent writers over one vault.
  **Resume = reload**: a fresh store over an existing vault reconstructs `completed()` / `get()`.
  `FileVaultStateStore.from_config(vault_path=, session_id=)` is the persistence-by-default
  constructor; the bare `InProcessStateStore` remains the ephemeral default.
- **`rundb.build_run_db`** — a **derived, rebuildable SQLite** graph/index over a persisted run's
  notes, mirroring the slipbox's `build_unified_db` discipline: a `records` metadata-postings
  table (indexed on node/status/record_type/schema/producer), a `consumes_edges` data-dependency
  table (the `AgentRef` graph at rest), a `run_addresses` execution-tree table, and a
  `projection` VIEW (latest validated per node). Reads **only** the notes (the single source of
  truth); the DB is gitignored and disposable — deleting it loses nothing.
- **CLI** — `concursus run --vault <dir> --execute` persists the run to the on-disk vault and
  builds its derived run DB; exposes `FileVaultStateStore` and `build_run_db` from the package
  root.

### Notes

- The on-disk notes stay the single source of truth; `RunGraph` / `RunIndex` remain the fast
  in-process derived structures, and the SQLite DB is the queryable-at-rest mirror. This is the
  offline / air-gapped / CI / debuggable durability tier (FZ 35e1b1); for AgentCore-hosted runs a
  BYO EFS/S3 Files mount or the managed Memory log remain the aligned choices (FZ 35e1b2).

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
