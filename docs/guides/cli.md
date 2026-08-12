# Guide: Command-Line Interface

*Full reference for `concursus`: `info`, `validate`, `plan`, `deploy`, `run` — a compiler front-end where every AWS-touching step is opt-in behind `--execute`.*

The CLI is the command-line face of the same compiler you drive from Python (see [Getting Started](../getting-started.md) and [Compiling & Running a Team](compiling-and-running.md)). It takes a set of `.agent.yaml` manifests plus an optional DAG, assembles them into a single **frozen** [`ProvisioningPlan`](../reference/assemble.md), and then either previews or actuates that plan. Nothing is generated or mutated after assembly — the plan is compiled once and treated as immutable, so the safe dry-run paths and the billed `--execute` paths run over byte-for-byte the same plan.

Source: [`cli.py`](../../src/concursus/cli.py). The two public symbols are `build_parser()` and `main()`; everything else is a private handler.

---

## Invocation

The package installs a console entry point:

```toml
# pyproject.toml
[project.scripts]
concursus = "concursus.cli:main"
```

```bash
concursus <verb> [args...]
```

Equivalent module form (the module's `__main__` guard calls `raise SystemExit(main())`):

```bash
python -m concursus.cli <verb> [args...]
```

And from Python, exactly as the console script does:

```python
from concursus.cli import main
raise SystemExit(main(["run", "a.agent.yaml"]))
```

`main(argv=None)` builds the parser, parses `argv` (defaulting to `sys.argv`), and dispatches to the selected verb, returning its `int` exit code. **With no subcommand it falls back to `info`** rather than erroring.

Global options:

| Flag | Effect |
|---|---|
| `--version` | Print `concursus <version>` and exit (argparse `action='version'`). |
| `-h`, `--help` | Print help for the program or a verb and exit. |

---

## The verbs at a glance

| Verb | What it does | Touches AWS? |
|---|---|---|
| `info` | Print the version banner + command overview. | No |
| `validate MANIFEST..` | Load and validate each `.agent.yaml`; print `OK`/`FAIL` per file. | No |
| `plan MANIFEST..` | Assemble the frozen plan and print it as JSON. | No |
| `deploy MANIFEST..` | Dry-run the provisioning steps; with `--execute`, run `CreateAgentRuntime`. | Only with `--execute` |
| `run MANIFEST..` | Dry-run the topological dispatch; with `--execute`, drive a live `Supervisor`. | Only with `--execute` |

### The dry-run-by-default contract

`deploy` and `run` are **dry-run by default**. Without `--execute` they print exactly what they *would* do and touch nothing — and, critically, they do not even import `boto3`/`docker`. Only `--execute` binds those dependencies, lazily, at the moment the side-effecting path is taken. `info`, `validate`, `plan`, and both dry-run paths run entirely on the stdlib-plus-core import surface, so they work without the `[agentcore]` extra installed. See [Deploying to AgentCore](deploying-to-agentcore.md) for what the `[agentcore]` extra brings in.

---

## `info`

```bash
concursus info
```

Prints the version banner (`concursus <version>`) and a one-screen command overview to stdout, then returns `0`. Takes no arguments. This is also what runs when you invoke the CLI with no verb.

---

## `validate`

```bash
concursus validate MANIFEST [MANIFEST ...]
```

Loads each path via `AgentManifest.from_yaml(path).validate()` (see [`core/manifest.py`](../../src/concursus/core/manifest.py) and the [core reference](../reference/core.md)) and prints one line per file:

```
OK    ingest.agent.yaml  (agent 'ingest', protocol HTTP)
FAIL  broken.agent.yaml  -> <error>     # to stderr
```

A per-file failure (`ManifestError`, `OSError`, or `ValueError`) is caught, reported to stderr, and does **not** stop the remaining files. Exit code is `0` if every manifest passes, `1` if any failed. No AWS access. This is the fastest way to check manifests before authoring a DAG — see [Authoring Agents](authoring-agents.md).

| Argument | Type | Description |
|---|---|---|
| `manifests` | positional, 1+ | One or more `.agent.yaml` paths to validate. |

---

## The compiler backbone (shared by `plan` / `deploy` / `run`)

All three compiler verbs share the same assembly front-end and the same base arguments:

| Flag / argument | Applies to | Description |
|---|---|---|
| `manifests` | plan, deploy, run | Positional, 1+. Paths to `.agent.yaml` files. Manifests are keyed by their declared `name`; a later path with a duplicate name overwrites an earlier one. |
| `--dag FROM->TO` | plan, deploy, run | Explicit dependency edge, **repeatable**. Accepts `FROM->TO` or `FROM:TO` (`->` wins if both separators appear). Empty endpoints are an error. |
| `--account` | plan, deploy, run | AWS account id, threaded into synthesized IAM roles. |
| `--region` | plan, deploy, run | AWS region, threaded into synthesized IAM roles. |

**DAG construction.** One node is created per manifest via `AgentDAG.add_node(name)`. If any `--dag` edges are given they take **full precedence**. Otherwise edges are inferred from each manifest's `depends_on`: the producer is the part of an edge's `from` value before the first `.`, and the edge `producer -> node` is added only if that producer is a known manifest name. Unresolvable producers are left for the assembler's alignment check to report.

**Assembly.** The manifests and DAG are handed to `OrchestrationAssembler.assemble(dag, manifests)` (see [assemble reference](../reference/assemble.md) and [`assemble/assemble.py`](../../src/concursus/assemble/assemble.py)), producing the single frozen `ProvisioningPlan` that every downstream path reads but never mutates. Setup errors (`ValueError`, `OSError` from loading, parsing, or alignment) are caught by each verb and reported as `FAIL  <error>` on stderr with exit code `1`.

---

## `plan`

```bash
concursus plan MANIFEST.. [--dag FROM->TO ...] [--account ID] [--region R]
```

Assembles the frozen plan and prints `plan.to_dict()` as indented JSON to stdout. **No AWS access.** This is the canonical way to inspect what `deploy`/`run` will act on. Exit `0` on success; on a load/parse/assembly error it prints `FAIL  <error>` to stderr and returns `1`.

```bash
concursus plan *.agent.yaml \
  --dag 'ingest->summarize' \
  --account 123456789012 --region us-east-1
```

---

## `deploy`

```bash
concursus deploy MANIFEST.. [--dag ...] [--account ID] [--region R] \
    [--execute] [--source-dir DIR|NODE=DIR ...] [--tag TAG] \
    [--min-autonomy GRADE] [--require-approval]
```

Assembles the plan, then either prints the provisioning dry-run (default) or actuates it (`--execute`).

**Dry run (default).** Prints, per agent in topological order, what `--execute` *would* do: whether the runtime is **REUSED** (the request already carries an `agentRuntimeArn`), or the ordered steps to create it — create IAM execution role (if one is synthesized), build + push the container image to ECR (for `container` build mode with an unresolved image URI), and `CreateAgentRuntime` with the resolved protocol. Ends with the `pip install concursus[agentcore]` hint. No `boto3`/`docker` import. Exit `0`.

**Execute (`--execute`).** Lazily imports and calls `provision_plan(...)` (see [build reference](../reference/build.md) and [`build/provision.py`](../../src/concursus/build/provision.py)) to ensure IAM roles, build + push images, and call `CreateAgentRuntime`. Provisioning runs with `halt_on_error=False`: **a failing node is reported and the rest are still attempted** (partial-result safe). Each result prints a fixed-width verb:

| Verb | Meaning |
|---|---|
| `REUSE` | Existing runtime reused. |
| `CREATED` | New runtime created. |
| `UPDATED` | Existing runtime updated. |
| `ESCALATE` | Held by the create-time trust gate — `HELD: <reason>` on stderr, sets exit `1`. |
| `FAILED` | Provisioning error — `-> <error>` on stderr, sets exit `1`. |

Successful lines also append qualifier (when a non-`DEFAULT` shadow endpoint), image URI, and role ARN. Exit code is `1` if any node escalated or failed, else `0`. A broad provisioning failure (boto3/Docker) is caught and reported as `FAIL  <error>` on stderr with exit `1`.

### `deploy` flags

| Flag | Description |
|---|---|
| `--execute` | Actually provision on AWS (binds `boto3` + the `docker` CLI). Without it, a dry-run that imports nothing. |
| `--source-dir DIR` | Build-context dir holding agent code + `requirements.txt` (default `.`). |
| `--source-dir NODE=DIR` | Per-agent override of the build context. **Repeatable**; combine a bare `DIR` (the default) with `NODE=DIR` overrides. Empty node or path is an error. |
| `--tag TAG` | Container image tag to build/push (defaults to `latest`). |
| `--min-autonomy GRADE` | Create-time trust floor. A `TrustGrade` name (`L0_SHADOW`, `L1_CANARY`, `L2_GUARDED`, `L3_AUTONOMOUS`) or an int `0`–`3`. A side-effecting agent whose declared `trust_seed` is below the floor is **ESCALATED** (held, not deployed); a cleared-but-`L0` grade deploys to a shadow endpoint. **Omit to disable the gate.** |
| `--require-approval` | Hold *every* side-effecting agent for explicit approval (ESCALATE, no create), regardless of `trust_seed`. Off by default. |

The `GRADE` parsing and gate semantics come from `TrustGrade` in [`build/trust.py`](../../src/concursus/build/trust.py); the full Trust Ladder is documented in the [deploy guide](deploying-to-agentcore.md).

```bash
concursus deploy *.agent.yaml --execute \
  --source-dir .  --source-dir summarizer=./svc \
  --tag v2 --min-autonomy L2_GUARDED
```

---

## `run`

```bash
concursus run MANIFEST.. [--dag ...] [--account ID] [--region R] \
    [--inputs JSON|@file] [--execute] \
    [--vault DIR] [--lean-form] [--memory-id ID] [--actor-id ID] \
    [--approve | --plan-approval] [--yes]
```

Assembles the plan and parses `--inputs`, then either prints the dispatch dry-run (default) or drives a live `Supervisor` (`--execute`).

**Dry run (default).** Explains the topological dispatch `--execute` would perform: the agent count and the note that one stable `runtimeSessionId` spans the run, the topological order, the run inputs JSON, and per node the `InvokeAgentRuntime` ARN + qualifier plus its input wiring (`input <name> <- <producer> <path>`, or `(source node) external run inputs` when unwired). No runtime invoked. Exit `0`.

**Execute (`--execute`).** Builds a [`Supervisor`](../reference/execute.md) over the frozen plan (see [`execute/supervisor.py`](../../src/concursus/execute/supervisor.py)) and calls `supervisor.run(inputs)`, printing the outputs JSON on success (exit `0`). On any AWS/runtime/schema failure it prints `FAIL  <error>` plus a best-effort `supervisor.summary_line()` to stderr (guarded so a summary error never masks the original) and returns `1`.

### `--inputs @file.json`

The `--inputs` value is either a **JSON object literal** or `@path` pointing to a JSON file (read UTF-8, `json.load`ed). It must resolve to a JSON object; a non-object raises an error. Omitting it yields `{}`.

```bash
concursus run *.agent.yaml --inputs '{"ticket_id":"T-42"}'   # literal
concursus run *.agent.yaml --inputs @inputs.json            # from a file
```

### Durable state: `--vault` and `--memory-id`

By default an `--execute` run uses the offline in-process store. Two opt-in backends make it durable and resumable (see [Durable Run State](durable-state.md) and [state reference](../reference/state.md)):

- **`--vault DIR`** — persists the run as round-trip-exact markdown notes under `DIR/runs/<session>/` (offline, resumable, no AWS) via `FileVaultStateStore` ([`state/filevault.py`](../../src/concursus/state/filevault.py)). After a successful execute run it also builds a derived SQLite run DB (`build_run_db`) and prints the persisted paths to stderr.
  - **`--lean-form`** — with `--vault`, emit the lean machine note form (node/attempt/status/consumes/payload) instead of the default authentic slipbox notes (lineage/building_block/Related-Notes + a `_run.md` entry point).
- **`--memory-id ID`** — back the run with an AgentCore `MemoryStateStore` ([`state/statestore.py`](../../src/concursus/state/statestore.py)); durable and resumable, sharing the supervisor's `runtimeSessionId`. Requires `--execute` + `boto3`.
  - **`--actor-id ID`** — scopes the Memory event stream (default `run`); used with `--memory-id`.

`--vault` takes precedence over `--memory-id` when both are given.

### The `--approve` gate

`--approve` (alias `--plan-approval`) turns on an opt-in, between-phases plan-approval gate that runs **strictly between assembly and `supervisor.run`** — before any billed `InvokeAgentRuntime`. It is off by default, so today's `run --execute` path is byte-for-byte unchanged. When on, it prints the frozen `plan.to_dict()` JSON, then:

- **`--yes`** → approved without prompting (scripted / non-interactive approval).
- **interactive TTY** → prompts `Approve this plan and invoke? [y/N]`; only `y`/`yes` approves.
- **non-interactive without `--yes`** → **aborts**, invoking nothing.

Aborting is a clean **exit `0`** (nothing was invoked), not an error. The gate is safe precisely because the plan is frozen — approving invokes it, aborting invokes nothing; there is no mid-flight re-plan (any "adjust" must route back through `OrchestrationAssembler.recompile`, never a live executor).

### `run` flags

| Flag | Description |
|---|---|
| `--inputs JSON\|@file` | Run inputs: a JSON object literal, or `@path` to a JSON file. Must be an object; default `{}`. |
| `--execute` | Actually invoke the live runtimes via `boto3` (otherwise a dry-run). |
| `--vault DIR` | Persist the run as durable markdown notes under `DIR/runs/<session>/` and build a derived SQLite run DB. |
| `--lean-form` | With `--vault`, emit the lean machine note form instead of the default slipbox notes. |
| `--memory-id ID` | Back the run with an AgentCore Memory `StateStore` (durable, resumable); requires `--execute` + `boto3`. |
| `--actor-id ID` | Actor id scoping the Memory event stream (default `run`); used with `--memory-id`. |
| `--approve` / `--plan-approval` | Preview the frozen plan and pause for confirmation before any billed invoke. Off by default. |
| `--yes` | Approve the `--approve` preview without prompting (non-interactive approval). |

```bash
concursus run *.agent.yaml \
  --inputs '@inputs.json' --execute \
  --vault ./slipbox --approve --yes
```

---

## Exit codes and OK/FAIL output

Every verb handler returns an `int` process exit code, which `main()` returns to the shell.

| Verb | Exit `0` | Exit `1` |
|---|---|---|
| `info` | Always. | — |
| `validate` | All manifests valid. | Any manifest failed (`FAIL  <path> -> <error>` on stderr). |
| `plan` | Plan printed. | Load / parse / assembly error (`FAIL  <error>` on stderr). |
| `deploy` (dry-run) | Always. | Setup error (`FAIL  <error>` on stderr). |
| `deploy --execute` | All nodes provisioned/reused. | Any node **ESCALATE**d or **FAILED**, or a broad provision error. |
| `run` (dry-run) | Always. | Setup error (`FAIL  <error>` on stderr). |
| `run --execute` | Run completed (outputs printed); or `--approve` **aborted** (nothing invoked). | Execution failure (`FAIL  <error>` + best-effort summary on stderr). |

Conventions: success lines print to **stdout** with a leading verb (`OK`, `REUSE`/`CREATED`/`UPDATED`, or the outputs JSON); failures and holds print to **stderr** with `FAIL`, `ESCALATE`, or `FAILED`. `--version`/`--help` and argparse parse errors raise `SystemExit` (handled by argparse, not the verb handlers).

---

## Copy-paste examples

```bash
# 1. Overview (no verb also prints this).
concursus info

# 2. Validate manifests before wiring a DAG.
concursus validate a.agent.yaml b.agent.yaml

# 3. Inspect the compiled plan as JSON (no AWS).
concursus plan *.agent.yaml --dag 'ingest->summarize' \
  --account 123456789012 --region us-east-1

# 4. Provision for real, with a per-agent build context and a trust floor.
concursus deploy *.agent.yaml --execute \
  --source-dir . --source-dir summarizer=./svc \
  --tag v2 --min-autonomy L2_GUARDED

# 5. Run live, durable to a file vault, gated by scripted approval.
concursus run *.agent.yaml --inputs '@inputs.json' --execute \
  --vault ./slipbox --approve --yes
```

---

## See also

- [Getting Started](../getting-started.md) — install, declare a team, compile and run (Python API + CLI).
- [Guide: Compiling & Running a Team](compiling-and-running.md) — the resolve → assemble → freeze → supervise pipeline the CLI drives.
- [Guide: Deploying to AWS Bedrock AgentCore](deploying-to-agentcore.md) — what `deploy --execute` actuates, the Trust Ladder, and the ledger.
- [Guide: Durable Run State](durable-state.md) — the `--vault` / `--memory-id` backends behind `run`.
- Reference: [assemble](../reference/assemble.md) · [build](../reference/build.md) · [execute](../reference/execute.md) · [state](../reference/state.md) · [core](../reference/core.md).
- Source: [`cli.py`](../../src/concursus/cli.py).
