# Session-End Knowledge Transfer

> **Status: opt-in, default-off connector.** The `slipbox_transfer` terminal node + its fail-closed
> acceptance gate, the episodic-log export to an external knowledge vault, the strictly-outer trigger
> that fires it, and the transfer-inclusive session rollup. Wire none of it and a run is
> `AgentDAG → assemble → frozen ProvisioningPlan → Supervisor.run`, **byte-for-byte unchanged**.

A Concursus run already writes slipbox-shaped notes to its run dir as it goes — one round-trip-exact
markdown note per record under `<vault>/runs/<session>/` (see *Durable run state* in the
[README](../../README.md)). But those notes are **episodic**: they live within the session and die with
the run dir. This connector is the egress — it flows a finished session's episodic memory *out* into
a **permanent** external Slipbox via a knowledge-consolidation sub-agent, on both request-completion
and termination, so what a run learned becomes durable, retrievable knowledge instead of being lost
on teardown.

Concursus is a **compiler, not a runtime governor.** Every piece of this connector is either a
**compile-time** manifest/DAG builder or a **strictly-outer** episode-boundary observer / next-boot
sweep — never an in-`Supervisor.run` mutation (identity invariants INV-1..INV-5). The transfer node
runs as a normal terminal node in the single forward pass; its trigger observes the frozen `decision`
event *between* episodes; its export reads the finished log and writes an external target. The whole
connector is opt-in: nothing here runs unless a caller explicitly builds the node, wires the trigger,
or calls an exporter.

Everything lives in `concursus.state.transfer` (import by that path); only the `EventSink` types
(`FanOutEventSink`, `NullEventSink`, `EventSink`) come from `concursus.governor`. Concursus never
imports the consolidation runtime — ingestion is an **injected** callable.

## At a glance

| Symbol | Role |
|---|---|
| `build_slipbox_transfer_manifest` / `wire_slipbox_transfer_terminal` / `slipbox_transfer_acceptance_fn` | Author the `slipbox_transfer` terminal node, wire it as the run's sole sink, and gate only that node. |
| `register_slipbox_foundry` | Register the consolidation sub-agent so `match_task("slipbox_transfer")` resolves it. |
| `export_run_log` / `distill_export` | Copy the run's episodic notes into the ingestion inbox; wire the cross-run precedent. |
| `TransferTriggerSink` / `FanOutEventSink` | Fire the export at the `synthesize` boundary; compose observers into the one `event_sink` slot. |
| `sweep_untransferred_runs` / `transfer_run` / `run_needs_transfer` / `mark_transferred` / `recover_trail_id` | The reaper/next-boot backstop + the idempotent digest primitive + its marker gate. |
| `session_overall_ok` / `transfer_node_ok` | The transfer-inclusive session verdict. |
| `SLIPBOX_TRANSFER_NODE` · `SLIPBOX_FOUNDRY_CAPABILITIES` · `CONSOLIDATOR_JOB_DICT_KEYS` · `CONSOLIDATOR_COMPLETE_STATE` | The connector's constants. |

> This is the parity-safe subset of a larger design — a consolidation-digestible digest bundle and an
> object-store push variant depend on subsystems not present here, so the export is the local-inbox
> raw-notes path.

## The transfer node + its fail-closed acceptance gate

```python
from concursus.state.transfer import (
    build_slipbox_transfer_manifest, wire_slipbox_transfer_terminal, slipbox_transfer_acceptance_fn,
)

# 1. author the manifest (one AgentCore hosting handle is required)
manifest = build_slipbox_transfer_manifest(agent_runtime_arn="arn:aws:bedrock-agentcore:...:runtime/slipbox")

# 2. wire it as the sole terminal sink over the run's current sinks
wire_slipbox_transfer_terminal(dag, manifest, producer_outputs={"analyze": "report"})

# 3. gate ONLY this node when you run
sup = Supervisor(plan, manifests, on_error="record",
                 check_acceptance=True, acceptance_fn=slipbox_transfer_acceptance_fn)
```

`build_slipbox_transfer_manifest(*, agent_runtime_arn=None, container_uri=None, role_arn=None, qualifier="DEFAULT", name=SLIPBOX_TRANSFER_NODE)` authors an MCP node whose `contract.outputs` **mirror the consolidation sub-agent's real job dict** (`job_id` / `state` / `result_path` / `last_error`) with per-field `acceptance` rules: `state` must be in `["complete"]` and `result_path` must be `non_empty`. That makes the transfer **mandatory and fail-closed** — the node cannot be reported green unless the job settled at terminal success with a committed archive path. The node is `side_effecting=True` (it writes an external vault), so it enters the Trust Ladder at `L0_SHADOW`. It is fail-closed on its hosting handle too: pass neither `agent_runtime_arn` nor `container_uri` and `validate()` raises `ManifestError` (never a fabricated ARN). The declared output keys are a **subset** of `CONSOLIDATOR_JOB_DICT_KEYS` so the contract can't drift from the real API.

`wire_slipbox_transfer_terminal(dag, manifest, *, producer_outputs=None, node=SLIPBOX_TRANSFER_NODE)` adds the node and, for each current sink, adds **both** the DAG edge `producer → node` **and** a `spec.depends_on` entry on a **distinct** per-producer input `from_<producer>` — both required by `check_alignment` (the edge gate and the single-writer gate). The per-producer inputs are declared on the **manifest** (`contract.inputs`), which is where `check_alignment` reads consumer inputs. `producer_outputs` maps each sink to the output field the edge reads (a bare DAG node carries no declared outputs); a producer absent from the map falls back to a conventional `result` field. After wiring, the transfer node is the sole sink.

`slipbox_transfer_acceptance_fn(node)` narrows the `Supervisor`'s own opt-in `check_acceptance` gate to just the transfer node — every other node runs unguarded. `check_acceptance=True` is the master switch; the predicate is inert without it. Use `on_error="record"` so a non-`complete` job is recorded-failed (and prunes only its subtree) rather than raising the whole run.

## Registering the consolidation sub-agent

```python
from concursus.state.transfer import register_slipbox_foundry

register_slipbox_foundry(registry, ledger, arn="arn:aws:bedrock-agentcore:...:runtime/slipbox")
# now registry.match_task("slipbox_transfer") resolves the standing sub-agent
```

`register_slipbox_foundry(registry, ledger, *, manifest=None, fingerprint="slipbox-foundry-dev", deployed_at="1970-01-01T00:00:00Z", arn=None)` makes two append-only writes: `registry.register_agent(manifest, capabilities=SLIPBOX_FOUNDRY_CAPABILITIES)` teaches the registry which tasks the *named* agent serves, and `ledger.record(name=manifest.name, …)` is the standing row that makes `match_task("slipbox_transfer")` resolve it. `SLIPBOX_FOUNDRY_CAPABILITIES` is `{"slipbox_transfer", "slipbox_foundry"}`. The `arn` propagates to `AgentVersion.arn` — the runtime handle the scheduler dispatches to. Capabilities key on the agent **name**, so the manifest `name` and the ledger `name` must be identical (both use `manifest.name`). The dev `ledger.record` path makes the agent dispatchable immediately; the `L0_SHADOW` seed governs *live* dispatch, not registry resolution.

## Exporting the episodic log

```python
from concursus.state.transfer import export_run_log

result = export_run_log(run_dir, inbox_dir, admit_fn=admit_bundle, trail_id="sess-42")
# {"members": [<paths>], "objective": "hive-session-sess-42", "admitted": <admit_bundle result>}
```

`export_run_log(run_dir, target_dir, *, admit_fn=None, objective=None, trail_id="run")` copies every **top-level** `*.md` note under `run_dir` into `target_dir`, byte-identical — the run's slipbox-form notes ARE the multi-member corpus the sub-agent admits as one objective. The glob is non-recursive, so the derived CQRS sidecar trees (`versions/`, `index/`) never leak into ingestion. `objective` defaults to `hive-session-<trail_id>`. When `admit_fn` is given it is called with the exported member paths *after* they land (admission requires real files in the inbox root); its return value is surfaced as `"admitted"`. When `admit_fn` is `None`, the export is a pure file-drop.

Ingestion is an **injected** `admit_fn(members, objective=…)` callable — Concursus never imports the consolidation runtime, so the import graph stays clean and the export is unit-testable offline. `distill_export(store, *, vault_path=None)` is the thin explicit caller for `distill_store`, folding the finished run into one precedent note under `<vault>/precedents/` (the planner's cross-run retrieval path).

**Re-export is idempotent.** The sub-agent's ingestion dedup key binds the source file's inode + mtime + size, so an unconditional overwrite would churn identity on every export and duplicate the digestion job. The export uses a skip-if-identical write that leaves an unchanged note's inode/mtime intact, so a re-export of the same run is seen as an unchanged source and dedups — at-least-once → exactly-once. INV-4 safe: it reads the finished log / on-disk notes and writes an external target, never re-putting a `Record`.

## Firing the transfer at session end

```python
from concursus.governor import GovernorLoop, FanOutEventSink
from concursus.state.transfer import TransferTriggerSink

trigger = TransferTriggerSink(run_dir, inbox_dir, admit_fn=admit_bundle, trail_id="sess-42")
loop = GovernorLoop(
    goal="triage-abuse-signal", manifests=manifests,
    event_sink=FanOutEventSink([trigger, my_own_observer]),   # one slot, many observers
)
```

`TransferTriggerSink(run_dir, target_dir, *, admit_fn=None, trail_id="run", date="")` is an opt-in `EventSink` that fires the export on the episode-boundary `decision` event whose `route == "synthesize"` — the frontier-exhaust / bounded-termination moment, the true end of the run. It reads `route` off the plain-dict event, **not** `episode_end.done`: a done round can still route back to the planner, so `synthesize` is the real end. It is observer-only (INV-3): it reads the frozen event VALUE and writes the external inbox + a run-dir marker, never touching ctx / plan / log. Its result is on `.last_result`, and any error is swallowed by the loop's emit guard.

`FanOutEventSink(sinks)` composes several observers into the loop's single `event_sink` slot — each child's `emit` is individually guarded, so one misbehaving child can't starve the others; an empty list (or `None`) is a no-op. Use it to run the transfer trigger alongside your own observer.

**Exactly-once** is marker-based. `transfer_run(run_dir, target_dir, *, admit_fn=None, trail_id="run", when="")` runs the export then writes a success marker; `run_needs_transfer(run_dir)` gates on it; `mark_transferred(run_dir, *, objective="", when="")` writes the sentinel `.slipbox_transferred` — deliberately **not** a `*.md` file, so the note globs and loaders never see it.

`sweep_untransferred_runs(runs_root, target_dir_for, *, admit_fn=None, trail_id_for=None, when="")` is the reaper / next-boot **backstop**. A graceful-`synthesize` miss or a hard micro-VM teardown can leave a durable run log with no transfer success, so the sweep transfers every run under `runs_root` still lacking a marker — at-least-once across the trigger, the reaper, and the next boot converges to exactly-once. It recovers each run's real `trail_id` via `recover_trail_id(run_dir)` — read from the first note's `lineage:` frontmatter, because the run dir is named `_slug(session_id)` while the `trail_id` is a *different* transform of `session_id` — so the backstop's `objective` matches the graceful trigger's and the sub-agent dedups the re-admission instead of creating a second bundle.

## The session rollup — not green unless the transfer ran

```python
from concursus.state.transfer import session_overall_ok

verdict = session_overall_ok(store)
# {"overall_ok": bool, "transfer_ok": bool, "transfer_present": bool,
#  "work_complete": bool, "completed": [...], "total_completed": int}
```

`transfer_node_ok(store, *, node=SLIPBOX_TRANSFER_NODE)` is `True` iff the transfer node is in `store.completed()` (so it passed the acceptance gate) **and** its recorded output carries `state == "complete"` with a non-empty `result_path` (a defense-in-depth re-check). `session_overall_ok(store, *, node=SLIPBOX_TRANSFER_NODE, plan_order=None)` computes the transfer-inclusive verdict: `overall_ok` is `transfer_ok and work_complete`. Pass `plan_order` (the frozen plan's node order — the store alone does not carry it) to *also* require every planned node completed.

Both are **pure reads** over a finished store, never a runtime gate. The default is **fail-closed**: a session with no transfer node — or one whose job dead-lettered (and so was gated out of `completed()`) — is `overall_ok=False`. This is the success criterion of the whole connector: **a session cannot report green unless the transfer ran and was accepted.**

## The identity guard

Nothing here erodes *compiler, not a runtime governor*:

- Authoring the node + registering the sub-agent are compile-time — a manifest, a DAG wiring, and a registry/ledger seed, all *strictly before* `assemble`. The acceptance gate rides the `Supervisor`'s own opt-in `check_acceptance` seam; the node runs as a normal terminal node in the single forward pass and the gate only *rejects* (records-failed), never mutates the plan.
- The export reads the append-only log / on-disk notes (lossy display copies, safe to read) and writes an **external** target; it never re-puts a `Record` (INV-4).
- The trigger observes the frozen `decision` VALUE *between* episodes; the reaper/next-boot sweep is a separate pass over durable logs (INV-3). The rollup is a pure read.

Wire none of it and the default `plan → deploy → run` is byte-for-byte unchanged.

## See also

- [Concursus README](../../README.md) — the product overview; *Durable run state* and *The governor* sections.
- [AgentCore-aligned durable placement](../agentcore_placement.md) — where the durable log lives under AgentCore hosting.
