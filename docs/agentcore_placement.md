# AI-19 — AgentCore-aligned durable placement (design note)

> **Status: design deliverable, not runtime code.** This note records *where* Concursus's durable
> state should live when a run is hosted on AWS Bedrock AgentCore, and the concrete alignment
> checklist for that hosting. It introduces **no** `boto3` and **no** new code path — the durable
> vault stays **external and opt-in behind the `StateStore` seam** (`concursus.statestore.StateStore`).
> Nothing here changes a default run: with neither `--vault` nor `--memory-id`, the supervisor
> keeps its offline `InProcessStateStore` and touches no AWS.

## The seam this respects

Concursus is a compiler: `AgentDAG -> assemble -> frozen ProvisioningPlan -> Supervisor.run` as a
static topological walk, and **resume is replay**. The supervisor writes run state *only* through
the `StateStore` Protocol (`put` / `get` / `completed` / `records`). Two backends exist today —
the offline `InProcessStateStore` (default) and the AgentCore `MemoryStateStore` (opt-in) — plus
the on-disk `FileVaultStateStore`. This note is about the *durable* option and how it maps onto an
AgentCore Runtime; it is deliberately expressed as placement + a checklist so it can be adopted
without adding a hard AWS dependency to the core.

## Option 1 (recommended): AgentCore Memory as the canonical log + a derived on-disk vault

Two tiers, with a strict source-of-truth / derived-projection discipline (the same
single-source-of-truth + rebuildable-projection rule the `StateStore` already follows):

1. **Canonical append-only log — AgentCore Memory.** Every validated (or failed) node output is
   one `create_event` **Blob** event, session-scoped to the run. This *is* the source of truth;
   `MemoryStateStore.replay()` paginates `list_events` to rebuild the projection, so a run
   survives micro-VM teardown and resumes by replaying its own event log. Blob (not Conversational)
   is deliberate — Conversational payloads trigger long-term extraction, which we do not want for
   verbatim run state.

2. **Derived on-disk vault — a `FileVault` (notes + `rundb`) on a BYO EFS mount.** A read-model
   projection of the log: round-trip-exact markdown notes under `runs/<session>/` plus a derived,
   disposable SQLite `rundb`. It is **derived and rebuildable** — deleting it loses nothing that
   the canonical Memory log cannot reproduce. It exists for grep/index/human-navigability, not as
   truth. The EFS mount is **declared via `filesystemConfigurations` on `CreateAgentRuntime`** (a
   BYO file system the runtime mounts), so the vault is durable across sessions and micro-VM
   recycling without the agent code owning a disk lifecycle.

Because the vault is a *derived* projection reached only through the `StateStore` seam, it stays
**external and opt-in**: the core never requires EFS or Memory; a caller wires them in exactly as
`concursus run --vault` / `--memory-id` do today.

### Why not "vault as canonical"?

Making the on-disk vault the source of truth would couple the compiler's identity to a specific
disk/filesystem and re-introduce the durability problem AgentCore Memory already solves (a
micro-VM can vanish mid-run). Keeping Memory canonical and the vault derived means resume =
`replay()` off the durable log, and the vault can always be rebuilt.

## Alignment checklist (AgentCore hosting)

- **Session-scoped writes keyed to `runtimeSessionId`.** One stable `runtimeSessionId` spans every
  invoke in a run (the supervisor already mints and threads it). Memory events are written under
  that session id (`sessionId` + a stable `actorId`), so the log for one run is exactly one session
  and `replay()` reconstructs precisely that run's `completed()` / `get()`. Do **not** share a
  session across independent runs.
- **`networkMode: VPC` + mount IAM + TCP 2049.** An EFS-mounted vault requires the runtime in
  `VPC` network mode (`networkConfiguration.networkMode = VPC` with subnets + security groups),
  the execution role granted `elasticfilesystem` mount/describe permissions on the target file
  system, and the security group allowing **NFS over TCP 2049** to the EFS mount targets. Without
  all three the mount silently fails and the derived vault never materializes.
- **`HealthyBusy` / `add_async_task` for long index rebuilds.** A full `rundb` / index rebuild can
  outlast an invoke. Run it as a background task (`add_async_task`) and report `HealthyBusy` on the
  `/ping` health check while it proceeds, so AgentCore does not consider the runtime idle-and-done
  (or unhealthy) during a long derived-projection rebuild. The rebuild is a *derived* step — it
  must never block or gate dispatch.
- **EFS advisory locks.** Concurrent writers to the shared vault (e.g. a future concurrent-dispatch
  supervisor, or two sessions pointed at one mount) coordinate with **advisory file locks**
  (`flock`/`fcntl`) around the note-write + rundb-update critical section, matching
  `FileVaultStateStore`'s fcntl+OCC discipline. Advisory (not mandatory) locking is what EFS
  supports; every writer must cooperate.

## Non-goals / guardrails

- No `boto3`, no live AWS call, and no new dispatch behavior is introduced by this note.
- The vault never influences dispatch order and makes no runtime decision — it is a write-time,
  read-only projection reached only through the `StateStore` seam.
- The durable tiers are **opt-in**; the default run is byte-for-byte unchanged (offline
  `InProcessStateStore`).
