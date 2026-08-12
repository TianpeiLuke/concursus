# Guide: Durable Run State

*The `StateStore` seam, three backends, replay-resume, and disposable projections over one append-only log.*

A Concursus run is a single forward pass over a frozen plan — but a pass can span a
microVM teardown, an operator interrupt, or a KTLO loop that never really ends. Durable
run state is what makes that pass **resumable**: every validated agent output lands in an
append-only log, and resume is *replay of that log*, never a re-plan mid-flight. This guide
covers the one seam the [Supervisor](../reference/execute.md) writes through, the three
backends behind it, how replay-resume works, and the discipline that keeps every index,
DB, and precedent a **disposable projection** over the single source of truth.

Remember the invariant: **Concursus is a compiler, not a runtime governor.** The state
tier only *records* and *replays* — it never re-authors a plan. Everything generative
happens strictly before `assemble`; the log is the ledger of a plan already frozen.

## The state tier at a glance

The `state` tier ([`src/concursus/state/`](../../src/concursus/state/)) is
one append-only log of validated outputs plus rebuildable views over it:

| Module | Role | Source of truth? |
|---|---|---|
| [`statestore.py`](../../src/concursus/state/statestore.py) | `Record`, the `StateStore` Protocol, `content_hash`, `InProcessStateStore`, `MemoryStateStore`; opt-in `coordination` notices + the `RunEvent` governor-episode contract | **yes** — the log |
| [`filevault.py`](../../src/concursus/state/filevault.py) | `FileVaultStateStore` (on-disk markdown notes) + `capture_*_note` renderers + the opt-in `versioned=` note timeline | **yes** — the notes |
| [`rungraph.py`](../../src/concursus/state/rungraph.py) | `RunGraph` — the data-dependency DAG (producer→consumer) | derived |
| [`runindex.py`](../../src/concursus/state/runindex.py) | `RunIndex` (execution tree + metadata index), `PrecedentIndex` | derived |
| [`rundb.py`](../../src/concursus/state/rundb.py) | `build_run_db`, `build_precedent_db` — disposable SQLite indexes; `get_run_snapshot` / `redact_snapshot` — at-rest reads | derived |
| [`distill.py`](../../src/concursus/state/distill.py) | `distill_run`, `render_precedent_hub` — post-run memory loop | derived |
| [`precedent.py`](../../src/concursus/state/precedent.py) | `PrecedentRetriever` — compile-time cross-run lookup | derived (read) |

Everything is pure-stdlib Python. `boto3` is imported lazily *only* inside
`MemoryStateStore`, so the core and the full test suite run with neither the `[agentcore]`
nor the `[reasoning]` extra installed.

## The `StateStore` Protocol — the single seam

The Supervisor never touches AWS, a filesystem, or a dict directly. It writes and resumes
through a four-method Protocol, so a run is portable across backends by swapping one object:

```python
from typing import List, Optional, Set
from concursus import StateStore  # a typing.Protocol

class StateStore(Protocol):
    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None: ...
    def get(self, node: str) -> dict: ...
    def completed(self) -> Set[str]: ...
    def records(self) -> List[Record]: ...
```

- **`put(node, output, *, meta=None)`** — admit one validated (or, via `meta`, `failed`)
  output. Every `put` auto-increments that node's `attempt`; a content-identical re-`put`
  is recorded as a **dedup no-op** (a `dedup` `record_type`), never an error.
- **`get(node)`** — the latest *validated* output for `node`; raises `KeyError` if absent.
- **`completed()`** — the validated frontier: nodes whose **latest** record is `validated`.
- **`records()`** — the full append-only log as a `List[Record]`.

### `content_hash` and `Record`

`content_hash(output: dict) -> str` is a SHA-256 hex digest of the canonical JSON
(`json.dumps(sort_keys=True)`) of an output — a stable content address, so identical
outputs hash identically regardless of key order. It drives dedup, memoization, and
staleness:

```python
from concursus import content_hash
content_hash({"b": 2, "a": 1}) == content_hash({"a": 1, "b": 2})  # True — canonical
```

Because `content_hash` canonicalizes through `json.dumps`, an agent output must be
**JSON-serializable** to be stored at all — a non-JSON output (a `set`, a bespoke object)
would fail the log write here. `Supervisor(check_acceptance=True)` now enforces this
platform-boundary contract legibly at dispatch (run before the declarative acceptance gate),
raising a `SchemaError` up front instead of letting the write crash later at `content_hash`.
This gate is **off by default** — with `check_acceptance=False` (the default) the store
behaves byte-for-byte as before.

A [`Record`](../../src/concursus/state/statestore.py) is the unit of the log: one
node output plus its slipbox metadata. Its key fields:

| Field | Meaning |
|---|---|
| `node` | the DAG node id (semantic id) |
| `output` | the verbatim agent output dict |
| `attempt` | 1-based retry sequence, auto-incremented per `put` |
| `status` | `validated` / `failed` / `superseded` |
| `record_type` | `agent_output` (default) / `dedup` / `checkpoint` / `coordination` (opt-in notice; see below) |
| `content_hash` | `content_hash(output)` |
| `consumes` | resolved `AgentRef` edges as `"producer:$.jsonpath"` strings |
| `seq` | store-assigned strict-monotonic sequence — the deterministic ordering tie-breaker (`None` for hand-built records) |
| `address` | Folgezettel materialized path (defaults to the node name; a retry/fan-out appends a `/` segment, e.g. `map/0`) |
| `epoch` | checkpoint-compaction window id (Memory backend) |

Two immutability rules follow from the append-only model, and both are load-bearing:

1. **A re-`put` is a new `Record`, never an edit.** Content-identical re-puts become dedup
   no-ops — never errors.
2. **`seq` (not `timestamp`) is the ordering tie-breaker.** `timestamp` is display-only when
   Memory-backed; concurrent branch/retry writes that share an AgentCore `eventTimestamp`
   resolve identically on every replay because `seq` breaks the tie.

An unknown `status` raises `StateStoreError`; an unknown `record_type` only widens-and-warns
(so a future record kind never hard-fails a run).

### Coordination notices — opt-in, append-only, never dispatched

Sometimes one node wants to leave a *note* for the rest of a run — "upstream is slow", "I saw
schema drift" — without producing an output or touching the plan. That is a **coordination
notice**: a plain `Record` on the same append-only log (the sole source of truth), added by a
free function, never a fifth `StateStore` method and never mutable state. The whole facility is
**opt-in** — a run that never calls it has a byte-for-byte unchanged log — and it cannot perturb
the executed prefix a recompile pins:

```python
from concursus.state.statestore import (
    append_coordination_notice, list_pending_notices,
)

append_coordination_notice(store, "fetch", {"note": "slow upstream"})
# later — a pure staleness read; a notice about a finished node is dropped:
pending = list_pending_notices(store.records(), terminal_nodes=store.completed())
```

- **`append_coordination_notice(store, node, payload)`** appends one `Record` with
  `record_type="coordination"`, a **non-`validated`** status, keyed under a dedicated sentinel
  log node (`__coordination__`), *not* under `node`. Keying it under the sentinel (rather than
  the referenced `node`) is deliberate: it means a notice can never flip that node's
  latest-overall record and so can never contaminate `completed()`, `get()`, or the validated
  projection. The referenced `node` rides in the record's `producer` field and the payload.
- **`list_pending_notices(records, terminal_nodes)`** is a **pure reader**: it selects the
  `coordination` records and drops any whose referenced node is already terminal (e.g. present in
  `store.completed()`), returning the rest in append order. It mutates nothing and marks nothing
  consumed — "marking a notice consumed" is itself just appending a follow-up notice, so there is
  deliberately no mutable consume flag.

Because a notice is non-`validated` and lives under the sentinel node, `completed()` / `get()` /
the projection are **unaffected** — this stays consistent with the compiler framing: the notice
is a ledger entry about a plan already frozen, never an instruction to re-author one.

### `RunEvent` — the typed governor-episode boundary contract

`statestore.py` also defines the frozen typed shape of the events the
[governor loop](../reference/governor.md)'s **opt-in** `EventSink` emits at each episode boundary.
`RunEvent` is a `TypedDict` (`total=False`): the boundary-invariant keys (`type` / `run_id` / `round` /
`completed` / `frontier`) always ride, and per-kind extras (`done` / `progressed` on
`episode_end`, `route` / `terminated_by` on `decision`) are added only where meaningful. The
`type` is one of the **closed** `RunEventKind` vocabulary — `episode_start`, `episode_end`,
`decision` (also exposed as the bare-string set `RUN_EVENT_KINDS`):

```python
from concursus import RUN_EVENT_KINDS, check_run_event_alignment, RunEventContractError

sorted(RUN_EVENT_KINDS)                 # ['decision', 'episode_end', 'episode_start']
check_run_event_alignment(some_kinds)   # raises RunEventContractError on an out-of-vocab kind
```

Two properties keep the compiler framing intact. A `RunEvent` is a **plain-dict value** — never a
live ctx/plan handle — so an observer can never reach inside a running `Supervisor` or mutate a
frozen plan; it only *reports* boundaries. And `check_run_event_alignment` is a **build-time drift
guard** (mirroring the compiler's `check_alignment` for manifest edges): if an emitter ever emits a
kind the readers don't know, it raises `RunEventContractError` at test/build time rather than
letting a stray event reach a reader at runtime. The whole facility is emitted only through the
governor's opt-in `EventSink`, so a run with no sink attached is byte-for-byte unchanged.

## The three backends — and when to use each

### `InProcessStateStore` — the offline default

Zero-dependency, in-memory, and the Supervisor's default when you pass no `state_store`. It
holds the append-only `list[Record]` (source of truth) plus a `{node: latest validated
output}` projection; nothing touches AWS. Use it for local runs, tests, and CI.

```python
from concursus import InProcessStateStore

s = InProcessStateStore()
s.put("fetch", {"ok": True})
s.get("fetch")        # {'ok': True}
s.put("fetch", {"ok": True})  # content-identical → dedup no-op, attempt 2
s.completed()         # {'fetch'}
```

All methods are guarded by a reentrant `threading.RLock`.

### `MemoryStateStore` — AgentCore Memory, resume by replay

The durable, resumable backend for a run hosted on AWS Bedrock AgentCore. Each `put`
appends **one Blob event** plus string metadata to AgentCore Memory; the event log is the
single source of truth and a cached projection is (re)built by replay. Resume across a
microVM teardown is exactly a replay over the same `(memory_id, actor_id, session_id)` — a
node already in `completed()` is skipped, so a re-run picks up where it left off.

```python
from concursus import MemoryStateStore

m = MemoryStateStore(memory_id="m-123", session_id="sess", actor_id="run")
m.put("triage", {"root_cause": "disk-full"})
m.get("triage")   # {'root_cause': 'disk-full'} — lazily replays once, then reads cache
```

`client` defaults to a lazily-constructed `boto3` `bedrock-agentcore` data-plane client
(and raises `RuntimeError` if `boto3` is missing and no client is injected — install the
`[agentcore]` extra or pass `client=`). Blob (not Conversational) events are deliberate, to
avoid AgentCore's long-term extraction. Because AgentCore sanitizes metadata charsets, the
lossless copy of a record's fields rides in a Blob `__meta__` sidecar.

**Checkpoint-compaction warm resume + epoch tagging (opt-in).** A long-running or standing
loop's log grows without bound; two extra methods keep warm resume cheap:

```python
def checkpoint(self) -> Optional[str]:   # C-4 compaction
def replay(self, *, force_full: bool = False) -> None:
```

- `checkpoint()` writes **one** `CHECKPOINT` event carrying the compacted latest-per-node
  snapshot of the current *epoch*, then rotates `epoch += 1`. It returns the checkpoint
  event id (or `None` if there is nothing to compact). This is a **single-writer-per-session**
  contract — the one synchronous writer calls it. Raw events are **never deleted**; the
  checkpoint is a *derived* snapshot that only makes a later warm replay cheaper.
- `replay()` rebuilds the caches. On the **warm path** (a checkpoint exists, `force_full=False`)
  it re-hydrates the latest checkpoint's compacted snapshot, then folds only the open-epoch
  tail — `O(events-since-the-last-checkpoint)`, not `O(whole log)`, expressed as a bounded
  `epoch=<n>` `EQUALS_TO` query. On the **cold path** (no checkpoint, or `force_full=True`)
  it paginates the whole session. Warm-path anomalies fall back to a full rebuild, so the
  fast path can never disagree with a cold replay.

```python
m.checkpoint()          # compact the closed epoch, rotate forward
m.replay()              # warm resume: snapshot + open-epoch tail only
```

> **Gotcha:** after a *warm* resume, `records()` returns the compacted latest-per-node
> records plus the open-window tail — **not** every historical attempt. Pass
> `replay(force_full=True)` if you need the full attempt history. `completed()` and `get()`
> are unaffected (always latest-per-node). The [governor loop](../reference/governor.md)
> automates this cadence via `GovernorLoop(checkpoint_every=N)`.

### `FileVaultStateStore` — durable on-disk markdown notes

The third backend persists **one round-trip-exact markdown note per record** under
`<vault>/runs/<session>/`, and resumes by reloading those notes. It mirrors
`InProcessStateStore`'s `put` semantics (append log + projection, attempt auto-increment,
content-hash dedup) but survives a process exit with no AWS dependency. Bind a run with
`from_config`:

```python
from concursus import FileVaultStateStore

v = FileVaultStateStore.from_config(vault_path="/vault", session_id="TKT-42")
v.put("triage", {"root_cause": "x"})   # writes a slipbox note + regenerates _run.md
v.get("triage")                        # {'root_cause': 'x'}
# a fresh store over the same vault reloads from disk on the first read (resume = reload):
FileVaultStateStore.from_config(vault_path="/vault", session_id="TKT-42").get("triage")
```

`from_config` binds `run_dir = <vault_path>/runs/<slug(session_id)>/` and derives
`trail_id` from the session — the family key for cross-run precedent, exposed as the
read-only `run_dir` and `trail_id` properties. Each note carries the **authoritative
payload as an embedded base64-JSON blob** (byte-identical to `MemoryStateStore`'s Blob) plus
a `meta:` blob; the frontmatter, H1, and body are *lossy* display copies the loader never
reads — so an arbitrary output dict round-trips exactly. File and AgentCore backends differ
only in transport: both marshal through the same `statestore` helpers, so they never drift.

Two knobs worth knowing:

- `slipbox_form=True` (**the default here**) emits slipbox-conformant notes and
  regenerates `_run.md`; `slipbox_form=False` emits the lean machine schema (and does *not*
  regenerate `_run.md`).
- Records written by this backend do **not** carry `seq`; on reload they are ordered by
  `timestamp` (the monotonic write clock), so last-write-wins depends on `timestamp` here.
- `versioned=False` (**the default**) — opt in with `versioned=True` for the append-only note
  version timeline described below. Left off, no `versions/` dir is ever created and the store's
  on-disk bytes are identical to before.

Writes are atomic (temp file + `os.replace`); concurrent writers over one vault are
serialized by an `RLock` plus an advisory `fcntl` lock and a generation-token OCC over
`.gen` (degrading to `RLock`-only on non-POSIX).

#### The `capture_*` note renderers

`filevault.py` also ships the write-time note renderers. They are **pure projections** over
already-frozen state — none influences dispatch order:

| Function | Renders | Returns |
|---|---|---|
| `capture_run_plan_note(plan, run_dir, ...)` | a compiled `ProvisioningPlan` as a durable `_plan.md` (a Mermaid DAG + `plan.to_summary_dict()`) | the note **path** — and it **writes** the file |
| `capture_agent_response_note(record, ...)` | one agent-response `Record` as a round-trip-exact note | the note **text** |
| `capture_agent_log_note(record, ...)` | a raw agent log — **only when `status=='failed'`** | note text, or **`None`** for any non-failed record |
| `capture_run_output_note(record, ...)` | dispatches a record to the renderer for its `record_type` (all types route to the response renderer) | the note text |

> **Gotcha:** `capture_run_plan_note` is the odd one out — it *writes* a file and returns a
> *path*, while the others return note *text*. And `capture_agent_log_note` returns
> `Optional[str]`: the only promotion trigger for a log is **failure**, so callers must
> handle `None`. `_plan.md` and `_run.md` are navigation notes, not run records — the record
> loaders skip them, so they never corrupt a resume.

#### Opt-in note version timeline + revert

Some notes get **re-written** over a run: `_run.md` is regenerated on every `put`, and a post-run
pass can amend a producer note. By default the prior bytes are simply overwritten — there is no
history. Passing `FileVaultStateStore(versioned=True)` (or `from_config(..., versioned=True)`)
turns on an **append-only version timeline**: each *distinct* content of a note is snapshotted
into a `versions/` sidecar tree at `<run_dir>/versions/<note_stem>/vNNN.md` (newest = highest N),
carrying typed provenance frontmatter (`version` / `when` / `content_hash` / `source_note`, plus
`reverted_from` for a revert) and embedding the full note text as an authoritative `b64:` blob so
it round-trips byte-exact.

This is **opt-in and default-off** — with `versioned=False` (the default) no `versions/` dir is
ever created and the single-write path is **byte-for-byte unchanged**. The timeline also can
never leak into a resume: every version note is stamped `concursus_note_kind: note_version` so the
record parser refuses it, and because the record loaders glob `*.md` **non-recursively** they
never descend into `versions/` at all.

```python
from concursus import FileVaultStateStore
from concursus.state.filevault import read_note_versions, revert_note

v = FileVaultStateStore.from_config(vault_path="/vault", session_id="TKT-42", versioned=True)
v.put("triage", {"root_cause": "disk-full"})
v.put("triage", {"root_cause": "quota-exceeded"})   # _run.md changed → a new snapshot

read_note_versions(v.run_dir, "_run.md")   # [{version:1,...}, {version:2,...}] oldest→newest
revert_note(v.run_dir, "_run.md", 1)        # revert = a FORWARD step (see below)
```

Three free functions drive it (all in `state.filevault`, all pure/offline — no AWS):

- **`append_note_version(run_dir, note_name, content, *, when="", reverted_from=None, force=False)`**
  writes the next `vNNN.md`. It is **content-deduplicated**: a re-write whose content matches the
  current head is a no-op that returns `None`, so only a genuinely changed note grows its timeline
  (`force=True` appends anyway — used by revert). The store calls this internally on each write
  only when `versioned=True`.
- **`revert_note(run_dir, note_name, version, *, when="", restore_live=True)`** reverts to a prior
  `version` by writing that version's content **forward** as a *new* latest version (stamped
  `reverted_from=version`) and, by default, restoring it to the live note file. History is never
  rewritten — the state you reverted *away from* stays in the timeline — so a revert is a forward
  append, consistent with the append-only model. It raises `ValueError` if `version` is not in the
  timeline.
- **`read_note_versions(run_dir, note_name)`** returns every snapshot for one note, oldest→newest
  (empty when the run was never versioned).

The derived `run.sqlite` (see below) surfaces this as a `note_versions` table — DROP+recreated on
every build, empty for an unversioned run, so the default DB shape is unchanged.

## Single source of truth, disposable projections

The load-bearing discipline: **the log (or the on-disk notes) is the one source of truth;
every graph, index, DB, hub, and precedent is a rebuildable projection over it.** Delete any
projection and you lose nothing — rebuild regenerates it byte-for-byte. This is what keeps
"resume = replay" honest: there is exactly one thing to replay.

- **`RunGraph`** — projects records into the **data-dependency DAG** (producer→consumer via
  `consumes` `AgentRef` edges). Answers transitive `upstream`/`downstream` blast-radius and
  provides a pre-dispatch structural `validate()` (rejects a cycle or a dangling `AgentRef`).
  The Supervisor uses it for both its one-time plan-structure gate and `context()`.

  ```python
  from concursus import RunGraph
  g = RunGraph.from_records(store.records())
  g.validate()                # raises RunGraphError on a cycle / dangling edge
  g.downstream("fetch")       # blast radius that must re-run when 'fetch' changes
  ```

- **`RunIndex`** — the orthogonal **execution tree**: a metadata inverted index
  (`node`/`status`/`record_type`/`schema`/`producer`) for lookup-not-scan queries, plus
  Folgezettel materialized-path traversal (`ancestors`/`children`/`descendants`) over each
  record's `address`. `PrecedentIndex` is its cross-run analogue keyed by `trail_id`.

  ```python
  from concursus import RunIndex
  idx = RunIndex.from_store(store)
  idx.query(status="failed")  # inverted-index lookup, no payload scan
  idx.latest("triage")        # newest validated record for 'triage'
  ```

- **`build_run_db`** — materializes a disposable, gitignored **SQLite** index over a
  persisted run's notes (default `<run_dir>/index/run.sqlite`). Reads only the notes (the
  source of truth); deleting the DB loses nothing. Its latest-validated projection VIEW is
  implemented with a **portable correlated `NOT EXISTS`** (no window functions), so it runs
  on older SQLite (< 3.25). `build_precedent_db` is the cross-run analogue.

  ```python
  from concursus import build_run_db
  build_run_db("/vault/runs/tkt_42")            # → '/vault/runs/tkt_42/index/run.sqlite'
  build_run_db("/vault/runs/tkt_42", incremental=False)  # full DROP + recreate
  ```

- **`get_run_snapshot` + `redact_snapshot`** — the **at-rest, cross-process read** over a stopped
  run's notes (no live plan, no SQLite build). `get_run_snapshot(run_dir, *, agent=None,
  step=None)` re-derives records from the note SSOT via `load_records`, assigns each a 1-based
  `step` ordinal over the *whole* run (the canonical deterministic at-rest order — address then
  attempt, not an execution clock), then optionally narrows to one `agent`/node and/or a `step`
  window. It returns a plain JSON-serializable dict
  (`{"run_id", "agent", "step", "total", "count", "records": [...]}`) and is a **pure read
  projection**: it opens no live plan, drives no dispatch, and mutates nothing. An absent/empty run
  dir yields a well-formed empty snapshot. `redact_snapshot(snapshot, pattern)` is an **optional**
  egress guard — a deep copy with every `pattern` match masked as `[REDACTED]` (logging a WARN with
  the count when it masks anything); it is off unless a caller wraps the output.

  ```python
  from concursus.state.rundb import get_run_snapshot, redact_snapshot
  snap = get_run_snapshot("/vault/runs/tkt_42", step=(2, 4))   # inclusive window; agent=None → all
  safe = redact_snapshot(snap, r"\d{3}-\d{2}-\d{4}")           # mask before egress (opt-in)
  ```

  `step` accepts `None` (every step), an `int` (that single step), or a `(lo, hi)` pair (inclusive,
  either bound `None` for open-ended); a malformed window raises `ValueError`.

- **`distill_run` + the precedent store** — the offline post-run memory loop. `distill_run`
  folds one finished run's `{node: output}` + consumes graph + outcome into a single compact
  precedent note under `<vault>/precedents/` (a sibling of `runs/`, so a precedent is never
  globbed back as run state); `render_precedent_hub` renders the cross-run hub. Both are
  **pure post-run** — they run only after `Supervisor.run` returns and never feed back into a
  running plan.

  ```python
  from concursus import distill_run, render_precedent_hub
  distill_run(result, store.records(), vault_path="/vault", trail_id="tkt_42")
  render_precedent_hub("/vault")                # → '/vault/precedents/_index.md'
  ```

- **`PrecedentRetriever`** — the **compile-time read half**. It reads the durable precedent
  store and ranks prior resolved runs for a query through a StructuredKey → Lexical(BM25) →
  optional-Dense ladder, feeding the plan-author context *before* a plan is frozen. It never
  mutates a plan, starts a run, or reads a run log — see the
  [Reasoning guide](reasoning.md) for how precedent reuse threads into deliberation.

  ```python
  from concursus import PrecedentRetriever
  r = PrecedentRetriever("/vault")
  r.retrieve("disk full on host", nodes=["triage", "remediate"])
  r.retrieve(key="tkt_42")   # exact structured match → single precedent at score 1.0
  ```

## Wiring a backend into the Supervisor

Pass any backend as `state_store=` to `Supervisor`; omit it and you get an
`InProcessStateStore`. The rest of the run is identical — the seam is the whole point:

```python
from concursus import Supervisor, FileVaultStateStore

store = FileVaultStateStore.from_config(vault_path="/vault", session_id="TKT-42")
sup = Supervisor(plan, manifests, state_store=store)
result = sup.run(inputs)          # {node: output} for every completed node
```

Swap in `MemoryStateStore(memory_id=..., session_id=..., actor_id=...)` for a durable
AgentCore-backed run; re-running the same Supervisor against the same durable store resumes
by replay (completed nodes are skipped). See
[Compiling & Running](compiling-and-running.md) for the full compile pipeline and
[Deploying to AgentCore](deploying-to-agentcore.md) for the live-runtime path.

The Supervisor also exposes graph-aware upstream context read straight off the store's
recorded `consumes` edges:

```python
sup.context("summarize")   # {producer: latest output} — nearest-first, bounded
```

`context(node)` rebuilds a `RunGraph` from `store.records()` and returns the latest
validated output of every node in its bounded `context_order` — shared upstream state as a
query, not point-to-point wiring.

## See also

- [API Reference: state](../reference/state.md) — every symbol in the state tier.
- [API Reference: execute](../reference/execute.md) — the `Supervisor` that writes the log.
- [Guide: The Reasoning Tier](reasoning.md) — precedent reuse feeding the plan author.
- [Guide: The Governor](governor.md) — the standing loop that drives `checkpoint_every`.
- [AI-19 — AgentCore-aligned durable placement](../agentcore_placement.md) — where the
  durable log should live on AWS Bedrock AgentCore.
- [Core Concepts](../concepts.md) and the [documentation index](../README.md).
