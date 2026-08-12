# API Reference: `state`

*`StateStore` backends and the disposable projections over the append-only run log.*

The `state` tier is `concursus`'s durable run-state substrate: **one append-only log of validated agent outputs**, plus a family of **rebuildable projections and indexes** over it (the single-source-of-truth / derived-DB discipline). The `Record` log is canonical; every graph, tree, SQLite database, and precedent hub built from it is a deletable derivation — regenerate and you lose nothing.

Nine modules cohere around that log:

| Module | Source | Owns |
|---|---|---|
| `state.statestore` | [`../../src/concursus/state/statestore.py`](../../src/concursus/state/statestore.py) | The `Record` model, the `StateStore` Protocol, content-hashing, the event↔record marshalling seam, two backends (`InProcessStateStore`, `MemoryStateStore`), the opt-in typed `RunEvent` contract, and opt-in append-only coordination notices. |
| `state.filevault` | [`../../src/concursus/state/filevault.py`](../../src/concursus/state/filevault.py) | `FileVaultStateStore` — one round-trip-exact markdown note per record on disk — plus the write-time note renderers (`capture_*_note`), the payload-note writer, the reciprocal-backlink post-pass, and the opt-in append-only note version timeline (`revert_note`, default off). |
| `state.rungraph` | [`../../src/concursus/state/rungraph.py`](../../src/concursus/state/rungraph.py) | `RunGraph` — the producer→consumer **data-dependency DAG**: upstream/downstream blast radius and a structural gate. |
| `state.runindex` | [`../../src/concursus/state/runindex.py`](../../src/concursus/state/runindex.py) | `RunIndex` — the **execution tree** (Folgezettel materialized-path traversal + a metadata inverted index) — and `PrecedentIndex` (cross-run). |
| `state.rundb` | [`../../src/concursus/state/rundb.py`](../../src/concursus/state/rundb.py) | Disposable, rebuildable **SQLite** indexes over a run's notes (`build_run_db`) and over the cross-run precedent store (`build_precedent_db`). |
| `state.distill` | [`../../src/concursus/state/distill.py`](../../src/concursus/state/distill.py) | The **offline memory loop**: fold a finished run into one precedent note (`distill_run`) and render the cross-run hub (`render_precedent_hub`). |
| `state.precedent` | [`../../src/concursus/state/precedent.py`](../../src/concursus/state/precedent.py) | `PrecedentRetriever` — the compile-time StructuredKey→Lexical→Dense retrieval ladder over the durable precedent store. |
| `state.capture` | [`../../src/concursus/state/capture.py`](../../src/concursus/state/capture.py) | `CaptureEnvelope` + a source-agnostic dispatcher (`capture` / `capture_run`) over the shipped FileVault writers, plus the read-only run-dir gate and the payload-tier read-back — the post-run capture seam, **not** a runtime. |
| `state.transfer` | [`../../src/concursus/state/transfer.py`](../../src/concursus/state/transfer.py) | *(opt-in)* The **session-end knowledge-transfer connector** — the `slipbox_transfer` terminal node + fail-closed acceptance contract (C1), the consolidation-sub-agent registration (C4), the episodic-log export to the sub-agent's ingestion (C2), the strictly-outer `synthesize` trigger + reaper/next-boot backstop (C3), and the transfer-inclusive session rollup. |

Everything here is pure-stdlib Python; `boto3` is imported lazily only inside `MemoryStateStore` (the optional `[agentcore]` extra), and the pure core plus the full test suite run with it uninstalled.

> Reminder: **Concursus is a compiler, not a runtime governor.** The `state` tier is the durable substrate of a single forward pass: `put` appends to an append-only log during `Supervisor.run`, and resume is *replay* of that log — never a re-plan mid-flight. Every projection here (`RunGraph`, `RunIndex`, the SQLite DBs, the precedent hub) is a disposable derivation. The two memory-loop halves run strictly *outside* a run — `distill_*` **after** `Supervisor.run` returns, `PrecedentRetriever` **before** a plan is frozen — and never feed back into a running plan.

Most public symbols are re-exported from the package root:

```python
from concursus import (
    StateStore, InProcessStateStore, MemoryStateStore, FileVaultStateStore,
    Record, content_hash,
    RunEvent, RunEventKind, RUN_EVENT_KINDS, RunEventContractError, check_run_event_alignment,
    capture_run_plan_note, capture_agent_response_note,
    capture_agent_log_note, capture_run_output_note,
    RunGraph, RunGraphError, RunIndex, PrecedentIndex,
    build_run_db, build_precedent_db,
    distill_run, distill_store, build_precedent_payload,
    render_precedent_hub, load_precedents,
    PrecedentRetriever, RetrievedPrecedent,
)
```

> The typed `RunEvent` contract (`RunEvent` / `RunEventKind` / `RUN_EVENT_KINDS` / `RunEventContractError` / `check_run_event_alignment`) is one of the opt-in, default-off additions (the flexibility & robustness layer completed in 0.6.0). It ships in `state.statestore` (the closed vocabulary the governor's opt-in `EventSink` emitter and its readers share); nothing in a default `Supervisor.run` emits an event, so a run with no `EventSink` wired in is byte-for-byte unchanged.

A few symbols are public but **not** re-exported at the root — import them from their module:

```python
from concursus.state.statestore import (
    StateStoreError, RecordStatus, RecordType,
    append_coordination_notice, list_pending_notices,   # opt-in
)
from concursus.state.runindex import RunIndexError, address_of, INDEXED_FIELDS
from concursus.state.rungraph import Edge
from concursus.state.distill import precedents_dir
from concursus.state.capture import (
    CaptureEnvelope, CaptureError,
    capture, capture_run, adapt_plan, adapt_payload,
    gate_run_dir, load_payload_tiers,
)
from concursus.state.filevault import (
    capture_payload_note, redact, add_reciprocal_backlinks,
    append_note_version, read_note_versions, revert_note, iter_note_versions,   # opt-in
)
from concursus.state.rundb import get_run_snapshot, redact_snapshot
```

For the narrative — the `StateStore` seam, the three backends, replay-resume, and the disposable projections — see [Guide: Durable Run State](../guides/durable-state.md).

---

## `state.statestore`

Source: [`../../src/concursus/state/statestore.py`](../../src/concursus/state/statestore.py)

The foundation: run state is a single append-only log of validated (or failed) node outputs plus a rebuildable `{node: latest validated output}` projection. Two backends share one `StateStore` Protocol — `InProcessStateStore` (the zero-dependency, offline default) and `MemoryStateStore` (opt-in, AgentCore Memory-backed, resumable by replay). `FileVaultStateStore` (below) is the third, reusing this module's marshalling seam so the file and Memory backends never drift.

| Symbol | Kind | Summary |
|---|---|---|
| [`StateStoreError`](#statestoreerror) | exception | A `Record` field carries a value outside its typed vocabulary (e.g. an unknown status). |
| [`RecordStatus`](#recordstatus--recordtype) | enum | `validated` \| `failed` \| `superseded` (a `str` subclass). |
| [`RecordType`](#recordstatus--recordtype) | enum | `agent_output` \| `dedup` \| `checkpoint` \| `coordination` (a `str` subclass; unknown values widen-and-warn). |
| [`content_hash`](#content_hash) | function | SHA-256 of the canonical JSON of an output — a stable content address. |
| [`Record`](#record) | dataclass | One node output plus its slipbox metadata — the unit of the log. |
| [`StateStore`](#statestore) | protocol | The 4-method durable run-state seam (`put` / `get` / `completed` / `records`). |
| [`InProcessStateStore`](#inprocessstatestore) | class | Offline, in-memory default backend. |
| [`MemoryStateStore`](#memorystatestore) | class | AgentCore Memory-backed backend (durable, resumable, checkpoint-compacting). |
| [`RunEventKind`](#the-run-event-contract-opt-in) | enum | *(opt-in)* The closed vocabulary of governor episode-boundary event kinds — `episode_start` \| `episode_end` \| `decision` (a `str` subclass). |
| [`RUN_EVENT_KINDS`](#the-run-event-contract-opt-in) | constant | *(opt-in)* The `RunEventKind` members as a bare-string `frozenset` — a membership check that never needs the enum type in hand. |
| [`RunEvent`](#the-run-event-contract-opt-in) | TypedDict | *(opt-in)* The frozen typed shape of one episode-boundary event (`total=False`; a plain-dict value, never a live handle). |
| [`RunEventContractError`](#the-run-event-contract-opt-in) | exception | *(opt-in)* An emitter's run-event kinds drift from the closed `RunEventKind` set. |
| [`check_run_event_alignment`](#the-run-event-contract-opt-in) | function | *(opt-in)* Build-time drift guard: assert every emitted kind is a `RunEventKind` member. |
| [`append_coordination_notice`](#coordination-notices-opt-in) | function | *(opt-in)* Append one opt-in, append-only cross-node coordination notice (never dispatched). |
| [`list_pending_notices`](#coordination-notices-opt-in) | function | *(opt-in)* Pure staleness-filtered read of the coordination notices whose referenced node is not terminal. |

### `StateStoreError`

```python
class StateStoreError(ValueError)
```

Raised when a `Record` field carries a value outside its typed vocabulary — in practice, an unknown `status`. Raised by the internal status coercion, and therefore by `Record.__post_init__` and by a `put`'s `meta`-merge. Subclasses `ValueError`. Not re-exported at the root; import from `concursus.state.statestore`.

### `RecordStatus` & `RecordType`

```python
class RecordStatus(str, Enum):
    VALIDATED = "validated"
    FAILED = "failed"
    SUPERSEDED = "superseded"

class RecordType(str, Enum):
    AGENT_OUTPUT = "agent_output"
    DEDUP = "dedup"
    CHECKPOINT = "checkpoint"
    COORDINATION = "coordination"   # opt-in cross-node notice (append-only; never dispatched)
```

Both are `str` subclasses, so `record.status == "validated"` and all-string metadata projection keep working untouched; both override `__str__` to return the bare value on Python 3.11+. **Asymmetric coercion:** an unknown `status` raises `StateStoreError`, whereas an unknown `record_type` is kept verbatim with a `warnings.warn` (widen-and-warn) so a future record kind never hard-fails a run.

> **The `coordination` record type is opt-in.** `RecordType.COORDINATION` is only ever written via [`append_coordination_notice`](#coordination-notices-opt-in); nothing in a default `Supervisor.run` produces one, so the default log is byte-for-byte unchanged. A coordination notice is never a validated agent output — it never enters `completed()` / `get()` / the projection (see [Coordination notices](#coordination-notices-opt-in)).

### `content_hash`

```python
def content_hash(output: dict) -> str
```

The SHA-256 hex digest of the canonical JSON of `output` (`json.dumps(output, sort_keys=True)`). A stable content address: identical outputs hash identically, so a re-`put` of an unchanged output is a detectable no-op (dedup / memoization / staleness). Also used to salt filesystem slugs and to hash checkpoint snapshots.

- **Returns:** a 64-character hex string.

```python
from concursus import content_hash

content_hash({"b": 2, "a": 1}) == content_hash({"a": 1, "b": 2})   # True — canonical, sort_keys
```

### `Record`

```python
@dataclass
class Record:
    node: str
    output: dict
    attempt: int = 1
    status: str = "validated"
    record_type: str = "agent_output"
    schema: Optional[str] = None
    producer: Optional[str] = None
    consumes: List[str] = field(default_factory=list)
    supersedes: Optional[str] = None
    content_hash: Optional[str] = None
    timestamp: Optional[int] = None
    seq: Optional[int] = None
    event_id: Optional[str] = None
    address: Optional[str] = None
    blocked_on: Optional[str] = None
    failure_class: Optional[str] = None
    epoch: Optional[int] = None
```

One validated (or failed) node output plus its slipbox metadata — the unit of the append-only run log.

| Field | Meaning |
|---|---|
| `node` | The DAG node id this output addresses (the semantic id). |
| `output` | The verbatim agent output. |
| `attempt` | 1-based retry sequence for `node`, **auto-incremented on every `put`** (see the gotcha below). |
| `status` | `validated` \| `failed` \| `superseded`. |
| `record_type` | `agent_output` (default), `dedup` (a content-identical no-op re-put), or `checkpoint` (a compaction snapshot). |
| `schema` | The output schema tag (trustworthy — validation ran before admission). |
| `producer` | The upstream node id, when this record is a producer projection. |
| `consumes` | Resolved `AgentRef` edges as `"producer:$.jsonpath"` strings. |
| `supersedes` | The prior attempt's `event_id` (deterministic replay ordering). |
| `content_hash` | `content_hash(output)`. |
| `timestamp` | Display-only event time — **not** the ordering key for a Memory-backed store. |
| `seq` | A store-assigned strict-monotonic sequence — the deterministic tie-breaker in ordering. `None` for a hand-built record. |
| `event_id` | The backing Memory event id (`None` in-process). |
| `address` | The Folgezettel execution address — a materialized path (default the `node` name; a retry / fan-out / branch appends a `"/"` segment, e.g. `"map/0"`). |
| `blocked_on` | A failure / blocked reason (meta). |
| `failure_class` | *(opt-in; `Supervisor(on_error="record")` only)* `"crash"` (this node's own invoke/validate/ARN-integrity raised) vs `"hold"` (never invoked because an upstream producer it consumes failed or was held — a pruned-subtree skip, not this node's fault). `None` for a validated or legacy failed record. |
| `epoch` | The checkpoint-compaction window id (C-4). |

`__post_init__` coerces `status` and `record_type` through their enums; an unknown `status` raises `StateStoreError`.

- **Raises:** `StateStoreError` — on an unknown `status`.

### `StateStore`

```python
class StateStore(Protocol):
    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None: ...
    def get(self, node: str) -> dict: ...
    def completed(self) -> Set[str]: ...
    def records(self) -> List[Record]: ...
```

The 4-method durable run-state seam the Supervisor writes through and resumes from, implemented by `InProcessStateStore`, `MemoryStateStore`, and `FileVaultStateStore`.

| Method | Contract |
|---|---|
| `put(node, output, *, meta=None)` | Admit a validated output for `node` (auto-incrementing its attempt). `meta` merges recognized keys (`producer`, `consumes`, `schema`, `record_type`, `status`, `address`, `blocked_on`, `failure_class`) onto the `Record`. |
| `get(node)` | The latest **validated** output for `node`; raises `KeyError` if absent. |
| `completed()` | The validated frontier: nodes whose **latest** record (regardless of status) is `validated`. |
| `records()` | The full append-only log. |

> `completed()` uses *latest-overall* (a node is complete only if its newest record validated), whereas `get()` returns the latest *validated* record. A node whose last attempt failed is therefore **not** in `completed()`, even though `get()` may still return an earlier validated output.

### `InProcessStateStore`

```python
class InProcessStateStore:
    def __init__(self) -> None
```

The zero-dependency, offline default that replaces the Supervisor's plain `outputs` dict. Holds an append-only `list[Record]` (source of truth) plus a `{node: latest validated output}` projection; nothing touches AWS. All four methods are guarded by a reentrant `threading.RLock`.

- `put` — increments the node's attempt, computes `content_hash`, marks a content-identical re-put as a `dedup` `record_type` no-op (never an error), stamps `timestamp = seq = ` a monotonic clock, applies `meta`, appends, and updates the projection when `status == "validated"`.
- `get` — raises `KeyError` if the node has no validated output.
- `completed` / `records` — as per the Protocol; `records` returns a copy.

```python
from concursus import InProcessStateStore

s = InProcessStateStore()
s.put("fetch", {"ok": True})
s.get("fetch")                 # {'ok': True}
s.put("fetch", {"ok": True})   # content-identical -> a dedup no-op, attempt 2
s.completed()                  # {'fetch'}
```

### `MemoryStateStore`

```python
class MemoryStateStore:
    def __init__(
        self, *, memory_id: str, session_id: str, actor_id: str, client: Any = None
    ) -> None
```

The AgentCore Memory-backed backend — durable, resumable run state. Each `put` appends one **Blob** event plus string metadata; the event log is the single source of truth, and a cached projection is (re)built by replaying it. Resume is just replay over the same `(memory_id, actor_id, session_id)`. Blob (not Conversational) is deliberate — it avoids AgentCore's long-term extraction of verbatim run state.

- **Parameters:**
  - `memory_id`, `session_id`, `actor_id` — the AgentCore Memory coordinates that scope the event log.
  - `client` — an injected `bedrock-agentcore` data-plane client. Defaults to a lazily-constructed `boto3.client("bedrock-agentcore")` on first use.
- **Raises (on data-plane use):** `RuntimeError` — if `boto3` is missing and no `client` was injected (install the `agentcore` extra or pass `client=...`).

All methods are `RLock`-guarded. Because AgentCore metadata values are charset-sanitized, the lossless record fields also ride in a Blob `__meta__` sidecar that the marshalling seam prefers over event metadata.

#### `MemoryStateStore.put`

```python
def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None
```

Append one Blob event for `node`'s output and update the cached projection in place (so a `get` right after a `put` reflects it). Assigns a local monotonic `seq` (the ordering tie-breaker — **not** the AgentCore `eventTimestamp`), stamps `record.epoch` with the current checkpoint window, sets `supersedes` from `meta["supersedes"]` or the node's last event id, and derives an AgentCore branch from the address parent when the address is a sub-path.

#### `MemoryStateStore.get` / `.completed` / `.records`

```python
def get(self, node: str) -> dict
def completed(self) -> Set[str]
def records(self) -> List[Record]
```

Each lazily replays the log exactly once before the first read, then serves from the cache. `get` raises `KeyError` if the node has no validated output.

> After a **warm** (checkpoint) resume, `records()` returns the compacted latest-per-node records plus the open-window tail — **not** every historical attempt. `completed()` / `get()` are unaffected (they only ever use latest-per-node). Call `replay(force_full=True)` when you need the full attempt history.

#### `MemoryStateStore.checkpoint`

```python
def checkpoint(self) -> Optional[str]
```

**C-4 compaction.** Write ONE `checkpoint`-type event for the current epoch — its Blob carries the compacted latest-per-node `Record`s as-of now — then rotate `epoch += 1`. Returns the checkpoint event id, or `None` if there is nothing to compact yet.

- **Single-writer-per-session contract:** correctness depends on exactly one writer per `(memory_id, actor_id, session_id)`, calling `checkpoint` synchronously. Raw events are **never** deleted — the append-only log stays the source of truth; the checkpoint is a derived snapshot that only makes a warm `replay` cheaper (a bounded `EQUALS_TO` on the epoch).
- **Returns:** `Optional[str]` — the event id, or `None` when the log is empty.

#### `MemoryStateStore.replay`

```python
def replay(self, *, force_full: bool = False) -> None
```

Rebuild the projection / attempts / records caches from the event log.

- **Warm path** (a checkpoint exists and `force_full` is false): re-hydrate the latest checkpoint's compacted snapshot, then fold in only the open-epoch tail — `O(events-in-window)`.
- **Cold path** (no checkpoint, or `force_full=True`): paginate the whole session and replace the caches.

Warm-path anomalies (a missing or undecodable snapshot) fall back to a full rebuild, so the fast path can never return a projection that differs from a cold replay. Returns `None`. (An `O(new)` incremental resume is impossible on the data plane: `nextToken` is opaque and the metadata filter offers only `EQUALS_TO | EXISTS | NOT_EXISTS`, no range.)

```python
m = MemoryStateStore(memory_id="m", session_id="sess", actor_id="a", client=fake)
m.put("n", {"x": 1})
m.checkpoint()          # write a compaction snapshot, rotate the epoch
m.replay()              # warm resume from the snapshot + open-window tail
```

> **Gotchas.** `put` always auto-increments `attempt`, even for a `dedup` no-op — `attempt` counts every put, not just distinct outputs. `timestamp` is display-only for a Memory-backed store; order on `seq`, never on `timestamp`.

### The run-event contract (opt-in)

*Opt-in, default-off.* A closed, typed contract for the governor's **episode-boundary events** — the payload the [Governor](../guides/governor.md)'s optional `EventSink` emitter hands to its readers. It is a compile-time / build-time seam only: **Concursus is a compiler, not a runtime governor**, so this contract does not add a runtime — `Supervisor.run` is still a single static pass over a frozen `plan.order`, and the events are boundary markers the *outer* governor loop emits *around* those passes. **Every part of it is opt-in and default-off:** a run with no `EventSink` wired in emits nothing and is byte-for-byte unchanged.

```python
class RunEventKind(str, Enum):
    EPISODE_START = "episode_start"   # before a Supervisor episode is dispatched
    EPISODE_END = "episode_end"       # after `collect` folds the episode's outputs into the log
    DECISION = "decision"             # the bounded routing verdict after `collect`

RUN_EVENT_KINDS: frozenset = frozenset(k.value for k in RunEventKind)

class RunEvent(TypedDict, total=False):
    type: str            # a RunEventKind value (one of RUN_EVENT_KINDS)
    run_id: str          # the outer run id
    round: int           # completed-episode count at this boundary
    completed: List[str] # sorted completed node ids, re-derived from the append-only log
    frontier: List[str]  # the still-open frontier at this boundary
    done: bool           # (episode_end) whether the plan's frontier is exhausted
    progressed: bool     # (episode_end) whether the round advanced the completed frontier
    route: str           # (decision) the routing verdict (`planner` | `synthesize`)
    terminated_by: Optional[str]  # (decision) the terminal label, when the loop stopped this round

class RunEventContractError(ValueError): ...

def check_run_event_alignment(emitted_kinds: Any) -> None
```

- **`RunEventKind`** — the CLOSED vocabulary of governor episode-boundary event kinds, shared by the emitter (the `GovernorLoop`'s opt-in `EventSink`) and its readers. A `str` subclass, so `event["type"] == "episode_start"` and JSON serialization keep working (mirrors `RecordStatus` / `RecordType`).
- **`RUN_EVENT_KINDS`** — the closed set of emitter event-`type` values as bare strings, for a membership check that never needs the enum type in hand.
- **`RunEvent`** — the frozen typed SHAPE of one episode-boundary event (the `EventSink.emit` payload). A plain-dict VALUE — never a live ctx/plan handle — so an observer can never reach inside a running `Supervisor` or mutate a frozen plan. `total=False` because the boundary-invariant keys (`type` / `run_id` / `round` / `completed` / `frontier`) always ride, while the per-kind extras are added only where meaningful: `done` / `progressed` on `episode_end`, and `route` / `terminated_by` on `decision`.
- **`RunEventContractError`** — raised when an emitter's run-event kinds drift from the closed `RunEventKind` set.
- **`check_run_event_alignment(emitted_kinds)`** — the build-time drift guard: assert every emitter kind is a `RunEventKind` member, else raise `RunEventContractError` so an emitter/reader mismatch fails at test/build time rather than reaching a reader as an unhandled event at runtime. Mirrors the `check_alignment` seam the compiler uses to type-gate manifest edges.

```python
from concursus import RunEventKind, RUN_EVENT_KINDS, check_run_event_alignment

check_run_event_alignment(["episode_start", "episode_end"])   # ok — both are members
check_run_event_alignment(["episode_start", "replanned"])     # raises RunEventContractError
```

### Coordination notices (opt-in)

*Opt-in, default-off.* An opt-in way to record a cross-node **coordination notice** on the SAME append-only log — without adding a `StateStore` Protocol method, mutable state, or any dispatch. **It is default-off:** nothing in a default run writes one, so the default log and the executed prefix `recompile` pins are byte-for-byte unchanged. A notice is **never dispatched** — it is a passive annotation a reader can staleness-filter, not a message that triggers work (consistent with the compiler framing: all dynamism is bounded recompile / pre-compile / the outer governor loop, never a runtime signal into a running plan).

```python
def append_coordination_notice(store: StateStore, node: str, payload: dict) -> None
def list_pending_notices(records: List[Record], terminal_nodes: Any) -> List[Record]
```

- **`append_coordination_notice(store, node, payload)`** — append one notice ABOUT `node` to `store` (pure, append-only). It is a plain `Record` on the sole append-only log, keyed under a dedicated `__coordination__` sentinel node (**not** `node` itself, which would flip that node's latest-overall record and corrupt `completed()` / `get()`), with `record_type="coordination"` and a non-`validated` (`superseded`) status — so it **never enters the validated projection** and can never perturb the executed prefix `recompile` pins (INV-3 / INV-5). The referenced `node` rides in the record's `producer` field (and the payload) so `list_pending_notices` can staleness-filter on it. "Marking a notice consumed" is itself just appending a follow-up notice — there is deliberately no mutable consume flag.
- **`list_pending_notices(records, terminal_nodes)`** — a PURE reader over the log: select the `record_type="coordination"` records and drop any whose referenced node is already terminal (present in `terminal_nodes` — e.g. the governor's `store.completed()` set), since a notice about a finished node is stale. It mutates nothing and marks nothing consumed, so calling it leaves the log byte-identical. Notices are returned in append (log) order.

```python
from concursus.state.statestore import append_coordination_notice, list_pending_notices

append_coordination_notice(store, "remediate", {"reason": "waiting on triage root_cause"})
list_pending_notices(store.records(), store.completed())   # notices whose node is not yet terminal
```

> Coordination notices are keyed under `__coordination__` with a non-`validated` status precisely so they can NEVER contaminate `completed()` / `get()` / the projection — the same append-only-log discipline every other record obeys, applied to a passive annotation that drives no dispatch.

---

## `state.filevault`

Source: [`../../src/concursus/state/filevault.py`](../../src/concursus/state/filevault.py)

A persistent, on-disk `StateStore` that writes **one round-trip-exact markdown note per record** under `<vault>/runs/<session>/` and resumes by reload. The authoritative payload is an embedded base64-JSON blob (byte-identical to `MemoryStateStore`'s Blob) plus a base64-JSON meta blob; the frontmatter and body are lossy display/index copies that the loader never reads — so an arbitrary `output` dict round-trips exactly. It reuses the `statestore` marshalling seam, so the file and AgentCore backends differ only in transport. This module also holds the write-time note renderers (`capture_*_note`).

| Symbol | Kind | Summary |
|---|---|---|
| [`FileVaultStateStore`](#filevaultstatestore) | class | On-disk `StateStore`; one markdown note per record, resume by reload. |
| [`FileVaultStateStore.from_config`](#filevaultstatestorefrom_config) | classmethod | Bind a run to `<vault>/runs/<slug(session_id)>/`. |
| [`FileVaultStateStore.run_dir`](#filevaultstatestorerun_dir--trail_id) | property | The run's on-disk directory. |
| [`FileVaultStateStore.trail_id`](#filevaultstatestorerun_dir--trail_id) | property | The run's SlipBox lineage/trail id. |
| [`FileVaultStateStore._atomic_write`](#filevaultstatestore_atomic_write) | staticmethod | The module's single atomic-write primitive (temp + `os.replace`). |
| [`capture_run_plan_note`](#capture_run_plan_note) | function | Persist a compiled plan as `<run_dir>/_plan.md`; returns a **path**. |
| [`capture_agent_response_note`](#capture_agent_response_note) | function | Render one agent-response record as a note; returns note **text**. |
| [`capture_agent_log_note`](#capture_agent_log_note) | function | Promote a raw agent log to a note **only when failed**; else `None`. |
| [`capture_run_output_note`](#capture_run_output_note) | function | Dispatch a record to the renderer for its `record_type`; returns **text**. |
| [`capture_payload_note`](#capture_payload_note) | function | Persist a node's frozen invoke payload as a **non-record** payload note (T3); redacts PII first. Returns a **path**. |
| [`redact`](#redact) | function | Mask top-level PII/secret keys in a payload dict — the PII/ACL gate prerequisite. |
| [`add_reciprocal_backlinks`](#add_reciprocal_backlinks) | function | T6 post-pass: append a `## Consumed By` section to each producer note over the recorded `consumes` edges (closes the forward-only gap). Returns a **count**. |
| [`append_note_version`](#the-note-version-timeline-opt-in) | function | *(opt-in)* Append one snapshot of a note to its opt-in append-only `versions/` timeline; `None` on an unchanged no-op. |
| [`read_note_versions`](#the-note-version-timeline-opt-in) | function | *(opt-in)* Every append-only version of one note, oldest→newest. |
| [`revert_note`](#the-note-version-timeline-opt-in) | function | *(opt-in)* Forward-revert a note to a prior version by appending it as the new latest version (never rewriting history). |
| [`iter_note_versions`](#the-note-version-timeline-opt-in) | function | *(opt-in)* Yield `(note_stem, version)` for every snapshot under `versions/` (drives the derived DB index). |

### `FileVaultStateStore`

```python
class FileVaultStateStore:
    def __init__(
        self, run_dir, *, slipbox_form: bool = True, trail_id: str = "run", date: str = "",
        versioned: bool = False,
    ) -> None
```

Each `put` writes one immutable markdown note atomically; a fresh store over an existing vault lazily reloads before the first read (resume = replay over the filesystem). Mirrors `InProcessStateStore`'s `put` semantics (append log + projection, attempt auto-increment, content-hash dedup) and adds durability. Creates `run_dir` on init.

- **Parameters:**
  - `run_dir` — the directory this run's notes live in.
  - `slipbox_form` — `True` (default) emits SlipBox-conformant notes and regenerates a `_run.md` entry point; `False` emits the lean machine schema (`node` / `attempt` / `status` / `consumes` / `payload`) and does **not** write `_run.md`.
  - `trail_id` — the SlipBox lineage id stamped into each note.
  - `date` — the `date of note` frontmatter value.
  - `versioned` — *(opt-in)* **`False` (default, OFF).** When `True`, every note the store re-writes with DIFFERENT content is snapshotted into the opt-in append-only `versions/` timeline (see [The note version timeline](#the-note-version-timeline-opt-in)). Off ⇒ no `versions/` dir is ever created, so the store's on-disk bytes are byte-for-byte identical to before. `from_config` forwards the same `versioned=False` default.
- **Concurrency:** concurrent writers over one vault are serialized by a reentrant `RLock`, a generation-token OCC read-fresh over a `.gen` sidecar, and an advisory `fcntl` lock over a `.lock` sidecar (degrading to `RLock`-only on non-POSIX platforms).
- **`put` / `get` / `completed` / `records`:** the `StateStore` Protocol; `get` raises `KeyError`.

> **Ordering gotcha.** FileVault records do **not** carry `seq`; on reload they are ordered by `timestamp` (the monotonic clock at write time), so last-write-wins depends on `timestamp`. A malformed/partial note is skipped on reload (`ValueError` / `JSONDecodeError` / `OSError` are caught), never fatal — a corrupted file silently drops that record.

### `FileVaultStateStore.from_config`

```python
@classmethod
def from_config(
    cls, *, vault_path, session_id: str, slipbox_form: bool = True, date: str = "",
    versioned: bool = False,
) -> "FileVaultStateStore"
```

Bind a run to `<vault_path>/runs/<slug(session_id)>/` — the explicit persistence-by-default posture (mirrors `MemoryService.from_config`). Derives `trail_id` from `session_id`. Forwards `versioned` (*opt-in*, default `False` / OFF) to the constructor.

```python
from concursus import FileVaultStateStore

v = FileVaultStateStore.from_config(vault_path="/vault", session_id="TKT-42")
v.put("triage", {"root_cause": "x"})   # writes a slipbox note + _run.md
v.get("triage")                        # {'root_cause': 'x'} (reloads from disk on a fresh store)
```

### `FileVaultStateStore.run_dir` & `.trail_id`

```python
@property
def run_dir(self) -> Path

@property
def trail_id(self) -> str
```

Read-only accessors for post-run distillation: `run_dir` is the note substrate `Path`; `trail_id` is the run's SlipBox lineage id (the family key for cross-run precedent).

### `FileVaultStateStore._atomic_write`

```python
@staticmethod
def _atomic_write(path: Path, text: str) -> None
```

Write `text` to `path` atomically (temp file in the same dir + `os.replace`). Underscore-prefixed but load-bearing across the tier — it is the single atomic-write primitive reused by `distill_run`, `render_precedent_hub`, and `capture_run_plan_note`.

### `capture_run_plan_note`

```python
def capture_run_plan_note(
    plan, run_dir, *, trail_id: str = "run", date: str = "", slipbox_form: bool = True
) -> str
```

Persist a compiled `ProvisioningPlan` as a durable model+navigation note `<run_dir>/_plan.md` — a Mermaid DAG of `plan.order` + wiring plus `plan.to_summary_dict()` (which **drops** the bulky per-node deploy payload). Returns **the note's path**.

It is **not** a run record: it carries no payload/meta blob and is stamped as a `run_plan` note kind (`concursus_note_kind: run_plan`), so the record parser refuses to parse it and record loaders skip it. A pure write-time projection of the frozen plan; it drives no dispatch.

```python
capture_run_plan_note(plan, v.run_dir, trail_id=v.trail_id)   # -> '/vault/runs/<slug>/_plan.md'
```

### `capture_agent_response_note`

```python
def capture_agent_response_note(
    record: Record, *, slipbox_form: bool = True, position: int = 1,
    trail_id: str = "run", date: str = "", related: Optional[List[str]] = None,
) -> str
```

Render one agent-response `Record` as a durable, round-trip-exact note (a thin named seam over the internal renderer). The body carries the Did → Observed → Outcome summary plus any machine findings the output happens to carry; the authoritative payload/meta base64 blobs are untouched. Returns **the rendered note text** (not a path).

### `capture_agent_log_note`

```python
def capture_agent_log_note(
    record: Record, *, slipbox_form: bool = True, position: int = 1,
    trail_id: str = "run", date: str = "", related: Optional[List[str]] = None,
) -> Optional[str]
```

**Selective-ingestion policy.** Promote a raw agent log to a durable note **only when `record.status == "failed"`**; otherwise return `None` (the verbose log stays a derived, non-promoted sidecar). Failure is the only promotion trigger — a failed record is a `counter_argument` note. When failed, it renders through the same round-trip-exact path as any other response note.

- **Returns:** `Optional[str]` — the note text, or `None` for a non-failed record. **Callers must handle `None`.**

```python
capture_agent_log_note(failed_record)   # -> note text
capture_agent_log_note(ok_record)       # -> None (never promoted)
```

### `capture_run_output_note`

```python
def capture_run_output_note(
    record: Record, *, slipbox_form: bool = True, position: int = 1,
    trail_id: str = "run", date: str = "", related: Optional[List[str]] = None,
) -> str
```

Dispatch a run-output `Record` to the renderer for its `record_type` (`dedup` / `agent_output` / `checkpoint` all map to `capture_agent_response_note`; unknown types fall back to it too). This umbrella renders **every** record — a failed record still routes through the response renderer here; the failure-only *promotion* policy lives in `capture_agent_log_note`. Returns note **text**. Read-only projection.

### `capture_payload_note`

```python
def capture_payload_note(
    node: str, payload: Mapping[str, Any], run_dir, *, trust_tier: str = "", trail_id: str = "run",
    date: str = "", related: Optional[List[str]] = None, redact_keys: Optional[List[str]] = None,
    slipbox_form: bool = True,
) -> str
```

**T3 — persist the frozen invoke payload.** Render `node`'s frozen invoke `payload` as a durable note under `run_dir` and return **its path**. It is a **non-record**: the note is stamped `concursus_note_kind: payload`, so the internal `_note_to_record` parser **refuses** it — a payload note never leaks back into replay as run state. Redacts PII via [`redact`](#redact) (with `redact_keys` overriding the default deny list) **before** writing, and records the `trust_tier` for the payload-contract read-back ([`load_payload_tiers`](#load_payload_tiers)).

- **Filename:** `<slug(node)>__payload.md`.
- **Returns:** the note's **path** (`str`).

```python
from concursus.state.filevault import capture_payload_note

capture_payload_note("summarize", {"sop": "...", "case_data": "..."}, v.run_dir, trust_tier="GUARDED")
# -> '/vault/runs/<slug>/summarize__payload.md'  (case_data masked)
```

### `redact`

```python
def redact(payload: Mapping[str, Any], *, deny: Optional[List[str]] = None) -> Dict[str, Any]
```

Return a copy of `payload` with every **top-level** key named in `deny` replaced by `"<redacted>"`. `deny` defaults to `_DEFAULT_REDACT_KEYS` (`pii` / `secret` / `credentials` / `customer_id` / `case_data` / `raw_input`). The PII/ACL gate prerequisite that [`capture_payload_note`](#capture_payload_note) runs before any payload touches disk. Pure — it does not mutate the input.

### `add_reciprocal_backlinks`

```python
def add_reciprocal_backlinks(run_dir) -> int
```

**T6 post-pass — close the forward-only backlink gap.** FileVault's internal `_related_for` only records **forward** `consumes` edges (a consumer note points back at its producers), so a producer note has no record of who consumed it. This appends a `## Consumed By` section to each producer note over the **already-recorded** `consumes` edges — a pure projection of existing data, adding no new edges. Idempotent (re-running yields the same sections) and strictly **post-run**. Returns the number of producer notes amended.

- **Returns:** `int` — producer notes amended.

```python
from concursus.state.filevault import add_reciprocal_backlinks

add_reciprocal_backlinks(v.run_dir)   # -> 3  (three producer notes gained a "## Consumed By" section)
```

> **Return-type gotcha.** The `capture_*_note` functions return note **text**, except `capture_run_plan_note` and `capture_payload_note`, which **write** the file and return a **path**. `add_reciprocal_backlinks` returns a **count**.

### The note version timeline (opt-in)

*Opt-in, default-off.* A note the store re-writes with different content (the `_run.md` entry point grows on every `put`; the T6 post-pass amends a producer note) has, by default, **no history** — the prior bytes are simply overwritten. This opt-in timeline closes that gap WITHOUT ever rewriting the append-only log: each distinct content of a note is snapshotted into a `versions/` sidecar tree (`<run_dir>/versions/<note_stem>/vNNN.md`), append-only, newest = highest `N`. Each snapshot carries typed provenance frontmatter (`version` / `when` / `content_hash` / `source_note`, plus `reverted_from` for a forward revert) and embeds the full versioned note text as an authoritative `b64:` snapshot blob so it round-trips byte-exact.

**Default-off, and inert to replay.** The whole feature is gated behind `FileVaultStateStore(versioned=…)` (default `False`), so the DEFAULT single-write path is byte-for-byte identical to before — **no `versions/` dir is ever created** unless a caller opts in. A version snapshot is stamped `concursus_note_kind: note_version`, so `_note_to_record` REFUSES to parse it; and because every record loader globs `*.md` **non-recursively**, a version note under `versions/` is never seen by a resume/replay — the timeline can NEVER leak into run state.

```python
def append_note_version(
    run_dir, note_name: str, content: str, *,
    when: str = "", reverted_from: Optional[int] = None, force: bool = False,
) -> Optional[str]

def read_note_versions(run_dir, note_name: str) -> List[Dict[str, Any]]

def revert_note(
    run_dir, note_name: str, version: int, *, when: str = "", restore_live: bool = True
) -> str

def iter_note_versions(run_dir)   # -> Iterator[Tuple[str, Dict[str, Any]]]
```

- **`append_note_version`** — append a new version of `note_name` to its append-only timeline; return the new snapshot path, or `None` when the content is unchanged (a content-hash-deduped no-op, so only a note that actually changed grows the timeline). APPEND-ONLY: an existing `vNNN.md` is NEVER rewritten. Pass `force=True` (used by `revert_note`) to always append even when the content matches the head; `reverted_from` stamps the forward-revert provenance.
- **`read_note_versions`** — every append-only version of one note, oldest→newest (empty when the note has no timeline); each entry is `{version, when, content_hash, reverted_from, content, source_note}`.
- **`revert_note`** — a **forward revert**: it reads the immutable snapshot at `version`, `force`-appends its content as the NEW latest version (stamped `reverted_from=version`), and — with `restore_live=True` (default) — also restores that content to the live note file. It never rewrites history: the prior snapshots (including the state reverted away from) are preserved. Raises `ValueError` if `version` is not in the timeline.
- **`iter_note_versions`** — yield `(note_stem, version_dict)` for every snapshot under `<run_dir>/versions/` — the flat read the derived [`state.rundb`](#staterundb) `note_versions` index consumes. Empty when the run was never versioned.

```python
from concursus.state.filevault import read_note_versions, revert_note

# with a versioned store: v = FileVaultStateStore.from_config(..., versioned=True)
read_note_versions(v.run_dir, "_run.md")          # [{'version': 1, ...}, {'version': 2, ...}]
revert_note(v.run_dir, "_run.md", 1)               # forward-appends v1's content as v3 (+ restores live)
```

> **Note-schema evolution is forward-only, and default-off.** The on-disk note format carries an OPT-IN `schema_version` stamp (`stamp_schema_version`, default OFF — an unstamped note reads back as v1, the baseline). Evolution is applied on READ (`_note_schema_version` / `_migrate_note_meta` upgrade a parsed note's `meta` to the current shape in memory); the append-only log is never rewritten or downgraded. While v1 is current the migration registry is empty, so the read path is a no-op and the round-trip stays byte-exact — the DEFAULT write path emits identical bytes to before.

---

## `state.rungraph`

Source: [`../../src/concursus/state/rungraph.py`](../../src/concursus/state/rungraph.py)

Projects `StateStore` records into a directed **data-dependency** graph (producer→consumer via the recorded `consumes` `AgentRef` edges) and answers structural questions: transitive upstream/downstream blast radius, a pre-dispatch DAG/dangling-edge gate, and a bounded nearest-first `context_order` for graph-aware context assembly. Pure Python — no `networkx`, no AWS.

| Symbol | Kind | Summary |
|---|---|---|
| [`Edge`](#edge) | type alias | `(producer, consumer, jsonpath)` triple. |
| [`RunGraphError`](#rungrapherror) | exception | Structurally invalid graph — a cycle or a dangling `AgentRef`. |
| [`RunGraph`](#rungraph) | dataclass | The directed graph (`nodes` + `edges`). |
| [`RunGraph.from_records`](#rungraphfrom_records--from_edges) | classmethod | Build from `StateStore` records. |
| [`RunGraph.from_edges`](#rungraphfrom_records--from_edges) | classmethod | Build from an explicit node set + edges. |
| [`RunGraph.upstream`](#rungraphupstream--downstream) | method | Transitive producers (ancestors). |
| [`RunGraph.downstream`](#rungraphupstream--downstream) | method | Transitive consumers (descendants) — the re-run blast radius. |
| [`RunGraph.validate`](#rungraphvalidate) | method | Assert a valid DAG with no dangling edges. |
| [`RunGraph.context_order`](#rungraphcontext_order) | method | Bounded, nearest-first producers for context assembly. |

### `Edge`

```python
Edge = Tuple[str, str, str]   # (producer, consumer, jsonpath)
```

A type alias for a graph edge — one per resolved `AgentRef`. Import from `concursus.state.rungraph`.

### `RunGraphError`

```python
class RunGraphError(ValueError)
```

Raised when a run graph is structurally invalid — a cycle, or a dangling `AgentRef` (an edge naming a producer absent from `nodes`). Raised by `RunGraph.validate`. Subclasses `ValueError`.

### `RunGraph`

```python
@dataclass
class RunGraph:
    nodes: Set[str] = field(default_factory=set)
    edges: List[Edge] = field(default_factory=list)
```

A directed graph of a run's data dependencies. `nodes` is every node id (each record's node plus every referenced producer); `edges` is `(producer, consumer, jsonpath)` triples.

### `RunGraph.from_records` & `.from_edges`

```python
@classmethod
def from_records(cls, records: Iterable[Any]) -> "RunGraph"

@classmethod
def from_edges(cls, nodes: Iterable[str], edges: Iterable[Edge]) -> "RunGraph"
```

`from_records` is duck-typed on `.node` + `.consumes`: each record contributes its node, and each `consumes` entry `"producer:$.path"` is split on its **first** `":"` (via `str.partition`, so a JSONPath containing `:` survives) into the edge `(producer, node, path)`; the referenced producer is added as a node too — so a consumer naming an unwritten producer still surfaces for `validate` to catch. `from_edges` builds directly from an explicit node set and edge list.

> A wrong record shape (missing `.node` / `.consumes`) raises `AttributeError`, not `RunGraphError`.

### `RunGraph.upstream` & `.downstream`

```python
def upstream(self, node: str) -> Set[str]
def downstream(self, node: str) -> Set[str]
```

`upstream` is the transitive producers (ancestors) via the reverse adjacency; `downstream` is the transitive consumers (descendants) via the forward adjacency — the blast radius that must re-run when `node` changes. Both **exclude** `node` itself.

```python
g = RunGraph.from_records(store.records())
g.downstream("fetch")   # {'triage', 'summarize'} — blast radius of re-running fetch
```

### `RunGraph.validate`

```python
def validate(self) -> None
```

Assert the graph is a valid DAG with no dangling edges — the structural complement to output-schema validation; nothing should dispatch until it passes. Cycle detection is Kahn's algorithm (matching `core.dag`). Returns `None` on success.

- **Raises:** `RunGraphError` — if any edge names a producer absent from `nodes` (dangling), **or** if the graph contains a cycle. Dangling edges are checked **before** cycles, so a graph with both reports the dangling edge first.

```python
RunGraph.from_records(store.records()).validate()   # raises RunGraphError on a cycle/dangling edge
```

### `RunGraph.context_order`

```python
def context_order(self, node: str, *, max_depth: int = 2, max_nodes: int = 20) -> List[str]
```

Producers relevant to `node`, nearest-first, deduped, and bounded — a BFS over the reverse adjacency (direct producers first, then theirs), excluding `node`, sorted within each hop for determinism, capped at `max_depth` hops and `max_nodes` results. This is the app-layer traversal the Memory-backed store has no recursive query for.

- **Note:** the `max_nodes` cap can truncate **mid-hop** (it returns as soon as the cap is hit), so results are not guaranteed to include all of a given depth.

---

## `state.runindex`

Source: [`../../src/concursus/state/runindex.py`](../../src/concursus/state/runindex.py)

Two orthogonal, rebuildable indexes over one run's record log: a **metadata inverted index** (`node` / `status` / `record_type` / `schema` / `producer`) for lookup-not-scan queries, and a **Folgezettel materialized-path execution tree** over each record's `address` (a node's retries / fan-outs / branches). Plus `PrecedentIndex`, a cross-run retrieval index over distilled precedent payloads. `RunIndex` is the *execution* tree — distinct from `RunGraph`'s *data* dependency DAG. Pure Python.

| Symbol | Kind | Summary |
|---|---|---|
| [`INDEXED_FIELDS`](#indexed_fields) | constant | The metadata fields the inverted index covers. |
| [`RunIndexError`](#runindexerror) | exception | A materialized-path address violates the honest-tree invariants. |
| [`address_of`](#address_of) | function | A record's Folgezettel address (its `address` or `node`). |
| [`RunIndex`](#runindex) | class | The dual index (metadata query + tree traversal). |
| [`PrecedentIndex`](#precedentindex) | class | A cross-run retrieval index keyed by `trail_id`. |

### `INDEXED_FIELDS`

```python
INDEXED_FIELDS = ("node", "status", "record_type", "schema", "producer")
```

The metadata fields queryable without deserializing payloads. `RunIndex.query` uses inverted postings for these; any other filter key falls back to a linear scan. Import from `concursus.state.runindex`.

### `RunIndexError`

```python
class RunIndexError(ValueError)
```

Raised by `RunIndex.validate` when materialized-path addresses violate the honest-tree invariants: an orphaned sub-address, an unknown root segment, or (when requested) a non-contiguous attempt sequence. Subclasses `ValueError`.

### `address_of`

```python
def address_of(record: Record) -> str
```

The record's Folgezettel address — its explicit `.address`, or, by default, its `.node`.

### `RunIndex`

```python
class RunIndex:
    SEP = _ADDR_SEP   # "/"
    def __init__(self, records: Iterable[Record]) -> None
```

A dual index over a run's records. `__init__` builds the inverted postings over `INDEXED_FIELDS`, the by-address map, and an address set that back-fills every ancestor prefix (for traversal).

| Method | Signature | Behavior |
|---|---|---|
| `from_records` | `@classmethod from_records(cls, records) -> "RunIndex"` | Build over an explicit records iterable. |
| `from_store` | `@classmethod from_store(cls, store) -> "RunIndex"` | Build over a store's current log (duck-typed on `.records()`). |
| `query` | `query(self, **filters) -> List[Record]` | Records matching **every** filter (AND), in log order. Indexed fields use postings (set-intersection on `id(r)`); other fields fall back to a linear scan by `getattr` equality. |
| `by` | `by(self, field_name: str) -> Dict[str, List[Record]]` | The inverted postings for one indexed field (`value -> [records]`), a defensive copy; `{}` for a non-indexed field. |
| `latest` | `latest(self, node, *, status="validated") -> Optional[Record]` | The newest record for `node`, optionally status-filtered (default `"validated"`); `None` if none. Pass `status=None` to consider all statuses. |
| `nodes` | `nodes(self) -> Set[str]` | Every node id present in the log. |
| `validate` | `validate(self, *, check_attempts=False) -> "RunIndex"` | Assert the honest-tree invariants; returns `self`. See below. |
| `addresses` | `addresses(self) -> List[str]` | Every address (records' + ancestor prefixes), sorted. |
| `record_at` | `record_at(self, address) -> Optional[Record]` | The newest record exactly at `address`; `None` for a bare prefix. |
| `parent` | `parent(self, address) -> Optional[str]` | The prefix-derived parent (strip the last `/` segment); `None` for a root. |
| `children` | `children(self, address) -> List[str]` | Direct child addresses (one segment deeper), sorted. |
| `siblings` | `siblings(self, address) -> List[str]` | Addresses sharing this address's parent, excluding itself. |
| `ancestors` | `ancestors(self, address) -> List[str]` | The ancestor chain, **nearest parent first**. |
| `descendants` | `descendants(self, address) -> List[str]` | Every address strictly below `address`, sorted. |
| `subtree` | `subtree(self, address) -> List[str]` | `address` (if present) plus all descendants. |
| `roots` | `roots(self) -> List[str]` | Top-level addresses (no parent), sorted. |
| `leaves` | `leaves(self) -> List[str]` | Addresses with no children (tips of the tree), sorted. |
| `traverse` | `traverse(self, address) -> Dict[str, List[str]]` | `{ancestors (root-first), children, descendants, siblings}`. |

**`validate(*, check_attempts=False)`** asserts: (1) **no orphaned sub-address** — every non-root address's parent-prefix must be a *real* record's address, not a synthesized prefix; (2) **known root** — every first path segment must name a known node. With `check_attempts=True` it also asserts each node's attempts form a contiguous `1..N` sequence. Returns `self` for chaining; raises `RunIndexError` on the first violation. A static binding-integrity assertion — it reads addresses and fails, never repairing or mutating.

```python
idx = RunIndex.from_store(store)
idx.query(status="failed")             # inverted-index lookup, not a payload scan
idx.latest("triage")                   # newest validated record for 'triage'
idx.traverse("map")                    # {'ancestors': [...], 'children': ['map/0','map/1'], ...}
idx.validate(check_attempts=True)      # raises RunIndexError on a gap
```

> **Gotchas.** `traverse` returns `ancestors` **root-first**, but `ancestors()` returns them **nearest-first** — opposite orders for the same relationship. `latest` defaults to `status="validated"`; pass `status=None` to find a failed attempt. `validate(check_attempts=True)` requires a contiguous `1..N` set, so a store that only kept latest-per-node (e.g. after a `MemoryStateStore` warm resume) will spuriously fail it. `query` intersects on `id(r)` — do not mix `Record`s from different index instances.

### `PrecedentIndex`

```python
class PrecedentIndex:
    def __init__(self, precedents: Iterable[Dict[str, object]]) -> None
```

A read-only, cross-run retrieval index over distilled precedent payloads (the in-process analogue of `render_precedent_hub`) — one entry per run/family, keyed by `str(payload["trail_id"])`. Payloads with a falsy `trail_id` are dropped. A pure projection; it starts and seeds no run.

| Method | Signature | Behavior |
|---|---|---|
| `from_vault` | `@classmethod from_vault(cls, vault_path) -> "PrecedentIndex"` | Build over every precedent note under `<vault>/precedents/`. |
| `trails` | `trails(self) -> List[str]` | Every distilled run/family id, sorted. |
| `get` | `get(self, trail_id: str) -> Optional[Dict[str, object]]` | The payload for one run/family; `None` if not distilled. |
| `query` | `query(self, *, status=None) -> List[Dict[str, object]]` | Payloads, optionally filtered to a status, in trail-id order. |

```python
PrecedentIndex.from_vault("/vault").query(status="completed")
```

---

## `state.rundb`

Source: [`../../src/concursus/state/rundb.py`](../../src/concursus/state/rundb.py)

A derived, rebuildable, gitignorable **SQLite** graph/index over a persisted run's notes (and a cross-run precedent DB). It reads FileVault notes (the single source of truth) and materializes a records postings table, `consumes` edges, the Folgezettel address tree, and a latest-validated projection VIEW. When the run opted into versioning, it also derives a `note_versions` index over the `versions/` timeline (empty for the default unversioned run, so the default path is unchanged). Pure stdlib `sqlite3` — deleting the DB loses nothing.

| Symbol | Kind | Summary |
|---|---|---|
| [`load_records`](#load_records) | function | Read every record note under a run dir into a timestamp-ordered `List[Record]`. |
| [`build_run_db`](#build_run_db) | function | Build/refresh the derived SQLite DB for one run; returns the DB path. |
| [`build_precedent_db`](#build_precedent_db) | function | Rebuild the cross-run precedent SQLite DB; returns the DB path. |
| [`get_run_snapshot`](#get_run_snapshot) | function | Pure offline read of one run's ordered snapshot, optionally windowed by agent/step. |
| [`redact_snapshot`](#redact_snapshot) | function | Deep-copy a snapshot with every regex match masked as `[REDACTED]` — an optional egress guard. |

### `load_records`

```python
def load_records(run_dir) -> List[Record]
```

Read every record note under `run_dir` (excluding `_run.md`) into a `timestamp`-ordered list of `Record`s, tolerating malformed files (skipped — a parse error skips that note rather than aborting).

```python
recs = load_records("/vault/runs/tkt_42")   # timestamp-ordered List[Record]
```

### `build_run_db`

```python
def build_run_db(run_dir, db_path: Optional[str] = None, *, incremental: bool = True) -> str
```

Build/refresh the derived SQLite DB for one run from its note files; return the DB path (default `<run_dir>/index/run.sqlite`, whose `index/` dir is created when `db_path` is `None`). Reads only the notes (the source of truth); the DB is a pure disposable projection.

- `incremental=True` (default): keep existing `records` rows, re-ingest only notes whose `file_path` is new or whose `st_mtime` changed, drop rows for vanished notes, then rebuild the derived read-models (`consumes_edges` / `run_addresses` / the `projection` VIEW / the optional `records_fts`). Byte-for-byte identical to a full rebuild.
- `incremental=False`: `DROP` + recreate everything.

- **Returns:** the DB path (`str`).

```python
build_run_db("/vault/runs/tkt_42")                    # -> '/vault/runs/tkt_42/index/run.sqlite'
build_run_db("/vault/runs/tkt_42", incremental=False) # full DROP+recreate rebuild
```

> The `projection` VIEW (latest validated per node) is implemented **without** window functions, so it runs on older SQLite (< 3.25). FTS5 is optional — `records_fts` is created only when the SQLite build ships it; everything else degrades gracefully. The incremental delta is keyed on `st_mtime` equality, so a note rewritten within the same mtime tick could be skipped. **Self-healing:** before trusting a pre-existing DB, an incremental pass runs `PRAGMA quick_check`; a corrupt file (and its WAL/SHM sidecars) is discarded and the run rebuilt from the untouched note SSOT.

### `build_precedent_db`

```python
def build_precedent_db(vault_path, db_path: Optional[str] = None) -> str
```

Rebuild the derived cross-run precedent DB from the precedent notes; return the DB path (default `<vault>/precedents/index/precedents.sqlite`). The at-rest analogue of `render_precedent_hub`: it reads **only** the notes under `<vault>/precedents/` (via `distill.load_precedents`), then `DROP`+recreates the `precedents` table (one row per `trail_id`, carrying `status` / `total` / `completed` / `n_failed` / `nodes_json` / `payload_json`). A read-only retrieval index over finished runs — never a live router; idempotent and disposable.

- **Returns:** the DB path (`str`).

```python
build_precedent_db("/vault")   # -> '/vault/precedents/index/precedents.sqlite'
```

> Rows key on `payload["trail_id"]`, falling back to `record.node`; a precedent payload missing `trail_id` collapses onto the node name.

### `get_run_snapshot`

```python
def get_run_snapshot(run_id, *, agent: Optional[str] = None, step: Any = None) -> Dict[str, Any]
```

Return one run's ordered, JSON-serializable snapshot — optionally narrowed to one agent/node and/or a step window. A single OFFLINE read over the run's note SSOT: `run_id` is the run directory (the `FileVaultStateStore` run dir whose `*.md` notes are the single source of truth). Records are loaded via [`load_records`](#load_records) (the canonical deterministic at-rest order) and each is assigned a 1-based `step` ordinal over the WHOLE run; the agent/node filter runs through the derived `RunIndex` metadata index, and the step window is applied on top.

It is a PURE read projection (INV-5): it re-derives everything from the append-only notes on each call, opens no live plan, drives no dispatch, mutates nothing, and pulls no boto3/langgraph — the at-rest, cross-process analogue of `DirectorCockpit.snapshot`. An absent/empty run dir yields an empty (but well-formed) snapshot.

- **Parameters:**
  - `agent` — restrict to one node id (via the `RunIndex` metadata index); `None` (default) selects every record.
  - `step` — a **1-based ordinal** in the run's canonical at-rest order (not a wall-clock): `None` (default) = every step; an `int` = that single step; a `(lo, hi)` pair = that inclusive window (either bound may be `None` for an open side). A malformed window raises `ValueError`. Ordinals are assigned over the WHOLE run **before** the agent filter, so a step number names the same record regardless of the agent scope.
- **Returns:** `{"run_id", "agent", "step", "total", "count", "records": [<row>, ...]}` — the selected rows in step order, where `total` is the full log length. Each row is `{step, node, address, attempt, status, record_type, schema, producer, consumes, content_hash, timestamp, output}`.

```python
from concursus.state.rundb import get_run_snapshot

get_run_snapshot("/vault/runs/tkt_42")                       # the whole run, step-ordered
get_run_snapshot("/vault/runs/tkt_42", agent="triage")       # only 'triage' records
get_run_snapshot("/vault/runs/tkt_42", step=(2, 4))          # the 2nd–4th records inclusive
```

### `redact_snapshot`

```python
def redact_snapshot(snapshot: Any, pattern: Any) -> Any
```

Return a deep copy of `snapshot` with every `pattern` match masked as `[REDACTED]` — an OPTIONAL egress guard for [`get_run_snapshot`](#get_run_snapshot) output (or any JSON-serializable value). A single compiled pattern is applied to every string in the structure (dict values, list items, nested); `pattern` may be a `str` (compiled here) or an already-compiled `re.Pattern`. When at least one match is masked a WARN is logged (so an operator sees egress carried a secret). Pure and read-only: it copies rather than mutating its input and touches no disk/network — you opt in by calling it, so default snapshot output is unchanged.

```python
from concursus.state.rundb import get_run_snapshot, redact_snapshot

snap = get_run_snapshot("/vault/runs/tkt_42")
redact_snapshot(snap, r"\b\d{12}\b")   # mask anything that looks like a 12-digit id before egress
```

---

## `state.distill`

Source: [`../../src/concursus/state/distill.py`](../../src/concursus/state/distill.py)

The **offline memory loop** — pure post-run distillation. `distill_run` folds ONE finished run's `{node: output}` + `consumes` graph + outcome into a single compact precedent note under `<vault>/precedents/`; `render_precedent_hub` projects the accumulated set of precedent notes into one cross-run hub. Both are pure post-run — they never feed back into a running plan. Stdlib only.

| Symbol | Kind | Summary |
|---|---|---|
| [`build_precedent_payload`](#build_precedent_payload) | function | Fold a finished run into one JSON-serializable precedent payload (pure, no I/O). |
| [`precedents_dir`](#precedents_dir) | function | The `<vault>/precedents/` tree (a sibling of `runs/`). |
| [`distill_run`](#distill_run) | function | Distill one finished run into a precedent note; returns its path. |
| [`distill_store`](#distill_store) | function | Convenience: distill straight from a `FileVaultStateStore`. |
| [`load_precedents`](#load_precedents) | function | Read every precedent note into `Record`s (skips the hub note). |
| [`render_precedent_hub`](#render_precedent_hub) | function | Render the cross-run hub `_index.md`; returns its path. |

### `build_precedent_payload`

```python
def build_precedent_payload(
    result: Dict[str, dict], records: Sequence[Record], *,
    trail_id: str, outcome: Optional[Dict[str, object]] = None,
) -> Dict[str, object]
```

Fold a finished run into ONE compact, JSON-serializable precedent payload: `trail_id`, a one-word `status`, outcome counts (`total` / `completed` / `failed`), the executed `nodes`, the reconstructed `consumes` graph (`[consumer, producer, jsonpath]` rows, from each record's own edges — never re-derived from a live plan), and the final `{node: output}` `results`. Pure function; no I/O, no plan access.

- `outcome` defaults to one derived from `records` when `None`. `status` is `completed` (all done, none failed), `partial` (some done), or `failed` (nothing validated — including a zero-node run).

### `precedents_dir`

```python
def precedents_dir(vault_path) -> Path
```

The dedicated `<vault>/precedents/` tree — a **sibling** of `<vault>/runs/`, deliberately outside any run dir so a precedent note is never globbed back as a run record. Import from `concursus.state.distill`.

### `distill_run`

```python
def distill_run(
    result: Dict[str, dict], records: Sequence[Record], *,
    vault_path, trail_id: str = "run", outcome: Optional[Dict[str, object]] = None,
    run_dir=None, slipbox_form: bool = False, date: str = "",
) -> str
```

Distill ONE finished run into a single precedent note under `<vault>/precedents/`; return its path. Builds a precedent payload, wraps it in a synthetic run-summary `Record` (`record_type="checkpoint"`, `schema="run_precedent"`), and renders it through the same round-trip-exact renderer FileVault uses (so the precedent reloads via the same parser). The note lands under `precedents/` — never a run dir — so it can never be replayed as run state.

A pure POST-RUN write: invoked only after `Supervisor.run` returns; it mutates no topology and seeds nothing.

```python
distill_run(result, store.records(), vault_path="/vault", trail_id="tkt_42")
# -> '/vault/precedents/tkt_42.md'
```

### `distill_store`

```python
def distill_store(
    store: FileVaultStateStore, *,
    result: Optional[Dict[str, dict]] = None, outcome: Optional[Dict[str, object]] = None,
    vault_path=None, slipbox_form: Optional[bool] = None, date: Optional[str] = None,
) -> str
```

Convenience: distill a run straight from its `FileVaultStateStore` (post-run). Reads the store's `run_dir` / `trail_id`; when no explicit `result` is passed, it projects `{node: latest validated output}` from the store's completed frontier. `vault_path` defaults to `run_dir.parent.parent` (the `from_config` `<vault>/runs/<slug>` layout); `slipbox_form` / `date` default to the store's own settings when the arg is `None`. Returns the precedent note path.

> `distill_store`'s `vault_path` default only works for the `from_config` layout; a store constructed with a custom `run_dir` needs an explicit `vault_path`.

### `load_precedents`

```python
def load_precedents(vault_path) -> List[Record]
```

Read every precedent note under `<vault>/precedents/` into `Record`s — the single source of truth for the cross-run projections. Skips the hub note (`_index.md`) and tolerates malformed files (skipped). Returns `[]` if the precedents dir does not exist.

### `render_precedent_hub`

```python
def render_precedent_hub(vault_path, *, slipbox_form: bool = False, date: str = "") -> str
```

Render the cross-run precedent hub `<vault>/precedents/_index.md` and return its path. A pure, idempotent read-only projection over the set of per-run precedent notes: one row per run/family (keyed by `trail_id`), sorted, regenerated from scratch each call (same notes → byte-identical output). A retrieval index, **not** a live router/scheduler — it selects no run and seeds nothing. Deleting it loses nothing.

```python
render_precedent_hub("/vault")   # -> '/vault/precedents/_index.md'
```

> **`slipbox_form` default gotcha.** `distill_run`, `distill_store`, and `render_precedent_hub` all default `slipbox_form=False` — the **opposite** of `FileVaultStateStore`'s default `True`.

---

## `state.precedent`

Source: [`../../src/concursus/state/precedent.py`](../../src/concursus/state/precedent.py)

The compile-time, read-only cross-run precedent lookup — the matching *read* half of `distill.py`. `PrecedentRetriever` reads the durable precedent store (`PrecedentIndex.from_vault`) and ranks prior resolved runs for a query via a **StructuredKey → Lexical (BM25) → optional Dense** retrieval ladder, feeding the plan-author context. The Dense rung is off unless an `embed_fn` is wired in; `make_hashing_embed_fn` is the built-in, offline, dependency-free embedder that makes it usable for cross-domain / cross-family transfer. A pure compile-time read; it runs *before* a plan is frozen and never triggers a runtime replan. Stdlib only.

| Symbol | Kind | Summary |
|---|---|---|
| [`RetrievedPrecedent`](#retrievedprecedent) | dataclass | One retrieved prior run plus retrieval provenance. |
| [`PrecedentRetriever`](#precedentretriever) | class | The retrieval ladder over the durable precedent store. |
| [`make_hashing_embed_fn`](#make_hashing_embed_fn) | function | A deterministic, offline, dependency-free hashing `embed_fn` that makes the dense rung usable (off by default). |

### `RetrievedPrecedent`

```python
@dataclass
class RetrievedPrecedent:
    trail_id: str
    method: str
    score: float
    payload: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict
```

One retrieved prior run: the matched precedent payload plus retrieval provenance.

| Field | Meaning |
|---|---|
| `trail_id` | The matched run/family id. |
| `method` | Which ladder rung matched — `structured` \| `lexical` \| `dense`. |
| `score` | The rung's relevance (`1.0` for an exact structured key match). |
| `payload` | The verbatim precedent payload (read-only context). |

`to_dict()` returns a JSON-serializable view `{trail_id, method, score, precedent}` — note the payload is emitted under the key **`precedent`**, not `payload`.

### `PrecedentRetriever`

```python
class PrecedentRetriever:
    def __init__(
        self, vault_path, *,
        embed_fn: Optional[Callable[[str], Sequence[float]]] = None, limit: int = 5,
    ) -> None
```

Read-only, compile-time retrieval over the durable cross-run precedent store. On each `retrieve` it reads the current precedent notes via `PrecedentIndex.from_vault` and ranks them. `embed_fn` (`str -> vector`) enables the dense rung; the default `None` skips it, so importing/using `concursus` needs no model. `limit` defaults to `5`. Safe to call any number of times at compile time; deleting the notes simply empties the result.

#### `PrecedentRetriever.retrieve`

```python
def retrieve(
    self, text: str = "", *,
    key: Optional[str] = None, nodes: Optional[Sequence[str]] = None,
    status: Optional[str] = None, limit: Optional[int] = None,
) -> List[RetrievedPrecedent]
```

Return the most relevant prior runs for a query, ranked by the retrieval ladder.

- **Parameters:**
  - `text` — a free-text goal (lexical / dense).
  - `key` — an exact `trail_id` / family key (rung 1; short-circuits alone on a hit).
  - `nodes` — DAG-shape node names, folded into the lexical query.
  - `status` — restrict candidates to a run status (`completed` / `partial` / `failed`).
  - `limit` — overrides the retriever's `limit`.
- **Ladder:** (1) an exact structured key match returns that precedent alone at score `1.0`; (2) a BM25-ish lexical rank over payload docs (zero-score non-matches dropped, sorted by `-score` then `trail_id`); (3) a dense cosine rank over `embed_fn`, only if one was injected — wire in [`make_hashing_embed_fn`](#make_hashing_embed_fn) (or a real semantic embedder) to enable it.
- **Returns:** `List[RetrievedPrecedent]` — `[]` for an empty store, an empty query (no `key` **and** no tokens), or a query matching nothing.

```python
r = PrecedentRetriever("/vault")
r.retrieve("disk full on host", nodes=["triage", "remediate"])
r.retrieve(key="tkt_42")                          # -> [RetrievedPrecedent(method='structured', score=1.0)]
r.retrieve("oom crash", status="completed", limit=3)
PrecedentRetriever("/vault", embed_fn=my_embed).retrieve("rare goal")   # dense fallback when lexical misses
```

> **Gotchas.** `retrieve` returns `[]` when there is no `key` **and** the query tokens are empty (empty `text` and empty `nodes`) — so an empty `text` with a `status` filter but no `nodes` yields nothing. The structured-key rung honors the `status` filter: a key hit whose status ≠ the requested status falls **through** to the lexical rung. Lexical always wins over dense when it produces any positive-scoring match — the dense rung fires only when lexical returns nothing, and it recomputes `embed_fn` per document on every call (no caching).

### `make_hashing_embed_fn`

```python
def make_hashing_embed_fn(dim: int = 256) -> Callable[[str], List[float]]
```

A deterministic, offline, dependency-free `embed_fn` for the dense rung. Returns a `str -> vector` embedder built on the hashing trick: each token is hashed into one of `dim` buckets and counted, yielding an L2-normalizable bag-of-words vector. The hash uses a **stable content hash** (a fixed polynomial roll over the characters), **not** Python's per-process-salted `hash()`, so the same text embeds identically across processes and runs — the property `_cosine` and the tests rely on. It is not an ML model; it makes the dense rung *usable* so a lexically-disjoint but semantically-related precedent (cross-domain / cross-family warm start) gets a non-trivial cosine, entirely offline. A real semantic embedder can be injected in its place.

Wiring it in is an **explicit opt-in** — `PrecedentRetriever(vault, embed_fn=None)` still defaults `embed_fn=None`, so the dense rung stays **off** and the package imports with no model dependency. Not re-exported at the root; import from `concursus.state.precedent`.

- **Parameters:**
  - `dim` — the embedding dimensionality (bucket count); defaults to `256` (module constant `_HASH_DIM`).
- **Returns:** `Callable[[str], List[float]]` — a stateless, deterministic embedder.

```python
from concursus.state.precedent import make_hashing_embed_fn
from concursus import PrecedentRetriever

r = PrecedentRetriever("/vault", embed_fn=make_hashing_embed_fn())   # opt-in: dense rung now usable
r.retrieve("rare cross-domain goal")   # dense cosine fallback when the lexical rung misses
```

---

## `state.capture`

Source: [`../../src/concursus/state/capture.py`](../../src/concursus/state/capture.py)

The **source-agnostic post-run capture seam**. One frozen shape — `CaptureEnvelope` — describes *what* to persist (a plan, an agent response, a payload), and `capture` **dispatches** it to the already-shipped FileVault writer for that kind. `capture_run` is the post-run trigger that captures a frozen plan plus each node's payload and then runs the reciprocal-backlink post-pass; `gate_run_dir` is a read-only safety-net over the written notes; `load_payload_tiers` reads the persisted payload notes back.

`capture` is **not** a runtime and **not** a LangGraph graph — it is a ~dict dispatcher over the shipped [`state.filevault`](#statefilevault) / [`state.distill`](#statedistill) writers. It runs strictly **after** `Supervisor.run` returns, writes NOTES (never run records) into the run's own memory (`<run_dir>`, not any external knowledge vault), never mutates a frozen or running plan, and is opt-in with byte-for-byte-unchanged defaults. Stdlib only. Import from `concursus.state.capture`.

| Symbol | Kind | Summary |
|---|---|---|
| [`CaptureEnvelope`](#captureenvelope) | dataclass | The one source-agnostic capture shape (`source_kind` + `artifact` + `run_dir`). |
| [`CaptureError`](#captureerror) | exception | An empty/invalid or unwired `source_kind` (or empty `run_dir`). |
| [`adapt_plan`](#adapt_plan) | function | Wrap a compiled plan as a `PLAN` envelope. |
| [`adapt_payload`](#adapt_payload) | function | Wrap a node's frozen payload as a `PAYLOAD` envelope. |
| [`capture`](#capture) | function | Dispatch one envelope to the shipped FileVault writer; returns a note **path**. |
| [`capture_run`](#capture_run) | function | The post-run trigger: capture the plan + payloads, then run the backlink post-pass. |
| [`gate_run_dir`](#gate_run_dir) | function | Read-only safety-net gate over a run dir's notes. |
| [`load_payload_tiers`](#load_payload_tiers) | function | Read the persisted payload notes back into `{node: trust_tier}`. |

### `CaptureEnvelope`

```python
@dataclass(frozen=True)
class CaptureEnvelope:
    source_kind: str
    artifact: Any
    run_dir: str
    trail_id: str = "run"
    related: Optional[List[str]] = None
    date: str = ""
```

The one source-agnostic shape every capture flows through — *what* to persist (`artifact`) and *where* (`run_dir`), tagged by `source_kind`. A frozen dataclass. `source_kind` is one of `PLAN`, `AGENT_RESPONSE`, `PAYLOAD` (**wired**), or `AGENT_LOG`, `RUN_SUMMARY`, `BINDING` (**declared but unwired** — a `capture` on one raises `CaptureError`).

`__post_init__` raises `CaptureError` on an empty `source_kind` or empty `run_dir`.

- **Raises:** `CaptureError` — on an empty `source_kind` / `run_dir`.

### `CaptureError`

```python
class CaptureError(ValueError)
```

Raised for an empty/invalid `CaptureEnvelope` (empty `source_kind` / `run_dir`) or when `capture` is handed an envelope whose `source_kind` is declared but **unwired**. Subclasses `ValueError`.

### `adapt_plan`

```python
def adapt_plan(plan, run_dir, *, trail_id: str = "run", date: str = "") -> CaptureEnvelope
```

Wrap a compiled `ProvisioningPlan` as a `source_kind="PLAN"` `CaptureEnvelope` — a thin adapter. `capture()` routes it to the shipped [`capture_run_plan_note`](#capture_run_plan_note); this function itself writes nothing.

### `adapt_payload`

```python
def adapt_payload(
    node: str, payload: Any, run_dir, *, trust_tier: str = "", trail_id: str = "run",
    date: str = "", related: Optional[List[str]] = None,
) -> CaptureEnvelope
```

Wrap a node's frozen invoke `payload` as a `source_kind="PAYLOAD"` `CaptureEnvelope`, with `artifact=(node, payload, trust_tier)`. `capture()` routes it to [`capture_payload_note`](#capture_payload_note).

### `capture`

```python
def capture(env: CaptureEnvelope) -> str
```

Dispatch one `CaptureEnvelope` to the shipped FileVault writer for its `source_kind` and return the written note's **path**. A ~dict dispatcher over the already-shipped `filevault` seams — **not** a LangGraph graph and **not** a runtime. Raises `CaptureError` for a declared-but-unwired `source_kind`.

- **Returns:** the note's **path** (`str`).
- **Raises:** `CaptureError` — on an unwired `source_kind`.

```python
from concursus.state.capture import adapt_payload, capture

capture(adapt_payload("summarize", {"sop": "..."}, v.run_dir, trust_tier="GUARDED"))
# -> '/vault/runs/<slug>/summarize__payload.md'
```

### `capture_run`

```python
def capture_run(
    run_dir, *, plan=None, payloads: Optional[Dict[str, Any]] = None,
    trust_tiers: Optional[Dict[str, str]] = None, trail_id: str = "run", date: str = "",
    backlinks: bool = True, version_notes: bool = False,
) -> Dict[str, Any]
```

**The post-run capture trigger.** Capture the frozen `plan` (when given) and each entry in `payloads`, then — when `backlinks` is true — run the [`add_reciprocal_backlinks`](#add_reciprocal_backlinks) post-pass. Returns `{"paths": [...], "backlinks": n}` (the written note paths plus the count of producer notes amended), plus `"versioned": n` when `version_notes=True`. Pure post-run — it captures the frozen artifacts and mutates no topology.

- **Derive payloads from the plan's contract:** when `payloads` is `None` **and** the `plan` carries a `payload_contract`, the payloads are **derived** from that contract (the compiler *authors* the contract; capture merely *persists* it). `trust_tiers` overlays per-node tiers.
- **`version_notes`** *(opt-in, default OFF)*: snapshot the run's current top-level notes into the append-only version timeline AFTER the backlink pass, capturing the post-run amendments as history. OFF by default, so `capture_run` writes byte-identically to before (no `versions/` dir is created).
- **Returns:** `{"paths": List[str], "backlinks": int}` (`+ "versioned": int` when opted in).

```python
from concursus.state.capture import capture_run

capture_run(v.run_dir, plan=frozen_plan)          # payloads derived from plan.payload_contract
capture_run(v.run_dir, payloads={"summarize": {"sop": "..."}}, trust_tiers={"summarize": "GUARDED"})
```

### `gate_run_dir`

```python
def gate_run_dir(run_dir) -> Dict[str, object]
```

**Read-only safety-net gate.** Scan a run dir's notes and flag missing frontmatter and dangling same-dir `.md` links. Returns `{"ok": bool, "checked": n, "issues": [...]}`. Read-only: it reports, never repairs or mutates.

```python
from concursus.state.capture import gate_run_dir

gate_run_dir(v.run_dir)   # -> {'ok': True, 'checked': 7, 'issues': []}
```

### `load_payload_tiers`

```python
def load_payload_tiers(run_dir) -> Dict[str, str]
```

Read the persisted `__payload.md` notes back into `{node: trust_tier}` — a read-back primitive (the inverse of [`capture_payload_note`](#capture_payload_note)'s tier stamp; pure read).

```python
from concursus.state.capture import load_payload_tiers

load_payload_tiers(v.run_dir)   # -> {'summarize': 'GUARDED', ...}
```

---

## `state.transfer`

Source: [`../../src/concursus/state/transfer.py`](../../src/concursus/state/transfer.py)

*(opt-in, default off)* The **session-end knowledge-transfer connector** — turns a finished run's episodic notes into permanent Slipbox knowledge via a knowledge-consolidation sub-agent, on both request-completion and termination. Four pieces: the `slipbox_transfer` terminal node + fail-closed acceptance contract (**C1**), the consolidation-sub-agent registration (**C4**), the episodic-log export to the sub-agent's ingestion (**C2**), and the strictly-outer `synthesize` trigger + reaper/next-boot backstop (**C3**), plus a session rollup that is not green unless the transfer ran and was accepted. Stdlib only — concursus never imports the consolidation runtime; ingestion is an injected `admit_fn`. Import from `concursus.state.transfer`. For the narrative, see [Guide: Session-End Knowledge Transfer](../guides/knowledge-transfer.md).

> This is the parity-safe subset for the public mirror: the export is the **local-inbox raw-notes** path (a digest-view bundle and an S3-push variant depend on subsystems not present here). Everything is a **compile-time** manifest/DAG builder (C1/C4) or a **strictly-outer** episode-boundary observer / post-run reader (C2/C3/rollup) — never an in-`Supervisor.run` mutation (INV-1..5). Wire none of it and a run is byte-for-byte unchanged.

| Symbol | Kind | Summary |
|---|---|---|
| [`build_slipbox_transfer_manifest`](#build_slipbox_transfer_manifest) | function | *(C1)* Author the `slipbox_transfer` MCP terminal-node manifest with a fail-closed acceptance contract. |
| [`wire_slipbox_transfer_terminal`](#wire_slipbox_transfer_terminal) | function | *(C1)* Wire the node as the sole DAG sink over the run's current sinks. |
| [`slipbox_transfer_acceptance_fn`](#slipbox_transfer_acceptance_fn) | function | *(C1)* Predicate that narrows the Supervisor's QA gate to just the transfer node. |
| [`register_slipbox_foundry`](#register_slipbox_foundry) | function | *(C4)* Register the consolidation sub-agent so `match_task("slipbox_transfer")` resolves it. |
| [`export_run_log`](#export_run_log) | function | *(C2)* Copy the run's episodic notes byte-identical into the ingestion inbox (idempotent). |
| [`distill_export`](#distill_export) | function | *(C2)* Wire `distill_store` — the cross-run precedent — alongside the export. |
| [`TransferTriggerSink`](#transfertriggersink) | class | *(C3)* An `EventSink` that fires the export at the `decision`/`route=="synthesize"` boundary. |
| [`sweep_untransferred_runs`](#sweep_untransferred_runs) | function | *(C3)* Reaper/next-boot backstop: transfer every run lacking a success marker. |
| [`transfer_run`](#transfer_run) | function | *(C3)* The idempotent digest primitive — export then mark. |
| [`run_needs_transfer`](#run_needs_transfer) / [`mark_transferred`](#mark_transferred) | function | *(C3)* The marker gate + its writer (`.slipbox_transferred`, a non-`.md` sentinel). |
| [`recover_trail_id`](#recover_trail_id) | function | *(C3)* Recover a run's real `trail_id` from its note `lineage:` frontmatter. |
| [`session_overall_ok`](#session_overall_ok) / [`transfer_node_ok`](#transfer_node_ok) | function | *(rollup)* The transfer-inclusive session verdict (pure read; fail-closed). |
| `SLIPBOX_TRANSFER_NODE` · `SLIPBOX_FOUNDRY_CAPABILITIES` · `CONSOLIDATOR_JOB_DICT_KEYS` · `CONSOLIDATOR_COMPLETE_STATE` | constants | The node id, the served capabilities, the real job-dict parity set, and the terminal-success state (`"complete"`). |

### `build_slipbox_transfer_manifest`

```python
def build_slipbox_transfer_manifest(
    *, agent_runtime_arn=None, container_uri=None, role_arn=None,
    qualifier="DEFAULT", name=SLIPBOX_TRANSFER_NODE,
) -> AgentManifest
```

*(C1)* Author + `validate()` an MCP terminal-node manifest whose `contract.outputs` mirror the consolidation sub-agent's real job dict (`job_id` / `state` / `result_path` / `last_error`) with per-field `acceptance`: `state` must be in `["complete"]` and `result_path` must be `non_empty` — MANDATORY + FAIL-CLOSED. `side_effecting=True` (enters the Trust Ladder at `L0_SHADOW`). Fail-closed on the hosting handle: pass neither `agent_runtime_arn` nor `container_uri` and `validate()` raises `ManifestError` (never a fabricated ARN). The declared output keys are a **subset** of [`CONSOLIDATOR_JOB_DICT_KEYS`](#statetransfer) so the contract can't drift from the real API.

### `wire_slipbox_transfer_terminal`

```python
def wire_slipbox_transfer_terminal(
    dag, manifest, *, producer_outputs=None, node=SLIPBOX_TRANSFER_NODE,
) -> AgentManifest
```

*(C1)* Add `node` to `dag` and, for each current sink, add **both** the DAG edge `producer → node` **and** a `spec.depends_on` on a distinct per-producer input `from_<producer>` — both required by `check_alignment` (edge + single-writer gates). The producers default to `dag.sinks()` captured *before* the node is added, so afterward the transfer node is the sole sink. `producer_outputs` maps each sink to the output field its edge should read (a producer absent from the map falls back to the conventional `result` field). The per-producer inputs are declared on the **manifest** (`contract.inputs`) — the bare DAG node carries no I/O — and `check_alignment` reads consumer inputs from there. Returns the mutated `manifest` (its `spec.depends_on` + `contract.inputs` populated); the `dag` is mutated in place. Mutates only the pre-freeze `dag` + `manifest` (INV-1).

### `slipbox_transfer_acceptance_fn`

```python
def slipbox_transfer_acceptance_fn(node: str) -> bool
```

*(C1)* Pass to `Supervisor(check_acceptance=True, acceptance_fn=slipbox_transfer_acceptance_fn)` to QA-gate ONLY the transfer node. `check_acceptance` is the master switch — the predicate is inert without it (default-off).

### `register_slipbox_foundry`

```python
def register_slipbox_foundry(
    registry, ledger, *, manifest=None,
    fingerprint="slipbox-foundry-dev", deployed_at="1970-01-01T00:00:00Z", arn=None,
) -> AgentManifest
```

*(C4)* Two append-only writes: `registry.register_agent(manifest, capabilities=SLIPBOX_FOUNDRY_CAPABILITIES)` + `ledger.record(name=manifest.name, …)`, so `match_task("slipbox_transfer")` resolves the consolidation sub-agent with `arn` as its runtime handle. Capabilities key on the agent **name**, so the registered name and the ledger name must be identical (both `manifest.name`). The dev `ledger.record` path bypasses the create-time trust gate; the `L0_SHADOW` seed governs live dispatch, not resolution.

### `export_run_log`

```python
def export_run_log(run_dir, target_dir, *, admit_fn=None, objective=None, trail_id="run") -> dict
```

*(C2)* Copy every **top-level** `*.md` note under `run_dir` into `target_dir`, byte-identical (a non-recursive glob, so derived sidecar trees like `versions/` and `index/` never leak). Returns `{"members", "objective", "admitted"}`; `objective` defaults to `hive-session-<trail_id>`. When `admit_fn` is given it is called with the on-disk member paths after they land. Re-export is idempotent (skip-if-identical write preserves inode/mtime so the sub-agent dedups). Pure post-run (INV-4).

### `distill_export`

```python
def distill_export(store, *, vault_path=None) -> str
```

*(C2)* The thin explicit caller for [`distill_store`](#distill_store): fold the finished `store` into one precedent note under `<vault>/precedents/` (the planner's cross-run retrieval path). Pure post-run.

### `TransferTriggerSink`

```python
class TransferTriggerSink:
    def __init__(self, run_dir, target_dir, *, admit_fn=None, trail_id="run", date=""): ...
    def emit(self, event) -> None: ...
```

*(C3)* An opt-in `EventSink` (compose it with [`FanOutEventSink`](governor.md#fanouteventsink)) that fires the export on the `decision` event whose `route == "synthesize"` — the true end of the run (read off the plain-dict event, NOT `episode_end.done`, since a done round can route back to the planner). Observer-only (INV-3): reads the frozen event VALUE, writes the external inbox + a run-dir marker; fires at most once (marker-gated); errors are swallowed by the loop's emit guard. Result on `.last_result`.

### `sweep_untransferred_runs`

```python
def sweep_untransferred_runs(runs_root, target_dir_for, *, admit_fn=None, trail_id_for=None, when="") -> list
```

*(C3)* The reaper/next-boot backstop: transfer every run under `runs_root` still lacking a success marker (a graceful-synthesize miss or hard teardown can leave one untransferred). Recovers each run's real `trail_id` via [`recover_trail_id`](#recover_trail_id) so the backstop `objective` matches the graceful trigger's and the sub-agent dedups. `target_dir_for(run_dir, trail_id)` maps a run to its inbox target; `trail_id_for(run_dir)` overrides recovery.

### `transfer_run`

```python
def transfer_run(run_dir, target_dir, *, admit_fn=None, trail_id="run", when="") -> dict
```

*(C3)* The idempotent digest primitive the triggers fire: [`export_run_log`](#export_run_log) then [`mark_transferred`](#mark_transferred) on success. A run already marked is a no-op (`{"skipped": True}`), so at-least-once converges to exactly-once.

### `run_needs_transfer`

```python
def run_needs_transfer(run_dir) -> bool
```

*(C3)* `True` iff `run_dir` is a real run dir with no `.slipbox_transferred` marker yet. Pure read — the gate the backstop consults.

### `mark_transferred`

```python
def mark_transferred(run_dir, *, objective="", when="") -> str
```

*(C3)* Write the success marker `.slipbox_transferred` — deliberately **not** a `*.md` file, so the note globs and loaders never see it (it can never leak into a bundle or parse as a `Record`).

### `recover_trail_id`

```python
def recover_trail_id(run_dir) -> Optional[str]
```

*(C3)* Recover a run's real `trail_id` from the first note's `lineage:` frontmatter. The run dir is named `_slug(session_id)` while the `trail_id` is a *different* transform of `session_id`, so `run_dir.name` is not the trail_id; this reads the authoritative value. `None` for an empty/non-run dir.

### `session_overall_ok`

```python
def session_overall_ok(store, *, node=SLIPBOX_TRANSFER_NODE, plan_order=None) -> dict
```

*(rollup)* The transfer-inclusive verdict: `{"overall_ok", "transfer_ok", "transfer_present", "work_complete", "completed", "total_completed"}`. `overall_ok` is `transfer_ok and work_complete`; pass `plan_order` to also require every planned node completed. Fail-closed (no transfer node ⇒ not green). Pure read over a finished store — never a runtime gate.

### `transfer_node_ok`

```python
def transfer_node_ok(store, *, node=SLIPBOX_TRANSFER_NODE) -> bool
```

*(rollup)* `True` iff the transfer node is in `store.completed()` (it passed the C1 gate) AND its recorded output is `state == "complete"` with a non-empty `result_path` (a defense-in-depth re-check). Pure read.

---

## Invariants at a glance

- **One append-only log, many disposable derivations.** The `Record` log is the single source of truth; the `{node: latest validated output}` projection, `RunGraph`, `RunIndex`, the SQLite DBs, and the precedent hub are all rebuildable derivations — delete any and regenerate.
- **Append-only, never edit.** A re-`put` is a *new* `Record`; a content-identical re-put becomes a `dedup` no-op, never an error or an edit.
- **`seq`, not `timestamp`, is the ordering tie-breaker** for the in-process and Memory stores — so concurrent branch/retry writes sharing an AgentCore `eventTimestamp` resolve identically on every replay. (FileVault carries no `seq` and orders on `timestamp`.)
- **`completed()` is latest-overall; `get()` is latest-validated.** A node whose last attempt failed is not `completed()`, even if an earlier attempt validated.
- **Checkpoints are derived snapshots.** A `checkpoint` event never enters the projection; raw compacted events are never deleted. `MemoryStateStore.checkpoint` assumes exactly one writer per session.
- **Two orthogonal structures:** `RunGraph` is the *data-dependency DAG* (producer→consumer via `AgentRef`); `RunIndex` is the *execution tree* (a node's retries/fan-outs/branches, keyed by `address`).
- **The memory loop is strictly outside a run:** `distill_*` writes only *after* `Supervisor.run` returns (never into a run dir, never a replay); `PrecedentRetriever` reads only *before* a plan is frozen (never a runtime replan).
- **`slipbox_form` defaults differ:** `True` for `FileVaultStateStore`; `False` for `distill_run` / `distill_store` / `render_precedent_hub`.
- **Capture is a dispatcher, not a runtime.** `state.capture` (`capture` / `capture_run`) is a ~dict dispatcher over the shipped FileVault writers — no new runtime, no LangGraph. It runs strictly post-run, writes NOTES (never run records) into the run's own memory (`<run_dir>`), never mutates a frozen/running plan, and is opt-in with byte-for-byte-unchanged defaults. A payload note is a **non-record** (`concursus_note_kind: payload`) the record parser refuses, so it never leaks into replay; `add_reciprocal_backlinks` only *projects* recorded `consumes` edges, adding no new data.
- **The knowledge-transfer connector (`state.transfer`) never touches a running plan.** C1/C4 are compile-time (author a manifest, wire the DAG, seed the registry/ledger — strictly before `assemble`); C2 reads the finished log / on-disk notes and writes an EXTERNAL target (never re-puts a `Record`, INV-4); C3's `TransferTriggerSink` observes the frozen `decision` VALUE strictly *between* episodes (INV-3) and the reaper/next-boot sweep is a separate post-run pass; the rollup is a pure read. concursus never imports the consolidation runtime (ingestion is an injected `admit_fn`), and the success marker is a non-`.md` sentinel invisible to the note loaders — so wiring none of it leaves a run byte-for-byte unchanged.
- **Every opt-in addition is default-off; the default behavior is byte-for-byte unchanged.** The typed `RunEvent` contract emits nothing without an `EventSink` wired into the outer governor loop; `coordination` notices are written only via `append_coordination_notice` and are keyed under `__coordination__` with a non-`validated` status, so they never touch `completed()` / `get()` / the projection or perturb the prefix `recompile` pins; the FileVault `versions/` timeline (and `revert_note`) never creates a `versions/` dir unless `versioned=True`, and its snapshots are stamped non-records globbed non-recursively, so they can never leak into resume/replay; forward-only note-schema migration is a read-time no-op while v1 is current; and `get_run_snapshot` / `redact_snapshot` are pure offline reads over the note SSOT. Concursus remains a **compiler, not a runtime governor** — none of these adds a runtime signal into a running plan.

## See also

- [Guide: Durable Run State](../guides/durable-state.md) — the `StateStore` seam, the three backends, replay-resume, and the disposable projections, in narrative form.
- [Guide: Session-End Knowledge Transfer](../guides/knowledge-transfer.md) — the `state.transfer` connector in narrative form: the `slipbox_transfer` node, the export, the trigger, and the session rollup.
- [Guide: Compiling & Running a Team](../guides/compiling-and-running.md) — where the store fits into `resolve → assemble → freeze → supervise`.
- [Guide: The Governor](../guides/governor.md) — the strictly-outer standing loop that schedules runs over this state.
- [Guide: Deploying to AWS Bedrock AgentCore](../guides/deploying-to-agentcore.md) — where `MemoryStateStore`'s AgentCore Memory backend lives.
- [Guide: Command-Line Interface](../guides/cli.md) — the `concursus` commands.
- [`core` reference](core.md) — `AgentDAG`, `AgentManifest`, and the resolver whose `AgentRef` edges the log records.
- [Core Concepts](../concepts.md) — the DAG / manifest / plan / state vocabulary and invariants.
- [Overview](../overview.md) and the [documentation index](../README.md).
