"""The **StateStore** — durable, addressable run state for the supervisor.

Run state is a single append-only log of validated agent outputs plus a rebuildable
projection (``{node: latest validated output}``) — the slipbox's single-source-of-truth /
derived-DB discipline. Two backends share one:class:`StateStore` Protocol:
:class:`InProcessStateStore` (the zero-dependency, offline default that replaces the
supervisor's plain ``outputs`` dict) and :class:`MemoryStateStore` (opt-in, AgentCore
Memory-backed, so a run survives micro-VM teardown and resumes by *replaying* its event
log). Each :class:`Record` also persists its resolved ``AgentRef`` edges (``consumes``),
turning the log into a queryable run graph (see :mod:`concursus.rungraph`).

Pure-Python core: the module imports only the stdlib; boto3 is imported lazily inside
:class:`MemoryStateStore` (the optional ``[agentcore]`` extra) and every unit test injects a
fake client, so nothing here ever touches AWS.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import re
import threading
import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple


# -- typed run-state vocabulary ---------------------------------------------
class StateStoreError(ValueError):
    """Raised when a :class:`Record` field carries a value outside its typed vocabulary."""


class RecordStatus(str, Enum):
    """The lifecycle status of a :class:`Record` (a ``str`` subclass, so ``== "validated"``
    and all-string metadata projection keep working untouched)."""

    VALIDATED = "validated"
    FAILED = "failed"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:  # keep f-string / str() output == the bare value (Py3.11+ change)
        return self.value


class RecordType(str, Enum):
    """The kind of a :class:`Record` (a ``str`` subclass; unknown values widen-and-warn rather
    than reject, so a future record kind never hard-fails a run)."""

    AGENT_OUTPUT = "agent_output"
    DEDUP = "dedup"
    CHECKPOINT = "checkpoint"

    def __str__(self) -> str:  # keep f-string / str() output == the bare value (Py3.11+ change)
        return self.value


def _coerce_status(value: Any) -> str:
    """Coerce ``value`` to a :class:`RecordStatus`; raise :class:`StateStoreError` if unknown."""
    if isinstance(value, RecordStatus):
        return value
    try:
        return RecordStatus(value)
    except ValueError as exc:
        allowed = sorted(s.value for s in RecordStatus)
        raise StateStoreError(f"unknown record status {value!r} (allowed: {allowed})") from exc


def _coerce_record_type(value: Any) -> str:
    """Coerce ``value`` to a :class:`RecordType`; an unknown value is kept verbatim with a
    warning (widen-and-warn) so a novel record kind never hard-fails a run."""
    if isinstance(value, RecordType):
        return value
    try:
        return RecordType(value)
    except ValueError:
        warnings.warn(
            f"unknown record_type {value!r}; keeping it verbatim "
            f"(known: {sorted(t.value for t in RecordType)})",
            stacklevel=3,
        )
        return value


# -- content addressing -----------------------------------------------------
def content_hash(output: dict) -> str:
    """SHA-256 of the canonical JSON of ``output`` (``json.dumps(sort_keys=True)``).

    A stable content address for a node output: identical outputs hash identically, so a
    re-``put`` of an unchanged output is a detectable no-op (dedup / memoization / staleness).
    """
    canonical = json.dumps(output, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# -- record model -----------------------------------------------------------
@dataclass
class Record:
    """One validated (or failed) node output plus its slipbox metadata.

    Attributes:
        node: The DAG node id this output addresses (the slipbox "semantic id").
        output: The verbatim agent output (the Blob payload's inner form).
        attempt: The retry sequence for ``node`` (1-based, auto-incremented per put).
        status: ``validated`` | ``failed`` | ``superseded``.
        record_type: ``agent_output`` (default), ``dedup`` for a no-op re-put, etc.
        schema: The output schema tag (trustworthy — validation ran before admission).
        producer: The upstream node id, when this record is a producer projection.
        consumes: The resolved ``AgentRef`` edges as ``"producer:$.jsonpath"`` strings.
        supersedes: The prior attempt's ``event_id`` (deterministic replay ordering).
        content_hash: :func:`content_hash` of ``output``.
        timestamp: The event timestamp (monotonic in-process; AgentCore ``eventTimestamp`` when
            backed — display only, NOT the ordering key for a Memory-backed store).
        seq: A local strict-monotonic sequence assigned by the store on ``put`` (and by
            :meth:`MemoryStateStore.replay` in log order). This — not ``timestamp`` — is the
            deterministic tie-breaker in :func:`_is_newer`, so concurrent branch/retry writes
            that share an AgentCore ``eventTimestamp`` still resolve identically on every replay.
            ``None`` for a record not sourced from a store (e.g. a hand-built one).
        event_id: The backing Memory event id (``None`` for the in-process store).
        address: The Folgezettel execution address — a materialized path (default the ``node``
            name; a retry / fan-out / branch appends a ``"/"`` segment, e.g. ``"map/0"``). Its
            prefix-derivable parent lets :class:`~concursus.runindex.RunIndex` reconstruct the
            retry/fan-out/branch tree; the last segment maps to an AgentCore ``branch`` name.
    """

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
    #: Failure classification (opt-in; Supervisor ``on_error='record'`` only): ``"crash"`` — this
    #: node's own invoke / validate / ARN-integrity raised — vs ``"hold"`` — the node was never
    #: invoked because an upstream producer it consumes failed or was itself held/blocked (a
    #: pruned-subtree skip, not this node's fault). ``None`` for a validated record or a legacy
    #: failed record (``Supervisor.summary()`` then derives it from ``blocked_on`` presence).
    #: Mirrors ``blocked_on`` exactly: carried on the in-process record via :func:`_apply_meta`,
    #: NOT projected to charset-restricted AgentCore metadata.
    failure_class: Optional[str] = None
    #: Checkpoint-compaction epoch (C-4). The monotonic window id current when this event was
    #: written; a ``CHECKPOINT`` event compacts everything written at its own ``epoch`` and rotates
    #: the store to ``epoch + 1``, so a warm resume can bound its tail fetch with an EQUALS_TO
    #: filter on ``epoch``. ``None`` for a hand-built record or a store that never checkpointed.
    epoch: Optional[int] = None

    def __post_init__(self) -> None:
        """Self-validate the typed fields: coerce ``status``/``record_type`` through their enums.

        Because both enums subclass ``str``, the coerced values compare equal to their bare
        strings (``== "validated"``) and serialize as plain strings, so existing comparisons and
        :func:`_build_metadata` keep working untouched. An unknown ``status`` raises
        :class:`StateStoreError`; an unknown ``record_type`` widens-and-warns (kept verbatim).
        """
        self.status = _coerce_status(self.status)
        self.record_type = _coerce_record_type(self.record_type)


# The metadata keys :meth:`StateStore.put` merges from ``meta`` onto a :class:`Record`.
_META_KEYS: Tuple[str, ...] = (
    "producer",
    "consumes",
    "schema",
    "record_type",
    "status",
    "address",
    "blocked_on",
    "failure_class",
)

# The ``record_type`` marking a content-hash no-op re-put (identical to the latest output).
_DEDUP_RECORD_TYPE = "dedup"

# C-4: the ``record_type`` of a checkpoint-compaction event (a derived snapshot of the log).
_CHECKPOINT_RECORD_TYPE = "checkpoint"


#: AgentCore metadata string values must match this charset; anything else is sanitized to ``_``
#: for the (filterable) event metadata. The lossless value rides in the Blob ``__meta__`` sidecar.
_SAFE_METADATA_VALUE = re.compile(r"[^a-zA-Z0-9\s._:/=+@-]")


def _metadata_equals_filter(**pairs: str) -> dict:
    """Build a ``ListEvents`` ``FilterInput`` that ANDs an ``EQUALS_TO`` per metadata key.

    Only the API-supported ``EQUALS_TO`` operator is used (there is no range/``>`` operator), and
    at most 5 expressions are allowed — C-4 uses one or two, well within the limit.
    """
    return {
        "eventMetadata": [
            {
                "left": {"metadataKey": k},
                "operator": "EQUALS_TO",
                # AgentCore metadata values are the typed MetadataValue union, not bare strings;
                # sanitize to match what was stored (a no-op for the safe C-4 keys record_type/epoch).
                "right": {"metadataValue": {"stringValue": _SAFE_METADATA_VALUE.sub("_", v)}},
            }
            for k, v in pairs.items()
        ]
    }

# The Folgezettel address separator (a materialized path: parent = strip the last segment).
_ADDR_SEP = "/"


def _apply_meta(record: Record, meta: Optional[dict]) -> None:
    """Merge the recognized ``meta`` keys (:data:`_META_KEYS`) onto ``record`` in place."""
    if not meta:
        return
    for key in _META_KEYS:
        value = meta.get(key)
        if value is None:
            continue
        if key == "consumes":
            value = list(value)
        elif key == "status":
            value = _coerce_status(value)  # re-validate: __post_init__ already ran
        elif key == "record_type":
            value = _coerce_record_type(value)
        setattr(record, key, value)


# -- record indexing --------------------------------------------------------
def _is_newer(candidate: Record, current: Optional[Record]) -> bool:
    """Whether ``candidate`` supersedes ``current``: higher attempt, then higher local ``seq``,
    then newer ``timestamp``.

    The tie-breaker is the store-assigned strict-monotonic ``seq`` (put / replay order), NOT the
    raw ``timestamp``: an AgentCore ``eventTimestamp`` can tie for concurrent branch/retry writes,
    so ordering on it alone would resolve NON-DETERMINISTICALLY on replay. ``seq`` gives every
    store the same total order the :class:`InProcessStateStore`'s monotonic clock already did.
    ``timestamp`` remains the fallback for records with no ``seq`` (hand-built, or the file-vault
    store whose monotonic clock is its ``timestamp``). Remaining ties (equal attempt + seq +
    timestamp) resolve to ``candidate`` — records are iterated in append order, so last-seen wins.
    """
    if current is None:
        return True
    if candidate.attempt != current.attempt:
        return candidate.attempt > current.attempt
    if candidate.seq is not None and current.seq is not None and candidate.seq != current.seq:
        return candidate.seq > current.seq
    ct = candidate.timestamp if candidate.timestamp is not None else 0
    pt = current.timestamp if current.timestamp is not None else 0
    if ct != pt:
        return ct > pt
    return True


def _index_records(
    records: List[Record],
) -> Tuple[Dict[str, Record], Dict[str, Record], Dict[str, int]]:
    """Index ``records`` by node into ``(latest_overall, latest_validated, max_attempt)``.

    ``latest_overall`` is the newest record per node regardless of status (drives
    ``completed()`` — a node is complete only if its *latest* record validated);
    ``latest_validated`` is the newest ``status == "validated"`` record per node (drives
    ``get()``); ``max_attempt`` is the highest attempt seen per node (drives auto-increment).
    """
    latest_overall: Dict[str, Record] = {}
    latest_validated: Dict[str, Record] = {}
    attempts: Dict[str, int] = {}
    for r in records:
        attempts[r.node] = max(attempts.get(r.node, 0), r.attempt)
        if _is_newer(r, latest_overall.get(r.node)):
            latest_overall[r.node] = r
        if r.status == "validated" and _is_newer(r, latest_validated.get(r.node)):
            latest_validated[r.node] = r
    return latest_overall, latest_validated, attempts


# -- protocol ---------------------------------------------------------------
class StateStore(Protocol):
    """Durable run-state seam the supervisor writes through and resumes from.

    Two backends implement it: :class:`InProcessStateStore` (default, offline) and
    :class:`MemoryStateStore` (AgentCore Memory-backed). ``put`` admits a validated output;
    ``get`` returns the latest validated output for a node; ``completed`` is the validated
    frontier; ``records`` exposes the full log for the run graph / context assembly.
    """

    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None:
        """Admit ``output`` for ``node`` (auto-incrementing its attempt)."""
        ...

    def get(self, node: str) -> dict:
        """Latest validated output for ``node``; raises ``KeyError`` if absent."""
        ...

    def completed(self) -> Set[str]:
        """Nodes whose latest record is ``status == "validated"``."""
        ...

    def records(self) -> List[Record]:
        """Every record (the append-only log), for the run graph / context."""
        ...


# -- in-process store (default) ---------------------------------------------
class InProcessStateStore:
    """Zero-dependency, offline :class:`StateStore` — the supervisor default.

    Holds an append-only ``list[Record]`` (the source of truth) and a projection dict
    ``{node: latest validated output}`` (the read model). ``put`` auto-increments the node's
    attempt, computes a :func:`content_hash`, and — when the output is identical to the node's
    latest validated output — still records it but marks it a :data:`_DEDUP_RECORD_TYPE`
    no-op (never an error). Everything lives in memory; nothing touches AWS.
    """

    def __init__(self) -> None:
        self._records: List[Record] = []
        self._projection: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._clock: int = 0
        # Reentrant: guards the read-then-write bodies (attempt++ / dedup lookup / append /
        # projection) so a future concurrent-dispatch supervisor cannot lose-update this
        # in-memory state. RLock (not fcntl/OCC) — these are single-process in-memory stores.
        self._lock = threading.RLock()

    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None:
        with self._lock:
            self._clock += 1
            attempt = self._attempts.get(node, 0) + 1
            self._attempts[node] = attempt
            chash = content_hash(output)
            dedup = node in self._projection and content_hash(self._projection[node]) == chash

            record = Record(
                node=node,
                output=dict(output),
                attempt=attempt,
                content_hash=chash,
                timestamp=self._clock,
                seq=self._clock,
            )
            _apply_meta(record, meta)
            if dedup and record.record_type == "agent_output":
                record.record_type = _DEDUP_RECORD_TYPE

            self._records.append(record)
            if record.status == "validated":
                self._projection[node] = record.output

    def get(self, node: str) -> dict:
        with self._lock:
            if node not in self._projection:
                raise KeyError(node)
            return self._projection[node]

    def completed(self) -> Set[str]:
        with self._lock:
            latest_overall, _, _ = _index_records(self._records)
            return {node for node, r in latest_overall.items() if r.status == "validated"}

    def records(self) -> List[Record]:
        with self._lock:
            return list(self._records)


# -- agentcore memory store (opt-in) ----------------------------------------
class MemoryStateStore:
    """AgentCore Memory-backed :class:`StateStore` — durable, resumable run state.

    Each ``put`` appends one **Blob** event (``payload=[{"blob": json({node: output})}]``)
    plus string metadata (node, attempt, status, record_type, schema, producer, ``consumes``
    joined on ``","``, content_hash, supersedes). The event log is the single source of truth;
    a cached projection is the read model, (re)built by :meth:`replay` — which paginates
    ``list_events`` over the session and keeps the latest validated record per node. Resume is
    just replay: a fresh store over the same ``(memory_id, actor_id, session_id)`` reconstructs
    the prior run's ``completed()`` / ``get()``. ``get``/``completed``/``records`` lazily replay
    once; ``put`` updates the projection in place so a get right after a put reflects it.

    Blob (not Conversational) is deliberate: Conversational payloads trigger long-term
    extraction, which we do not want for verbatim run state.
    """

    def __init__(
        self,
        *,
        memory_id: str,
        session_id: str,
        actor_id: str,
        client: Any = None,
    ) -> None:
        self._memory_id = memory_id
        self._session_id = session_id
        self._actor_id = actor_id
        self._client = client  # default constructed lazily in :meth:`_ensure_client`
        self._records: List[Record] = []
        self._projection: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._last_event_id: Dict[str, str] = {}
        self._event_id_by_address: Dict[str, str] = {} # FZ address -> event id (branch roots)
        # Local strict-monotonic sequence (mirrors InProcessStateStore._clock): assigned per
        # put and per replayed event, it — not the AgentCore eventTimestamp — is the ordering
        # tie-breaker, so concurrent branch/retry writes resolve DETERMINISTICALLY on replay.
        self._clock: int = 0
        # C-4 checkpoint-compaction: the current epoch (window id) tagged onto every put. A
        # :meth:`checkpoint` compacts the window at ``_epoch`` and rotates to ``_epoch + 1``, so a
        # warm :meth:`replay` re-hydrates from the latest checkpoint and fetches only the events
        # tagged with the checkpoint's epoch (a bounded EQUALS_TO filter). ``0`` until the first
        # checkpoint; a store that never checkpoints stays epoch 0 and resumes by full rebuild.
        self._epoch: int = 0
        self._loaded = False
        # Reentrant: guards the read-then-write bodies (attempt++ / append / projection /
        # replay) so a future concurrent-dispatch supervisor cannot lose-update the cached
        # read model. RLock (not fcntl/OCC) — the durable log is AgentCore's; this only
        # guards this process's in-memory cache.
        self._lock = threading.RLock()

    # -- writes -------------------------------------------------------------
    def put(self, node: str, output: dict, *, meta: Optional[dict] = None) -> None:
        meta = meta or {}
        with self._lock:
            self._clock += 1
            attempt = self._attempts.get(node, 0) + 1
            self._attempts[node] = attempt

            record = Record(
                node=node,
                output=dict(output),
                attempt=attempt,
                content_hash=content_hash(output),
                seq=self._clock,
                epoch=self._epoch,  # C-4: stamp the current checkpoint window
            )
            _apply_meta(record, meta)
            record.supersedes = meta.get("supersedes") or self._last_event_id.get(node)

            # FZ address -> AgentCore branch: a sub-address (has a parent segment) becomes a
            # branch off its parent's event, so the retry/fan-out/branch tree lives in the log.
            branch = meta.get("branch")
            if branch is None and record.address and _ADDR_SEP in record.address:
                parent, _, name = record.address.rpartition(_ADDR_SEP)
                root_event = self._event_id_by_address.get(parent)
                if root_event:
                    branch = {"name": name, "rootEventId": root_event}

            metadata = _build_metadata(record)
            # __meta__ sidecar: the LOSSLESS record fields (AgentCore metadata is charset-sanitized).
            payload = [{"blob": json.dumps({node: record.output, "__meta__": metadata})}]
            response = self._create_event(metadata=metadata, payload=payload, branch=branch)
            record.event_id = response.get("eventId")
            record.timestamp = response.get("eventTimestamp")

            self._records.append(record)
            if record.event_id:
                self._last_event_id[node] = record.event_id
                self._event_id_by_address[record.address or node] = record.event_id
            if record.status == "validated":
                self._projection[node] = record.output

    # -- reads --------------------------------------------------------------
    def get(self, node: str) -> dict:
        with self._lock:
            self._ensure_loaded()
            if node not in self._projection:
                raise KeyError(node)
            return self._projection[node]

    def completed(self) -> Set[str]:
        with self._lock:
            self._ensure_loaded()
            latest_overall, _, _ = _index_records(self._records)
            return {node for node, r in latest_overall.items() if r.status == "validated"}

    def records(self) -> List[Record]:
        with self._lock:
            self._ensure_loaded()
            return list(self._records)

    # -- checkpoint-compaction (C-4) ----------------------------------------
    def checkpoint(self) -> Optional[str]:
        """Write a compaction CHECKPOINT for the current epoch and rotate to the next (C-4).

        SINGLE-WRITER-PER-SESSION — this store's ordering already relies on one writer per
        ``(memory_id, actor_id, session_id)`` (the local ``_clock`` seq, not the ambiguous
        AgentCore ``eventTimestamp``, is the tie-breaker), so ``checkpoint`` is called synchronously
        by that one writer. It appends ONE event with ``record_type=checkpoint`` whose Blob payload
        carries the COMPACTED latest-per-node records as-of now (one :class:`Record` per node — the
        latest-overall, so ``completed()``/``get()``/attempts reconstruct identically), tagged with
        the CURRENT ``_epoch``; then it rotates ``_epoch += 1`` so every subsequent :meth:`put`
        carries the new window. The raw events are NEVER deleted — the append-only log stays the
        single source of truth (INV-5); the checkpoint is a derived snapshot that only makes a warm
        :meth:`replay` cheaper. Returns the checkpoint event id (``None`` if there is nothing to
        compact yet).

        Bounded-resume payoff: because everything written at the checkpoint's epoch is captured in
        the snapshot and nothing more is ever tagged with that epoch (the rotation guarantees it), a
        later resume re-hydrates from the snapshot and re-reads ONLY that epoch's events via a single
        ``EQUALS_TO`` filter — O(events-in-window), never the whole log.
        """
        with self._lock:
            self._ensure_loaded()
            if not self._records:
                return None
            latest_overall, _, attempts = _index_records(self._records)
            epoch = self._epoch
            snapshot = {
                "epoch": epoch,
                "attempts": dict(attempts),
                "records": [_record_to_snapshot(r) for r in latest_overall.values()],
            }
            metadata = {
                "node": "__checkpoint__",
                "attempt": "1",
                "status": "validated",
                "record_type": _CHECKPOINT_RECORD_TYPE,
                "content_hash": content_hash(snapshot),
                "epoch": str(epoch),
            }
            payload = [{"blob": json.dumps({"__checkpoint__": snapshot, "__meta__": metadata})}]
            response = self._create_event(metadata=metadata, payload=payload)
            # Rotate the window so no future put reuses this (now-compacted) epoch.
            self._epoch = epoch + 1
            return response.get("eventId")

    # -- replay / resume ----------------------------------------------------
    def replay(self, *, force_full: bool = False) -> None:
        """Rebuild the projection from the event log; return ``None``.

        Two paths, both producing an IDENTICAL ``completed()``/``get()``/``_projection``/
        ``_attempts`` for the same durable log:

        * **Warm (checkpoint fast-path, C-4)** — when a ``checkpoint`` event exists and
          ``force_full`` is false: fetch only the checkpoint events (a bounded
          ``record_type=checkpoint`` ``EQUALS_TO`` query), pick the latest by epoch, re-hydrate the
          compacted latest-per-node records from its snapshot, then fetch ONLY that epoch's tail (a
          bounded ``epoch=<E>`` ``EQUALS_TO`` query) and fold it in. Reads O(events-in-the-open-
          window), not the whole log. This is safe under the single-writer model (see
          :meth:`checkpoint`): a rotated epoch is closed, so no event can later appear at a folded
          epoch. As defense-in-depth, any anomaly (missing/undecodable snapshot) falls back to the
          full rebuild — the fast-path can never return a projection that differs from a cold replay.
        * **Cold (full rebuild)** — no checkpoint, or ``force_full=True`` (e.g. a caller that needs
          the full retry/attempt HISTORY in ``records()`` rather than the compacted latest-per-node
          view): paginate the whole session end-to-end and replace the caches. This is the original
          resume path; an O(new) "watermark" resume remains impossible on the data plane
          (``nextToken`` is an opaque pagination cursor; the metadata filter has only
          ``EQUALS_TO | EXISTS | NOT_EXISTS``, no range) — the epoch tag is the discrete key that
          makes a *bounded* (not incremental-suffix) warm resume expressible.

        NOTE on ``records()`` after a warm resume: it returns the COMPACTED latest-per-node records
        plus the open-window tail, NOT every historical attempt. ``completed()``/``get()`` are
        unaffected (they only ever use latest-per-node). Force ``replay(force_full=True)`` if the
        full attempt history is required.
        """
        with self._lock:
            if not force_full:
                checkpoint = self._latest_checkpoint()
                if checkpoint is not None:
                    self._replay_from_checkpoint(checkpoint)
                    return
            self._full_rebuild()

    def _full_rebuild(self) -> None:
        """The cold path: paginate the entire session and REPLACE every cache (see :meth:`replay`)."""
        events = self._paginate_events()
        records: List[Record] = []
        seq = 0
        for event in events:
            record = _event_to_record(event)
            # A checkpoint event is a derived snapshot, NOT a node output — it must never enter the
            # projection (else it would show up as a spurious completed node). Skip it on rebuild;
            # the raw node events it compacted are all still in the log.
            if record.record_type == _CHECKPOINT_RECORD_TYPE:
                continue
            seq += 1
            # Local strict-monotonic seq in log (pagination) order, so a resumed store tie-breaks
            # EXACTLY as the original run's put order did — never on the ambiguous eventTimestamp.
            record.seq = seq
            records.append(record)
        self._install(records, clock=seq)

    def _latest_checkpoint(self) -> Optional[Record]:
        """Return the highest-epoch ``checkpoint`` event as a :class:`Record`, or ``None``.

        A bounded ``record_type=checkpoint`` ``EQUALS_TO`` query — reads only checkpoint events
        (one per checkpoint call), not the whole log.
        """
        events = self._paginate_events(
            filter=_metadata_equals_filter(record_type=_CHECKPOINT_RECORD_TYPE)
        )
        best: Optional[Record] = None
        for event in events:
            rec = _event_to_record(event)
            if rec.record_type != _CHECKPOINT_RECORD_TYPE:
                continue  # a filter-ignoring client returned everything — skip non-checkpoints
            if best is None or (rec.epoch or 0) >= (best.epoch or 0):
                best = rec
        return best

    def _replay_from_checkpoint(self, checkpoint: Record) -> None:
        """Warm path: re-hydrate from ``checkpoint``'s snapshot + fold the open-window tail (C-4)."""
        snapshot = checkpoint.output if isinstance(checkpoint.output, dict) else {}
        snap_records = snapshot.get("records")
        cp_epoch = checkpoint.epoch
        if not isinstance(snap_records, list) or cp_epoch is None:
            # Malformed/legacy snapshot — never risk a wrong projection; do a full rebuild.
            self._full_rebuild()
            return

        records: List[Record] = []
        seq = 0
        for raw in snap_records:
            seq += 1
            rec = _snapshot_to_record(raw)
            rec.seq = seq  # compacted records sort BEFORE the tail (all pre-checkpoint)
            records.append(rec)

        # The open-window tail = events written AFTER the checkpoint. `checkpoint` captures the
        # whole `cp_epoch` window into the snapshot then rotates to `cp_epoch + 1`, and — because
        # this is the LATEST checkpoint (no later rotation) under a single writer — every
        # post-checkpoint put carries exactly `cp_epoch + 1`. So a single bounded EQUALS_TO on that
        # open epoch fetches the entire tail; the `cp_epoch` window needs no fetch (it is fully in
        # the snapshot). A rotated epoch is closed, so this window is final.
        open_epoch = cp_epoch + 1
        tail = self._paginate_events(filter=_metadata_equals_filter(epoch=str(open_epoch)))
        for event in tail:
            rec = _event_to_record(event)
            if rec.record_type == _CHECKPOINT_RECORD_TYPE:
                continue  # the checkpoint marker is not a node output
            seq += 1
            rec.seq = seq
            records.append(rec)

        # Resume the open window so new puts never reuse a folded (compacted) epoch.
        self._epoch = open_epoch
        self._install(records, clock=seq)

    def _install(self, records: List[Record], *, clock: int) -> None:
        """Replace the caches from ``records`` (shared by the warm + cold paths)."""
        self._clock = clock
        latest_overall, latest_validated, attempts = _index_records(records)
        self._records = records
        self._projection = {node: r.output for node, r in latest_validated.items()}
        self._attempts = attempts
        self._last_event_id = {
            node: r.event_id for node, r in latest_overall.items() if r.event_id
        }
        self._event_id_by_address = {
            (r.address or r.node): r.event_id for r in records if r.event_id
        }
        self._loaded = True

    def _ensure_loaded(self) -> None:
        """Lazily :meth:`replay` the log exactly once before the first read."""
        if not self._loaded:
            self.replay()

    # -- AgentCore Memory data plane (the ONLY methods that touch the client) ------
    # NOTE: these two helpers isolate the AgentCore Memory wire shape. The kwargs below
    # follow the documented ``bedrock-agentcore`` create_event / list_events data-plane API,
    # but the exact wire shape is unverified against a live service — every test injects a
    # fake client, so only these helpers would change if the real API differs.
    def _create_event(
        self, *, metadata: Dict[str, str], payload: List[dict], branch: Optional[dict] = None
    ) -> dict:
        """Append one Blob event; returns ``{"eventId": ..., "eventTimestamp": ...}``."""
        # AgentCore metadata values are the typed MetadataValue union ({"stringValue": ...}), not
        # bare strings, AND the string must match a restricted charset ([a-zA-Z0-9\\s._:/=+@-]) —
        # so values like ``consumes`` (``$`` JSONPath) or ``supersedes`` (event id with ``#``) are
        # SANITIZED here for the filterable index. The lossless copy travels in the Blob __meta__
        # sidecar (see put/checkpoint + _event_to_record); wrap at this wire-shape boundary so
        # callers keep flat {k: str} maps.
        typed_metadata = {
            k: {"stringValue": _SAFE_METADATA_VALUE.sub("_", v)} for k, v in metadata.items()
        }
        kwargs: Dict[str, Any] = {
            "memoryId": self._memory_id,
            "actorId": self._actor_id,
            "sessionId": self._session_id,
            # eventTimestamp is a REQUIRED CreateEvent param; ordering still uses the local
            # _clock seq (not this wall-clock), per the single-writer checkpoint contract.
            "eventTimestamp": datetime.datetime.now(datetime.timezone.utc),
            "payload": payload,
            "metadata": typed_metadata,
        }
        if branch is not None:
            kwargs["branch"] = branch
        resp = self._ensure_client().create_event(**kwargs)
        # CreateEvent nests the created event under "event"; unwrap so callers read eventId /
        # eventTimestamp flat (tolerant of test fakes that already return the flat shape).
        return resp.get("event", resp) if isinstance(resp, dict) else resp

    def _list_events(
        self,
        *,
        includePayloads: bool,
        maxResults: int = 100,
        nextToken: Optional[str] = None,
        filter: Optional[dict] = None,
    ) -> dict:
        """List this session's events (one page); returns ``{"events": [...], "nextToken"?}``.

        ``filter`` (C-4) is the documented ``ListEvents`` ``FilterInput`` — a
        ``{"eventMetadata": [ {"left": {"metadataKey": k}, "operator": "EQUALS_TO",
        "right": {"metadataValue": v}} ]}`` shape whose operators are exactly
        ``EQUALS_TO | EXISTS | NOT_EXISTS`` (no range). Used to fetch ONLY checkpoint events or
        ONLY a given epoch's tail, so a warm resume reads O(events-since-checkpoint), not the log.
        """
        kwargs: Dict[str, Any] = {
            "memoryId": self._memory_id,
            "actorId": self._actor_id,
            "sessionId": self._session_id,
            "includePayloads": includePayloads,
            "maxResults": maxResults,
        }
        if nextToken is not None:
            kwargs["nextToken"] = nextToken
        if filter is not None:
            kwargs["filter"] = filter
        return self._ensure_client().list_events(**kwargs)

    def _paginate_events(self, *, filter: Optional[dict] = None) -> List[dict]:
        """Fetch EVERY event matching ``filter`` (all pages, with payloads), in log order."""
        events: List[dict] = []
        token: Optional[str] = None
        while True:
            response = self._list_events(includePayloads=True, nextToken=token, filter=filter)
            events.extend(response.get("events", []))
            token = response.get("nextToken")
            if not token:
                break
        return events

    def _ensure_client(self) -> Any:
        """Return the injected client, or lazily construct the boto3 data-plane default."""
        if self._client is None:
            try:
                import boto3  # lazy: only a live Memory backend needs the AWS SDK
            except ImportError as exc:  # pragma: no cover - exercised only without boto3
                raise RuntimeError(
                    "MemoryStateStore requires boto3 — install the 'agentcore' extra "
                    "(pip install concursus[agentcore]) or pass client=..."
                ) from exc
            self._client = boto3.client("bedrock-agentcore")  # data plane
        return self._client


# -- event <-> record marshalling -------------------------------------------
def _build_metadata(record: Record) -> Dict[str, str]:
    """Project a :class:`Record` onto AgentCore event metadata (all-string k/v)."""
    metadata: Dict[str, str] = {
        "node": record.node,
        "attempt": str(record.attempt),
        "status": record.status,
        "record_type": record.record_type,
        "content_hash": record.content_hash or "",
    }
    if record.schema is not None:
        metadata["schema"] = record.schema
    if record.producer is not None:
        metadata["producer"] = record.producer
    if record.consumes:
        metadata["consumes"] = ",".join(record.consumes)
    if record.supersedes:
        metadata["supersedes"] = record.supersedes
    if record.address is not None:
        metadata["address"] = record.address
    if record.epoch is not None:
        metadata["epoch"] = str(record.epoch)  # C-4: the checkpoint window id (EQUALS_TO filterable)
    return metadata


# C-4: the per-record fields a checkpoint snapshot round-trips (a compacted latest-per-node view).
_SNAPSHOT_FIELDS: Tuple[str, ...] = (
    "node", "output", "attempt", "status", "record_type", "schema", "producer",
    "consumes", "supersedes", "content_hash", "event_id", "address", "epoch",
)


def _record_to_snapshot(record: Record) -> dict:
    """Project a :class:`Record` to a JSON-safe dict for a checkpoint snapshot blob."""
    return {f: getattr(record, f) for f in _SNAPSHOT_FIELDS}


def _snapshot_to_record(raw: dict) -> Record:
    """Rebuild a :class:`Record` from a checkpoint snapshot entry (see :func:`_record_to_snapshot`).

    Consumes is normalized to a list; ``seq``/``timestamp`` are reassigned by the caller in resume
    order, so they are intentionally omitted here.
    """
    data = {f: raw.get(f) for f in _SNAPSHOT_FIELDS}
    consumes = data.get("consumes")
    data["consumes"] = list(consumes) if isinstance(consumes, list) else []
    data["output"] = data.get("output") or {}
    return Record(**data)


def _event_to_record(event: dict) -> Record:
    """Parse a ``list_events`` event (metadata + Blob payload) back into a :class:`Record`."""
    payload = event.get("payload") or []
    blob = payload[0].get("blob") if payload else None
    if isinstance(blob, (str, bytes, bytearray)):
        try:
            blob = json.loads(blob)
        except (ValueError, TypeError):
            blob = None

    # Prefer the faithful ``__meta__`` sidecar carried in the Blob (unrestricted charset); fall back
    # to AgentCore event metadata, unwrapping the typed MetadataValue union and tolerating already-
    # flat values from injected test fakes. AgentCore metadata values are sanitized to a restricted
    # charset on write, so the sidecar is the lossless source of truth for fields like ``consumes``
    # (a ``$`` JSONPath) and ``supersedes`` (an event id with ``#``).
    meta: Dict[str, Any]
    if isinstance(blob, dict) and isinstance(blob.get("__meta__"), dict):
        meta = dict(blob["__meta__"])
    else:
        raw_meta = event.get("metadata") or {}
        meta = {
            k: (v.get("stringValue") if isinstance(v, dict) else v)
            for k, v in raw_meta.items()
        }
    node = meta.get("node", "")

    consumes_raw = meta.get("consumes", "")
    consumes = [c for c in consumes_raw.split(",") if c] if consumes_raw else []

    output: dict = {}
    if isinstance(blob, dict):
        body = {k: v for k, v in blob.items() if k != "__meta__"}
        output = body.get(node, body)
    elif blob is not None:
        output = blob

    try:
        attempt = int(meta.get("attempt", "1"))
    except (TypeError, ValueError):
        attempt = 1

    epoch_raw = meta.get("epoch")
    try:
        epoch = int(epoch_raw) if epoch_raw is not None and epoch_raw != "" else None
    except (TypeError, ValueError):
        epoch = None

    return Record(
        node=node,
        output=output,
        attempt=attempt,
        status=meta.get("status", "validated"),
        record_type=meta.get("record_type", "agent_output"),
        schema=meta.get("schema"),
        producer=meta.get("producer"),
        consumes=consumes,
        supersedes=meta.get("supersedes"),
        content_hash=meta.get("content_hash"),
        timestamp=event.get("eventTimestamp"),
        event_id=event.get("eventId"),
        address=meta.get("address"),
        epoch=epoch,
    )
