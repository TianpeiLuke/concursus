"""The **StateStore** — durable, addressable run state for the supervisor.

Run state is a single append-only log of validated agent outputs plus a rebuildable
projection (``{node: latest validated output}``) — the slipbox's single-source-of-truth /
derived-DB discipline (FZ 35a2b1a). Two backends share one :class:`StateStore` Protocol:
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

import hashlib
import json
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
)

# The ``record_type`` marking a content-hash no-op re-put (identical to the latest output).
_DEDUP_RECORD_TYPE = "dedup"

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
        self._event_id_by_address: Dict[str, str] = {}  # FZ address -> event id (branch roots)
        # Local strict-monotonic sequence (mirrors InProcessStateStore._clock): assigned per
        # put and per replayed event, it — not the AgentCore eventTimestamp — is the ordering
        # tie-breaker, so concurrent branch/retry writes resolve DETERMINISTICALLY on replay.
        self._clock: int = 0
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
            payload = [{"blob": json.dumps({node: record.output})}]
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

    # -- replay / resume ----------------------------------------------------
    def replay(self) -> None:
        """Rebuild the projection from the event log: paginate ``list_events`` (with payloads),
        parse each event into a :class:`Record`, and keep the latest validated per node.

        This is the resume path — a run survives micro-VM teardown / mid-run crashes because
        the supervisor re-reads the durable log rather than remembering.

        Every call is a **full cold rebuild** of the whole session log from scratch: ``list_events``
        is paginated end-to-end via its opaque ``nextToken`` continuation, ``seq`` is re-assigned
        1..N in log order, and the ``_records`` / ``_projection`` / ``_attempts`` caches are
        REPLACED (never appended to). This is deliberate and required for correctness — an O(new)
        "watermark" resume is **not expressible on the AgentCore Memory data plane**, verified
        against the ``ListEvents`` API reference (2024-02-28):

        - ``nextToken`` is an opaque *pagination* cursor, and the response returns it as ``null``
          "when there are no more results" — so a drained full replay leaves NO position token to
          feed back in later to ask for "only what arrived since".
        - ``ListEvents`` takes a ``filter`` (``FilterInput``), but its ``eventMetadata`` filter
          operators are exactly ``EQUALS_TO | EXISTS | NOT_EXISTS`` — there is **no range/``>``
          operator** and no created-after/timestamp-after parameter, so "events with seq/timestamp
          greater than my watermark" cannot be expressed on the wire at all.

        Consequently an "incremental" replay would either (a) re-read the WHOLE log anyway and
        duplicate the retained prefix, or (b) against a hypothetical watermark-honoring client
        silently drop a concurrent-writer event ordered before a locally-appended tail event. A
        full rebuild is the only path that stays IDENTICAL to the durable log on every call — and it
        is exactly the INV-5 discipline the governor relies on: re-derive the executed prefix from
        the append-only log each round, never from a mutably-cached suffix. The projection is
        idempotent, so re-reading is safe; a warm resume that picks up a concurrent writer's new
        events is just another full replay.
        """
        with self._lock:
            records: List[Record] = []
            seq = 0
            token: Optional[str] = None
            while True:
                response = self._list_events(includePayloads=True, nextToken=token)
                for event in response.get("events", []):
                    seq += 1
                    record = _event_to_record(event)
                    # Assign the local strict-monotonic seq in log (pagination) order, so a
                    # resumed store tie-breaks EXACTLY as the original run's put order did —
                    # never on the ambiguous AgentCore eventTimestamp.
                    record.seq = seq
                    records.append(record)
                token = response.get("nextToken")
                if not token:
                    break

            self._clock = seq
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
        kwargs: Dict[str, Any] = {
            "memoryId": self._memory_id,
            "actorId": self._actor_id,
            "sessionId": self._session_id,
            "payload": payload,
            "metadata": metadata,
        }
        if branch is not None:
            kwargs["branch"] = branch
        return self._ensure_client().create_event(**kwargs)

    def _list_events(
        self,
        *,
        includePayloads: bool,
        maxResults: int = 100,
        nextToken: Optional[str] = None,
    ) -> dict:
        """List this session's events (one page); returns ``{"events": [...], "nextToken"?}``."""
        kwargs: Dict[str, Any] = {
            "memoryId": self._memory_id,
            "actorId": self._actor_id,
            "sessionId": self._session_id,
            "includePayloads": includePayloads,
            "maxResults": maxResults,
        }
        if nextToken is not None:
            kwargs["nextToken"] = nextToken
        return self._ensure_client().list_events(**kwargs)

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
    return metadata


def _event_to_record(event: dict) -> Record:
    """Parse a ``list_events`` event (metadata + Blob payload) back into a :class:`Record`."""
    meta = event.get("metadata") or {}
    node = meta.get("node", "")

    consumes_raw = meta.get("consumes", "")
    consumes = [c for c in consumes_raw.split(",") if c] if consumes_raw else []

    output: dict = {}
    payload = event.get("payload") or []
    if payload:
        blob = payload[0].get("blob")
        if isinstance(blob, (str, bytes, bytearray)):
            blob = json.loads(blob)
        if isinstance(blob, dict):
            output = blob.get(node, blob)
        elif blob is not None:
            output = blob

    try:
        attempt = int(meta.get("attempt", "1"))
    except (TypeError, ValueError):
        attempt = 1

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
    )
