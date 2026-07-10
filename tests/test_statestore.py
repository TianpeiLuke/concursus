"""Tests for the StateStore — the durable, addressable run-state store.

Covers the offline :class:`InProcessStateStore` default (put/get/completed/records, attempt
auto-increment, content-hash dedup) and the AgentCore Memory-backed :class:`MemoryStateStore`
driven by a FAKE data-plane client that implements ``create_event`` / ``list_events`` — so the
event-log write shape, the replay/resume path, and latest-validated selection are exercised
without boto3 and without ever touching AWS.
"""

import json

import pytest

from concursus.state.statestore import (
    InProcessStateStore,
    MemoryStateStore,
    Record,
    RecordStatus,
    RecordType,
    StateStoreError,
    content_hash,
)


# -- fake AgentCore Memory data-plane client --------------------------------
class FakeMemoryClient:
    """A fake ``bedrock-agentcore`` client: records ``create_event`` calls and replays them.

    ``create_event`` appends a stored event and returns ``{"eventId", "eventTimestamp"}``;
    ``list_events`` returns the stored events (dropping payloads when ``includePayloads`` is
    false), so a store can be seeded with prior events to test resume.
    """

    def __init__(self, events=None):
        self._events = list(events or [])
        self.created = []  # kwargs of every create_event call
        self._seq = len(self._events)

    def create_event(self, **kwargs):
        self.created.append(kwargs)
        self._seq += 1
        event = {
            "eventId": f"ev-{self._seq}",
            "eventTimestamp": self._seq,
            "payload": list(kwargs.get("payload", [])),
            "metadata": dict(kwargs.get("metadata", {})),
        }
        self._events.append(event)
        return {"eventId": event["eventId"], "eventTimestamp": event["eventTimestamp"]}

    def list_events(self, **kwargs):
        include = kwargs.get("includePayloads", True)
        events = []
        for e in self._events:
            item = {
                "eventId": e["eventId"],
                "eventTimestamp": e["eventTimestamp"],
                "metadata": dict(e["metadata"]),
            }
            if include:
                item["payload"] = list(e["payload"])
            events.append(item)
        return {"events": events}


def _seed_event(node, output, attempt, *, status="validated", consumes=None, ts=None):
    """Build a stored event matching what ``create_event`` would have persisted."""
    metadata = {
        "node": node,
        "attempt": str(attempt),
        "status": status,
        "record_type": "agent_output",
        "content_hash": content_hash(output),
    }
    if consumes:
        metadata["consumes"] = ",".join(consumes)
    return {
        "eventId": f"{node}-{attempt}",
        "eventTimestamp": ts if ts is not None else attempt,
        "payload": [{"blob": json.dumps({node: output})}],
        "metadata": metadata,
    }


def _store(client):
    return MemoryStateStore(
        memory_id="mem-1", session_id="S" * 40, actor_id="team-1", client=client
    )


# == (a) InProcessStateStore =================================================
def test_inprocess_put_get_completed_records_happy_path():
    store = InProcessStateStore()
    store.put("ingest", {"document": "DOC"})
    store.put(
        "summarize",
        {"summary": "SUM"},
        meta={"consumes": ["ingest:$.document"], "schema": "sum.v1", "producer": "summarize"},
    )

    # get returns the latest validated output.
    assert store.get("ingest") == {"document": "DOC"}
    assert store.get("summarize") == {"summary": "SUM"}

    # completed is the validated frontier.
    assert store.completed() == {"ingest", "summarize"}

    # records is the full append-only log, carrying the resolved AgentRef edges.
    records = store.records()
    assert [r.node for r in records] == ["ingest", "summarize"]
    summarize = records[1]
    assert summarize.consumes == ["ingest:$.document"]
    assert summarize.schema == "sum.v1"
    assert summarize.content_hash == content_hash({"summary": "SUM"})


def test_inprocess_get_missing_raises_keyerror():
    store = InProcessStateStore()
    with pytest.raises(KeyError):
        store.get("nope")


def test_inprocess_attempt_auto_increment_on_re_put():
    store = InProcessStateStore()
    store.put("ingest", {"document": "V1"})
    store.put("ingest", {"document": "V2"})  # a retry with a new output

    ingest_records = [r for r in store.records() if r.node == "ingest"]
    assert [r.attempt for r in ingest_records] == [1, 2]
    # get reflects the latest attempt.
    assert store.get("ingest") == {"document": "V2"}


def test_inprocess_content_hash_dedup_marker():
    store = InProcessStateStore()
    store.put("ingest", {"document": "SAME"})
    store.put("ingest", {"document": "SAME"})  # identical -> a no-op dedup

    records = [r for r in store.records() if r.node == "ingest"]
    assert len(records) == 2  # still recorded, not an error
    assert records[0].record_type == "agent_output"
    assert records[1].record_type == "dedup"  # the dedup marker
    assert records[1].attempt == 2  # attempt still auto-incremented
    assert records[0].content_hash == records[1].content_hash
    assert store.get("ingest") == {"document": "SAME"}
    assert store.completed() == {"ingest"}


def test_inprocess_blocked_on_meta_is_recorded():
    store = InProcessStateStore()
    store.put(
        "critique",
        {},
        meta={"status": "failed", "producer": "critique", "blocked_on": "blocked on summarize"},
    )
    rec = store.records()[0]
    assert rec.status == "failed"
    assert rec.blocked_on == "blocked on summarize"
    assert "critique" not in store.completed()


def test_inprocess_has_reentrant_lock_guard():
    import threading

    store = InProcessStateStore()
    assert isinstance(store._lock, type(threading.RLock()))
    # reentrancy: put (which locks) is callable from within a held lock without deadlock.
    with store._lock:
        store.put("n", {"x": 1})
    assert store.get("n") == {"x": 1}


def test_inprocess_failed_latest_is_not_completed_but_get_returns_prior_validated():
    store = InProcessStateStore()
    store.put("critique", {"critique": "OK"})
    store.put("critique", {"critique": "BROKEN"}, meta={"status": "failed"})

    # completed keys off the LATEST record's status (failed) ...
    assert "critique" not in store.completed()
    # ... but get still returns the latest VALIDATED output.
    assert store.get("critique") == {"critique": "OK"}


# == (a2) Record self-validating typed enums =================================
def test_record_coerces_status_and_record_type_through_enums():
    r = Record(node="n", output={}, status="validated", record_type="agent_output")
    # coerced to the enum members, which (being str subclasses) still compare == the bare value.
    assert r.status == "validated"
    assert isinstance(r.status, RecordStatus)
    assert r.record_type == "agent_output"
    assert isinstance(r.record_type, RecordType)
    # str()/f-string render as the bare value (so _build_metadata stays byte-for-byte).
    assert f"{r.status}" == "validated"
    assert str(r.record_type) == "agent_output"


def test_record_rejects_unknown_status():
    with pytest.raises(StateStoreError, match="unknown record status"):
        Record(node="n", output={}, status="bogus")


def test_record_accepts_checkpoint_record_type():
    # 'checkpoint' is a first-class RecordType member (test_runindex.py depends on it).
    r = Record(node="n", output={}, record_type="checkpoint")
    assert r.record_type == "checkpoint"
    assert isinstance(r.record_type, RecordType)


def test_record_unknown_record_type_widens_and_warns():
    with pytest.warns(UserWarning, match="unknown record_type"):
        r = Record(node="n", output={}, record_type="future_kind")
    assert r.record_type == "future_kind"  # kept verbatim, not rejected


def test_all_known_status_and_record_type_literals_are_enum_members():
    assert {s.value for s in RecordStatus} == {"validated", "failed", "superseded"}
    assert {"agent_output", "dedup", "checkpoint"} <= {t.value for t in RecordType}


def test_put_with_unknown_status_meta_raises_statestoreerror():
    store = InProcessStateStore()
    with pytest.raises(StateStoreError, match="unknown record status"):
        store.put("n", {"x": 1}, meta={"status": "bogus"})


# == (b) MemoryStateStore: put writes a Blob event, get returns it ============
def test_memory_put_writes_blob_event_with_metadata_and_get_returns_it():
    client = FakeMemoryClient()
    store = _store(client)

    store.put("ingest", {"document": "DOC"})
    store.put(
        "summarize",
        {"summary": "SUM"},
        meta={"consumes": ["ingest:$.document"], "schema": "sum.v1", "producer": "ingest"},
    )

    # two Blob events were written, in order.
    assert len(client.created) == 2
    call = client.created[1]
    assert call["memoryId"] == "mem-1"
    assert call["actorId"] == "team-1"
    assert call["sessionId"] == "S" * 40

    meta = call["metadata"]
    assert meta["node"] == "summarize"
    assert meta["attempt"] == "1"
    assert meta["status"] == "validated"
    assert meta["record_type"] == "agent_output"
    assert meta["consumes"] == "ingest:$.document"
    assert meta["producer"] == "ingest"
    assert meta["schema"] == "sum.v1"
    assert meta["content_hash"] == content_hash({"summary": "SUM"})

    # the Blob payload carries the verbatim output keyed by node.
    blob = json.loads(call["payload"][0]["blob"])
    assert blob == {"summarize": {"summary": "SUM"}}

    # get reflects the write (lazy replay reconstructs it from the event log).
    assert store.get("summarize") == {"summary": "SUM"}
    assert store.get("ingest") == {"document": "DOC"}
    assert store.completed() == {"ingest", "summarize"}


def test_memory_get_after_put_reflects_new_value_without_error():
    client = FakeMemoryClient()
    store = _store(client)
    store.completed()  # force an initial (empty) replay -> loaded
    store.put("ingest", {"document": "DOC"})
    # projection updated in place on put, no re-replay needed.
    assert store.get("ingest") == {"document": "DOC"}


# == (c) Memory replay / resume: rebuild projection from a prior run ==========
def test_memory_replay_rebuilds_projection_for_resume():
    prior = [
        _seed_event("ingest", {"document": "DOC"}, 1),
        _seed_event("summarize", {"summary": "SUM"}, 1, consumes=["ingest:$.document"]),
    ]
    client = FakeMemoryClient(events=prior)

    # a FRESH store over the same session resumes purely by replaying the log.
    store = _store(client)
    store.replay()

    assert store.completed() == {"ingest", "summarize"}
    assert store.get("ingest") == {"document": "DOC"}
    assert store.get("summarize") == {"summary": "SUM"}

    # the recorded AgentRef edges survive the round-trip (the run graph is rebuildable).
    summarize = next(r for r in store.records() if r.node == "summarize")
    assert summarize.consumes == ["ingest:$.document"]
    # no new events were written during a pure resume.
    assert client.created == []


def test_memory_lazy_replay_triggers_on_first_read():
    client = FakeMemoryClient(events=[_seed_event("ingest", {"document": "DOC"}, 1)])
    store = _store(client)
    # never called replay() explicitly; the first read loads the log once.
    assert store.get("ingest") == {"document": "DOC"}


# == (d) latest-validated selection across multiple attempts =================
def test_memory_latest_validated_selection_across_attempts():
    prior = [
        _seed_event("summarize", {"summary": "v1"}, 1, ts=1),
        _seed_event("summarize", {"summary": "v2"}, 2, ts=2),  # higher attempt wins
        _seed_event("critique", {"critique": "OK"}, 1, ts=3),
        _seed_event("critique", {"critique": "BAD"}, 2, status="failed", ts=4),
    ]
    store = _store(FakeMemoryClient(events=prior))
    store.replay()

    # highest attempt wins for a node whose retries all validated.
    assert store.get("summarize") == {"summary": "v2"}
    # a node whose latest attempt FAILED is not complete, but get returns its last validated.
    assert "critique" not in store.completed()
    assert store.get("critique") == {"critique": "OK"}
    assert store.completed() == {"summarize"}


def test_memory_replay_ordering_is_deterministic_and_matches_inprocess():
    """AI-6: after AI-1-style concurrent-attempt writes, MemoryStateStore replay resolves the
    latest record by the store-local strict-monotonic ``seq`` (log order), NOT the ambiguous
    AgentCore eventTimestamp — so it is deterministic across replays and last-write-wins like
    the InProcessStateStore.

    The two seeded events tie on node+attempt (competing fan-out/branch writes) but their
    eventTimestamps are INVERTED relative to log order: the last-logged event carries the
    SMALLER timestamp. Timestamp-only ordering would pick the first-logged event (and could flip
    if the backend's wall-clock ordering ever disagreed with log order); seq pins it to the
    last-logged event on every replay.
    """
    prior = [
        _seed_event("map", {"r": "A"}, 1, ts=100),  # logged first, LATER wall-clock
        _seed_event("map", {"r": "B"}, 1, ts=50),  # logged last, EARLIER wall-clock
    ]

    # Two independent fresh stores replaying the SAME log agree (determinism).
    r1 = _store(FakeMemoryClient(events=list(prior)))
    r1.replay()
    r2 = _store(FakeMemoryClient(events=list(prior)))
    r2.replay()
    assert r1.get("map") == r2.get("map")

    # The winner is the LAST-logged event (seq order), not the higher-timestamp one.
    assert r1.get("map") == {"r": "B"}

    # replay assigns a strict-monotonic seq in log order.
    assert [r.seq for r in r1.records()] == [1, 2]

    # ... and this matches InProcessStateStore's last-write-wins (its monotonic clock == seq).
    ip = InProcessStateStore()
    ip.put("map", {"r": "A"})
    ip.put("map", {"r": "B"})
    assert ip.get("map") == r1.get("map") == {"r": "B"}
    assert ip.completed() == r1.completed() == {"map"}


def test_memory_put_assigns_strict_monotonic_seq_independent_of_event_timestamp():
    """put assigns a local seq (mirrors InProcessStateStore._clock) used for tie-breaking; the
    eventTimestamp stays for display only."""
    client = FakeMemoryClient()
    store = _store(client)
    store.put("a", {"v": 1})
    store.put("a", {"v": 2}, meta={"status": "failed"})
    store.put("b", {"v": 3})
    seqs = [r.seq for r in store.records()]
    assert seqs == [1, 2, 3]  # strictly monotonic across nodes, in put order
    # a failed latest is not complete; get returns the last validated (attempt-based).
    assert "a" not in store.completed()
    assert store.get("a") == {"v": 1}


def test_memory_replay_paginates_next_token():
    """replay() follows nextToken across pages."""

    class PagedClient(FakeMemoryClient):
        def list_events(self, **kwargs):
            token = kwargs.get("nextToken")
            if token is None:
                page = {
                    "events": [
                        {
                            "eventId": "a-1",
                            "eventTimestamp": 1,
                            "metadata": {"node": "a", "attempt": "1", "status": "validated"},
                            "payload": [{"blob": json.dumps({"a": {"v": 1}})}],
                        }
                    ],
                    "nextToken": "PAGE2",
                }
                return page
            return {
                "events": [
                    {
                        "eventId": "b-1",
                        "eventTimestamp": 2,
                        "metadata": {"node": "b", "attempt": "1", "status": "validated"},
                        "payload": [{"blob": json.dumps({"b": {"v": 2}})}],
                    }
                ]
            }

    store = _store(PagedClient())
    store.replay()
    assert store.completed() == {"a", "b"}
    assert store.get("a") == {"v": 1}
    assert store.get("b") == {"v": 2}
