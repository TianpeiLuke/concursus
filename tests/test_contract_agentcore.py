"""Contract tests — validate AgentCore requests/responses against the REAL botocore service model.

Unit tests elsewhere inject hand-rolled fakes; a fake encodes the SAME assumptions as the code, so
a green suite can coexist with a broken live backend (exactly what the live run exposed —
five MemoryStateStore wire-shape bugs). These tests close that gap: they drive the real boto3
clients through ``botocore.stub.Stubber``, which runs the normal serializer (so a malformed request
fails model validation exactly as it would live) and validates canned responses against the output
model. No AWS account, no network.

Skips cleanly when the installed botocore predates the bedrock-agentcore models (pyproject pins
boto3>=1.34.0, older than these services) — a skip, never a false green.
"""

import datetime

import pytest

boto3 = pytest.importorskip("boto3")
from botocore.stub import ANY, Stubber  # noqa: E402

from concursus.state.statestore import MemoryStateStore, _metadata_equals_filter  # noqa: E402

_TS = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
_SESSION = "S" * 40
_MEM = "memtest-0123456789"  # model-valid memoryId (>=12, matches the name-XXXXXXXXXX pattern)


def _client(name):
    try:
        return boto3.client(name, region_name="us-east-1")
    except Exception as exc:  # UnknownServiceError on an older botocore
        pytest.skip(f"{name} service model unavailable in this botocore: {exc}")


def _event(**over):
    ev = {
        "memoryId": _MEM,
        "actorId": "team-1",
        "sessionId": _SESSION,
        "eventId": "0000001#abc",
        "eventTimestamp": _TS,
        "payload": [{"blob": "{}"}],  # required Event member
    }
    ev.update(over)
    return ev


# -- the wire-shape guard: the model rejects the pre-fix shapes, accepts the fixed ones ----------
def test_model_rejects_bare_string_metadata_and_accepts_typed():
    """Proves the contract layer catches statestore bugs #1/#2: AgentCore metadata is the typed
    MetadataValue union, so a flat {k: str} map fails serialization while {k: {'stringValue': v}}
    passes."""
    client = _client("bedrock-agentcore")
    with Stubber(client):
        with pytest.raises(Exception):  # botocore ParamValidationError during serialization
            client.create_event(
                memoryId=_MEM, actorId="a", sessionId=_SESSION, eventTimestamp=_TS,
                payload=[{"blob": "{}"}], metadata={"record_type": "agent_output"},  # BARE -> invalid
            )
    with Stubber(client) as stub:
        stub.add_response("create_event", {"event": _event()})
        # typed metadata + a required eventTimestamp serialize cleanly (the fixed shape)
        client.create_event(
            memoryId=_MEM, actorId="a", sessionId=_SESSION, eventTimestamp=_TS,
            payload=[{"blob": "{}"}], metadata={"record_type": {"stringValue": "agent_output"}},
        )


def test_create_event_requires_event_timestamp():
    """Proves bug #3: eventTimestamp is a required CreateEvent parameter."""
    client = _client("bedrock-agentcore")
    with Stubber(client):
        with pytest.raises(Exception):  # missing required 'eventTimestamp'
            client.create_event(
                memoryId=_MEM, actorId="a", sessionId=_SESSION,
                payload=[{"blob": "{}"}], metadata={"record_type": {"stringValue": "x"}},
            )


def test_list_events_filter_value_must_be_typed():
    """Proves bug #1 at the filter site: ListEvents right.metadataValue is the typed union."""
    client = _client("bedrock-agentcore")
    with Stubber(client):
        with pytest.raises(Exception):
            client.list_events(
                memoryId=_MEM, actorId="a", sessionId=_SESSION, includePayloads=True,
                filter={"eventMetadata": [
                    {"left": {"metadataKey": "record_type"}, "operator": "EQUALS_TO",
                     "right": {"metadataValue": "checkpoint"}}  # BARE -> invalid
                ]},
            )
    # the helper the store actually uses produces the model-valid typed filter:
    good = _metadata_equals_filter(record_type="checkpoint")
    with Stubber(client) as stub:
        stub.add_response("list_events", {"events": []})
        client.list_events(
            memoryId=_MEM, actorId="a", sessionId=_SESSION, includePayloads=True, filter=good,
        )


# -- the store drives the real client end-to-end through the stub --------------------------------
def test_memory_store_put_and_read_over_stubbed_real_client():
    """MemoryStateStore.put -> create_event serializes against the real model (typed metadata +
    eventTimestamp) AND the store reads the nested response['event']['eventId'] (bug #5)."""
    client = _client("bedrock-agentcore")
    stub = Stubber(client)
    stub.add_response(
        "create_event",
        {"event": _event(eventId="0000042#deadbeef")},
        expected_params={
            "memoryId": _MEM, "actorId": "team-1", "sessionId": _SESSION,
            "eventTimestamp": ANY, "payload": ANY, "metadata": ANY,
        },
    )
    with stub:
        store = MemoryStateStore(memory_id=_MEM, session_id=_SESSION, actor_id="team-1", client=client)
        store.put("ingest", {"doc": "D"}, meta={"status": "validated", "record_type": "agent_output"})
        # read the in-memory record directly (records() would trigger a lazy replay -> list_events)
        assert store._records[-1].event_id == "0000042#deadbeef"  # unwrapped from the "event" envelope


def test_memory_store_replay_reads_typed_metadata_and_blob():
    """A ListEvents response in the real shape (typed metadata map + Blob payload) rebuilds the
    projection — the read path tolerates the live typed shape."""
    client = _client("bedrock-agentcore")
    stub = Stubber(client)
    import json as _json
    blob = _json.dumps({"ingest": {"doc": "D"}, "__meta__": {"node": "ingest", "status": "validated",
                                                             "record_type": "agent_output", "attempt": "1"}})
    event = _event(
        payload=[{"blob": blob}],
        metadata={"node": {"stringValue": "ingest"}, "status": {"stringValue": "validated"},
                  "record_type": {"stringValue": "agent_output"}},
    )
    # replay() first issues the bounded record_type=checkpoint query (none here), then the full
    # rebuild — so stub two ListEvents in that order.
    stub.add_response("list_events", {"events": []})            # checkpoint probe -> no checkpoint
    stub.add_response("list_events", {"events": [event]})       # cold full rebuild
    with stub:
        store = MemoryStateStore(memory_id=_MEM, session_id=_SESSION, actor_id="team-1", client=client)
        store.replay()
        assert store.completed() == {"ingest"}
        assert store.get("ingest") == {"doc": "D"}


# -- control plane: CreateAgentRuntime is async (status), GetAgentRuntime reads it ---------------
def test_create_agent_runtime_returns_status_and_get_reads_it():
    """Proves the readiness contract fix #1 rests on real fields: CreateAgentRuntime returns
    agentRuntimeId + status, and GetAgentRuntime(agentRuntimeId) returns status/failureReason."""
    control = _client("bedrock-agentcore-control")
    stub = Stubber(control)
    stub.add_response(
        "create_agent_runtime",
        {"agentRuntimeArn": "arn:aws:bedrock-agentcore:us-east-1:111:runtime/x-1",
         "agentRuntimeId": "x-1", "agentRuntimeVersion": "1",
         "createdAt": _TS, "status": "CREATING",
         "workloadIdentityDetails": {"workloadIdentityArn": "arn:aws:...:workload-identity/x"}},
    )
    stub.add_response(
        "get_agent_runtime",
        {"agentRuntimeArn": "arn:aws:bedrock-agentcore:us-east-1:111:runtime/x-1",
         "agentRuntimeName": "x", "agentRuntimeId": "x-1", "agentRuntimeVersion": "1",
         "createdAt": _TS, "lastUpdatedAt": _TS, "roleArn": "arn:aws:iam::111:role/x",
         "networkConfiguration": {"networkMode": "PUBLIC"},
         "lifecycleConfiguration": {}, "status": "READY"},
        expected_params={"agentRuntimeId": "x-1"},
    )
    with stub:
        created = control.create_agent_runtime(
            agentRuntimeName="x",
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": "111.dkr.ecr.us-east-1.amazonaws.com/x:latest"}},
            roleArn="arn:aws:iam::111:role/x", networkConfiguration={"networkMode": "PUBLIC"},
            protocolConfiguration={"serverProtocol": "HTTP"},
        )
        assert created["status"] == "CREATING" and created["agentRuntimeId"] == "x-1"
        got = control.get_agent_runtime(agentRuntimeId=created["agentRuntimeId"])
        assert got["status"] == "READY"
