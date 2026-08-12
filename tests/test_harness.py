"""Tests for concursus.execute.harness — the AgentHarness."""

from __future__ import annotations

import hashlib
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from concursus.execute.harness import AgentHarness
from concursus.execute.types import (
    HealthSignal,
    HealthStatus,
    LogEvent,
    LogEventType,
    LogSeverity,
    PreemptiveTermination,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures / Fakes
# ──────────────────────────────────────────────────────────────────────────────


class FakeObjectStore:
    """In-memory object store for testing."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    async def get_object(self, uri: str) -> bytes:
        if uri not in self.objects:
            raise FileNotFoundError(f"No object at {uri}")
        return self.objects[uri]

    async def put_object(self, uri: str, data: bytes, content_type: str) -> str:
        self.objects[uri] = data
        return uri


class FakeMonitor:
    """Fake ExecutionMonitor that records events and returns a configurable signal."""

    def __init__(self, signal: HealthSignal = None):
        self.signal = signal or HealthSignal(status=HealthStatus.COMPLETED)
        self.events_received: list[LogEvent] = []

    async def watch(self, log_stream):
        async for event in log_stream:
            self.events_received.append(event)
        return self.signal


def _manifest(outputs_type="string", required=False) -> dict:
    """Build a test manifest."""
    output_schema = {"type": outputs_type}
    if outputs_type == "artifact":
        output_schema["content_type"] = "application/json"
    if required:
        output_schema["required"] = True

    return {
        "name": "test-node",
        "runtime": {"backend": "callable", "entry": "fake:run"},
        "contract": {
            "inputs": {"properties": {"source": {"type": "artifact", "content_type": "application/json"}}},
            "outputs": {"properties": {"result": output_schema}},
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Input deref
# ──────────────────────────────────────────────────────────────────────────────


class TestInputDeref:
    """Tests for input materialization (step 1)."""

    @pytest.mark.asyncio
    async def test_deref_artifact_input(self):
        """ArtifactRef inputs are fetched from object store and deserialized."""
        store = FakeObjectStore()
        data = json.dumps({"key": "value"}).encode()
        content_hash = f"sha256:{hashlib.sha256(data).hexdigest()}"
        store.objects["s3://bucket/input.json"] = data

        manifest = _manifest()
        # Patch the invoker to return immediately
        harness = AgentHarness(manifest, store=store, output_prefix="s3://bucket/out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"result": "done"})

        envelope = {
            "inputs": {
                "source": {
                    "uri": "s3://bucket/input.json",
                    "content_type": "application/json",
                    "content_hash": content_hash,
                }
            }
        }

        result = await harness.run(envelope)
        # Verify the invoker received materialized data, not the ArtifactRef
        call_args = harness.invoker.invoke.call_args
        _, inputs_arg, _ = call_args[0]
        assert inputs_arg["source"] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_deref_hash_mismatch_raises(self):
        """Hash mismatch on artifact deref raises ValueError."""
        store = FakeObjectStore()
        store.objects["s3://bucket/bad.json"] = b'{"corrupted": true}'

        manifest = _manifest()
        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"result": "x"})

        envelope = {
            "inputs": {
                "source": {
                    "uri": "s3://bucket/bad.json",
                    "content_type": "application/json",
                    "content_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                }
            }
        }

        with pytest.raises(ValueError, match="Hash mismatch"):
            await harness.run(envelope)

    @pytest.mark.asyncio
    async def test_scalar_inputs_pass_inline(self):
        """Non-artifact inputs pass through without deref."""
        store = FakeObjectStore()
        manifest = {
            "name": "test",
            "runtime": {"backend": "callable", "entry": "x:y"},
            "contract": {
                "inputs": {"properties": {"query": {"type": "string"}}},
                "outputs": {"properties": {"answer": {"type": "string"}}},
            },
        }

        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"answer": "42"})

        envelope = {"inputs": {"query": "what is 6*7?"}}
        await harness.run(envelope)

        call_args = harness.invoker.invoke.call_args
        _, inputs_arg, _ = call_args[0]
        assert inputs_arg["query"] == "what is 6*7?"


# ──────────────────────────────────────────────────────────────────────────────
# Step 3+4: Invoke + Monitor
# ──────────────────────────────────────────────────────────────────────────────


class TestInvokeWithMonitor:
    """Tests for invoke with optional ExecutionMonitor (steps 3-4)."""

    @pytest.mark.asyncio
    async def test_no_monitor_calls_plain_invoke(self):
        """Without a monitor, harness calls invoke() not invoke_with_tap()."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string")
        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"result": "ok"})
        harness.invoker.invoke_with_tap = AsyncMock()

        await harness.run({"inputs": {}})

        harness.invoker.invoke.assert_called_once()
        harness.invoker.invoke_with_tap.assert_not_called()

    @pytest.mark.asyncio
    async def test_with_monitor_calls_invoke_with_tap(self):
        """With a monitor, harness calls invoke_with_tap() and forwards stream."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string")
        monitor = FakeMonitor()

        harness = AgentHarness(manifest, store=store, output_prefix="s3://out", monitor=monitor)
        harness.invoker = MagicMock()

        async def _fake_tap(prompt, inputs, context=None):
            async def _stream():
                yield LogEvent(
                    timestamp=None, node_id="test",
                    event_type=LogEventType.REASONING, content="thinking..."
                )
            return {"result": "monitored"}, _stream()

        harness.invoker.invoke_with_tap = _fake_tap

        result = await harness.run({"inputs": {}})
        assert result["result"] == "monitored"
        assert len(monitor.events_received) == 1
        assert monitor.events_received[0].content == "thinking..."

    @pytest.mark.asyncio
    async def test_monitor_terminate_raises(self):
        """When monitor signals termination, harness raises PreemptiveTermination."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string")
        terminate_signal = HealthSignal(
            status=HealthStatus.TERMINATE,
            should_terminate=True,
            reason="agent is looping",
        )
        monitor = FakeMonitor(signal=terminate_signal)

        harness = AgentHarness(manifest, store=store, output_prefix="s3://out", monitor=monitor)
        harness.invoker = MagicMock()

        async def _fake_tap(prompt, inputs, context=None):
            async def _stream():
                yield LogEvent(
                    timestamp=None, node_id="test",
                    event_type=LogEventType.ERROR, content="loop detected"
                )
            return {"result": "never used"}, _stream()

        harness.invoker.invoke_with_tap = _fake_tap

        with pytest.raises(PreemptiveTermination, match="looping"):
            await harness.run({"inputs": {}})


# ──────────────────────────────────────────────────────────────────────────────
# Step 5: Output write
# ──────────────────────────────────────────────────────────────────────────────


class TestOutputWrite:
    """Tests for output artifact writing (step 5)."""

    @pytest.mark.asyncio
    async def test_artifact_output_written_to_store(self):
        """Artifact outputs are serialized and written to the object store."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="artifact")
        harness = AgentHarness(manifest, store=store, output_prefix="s3://bucket/node1")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"result": {"data": [1, 2, 3]}})

        result = await harness.run({"inputs": {}})

        assert "result" in result
        ref = result["result"]
        assert ref["uri"] == "s3://bucket/node1/result"
        assert ref["content_type"] == "application/json"
        assert ref["content_hash"].startswith("sha256:")
        assert ref["bytes"] > 0
        # Verify it's actually in the store
        assert ref["uri"] in store.objects

    @pytest.mark.asyncio
    async def test_scalar_output_passes_inline(self):
        """Scalar outputs pass through without S3 write."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string")
        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"result": "inline-value"})

        result = await harness.run({"inputs": {}})
        assert result["result"] == "inline-value"
        assert len(store.objects) == 0  # nothing written to store


# ──────────────────────────────────────────────────────────────────────────────
# Step 6: Contract enforcement
# ──────────────────────────────────────────────────────────────────────────────


class TestContractEnforcement:
    """Tests for output contract validation (step 6)."""

    @pytest.mark.asyncio
    async def test_missing_required_field_raises(self):
        """Missing a required output field raises ValueError."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string", required=True)
        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"wrong_field": "oops"})

        with pytest.raises(ValueError, match="Required output field"):
            await harness.run({"inputs": {}})

    @pytest.mark.asyncio
    async def test_non_required_missing_field_ok(self):
        """Missing a non-required field doesn't raise."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string", required=False)
        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"other": "stuff"})

        result = await harness.run({"inputs": {}})
        # Should succeed without raising
        assert "result" not in result


# ──────────────────────────────────────────────────────────────────────────────
# Output mapping
# ──────────────────────────────────────────────────────────────────────────────


class TestOutputMapping:
    """Tests for output_mapping (response_key → contract_field)."""

    @pytest.mark.asyncio
    async def test_mapping_renames_keys(self):
        """output_mapping renames agent response keys to contract fields."""
        store = FakeObjectStore()
        manifest = _manifest(outputs_type="string")
        manifest["output_mapping"] = {"answer": "result"}

        harness = AgentHarness(manifest, store=store, output_prefix="s3://out")
        harness.invoker = MagicMock()
        harness.invoker.invoke = AsyncMock(return_value={"answer": "mapped!"})

        result = await harness.run({"inputs": {}})
        assert result["result"] == "mapped!"
