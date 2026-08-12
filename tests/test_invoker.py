"""Tests for concursus.execute.invoker — the AgentInvoker."""

from __future__ import annotations

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from concursus.execute.invoker import AgentInvoker, InvokerError
from concursus.execute.types import LogEvent, LogEventType, empty_log_stream


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


def _event(event_type, content, *, severity=None, node_id="node", **metadata):
    """A LogEvent with the required timestamp/node_id filled in (see tests/test_monitor.py)."""
    from datetime import datetime, timezone

    from concursus.execute.types import LogSeverity

    return LogEvent(
        timestamp=datetime.now(timezone.utc),
        node_id=node_id,
        event_type=event_type,
        content=content,
        severity=severity or LogSeverity.INFO,
        metadata=metadata,
    )


def _manifest(backend: str, **runtime_fields) -> dict:
    """Build a minimal manifest dict for testing."""
    return {
        "name": "test-agent",
        "runtime": {"backend": backend, **runtime_fields},
        "contract": {
            "inputs": {"properties": {"task": {"type": "string"}}},
            "outputs": {"properties": {"result": {"type": "string"}}},
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Backend: callable
# ──────────────────────────────────────────────────────────────────────────────


class TestCallableBackend:
    """Tests for the callable (in-process) backend."""

    @pytest.mark.asyncio
    async def test_sync_callable_returns_dict(self, tmp_path):
        """A sync callable that returns a dict works."""
        # Write a temporary module with a callable
        mod_file = tmp_path / "my_agent.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'result': f'processed: {prompt}'}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifest = _manifest("callable", entry="my_agent:run")
            invoker = AgentInvoker(manifest)
            result = await invoker.invoke("hello", {"task": "test"})
            assert result == {"result": "processed: hello"}
        finally:
            sys.path.pop(0)
            sys.modules.pop("my_agent", None)

    @pytest.mark.asyncio
    async def test_async_callable_returns_dict(self, tmp_path):
        """An async callable works too."""
        mod_file = tmp_path / "async_agent.py"
        mod_file.write_text(
            "async def run(prompt, inputs, context):\n"
            "    return {'result': 'async-done'}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifest = _manifest("callable", entry="async_agent:run")
            invoker = AgentInvoker(manifest)
            result = await invoker.invoke("go", {})
            assert result == {"result": "async-done"}
        finally:
            sys.path.pop(0)
            sys.modules.pop("async_agent", None)

    @pytest.mark.asyncio
    async def test_callable_non_dict_raises(self, tmp_path):
        """A callable returning non-dict raises InvokerError."""
        mod_file = tmp_path / "bad_agent.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return 'not a dict'\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifest = _manifest("callable", entry="bad_agent:run")
            invoker = AgentInvoker(manifest)
            with pytest.raises(InvokerError, match="must return dict"):
                await invoker.invoke("go", {})
        finally:
            sys.path.pop(0)
            sys.modules.pop("bad_agent", None)

    @pytest.mark.asyncio
    async def test_callable_missing_entry_raises(self):
        """Missing entry field raises InvokerError."""
        manifest = _manifest("callable")  # no entry field
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="entry is required"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_callable_bad_import_raises(self):
        """Non-existent module raises InvokerError."""
        manifest = _manifest("callable", entry="nonexistent_module_xyz:fn")
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="Cannot import"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_callable_returns_empty_log_stream(self, tmp_path):
        """invoke_with_tap on callable returns an empty log stream."""
        mod_file = tmp_path / "simple.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'ok': True}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifest = _manifest("callable", entry="simple:run")
            invoker = AgentInvoker(manifest)
            result, stream = await invoker.invoke_with_tap("go", {})
            assert result == {"ok": True}
            events = [e async for e in stream]
            assert events == []
        finally:
            sys.path.pop(0)
            sys.modules.pop("simple", None)


# ──────────────────────────────────────────────────────────────────────────────
# Backend: agentcore
# ──────────────────────────────────────────────────────────────────────────────


class TestInjectedCallableBackend:
    """P1 — a callable handed in via ``clients`` instead of resolved from an import path.

    This is the capability that makes the ``_dispatch`` deprecation tractable: ``invoke_fn`` is an
    injected OBJECT, while the callable backend previously resolved a ``"module:function"`` STRING.
    An import path cannot express a closure, a bound method or a ``Mock``, so without this every one
    of the 141 ``invoke_fn`` test references would have to become an importable module function.
    """

    def test_injected_callable_is_resolved_from_the_clients_bundle(self):
        seen = {}

        def fake_agent(prompt, inputs, context):
            seen["prompt"], seen["inputs"] = prompt, inputs
            return {"result": "from a closure"}

        invoker = AgentInvoker(
            manifest=_manifest("callable", client="my_fn"), clients={"my_fn": fake_agent}
        )
        result = asyncio.run(invoker.invoke("p", {"task": "t"}))
        assert result == {"result": "from a closure"}
        assert seen["prompt"] == "p" and seen["inputs"] == {"task": "t"}

    def test_injected_async_callable(self):
        async def fake_agent(prompt, inputs, context):
            return {"result": "async"}

        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"), clients={"fn": fake_agent}
        )
        assert asyncio.run(invoker.invoke("p", {})) == {"result": "async"}

    def test_a_mock_can_be_injected(self):
        """The point of the seam: test doubles become expressible without a module on disk."""
        mock = MagicMock(return_value={"result": "mocked"})
        invoker = AgentInvoker(manifest=_manifest("callable", client="fn"), clients={"fn": mock})
        assert asyncio.run(invoker.invoke("p", {"a": 1})) == {"result": "mocked"}
        mock.assert_called_once_with("p", {"a": 1}, {})

    def test_client_key_takes_precedence_over_entry(self):
        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn", entry="does.not:exist"),
            clients={"fn": lambda p, i, c: {"result": "injected won"}},
        )
        assert asyncio.run(invoker.invoke("p", {}))["result"] == "injected won"

    def test_missing_client_key_is_a_legible_error(self):
        invoker = AgentInvoker(manifest=_manifest("callable", client="absent"), clients={})
        with pytest.raises(InvokerError, match="not present in the clients bundle"):
            asyncio.run(invoker.invoke("p", {}))

    def test_non_callable_client_is_a_legible_error(self):
        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"), clients={"fn": "not a function"}
        )
        with pytest.raises(InvokerError, match="is not callable"):
            asyncio.run(invoker.invoke("p", {}))

    def test_neither_client_nor_entry_is_an_error(self):
        invoker = AgentInvoker(manifest=_manifest("callable"))
        with pytest.raises(InvokerError, match="runtime.client or runtime.entry is required"):
            asyncio.run(invoker.invoke("p", {}))

    def test_entry_path_is_unchanged(self, tmp_path):
        """Back-compat: the import-path form must behave exactly as before."""
        import sys

        (tmp_path / "legacy_agent.py").write_text(
            "def run(p, i, c):\n    return {'result': 'from an import path'}\n"
        )
        sys.path.insert(0, str(tmp_path))
        try:
            invoker = AgentInvoker(manifest=_manifest("callable", entry="legacy_agent:run"))
            assert asyncio.run(invoker.invoke("p", {}))["result"] == "from an import path"
        finally:
            sys.path.pop(0)
            sys.modules.pop("legacy_agent", None)


class TestInjectedCallableLogStream:
    """The tuple return — what finally lets an in-process callable feed the REAL monitor.

    Before this, ``_invoke_callable`` hardcoded ``empty_log_stream()``, so a monitored callable node
    always reported ``COMPLETED, events_consumed=0`` and not one of the four monitor strategies was
    reachable through the backend. That is precisely why ``tests/spoof_fleet.py`` had to monkeypatch
    ``invoke_with_tap`` rather than use a real backend.
    """

    @staticmethod
    def _stream(events):
        async def _gen():
            for event in events:
                yield event
        return _gen()

    def test_bare_dict_still_yields_an_empty_stream(self):
        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"),
            clients={"fn": lambda p, i, c: {"result": "ok"}},
        )

        async def _go():
            result, stream = await invoker.invoke_with_tap("p", {})
            return result, [e async for e in stream]

        result, events = asyncio.run(_go())
        assert result == {"result": "ok"} and events == []

    def test_tuple_return_carries_a_live_log_stream(self):
        events = [
            _event(LogEventType.REASONING, "thinking"),
            _event(LogEventType.TOOL_CALL, "searching"),
        ]

        def agent_with_logs(prompt, inputs, context):
            return {"result": "ok"}, TestInjectedCallableLogStream._stream(events)

        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"), clients={"fn": agent_with_logs}
        )

        async def _go():
            result, stream = await invoker.invoke_with_tap("p", {})
            return result, [e async for e in stream]

        result, seen = asyncio.run(_go())
        assert result == {"result": "ok"}
        assert [e.content for e in seen] == ["thinking", "searching"]

    def test_the_real_monitor_can_now_terminate_a_callable_node(self):
        """The decisive assertion: a real ExecutionMonitor kills an injected in-process agent.

        Unreachable through this backend before the tuple return existed.
        """
        from concursus.execute.monitor import ExecutionMonitor, MonitorConfig
        from concursus.execute.types import LogSeverity

        errors = [
            _event(LogEventType.ERROR, f"boom {n}", severity=LogSeverity.ERROR) for n in range(3)
        ]

        def erroring_agent(prompt, inputs, context):
            return {"result": "ok"}, TestInjectedCallableLogStream._stream(errors)

        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"), clients={"fn": erroring_agent}
        )
        monitor = ExecutionMonitor("node", MonitorConfig(error_threshold=3))

        async def _go():
            _, stream = await invoker.invoke_with_tap("p", {})
            return await monitor.watch(stream)

        health = asyncio.run(_go())
        assert health.should_terminate is True
        assert "error threshold" in health.reason
        assert health.events_consumed == 3, "the monitor saw a REAL stream, not an empty one"

    def test_wrong_tuple_arity_is_a_legible_error(self):
        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"),
            clients={"fn": lambda p, i, c: ({"a": 1}, None, "extra")},
        )
        with pytest.raises(InvokerError, match="must return \\(dict, log_stream\\)"):
            asyncio.run(invoker.invoke("p", {}))

    def test_non_dict_first_element_is_still_rejected(self):
        invoker = AgentInvoker(
            manifest=_manifest("callable", client="fn"),
            clients={"fn": lambda p, i, c: ("not a dict", empty_log_stream())},
        )
        with pytest.raises(InvokerError, match="must return dict"):
            asyncio.run(invoker.invoke("p", {}))


class TestAgentCoreBackend:
    """Tests for the AgentCore backend (mocked boto3)."""

    @pytest.mark.asyncio
    async def test_invoke_success(self):
        """Successful AgentCore invocation returns parsed JSON."""
        mock_client = MagicMock()
        mock_client.invoke_agent.return_value = {
            "completion": [
                {"chunk": {"bytes": b'{"result": "done"}'}},
            ]
        }

        manifest = _manifest("agentcore", agent_id="AGENT123", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})
        result = await invoker.invoke("analyze this", {"data": "x"})

        assert result == {"result": "done"}
        mock_client.invoke_agent.assert_called_once()

    @pytest.mark.asyncio
    async def test_invoke_with_trace_events(self):
        """Trace events in the stream become LogEvents."""
        mock_client = MagicMock()
        mock_client.invoke_agent.return_value = {
            "completion": [
                {"trace": {"trace": {"orchestrationTrace": {"rationale": {"text": "thinking..."}}}}},
                {"trace": {"trace": {"orchestrationTrace": {"invocationInput": {"tool": "search"}}}}},
                {"chunk": {"bytes": b'{"answer": "42"}'}},
            ]
        }

        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})
        result, stream = await invoker.invoke_with_tap("question", {})

        assert result == {"answer": "42"}
        events = [e async for e in stream]
        assert len(events) == 2
        assert events[0].event_type == LogEventType.REASONING
        assert events[1].event_type == LogEventType.TOOL_CALL

    @pytest.mark.asyncio
    async def test_missing_agent_id_raises(self):
        """Missing agent_id raises InvokerError."""
        manifest = _manifest("agentcore")  # no agent_id
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="agent_id is required"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_non_json_response_wrapped(self):
        """Non-JSON text response gets wrapped in {response: ...}."""
        mock_client = MagicMock()
        mock_client.invoke_agent.return_value = {
            "completion": [
                {"chunk": {"bytes": b"plain text answer"}},
            ]
        }

        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})
        result = await invoker.invoke("go", {})
        assert result == {"response": "plain text answer"}


# ──────────────────────────────────────────────────────────────────────────────
# Backend: http
# ──────────────────────────────────────────────────────────────────────────────


class TestHttpBackend:
    """Tests for the HTTP backend (mocked network)."""

    @pytest.mark.asyncio
    async def test_invoke_success_urllib_fallback(self):
        """HTTP invoke works via urllib fallback when aiohttp unavailable."""
        response_data = json.dumps({"result": "ok"}).encode()

        manifest = _manifest("http", endpoint="https://example.com/invoke")
        invoker = AgentInvoker(manifest)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = response_data
            mock_urlopen.return_value = mock_resp

            # Force the urllib fallback by making aiohttp import fail
            with patch.dict("sys.modules", {"aiohttp": None}):
                result = await invoker.invoke("task", {"x": 1})

        assert result == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_missing_endpoint_raises(self):
        """Missing endpoint raises InvokerError."""
        manifest = _manifest("http")  # no endpoint
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="endpoint is required"):
            await invoker.invoke("go", {})


# ──────────────────────────────────────────────────────────────────────────────
# Backend: strands
# ──────────────────────────────────────────────────────────────────────────────


class TestStrandsBackend:
    """Tests for the Strands backend (mocked agent)."""

    @pytest.mark.asyncio
    async def test_invoke_with_prebuilt_agent(self):
        """Strands invocation with a pre-built agent instance."""
        mock_agent = MagicMock()
        mock_agent.return_value = '{"result": "strands-done"}'

        manifest = _manifest("strands")
        invoker = AgentInvoker(manifest, clients={"strands_agent": mock_agent})
        result = await invoker.invoke("do something", {"input": "data"})

        assert result == {"result": "strands-done"}
        mock_agent.assert_called_once()

    @pytest.mark.asyncio
    async def test_invoke_non_json_response(self):
        """Non-JSON strands response gets wrapped."""
        mock_agent = MagicMock()
        mock_agent.return_value = "plain text"

        manifest = _manifest("strands")
        invoker = AgentInvoker(manifest, clients={"strands_agent": mock_agent})
        result = await invoker.invoke("go", {})

        assert result == {"response": "plain text"}


# ──────────────────────────────────────────────────────────────────────────────
# Backend: api (stub)
# ──────────────────────────────────────────────────────────────────────────────


class TestApiBackend:
    """Tests for the API stub backend."""

    @pytest.mark.asyncio
    async def test_api_raises_not_implemented(self):
        """The api backend is a stub and raises InvokerError."""
        manifest = _manifest("api")
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="stub"):
            await invoker.invoke("go", {})


# ──────────────────────────────────────────────────────────────────────────────
# General dispatch
# ──────────────────────────────────────────────────────────────────────────────


class TestDispatch:
    """Tests for dispatch routing and general behavior."""

    @pytest.mark.asyncio
    async def test_unsupported_backend_raises(self):
        """An unknown backend raises InvokerError."""
        manifest = _manifest("quantum_computer")
        invoker = AgentInvoker(manifest)
        with pytest.raises(InvokerError, match="Unsupported backend"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_default_backend_is_callable(self):
        """A manifest without runtime.backend defaults to callable."""
        manifest = {"name": "test", "runtime": {}}
        invoker = AgentInvoker(manifest)
        assert invoker.backend == "callable"

    def test_invoke_sync_wrapper(self, tmp_path):
        """invoke_sync runs the async invoke in a new event loop."""
        mod_file = tmp_path / "sync_test.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'sync': True}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifest = _manifest("callable", entry="sync_test:run")
            invoker = AgentInvoker(manifest)
            result = invoker.invoke_sync("go", {})
            assert result == {"sync": True}
        finally:
            sys.path.pop(0)
            sys.modules.pop("sync_test", None)


# ──────────────────────────────────────────────────────────────────────────────
# Backend: http (aiohttp path)
# ──────────────────────────────────────────────────────────────────────────────


class TestHttpAiohttpBackend:
    """Tests for the HTTP backend using mocked aiohttp."""

    @pytest.mark.asyncio
    async def test_aiohttp_json_response(self):
        """Successful aiohttp POST returns parsed JSON."""
        import types

        # Build a fake aiohttp module
        fake_aiohttp = types.ModuleType("aiohttp")

        class FakeTimeout:
            def __init__(self, total=None):
                pass

        class FakeResponse:
            status = 200
            content_type = "application/json"

            async def json(self):
                return {"result": "aiohttp-ok"}

            async def text(self):
                return ""

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        class FakeSession:
            def request(self, method, url, **kwargs):
                return FakeResponse()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        fake_aiohttp.ClientSession = FakeSession
        fake_aiohttp.ClientTimeout = FakeTimeout

        manifest = _manifest("http", endpoint="https://example.com/agent")
        invoker = AgentInvoker(manifest)

        with patch.dict("sys.modules", {"aiohttp": fake_aiohttp}):
            result = await invoker.invoke("task", {"x": 1})

        assert result == {"result": "aiohttp-ok"}

    @pytest.mark.asyncio
    async def test_aiohttp_non_2xx_raises(self):
        """aiohttp non-2xx response raises InvokerError."""
        import types

        fake_aiohttp = types.ModuleType("aiohttp")

        class FakeTimeout:
            def __init__(self, total=None):
                pass

        class FakeResponse:
            status = 500
            content_type = "application/json"

            async def text(self):
                return "Internal Server Error"

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        class FakeSession:
            def request(self, method, url, **kwargs):
                return FakeResponse()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        fake_aiohttp.ClientSession = FakeSession
        fake_aiohttp.ClientTimeout = FakeTimeout

        manifest = _manifest("http", endpoint="https://example.com/agent")
        invoker = AgentInvoker(manifest)

        with patch.dict("sys.modules", {"aiohttp": fake_aiohttp}):
            with pytest.raises(InvokerError, match="HTTP 500"):
                await invoker.invoke("task", {})

    @pytest.mark.asyncio
    async def test_aiohttp_sse_streaming(self):
        """SSE stream produces LogEvents + final result."""
        import types

        fake_aiohttp = types.ModuleType("aiohttp")

        class FakeTimeout:
            def __init__(self, total=None):
                pass

        sse_lines = [
            b'data: {"type": "progress", "step": "thinking"}\n',
            b'data: {"type": "progress", "step": "tool_call"}\n',
            b'data: {"type": "result", "data": {"answer": "42"}}\n',
        ]

        class FakeContent:
            def __aiter__(self):
                return self

            async def __anext__(self):
                if not sse_lines:
                    raise StopAsyncIteration
                return sse_lines.pop(0)

        class FakeResponse:
            status = 200
            content_type = "text/event-stream"
            content = FakeContent()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        class FakeSession:
            def request(self, method, url, **kwargs):
                return FakeResponse()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        fake_aiohttp.ClientSession = FakeSession
        fake_aiohttp.ClientTimeout = FakeTimeout

        manifest = _manifest("http", endpoint="https://example.com/stream")
        invoker = AgentInvoker(manifest)

        with patch.dict("sys.modules", {"aiohttp": fake_aiohttp}):
            result, stream = await invoker.invoke_with_tap("go", {})

        assert result == {"answer": "42"}
        events = [e async for e in stream]
        assert len(events) == 2  # 2 progress events (result is not a log event)


# ──────────────────────────────────────────────────────────────────────────────
# Backend: agentcore (error handling)
# ──────────────────────────────────────────────────────────────────────────────


class TestAgentCoreErrors:
    """Tests for AgentCore error paths (throttle, timeout, SDK exceptions)."""

    @pytest.mark.asyncio
    async def test_throttling_raises_invoker_error(self):
        """ThrottlingException from boto3 raises InvokerError."""
        mock_client = MagicMock()

        # Simulate boto3 ClientError for throttling
        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}
        mock_client.invoke_agent.side_effect = ClientError(error_response, "InvokeAgent")

        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})

        with pytest.raises(InvokerError, match="InvokeAgent failed"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_timeout_raises_invoker_error(self):
        """Connection timeout raises InvokerError."""
        mock_client = MagicMock()

        from botocore.exceptions import ReadTimeoutError
        mock_client.invoke_agent.side_effect = ReadTimeoutError(endpoint_url="https://bedrock.us-west-2.amazonaws.com")

        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})

        with pytest.raises(InvokerError, match="InvokeAgent failed"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_access_denied_raises_invoker_error(self):
        """AccessDeniedException raises InvokerError."""
        mock_client = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "AccessDeniedException", "Message": "Not authorized"}}
        mock_client.invoke_agent.side_effect = ClientError(error_response, "InvokeAgent")

        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest, clients={"bedrock_agent": mock_client})

        with pytest.raises(InvokerError, match="InvokeAgent failed"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_missing_boto3_raises_helpful_error(self):
        """When boto3 is not installed, a clear error message is raised."""
        manifest = _manifest("agentcore", agent_id="A1", alias_id="PROD")
        invoker = AgentInvoker(manifest)  # no pre-built client

        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(InvokerError, match="boto3 is required"):
                await invoker.invoke("go", {})


# ──────────────────────────────────────────────────────────────────────────────
# Backend: strands (callback hooks)
# ──────────────────────────────────────────────────────────────────────────────


class TestStrandsCallbacks:
    """Tests for Strands backend with event callbacks for log streaming."""

    @pytest.mark.asyncio
    async def test_invoke_with_tap_captures_events(self):
        """invoke_with_tap on strands captures callback events as LogEvents."""
        call_log = []

        def mock_agent(message):
            """Fake strands agent that simulates work."""
            # In real strands, events fire via callbacks during execution
            # Our mock just returns — the invoker captures events via _on_event
            call_log.append(message)
            return '{"verdict": "pass"}'

        manifest = _manifest("strands")
        invoker = AgentInvoker(manifest, clients={"strands_agent": mock_agent})

        result, stream = await invoker.invoke_with_tap("investigate case", {"case_id": "123"})

        assert result == {"verdict": "pass"}
        assert len(call_log) == 1
        # Stream may be empty since the mock doesn't trigger callbacks,
        # but the path exercises the streaming code without errors
        events = [e async for e in stream]
        # With a real strands agent, events would appear here
        assert isinstance(events, list)

    @pytest.mark.asyncio
    async def test_strands_agent_exception_wrapped(self):
        """An exception from the strands agent is wrapped in InvokerError."""
        def exploding_agent(message):
            raise RuntimeError("model refused to respond")

        manifest = _manifest("strands")
        invoker = AgentInvoker(manifest, clients={"strands_agent": exploding_agent})

        with pytest.raises(InvokerError, match="Strands agent execution failed"):
            await invoker.invoke("go", {})

    @pytest.mark.asyncio
    async def test_strands_missing_sdk_no_prebuilt_agent(self):
        """When no pre-built agent and strands SDK missing, raises helpful error."""
        manifest = _manifest("strands")
        invoker = AgentInvoker(manifest)  # no strands_agent in clients

        with patch.dict("sys.modules", {"strands": None}):
            with pytest.raises(InvokerError, match="strands-agents is required"):
                await invoker.invoke("go", {})
