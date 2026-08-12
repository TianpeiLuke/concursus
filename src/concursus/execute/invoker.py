"""Unified AgentInvoker — dispatches to leaf agents based on manifest.runtime.backend.

The invoker is created by the AgentHarness in __init__ and handles wire-level dispatch.
It never touches object storage (S3) — that's the harness's job.

Supported backends:
    - callable: in-process Python function
    - agentcore: AWS Bedrock AgentCore InvokeAgent API
    - http: standalone HTTPS service
    - strands: AWS Strands Agent SDK
    - api: (stub) generic REST API backend

Dependencies (boto3, strands) are lazily imported — no hard requirement on install.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, AsyncIterator, Callable, Dict, Optional, Tuple

from .types import (
    LogEvent,
    LogEventType,
    LogSeverity,
    empty_log_stream,
)

logger = logging.getLogger(__name__)


class InvokerError(RuntimeError):
    """Raised when an invocation fails at the transport level."""

    def __init__(self, backend: str, message: str, cause: Optional[Exception] = None):
        self.backend = backend
        self.cause = cause
        super().__init__(f"[{backend}] {message}")


class AgentInvoker:
    """Unified agent invocation. Dispatches based on manifest runtime config.

    Args:
        manifest: The agent's manifest (must contain ``runtime.backend``).
        clients: Optional pre-built client bundle for connection reuse.
                 If None, clients are created per-invocation.
    """

    def __init__(self, manifest: Dict[str, Any], clients: Optional[Dict[str, Any]] = None):
        self.manifest = manifest
        self.runtime: Dict[str, Any] = manifest.get("runtime", {})
        self.backend: str = self.runtime.get("backend", "callable")
        self._clients = clients or {}
        self._callable_cache: Dict[str, Callable] = {}

    # ──────────────────────────────────────────────────────────────────────
    # Public interface
    # ──────────────────────────────────────────────────────────────────────

    async def invoke(self, prompt: str, inputs: Dict[str, Any],
                     context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fire-and-forget invoke. No log streaming.

        Returns:
            Raw dict from the leaf agent.
        """
        result, _ = await self._dispatch(prompt, inputs, context, stream=False)
        return result

    async def invoke_with_tap(self, prompt: str, inputs: Dict[str, Any],
                              context: Optional[Dict[str, Any]] = None
                              ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """Invoke with in-band log tap.

        Returns:
            Tuple of (raw result dict, async iterator of LogEvents).
            Backends that don't support streaming return an empty iterator.
        """
        return await self._dispatch(prompt, inputs, context, stream=True)

    def invoke_sync(self, prompt: str, inputs: Dict[str, Any],
                    context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Synchronous wrapper around invoke(). Runs the event loop if needed."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # We're inside an existing event loop — use a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.invoke(prompt, inputs, context))
                return future.result()
        else:
            return asyncio.run(self.invoke(prompt, inputs, context))

    # ──────────────────────────────────────────────────────────────────────
    # Dispatch router
    # ──────────────────────────────────────────────────────────────────────

    async def _dispatch(self, prompt: str, inputs: Dict[str, Any],
                        context: Optional[Dict[str, Any]], stream: bool
                        ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """Route to the appropriate backend method."""
        context = context or {}

        dispatch_table = {
            "callable": self._invoke_callable,
            "agentcore": self._invoke_agentcore,
            "http": self._invoke_http,
            "strands": self._invoke_strands,
            "api": self._invoke_api,
        }

        handler = dispatch_table.get(self.backend)
        if handler is None:
            raise InvokerError(self.backend, f"Unsupported backend: {self.backend!r}")

        return await handler(prompt, inputs, context, stream)

    # ──────────────────────────────────────────────────────────────────────
    # Backend: callable
    # ──────────────────────────────────────────────────────────────────────

    async def _invoke_callable(self, prompt: str, inputs: Dict[str, Any],
                               context: Dict[str, Any], stream: bool
                               ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """In-process Python callable — either INJECTED via ``clients`` or imported from ``entry``.

        Two ways to name the function, checked in this order:

        * ``runtime.client: "<key>"`` — resolve ``clients[key]``, a live callable handed in at
          construction. This is the **injected** form: the manifest stays
          JSON-serializable (it names a key, not a function), while the object itself arrives through
          the same ``clients`` bundle the ``strands`` and ``agentcore`` backends already use for
          connection reuse. It is the only way to hand this backend a closure, a bound method or a
          ``Mock`` — an import path cannot express any of those.
        * ``runtime.entry: "module:function"`` — import and cache. Unchanged.

        The callable may be sync or async, and may return EITHER:

        * ``dict`` — the result, with no log stream (an empty iterator), or
        * ``(dict, AsyncIterator[LogEvent])`` — result plus a live log stream.

        The tuple form matters: it is what lets an in-process agent feed the real
        :class:`~.monitor.ExecutionMonitor`. Until now this backend hardcoded
        :func:`empty_log_stream`, so a monitored callable node always reported
        ``COMPLETED, events_consumed=0`` and not one of the four monitor strategies was reachable
        through it.
        """
        client_key = self.runtime.get("client")
        if client_key is not None:
            fn = self._clients.get(client_key)
            if fn is None:
                raise InvokerError(
                    "callable", f"runtime.client {client_key!r} is not present in the clients bundle"
                )
            if not callable(fn):
                raise InvokerError(
                    "callable", f"clients[{client_key!r}] is not callable ({type(fn).__name__})"
                )
        else:
            entry = self.runtime.get("entry")
            if not entry:
                raise InvokerError(
                    "callable",
                    "runtime.client or runtime.entry is required for the callable backend",
                )
            fn = self._import_callable(entry)

        # Call the function — may be sync or async
        if asyncio.iscoroutinefunction(fn):
            result = await fn(prompt, inputs, context)
        else:
            # Run sync callable in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, fn, prompt, inputs, context)

        # A (result, log_stream) pair opts this node into real monitoring; a bare dict does not.
        log_stream: AsyncIterator[LogEvent] = empty_log_stream()
        if isinstance(result, tuple):
            if len(result) != 2:
                raise InvokerError(
                    "callable",
                    f"Callable returning a tuple must return (dict, log_stream), got "
                    f"{len(result)} elements",
                )
            result, log_stream = result

        if not isinstance(result, dict):
            raise InvokerError("callable", f"Callable must return dict, got {type(result).__name__}")

        return result, log_stream

    @lru_cache(maxsize=64)
    def _import_callable(self, entry: str) -> Callable:
        """Import and cache a 'module:function' entry point."""
        if ":" not in entry:
            raise InvokerError("callable", f"entry must be 'module:function', got {entry!r}")

        module_path, func_name = entry.rsplit(":", 1)
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise InvokerError("callable", f"Cannot import {module_path}: {e}", cause=e)

        fn = getattr(module, func_name, None)
        if fn is None:
            raise InvokerError("callable", f"{module_path} has no attribute {func_name!r}")
        if not callable(fn):
            raise InvokerError("callable", f"{entry} is not callable")

        return fn

    # ──────────────────────────────────────────────────────────────────────
    # Backend: agentcore
    # ──────────────────────────────────────────────────────────────────────

    async def _invoke_agentcore(self, prompt: str, inputs: Dict[str, Any],
                                context: Dict[str, Any], stream: bool
                                ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """AWS Bedrock AgentCore InvokeAgent API with optional trace event streaming."""
        agent_id = self.runtime.get("agent_id")
        alias_id = self.runtime.get("alias_id", "TSTALIASID")
        session_id = context.get("session_id", self._generate_session_id())
        timeout_s = self.runtime.get("timeout_s", 120)

        if not agent_id:
            raise InvokerError("agentcore", "runtime.agent_id is required")

        client = self._get_bedrock_agent_client()

        # Build the input text — serialize prompt + inputs as the agent's task
        input_text = self._build_agent_input(prompt, inputs)

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.invoke_agent(
                    agentId=agent_id,
                    agentAliasId=alias_id,
                    sessionId=session_id,
                    inputText=input_text,
                ),
            )
        except Exception as e:
            raise InvokerError("agentcore", f"InvokeAgent failed: {e}", cause=e)

        # Process the streaming response
        completion_stream = response.get("completion", [])

        if stream:
            # Return result + log stream (consumed concurrently)
            result, log_events = await self._process_agentcore_stream(completion_stream)
            return result, self._iter_events(log_events)
        else:
            # Just collect the final result, discard trace events
            result, _ = await self._process_agentcore_stream(completion_stream)
            return result, empty_log_stream()

    async def _process_agentcore_stream(
        self, completion_stream: Any
    ) -> Tuple[Dict[str, Any], list[LogEvent]]:
        """Process AgentCore response stream, extracting result + log events."""
        output_text = ""
        log_events: list[LogEvent] = []
        node_id = self.manifest.get("name", "unknown")

        for event in completion_stream:
            if "chunk" in event:
                chunk_bytes = event["chunk"].get("bytes", b"")
                output_text += chunk_bytes.decode("utf-8") if isinstance(chunk_bytes, bytes) else str(chunk_bytes)

            elif "trace" in event:
                # Trace events = log stream
                trace = event["trace"].get("trace", {})
                log_events.append(LogEvent(
                    timestamp=datetime.now(timezone.utc),
                    node_id=node_id,
                    event_type=self._classify_trace_event(trace),
                    content=json.dumps(trace, default=str)[:500],
                    severity=LogSeverity.INFO,
                    metadata=trace,
                ))

        # Parse the final output as JSON if possible
        try:
            result = json.loads(output_text)
        except (json.JSONDecodeError, TypeError):
            result = {"response": output_text}

        return result, log_events

    def _classify_trace_event(self, trace: Dict[str, Any]) -> LogEventType:
        """Map AgentCore trace event types to LogEventType."""
        if "orchestrationTrace" in trace:
            ot = trace["orchestrationTrace"]
            if "invocationInput" in ot:
                return LogEventType.TOOL_CALL
            elif "rationale" in ot:
                return LogEventType.REASONING
            elif "observation" in ot:
                return LogEventType.OUTPUT_CHUNK
        elif "failureTrace" in trace:
            return LogEventType.ERROR
        return LogEventType.PROGRESS

    def _get_bedrock_agent_client(self):
        """Get or create the bedrock-agent-runtime client."""
        if "bedrock_agent" in self._clients:
            return self._clients["bedrock_agent"]

        try:
            import boto3
        except ImportError:
            raise InvokerError(
                "agentcore",
                "boto3 is required for the agentcore backend. "
                "Install with: pip install 'concursus[agentcore]'"
            )

        region = self.runtime.get("region", "us-west-2")
        client = boto3.client("bedrock-agent-runtime", region_name=region)
        self._clients["bedrock_agent"] = client
        return client

    # ──────────────────────────────────────────────────────────────────────
    # Backend: http
    # ──────────────────────────────────────────────────────────────────────

    async def _invoke_http(self, prompt: str, inputs: Dict[str, Any],
                           context: Dict[str, Any], stream: bool
                           ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """HTTPS service invocation. SSE streaming if supported."""
        endpoint = self.runtime.get("endpoint")
        if not endpoint:
            raise InvokerError("http", "runtime.endpoint is required")

        method = self.runtime.get("method", "POST").upper()
        headers = self.runtime.get("headers", {})
        timeout_s = self.runtime.get("timeout_s", 30)

        payload = {"prompt": prompt, "inputs": inputs, "context": context}

        try:
            import aiohttp
        except ImportError:
            # Fallback to synchronous requests
            return await self._invoke_http_sync(endpoint, method, headers, timeout_s, payload)

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, endpoint,
                json=payload,
                headers={**{"Content-Type": "application/json"}, **headers},
                timeout=aiohttp.ClientTimeout(total=timeout_s),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    raise InvokerError("http", f"HTTP {resp.status}: {body[:200]}")

                # Check for SSE streaming
                if stream and resp.content_type == "text/event-stream":
                    result, events = await self._process_sse_stream(resp)
                    return result, self._iter_events(events)

                # Standard JSON response
                result = await resp.json()
                if not isinstance(result, dict):
                    raise InvokerError("http", f"Response must be JSON object, got {type(result).__name__}")
                return result, empty_log_stream()

    async def _invoke_http_sync(self, endpoint: str, method: str,
                                headers: Dict, timeout_s: int,
                                payload: Dict) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """Fallback HTTP invocation using urllib (no aiohttp dependency)."""
        import urllib.request
        import urllib.error

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            endpoint, data=data, method=method,
            headers={**{"Content-Type": "application/json"}, **headers},
        )

        try:
            loop = asyncio.get_event_loop()
            resp_bytes = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=timeout_s).read(),
            )
        except urllib.error.HTTPError as e:
            raise InvokerError("http", f"HTTP {e.code}: {e.read().decode()[:200]}", cause=e)
        except Exception as e:
            raise InvokerError("http", f"Request failed: {e}", cause=e)

        result = json.loads(resp_bytes)
        if not isinstance(result, dict):
            raise InvokerError("http", f"Response must be JSON object, got {type(result).__name__}")
        return result, empty_log_stream()

    async def _process_sse_stream(self, resp) -> Tuple[Dict[str, Any], list[LogEvent]]:
        """Process Server-Sent Events stream for log events + final result."""
        log_events: list[LogEvent] = []
        final_data = ""
        node_id = self.manifest.get("name", "unknown")

        async for line in resp.content:
            line = line.decode("utf-8").strip()
            if line.startswith("data: "):
                data = line[6:]
                try:
                    event_data = json.loads(data)
                    if event_data.get("type") == "result":
                        final_data = event_data.get("data", {})
                    else:
                        log_events.append(LogEvent(
                            timestamp=datetime.now(timezone.utc),
                            node_id=node_id,
                            event_type=LogEventType.PROGRESS,
                            content=data[:500],
                            metadata=event_data,
                        ))
                except json.JSONDecodeError:
                    final_data = data

        result = final_data if isinstance(final_data, dict) else {"response": final_data}
        return result, log_events

    # ──────────────────────────────────────────────────────────────────────
    # Backend: strands
    # ──────────────────────────────────────────────────────────────────────

    async def _invoke_strands(self, prompt: str, inputs: Dict[str, Any],
                              context: Dict[str, Any], stream: bool
                              ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """AWS Strands Agent SDK invocation. Takes a pre-built strands.Agent instance."""
        agent_instance = self._clients.get("strands_agent")
        if agent_instance is None:
            agent_instance = self._build_strands_agent()

        # Build the task message for the agent
        task_message = self._build_agent_input(prompt, inputs)

        try:
            # Strands agents are synchronous — run in executor
            loop = asyncio.get_event_loop()

            if stream:
                # Use strands streaming callback to capture events
                log_events: list[LogEvent] = []
                node_id = self.manifest.get("name", "unknown")

                def _on_event(event_type: str, data: Any):
                    """Callback hook for strands agent events."""
                    log_events.append(LogEvent(
                        timestamp=datetime.now(timezone.utc),
                        node_id=node_id,
                        event_type=self._map_strands_event(event_type),
                        content=str(data)[:500],
                        severity=LogSeverity.INFO,
                        metadata={"strands_event_type": event_type, "data": data},
                    ))

                result = await loop.run_in_executor(
                    None,
                    lambda: self._run_strands_agent(agent_instance, task_message, _on_event),
                )
                return result, self._iter_events(log_events)
            else:
                result = await loop.run_in_executor(
                    None,
                    lambda: self._run_strands_agent(agent_instance, task_message, None),
                )
                return result, empty_log_stream()

        except Exception as e:
            raise InvokerError("strands", f"Strands agent execution failed: {e}", cause=e)

    def _build_strands_agent(self):
        """Build a strands Agent from runtime config (lazy import)."""
        try:
            from strands import Agent
        except ImportError:
            raise InvokerError(
                "strands",
                "strands-agents is required for the strands backend. "
                "Install with: pip install 'concursus[strands]'"
            )

        # Build from runtime config — minimal: model + system prompt
        model_id = self.runtime.get("model_id", "anthropic.claude-sonnet-4-20250514")
        system_prompt = self.runtime.get("system_prompt", "")

        agent = Agent(model=model_id, system_prompt=system_prompt)
        self._clients["strands_agent"] = agent
        return agent

    def _run_strands_agent(self, agent, message: str,
                           callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Execute a strands agent synchronously. Returns parsed result dict."""
        # Strands Agent __call__ returns a result object
        response = agent(message)

        # Extract the response text and attempt JSON parse
        response_text = str(response)
        try:
            return json.loads(response_text)
        except (json.JSONDecodeError, TypeError):
            return {"response": response_text}

    def _map_strands_event(self, event_type: str) -> LogEventType:
        """Map strands internal event types to LogEventType."""
        mapping = {
            "tool_use": LogEventType.TOOL_CALL,
            "thinking": LogEventType.REASONING,
            "error": LogEventType.ERROR,
            "text": LogEventType.OUTPUT_CHUNK,
        }
        return mapping.get(event_type, LogEventType.PROGRESS)

    # ──────────────────────────────────────────────────────────────────────
    # Backend: api (stub)
    # ──────────────────────────────────────────────────────────────────────

    async def _invoke_api(self, prompt: str, inputs: Dict[str, Any],
                          context: Dict[str, Any], stream: bool
                          ) -> Tuple[Dict[str, Any], AsyncIterator[LogEvent]]:
        """Generic REST API backend — STUB.

        Placeholder for a future backend that differs from the HTTP backend
        in auth, payload shape, or response contract. Currently raises NotImplementedError.
        """
        raise InvokerError(
            "api",
            "The 'api' backend is a stub — not yet implemented. "
            "Use 'http' for standard REST services or 'agentcore' for Bedrock agents."
        )

    # ──────────────────────────────────────────────────────────────────────
    # Utilities
    # ──────────────────────────────────────────────────────────────────────

    def _build_agent_input(self, prompt: str, inputs: Dict[str, Any]) -> str:
        """Serialize prompt + inputs into a single text payload for the agent."""
        if inputs:
            return f"{prompt}\n\nInputs:\n{json.dumps(inputs, default=str)}"
        return prompt

    def _generate_session_id(self) -> str:
        """Generate a unique session ID for agentcore invocations."""
        import uuid
        return str(uuid.uuid4())

    async def _iter_events(self, events: list[LogEvent]) -> AsyncIterator[LogEvent]:
        """Convert a collected list of events into an async iterator."""
        for event in events:
            yield event
