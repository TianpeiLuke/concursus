"""ExecutionMonitor — consumes leaf agent log streams and assesses execution health.

Implements the design in docs/design/02_execution_monitor.md:
- Consumes the AsyncIterator[LogEvent] produced by AgentInvoker.invoke_with_tap()
- Applies rule-based health strategies (idle timeout, error accumulation,
  loop detection, token budget)
- Emits a HealthSignal; `should_terminate=True` tells the harness to raise
  PreemptiveTermination

The monitor observes — it never alters plans, injects into agent execution,
or touches object storage. The Supervisor retains termination authority via
the harness's PreemptiveTermination path.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from .types import HealthSignal, HealthStatus, LogEvent, LogEventType, LogSeverity

logger = logging.getLogger(__name__)


# -- failure-mode tags ------------------------------------------------------
#: Which rule fired, as a stable tag on :attr:`~.types.HealthSignal.failure_mode`. Deliberately a
#: stable verdict vocabulary a future semantic assessor can also emit, so everything downstream keeps
#: working unchanged.
FAILURE_IDLE_TIMEOUT = "idle_timeout"
FAILURE_ERROR_THRESHOLD = "error_threshold"
FAILURE_TOOL_LOOP = "tool_loop"
FAILURE_TOKEN_BUDGET = "token_budget"


def remediation_for(signal: "HealthSignal") -> Optional[str]:
    """A prompt amendment derived from a TERMINATING signal, for a corrective retry.

    Turns the monitor's structured ``failure_mode`` + ``evidence`` into supplementary
    text the harness injects into the retry's envelope as ``remediation_context`` — never into the
    frozen compiler-vended task (an overlay, never a plan mutation).

    **This is the RULE tier's amendment, not the judge's diagnosis.** A *diagnosed* remediation is
    reserved for a future semantic assessor, on the grounds that a rule knows only WHAT
    fired and not WHY. That still holds: everything below states an observed fact plus a generic
    corrective, and never attributes a cause. It is strictly more than a blind retry (the agent is told
    what it did) and strictly less than the judge (nothing here can say the task was ambiguous).
    Principle 6 should be read as "only the judge can *diagnose*", not "the rules must stay mute".

    Returns ``None`` when there is nothing useful to say, so the caller falls back to a plain retry.
    """
    mode = getattr(signal, "failure_mode", None)
    evidence = dict(getattr(signal, "evidence", None) or {})
    if not mode:
        return None

    if mode == FAILURE_TOOL_LOOP:
        tool = evidence.get("tool")
        window = evidence.get("window")
        target = f"the tool `{tool}`" if tool else "the same tool call"
        return (
            f"On the previous attempt you called {target} {window} times in a row with identical "
            "arguments and made no progress. Do not repeat an identical tool call. Either vary the "
            "arguments, use a different tool, or conclude with the evidence you already have."
        )
    if mode == FAILURE_ERROR_THRESHOLD:
        last = evidence.get("last_error")
        tail = f' The last error was: "{last}".' if last else ""
        return (
            f"On the previous attempt you emitted {evidence.get('error_count')} errors and execution "
            f"was stopped.{tail} Address the cause before retrying the same operation, and stop and "
            "report if it is not something you can resolve."
        )
    if mode == FAILURE_IDLE_TIMEOUT:
        return (
            f"On the previous attempt you produced no output for "
            f"{evidence.get('idle_timeout_s')}s and execution was stopped. Work in smaller steps and "
            "emit progress as you go; avoid a single long silent operation."
        )
    if mode == FAILURE_TOKEN_BUDGET:
        return (
            f"On the previous attempt you exceeded the output budget "
            f"(~{evidence.get('est_tokens')} of {evidence.get('budget')} tokens). Be substantially "
            "more concise and go straight to the required output fields."
        )
    return None


@dataclass
class MonitorConfig:
    """Per-node monitor configuration, parsed from the manifest's `monitor` block.

    A node without a `monitor` block gets these defaults (timeout-only monitoring).
    """

    enabled: bool = True
    #: Seconds without any log event before the node is considered stalled.
    idle_timeout_s: float = 300.0
    #: Number of ERROR-severity events within the run before termination.
    error_threshold: int = 3
    #: Number of identical consecutive tool calls that indicates a loop.
    loop_detection_window: int = 5
    #: Approximate token budget for the node (0 = unlimited). Estimated at
    #: ~4 chars/token over event content.
    token_budget: int = 0

    @classmethod
    def from_manifest(cls, manifest: Dict[str, Any]) -> "MonitorConfig":
        """Build a MonitorConfig from a manifest dict's optional `monitor` block."""
        block = manifest.get("monitor", {})
        if not isinstance(block, dict):
            return cls()
        return cls(
            enabled=bool(block.get("enabled", True)),
            idle_timeout_s=float(block.get("idle_timeout_s", 300.0)),
            error_threshold=int(block.get("error_threshold", 3)),
            loop_detection_window=int(block.get("loop_detection_window", 5)),
            token_budget=int(block.get("token_budget", 0)),
        )


class ExecutionMonitor:
    """Rule-based health monitor for a single node invocation.

    Consumes the log stream concurrently with the invoke; assesses each event
    against the configured strategies. Returns a HealthSignal when the stream
    ends (COMPLETED) or a strategy fires (TERMINATE).

    Args:
        node_id: The node being monitored (for logging/signal attribution).
        config: Monitor thresholds. Defaults give timeout-only behavior.
        event_sink: Optional callable receiving each LogEvent (UI forwarding,
            CloudWatch, run-log). Failures in the sink never break monitoring.
    """

    def __init__(
        self,
        node_id: str,
        config: Optional[MonitorConfig] = None,
        event_sink: Optional[Any] = None,
    ):
        self.node_id = node_id
        self.config = config or MonitorConfig()
        self.event_sink = event_sink
        self.events: List[LogEvent] = []

    async def watch(self, log_stream: AsyncIterator[LogEvent]) -> HealthSignal:
        """Consume the stream, assess health per-event, return the final signal.

        The idle timeout applies BETWEEN events: if the stream produces nothing
        for `idle_timeout_s`, the node is considered stalled. An empty stream
        (backends without streaming) completes immediately — timeout-only
        monitoring for those backends is enforced by the invoke timeout itself.
        """
        if not self.config.enabled:
            # Drain without assessment so the invoke isn't back-pressured.
            async for event in log_stream:
                self.events.append(event)
                self._forward(event)
            return HealthSignal(
                status=HealthStatus.COMPLETED, events_consumed=len(self.events)
            )

        error_count = 0
        recent_tool_calls: List[str] = []
        est_tokens = 0
        iterator = log_stream.__aiter__()

        while True:
            # -- idle timeout between events -------------------------------
            try:
                event = await asyncio.wait_for(
                    iterator.__anext__(), timeout=self.config.idle_timeout_s
                )
            except StopAsyncIteration:
                return HealthSignal(
                    status=HealthStatus.COMPLETED, events_consumed=len(self.events)
                )
            except asyncio.TimeoutError:
                return HealthSignal(
                    status=HealthStatus.TERMINATE,
                    should_terminate=True,
                    reason=(
                        f"idle timeout: no log events for "
                        f"{self.config.idle_timeout_s}s"
                    ),
                    events_consumed=len(self.events),
                    failure_mode=FAILURE_IDLE_TIMEOUT,
                    evidence={"idle_timeout_s": self.config.idle_timeout_s},
                )

            self.events.append(event)
            self._forward(event)

            # -- error accumulation ----------------------------------------
            if (
                event.severity == LogSeverity.ERROR
                or event.event_type == LogEventType.ERROR
            ):
                error_count += 1
                if error_count >= self.config.error_threshold:
                    return HealthSignal(
                        status=HealthStatus.TERMINATE,
                        should_terminate=True,
                        reason=(
                            f"error threshold: {error_count} errors "
                            f"(limit {self.config.error_threshold})"
                        ),
                        events_consumed=len(self.events),
                        failure_mode=FAILURE_ERROR_THRESHOLD,
                        evidence={
                            "error_count": error_count,
                            "threshold": self.config.error_threshold,
                            "last_error": event.content[:400],
                        },
                    )

            # -- loop detection --------------------------------------------
            if event.event_type == LogEventType.TOOL_CALL:
                signature = self._tool_signature(event)
                recent_tool_calls.append(signature)
                window = self.config.loop_detection_window
                if len(recent_tool_calls) >= window and len(
                    set(recent_tool_calls[-window:])
                ) == 1:
                    return HealthSignal(
                        status=HealthStatus.TERMINATE,
                        should_terminate=True,
                        reason=(
                            f"loop detected: same tool call repeated "
                            f"{window} times ({signature[:80]})"
                        ),
                        events_consumed=len(self.events),
                        failure_mode=FAILURE_TOOL_LOOP,
                        evidence={
                            "tool": self._tool_name(event),
                            "tool_signature": signature,
                            "window": window,
                        },
                    )

            # -- token budget ----------------------------------------------
            if self.config.token_budget > 0:
                est_tokens += max(1, len(event.content) // 4)
                if est_tokens > self.config.token_budget:
                    return HealthSignal(
                        status=HealthStatus.TERMINATE,
                        should_terminate=True,
                        reason=(
                            f"token budget exceeded: ~{est_tokens} tokens "
                            f"(budget {self.config.token_budget})"
                        ),
                        events_consumed=len(self.events),
                        failure_mode=FAILURE_TOKEN_BUDGET,
                        evidence={
                            "est_tokens": est_tokens,
                            "budget": self.config.token_budget,
                        },
                    )

    def _tool_signature(self, event: LogEvent) -> str:
        """Stable identity for a tool call: tool name + args when available."""
        meta = event.metadata or {}
        tool = meta.get("tool") or meta.get("tool_name") or ""
        args = meta.get("args") or meta.get("arguments") or ""
        if tool:
            return f"{tool}:{args}"
        return event.content[:200]

    def _tool_name(self, event: LogEvent) -> Optional[str]:
        """Just the tool name, when the event declares one.

        Split out from :meth:`_tool_signature` because a remediation needs to NAME the tool ("you are
        looping on ``search``"), and the signature concatenates the args — which are the part that
        makes each call identical, not the part worth quoting back.
        """
        meta = event.metadata or {}
        return meta.get("tool") or meta.get("tool_name") or None

    def _forward(self, event: LogEvent) -> None:
        """Best-effort forward to the event sink (UI/CW). Never raises."""
        if self.event_sink is None:
            return
        try:
            self.event_sink(event)
        except Exception:  # noqa: BLE001 — sink failures never break monitoring
            logger.debug("event sink failed for node %s", self.node_id, exc_info=True)


class DefaultMonitorFactory:
    """MonitorFactory for HarnessFactory: builds an ExecutionMonitor per node.

    Reads each node's optional `monitor` manifest block. Returns None for
    nodes that explicitly disable monitoring (`monitor: {enabled: false}`),
    which makes the harness fall back to plain invoke() with zero overhead.
    """

    def __init__(self, event_sink: Optional[Any] = None):
        self.event_sink = event_sink

    def create(
        self, node_id: str, manifest: Dict[str, Any]
    ) -> Optional[ExecutionMonitor]:
        config = MonitorConfig.from_manifest(manifest)
        if not config.enabled:
            return None
        return ExecutionMonitor(
            node_id=node_id, config=config, event_sink=self.event_sink
        )
