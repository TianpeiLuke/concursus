"""Shared types for the execute layer (harness, invoker, monitor)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Dict, Optional


class LogSeverity(str, Enum):
    """Severity level of a log event from a leaf agent."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class LogEventType(str, Enum):
    """Categories of observable events during leaf agent execution."""

    TOOL_CALL = "tool_call"
    REASONING = "reasoning"
    ERROR = "error"
    PROGRESS = "progress"
    OUTPUT_CHUNK = "output_chunk"


class HealthStatus(str, Enum):
    """Health assessment outcome from the ExecutionMonitor."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    TERMINATE = "terminate"
    COMPLETED = "completed"


@dataclass
class LogEvent:
    """A single observable event from a leaf agent's execution.

    Produced by the AgentInvoker's log tap and consumed by the ExecutionMonitor.
    """

    timestamp: datetime
    node_id: str
    event_type: LogEventType
    content: str
    severity: LogSeverity = LogSeverity.INFO
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthSignal:
    """Assessment emitted by the ExecutionMonitor.

    ``failure_mode`` and ``evidence`` exist so a *retry* can be corrective rather than blind.
    ``reason`` is prose for a human; those two are structured for a machine. The loop detector, for
    instance, computes a tool signature to detect repetition — before ``evidence`` existed that
    signature only survived embedded in the reason string, so nothing downstream could reliably say
    WHICH tool an agent was looping on.

    Both are ``None`` / empty for a healthy or completed signal; only a terminating assessment
    populates them.
    """

    status: HealthStatus
    reason: str = ""
    should_terminate: bool = False
    events_consumed: int = 0
    #: Which strategy fired, as a stable tag: ``idle_timeout`` | ``error_threshold`` |
    #: ``tool_loop`` | ``token_budget``. A stable verdict vocabulary so a future semantic assessor
    #: can emit the same tags.
    failure_mode: Optional[str] = None
    #: Strategy-specific observations — the raw material for a prompt amendment.
    evidence: Dict[str, Any] = field(default_factory=dict)


class PreemptiveTermination(Exception):
    """Raised by the harness when the ExecutionMonitor signals early termination."""

    def __init__(self, reason: str, health_signal: Optional[HealthSignal] = None):
        self.reason = reason
        self.health_signal = health_signal
        super().__init__(f"Preemptive termination: {reason}")


@dataclass
class InvokeResult:
    """Result from invoke_with_tap: the final response + the log stream.

    The harness awaits `result` while concurrently consuming `log_stream`.
    """

    result: Dict[str, Any]
    log_stream: AsyncIterator[LogEvent]


# Type alias for an empty log stream (backends that don't support streaming)
async def empty_log_stream() -> AsyncIterator[LogEvent]:
    """Yields nothing — used by backends that don't support log streaming."""
    return
    yield  # noqa: makes this an async generator
