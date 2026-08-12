"""Tests for ExecutionMonitor — rule-based health assessment over log streams."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from concursus.execute.monitor import (
    DefaultMonitorFactory,
    ExecutionMonitor,
    MonitorConfig,
)
from concursus.execute.types import (
    HealthStatus,
    LogEvent,
    LogEventType,
    LogSeverity,
)


def _event(
    event_type=LogEventType.PROGRESS,
    content="working...",
    severity=LogSeverity.INFO,
    metadata=None,
):
    return LogEvent(
        timestamp=datetime.now(timezone.utc),
        node_id="test-node",
        event_type=event_type,
        content=content,
        severity=severity,
        metadata=metadata or {},
    )


async def _stream(events, delay_s=0.0):
    """Async generator yielding the given events, optionally spaced by delay."""
    for e in events:
        if delay_s:
            await asyncio.sleep(delay_s)
        yield e


class TestMonitorConfig:
    """Config parsing from manifest."""

    def test_defaults_without_monitor_block(self):
        cfg = MonitorConfig.from_manifest({"name": "n"})
        assert cfg.enabled is True
        assert cfg.idle_timeout_s == 300.0
        assert cfg.error_threshold == 3

    def test_parses_manifest_block(self):
        cfg = MonitorConfig.from_manifest(
            {
                "monitor": {
                    "enabled": True,
                    "idle_timeout_s": 60,
                    "error_threshold": 2,
                    "loop_detection_window": 3,
                    "token_budget": 1000,
                }
            }
        )
        assert cfg.idle_timeout_s == 60.0
        assert cfg.error_threshold == 2
        assert cfg.loop_detection_window == 3
        assert cfg.token_budget == 1000

    def test_disabled_block(self):
        cfg = MonitorConfig.from_manifest({"monitor": {"enabled": False}})
        assert cfg.enabled is False


class TestHealthyCompletion:
    """Streams that finish cleanly return COMPLETED."""

    @pytest.mark.asyncio
    async def test_empty_stream_completes(self):
        monitor = ExecutionMonitor("n")
        signal = await monitor.watch(_stream([]))
        assert signal.status == HealthStatus.COMPLETED
        assert signal.should_terminate is False
        assert signal.events_consumed == 0

    @pytest.mark.asyncio
    async def test_normal_stream_completes(self):
        events = [_event(content=f"step {i}") for i in range(5)]
        monitor = ExecutionMonitor("n")
        signal = await monitor.watch(_stream(events))
        assert signal.status == HealthStatus.COMPLETED
        assert signal.events_consumed == 5
        assert len(monitor.events) == 5

    @pytest.mark.asyncio
    async def test_disabled_monitor_drains_without_assessment(self):
        # 5 errors would normally terminate — disabled monitor just drains.
        events = [
            _event(event_type=LogEventType.ERROR, severity=LogSeverity.ERROR)
            for _ in range(5)
        ]
        monitor = ExecutionMonitor("n", MonitorConfig(enabled=False))
        signal = await monitor.watch(_stream(events))
        assert signal.status == HealthStatus.COMPLETED
        assert signal.events_consumed == 5


class TestIdleTimeout:
    """No events within idle_timeout_s → TERMINATE."""

    @pytest.mark.asyncio
    async def test_idle_timeout_fires(self):
        async def stalled_stream():
            yield _event(content="first")
            await asyncio.sleep(1.0)  # exceeds the 0.05s timeout below
            yield _event(content="never seen")

        monitor = ExecutionMonitor("n", MonitorConfig(idle_timeout_s=0.05))
        signal = await monitor.watch(stalled_stream())
        assert signal.should_terminate is True
        assert "idle timeout" in signal.reason
        assert signal.events_consumed == 1

    @pytest.mark.asyncio
    async def test_fast_stream_does_not_timeout(self):
        events = [_event(content=f"e{i}") for i in range(3)]
        monitor = ExecutionMonitor("n", MonitorConfig(idle_timeout_s=5.0))
        signal = await monitor.watch(_stream(events, delay_s=0.001))
        assert signal.status == HealthStatus.COMPLETED


class TestErrorAccumulation:
    """N errors → TERMINATE."""

    @pytest.mark.asyncio
    async def test_error_threshold_fires(self):
        events = [
            _event(content="ok"),
            _event(event_type=LogEventType.ERROR, content="boom 1"),
            _event(event_type=LogEventType.ERROR, content="boom 2"),
            _event(content="never reached"),
        ]
        monitor = ExecutionMonitor("n", MonitorConfig(error_threshold=2))
        signal = await monitor.watch(_stream(events))
        assert signal.should_terminate is True
        assert "error threshold" in signal.reason
        assert signal.events_consumed == 3  # stopped at the 2nd error

    @pytest.mark.asyncio
    async def test_error_severity_counts_too(self):
        events = [
            _event(severity=LogSeverity.ERROR, content="bad output"),
            _event(severity=LogSeverity.ERROR, content="bad again"),
        ]
        monitor = ExecutionMonitor("n", MonitorConfig(error_threshold=2))
        signal = await monitor.watch(_stream(events))
        assert signal.should_terminate is True

    @pytest.mark.asyncio
    async def test_below_threshold_completes(self):
        events = [_event(event_type=LogEventType.ERROR, content="one error")]
        monitor = ExecutionMonitor("n", MonitorConfig(error_threshold=3))
        signal = await monitor.watch(_stream(events))
        assert signal.status == HealthStatus.COMPLETED


class TestLoopDetection:
    """Same tool call repeated N times → TERMINATE."""

    @pytest.mark.asyncio
    async def test_identical_tool_calls_terminate(self):
        events = [
            _event(
                event_type=LogEventType.TOOL_CALL,
                content="search",
                metadata={"tool": "search", "args": {"q": "same"}},
            )
            for _ in range(3)
        ]
        monitor = ExecutionMonitor("n", MonitorConfig(loop_detection_window=3))
        signal = await monitor.watch(_stream(events))
        assert signal.should_terminate is True
        assert "loop detected" in signal.reason

    @pytest.mark.asyncio
    async def test_varied_tool_calls_do_not_terminate(self):
        events = [
            _event(
                event_type=LogEventType.TOOL_CALL,
                content=f"tool {i}",
                metadata={"tool": "search", "args": {"q": f"query-{i}"}},
            )
            for i in range(6)
        ]
        monitor = ExecutionMonitor("n", MonitorConfig(loop_detection_window=3))
        signal = await monitor.watch(_stream(events))
        assert signal.status == HealthStatus.COMPLETED


class TestTokenBudget:
    """Estimated tokens over budget → TERMINATE."""

    @pytest.mark.asyncio
    async def test_budget_exceeded_terminates(self):
        # ~100 tokens/event at 4 chars/token; budget 150 → fires on event 2.
        events = [_event(content="x" * 400) for _ in range(5)]
        monitor = ExecutionMonitor("n", MonitorConfig(token_budget=150))
        signal = await monitor.watch(_stream(events))
        assert signal.should_terminate is True
        assert "token budget" in signal.reason
        assert signal.events_consumed == 2

    @pytest.mark.asyncio
    async def test_zero_budget_is_unlimited(self):
        events = [_event(content="x" * 4000) for _ in range(3)]
        monitor = ExecutionMonitor("n", MonitorConfig(token_budget=0))
        signal = await monitor.watch(_stream(events))
        assert signal.status == HealthStatus.COMPLETED


class TestEventSink:
    """Events forward to the sink; sink failures never break monitoring."""

    @pytest.mark.asyncio
    async def test_events_forwarded(self):
        received = []
        monitor = ExecutionMonitor("n", event_sink=received.append)
        await monitor.watch(_stream([_event(content="a"), _event(content="b")]))
        assert [e.content for e in received] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_sink_exception_swallowed(self):
        def exploding_sink(event):
            raise RuntimeError("sink down")

        monitor = ExecutionMonitor("n", event_sink=exploding_sink)
        signal = await monitor.watch(_stream([_event()]))
        assert signal.status == HealthStatus.COMPLETED


class TestDefaultMonitorFactory:
    """Factory reads manifest monitor blocks."""

    def test_creates_monitor_with_manifest_config(self):
        factory = DefaultMonitorFactory()
        monitor = factory.create("n", {"monitor": {"error_threshold": 7}})
        assert monitor is not None
        assert monitor.config.error_threshold == 7

    def test_disabled_manifest_returns_none(self):
        factory = DefaultMonitorFactory()
        assert factory.create("n", {"monitor": {"enabled": False}}) is None

    def test_no_block_returns_default_monitor(self):
        factory = DefaultMonitorFactory()
        monitor = factory.create("n", {"name": "n"})
        assert monitor is not None
        assert monitor.config.idle_timeout_s == 300.0
