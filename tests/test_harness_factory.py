"""Tests for HarnessFactory — the NodeExecutor bridge into Supervisor."""

from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch

from concursus.core.manifest import AgentManifest
from concursus.execute.harness_factory import HarnessFactory, HARNESS_NODE_KIND
from concursus.execute.monitor import DefaultMonitorFactory


class FakeObjectStore:
    """In-memory object store."""
    def __init__(self):
        self.objects = {}

    async def get_object(self, uri):
        return self.objects.get(uri, b'{}')

    async def put_object(self, uri, data, content_type):
        self.objects[uri] = data
        return uri


class TestHarnessFactory:
    """Tests for HarnessFactory construction."""

    def test_create_harness_builds_with_prefix(self):
        """create_harness computes output prefix from root + session + node."""
        manifests = {
            "scorer": {
                "name": "scorer",
                "runtime": {"backend": "callable", "entry": "x:y"},
                "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {}}},
            }
        }
        store = FakeObjectStore()
        factory = HarnessFactory(manifests=manifests, store=store, output_prefix_root="s3://bucket/out")

        harness = factory.create_harness("scorer", "session-123")
        assert harness.output_prefix == "s3://bucket/out/session-123/scorer"
        assert harness.store is store

    def test_create_harness_with_monitor(self):
        """Monitor factory injects a monitor into the harness."""
        manifests = {"n1": {"name": "n1", "runtime": {"backend": "callable", "entry": "x:y"},
                           "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {}}}}}
        store = FakeObjectStore()
        mock_monitor = MagicMock()
        mock_mf = MagicMock()
        mock_mf.create.return_value = mock_monitor

        factory = HarnessFactory(manifests=manifests, store=store, monitor_factory=mock_mf)
        harness = factory.create_harness("n1", "s1")
        assert harness.monitor is mock_monitor

    def test_make_kind_fn_routes_runtime_nodes(self):
        """Nodes with 'runtime' route to harness kind; others to default."""
        manifests = {
            "new_agent": {"name": "new_agent", "runtime": {"backend": "agentcore", "agent_id": "X"}},
            "legacy_agent": {"name": "legacy_agent", "registry": {"agent_runtime_arn": "arn:..."}},
        }
        factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
        kind_fn = factory.make_kind_fn()

        assert kind_fn("new_agent") == HARNESS_NODE_KIND
        assert kind_fn("legacy_agent") == "default"
        assert kind_fn("unknown_node") == "default"

    def test_make_executor_returns_callable(self):
        """make_executor returns a function with the NodeExecutor signature."""
        manifests = {"n": {"name": "n", "runtime": {"backend": "callable", "entry": "x:y"},
                          "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {}}}}}
        factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
        executor = factory.make_executor()
        assert callable(executor)


class TestHarnessExecutor:
    """Tests for the NodeExecutor that runs through the harness."""

    def test_executor_runs_callable_agent(self, tmp_path):
        """The executor routes through harness → invoker → callable agent."""
        mod_file = tmp_path / "exec_agent.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'score': 0.99}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            manifests = {
                "scorer": {
                    "name": "scorer",
                    "runtime": {"backend": "callable", "entry": "exec_agent:run"},
                    "contract": {
                        "inputs": {"properties": {"task": {"type": "string"}}},
                        "outputs": {"properties": {"score": {"type": "number"}}},
                    },
                }
            }
            store = FakeObjectStore()
            factory = HarnessFactory(manifests=manifests, store=store)
            executor = factory.make_executor()

            # Mock a minimal Supervisor
            mock_supervisor = MagicMock()
            mock_supervisor._manifests = {
                "scorer": MagicMock(name="scorer", output_schema={"properties": {"score": {"type": "number"}}})
            }
            mock_supervisor._manifests["scorer"].name = "scorer"
            mock_supervisor._plan = MagicMock()
            mock_supervisor._plan.payload_contract = {
                "scorer": {"task": "evaluate the input", "static_context": "You are a scorer."}
            }
            mock_supervisor._session_id = "test-session"
            mock_supervisor._store = MagicMock()
            mock_supervisor._store.completed.return_value = set()

            # Call the executor
            executor(mock_supervisor, "scorer", {"scorer": {"data": "test"}}, [])

            # Verify it stored the result
            mock_supervisor._store.put.assert_called_once()
            call_args = mock_supervisor._store.put.call_args
            assert call_args[0][0] == "scorer"  # node_id
            result = call_args[0][1]
            assert result["score"] == 0.99
        finally:
            sys.path.pop(0)
            sys.modules.pop("exec_agent", None)

    def test_executor_records_preemptive_termination(self, tmp_path):
        """PreemptiveTermination from monitor is recorded as failure."""
        mod_file = tmp_path / "slow_agent.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'x': 1}\n"
        )

        import sys
        sys.path.insert(0, str(tmp_path))
        try:
            from concursus.execute.types import HealthSignal, HealthStatus

            # Create a monitor that always terminates
            class TerminatingMonitor:
                async def watch(self, stream):
                    async for _ in stream:
                        pass
                    return HealthSignal(
                        status=HealthStatus.TERMINATE,
                        should_terminate=True,
                        reason="looping detected",
                    )

            class TerminatingMonitorFactory:
                def create(self, node_id, manifest):
                    return TerminatingMonitor()

            manifests = {
                "bad_node": {
                    "name": "bad_node",
                    "runtime": {"backend": "callable", "entry": "slow_agent:run"},
                    "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {"x": {"type": "number"}}}},
                }
            }
            store = FakeObjectStore()
            factory = HarnessFactory(
                manifests=manifests, store=store, monitor_factory=TerminatingMonitorFactory()
            )
            executor = factory.make_executor()

            mock_supervisor = MagicMock()
            mock_supervisor._manifests = {"bad_node": MagicMock(name="bad_node")}
            mock_supervisor._manifests["bad_node"].name = "bad_node"
            mock_supervisor._plan = MagicMock()
            mock_supervisor._plan.payload_contract = {}
            mock_supervisor._session_id = "s1"
            mock_supervisor._store = MagicMock()
            mock_supervisor._store.completed.return_value = set()

            executor(mock_supervisor, "bad_node", {}, [])

            # Should store a failure, not a result
            call_args = mock_supervisor._store.put.call_args
            stored = call_args[0][1]
            assert "error" in stored
            assert "PreemptiveTermination" in stored["error_type"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("slow_agent", None)


class TestHarnessExecutorCancellation:
    """The harness side of futility: self-registration plus the futility-cancelled record."""

    @staticmethod
    def _agent(tmp_path, sys_module):
        mod_file = tmp_path / "cancel_agent.py"
        mod_file.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'x': 1}\n"
        )
        sys_module.path.insert(0, str(tmp_path))
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": "cancel_agent:run"},
                "contract": {
                    "inputs": {"properties": {}},
                    "outputs": {"properties": {"x": {"type": "number"}}},
                },
            }
        }

    @staticmethod
    def _mock_supervisor(tokens):
        sup = MagicMock()
        sup._manifests = {"node": MagicMock(name="node")}
        sup._manifests["node"].name = "node"
        sup._plan = MagicMock()
        sup._plan.payload_contract = {}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = tokens
        return sup

    def test_condemned_node_records_the_futility_class_and_reason(self, tmp_path):
        """A node condemned before it registers is cancelled and records the retained reason."""
        import sys

        from concursus.execute.futility import CancelTokenRegistry
        from concursus.execute.supervisor import _FAILURE_FUTILITY

        manifests = self._agent(tmp_path, sys)
        try:
            tokens = CancelTokenRegistry()
            tokens.condemn("node", "futility-cancelled on upstream")

            factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
            sup = self._mock_supervisor(tokens)

            factory.make_executor()(sup, "node", {}, [])

            args, kwargs = sup._store.put.call_args
            assert args[1]["error_type"] == "CancelledError"
            meta = kwargs["meta"]
            assert meta["status"] == "failed"
            # Guards drift between the literal written here and the Supervisor's constant.
            assert meta["failure_class"] == _FAILURE_FUTILITY
            assert meta["blocked_on"] == "futility-cancelled on upstream"
        finally:
            sys.path.pop(0)
            sys.modules.pop("cancel_agent", None)

    def test_registry_absent_leaves_the_call_unwrapped(self, tmp_path):
        """Default path: no registry means the agent runs and stores its result as before."""
        import sys

        manifests = self._agent(tmp_path, sys)
        try:
            factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
            sup = self._mock_supervisor(None)

            factory.make_executor()(sup, "node", {}, [])

            args, kwargs = sup._store.put.call_args
            assert args[1]["x"] == 1
            assert "status" not in kwargs["meta"]  # a success record, not a failure
        finally:
            sys.path.pop(0)
            sys.modules.pop("cancel_agent", None)

    def test_preemptive_termination_literal_matches_the_supervisor_constant(self, tmp_path):
        """Drift guard for the OTHER literal this module writes."""
        import sys

        from concursus.execute.supervisor import _FAILURE_PREEMPTIVE
        from concursus.execute.types import HealthSignal, HealthStatus

        class TerminatingMonitor:
            async def watch(self, stream):
                async for _ in stream:
                    pass
                return HealthSignal(
                    status=HealthStatus.TERMINATE, should_terminate=True, reason="loop"
                )

        class TerminatingMonitorFactory:
            def create(self, node_id, manifest):
                return TerminatingMonitor()

        manifests = self._agent(tmp_path, sys)
        try:
            factory = HarnessFactory(
                manifests=manifests,
                store=FakeObjectStore(),
                monitor_factory=TerminatingMonitorFactory(),
            )
            sup = self._mock_supervisor(None)

            factory.make_executor()(sup, "node", {}, [])

            assert sup._store.put.call_args[1]["meta"]["failure_class"] == _FAILURE_PREEMPTIVE
        finally:
            sys.path.pop(0)
            sys.modules.pop("cancel_agent", None)


class TestHarnessExecutorFailureSymmetry:
    """A harness node that raises is RECORDED under ``on_error='record'``, as ``_dispatch`` does.

    The executor previously caught only ``PreemptiveTermination`` and ``asyncio.CancelledError``, so
    any other exception escaped to ``_run_parallel`` and aborted the WHOLE run even under
    ``on_error='record'`` — unlike :meth:`Supervisor._dispatch`, which records. The most important
    case is a CONTRACT VIOLATION from ``harness._check_contract``: the agent ran healthily, returned,
    and simply did not produce a required output field. That is "task not completed", and it must prune
    only its own subtree rather than kill the run.
    """

    @staticmethod
    def _incomplete_agent(tmp_path, sys_module):
        """A manifest whose contract REQUIRES ``finding``, plus an agent that never returns it."""
        mod = tmp_path / "incomplete_agent.py"
        mod.write_text(
            "def run(prompt, inputs, context):\n"
            "    return {'other': 'present but not the required field'}\n"
        )
        sys_module.path.insert(0, str(tmp_path))
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": "incomplete_agent:run"},
                "contract": {
                    "inputs": {"properties": {}},
                    # `required` is what makes _check_contract raise; without it the gate is silent.
                    "outputs": {"properties": {"finding": {"type": "string", "required": True}}},
                },
            }
        }

    @staticmethod
    def _supervisor(on_error):
        sup = MagicMock()
        sup._manifests = {"node": MagicMock(name="node")}
        sup._manifests["node"].name = "node"
        sup._plan = MagicMock()
        sup._plan.payload_contract = {}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = None
        # MagicMock would auto-create `_on_error` as a Mock (never == "record"), so set it explicitly.
        sup._on_error = on_error
        return sup

    def test_contract_violation_is_recorded_as_crash_under_record(self, tmp_path):
        import sys

        from concursus.execute.supervisor import _FAILURE_CRASH

        manifests = self._incomplete_agent(tmp_path, sys)
        try:
            factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
            sup = self._supervisor("record")

            factory.make_executor()(sup, "node", {}, [])  # must NOT raise

            args, kwargs = sup._store.put.call_args
            meta = kwargs["meta"]
            assert meta["status"] == "failed"
            assert meta["failure_class"] == _FAILURE_CRASH
            assert args[1]["error_type"] == "ValueError"
            assert "finding" in args[1]["error"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("incomplete_agent", None)


    def test_contract_violation_still_propagates_under_raise(self, tmp_path):
        # Symmetry cuts both ways: 'raise' must stay fail-fast, exactly as _dispatch does.
        import sys

        manifests = self._incomplete_agent(tmp_path, sys)
        try:
            factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
            sup = self._supervisor("raise")
            with pytest.raises(ValueError, match="finding"):
                factory.make_executor()(sup, "node", {}, [])
            sup._store.put.assert_not_called()
        finally:
            sys.path.pop(0)
            sys.modules.pop("incomplete_agent", None)

    def test_absent_on_error_attribute_defaults_to_raising(self, tmp_path):
        # Defensive: the executor reads on_error via `getattr(supervisor, "_on_error", "raise")`, so a
        # supervisor that never exposes the attribute must fail fast rather than silently swallow.
        # A MagicMock would auto-create it, so this uses a plain object that genuinely lacks it.
        import sys
        import types as _types

        manifests = self._incomplete_agent(tmp_path, sys)
        try:
            factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
            manifest_stub = MagicMock(name="node")
            manifest_stub.name = "node"
            sup = _types.SimpleNamespace(
                _manifests={"node": manifest_stub},
                _plan=_types.SimpleNamespace(payload_contract={}),
                _session_id="s1",
                _store=MagicMock(),
                _cancel_tokens=None,
                # deliberately NO _on_error
            )
            sup._store.completed.return_value = set()
            assert not hasattr(sup, "_on_error")

            with pytest.raises(ValueError, match="finding"):
                factory.make_executor()(sup, "node", {}, [])
            sup._store.put.assert_not_called()
        finally:
            sys.path.pop(0)
            sys.modules.pop("incomplete_agent", None)

class TestHarnessRetryPolicy:
    """``max_attempts`` now means the same on both node-kind branches.

    Three constraints are asserted here rather than assumed, because each is a decision:
    a side-effecting node is NEVER retried, a futility cancellation is NEVER retried, and each
    attempt gets a FRESH harness (so the monitor's accumulated counts do not re-trip instantly).
    """

    @staticmethod
    def _flaky(tmp_path, sys_module, fail_times: int, *, name="flaky_agent"):
        """An agent that raises on its first ``fail_times`` calls, then succeeds.

        Call state lives on the module, so it survives the fresh-harness-per-attempt rebuild while
        still proving each attempt is a distinct invocation.
        """
        (tmp_path / f"{name}.py").write_text(
            "CALLS = 0\n"
            "def run(p, i, c):\n"
            "    global CALLS\n"
            "    CALLS += 1\n"
            f"    if CALLS <= {fail_times}:\n"
            "        raise RuntimeError(f'transient {CALLS}')\n"
            "    return {'finding': f'ok after {CALLS}'}\n"
        )
        sys_module.path.insert(0, str(tmp_path))

    @staticmethod
    def _manifests(entry: str, *, side_effecting: bool = False):
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": entry},
                "side_effecting": side_effecting,
                "contract": {
                    "inputs": {"properties": {}},
                    "outputs": {"properties": {"finding": {"type": "string", "required": True}}},
                },
            }
        }

    @staticmethod
    def _supervisor(*, max_attempts=1, side_effecting=False, on_error="record"):
        sup = MagicMock()
        manifest = MagicMock()
        manifest.name = "node"
        manifest.output_schema = {}
        manifest.side_effecting = side_effecting
        sup._manifests = {"node": manifest}
        sup._plan = MagicMock()
        sup._plan.payload_contract = {}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = None
        sup._on_error = on_error
        sup._max_attempts = max_attempts
        sup._check_acceptance = False
        sup._acceptance_fn = None
        return sup

    def test_a_transient_failure_is_retried_and_then_succeeds(self, tmp_path):
        import sys

        self._flaky(tmp_path, sys, fail_times=2)
        try:
            factory = HarnessFactory(
                manifests=self._manifests("flaky_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(max_attempts=3)

            factory.make_executor()(sup, "node", {}, [])

            import flaky_agent

            assert flaky_agent.CALLS == 3, "the executor did not retry"
            meta = sup._store.put.call_args.kwargs["meta"]
            assert "status" not in meta, "a node that eventually succeeded was recorded as failed"
        finally:
            sys.path.pop(0)
            sys.modules.pop("flaky_agent", None)

    def test_retries_are_bounded_by_max_attempts(self, tmp_path):
        import sys

        from concursus.execute.supervisor import _FAILURE_CRASH

        self._flaky(tmp_path, sys, fail_times=99)
        try:
            factory = HarnessFactory(
                manifests=self._manifests("flaky_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(max_attempts=3)

            factory.make_executor()(sup, "node", {}, [])

            import flaky_agent

            assert flaky_agent.CALLS == 3, "attempts were not bounded by max_attempts"
            meta = sup._store.put.call_args.kwargs["meta"]
            assert meta["failure_class"] == _FAILURE_CRASH
            # matches _dispatch: the attempt is encoded so successive tries stay distinguishable
            assert meta["address"] == "node/3"
        finally:
            sys.path.pop(0)
            sys.modules.pop("flaky_agent", None)

    def test_a_side_effecting_node_is_never_retried(self, tmp_path):
        """The hazard this guards: retrying an agent that already acted repeats the side effect."""
        import sys

        self._flaky(tmp_path, sys, fail_times=99)
        try:
            factory = HarnessFactory(
                manifests=self._manifests("flaky_agent:run", side_effecting=True),
                store=FakeObjectStore(),
            )
            sup = self._supervisor(max_attempts=5, side_effecting=True)

            factory.make_executor()(sup, "node", {}, [])

            import flaky_agent

            assert flaky_agent.CALLS == 1, "a side-effecting node was retried"
            # single-attempt nodes keep the bare node id, as before the retry loop existed
            assert sup._store.put.call_args.kwargs["meta"]["address"] == "node"
        finally:
            sys.path.pop(0)
            sys.modules.pop("flaky_agent", None)

    def test_default_max_attempts_is_still_one(self, tmp_path):
        """Back-compat: the retry loop must be inert unless a caller dials it up."""
        import sys

        self._flaky(tmp_path, sys, fail_times=99)
        try:
            factory = HarnessFactory(
                manifests=self._manifests("flaky_agent:run"), store=FakeObjectStore()
            )
            factory.make_executor()(self._supervisor(), "node", {}, [])

            import flaky_agent

            assert flaky_agent.CALLS == 1
        finally:
            sys.path.pop(0)
            sys.modules.pop("flaky_agent", None)

    def test_a_monitor_termination_is_retried(self, tmp_path):
        """even a rule-detected failure fires terminate->retry."""
        import sys

        (tmp_path / "unhealthy_agent.py").write_text(
            "CALLS = 0\n"
            "def run(p, i, c):\n"
            "    global CALLS\n"
            "    CALLS += 1\n"
            "    from concursus.execute.types import PreemptiveTermination\n"
            "    if CALLS <= 1:\n"
            "        raise PreemptiveTermination('loop detected: spoofed')\n"
            "    return {'finding': 'recovered'}\n"
        )
        sys.path.insert(0, str(tmp_path))
        try:
            factory = HarnessFactory(
                manifests=self._manifests("unhealthy_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(max_attempts=2)

            factory.make_executor()(sup, "node", {}, [])

            import unhealthy_agent

            assert unhealthy_agent.CALLS == 2, "a monitor termination was not retried"
            assert "status" not in sup._store.put.call_args.kwargs["meta"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("unhealthy_agent", None)

    def test_each_attempt_gets_a_fresh_harness(self, tmp_path):
        """A reused ExecutionMonitor would re-trip its accumulated thresholds instantly."""
        import sys

        self._flaky(tmp_path, sys, fail_times=1)
        try:
            factory = HarnessFactory(
                manifests=self._manifests("flaky_agent:run"), store=FakeObjectStore()
            )
            built = []
            original = factory.create_harness

            def _counting(node_id, session_id):
                harness = original(node_id, session_id)
                built.append(harness)
                return harness

            factory.create_harness = _counting  # type: ignore[method-assign]
            factory.make_executor()(self._supervisor(max_attempts=2), "node", {}, [])

            assert len(built) == 2, "the retry reused the first attempt's harness"
            assert built[0] is not built[1]
        finally:
            sys.path.pop(0)
            sys.modules.pop("flaky_agent", None)


class TestCorrectiveRetry:
    """the retry is no longer blind: it carries a prompt amendment.

    The monitor's structured ``failure_mode`` + ``evidence`` become supplementary
    ``remediation_context`` on the retry's envelope (an overlay, never a
    plan mutation). This is the RULE tier's amendment -- it states what was observed and gives a
    generic corrective; only the v2 judge can attach a *diagnosed* cause.
    """

    @staticmethod
    def _looping_agent(tmp_path, sys_module, *, recover_on: int):
        """Emits identical tool calls until it is TOLD not to, then succeeds.

        The agent reads ``remediation_context`` out of its own prompt, which is what makes this an
        end-to-end assertion: if the amendment never reached the prompt, the agent keeps looping and
        the test fails.
        """
        (tmp_path / "looping_agent.py").write_text(
            "CALLS = 0\n"
            "PROMPTS = []\n"
            "def run(prompt, inputs, context):\n"
            "    global CALLS\n"
            "    CALLS += 1\n"
            "    PROMPTS.append(prompt)\n"
            "    from datetime import datetime, timezone\n"
            "    from concursus.execute.types import LogEvent, LogEventType\n"
            "    corrected = 'Correction from your previous attempt' in prompt\n"
            "    async def _stream():\n"
            "        if corrected:\n"
            "            yield LogEvent(timestamp=datetime.now(timezone.utc), node_id='node',\n"
            "                           event_type=LogEventType.PROGRESS, content='varied approach')\n"
            "            return\n"
            "        for _ in range(6):\n"
            "            yield LogEvent(timestamp=datetime.now(timezone.utc), node_id='node',\n"
            "                           event_type=LogEventType.TOOL_CALL, content='search',\n"
            "                           metadata={'tool': 'search', 'args': 'same-query'})\n"
            "    return {'finding': f'done on {CALLS}'}, _stream()\n"
        )
        sys_module.path.insert(0, str(tmp_path))

    @staticmethod
    def _manifests():
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": "looping_agent:run"},
                "monitor": {"loop_detection_window": 5, "idle_timeout_s": 30},
                "contract": {
                    "inputs": {"properties": {}},
                    "outputs": {"properties": {"finding": {"type": "string", "required": True}}},
                },
            }
        }

    @staticmethod
    def _supervisor(max_attempts=2):
        sup = MagicMock()
        manifest = MagicMock()
        manifest.name = "node"
        manifest.output_schema = {}
        manifest.side_effecting = False
        sup._manifests = {"node": manifest}
        sup._plan = MagicMock()
        sup._plan.payload_contract = {"node": {"task": "find the cause", "static_context": ""}}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = None
        sup._on_error = "record"
        sup._max_attempts = max_attempts
        sup._check_acceptance = False
        sup._acceptance_fn = None
        return sup

    def test_a_looping_agent_is_told_which_tool_it_looped_on_and_recovers(self, tmp_path):
        import sys

        self._looping_agent(tmp_path, sys, recover_on=2)
        try:
            factory = HarnessFactory(
                manifests=self._manifests(), store=FakeObjectStore(),
                monitor_factory=DefaultMonitorFactory(),
            )
            sup = self._supervisor(max_attempts=2)

            factory.make_executor()(sup, "node", {}, [])

            import looping_agent

            assert looping_agent.CALLS == 2, "the monitor termination was not retried"
            first, second = looping_agent.PROMPTS
            assert "Correction from your previous attempt" not in first, "first attempt was amended"
            assert "Correction from your previous attempt" in second
            # the amendment NAMES the tool -- the whole point of evidence over a reason string
            assert "`search`" in second
            assert "identical" in second
            # the frozen task survives alongside the overlay
            assert "find the cause" in second
            assert "status" not in sup._store.put.call_args.kwargs["meta"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("looping_agent", None)

    def test_the_same_failure_mode_is_not_remediated_twice(self, tmp_path):
        """a mode that survives its own fix escalates, not loops."""
        import sys

        # never recovers, so the same tool_loop mode recurs after the correction was given
        (tmp_path / "stubborn_agent.py").write_text(
            "CALLS = 0\n"
            "def run(prompt, inputs, context):\n"
            "    global CALLS\n"
            "    CALLS += 1\n"
            "    from datetime import datetime, timezone\n"
            "    from concursus.execute.types import LogEvent, LogEventType\n"
            "    async def _stream():\n"
            "        for _ in range(6):\n"
            "            yield LogEvent(timestamp=datetime.now(timezone.utc), node_id='node',\n"
            "                           event_type=LogEventType.TOOL_CALL, content='search',\n"
            "                           metadata={'tool': 'search', 'args': 'same'})\n"
            "    return {'finding': 'never'}, _stream()\n"
        )
        sys.path.insert(0, str(tmp_path))
        try:
            manifests = self._manifests()
            manifests["node"]["runtime"]["entry"] = "stubborn_agent:run"
            factory = HarnessFactory(
                manifests=manifests, store=FakeObjectStore(),
                monitor_factory=DefaultMonitorFactory(),
            )
            # 5 attempts allowed, but the mode repeats -- so it must stop after the one correction
            factory.make_executor()(self._supervisor(max_attempts=5), "node", {}, [])

            import stubborn_agent

            assert stubborn_agent.CALLS == 2, (
                f"expected 1 attempt + 1 remediated retry, got {stubborn_agent.CALLS}"
            )
        finally:
            sys.path.pop(0)
            sys.modules.pop("stubborn_agent", None)


class TestArnIntegrityOnTheHarnessPath:
    """ported by REUSING Supervisor._check_arn_integrity."""

    @staticmethod
    def _manifests(backend: str):
        return {
            "node": {
                "name": "node",
                "runtime": ({"backend": "agentcore", "agent_id": "A1"} if backend == "agentcore"
                            else {"backend": "callable", "entry": "never_called:run"}),
                "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {}}},
            }
        }

    @staticmethod
    def _supervisor(*, arn, on_error="record", error=None):
        sup = MagicMock()
        manifest = MagicMock()
        manifest.name = "node"
        manifest.output_schema = {}
        manifest.side_effecting = False
        sup._manifests = {"node": manifest}
        sup._plan = MagicMock()
        sup._plan.payload_contract = {}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = None
        sup._on_error = on_error
        sup._max_attempts = 1
        sup._check_acceptance = False
        sup._acceptance_fn = None
        sup._arns = {"node": arn}
        sup._check_arn_integrity = MagicMock(return_value=error)
        return sup

    def test_an_unprovisioned_agentcore_node_is_recorded_before_invoke(self):
        from concursus.execute.supervisor import _ARN_PLACEHOLDER, _FAILURE_CRASH

        factory = HarnessFactory(manifests=self._manifests("agentcore"), store=FakeObjectStore())
        sup = self._supervisor(
            arn=_ARN_PLACEHOLDER, error=RuntimeError("node has no provisioned runtime ARN")
        )

        factory.make_executor()(sup, "node", {}, [])

        sup._check_arn_integrity.assert_called_once()
        meta = sup._store.put.call_args.kwargs["meta"]
        assert meta["failure_class"] == _FAILURE_CRASH
        assert "provisioned" in sup._store.put.call_args.args[1]["error"]

    def test_it_propagates_under_raise(self):
        from concursus.execute.supervisor import _ARN_PLACEHOLDER

        factory = HarnessFactory(manifests=self._manifests("agentcore"), store=FakeObjectStore())
        sup = self._supervisor(
            arn=_ARN_PLACEHOLDER, on_error="raise", error=RuntimeError("stale binding")
        )
        with pytest.raises(RuntimeError, match="stale binding"):
            factory.make_executor()(sup, "node", {}, [])

    def test_a_non_agentcore_node_is_never_arn_checked(self):
        """A callable/http node legitimately has no runtime ARN; gating it would break every
        in-process agent (including the whole spoofed fleet)."""
        from concursus.execute.supervisor import _ARN_PLACEHOLDER

        factory = HarnessFactory(manifests=self._manifests("callable"), store=FakeObjectStore())
        sup = self._supervisor(arn=_ARN_PLACEHOLDER, error=RuntimeError("would have failed"))

        # the agent import will fail, which is fine -- what matters is the ARN gate never ran
        factory.make_executor()(sup, "node", {}, [])
        sup._check_arn_integrity.assert_not_called()

    def test_an_intact_binding_lets_the_node_proceed(self):
        # `agent_id` is deliberately omitted so the invoke fails IMMEDIATELY inside the invoker
        # (InvokerError: runtime.agent_id is required) instead of constructing a boto3 client and
        # attempting a real AgentCore call -- a unit test must not touch the network.
        manifests = {
            "node": {
                "name": "node",
                "runtime": {"backend": "agentcore"},
                "contract": {"inputs": {"properties": {}}, "outputs": {"properties": {}}},
            }
        }
        factory = HarnessFactory(manifests=manifests, store=FakeObjectStore())
        sup = self._supervisor(arn="arn:aws:bedrock-agentcore:us-west-2:1:runtime/x", error=None)

        factory.make_executor()(sup, "node", {}, [])

        sup._check_arn_integrity.assert_called_once()
        # it got PAST the gate: the recorded failure is the invoker's, not the binding's
        error = sup._store.put.call_args.args[1].get("error", "")
        assert "agent_id is required" in error
        assert "provisioned" not in error and "stale" not in error


class TestOutputFilteringDivergence:
    """P4 — the two branches disagree about UNDECLARED output fields.

    Found 2026-08-06 while scoping Phase 3, and it is the blocker that stops the flip:

    * ``Supervisor._dispatch`` stores the invoke result **verbatim** (``store.put(node, result)``).
    * ``AgentHarness`` stores only fields the manifest DECLARES -- ``_write_outputs`` iterates
      ``contract.outputs`` and silently drops everything else.

    So routing a previously-``default`` node through the harness would silently discard data. This is
    not hypothetical: ``tests/test_supervisor.py``'s own primary fixture returns
    ``{"document": "DOC", "extra": 1}`` against a contract that declares only ``document``.

    These tests exist to make the divergence FAIL LOUDLY if someone flips the default before deciding
    which behaviour is correct -- pass-through, or declare-or-drop.
    """

    @staticmethod
    def _manifests(entry: str):
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": entry},
                # declares ONE field; the agent will return two
                "contract": {
                    "inputs": {"properties": {}},
                    "outputs": {"properties": {"document": {"type": "string"}}},
                },
            }
        }

    def test_the_harness_drops_undeclared_output_fields(self, tmp_path):
        import sys

        (tmp_path / "extra_agent.py").write_text(
            "def run(p, i, c):\n    return {'document': 'DOC', 'extra': 1}\n"
        )
        sys.path.insert(0, str(tmp_path))
        try:
            factory = HarnessFactory(
                manifests=self._manifests("extra_agent:run"), store=FakeObjectStore()
            )
            sup = MagicMock()
            manifest = MagicMock()
            manifest.name = "node"
            manifest.output_schema = {}
            manifest.side_effecting = False
            sup._manifests = {"node": manifest}
            sup._plan = MagicMock()
            sup._plan.payload_contract = {}
            sup._session_id = "s1"
            sup._store = MagicMock()
            sup._store.completed.return_value = set()
            sup._cancel_tokens = None
            sup._on_error = "record"
            sup._max_attempts = 1
            sup._check_acceptance = False
            sup._acceptance_fn = None

            factory.make_executor()(sup, "node", {}, [])

            stored = sup._store.put.call_args.args[1]
            assert stored == {"document": "DOC"}, (
                "harness output filtering changed -- re-read P4 before flipping the default"
            )
            assert "extra" not in stored
        finally:
            sys.path.pop(0)
            sys.modules.pop("extra_agent", None)

    def test_dispatch_stores_the_result_verbatim(self):
        """The other half of the divergence, asserted on the legacy branch."""
        import types as _types

        from concursus.execute.supervisor import Supervisor

        plan = _types.SimpleNamespace(
            order=["node"], wiring={"node": []}, entries={}, payload_contract={}, revision=0
        )
        manifest = AgentManifest.from_dict(
            {
                "name": "node",
                "registry": {"agent_runtime_arn": "arn:node"},
                "contract": {"outputs": {"properties": {"document": {"type": "string"}}}},
            }
        )
        sup = Supervisor(
            plan,
            {"node": manifest},
            invoke_fn=lambda arn, q, s, payload: {"document": "DOC", "extra": 1},
            arns={"node": "arn:node"},
        )
        out = sup.run({})
        assert out["node"] == {"document": "DOC", "extra": 1}, (
            "_dispatch no longer passes the result through verbatim"
        )
        assert "extra" in out["node"]


class TestSharedFailureWriter:
    """one failure-record writer, and the manifest output gates on BOTH branches.

    The two node-kind branches used to hand-roll their own ``store.put`` for failures and drifted
    twice: first the harness path had no generic ``except`` at all (``612adc5``), then its failure
    writes omitted the ``consumes`` / ``schema`` provenance its own SUCCESS write included. These
    tests pin the shared writer and the gates the harness path was missing entirely.
    """

    @staticmethod
    def _agent(tmp_path, sys_module, body: str, name: str = "gate_agent"):
        (tmp_path / f"{name}.py").write_text(body)
        sys_module.path.insert(0, str(tmp_path))

    @staticmethod
    def _supervisor(*, on_error="record", output_schema=None, check_acceptance=False):
        sup = MagicMock()
        manifest = MagicMock()
        manifest.name = "node"
        # A REAL dict: validate_output returns early on a non-dict schema, so a MagicMock here would
        # make the gate a silent no-op and the assertion vacuous.
        manifest.output_schema = output_schema if output_schema is not None else {}
        sup._manifests = {"node": manifest}
        sup._plan = MagicMock()
        sup._plan.payload_contract = {}
        sup._session_id = "s1"
        sup._store = MagicMock()
        sup._store.completed.return_value = set()
        sup._cancel_tokens = None
        sup._on_error = on_error
        sup._check_acceptance = check_acceptance
        sup._acceptance_fn = None
        return sup

    @staticmethod
    def _permissive_manifest(entry: str):
        """Raw contract that DECLARES both fields but requires NEITHER.

        Two things this isolates. `_check_contract` cannot be the trigger, since nothing is required —
        so only ``validate_output`` against the TYPED manifest's ``output_schema`` can fail. And both
        fields must be *declared* here because ``harness._write_outputs`` iterates the RAW contract's
        outputs and DROPS anything undeclared: the gate validates the written refs, not the agent's
        raw response, so a field the raw contract omits could never satisfy the typed schema no matter
        what the agent returned. That coupling between the two manifest representations is exactly what
        P3 (converge them) is about.
        """
        return {
            "node": {
                "name": "node",
                "runtime": {"backend": "callable", "entry": entry},
                "contract": {
                    "inputs": {"properties": {}},
                    "outputs": {
                        "properties": {
                            "finding": {"type": "string"},
                            "other": {"type": "string"},
                        }
                    },
                },
            }
        }

    def test_shape_invalid_output_is_now_rejected_by_the_manifest_gate(self, tmp_path):
        """The gate `_dispatch` always had and the harness path did not.

        Before this, a harness node could admit an output that the legacy path would have rejected.
        """
        import sys

        from concursus.execute.supervisor import _FAILURE_CRASH

        self._agent(tmp_path, sys, "def run(p, i, c):\n    return {'other': 'ok'}\n")
        try:
            factory = HarnessFactory(
                manifests=self._permissive_manifest("gate_agent:run"), store=FakeObjectStore()
            )
            # raw contract permits it; the TYPED manifest requires `finding`
            sup = self._supervisor(
                output_schema={"properties": {"finding": {"type": "string", "required": True}}}
            )

            factory.make_executor()(sup, "node", {}, [])  # must NOT raise

            args, kwargs = sup._store.put.call_args
            assert kwargs["meta"]["status"] == "failed"
            assert kwargs["meta"]["failure_class"] == _FAILURE_CRASH
            assert args[1]["error_type"] == "SchemaError"
            assert "finding" in args[1]["error"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("gate_agent", None)

    def test_a_conforming_output_still_passes_the_gate(self, tmp_path):
        import sys

        self._agent(tmp_path, sys, "def run(p, i, c):\n    return {'finding': 'x', 'other': 'y'}\n")
        try:
            factory = HarnessFactory(
                manifests=self._permissive_manifest("gate_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(
                output_schema={"properties": {"finding": {"type": "string", "required": True}}}
            )

            factory.make_executor()(sup, "node", {}, [])

            args, kwargs = sup._store.put.call_args
            assert "status" not in kwargs["meta"], "a passing node must not be recorded as failed"
            assert kwargs["meta"]["producer"] == "node"
        finally:
            sys.path.pop(0)
            sys.modules.pop("gate_agent", None)

    def test_gate_failure_under_raise_propagates(self, tmp_path):
        """Fail-fast is preserved: the gate must not swallow under the default on_error."""
        import sys

        from concursus.execute.supervisor import SchemaError

        self._agent(tmp_path, sys, "def run(p, i, c):\n    return {'other': 'ok'}\n")
        try:
            factory = HarnessFactory(
                manifests=self._permissive_manifest("gate_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(
                on_error="raise",
                output_schema={"properties": {"finding": {"type": "string", "required": True}}},
            )
            with pytest.raises(SchemaError, match="finding"):
                factory.make_executor()(sup, "node", {}, [])
        finally:
            sys.path.pop(0)
            sys.modules.pop("gate_agent", None)

    def test_failure_records_carry_the_same_provenance_as_the_success_write(self, tmp_path):
        """The drift this closes: failed harness records used to lose `consumes` and `schema`."""
        import sys
        import types as _types

        self._agent(tmp_path, sys, "def run(p, i, c):\n    return {'other': 'ok'}\n")
        try:
            factory = HarnessFactory(
                manifests=self._permissive_manifest("gate_agent:run"), store=FakeObjectStore()
            )
            sup = self._supervisor(
                output_schema={"properties": {"finding": {"type": "string", "required": True}}}
            )
            wiring = [_types.SimpleNamespace(producer="up", path="$.finding", input_name="upstream")]

            factory.make_executor()(sup, "node", {}, wiring)

            meta = sup._store.put.call_args.kwargs["meta"]
            assert meta["status"] == "failed"
            assert meta["consumes"] == ["up:$.finding"], "failed record lost its edge provenance"
            assert meta["schema"] == "node", "failed record lost its schema provenance"
            assert meta["address"] == "node"
        finally:
            sys.path.pop(0)
            sys.modules.pop("gate_agent", None)

    def test_acceptance_gate_only_fires_when_dialed_on(self, tmp_path):
        """`check_acceptance` is opt-in on both branches; off by default, honored when set.

        Note the rule must be DECLARED: `check_acceptance` is conservative by design -- a field with
        no ``acceptance`` mapping is unconstrained, so a manifest declaring none is never newly
        rejected. An empty string only fails because ``non_empty`` is asked for here.
        """
        import sys

        self._agent(
            tmp_path, sys,
            # present but EMPTY -- passes validate_output's presence check, fails `non_empty`
            "def run(p, i, c):\n    return {'finding': ''}\n",
        )
        schema = {
            "properties": {
                "finding": {"type": "string", "required": True, "acceptance": {"non_empty": True}}
            }
        }
        try:
            manifests = self._permissive_manifest("gate_agent:run")

            off = self._supervisor(output_schema=schema, check_acceptance=False)
            HarnessFactory(manifests=manifests, store=FakeObjectStore()).make_executor()(
                off, "node", {}, []
            )
            assert "status" not in off._store.put.call_args.kwargs["meta"], "gate fired while OFF"

            on = self._supervisor(output_schema=schema, check_acceptance=True)
            HarnessFactory(manifests=manifests, store=FakeObjectStore()).make_executor()(
                on, "node", {}, []
            )
            meta = on._store.put.call_args.kwargs["meta"]
            assert meta["status"] == "failed", "gate did not fire while ON"
            assert "acceptance" in on._store.put.call_args.args[1]["error"]
        finally:
            sys.path.pop(0)
            sys.modules.pop("gate_agent", None)

    def test_record_failure_metadata_shape(self):
        """The shared writer's contract, asserted directly -- both branches inherit this shape."""
        from concursus.execute.supervisor import _FAILURE_HOLD, record_failure

        sup = MagicMock()
        record_failure(
            sup, "n",
            failure_class=_FAILURE_HOLD, error="boom", error_type="ValueError",
            consumes=["a:$.x"], schema="n", blocked_on="a", address="n/2",
        )
        args, kwargs = sup._store.put.call_args
        assert args[1] == {"error": "boom", "error_type": "ValueError"}
        assert kwargs["meta"] == {
            "status": "failed", "producer": "n", "failure_class": _FAILURE_HOLD,
            "address": "n/2", "consumes": ["a:$.x"], "schema": "n", "blocked_on": "a",
        }

    def test_record_failure_omits_absent_optional_metadata(self):
        """Optional keys stay ABSENT rather than None, so the log shape matches the old writers."""
        from concursus.execute.supervisor import _FAILURE_CRASH, record_failure

        sup = MagicMock()
        record_failure(sup, "n", failure_class=_FAILURE_CRASH, error="e", error_type="T")
        meta = sup._store.put.call_args.kwargs["meta"]
        assert set(meta) == {"status", "producer", "failure_class", "address"}
        assert meta["address"] == "n", "address must default to the node id"

