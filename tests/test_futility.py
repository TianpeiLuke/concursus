"""Tests for futility cancellation — closure math, cancel tokens, and the wave integration.

Three layers, deliberately separable:

1. :func:`futility_closure` and friends are pure graph math — tested with no threads and no store.
2. :class:`CancelTokenRegistry` + :func:`run_registered` are tested against a REAL cross-thread
   ``asyncio.run``, since the whole point is reaching a task whose loop is not externally visible.
3. The :meth:`Supervisor._run_parallel` seam is driven end-to-end through a custom NodeExecutor that
   mirrors the harness executor's shape, so the closure→condemn→cancel→record loop is exercised
   without dragging in the harness/invoker stack.
"""

from __future__ import annotations

import asyncio
import threading
import time
import types

import pytest

from concursus.core.resolve import AgentRef
from concursus.execute.futility import (
    CancelTokenRegistry,
    descendants,
    futility_closure,
    invert_wiring,
    run_registered,
)
from concursus.execute.supervisor import (
    _FAILURE_CLASSES,
    _FAILURE_FUTILITY,
    _FAILURE_PREEMPTIVE,
    Supervisor,
)
from concursus.state.statestore import InProcessStateStore


# -- fixtures ---------------------------------------------------------------
def _ref(producer: str) -> AgentRef:
    return AgentRef(producer=producer, path=f"$.{producer}_out", input_name=f"in_{producer}")


def _wiring(**edges):
    """Build ``{node: [AgentRef(producer)]}`` from ``node="producers space separated"``."""
    return {
        node: [_ref(p) for p in producers.split()] if producers else []
        for node, producers in edges.items()
    }


def _plan(order, wiring):
    """A ProvisioningPlan-like stand-in exposing the duck-typed ``.order`` + ``.wiring``."""
    return types.SimpleNamespace(order=list(order), wiring=dict(wiring))


# -- 1. pure closure math ---------------------------------------------------
class TestClosureMath:
    def test_invert_wiring_maps_producer_to_consumers(self):
        wiring = _wiring(a="", b="", c="a b", d="a")
        assert invert_wiring(wiring) == {"a": {"c", "d"}, "b": {"c"}}

    def test_invert_wiring_omits_nodes_that_produce_for_nobody(self):
        # A sink has no entry at all — absence is what makes `consumers.get(n, set())` falsy.
        assert "c" not in invert_wiring(_wiring(a="", c="a"))

    def test_descendants_is_transitive_and_excludes_self(self):
        consumers = invert_wiring(_wiring(a="", b="a", c="b", d="c"))
        assert descendants("a", consumers) == {"b", "c", "d"}
        assert "a" not in descendants("a", consumers)

    def test_descendants_of_a_sink_is_empty(self):
        assert descendants("z", invert_wiring(_wiring(a="", b="a"))) == set()

    def test_descendants_is_cycle_safe(self):
        # A compiled plan is acyclic, but the walk must not hang if handed a cycle.
        consumers = {"a": {"b"}, "b": {"a"}}
        assert descendants("a", consumers) == {"a", "b"}

    def test_degenerate_two_producers_one_consumer_cancels_the_sibling(self):
        # wave = [a, b], both producing c. a fails -> b's only consumer is doomed.
        consumers = invert_wiring(_wiring(a="", b="", c="a b"))
        assert futility_closure(consumers, "a", {"b"}) == {"b"}

    def test_independent_branch_survives_a_failure(self):
        # wave = [a, b, d, e]; (a+b) -> c and (d+e) -> f. a fails: ONLY b is futile.
        consumers = invert_wiring(_wiring(a="", b="", d="", e="", c="a b", f="d e"))
        assert futility_closure(consumers, "a", {"b", "d", "e"}) == {"b"}

    def test_sink_node_is_never_futile(self):
        # b produces nothing; its output IS a deliverable, so a's death cannot condemn it.
        consumers = invert_wiring(_wiring(a="", b="", c="a"))
        assert futility_closure(consumers, "a", {"b"}) == set()

    def test_partial_fan_survives(self):
        # d feeds both the doomed f and an independent g -> g still needs d's output.
        consumers = invert_wiring(_wiring(a="", d="", c="a", f="d c", g="d"))
        assert "d" not in futility_closure(consumers, "a", {"d"})

    def test_transitive_doom_condemns_a_second_hop_producer(self):
        # (d+e) -> f AND c -> f, with a -> c. a's death dooms c, which dooms f, which dooms d and e.
        consumers = invert_wiring(_wiring(a="", d="", e="", c="a", f="d e c"))
        assert descendants("a", consumers) == {"c", "f"}
        assert futility_closure(consumers, "a", {"d", "e"}) == {"d", "e"}

    def test_failed_node_is_never_in_its_own_closure(self):
        consumers = invert_wiring(_wiring(a="", b="", c="a b"))
        assert "a" not in futility_closure(consumers, "a", {"a", "b"})

    def test_closure_empty_when_failed_node_is_a_sink(self):
        consumers = invert_wiring(_wiring(a="", b="", c="b"))
        assert futility_closure(consumers, "a", {"b"}) == set()


# -- 2. cancel token registry ----------------------------------------------
class TestCancelTokenRegistry:
    def test_condemn_with_no_token_returns_false_but_remembers(self):
        reg = CancelTokenRegistry()
        assert reg.condemn("a", "because") is False
        assert reg.reason_for("a") == "because"

    def test_first_reason_wins(self):
        reg = CancelTokenRegistry()
        reg.condemn("a", "first")
        reg.condemn("a", "second")
        assert reg.reason_for("a") == "first"

    def test_reason_for_unknown_node_is_none(self):
        assert CancelTokenRegistry().reason_for("nope") is None

    def test_decisions_is_a_copy(self):
        reg = CancelTokenRegistry()
        reg.condemn("a", "r")
        snapshot = reg.decisions
        snapshot["b"] = "mutated"
        assert "b" not in reg.decisions

    def test_condemn_many_returns_only_the_nodes_actually_reached(self):
        reg = CancelTokenRegistry()
        reached = reg.condemn_many(["a", "b"], "r")
        assert reached == set()  # no live tokens
        assert reg.decisions == {"a": "r", "b": "r"}

    def test_revoke_then_condemn_is_a_silent_noop(self):
        # The benign late-cancel race: the node already finished; its store record is authoritative.
        reg = CancelTokenRegistry()

        async def _main():
            reg.register("a", 1, asyncio.get_running_loop(), asyncio.current_task())
            reg.revoke("a", 1)
            assert reg.condemn("a", "too late") is False

        asyncio.run(_main())

    def test_attempt_keying_isolates_a_retry_from_its_predecessor(self):
        reg = CancelTokenRegistry()

        async def _main():
            loop, task = asyncio.get_running_loop(), asyncio.current_task()
            reg.register("a", 1, loop, task)
            reg.register("a", 2, loop, task)
            reg.revoke("a", 1)  # attempt 1's cleanup must NOT drop attempt 2's token
            assert reg.condemn("a", "r") is True

        asyncio.run(_main())

    def test_register_after_condemnation_cancels_immediately(self):
        # Closes the submit->register race: the Supervisor may condemn before the loop exists.
        reg = CancelTokenRegistry()
        reg.condemn("a", "condemned first")

        async def _main():
            cancelled = reg.register("a", 1, asyncio.get_running_loop(), asyncio.current_task())
            assert cancelled is True
            await asyncio.sleep(5)  # must not be reached

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(_main())


class TestRunRegistered:
    def test_returns_the_coroutine_result_and_revokes(self):
        reg = CancelTokenRegistry()

        async def _work():
            return {"ok": True}

        assert asyncio.run(run_registered(reg, "a", 1, _work())) == {"ok": True}
        assert reg.condemn("a", "after") is False  # token was revoked in the finally

    def test_revokes_even_when_the_coroutine_raises(self):
        reg = CancelTokenRegistry()

        async def _boom():
            raise ValueError("defiant")

        with pytest.raises(ValueError, match="defiant"):
            asyncio.run(run_registered(reg, "a", 1, _boom()))
        assert reg.condemn("a", "after") is False

    def test_pre_condemned_node_is_cancelled_at_the_first_suspension(self):
        reg = CancelTokenRegistry()
        reg.condemn("a", "futility-cancelled on x")
        reached = []

        async def _work():
            await asyncio.sleep(5)
            reached.append(True)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(run_registered(reg, "a", 1, _work()))
        assert reached == []

    def test_cross_thread_condemn_aborts_a_running_task(self):
        """The irreducible mechanism: cancel a task whose event loop is owned by another thread."""
        reg = CancelTokenRegistry()
        registered = threading.Event()
        outcome = {}

        async def _work():
            registered.set()
            await asyncio.sleep(30)  # would dominate the test if cancellation did not land

        def _worker():
            try:
                asyncio.run(run_registered(reg, "slow", 1, _work()))
            except asyncio.CancelledError:
                outcome["cancelled"] = True

        thread = threading.Thread(target=_worker)
        started = time.monotonic()
        thread.start()
        assert registered.wait(timeout=5), "worker never reached its event loop"
        # Give the loop a moment to actually suspend on the sleep before condemning.
        time.sleep(0.05)
        assert reg.condemn("slow", "futility-cancelled on a") is True
        thread.join(timeout=5)

        assert outcome.get("cancelled") is True
        assert not thread.is_alive()
        assert time.monotonic() - started < 5, "cancellation did not collapse the 30s sleep"


# -- 3. the Supervisor wave seam -------------------------------------------
_FAIL_KIND, _SLOW_KIND = "fail", "slow"


def _fail_executor(supervisor, node, inputs, wiring):
    """Write a recorded failure immediately, as ``_dispatch`` does under ``on_error='record'``."""
    supervisor._store.put(
        node,
        {"error": "boom"},
        meta={"status": "failed", "producer": node, "failure_class": "crash", "address": node},
    )


def _make_slow_executor(sleeps, started=None):
    """A cancellable NodeExecutor mirroring the harness executor's registration + record shape."""

    def _executor(supervisor, node, inputs, wiring):
        tokens = getattr(supervisor, "_cancel_tokens", None)

        async def _work():
            if started is not None:
                started.setdefault(node, threading.Event()).set()
            await asyncio.sleep(sleeps.get(node, 0.0))
            return {f"{node}_out": node}

        coro = _work()
        if tokens is not None:
            coro = run_registered(tokens, node, 1, coro)
        try:
            result = asyncio.run(coro)
        except asyncio.CancelledError:
            reason = (tokens.reason_for(node) if tokens else None) or "futility-cancelled"
            supervisor._store.put(
                node,
                {"error": reason},
                meta={
                    "status": "failed",
                    "producer": node,
                    "blocked_on": reason,
                    "failure_class": _FAILURE_FUTILITY,
                    "address": node,
                },
            )
            return
        supervisor._store.put(node, result, meta={"producer": node, "address": node})

    return _executor


def _branching_supervisor(*, cancel_futile, sleeps, store=None, started=None):
    """wave = [a, b, d, e] with (a+b) -> c and (d+e) -> f; ``a`` fails immediately."""
    wiring = _wiring(a="", b="", d="", e="", c="a b", f="d e")
    order = ["a", "b", "d", "e", "c", "f"]
    slow = _make_slow_executor(sleeps, started=started)
    return Supervisor(
        _plan(order, wiring),
        {},
        state_store=store or InProcessStateStore(),
        on_error="record",
        cancel_futile=cancel_futile,
        node_executors={_FAIL_KIND: _fail_executor, _SLOW_KIND: slow},
        node_kind_fn=lambda node: _FAIL_KIND if node == "a" else _SLOW_KIND,
    )


class TestSupervisorSeam:
    def test_registry_is_absent_by_default_and_never_leaks(self):
        sup = _branching_supervisor(cancel_futile=False, sleeps={})
        assert sup._cancel_tokens is None
        sup.run({}, parallel=4)
        assert sup._cancel_tokens is None  # cleared in _run_parallel's finally

    def test_registry_is_cleared_after_an_opt_in_run(self):
        sup = _branching_supervisor(cancel_futile=True, sleeps={})
        sup.run({}, parallel=4)
        assert sup._cancel_tokens is None

    def test_default_off_lets_the_futile_sibling_run_to_completion(self):
        # The control case: with the flag off, b completes despite its only consumer being dead.
        store = InProcessStateStore()
        sup = _branching_supervisor(cancel_futile=False, sleeps={}, store=store)
        sup.run({}, parallel=4)
        assert "b" in store.completed()
        assert store.get("b") == {"b_out": "b"}

    def test_futile_sibling_is_cancelled_and_recorded(self):
        # b's only consumer is c, which a's failure doomed -> b is condemned mid-flight. d and e
        # feed the independent f, so they survive and f runs in the NEXT wave.
        store = InProcessStateStore()
        started = {}
        sup = _branching_supervisor(
            cancel_futile=True,
            sleeps={"b": 30.0, "d": 0.2, "e": 0.2},
            store=store,
            started=started,
        )
        began = time.monotonic()
        sup.run({}, parallel=4)
        elapsed = time.monotonic() - began

        completed = store.completed()
        assert {"d", "e", "f"} <= completed, "the independent branch must finish"
        assert completed.isdisjoint({"a", "b", "c"}), "the doomed branch must not complete"
        assert elapsed < 30, "b's 30s sleep was not collapsed by cancellation"

        b_failed = [r for r in store.records() if r.node == "b" and r.status == "failed"]
        assert len(b_failed) == 1
        assert b_failed[0].failure_class == _FAILURE_FUTILITY
        assert "futility-cancelled on a" in (b_failed[0].blocked_on or "")

    def test_blocked_consumer_still_cascades_from_phase_three(self):
        # c was never dispatched, so the existing blocked-remainder pass records it unchanged.
        store = InProcessStateStore()
        sup = _branching_supervisor(
            cancel_futile=True, sleeps={"b": 30.0, "d": 0.2, "e": 0.2}, store=store
        )
        sup.run({}, parallel=4)
        c_failed = [r for r in store.records() if r.node == "c" and r.status == "failed"]
        assert len(c_failed) == 1 and c_failed[0].failure_class == "hold"
        assert "a" in (c_failed[0].blocked_on or "")

    def test_summary_counts_the_futility_class(self):
        sup = _branching_supervisor(
            cancel_futile=True, sleeps={"b": 30.0, "d": 0.2, "e": 0.2}
        )
        sup.run({}, parallel=4)
        classes = sup.summary()["failure_classes"]
        assert classes[_FAILURE_FUTILITY] == 1  # b
        assert classes["crash"] == 1  # a
        assert classes["hold"] == 1  # c
        assert set(classes) == set(_FAILURE_CLASSES)


class TestOrderedRaise:
    """``as_completed`` yields in completion order; the raise must stay in ``plan.order``."""

    def test_earliest_failure_in_plan_order_is_raised(self):
        # Both x and y fail in one wave. y fails FIRST in wall-clock time, but x precedes it in
        # plan.order, so x's exception is the one that must surface — as wait()'s futures-list
        # inspection produced before this change.
        def _executor(supervisor, node, inputs, wiring):
            if node == "x":
                time.sleep(0.2)
                raise RuntimeError("x failed")
            if node == "y":
                raise RuntimeError("y failed")

        sup = Supervisor(
            _plan(["x", "y"], _wiring(x="", y="")),
            {},
            on_error="raise",
            node_executors={"k": _executor},
            node_kind_fn=lambda node: "k",
        )
        with pytest.raises(RuntimeError, match="x failed"):
            sup.run({}, parallel=4)

    def test_a_failing_wave_never_starts_the_next_one(self):
        seen = []

        def _executor(supervisor, node, inputs, wiring):
            seen.append(node)
            if node == "a":
                raise RuntimeError("a failed")
            supervisor._store.put(node, {"a_out": node}, meta={"producer": node, "address": node})

        sup = Supervisor(
            _plan(["a", "b"], _wiring(a="", b="a")),
            {},
            on_error="raise",
            node_executors={"k": _executor},
            node_kind_fn=lambda node: "k",
        )
        with pytest.raises(RuntimeError, match="a failed"):
            sup.run({}, parallel=4)
        assert seen == ["a"], "b must never be dispatched after the wave failed"

    def test_fail_fast_condemns_every_in_flight_sibling(self):
        # Under a raising failure the whole run is ending, so NO in-flight output can be consumed —
        # even an independent branch is condemned, unlike the record-mode closure.
        store = InProcessStateStore()
        registered = threading.Event()

        def _executor(supervisor, node, inputs, wiring):
            tokens = getattr(supervisor, "_cancel_tokens", None)
            if node == "a":
                assert registered.wait(timeout=5)
                time.sleep(0.05)
                raise RuntimeError("a failed")

            async def _work():
                registered.set()
                await asyncio.sleep(30)

            try:
                asyncio.run(run_registered(tokens, node, 1, _work()))
            except asyncio.CancelledError:
                store.put(
                    node,
                    {"error": tokens.reason_for(node)},
                    meta={
                        "status": "failed",
                        "producer": node,
                        "blocked_on": tokens.reason_for(node),
                        "failure_class": _FAILURE_FUTILITY,
                        "address": node,
                    },
                )

        # `independent` is a SINK — the record-mode closure would never condemn it.
        sup = Supervisor(
            _plan(["a", "independent"], _wiring(a="", independent="")),
            {},
            state_store=store,
            on_error="raise",
            cancel_futile=True,
            node_executors={"k": _executor},
            node_kind_fn=lambda node: "k",
        )
        began = time.monotonic()
        with pytest.raises(RuntimeError, match="a failed"):
            sup.run({}, parallel=4)
        assert time.monotonic() - began < 30, "the independent sibling was not condemned"
        cancelled = [r for r in store.records() if r.node == "independent"]
        assert cancelled and "fail-fast on a" in (cancelled[0].blocked_on or "")


# -- classifier widening ----------------------------------------------------
class TestFailureClassification:
    def test_preemptive_termination_no_longer_buckets_as_crash(self):
        record = types.SimpleNamespace(failure_class=_FAILURE_PREEMPTIVE, blocked_on=None)
        assert Supervisor._classify_failure(record) == _FAILURE_PREEMPTIVE

    def test_futility_cancelled_is_recognized(self):
        record = types.SimpleNamespace(failure_class=_FAILURE_FUTILITY, blocked_on="x")
        assert Supervisor._classify_failure(record) == _FAILURE_FUTILITY

    def test_legacy_record_without_a_class_still_derives_from_blocked_on(self):
        assert Supervisor._classify_failure(
            types.SimpleNamespace(failure_class=None, blocked_on="p")
        ) == "hold"
        assert Supervisor._classify_failure(
            types.SimpleNamespace(failure_class=None, blocked_on=None)
        ) == "crash"

    def test_unknown_class_string_falls_back_rather_than_leaking(self):
        record = types.SimpleNamespace(failure_class="something_new", blocked_on=None)
        assert Supervisor._classify_failure(record) == "crash"
