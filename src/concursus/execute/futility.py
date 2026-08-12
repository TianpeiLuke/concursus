"""Futility cancellation — stop in-flight work whose output can no longer be consumed.

Supervisor-owned dispatch machinery behind the OPT-IN ``cancel_futile`` seam of
:meth:`~concursus.execute.supervisor.Supervisor._run_parallel`. Two
pieces, deliberately small:

* :func:`futility_closure` (with :func:`invert_wiring` / :func:`descendants`) — pure graph math over
  the FROZEN ``plan.wiring``. No threads, no I/O, no store: unit-testable in isolation.
* :class:`CancelTokenRegistry` — the one irreducible piece of machinery. ``asyncio.run`` creates its
  event loop internally and returns no handle, so a running harness task can only be reached if it
  SELF-registers from inside (:func:`run_registered`); cancellation then crosses the thread boundary
  via ``loop.call_soon_threadsafe(task.cancel)``.

This module is policy *computation* and *handles* only. It never chooses what to dispatch (the
Supervisor's job), never mutates the plan (the compiler's, INV-3), never judges whether a node is
unhealthy (the monitor's), and never writes to the StateStore — a condemned worker writes its own
failed record on the existing path.

Relationship to blocked-skip: :meth:`Supervisor._run_parallel`'s phase-3 pass already records nodes
whose producers never completed. Futility cancellation is that same judgement — *"this work cannot be
consumed, stop spending on it"* — evaluated DURING dispatch rather than after the wave.
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any, Awaitable, Dict, Iterable, Mapping, Optional, Set, Tuple

if TYPE_CHECKING:  # pragma: no cover - hints only
    from ..core.resolve import AgentRef

__all__ = [
    "CancelTokenRegistry",
    "descendants",
    "futility_closure",
    "invert_wiring",
    "run_registered",
]


# -- graph math over the frozen plan ----------------------------------------
def invert_wiring(
    wiring_by_node: Mapping[str, Iterable["AgentRef"]],
) -> Dict[str, Set[str]]:
    """Invert ``node -> [AgentRef(producer=...)]`` into ``producer -> {consumers}``.

    Computed ONCE at wave-loop entry: ``plan.wiring`` is frozen for the run (INV-3), so the consumer
    graph cannot go stale and every later futility question is a set-containment test. A mutable-plan
    system would have to recompute this on every replan.
    """
    consumers: Dict[str, Set[str]] = {}
    for node, refs in wiring_by_node.items():
        for ref in refs:
            consumers.setdefault(ref.producer, set()).add(node)
    return consumers


def descendants(node: str, consumers: Mapping[str, Set[str]]) -> Set[str]:
    """Transitive consumers of ``node`` — the DOOMED region once ``node`` has failed.

    Excludes ``node`` itself. The result is downward-closed by construction (a doomed node's own
    consumers are also doomed), which is precisely what makes the cheap direct-consumer test in
    :func:`futility_closure` equivalent to checking every downstream path. Iterative and
    visited-guarded, so it is cycle-safe even though a compiled plan is acyclic.
    """
    doomed: Set[str] = set()
    stack = list(consumers.get(node, ()))
    while stack:
        current = stack.pop()
        if current in doomed:
            continue
        doomed.add(current)
        stack.extend(consumers.get(current, ()))
    return doomed


def futility_closure(
    consumers: Mapping[str, Set[str]],
    failed: str,
    in_flight: Iterable[str],
) -> Set[str]:
    """The in-flight nodes whose output now feeds ONLY the doomed region.

    ``B`` is futile iff ``consumers(B)`` is **non-empty** and wholly inside
    ``descendants(failed)``. Both halves carry weight:

    * *non-empty* — a SINK node (nothing consumes it) is never futile; its output is a deliverable
      of the run, not an intermediate.
    * *wholly inside* — if ``B`` also feeds a node outside the doomed region, that consumer still
      needs ``B``'s output, so ``B`` survives. This is what makes cancellation discriminating rather
      than a blunt cancel-the-whole-wave.

    ``failed`` is excluded (it has already resolved). Returns an empty set when nothing is doomed,
    i.e. when the failed node was itself a sink.
    """
    doomed = descendants(failed, consumers)
    if not doomed:
        return set()
    futile: Set[str] = set()
    for node in in_flight:
        if node == failed:
            continue
        node_consumers = consumers.get(node, set())
        if node_consumers and node_consumers <= doomed:
            futile.add(node)
    return futile


# -- reaching inside a running harness --------------------------------------
class CancelTokenRegistry:
    """Thread-safe map of ``(node, attempt) -> (loop, task)`` for cancellable in-flight work.

    A *token* is the ``(event loop, task)`` pair that only exists INSIDE an ``asyncio.run``
    invocation — the sole handle able to abort a running invoke from another thread, since
    :meth:`concurrent.futures.Future.cancel` returns ``False`` once a callable has started and every
    wave member starts immediately. Workers self-register via :func:`run_registered`; the Supervisor
    condemns from its own thread.

    Two properties make the races benign:

    * A condemnation issued BEFORE its token registers is REMEMBERED and fires at registration,
      closing the submit-then-register window.
    * A condemnation arriving AFTER revocation is a silent no-op — the node already finished and its
      store record is authoritative either way.

    Keying on ``attempt`` keeps a retry's fresh token from being revoked by its predecessor's
    cleanup. All methods lock internally and are safe to call from any thread.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: Dict[Tuple[str, int], Tuple[asyncio.AbstractEventLoop, "asyncio.Task[Any]"]] = {}
        # node -> reason. Retained (never popped) so the condemned worker can read it when writing
        # its own failed record, and so the whole run's decisions stay legible to an operator.
        self._reasons: Dict[str, str] = {}

    # -- worker-thread side ------------------------------------------------
    def register(
        self,
        node: str,
        attempt: int,
        loop: asyncio.AbstractEventLoop,
        task: "asyncio.Task[Any]",
    ) -> bool:
        """Bind ``node``'s cancel handle; return True if it was cancelled on the spot.

        Called from inside the harness's own event loop the first moment that loop exists. When
        ``node`` was already condemned the task is cancelled immediately and NO token is stored —
        the caller is on the loop's own thread, so a direct ``task.cancel()`` is correct here.
        """
        with self._lock:
            condemned = node in self._reasons
            if not condemned:
                self._tokens[(node, attempt)] = (loop, task)
        if condemned:
            task.cancel()
            return True
        return False

    def revoke(self, node: str, attempt: int) -> None:
        """Drop ``node``'s handle — called in the worker's ``finally`` once the node has resolved."""
        with self._lock:
            self._tokens.pop((node, attempt), None)

    # -- supervisor-thread side --------------------------------------------
    def condemn(self, node: str, reason: str) -> bool:
        """Mark ``node`` futile and cancel its live token; return True if a token was reached.

        ``reason`` is recorded unconditionally (first reason wins), so a token registering LATER is
        still cancelled and the reason remains available to whoever writes the failed record. False
        means either the event loop does not exist yet — the harness path, where registration will
        enforce it — or the node is structurally uncancellable, i.e. the legacy synchronous
        ``_dispatch`` path, which is a documented limit rather than a defect.
        """
        with self._lock:
            self._reasons.setdefault(node, reason)
            live = [handle for key, handle in self._tokens.items() if key[0] == node]
        for loop, task in live:
            loop.call_soon_threadsafe(task.cancel)
        return bool(live)

    def condemn_many(self, nodes: Iterable[str], reason: str) -> Set[str]:
        """Condemn every node in ``nodes``; return the subset whose live token was reached.

        Preferred over condemning only *registered* tokens: the Supervisor knows the full in-flight
        set, including workers that have not yet reached their event loop, and those must be
        condemned too so :meth:`register` can enforce it when they arrive.
        """
        return {node for node in nodes if self.condemn(node, reason)}

    # -- introspection ------------------------------------------------------
    def reason_for(self, node: str) -> Optional[str]:
        """The condemnation reason for ``node``, or None if it was never condemned."""
        with self._lock:
            return self._reasons.get(node)

    @property
    def decisions(self) -> Dict[str, str]:
        """``{node: reason}`` for every condemnation this wave loop made (a copy).

        The read-out the GovernorLoop harvests at the episode boundary; feeding it into the Trust
        Ladder is deferred to v2.
        """
        with self._lock:
            return dict(self._reasons)


async def run_registered(
    registry: CancelTokenRegistry,
    node: str,
    attempt: int,
    coro: Awaitable[Any],
) -> Any:
    """Await ``coro`` with its cancel token registered for the duration.

    ``asyncio.run`` builds its event loop internally and hands back no handle, so the token cannot be
    obtained from the calling thread — the task must self-register. Wrap the harness coroutine in
    this and pass the WRAPPER to ``asyncio.run``. If ``node`` was condemned before registration, the
    cancellation is delivered at the first suspension point inside ``coro``.
    """
    registry.register(node, attempt, asyncio.get_running_loop(), asyncio.current_task())
    try:
        return await coro
    finally:
        registry.revoke(node, attempt)
