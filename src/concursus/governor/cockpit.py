"""Read-only director cockpit v0 (S6-G5).

A thin PROJECTION layer over ALREADY-SHIPPED read models. It composes three
director surfaces out of nothing but query/summary/render* calls:

  (a) briefing        -> render_precedent_hub + Supervisor.summary
  (b) exception_queue -> RunIndex.query(status="failed") + summary().failed
  (c) runs_monitor    -> RunIndex metadata + plan-version / progress

INV-5 (memory seam): cockpit/registry/scope are READ-ONLY. This module
SELECTS nothing, SEEDS nothing, SCHEDULES nothing, and holds no mutable
executed-prefix cache. It NEVER calls assemble(), Supervisor.run(), or
StateStore.put() -- it re-derives every view from the append-only log on
each call via read-only surfaces. Imports are restricted to the render*
projection and read models exposed through the injected Supervisor.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from concursus.governor.scope import ACTUATING_VERBS, READ_VERBS
from concursus.state.distill import render_precedent_hub


def _rec_seq(rec: Any) -> int:
    """The store-assigned strict-monotonic sequence of a record (0 when absent).

    ``seq`` (not ``timestamp``) is the deterministic replay/offset key — see
    :func:`~concursus.state.statestore._supersedes`.
    """
    seq = getattr(rec, "seq", None)
    return seq if seq is not None else 0


class NodeEventBus:
    """A ~read-side per-node stream multiplexer (KiRoom 36b13 ``terminalEventBus``).

    One ingest point, N per-node listeners: an observer subscribes to a single
    node id and receives only that node's chunks, so an operator can isolate one
    agent's (or one inner-graph investigator's) output without threading callbacks
    through producers. Pure dispatch — it holds no run state and drives nothing
    (INV-5); producers are decoupled from consumers.
    """

    def __init__(self) -> None:
        self._listeners: Dict[str, Set[Callable[[str, Any], None]]] = {}

    def subscribe(self, node_id: str, listener: Callable[[str, Any], None]) -> Callable[[], None]:
        """Register ``listener`` for ``node_id``; returns an unsubscribe thunk."""
        self._listeners.setdefault(node_id, set()).add(listener)

        def _unsub() -> None:
            subs = self._listeners.get(node_id)
            if subs is not None:
                subs.discard(listener)

        return _unsub

    def emit(self, node_id: str, chunk: Any) -> None:
        """Fan ``chunk`` out to every listener subscribed to ``node_id`` (no-op if none)."""
        for listener in tuple(self._listeners.get(node_id, ())):
            listener(node_id, chunk)


class DirectorCockpit:
    """A read-only director view over one run's shipped read models.

    The cockpit is handed an ALREADY-EXECUTED (or resumable) ``Supervisor``
    plus an optional vault path and plan value. It never drives the run; it
    only reads ``supervisor.summary()`` / ``supervisor.index()`` and renders
    the (idempotent) precedent hub.
    """

    def __init__(self, *, supervisor: Any, vault_path: Optional[str] = None,
                 plan: Any = None,
                 escalated: Optional[List[str]] = None,
                 unmatched: Optional[List[str]] = None) -> None:
        self._supervisor = supervisor
        self._vault_path = vault_path
        self._plan = plan
        # OPT-IN read-only governance sets from the last GovernorLoop run (I-1). Default None => []
        # => today's failed-only exception queue is byte-for-byte unchanged. These are just VALUES;
        # the cockpit NEVER re-derives, assembles, or dispatches to obtain them (INV-5).
        self._escalated: List[str] = list(escalated or [])
        self._unmatched: List[str] = list(unmatched or [])

    # ---- (a) briefing -------------------------------------------------
    def briefing(self, *, slipbox_form: bool = False, date: str = "") -> Dict[str, Any]:
        """A director briefing: run summary + (optional) precedent-hub path.

        Purely a read: it renders the IDEMPOTENT precedent hub (select-nothing,
        seed-nothing projection) when a vault path is present and folds in the
        supervisor's read-only summary. No plan is assembled, no node dispatched.
        """
        summary = self._supervisor.summary()
        hub_path: Optional[str] = None
        if self._vault_path is not None:
            hub_path = render_precedent_hub(
                self._vault_path, slipbox_form=slipbox_form, date=date
            )
        return {
            "summary": summary,
            "summary_line": self._supervisor.summary_line(),
            "precedent_hub": hub_path,
            "revision": self._revision(),
        }

    # ---- (b) exception / judgment queue ------------------------------
    def exception_queue(self) -> List[Dict[str, Any]]:
        """The failed/blocked nodes awaiting a director judgment.

        Driven by ``Supervisor.summary()['failed']`` (the shipped read model
        over ``store.completed()`` + terminal failures) and enriched, where
        available, with the latest failed :class:`Record` from
        ``RunIndex.query(status='failed')`` for attempt/address metadata. The
        failed-node set and reason are always exactly the summary's failed rows.

        In addition, when the cockpit was handed the last run's read-only
        governance sets (I-1), one distinct row per escalated node
        (``reason='escalated'``) and per unmatched node (``reason='unmatched'``)
        is APPENDED. These are read-only VALUES passed in at construction — the
        cockpit re-derives nothing and drives no dispatch (INV-5). With no
        governance sets (the default), the queue is exactly the failed rows.
        """
        failed = self._supervisor.summary()["failed"]
        index = self._run_index()
        latest_by_node: Dict[str, Any] = {}
        for rec in index.query(status="failed"):
            prior = latest_by_node.get(rec.node)
            if prior is None or getattr(rec, "seq", 0) >= getattr(prior, "seq", 0):
                latest_by_node[rec.node] = rec

        queue: List[Dict[str, Any]] = []
        for node in self._supervisor.summary()["order"]:
            if node not in failed:
                continue
            rec = latest_by_node.get(node)
            queue.append({
                "node": node,
                "reason": failed[node],
                "attempt": getattr(rec, "attempt", None) if rec is not None else None,
                "address": getattr(rec, "address", None) if rec is not None else None,
                "content_hash": getattr(rec, "content_hash", None) if rec is not None else None,
            })
        # Append read-only governance rows (I-1): escalations then unmatched, in stable order.
        for node in self._escalated:
            queue.append({
                "node": node,
                "reason": "escalated",
                "attempt": None,
                "address": None,
                "content_hash": None,
            })
        for node in self._unmatched:
            queue.append({
                "node": node,
                "reason": "unmatched",
                "attempt": None,
                "address": None,
                "content_hash": None,
            })
        return queue

    # ---- (c) runs-index monitor --------------------------------------
    def runs_monitor(self) -> Dict[str, Any]:
        """A runs-index monitor: plan version + progress over log metadata.

        Reads ``RunIndex`` metadata (node set, record count) and the
        supervisor summary's progress counters. Reports the frozen plan's
        ``revision`` so a director can see which compiled version produced the
        log. Read-only: it never touches the plan or the store.
        """
        summary = self._supervisor.summary()
        index = self._run_index()
        return {
            "session_id": self._supervisor.session_id,
            "revision": self._revision(),
            "total": summary["total"],
            "completed": summary["completed"],
            "failed_count": len(summary["failed"]),
            "completed_nodes": summary["completed_nodes"],
            "indexed_nodes": sorted(index.nodes()),
            "record_count": len(index.query()),
            "order": summary["order"],
        }

    # ---- (d) snapshot-then-follow over the append-only log (S6-G5, KiRoom 36b13) ----
    def snapshot(self) -> Dict[str, Any]:
        """A point-in-time view of the run: every record replayed to the current offset.

        The append-only ``StateStore`` log IS the ordered event stream, so a live
        view is a pure replay of ``RunIndex.query()`` ordered by the store-assigned
        strict-monotonic ``seq`` (INV-4). Returns the ordered records plus the
        ``offset`` (max ``seq`` seen) an observer passes to :meth:`follow` to tail
        forward. Read-only: it drives nothing and mutates nothing (INV-5).
        """
        records = sorted(self._run_index().query(), key=_rec_seq)
        offset = _rec_seq(records[-1]) if records else 0
        return {"offset": offset, "records": records, "count": len(records)}

    def follow(self, from_offset: int) -> Dict[str, Any]:
        """Tail records appended AFTER ``from_offset`` — replay-from-offset, never a full reconcile.

        A late-attaching or reconnecting observer passes its last-seen ``offset``;
        it receives only the newer slice (``seq > from_offset``), ordered by ``seq``,
        and a fresh ``offset``. Because the log is append-only and single-writer,
        this is loss-free with no drift and no reconcile branch (the KiRoom
        ``fetchEventsSince`` guarantee, obtained for free from the SSOT).
        """
        newer = sorted(
            (r for r in self._run_index().query() if _rec_seq(r) > from_offset),
            key=_rec_seq,
        )
        offset = _rec_seq(newer[-1]) if newer else from_offset
        return {"offset": offset, "records": newer, "count": len(newer)}

    # ---- (e) live family-tree over the frozen AgentDAG (S6-G5, KiRoom 36b13) ----
    def family_tree(self) -> Dict[str, Any]:
        """The frozen ``AgentDAG`` rendered as a lineage tree annotated with live status.

        Unlike KiRoom (which reconstructs lineage from runtime parent links), the
        full topology is known at compile time — ``plan.order`` for the node set and
        ``plan.wiring`` for the edges — so the cockpit draws the whole tree up front
        and merely colors each node ``done`` | ``failed`` | ``running`` | ``pending``
        from the append-only log. ``running`` = a node that has emitted at least one
        record but is neither completed nor failed. Read-only; no plan is touched.
        """
        summary = self._supervisor.summary()
        order: List[str] = summary["order"]
        failed = summary["failed"]
        done = set(summary["completed_nodes"])
        seen = {r.node for r in self._run_index().query()}
        wiring = getattr(self._plan, "wiring", {}) if self._plan is not None else {}

        def _status(node: str) -> str:
            if node in failed:
                return "failed"
            if node in done:
                return "done"
            if node in seen:
                return "running"
            return "pending"

        nodes = [
            {
                "node": node,
                "status": _status(node),
                "producers": [getattr(ref, "producer", ref) for ref in wiring.get(node, [])],
            }
            for node in order
        ]
        return {
            "revision": self._revision(),
            "nodes": nodes,
            "counts": {
                "done": sum(1 for n in nodes if n["status"] == "done"),
                "failed": sum(1 for n in nodes if n["status"] == "failed"),
                "running": sum(1 for n in nodes if n["status"] == "running"),
                "pending": sum(1 for n in nodes if n["status"] == "pending"),
            },
        }

    # ---- read-only helpers -------------------------------------------
    def _run_index(self) -> Any:
        """The run's read-only :class:`RunIndex` over the append-only log."""
        return self._supervisor.index()

    def _revision(self) -> Optional[int]:
        """The frozen plan's revision, or ``None`` if no plan value was handed in."""
        if self._plan is None:
            return None
        return getattr(self._plan, "revision", None)


class ControlSurfaceError(RuntimeError):
    """Raised when a control-surface verb is refused (unauthorized, inactive, or offline)."""


class ControlSurface:
    """A thin, OPT-IN, agent-facing CONTROL SURFACE over the SSOT (-CS).

    Where :class:`DirectorCockpit` gives a HUMAN director read-only projections, this gives a
    governed AGENT a narrow, in-process (NOT HTTP) handle over the same single source of truth.
    It is READ-MOSTLY and offline-by-default:

    * **Read verbs** (:data:`~concursus.governor.scope.READ_VERBS`) are ALWAYS on — pure
      projections over the frozen plan, the append-only log, the run-db FTS, and the durable
      precedents. They mutate nothing and cannot touch the frozen plan.
    * **Actuating verbs** (:data:`~concursus.governor.scope.ACTUATING_VERBS` — deploy / run /
      recompile) route THROUGH the EXISTING actuators only (the surface owns no side-effect logic).
      Availability is resolved from the compiled :class:`~concursus.governor.scope.ControlScope`
      (the frozen plan/scope), NOT an env var: a verb the scope did not authorize is ABSENT
      (:meth:`invoke` raises rather than routing — non-registration, no bypassable deny stub).
      Present actuating verbs are further gated by an explicit :meth:`activate` gate, and the
      requested :class:`~concursus.build.trust.TrustGrade` is CLAMPED down to the compiled
      ceiling — never above it.

    INV-5 / compiler discipline: the surface holds the SAME read-only ``supervisor`` /
    ``plan`` the cockpit does. It re-derives every read from the log on each call, cannot mutate
    the frozen plan, and pulls NO boto3/langgraph at import (actuators are INJECTED thunks; a
    surface with none is fully read-only + offline). Every actuating capability is default-off.
    """

    def __init__(
        self,
        *,
        supervisor: Any,
        scope: Any,
        vault_path: Optional[str] = None,
        plan: Any = None,
        actuators: Optional[Dict[str, Callable[..., Any]]] = None,
    ) -> None:
        self._supervisor = supervisor
        self._scope = scope
        self._vault_path = vault_path
        self._plan = plan
        # INJECTED existing-actuator thunks ({verb: callable}); default {} => fully read-only.
        # A verb is only ACTUALLY invocable when BOTH the compiled scope authorizes it AND an
        # actuator was injected for it — the surface itself owns no side-effecting code path.
        self._actuators: Dict[str, Callable[..., Any]] = dict(actuators or {})
        # Explicit activation gate for dangerous (actuating) verbs — default OFF.
        self._active: Set[str] = set()

    # ---- verb resolution (from the COMPILED scope, never an env var) ----
    def verbs(self) -> List[str]:
        """The verbs this surface exposes: always-on read verbs + scope-authorized actuating verbs.

        Sorted for a stable listing. An actuating verb the compiled scope omitted is not here —
        non-registration means it cannot be invoked at all (there is no disabled stub)."""
        return sorted(self._scope.available_verbs())

    def has_verb(self, verb: str) -> bool:
        """Whether ``verb`` is registered on this surface (read verb, or scope-authorized actuator)."""
        return self._scope.authorizes(verb)

    # ---- read verbs (ALWAYS on; pure projections; mutate nothing) -------
    def query_plan(self) -> Dict[str, Any]:
        """READ: the compiled dispatch order + revision of the frozen plan (never mutates it)."""
        return {
            "order": list(getattr(self._plan, "order", []) or []),
            "revision": getattr(self._plan, "revision", None) if self._plan is not None else None,
        }

    def tail_log(self, from_offset: int = 0) -> Dict[str, Any]:
        """READ: tail the append-only run log after ``from_offset`` (a :meth:`DirectorCockpit.follow`)."""
        return DirectorCockpit(
            supervisor=self._supervisor, plan=self._plan
        ).follow(from_offset)

    def search_runs(self, text: str = "", *, key: Optional[str] = None,
                    nodes: Optional[List[str]] = None, status: Optional[str] = None,
                    limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """READ: rundb/precedent FTS over the durable cross-run store (empty when no vault).

        A pure read over the precedent notes via
        :class:`~concursus.state.precedent.PrecedentRetriever` (offline lexical rung by
        default — no embed model). Returns ``[]`` when no vault path was handed in."""
        if self._vault_path is None:
            return []
        # Lazy import keeps the surface import-light (no retriever pulled unless a search fires).
        from concursus.state.precedent import PrecedentRetriever

        retriever = PrecedentRetriever(self._vault_path)
        hits = retriever.retrieve(text, key=key, nodes=nodes, status=status, limit=limit)
        return [h.to_dict() for h in hits]

    def precedents(self) -> List[dict]:
        """READ: the frozen plan's advisory precedent context (a copy; the plan is untouched)."""
        return [dict(p) for p in (getattr(self._plan, "precedents", None) or [])]

    # ---- activation gate for dangerous (actuating) verbs ----------------
    def activate(self, verb: str) -> None:
        """Explicitly ARM a dangerous (actuating) verb before it may be invoked.

        Non-registration first: a verb the compiled scope did not authorize can NEVER be armed
        (raises) — activation only lifts the second, explicit safety catch on an already-authorized
        actuating verb. Read verbs need no activation (arming one is a no-op-level error)."""
        if verb not in ACTUATING_VERBS:
            raise ControlSurfaceError(
                f"verb {verb!r} is not an actuating verb; only {sorted(ACTUATING_VERBS)} are armable"
            )
        if not self._scope.authorizes(verb):
            raise ControlSurfaceError(
                f"verb {verb!r} is not authorized by the compiled scope (revision "
                f"{getattr(self._scope, 'revision', None)}); it cannot be activated"
            )
        self._active.add(verb)

    def is_active(self, verb: str) -> bool:
        """Whether a dangerous verb has been explicitly armed via :meth:`activate`."""
        return verb in self._active

    # ---- actuating verbs (route THROUGH existing actuators only) --------
    def effective_trust(self, requested: Any) -> Any:
        """The requested TrustGrade CLAMPED down to the compiled ceiling (never above it).

        Delegates to :func:`~concursus.build.trust.clamp_trust_grade`. With no compiled
        ceiling on the scope, the requested grade passes through (the activation gate is then the
        sole guard)."""
        from concursus.build.trust import TrustGrade, clamp_trust_grade

        ceiling = getattr(self._scope, "trust_ceiling", None)
        if ceiling is None:
            return TrustGrade.parse(requested)
        return clamp_trust_grade(ceiling, requested)

    def invoke(self, verb: str, /, *args: Any, **kwargs: Any) -> Any:
        """Route an ACTUATING verb through its INJECTED existing actuator, under three gates.

        1. **Non-registration** — a verb the compiled scope did not authorize is absent: raise.
        2. **Activation** — a dangerous verb must have been explicitly :meth:`activate`\\ d.
        3. **Trust clamp** — when a ``trust``/``requested_trust`` kwarg is present, it is clamped
           DOWN to the compiled ceiling (:meth:`effective_trust`) before the actuator sees it.

        Read verbs are NOT routed here (they are pure methods that mutate nothing); calling
        :meth:`invoke` for a read verb raises so actuation stays the only thing that flows through
        the gated path. The surface owns no side-effect logic — it merely calls the injected thunk;
        with no actuator wired for ``verb`` it raises (offline-by-default)."""
        if verb not in ACTUATING_VERBS:
            raise ControlSurfaceError(
                f"verb {verb!r} is not an actuating verb; use the read methods for "
                f"{sorted(READ_VERBS)}"
            )
        if not self._scope.authorizes(verb):
            raise ControlSurfaceError(
                f"verb {verb!r} is not authorized by the compiled scope (revision "
                f"{getattr(self._scope, 'revision', None)}) — it is absent from this surface"
            )
        if verb not in self._active:
            raise ControlSurfaceError(
                f"verb {verb!r} is authorized but not activated; call activate({verb!r}) first"
            )
        actuator = self._actuators.get(verb)
        if actuator is None:
            raise ControlSurfaceError(
                f"verb {verb!r} has no injected actuator (offline-by-default); wire one to invoke it"
            )
        if "trust" in kwargs:
            kwargs["trust"] = self.effective_trust(kwargs["trust"])
        if "requested_trust" in kwargs:
            kwargs["requested_trust"] = self.effective_trust(kwargs["requested_trust"])
        return actuator(*args, **kwargs)
