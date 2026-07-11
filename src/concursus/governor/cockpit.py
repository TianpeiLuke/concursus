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

from typing import Any, Dict, List, Optional

from concursus.state.distill import render_precedent_hub


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

    # ---- read-only helpers -------------------------------------------
    def _run_index(self) -> Any:
        """The run's read-only :class:`RunIndex` over the append-only log."""
        return self._supervisor.index()

    def _revision(self) -> Optional[int]:
        """The frozen plan's revision, or ``None`` if no plan value was handed in."""
        if self._plan is None:
            return None
        return getattr(self._plan, "revision", None)
