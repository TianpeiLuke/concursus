"""Node execution status and its presentation — the single source for every renderer.

This vocabulary used to live in ``tests/dag_render.py``. It moved here (U0) because
presentation belongs with the UI layer, not with test support: ``dag_render``'s renderer is replaced
at U3, while its *record* layer stays, and the status table must outlive the renderer that
happened to define it first.

``dag_render`` re-exports every name below, so its ~94 existing references keep working unchanged
and the import direction is now correct — test support consumes the shipped vocabulary rather than
owning it.

One table drives the mermaid ``classDef``, the HTML badge, the text report, the DAG node and the
Kanban column, so those renderers can never disagree about what a colour means.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

__all__ = [
    "STATUS_COMPLETED",
    "STATUS_CRASH",
    "STATUS_FUTILITY",
    "STATUS_HELD",
    "STATUS_HOLD",
    "STATUS_NOT_REACHED",
    "STATUS_ORDER",
    "STATUS_PENDING",
    "STATUS_PREEMPTED",
    "STATUS_RUNNING",
    "STATUS_STYLE",
    "status_style",
]

# --------------------------------------------------------------- terminal execution status
#: The terminal statuses a node can hold after a run. The four failure values are exactly the
#: Supervisor's ``_FAILURE_CLASSES``, so a record's own ``failure_class`` maps straight through;
#: ``held`` and ``not_reached`` cover the two cases that write NO record at all.
STATUS_COMPLETED = "completed"
STATUS_CRASH = "crash"
STATUS_HOLD = "hold"
STATUS_PREEMPTED = "preemptive_termination"
STATUS_FUTILITY = "futility_cancelled"
STATUS_HELD = "held"
STATUS_NOT_REACHED = "not_reached"

# ------------------------------------------------------------- transient (live) execution status
#: Two NON-terminal statuses, used only while a run is still in flight. They never appear in a
#: post-run record: by the time the store is final, every node has resolved into one of the seven
#: terminal values above. ``running`` is derived from the fleet timeline rather than the store,
#: because a dispatched-but-unresolved node writes NO record — that absence is exactly what
#: distinguishes "in flight" from "never reached".
STATUS_RUNNING = "running"
STATUS_PENDING = "pending"

#: Reporting order (best outcome first) — used for the summary counts and the HTML legend.
#:
#: This is a **reporting** order, not a flow order. A Kanban board must NOT lay its columns out in
#: this sequence: ``completed`` first would make a card travel right-to-left as it progresses. See
#: :data:`concursus.ui.console.COLUMN_ORDER` for the flow order.
STATUS_ORDER: Tuple[str, ...] = (
    STATUS_COMPLETED,
    STATUS_RUNNING,
    STATUS_PENDING,
    STATUS_PREEMPTED,
    STATUS_FUTILITY,
    STATUS_CRASH,
    STATUS_HOLD,
    STATUS_HELD,
    STATUS_NOT_REACHED,
)

#: Per-status presentation. ``label`` is the human phrasing, ``color`` the stroke/text colour and
#: ``fill`` the card/node background.
STATUS_STYLE: Dict[str, Dict[str, str]] = {
    STATUS_COMPLETED: {"label": "completed", "color": "#86efac", "fill": "#12271b"},
    STATUS_RUNNING: {"label": "running", "color": "#fde047", "fill": "#2a2410"},
    STATUS_PENDING: {"label": "pending", "color": "#4b5563", "fill": "#12141a"},
    STATUS_PREEMPTED: {"label": "monitor-terminated", "color": "#fdba74", "fill": "#2a1d10"},
    STATUS_FUTILITY: {"label": "futility-cancelled", "color": "#c4b5fd", "fill": "#1e1a2e"},
    STATUS_CRASH: {"label": "crashed", "color": "#fca5a5", "fill": "#2a1416"},
    STATUS_HOLD: {"label": "blocked", "color": "#9aa4b2", "fill": "#1a1d24"},
    STATUS_HELD: {"label": "governance hold", "color": "#7dd3fc", "fill": "#0f2027"},
    STATUS_NOT_REACHED: {"label": "not reached", "color": "#6b7280", "fill": "#14161b"},
}


def status_style(status: Optional[str]) -> Dict[str, str]:
    """Presentation for ``status``, falling back to ``not_reached`` for anything unrecognized."""
    return STATUS_STYLE.get(status or "", STATUS_STYLE[STATUS_NOT_REACHED])
