"""The **memory loop** — post-run distillation of a completed run into retrievable precedent.

Concursus is a *compiler*: ``AgentDAG -> assemble -> frozen ProvisioningPlan -> Supervisor.run``
as a static topological walk, and resume is replay of the append-only run log. That machinery is
strictly *forward* — nothing about a run's result ever feeds back into a running plan. This module
is the other, offline half: once a run has *finished*, it folds the run's ``{node: output}``
result + its recorded ``consumes`` graph + its outcome into **one compact precedent note** on the
FileVault substrate (:func:`distill_run`, AI-15), and projects the accumulated SET of those
per-run precedent notes into **one cross-run hub** (:func:`render_precedent_hub`, AI-16 — the
``entry_folgezettel_trails`` analogue) so past runs become retrievable precedent.

Identity guard (non-negotiable): both halves are **pure post-run** operations.

* :func:`distill_run` is a WRITE that runs *after* :meth:`Supervisor.run` returns. It NEVER feeds
  back into a running plan — it does not pick, seed, replan, or mutate any topology. It reuses
  :func:`~concursus.filevault._record_to_note` (never a greenfield writer) and writes to a
  dedicated ``<vault>/precedents/`` tree, kept OUT of the run dirs so it can never be reloaded as
  a run :class:`~concursus.statestore.Record` (i.e. it can never leak into a resume/replay).
* :func:`render_precedent_hub` is a READ-ONLY render regenerated from the existing precedent notes
  (the single source of truth). It is NOT a live cross-run router/scheduler; it selects nothing
  and starts no run. Deleting it (or the derived :func:`~concursus.rundb.build_precedent_db`
  SQLite) loses nothing — both rebuild from the notes.

Pure-Python, stdlib only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .filevault import (
    FileVaultStateStore,
    _record_to_note,
    _note_to_record,
    _slug,
    _SLIPBOX_TOPICS,
)
from .statestore import Record, _index_records, content_hash

# The dedicated cross-run precedent tree, a SIBLING of ``<vault>/runs/`` — deliberately NOT under
# any run dir, so a precedent note is never globbed back as a run record (never leaks into replay).
_PRECEDENTS_DIRNAME = "precedents"

# The cross-run hub note (the ``entry_folgezettel_trails`` analogue). Skipped by the precedent
# loaders exactly as ``_run.md`` is skipped by the run-record loaders.
_HUB_NAME = "_index.md"

# The precedent note's schema tag / record kind: a run-summary checkpoint (a produced artifact,
# so :func:`~concursus.filevault._building_block_for` derives ``empirical_observation``), NOT an
# agent output — it must never be mistaken for a node's result on resume.
_PRECEDENT_SCHEMA = "run_precedent"


# --------------------------------------------------------------------------- outcome derivation
def _derive_outcome(records: Sequence[Record]) -> Dict[str, object]:
    """Derive a run outcome from its records alone (when the caller passes no explicit outcome).

    Mirrors :meth:`Supervisor.summary`: the latest record per node decides completion, ``total``
    is the count of distinct executed nodes, and a node whose latest record is ``failed`` becomes
    a ``failed`` row (reason read from its ``blocked_on`` meta, ``""`` for a genuine failure).
    """
    latest_overall, _, _ = _index_records(list(records))
    completed = sorted(n for n, r in latest_overall.items() if r.status == "validated")
    failed = {
        n: (getattr(r, "blocked_on", None) or "")
        for n, r in latest_overall.items()
        if r.status == "failed"
    }
    return {
        "total": len(latest_overall),
        "completed": len(completed),
        "completed_nodes": completed,
        "failed": failed,
    }


def _run_status(outcome: Dict[str, object]) -> str:
    """A one-word run verdict: ``completed`` (all done, none failed), ``partial`` (some done, some
    not / failed), or ``failed`` (nothing validated). Derived, never stored by the supervisor."""
    total = int(outcome.get("total", 0) or 0)
    completed = int(outcome.get("completed", 0) or 0)
    failed = outcome.get("failed") or {}
    if not failed and total > 0 and completed >= total:
        return "completed"
    if completed > 0:
        return "partial"
    return "failed"


def build_precedent_payload(
    result: Dict[str, dict],
    records: Sequence[Record],
    *,
    trail_id: str,
    outcome: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """Fold a finished run into ONE compact, JSON-serializable precedent payload.

    Captures what makes a run retrievable precedent: its ``trail_id``, a one-word ``status``, the
    outcome counts (``total`` / ``completed`` / ``failed``), the executed ``nodes``, the recorded
    ``consumes`` data-dependency graph (``[consumer, producer, jsonpath]`` rows, reconstructed from
    each record's edges — never re-derived from a live plan), and the final ``{node: output}``
    ``results``. Pure function; no I/O, no plan access.
    """
    records = list(records)
    edges: List[List[str]] = []
    for r in records:
        for edge in r.consumes:
            prod, _, path = edge.partition(":")
            edges.append([r.node, prod, path or ""])
    if outcome is None:
        outcome = _derive_outcome(records)
    return {
        "trail_id": trail_id,
        "status": _run_status(outcome),
        "outcome": {
            "total": int(outcome.get("total", 0) or 0),
            "completed": int(outcome.get("completed", 0) or 0),
            "failed": dict(outcome.get("failed") or {}),
        },
        "nodes": sorted({r.node for r in records}),
        "consumes": edges,
        "results": dict(result),
    }


# --------------------------------------------------------------------------- AI-15: the write half
def precedents_dir(vault_path) -> Path:
    """The dedicated ``<vault>/precedents/`` tree (a sibling of ``<vault>/runs/``)."""
    return Path(vault_path) / _PRECEDENTS_DIRNAME


def _precedent_note_name(trail_id: str) -> str:
    """The precedent note's filename — deterministic in ``trail_id`` (re-distilling one run's
    family overwrites its single note, so the hub stays one-row-per-run)."""
    return f"{_slug(trail_id)}.md"


def _precedent_related(vault_path, run_dir) -> List[str]:
    """The precedent note's ``## Related Notes`` links (slipbox form only): back to the run's own
    Folgezettel entry point when known, else the cross-run hub — so a precedent is never orphaned."""
    if run_dir is not None:
        rel = os.path.relpath(Path(run_dir) / "_run.md", precedents_dir(vault_path))
        return [f"[Run entry point]({rel})"]
    return [f"[Precedent hub]({_HUB_NAME})"]


def distill_run(
    result: Dict[str, dict],
    records: Sequence[Record],
    *,
    vault_path,
    trail_id: str = "run",
    outcome: Optional[Dict[str, object]] = None,
    run_dir=None,
    slipbox_form: bool = False,
    date: str = "",
) -> str:
    """**AI-15.** Distill ONE finished run into a single precedent note; return its path.

    A pure POST-RUN write: it takes the run's ``{node: output}`` ``result``, its recorded
    ``records`` (for the ``consumes`` graph + statuses), and its ``outcome`` (defaulting to one
    derived from the records), builds a :func:`build_precedent_payload`, wraps it in a synthetic
    run-summary :class:`Record`, and renders it through the SAME
    :func:`~concursus.filevault._record_to_note` the FileVault store uses (round-trip-exact, so the
    precedent reloads via :func:`~concursus.filevault._note_to_record`). The note lands under
    ``<vault>/precedents/`` — never a run dir — so it can never be replayed as run state.

    This NEVER feeds back into a running plan: it is invoked only after :meth:`Supervisor.run`
    returns, mutates no topology, and seeds nothing.
    """
    payload = build_precedent_payload(result, records, trail_id=trail_id, outcome=outcome)
    record = Record(
        node=trail_id,
        output=payload,
        attempt=1,
        status="validated",
        record_type="checkpoint",
        schema=_PRECEDENT_SCHEMA,
        producer=trail_id,
        content_hash=content_hash(payload),
        address=trail_id,
    )
    text = _record_to_note(
        record,
        slipbox_form=slipbox_form,
        position=1,
        trail_id=trail_id,
        date=date,
        related=_precedent_related(vault_path, run_dir),
    )
    prec_dir = precedents_dir(vault_path)
    prec_dir.mkdir(parents=True, exist_ok=True)
    path = prec_dir / _precedent_note_name(trail_id)
    FileVaultStateStore._atomic_write(path, text)
    return str(path)


def distill_store(
    store: FileVaultStateStore,
    *,
    result: Optional[Dict[str, dict]] = None,
    outcome: Optional[Dict[str, object]] = None,
    vault_path=None,
    slipbox_form: Optional[bool] = None,
    date: Optional[str] = None,
) -> str:
    """Convenience: distill a run straight from its :class:`FileVaultStateStore` (post-run).

    Reads the store's ``run_dir`` / ``trail_id`` (public accessors) and, when the caller passes no
    explicit ``result``, projects ``{node: latest validated output}`` from the store's completed
    frontier. ``vault_path`` defaults to the store's ``<vault>/runs/<slug>`` grandparent (the
    ``from_config`` layout). Still a pure post-run write — it only reads the finished store.
    """
    run_dir = Path(store.run_dir)
    trail_id = store.trail_id
    if vault_path is None:
        vault_path = run_dir.parent.parent  # <vault>/runs/<slug> -> <vault>
    if result is None:
        result = {node: store.get(node) for node in sorted(store.completed())}
    return distill_run(
        result,
        store.records(),
        vault_path=vault_path,
        trail_id=trail_id,
        outcome=outcome,
        run_dir=run_dir,
        slipbox_form=store._slipbox_form if slipbox_form is None else slipbox_form,
        date=store._date if date is None else date,
    )


# --------------------------------------------------------------------------- AI-16: the read half
def load_precedents(vault_path) -> List[Record]:
    """Read every precedent note under ``<vault>/precedents/`` into records (the single source of
    truth for the cross-run projections). Skips the hub note and any tolerant-of-malformed file."""
    prec_dir = precedents_dir(vault_path)
    out: List[Record] = []
    if not prec_dir.exists():
        return out
    for note in sorted(prec_dir.glob("*.md")):
        if note.name == _HUB_NAME:
            continue
        try:
            out.append(_note_to_record(note.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out


def _precedents_by_trail(vault_path) -> Dict[str, Dict[str, object]]:
    """The precedent payloads keyed by ``trail_id`` (one entry per run/family). Deterministic."""
    by_trail: Dict[str, Dict[str, object]] = {}
    for r in load_precedents(vault_path):
        payload = r.output if isinstance(r.output, dict) else {}
        tid = str(payload.get("trail_id") or r.node)
        by_trail[tid] = payload
    return by_trail


def _hub_row(trail_id: str, payload: Dict[str, object]) -> str:
    """One hub row for a run: a link to its precedent note plus a compact outcome digest."""
    oc = payload.get("outcome") or {}
    status = payload.get("status", "")
    completed = oc.get("completed", 0)
    total = oc.get("total", 0)
    failed = oc.get("failed") or {}
    fail_note = f", failed {sorted(failed)}" if failed else ""
    return f"- [{trail_id}]({_precedent_note_name(trail_id)}) — {status} {completed}/{total}{fail_note}"


def render_precedent_hub(vault_path, *, slipbox_form: bool = False, date: str = "") -> str:
    """**AI-16.** Render the cross-run precedent hub (``<vault>/precedents/_index.md``); return its
    path — the ``entry_folgezettel_trails`` analogue for accumulated runs.

    A pure, idempotent READ-ONLY projection over the SET of per-run precedent notes: one row per
    run/family (keyed by ``trail_id``), sorted, regenerated from scratch each call (same notes ->
    byte-identical output). It is a *retrieval index*, NOT a live router/scheduler: it selects no
    run and seeds nothing. Deleting it loses nothing (this rebuilds it from the notes).
    """
    by_trail = _precedents_by_trail(vault_path)
    rows = [_hub_row(tid, by_trail[tid]) for tid in sorted(by_trail)]

    lines: List[str] = []
    if slipbox_form:
        fm = {
            "tags": ["resource", "concursus", "run_state", "entry_point"],
            "keywords": ["concursus precedent", "cross-run hub", "run precedent index"],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": date,
            "status": "active",
            "building_block": "navigation",
            "folgezettel": "1",
            "lineage": ["concursus_precedents:1"],
            "access_control_group": ["general"],
        }
        import json as _json

        lines.append("---")
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {_json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {_json.dumps(value)}")
        lines.append("---")
        lines.append("")

    lines.append("# Concursus Precedent Hub")
    lines.append("")
    lines.append(
        "Cross-run precedent index — one row per distilled run. A read-only projection regenerated "
        "from the per-run precedent notes under `precedents/` (the single source of truth); it "
        "selects and seeds nothing."
    )
    lines.append("")
    lines.extend(rows if rows else ["- (no runs distilled yet)"])
    lines.append("")

    prec_dir = precedents_dir(vault_path)
    prec_dir.mkdir(parents=True, exist_ok=True)
    path = prec_dir / _HUB_NAME
    FileVaultStateStore._atomic_write(path, "\n".join(lines))
    return str(path)
