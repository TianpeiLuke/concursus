"""The **inner graph** — parallel hypothesis-investigator dispatch + DIGEST write-back (AI-25 + AI-29).

Concursus is a **compiler, not a runtime governor**. This module is the FAR-HORIZON reasoning tier
and therefore the highest identity risk, so its contract is deliberately narrow: everything here is
**PLAN-FORMATION**, STRICTLY BEFORE :meth:`~concursus.assemble.OrchestrationAssembler.assemble`, and
it NEVER dispatches a committed agent, is NEVER wired inside
:meth:`~concursus.supervisor.Supervisor.run` (which stays a single forward topo pass over a frozen
``plan.order``), and NEVER writes a ``.3`` verdict (that is the engine's job, via
:meth:`~concursus.trailstore.HypothesisTrail.write_verdict`). It is confined to the ``.2`` worker-log
lane — exactly the lane a run's per-worker execution logs live in.

Two layers, both pure stdlib — concursus imports and its full suite passes with NEITHER langgraph
NOR any LLM installed:

* **AI-25 — parallel hypothesis-investigator dispatch (a disposable per-round projection).**
  :func:`partition_frontier` splits an open frontier into a bounded fan-out (each batch ``<=`` a
  ``concurrency_ceiling``). :func:`compile_inner_graph` snapshots the CURRENT open frontier of the
  pre-commit mutable hypothesis set into a FRESH, DISPOSABLE :class:`InnerGraph` — it is rebuilt
  each round and thrown away, so it can NEVER ossify into a cyclic executor over the frozen
  committed plan. :func:`dispatch_frontier` runs ONE injected ``investigator`` per open hypothesis,
  clamped to the ``concurrency_ceiling`` via a bounded thread pool, and merge-reduces the results
  ORDER-INSENSITIVELY (keyed by hypothesis id, so completion order is irrelevant). The investigator
  is an INJECTED callable defaulting to a deterministic stub, so import and tests need NO LLM/agents.
* **AI-29 — DIGEST write-back through the SlipBox capture-validate-fix workflow.** Each investigator
  result is digested by :class:`InnerGraphDigest` as (1) an append-only **ACTION marker** on the
  ``.2/<k>`` worker-log lane (deduped on ``node.id:action``) and (2) a **slipbox-card RESULT** whose
  raw payload is OFFLOADED to a ``log_ref`` file (never inlined into the card). A worker FAILURE is a
  first-class :class:`InvestigationResult` with ``ok=False`` — NEVER a raised exception. A retried
  digest carrying the same ``dedup_key`` is an idempotent NO-OP (verified on reload, so it survives
  process restart). The digest is confined to the ``.2`` lane; it NEVER touches the ``.3`` reasoning
  branch — turning a result into a cited ``.3`` verdict is the engine's job via
  :meth:`~concursus.trailstore.HypothesisTrail.write_verdict`.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from .filevault import FileVaultStateStore, _slug
from .statestore import content_hash
from .trailstore import Hypothesis, HypothesisTrail

# The ``.2`` worker-log lane — the SAME reserved branch a run's per-worker execution logs live in.
# The inner graph is confined here: it appends ACTION markers + slipbox-card RESULTs to ``.2`` and
# NEVER writes a ``.3`` verdict (that is the engine's job via HypothesisTrail.write_verdict).
_LANE = ".2"

# Per-worker append-only log lane files (``.2/log_<k>.jsonl``), the slipbox-card RESULT notes
# (``.2/cards/<slug>.md``), and the OFFLOADED raw payloads the cards reference (``.2/raw/<slug>.json``).
_LOG_PREFIX = "log_"
_CARDS_DIR = "cards"
_RAW_DIR = "raw"

_KIND_ACTION = "action"  # the ACTION-marker record kind on a worker-log lane

# The default bounded fan-out width — a small, safe concurrency ceiling.
_DEFAULT_CEILING = 4


class InnerGraphError(ValueError):
    """Raised on an invalid inner-graph operation (a non-positive ceiling, a missing hypothesis)."""


# The AI-25 per-hypothesis work seam: given a hypothesis, return either a verdict spec
# ``{"verdict": "ACCEPT|REJECT|UNDEC", "evidence": {...}}`` or a list of child candidates — the SAME
# contract the DKS engine's ``investigator=`` uses. Defaults to a deterministic stub (no LLM). The
# inner graph never APPLIES the outcome to the ``.3`` trail; it only investigates + digests to ``.2``.
Investigator = Callable[[Hypothesis], object]


def _default_investigator(h: Hypothesis) -> dict:
    """The deterministic default per-hypothesis worker — closes nothing, proposes ``UNDEC``.

    Needs no LLM/agent so import and tests are model-free; a real deployment injects an LLM/agent
    ``investigator=``. The inner graph merely records this outcome as a ``.2`` digest — it never
    writes it as a ``.3`` verdict (the engine does that via ``write_verdict``).
    """
    return {"verdict": "UNDEC", "evidence": {"reason": "default deterministic stub"}}


@dataclass
class InvestigationResult:
    """One investigator's result over a single open hypothesis (AI-25/AI-29).

    A FAILURE is a first-class result with ``ok=False`` and an ``error`` string — NEVER a raised
    exception, so one worker's crash never aborts the fan-out or the merge-reduce.

    Attributes:
        hypothesis_id: The ``.3`` address of the hypothesis this result is about.
        ok: ``True`` on a clean investigation; ``False`` if the investigator raised (see ``error``).
        outcome: The investigator's return value (a verdict spec dict or a child-candidate list) on
            success; ``None`` on failure. The inner graph does NOT apply it to the ``.3`` trail.
        action: The action label recorded on the worker-log ACTION marker (default ``investigate``).
        error: ``"<ExcType>: <msg>"`` when ``ok`` is ``False``; ``None`` otherwise.
        worker: The ``.2/<worker>`` lane index this result was digested onto (assigned at dispatch).
        dedup_key: The idempotency key; defaults to ``"<hypothesis_id>:<action>"`` (``node.id:action``).
        log_ref: The relative path of the OFFLOADED raw payload the slipbox-card references (set by
            the digest); the raw outcome/error is never inlined into the card.
        card_ref: The relative path of the slipbox-card RESULT note (set by the digest).
        digested: Whether a digest wrote this result (``False`` if it was a dedup no-op or undigested).
    """

    hypothesis_id: str
    ok: bool = True
    outcome: Optional[object] = None
    action: str = "investigate"
    error: Optional[str] = None
    worker: int = 0
    dedup_key: str = ""
    log_ref: Optional[str] = None
    card_ref: Optional[str] = None
    digested: bool = False

    def key(self) -> str:
        """The idempotency key: the explicit ``dedup_key`` or the default ``node.id:action``."""
        return self.dedup_key or f"{self.hypothesis_id}:{self.action}"


@dataclass
class InnerGraph:
    """A FRESH, DISPOSABLE per-round projection of the OPEN frontier (AI-25).

    NOT a cyclic executor over the frozen committed plan: :func:`compile_inner_graph` rebuilds it
    each round from the pre-commit MUTABLE hypothesis set and it is thrown away after
    :func:`dispatch_frontier`. It carries only a read snapshot (``projection``) of the frontier
    hypotheses plus the bounded fan-out ``batches`` — never the durable trail, never the committed
    plan, so it cannot ossify into a runtime governor.

    Attributes:
        root: The ``.3`` root hypothesis whose subtree this projection was compiled from.
        batches: The bounded fan-out — a list of hypothesis-id batches, each ``<=`` ``ceiling``.
        ceiling: The concurrency ceiling this projection was partitioned to (the fan-out clamp).
        projection: A read-only ``{id: Hypothesis}`` snapshot of the frontier at compile time.
    """

    root: str
    batches: List[List[str]]
    ceiling: int
    projection: Dict[str, Hypothesis] = field(default_factory=dict)

    @property
    def frontier(self) -> List[str]:
        """The flat open frontier (all batches concatenated, in partition order)."""
        return [hid for batch in self.batches for hid in batch]

    def __len__(self) -> int:
        return sum(len(b) for b in self.batches)


# ================================================================= AI-25
def partition_frontier(frontier: Sequence[str], ceiling: int) -> List[List[str]]:
    """Split an open ``frontier`` into a BOUNDED fan-out — a list of batches each ``<=`` ``ceiling``.

    The ``ceiling`` is the concurrency clamp: every batch has at most ``ceiling`` hypotheses, so a
    later :func:`dispatch_frontier` never runs more than ``ceiling`` investigators at once. A
    non-positive ceiling is rejected (the fan-out must be bounded and make progress). Deterministic
    and order-preserving; an empty frontier yields ``[]``.
    """
    if ceiling < 1:
        raise InnerGraphError(f"concurrency ceiling must be >= 1 (bounded fan-out), got {ceiling}")
    ids = list(frontier)
    return [ids[i : i + ceiling] for i in range(0, len(ids), ceiling)]


def compile_inner_graph(
    trail: HypothesisTrail,
    root: str,
    *,
    concurrency_ceiling: int = _DEFAULT_CEILING,
    depth_cap: int = 5,
    confidence_floor: float = 0.6,
) -> InnerGraph:
    """Snapshot the CURRENT open frontier into a fresh, disposable :class:`InnerGraph` (AI-25).

    A per-round projection of the pre-commit mutable hypothesis set: it reads
    :meth:`~concursus.trailstore.HypothesisTrail.open_frontier` (within the ``depth_cap`` /
    ``confidence_floor`` caps), captures a read-only ``{id: Hypothesis}`` snapshot, and partitions
    the frontier into a bounded fan-out via :func:`partition_frontier`. The result is meant to be
    rebuilt every round and discarded — it holds NO reference to the durable trail or the committed
    plan, so it can never become a cyclic executor. Purely plan-formation; it mutates nothing.
    """
    frontier = trail.open_frontier(
        root, depth_cap=depth_cap, confidence_floor=confidence_floor
    )
    model = trail.hypotheses(root)
    projection = {hid: model[hid] for hid in frontier if hid in model}
    batches = partition_frontier(frontier, concurrency_ceiling)
    return InnerGraph(
        root=root, batches=batches, ceiling=concurrency_ceiling, projection=projection
    )


def _run_investigator(hid: str, hyp: Hypothesis, investigator: Investigator) -> InvestigationResult:
    """Run ONE investigator over ONE hypothesis, turning any crash into ``ok=False`` (never raise).

    A worker FAILURE is a first-class :class:`InvestigationResult` (``ok=False`` + an ``error``
    string), so one investigator's exception never aborts the fan-out or the order-insensitive merge.
    """
    try:
        outcome = investigator(hyp)
        return InvestigationResult(hypothesis_id=hid, ok=True, outcome=outcome)
    except Exception as exc:  # a worker failure is DATA, not control flow — never propagate
        return InvestigationResult(
            hypothesis_id=hid, ok=False, outcome=None, error=f"{type(exc).__name__}: {exc}"
        )


def dispatch_frontier(
    graph: InnerGraph,
    investigator: Optional[Investigator] = None,
    *,
    digest: Optional["InnerGraphDigest"] = None,
) -> Dict[str, InvestigationResult]:
    """Run ONE investigator per open hypothesis, clamped to the ceiling, merged ORDER-INSENSITIVELY.

    Dispatches the :class:`InnerGraph`'s bounded fan-out one batch at a time through a thread pool
    capped at ``graph.ceiling`` (so at most ``ceiling`` investigators run at once — a hard
    concurrency clamp). Each result is keyed by its hypothesis id, so the merge-reduce is
    order-insensitive: which worker finishes first is irrelevant. A worker FAILURE arrives as an
    ``ok=False`` result (:func:`_run_investigator` never lets an exception escape). When a ``digest``
    is supplied, each result is written back to the ``.2`` worker-log lane (AI-29) BEFORE it is
    merged; the inner graph never applies an outcome to the ``.3`` trail. Returns ``{id: result}``.
    """
    investigator = investigator or _default_investigator
    merged: Dict[str, InvestigationResult] = {}
    worker = 0
    for batch in graph.batches:
        if not batch:
            continue
        max_workers = min(graph.ceiling, len(batch))
        fut_worker = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for hid in batch:
                hyp = graph.projection.get(hid)
                if hyp is None:  # frontier drifted since compile — skip the stale id
                    worker += 1
                    continue
                fut = pool.submit(_run_investigator, hid, hyp, investigator)
                fut_worker[fut] = worker
                worker += 1
            for fut in as_completed(fut_worker):
                res = fut.result()
                res.worker = fut_worker[fut]
                if digest is not None:
                    digest.write_back(res)
                merged[res.hypothesis_id] = res  # order-insensitive: keyed by hypothesis id
    return merged


# ================================================================= AI-29
class InnerGraphDigest:
    """DIGEST an investigator result to the ``.2`` worker-log lane (AI-29) — capture-validate-fix.

    For each result it appends (1) an append-only **ACTION marker** to the ``.2/log_<k>`` worker-log
    lane (deduped on ``node.id:action``) and (2) a **slipbox-card RESULT** note whose raw payload is
    OFFLOADED to a ``log_ref`` file (never inlined). A retried digest carrying the same ``dedup_key``
    is an idempotent NO-OP — dedup keys are reloaded from the existing lane logs, so idempotency
    survives process restart. Confined to the ``.2`` lane: it NEVER writes a ``.3`` verdict (that is
    the engine's job via :meth:`~concursus.trailstore.HypothesisTrail.write_verdict`). Pure stdlib,
    atomic writes (reuses :meth:`FileVaultStateStore._atomic_write`), thread-safe under an ``RLock``.
    """

    def __init__(self, run_dir, *, lane: str = _LANE) -> None:
        self._lane = lane
        self._dir = Path(run_dir) / lane
        self._lock = threading.RLock()
        self._seen: Dict[str, str] = {}  # dedup_key -> the log-lane file it was recorded on
        self._loaded = False

    # -- identity accessors -------------------------------------------------
    @property
    def lane_dir(self) -> Path:
        """The on-disk ``.2`` worker-log lane directory this digest writes to."""
        return self._dir

    # -- write-back ---------------------------------------------------------
    def write_back(self, result: InvestigationResult) -> InvestigationResult:
        """Digest ONE result to the ``.2`` lane; a same-``dedup_key`` retry is an idempotent no-op.

        Idempotent by design: if this result's :meth:`InvestigationResult.key` was already recorded
        (in memory or on a reloaded lane log), nothing is written and the result is returned
        untouched (``digested`` stays ``False``). Otherwise it OFFLOADS the raw payload to a
        ``log_ref`` file, writes the slipbox-card RESULT that references it, and appends the ACTION
        marker to the ``.2/log_<worker>`` lane — all atomically. Sets ``log_ref`` / ``card_ref`` /
        ``digested`` on the returned result. Never touches the ``.3`` branch.
        """
        with self._lock:
            self._ensure_loaded()
            key = result.key()
            if key in self._seen:
                return result  # idempotent no-op — a retried digest with the same dedup_key

            slug = _slug(key)
            # 1. OFFLOAD the raw payload to a log_ref file (never inlined into the card).
            raw_path = self._dir / _RAW_DIR / f"{slug}.json"
            raw_payload = {
                "hypothesis_id": result.hypothesis_id,
                "action": result.action,
                "ok": result.ok,
                "outcome": _jsonable(result.outcome),
                "error": result.error,
            }
            self._atomic_write_json(raw_path, raw_payload)
            log_ref = f"{_RAW_DIR}/{slug}.json"

            # 2. Write the slipbox-card RESULT note (references the log_ref; raw stays offloaded).
            card_path = self._dir / _CARDS_DIR / f"{slug}.md"
            self._atomic_write_text(card_path, self._render_card(result, log_ref, key))
            card_ref = f"{_CARDS_DIR}/{slug}.md"

            # 3. Append the ACTION marker to this worker's ``.2/log_<k>`` lane (deduped on the key).
            marker = {
                "kind": _KIND_ACTION,
                "id": result.hypothesis_id,
                "action": result.action,
                "dedup_key": key,
                "ok": result.ok,
                "log_ref": log_ref,
                "card_ref": card_ref,
                "content_hash": content_hash(raw_payload),
            }
            self._append_marker(result.worker, marker)

            self._seen[key] = self._lane_file(result.worker).name
            result.log_ref = log_ref
            result.card_ref = card_ref
            result.digested = True
            return result

    # -- reads (for inspection / assertions) --------------------------------
    def markers(self) -> List[dict]:
        """Every ACTION marker across all worker-log lanes, in ``(worker, seq)`` order."""
        with self._lock:
            self._ensure_loaded()
            out: List[dict] = []
            if not self._dir.exists():
                return out
            for log in sorted(self._dir.glob(f"{_LOG_PREFIX}*.jsonl")):
                for line in log.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            return out

    def seen_keys(self) -> List[str]:
        """The dedup keys already digested (the idempotency set), sorted."""
        with self._lock:
            self._ensure_loaded()
            return sorted(self._seen)

    # -- persistence helpers ------------------------------------------------
    def _lane_file(self, worker: int) -> Path:
        return self._dir / f"{_LOG_PREFIX}{worker}.jsonl"

    def _append_marker(self, worker: int, marker: dict) -> None:
        """Append one ACTION marker line to a worker's lane log (atomic full rewrite)."""
        path = self._lane_file(worker)
        existing = ""
        if path.exists():
            existing = path.read_text(encoding="utf-8")
            if existing and not existing.endswith("\n"):
                existing += "\n"
        text = existing + json.dumps(marker, sort_keys=True) + "\n"
        self._atomic_write_text(path, text)

    def _atomic_write_text(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        FileVaultStateStore._atomic_write(path, text)

    def _atomic_write_json(self, path: Path, obj: dict) -> None:
        self._atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True))

    def _render_card(self, result: InvestigationResult, log_ref: str, key: str) -> str:
        """Render a compact slipbox-card RESULT — the raw payload stays OFFLOADED at ``log_ref``."""
        status = "ok" if result.ok else "FAILED"
        lines = [
            "---",
            'tags: ["resource", "concursus", "inner_graph", "worker_log"]',
            f"building_block: {'empirical_observation' if result.ok else 'counter_argument'}",
            f"hypothesis_id: {json.dumps(result.hypothesis_id)}",
            f"action: {json.dumps(result.action)}",
            f"dedup_key: {json.dumps(key)}",
            f"ok: {json.dumps(result.ok)}",
            f"log_ref: {json.dumps(log_ref)}",
            "access_control_group: [\"general\"]",
            "---",
            "",
            f"# Inner-Graph Result: {result.hypothesis_id} ({status})",
            "",
            "A `.2` worker-log RESULT card for one hypothesis investigation. The raw investigator "
            f"payload is OFFLOADED (not inlined) to [`{log_ref}`]({log_ref}); this card is the "
            "lean, greppable index over it. This lane NEVER writes a `.3` verdict — closing a "
            "hypothesis is the engine's job via `HypothesisTrail.write_verdict`.",
            "",
            "## Summary",
            "",
            f"- **Hypothesis**: `{result.hypothesis_id}`",
            f"- **Action**: `{result.action}`",
            f"- **Outcome**: {status}",
        ]
        if not result.ok:
            lines.append(f"- **Error**: {result.error}")
        lines += [
            f"- **Raw payload**: [`{log_ref}`]({log_ref})",
            "",
        ]
        return "\n".join(lines)

    def _ensure_loaded(self) -> None:
        """Reload the dedup set from the existing lane logs so idempotency survives a restart."""
        if self._loaded:
            return
        self._loaded = True
        if not self._dir.exists():
            return
        for log in sorted(self._dir.glob(f"{_LOG_PREFIX}*.jsonl")):
            try:
                for line in log.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    dk = rec.get("dedup_key")
                    if dk:
                        self._seen[dk] = log.name
            except OSError:
                continue


# ------------------------------------------------------------------ helpers
def _jsonable(value: object) -> object:
    """Best-effort JSON-safe view of an investigator outcome for the offloaded raw payload."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)
