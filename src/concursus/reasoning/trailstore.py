"""The **HypothesisTrail** — the durable ``.3`` reasoning-branch store (Phase 5, AI-23 + AI-26).

Concursus is a **compiler, not a runtime governor**. This module is the FAR-HORIZON reasoning
tier and therefore the highest identity risk, so its contract is deliberately narrow: everything
here belongs to **PLAN-FORMATION**, STRICTLY BEFORE :meth:`~concursus.assemble.OrchestrationAssembler.assemble`.
It records a *deliberation* — a tree of hypotheses fanned out under a run's ``.3`` reasoning
branch, each closed by a verdict — into a durable, replayable log. It **never** dispatches an
agent, and it is **NEVER** wired inside :meth:`~concursus.supervisor.Supervisor.run` (which stays
a single forward topo pass over a frozen ``plan.order``). The whole deliberation MUST TERMINATE:
:meth:`HypothesisTrail.open_frontier` returning ``[]`` (guarded by
:func:`require_resolved` / :class:`ThreadNotResolved`) is the convergence check a later LOWER
step (AI-30) requires before it may distill the debate into an immutable :class:`AgentDAG`.

Two layers, both pure stdlib:

* **AI-23 — hypothesis-branch API.** :meth:`~HypothesisTrail.fanout_root_hypotheses` seeds root
  hypotheses under ``.3``; :meth:`~HypothesisTrail.fanout_hypotheses` fans sharper children under
  one; :meth:`~HypothesisTrail.open_frontier` is the un-resolved leaves within the depth/confidence
  caps; :meth:`~HypothesisTrail.write_verdict` closes a hypothesis by appending a ``VERDICT`` child
  **and** flipping its ``RESOLVED`` marker in ONE atomic critical section (a scan never sees a
  verdict without its resolved marker); :meth:`~HypothesisTrail.hypotheses` reads the tree.
* **AI-26 — Dung grounded-semantics labels.** :meth:`~HypothesisTrail.attack` adds a
  contradicts/attacks edge; :meth:`~HypothesisTrail.compute_grounded_extension` is the
  least-fixed-point of the characteristic function over the attack graph (``in`` = unattacked or
  all attackers ``out``; ``out`` = attacked by an ``in``; ``undec`` = the rest);
  :meth:`~HypothesisTrail.arg_label` exposes one node's label. This layer is a **pure computation
  over the ``.3`` trail**: a later LOWER (AI-30) CONSUMES these labels, but AI-26 does NOT import
  or depend on AI-30 — the dependency is one-directional (no import cycle).

Durability reuses the FileVault machinery: addresses are materialized ``/``-paths like the run
addresses (:data:`~concursus.statestore._ADDR_SEP`), and every mutation is persisted as a lean
append-only **JSONL** log under ``<run_dir>/.3/`` rewritten atomically (temp + ``os.replace`` via
:meth:`~concursus.filevault.FileVaultStateStore._atomic_write`). A fresh trail over an existing
``.3`` reloads by replay, so a deliberation survives process exit — offline, no AWS, stdlib only.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Union

from ..state.filevault import FileVaultStateStore
from ..state.statestore import _ADDR_SEP, content_hash

# The Phase-5 reasoning branch under a run dir (the ``.3`` sibling of the ``.1``/``.2`` tiers).
_BRANCH = ".3"

# The lean append-only JSONL log holding the deliberation (one record per line). Rewritten wholesale
# and atomically on every mutation, so a verdict + its resolved marker land in ONE os.replace.
_TRAIL_LOG = "trail.jsonl"

# The three record kinds and the verdict vocabulary.
_KIND_HYPOTHESIS = "hypothesis"
_KIND_ATTACK = "attack"
_KIND_VERDICT = "verdict"
_KIND_RESOLVED = "resolved"

_VERDICTS = ("ACCEPT", "REJECT", "UNDEC")

# Dung grounded labels.
LABEL_IN = "in"
LABEL_OUT = "out"
LABEL_UNDEC = "undec"

# A candidate is either bare text or a ``{"text": ..., "confidence": ...}`` dict.
Candidate = Union[str, Dict[str, object]]


class TrailStoreError(ValueError):
    """Raised on an invalid trailstore operation (unknown id, bad verdict, self-attack)."""


class ThreadNotResolved(TrailStoreError):
    """Raised by :func:`require_resolved` when a deliberation has NOT converged — its
    :meth:`HypothesisTrail.open_frontier` is non-empty. A later LOWER step (AI-30) must guard on
    this before it may distill the debate into an immutable :class:`~concursus.dag.AgentDAG`: you
    may only lower from a CONVERGED debate, never a live one."""


@dataclass
class Hypothesis:
    """One node in the ``.3`` deliberation tree (a hypothesis; verdicts live as attributes on it).

    Attributes:
        id: The materialized-path address (``.3/h1``, ``.3/h1/c4`` …); parent = strip last segment.
        parent: The parent hypothesis id (``None`` for a root seeded directly under ``.3``).
        text: The hypothesis statement.
        confidence: A ``[0, 1]`` self-confidence; below ``confidence_floor`` keeps a leaf open.
        depth: Distance from its root (roots are depth ``0``).
        goal: The seeding goal (roots only; ``None`` for children).
        resolved: Whether a verdict has closed this hypothesis.
        verdict: ``ACCEPT`` | ``REJECT`` | ``UNDEC`` once resolved (else ``None``).
        evidence: The optional evidence dict recorded with the verdict.
        children: Ids of sharper child hypotheses fanned out under this one.
        attacks: Ids this hypothesis contradicts/attacks (the Dung edges out of this node).
        verdict_id: The addressable id of the appended ``VERDICT`` child (once resolved).
    """

    id: str
    parent: Optional[str]
    text: str
    confidence: float = 0.0
    depth: int = 0
    goal: Optional[str] = None
    resolved: bool = False
    verdict: Optional[str] = None
    evidence: Optional[dict] = None
    children: List[str] = field(default_factory=list)
    attacks: List[str] = field(default_factory=list)
    verdict_id: Optional[str] = None


class HypothesisTrail:
    """A durable, replayable deliberation over a run's ``.3`` reasoning branch (AI-23 + AI-26).

    Pure plan-formation: it records hypotheses/verdicts/attacks and computes grounded labels; it
    dispatches nothing and is never wired into :meth:`~concursus.supervisor.Supervisor.run`. Every
    mutation persists as an append-only JSONL record and rewrites ``<run_dir>/.3/trail.jsonl``
    atomically; a fresh trail over an existing branch reloads by replay. All bounded — fan-out is
    caller-bounded and :meth:`open_frontier` enforces ``depth_cap`` / ``confidence_floor``.
    """

    def __init__(self, run_dir: Union[str, Path], *, branch: str = _BRANCH) -> None:
        self._branch = branch
        self._dir = Path(run_dir) / branch
        self._log = self._dir / _TRAIL_LOG
        self._lock = threading.RLock()
        self._records: List[dict] = []
        self._counter = 0
        self._loaded = False
        self._loaded_mtime: Optional[float] = None

    @classmethod
    def from_config(
        cls, *, vault_path: Union[str, Path], session_id: str, branch: str = _BRANCH
    ) -> "HypothesisTrail":
        """Bind a deliberation to the SAME run dir a :class:`FileVaultStateStore` would use.

        Reuses :meth:`FileVaultStateStore.from_config`'s ``<vault>/runs/<slug(session_id)>/``
        addressing so a run's ``.1``/``.2`` state notes and its ``.3`` reasoning branch live under
        one directory. SEED (a fanout) is a NEW-goal/ticket action only — a retrieval query must
        never open this branch (retrieval-to-DKS is an anti-pattern).
        """
        store = FileVaultStateStore.from_config(vault_path=vault_path, session_id=session_id)
        return cls(store.run_dir, branch=branch)

    # -- identity accessors -------------------------------------------------
    @property
    def branch_dir(self) -> Path:
        """The on-disk ``.3`` reasoning-branch directory for this run."""
        return self._dir

    # ================================================================= AI-23
    def fanout_root_hypotheses(self, goal: str, candidates: Sequence[Candidate]) -> List[str]:
        """Seed root hypotheses (one per candidate) under the ``.3`` branch; return their ids.

        SEED is triggered by a NEW goal/ticket only — never by a retrieval query. Each candidate is
        bare text or ``{"text", "confidence"}``. Roots are depth ``0`` with parent ``None``.
        """
        with self._critical():
            self._ensure_loaded_locked()
            ids: List[str] = []
            for cand in candidates:
                text, conf = _normalize_candidate(cand)
                ids.append(self._append_hypothesis(parent=None, text=text, confidence=conf,
                                                    depth=0, goal=goal))
            self._flush_locked()
            return ids

    def fanout_hypotheses(self, parent_id: str, children: Sequence[Candidate]) -> List[str]:
        """Fan sharper child hypotheses under ``parent_id``; return their ids.

        Children are materialized under the parent's address (``parent/cN``) at ``depth + 1``. This
        is BOUNDED expansion: callers cap breadth, and :meth:`open_frontier`'s ``depth_cap`` caps
        depth — no unbounded growth.
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if parent_id not in model:
                raise TrailStoreError(f"unknown parent hypothesis: {parent_id!r}")
            parent = model[parent_id]
            ids: List[str] = []
            for cand in children:
                text, conf = _normalize_candidate(cand)
                ids.append(self._append_hypothesis(parent=parent_id, text=text, confidence=conf,
                                                   depth=parent.depth + 1, goal=None))
            self._flush_locked()
            return ids

    def open_frontier(
        self, root: str, *, depth_cap: int = 5, confidence_floor: float = 0.6
    ) -> List[str]:
        """The un-resolved leaf hypotheses in ``root``'s subtree, within the caps.

        A hypothesis is CLOSED (excluded) once it has a verdict/resolved marker, once it has fanned
        children (no longer a leaf), or once its depth EXCEEDS ``depth_cap``. A leaf whose
        ``confidence`` is at/above ``confidence_floor`` is confident enough and also excluded. The
        remainder is the open frontier; ``[]`` means the debate has converged (the termination
        guard — see :func:`require_resolved`).
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if root not in model:
                raise TrailStoreError(f"unknown root hypothesis: {root!r}")
            frontier: List[str] = []
            for hid in self._subtree_ids(model, root):
                h = model[hid]
                if h.resolved:
                    continue
                if h.children:  # not a leaf — an internal node is not itself open
                    continue
                if h.depth > depth_cap:  # exceeds the depth cap → closed
                    continue
                if h.confidence >= confidence_floor:  # confident enough → closed
                    continue
                frontier.append(hid)
            return sorted(frontier)

    def write_verdict(
        self, id: str, verdict: str, evidence: Optional[dict] = None
    ) -> str:
        """Close a hypothesis: append a ``VERDICT`` child AND flip its ``RESOLVED`` marker atomically.

        Both records are written in ONE critical section and persisted in ONE atomic file replace,
        so a concurrent scan/reload NEVER observes a verdict without its resolved marker (or vice
        versa). Returns the addressable id of the appended verdict child. ``verdict`` must be one of
        ``ACCEPT`` | ``REJECT`` | ``UNDEC``.
        """
        verdict = verdict.upper()
        if verdict not in _VERDICTS:
            raise TrailStoreError(f"verdict must be one of {_VERDICTS}, got {verdict!r}")
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if id not in model:
                raise TrailStoreError(f"unknown hypothesis: {id!r}")
            # Both records staged, then flushed together → one atomic os.replace.
            self._counter += 1
            seq = self._counter
            verdict_id = f"{id}{_ADDR_SEP}v{seq}"
            self._records.append({
                "seq": seq,
                "kind": _KIND_VERDICT,
                "id": verdict_id,
                "parent": id,
                "verdict": verdict,
                "evidence": evidence,
                "content_hash": content_hash({"verdict": verdict, "evidence": evidence or {}}),
            })
            self._records.append({
                "seq": self._next_seq(),
                "kind": _KIND_RESOLVED,
                "target": id,
            })
            self._flush_locked()
            return verdict_id

    def hypotheses(self, root: Optional[str] = None) -> Dict[str, Hypothesis]:
        """Read the current deliberation tree as ``{id: Hypothesis}``.

        With ``root=None`` returns every hypothesis; with a ``root`` id returns that node's subtree
        (inclusive). Verdicts appear as attributes (``resolved`` / ``verdict`` / ``evidence``) on
        their hypothesis, not as separate nodes.
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if root is None:
                return model
            if root not in model:
                raise TrailStoreError(f"unknown root hypothesis: {root!r}")
            return {hid: model[hid] for hid in self._subtree_ids(model, root)}

    # ================================================================= AI-26
    def attack(self, attacker_id: str, target_id: str) -> None:
        """Add a contradicts/attacks edge ``attacker_id -> target_id`` between two hypotheses.

        The Dung attack graph is INDEPENDENT of the parent/child tree: an attack can cross subtrees.
        Self-attacks are rejected. Persisted as a durable record so the edge survives replay.
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            for hid in (attacker_id, target_id):
                if hid not in model:
                    raise TrailStoreError(f"unknown hypothesis: {hid!r}")
            if attacker_id == target_id:
                raise TrailStoreError(f"self-attack not allowed: {attacker_id!r}")
            if target_id in model[attacker_id].attacks:
                return  # idempotent — the edge already exists
            self._records.append({
                "seq": self._next_seq(),
                "kind": _KIND_ATTACK,
                "src": attacker_id,
                "dst": target_id,
            })
            self._flush_locked()

    # A friendlier alias — a contradiction is a symmetric intent, but Dung attacks are directed, so
    # this stays directed (attacker contradicts target) to keep the semantics unambiguous.
    contradicts = attack

    def compute_grounded_extension(self, root: str) -> Dict[str, str]:
        """The Dung **grounded extension** labels over ``root``'s subtree: ``id -> in|out|undec``.

        The least fixed point of the characteristic function F(S) = {a : every attacker of a is
        attacked by some member of S}, computed by the standard grounded labelling: repeatedly label
        ``in`` any argument whose attackers are all ``out`` (vacuously true for the unattacked), and
        ``out`` any argument attacked by an ``in``; whatever never settles is ``undec``. Pure
        computation over the ``.3`` trail — a later LOWER (AI-30) consumes these labels, but this
        method neither imports nor depends on AI-30.
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if root not in model:
                raise TrailStoreError(f"unknown root hypothesis: {root!r}")
            nodes = set(self._subtree_ids(model, root))
            attackers = _attackers_within(model, nodes)
            return _grounded_labels(nodes, attackers)

    def arg_label(self, id: str) -> str:
        """The grounded label (``in`` | ``out`` | ``undec``) of a single hypothesis.

        Computes the grounded extension over the argumentation framework the hypothesis belongs to
        (its root's subtree) and returns its label.
        """
        with self._critical():
            self._ensure_loaded_locked()
            model = self._build_model()
            if id not in model:
                raise TrailStoreError(f"unknown hypothesis: {id!r}")
            root = self._root_of(model, id)
            return self.compute_grounded_extension(root)[id]

    # ================================================================= replay
    def _build_model(self) -> Dict[str, Hypothesis]:
        """Replay the append-only records into the ``{id: Hypothesis}`` tree (read model)."""
        model: Dict[str, Hypothesis] = {}
        for rec in sorted(self._records, key=lambda r: r.get("seq", 0)):
            kind = rec.get("kind")
            if kind == _KIND_HYPOTHESIS:
                h = Hypothesis(
                    id=rec["id"],
                    parent=rec.get("parent"),
                    text=rec.get("text", ""),
                    confidence=float(rec.get("confidence", 0.0)),
                    depth=int(rec.get("depth", 0)),
                    goal=rec.get("goal"),
                )
                model[h.id] = h
                parent = rec.get("parent")
                if parent and parent in model and h.id not in model[parent].children:
                    model[parent].children.append(h.id)
            elif kind == _KIND_ATTACK:
                src, dst = rec.get("src"), rec.get("dst")
                if src in model and dst not in model[src].attacks:
                    model[src].attacks.append(dst)
            elif kind == _KIND_VERDICT:
                parent = rec.get("parent")
                if parent in model:
                    model[parent].verdict = rec.get("verdict")
                    model[parent].evidence = rec.get("evidence")
                    model[parent].verdict_id = rec.get("id")
            elif kind == _KIND_RESOLVED:
                target = rec.get("target")
                if target in model:
                    model[target].resolved = True
        return model

    def _subtree_ids(self, model: Dict[str, Hypothesis], root: str) -> List[str]:
        """Ids in ``root``'s subtree (inclusive), pre-order over the parent/child tree."""
        out: List[str] = []
        stack = [root]
        while stack:
            hid = stack.pop()
            if hid not in model or hid in out:
                continue
            out.append(hid)
            stack.extend(reversed(model[hid].children))
        return out

    def _root_of(self, model: Dict[str, Hypothesis], id: str) -> str:
        """Walk parent links up to the root of ``id``'s tree."""
        cur = id
        seen: Set[str] = set()
        while True:
            parent = model[cur].parent
            if parent is None or parent not in model or parent in seen:
                return cur
            seen.add(cur)
            cur = parent

    # -- write helpers (must be called inside a critical section) -----------
    def _append_hypothesis(
        self, *, parent: Optional[str], text: str, confidence: float, depth: int,
        goal: Optional[str],
    ) -> str:
        self._counter += 1
        seq = self._counter
        if parent is None:
            hid = f"{self._branch}{_ADDR_SEP}h{seq}"
        else:
            hid = f"{parent}{_ADDR_SEP}c{seq}"
        self._records.append({
            "seq": seq,
            "kind": _KIND_HYPOTHESIS,
            "id": hid,
            "parent": parent,
            "text": text,
            "confidence": confidence,
            "depth": depth,
            "goal": goal,
            "content_hash": content_hash({"text": text, "parent": parent or ""}),
        })
        return hid

    def _next_seq(self) -> int:
        self._counter += 1
        return self._counter

    # -- persistence (atomic JSONL rewrite, replay-on-reload) ---------------
    def _flush_locked(self) -> None:
        """Rewrite the whole JSONL log atomically (temp + os.replace) so multi-record mutations
        (verdict + resolved) commit as ONE indivisible file swap."""
        self._dir.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(rec, sort_keys=True) for rec in
                 sorted(self._records, key=lambda r: r.get("seq", 0))]
        text = "\n".join(lines) + ("\n" if lines else "")
        FileVaultStateStore._atomic_write(self._log, text)
        try:
            self._loaded_mtime = self._log.stat().st_mtime
        except OSError:
            self._loaded_mtime = None

    def _load(self) -> None:
        """Replay the JSONL log from disk into the in-memory record list (resume = replay)."""
        records: List[dict] = []
        if self._log.exists():
            try:
                for line in self._log.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue  # skip a torn/partial line rather than abort the reload
            except OSError:
                records = []
        self._records = records
        self._counter = max((int(r.get("seq", 0)) for r in records), default=0)
        try:
            self._loaded_mtime = self._log.stat().st_mtime
        except OSError:
            self._loaded_mtime = None
        self._loaded = True

    def _ensure_loaded_locked(self) -> None:
        """Load on first use, and re-load if a peer rewrote the log since our last read (freshness)."""
        if not self._loaded:
            self._load()
            return
        try:
            mtime = self._log.stat().st_mtime
        except OSError:
            return
        if self._loaded_mtime is None or mtime != self._loaded_mtime:
            self._load()

    def _critical(self):
        return self._lock


# ------------------------------------------------------------------ helpers
def _normalize_candidate(cand: Candidate) -> tuple:
    """Normalize a candidate into ``(text, confidence)``."""
    if isinstance(cand, str):
        return cand, 0.0
    if isinstance(cand, dict):
        text = str(cand.get("text", cand.get("statement", "")))
        conf = float(cand.get("confidence", 0.0))
        return text, conf
    raise TrailStoreError(f"candidate must be a str or dict, got {type(cand).__name__}")


def _attackers_within(
    model: Dict[str, Hypothesis], nodes: Set[str]
) -> Dict[str, Set[str]]:
    """The in-restriction of the attack graph: ``{node: {attackers within nodes}}``."""
    attackers: Dict[str, Set[str]] = {n: set() for n in nodes}
    for src in nodes:
        for dst in model[src].attacks:
            if dst in nodes:
                attackers[dst].add(src)
    return attackers


def _grounded_labels(nodes: Set[str], attackers: Dict[str, Set[str]]) -> Dict[str, str]:
    """The Dung grounded labelling over ``nodes`` given ``{node: attackers}``.

    Least fixed point of the characteristic function: iterate labelling ``in`` any node whose
    attackers are all ``out`` (vacuously true when unattacked) and ``out`` any node with an ``in``
    attacker, until a pass changes nothing; unlabelled remainder is ``undec``.
    """
    label: Dict[str, str] = {}
    changed = True
    while changed:
        changed = False
        for n in nodes:
            if n in label:
                continue
            if all(label.get(a) == LABEL_OUT for a in attackers[n]):
                label[n] = LABEL_IN
                changed = True
        for n in nodes:
            if n in label:
                continue
            if any(label.get(a) == LABEL_IN for a in attackers[n]):
                label[n] = LABEL_OUT
                changed = True
    for n in nodes:
        label.setdefault(n, LABEL_UNDEC)
    return label


def require_resolved(
    trail: HypothesisTrail, root: str, *, depth_cap: int = 5, confidence_floor: float = 0.6
) -> None:
    """Assert a deliberation has CONVERGED — raise :class:`ThreadNotResolved` on an open frontier.

    The termination guard a later LOWER step (AI-30) must call BEFORE distilling the ``.3`` debate
    into an immutable :class:`~concursus.dag.AgentDAG`: you may only lower from a converged debate,
    never a live one. Re-opening the branch is a NEW formation episode, never mutation of a live
    plan. This helper is pure structure over the trail; it imports nothing from AI-30 (no cycle).
    """
    frontier = trail.open_frontier(root, depth_cap=depth_cap, confidence_floor=confidence_floor)
    if frontier:
        raise ThreadNotResolved(
            f"deliberation under {root!r} has not converged: {len(frontier)} open hypotheses "
            f"{frontier} — resolve them (write_verdict) before lowering to an AgentDAG"
        )


def drive_deliberation(
    trail: HypothesisTrail,
    root: str,
    investigator: Callable[[Hypothesis], object],
    *,
    max_rounds: int = 8,
    depth_cap: int = 5,
    confidence_floor: float = 0.6,
) -> int:
    """A BOUNDED, pure-Python deliberation driver over an INJECTED ``investigator`` seam.

    Plan-formation only — it runs STRICTLY BEFORE assemble and terminates: it repeatedly asks the
    injected ``investigator`` to resolve each open-frontier hypothesis until the frontier is empty
    OR ``max_rounds`` is spent (a hard budget — no unbounded expansion). ``investigator(h)`` returns
    either a verdict spec ``{"verdict": "ACCEPT|REJECT|UNDEC", "evidence": {...}}`` (which closes the
    hypothesis) or a list of child candidates (which fans sharper children). The seam is fully
    optional and needs NO LLM/LangGraph: a stub callable drives it in tests. Returns the round count.

    This is a fallback DRIVER, not a runtime governor — it never touches ``Supervisor.run``; the
    caller lowers the CONVERGED trail into an immutable AgentDAG afterwards (guarded by
    :func:`require_resolved`).
    """
    rounds = 0
    while rounds < max_rounds:
        frontier = trail.open_frontier(root, depth_cap=depth_cap, confidence_floor=confidence_floor)
        if not frontier:
            break
        rounds += 1
        model = trail.hypotheses(root)
        for hid in frontier:
            outcome = investigator(model[hid])
            if isinstance(outcome, dict) and "verdict" in outcome:
                trail.write_verdict(hid, outcome["verdict"], outcome.get("evidence"))
            elif outcome:  # a non-empty child-candidate list → fan sharper children
                trail.fanout_hypotheses(hid, list(outcome))
            else:  # nothing to do — close it UNDEC so the loop terminates (bounded)
                trail.write_verdict(hid, "UNDEC", {"reason": "investigator returned nothing"})
    return rounds
