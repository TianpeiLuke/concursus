"""The **assembler** — compile an ``AgentDAG`` + manifests into a provisioning plan.

This is the offline convergence point of the core: it validates the topology and every
manifest, type-gates the declared ``depends_on`` edges (:func:`concursus.resolve.check_alignment`),
compiles each edge into :class:`~concursus.resolve.AgentRef` wiring, synthesizes one
:class:`~concursus.build.BuildPlanEntry` per node, and orders the nodes for dispatch. The
resulting :class:`ProvisioningPlan` is a pure, JSON-serializable preview (a ``concursus plan``)
— no AWS is touched here; deploy + the supervisor consume the plan downstream.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Set

from . import resolve
from .build import BuildPlanEntry, RuntimeBuilderFactory
from .resolve import AgentRef

if TYPE_CHECKING:  # pragma: no cover - hints only; keeps the runtime import graph pure
    from .dag import AgentDAG
    from .manifest import AgentManifest
    from .precedent import PrecedentRetriever

#: Default ceiling on monotonic re-compiles (AI-20). The plan-generation feedback edge lives
#: AROUND ``assemble`` (run -> distill -> precedent -> next compile); this cap makes that outer
#: loop BOUNDED so a mis-behaving planner can never re-compile without end.
DEFAULT_MAX_REVISIONS = 16


class AssemblyError(ValueError):
    """Raised when a DAG/manifest set cannot be compiled into a provisioning plan."""


class MonotonicityError(AssemblyError):
    """Raised when a re-compile would edit, remove, or reorder an already-executed node.

    The adaptive-compiler contract (AI-20): every plan mutation is a BOUNDED, MONOTONIC
    re-compile that emits a fully-frozen SUPERSET plan pinning already-executed nodes; an edit
    to (or removal / reordering of) a node the supervisor has already run is rejected here rather
    than silently replayed differently — resume must stay a faithful replay of the prior plan.
    """


@dataclass
class ProvisioningPlan:
    """The compiled orchestration plan for one agent team.

    Attributes:
        order: A valid dispatch order (topological sort of the DAG).
        entries: ``{node_id: BuildPlanEntry}`` — the packaging + ``create_agent_runtime``
            params for each agent.
        wiring: ``{node_id: [AgentRef, ...]}`` — resolved producer→consumer data edges.
    """

    order: List[str] = field(default_factory=list)
    entries: Dict[str, BuildPlanEntry] = field(default_factory=dict)
    wiring: Dict[str, List[AgentRef]] = field(default_factory=dict)
    #: Read-only cross-run precedent context (AI-17), surfaced for the plan author (AI-22) to
    #: consult. Empty by default. This NEVER participates in the compiled topology — ``order`` /
    #: ``entries`` / ``wiring`` are computed identically whether or not it is populated; it is
    #: pure advisory context attached alongside the frozen plan.
    precedents: List[dict] = field(default_factory=list)
    #: Monotonic re-compile counter (AI-20). ``0`` for a first ``assemble``; each
    #: :meth:`OrchestrationAssembler.recompile` emits a FRESH plan with ``revision`` one higher
    #: than the prior plan, bounded by ``max_revisions``. Surfaced in :meth:`to_dict` ONLY when
    #: non-zero, so a first-compile plan's preview is byte-for-byte unchanged.
    revision: int = 0

    def to_dict(self) -> dict:
        """Render the plan as a JSON-serializable dict (for a ``concursus plan`` preview).

        The compiled topology (``order`` / ``entries`` / ``wiring``) is always present; the
        read-only ``precedents`` field is emitted ONLY when non-empty, so a plan compiled with no
        retriever is byte-for-byte unchanged.
        """
        out = {
            "order": list(self.order),
            "entries": {name: entry.to_dict() for name, entry in self.entries.items()},
            "wiring": {
                node: [asdict(ref) for ref in refs] for node, refs in self.wiring.items()
            },
        }
        if self.precedents:
            out["precedents"] = [dict(p) for p in self.precedents]
        if self.revision:
            out["revision"] = self.revision
        return out

    def to_summary_dict(self) -> dict:
        """A COMPACT, navigable projection of the plan for a durable plan note (AI-18).

        Unlike :meth:`to_dict` (the full, byte-exact ``concursus plan`` preview that inlines each
        :class:`~concursus.build.BuildPlanEntry`'s ``wrapper`` source, ``dockerfile``, and
        ``create_agent_runtime`` request — potentially megabytes), this DROPS those bulky deploy
        payloads, keeping only a per-node **hosting digest** (``build_mode`` / ``protocol`` /
        ``port`` / ``fingerprint`` / ``ecr_repo`` + whether a wrapper/dockerfile was synthesized).
        The compiled topology (``order`` and the resolved ``wiring`` as ``producer→consumer``
        edges) is preserved verbatim so a note can render the DAG. This is a read-only projection
        of the *frozen* plan — it influences no dispatch and mutates nothing.
        """
        return {
            "order": list(self.order),
            "wiring": {
                node: [
                    {"producer": ref.producer, "path": ref.path, "input_name": ref.input_name}
                    for ref in refs
                ]
                for node, refs in self.wiring.items()
            },
            "entries": {
                name: {
                    "build_mode": entry.build_mode,
                    "protocol": entry.invoke.get("protocol"),
                    "port": entry.invoke.get("port"),
                    "fingerprint": entry.fingerprint,
                    "ecr_repo": entry.ecr_repo,
                    "has_wrapper": entry.wrapper is not None,
                    "has_dockerfile": entry.dockerfile is not None,
                }
                for name, entry in self.entries.items()
            },
        }


class OrchestrationAssembler:
    """Compile an :class:`~concursus.dag.AgentDAG` + manifests into a :class:`ProvisioningPlan`.

    The assembler is pure and offline: given the topology and per-agent manifests it validates
    everything, resolves the wiring, and synthesizes the build/deploy entries — it never imports
    boto3 or calls AWS. ``account``/``region`` are threaded into the synthesized IAM roles so the
    plan is previewable ahead of a real deploy.
    """

    def __init__(
        self,
        *,
        account: Optional[str] = None,
        region: Optional[str] = None,
        precedent_retriever: Optional["PrecedentRetriever"] = None,
    ) -> None:
        self.account = account
        self.region = region
        #: Optional COMPILE-TIME, read-only precedent retriever (AI-17). When supplied, ``assemble``
        #: retrieves relevant prior resolved runs BEFORE freezing and attaches them to the plan as
        #: advisory context for the plan author (AI-22). It NEVER changes the compiled topology and
        #: NEVER touches AWS or a run log. Default ``None`` keeps ``assemble`` byte-for-byte
        #: unchanged (the feedback edge lives AROUND assemble, never inside ``Supervisor.run``).
        self.precedent_retriever = precedent_retriever

    def assemble(
        self, dag: "AgentDAG", manifests: Dict[str, "AgentManifest"]
    ) -> ProvisioningPlan:
        """Validate, align, wire, and synthesize — returning the full provisioning plan.

        Steps: (1) validate the DAG; (2) validate each manifest; (3) type-gate the
        ``depends_on`` edges via :func:`concursus.resolve.check_alignment`; (4) compile the
        wiring via :func:`concursus.resolve.resolve_edges`; (5) synthesize a
        :class:`~concursus.build.BuildPlanEntry` per node; (6) order the nodes by
        topological sort. Raises :class:`AssemblyError` if a node has no manifest, and
        propagates the underlying validation/alignment errors otherwise.
        """
        dag.validate()
        for manifest in manifests.values():
            manifest.validate()
        resolve.check_alignment(dag, manifests)
        wiring = resolve.resolve_edges(dag, manifests)

        entries: Dict[str, BuildPlanEntry] = {}
        for node in dag.nodes:
            manifest = manifests.get(node)
            if manifest is None:
                raise AssemblyError(f"DAG node {node!r} has no manifest to provision")
            entries[node] = RuntimeBuilderFactory.synthesize(
                manifest, account=self.account, region=self.region
            )

        order = dag.topological_sort()

        # AI-17 hook: retrieve read-only precedent context BEFORE freezing, purely as advisory
        # input for the plan author. This is computed AFTER the topology is fully resolved and does
        # NOT influence ``order`` / ``entries`` / ``wiring`` in any way — the compiled plan is
        # identical to one produced without a retriever. Default (no retriever) => empty list, so
        # the plan (and its ``to_dict``) is byte-for-byte unchanged.
        precedents: List[dict] = []
        if self.precedent_retriever is not None:
            precedents = [
                p.to_dict()
                for p in self.precedent_retriever.retrieve(nodes=order)
            ]

        return ProvisioningPlan(
            order=order, entries=entries, wiring=wiring, precedents=precedents
        )

    # -- AI-20: bounded, monotonic re-compile -------------------------------
    def recompile(
        self,
        prior_plan: ProvisioningPlan,
        *,
        completed: Set[str],
        content_hashes: Optional[Mapping[str, str]] = None,
        dag: Optional["AgentDAG"] = None,
        manifests: Optional[Dict[str, "AgentManifest"]] = None,
        max_revisions: int = DEFAULT_MAX_REVISIONS,
    ) -> ProvisioningPlan:
        """Emit a FRESH, FROZEN, MONOTONIC-SUPERSET plan superseding ``prior_plan`` (AI-20).

        This is the ONLY sanctioned plan mutation: the plan-generation feedback edge lives AROUND
        the compiler (run -> distill -> precedent -> next compile), never inside
        :meth:`~concursus.supervisor.Supervisor.run`. It re-compiles ``dag`` + ``manifests`` into a
        brand-new plan (a fresh ``assemble``), then guarantees monotonicity against the prior plan:

        (a) **Pins** every already-executed node (present in ``completed``) to its PRIOR
            ``entries``/``wiring`` entry, so a resumed run replays those nodes byte-identically.
        (b) A :meth:`_check_monotonic` guard RAISES :class:`MonotonicityError` if the re-compile
            would edit, remove, or reorder an already-executed node — or drop / reorder any
            already-planned node (the prior ``order`` must survive as a subsequence).
        (c) Is **bounded**: it refuses once the running ``revision`` would exceed ``max_revisions``.

        The returned plan is a NEW frozen object with ``revision = prior_plan.revision + 1``; the
        prior plan (and any running supervisor over it) is never mutated. ``content_hashes`` (an
        optional ``{node: output content_hash}`` snapshot from the durable store) is accepted as
        read-only provenance of what was executed; it does not relax the guard.

        Args:
            prior_plan: The frozen plan the current run replayed; its ``order``/``entries``/
                ``wiring`` are the monotonic floor.
            completed: The already-executed node ids (e.g. ``state_store.completed()``) — pinned.
            content_hashes: Optional ``{node: content_hash}`` provenance for the executed outputs.
            dag: The (possibly extended) topology to re-compile. Required.
            manifests: The manifests to re-compile. Required.
            max_revisions: The revision ceiling (default :data:`DEFAULT_MAX_REVISIONS`).

        Raises:
            MonotonicityError: on a non-monotonic edit or once the revision cap is exceeded.
            AssemblyError: if ``dag`` / ``manifests`` are missing, or the fresh compile fails.
        """
        revision = int(prior_plan.revision) + 1
        if revision > max_revisions:
            raise MonotonicityError(
                f"re-compile refused: revision {revision} exceeds max_revisions={max_revisions}; "
                "the adaptive-compile loop is bounded — raise max_revisions to allow more passes"
            )
        if dag is None or manifests is None:
            raise AssemblyError(
                "recompile requires dag= and manifests= (the re-authored topology to compile)"
            )

        completed_set: Set[str] = set(completed)
        fresh = self.assemble(dag, manifests)
        self._check_monotonic(prior_plan, fresh, completed=completed_set)

        # Pin executed nodes to their PRIOR entry/wiring (identical after the guard, but make the
        # pin explicit so the frozen executed slice is provably the prior plan's, not a re-derived
        # look-alike). Newly-added nodes take the freshly-compiled entry/wiring.
        entries: Dict[str, BuildPlanEntry] = dict(fresh.entries)
        wiring: Dict[str, List[AgentRef]] = dict(fresh.wiring)
        for node in completed_set:
            if node in prior_plan.entries:
                entries[node] = prior_plan.entries[node]
            if node in prior_plan.wiring:
                wiring[node] = list(prior_plan.wiring[node])

        return ProvisioningPlan(
            order=fresh.order,
            entries=entries,
            wiring=wiring,
            precedents=fresh.precedents,
            revision=revision,
        )

    @staticmethod
    def _check_monotonic(
        prior: ProvisioningPlan, new: ProvisioningPlan, *, completed: Set[str]
    ) -> None:
        """Assert ``new`` is a monotonic superset of ``prior`` that leaves executed nodes intact.

        Two invariants (see :class:`MonotonicityError`):

        1. **Prior order survives as a subsequence** — no already-planned node is dropped, and the
           prior nodes keep their exact relative order (new nodes may be interleaved). This forbids
           reordering any node before an already-planned peer.
        2. **Executed nodes are frozen** — for every node in ``completed``, its ``entries`` and
           ``wiring`` in ``new`` must equal ``prior`` verbatim (no edit) and it must still be
           present (no removal).
        """
        prior_order = list(prior.order)
        new_order = list(new.order)
        prior_set = set(prior_order)
        new_set = set(new_order)

        restricted = [n for n in new_order if n in prior_set]
        if restricted != prior_order:
            dropped = [n for n in prior_order if n not in new_set]
            if dropped:
                raise MonotonicityError(
                    f"re-compile drops already-planned node(s) {dropped}; a monotonic re-compile "
                    "may only ADD nodes, never remove them"
                )
            raise MonotonicityError(
                "re-compile reorders already-planned nodes; the prior dispatch order "
                f"{prior_order} must survive as a subsequence (got {restricted})"
            )

        for node in sorted(completed):
            if node not in new_set:
                raise MonotonicityError(
                    f"re-compile removes already-executed node {node!r}; executed nodes must be "
                    "pinned, never dropped"
                )
            if node in prior.entries and new.entries.get(node) != prior.entries.get(node):
                raise MonotonicityError(
                    f"re-compile edits already-executed node {node!r} (its BuildPlanEntry "
                    "changed); executed nodes are frozen — route a change through a NEW node"
                )
            if list(prior.wiring.get(node, [])) != list(new.wiring.get(node, [])):
                raise MonotonicityError(
                    f"re-compile rewires already-executed node {node!r}; executed nodes are "
                    "frozen — route a change through a NEW node"
                )
