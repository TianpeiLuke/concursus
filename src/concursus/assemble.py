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
from typing import TYPE_CHECKING, Dict, List, Optional

from . import resolve
from .build import BuildPlanEntry, RuntimeBuilderFactory
from .resolve import AgentRef

if TYPE_CHECKING:  # pragma: no cover - hints only; keeps the runtime import graph pure
    from .dag import AgentDAG
    from .manifest import AgentManifest


class AssemblyError(ValueError):
    """Raised when a DAG/manifest set cannot be compiled into a provisioning plan."""


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

    def to_dict(self) -> dict:
        """Render the plan as a JSON-serializable dict (for a ``concursus plan`` preview)."""
        return {
            "order": list(self.order),
            "entries": {name: entry.to_dict() for name, entry in self.entries.items()},
            "wiring": {
                node: [asdict(ref) for ref in refs] for node, refs in self.wiring.items()
            },
        }


class OrchestrationAssembler:
    """Compile an :class:`~concursus.dag.AgentDAG` + manifests into a :class:`ProvisioningPlan`.

    The assembler is pure and offline: given the topology and per-agent manifests it validates
    everything, resolves the wiring, and synthesizes the build/deploy entries — it never imports
    boto3 or calls AWS. ``account``/``region`` are threaded into the synthesized IAM roles so the
    plan is previewable ahead of a real deploy.
    """

    def __init__(self, *, account: Optional[str] = None, region: Optional[str] = None) -> None:
        self.account = account
        self.region = region

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
        return ProvisioningPlan(order=order, entries=entries, wiring=wiring)
