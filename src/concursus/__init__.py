"""Concursus — compile a DAG of subagents into an orchestrated team on AWS Bedrock AgentCore.

Where **cursus** compiles a pipeline DAG + configs into a SageMaker pipeline, **Concursus**
(Latin *"a running-together / convergence"*) compiles an ``AgentDAG`` + per-agent
``.agent.yaml`` manifests into (1) an AgentCore provisioning plan — one ``CreateAgentRuntime``
per agent — and (2) a supervisor that dispatches the agents in topological order, wires each
agent's declared output into its dependents' input, and routes shared state through AgentCore
Memory. It is the coordinator AgentCore deliberately does not ship.

Status: early. This release provides the declarative core — the backend-agnostic
:class:`~concursus.dag.AgentDAG` and the :class:`~concursus.manifest.AgentManifest`
(``.agent.yaml``) model — plus the offline compiler: the dependency resolver
(:mod:`~concursus.resolve`), the runtime builder (:mod:`~concursus.build`), the
:class:`~concursus.assemble.OrchestrationAssembler` (DAG + manifests → a
:class:`~concursus.assemble.ProvisioningPlan`), and the topological
:class:`~concursus.supervisor.Supervisor`. Provisioning + invocation over AWS stay behind
the optional ``[agentcore]`` extra (boto3 is imported lazily).

Basic usage:
    >>> from concursus import AgentDAG
    >>> dag = AgentDAG()
    >>> for n in ["ingest", "summarize", "critique", "format"]:
    ...     dag.add_node(n)
    >>> dag.add_edge("ingest", "summarize").add_edge("summarize", "critique")
    >>> dag.add_edge("critique", "format").topological_sort()
    ['ingest', 'summarize', 'critique', 'format']
"""

from __future__ import annotations


def _resolve_version() -> str:
    """Resolve the version: a VERSION file (dev source of truth) wins over installed metadata."""
    from pathlib import Path

    version = "0.0.0"
    try:
        from importlib.metadata import version as _dist_version

        version = _dist_version("concursus")
    except Exception:  # pragma: no cover - not installed / odd checkout
        pass
    _v_file = Path(__file__).resolve().parent.parent.parent / "VERSION"
    if _v_file.exists():
        try:
            text = _v_file.read_text().strip()
            if text:
                version = text
        except OSError:
            pass
    return version


__version__ = _resolve_version()

from .assemble.assemble import (
    AssemblyError,
    MonotonicityError,
    OrchestrationAssembler,
    ProvisioningPlan,
)
from .build.build import BuildPlanEntry, RuntimeBuilderFactory
from .core.dag import AgentDAG, DAGError
from .state.filevault import (
    FileVaultStateStore,
    capture_agent_log_note,
    capture_agent_response_note,
    capture_run_output_note,
    capture_run_plan_note,
)
from .core.manifest import AgentManifest, ManifestError
from .build.ledger import DeployLedger, DeployRow
from .build.provision import Clients, ProvisionError, provision_agent, provision_plan
from .core.resolve import AgentRef, AlignmentError, check_alignment, resolve_edges
from .build.trust import GateDecision, TrustGrade, evaluate_deploy_gate
from .state.distill import (
    build_precedent_payload,
    distill_run,
    distill_store,
    load_precedents,
    render_precedent_hub,
)
from .assemble.planner import PlanAuthorError, plan_from_goal
from .state.precedent import PrecedentRetriever, RetrievedPrecedent
from .state.rundb import build_precedent_db, build_run_db
from .state.rungraph import RunGraph, RunGraphError
from .state.runindex import PrecedentIndex, RunIndex
from .state.statestore import (
    InProcessStateStore,
    MemoryStateStore,
    Record,
    StateStore,
    content_hash,
)
from .execute.supervisor import SchemaError, Supervisor
from .reasoning.trailstore import (
    Hypothesis,
    HypothesisTrail,
    ThreadNotResolved,
    TrailStoreError,
    drive_deliberation,
    require_resolved,
)
from .reasoning.dks_engine import (
    BAND_ARGUE_COUNTER,
    BAND_AUTO_ACCEPT,
    BAND_ESCALATE,
    CCSWeights,
    DKSEngine,
    DKSEngineError,
    DKSResult,
    DKSState,
    compute_ccs,
    route_by_confidence,
)
from .reasoning.inner_graph import (
    InnerGraph,
    InnerGraphDigest,
    InnerGraphError,
    InvestigationResult,
    compile_inner_graph,
    dispatch_frontier,
    partition_frontier,
)
from .reasoning.deliberate import form_plan, lower_to_dag, seed

__all__ = [
    "AgentDAG",
    "DAGError",
    "AgentManifest",
    "ManifestError",
    "AgentRef",
    "AlignmentError",
    "resolve_edges",
    "check_alignment",
    "RuntimeBuilderFactory",
    "BuildPlanEntry",
    "OrchestrationAssembler",
    "ProvisioningPlan",
    "AssemblyError",
    "MonotonicityError",
    "plan_from_goal",
    "PlanAuthorError",
    "Supervisor",
    "SchemaError",
    "HypothesisTrail",
    "Hypothesis",
    "TrailStoreError",
    "ThreadNotResolved",
    "require_resolved",
    "drive_deliberation",
    "DKSEngine",
    "DKSEngineError",
    "DKSState",
    "DKSResult",
    "CCSWeights",
    "compute_ccs",
    "route_by_confidence",
    "BAND_AUTO_ACCEPT",
    "BAND_ARGUE_COUNTER",
    "BAND_ESCALATE",
    "InnerGraph",
    "InnerGraphDigest",
    "InnerGraphError",
    "InvestigationResult",
    "compile_inner_graph",
    "seed",
    "lower_to_dag",
    "form_plan",
    "dispatch_frontier",
    "partition_frontier",
    "StateStore",
    "InProcessStateStore",
    "MemoryStateStore",
    "FileVaultStateStore",
    "capture_run_plan_note",
    "capture_agent_response_note",
    "capture_agent_log_note",
    "capture_run_output_note",
    "Record",
    "content_hash",
    "RunGraph",
    "RunGraphError",
    "RunIndex",
    "PrecedentIndex",
    "PrecedentRetriever",
    "RetrievedPrecedent",
    "build_run_db",
    "build_precedent_db",
    "distill_run",
    "distill_store",
    "build_precedent_payload",
    "render_precedent_hub",
    "load_precedents",
    "provision_plan",
    "provision_agent",
    "Clients",
    "ProvisionError",
    "TrustGrade",
    "GateDecision",
    "evaluate_deploy_gate",
    "DeployLedger",
    "DeployRow",
    "__version__",
]
