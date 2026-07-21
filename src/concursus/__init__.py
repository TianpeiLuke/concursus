"""Concursus — substrate of the OPC (one-person-company) operating model: persistent, governed agent crews run programs end-to-end on AWS Bedrock AgentCore with the human as *director*, not operator.

Where **cursus** compiles a pipeline DAG + configs into a SageMaker pipeline, **Concursus**
(Latin *"a running-together / convergence"*) compiles an ``AgentDAG`` + per-agent
``.agent.yaml`` manifests into (1) an AgentCore provisioning plan — one ``CreateAgentRuntime``
per agent — and (2) a supervisor that dispatches the agents in topological order, wires each
agent's declared output into its dependents' input, and routes shared state through AgentCore
Memory. The compiler is *one organ* — the coordinator AgentCore deliberately does not ship —
and it stays a pure compiler (assemble = a value; one static forward pass over a frozen plan).
Around it stand the OPC's other organs, and the plan/execute discipline is not a refusal to
govern: it is *how* Concursus governs at OPC scale, safely and auditably.

Status: early. This release provides the declarative core — the backend-agnostic
:class:`~concursus.dag.AgentDAG` and the :class:`~concursus.manifest.AgentManifest`
(``.agent.yaml``) model — plus the compiler organ: the dependency resolver
(:mod:`~concursus.resolve`), the runtime builder (:mod:`~concursus.build`), the
:class:`~concursus.assemble.OrchestrationAssembler` (DAG + manifests → a frozen
:class:`~concursus.assemble.ProvisioningPlan`), and the topological
:class:`~concursus.supervisor.Supervisor`. Around the compiler stand the runtime-governance,
standing-crew, director, and deliberation organs: fleet memory (:mod:`~concursus.state`);
the runtime governor and per-episode standing crews (:mod:`~concursus.governor` — a
strictly-outer bounded loop that never reaches inside a running Supervisor nor mutates a
frozen plan, plus the director cockpit); and the bounded deliberation tier
(:mod:`~concursus.reasoning` — terminates, forming a fresh frozen plan strictly *before*
assemble, then hands off to the static topo walk; re-opening a trail is a new episode, never
a live-plan mutation). Provisioning + invocation over AWS stay behind
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
from .core.manifest import (
    MAX_SUPPORTED_CONTRACT_VERSION,
    AgentCapabilities,
    AgentManifest,
    ManifestError,
)
from .build.ledger import DeployLedger, DeployRow
from .build.provision import Clients, ProvisionError, provision_agent, provision_plan
from .core.resolve import (
    AgentRef,
    AlignmentError,
    check_alignment,
    resolve_context_mode,
    resolve_edges,
)
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
    RUN_EVENT_KINDS,
    InProcessStateStore,
    MemoryStateStore,
    Record,
    RunEvent,
    RunEventContractError,
    RunEventKind,
    StateStore,
    check_run_event_alignment,
    content_hash,
)
from .execute.supervisor import (
    PlanIdentityError,
    SchemaError,
    Supervisor,
    plan_fingerprint,
)
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
from .governor.state import GovernorState
from .governor.cockpit import DirectorCockpit
from .governor.scope import (
    SCOPE_LEVELS,
    SCOPE_SEP,
    ScopeAddress,
    ScopeError,
    build_programs_index,
    director_leverage_view,
    programs_dir,
    render_programs_index,
)
from .governor.registry import AgentRegistry, AgentVersion, RegistryError
from .governor.scheduler import (
    DISPATCH,
    ESCALATE,
    UNMATCHED,
    FrontierProposal,
    ScheduleDecision,
    SchedulerError,
    TrustLadderScheduler,
)
from .governor.loop import (
    GOV_NODES,
    CheckpointStore,
    EventSink,
    GovernorLoop,
    GovernorLoopError,
    GovernorResult,
    InProcessCheckpointStore,
    NullEventSink,
)
from .governor.ktlo import (
    KTLO,
    LAUNCH,
    TRIAGE_CLOSE,
    TRIAGE_ESCALATE,
    TRIAGE_INVESTIGATE,
    DetectionMode,
    EpisodeAdmissionGate,
    EventSource,
    FireBudgetGate,
    InProcessEventQueue,
    KTLODaemon,
    KTLODaemonError,
    KTLOResult,
    ProvenanceGuard,
    ScriptedEventSource,
)

__all__ = [
    "AgentDAG",
    "DAGError",
    "AgentManifest",
    "ManifestError",
    "AgentCapabilities",
    "MAX_SUPPORTED_CONTRACT_VERSION",
    "AgentRef",
    "AlignmentError",
    "resolve_edges",
    "resolve_context_mode",
    "check_alignment",
    "RuntimeBuilderFactory",
    "BuildPlanEntry",
    "OrchestrationAssembler",
    "DirectorCockpit",
    "ScopeAddress",
    "ScopeError",
    "SCOPE_LEVELS",
    "SCOPE_SEP",
    "build_programs_index",
    "programs_dir",
    "render_programs_index",
    "director_leverage_view",
    "AgentRegistry",
    "AgentVersion",
    "RegistryError",
    "TrustLadderScheduler",
    "ScheduleDecision",
    "FrontierProposal",
    "SchedulerError",
    "DISPATCH",
    "ESCALATE",
    "UNMATCHED",
    "ProvisioningPlan",
    "AssemblyError",
    "MonotonicityError",
    "plan_from_goal",
    "PlanAuthorError",
    "Supervisor",
    "SchemaError",
    "PlanIdentityError",
    "plan_fingerprint",
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
    "RunEvent",
    "RunEventKind",
    "RUN_EVENT_KINDS",
    "RunEventContractError",
    "check_run_event_alignment",
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
    "GovernorState",
    "GovernorLoop",
    "GovernorLoopError",
    "GovernorResult",
    "CheckpointStore",
    "InProcessCheckpointStore",
    "EventSink",
    "NullEventSink",
    "GOV_NODES",
    "KTLODaemon",
    "KTLODaemonError",
    "KTLOResult",
    "EventSource",
    "InProcessEventQueue",
    "ScriptedEventSource",
    "LAUNCH",
    "KTLO",
    "TRIAGE_CLOSE",
    "TRIAGE_INVESTIGATE",
    "TRIAGE_ESCALATE",
    "FireBudgetGate",
    "ProvenanceGuard",
    "EpisodeAdmissionGate",
    "DetectionMode",
    "__version__",
]
