"""Governor: the runtime-governance organ of the concursus OPC substrate.

concursus is the substrate of the OPC (One-Person-Company) operating model — a
director-not-operator system of persistent, governed crews.  The compiler is
ONE organ: ``assemble()``/``recompile()`` turn a DAG + manifests into a frozen
:class:`ProvisioningPlan` VALUE, and the ``Supervisor`` executes that plan in a
single static forward pass.  The governor is the runtime-governance organ that
runs a bounded cycle *around* the compiler: each round it forms a fresh frozen
plan at the compiler front and dispatches a new bounded episode.  Governing at
OPC scale safely and auditably is exactly WHY this loop stays strictly outer —
it never reaches inside a running Supervisor, never mutates a frozen plan, and
never collapses the compiler into the loop.  Plan/execute is not a refusal to
govern; it is HOW the governor governs.

This package holds the outer-loop, standing-crew, and director machinery.  The
persistent outer-loop state lives in :mod:`concursus.governor.state`.
"""

from concursus.governor.ktlo import (
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
from concursus.governor.loop import (
    GOV_NODES,
    CheckpointStore,
    EventSink,
    GovernorLoop,
    GovernorLoopError,
    GovernorResult,
    InProcessCheckpointStore,
    NullEventSink,
)
from concursus.governor.registry import (
    AgentRegistry,
    AgentVersion,
    RegistryError,
)
from concursus.governor.scheduler import (
    DISPATCH,
    ESCALATE,
    UNMATCHED,
    FrontierProposal,
    ScheduleDecision,
    SchedulerError,
    Tier,
    TrustLadderScheduler,
    make_payload_tier,
    make_trust_strictness,
    manifest_is_programmatic,
    project_context,
)
from concursus.governor.scope import (
    SCOPE_LEVELS,
    SCOPE_SEP,
    ScopeAddress,
    ScopeError,
    build_programs_index,
    director_leverage_view,
    programs_dir,
    render_programs_index,
)
from concursus.governor.state import GovernorState

__all__ = [
    "GovernorState",
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
    "make_trust_strictness",
    "make_payload_tier",
    "manifest_is_programmatic",
    "project_context",
    "Tier",
    "ScheduleDecision",
    "FrontierProposal",
    "SchedulerError",
    "DISPATCH",
    "ESCALATE",
    "UNMATCHED",
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
]
