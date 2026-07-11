"""Governor: the OUTER cyclic driver layer around the concursus compiler.

concursus proper is a COMPILER: ``assemble()``/``recompile()`` turn a DAG +
manifests into a frozen :class:`ProvisioningPlan` VALUE, and the ``Supervisor``
executes that plan in a single static forward pass.  The governor is a NEW,
strictly-outer layer that runs a bounded cycle *around* the compiler: each
round it forms a fresh frozen plan at the compiler front and dispatches a new
bounded episode.  It never reaches inside a running Supervisor, never mutates a
frozen plan, and never turns the compiler into a runtime governor.

This package intentionally holds only outer-loop machinery.  The persistent
outer-loop state lives in :mod:`concursus.governor.state`.
"""

from concursus.governor.ktlo import (
    KTLO,
    LAUNCH,
    TRIAGE_CLOSE,
    TRIAGE_ESCALATE,
    TRIAGE_INVESTIGATE,
    EventSource,
    InProcessEventQueue,
    KTLODaemon,
    KTLODaemonError,
    KTLOResult,
    ScriptedEventSource,
)
from concursus.governor.loop import (
    GOV_NODES,
    CheckpointStore,
    GovernorLoop,
    GovernorLoopError,
    GovernorResult,
    InProcessCheckpointStore,
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
    TrustLadderScheduler,
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
]
