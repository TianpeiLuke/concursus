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
from concursus.governor.state import GovernorState

__all__ = [
    "GovernorState",
    "AgentRegistry",
    "AgentVersion",
    "RegistryError",
    "GovernorLoop",
    "GovernorLoopError",
    "GovernorResult",
    "CheckpointStore",
    "InProcessCheckpointStore",
    "GOV_NODES",
]
