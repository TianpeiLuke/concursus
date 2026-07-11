"""Tests for the governor's persistent outer-loop state.

The governor holds a SEQUENCE of frozen plan VALUEs + a log pointer, never a
mutable compiler plan.  A version bump must leave the prior frozen plan object
byte-identical (INV-3/INV-4).
"""

import copy

from concursus import (
    AgentDAG,
    AgentManifest,
    GovernorState,
    OrchestrationAssembler,
)
from concursus.governor import GovernorState as GovernorStateFromSubpkg
from concursus.state.statestore import InProcessStateStore

# Governor state is exported both from the top-level package and its subpackage.
assert GovernorState is GovernorStateFromSubpkg


def _manifest(name, *, inputs=None, depends_on=None):
    return AgentManifest.from_dict(
        {
            "name": name,
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
                "protocol": "HTTP",
                "entry": f"agents.{name}:run",
                "role_arn": "arn:aws:iam::123456789012:role/agent",
            },
            "contract": {
                "inputs": inputs or {},
                "outputs": {"doc": {"type": "string", "required": True}},
            },
            "spec": {"depends_on": depends_on or []},
        }
    )


def _two_node_plan():
    dag = AgentDAG()
    dag.add_node("ingest")
    dag.add_node("summarize")
    dag.add_edge("ingest", "summarize")
    manifests = {
        "ingest": _manifest("ingest"),
        "summarize": _manifest(
            "summarize",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }
    plan = OrchestrationAssembler().assemble(dag, manifests)
    return plan, dag, manifests


def test_state_holds_plan_sequence_not_mutable_plan():
    plan0, dag, manifests = _two_node_plan()
    store = InProcessStateStore()

    state = GovernorState(current_frozen_plan=plan0, store=store)
    assert state.plan_version == plan0.revision == 0
    assert state.iteration == 0
    assert state.store is store  # holds a pointer, not an inline copy
    assert state.plan_history == [plan0]

    # Capture a deep snapshot of the prior plan BEFORE advancing.
    prior_snapshot = copy.deepcopy(plan0.to_dict())

    # Form a NEW frozen plan at the compiler front (recompile bumps revision).
    plan1 = OrchestrationAssembler().recompile(
        plan0, completed=set(), dag=dag, manifests=manifests
    )
    assert plan1.revision == 1
    assert plan1 is not plan0

    returned = state.advance(plan1, reason="new-evidence")

    # The advance helper swapped in the new plan and bumped the version...
    assert returned is state
    assert state.plan_version == plan1.revision == 1
    assert state.current_frozen_plan is plan1
    assert state.iteration == 1
    assert state.no_progress == 0
    assert state.replan_reason == "new-evidence"

    # ...WITHOUT editing the prior plan object: byte-identical / deep-equal.
    assert plan0.to_dict() == prior_snapshot
    # The prior VALUE is preserved verbatim in the sequence.
    assert state.plan_history[0] is plan0
    assert state.plan_history == [plan0, plan1]
    # Strictly increasing revisions across the sequence (INV-4).
    revs = [p.revision for p in state.plan_history]
    assert revs == sorted(revs) and len(set(revs)) == len(revs)


def test_no_progress_counter_accumulates_on_stall():
    plan0, dag, manifests = _two_node_plan()
    state = GovernorState(current_frozen_plan=plan0, store=InProcessStateStore())

    plan1 = OrchestrationAssembler().recompile(
        plan0, completed=set(), dag=dag, manifests=manifests
    )
    state.advance(plan1, progressed=False)
    assert state.no_progress == 1

    plan2 = OrchestrationAssembler().recompile(
        plan1, completed=set(), dag=dag, manifests=manifests
    )
    state.advance(plan2, progressed=False)
    assert state.no_progress == 2
    assert state.plan_version == 2

    plan3 = OrchestrationAssembler().recompile(
        plan2, completed=set(), dag=dag, manifests=manifests
    )
    state.advance(plan3, progressed=True)
    assert state.no_progress == 0  # progress resets the stall counter
