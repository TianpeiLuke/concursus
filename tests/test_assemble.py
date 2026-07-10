"""Tests for the assembler — DAG + manifests compiled into a provisioning plan."""

import json

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble.assemble import (
    AssemblyError,
    MonotonicityError,
    OrchestrationAssembler,
    ProvisioningPlan,
)
from concursus.build.build import BuildPlanEntry
from concursus.core.resolve import AgentRef, AlignmentError


# -- fixtures ---------------------------------------------------------------
def _agent(name, inputs, outputs, depends_on=None, **registry):
    """Build a container-hosted HTTP manifest with the given contract + wiring."""
    reg = {
        "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
        "protocol": "HTTP",
        "entry": f"agents.{name}:run",
        "role_arn": "arn:aws:iam::123456789012:role/agent",
    }
    reg.update(registry)
    data = {
        "name": name,
        "registry": reg,
        "contract": {"inputs": inputs, "outputs": outputs},
    }
    if depends_on is not None:
        data["spec"] = {"depends_on": depends_on}
    return AgentManifest.from_dict(data)


def _chain():
    """A well-formed 4-node chain: ingest -> summarize -> critique -> format."""
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique", "format"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")
    dag.add_edge("critique", "format")

    manifests = {
        "ingest": _agent(
            "ingest",
            {"uri": {"type": "string"}},
            {"document": {"type": "string"}},
        ),
        "summarize": _agent(
            "summarize",
            {"document": {"type": "string"}},
            {"properties": {"summary": {"type": "string"}}},
            depends_on=[{"from": "ingest.document", "to": "document"}],
        ),
        "critique": _agent(
            "critique",
            {"summary": {"type": "string"}},
            {"critique": {"type": "string"}},
            depends_on=[{"from": "summarize.summary", "to": "summary"}],
        ),
        "format": _agent(
            "format",
            {"critique": {"type": "string"}},
            {"report": {"type": "string"}},
            depends_on=[{"from": "critique.critique", "to": "critique"}],
        ),
    }
    return dag, manifests


# -- assemble ---------------------------------------------------------------
def test_assemble_returns_provisioning_plan():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert isinstance(plan, ProvisioningPlan)


def test_assemble_order_is_topological():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.order == ["ingest", "summarize", "critique", "format"]


def test_assemble_has_one_entry_per_node():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert set(plan.entries) == set(dag.nodes)
    assert all(isinstance(e, BuildPlanEntry) for e in plan.entries.values())
    assert plan.entries["summarize"].name == "summarize"
    assert plan.entries["summarize"].build_mode == "container"


def test_assemble_wiring_has_right_agent_refs():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.wiring["ingest"] == []
    assert plan.wiring["summarize"] == [
        AgentRef(producer="ingest", path="$.document", input_name="document")
    ]
    assert plan.wiring["critique"] == [
        AgentRef(producer="summarize", path="$.summary", input_name="summary")
    ]
    assert plan.wiring["format"] == [
        AgentRef(producer="critique", path="$.critique", input_name="critique")
    ]


def test_assemble_threads_account_and_region_into_role():
    dag, manifests = _chain()
    manifests["ingest"].registry.pop("role_arn")  # force role synthesis
    plan = OrchestrationAssembler(account="123456789012", region="us-east-1").assemble(
        dag, manifests
    )
    role = plan.entries["ingest"].execution_role
    assert role is not None
    assert "123456789012" in str(role) and "us-east-1" in str(role)


# -- to_dict / preview ------------------------------------------------------
def test_plan_to_dict_round_trips_through_json():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)
    d = plan.to_dict()
    text = json.dumps(d)  # must be JSON-serializable for a `concursus plan` preview
    reloaded = json.loads(text)
    assert reloaded["order"] == ["ingest", "summarize", "critique", "format"]
    assert set(reloaded["entries"]) == set(dag.nodes)
    assert reloaded["wiring"]["summarize"] == [
        {"producer": "ingest", "path": "$.document", "input_name": "document"}
    ]
    assert reloaded["entries"]["summarize"]["invoke"]["protocol"] == "HTTP"


# -- failure modes ----------------------------------------------------------
def test_assemble_raises_on_misaligned_manifests():
    dag, manifests = _chain()
    # summarize points at an output field ingest never declares.
    manifests["summarize"].spec["depends_on"] = [
        {"from": "ingest.nonexistent", "to": "document"}
    ]
    with pytest.raises(AlignmentError):
        OrchestrationAssembler().assemble(dag, manifests)


def test_assemble_raises_on_node_without_manifest():
    dag, manifests = _chain()
    del manifests["format"]
    with pytest.raises(AssemblyError, match="format"):
        OrchestrationAssembler().assemble(dag, manifests)


# -- AI-20: monotonic re-compile --------------------------------------------
def _chain_plus_publish():
    """The 4-node chain extended with a 5th node ``publish`` consuming format.report."""
    dag, manifests = _chain()
    dag.add_node("publish")
    dag.add_edge("format", "publish")
    manifests["publish"] = _agent(
        "publish",
        {"report": {"type": "string"}},
        {"url": {"type": "string"}},
        depends_on=[{"from": "format.report", "to": "report"}],
    )
    return dag, manifests


def test_recompile_pins_executed_nodes_and_is_monotonic_superset():
    dag, manifests = _chain()
    asm = OrchestrationAssembler()
    prior = asm.assemble(dag, manifests)
    assert prior.revision == 0
    assert prior.to_dict().get("revision") is None  # unchanged first-compile preview

    # Two nodes have executed; re-compile the topology extended with a new `publish` node.
    ext_dag, ext_manifests = _chain_plus_publish()
    new_plan = asm.recompile(
        prior,
        completed={"ingest", "summarize"},
        content_hashes={"ingest": "h0", "summarize": "h1"},
        dag=ext_dag,
        manifests=ext_manifests,
    )
    # fresh frozen plan, revision bumped, surfaced in to_dict
    assert new_plan is not prior
    assert new_plan.revision == 1
    assert new_plan.to_dict()["revision"] == 1
    # monotonic superset: prior order survives as a subsequence, publish appended
    assert new_plan.order == ["ingest", "summarize", "critique", "format", "publish"]
    # executed nodes pinned to the PRIOR entry/wiring objects (identity, not just equality)
    assert new_plan.entries["ingest"] is prior.entries["ingest"]
    assert new_plan.entries["summarize"] is prior.entries["summarize"]
    assert new_plan.wiring["summarize"] == prior.wiring["summarize"]
    # a brand-new node is present with freshly-compiled wiring
    assert new_plan.wiring["publish"] == [
        AgentRef(producer="format", path="$.report", input_name="report")
    ]


def test_check_monotonic_raises_on_edit_to_executed_node():
    dag, manifests = _chain()
    asm = OrchestrationAssembler()
    prior = asm.assemble(dag, manifests)

    # Re-author the EXECUTED node `summarize` so its hosting identity changes -> a new entry.
    edited = {name: m for name, m in manifests.items()}
    edited["summarize"] = _agent(
        "summarize",
        {"document": {"type": "string"}},
        {"properties": {"summary": {"type": "string"}}},
        depends_on=[{"from": "ingest.document", "to": "document"}],
        protocol="MCP",  # changes fingerprint + create request -> a different BuildPlanEntry
    )
    with pytest.raises(MonotonicityError, match="summarize"):
        asm.recompile(
            prior,
            completed={"ingest", "summarize"},
            dag=dag,
            manifests=edited,
        )


def test_recompile_refuses_past_max_revisions():
    dag, manifests = _chain()
    asm = OrchestrationAssembler()
    prior = asm.assemble(dag, manifests)
    prior.revision = 3  # pretend we're already at revision 3
    with pytest.raises(MonotonicityError, match="max_revisions"):
        asm.recompile(
            prior,
            completed={"ingest"},
            dag=dag,
            manifests=manifests,
            max_revisions=3,
        )


def test_recompile_rejects_dropping_a_planned_node():
    dag, manifests = _chain()
    asm = OrchestrationAssembler()
    prior = asm.assemble(dag, manifests)

    # Re-author a smaller DAG that drops the (unexecuted) `format` node — not a superset.
    smaller = AgentDAG()
    for n in ["ingest", "summarize", "critique"]:
        smaller.add_node(n)
    smaller.add_edge("ingest", "summarize")
    smaller.add_edge("summarize", "critique")
    fewer = {k: manifests[k] for k in ["ingest", "summarize", "critique"]}
    with pytest.raises(MonotonicityError, match="drop"):
        asm.recompile(prior, completed={"ingest"}, dag=smaller, manifests=fewer)


def test_recompile_requires_dag_and_manifests():
    dag, manifests = _chain()
    asm = OrchestrationAssembler()
    prior = asm.assemble(dag, manifests)
    with pytest.raises(AssemblyError, match="recompile requires"):
        asm.recompile(prior, completed=set())
