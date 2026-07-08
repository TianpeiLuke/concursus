"""Tests for the assembler — DAG + manifests compiled into a provisioning plan."""

import json

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.assemble import AssemblyError, OrchestrationAssembler, ProvisioningPlan
from concursus.build import BuildPlanEntry
from concursus.resolve import AgentRef, AlignmentError


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
