"""Smoke tests for the Concursus declarative core."""

import pytest

from concursus import AgentDAG, AgentManifest, DAGError, ManifestError, __version__


def test_version_resolves():
    assert __version__ and __version__ != "0.0.0"


def test_dag_topological_order():
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique", "format"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")
    dag.add_edge("critique", "format")
    assert dag.topological_sort() == ["ingest", "summarize", "critique", "format"]
    assert dag.sources() == ["ingest"]
    assert dag.sinks() == ["format"]
    assert dag.get_dependencies("critique") == ["summarize"]


def test_dag_detects_cycle():
    dag = AgentDAG()
    for n in ["a", "b"]:
        dag.add_node(n)
    dag.add_edge("a", "b")
    dag.add_edge("b", "a")
    with pytest.raises(DAGError):
        dag.topological_sort()


def test_dag_round_trips():
    dag = AgentDAG.from_dict({"nodes": ["a", "b"], "edges": [["a", "b"]]})
    assert dag.to_dict() == {"nodes": ["a", "b"], "edges": [["a", "b"]]}


def test_dag_rejects_unknown_edge_node():
    dag = AgentDAG().add_node("a")
    with pytest.raises(DAGError):
        dag.add_edge("a", "missing")


def test_manifest_validates():
    m = AgentManifest.from_dict(
        {
            "name": "summarize",
            "registry": {"container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/x:latest",
                         "protocol": "HTTP"},
            "contract": {"inputs": {"document": {"type": "string"}},
                         "outputs": {"summary": {"type": "string"}}},
        }
    ).validate()
    assert m.protocol == "HTTP"
    assert "summary" in m.output_schema


def test_manifest_requires_output_schema():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            {"name": "x", "registry": {"container_uri": "y"}, "contract": {}}
        ).validate()


def test_manifest_requires_hosting_binding():
    with pytest.raises(ManifestError):
        AgentManifest.from_dict(
            {"name": "x", "contract": {"outputs": {"o": {"type": "string"}}}}
        ).validate()
