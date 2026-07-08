"""Tests for the dependency resolver — extract, edge compilation, and type-gating."""

import pytest

from concursus import AgentDAG, AgentManifest
from concursus.resolve import (
    AgentRef,
    AlignmentError,
    check_alignment,
    extract,
    resolve_edges,
)


# -- extract ----------------------------------------------------------------
def test_extract_dollar_field():
    assert extract({"summary": "hi"}, "$.summary") == "hi"


def test_extract_dotted():
    assert extract({"a": {"b": {"c": 1}}}, "$.a.b.c") == 1


def test_extract_list_index():
    assert extract({"a": {"b": [10, 20, 30]}}, "$.a.b[1]") == 20


def test_extract_without_dollar_prefix():
    assert extract({"a": 1}, "a") == 1


def test_extract_bare_dollar_returns_whole_object():
    obj = {"a": 1, "b": 2}
    assert extract(obj, "$") == obj


def test_extract_missing_key_raises_keyerror():
    with pytest.raises(KeyError):
        extract({"a": 1}, "$.b")


def test_extract_index_out_of_range_raises_indexerror():
    with pytest.raises(IndexError):
        extract({"a": [1]}, "$.a[5]")


# -- fixtures ---------------------------------------------------------------
def _chain():
    """A well-formed 3-node chain: ingest -> summarize -> critique.

    ``ingest`` uses a flat output schema; ``summarize`` uses the nested
    ``{"properties": {...}}`` shape — exercising both accepted forms.
    """
    dag = AgentDAG()
    for n in ["ingest", "summarize", "critique"]:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    dag.add_edge("summarize", "critique")

    manifests = {
        "ingest": AgentManifest.from_dict(
            {
                "name": "ingest",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"uri": {"type": "string"}},
                    "outputs": {"document": {"type": "string"}},
                },
            }
        ),
        "summarize": AgentManifest.from_dict(
            {
                "name": "summarize",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"document": {"type": "string"}},
                    "outputs": {"properties": {"summary": {"type": "string"}}},
                },
                "spec": {"depends_on": [{"from": "ingest.document", "to": "document"}]},
            }
        ),
        "critique": AgentManifest.from_dict(
            {
                "name": "critique",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"summary": {"type": "string"}},
                    "outputs": {"critique": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "summarize.summary", "to": "summary"}]},
            }
        ),
    }
    return dag, manifests


# -- resolve_edges ----------------------------------------------------------
def test_resolve_edges_builds_agent_refs():
    dag, manifests = _chain()
    wiring = resolve_edges(dag, manifests)
    assert wiring["ingest"] == []
    assert wiring["summarize"] == [
        AgentRef(producer="ingest", path="$.document", input_name="document")
    ]
    assert wiring["critique"] == [
        AgentRef(producer="summarize", path="$.summary", input_name="summary")
    ]


def test_resolve_edges_covers_every_dag_node():
    dag, manifests = _chain()
    assert set(resolve_edges(dag, manifests)) == set(dag.nodes)


def test_resolve_edges_preserves_nested_path():
    dag = AgentDAG()
    dag.add_node("p").add_node("c").add_edge("p", "c")
    manifests = {
        "p": AgentManifest.from_dict(
            {
                "name": "p",
                "registry": {"container_uri": "x"},
                "contract": {"outputs": {"data": {"type": "object"}}},
            }
        ),
        "c": AgentManifest.from_dict(
            {
                "name": "c",
                "registry": {"container_uri": "x"},
                "contract": {
                    "inputs": {"v": {"type": "string"}},
                    "outputs": {"o": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "p.data.items[0]", "to": "v"}]},
            }
        ),
    }
    assert resolve_edges(dag, manifests)["c"] == [
        AgentRef(producer="p", path="$.data.items[0]", input_name="v")
    ]


# -- check_alignment --------------------------------------------------------
def test_check_alignment_passes_well_formed_chain():
    dag, manifests = _chain()
    assert check_alignment(dag, manifests) is None


def test_check_alignment_rejects_unknown_producer():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ghost.document", "to": "document"}]
    with pytest.raises(AlignmentError, match="unknown producer 'ghost'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_output_field_not_in_schema():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.missing", "to": "document"}]
    with pytest.raises(AlignmentError, match="does not declare output field 'missing'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_undeclared_consumer_input():
    dag, manifests = _chain()
    manifests["summarize"].spec["depends_on"] = [{"from": "ingest.document", "to": "nope"}]
    with pytest.raises(AlignmentError, match="target input 'nope'"):
        check_alignment(dag, manifests)


def test_check_alignment_rejects_missing_dag_edge():
    dag, manifests = _chain()
    # Declare a valid producer/field/input but omit the ingest -> critique DAG edge.
    manifests["critique"].contract["inputs"]["document"] = {"type": "string"}
    manifests["critique"].spec["depends_on"].append(
        {"from": "ingest.document", "to": "document"}
    )
    with pytest.raises(AlignmentError, match="no edge 'ingest' -> 'critique'"):
        check_alignment(dag, manifests)


def test_check_alignment_accepts_flat_output_schema():
    dag = AgentDAG()
    dag.add_node("p").add_node("c").add_edge("p", "c")
    manifests = {
        "p": AgentManifest.from_dict(
            {
                "name": "p",
                "registry": {"container_uri": "x"},
                "contract": {"outputs": {"result": {"type": "string"}}},
            }
        ),
        "c": AgentManifest.from_dict(
            {
                "name": "c",
                "registry": {"container_uri": "x"},
                "contract": {
                    "inputs": {"r": {"type": "string"}},
                    "outputs": {"o": {"type": "string"}},
                },
                "spec": {"depends_on": [{"from": "p.result", "to": "r"}]},
            }
        ),
    }
    assert check_alignment(dag, manifests) is None
