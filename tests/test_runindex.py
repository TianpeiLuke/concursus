"""Tests for the RunIndex — metadata query + Folgezettel-tree traversal over the log."""

import pytest

from concursus.runindex import RunIndex, RunIndexError, address_of
from concursus.statestore import InProcessStateStore, Record


def _rec(
    node,
    *,
    address=None,
    status="validated",
    record_type="agent_output",
    producer=None,
    schema=None,
    attempt=1,
    output=None,
):
    return Record(
        node=node,
        output=output or {},
        attempt=attempt,
        status=status,
        record_type=record_type,
        producer=producer,
        schema=schema,
        address=address,
    )


# -- metadata query ---------------------------------------------------------
def test_query_filters_by_metadata():
    records = [
        _rec("ingest", schema="ingest"),
        _rec("summarize", producer="summarize", schema="summarize"),
        _rec("summarize", status="failed", attempt=2),
        _rec("critique", record_type="checkpoint"),
    ]
    idx = RunIndex.from_records(records)

    assert {r.node for r in idx.query(status="validated")} == {
        "ingest",
        "summarize",
        "critique",
    }
    assert [r.attempt for r in idx.query(node="summarize", status="failed")] == [2]
    assert [r.node for r in idx.query(record_type="checkpoint")] == ["critique"]
    assert idx.query(node="summarize", record_type="checkpoint") == []  # AND across fields
    assert idx.nodes() == {"ingest", "summarize", "critique"}


def test_latest_and_by():
    records = [
        _rec("a", attempt=1),
        _rec("a", attempt=2),
        _rec("a", status="failed", attempt=3),
    ]
    idx = RunIndex.from_records(records)
    assert idx.latest("a").attempt == 2  # latest VALIDATED
    assert idx.latest("a", status=None).attempt == 3  # latest of any status
    assert set(idx.by("status")) == {"validated", "failed"}
    assert len(idx.by("node")["a"]) == 3


def test_from_store_indexes_the_log():
    store = InProcessStateStore()
    store.put("ingest", {"doc": "x"}, meta={"schema": "ingest"})
    store.put(
        "summarize", {"sum": "y"}, meta={"producer": "summarize", "consumes": ["ingest:$.doc"]}
    )
    idx = RunIndex.from_store(store)
    assert idx.nodes() == {"ingest", "summarize"}
    assert [r.node for r in idx.query(schema="ingest")] == ["ingest"]


# -- Folgezettel tree -------------------------------------------------------
def _tree_index():
    # node 'a' retried twice; a fan-out node 'map' with two children; a plain node 'b'.
    return RunIndex.from_records(
        [
            _rec("a", address="a"),
            _rec("a", address="a/r2", attempt=2),
            _rec("a", address="a/r2/r3", attempt=3),
            _rec("map", address="map"),
            _rec("map", address="map/0"),
            _rec("map", address="map/1"),
            _rec("b", address="b"),
        ]
    )


def test_address_defaults_to_node():
    assert address_of(_rec("x")) == "x"
    assert address_of(_rec("x", address="x/r2")) == "x/r2"


def test_tree_parent_children_siblings():
    idx = _tree_index()
    assert idx.parent("a/r2") == "a"
    assert idx.parent("a") is None
    assert idx.children("a") == ["a/r2"]
    assert idx.children("map") == ["map/0", "map/1"]
    assert idx.siblings("map/0") == ["map/1"]
    assert set(idx.roots()) == {"a", "map", "b"}


def test_tree_ancestors_descendants_subtree():
    idx = _tree_index()
    assert idx.ancestors("a/r2/r3") == ["a/r2", "a"]  # nearest-first
    assert idx.descendants("a") == ["a/r2", "a/r2/r3"]
    assert idx.subtree("a") == ["a", "a/r2", "a/r2/r3"]
    assert idx.leaves() == sorted(["a/r2/r3", "b", "map/0", "map/1"])


def test_traverse_neighbourhood_and_record_at():
    idx = _tree_index()
    t = idx.traverse("a/r2")
    assert t["ancestors"] == ["a"]  # root-first
    assert t["children"] == ["a/r2/r3"]
    assert t["descendants"] == ["a/r2/r3"]
    assert idx.record_at("a/r2").attempt == 2
    assert idx.record_at("map").node == "map"


# -- structural layout guard (AI-7) -----------------------------------------
def test_validate_passes_on_well_formed_run():
    # every non-root address's parent is a real record; every root names a known node.
    idx = _tree_index()
    assert idx.validate() is idx  # returns self for chaining
    # attempts per node are contiguous 1..N here, so the optional check also passes.
    idx2 = RunIndex.from_records(
        [_rec("a", address="a"), _rec("a", address="a/r2", attempt=2)]
    )
    assert idx2.validate(check_attempts=True) is idx2


def test_validate_raises_on_synthesized_orphan_address():
    # 'map/0' is a fan-out sub-address, but 'map' itself never executed (no record at 'map') —
    # __init__ back-fills the bare 'map' prefix for traversal, so this is an orphan.
    idx = RunIndex.from_records([_rec("map", address="map/0")])
    with pytest.raises(RunIndexError, match="orphaned address 'map/0'"):
        idx.validate()


def test_validate_raises_on_unknown_root_segment():
    # a record addressed under a root that is not a known node id.
    rec = _rec("realnode", address="realnode")
    ghost = _rec("realnode", address="ghostroot/child")  # rooted at unknown 'ghostroot'
    idx = RunIndex.from_records([rec, ghost])
    with pytest.raises(RunIndexError, match="not a known node"):
        idx.validate()


def test_validate_check_attempts_flags_non_contiguous_attempts():
    # node 'a' jumps 1 -> 3, skipping attempt 2 (a missing retry record).
    idx = RunIndex.from_records(
        [_rec("a", address="a", attempt=1), _rec("a", address="a/r3", attempt=3)]
    )
    # default validate() passes (the tree is honest); only check_attempts flags the gap.
    idx.validate()
    with pytest.raises(RunIndexError, match="non-contiguous attempts"):
        idx.validate(check_attempts=True)
