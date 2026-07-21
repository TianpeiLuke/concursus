"""Tests for net-new agent-manifest authoring ( Phase 3b).

The deterministic skeleton authors a valid, low-trust manifest LLM-free; an injected
manifest_author_fn upgrades it; malformed author output is rejected.
"""

import pytest

from concursus.build.trust import TrustGrade
from concursus.core.manifest import AgentManifest, ManifestError
from concursus import AgentDAG, OrchestrationAssembler
from concursus.assemble.planner import plan_from_goal
from concursus.governor.authoring import (
    ManifestAuthorError,
    RebindExhausted,
    author_manifest,
    staff_capability_dag,
    staff_with_rebind,
)


def test_skeleton_manifest_is_valid_and_low_trust():
    """Default (no author fn) -> a valid, provisionable skeleton at a LOW trust seed."""
    m = author_manifest("triage_alarm_burst")
    assert isinstance(m, AgentManifest)
    m.validate()  # must not raise
    assert m.trust_seed == TrustGrade.L0_SHADOW      # unproven -> must earn autonomy
    assert m.output_schema                            # mandatory type gate is present
    assert "triage_alarm_burst" in m.registry.get("capabilities", [])
    assert m.protocol == "HTTP"


def test_skeleton_not_side_effecting_by_default():
    """A freshly-authored role is non-side-effecting until declared otherwise (safe default)."""
    assert author_manifest("summarize").side_effecting is False


def test_injected_author_upgrades_the_skeleton():
    """An injected manifest_author_fn (the LLM seam) replaces the skeleton, and is validated."""
    def author(task, ctx):
        return {
            "name": "custom_role",
            "registry": {"container_uri": "img", "protocol": "MCP", "entry": "a.b:run"},
            "contract": {"inputs": {}, "outputs": {"answer": {"type": "string", "required": True}}},
            "trust_seed": "L1_CANARY",
        }
    m = author_manifest("do_x", manifest_author_fn=author)
    assert m.name == "custom_role" and m.protocol == "MCP"
    assert m.trust_seed == TrustGrade.L1_CANARY


def test_author_accepts_a_manifest_object():
    """An author fn may return an AgentManifest directly."""
    obj = AgentManifest.from_dict({
        "name": "direct",
        "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.b:run"},
        "contract": {"inputs": {}, "outputs": {"r": {"type": "string", "required": True}}},
    })
    m = author_manifest("t", manifest_author_fn=lambda task, ctx: obj)
    assert m is obj


def test_bad_author_output_raises_manifest_author_error():
    """A non-manifest / non-mapping author output is rejected cleanly."""
    with pytest.raises(ManifestAuthorError):
        author_manifest("t", manifest_author_fn=lambda task, ctx: 42)


def test_author_validates_invalid_manifest():
    """An author fn returning a schema-less manifest fails validation (the mandatory output gate)."""
    def bad(task, ctx):
        # missing contract.outputs -> AgentManifest.validate must reject
        return {"name": "noout", "registry": {"container_uri": "img", "protocol": "HTTP",
                                               "entry": "a.b:run"},
                "contract": {"inputs": {}, "outputs": {}}}
    with pytest.raises(ManifestAuthorError):
        author_manifest("t", manifest_author_fn=bad)


def test_empty_task_rejected():
    with pytest.raises(ManifestAuthorError):
        author_manifest("   ")


# -- A1-A3: staff a capability DAG into an assemblable manifest set --------------
def test_staff_capability_dag_cold_start_assembles_end_to_end():
    """The north-star: a decomposed CAPABILITY DAG (no manifests, no wiring) is staffed and
    ASSEMBLED end-to-end with ZERO hand-authored manifests (cold start, no binder)."""
    dag = plan_from_goal("investigate the checkout latency regression", decompose=True)
    assert len(dag.nodes) > 1 and all("__" in n for n in dag.nodes)  # agent-agnostic capabilities

    manifests = staff_capability_dag(dag)  # bind_fn=None => author every node

    # A manifest per node, KEYED by the node id and NAMED the node id (assemble requires name==key).
    assert set(manifests) == set(dag.nodes)
    assert all(manifests[n].name == n for n in dag.nodes)
    # Fresh authored roles enter UNPROVEN.
    assert all(manifests[n].trust_seed == TrustGrade.L0_SHADOW for n in dag.nodes)

    # It assembles + freezes exactly like a hand-authored set.
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.order == dag.topological_sort()
    # The chain's wiring was synthesized from the DAG edges (each non-source node consumes upstream).
    sink = dag.topological_sort()[-1]
    assert plan.wiring[sink], "expected synthesized wiring on the sink node"
    assert plan.wiring[sink][0].path == "$.result"


def test_staff_capability_dag_binds_when_bind_fn_returns_agent():
    """When bind_fn returns a standing agent name, the node manifest records the binding
    (bound_agent) but stays keyed by the capability node id (topology preserved)."""
    dag = AgentDAG()
    dag.add_node("cap_a").add_node("cap_b").add_edge("cap_a", "cap_b")
    # A binder that staffs cap_a from the bench, cap_b unmatched (authored).
    bench = {"cap_a": "veteran_ingestor"}
    manifests = staff_capability_dag(dag, bind_fn=lambda node: bench.get(node))

    assert manifests["cap_a"].registry.get("bound_agent") == "veteran_ingestor"
    assert manifests["cap_a"].name == "cap_a"  # keyed/named by the capability node, not the agent
    assert "bound_agent" not in manifests["cap_b"].registry  # cap_b was authored (unmatched)
    # Still assembles: the bound + authored mix is a valid manifest set.
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.order == ["cap_a", "cap_b"]


def test_staff_capability_dag_uses_injected_author_for_unmatched():
    """An injected manifest_author_fn synthesizes the role for an unmatched capability."""
    seen = []

    def author(task, ctx):
        seen.append(task)
        return {
            "name": task,  # will be re-pinned to the node id by staff_capability_dag anyway
            "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.b:run"},
            "contract": {"inputs": {}, "outputs": {"result": {"type": "string", "required": True}}},
        }

    dag = AgentDAG()
    dag.add_node("solo_task")
    manifests = staff_capability_dag(dag, manifest_author_fn=author)
    assert seen == ["solo_task"]
    assert manifests["solo_task"].name == "solo_task"


def test_staff_capability_dag_single_node():
    """A single-node (undecomposed) capability DAG staffs to one authored manifest, no wiring."""
    dag = AgentDAG()
    dag.add_node("resolve_ticket")
    manifests = staff_capability_dag(dag)
    assert set(manifests) == {"resolve_ticket"}
    plan = OrchestrationAssembler().assemble(dag, manifests)
    assert plan.order == ["resolve_ticket"]
    assert plan.wiring["resolve_ticket"] == []  # a source node has no inbound wiring


# -- C2: re-bind on alignment failure (staff_with_rebind) ------------------------
def _cand(name, out_type):
    """A candidate agent manifest declaring a given output ``result`` type."""
    from concursus.core.manifest import AgentManifest
    return AgentManifest.from_dict({
        "name": name,
        "registry": {"container_uri": "img", "protocol": "HTTP", "entry": "a.b:run"},
        "contract": {"inputs": {}, "outputs": {"result": {"type": out_type}}},
    })


def _ab_dag():
    from concursus import AgentDAG
    dag = AgentDAG()
    dag.add_node("cap_a").add_node("cap_b").add_edge("cap_a", "cap_b")
    return dag


def test_staff_with_rebind_selects_aligning_producer():
    """The first candidate for the producer type-MISMATCHES the consumer; staff_with_rebind advances
    to the next candidate that ALIGNS, and the result assembles under strict_types."""
    dag = _ab_dag()

    def candidates(node):
        if node == "cap_a":
            return [_cand("int_agent", "integer"), _cand("str_agent", "string")]  # 1st mismatches
        return [_cand("consumer", "string")]

    manifests = staff_with_rebind(dag, candidates)
    # The producer was re-bound to the aligning candidate (str_agent), not the first (int_agent).
    assert manifests["cap_a"].registry.get("bound_agent") == "str_agent"
    assert manifests["cap_a"].name == "cap_a"  # topology preserved (keyed by node id)
    # It assembles under the strict deep gate (the whole point — a type-aligning team).
    plan = OrchestrationAssembler(strict_types=True).assemble(dag, manifests)
    assert plan.order == ["cap_a", "cap_b"]


def test_staff_with_rebind_first_candidate_wins_when_it_aligns():
    """No re-bind needed when the first candidate already aligns (the common fast path)."""
    dag = _ab_dag()
    manifests = staff_with_rebind(
        dag, lambda node: [_cand(f"{node}_agent", "string")]  # everything string => aligns
    )
    assert manifests["cap_a"].registry.get("bound_agent") == "cap_a_agent"
    OrchestrationAssembler(strict_types=True).assemble(dag, manifests)  # must not raise


def test_staff_with_rebind_exhausts_when_no_candidate_aligns():
    """When no producer candidate can align the edge, the bounded search raises RebindExhausted."""
    dag = _ab_dag()

    def candidates(node):
        if node == "cap_a":
            return [_cand("int_agent", "integer")]  # only a mismatching producer
        return [_cand("consumer", "string")]

    with pytest.raises(RebindExhausted):
        staff_with_rebind(dag, candidates)


def test_staff_with_rebind_is_bounded():
    """The search is bounded by max_rebinds even with many mismatching candidates (INV-2)."""
    dag = _ab_dag()

    def candidates(node):
        if node == "cap_a":
            return [_cand(f"int{i}", "integer") for i in range(50)]  # all mismatch
        return [_cand("consumer", "string")]

    # It stops (raises) rather than looping unbounded.
    with pytest.raises(RebindExhausted):
        staff_with_rebind(dag, candidates, max_rebinds=5)
