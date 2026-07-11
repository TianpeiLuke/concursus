"""Freeze-boundary tests: the governor re-derives the executed prefix from the LOG each round.

G-3 asserts the memory seam (INV-5): on RE-ENTRY the planner re-declares the executed prefix —
the completed node-set AND its content-hash provenance — straight from the append-only
:class:`StateStore` log via ``store.completed()`` / ``store.records()``, then hands it to
:meth:`OrchestrationAssembler.recompile` so completed nodes are PINNED byte-identically and growth
is confined to the still-open frontier. The prefix is NEVER cached mutably in governor state: the
log is the sole structural anchor of what has executed, so it must be reconstructable from the log
ALONE (even with all in-memory governor state discarded).

Everything is offline: an :class:`InProcessStateStore`, no AWS touched.
"""

from concursus import (
    AgentManifest,
    GovernorLoop,
    OrchestrationAssembler,
    plan_from_goal,
)
from concursus.governor.state import GovernorState
from concursus.state.statestore import InProcessStateStore, content_hash


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


def _plan_model_fn(goal, precedents, directives):
    return {"nodes": ["ingest", "summarize"], "edges": [["ingest", "summarize"]]}


def _two_node_manifests():
    return {
        "ingest": _manifest("ingest"),
        "summarize": _manifest(
            "summarize",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "ingest.doc", "to": "document"}],
        ),
    }


def _loop_over(store, manifests):
    """A governor loop bound to ``store`` — deliberately handed NO GovernorState, so the executed
    prefix must be reconstructable from the log alone."""
    return GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        plan_model_fn=_plan_model_fn,
        backend="python",
    )


def test_prefix_reconstructed_from_log_only():
    """Discard all in-memory governor state, rebuild the prefix from records() alone, recompile —
    the executed node is PINNED byte-identically to the prior frozen plan (INV-5)."""
    manifests = _two_node_manifests()
    dag = plan_from_goal("summarize the document", plan_model_fn=_plan_model_fn)
    assembler = OrchestrationAssembler()
    plan0 = assembler.assemble(dag, manifests)  # the prior frozen plan (revision 0)
    assert plan0.revision == 0

    # Simulate round 1 having executed "ingest": the SOLE record of that fact is the append-only
    # log — there is no governor-side cache of the prefix anywhere.
    ingest_out = {"doc": "ingest-out"}
    store = InProcessStateStore()
    store.put("ingest", ingest_out)

    loop = _loop_over(store, manifests)

    # "Delete in-memory state": there is none to delete — the loop caches no prefix. Re-derive it
    # purely from the log.
    completed, content_hashes = loop._executed_prefix_from_log()
    assert completed == {"ingest"}
    assert content_hashes == {"ingest": content_hash(ingest_out)}

    # Independently reconstruct the same prefix straight from records() (no loop involvement) — the
    # log is the sole anchor and both derivations must agree.
    validated = store.completed()
    rebuilt = {
        r.node: r.content_hash
        for r in store.records()
        if r.status == "validated" and r.node in validated
    }
    assert rebuilt == content_hashes

    # Recompile the PRIOR plan with the log-derived prefix: the executed node is pinned to its prior
    # BuildPlanEntry/wiring — byte-identical (in fact the same object), and the frontier grows.
    plan1 = assembler.recompile(
        plan0,
        completed=completed,
        content_hashes=content_hashes,
        dag=dag,
        manifests=manifests,
    )
    assert plan1.revision == plan0.revision + 1
    assert plan1.entries["ingest"] == plan0.entries["ingest"]  # byte-equal
    assert plan1.entries["ingest"] is plan0.entries["ingest"]  # the pin reuses the prior object
    assert plan1.wiring["ingest"] == plan0.wiring["ingest"]
    # The prior plan VALUE is untouched by the recompile (INV-3): still revision 0.
    assert plan0.revision == 0
    assert plan0.to_dict() == assembler.assemble(dag, manifests).to_dict()


def test_no_mutation_path_to_executed_prefix():
    """There is NO write path to the executed prefix except appending to the log: the derivation is
    recomputed each call, returns fresh copies, and governor state exposes no set_output-style
    mutator and caches no prefix (INV-5)."""
    manifests = _two_node_manifests()
    dag = plan_from_goal("summarize the document", plan_model_fn=_plan_model_fn)
    plan0 = OrchestrationAssembler().assemble(dag, manifests)

    ingest_out = {"doc": "ingest-out"}
    store = InProcessStateStore()
    store.put("ingest", ingest_out)
    loop = _loop_over(store, manifests)

    # The prefix is RE-DERIVED each call and returned as FRESH copies: mutating what one call
    # returns cannot poison the next derivation (no shared mutable prefix state).
    c1, h1 = loop._executed_prefix_from_log()
    c1.add("phantom")
    h1["phantom"] = "deadbeef"
    c2, h2 = loop._executed_prefix_from_log()
    assert c2 == {"ingest"}
    assert h2 == {"ingest": content_hash(ingest_out)}

    # GovernorState holds a plan VALUE by version + a log pointer — NOT a cached prefix, and NO
    # set_output-style in-place mutator (the HiveFleet anti-patterns are absent).
    state = GovernorState(current_frozen_plan=plan0, store=store)
    for banned in ("set_output", "_bind_ready_steps", "_writeback_plan_status"):
        assert not hasattr(state, banned)
    for banned in ("completed", "prefix", "executed_prefix", "content_hashes"):
        assert banned not in vars(state)  # the prefix is never a mutable field of governor state

    # The ONLY way to change the derived prefix is to append to the append-only log — do so and the
    # re-derivation reflects it, with no other write path involved.
    store.put("summarize", {"doc": "summarize-out"})
    c3, h3 = loop._executed_prefix_from_log()
    assert c3 == {"ingest", "summarize"}
    assert h3 == {
        "ingest": content_hash(ingest_out),
        "summarize": content_hash({"doc": "summarize-out"}),
    }
