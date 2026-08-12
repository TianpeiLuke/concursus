"""End-to-end: the REAL GovernorLoop drives the harness path.

Exercises the complete govern chain with no Concursus mocks:

    GovernorLoop.run(inputs)
      └─ planner (plan_from_goal + injected plan_model_fn) authors the AgentDAG
      └─ OrchestrationAssembler.assemble → frozen ProvisioningPlan (wiring type-gated)
      └─ run_episode → make_harness_supervisor_factory (the NEW glue)
           └─ REAL Supervisor with node_executors + node_kind_fn injected
                └─ harness NodeExecutor → AgentHarness → AgentInvoker → callable agents

    extractor produces a CSV artifact → FileStore; analyzer consumes the
    ArtifactRef via the ASSEMBLER-compiled wiring (manifest spec.depends_on),
    not hand-built AgentRefs.

The only test doubles are the two leaf agent functions (tmp modules) and the
deterministic plan_model_fn — both by design: leaf agents are black boxes and
the plan model is the planner's injectable seam.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from concursus.core.manifest import AgentManifest
from concursus.execute.harness_factory import (
    HarnessFactory,
    make_harness_supervisor_factory,
)
from concursus.execute.object_store import FileStore
from concursus.governor.loop import GovernorLoop, GovernorResult
from concursus.state.statestore import InProcessStateStore


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures: contracts (shared), AgentManifests (Supervisor/assemble side),
# raw manifests (harness side), and the deterministic plan model.
# ──────────────────────────────────────────────────────────────────────────────

_EXTRACTOR_CONTRACT = {
    "inputs": {"properties": {"query": {"type": "string"}}},
    "outputs": {
        "properties": {
            "report_csv": {
                "type": "artifact",
                "content_type": "text/csv",
                "required": True,
            },
            "row_count": {"type": "number"},
        }
    },
}

_ANALYZER_CONTRACT = {
    "inputs": {
        "properties": {
            "source_data": {"type": "artifact", "content_type": "text/csv"},
        }
    },
    "outputs": {"properties": {"pass_rate": {"type": "number"}}},
}

# check_alignment now normalizes consumer inputs exactly like outputs (nested
# {'properties': {...}} or flat), so BOTH sides — the AgentManifest contracts
# (assemble/Supervisor) and the harness raw manifests — share the ONE canonical
# nested form. No dual-form contracts needed.


def _agent_manifests():
    """AgentManifest objects for assemble + Supervisor.

    registry.agent_runtime_arn satisfies manifest.validate() (reuse mode); the
    ARN is never invoked because both nodes route to the harness kind.
    spec.depends_on is what the ASSEMBLER compiles into the plan wiring.
    """
    return {
        "extractor": AgentManifest.from_dict(
            {
                "name": "extractor",
                "registry": {
                    "agent_runtime_arn": "arn:aws:bedrock:us-west-2:1:agent-runtime/EXTRACT",
                    "protocol": "HTTP",
                },
                "contract": _EXTRACTOR_CONTRACT,
            }
        ),
        "analyzer": AgentManifest.from_dict(
            {
                "name": "analyzer",
                "registry": {
                    "agent_runtime_arn": "arn:aws:bedrock:us-west-2:1:agent-runtime/ANALYZE",
                    "protocol": "HTTP",
                },
                "contract": _ANALYZER_CONTRACT,
                "spec": {
                    "depends_on": [
                        {"from": "extractor.report_csv", "to": "source_data"}
                    ]
                },
            }
        ),
    }


RAW_MANIFESTS = {
    "extractor": {
        "name": "extractor",
        "runtime": {"backend": "callable", "entry": "loop_extractor:run"},
        "contract": _EXTRACTOR_CONTRACT,
    },
    "analyzer": {
        "name": "analyzer",
        "runtime": {"backend": "callable", "entry": "loop_analyzer:run"},
        "contract": _ANALYZER_CONTRACT,
    },
}


def _plan_model_fn(goal, precedents, directives):
    """Deterministic planner seam: the two-node topology the manifests wire."""
    return {"nodes": ["extractor", "analyzer"], "edges": [["extractor", "analyzer"]]}


@pytest.fixture
def leaf_agents(tmp_path):
    (tmp_path / "loop_extractor.py").write_text(
        "def run(prompt, inputs, context):\n"
        "    csv = 'name,score\\nalice,0.95\\nbob,0.72\\ncharlie,0.88\\n'\n"
        "    return {'report_csv': csv, 'row_count': 3}\n"
    )
    (tmp_path / "loop_analyzer.py").write_text(
        "def run(prompt, inputs, context):\n"
        "    lines = str(inputs['source_data']).strip().splitlines()[1:]\n"
        "    scores = [float(r.split(',')[1]) for r in lines]\n"
        "    return {'pass_rate': sum(1 for s in scores if s >= 0.8) / len(scores)}\n"
    )
    sys.path.insert(0, str(tmp_path))
    yield tmp_path
    sys.path.pop(0)
    sys.modules.pop("loop_extractor", None)
    sys.modules.pop("loop_analyzer", None)
    from concursus.execute.invoker import AgentInvoker

    AgentInvoker._import_callable.cache_clear()


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestGovernorLoopHarnessGlue:
    """make_harness_supervisor_factory wires the harness into loop episodes."""

    def test_loop_runs_harness_path_to_frontier_exhaust(self, leaf_agents, tmp_path):
        object_store = FileStore(root=str(tmp_path / "artifacts"))
        harness_factory = HarnessFactory(
            manifests=RAW_MANIFESTS,
            store=object_store,
            output_prefix_root="s3://loop-bucket/runs",
        )

        loop = GovernorLoop(
            "extract order data and compute the pass rate",
            _agent_manifests(),
            store=InProcessStateStore(),
            supervisor_factory=make_harness_supervisor_factory(harness_factory),
            plan_model_fn=_plan_model_fn,
            session_id="loop-e2e",
            max_rounds=4,
            backend="python",
        )

        result = loop.run({"extractor": {"query": "all orders"}})

        # The loop terminated naturally: every planned node completed.
        assert isinstance(result, GovernorResult)
        assert result.done is True
        assert result.terminated_by == "frontier_exhaust"
        assert result.completed == ["analyzer", "extractor"]
        assert result.escalated == [] and result.unmatched == []

        # The harness path actually ran: extractor's output is an ArtifactRef
        # envelope (the legacy invoke_fn path could never produce this shape
        # because no invoke_fn was supplied — it would have hit the live
        # AgentCore client instead).
        ref = result.outputs["extractor"]["report_csv"]
        assert ref["uri"] == "s3://loop-bucket/runs/loop-e2e/extractor/report_csv"
        assert ref["content_hash"].startswith("sha256:")
        assert result.outputs["extractor"]["row_count"] == 3

        # The ASSEMBLER-compiled wiring (spec.depends_on) threaded the ref, the
        # analyzer harness deref'd real bytes, and the answer came back inline.
        assert result.outputs["analyzer"]["pass_rate"] == pytest.approx(2 / 3)

        # The artifact bytes are physically in the object store.
        data = asyncio.run(object_store.get_object(ref["uri"]))
        assert b"alice,0.95" in data

    def test_glue_preserves_held_set_semantics(self, leaf_agents, tmp_path):
        """The factory accepts and forwards a held set (scheduler-configured loops)."""
        object_store = FileStore(root=str(tmp_path / "artifacts"))
        harness_factory = HarnessFactory(manifests=RAW_MANIFESTS, store=object_store)
        factory = make_harness_supervisor_factory(harness_factory)

        supervisor = factory(
            plan=None.__class__ and __import__("types").SimpleNamespace(
                order=["extractor"], wiring={"extractor": []}
            ),
            manifests=_agent_manifests(),
            store=InProcessStateStore(),
            invoke_fn=None,
            arns=None,
            session_id="held-test",
            held={"extractor"},
        )
        # The held node is never dispatched — run returns with nothing completed.
        outputs = supervisor.run({})
        assert outputs == {}

    def test_mixed_kind_routing_from_loop(self, leaf_agents, tmp_path):
        """A node WITHOUT a runtime block in the raw manifests keeps the legacy
        path: the kind_fn returns 'default' for it and 'harness' for the rest."""
        object_store = FileStore(root=str(tmp_path / "artifacts"))
        # Analyzer is absent from the harness factory's raw manifests → legacy kind.
        harness_factory = HarnessFactory(
            manifests={"extractor": RAW_MANIFESTS["extractor"]}, store=object_store
        )
        kind_fn = harness_factory.make_kind_fn()
        assert kind_fn("extractor") == "harness"
        assert kind_fn("analyzer") == "default"
