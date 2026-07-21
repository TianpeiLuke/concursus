"""Tests for the opt-in Strategy/Registry dispatch seams (supervisor + build).

Two thin seams, both DEFAULT-OFF and byte-for-byte identical to today's single path:

* :class:`Supervisor` routes ``run`` through a per-node-kind executor registry
  (``NODE_EXECUTORS`` + ``_route_dispatch``). The ``default`` kind delegates verbatim to
  ``_dispatch``; a caller can register a custom node-kind handler and select it per node.
* :meth:`RuntimeBuilderFactory.synthesize` routes through a per-runtime-kind builder registry
  (``RUNTIME_BUILDERS``). The ``default`` kind is today's exact compile path; a manifest can opt
  in to a custom builder via ``registry.runtime_kind``.

These tests assert a newly-registered custom kind is invoked while the DEFAULT kind is unchanged.
"""

import json
import types

from concursus import AgentDAG, AgentManifest
from concursus.core.resolve import resolve_edges
from concursus.build.build import (
    BuildPlanEntry,
    RuntimeBuilderFactory,
    _default_runtime_builder,
)
from concursus.execute.supervisor import (
    NODE_EXECUTORS,
    Supervisor,
    _DEFAULT_NODE_KIND,
    _default_node_executor,
)


# -- supervisor fixtures (mirror test_supervisor.py) ------------------------
def _chain():
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
                    "outputs": {"document": {"type": "string", "required": True}},
                },
            }
        ),
        "summarize": AgentManifest.from_dict(
            {
                "name": "summarize",
                "registry": {"container_uri": "x", "protocol": "HTTP"},
                "contract": {
                    "inputs": {"document": {"type": "string"}},
                    "outputs": {
                        "properties": {"summary": {"type": "string"}},
                        "required": ["summary"],
                    },
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
                    "outputs": {"critique": {"type": "string", "required": True}},
                },
                "spec": {"depends_on": [{"from": "summarize.summary", "to": "summary"}]},
            }
        ),
    }
    return dag, manifests


def _plan(dag, manifests):
    return types.SimpleNamespace(
        order=dag.topological_sort(),
        wiring=resolve_edges(dag, manifests),
    )


class FakeInvoker:
    def __init__(self, outputs_by_arn):
        self.outputs_by_arn = outputs_by_arn
        self.calls = []

    def __call__(self, arn, qualifier, session_id, payload_bytes):
        self.calls.append((arn, qualifier, session_id, json.loads(payload_bytes)))
        return dict(self.outputs_by_arn[arn])


_ARNS = {"ingest": "arn:ingest", "summarize": "arn:summarize", "critique": "arn:critique"}


def _fake_outputs():
    return {
        "arn:ingest": {"document": "DOC"},
        "arn:summarize": {"summary": "SUM"},
        "arn:critique": {"critique": "OK"},
    }


# -- supervisor: default kind is unchanged ----------------------------------
def test_shipped_node_registry_carries_only_the_default_kind():
    assert set(NODE_EXECUTORS) == {_DEFAULT_NODE_KIND}
    assert NODE_EXECUTORS[_DEFAULT_NODE_KIND] is _default_node_executor


def test_default_dispatch_unchanged_when_no_custom_kind():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    sup = Supervisor(_plan(dag, manifests), manifests, invoke_fn=fake, arns=_ARNS)

    outputs = sup.run({"uri": "s3://doc"})

    # every node ran through the default _dispatch path -> one invoke each, outputs threaded.
    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["summarize"] == {"summary": "SUM"}
    assert len(fake.calls) == 3


# -- supervisor: a custom node-kind handler is invoked ----------------------
def test_custom_node_kind_handler_invoked_while_default_kind_unchanged():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())

    seen = []

    def custom_executor(supervisor, node, inputs, wiring):
        # A uniform handler: record that we ran, then delegate to the default so the store still
        # gets a validated output (keeps the downstream chain resolvable).
        seen.append(node)
        _default_node_executor(supervisor, node, inputs, wiring)

    # Route ONLY 'summarize' to the custom kind; everything else uses the default kind.
    def node_kind_fn(node):
        return "custom" if node == "summarize" else _DEFAULT_NODE_KIND

    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        node_executors={"custom": custom_executor},
        node_kind_fn=node_kind_fn,
    )
    outputs = sup.run({"uri": "s3://doc"})

    # the custom handler fired for exactly the selected node...
    assert seen == ["summarize"]
    # ...and the default kind produced identical results for the rest (chain completed).
    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert outputs["critique"] == {"critique": "OK"}
    # the shipped global registry is not mutated by per-instance registration.
    assert set(NODE_EXECUTORS) == {_DEFAULT_NODE_KIND}


def test_unregistered_node_kind_falls_back_to_default():
    dag, manifests = _chain()
    fake = FakeInvoker(_fake_outputs())
    # a selector naming a kind with no registered handler must fall back to the default handler.
    sup = Supervisor(
        _plan(dag, manifests),
        manifests,
        invoke_fn=fake,
        arns=_ARNS,
        node_kind_fn=lambda node: "does-not-exist",
    )
    outputs = sup.run({"uri": "s3://doc"})
    assert set(outputs) == {"ingest", "summarize", "critique"}
    assert len(fake.calls) == 3


# -- build: default runtime kind unchanged + custom kind invoked ------------
def _build_manifest(**registry):
    reg = {
        "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
        "protocol": "HTTP",
        "entry": "agents.summarize:run",
        "role_arn": "arn:aws:iam::123456789012:role/agent",
    }
    reg.update(registry)
    return AgentManifest.from_dict(
        {
            "name": "summarize",
            "registry": reg,
            "contract": {
                "inputs": {"document": {"type": "string"}},
                "outputs": {"summary": {"type": "string"}},
            },
        }
    )


def test_default_runtime_kind_matches_direct_default_builder():
    m = _build_manifest()
    via_factory = RuntimeBuilderFactory.synthesize(m).to_dict()
    direct = _default_runtime_builder(m).to_dict()
    # routing through the registry with no custom kind is byte-for-byte the default compile path.
    assert via_factory == direct


def test_custom_runtime_kind_builder_invoked_while_default_unchanged():
    calls = []

    def custom_builder(m, *, account=None, region=None):
        calls.append(m.name)
        return BuildPlanEntry(
            name=m.name,
            build_mode="custom",
            wrapper=None,
            dockerfile=None,
            execution_role=None,
            create_agent_runtime={"marker": "custom"},
            invoke={},
            ecr_repo=None,
            fingerprint="custom-fp",
        )

    custom_m = _build_manifest(runtime_kind="custom")
    entry = RuntimeBuilderFactory.synthesize(
        custom_m, runtime_builders={"custom": custom_builder}
    )
    assert calls == ["summarize"]
    assert entry.build_mode == "custom"
    assert entry.create_agent_runtime == {"marker": "custom"}

    # a manifest WITHOUT runtime_kind still uses the default builder even when a custom kind is
    # registered — the default path is unchanged.
    default_m = _build_manifest()
    default_entry = RuntimeBuilderFactory.synthesize(
        default_m, runtime_builders={"custom": custom_builder}
    )
    assert default_entry.build_mode == "container"
    assert calls == ["summarize"]  # custom builder not called for the default-kind manifest


def test_unregistered_runtime_kind_falls_back_to_default():
    # a manifest declaring a runtime_kind with no registered builder falls back to the default.
    m = _build_manifest(runtime_kind="never-registered")
    entry = RuntimeBuilderFactory.synthesize(m)
    assert entry.build_mode == "container"
    assert entry.to_dict() == _default_runtime_builder(_build_manifest()).to_dict()
