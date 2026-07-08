"""The **Supervisor** — dispatch a provisioning plan's agents in topological order.

Runtime half of Concursus: walk ``plan.order``, build each agent's invoke payload from the
external run inputs overlaid with its resolved upstream outputs (the :class:`AgentRef`
wiring), call its AgentCore endpoint, shape-check the result against the manifest's output
schema, and thread every output forward into its dependents. Outputs thread through a
:class:`~concursus.statestore.StateStore` seam (the offline :class:`InProcessStateStore` by
default, an AgentCore ``MemoryStateStore`` opt-in) so a run can *resume* — a node whose
validated output is already recorded is skipped — and every recorded ``consumes`` edge feeds
:meth:`Supervisor.context`, graph-aware upstream context. The invoke transport is injectable
(:data:`InvokeFn`); the default lazily binds boto3's ``bedrock-agentcore`` data-plane client,
so importing this module needs no AWS SDK. One stable ``runtimeSessionId`` spans every invoke
in a run (session affinity + AgentCore Memory).
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set

from .resolve import extract
from .rungraph import RunGraph
from .runindex import RunIndex
from .statestore import InProcessStateStore, StateStore

if TYPE_CHECKING:  # pragma: no cover - hints only; keeps the runtime import graph AWS-free
    from .assemble import ProvisioningPlan
    from .manifest import AgentManifest
    from .resolve import AgentRef

# The invoke transport: ``(arn, qualifier, session_id, payload_bytes) -> parsed output dict``.
InvokeFn = Callable[[str, str, str, bytes], dict]

_ARN_PLACEHOLDER = "<agent-runtime-arn>"  # stands in until the runtime is provisioned


class SchemaError(ValueError):
    """Raised when an agent's output fails to satisfy its declared output schema."""


# -- output validation ------------------------------------------------------
def validate_output(obj: Any, schema: Dict[str, Any]) -> None:
    """Shape-check an agent's output against its (JSON-Schema-ish) output schema.

    Minimal gate (no ``jsonschema``): ``obj`` must be a dict, and every *required* property
    must be present. Requiredness is either a per-property ``"required": true`` flag or
    membership in a top-level ``schema["required"]`` list. Both the nested
    ``{"properties": {...}}`` shape and a flat ``{prop: {...}}`` map are supported. Raises
    :class:`SchemaError` on any violation.
    """
    if not isinstance(obj, dict):
        raise SchemaError(f"agent output must be a JSON object/dict, got {type(obj).__name__}")
    if not isinstance(schema, dict) or not schema:
        return
    props = schema.get("properties")
    if isinstance(props, dict):
        properties = props
    else:
        properties = {k: v for k, v in schema.items() if k != "required"}
    declared = schema.get("required")
    required: Set[str] = set(declared) if isinstance(declared, list) else set()
    for prop, subschema in properties.items():
        if isinstance(subschema, dict) and subschema.get("required") is True:
            required.add(prop)
    missing = sorted(p for p in required if p not in obj)
    if missing:
        raise SchemaError(
            f"agent output missing required field(s): {missing} (present: {sorted(obj)})"
        )


# -- default invoke transport -----------------------------------------------
def _default_invoke_fn(arn: str, qualifier: str, session_id: str, payload: bytes) -> dict:
    """Default :data:`InvokeFn`: invoke a live AgentCore runtime endpoint via boto3.

    boto3 is imported lazily (the optional ``[agentcore]`` extra) so this module — and every
    unit test that injects a fake transport — imports fine without the AWS SDK.
    """
    try:
        import boto3  # lazy: only needed for a real, over-the-wire invoke
    except ImportError as exc:  # pragma: no cover - exercised only without boto3
        raise RuntimeError(
            "invoking a live AgentCore runtime requires boto3 — install the 'agentcore' "
            "extra (pip install concursus[agentcore]) or pass invoke_fn=..."
        ) from exc
    client = boto3.client("bedrock-agentcore")  # data plane
    response = client.invoke_agent_runtime(
        agentRuntimeArn=arn,
        runtimeSessionId=session_id,
        payload=payload,
        qualifier=qualifier,
    )
    body = response["response"].read()  # streaming response
    return json.loads(body)


def _new_session_id() -> str:
    """Generate a stable, >=33-char ``runtimeSessionId`` (AgentCore requires >= 33)."""
    return uuid.uuid4().hex + uuid.uuid4().hex


# -- supervisor -------------------------------------------------------------
class Supervisor:
    """Drive a :class:`~concursus.assemble.ProvisioningPlan` to completion, offline or live.

    Walks ``plan.order`` (a topological order); for each node it assembles the invoke payload
    from the external run inputs overlaid with its resolved upstream outputs (``plan.wiring``),
    calls the injected :data:`InvokeFn`, validates the result against the manifest's output
    schema, and threads it forward. The plan is duck-typed on ``.order`` and ``.wiring`` only.
    """

    def __init__(
        self,
        plan: "ProvisioningPlan",
        manifests: Dict[str, "AgentManifest"],
        *,
        invoke_fn: Optional[InvokeFn] = None,
        session_id: Optional[str] = None,
        arns: Optional[Dict[str, str]] = None,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self._plan = plan
        self._manifests: Dict[str, "AgentManifest"] = dict(manifests)
        self._invoke_fn: InvokeFn = invoke_fn or _default_invoke_fn
        self._session_id = session_id or _new_session_id()
        self._store: StateStore = state_store or InProcessStateStore()

        supplied = dict(arns or {})
        self._arns: Dict[str, str] = {}
        for node, manifest in self._manifests.items():
            self._arns[node] = (
                supplied.get(node)
                or manifest.registry.get("agent_runtime_arn")
                or _ARN_PLACEHOLDER
            )
        for node, arn in supplied.items():  # arns for nodes lacking a manifest
            self._arns.setdefault(node, arn)

    @property
    def session_id(self) -> str:
        """The stable per-run ``runtimeSessionId`` shared across every invoke."""
        return self._session_id

    # -- payload assembly ---------------------------------------------------
    def _external_inputs(
        self, node: str, inputs: Dict[str, Any], wiring: List["AgentRef"]
    ) -> Dict[str, Any]:
        """External inputs for ``node``: its ``inputs[node]`` block, or — for a source node
        (no inbound wiring) — the top-level ``inputs`` mapping."""
        explicit = inputs.get(node)
        if isinstance(explicit, dict):
            return dict(explicit)
        if not wiring:
            return dict(inputs)
        return {}

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Dict]:
        """Invoke every agent in topological order; return ``{node_id: output_dict}``.

        For each node, a node already in ``self._store.completed()`` is skipped (resume — its
        validated output was recorded on a prior run). Otherwise the payload starts from
        :meth:`_external_inputs`, then each wiring :class:`AgentRef` overlays
        ``payload[ref.input_name] = extract(self._store.get(ref.producer), ref.path)`` — the
        upstream field the resolver promised. The invoke result is :func:`validate_output`-checked
        against the manifest's output schema, then ``put`` into the store (with its ``producer`` /
        ``consumes`` edges / ``schema`` tag) and so made available to downstream nodes.
        """
        for node in self._plan.order:
            if node in self._store.completed():
                continue  # resume: this node already has a recorded validated output
            manifest = self._manifests.get(node)
            wiring: List["AgentRef"] = list(self._plan.wiring.get(node, []))
            payload = self._external_inputs(node, inputs, wiring)
            for ref in wiring:
                payload[ref.input_name] = extract(self._store.get(ref.producer), ref.path)

            arn = self._arns.get(node, _ARN_PLACEHOLDER)
            qualifier = (
                str(manifest.registry.get("qualifier", "DEFAULT"))
                if manifest is not None
                else "DEFAULT"
            )
            result = self._invoke_fn(
                arn, qualifier, self._session_id, json.dumps(payload).encode()
            )
            validate_output(result, manifest.output_schema if manifest else {})
            self._store.put(
                node,
                result,
                meta={
                    "producer": node,
                    "consumes": [f"{r.producer}:{r.path}" for r in wiring],
                    "schema": manifest.name if manifest else None,
                },
            )
        return {node: self._store.get(node) for node in self._plan.order}

    # -- graph-aware context (v2) -------------------------------------------
    def context(self, node: str) -> Dict[str, dict]:
        """Transitive upstream context for ``node``: ``{producer: latest output}``.

        Rebuilds the run graph from the store's recorded ``consumes`` edges and returns the
        latest validated output of every node in :meth:`~concursus.rungraph.RunGraph.context_order`
        (its producers, nearest-first, bounded) — shared upstream state as a query rather than
        point-to-point wiring.
        """
        graph = RunGraph.from_records(self._store.records())
        return {n: self._store.get(n) for n in graph.context_order(node)}

    def index(self) -> RunIndex:
        """A :class:`~concursus.runindex.RunIndex` over the run's log — Folgezettel-tree
        traversal (retries / fan-out / branches) plus metadata queries (``status`` / ``schema`` /
        ``record_type`` / ``producer``) without scanning payloads."""
        return RunIndex.from_store(self._store)
