"""The ``.agent.yaml`` manifest model — a single agent's interface + AgentCore binding.

An agent manifest declares three things:

- ``registry`` — how the agent is hosted on AgentCore (a container image to provision, or an
  existing ``agent_runtime_arn`` to reuse), plus its protocol and endpoint qualifier.
- ``contract`` — the agent's typed interface: the input fields Concursus injects into the
  invoke payload, and the **output JSON Schema** (required — it is the dependency resolver's
  type gate).
- ``spec`` — optional author-declared edges (``depends_on``) wiring upstream outputs to this
  agent's inputs.

This is the declarative core Concursus compiles down to ``CreateAgentRuntime`` /
``InvokeAgentRuntime`` calls; the runtime/assembler that consumes it is on the roadmap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from ..build.trust import TrustGrade


class ManifestError(ValueError):
    """Raised on an invalid agent manifest."""


@dataclass
class AgentManifest:
    """Parsed ``<name>.agent.yaml``.

    Attributes:
        name: The agent/node id (unique within an :class:`~concursus.dag.AgentDAG`).
        registry: Hosting binding — e.g. ``container_uri`` + ``role_arn`` + ``network_mode``
            + ``protocol`` (``HTTP`` | ``MCP`` | ``A2A``) + ``qualifier``, or
            ``agent_runtime_arn`` to reuse an already-deployed runtime.
        contract: ``{"inputs": {...}, "outputs": {<json-schema>}}``.
        spec: Optional ``{"depends_on": [{"from": "producer.field", "to": "input"}]}``.
        trust_seed: The author-declared create-time autonomy of this node (see
            :class:`~concursus.trust.TrustGrade`); consulted ONCE at provision time by the
            deploy gate, never per-invocation. Defaults to ``L0_SHADOW``.
        side_effecting: Whether this agent takes real-world side effects (writes, sends, calls
            external systems). Only side-effecting agents are gated at deploy time; the default
            ``False`` keeps a read-only agent's deploy ungated.
        escalate_boundary: An opaque author-declared label naming who/what a held deploy should
            escalate to (informational; the compiler stores but does not act on it).
    """

    name: str
    registry: Dict[str, Any] = field(default_factory=dict)
    contract: Dict[str, Any] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW
    side_effecting: bool = False
    escalate_boundary: str = ""

    # -- accessors ----------------------------------------------------------
    @property
    def protocol(self) -> str:
        return str(self.registry.get("protocol", "HTTP")).upper()

    @property
    def inputs(self) -> Dict[str, Any]:
        return dict(self.contract.get("inputs", {}))

    @property
    def output_schema(self) -> Dict[str, Any]:
        return dict(self.contract.get("outputs", {}))

    @property
    def context(self) -> Dict[str, Any]:
        """Optional trust-tiered coaching context (FZ 35e2b1a1b2a1, SPIKE B ``B1``).

        A free-form ``contract.context`` block — ``{sop?, tools_available?, guardrails?,
        examples?, tool_calls?}`` — that a payload-tier overlay projects down per the bound agent's
        earned trust (:func:`~concursus.governor.scheduler.project_context`). Absent (the
        default) => an empty dict, so the invoke payload is byte-for-byte unchanged. This is
        *dimension 2/3* of the payload contract; *dimension 1* (I/O + acceptance) lives in
        ``inputs``/``outputs`` and is never tiered.
        """
        return dict(self.contract.get("context", {}))

    @property
    def depends_on(self) -> List[Dict[str, str]]:
        return list(self.spec.get("depends_on", []))

    # -- validation ---------------------------------------------------------
    def validate(self) -> "AgentManifest":
        """Enforce the manifest contract; returns self.

        Rules: a name is required; the registry must name either a ``container_uri`` (to
        provision) or an ``agent_runtime_arn`` (to reuse); and an **output schema is
        mandatory** — it is what makes dependency resolution meaningful for agents.
        """
        if not self.name or not str(self.name).strip():
            raise ManifestError("manifest requires a non-empty 'name'")
        reg = self.registry
        if not (reg.get("container_uri") or reg.get("agent_runtime_arn")):
            raise ManifestError(
                f"{self.name}: registry must set 'container_uri' (to provision) or "
                "'agent_runtime_arn' (to reuse an existing AgentCore Runtime)"
            )
        if self.protocol not in ("HTTP", "MCP", "A2A"):
            raise ManifestError(
                f"{self.name}: protocol must be HTTP, MCP, or A2A (got {self.protocol!r})"
            )
        if not self.output_schema:
            raise ManifestError(
                f"{self.name}: contract.outputs (a JSON Schema) is required — it is the "
                "dependency resolver's type gate"
            )
        return self

    # -- construction -------------------------------------------------------
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentManifest":
        raw_seed = data.get("trust_seed")
        try:
            trust_seed = (
                TrustGrade.L0_SHADOW if raw_seed is None else TrustGrade.parse(raw_seed)
            )
        except ValueError as exc:
            raise ManifestError(
                f"{data.get('name', '')}: invalid trust_seed {raw_seed!r} — expected a "
                "TrustGrade (0-3, or a name like 'L0_SHADOW')"
            ) from exc
        return cls(
            name=data.get("name", ""),
            registry=dict(data.get("registry", {})),
            contract=dict(data.get("contract", {})),
            spec=dict(data.get("spec", {})),
            trust_seed=trust_seed,
            side_effecting=bool(data.get("side_effecting", False)),
            escalate_boundary=str(data.get("escalate_boundary", "") or ""),
        )

    @classmethod
    def from_yaml(cls, path: str) -> "AgentManifest":
        """Load a ``.agent.yaml`` file. The name defaults to the file stem if unset."""
        import os

        import yaml  # lazy: only needed when actually loading a manifest file

        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        if not data.get("name"):
            data["name"] = os.path.basename(path).split(".", 1)[0]
        return cls.from_dict(data)
