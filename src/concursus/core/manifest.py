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
from typing import Any, Dict, List, Tuple

from ..build.trust import TrustGrade


class ManifestError(ValueError):
    """Raised on an invalid agent manifest."""


#: The newest ``.agent.yaml`` contract revision this compiler knows how to compile.
#: A manifest may pin an OPTIONAL ``contract_version``; :meth:`AgentManifest.validate`
#: fails closed if that pin is *greater* than this constant (the manifest was authored
#: against a newer compiler). Bump this only when the manifest schema itself changes.
MAX_SUPPORTED_CONTRACT_VERSION = 1

#: The valid literals for an :class:`AgentManifest`'s OPTIONAL ``context_mode`` — the per-agent
#: content-reuse policy. ``"reuse"`` = this node's prior content may be reused; ``"isolation"`` =
#: always re-provision (never reuse); ``""`` = INHERIT (defer to a team/group default, then a
#: hardcoded ``"isolation"`` floor — see
#: :func:`~concursus.core.resolve.resolve_context_mode`).
CONTEXT_MODES = ("", "reuse", "isolation")


#: The invoker backends `execute/invoker.py` can dispatch to. ``validate`` fails closed on anything
#: else, so a typo in a manifest is a compile-time error rather than a mid-run
#: ``Unsupported backend`` from the invoker.
RUNTIME_BACKENDS = ("callable", "agentcore", "http", "strands", "api")


@dataclass(frozen=True)
class AgentRuntime:
    """OPTIONAL declaration of HOW to invoke this agent — the harness path's addressing.

    Until now this block existed only in the RAW manifest dicts
    ``execute/harness_factory.HarnessFactory`` consumes, and was **absent from the typed manifest
    entirely** — which is why callers had to build and hand-maintain two representations of the same
    agent, and why a field required by one could silently disagree with the other.

    ``backend`` is the one key every backend needs, so it is typed and validated. The rest is
    deliberately left as an open ``config`` mapping: the backends' remaining keys are heterogeneous
    (``entry`` / ``client`` for callable, ``agent_id`` + ``alias_id`` for agentcore, ``endpoint`` for
    http/api), the invoker already validates them per-backend at dispatch, and modelling that union
    here would either lose information or duplicate the invoker's own checks.

    The empty default is **falsy**, so a manifest with no ``runtime:`` block behaves exactly as before
    — including routing to the legacy ``_dispatch`` path via ``HarnessFactory.make_kind_fn``.
    """

    backend: str = ""
    config: Dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:  # empty => falsy => the legacy dispatch path, unchanged
        return bool(self.backend or self.config)

    def to_dict(self) -> Dict[str, Any]:
        """Back to the FLAT raw shape the harness stack reads (``{"backend": ..., **config}``).

        Round-tripping matters: this is what lets a caller hold one typed manifest and derive the raw
        dict, instead of maintaining both by hand.
        """
        if not self:
            return {}
        return {"backend": self.backend, **dict(self.config)}

    @classmethod
    def from_obj(cls, obj: Any, *, agent: str = "") -> "AgentRuntime":
        """Build from ``None`` / a flat ``dict`` / an :class:`AgentRuntime`.

        Accepts the flat authored shape (``{backend: "callable", entry: "m:f"}``) and splits
        ``backend`` off from the rest, so an authored manifest needs no nesting it did not have.
        """
        if obj is None:
            return cls()
        if isinstance(obj, AgentRuntime):
            return obj
        if not isinstance(obj, dict):
            raise ManifestError(
                f"{agent}: runtime must be a mapping with a 'backend' (got {type(obj).__name__})"
            )
        if not obj:
            return cls()
        backend = obj.get("backend")
        if backend is None or not str(backend).strip():
            raise ManifestError(
                f"{agent}: runtime must declare a 'backend' (one of {list(RUNTIME_BACKENDS)!r})"
            )
        return cls(
            backend=str(backend),
            config={k: v for k, v in obj.items() if k != "backend"},
        )


@dataclass(frozen=True)
class AgentCapabilities:
    """OPTIONAL, purely-declarative inventory of what an agent's *runtime* provides.

    Three sequences of opaque author-declared labels:

    - ``features`` — runtime features/behaviours this agent enables.
    - ``tools`` — tool ids the agent's runtime is allowed to call.
    - ``egress_hosts`` — network hosts the runtime may reach.

    The compiler *stores and shape-validates* this block but takes no action on it — it
    is documentation/attestation, not a runtime governor. The empty default declares
    nothing extra and is **falsy** (``bool(AgentCapabilities()) is False``), so a manifest
    with no ``capabilities:`` block behaves byte-for-byte as before everywhere a manifest
    is inspected (notably the governor registry's capability-derivation fallback).
    """

    features: Tuple[str, ...] = ()
    tools: Tuple[str, ...] = ()
    egress_hosts: Tuple[str, ...] = ()

    def __bool__(self) -> bool:  # empty => falsy => governor/default paths unchanged
        return bool(self.features or self.tools or self.egress_hosts)

    def to_dict(self) -> Dict[str, List[str]]:
        return {
            "features": list(self.features),
            "tools": list(self.tools),
            "egress_hosts": list(self.egress_hosts),
        }

    @classmethod
    def from_obj(cls, obj: Any, *, agent: str = "") -> "AgentCapabilities":
        """Build (and shape-validate) from ``None`` / a ``dict`` / an ``AgentCapabilities``.

        Raises :class:`ManifestError` on an unknown key or a non-list-of-strings value.
        """
        if obj is None:
            return cls()
        if isinstance(obj, AgentCapabilities):
            return obj
        if not isinstance(obj, dict):
            raise ManifestError(
                f"{agent}: capabilities must be a mapping of "
                f"{{features?, tools?, egress_hosts?}} (got {type(obj).__name__})"
            )
        unknown = set(obj) - {"features", "tools", "egress_hosts"}
        if unknown:
            raise ManifestError(
                f"{agent}: capabilities has unknown key(s) {sorted(unknown)!r}; "
                "allowed: 'features', 'tools', 'egress_hosts'"
            )

        def _seq(key: str) -> Tuple[str, ...]:
            v = obj.get(key, ())
            # a bare string is a common mistake for a one-element list — reject it.
            if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
                raise ManifestError(
                    f"{agent}: capabilities.{key} must be a list of strings (got "
                    f"{type(v).__name__})"
                )
            return tuple(str(x) for x in v)

        return cls(features=_seq("features"), tools=_seq("tools"), egress_hosts=_seq("egress_hosts"))


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
        capabilities: OPTIONAL typed :class:`AgentCapabilities` declaring the
            features/tools/egress-hosts this agent's runtime provides. The empty default
            declares nothing extra and is falsy, so an absent ``capabilities:`` block is
            byte-for-byte identical to before.
        contract_version: OPTIONAL manifest-schema revision this ``.agent.yaml`` was
            authored against; defaults to :data:`MAX_SUPPORTED_CONTRACT_VERSION`. ``validate``
            fails closed if it exceeds what this compiler supports.
        context_mode: OPTIONAL per-agent content-reuse policy — one of ``"reuse"`` (this node's
            already-stood-up content may be reused), ``"isolation"`` (always re-provision this node,
            never reuse a prior deployment's content), or ``""`` (INHERIT — the default; defer to a
            team/group default, then a hardcoded ``"isolation"`` floor, via
            :func:`~concursus.core.resolve.resolve_context_mode`). The empty default is
            purely inherit and takes NO action on its own, so an absent ``context_mode:`` is
            byte-for-byte identical to before.
    """

    name: str
    registry: Dict[str, Any] = field(default_factory=dict)
    contract: Dict[str, Any] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW
    side_effecting: bool = False
    escalate_boundary: str = ""
    capabilities: AgentCapabilities = field(default_factory=AgentCapabilities)
    contract_version: int = MAX_SUPPORTED_CONTRACT_VERSION
    context_mode: str = ""
    # -- harness-path blocks ------------------------------------------------
    # These three existed ONLY in the raw manifest dicts the HarnessFactory consumes, so a caller had
    # to build and hand-maintain a second representation of the same agent. Modelling them here makes
    # the typed manifest the single source; `to_harness_dict()` derives the raw shape from it.
    runtime: AgentRuntime = field(default_factory=AgentRuntime)
    monitor: Dict[str, Any] = field(default_factory=dict)
    output_mapping: Dict[str, str] = field(default_factory=dict)

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
        """Optional trust-tiered coaching context (, SPIKE B ``B1``).

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

    # -- harness interop ----------------------------------------------------
    def to_harness_dict(self) -> Dict[str, Any]:
        """The RAW dict shape ``execute/harness_factory.HarnessFactory`` consumes.

        The convergence step. ``HarnessFactory`` takes raw dicts because the typed manifest had no
        ``runtime`` / ``monitor`` / ``output_mapping``; with those modelled, a caller can hold ONE
        typed manifest and derive the raw view instead of authoring both and hoping they agree.

        Emits only the keys the harness stack actually reads (``name``, ``runtime``, ``contract``,
        ``monitor``, ``output_mapping``, ``side_effecting``), and omits the empty ones so the result is
        identical in shape to what hand-written raw manifests look like today.
        """
        out: Dict[str, Any] = {"name": self.name, "contract": dict(self.contract)}
        if self.runtime:
            out["runtime"] = self.runtime.to_dict()
        if self.monitor:
            out["monitor"] = dict(self.monitor)
        if self.output_mapping:
            out["output_mapping"] = dict(self.output_mapping)
        if self.side_effecting:
            out["side_effecting"] = True
        return out

    # -- validation ---------------------------------------------------------
    def validate(self) -> "AgentManifest":
        """Enforce the manifest contract; returns self.

        Rules: a name is required; an **output schema is mandatory** (it is what makes dependency
        resolution meaningful for agents); and the registry must name either a ``container_uri`` (to
        provision) or an ``agent_runtime_arn`` (to reuse) — **unless** a ``runtime`` block declares a
        backend that is not AgentCore-hosted, in which case there is no AgentCore runtime to name.
        """
        if not self.name or not str(self.name).strip():
            raise ManifestError("manifest requires a non-empty 'name'")
        # Fail closed on an unknown backend, so a typo is caught at compile time rather than
        # surfacing mid-run as the invoker's `Unsupported backend`.
        if self.runtime and self.runtime.backend not in RUNTIME_BACKENDS:
            raise ManifestError(
                f"{self.name}: runtime.backend must be one of {list(RUNTIME_BACKENDS)!r} "
                f"(got {self.runtime.backend!r})"
            )
        reg = self.registry
        # An in-process / self-hosted agent (callable, http, strands, api) genuinely has no AgentCore
        # runtime to name, so demanding one forced callers to FABRICATE a container_uri purely to pass
        # this gate. The requirement now applies only where it means something: no runtime block at
        # all (the legacy _dispatch path, which invokes an ARN), or an explicitly agentcore-backed one.
        needs_agentcore_hosting = (not self.runtime) or self.runtime.backend == "agentcore"
        if needs_agentcore_hosting and not (
            reg.get("container_uri") or reg.get("agent_runtime_arn")
        ):
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
        # Fail closed on a manifest authored against a newer compiler than this one.
        if not isinstance(self.contract_version, int) or isinstance(self.contract_version, bool):
            raise ManifestError(
                f"{self.name}: contract_version must be an int (got "
                f"{type(self.contract_version).__name__})"
            )
        if self.contract_version > MAX_SUPPORTED_CONTRACT_VERSION:
            raise ManifestError(
                f"{self.name}: contract_version {self.contract_version} exceeds this "
                f"compiler's MAX_SUPPORTED_CONTRACT_VERSION {MAX_SUPPORTED_CONTRACT_VERSION} "
                "— upgrade the compiler or lower the manifest's contract_version"
            )
        # Shape-check the capabilities block (a no-op for the empty default).
        if not isinstance(self.capabilities, AgentCapabilities):
            raise ManifestError(
                f"{self.name}: capabilities must be an AgentCapabilities (got "
                f"{type(self.capabilities).__name__})"
            )
        # Reject an invalid content-reuse policy literal (the empty default is INHERIT, always OK).
        if self.context_mode not in CONTEXT_MODES:
            raise ManifestError(
                f"{self.name}: context_mode must be one of {list(CONTEXT_MODES)!r} "
                f"('' = inherit) (got {self.context_mode!r})"
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
        capabilities = AgentCapabilities.from_obj(
            data.get("capabilities"), agent=str(data.get("name", ""))
        )
        raw_version = data.get("contract_version")
        contract_version = (
            MAX_SUPPORTED_CONTRACT_VERSION if raw_version is None else raw_version
        )
        return cls(
            name=data.get("name", ""),
            registry=dict(data.get("registry", {})),
            contract=dict(data.get("contract", {})),
            spec=dict(data.get("spec", {})),
            trust_seed=trust_seed,
            side_effecting=bool(data.get("side_effecting", False)),
            escalate_boundary=str(data.get("escalate_boundary", "") or ""),
            capabilities=capabilities,
            contract_version=contract_version,
            context_mode=str(data.get("context_mode", "") or ""),
            runtime=AgentRuntime.from_obj(
                data.get("runtime"), agent=str(data.get("name", ""))
            ),
            monitor=dict(data.get("monitor", {}) or {}),
            output_mapping={
                str(k): str(v) for k, v in (data.get("output_mapping", {}) or {}).items()
            },
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
