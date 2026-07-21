"""The runtime **builder** — synthesize per-agent packaging + ``CreateAgentRuntime`` params.

Given an :class:`~concursus.manifest.AgentManifest`, produce a :class:`BuildPlanEntry`: the
serving wrapper (``app.py`` source), the container ``Dockerfile``, an IAM execution role
(policy + trust), and the ``create_agent_runtime`` request dict. One template per serving
protocol (HTTP/MCP/A2A); an already-built image or an existing runtime ARN is registered
as-is. This layer is pure and offline — it renders artifacts and parameter dicts, it never
imports boto3 or calls AWS (deploy consumes the plan later).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Protocol, Tuple

if TYPE_CHECKING:  # pragma: no cover - hints only; keeps the runtime import graph pure
    from ..core.manifest import AgentManifest

# HTTP serves POST /invocations + GET /ping on 8080; MCP serves /mcp on 8000; A2A serves the
# JSON-RPC 2.0 root on 9000. The serving contract fixes one port per protocol.
PORTS = {"HTTP": 8080, "MCP": 8000, "A2A": 9000}

_DEFAULT_BASE_IMAGE = "python:3.12-slim"
_IMAGE_PLACEHOLDER = "<image-uri>"  # filled at deploy, after the image is built + pushed
_ROLE_PLACEHOLDER = "<execution-role-arn>"  # filled at deploy from the synthesized role


class BuildError(ValueError):
    """Raised when an agent cannot be compiled into a runtime build plan."""


# -- helpers ----------------------------------------------------------------
def _runtime_name(name: str) -> str:
    """Sanitize an agent id into a valid ``agentRuntimeName`` (alphanumeric + underscore)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", str(name))


def _split_entry(entry: Optional[str]) -> Tuple[str, str]:
    """Split a ``"module:function"`` entry ref into ``(module, function)``."""
    if not entry or ":" not in str(entry):
        raise BuildError(
            "registry.entry must be 'module:function' to synthesize a serving wrapper "
            f"(got {entry!r})"
        )
    module, func = str(entry).split(":", 1)
    if not module.strip() or not func.strip():
        raise BuildError(f"registry.entry must be 'module:function' (got {entry!r})")
    return module.strip(), func.strip()


def _network_configuration(reg: Dict[str, Any]) -> dict:
    """Build ``networkConfiguration`` from ``registry`` (default ``PUBLIC``)."""
    explicit = reg.get("networkConfiguration")
    if isinstance(explicit, dict) and explicit:
        return dict(explicit)
    mode = str(reg.get("network_mode", "PUBLIC")).upper()
    if mode == "VPC":
        cfg = reg.get("network_mode_config") or reg.get("networkModeConfig") or {}
        return {
            "networkMode": "VPC",
            "networkModeConfig": {
                "securityGroups": list(
                    cfg.get("securityGroups", cfg.get("security_groups", []))
                ),
                "subnets": list(cfg.get("subnets", [])),
                "requireServiceS3Endpoint": bool(
                    cfg.get(
                        "requireServiceS3Endpoint",
                        cfg.get("require_service_s3_endpoint", False),
                    )
                ),
            },
        }
    return {"networkMode": mode}


def _authorizer_configuration(reg: Dict[str, Any]) -> Optional[dict]:
    """Build the optional ``authorizerConfiguration`` (custom JWT) from ``registry.auth``."""
    auth = reg.get("auth")
    if not auth:
        return None
    if "customJWTAuthorizer" in auth:
        return dict(auth)
    disc = auth.get("discoveryUrl") or auth.get("discovery_url")
    clients = auth.get("allowedClients") or auth.get("allowed_clients") or []
    if disc:
        return {"customJWTAuthorizer": {"discoveryUrl": disc, "allowedClients": list(clients)}}
    return dict(auth)


def _lifecycle_configuration(reg: Dict[str, Any]) -> Optional[dict]:
    """Build the optional ``lifecycleConfiguration`` from ``registry.lifecycle``."""
    life = reg.get("lifecycle")
    if not life:
        return None
    out: Dict[str, Any] = {}
    idle = life.get("idleRuntimeSessionTimeout", life.get("idle_runtime_session_timeout"))
    maxl = life.get("maxLifetime", life.get("max_lifetime"))
    if idle is not None:
        out["idleRuntimeSessionTimeout"] = idle
    if maxl is not None:
        out["maxLifetime"] = maxl
    return out or None


def _container_create_request(
    m: "AgentManifest", image_uri: Optional[str], protocol: str
) -> dict:
    """Assemble the ``create_agent_runtime`` param dict for a container-hosted agent."""
    reg = m.registry
    req: Dict[str, Any] = {
        "agentRuntimeName": _runtime_name(m.name),
        "agentRuntimeArtifact": {
            "containerConfiguration": {"containerUri": image_uri or _IMAGE_PLACEHOLDER}
        },
        "roleArn": reg.get("role_arn", _ROLE_PLACEHOLDER),
        "networkConfiguration": _network_configuration(reg),
        "protocolConfiguration": {"serverProtocol": protocol},
    }
    auth = _authorizer_configuration(reg)
    if auth:
        req["authorizerConfiguration"] = auth
    life = _lifecycle_configuration(reg)
    if life:
        req["lifecycleConfiguration"] = life
    return req


# -- IAM execution role -----------------------------------------------------
def render_execution_role(
    m: "AgentManifest",
    account: Optional[str],
    region: Optional[str],
    *,
    container: bool,
) -> dict:
    """Render the agent's AgentCore execution role as ``{"policy": ..., "trust": ...}``.

    ``container=True`` grants the ECR pull + auth-token + workload-access-token statements a
    container runtime needs; a codezip/direct role omits those. Unknown ``account``/``region``
    fall back to the literal placeholders ``ACCOUNT_ID`` / ``REGION`` so the plan is previewable.
    """
    acct = account or "ACCOUNT_ID"
    reg = region or "REGION"
    name = _runtime_name(m.name)

    statements = []
    if container:
        statements.append(
            {
                "Sid": "ECRImageAccess",
                "Effect": "Allow",
                "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
                "Resource": [f"arn:aws:ecr:{reg}:{acct}:repository/*"],
            }
        )
    statements.append(
        {
            "Effect": "Allow",
            "Action": ["logs:DescribeLogStreams", "logs:CreateLogGroup"],
            "Resource": [
                f"arn:aws:logs:{reg}:{acct}:log-group:/aws/bedrock-agentcore/runtimes/*"
            ],
        }
    )
    statements.append(
        {
            "Effect": "Allow",
            "Action": ["logs:DescribeLogGroups"],
            "Resource": [f"arn:aws:logs:{reg}:{acct}:log-group:*"],
        }
    )
    statements.append(
        {
            "Effect": "Allow",
            "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": [
                f"arn:aws:logs:{reg}:{acct}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*"
            ],
        }
    )
    if container:
        statements.append(
            {
                "Sid": "ECRTokenAccess",
                "Effect": "Allow",
                "Action": ["ecr:GetAuthorizationToken"],
                "Resource": "*",
            }
        )
    statements.append(
        {
            "Effect": "Allow",
            "Action": [
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets",
            ],
            "Resource": ["*"],
        }
    )
    statements.append(
        {
            "Effect": "Allow",
            "Resource": "*",
            "Action": "cloudwatch:PutMetricData",
            "Condition": {"StringEquals": {"cloudwatch:namespace": "bedrock-agentcore"}},
        }
    )
    if container:
        statements.append(
            {
                "Sid": "GetAgentAccessToken",
                "Effect": "Allow",
                "Action": [
                    "bedrock-agentcore:GetWorkloadAccessToken",
                    "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
                    "bedrock-agentcore:GetWorkloadAccessTokenForUserId",
                ],
                "Resource": [
                    f"arn:aws:bedrock-agentcore:{reg}:{acct}:workload-identity-directory/default",
                    f"arn:aws:bedrock-agentcore:{reg}:{acct}:workload-identity-directory/default"
                    f"/workload-identity/{name}-*",
                ],
            }
        )
    statements.append(
        {
            "Sid": "BedrockModelInvocation",
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
            "Resource": [
                "arn:aws:bedrock:*::foundation-model/*",
                f"arn:aws:bedrock:{reg}:{acct}:*",
            ],
        }
    )

    policy = {"Version": "2012-10-17", "Statement": statements}
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": acct},
                    "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{reg}:{acct}:*"},
                },
            }
        ],
    }
    return {"policy": policy, "trust": trust}


# -- content fingerprint (DEPLOY-IDENTITY) ----------------------------------
def _canonical_hash(obj: Any) -> str:
    """SHA-256 of the canonical JSON of ``obj`` — mirrors ``statestore.content_hash``.

    Canonical form is ``json.dumps(sort_keys=True, separators=(",", ":"))`` so equal inputs
    hash identically regardless of key order or incidental whitespace.
    """
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fingerprint(
    m: "AgentManifest",
    *,
    account: Optional[str] = None,
    region: Optional[str] = None,
) -> str:
    """Content fingerprint of an agent's **hosting** identity (DEPLOY-IDENTITY inputs).

    Hashes only what changes *how the runtime is deployed*: the image/source, serving protocol,
    entrypoint, network mode, execution-role identity (an explicit ``role_arn`` or a hash of the
    synthesized policy), the sorted set of declared input keys, and the output schema. It does
    **not** fold in agent-behavior inputs (model / prompt / SOPs) — this is deploy dedup, not a
    trust re-earning check, and it must never be used to select among versions at dispatch time.

    Two manifests with identical hosting inputs produce the same fingerprint; a change to any
    hosting input (e.g. a new ``container_uri``, protocol, or output schema) changes it.
    """
    reg = m.registry
    build_mode = str(reg.get("build_mode", "container")).lower()
    if reg.get("role_arn"):
        role_identity: Dict[str, Any] = {"role_arn": reg["role_arn"]}
    else:
        role = render_execution_role(m, account, region, container=(build_mode == "container"))
        role_identity = {"policy_hash": _canonical_hash(role["policy"])}
    identity = {
        "image": reg.get("container_uri") or reg.get("source_digest"),
        "protocol": m.protocol,
        "entry": reg.get("entry"),
        "network": _network_configuration(reg),
        "role": role_identity,
        "input_keys": sorted(m.inputs.keys()),
        "output_schema": m.output_schema,
    }
    return _canonical_hash(identity)


# -- build plan entry -------------------------------------------------------
@dataclass
class BuildPlanEntry:
    """The compiled build/deploy artifacts + parameters for one agent node.

    Attributes:
        name: The agent/node id.
        build_mode: ``"container"`` | ``"codezip"`` | ``"prebuilt"``.
        wrapper: ``app.py`` source hosting the agent (``None`` for prebuilt/arn-reuse).
        dockerfile: Container ``Dockerfile`` (``None`` for codezip/prebuilt).
        execution_role: ``{"policy": ..., "trust": ...}``, or ``None`` when ``role_arn`` is given.
        create_agent_runtime: The ``create_agent_runtime`` param dict (or an arn-reuse marker).
        invoke: ``{"protocol": ..., "qualifier": ..., "port": ...}`` for the supervisor.
        ecr_repo: Target ECR repository for the image, when configured.
        fingerprint: Content fingerprint of the agent's hosting identity (see :func:`fingerprint`).
            Used at deploy time for reuse-by-content (a matching fingerprint ⇒ ``reused``,
            a changed one ⇒ ``updated``); never consulted to select a version at dispatch time.
    """

    name: str
    build_mode: str
    wrapper: Optional[str]
    dockerfile: Optional[str]
    execution_role: Optional[dict]
    create_agent_runtime: dict
    invoke: dict
    ecr_repo: Optional[str]
    fingerprint: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# -- protocol templates -----------------------------------------------------
class RuntimeTemplate(Protocol):
    """Structural contract for a per-protocol AgentCore runtime template."""

    def render_wrapper(self, m: "AgentManifest") -> str: ...

    def render_packaging(self, m: "AgentManifest") -> str: ...

    def create_runtime_request(self, m: "AgentManifest", image_uri: Optional[str]) -> dict: ...


class _BaseAgentTemplate:
    """Shared packaging + request logic; subclasses fix the protocol and serving harness."""

    protocol: str = "HTTP"

    @property
    def port(self) -> int:
        return PORTS[self.protocol]

    def render_packaging(self, m: "AgentManifest") -> str:
        """Render a ``Dockerfile`` that installs requirements and runs ``app.py``."""
        base = m.registry.get("base_image") or _DEFAULT_BASE_IMAGE
        return (
            f"FROM {base}\n"
            f"WORKDIR /app\n"
            f"COPY . /app\n"
            f"RUN pip install --no-cache-dir -r requirements.txt\n"
            f"EXPOSE {self.port}\n"
            f'CMD ["python", "app.py"]\n'
        )

    def create_runtime_request(self, m: "AgentManifest", image_uri: Optional[str]) -> dict:
        """Assemble the ``create_agent_runtime`` param dict for this protocol."""
        return _container_create_request(m, image_uri, self.protocol)

    def render_wrapper(self, m: "AgentManifest") -> str:  # pragma: no cover - abstract
        raise NotImplementedError


class HttpAgentTemplate(_BaseAgentTemplate):
    """HTTP agent: ``BedrockAgentCoreApp`` serving ``/invocations`` + ``/ping`` on 8080."""

    protocol = "HTTP"

    def render_wrapper(self, m: "AgentManifest") -> str:
        module, func = _split_entry(m.registry.get("entry"))
        keys_repr = ", ".join(repr(k) for k in m.inputs)
        return (
            f'"""Auto-generated AgentCore HTTP entrypoint for agent {m.name!r} '
            f'(Concursus)."""\n'
            f"\n"
            f"from {module} import {func} as _agent_callable\n"
            f"\n"
            f"from bedrock_agentcore.runtime import BedrockAgentCoreApp\n"
            f"\n"
            f"app = BedrockAgentCoreApp()\n"
            f"\n"
            f"_INPUT_KEYS = [{keys_repr}]\n"
            f"\n"
            f"\n"
            f"@app.entrypoint\n"
            f"def handler(payload, context):\n"
            f'    """Pull declared contract.inputs from the payload; call the agent; '
            f'return its dict."""\n'
            f"    kwargs = {{key: payload.get(key) for key in _INPUT_KEYS}}\n"
            f"    return _agent_callable(**kwargs)\n"
            f"\n"
            f"\n"
            f'if __name__ == "__main__":\n'
            f"    app.run()\n"
        )


class McpAgentTemplate(_BaseAgentTemplate):
    """MCP agent: ``FastMCP`` serving ``/mcp`` (streamable-http) on 8000."""

    protocol = "MCP"

    def render_wrapper(self, m: "AgentManifest") -> str:
        module, func = _split_entry(m.registry.get("entry"))
        keys_repr = ", ".join(repr(k) for k in m.inputs)
        return (
            f'"""Auto-generated AgentCore MCP entrypoint for agent {m.name!r} '
            f'(Concursus)."""\n'
            f"\n"
            f"from {module} import {func} as _agent_callable\n"
            f"\n"
            f"from mcp.server.fastmcp import FastMCP\n"
            f"\n"
            f'mcp = FastMCP("{m.name}", host="0.0.0.0", port={self.port})\n'
            f"\n"
            f"_INPUT_KEYS = [{keys_repr}]\n"
            f"\n"
            f"\n"
            f"@mcp.tool()\n"
            f"def {func}(payload: dict) -> dict:\n"
            f'    """Pull declared contract.inputs from the payload; call the agent; '
            f'return its dict."""\n'
            f"    kwargs = {{key: payload.get(key) for key in _INPUT_KEYS}}\n"
            f"    return _agent_callable(**kwargs)\n"
            f"\n"
            f"\n"
            f'if __name__ == "__main__":\n'
            f'    mcp.run(transport="streamable-http")\n'
        )


class A2AAgentTemplate(_BaseAgentTemplate):
    """A2A agent: JSON-RPC 2.0 app served at the root on 9000."""

    protocol = "A2A"

    def render_wrapper(self, m: "AgentManifest") -> str:
        module, func = _split_entry(m.registry.get("entry"))
        keys_repr = ", ".join(repr(k) for k in m.inputs)
        return (
            f'"""Auto-generated AgentCore A2A entrypoint for agent {m.name!r} '
            f'(Concursus)."""\n'
            f"\n"
            f"from {module} import {func} as _agent_callable\n"
            f"\n"
            f"from bedrock_agentcore.runtime import BedrockAgentCoreApp\n"
            f"\n"
            f"app = BedrockAgentCoreApp()\n"
            f"\n"
            f"_INPUT_KEYS = [{keys_repr}]\n"
            f"\n"
            f"\n"
            f"@app.entrypoint\n"
            f"def handler(payload, context):\n"
            f'    """A2A (JSON-RPC 2.0) entrypoint served at / on port {self.port}; '
            f'call the agent; return its dict."""\n'
            f"    kwargs = {{key: payload.get(key) for key in _INPUT_KEYS}}\n"
            f"    return _agent_callable(**kwargs)\n"
            f"\n"
            f"\n"
            f'if __name__ == "__main__":\n'
            f"    app.run(port={self.port})\n"
        )


class PreBuiltRegistrar:
    """Register an already-built image or reuse an existing runtime ARN.

    No wrapper or ``Dockerfile`` is synthesized: either ``registry.container_uri`` names an
    image already built + pushed (register it as-is) or ``registry.agent_runtime_arn`` names a
    live runtime (skip creation, only its endpoint is invoked).
    """

    def synthesize(
        self,
        m: "AgentManifest",
        *,
        account: Optional[str] = None,
        region: Optional[str] = None,
    ) -> BuildPlanEntry:
        reg = m.registry
        protocol = m.protocol
        qualifier = reg.get("qualifier", "DEFAULT")
        invoke = {"protocol": protocol, "qualifier": qualifier, "port": PORTS.get(protocol)}
        arn = reg.get("agent_runtime_arn")
        if arn:
            create_req: dict = {"agentRuntimeArn": arn}  # arn-reuse: nothing to create
        else:
            create_req = _container_create_request(m, reg.get("container_uri"), protocol)
        return BuildPlanEntry(
            name=m.name,
            build_mode="prebuilt",
            wrapper=None,
            dockerfile=None,
            execution_role=None,
            create_agent_runtime=create_req,
            invoke=invoke,
            ecr_repo=reg.get("ecr_repo"),
            fingerprint=fingerprint(m, account=account, region=region),
        )


_TEMPLATE_BY_PROTOCOL: Dict[str, type] = {
    "HTTP": HttpAgentTemplate,
    "MCP": McpAgentTemplate,
    "A2A": A2AAgentTemplate,
}


# -- pluggable per-runtime-kind builder registry (opt-in Strategy/Registry seam) ----------
#: A runtime builder is a UNIFORM ``(m, *, account, region) -> BuildPlanEntry`` callable — the
#: Strategy generalization of today's single compile path. :data:`_DEFAULT_RUNTIME_KIND` maps to
#: :func:`_default_runtime_builder`, which is today's EXACT synthesize body (prebuilt-registrar or
#: protocol-template dispatch). A manifest can OPT IN to a custom builder via ``registry.runtime_kind``;
#: an absent or unregistered kind falls back to the default builder, so the default compile is
#: byte-for-byte unchanged.
RuntimeBuilder = Callable[..., BuildPlanEntry]  # (m, *, account=None, region=None) -> BuildPlanEntry

#: The default runtime-kind key: today's one and only compile path.
_DEFAULT_RUNTIME_KIND = "default"


def _default_runtime_builder(
    m: "AgentManifest",
    *,
    account: Optional[str] = None,
    region: Optional[str] = None,
) -> BuildPlanEntry:
    """The DEFAULT runtime-kind builder: today's exact compile path.

    An existing ``agent_runtime_arn`` or a prebuilt ``container_uri`` (``build_mode == "prebuilt"``)
    routes to :class:`PreBuiltRegistrar`; otherwise the ``protocol`` selects an HTTP/MCP/A2A template
    and ``build_mode`` (default ``"container"``) decides whether a ``Dockerfile`` is emitted.
    """
    reg = m.registry
    build_mode = str(reg.get("build_mode", "container")).lower()
    protocol = m.protocol

    if reg.get("agent_runtime_arn") or (
        reg.get("container_uri") and build_mode == "prebuilt"
    ):
        return PreBuiltRegistrar().synthesize(m, account=account, region=region)

    template_cls = _TEMPLATE_BY_PROTOCOL.get(protocol)
    if template_cls is None:
        raise BuildError(
            f"{m.name}: unsupported protocol {protocol!r} (expected HTTP, MCP, or A2A)"
        )
    template = template_cls()

    wrapper = template.render_wrapper(m)
    dockerfile = template.render_packaging(m) if build_mode == "container" else None
    create_req = template.create_runtime_request(m, image_uri=None)
    invoke = {
        "protocol": template.protocol,
        "qualifier": reg.get("qualifier", "DEFAULT"),
        "port": template.port,
    }
    if reg.get("role_arn"):
        execution_role: Optional[dict] = None
    else:
        execution_role = render_execution_role(
            m, account, region, container=(build_mode == "container")
        )
    return BuildPlanEntry(
        name=m.name,
        build_mode=build_mode,
        wrapper=wrapper,
        dockerfile=dockerfile,
        execution_role=execution_role,
        create_agent_runtime=create_req,
        invoke=invoke,
        ecr_repo=reg.get("ecr_repo"),
        fingerprint=fingerprint(m, account=account, region=region),
    )


#: The shipped registry seeded with the single default kind. ``synthesize`` copies this per call and
#: layers any caller-supplied kinds atop it, so no shared global state is mutated.
RUNTIME_BUILDERS: Dict[str, RuntimeBuilder] = {_DEFAULT_RUNTIME_KIND: _default_runtime_builder}


class RuntimeBuilderFactory:
    """Dispatch a manifest to the template/registrar that compiles it into a build plan entry."""

    @staticmethod
    def synthesize(
        m: "AgentManifest",
        *,
        account: Optional[str] = None,
        region: Optional[str] = None,
        runtime_builders: Optional[Dict[str, RuntimeBuilder]] = None,
    ) -> BuildPlanEntry:
        """Compile one agent manifest into a :class:`BuildPlanEntry`.

        Routes through the per-runtime-kind builder registry (Strategy/Registry seam): the kind is
        ``registry.runtime_kind`` (opt-in; absent => :data:`_DEFAULT_RUNTIME_KIND`), and the builder is
        ``registry.get(kind, <default builder>)``. With no ``runtime_kind`` declared and no custom
        ``runtime_builders`` supplied (the default), this is EXACTLY :func:`_default_runtime_builder` —
        byte-for-byte today's compile path (prebuilt-registrar or protocol-template dispatch). A
        manifest that declares a registered custom ``runtime_kind`` routes to that builder instead; an
        unregistered kind falls back to the default builder.
        """
        registry: Dict[str, RuntimeBuilder] = dict(RUNTIME_BUILDERS)
        if runtime_builders:
            registry.update(runtime_builders)
        kind = str(m.registry.get("runtime_kind") or _DEFAULT_RUNTIME_KIND)
        builder = registry.get(kind, _default_runtime_builder)
        return builder(m, account=account, region=region)
