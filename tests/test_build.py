"""Tests for the runtime builder — template dispatch, wrapper/Dockerfile + IAM rendering."""

import pytest

from concursus import AgentManifest
from concursus.build.build import (
    PORTS,
    A2AAgentTemplate,
    BuildError,
    BuildPlanEntry,
    HttpAgentTemplate,
    McpAgentTemplate,
    PreBuiltRegistrar,
    RuntimeBuilderFactory,
    fingerprint,
    render_execution_role,
)


def _manifest(**registry):
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
                "inputs": {"document": {"type": "string"}, "lang": {"type": "string"}},
                "outputs": {"summary": {"type": "string"}},
            },
        }
    )


# -- factory dispatch by protocol -------------------------------------------
@pytest.mark.parametrize(
    "protocol,template_cls,port,harness",
    [
        ("HTTP", HttpAgentTemplate, 8080, "BedrockAgentCoreApp"),
        ("MCP", McpAgentTemplate, 8000, "FastMCP"),
        ("A2A", A2AAgentTemplate, 9000, "BedrockAgentCoreApp"),
    ],
)
def test_factory_dispatch_by_protocol(protocol, template_cls, port, harness):
    m = _manifest(protocol=protocol)
    entry = RuntimeBuilderFactory.synthesize(m)
    assert isinstance(entry, BuildPlanEntry)
    assert entry.build_mode == "container"
    assert entry.invoke == {"protocol": protocol, "qualifier": "DEFAULT", "port": port}
    assert entry.invoke["port"] == PORTS[protocol]
    assert entry.create_agent_runtime["protocolConfiguration"]["serverProtocol"] == protocol
    # the chosen template's serving harness shows up in the emitted wrapper
    assert harness in entry.wrapper
    assert template_cls().protocol == protocol
    assert template_cls().port == port


def test_mcp_wrapper_uses_streamable_http():
    wrapper = McpAgentTemplate().render_wrapper(_manifest(protocol="MCP"))
    assert 'transport="streamable-http"' in wrapper
    compile(wrapper, "<app.py>", "exec")


def test_a2a_wrapper_runs_on_port_9000():
    wrapper = A2AAgentTemplate().render_wrapper(_manifest(protocol="A2A"))
    assert "app.run(port=9000)" in wrapper
    compile(wrapper, "<app.py>", "exec")


# -- wrapper contents --------------------------------------------------------
def test_http_wrapper_emits_valid_entrypoint():
    wrapper = HttpAgentTemplate().render_wrapper(_manifest())
    assert "@app.entrypoint" in wrapper
    assert "from bedrock_agentcore.runtime import BedrockAgentCoreApp" in wrapper
    # imports the user callable from registry.entry ("agents.summarize:run")
    assert "from agents.summarize import run as _agent_callable" in wrapper
    assert "run" in wrapper  # the entry function name
    # declared contract.inputs keys are pulled from the payload
    assert "'document'" in wrapper and "'lang'" in wrapper
    assert "app.run()" in wrapper
    compile(wrapper, "<app.py>", "exec")  # emitted source is syntactically valid


def test_wrapper_requires_entry():
    m = _manifest()
    m.registry.pop("entry")
    with pytest.raises(BuildError):
        HttpAgentTemplate().render_wrapper(m)


# -- packaging (Dockerfile) --------------------------------------------------
def test_render_packaging_defaults_to_python_slim():
    dockerfile = HttpAgentTemplate().render_packaging(_manifest())
    assert dockerfile.startswith("FROM python:3.12-slim")
    assert "pip install --no-cache-dir -r requirements.txt" in dockerfile
    assert "EXPOSE 8080" in dockerfile
    assert 'CMD ["python", "app.py"]' in dockerfile


def test_render_packaging_honors_base_image_override():
    dockerfile = HttpAgentTemplate().render_packaging(
        _manifest(base_image="123.dkr.ecr.us-east-1.amazonaws.com/concursus-runtime-base:1")
    )
    assert "FROM 123.dkr.ecr.us-east-1.amazonaws.com/concursus-runtime-base:1" in dockerfile


# -- create_agent_runtime request -------------------------------------------
def test_create_runtime_request_shape():
    req = HttpAgentTemplate().create_runtime_request(_manifest(), image_uri="repo/img:sha")
    assert (
        req["agentRuntimeArtifact"]["containerConfiguration"]["containerUri"] == "repo/img:sha"
    )
    assert req["protocolConfiguration"]["serverProtocol"] == "HTTP"
    assert req["networkConfiguration"] == {"networkMode": "PUBLIC"}
    assert req["roleArn"] == "arn:aws:iam::123456789012:role/agent"
    assert req["agentRuntimeName"] == "summarize"


def test_create_runtime_request_uses_placeholder_when_image_missing():
    req = HttpAgentTemplate().create_runtime_request(_manifest(), image_uri=None)
    assert req["agentRuntimeArtifact"]["containerConfiguration"]["containerUri"]


def test_create_runtime_request_includes_auth_and_lifecycle():
    m = _manifest(
        auth={
            "discoveryUrl": "https://issuer/.well-known/openid-configuration",
            "allowedClients": ["c1"],
        },
        lifecycle={"idleRuntimeSessionTimeout": 900, "maxLifetime": 28800},
    )
    req = HttpAgentTemplate().create_runtime_request(m, image_uri=None)
    assert req["authorizerConfiguration"]["customJWTAuthorizer"]["discoveryUrl"]
    assert req["authorizerConfiguration"]["customJWTAuthorizer"]["allowedClients"] == ["c1"]
    assert req["lifecycleConfiguration"] == {
        "idleRuntimeSessionTimeout": 900,
        "maxLifetime": 28800,
    }


def test_create_runtime_request_vpc_network_mode():
    m = _manifest(
        network_mode="VPC",
        network_mode_config={"subnets": ["subnet-1"], "securityGroups": ["sg-1"]},
    )
    req = HttpAgentTemplate().create_runtime_request(m, image_uri=None)
    net = req["networkConfiguration"]
    assert net["networkMode"] == "VPC"
    assert net["networkModeConfig"]["subnets"] == ["subnet-1"]
    assert net["networkModeConfig"]["securityGroups"] == ["sg-1"]
    assert net["networkModeConfig"]["requireServiceS3Endpoint"] is False


# -- IAM execution role ------------------------------------------------------
def _sids(policy):
    return {s.get("Sid") for s in policy["Statement"]}


def _actions(policy):
    acts = set()
    for s in policy["Statement"]:
        a = s["Action"]
        acts.update([a] if isinstance(a, str) else a)
    return acts


def test_execution_role_container_has_ecr_and_workload_token():
    role = render_execution_role(_manifest(), "123456789012", "us-east-1", container=True)
    sids = _sids(role["policy"])
    actions = _actions(role["policy"])
    assert "ECRImageAccess" in sids
    assert "ECRTokenAccess" in sids
    assert "GetAgentAccessToken" in sids
    assert "BedrockModelInvocation" in sids
    assert "bedrock-agentcore:GetWorkloadAccessToken" in actions
    assert "ecr:BatchGetImage" in actions
    # region/account substituted into the ARNs
    assert any(
        "arn:aws:ecr:us-east-1:123456789012:repository/*" in s.get("Resource", [])
        for s in role["policy"]["Statement"]
        if s.get("Sid") == "ECRImageAccess"
    )
    # trust principal
    trust_stmt = role["trust"]["Statement"][0]
    assert trust_stmt["Principal"]["Service"] == "bedrock-agentcore.amazonaws.com"
    assert trust_stmt["Action"] == "sts:AssumeRole"
    assert trust_stmt["Condition"]["StringEquals"]["aws:SourceAccount"] == "123456789012"


def test_execution_role_codezip_omits_ecr_and_workload_token():
    role = render_execution_role(_manifest(), "123456789012", "us-east-1", container=False)
    sids = _sids(role["policy"])
    actions = _actions(role["policy"])
    assert "ECRImageAccess" not in sids
    assert "ECRTokenAccess" not in sids
    assert "GetAgentAccessToken" not in sids
    assert not any(a.startswith("ecr:") for a in actions)
    assert not any(a.startswith("bedrock-agentcore:") for a in actions)
    # still carries the always-on statements
    assert "BedrockModelInvocation" in sids


def test_execution_role_uses_placeholders_when_unknown():
    role = render_execution_role(_manifest(), None, None, container=True)
    blob = str(role)
    assert "ACCOUNT_ID" in blob
    assert "REGION" in blob


# -- role wiring into the plan entry ----------------------------------------
def test_role_arn_present_skips_execution_role():
    entry = RuntimeBuilderFactory.synthesize(_manifest())  # role_arn set in defaults
    assert entry.execution_role is None


def test_missing_role_arn_synthesizes_execution_role():
    m = _manifest()
    m.registry.pop("role_arn")
    entry = RuntimeBuilderFactory.synthesize(m, account="123456789012", region="us-east-1")
    assert entry.execution_role is not None
    assert "policy" in entry.execution_role and "trust" in entry.execution_role


def test_codezip_build_omits_dockerfile():
    entry = RuntimeBuilderFactory.synthesize(_manifest(build_mode="codezip"))
    assert entry.build_mode == "codezip"
    assert entry.dockerfile is None
    assert entry.wrapper is not None  # app.py source still emitted


# -- prebuilt / arn-reuse ----------------------------------------------------
def test_agent_runtime_arn_routes_to_prebuilt_registrar():
    m = AgentManifest.from_dict(
        {
            "name": "reused",
            "registry": {
                "agent_runtime_arn": "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/x",
                "protocol": "HTTP",
                "qualifier": "PROD",
            },
            "contract": {"outputs": {"o": {"type": "string"}}},
        }
    )
    entry = RuntimeBuilderFactory.synthesize(m)
    assert entry.build_mode == "prebuilt"
    assert entry.wrapper is None
    assert entry.dockerfile is None
    assert entry.execution_role is None
    assert entry.create_agent_runtime["agentRuntimeArn"].endswith("runtime/x")
    assert entry.invoke == {"protocol": "HTTP", "qualifier": "PROD", "port": 8080}


def test_prebuilt_container_uri_registers_image_as_is():
    m = AgentManifest.from_dict(
        {
            "name": "prebuilt",
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/img:1",
                "protocol": "MCP",
                "build_mode": "prebuilt",
                "role_arn": "arn:aws:iam::123456789012:role/agent",
            },
            "contract": {"outputs": {"o": {"type": "string"}}},
        }
    )
    entry = RuntimeBuilderFactory.synthesize(m)
    assert isinstance(PreBuiltRegistrar(), PreBuiltRegistrar)
    assert entry.build_mode == "prebuilt"
    cfg = entry.create_agent_runtime["agentRuntimeArtifact"]["containerConfiguration"]
    assert cfg["containerUri"] == "acct.dkr.ecr.us-east-1.amazonaws.com/img:1"
    assert entry.create_agent_runtime["protocolConfiguration"]["serverProtocol"] == "MCP"
    assert entry.invoke["port"] == 8000


def test_build_plan_entry_to_dict_round_trips():
    entry = RuntimeBuilderFactory.synthesize(_manifest())
    d = entry.to_dict()
    assert d["name"] == "summarize"
    assert d["invoke"]["protocol"] == "HTTP"
    assert d["create_agent_runtime"]["protocolConfiguration"]["serverProtocol"] == "HTTP"
    assert d["fingerprint"] == entry.fingerprint


# -- content fingerprint (DEPLOY-IDENTITY / AI-11) --------------------------
def test_identical_manifests_produce_equal_fingerprints():
    a = RuntimeBuilderFactory.synthesize(_manifest())
    b = RuntimeBuilderFactory.synthesize(_manifest())
    assert a.fingerprint == b.fingerprint
    assert a.fingerprint  # non-empty
    # the free helper agrees with the value stamped on the entry
    assert fingerprint(_manifest()) == a.fingerprint


def test_fingerprint_changes_when_container_uri_changes():
    base = fingerprint(_manifest())
    other = fingerprint(_manifest(container_uri="acct.dkr.ecr.us-east-1.amazonaws.com/other:2"))
    assert base != other


def test_fingerprint_changes_when_protocol_changes():
    assert fingerprint(_manifest(protocol="HTTP")) != fingerprint(_manifest(protocol="MCP"))


def test_fingerprint_changes_when_output_schema_changes():
    m1 = _manifest()
    m2 = _manifest()
    m2.contract["outputs"] = {"summary": {"type": "string"}, "score": {"type": "number"}}
    assert fingerprint(m1) != fingerprint(m2)


def test_fingerprint_stable_across_registry_key_order():
    # Same hosting inputs supplied in a different dict order hash identically (canonical JSON).
    m1 = _manifest()
    m2 = AgentManifest.from_dict(
        {
            "name": "summarize",
            "registry": {
                "role_arn": "arn:aws:iam::123456789012:role/agent",
                "entry": "agents.summarize:run",
                "protocol": "HTTP",
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
            },
            "contract": {
                "inputs": {"lang": {"type": "string"}, "document": {"type": "string"}},
                "outputs": {"summary": {"type": "string"}},
            },
        }
    )
    assert fingerprint(m1) == fingerprint(m2)


def test_fingerprint_ignores_agent_behavior_inputs():
    # Behavior inputs (model/prompt/sops) are NOT hosting identity — they must not shift the fp.
    base = fingerprint(_manifest())
    with_behavior = fingerprint(
        _manifest(model="anthropic.claude-3", prompt="be terse", sops=["sop-1"])
    )
    assert base == with_behavior


def test_fingerprint_folds_synthesized_role_when_no_role_arn():
    m = _manifest()
    m.registry.pop("role_arn")
    fp = fingerprint(m, account="123456789012", region="us-east-1")
    assert fp
    # a synthesized-role fingerprint differs from the explicit-role one
    assert fp != fingerprint(_manifest())
