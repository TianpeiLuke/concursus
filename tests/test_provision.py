"""Tests for the deploy-time actuator (concursus.provision) — all with fakes, no AWS/Docker."""

import base64
import os

import pytest

from concursus.build import BuildPlanEntry, RuntimeBuilderFactory
from concursus.manifest import AgentManifest
from concursus.provision import (
    Clients,
    ProvisionError,
    ensure_ecr_repo,
    ensure_execution_role,
    provision_agent,
    provision_plan,
    repo_name,
    role_name,
)


# -- fakes ------------------------------------------------------------------
def _client_error(code):
    exc = Exception(code)
    exc.response = {"Error": {"Code": code}}
    return exc


class FakeIam:
    def __init__(self, exists=False):
        self.exists = exists
        self.calls = []

    def create_role(self, **kw):
        self.calls.append(("create_role", kw))
        if self.exists:
            raise _client_error("EntityAlreadyExistsException")
        return {"Role": {"Arn": f"arn:aws:iam::111:role/{kw['RoleName']}"}}

    def update_assume_role_policy(self, **kw):
        self.calls.append(("update_assume_role_policy", kw))

    def get_role(self, **kw):
        self.calls.append(("get_role", kw))
        return {"Role": {"Arn": f"arn:aws:iam::111:role/{kw['RoleName']}"}}

    def put_role_policy(self, **kw):
        self.calls.append(("put_role_policy", kw))


class FakeEcr:
    def __init__(self, exists=False):
        self.exists = exists
        self.calls = []

    def create_repository(self, **kw):
        self.calls.append(("create_repository", kw))
        if self.exists:
            raise _client_error("RepositoryAlreadyExistsException")
        uri = f"111.dkr.ecr.us-east-1.amazonaws.com/{kw['repositoryName']}"
        return {"repository": {"repositoryUri": uri}}

    def describe_repositories(self, **kw):
        self.calls.append(("describe_repositories", kw))
        name = kw["repositoryNames"][0]
        return {
            "repositories": [{"repositoryUri": f"111.dkr.ecr.us-east-1.amazonaws.com/{name}"}]
        }

    def get_authorization_token(self, **kw):
        token = base64.b64encode(b"AWS:secret").decode()
        return {
            "authorizationData": [
                {
                    "authorizationToken": token,
                    "proxyEndpoint": "https://111.dkr.ecr.us-east-1.amazonaws.com",
                }
            ]
        }


class FakeControl:
    def __init__(self):
        self.calls = []

    def create_agent_runtime(self, **kw):
        self.calls.append(kw)
        return {
            "agentRuntimeArn": f"arn:aws:bedrock-agentcore:us-east-1:111:runtime/{kw['agentRuntimeName']}-xyz"
        }


class FakeRun:
    """Records shell commands; captures the build context's files at ``docker build`` time."""

    def __init__(self):
        self.cmds = []
        self.captured = {}
        self.logins = []

    def __call__(self, cmd, *, input=None, cwd=None):
        self.cmds.append(cmd)
        if cmd[:2] == ["docker", "login"]:
            self.logins.append(input)
        if cmd[:2] == ["docker", "build"]:
            ctx = cmd[-1]
            for f in ("app.py", "Dockerfile", "requirements.txt"):
                path = os.path.join(ctx, f)
                if os.path.exists(path):
                    with open(path, encoding="utf-8") as fh:
                        self.captured[f] = fh.read()


def _fakes():
    return Clients(iam=FakeIam(), ecr=FakeEcr(), control=FakeControl()), FakeRun()


def _container_entry():
    m = AgentManifest.from_dict(
        {
            "name": "summarize",
            "registry": {
                "container_uri": "x",  # required by validate(); a build_mode=container agent is rebuilt
                "protocol": "HTTP",
                "entry": "agents.summarize:handler",
                "ecr_repo": "team/summarize",
            },
            "contract": {
                "inputs": {"document": {"type": "string"}},
                "outputs": {"summary": {"type": "string", "required": True}},
            },
        }
    )
    return RuntimeBuilderFactory.synthesize(m, account="111", region="us-east-1")


# -- naming -----------------------------------------------------------------
def test_role_and_repo_names():
    entry = _container_entry()
    assert role_name(entry) == "concursus-summarize-exec"
    assert repo_name(entry) == "team/summarize"


# -- IAM role ---------------------------------------------------------------
def test_ensure_execution_role_creates_and_attaches_policy():
    iam = FakeIam()
    role = {"policy": {"Version": "2012-10-17"}, "trust": {"Version": "2012-10-17"}}
    arn = ensure_execution_role(role, "concursus-x-exec", iam)
    assert arn == "arn:aws:iam::111:role/concursus-x-exec"
    kinds = [c[0] for c in iam.calls]
    assert "create_role" in kinds and "put_role_policy" in kinds


def test_ensure_execution_role_is_idempotent():
    iam = FakeIam(exists=True)
    role = {"policy": {}, "trust": {}}
    arn = ensure_execution_role(role, "concursus-x-exec", iam)
    assert arn == "arn:aws:iam::111:role/concursus-x-exec"
    kinds = [c[0] for c in iam.calls]
    assert (
        "update_assume_role_policy" in kinds
        and "get_role" in kinds
        and "put_role_policy" in kinds
    )


# -- ECR repo ---------------------------------------------------------------
def test_ensure_ecr_repo_creates_then_reuses():
    fresh = FakeEcr()
    assert ensure_ecr_repo("team/x", fresh).endswith("/team/x")
    existing = FakeEcr(exists=True)
    assert ensure_ecr_repo("team/x", existing).endswith("/team/x")
    assert any(c[0] == "describe_repositories" for c in existing.calls)


# -- provision_agent: full container build ----------------------------------
def test_provision_agent_container_builds_role_image_and_creates(tmp_path):
    (tmp_path / "requirements.txt").write_text("strands\n")
    clients, run = _fakes()
    entry = _container_entry()
    res = provision_agent(entry, clients=clients, source_dir=str(tmp_path), tag="v1", run=run)

    assert res["action"] == "created"
    assert res["role_arn"].startswith("arn:aws:iam::111:role/concursus-summarize-exec")
    assert res["image_uri"].endswith("/team/summarize:v1")
    assert res["arn"].startswith("arn:aws:bedrock-agentcore:")

    # the create call received the REAL role + image (placeholders substituted)
    call = clients.control.calls[0]
    assert call["roleArn"] == res["role_arn"]
    assert (
        call["agentRuntimeArtifact"]["containerConfiguration"]["containerUri"]
        == res["image_uri"]
    )

    # docker login -> build -> push happened, and the build context had the generated files
    verbs = [c[:2] for c in run.cmds]
    assert ["docker", "login"] in verbs
    assert ["docker", "build"] in verbs
    assert ["docker", "push"] in verbs
    assert run.logins == ["secret"]
    assert "@app.entrypoint" in run.captured["app.py"]
    assert "FROM" in run.captured["Dockerfile"]
    assert run.captured["requirements.txt"] == "strands\n"  # user's file preserved


def test_provision_agent_does_not_mutate_the_plan_entry(tmp_path):
    clients, run = _fakes()
    entry = _container_entry()
    before = entry.create_agent_runtime["agentRuntimeArtifact"]["containerConfiguration"][
        "containerUri"
    ]
    provision_agent(entry, clients=clients, source_dir=str(tmp_path), run=run)
    after = entry.create_agent_runtime["agentRuntimeArtifact"]["containerConfiguration"][
        "containerUri"
    ]
    assert before == after == "<image-uri>"  # the plan is deep-copied, not mutated


# -- provision_agent: reuse + prebuilt + error ------------------------------
def test_provision_agent_reuses_existing_runtime():
    m = AgentManifest.from_dict(
        {
            "name": "critique",
            "registry": {
                "agent_runtime_arn": "arn:aws:bedrock-agentcore:us-east-1:111:runtime/critique-abc"
            },
            "contract": {"outputs": {"critique": {"type": "string", "required": True}}},
        }
    )
    entry = RuntimeBuilderFactory.synthesize(m)
    clients, run = _fakes()
    res = provision_agent(entry, clients=clients, run=run)
    assert res["action"] == "reused"
    assert res["arn"] == "arn:aws:bedrock-agentcore:us-east-1:111:runtime/critique-abc"
    assert clients.control.calls == []  # nothing created
    assert run.cmds == []  # nothing built


def test_provision_agent_prebuilt_image_skips_build(tmp_path):
    m = AgentManifest.from_dict(
        {
            "name": "ranker",
            "registry": {
                "container_uri": "111.dkr.ecr.us-east-1.amazonaws.com/ranker:latest",
                "build_mode": "prebuilt",
                "role_arn": "arn:aws:iam::111:role/preexisting",
                "protocol": "HTTP",
            },
            "contract": {"outputs": {"ranked": {"type": "string", "required": True}}},
        }
    )
    entry = RuntimeBuilderFactory.synthesize(m)
    clients, run = _fakes()
    res = provision_agent(entry, clients=clients, source_dir=str(tmp_path), run=run)
    assert res["action"] == "created" and res["image_uri"] is None
    assert run.cmds == []  # prebuilt image: no docker build
    call = clients.control.calls[0]
    assert call["agentRuntimeArtifact"]["containerConfiguration"]["containerUri"].endswith(
        "/ranker:latest"
    )
    assert call["roleArn"] == "arn:aws:iam::111:role/preexisting"


def test_provision_agent_without_role_raises():
    # A prebuilt image with neither a synthesized role nor a role_arn cannot be provisioned.
    entry = BuildPlanEntry(
        name="x",
        build_mode="prebuilt",
        wrapper=None,
        dockerfile=None,
        execution_role=None,
        create_agent_runtime={
            "agentRuntimeName": "x",
            "agentRuntimeArtifact": {"containerConfiguration": {"containerUri": "repo:tag"}},
            "roleArn": "<execution-role-arn>",
            "networkConfiguration": {"networkMode": "PUBLIC"},
            "protocolConfiguration": {"serverProtocol": "HTTP"},
        },
        invoke={"protocol": "HTTP", "qualifier": "DEFAULT", "port": 8080},
        ecr_repo=None,
    )
    clients, run = _fakes()
    with pytest.raises(ProvisionError):
        provision_agent(entry, clients=clients, run=run)


# -- reuse-by-content (fingerprint / AI-11) ---------------------------------
def test_provision_agent_reuses_on_matching_fingerprint(tmp_path):
    clients, run = _fakes()
    entry = _container_entry()
    res = provision_agent(
        entry,
        clients=clients,
        source_dir=str(tmp_path),
        run=run,
        known_fingerprints={entry.name: entry.fingerprint},
    )
    assert res["action"] == "reused"
    assert clients.control.calls == []  # nothing re-created
    assert run.cmds == []  # nothing rebuilt


def test_provision_agent_updates_on_changed_fingerprint(tmp_path):
    clients, run = _fakes()
    entry = _container_entry()
    res = provision_agent(
        entry,
        clients=clients,
        source_dir=str(tmp_path),
        run=run,
        known_fingerprints={entry.name: "stale-fingerprint"},
    )
    assert res["action"] == "updated"
    assert len(clients.control.calls) == 1  # re-provisioned
    assert res["arn"].startswith("arn:aws:bedrock-agentcore:")


def test_provision_agent_defaults_to_created_without_known_fingerprints(tmp_path):
    # Default path is byte-for-byte unchanged: no known_fingerprints ⇒ always "created".
    clients, run = _fakes()
    entry = _container_entry()
    res = provision_agent(entry, clients=clients, source_dir=str(tmp_path), run=run)
    assert res["action"] == "created"


# -- provision_plan ---------------------------------------------------------
def test_provision_plan_runs_every_node_in_order(tmp_path):
    from concursus.assemble import OrchestrationAssembler
    from concursus.dag import AgentDAG

    manifests = {
        "ingest": AgentManifest.from_dict(
            {
                "name": "ingest",
                "registry": {"container_uri": "x", "protocol": "HTTP", "entry": "a.ingest:h"},
                "contract": {"outputs": {"document": {"type": "string", "required": True}}},
            }
        ),
        "summarize": AgentManifest.from_dict(
            {
                "name": "summarize",
                "registry": {
                    "container_uri": "x",
                    "protocol": "HTTP",
                    "entry": "a.summarize:h",
                },
                "contract": {
                    "inputs": {"document": {"type": "string"}},
                    "outputs": {"summary": {"type": "string", "required": True}},
                },
                "spec": {"depends_on": [{"from": "ingest.document", "to": "document"}]},
            }
        ),
    }
    dag = AgentDAG()
    for n in manifests:
        dag.add_node(n)
    dag.add_edge("ingest", "summarize")
    plan = OrchestrationAssembler(account="111", region="us-east-1").assemble(dag, manifests)

    clients, run = _fakes()
    results = provision_plan(
        plan, region="us-east-1", default_source_dir=str(tmp_path), clients=clients, run=run
    )
    assert [r["node"] for r in results] == ["ingest", "summarize"]
    assert all(r["action"] == "created" for r in results)
    assert len(clients.control.calls) == 2
