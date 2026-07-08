"""Deploy-time **actuator** — turn a :class:`~concursus.assemble.ProvisioningPlan` into live
AgentCore runtimes.

For each plan entry, in topological order: ensure its IAM **execution role** exists (create the
role + attach the synthesized policy, idempotently), **build and push** its container image to
ECR when the plan still carries the ``<image-uri>`` placeholder, substitute the real ``roleArn``
and ``containerUri`` into the ``create_agent_runtime`` params, and call ``CreateAgentRuntime`` on
the control plane. An already-pushed image or an existing runtime ARN is registered/reused as-is.

This is the one module that talks to AWS and Docker (the optional ``[agentcore]`` extra + the
``docker`` CLI). Every AWS client (:class:`Clients`) and the shell runner (``run``) is injectable,
so the whole orchestration is unit-testable with fakes — no AWS account and no Docker daemon. The
module imports only stdlib at top; boto3 is bound lazily in :meth:`Clients.default`.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:  # pragma: no cover - hints only
    from .assemble import ProvisioningPlan
    from .build import BuildPlanEntry

# Placeholders the build plan carries until deploy fills them (must match build.py).
_IMAGE_PLACEHOLDER = "<image-uri>"
_ROLE_PLACEHOLDER = "<execution-role-arn>"

# A shell runner: ``(cmd, input=?, cwd=?) -> None``; raises on non-zero exit.
RunFn = Callable[..., None]


class ProvisionError(RuntimeError):
    """Raised when provisioning a plan against AWS fails in a way we can explain."""


# -- injectable AWS clients -------------------------------------------------
@dataclass
class Clients:
    """The three AWS clients provisioning needs; inject fakes in tests."""

    iam: Any
    ecr: Any
    control: Any

    @classmethod
    def default(cls, region: Optional[str] = None) -> "Clients":
        """Bind real boto3 clients (IAM is global; ECR + control plane are regional)."""
        try:
            import boto3  # lazy: only a real deploy needs the AWS SDK (the [agentcore] extra)
        except ImportError as exc:  # pragma: no cover - exercised only without boto3
            raise ProvisionError(
                "deploy --execute requires boto3 — install the 'agentcore' extra "
                "(pip install concursus[agentcore])"
            ) from exc
        regional = {"region_name": region} if region else {}
        return cls(
            iam=boto3.client("iam"),
            ecr=boto3.client("ecr", **regional),
            control=boto3.client("bedrock-agentcore-control", **regional),
        )


def _default_run(
    cmd: List[str], *, input: Optional[str] = None, cwd: Optional[str] = None
) -> None:
    """Default shell runner: run ``cmd``, feeding ``input`` on stdin, raising on failure."""
    import subprocess  # lazy: only a real image build shells out

    subprocess.run(cmd, input=input, cwd=cwd, check=True, text=True)


# -- naming -----------------------------------------------------------------
def _sanitize(name: str) -> str:
    return "".join(c if (c.isalnum() or c in "-_") else "-" for c in str(name)).strip("-_")


def role_name(entry: "BuildPlanEntry") -> str:
    """The IAM role name for an agent's execution role (<=64 chars, AgentCore-valid)."""
    return f"concursus-{_sanitize(entry.name)}-exec"[:64]


def repo_name(entry: "BuildPlanEntry") -> str:
    """The ECR repository for an agent's image (``registry.ecr_repo`` or a derived default)."""
    return entry.ecr_repo or f"concursus/{_sanitize(entry.name).lower()}"


def _err_code(exc: Exception) -> str:
    """Best-effort AWS error code (botocore ``ClientError`` shape, else the class name)."""
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        return str(resp.get("Error", {}).get("Code", type(exc).__name__))
    return type(exc).__name__


# -- IAM execution role -----------------------------------------------------
def ensure_execution_role(role: dict, name: str, iam: Any) -> str:
    """Create (or update) the execution role and attach its policy; return the role ARN.

    Idempotent: an existing role has its trust policy refreshed instead of failing. ``role`` is
    the ``{"policy": ..., "trust": ...}`` document synthesized by the build plan.
    """
    trust = json.dumps(role["trust"])
    try:
        created = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
        arn = created["Role"]["Arn"]
    except Exception as exc:  # noqa: BLE001 - branch on the AWS error code
        if "EntityAlreadyExists" not in _err_code(exc):
            raise
        iam.update_assume_role_policy(RoleName=name, PolicyDocument=trust)
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=name,
        PolicyName="concursus-exec",
        PolicyDocument=json.dumps(role["policy"]),
    )
    return arn


# -- ECR image --------------------------------------------------------------
def ensure_ecr_repo(name: str, ecr: Any) -> str:
    """Create (or look up) the ECR repository; return its ``repositoryUri``. Idempotent."""
    try:
        return ecr.create_repository(repositoryName=name)["repository"]["repositoryUri"]
    except Exception as exc:  # noqa: BLE001 - branch on the AWS error code
        if "RepositoryAlreadyExists" not in _err_code(exc):
            raise
        described = ecr.describe_repositories(repositoryNames=[name])
        return described["repositories"][0]["repositoryUri"]


def _docker_login(ecr: Any, run: RunFn) -> None:
    """Authenticate the local Docker CLI to ECR via a fresh authorization token."""
    data = ecr.get_authorization_token()["authorizationData"][0]
    token = base64.b64decode(data["authorizationToken"]).decode("utf-8")
    _, _, password = token.partition(":")  # token is "AWS:<password>"
    run(
        ["docker", "login", "-u", "AWS", "--password-stdin", data["proxyEndpoint"]],
        input=password,
    )


def build_and_push_image(
    entry: "BuildPlanEntry",
    repo_uri: str,
    *,
    source_dir: str,
    tag: str,
    ecr: Any,
    run: RunFn,
) -> str:
    """Assemble a build context, ``docker build`` + ``docker push`` the image; return its URI.

    The context is a temp copy of ``source_dir`` (the user's agent code + ``requirements.txt``)
    with the plan's generated ``app.py`` + ``Dockerfile`` dropped in — so the user's project is
    never mutated. A missing ``requirements.txt`` is seeded with ``bedrock-agentcore``.
    """
    image = f"{repo_uri}:{tag}"
    context = tempfile.mkdtemp(prefix="concursus-build-")
    try:
        if source_dir and os.path.isdir(source_dir):
            shutil.copytree(source_dir, context, dirs_exist_ok=True)
        with open(os.path.join(context, "app.py"), "w", encoding="utf-8") as fh:
            fh.write(entry.wrapper or "")
        with open(os.path.join(context, "Dockerfile"), "w", encoding="utf-8") as fh:
            fh.write(entry.dockerfile or "")
        req_path = os.path.join(context, "requirements.txt")
        if not os.path.exists(req_path):
            with open(req_path, "w", encoding="utf-8") as fh:
                fh.write("bedrock-agentcore\n")
        _docker_login(ecr, run)
        run(["docker", "build", "-t", image, context])
        run(["docker", "push", image])
        return image
    finally:
        shutil.rmtree(context, ignore_errors=True)


# -- per-agent + whole-plan provisioning ------------------------------------
def provision_agent(
    entry: "BuildPlanEntry",
    *,
    clients: Clients,
    source_dir: str = ".",
    tag: str = "latest",
    run: Optional[RunFn] = None,
) -> Dict[str, Any]:
    """Provision one agent; return ``{"node", "arn", "action", "role_arn", "image_uri"}``.

    Order: reuse an existing ``agentRuntimeArn`` outright; otherwise ensure the IAM role, build +
    push the image when the URI is still a placeholder, substitute both into the request, and
    ``CreateAgentRuntime``.
    """
    run = run or _default_run
    req = copy.deepcopy(entry.create_agent_runtime)
    result: Dict[str, Any] = {"node": entry.name, "role_arn": None, "image_uri": None}

    if "agentRuntimeArn" in req:  # arn-reuse: nothing to create
        result.update(arn=req["agentRuntimeArn"], action="reused")
        return result

    # 1) IAM execution role — the plan carries a role doc only when no role_arn was supplied.
    if entry.execution_role is not None:
        role_arn = ensure_execution_role(entry.execution_role, role_name(entry), clients.iam)
        req["roleArn"] = role_arn
        result["role_arn"] = role_arn
    elif req.get("roleArn") == _ROLE_PLACEHOLDER:
        raise ProvisionError(
            f"{entry.name}: no execution role — set registry.role_arn or let the plan "
            "synthesize one (pass --account/--region)"
        )

    # 2) Container image — build + push only when the plan left a placeholder URI.
    if entry.build_mode == "container":
        artifact = req.get("agentRuntimeArtifact", {}).get("containerConfiguration", {})
        if artifact.get("containerUri") == _IMAGE_PLACEHOLDER:
            repo_uri = ensure_ecr_repo(repo_name(entry), clients.ecr)
            image_uri = build_and_push_image(
                entry, repo_uri, source_dir=source_dir, tag=tag, ecr=clients.ecr, run=run
            )
            req["agentRuntimeArtifact"]["containerConfiguration"]["containerUri"] = image_uri
            result["image_uri"] = image_uri

    # 3) Register the runtime.
    created = clients.control.create_agent_runtime(**req)
    result.update(arn=created.get("agentRuntimeArn"), action="created")
    return result


def provision_plan(
    plan: "ProvisioningPlan",
    *,
    region: Optional[str] = None,
    source_dirs: Optional[Dict[str, str]] = None,
    default_source_dir: str = ".",
    tag: str = "latest",
    clients: Optional[Clients] = None,
    run: Optional[RunFn] = None,
) -> List[Dict[str, Any]]:
    """Provision every agent in ``plan.order``; return one result dict per node (in order).

    ``clients``/``run`` default to real boto3 + the ``docker`` CLI; inject fakes to test the
    orchestration offline. ``source_dirs`` maps a node to its build-context directory (falling
    back to ``default_source_dir``).
    """
    clients = clients or Clients.default(region)
    run = run or _default_run
    source_dirs = source_dirs or {}
    results: List[Dict[str, Any]] = []
    for node in plan.order:
        entry = plan.entries[node]
        results.append(
            provision_agent(
                entry,
                clients=clients,
                source_dir=source_dirs.get(node, default_source_dir),
                tag=tag,
                run=run,
            )
        )
    return results
