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
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from .trust import HOLD, SHADOW, TrustGrade, evaluate_deploy_gate

if TYPE_CHECKING:  # pragma: no cover - hints only
    from ..assemble.assemble import ProvisioningPlan
    from .build import BuildPlanEntry
    from .ledger import DeployLedger
    from ..core.manifest import AgentManifest

# Placeholders the build plan carries until deploy fills them (must match build.py).
_IMAGE_PLACEHOLDER = "<image-uri>"
_ROLE_PLACEHOLDER = "<execution-role-arn>"

# AgentCore Runtime only accepts linux/arm64 images — build for it explicitly so a build on an
# x86 host (e.g. a CI runner) does not push a green-but-unlaunchable amd64 image.
_RUNTIME_PLATFORM = "linux/arm64"

# CreateAgentRuntime is asynchronous (returns status=CREATING); poll GetAgentRuntime to a terminal
# state before treating the runtime as deployed.
_READY_STATUS = "READY"
_FAILED_STATUSES = frozenset({"CREATE_FAILED", "UPDATE_FAILED"})
_READY_POLL_SECONDS = 5.0
_READY_TIMEOUT_SECONDS = 600.0

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


def _is_aws_error(exc: Exception) -> bool:
    """True if ``exc`` is a botocore AWS error (``ClientError``/``BotoCoreError``).

    Checks the duck-typed ``ClientError`` shape (a ``.response`` dict with an ``Error`` key — which
    a real ``ClientError`` also carries) first, then falls back to botocore's exception types
    (lazy-imported, so the module keeps no hard boto3 dependency at import).
    """
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict) and "Error" in resp:
        return True
    try:
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:  # pragma: no cover - only without the agentcore extra
        return False
    return isinstance(exc, (BotoCoreError, ClientError))


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
        # AgentCore Runtime only launches linux/arm64 images; build for it explicitly so an x86
        # host (e.g. CI) does not push a green-but-unlaunchable amd64 image.
        run(["docker", "build", "--platform", _RUNTIME_PLATFORM, "-t", image, context])
        run(["docker", "push", image])
        return image
    finally:
        shutil.rmtree(context, ignore_errors=True)


# -- runtime readiness ------------------------------------------------------
def _await_runtime_ready(
    control: Any,
    runtime_id: Optional[str],
    *,
    status: Optional[str],
    failure_reason: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
    poll: float = _READY_POLL_SECONDS,
    timeout: float = _READY_TIMEOUT_SECONDS,
) -> str:
    """Poll ``GetAgentRuntime`` until the runtime reaches ``READY``; return the terminal status.

    ``CreateAgentRuntime`` is asynchronous — it returns while the runtime is still ``CREATING`` —
    so a caller that treats the create response as "deployed" can record a runtime that later
    ``CREATE_FAILED`` (or invoke one that is not yet ready). This polls to a terminal state and
    raises :class:`ProvisionError` on a failure or a timeout. A ``None`` status (e.g. a minimal
    injected fake, or an already-terminal create) is treated as already-``READY`` — so the poll is
    a no-op unless the service actually reports ``CREATING``.
    """
    waited = 0.0
    while status is not None and status != _READY_STATUS:
        if status in _FAILED_STATUSES:
            raise ProvisionError(
                f"agent runtime {runtime_id} entered {status}: {failure_reason or 'no reason given'}"
            )
        if waited >= timeout:
            raise ProvisionError(
                f"agent runtime {runtime_id} did not reach READY within {int(timeout)}s "
                f"(last status {status!r})"
            )
        sleep(poll)
        waited += poll
        got = control.get_agent_runtime(agentRuntimeId=runtime_id)
        status = got.get("status")
        failure_reason = got.get("failureReason", failure_reason)
    return status or _READY_STATUS


# -- per-agent + whole-plan provisioning ------------------------------------
def provision_agent(
    entry: "BuildPlanEntry",
    *,
    clients: Clients,
    source_dir: str = ".",
    tag: str = "latest",
    run: Optional[RunFn] = None,
    known_fingerprints: Optional[Dict[str, str]] = None,
    manifest: Optional["AgentManifest"] = None,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
    ledger: Optional["DeployLedger"] = None,
    now: Optional[Union[str, int, float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """Provision one agent; return ``{"node", "arn", "action", "role_arn", "image_uri"}``.

    Order: reuse an existing ``agentRuntimeArn`` outright; otherwise ensure the IAM role, build +
    push the image when the URI is still a placeholder, substitute both into the request, and
    ``CreateAgentRuntime``.

    Reuse-by-content (opt-in): pass ``known_fingerprints`` (node -> the fingerprint recorded for
    the runtime already deployed for that node). When omitted the behavior is unchanged — every
    provisioned runtime is reported ``action="created"``. When supplied, a node whose recorded
    fingerprint equals ``entry.fingerprint`` is a no-op ``action="reused"`` (nothing is
    re-created); a node whose fingerprint changed is re-provisioned and reported
    ``action="updated"``. The fingerprint covers only *hosting* identity (see
    :func:`concursus.build.fingerprint`); it is dedup metadata, never a dispatch-time selector.

    Persisted reuse-by-content (opt-in, AI-14): pass a :class:`~concursus.ledger.DeployLedger`.
    A row already recorded for this ``(name, fingerprint)`` is a no-op ``action="reused"`` (build
    + create are skipped) **across separate CLI invocations**; a fresh create is appended to the
    ledger afterward. ``now`` supplies the recorded ``deployed_at`` (caller-injected; falls back
    to a call-time UTC timestamp — never a module-import clock read).

    Create-time trust gate (opt-in, AI-13): pass the node's ``manifest`` plus a caller policy
    (``min_autonomy`` and/or ``require_approval``). The gate fires **exactly once**, right before
    ``CreateAgentRuntime``, for the author-declared node: a *side-effecting* manifest whose
    ``trust_seed`` is below ``min_autonomy`` (or when ``require_approval`` is set) is **held** —
    returned as ``action="escalated"`` with a ``reason`` and **no** create; a cleared-but-not-live
    grade is deployed to a **non-default (shadow) qualifier** instead of ``DEFAULT``. It is never a
    per-invocation check, never re-earns trust from an outcome, and never picks among agents.
    With no manifest/policy the gate is a no-op and today's deploy is byte-for-byte unchanged.
    """
    run = run or _default_run
    req = copy.deepcopy(entry.create_agent_runtime)
    result: Dict[str, Any] = {"node": entry.name, "role_arn": None, "image_uri": None}

    if "agentRuntimeArn" in req:  # arn-reuse: nothing to create
        result.update(arn=req["agentRuntimeArn"], action="reused")
        return result

    # 0a) Persisted reuse-by-content (opt-in) — a ledger row for this exact content is a no-op.
    if ledger is not None and entry.fingerprint:
        prior = ledger.lookup(entry.name, entry.fingerprint)
        if prior is not None:
            result.update(
                arn=prior.arn,
                action="reused",
                role_arn=prior.role_arn,
                image_uri=prior.image_uri,
            )
            return result

    # 0b) In-memory reuse-by-content (opt-in) — a matching recorded fingerprint is a no-op.
    prior_fp = (known_fingerprints or {}).get(entry.name)
    if prior_fp is not None and entry.fingerprint and prior_fp == entry.fingerprint:
        result.update(arn=None, action="reused")
        return result
    changed = prior_fp is not None and entry.fingerprint and prior_fp != entry.fingerprint

    # 0c) Create-time trust gate (opt-in) — decide live | shadow | hold ONCE for this node.
    qualifier = "DEFAULT"
    if manifest is not None:
        decision = evaluate_deploy_gate(
            side_effecting=getattr(manifest, "side_effecting", False),
            trust_seed=getattr(manifest, "trust_seed", TrustGrade.L0_SHADOW),
            min_autonomy=min_autonomy,
            require_approval=require_approval,
        )
        if decision.mode == HOLD:  # held for approval — nothing is created
            result.update(arn=None, action="escalated", reason=decision.reason)
            return result
        if decision.mode == SHADOW:  # cleared but not live — deploy to the shadow endpoint
            qualifier = decision.qualifier or "SHADOW"
            result.update(qualifier=qualifier, reason=decision.reason)

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
    else:
        # A manifest-supplied registry.role_arn (no synthesized role) — echo it into the result +
        # ledger so the deployed role is observable (not silently reported as null).
        result["role_arn"] = req.get("roleArn")

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

    # 3) Register the runtime. (A SHADOW decision surfaces its non-DEFAULT qualifier in the
    #    result; the create request itself stays a clean CreateAgentRuntime — the shadow
    #    endpoint is a separate, downstream concern, not an unknown param smuggled into boto3.)
    created = clients.control.create_agent_runtime(**req)
    arn = created.get("agentRuntimeArn")
    result.update(arn=arn, action="updated" if changed else "created")

    # 3a) Wait out the async create — CreateAgentRuntime returns while CREATING, so a runtime that
    #     later CREATE_FAILED must NOT be recorded as a usable 'created' node (dedup would then skip
    #     re-creating it, and run --execute could invoke a dead/not-ready runtime). Raises on a
    #     terminal failure or timeout. A fake/create response with no status is treated as READY.
    runtime_id = created.get("agentRuntimeId") or (arn.rsplit("/", 1)[-1] if arn else None)
    result["status"] = _await_runtime_ready(
        clients.control,
        runtime_id,
        status=created.get("status"),
        failure_reason=created.get("failureReason"),
        sleep=sleep or time.sleep,
    )

    # 4) Persisted reuse-by-content (opt-in) — append this outcome to the ledger for audit +
    #    cross-invocation dedup. ``deployed_at`` is caller-injected (``now``), never a clock read
    #    at import; fall back to a call-time UTC timestamp only if the caller supplied none.
    if ledger is not None and entry.fingerprint:
        stamp = now if now is not None else _utc_now_iso()
        ledger.record(
            name=entry.name,
            fingerprint=entry.fingerprint,
            deployed_at=stamp,
            arn=result.get("arn"),
            image_uri=result.get("image_uri"),
            role_arn=result.get("role_arn"),
            action=result.get("action"),
        )
    return result


def _utc_now_iso() -> str:
    """A call-time UTC ISO-8601 timestamp (invoked only when a ledger write needs one — never
    at import, so the module has no import-time clock dependency)."""
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def provision_plan(
    plan: "ProvisioningPlan",
    *,
    region: Optional[str] = None,
    source_dirs: Optional[Dict[str, str]] = None,
    default_source_dir: str = ".",
    tag: str = "latest",
    clients: Optional[Clients] = None,
    run: Optional[RunFn] = None,
    known_fingerprints: Optional[Dict[str, str]] = None,
    halt_on_error: bool = True,
    manifests: Optional[Dict[str, "AgentManifest"]] = None,
    min_autonomy: Optional[TrustGrade] = None,
    require_approval: bool = False,
    ledger: Optional["DeployLedger"] = None,
    now: Optional[Union[str, int, float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> List[Dict[str, Any]]:
    """Provision every agent in ``plan.order``; return one result dict per node (in order).

    ``clients``/``run`` default to real boto3 + the ``docker`` CLI; inject fakes to test the
    orchestration offline. ``source_dirs`` maps a node to its build-context directory (falling
    back to ``default_source_dir``). ``known_fingerprints`` (opt-in) maps a node to the hosting
    fingerprint already deployed for it — enabling reuse-by-content (see :func:`provision_agent`);
    omit it to keep today's unconditional ``created`` behavior.

    Decision-style partial results (AI-12): each :func:`provision_agent` call is guarded. On a
    :class:`ProvisionError` a ``{"node", "action": "failed", "error"}`` result is recorded and —
    when ``halt_on_error`` is ``True`` (the default, preserving today's fail-fast deploy) — the
    walk stops. With ``halt_on_error=False`` the walk continues to the remaining nodes. **Either
    way the accumulated results are returned**, so a 5-node deploy whose 3rd node fails still
    reports the 2 already-provisioned nodes (with their ARNs) plus the 1 failed node.

    ``manifests`` (opt-in, AI-13) supplies each node's :class:`~concursus.manifest.AgentManifest`
    so the create-time trust gate can fire; ``min_autonomy``/``require_approval`` are the caller
    policy. ``ledger`` (opt-in, AI-14) enables persisted reuse-by-content across invocations, and
    ``now`` injects its ``deployed_at`` timestamp. All default to no-ops.
    """
    clients = clients or Clients.default(region)
    run = run or _default_run
    source_dirs = source_dirs or {}
    manifests = manifests or {}
    results: List[Dict[str, Any]] = []
    for node in plan.order:
        entry = plan.entries[node]
        try:
            results.append(
                provision_agent(
                    entry,
                    clients=clients,
                    source_dir=source_dirs.get(node, default_source_dir),
                    tag=tag,
                    run=run,
                    known_fingerprints=known_fingerprints,
                    manifest=manifests.get(node),
                    min_autonomy=min_autonomy,
                    require_approval=require_approval,
                    ledger=ledger,
                    now=now,
                    sleep=sleep,
                )
            )
        except Exception as exc:  # noqa: BLE001 - convert AWS/provision failures to a per-node result
            # A ProvisionError (explained) OR a raw botocore error (AWS-side: throttle, access
            # denied, conflict) becomes a per-node ``failed`` result so the partial-result
            # guarantee actually holds — a raw ClientError would otherwise escape and discard every
            # already-provisioned node's ARN. Anything else is a genuine bug: re-raise it.
            if not isinstance(exc, ProvisionError) and not _is_aws_error(exc):
                raise
            results.append({"node": node, "action": "failed", "error": str(exc)})
            if halt_on_error:
                break
    return results
