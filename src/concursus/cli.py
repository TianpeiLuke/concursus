"""The ``concursus`` command-line entry point.

Verbs: ``info`` (overview), ``validate`` (check ``.agent.yaml`` manifests), and the compiler
trio — ``plan`` (compile manifests + a DAG into a JSON provisioning plan), ``deploy`` (dry-run
the plan, or ``--execute`` it against AWS Bedrock AgentCore), and ``run`` (dry-run the
topological dispatch, or ``--execute`` it over live runtimes). Only ``deploy``/``run
--execute`` touch boto3, and only then — it is imported lazily.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

from . import __version__


def _cmd_info(_args: argparse.Namespace) -> int:
    print(
        "concursus {v}\n\n"
        "Compile a declarative DAG of subagents into an orchestrated team on AWS Bedrock\n"
        "AgentCore. You declare an AgentDAG (nodes = agents, edges = data dependencies) and\n"
        "one .agent.yaml manifest per agent; Concursus provisions each agent with\n"
        "CreateAgentRuntime and runs them with a topological supervisor over InvokeAgentRuntime,\n"
        "wiring outputs to inputs and routing shared state through AgentCore Memory.\n\n"
        "Commands:\n"
        "  info                 print this overview\n"
        "  validate  MANIFEST.. validate one or more .agent.yaml manifests\n"
        "  plan      MANIFEST.. compile a provisioning plan (JSON preview; no AWS)\n"
        "  deploy    MANIFEST.. dry-run the plan, or --execute CreateAgentRuntime on AWS\n"
        "  run       MANIFEST.. dry-run the dispatch, or --execute InvokeAgentRuntime live\n".format(
            v=__version__
        )
    )
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from .core.manifest import AgentManifest, ManifestError

    rc = 0
    for path in args.manifests:
        try:
            m = AgentManifest.from_yaml(path).validate()
            print(f"OK    {path}  (agent '{m.name}', protocol {m.protocol})")
        except (ManifestError, OSError, ValueError) as exc:
            print(f"FAIL  {path}  -> {exc}", file=sys.stderr)
            rc = 1
    return rc


# -- shared helpers ---------------------------------------------------------
def _load_manifests(paths: List[str]) -> Dict[str, "object"]:
    """Load ``.agent.yaml`` files into ``{agent_name: AgentManifest}``."""
    from .core.manifest import AgentManifest

    manifests: Dict[str, object] = {}
    for path in paths:
        m = AgentManifest.from_yaml(path)
        manifests[m.name] = m
    return manifests


def _parse_dag_edges(specs: Optional[List[str]]) -> List[Tuple[str, str]]:
    """Parse ``--dag`` edge specs (``"from->to"`` or ``"from:to"``) into ``(from, to)`` pairs."""
    edges: List[Tuple[str, str]] = []
    for spec in specs or []:
        sep = "->" if "->" in spec else ":" if ":" in spec else None
        if sep is None:
            raise ValueError(f"invalid --dag edge {spec!r} (expected 'FROM->TO' or 'FROM:TO')")
        frm, _, to = spec.partition(sep)
        frm, to = frm.strip(), to.strip()
        if not frm or not to:
            raise ValueError(f"invalid --dag edge {spec!r} (expected 'FROM->TO' or 'FROM:TO')")
        edges.append((frm, to))
    return edges


def _build_dag(manifests: Dict[str, "object"], dag_edges: List[Tuple[str, str]]) -> "object":
    """Build an :class:`AgentDAG`: one node per manifest, edges from ``--dag`` or ``depends_on``.

    Explicit ``--dag`` edges win; otherwise each manifest's ``depends_on`` producer supplies
    the edge ``producer -> node``. Unresolvable producers are left for the assembler's alignment
    check to report.
    """
    from .core.dag import AgentDAG

    dag = AgentDAG()
    for name in manifests:
        dag.add_node(name)
    if dag_edges:
        for frm, to in dag_edges:
            dag.add_edge(frm, to)
    else:
        for name, m in manifests.items():
            for edge in m.depends_on:
                producer = str(edge.get("from", "")).split(".", 1)[0]
                if producer and producer in manifests:
                    dag.add_edge(producer, name)
    return dag


def _assemble(args: argparse.Namespace) -> "object":
    """Load manifests, build the DAG, and assemble the provisioning plan (raises on error)."""
    from .assemble.assemble import OrchestrationAssembler

    manifests = _load_manifests(args.manifests)
    dag = _build_dag(manifests, _parse_dag_edges(getattr(args, "dag", None)))
    assembler = OrchestrationAssembler(
        account=getattr(args, "account", None), region=getattr(args, "region", None)
    )
    return manifests, assembler.assemble(dag, manifests)


def _load_inputs(value: Optional[str]) -> dict:
    """Parse ``--inputs``: a JSON object literal, or ``@path`` to a JSON file (``None`` -> {})."""
    if not value:
        return {}
    if value.startswith("@"):
        with open(value[1:], "r", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        data = json.loads(value)
    if not isinstance(data, dict):
        raise ValueError("--inputs must be a JSON object (a mapping of input names to values)")
    return data


def _entry_arn(manifest: "object") -> str:
    """The invoke ARN for a node: its manifest ``agent_runtime_arn`` or a deploy-time placeholder."""
    return manifest.registry.get("agent_runtime_arn", "<agent-runtime-arn>")


# -- plan -------------------------------------------------------------------
def _cmd_plan(args: argparse.Namespace) -> int:
    try:
        _manifests, plan = _assemble(args)
    except (ValueError, OSError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    print(json.dumps(plan.to_dict(), indent=2))
    return 0


# -- deploy -----------------------------------------------------------------
def _print_deploy_dryrun(plan: "object") -> None:
    """Explain what ``deploy --execute`` WOULD do per agent, without importing boto3/docker."""
    print(f"DRY RUN (no --execute): would provision {len(plan.order)} agent runtime(s) on")
    print("AWS Bedrock AgentCore. For each agent, in topological order:\n")
    for i, node in enumerate(plan.order, 1):
        entry = plan.entries[node]
        req = entry.create_agent_runtime
        if "agentRuntimeArn" in req:
            print(f"  [{i}] {node}: REUSE existing runtime {req['agentRuntimeArn']}")
            continue
        steps = []
        if entry.execution_role is not None:
            steps.append("create IAM execution role")
        ccfg = req.get("agentRuntimeArtifact", {}).get("containerConfiguration", {})
        if entry.build_mode == "container" and ccfg.get("containerUri") == "<image-uri>":
            steps.append("build + push image to ECR")
        proto = req.get("protocolConfiguration", {}).get("serverProtocol", "?")
        steps.append(f"CreateAgentRuntime (protocol={proto})")
        print(f"  [{i}] {node}: " + " -> ".join(steps))
    print(
        "\nPass --execute to run these steps (requires boto3 + the docker CLI: "
        "pip install concursus[agentcore])."
    )


def _parse_source_dirs(specs: Optional[List[str]]) -> Tuple[str, Dict[str, str]]:
    """Parse ``--source-dir`` specs into ``(default_dir, {node: dir})``.

    A bare ``DIR`` sets the default build context for every agent; ``NODE=DIR`` overrides one
    agent's context. The build context holds the agent's code + ``requirements.txt`` (the plan's
    generated ``app.py`` + ``Dockerfile`` are dropped into a copy of it at build time).
    """
    default_dir = "."
    per_node: Dict[str, str] = {}
    for spec in specs or []:
        if "=" in spec:
            node, _, path = spec.partition("=")
            node, path = node.strip(), path.strip()
            if not node or not path:
                raise ValueError(f"invalid --source-dir {spec!r} (expected DIR or NODE=DIR)")
            per_node[node] = path
        else:
            default_dir = spec.strip() or "."
    return default_dir, per_node


_DEPLOY_VERBS = {
    "reused": "REUSE   ",
    "created": "CREATED ",
    "updated": "UPDATED ",
    "escalated": "ESCALATE",
    "failed": "FAILED  ",
}


def _execute_deploy(
    plan: "object",
    region: Optional[str],
    default_source_dir: str,
    source_dirs: Dict[str, str],
    tag: str,
    manifests: Optional[Dict[str, "object"]] = None,
    min_autonomy: Optional["object"] = None,
    require_approval: bool = False,
) -> int:
    """Provision the plan for real: ensure IAM roles, build+push images, ``CreateAgentRuntime``.

    ``min_autonomy``/``require_approval`` feed the create-time trust gate (a side-effecting node
    below the floor is *escalated*, not created — reported, never deployed). Provisioning is
    partial-result safe: a node that fails is reported ``FAILED`` and, since the CLI opts out of
    fail-fast, the remaining nodes are still attempted; a non-zero exit reflects any failure.
    """
    from .build.provision import provision_plan

    try:
        results = provision_plan(
            plan,
            region=region,
            source_dirs=source_dirs,
            default_source_dir=default_source_dir,
            tag=tag,
            manifests=manifests,
            min_autonomy=min_autonomy,
            require_approval=require_approval,
            halt_on_error=False,
        )
    except Exception as exc:  # surface boto3/Docker/provision failures as a clean CLI error
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    rc = 0
    for r in results:
        action = r.get("action")
        verb = _DEPLOY_VERBS.get(action, "CREATED ")
        if action == "escalated":
            print(f"{verb} {r['node']}  HELD: {r.get('reason', 'trust gate')}", file=sys.stderr)
            rc = 1
            continue
        if action == "failed":
            print(f"{verb} {r['node']}  -> {r.get('error')}", file=sys.stderr)
            rc = 1
            continue
        extra = ""
        if r.get("qualifier") and r.get("qualifier") != "DEFAULT":
            extra += f"  qualifier={r['qualifier']}(shadow)"
        if r.get("image_uri"):
            extra += f"  image={r['image_uri']}"
        if r.get("role_arn"):
            extra += f"  role={r['role_arn']}"
        print(f"{verb} {r['node']}  -> {r.get('arn')}{extra}")
    return rc


def _parse_min_autonomy(value: Optional[str]) -> Optional["object"]:
    """Parse ``--min-autonomy`` (a TrustGrade name or 0-3) into a ``TrustGrade`` (``None`` off)."""
    if value is None:
        return None
    from .build.trust import TrustGrade

    return TrustGrade.parse(value)


def _cmd_deploy(args: argparse.Namespace) -> int:
    try:
        manifests, plan = _assemble(args)
        default_source_dir, source_dirs = _parse_source_dirs(getattr(args, "source_dir", None))
        min_autonomy = _parse_min_autonomy(getattr(args, "min_autonomy", None))
    except (ValueError, OSError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    if not args.execute:
        _print_deploy_dryrun(plan)
        return 0
    return _execute_deploy(
        plan,
        getattr(args, "region", None),
        default_source_dir,
        source_dirs,
        getattr(args, "tag", None) or "latest",
        manifests=manifests,
        min_autonomy=min_autonomy,
        require_approval=bool(getattr(args, "require_approval", False)),
    )


# -- run --------------------------------------------------------------------
def _print_run_dryrun(plan: "object", manifests: Dict[str, "object"], inputs: dict) -> None:
    """Explain the topological dispatch ``run --execute`` WOULD perform, without invoking."""
    print(
        f"DRY RUN (no --execute): topological dispatch over {len(plan.order)} agent(s); "
        "one stable runtimeSessionId spans the run."
    )
    print("Order: " + " -> ".join(plan.order) + "\n")
    print(f"Run inputs: {json.dumps(inputs)}\n")
    for i, node in enumerate(plan.order, 1):
        manifest = manifests.get(node)
        qualifier = manifest.registry.get("qualifier", "DEFAULT") if manifest else "DEFAULT"
        arn = _entry_arn(manifest) if manifest else "<agent-runtime-arn>"
        print(f"  [{i}] {node}: InvokeAgentRuntime arn={arn} qualifier={qualifier}")
        wiring = plan.wiring.get(node, [])
        if not wiring:
            print("       inputs: (source node) external run inputs")
        for ref in wiring:
            print(f"       input {ref.input_name!r} <- {ref.producer} {ref.path}")
    print("\nPass --execute to invoke the live runtimes (requires boto3 + deployed ARNs).")


def _make_run_supervisor(
    args: argparse.Namespace, plan: "object", manifests: Dict[str, "object"]
) -> "object":
    """A Supervisor for ``run --execute``, optionally backed by a durable StateStore.

    With ``--vault DIR`` the run persists as round-trip-exact markdown notes under
    ``DIR/runs/<session>/`` (offline, resumable, no AWS) — the durable on-disk slipbox tier.
    With ``--memory-id`` it threads through an AgentCore ``MemoryStateStore`` (opt-in, resumable)
    sharing the supervisor's ``runtimeSessionId``; ``--actor-id`` scopes the event stream
    (default ``"run"``). With neither, the supervisor keeps its offline in-process default. boto3
    is imported lazily (only the Memory backend needs it, and only on the first put).
    """
    from .execute.supervisor import Supervisor

    vault = getattr(args, "vault", None)
    memory_id = getattr(args, "memory_id", None)
    if not vault and not memory_id:
        return Supervisor(plan, manifests)

    supervisor = Supervisor(plan, manifests)  # mint the stable per-run session id

    if vault:
        import datetime

        from .state.filevault import FileVaultStateStore

        store = FileVaultStateStore.from_config(
            vault_path=vault,
            session_id=supervisor.session_id,
            date=datetime.date.today().isoformat(),
            slipbox_form=not getattr(args, "lean_form", False),
        )
    else:
        from .state.statestore import MemoryStateStore

        store = MemoryStateStore(
            memory_id=memory_id,
            session_id=supervisor.session_id,
            actor_id=getattr(args, "actor_id", None) or "run",
        )
    return Supervisor(plan, manifests, session_id=supervisor.session_id, state_store=store)


def _plan_approval_gate(plan: "object", args: argparse.Namespace) -> bool:
    """AI-21 between-phases gate: preview the FROZEN plan and require confirmation before invoke.

    Off by default (``--approve`` absent) — today's ``run --execute`` path is byte-for-byte
    unchanged. When on, this runs strictly BETWEEN ``_assemble`` and ``supervisor.run`` (before any
    billed ``InvokeAgentRuntime``): it prints ``ProvisioningPlan.to_dict()`` and pauses. It is safe
    precisely because the plan is FROZEN — approving invokes it, aborting invokes nothing, and any
    "adjust" must route through :meth:`OrchestrationAssembler.recompile` (never a live executor).

    Confirmation:
      * ``--yes`` -> approved without prompting (scripted/non-interactive approval).
      * an interactive TTY -> prompt ``Approve this plan and invoke? [y/N]``; only ``y``/``yes``
        approves.
      * non-interactive with no ``--yes`` -> ABORT (never auto-approve a billed run).

    Returns ``True`` to proceed, ``False`` to abort (the caller prints a notice and exits 0).
    """
    print("PLAN PREVIEW (--approve): review before any billed InvokeAgentRuntime.\n")
    print(json.dumps(plan.to_dict(), indent=2))
    print()
    if getattr(args, "yes", False):
        print("Plan approved via --yes.", file=sys.stderr)
        return True
    if not sys.stdin.isatty():
        print(
            "Plan approval required but no TTY is attached; pass --yes to approve "
            "non-interactively. Aborting (nothing invoked).",
            file=sys.stderr,
        )
        return False
    try:
        answer = input("Approve this plan and invoke? [y/N] ").strip().lower()
    except EOFError:
        answer = ""
    if answer in ("y", "yes"):
        return True
    print("Plan not approved; aborting (nothing invoked).", file=sys.stderr)
    return False


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        manifests, plan = _assemble(args)
        inputs = _load_inputs(args.inputs)
    except (ValueError, OSError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    if not args.execute:
        _print_run_dryrun(plan, manifests, inputs)
        return 0
    # AI-21: opt-in plan-approval gate, strictly between assemble and run (before any billed
    # invoke). Default OFF preserves today's behavior byte-for-byte.
    if getattr(args, "approve", False) and not _plan_approval_gate(plan, args):
        return 0
    supervisor = None
    try:
        supervisor = _make_run_supervisor(args, plan, manifests)
        outputs = supervisor.run(inputs)
    except Exception as exc:  # surface AWS/runtime/schema failures as a clean CLI error
        print(f"FAIL  {exc}", file=sys.stderr)
        if supervisor is not None:  # operator-legible partial summary (read-only, from the log)
            try:
                print(supervisor.summary_line(), file=sys.stderr)
            except Exception:  # a summary must never mask the original failure
                pass
        return 1
    print(json.dumps(outputs, indent=2))
    vault = getattr(args, "vault", None)
    if vault:
        from .state.filevault import _slug
        from .state.rundb import build_run_db

        run_dir = os.path.join(vault, "runs", _slug(supervisor.session_id))
        db = build_run_db(run_dir)
        print(f"\nPersisted run to {run_dir}\nDerived run DB: {db}", file=sys.stderr)
    return 0


def _add_plan_like_args(sp: argparse.ArgumentParser) -> None:
    """Shared arguments for the compiler verbs (manifests + topology + AWS targeting)."""
    sp.add_argument("manifests", nargs="+", help="Path(s) to .agent.yaml file(s).")
    sp.add_argument(
        "--dag",
        action="append",
        metavar="FROM->TO",
        help="Explicit dependency edge (repeatable); default: infer from each manifest's "
        "depends_on.",
    )
    sp.add_argument("--account", help="AWS account id (threaded into synthesized IAM roles).")
    sp.add_argument("--region", help="AWS region (threaded into synthesized IAM roles).")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="concursus",
        description="Compile a DAG of subagents into an orchestrated team on AWS Bedrock AgentCore.",
    )
    p.add_argument("--version", action="version", version=f"concursus {__version__}")
    sub = p.add_subparsers(dest="command")

    sub.add_parser("info", help="Print an overview of Concursus.").set_defaults(func=_cmd_info)

    v = sub.add_parser("validate", help="Validate one or more .agent.yaml manifests.")
    v.add_argument("manifests", nargs="+", help="Path(s) to .agent.yaml file(s).")
    v.set_defaults(func=_cmd_validate)

    pl = sub.add_parser(
        "plan", help="Compile manifests into a JSON provisioning plan (no AWS)."
    )
    _add_plan_like_args(pl)
    pl.set_defaults(func=_cmd_plan)

    dp = sub.add_parser(
        "deploy", help="Dry-run the plan, or --execute CreateAgentRuntime on AWS."
    )
    _add_plan_like_args(dp)
    dp.add_argument(
        "--execute",
        action="store_true",
        help="Actually provision on AWS (boto3 + docker; otherwise a dry-run, nothing imported).",
    )
    dp.add_argument(
        "--source-dir",
        action="append",
        metavar="DIR|NODE=DIR",
        help="Build-context dir holding agent code + requirements.txt (default '.'); repeatable, "
        "NODE=DIR overrides one agent.",
    )
    dp.add_argument("--tag", help="Container image tag to build/push (default 'latest').")
    dp.add_argument(
        "--min-autonomy",
        metavar="GRADE",
        help="Create-time trust floor (a TrustGrade name like L2_GUARDED, or 0-3). A "
        "side-effecting agent whose declared trust_seed is below this is ESCALATED (held, not "
        "deployed); a cleared-but-L0 grade deploys to a shadow endpoint. Omit to disable the gate.",
    )
    dp.add_argument(
        "--require-approval",
        action="store_true",
        help="Hold every side-effecting agent for explicit approval (ESCALATE, no create), "
        "regardless of trust_seed. Off by default.",
    )
    dp.set_defaults(func=_cmd_deploy)

    rn = sub.add_parser(
        "run", help="Dry-run the dispatch, or --execute InvokeAgentRuntime live."
    )
    _add_plan_like_args(rn)
    rn.add_argument(
        "--inputs",
        help="Run inputs: a JSON object literal, or @path to a JSON file.",
    )
    rn.add_argument(
        "--execute",
        action="store_true",
        help="Actually invoke the live runtimes via boto3 (otherwise a dry-run).",
    )
    rn.add_argument(
        "--vault",
        metavar="DIR",
        help="Persist the run as durable markdown notes under DIR/runs/<session>/ (offline, "
        "resumable, no AWS) and build a derived SQLite run DB. Omit for the in-process store.",
    )
    rn.add_argument(
        "--lean-form",
        action="store_true",
        help="With --vault, emit the lean machine form (node/attempt/status/consumes/payload) "
        "instead of the default authentic slipbox notes (lineage/building_block/"
        "Related-Notes + a _run.md entry point). Use for a smaller, non-indexed durable log.",
    )
    rn.add_argument(
        "--memory-id",
        help="Back the run with an AgentCore Memory StateStore (durable, resumable); "
        "requires --execute + boto3. Omit for the offline in-process store.",
    )
    rn.add_argument(
        "--actor-id",
        help="Actor id scoping the Memory event stream (default 'run'); used with --memory-id.",
    )
    rn.add_argument(
        "--approve",
        "--plan-approval",
        dest="approve",
        action="store_true",
        help="Preview the FROZEN provisioning plan and PAUSE for confirmation before any billed "
        "InvokeAgentRuntime (a safe between-phases gate; the plan is frozen). Interactive by "
        "default; in a non-TTY, requires --yes or it aborts. Off by default.",
    )
    rn.add_argument(
        "--yes",
        action="store_true",
        help="Approve the --approve plan preview without prompting (non-interactive approval).",
    )
    rn.set_defaults(func=_cmd_run)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        return _cmd_info(args)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
