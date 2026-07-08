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
    from .manifest import AgentManifest, ManifestError

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
    from .manifest import AgentManifest

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
    from .dag import AgentDAG

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
    from .assemble import OrchestrationAssembler

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
    """Explain what ``deploy --execute`` WOULD provision, without importing boto3."""
    print(f"DRY RUN (no --execute): would provision {len(plan.order)} agent runtime(s) via")
    print("AWS Bedrock AgentCore (bedrock-agentcore-control):\n")
    for i, node in enumerate(plan.order, 1):
        entry = plan.entries[node]
        req = entry.create_agent_runtime
        if "agentRuntimeArn" in req:
            print(f"  [{i}] {node}: REUSE existing runtime {req['agentRuntimeArn']}")
            continue
        proto = req.get("protocolConfiguration", {}).get("serverProtocol", "?")
        role = req.get("roleArn", "?")
        print(
            f"  [{i}] {node}: CreateAgentRuntime name={req.get('agentRuntimeName')!r} "
            f"protocol={proto} build_mode={entry.build_mode} role={role}"
        )
    print("\nPass --execute to call AWS (requires boto3: pip install concursus[agentcore]).")


def _execute_deploy(plan: "object", region: Optional[str]) -> int:
    """Provision the plan for real: ``CreateAgentRuntime`` per node on the control plane."""
    try:
        import boto3  # lazy: only --execute talks to AWS (the optional [agentcore] extra)
    except ImportError:
        print(
            "FAIL  deploy --execute requires boto3 — install the 'agentcore' extra "
            "(pip install concursus[agentcore])",
            file=sys.stderr,
        )
        return 1
    client = boto3.client(
        "bedrock-agentcore-control", **({"region_name": region} if region else {})
    )
    for node in plan.order:
        entry = plan.entries[node]
        req = entry.create_agent_runtime
        if "agentRuntimeArn" in req:
            print(f"REUSE   {node}  -> {req['agentRuntimeArn']}")
            continue
        resp = client.create_agent_runtime(**req)
        print(f"CREATED {node}  -> {resp.get('agentRuntimeArn')}")
    return 0


def _cmd_deploy(args: argparse.Namespace) -> int:
    try:
        _manifests, plan = _assemble(args)
    except (ValueError, OSError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    if not args.execute:
        _print_deploy_dryrun(plan)
        return 0
    return _execute_deploy(plan, getattr(args, "region", None))


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


def _cmd_run(args: argparse.Namespace) -> int:
    from .supervisor import Supervisor

    try:
        manifests, plan = _assemble(args)
        inputs = _load_inputs(args.inputs)
    except (ValueError, OSError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    if not args.execute:
        _print_run_dryrun(plan, manifests, inputs)
        return 0
    try:
        outputs = Supervisor(plan, manifests).run(inputs)
    except Exception as exc:  # surface AWS/runtime/schema failures as a clean CLI error
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1
    print(json.dumps(outputs, indent=2))
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
        help="Actually provision on AWS via boto3 (otherwise a dry-run; no boto3 imported).",
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
