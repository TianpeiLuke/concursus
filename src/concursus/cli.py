"""The ``concursus`` command-line entry point.

Minimal for now: report the version, print an overview, and validate ``.agent.yaml``
manifests. The ``plan`` / ``deploy`` / ``run`` verbs (compile an AgentDAG into an AgentCore
provisioning plan + supervisor) are the roadmap.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from . import __version__


def _cmd_info(_args: argparse.Namespace) -> int:
    print(
        "concursus {v}\n\n"
        "Compile a declarative DAG of subagents into an orchestrated team on AWS Bedrock\n"
        "AgentCore. You declare an AgentDAG (nodes = agents, edges = data dependencies) and\n"
        "one .agent.yaml manifest per agent; Concursus provisions each agent with\n"
        "CreateAgentRuntime and runs them with a topological supervisor over InvokeAgentRuntime,\n"
        "wiring outputs to inputs and routing shared state through AgentCore Memory.\n\n"
        "Status: early — this release ships the declarative core (AgentDAG + AgentManifest).\n"
        "Roadmap: the OrchestrationAssembler (provisioning plan + supervisor).".format(
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

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        return _cmd_info(args)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
