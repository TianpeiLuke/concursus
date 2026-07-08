"""Concursus — compile a DAG of subagents into an orchestrated team on AWS Bedrock AgentCore.

Where **cursus** compiles a pipeline DAG + configs into a SageMaker pipeline, **Concursus**
(Latin *"a running-together / convergence"*) compiles an ``AgentDAG`` + per-agent
``.agent.yaml`` manifests into (1) an AgentCore provisioning plan — one ``CreateAgentRuntime``
per agent — and (2) a supervisor that dispatches the agents in topological order, wires each
agent's declared output into its dependents' input, and routes shared state through AgentCore
Memory. It is the coordinator AgentCore deliberately does not ship.

Status: early. This release provides the declarative core — the backend-agnostic
:class:`~concursus.dag.AgentDAG` and the :class:`~concursus.manifest.AgentManifest`
(``.agent.yaml``) model. The AgentCore provisioning plan + supervisor (the
``OrchestrationAssembler``) are the roadmap.

Basic usage:
    >>> from concursus import AgentDAG
    >>> dag = AgentDAG()
    >>> for n in ["ingest", "summarize", "critique", "format"]:
    ...     dag.add_node(n)
    >>> dag.add_edge("ingest", "summarize").add_edge("summarize", "critique")
    >>> dag.add_edge("critique", "format").topological_sort()
    ['ingest', 'summarize', 'critique', 'format']
"""

from __future__ import annotations


def _resolve_version() -> str:
    """Resolve the version: a VERSION file (dev source of truth) wins over installed metadata."""
    from pathlib import Path

    version = "0.0.0"
    try:
        from importlib.metadata import version as _dist_version

        version = _dist_version("concursus")
    except Exception:  # pragma: no cover - not installed / odd checkout
        pass
    _v_file = Path(__file__).resolve().parent.parent.parent / "VERSION"
    if _v_file.exists():
        try:
            text = _v_file.read_text().strip()
            if text:
                version = text
        except OSError:
            pass
    return version


__version__ = _resolve_version()

from .dag import AgentDAG, DAGError
from .manifest import AgentManifest, ManifestError

__all__ = [
    "AgentDAG",
    "DAGError",
    "AgentManifest",
    "ManifestError",
    "__version__",
]
