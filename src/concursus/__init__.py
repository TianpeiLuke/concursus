"""Concursus — compile a DAG of subagents into an orchestrated team on AWS Bedrock AgentCore.

Where **cursus** compiles a pipeline DAG + configs into a SageMaker pipeline, **Concursus**
(Latin *"a running-together / convergence"*) compiles an ``AgentDAG`` + per-agent
``.agent.yaml`` manifests into (1) an AgentCore provisioning plan — one ``CreateAgentRuntime``
per agent — and (2) a supervisor that dispatches the agents in topological order, wires each
agent's declared output into its dependents' input, and routes shared state through AgentCore
Memory. It is the coordinator AgentCore deliberately does not ship.

Status: early. This release provides the declarative core — the backend-agnostic
:class:`~concursus.dag.AgentDAG` and the :class:`~concursus.manifest.AgentManifest`
(``.agent.yaml``) model — plus the offline compiler: the dependency resolver
(:mod:`~concursus.resolve`), the runtime builder (:mod:`~concursus.build`), the
:class:`~concursus.assemble.OrchestrationAssembler` (DAG + manifests → a
:class:`~concursus.assemble.ProvisioningPlan`), and the topological
:class:`~concursus.supervisor.Supervisor`. Provisioning + invocation over AWS stay behind
the optional ``[agentcore]`` extra (boto3 is imported lazily).

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

from .assemble import OrchestrationAssembler, ProvisioningPlan
from .build import BuildPlanEntry, RuntimeBuilderFactory
from .dag import AgentDAG, DAGError
from .filevault import FileVaultStateStore
from .manifest import AgentManifest, ManifestError
from .provision import Clients, ProvisionError, provision_plan
from .resolve import AgentRef, AlignmentError, check_alignment, resolve_edges
from .distill import (
    build_precedent_payload,
    distill_run,
    distill_store,
    load_precedents,
    render_precedent_hub,
)
from .rundb import build_precedent_db, build_run_db
from .rungraph import RunGraph, RunGraphError
from .runindex import PrecedentIndex, RunIndex
from .statestore import (
    InProcessStateStore,
    MemoryStateStore,
    Record,
    StateStore,
    content_hash,
)
from .supervisor import SchemaError, Supervisor

__all__ = [
    "AgentDAG",
    "DAGError",
    "AgentManifest",
    "ManifestError",
    "AgentRef",
    "AlignmentError",
    "resolve_edges",
    "check_alignment",
    "RuntimeBuilderFactory",
    "BuildPlanEntry",
    "OrchestrationAssembler",
    "ProvisioningPlan",
    "Supervisor",
    "SchemaError",
    "StateStore",
    "InProcessStateStore",
    "MemoryStateStore",
    "FileVaultStateStore",
    "Record",
    "content_hash",
    "RunGraph",
    "RunGraphError",
    "RunIndex",
    "PrecedentIndex",
    "build_run_db",
    "build_precedent_db",
    "distill_run",
    "distill_store",
    "build_precedent_payload",
    "render_precedent_hub",
    "load_precedents",
    "provision_plan",
    "Clients",
    "ProvisionError",
    "__version__",
]
