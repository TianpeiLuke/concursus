"""Agent Harness — per-node wrapper that owns I/O, contracts, and log forwarding.

Creates an AgentInvoker in __init__ based on the manifest. The harness owns:
1. Input deref (S3 → materialized data)
2. Prompt serialization
3. Invoke + log stream extraction
4. Log forwarding to ExecutionMonitor (if present)
5. Output write (data → S3 ArtifactRef)
6. Contract enforcement
7. Return envelope to Supervisor
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Protocol

from .invoker import AgentInvoker
from .types import HealthSignal, HealthStatus, LogEvent, PreemptiveTermination

logger = logging.getLogger(__name__)


def _schema_properties(block: Any) -> Dict[str, Any]:
    """The per-field map out of a contract block, accepting BOTH declared shapes.

    ``contract.outputs`` had two readers that disagreed:
    :func:`~concursus.execute.supervisor.validate_output` accepted a nested
    ``{"properties": {...}}`` block *or* a flat ``{field: schema}`` map, while this harness read
    ``.get("properties", {})`` exclusively — so a contract authored flat validated fine at the
    Supervisor and then silently produced NO outputs here, because ``_write_outputs`` iterates this
    map and drops anything it does not find.

    Silently is the operative word: the node returned ``{}``, and the failure surfaced as a contract
    violation about a missing required field rather than as a shape mismatch. This mirrors
    ``validate_output``'s logic exactly, so one contract declaration now means the same thing to both
    readers.
    """
    if not isinstance(block, dict):
        return {}
    props = block.get("properties")
    if isinstance(props, dict):
        return props
    # flat form: every key is a field, except the top-level `required` list
    return {k: v for k, v in block.items() if k != "required"}


class ExecutionMonitor(Protocol):
    """Protocol for the optional monitor injected by the Supervisor."""

    async def watch(self, log_stream: AsyncIterator[LogEvent]) -> HealthSignal: ...


class ObjectStore(Protocol):
    """Protocol for S3/file object store operations (injectable for testing)."""

    async def get_object(self, uri: str) -> bytes: ...
    async def put_object(self, uri: str, data: bytes, content_type: str) -> str: ...


class AgentHarness:
    """Per-node wrapper. Creates invoker, manages I/O, enforces contracts.

    Args:
        manifest: The agent's manifest dict.
        store: Object store for artifact read/write (injectable; use FileStore for tests).
        clients: Optional pre-built client bundle passed to invoker.
        monitor: Optional ExecutionMonitor injected by Supervisor for log streaming.
        output_prefix: Compiler-vended S3 prefix for this node's outputs.
    """

    def __init__(
        self,
        manifest: Dict[str, Any],
        store: ObjectStore,
        clients: Optional[Dict[str, Any]] = None,
        monitor: Optional[ExecutionMonitor] = None,
        output_prefix: str = "",
    ):
        self.manifest = manifest
        self.store = store
        self.monitor = monitor
        self.output_prefix = output_prefix
        self.invoker = AgentInvoker(manifest=manifest, clients=clients)

        # Extract contract for validation
        self.contract = manifest.get("contract", {})
        self.input_schema = _schema_properties(self.contract.get("inputs", {}))
        self.output_schema = _schema_properties(self.contract.get("outputs", {}))
        self.output_mapping = manifest.get("output_mapping", {})

    async def run(self, envelope: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the full harness lifecycle. Returns output envelope."""
        node_id = self.manifest.get("name", "unknown")
        logger.info(f"[{node_id}] Harness: starting")

        # The PLAN's authored declaration outranks the manifest's (manifest I/O is demoted to a
        # fallback, not deleted). Only the planner knows why a downstream node needs a particular
        # shape; a manifest can only state a per-agent default. Applied once, up front, so every
        # downstream step -- deref, prompt, write, contract check -- reads one effective schema.
        io_decl = envelope.get("io") or {}
        if io_decl.get("inputs"):
            self.input_schema = _schema_properties(io_decl["inputs"])
        if io_decl.get("outputs"):
            self.output_schema = _schema_properties(io_decl["outputs"])

        # Step 1: Deref inputs
        inputs = await self._deref_inputs(envelope)

        # Step 2: Serialize prompt
        prompt = self._serialize_prompt(inputs, envelope)

        # Step 3 + 4: Invoke with optional log forwarding
        if self.monitor:
            raw, log_stream = await self.invoker.invoke_with_tap(
                prompt, inputs, envelope.get("context", {})
            )
            health = await self.monitor.watch(log_stream)
            if health.should_terminate:
                logger.warning(f"[{node_id}] Monitor signaled termination: {health.reason}")
                raise PreemptiveTermination(health.reason, health)
        else:
            raw = await self.invoker.invoke(
                prompt, inputs, envelope.get("context", {})
            )

        # Step 5: Write outputs → ArtifactRef
        refs = await self._write_outputs(raw)

        # Step 6: Contract enforcement
        self._check_contract(refs)

        logger.info(f"[{node_id}] Harness: complete")
        return refs

    # ──────────────────────────────────────────────────────────────────────
    # Step 1: Input deref
    # ──────────────────────────────────────────────────────────────────────

    async def _deref_inputs(self, envelope: Dict[str, Any]) -> Dict[str, Any]:
        """Materialize inputs: deref ArtifactRefs from S3, pass scalars inline."""
        raw_inputs = envelope.get("inputs", {})
        materialized = {}

        for field_name, value in raw_inputs.items():
            field_schema = self.input_schema.get(field_name, {})

            if field_schema.get("type") == "artifact" and isinstance(value, dict) and "uri" in value:
                # Deref: fetch from object store and verify hash
                data = await self.store.get_object(value["uri"])
                expected_hash = value.get("content_hash")
                if expected_hash:
                    actual_hash = f"sha256:{hashlib.sha256(data).hexdigest()}"
                    if actual_hash != expected_hash:
                        raise ValueError(
                            f"Hash mismatch for {field_name}: "
                            f"expected {expected_hash}, got {actual_hash}"
                        )
                # Deserialize based on content_type
                content_type = value.get("content_type", "application/json")
                materialized[field_name] = self._deserialize(data, content_type)
            else:
                # Control scalar — pass inline
                materialized[field_name] = value

        return materialized

    # ──────────────────────────────────────────────────────────────────────
    # Step 2: Prompt serialization
    # ──────────────────────────────────────────────────────────────────────

    def _serialize_prompt(self, inputs: Dict[str, Any], envelope: Dict[str, Any]) -> str:
        """Assemble the prompt from context + task + declared output contract + inputs."""
        parts = []

        # Static context (tiered by trust — already projected by Supervisor)
        static_context = envelope.get("static_context", "")
        if static_context:
            parts.append(static_context)

        # Task description
        task = envelope.get("task", "")
        if task:
            parts.append(f"Task: {task}")

        # The declared OUTPUT CONTRACT. The harness already serializes each field by its declared
        # `content_type` and writes it to the object store — but before this the agent was never
        # TOLD what shape to emit, so a field declared `text/markdown` or `application/json` got
        # whatever prose the agent chose and was silently encoded anyway. Rendering the contract
        # closes that loop: same declaration, now read by the prompt as well as the writer.
        output_spec = self._describe_output_contract(envelope)
        if output_spec:
            parts.append(output_spec)

        # Corrective remediation for a RETRY. Appended
        # AFTER the task, never merged into it: the compiler-vended task is frozen, and this is an
        # overlay the executor adds only when a prior attempt was terminated. Absent on a first
        # attempt, so a normal run's prompt is byte-for-byte unchanged.
        remediation = envelope.get("remediation_context", "")
        if remediation:
            parts.append(f"Correction from your previous attempt: {remediation}")

        return "\n\n".join(parts) if parts else "Execute the assigned task."

    #: Human phrasing for each supported artifact encoding, used only when rendering the prompt.
    _FORMAT_PHRASING = {
        "application/json": "a single JSON object (no prose before or after it)",
        "text/markdown": "Markdown",
        "text/csv": "CSV, including a header row",
        "text/plain": "plain text",
    }

    def _describe_output_contract(self, envelope: Dict[str, Any]) -> str:
        """Render the declared outputs as an instruction block, or ``""`` when nothing is declared.

        Prefers the plan node's authored declaration (``envelope["io"]["outputs"]``) and falls back
        to the manifest's ``contract.outputs``. The plan is the better source because only the
        planner knows *why* a downstream node needs a particular shape; the manifest fallback keeps
        agents that predate per-node authoring working unchanged.

        ``sections`` / ``keys`` are ADVISORY structural hints — they are rendered for the agent and
        are never enforced. Nothing here can fail a run.
        """
        declared = ((envelope.get("io") or {}).get("outputs")) or self.output_schema
        if not declared:
            return ""
        lines: List[str] = ["Expected outputs:"]
        for field, spec in declared.items():
            if not isinstance(spec, Mapping):
                lines.append(f"- {field}")
                continue
            required = " (required)" if spec.get("required") else ""
            if spec.get("type") == "artifact":
                ctype = spec.get("content_type", "application/json")
                phrasing = self._FORMAT_PHRASING.get(ctype, ctype)
                lines.append(f"- {field}{required}: return as {phrasing}.")
                sections = spec.get("sections")
                if sections:
                    lines.append(
                        "    Use exactly these top-level sections, in order: "
                        + ", ".join(str(s) for s in sections)
                    )
                keys = spec.get("keys")
                if isinstance(keys, Mapping):
                    lines.append(
                        "    Include exactly these keys: "
                        + ", ".join(f"{k} ({v})" for k, v in keys.items())
                    )
                elif keys:
                    lines.append(
                        "    Include exactly these keys: "
                        + ", ".join(str(k) for k in keys)
                    )
            else:
                typ = spec.get("type", "any")
                lines.append(f"- {field}{required}: {typ}")
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────────────────
    # Step 5: Output write
    # ──────────────────────────────────────────────────────────────────────

    async def _write_outputs(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Map raw agent response to contract outputs, write artifacts to S3."""
        refs: Dict[str, Any] = {}

        # Apply output mapping (response_key → contract_field)
        mapped = {}
        if self.output_mapping:
            for response_key, contract_field in self.output_mapping.items():
                if response_key in raw:
                    mapped[contract_field] = raw[response_key]
            # Also pass through any keys not in the mapping
            for k, v in raw.items():
                if k not in self.output_mapping:
                    mapped[k] = v
        else:
            mapped = raw

        for field_name, field_schema in self.output_schema.items():
            value = mapped.get(field_name)
            if value is None:
                continue

            if field_schema.get("type") == "artifact":
                # Write to object store, return ArtifactRef
                content_type = field_schema.get("content_type", "application/json")
                data = self._serialize(value, content_type)
                uri = f"{self.output_prefix}/{field_name}"
                await self.store.put_object(uri, data, content_type)

                refs[field_name] = {
                    "uri": uri,
                    "content_type": content_type,
                    "content_hash": f"sha256:{hashlib.sha256(data).hexdigest()}",
                    "bytes": len(data),
                }
            else:
                # Control scalar — pass inline
                refs[field_name] = value

        return refs

    # ──────────────────────────────────────────────────────────────────────
    # Step 6: Contract enforcement
    # ──────────────────────────────────────────────────────────────────────

    def _check_contract(self, refs: Dict[str, Any]) -> None:
        """Validate outputs against declared contract schema."""
        for field_name, field_schema in self.output_schema.items():
            required = field_schema.get("required", False)
            if required and field_name not in refs:
                raise ValueError(
                    f"Required output field '{field_name}' missing from agent response"
                )

            if field_name in refs and field_schema.get("type") == "artifact":
                ref = refs[field_name]
                if not isinstance(ref, dict) or "uri" not in ref:
                    raise ValueError(
                        f"Output field '{field_name}' declared as artifact but got: {type(ref).__name__}"
                    )

    # ──────────────────────────────────────────────────────────────────────
    # Serialization helpers
    # ──────────────────────────────────────────────────────────────────────

    def _serialize(self, value: Any, content_type: str) -> bytes:
        """Serialize a value to bytes based on content_type.

        ``text/csv`` is handled before the generic ``text/*`` branch: a list-of-rows or a
        list-of-dicts is written as real CSV, so a node that declares a CSV handoff produces a
        parseable artifact rather than Python's ``repr`` of a list.
        """
        if content_type == "application/json":
            return json.dumps(value, default=str).encode("utf-8")
        elif content_type == "text/csv":
            return self._to_csv(value).encode("utf-8")
        elif content_type.startswith("text/"):
            return str(value).encode("utf-8")
        elif isinstance(value, bytes):
            return value
        else:
            return json.dumps(value, default=str).encode("utf-8")

    @staticmethod
    def _to_csv(value: Any) -> str:
        """Render ``value`` as CSV text, passing an already-CSV string through unchanged."""
        if isinstance(value, str):
            return value  # the agent already emitted CSV text
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        rows = value if isinstance(value, (list, tuple)) else [value]
        buf = io.StringIO()
        if rows and isinstance(rows[0], Mapping):
            fieldnames: List[str] = []
            for row in rows:
                for key in row:
                    if key not in fieldnames:
                        fieldnames.append(str(key))
            writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fieldnames})
        else:
            writer = csv.writer(buf, lineterminator="\n")
            for row in rows:
                writer.writerow(row if isinstance(row, (list, tuple)) else [row])
        return buf.getvalue()

    def _deserialize(self, data: bytes, content_type: str) -> Any:
        """Deserialize bytes based on content_type.

        Deliberately ASYMMETRIC for ``text/csv``: the write side accepts structured rows and emits
        real CSV, but the read side returns **text**, because the standing convention is that
        ``application/json`` yields data while ``text/*`` yields text. Returning parsed rows here
        instead would silently change what every existing CSV consumer receives — the e2e leaf
        agents that read ``inputs['source_data']`` as CSV text proved that the first time it was
        tried. A consumer that wants rows can ``csv.DictReader`` the text itself.
        """
        if content_type == "application/json":
            return json.loads(data)
        elif content_type.startswith("text/"):
            return data.decode("utf-8")
        else:
            return data
