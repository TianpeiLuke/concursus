"""HarnessFactory — bridges the AgentHarness into the Supervisor via the NodeExecutor seam.

The Supervisor supports pluggable node-kind handlers via `node_executors` + `node_kind_fn`.
This module provides:

1. `HarnessFactory` — builds harness instances per-node with proper config
2. `harness_node_executor` — a NodeExecutor that separates the payload into structured
   {task, inputs, context} and runs it through the harness (Option B wiring)
3. `make_node_kind_fn` — a selector that routes nodes with `runtime` in their manifest
   to the harness executor; others fall back to the default dispatch

Usage:
    factory = HarnessFactory(manifests=raw_manifests, store=s3_store)

    supervisor = Supervisor(
        plan,
        manifests,
        node_executors={"harness": factory.make_executor()},
        node_kind_fn=factory.make_kind_fn(),
    )
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Protocol, TYPE_CHECKING

from .futility import run_registered
from .harness import AgentHarness, ExecutionMonitor, ObjectStore
from .types import PreemptiveTermination

if TYPE_CHECKING:
    from concursus.core.manifest import AgentManifest

logger = logging.getLogger(__name__)

# The node-kind key we register
HARNESS_NODE_KIND = "harness"


class MonitorFactory(Protocol):
    """Protocol for creating ExecutionMonitors per-node."""

    def create(self, node_id: str, manifest: Dict[str, Any]) -> Optional[ExecutionMonitor]: ...


class HarnessFactory:
    """Constructs AgentHarness instances and provides Supervisor integration hooks.

    Args:
        manifests: Dict of node_id -> manifest dict (raw dicts, not AgentManifest objects).
        store: ObjectStore implementation (S3Store or FileStore).
        monitor_factory: Optional factory for creating per-node ExecutionMonitors.
        output_prefix_root: S3 prefix root for output artifacts.
        clients: Optional pre-built client bundle (shared across harness instances).
    """

    def __init__(
        self,
        manifests: Dict[str, Dict[str, Any]],
        store: ObjectStore,
        monitor_factory: Optional[MonitorFactory] = None,
        output_prefix_root: str = "",
        clients: Optional[Dict[str, Any]] = None,
    ):
        self.manifests = manifests
        self.store = store
        self.monitor_factory = monitor_factory
        self.output_prefix_root = output_prefix_root
        self.clients = clients

    def create_harness(self, node_id: str, session_id: str) -> AgentHarness:
        """Build an AgentHarness for a specific node invocation."""
        manifest = self.manifests.get(node_id, {})

        monitor = None
        if self.monitor_factory:
            monitor = self.monitor_factory.create(node_id, manifest)

        output_prefix = (
            f"{self.output_prefix_root}/{session_id}/{node_id}"
            if self.output_prefix_root else ""
        )

        return AgentHarness(
            manifest=manifest,
            store=self.store,
            clients=self.clients,
            monitor=monitor,
            output_prefix=output_prefix,
        )

    def make_executor(self):
        """Return a NodeExecutor function for the Supervisor.

        The executor:
        1. Separates the Supervisor's assembled payload into structured {task, inputs, context}
        2. Builds an envelope the harness understands
        3. Runs the harness
        4. Stores the result via the Supervisor's StateStore

        Signature matches NodeExecutor:
            (supervisor, node, inputs, wiring) -> None
        """
        factory = self  # capture for closure

        def _harness_executor(supervisor, node: str, inputs: Dict[str, Any], wiring: list) -> None:
            """NodeExecutor that routes through AgentHarness with structured envelope."""
            # the failure-record writer and the output gates are imported from the
            # supervisor rather than re-implemented, so the two node-kind branches cannot drift on
            # either. Lazy, mirroring this module's no-import-time-dependency rule.
            from concursus.execute.supervisor import (
                _ARN_PLACEHOLDER,
                _FAILURE_CRASH,
                _FAILURE_FUTILITY,
                _FAILURE_PREEMPTIVE,
                check_acceptance,
                check_hive_contract,
                extract,
                record_failure,
                validate_output,
            )
            from concursus.execute.monitor import remediation_for

            # --- Assemble structured envelope (Option B) ---

            # 1. Get the manifest's static context (tiered by trust, already projected)
            manifest = supervisor._manifests.get(node)
            frozen_contract = getattr(supervisor._plan, "payload_contract", None)
            static_context = ""
            if isinstance(frozen_contract, dict) and node in frozen_contract:
                ctx = frozen_contract[node].get("static_context") if isinstance(frozen_contract[node], dict) else None
                if isinstance(ctx, (str, dict)):
                    static_context = ctx if isinstance(ctx, str) else json.dumps(ctx)

            # 2. Resolve wired inputs from upstream producers
            wired_inputs: Dict[str, Any] = {}
            completed = supervisor._store.completed()
            for ref in wiring:
                if ref.producer in completed:
                    upstream_output = extract(
                        supervisor._store.get(ref.producer), ref.path
                    )
                    # Pass the value as-is — if it's an ArtifactRef dict (has "uri" key),
                    # the harness's _deref_inputs will fetch it from the ObjectStore.
                    # If it's a scalar, it passes through inline. The harness decides
                    # based on the input contract's declared type.
                    wired_inputs[ref.input_name] = upstream_output

            # 3. Get node-specific external inputs
            explicit = inputs.get(node)
            if isinstance(explicit, dict):
                external_inputs = dict(explicit)
            elif not wiring:
                external_inputs = dict(inputs)
            else:
                external_inputs = {}

            # 4. Merge: wired inputs overlay external inputs
            merged_inputs = {**external_inputs, **wired_inputs}

            # 5. Build the task description from the plan
            task = ""
            io_decl: Dict[str, Any] = {}
            if isinstance(frozen_contract, dict) and node in frozen_contract:
                node_contract = frozen_contract[node]
                if isinstance(node_contract, dict):
                    task = node_contract.get("task", "")
                    io_decl = node_contract.get("io", {}) or {}

            # 6. Construct the structured envelope for the harness
            envelope = {
                "task": task,
                "io": io_decl,
                "inputs": merged_inputs,
                "context": {"session_id": supervisor._session_id},
                "static_context": static_context,
            }

            # --- Run harness ---
            # Provenance metadata, computed BEFORE the run so every exit path -- success or any
            # failure class -- records the same edge and schema facts (the failure
            # writes used to omit these, losing a failed node's provenance from the log).
            consumes = [f"{r.producer}:{r.path}" for r in wiring]
            schema = manifest.name if manifest else None

            # --- Retry policy  -----------------------------------------
            # `max_attempts` now means the same thing on both node-kind branches. Two constraints
            # shape it, and neither is stylistic:
            #
            # SIDE EFFECTS. `Supervisor._dispatch` retries blindly with no side_effecting guard --
            # harmless only because max_attempts defaults to 1. A side-effecting agent that fails
            # AFTER acting (model rollback, ETL restart, code-fix authoring) would perform the effect
            # a second time on retry, so those nodes get exactly one attempt regardless of the dial.
            # Being wrong in this direction wastes a node; being wrong in the other corrupts state.
            #
            # FUTILITY IS NEVER RETRIED. A futility-cancelled node's output is provably unconsumable
            # , so retrying it is guaranteed waste -- handled by returning from that branch
            # rather than falling through to the loop.
            side_effecting = bool(
                getattr(manifest, "side_effecting", None)
                if manifest is not None
                else factory.manifests.get(node, {}).get("side_effecting", False)
            )
            configured = max(1, int(getattr(supervisor, "_max_attempts", 1) or 1))
            max_attempts = 1 if side_effecting else configured

            # --- binding integrity  ------------------------------
            # Reuses the SHIPPED `Supervisor._check_arn_integrity` rather than reimplementing it, so
            # the two branches cannot drift on what "a real and current binding" means. Scoped to
            # agentcore-backed nodes: a `callable`/`http` node legitimately has no runtime ARN, and
            # failing those would break every in-process agent.
            #
            # Known limitation until 's P3 converges the manifests: the harness's agentcore
            # backend addresses by `runtime.agent_id`, while this verifies the plan's compiled ARN.
            # They are two declarations of one binding, so a disagreement between them is still
            # undetected -- but an UNPROVISIONED or STALE plan binding is now caught here, before
            # invoke, instead of surfacing as an opaque AWS error mid-run.
            raw_runtime = (factory.manifests.get(node) or {}).get("runtime") or {}
            if raw_runtime.get("backend") == "agentcore":
                checker = getattr(supervisor, "_check_arn_integrity", None)
                if callable(checker):
                    arn = getattr(supervisor, "_arns", {}).get(node, _ARN_PLACEHOLDER)
                    integrity_error = checker(node, arn, manifest)
                    if integrity_error is not None:
                        if getattr(supervisor, "_on_error", "raise") != "record":
                            raise integrity_error
                        record_failure(
                            supervisor,
                            node,
                            failure_class=_FAILURE_CRASH,
                            error=str(integrity_error),
                            error_type=type(integrity_error).__name__,
                            consumes=consumes,
                            schema=schema,
                        )
                        return

            tokens = getattr(supervisor, "_cancel_tokens", None)
            attempt = 0
            # , rule tier: remember which failure modes
            # have already had a correction prescribed. A mode that recurs AFTER its remediation was
            # applied is not worth retrying again -- the fix did not take -- so it escalates to a
            # terminal record instead of prescribing the same text a second time.
            remediated: set = set()
            attempt_envelope = envelope
            while True:
                attempt += 1
                # A FRESH harness per attempt: ExecutionMonitor accumulates error counts and tool
                # signatures, so reusing one would re-trip its thresholds on the first event of the
                # retry and make every retry a no-op.
                harness = factory.create_harness(node, supervisor._session_id)

                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                # make this node cancellable. asyncio.run builds its event loop internally
                # and returns no handle, so the task must SELF-register from inside the coroutine. The
                # registry exists only while an opt-in cancel_futile parallel wave is running; None
                # (the default, and every serial run) leaves the coroutine unwrapped. The registry
                # keys on (node, attempt), which is what makes retries safe here -- a stale attempt's
                # condemnation cannot revoke its successor.
                coro = harness.run(attempt_envelope)
                if tokens is not None:
                    coro = run_registered(tokens, node, attempt, coro)

                try:
                    if loop and loop.is_running():
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                            result = pool.submit(asyncio.run, coro).result()
                    else:
                        result = asyncio.run(coro)
                except PreemptiveTermination as exc:
                    # The node's own monitor judged the run unhealthy. Retryable:                     # ladder is continue > retry > terminate, and principle 6 says even a
                    # rule-detected loop should fire terminate->retry.
                    signal = getattr(exc, "health_signal", None)
                    mode = getattr(signal, "failure_mode", None)
                    amendment = remediation_for(signal) if signal is not None else None
                    # Do not prescribe the same correction twice: a mode that survived its own fix
                    # escalates rather than looping on advice the agent has already been given.
                    already_tried = mode is not None and mode in remediated
                    if attempt < max_attempts and not already_tried:
                        if amendment:
                            # An OVERLAY on the envelope, never a mutation of the frozen task.
                            attempt_envelope = {
                                **envelope, "remediation_context": amendment
                            }
                            if mode is not None:
                                remediated.add(mode)
                        continue
                    record_failure(
                        supervisor,
                        node,
                        failure_class=_FAILURE_PREEMPTIVE,
                        error=str(exc),
                        error_type="PreemptiveTermination",
                        consumes=consumes,
                        schema=schema,
                        address=node if max_attempts == 1 else f"{node}/{attempt}",
                    )
                    return
                except asyncio.CancelledError:
                    # futility cancellation: while this node was running, every consumer of
                    # its output became unreachable, so the Supervisor condemned it. Never retried --
                    # the work is provably unconsumable. Record the skip with the reason the registry
                    # retained; blocked_on keeps summary_line() legible in the same vocabulary as a
                    # blocked-skip.
                    reason = (tokens.reason_for(node) if tokens else None) or "futility-cancelled"
                    record_failure(
                        supervisor,
                        node,
                        failure_class=_FAILURE_FUTILITY,
                        error=reason,
                        error_type="CancelledError",
                        consumes=consumes,
                        schema=schema,
                        blocked_on=reason,
                    )
                    return
                except Exception as exc:  # noqa: BLE001 - symmetry with Supervisor._dispatch
                    # ANY other failure from the harness lifecycle — most importantly a CONTRACT
                    # VIOLATION from `harness._check_contract` (a required output field missing, i.e.
                    # the agent ran healthily but did not complete its task). `_check_contract` raises
                    # a plain ValueError, and before this branch existed it escaped to `_run_parallel`
                    # and aborted the WHOLE run even under on_error='record' — unlike
                    # Supervisor._dispatch, which records. That asymmetry meant one agent failing its
                    # contract killed a run that should merely have pruned that node's subtree.
                    #
                    # Deliberately placed AFTER the two specific handlers: PreemptiveTermination is an
                    # Exception subclass and must keep its own class, while asyncio.CancelledError
                    # derives from BaseException (3.8+) so it is never swallowed here.
                    if getattr(supervisor, "_on_error", "raise") != "record":
                        raise
                    if attempt < max_attempts:
                        continue  # retry the SAME manifest-pinned node id
                    record_failure(
                        supervisor,
                        node,
                        failure_class=_FAILURE_CRASH,
                        error=str(exc),
                        error_type=type(exc).__name__,
                        consumes=consumes,
                        schema=schema,
                        address=node if max_attempts == 1 else f"{node}/{attempt}",
                    )
                    return

                break  # invoked cleanly — fall through to the output gates

            # --- Output gates  ------------------------------------------------
            # The harness's own `_check_contract` only asserts required-field PRESENCE on the written
            # refs. These are the manifest-level gates `Supervisor._dispatch` has always run and this
            # branch did not, which meant a harness node could admit a shape-invalid output that the
            # legacy path would have rejected. Same dials, same order, same failure class.
            #
            # NOTE what is being validated: `result` is the harness's REFS map, and
            # `harness._write_outputs` iterates the node's DECLARED outputs and DROPS anything
            # undeclared. Both this gate and that write now read the SAME per-plan-node declaration
            # (frozen at `payload_contract[node]["io"]["outputs"]`), which is what closes 's
            # P3 — there is no longer a second, divergent manifest copy to disagree with.
            try:
                validate_output(result, manifest.output_schema if manifest else {})
                if getattr(supervisor, "_check_acceptance", False) and (
                    getattr(supervisor, "_acceptance_fn", None) is None
                    or supervisor._acceptance_fn(node)
                ):
                    check_hive_contract(result)
                    check_acceptance(result, manifest.output_schema if manifest else {})
            except Exception as exc:  # noqa: BLE001 - mirrors _dispatch's validation handling
                if getattr(supervisor, "_on_error", "raise") != "record":
                    raise
                record_failure(
                    supervisor,
                    node,
                    failure_class=_FAILURE_CRASH,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    consumes=consumes,
                    schema=schema,
                )
                return

            # --- Store result (same as _dispatch does) ---
            supervisor._store.put(
                node,
                result,
                meta={"producer": node, "consumes": consumes, "schema": schema},
            )

        return _harness_executor

    def make_kind_fn(self) -> Callable[[str], str]:
        """Return a node_kind_fn that routes nodes with `runtime` in their manifest to harness.

        Nodes without a `runtime` block use the default Supervisor dispatch (legacy path).
        """
        manifests = self.manifests

        def _kind_fn(node: str) -> str:
            manifest = manifests.get(node, {})
            if "runtime" in manifest:
                return HARNESS_NODE_KIND
            return "default"

        return _kind_fn


def make_harness_supervisor_factory(harness_factory: "HarnessFactory"):
    """Build a GovernorLoop-compatible ``supervisor_factory`` with the harness seam wired in.

    The GovernorLoop constructs one Supervisor per episode via its ``supervisor_factory``
    seam, calling it with ``plan, manifests, store, invoke_fn, arns, session_id`` (plus
    ``held`` only when a Trust-Ladder scheduler is configured). The default factory
    (:func:`concursus.governor.loop._default_supervisor_factory`) does NOT pass
    ``node_executors`` / ``node_kind_fn``, so loop-driven runs never reach the harness.

    This wrapper preserves the default factory's exact contract (including the
    held-set semantics) and additionally injects:

    - ``node_executors``: the shipped ``NODE_EXECUTORS`` registry plus the harness
      executor under :data:`HARNESS_NODE_KIND`
    - ``node_kind_fn``: routes nodes whose RAW manifest (in ``harness_factory``)
      declares a ``runtime`` block to the harness; all other nodes keep the legacy
      ``_dispatch`` path byte-for-byte.

    Usage::

        factory = HarnessFactory(manifests=raw_manifests, store=s3_store)
        loop = GovernorLoop(
            goal, agent_manifests,
            supervisor_factory=make_harness_supervisor_factory(factory),
            ...,
        )
    """

    def _factory(*, plan, manifests, store, invoke_fn, arns, session_id, held=None):
        # Imported here so this module never hard-depends on the supervisor at import
        # time (mirrors the loop's own lazy construction).
        from concursus.execute.supervisor import NODE_EXECUTORS, Supervisor

        executors = dict(NODE_EXECUTORS)
        executors[HARNESS_NODE_KIND] = harness_factory.make_executor()

        kwargs: Dict[str, Any] = dict(
            invoke_fn=invoke_fn,
            arns=arns,
            state_store=store,
            session_id=session_id,
            node_executors=executors,
            node_kind_fn=harness_factory.make_kind_fn(),
        )
        held_set = set(held or ())
        if held_set:
            kwargs["held"] = held_set
        return Supervisor(plan, manifests, **kwargs)

    return _factory
