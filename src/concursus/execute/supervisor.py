"""The **Supervisor** — dispatch a provisioning plan's agents in topological order.

Runtime half of Concursus: walk ``plan.order``, build each agent's invoke payload from the
external run inputs overlaid with its resolved upstream outputs (the :class:`AgentRef`
wiring), call its AgentCore endpoint, shape-check the result against the manifest's output
schema, and thread every output forward into its dependents. Outputs thread through a
:class:`~concursus.statestore.StateStore` seam (the offline :class:`InProcessStateStore` by
default, an AgentCore ``MemoryStateStore`` opt-in) so a run can *resume* — a node whose
validated output is already recorded is skipped — and every recorded ``consumes`` edge feeds
:meth:`Supervisor.context`, graph-aware upstream context. The invoke transport is injectable
(:data:`InvokeFn`); the default lazily binds boto3's ``bedrock-agentcore`` data-plane client,
so importing this module needs no AWS SDK. One stable ``runtimeSessionId`` spans every invoke
in a run (session affinity + AgentCore Memory).
"""

from __future__ import annotations

import json
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from ..core.resolve import extract
from ..reasoning.inner_graph import _cpu_capacity, resolve_ceiling
from ..state.rungraph import RunGraph
from ..state.runindex import RunIndex
from ..state.statestore import InProcessStateStore, StateStore, content_hash
from .futility import CancelTokenRegistry, futility_closure, invert_wiring

if TYPE_CHECKING:  # pragma: no cover - hints only; keeps the runtime import graph AWS-free
    from ..assemble.assemble import ProvisioningPlan
    from ..core.manifest import AgentManifest
    from ..core.resolve import AgentRef

# The invoke transport: ``(arn, qualifier, session_id, payload_bytes) -> parsed output dict``.
InvokeFn = Callable[[str, str, str, bytes], dict]

_ARN_PLACEHOLDER = "<agent-runtime-arn>"  # stands in until the runtime is provisioned

# -- failed-record classification (on_error='record' only) ------------------
#: A failed record's ``failure_class`` when THIS node's own invoke / validate / ARN-integrity
#: raised — a genuine, self-inflicted failure (its subtree is what gets pruned downstream).
_FAILURE_CRASH = "crash"
#: A failed record's ``failure_class`` when the node was NEVER invoked because a producer it
#: consumes failed or was itself held/blocked — a pruned-subtree skip, not this node's fault.
_FAILURE_HOLD = "hold"
#: A failed record's ``failure_class`` when the node's per-node monitor judged the run unhealthy and
#: terminated it mid-flight (:class:`~.types.PreemptiveTermination`). Written by the
#: harness node executor, not by :meth:`Supervisor._dispatch`.
_FAILURE_PREEMPTIVE = "preemptive_termination"
#: A failed record's ``failure_class`` when the Supervisor cancelled this node mid-flight because
#: every consumer of its output had become unreachable (futility closure). Morally a
#: :data:`_FAILURE_HOLD` — not the node's own fault — but detected DURING dispatch rather than
#: before it, which is why it earns its own class.
_FAILURE_FUTILITY = "futility_cancelled"
#: Every recognized ``failure_class``, in reporting order. :meth:`Supervisor.summary` emits one count
#: per entry (always present, possibly zero) and :meth:`Supervisor._classify_failure` honors any of
#: them when written explicitly.
_FAILURE_CLASSES = (
    _FAILURE_CRASH,
    _FAILURE_HOLD,
    _FAILURE_PREEMPTIVE,
    _FAILURE_FUTILITY,
)


def record_failure(
    supervisor: "Supervisor",
    node: str,
    *,
    failure_class: str,
    error: str,
    error_type: str,
    consumes: Optional[List[str]] = None,
    schema: Optional[str] = None,
    blocked_on: Optional[str] = None,
    address: Optional[str] = None,
) -> None:
    """Write THE failed record for ``node`` — the single writer both node-kind branches share.

    Before this existed, :meth:`Supervisor._dispatch` and the harness node executor
    each hand-rolled their own ``store.put`` for failures, and the two drifted twice:

    * the harness path had no generic ``except Exception`` branch at all, so a contract violation
      aborted an ``on_error='record'`` run instead of pruning one subtree;
    * the harness path's failure writes then omitted the ``consumes`` / ``schema`` metadata its own
      SUCCESS write included, silently dropping a failed node's edge and schema provenance from the
      append-only log.

    Both are classes of bug a shared writer makes unrepresentable. Keeping the metadata shape in one
    place is the point — ``producer`` is always the node, ``consumes`` mirrors the wiring, and
    ``blocked_on`` is set only when a reason is worth surfacing to ``summary_line()``.

    ``address`` defaults to ``node`` but is overridable, because the retry path deliberately encodes
    the attempt (``f"{node}/{attempt}"``) so successive attempts are distinguishable in the log. That
    is a real difference between the two call sites, not an inconsistency to normalize away.

    Caller-agnostic on purpose: it does NOT consult ``on_error``. Deciding whether to record or
    re-raise stays with the caller, because only the caller knows whether the exception it is holding
    is recoverable in its own context.
    """
    meta: Dict[str, Any] = {
        "status": "failed",
        "producer": node,
        "failure_class": failure_class,
        "address": address if address is not None else node,
    }
    if consumes is not None:
        meta["consumes"] = consumes
    if schema is not None:
        meta["schema"] = schema
    if blocked_on is not None:
        meta["blocked_on"] = blocked_on
    supervisor._store.put(node, {"error": error, "error_type": error_type}, meta=meta)


class SchemaError(ValueError):
    """Raised when an agent's output fails to satisfy its declared output schema."""


class PlanIdentityError(ValueError):
    """Raised when a resume replays against a plan whose content-hash differs from the persisted run.

    The append-only log is the single source of truth; resume = replay of that log against a
    FROZEN plan. If the plan handed to :meth:`Supervisor.run` on resume is not the same plan the
    log was recorded under, silently skipping the recorded ``completed()`` nodes would mis-replay
    (a node id could now carry different wiring/entry). This turns that latent, silent hazard into
    a loud, legible error — re-compile monotonically (see
    :class:`~concursus.assemble.assemble.OrchestrationAssembler.recompile`) instead of
    resuming against a divergent plan.
    """


# -- plan content-identity (resume=replay integrity) ------------------------
#: Reserved store node id under which the frozen plan's content-hash is persisted (opt-in). It is
#: NOT a DAG node (never in ``plan.order``), so it never appears in :meth:`Supervisor.run`'s return.
_PLAN_IDENTITY_NODE = "__plan_identity__"


def plan_fingerprint(plan: "ProvisioningPlan") -> str:
    """A stable content-hash of a plan's compiled identity — ``order`` + ``wiring`` + ``entries``.

    Pure and duck-typed: reads only ``plan.order`` (the topological node list), ``plan.wiring``
    (the resolved :class:`AgentRef` producer→consumer edges, projected to
    ``[producer, path, input_name]`` triples in their exact list order — reordering an edge is a
    real difference, so it is preserved not sorted), and ``plan.entries`` (each
    :class:`~concursus.build.build.BuildPlanEntry`'s deploy-identity ``fingerprint``, the
    stable per-node hosting hash — not the bulky wrapper/dockerfile body). Two plans with the same
    compiled identity hash identically; any change to a node's order, wiring, or hosting identity
    changes the hash. Missing attributes default to empty, so a duck-typed ``.order``/``.wiring``
    stand-in (no ``entries``) is fully supported.
    """
    order = list(getattr(plan, "order", []) or [])
    wiring_raw = getattr(plan, "wiring", {}) or {}
    wiring = {
        node: [[ref.producer, ref.path, ref.input_name] for ref in refs]
        for node, refs in wiring_raw.items()
    }
    entries_raw = getattr(plan, "entries", {}) or {}
    entries = {node: getattr(entry, "fingerprint", "") for node, entry in entries_raw.items()}
    return content_hash({"order": order, "wiring": wiring, "entries": entries})


# -- output validation ------------------------------------------------------
def validate_output(obj: Any, schema: Dict[str, Any]) -> None:
    """Shape-check an agent's output against its (JSON-Schema-ish) output schema.

    Minimal gate (no ``jsonschema``): ``obj`` must be a dict, and every *required* property
    must be present. Requiredness is either a per-property ``"required": true`` flag or
    membership in a top-level ``schema["required"]`` list. Both the nested
    ``{"properties": {...}}`` shape and a flat ``{prop: {...}}`` map are supported. Raises
    :class:`SchemaError` on any violation.
    """
    if not isinstance(obj, dict):
        raise SchemaError(f"agent output must be a JSON object/dict, got {type(obj).__name__}")
    if not isinstance(schema, dict) or not schema:
        return
    props = schema.get("properties")
    if isinstance(props, dict):
        properties = props
    else:
        properties = {k: v for k, v in schema.items() if k != "required"}
    declared = schema.get("required")
    required: Set[str] = set(declared) if isinstance(declared, list) else set()
    for prop, subschema in properties.items():
        if isinstance(subschema, dict) and subschema.get("required") is True:
            required.add(prop)
    missing = sorted(p for p in required if p not in obj)
    if missing:
        raise SchemaError(
            f"agent output missing required field(s): {missing} (present: {sorted(obj)})"
        )


# -- output ACCEPTANCE contract (B3) -----------------------------
def _acceptance_violation(value: Any, rules: Dict[str, Any]) -> Any:
    """Return a human-readable reason string iff ``value`` fails a declared acceptance ``rules``
    mapping, else ``None``. Rules are DECLARATIVE + DETERMINISTIC (no code eval), a superset of
    the required-key presence check:

    - ``non_empty: true``  — a str/list/dict/tuple must be truthy (non-empty), and ``None`` fails.
    - ``min_length: N``    — ``len(value) >= N`` (str/list/etc.).
    - ``max_length: N``    — ``len(value) <= N``.
    - ``enum: [...]``      — ``value`` must be one of the listed values.
    - ``pattern: "re"``    — a str must fully match the regex.
    """
    if not isinstance(rules, dict):
        return None
    if rules.get("non_empty") is True:
        if value is None or (hasattr(value, "__len__") and len(value) == 0):
            return "must be non-empty"
    min_len = rules.get("min_length")
    if isinstance(min_len, int) and hasattr(value, "__len__") and len(value) < min_len:
        return f"length {len(value)} < min_length {min_len}"
    max_len = rules.get("max_length")
    if isinstance(max_len, int) and hasattr(value, "__len__") and len(value) > max_len:
        return f"length {len(value)} > max_length {max_len}"
    enum = rules.get("enum")
    if isinstance(enum, (list, tuple)) and value not in enum:
        return f"{value!r} not in enum {list(enum)}"
    pattern = rules.get("pattern")
    if isinstance(pattern, str) and isinstance(value, str):
        if re.fullmatch(pattern, value) is None:
            return f"{value!r} does not match pattern {pattern!r}"
    return None


def check_hive_contract(obj: Any) -> None:
    """The agent↔Hive-layer boundary gate (B2-remainder): an output must conform to what
    the OS layer routes, stores, and content-addresses — i.e. be a JSON-SERIALIZABLE object.

    ``validate_output`` checks dict-ness + required keys, but a dict carrying a non-JSON value (a
    ``set``, a bespoke object, ...) passes it and then CRASHES the append-only log write at
    :func:`~concursus.state.statestore.content_hash` (``json.dumps``). This turns that late, opaque
    crash into an early, legible :class:`SchemaError` at dispatch, so it rides the same retry/record
    path (a present-but-unstorable output does not complete and earns no trust). Raises on violation.
    """
    try:
        json.dumps(obj, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise SchemaError(
            f"agent output violates the Hive-layer contract — not JSON-serializable "
            f"(the OS log/dedup cannot store it): {exc}"
        ) from exc


def check_acceptance(obj: Any, schema: Dict[str, Any]) -> None:
    """Post-run QA gate: every declared per-field ``acceptance`` rule must hold (B3).

    This is DEEPER than :func:`validate_output` (which only checks required-key *presence*): it
    verifies each output field's *value* against a declared acceptance contract — the machine-checkable
    definition of "a good output" the Trust Ladder needs (a present-but-wrong output FAILS here and so
    does NOT earn trust). Conservative: a field with no ``acceptance`` mapping is unconstrained, so a
    manifest that declares none is never newly rejected. Raises :class:`SchemaError` on any violation.
    """
    if not isinstance(obj, dict) or not isinstance(schema, dict) or not schema:
        return
    props = schema.get("properties")
    properties = props if isinstance(props, dict) else {
        k: v for k, v in schema.items() if k != "required"
    }
    for field_name, subschema in properties.items():
        if not isinstance(subschema, dict):
            continue
        rules = subschema.get("acceptance")
        if not isinstance(rules, dict):
            continue
        reason = _acceptance_violation(obj.get(field_name), rules)
        if reason is not None:
            raise SchemaError(
                f"output field {field_name!r} fails its acceptance contract: {reason}"
            )


# -- default invoke transport -----------------------------------------------
def _default_invoke_fn(arn: str, qualifier: str, session_id: str, payload: bytes) -> dict:
    """Default :data:`InvokeFn`: invoke a live AgentCore runtime endpoint via boto3.

    boto3 is imported lazily (the optional ``[agentcore]`` extra) so this module — and every
    unit test that injects a fake transport — imports fine without the AWS SDK.
    """
    try:
        import boto3  # lazy: only needed for a real, over-the-wire invoke
    except ImportError as exc:  # pragma: no cover - exercised only without boto3
        raise RuntimeError(
            "invoking a live AgentCore runtime requires boto3 — install the 'agentcore' "
            "extra (pip install concursus[agentcore]) or pass invoke_fn=..."
        ) from exc
    client = boto3.client("bedrock-agentcore")  # data plane
    response = client.invoke_agent_runtime(
        agentRuntimeArn=arn,
        runtimeSessionId=session_id,
        payload=payload,
        qualifier=qualifier,
    )
    body = response["response"].read()  # streaming response
    return json.loads(body)


def _new_session_id() -> str:
    """Generate a stable, >=33-char ``runtimeSessionId`` (AgentCore requires >= 33)."""
    return uuid.uuid4().hex + uuid.uuid4().hex


# -- pluggable per-node-kind executor registry (opt-in Strategy/Registry seam) ------------
#: A node executor is a UNIFORM ``(supervisor, node, inputs, wiring) -> None`` handler — the
#: Strategy generalization of today's single, uniform node dispatch (one kind today). The
#: :data:`_DEFAULT_NODE_KIND` maps to :func:`_default_node_executor`, which delegates verbatim to
#: :meth:`Supervisor._dispatch` — so with NO custom kind selected (the default) a run behaves
#: byte-for-byte as before. A caller can register a custom node-kind handler (constructor
#: ``node_executors=``) and select it per node (constructor ``node_kind_fn=``); a kind key with no
#: registered handler falls back to the default handler.
NodeExecutor = Callable[["Supervisor", str, Dict[str, Any], List["AgentRef"]], None]

#: The default node-kind key: today's one and only dispatch path.
_DEFAULT_NODE_KIND = "default"


def _default_node_executor(
    supervisor: "Supervisor", node: str, inputs: Dict[str, Any], wiring: List["AgentRef"]
) -> None:
    """The DEFAULT node-kind handler: today's exact dispatch (delegates to :meth:`Supervisor._dispatch`)."""
    supervisor._dispatch(node, inputs, wiring)


#: The shipped registry seeded with the single default kind. Instances copy this at construction
#: (and may layer custom kinds atop it), so no test mutates shared global state.
NODE_EXECUTORS: Dict[str, NodeExecutor] = {_DEFAULT_NODE_KIND: _default_node_executor}


# -- supervisor -------------------------------------------------------------
class Supervisor:
    """Drive a :class:`~concursus.assemble.ProvisioningPlan` to completion, offline or live.

    Walks ``plan.order`` (a topological order); for each node it assembles the invoke payload
    from the external run inputs overlaid with its resolved upstream outputs (``plan.wiring``),
    calls the injected :data:`InvokeFn`, validates the result against the manifest's output
    schema, and threads it forward. The plan is duck-typed on ``.order`` and ``.wiring`` only.
    """

    def __init__(
        self,
        plan: "ProvisioningPlan",
        manifests: Dict[str, "AgentManifest"],
        *,
        invoke_fn: Optional[InvokeFn] = None,
        session_id: Optional[str] = None,
        arns: Optional[Dict[str, str]] = None,
        state_store: Optional[StateStore] = None,
        on_error: str = "raise",
        max_attempts: int = 1,
        arn_resolver: Optional[Callable[[str, "AgentManifest"], str]] = None,
        held: Optional[Set[str]] = None,
        check_acceptance: bool = False,
        acceptance_fn: Optional[Callable[[str], bool]] = None,
        payload_tier_fn: Optional[Callable[[str], Any]] = None,
        verify_plan_identity: bool = False,
        node_executors: Optional[Dict[str, NodeExecutor]] = None,
        node_kind_fn: Optional[Callable[[str], str]] = None,
        cancel_futile: bool = False,
        capture_agent_binding: bool = False,
    ) -> None:
        self._plan = plan
        self._manifests: Dict[str, "AgentManifest"] = dict(manifests)
        # (R1-1 wiring) OPT-IN (default OFF): when set, each validated ``put`` records the bound
        # agent name + ARN in the record meta (round-tripped only via the FileVault meta blob, never
        # the AgentCore Memory projection). OFF ⇒ the put meta is byte-identical to before.
        self._capture_agent_binding = capture_agent_binding
        # (R1-4) When capture_agent_binding is on, the REAL per-node invoke payload the supervisor
        # built in _dispatch is recorded here (redacted at capture time by capture_payload_note), so
        # capture_run(payloads=sup.recorded_payloads()) persists the actual asked-of-the-agent bytes
        # rather than only the compiler-authored static_context. Empty + unused when the flag is off.
        self._recorded_payloads: Dict[str, Any] = {}
        self._invoke_fn: InvokeFn = invoke_fn or _default_invoke_fn
        self._session_id = session_id or _new_session_id()
        self._store: StateStore = state_store or InProcessStateStore()

        if on_error not in ("raise", "record"):
            raise ValueError(f"on_error must be 'raise' or 'record', got {on_error!r}")
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        # DEFAULTS ('raise', 1) preserve today's byte-for-byte fail-fast single forward pass.
        self._on_error = on_error
        self._max_attempts = max_attempts
        # OPT-IN futility cancellation for the antichain-parallel wave. False (the default,
        # and every serial run) => no registry is built and no closure ever runs, so _run_parallel
        # behaves exactly as today. When True, a node that fails mid-wave cancels in-flight SIBLINGS
        # whose every consumer just became unreachable — the in-flight twin of the phase-3
        # blocked-skip. It PRUNES only: it never reroutes and never mutates the frozen plan (INV-3).
        self._cancel_futile = bool(cancel_futile)
        # The registry serving the CURRENTLY-RUNNING parallel wave loop, else None. The harness node
        # executor reads this to self-register its asyncio task (see execute.futility.run_registered);
        # it is set on entry to _run_parallel and cleared in that method's ``finally``, so it never
        # outlives the wave loop that owns it.
        self._cancel_tokens: Optional[CancelTokenRegistry] = None
        # OPT-IN governance HOLD set (governor I-1): node ids the outer Trust-Ladder router withheld
        # THIS episode. None/empty (default) preserves today's byte-for-byte pass. A held node is
        # NEVER invoked — run() skips it exactly like a resume/blocked skip, so the frozen plan.order
        # is untouched (INV-3) and no ARN/integrity gate fires for it. It is a NON-DISPATCH, not a
        # failure: nothing is written to the log for a held node, so it does not surface as a failed
        # record (no spurious replan signal) and stays in the still-open frontier for a later round.
        self._held: Set[str] = set(held or ())
        # OPT-IN dispatch-time ARN integrity assertion. None (default) preserves today's
        # behavior byte-for-byte. When supplied, it fetches the AUTHORITATIVE ARN so we can ASSERT
        # the compiled binding is still current — it NEVER re-binds the invoke to a re-fetched ARN.
        self._arn_resolver = arn_resolver
        # B3: OPT-IN post-run output-ACCEPTANCE gate. False (default) => only the
        # required-key presence check (validate_output) runs, byte-for-byte unchanged. When True,
        # after a successful shape-validate the output's VALUES are checked against each field's
        # declared ``acceptance`` contract (check_acceptance); a present-but-wrong output fails here
        # exactly like a schema failure — so it is NOT admitted to the store and does NOT earn trust
        # (the machine-checkable "good output" signal the Trust Ladder needs). It rides the existing
        # retry/record path; it never mutates a frozen plan (INV-3) and adds no compiler loop (INV-2).
        self._check_acceptance = bool(check_acceptance)
        # B4: OPT-IN adaptive-strictness dial for the acceptance gate. None (default)
        # applies check_acceptance to EVERY node. When set, a ``node -> bool`` predicate narrows the
        # QA gate to the nodes it returns truthy for — wire a trust-derived predicate so a WEAK agent
        # is QA-checked while a proven STRONG one runs lean. No effect when check_acceptance is off.
        self._acceptance_fn = acceptance_fn
        # SPIKE B (B3): OPT-IN trust-tiered payload overlay. None (default) =>
        # the invoke payload is byte-for-byte unchanged. When set, a ``node -> Tier`` selector
        # (wire ``governor.make_payload_tier(sched)``); a node whose manifest declares
        # ``contract.context`` gets ``project_context(context, tier_fn(node))`` overlaid into its
        # external inputs BEFORE the wired upstream outputs — so a WEAK agent receives the full
        # coaching context and a proven one runs lean. Author/dispatch-time only; it never mutates
        # a frozen plan (INV-3) and adds no compiler loop (INV-2). The A/B evidence gate that
        # decides whether the full payload contract is worth building rides on this seam.
        self._payload_tier_fn = payload_tier_fn
        # RESUME=REPLAY INTEGRITY (opt-in). False (default) preserves today's byte-for-byte resume:
        # a node in completed() is skipped with no identity check. When True, run() persists this
        # frozen plan's content-hash (plan_fingerprint) under a reserved store id on first pass, and
        # on any subsequent resume ASSERTS the persisted hash equals the current plan's hash BEFORE
        # skipping/replaying any completed node — a mismatch raises PlanIdentityError rather than
        # silently mis-replaying a recorded node under a divergent plan. The reserved id is not a
        # DAG node, so run()'s {node: output} return is unchanged. This is a verification, never a
        # rebind: it never mutates plan.order (INV-3) and adds no compiler loop (INV-2).
        self._verify_plan_identity = bool(verify_plan_identity)
        # Strategy/Registry dispatch (opt-in). The registry maps a node-kind key -> a uniform
        # ``(supervisor, node, inputs, wiring) -> None`` handler. Instances start from the shipped
        # NODE_EXECUTORS (which carries only the DEFAULT kind, delegating to _dispatch) and layer any
        # caller-supplied kinds atop it; ``node_kind_fn`` selects a kind per node. With neither
        # supplied (the default), EVERY node routes to _DEFAULT_NODE_KIND -> _dispatch, so the run is
        # byte-for-byte unchanged. A node whose selected kind has no registered handler also falls
        # back to the default handler (registry.get(kind, default_handler)).
        self._node_executors: Dict[str, NodeExecutor] = dict(NODE_EXECUTORS)
        if node_executors:
            self._node_executors.update(node_executors)
        self._node_kind_fn = node_kind_fn

        supplied = dict(arns or {})
        self._arns: Dict[str, str] = {}
        for node, manifest in self._manifests.items():
            self._arns[node] = (
                supplied.get(node)
                or manifest.registry.get("agent_runtime_arn")
                or _ARN_PLACEHOLDER
            )
        for node, arn in supplied.items():  # arns for nodes lacking a manifest
            self._arns.setdefault(node, arn)

        # C-3: a ONE-TIME pre-dispatch structural gate (INV-1: not a runtime loop). Project the
        # frozen plan (plan.order nodes + plan.wiring AgentRef edges) into a RunGraph and run its
        # shipped validate() — rejecting a dangling AgentRef (a wire naming a producer absent from
        # plan.order) or a cycle BEFORE the first invoke, the structural complement to the
        # per-output validate_output shape check. Evaluated once here at construction; run() stays
        # an untouched single static forward pass.
        self._validate_plan_structure()

    def _validate_plan_structure(self) -> None:
        """Reject a structurally-invalid plan (dangling AgentRef / cycle) before any dispatch.

        Builds a :class:`~concursus.rungraph.RunGraph` from ``plan.order`` (the node set) and the
        ``plan.wiring`` :class:`AgentRef` edges (``ref.producer -> node`` at ``ref.path``), then
        calls the shipped :meth:`RunGraph.validate`. Raises :class:`RunGraphError` if a wire names
        a producer that is not a planned node, or if the wiring contains a cycle. A one-time
        construction-time check — never a runtime loop, so :meth:`run` stays a static single pass.
        """
        nodes = list(self._plan.order)
        edges: List[tuple] = []
        for node in nodes:
            for ref in self._plan.wiring.get(node, []):
                edges.append((ref.producer, node, ref.path))
        RunGraph.from_edges(nodes, edges).validate()

    def _persisted_plan_fingerprint(self) -> Optional[str]:
        """The plan content-hash recorded under :data:`_PLAN_IDENTITY_NODE`, or ``None`` if none.

        Scans the append-only log (backend-agnostic — works for the in-process and Memory stores)
        for the reserved identity record. ``None`` means this store carries no prior run's identity
        (a cold first pass), so there is nothing to verify against yet.
        """
        for rec in self._store.records():
            if rec.node == _PLAN_IDENTITY_NODE and isinstance(rec.output, dict):
                fp = rec.output.get("plan_fingerprint")
                if isinstance(fp, str):
                    return fp
        return None

    def _verify_or_persist_plan_identity(self) -> None:
        """Guard resume=replay integrity (opt-in): the resumed plan must match the recorded one.

        No-op unless ``verify_plan_identity=True`` (so the default pass is byte-for-byte unchanged).
        When enabled: on the FIRST pass under a store (no persisted identity) it records this frozen
        plan's :func:`plan_fingerprint` under the reserved :data:`_PLAN_IDENTITY_NODE`; on any later
        resume (the identity is already recorded) it ASSERTS the persisted hash equals the current
        plan's hash BEFORE :meth:`run` skips/replays any completed node — raising
        :class:`PlanIdentityError` on mismatch instead of silently mis-replaying a recorded node id
        under a divergent plan. Idempotent: a second ``run`` on the SAME supervisor finds a matching
        identity and re-writes nothing. Never mutates ``plan.order`` (INV-3); never a compiler loop.
        """
        if not self._verify_plan_identity:
            return
        current = plan_fingerprint(self._plan)
        persisted = self._persisted_plan_fingerprint()
        if persisted is None:
            # cold first pass: record the frozen plan's identity for a future resume to verify.
            self._store.put(
                _PLAN_IDENTITY_NODE,
                {"plan_fingerprint": current},
                meta={"producer": _PLAN_IDENTITY_NODE, "address": _PLAN_IDENTITY_NODE},
            )
            return
        if persisted != current:
            raise PlanIdentityError(
                f"resume plan-identity mismatch: the persisted run was recorded under plan "
                f"{persisted!r}, but this Supervisor's frozen plan hashes to {current!r}. "
                "resume=replay requires the SAME frozen plan (a completed node id would otherwise "
                "replay under divergent wiring); re-compile monotonically instead of resuming a "
                "divergent plan."
            )

    @property
    def session_id(self) -> str:
        """The stable per-run ``runtimeSessionId`` shared across every invoke."""
        return self._session_id

    def recorded_payloads(self) -> Dict[str, Any]:
        """(R1-4) The REAL per-node invoke payloads captured during the run (empty unless
        ``capture_agent_binding=True``). Pass to ``capture_run(payloads=...)`` so the persisted
        payload notes carry what was actually asked of each agent, redacted at capture time."""
        return dict(self._recorded_payloads)

    # -- payload assembly ---------------------------------------------------
    def _external_inputs(
        self, node: str, inputs: Dict[str, Any], wiring: List["AgentRef"]
    ) -> Dict[str, Any]:
        """External inputs for ``node``: its ``inputs[node]`` block, or — for a source node
        (no inbound wiring) — the top-level ``inputs`` mapping. When an opt-in payload-tier
        selector is wired (SPIKE B), the node's tiered ``contract.context`` is overlaid underneath
        the caller-supplied inputs (which win on any key collision)."""
        explicit = inputs.get(node)
        if isinstance(explicit, dict):
            base = dict(explicit)
        elif not wiring:
            base = dict(inputs)
        else:
            base = {}
        return self._overlay_tiered_context(node, base)

    def _overlay_tiered_context(self, node: str, base: Dict[str, Any]) -> Dict[str, Any]:
        """Overlay the node's trust-tiered static context UNDER ``base`` (SPIKE B B3 / F3).

        Two sources, in precedence order:

        * **F3 — the FROZEN plan's ``payload_contract``**: when the compiled plan carries
          ``payload_contract[node]["static_context"]``, use it verbatim — the compiler already
          projected it to the node's tier at author time (a self-contained frozen payload; no live
          scheduler needed at dispatch).
        * **B3 — the live ``payload_tier_fn``**: else, when a ``node -> Tier`` selector was injected
          AND the manifest declares ``contract.context``, project it live at dispatch.

        Both are opt-in: with neither a frozen contract nor a ``payload_tier_fn``, this returns
        ``base`` unchanged (default payload byte-for-byte). The projected context is placed UNDER
        ``base`` so caller-supplied / wired inputs always win a key collision. Any error degrades to
        ``base`` (tiering is best-effort — never break a dispatch)."""
        projected: Dict[str, Any] = {}
        # F3: prefer the frozen, compiler-authored contract if the plan carries one for this node.
        frozen = getattr(self._plan, "payload_contract", None)
        if isinstance(frozen, dict) and node in frozen:
            ctx = frozen[node].get("static_context") if isinstance(frozen[node], dict) else None
            if isinstance(ctx, dict) and ctx:
                projected = dict(ctx)
        # B3: else project live via the injected tier fn.
        if not projected and self._payload_tier_fn is not None:
            manifest = self._manifests.get(node)
            context = getattr(manifest, "context", None) if manifest is not None else None
            if context:
                try:
                    from concursus.governor.scheduler import project_context

                    projected = project_context(context, self._payload_tier_fn(node))
                except Exception:  # noqa: BLE001 - best-effort; never break a dispatch
                    projected = {}
        if not projected:
            return base
        merged = dict(projected)
        merged.update(base)  # caller-supplied / wired inputs win on collision
        return merged

    def run(self, inputs: Dict[str, Any], *, parallel: int = 1) -> Dict[str, Dict]:
        """Invoke every agent in topological order; return ``{node_id: output_dict}``.

        A single forward pass over the frozen ``plan.order``. For each node, one already in
        ``self._store.completed()`` is skipped (resume — its validated output was recorded on a
        prior run); one whose upstream producers have not completed is recorded ``failed`` with a
        ``blocked_on`` reason and skipped (so :func:`extract` never hits a missing-producer
        ``KeyError``); otherwise it is handed to :meth:`_dispatch`.

        Default behavior (``on_error='raise'``, ``max_attempts=1``) is byte-for-byte the original
        fail-fast pass. With ``on_error='record'`` a terminal invoke/validate failure is recorded
        (never raised) and the pass continues, so a failure prunes only its dependent subtree —
        the return is ``{node: output for node in plan.order if node in completed()}``, so
        independent branches still return.

        ``parallel`` (OPT-IN, default ``1``) bounds an ANTICHAIN-PARALLEL wave. At ``1`` this is
        EXACTLY the serial single pass below (byte-for-byte unchanged). At ``> 1`` it delegates to
        :meth:`_run_parallel`, which repeatedly dispatches the current dispatchable antichain (nodes
        whose producers are all completed) concurrently on a bounded thread pool. The frozen
        ``plan.order`` is NEVER mutated and results are keyed by node id, so a node's inputs still
        come only from completed producers — making the run order-independent (same store contents
        for any ``parallel``). It is still a single static pass over the frozen plan: no cyclic
        replan, resume=replay, on_error semantics unchanged.

        The requested ``parallel`` is CLAMPED by the host CPU capacity via the SAME
        ``max(1, min(pref, cap))`` shape the inner graph's fan-out uses
        (:func:`~concursus.reasoning.inner_graph.resolve_ceiling`): a soft ``parallel``
        request can only TIGHTEN the pool below the host's capacity, never spawn more workers than
        the host (hard-capped by ``MAX_FANOUT_CAP``) can serve. It only ever shrinks the worker
        pool, never the set of nodes dispatched, so the store contents stay byte-for-byte identical
        to the serial pass — determinism is untouched, this bounds only pool width.
        """
        if parallel < 1:
            raise ValueError(f"parallel must be >= 1, got {parallel}")
        # RESUME=REPLAY INTEGRITY (opt-in): persist this frozen plan's content-hash on the first
        # pass and, on any resume, ASSERT it still matches BEFORE any completed-node skip below.
        # No-op unless verify_plan_identity=True, so the default pass is byte-for-byte unchanged.
        self._verify_or_persist_plan_identity()
        if parallel > 1:
            # Clamp the soft request by host CPU capacity (same min(pref, cap) shape as the inner
            # graph). Only tightens the pool WIDTH; the dispatched node set is unchanged, so the
            # store stays byte-for-byte identical to the serial pass.
            parallel = resolve_ceiling(parallel, _cpu_capacity())
        if parallel > 1:
            return self._run_parallel(inputs, parallel)
        for node in self._plan.order:
            if node in self._store.completed():
                continue  # resume: this node already has a recorded validated output
            if node in self._held:
                # governance HOLD (governor I-1): the outer router withheld this node this episode.
                # A pure NON-DISPATCH — never invoked, and NOTHING is written to the log (unlike a
                # blocked/failed skip), so the held node leaves no failed record (no spurious replan
                # signal) and simply stays in the still-open frontier for a later round once its
                # trust is re-earned. The frozen plan.order is untouched (INV-3).
                continue
            wiring: List["AgentRef"] = list(self._plan.wiring.get(node, []))

            # blocked-skip: a producer this node consumes never completed (e.g. it failed or was
            # itself blocked). Record the node failed with the reason and skip — this prunes
            # WITHIN plan.wiring, never rewrites the topology.
            completed = self._store.completed()
            blocked = [ref.producer for ref in wiring if ref.producer not in completed]
            if blocked:
                reason = f"blocked on {', '.join(sorted(set(blocked)))}"
                self._store.put(
                    node,
                    {},
                    meta={
                        "status": "failed",
                        "producer": node,
                        "blocked_on": reason,
                        "failure_class": _FAILURE_HOLD,
                        "address": node,
                    },
                )
                continue

            self._route_dispatch(node, inputs, wiring)
        return {node: self._store.get(node) for node in self._plan.order if node in self._store.completed()}

    def _run_parallel(self, inputs: Dict[str, Any], parallel: int) -> Dict[str, Dict]:
        """OPT-IN bounded antichain-parallel wave (``run(parallel>1)``); NOT a new execution model.

        Still a SINGLE static pass over the FROZEN ``plan.order`` — never mutated, never replanned
        (INV: Concursus is a compiler, not a runtime governor). The only difference from the serial
        pass is *when* independent nodes are dispatched: instead of one-at-a-time in ``plan.order``,
        each round dispatches the current ANTICHAIN — every still-open node whose ``plan.wiring``
        producers are ALL in ``self._store.completed()`` — concurrently on a bounded
        :class:`~concurrent.futures.ThreadPoolExecutor` (``max_workers=parallel``), waits for the
        wave, then recomputes. It loops until every node is completed or no node is dispatchable.

        Determinism / order-independence: every result is keyed by node id in the store (as serial),
        and a node is dispatched ONLY after all its producers completed, so its resolved inputs are
        identical to the serial run regardless of intra-wave completion order. The per-node outputs,
        statuses, consumes edges and content hashes are therefore byte-for-byte identical to
        ``parallel=1`` (only the store-local ``seq``/``timestamp`` reflect physical put order).

        ``on_error`` semantics are unchanged: ``_dispatch`` runs in the worker exactly as serial, so
        ``'raise'`` surfaces the first wave failure (fail-fast) and ``'record'`` writes ONE failed
        record per node and lets the pass continue (a failure prunes only its dependent subtree). A
        held node is never dispatched (governance HOLD), and a node whose producer failed/was held is
        recorded ``blocked_on`` exactly as the serial pass does — via the same store writes.

        Observation vs. dispatch granularity: the wave is drained with ``as_completed``
        rather than a single ``wait(futures)`` barrier, so each member's outcome is seen the moment it
        resolves instead of when the SLOWEST sibling lands. Dispatch stays wave-structured — a node
        still starts only after all its producers complete — so determinism is untouched; only the
        observation latency changes. Because ``as_completed`` yields in completion order while the old
        barrier inspected in futures-list order, failures are collected and the earliest in
        ``plan.order`` is raised after the pool drains, preserving WHICH exception a caller sees.

        That earlier observation is what makes the OPT-IN ``cancel_futile`` seam possible: when a node
        fails mid-wave, in-flight siblings whose EVERY consumer just became unreachable are condemned
        via :class:`~.futility.CancelTokenRegistry` (see :func:`~.futility.futility_closure`). This is
        the in-flight twin of the blocked-remainder pass below — the same "these inputs will never be
        consumed, record it and move on" judgement, evaluated during dispatch instead of after it.
        With the flag off (the default) no registry exists and no closure is computed.
        """
        order = list(self._plan.order)
        wiring_by_node: Dict[str, List["AgentRef"]] = {
            node: list(self._plan.wiring.get(node, [])) for node in order
        }
        dispatched: Set[str] = set()  # nodes handed to _dispatch this run (attempted at most once)
        # futility cancellation (OPT-IN, off by default). The consumer graph is inverted ONCE
        # — plan.wiring is frozen, so it cannot go stale — and the registry lives exactly as long as
        # this wave loop. With the flag off both stay empty/None and no closure is ever computed, so
        # the only difference from the previous implementation is WHEN futures are inspected.
        consumers: Dict[str, Set[str]] = {}
        tokens: Optional[CancelTokenRegistry] = None
        if self._cancel_futile:
            consumers = invert_wiring(wiring_by_node)
            tokens = CancelTokenRegistry()
        self._cancel_tokens = tokens
        # Exceptions are COLLECTED rather than raised at first sight: as_completed yields in
        # COMPLETION order whereas the previous wait(futures) inspected in FUTURES-LIST order, so
        # raising eagerly could surface a DIFFERENT failure than before when two wave members both
        # fail. plan.order is topological and the wave is built by walking it, so ranking the
        # collected errors by plan.order index reproduces the old futures-list ordering exactly.
        errors: List[Tuple[str, BaseException]] = []
        try:
            with ThreadPoolExecutor(max_workers=parallel) as pool:
                while True:
                    completed = self._store.completed()
                    open_nodes = [
                        node for node in order
                        if node not in completed
                        and node not in self._held
                        and node not in dispatched
                    ]
                    if not open_nodes:
                        break
                    # the current ANTICHAIN: open nodes whose producers are ALL completed.
                    wave = [
                        node for node in open_nodes
                        if all(ref.producer in completed for ref in wiring_by_node[node])
                    ]
                    if not wave:
                        break  # no dispatchable node: the rest are blocked (a producer failed/was held)
                    dispatched.update(wave)
                    fut_to_node = {
                        pool.submit(self._route_dispatch, node, inputs, wiring_by_node[node]): node
                        for node in wave
                    }
                    # in_flight is a plain local: a wave member is in flight from submit until its own
                    # future resolves. Tracking it here is why no registration API is needed.
                    in_flight: Set[str] = set(wave)
                    for fut in as_completed(fut_to_node):
                        node = fut_to_node[fut]
                        in_flight.discard(node)
                        exc = fut.exception()
                        if exc is not None:
                            errors.append((node, exc))
                        if tokens is None or not in_flight:
                            continue  # default path, or nothing left in flight to cancel
                        if exc is not None:
                            # An escaped exception means this method WILL raise once the wave drains
                            # (as it did before this change, under BOTH on_error modes), so the run is
                            # over and no in-flight output can ever be consumed — condemn all of them.
                            tokens.condemn_many(in_flight, f"fail-fast on {node}")
                        elif node not in self._store.completed():
                            # on_error='record': _route_dispatch already wrote the failed record and
                            # returned normally. Resolved-but-not-completed IS the failure test, so no
                            # index scan is needed. Prune only the PROVABLY futile siblings.
                            tokens.condemn_many(
                                futility_closure(consumers, node, in_flight),
                                f"futility-cancelled on {node}",
                            )
                    if errors:
                        break  # fail-fast: never start another wave, exactly as before
        finally:
            self._cancel_tokens = None

        if errors:
            # Surface the EARLIEST failure in plan.order — byte-for-byte the exception the previous
            # `for fut in futures: raise fut.exception()` would have surfaced.
            rank = {node: index for index, node in enumerate(order)}
            raise min(errors, key=lambda pair: rank.get(pair[0], len(order)))[1]

        # Blocked remainder: any still-open node has an uncompleted producer (a failure or a HOLD).
        # Record it exactly as the serial pass would, iterating plan.order so the block cascades.
        for node in order:
            completed = self._store.completed()
            if node in completed or node in self._held or node in dispatched:
                continue
            blocked = [
                ref.producer for ref in wiring_by_node[node] if ref.producer not in completed
            ]
            if blocked:
                reason = f"blocked on {', '.join(sorted(set(blocked)))}"
                self._store.put(
                    node,
                    {},
                    meta={
                        "status": "failed",
                        "producer": node,
                        "blocked_on": reason,
                        "failure_class": _FAILURE_HOLD,
                        "address": node,
                    },
                )
        return {node: self._store.get(node) for node in order if node in self._store.completed()}

    def _route_dispatch(
        self, node: str, inputs: Dict[str, Any], wiring: List["AgentRef"]
    ) -> None:
        """Route ``node`` to its node-kind handler (Strategy/Registry seam), defaulting to :meth:`_dispatch`.

        The kind is ``node_kind_fn(node)`` when a selector was injected, else :data:`_DEFAULT_NODE_KIND`;
        the handler is ``self._node_executors.get(kind, <default handler>)``. With no custom selector
        or registered kind (the default), this is exactly ``self._dispatch(node, inputs, wiring)`` —
        byte-for-byte the original single dispatch path. Every handler shares the same uniform
        ``(supervisor, node, inputs, wiring) -> None`` interface, so a custom kind rides the identical
        store/on_error path; it never mutates the frozen ``plan.order`` (INV-3) and adds no loop (INV-2).
        """
        kind = self._node_kind_fn(node) if self._node_kind_fn is not None else _DEFAULT_NODE_KIND
        handler = self._node_executors.get(kind, _default_node_executor)
        handler(self, node, inputs, wiring)

    def _dispatch(
        self, node: str, inputs: Dict[str, Any], wiring: List["AgentRef"]
    ) -> None:
        """Invoke one manifest-pinned node id and admit its validated output to the store.

        The payload starts from :meth:`_external_inputs`, then each wiring :class:`AgentRef`
        overlays ``payload[ref.input_name] = extract(self._store.get(ref.producer), ref.path)`` —
        the upstream field the resolver promised. The invoke result is
        :func:`validate_output`-checked against the manifest's output schema, then ``put`` into the
        store (with its ``producer`` / ``consumes`` edges / ``schema`` tag).

        With ``on_error='record'`` a transport/validation exception retries the SAME pinned node
        id up to ``max_attempts`` (never branching or synthesizing a node); on terminal failure it
        writes ONE failed record and returns. With the default ``on_error='raise'`` the exception
        propagates unchanged (fail-fast).
        """
        manifest = self._manifests.get(node)
        payload = self._external_inputs(node, inputs, wiring)
        for ref in wiring:
            payload[ref.input_name] = extract(self._store.get(ref.producer), ref.path)

        arn = self._arns.get(node, _ARN_PLACEHOLDER)
        qualifier = (
            str(manifest.registry.get("qualifier", "DEFAULT"))
            if manifest is not None
            else "DEFAULT"
        )
        payload_bytes = json.dumps(payload).encode()
        if self._capture_agent_binding:  # (R1-4) record the REAL invoke payload for post-run capture
            self._recorded_payloads[node] = payload
        consumes = [f"{r.producer}:{r.path}" for r in wiring]
        schema = manifest.name if manifest else None

        # dispatch-time ARN binding-INTEGRITY assertion, evaluated ONCE just before invoke.
        # This verifies the SINGLE compiled ARN is real and current; it is NEVER a runtime rebind
        # (a mismatch fails/records — it does not silently swap in the re-fetched value) and NEVER
        # a match-by-trust selection among candidate agents.
        integrity_error = self._check_arn_integrity(node, arn, manifest)
        if integrity_error is not None:
            if self._on_error != "record":
                raise integrity_error  # fail-fast: default path raises a clear binding error
            record_failure(
                self,
                node,
                failure_class=_FAILURE_CRASH,
                error=str(integrity_error),
                error_type=type(integrity_error).__name__,
                consumes=consumes,
                schema=schema,
            )
            return

        # Local attempt counter: Record.attempt only auto-increments INSIDE store.put(), which
        # runs after a successful invoke — so we track retries ourselves rather than reading it.
        attempt = 0
        while True:
            attempt += 1
            try:
                result = self._invoke_fn(arn, qualifier, self._session_id, payload_bytes)
                validate_output(result, manifest.output_schema if manifest else {})
                # B3 (opt-in): post-shape QA — the output's VALUES must satisfy each field's
                # declared acceptance contract. A present-but-wrong output raises here, so it is
                # NOT admitted and does NOT earn trust; rides the same retry/record path below.
                # B4 (opt-in dial): when acceptance_fn is set, QA-gate only the nodes it selects.
                if self._check_acceptance and (
                    self._acceptance_fn is None or self._acceptance_fn(node)
                ):
                    # B2-remainder: the agent<->Hive-layer boundary (output must be storable by the
                    # OS log) is checked first — a legible dispatch-time error instead of a late,
                    # opaque content_hash crash at log-write.
                    check_hive_contract(result)
                    check_acceptance(result, manifest.output_schema if manifest else {})
            except Exception as exc:
                if self._on_error != "record":
                    raise  # fail-fast: default path propagates unchanged
                if attempt < self._max_attempts:
                    continue  # retry the SAME manifest-pinned node id
                # terminal failure: write ONE failed record and stop (prune the subtree).
                record_failure(
                    self,
                    node,
                    failure_class=_FAILURE_CRASH,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    consumes=consumes,
                    schema=schema,
                    address=f"{node}/{attempt}",
                )
                return
            put_meta: Dict[str, Any] = {"producer": node, "consumes": consumes, "schema": schema}
            if self._capture_agent_binding:
                # present-only: a node with no bound manifest/ARN adds nothing (byte-identical meta)
                if manifest is not None and manifest.name:
                    put_meta["agent_name"] = manifest.name
                node_arn = self._arns.get(node)
                if node_arn and node_arn != _ARN_PLACEHOLDER:
                    put_meta["arn"] = node_arn
            self._store.put(node, result, meta=put_meta)
            return

    def _check_arn_integrity(
        self, node: str, arn: str, manifest: Optional["AgentManifest"]
    ) -> Optional[Exception]:
        """Return an ``Exception`` if ``node``'s compiled ARN fails the integrity check.

        Two independent checks, both purely a verification of the SINGLE compiled binding:

        (a) *unprovisioned* — the compiled ARN is still :data:`_ARN_PLACEHOLDER`, so there is no
            live runtime to invoke; the plan must be re-compiled after the runtime is deployed.
        (b) *stale* — if an ``arn_resolver`` was supplied, fetch the AUTHORITATIVE ARN and ASSERT
            it equals the compiled ``self._arns[node]``. On mismatch this returns an error rather
            than SILENTLY substituting the re-fetched value: a frozen binding is never rebound
            in-run; a change forces a re-compile.

        Returns ``None`` when the binding is intact (invoke proceeds normally).
        """
        if arn == _ARN_PLACEHOLDER:
            return RuntimeError(
                f"node {node} has no provisioned runtime ARN — deploy first"
            )
        if self._arn_resolver is not None:
            authoritative = self._arn_resolver(node, manifest)
            if authoritative != arn:
                return RuntimeError(
                    f"compiled ARN for {node} is stale; re-compile "
                    f"(compiled {arn!r} != authoritative {authoritative!r})"
                )
        return None

    # -- graph-aware context (v2) -------------------------------------------
    def context(self, node: str) -> Dict[str, dict]:
        """Transitive upstream context for ``node``: ``{producer: latest output}``.

        Rebuilds the run graph from the store's recorded ``consumes`` edges and returns the
        latest validated output of every node in :meth:`~concursus.rungraph.RunGraph.context_order`
        (its producers, nearest-first, bounded) — shared upstream state as a query rather than
        point-to-point wiring.
        """
        graph = RunGraph.from_records(self._store.records())
        return {n: self._store.get(n) for n in graph.context_order(node)}

    def index(self) -> RunIndex:
        """A :class:`~concursus.runindex.RunIndex` over the run's log — Folgezettel-tree
        traversal (retries / fan-out / branches) plus metadata queries (``status`` / ``schema`` /
        ``record_type`` / ``producer``) without scanning payloads."""
        return RunIndex.from_store(self._store)

    def summary(self) -> Dict[str, Any]:
        """A read-only, operator-legible partial-run summary derived purely from the store's log.

        Computed from ``RunIndex.query(status='failed')`` + ``store.completed()`` +
        ``len(plan.order)`` — no side effects, no change to the ``{node: output}`` return contract.
        Returns ``total`` / ``completed`` counts, the completed node set, and per-failed-node rows
        distinguishing a genuine failure from a ``blocked_on`` skip (the reason is read from the
        failed record's ``blocked_on`` meta).

        Also returns ``failure_classes`` — a ``{"crash": N, "hold": M}`` count over the terminal
        failed nodes, distinguishing a CRASH (this node's own invoke/validate/ARN-integrity raised)
        from a HOLD (never invoked because a producer it consumes failed/was blocked). The class is
        the failed record's ``failure_class`` when present; a legacy record with no ``failure_class``
        is derived from ``blocked_on`` presence, so the count is stable across store backends.
        """
        order = list(self._plan.order)
        # The reserved plan-identity record (opt-in resume=replay guard) is bookkeeping, not a DAG
        # node — drop it so it never inflates the completed count or the completed_nodes set.
        completed = self._store.completed() - {_PLAN_IDENTITY_NODE}
        failed_records = RunIndex.from_store(self._store).query(status="failed")
        # latest failed record per node wins (a node may have multiple failed attempts).
        failed: Dict[str, str] = {}
        classes: Dict[str, str] = {}
        for r in failed_records:
            if r.node in completed:
                continue  # a later attempt validated — not a terminal failure
            failed[r.node] = getattr(r, "blocked_on", None) or ""
            classes[r.node] = self._classify_failure(r)
        failure_classes = {
            cls: sum(1 for c in classes.values() if c == cls) for cls in _FAILURE_CLASSES
        }
        return {
            "total": len(order),
            "completed": len(completed),
            "completed_nodes": sorted(completed),
            "failed": failed,
            "failure_classes": failure_classes,
            "order": order,
        }

    @staticmethod
    def _classify_failure(record: Any) -> str:
        """Classify one terminal failed ``record`` as one of :data:`_FAILURE_CLASSES`.

        Prefer the record's own ``failure_class`` — written by :meth:`_dispatch` / :meth:`run` under
        ``on_error='record'`` (:data:`_FAILURE_CRASH`, :data:`_FAILURE_HOLD`) or by the harness node
        executor (:data:`_FAILURE_PREEMPTIVE`, :data:`_FAILURE_FUTILITY`). Fall back to ``blocked_on``
        presence for a legacy record that predates the field, so the classification is stable across
        store backends and older logs.

        This widened this from two classes to four: ``preemptive_termination`` records were
        previously written by the harness executor but not recognized here, so they silently bucketed
        as ``crash`` — a monitor-initiated termination counted as a self-inflicted failure.
        """
        explicit = getattr(record, "failure_class", None)
        if explicit in _FAILURE_CLASSES:
            return explicit
        return _FAILURE_HOLD if getattr(record, "blocked_on", None) else _FAILURE_CRASH

    def summary_line(self) -> str:
        """A one-line human rendering of :meth:`summary` for the CLI failure path.

        E.g. ``"completed 4/6; node summarize failed; node critique blocked on summarize"``.
        """
        s = self.summary()
        parts = [f"completed {s['completed']}/{s['total']}"]
        for node in s["order"]:
            reason = s["failed"].get(node)
            if reason is None:
                continue
            parts.append(f"node {node} {reason}" if reason else f"node {node} failed")
        return "; ".join(parts)
