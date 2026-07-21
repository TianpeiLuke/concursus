"""The **capture front** — a source-agnostic envelope + dispatcher onto the shipped note writers.

 SPIKE A. Hive's memory store (:mod:`concursus.state.filevault` +
:mod:`concursus.state.distill`) already writes durable, round-trip-exact slipbox notes per
run artifact. This module adds the ONE novel abstraction that lets *any* artifact reach those
writers uniformly: a frozen :class:`CaptureEnvelope` (filled by a per-source ``adapt_<kind>``
adapter) and a tiny :func:`capture` dispatcher that routes an envelope to the matching shipped
seam. There is NO new runtime here — the "graph" is a dict dispatch over functions that already
ship; the genuinely-net-new work is the envelope + the adapters.

Identity guard: capture is a PURE POST-RUN write. It runs after a run finishes, reads the frozen
artifact (a plan / a record), writes a NOTE (never a run :class:`~concursus.state.statestore.Record`
into the log), and never mutates a live/frozen plan (INV-1..5). It targets HIVE'S OWN memory store
(``<run_dir>``), not any external knowledge vault.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

# The known artifact sources (each maps to one shipped seam in :data:`_SEAMS`).
PLAN = "plan"
AGENT_RESPONSE = "agent_response"

# Sources named by the design but not yet wired to a seam — declaring them keeps
# the vocabulary in one place; :func:`capture` raises a clear error until an adapter/seam lands.
AGENT_LOG = "agent_log"
RUN_SUMMARY = "run_summary"
BINDING = "binding"
PAYLOAD = "payload"


class CaptureError(ValueError):
    """Raised on an invalid capture envelope or an unroutable source kind."""


@dataclass(frozen=True)
class CaptureEnvelope:
    """The ONE source-agnostic shape a per-source adapter fills; the dispatcher routes it.

    Attributes:
        source_kind: which artifact this is — :data:`PLAN` / :data:`AGENT_RESPONSE` / ...; picks
            the shipped seam in :func:`capture`.
        artifact: the raw frozen artifact (a :class:`~concursus.assemble.ProvisioningPlan`, a
            :class:`~concursus.state.statestore.Record`, ...). Consumed by the seam.
        run_dir: the target run's vault dir — Hive's OWN memory store (``<vault>/runs/<session>/``).
        trail_id: the run/family id threaded into the note frontmatter + Related links.
        related: optional ``## Related Notes`` link hints (else the seam's own default is used).
        date: optional ``date of note`` stamp (deterministic; never a clock read here).
    """

    source_kind: str
    artifact: Any
    run_dir: str
    trail_id: str = "run"
    related: Optional[List[str]] = None
    date: str = ""

    def __post_init__(self) -> None:
        if not self.source_kind:
            raise CaptureError("CaptureEnvelope requires a non-empty source_kind")
        if not self.run_dir:
            raise CaptureError("CaptureEnvelope requires a run_dir (Hive's own memory store)")


# -- per-source adapters (the only source-specific code) --------------------
def adapt_plan(plan: Any, run_dir: str, *, trail_id: str = "run", date: str = "") -> CaptureEnvelope:
    """Adapt a frozen :class:`~concursus.assemble.ProvisioningPlan` into a :data:`PLAN` envelope.

    A thin wrap — the artifact IS the plan; :func:`capture` routes it to the shipped
    :func:`~concursus.state.filevault.capture_run_plan_note`, which writes ``<run_dir>/_plan.md``
    (a ``run_plan``-stamped slipbox note that ``_note_to_record`` refuses to parse back). Realizes
     #1 (persist the frozen plan) through the envelope/dispatcher seam.
    """
    return CaptureEnvelope(PLAN, plan, run_dir, trail_id=trail_id, date=date)


def adapt_payload(
    node: str,
    payload: Any,
    run_dir: str,
    *,
    trust_tier: str = "",
    trail_id: str = "run",
    date: str = "",
    related: Optional[List[str]] = None,
) -> CaptureEnvelope:
    """Adapt a node's frozen invoke PAYLOAD into a:data:`PAYLOAD` envelope ( T3).

    The artifact is a ``(node, payload, trust_tier)`` tuple; :func:`capture` routes it to the
    :func:`~concursus.state.filevault.capture_payload_note` seam, which REDACTS PII and writes a
    ``payload``-stamped non-record slipbox note. Realizes #2 (persist per-agent payloads).
    """
    return CaptureEnvelope(
        PAYLOAD, (node, payload, trust_tier), run_dir,
        trail_id=trail_id, date=date, related=related,
    )


# -- the dispatcher (a dict over shipped seams; NOT a new runtime) ----------
def _seam_plan(env: CaptureEnvelope) -> str:
    from concursus.state.filevault import capture_run_plan_note

    return capture_run_plan_note(
        env.artifact, env.run_dir, trail_id=env.trail_id, date=env.date
    )


def _seam_agent_response(env: CaptureEnvelope) -> str:
    from concursus.state.filevault import capture_agent_response_note

    return capture_agent_response_note(
        env.artifact, trail_id=env.trail_id, date=env.date, related=env.related
    )


def _seam_payload(env: CaptureEnvelope) -> str:
    from concursus.state.filevault import capture_payload_note

    node, payload, trust_tier = env.artifact
    return capture_payload_note(
        node, payload, env.run_dir,
        trust_tier=trust_tier, trail_id=env.trail_id, date=env.date, related=env.related,
    )


#: The source_kind -> shipped-seam dispatch table. The "capture graph" of the design is exactly
#: this dict; adding a source = adding an adapter + one row here, never a new runtime.
_SEAMS: Dict[str, Callable[[CaptureEnvelope], str]] = {
    PLAN: _seam_plan,
    AGENT_RESPONSE: _seam_agent_response,
    PAYLOAD: _seam_payload,
}


def capture(env: CaptureEnvelope) -> str:
    """Route a :class:`CaptureEnvelope` to its shipped writer; return the written note path.

    Validates the envelope's ``source_kind`` against :data:`_SEAMS` and calls the matching shipped
    :mod:`~concursus.state.filevault` / :mod:`~concursus.state.distill` seam. Pure post-run
    write (INV-safe): it writes a note under ``env.run_dir`` and never touches a live plan or the
    run log's structural anchor. Raises :class:`CaptureError` for an unknown/not-yet-wired source.
    """
    seam = _SEAMS.get(env.source_kind)
    if seam is None:
        raise CaptureError(
            f"no capture seam for source_kind {env.source_kind!r} "
            f"(wired: {sorted(_SEAMS)})"
        )
    return seam(env)


# -- T4: the post-run trigger ----------------------------------------------
def capture_run(
    run_dir: str,
    *,
    plan: Any = None,
    payloads: Optional[Dict[str, Any]] = None,
    trust_tiers: Optional[Dict[str, str]] = None,
    trail_id: str = "run",
    date: str = "",
    backlinks: bool = True,
) -> Dict[str, Any]:
    """** T4.** The post-run capture trigger: build + dispatch envelopes for a finished
    run, then run the T6 reciprocal-backlink post-pass. Returns ``{"paths": [...], "backlinks": n}``.

    Call this AFTER ``Supervisor.run`` returns (or between governor episodes). It captures the
    frozen ``plan`` (if given) and each node's frozen invoke ``payload`` (if given) into Hive's OWN
    memory store at ``run_dir``, then — with ``backlinks=True`` — amends producer notes with
    "consumed by" backlinks over the recorded ``consumes`` edges. The run's per-response notes are
    already written by the store during the run (``FileVaultStateStore``); this trigger adds the
    plan + payload artifacts + the reciprocal edges. Pure post-run (INV-safe): notes-not-records,
    no live-plan mutation.
    """
    paths: List[str] = []
    tiers = dict(trust_tiers or {})
    if plan is not None:
        paths.append(capture(adapt_plan(plan, run_dir, trail_id=trail_id, date=date)))

    # I1 (Phase 3): where the tracks meet — when no explicit payloads are given, derive them from
    # the frozen plan's compiler-authored payload_contract (Phase 2 F1), so the tiered payload the
    # COMPILER authored is exactly what the capture session PERSISTS. Each node's static_context
    # becomes its payload note body and its trust_tier is recorded.
    if payloads is None and plan is not None:
        pc = getattr(plan, "payload_contract", None)
        if isinstance(pc, dict) and pc:
            payloads = {}
            for node, entry in pc.items():
                if isinstance(entry, dict):
                    payloads[node] = entry.get("static_context", {})
                    tiers.setdefault(node, str(entry.get("trust_tier", "")))

    for node, payload in (payloads or {}).items():
        tier = tiers.get(node, "")
        paths.append(
            capture(adapt_payload(node, payload, run_dir, trust_tier=tier,
                                  trail_id=trail_id, date=date))
        )
    n_backlinks = 0
    if backlinks:
        from concursus.state.filevault import add_reciprocal_backlinks

        n_backlinks = add_reciprocal_backlinks(run_dir)
    return {"paths": paths, "backlinks": n_backlinks}


# -- T5: the gate + verify pass over a run dir ------------------------------
def gate_run_dir(run_dir: str) -> Dict[str, Any]:
    """** T5.** Run a lightweight post-write GATE over a run's capture notes; return a
    ``{"ok": bool, "checked": n, "issues": [...]}`` verdict.

    A *safety net*, not a blocker (the deterministic writers rarely err): it re-reads every note
    under ``run_dir`` and flags (a) a missing YAML frontmatter block and (b) a **dangling
    ``consumes`` backlink** — a note whose Related-Notes line points to a producer note file that is
    not on disk. This is the [ #2] "actually RUN the format/link check, don't just
    assume compatibility" improvement, scoped to what a deterministic writer can regress. Read-only;
    it never rewrites a note. (Independent-verify of LLM-*enriched* notes — #3 — is a separate,
    enriched-only pass; the deterministic seams here are faithful by construction and skip it.)
    """
    import os
    import re

    run = os.fspath(run_dir)
    issues: List[str] = []
    checked = 0
    if not os.path.isdir(run):
        return {"ok": True, "checked": 0, "issues": []}
    names = {n for n in os.listdir(run) if n.endswith(".md")}
    link_re = re.compile(r"\]\(([^)]+\.md)\)")
    for name in sorted(names):
        if not name.endswith(".md"):
            continue
        checked += 1
        text = open(os.path.join(run, name), encoding="utf-8").read()
        if not text.startswith("---"):
            issues.append(f"{name}: missing YAML frontmatter")
        for target in link_re.findall(text):
            # only check same-dir relative note links (the run's own graph)
            if "/" in target:
                continue
            if target not in names:
                issues.append(f"{name}: dangling link -> {target}")
    return {"ok": not issues, "checked": checked, "issues": issues}


# -- I2 (Phase 3, FUTURE hook): the read-back primitive --------------------
def load_payload_tiers(run_dir: str) -> Dict[str, str]:
    """Read persisted payload notes back into ``{node: trust_tier}`` ( I2 — FUTURE hook).

    The minimal read-back primitive the P3/P4 flywheel needs: it re-reads the
    ``<node>__payload.md`` notes a prior run captured and recovers the trust tier each node ran at.
    A future warm-load (P4) primes ``make_payload_tier`` from durable memory so a node's tier
    survives a fresh process; a future retrieval (P3) matches a persisted payload/tier to a similar
    new run. THIS function is only the read primitive — the priming/matching logic is intentionally
    out of scope for this plan (flagged FUTURE). Pure read; returns ``{}`` for an absent dir.
    """
    import os
    import re

    run = os.fspath(run_dir)
    out: Dict[str, str] = {}
    if not os.path.isdir(run):
        return out
    tier_re = re.compile(r"trust tier:\s*`([^`]+)`")
    node_re = re.compile(r"# Invoke Payload:\s*(.+)")
    for name in sorted(os.listdir(run)):
        if not name.endswith("__payload.md"):
            continue
        text = open(os.path.join(run, name), encoding="utf-8").read()
        node_m = node_re.search(text)
        tier_m = tier_re.search(text)
        if node_m and tier_m:
            out[node_m.group(1).strip()] = tier_m.group(1).strip()
    return out


__all__ = [
    "capture_run",
    "load_payload_tiers",
    "gate_run_dir",
    "CaptureEnvelope",
    "CaptureError",
    "capture",
    "adapt_plan",
    "adapt_payload",
    "PLAN",
    "AGENT_RESPONSE",
    "AGENT_LOG",
    "RUN_SUMMARY",
    "BINDING",
    "PAYLOAD",
]
