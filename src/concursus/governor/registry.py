"""Versioned agent registry (S9-G7) — the governor's **process table**.

The shipped :class:`~concursus.build.ledger.DeployLedger` answers exactly one
create-time question: *"have I already stood up this exact content (name +
hosting fingerprint)?"*. It deliberately does **not** answer the dispatch-time
question the scheduler and cockpit need: *"which standing agent — at which
version — can do task X right now?"*.

This module adds that missing view as a strictly-outer governor layer built
**on top of** the ledger. It:

* derives a **standing process table** (one current-version row per agent name)
  purely by READING ``ledger.rows()`` and grouping by ``(name, fingerprint)``;
* tracks **versions** per agent — each distinct fingerprint recorded for a name
  is a monotonically-numbered version, newest wins as the *current* version
  (mirroring :meth:`DeployLedger.lookup`'s newest-row-wins semantics);
* **matches a task** to the *current* version of an agent whose declared
  capabilities cover that task;
* offers **on-demand spawn/fork** for an unmatched task by routing through the
  already-shipped :func:`~concursus.build.provision.provision_agent` actuator —
  it never introduces a new compiler/deploy path.

IDENTITY INVARIANTS (INV-5, memory seam):

* The registry is **READ-ONLY over the ledger**: every query re-reads
  ``ledger.rows()`` and never calls ``ledger.record`` / mutates a row. The
  ledger remains the sole persisted record of deployed content; the registry is
  a disposable derived view over it (a rebuildable projection).
* Capability metadata (which task labels an agent serves) is registry-side and
  is *never* written back into the ledger — the ledger answers content-identity
  only, never "who can do task X".
* Spawn/fork do **not** write the ledger themselves. They delegate to
  ``provision_agent``, which owns the (optional) ledger append. The registry
  merely re-reads the ledger afterward to see the new standing version.
* No compiler mutation, no ``assemble()`` call, no Supervisor reach-in: this is
  an outer projection + a thin actuator delegation, nothing more.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from concursus.build.ledger import DeployLedger, DeployRow

# A capability-derivation hook: manifest -> the set of task labels it serves.
CapabilityFn = Callable[[Any], Set[str]]


def _default_capabilities(manifest: Any) -> Set[str]:
    """Default capability set for a manifest: an explicit ``capabilities`` field
    if declared, else the agent's own name (so a task named after the agent
    matches out of the box)."""
    caps: Set[str] = set()
    name = getattr(manifest, "name", None)
    if name:
        caps.add(str(name))
    # Optional author-declared capabilities on the manifest (registry, spec, or attr).
    declared = _declared_capabilities(manifest)
    caps.update(declared)
    return caps


def _declared_capabilities(manifest: Any) -> Set[str]:
    """Pull any author-declared capability labels off a manifest, tolerantly."""
    declared: Any = None
    reg = getattr(manifest, "registry", None)
    if isinstance(reg, dict):
        declared = reg.get("capabilities")
    if declared is None:
        declared = getattr(manifest, "capabilities", None)
    if not declared:
        return set()
    if isinstance(declared, (list, tuple, set)):
        return {str(c) for c in declared}
    return {str(declared)}


@dataclass(frozen=True)
class AgentVersion:
    """One standing version of an agent, derived from the ledger.

    A distinct ``fingerprint`` recorded for a ``name`` is one version. ``version``
    is 1-based in order of first appearance of that fingerprint for the name. The
    newest recorded version is the *current* one the scheduler dispatches to.
    """

    name: str
    fingerprint: str
    version: int
    arn: Optional[str] = None
    image_uri: Optional[str] = None
    role_arn: Optional[str] = None
    deployed_at: Optional[Any] = None
    capabilities: frozenset = field(default_factory=frozenset)

    def serves(self, task: str) -> bool:
        """True iff this version's capabilities cover ``task``."""
        return task in self.capabilities


class AgentRegistry:
    """A versioned, read-only process-table view over a :class:`DeployLedger`.

    Construct with the ledger the deploy path writes to. Register manifests to
    supply per-agent capability metadata (which tasks an agent serves), then
    query the standing process table / match tasks to current versions. Every
    query re-reads the ledger, so a version deployed by another process becomes
    visible without any registry mutation.
    """

    def __init__(
        self,
        ledger: DeployLedger,
        *,
        capability_fn: Optional[CapabilityFn] = None,
    ) -> None:
        self._ledger = ledger
        self._capability_fn = capability_fn or _default_capabilities
        # Registry-side capability metadata only — NEVER written to the ledger.
        self._capabilities: Dict[str, Set[str]] = {}

    # -- capability registration (registry-side metadata only) --------------
    def register_agent(
        self,
        manifest: Any,
        *,
        capabilities: Optional[Set[str]] = None,
    ) -> Set[str]:
        """Record which task labels ``manifest``'s agent serves; return them.

        This records registry-side metadata only — it does NOT deploy anything
        and does NOT touch the ledger. Versions are still derived from the
        ledger; this only teaches the registry which tasks the named agent can
        serve so :meth:`match_task` can resolve to it.
        """
        name = str(getattr(manifest, "name", "") or "")
        if not name:
            raise ValueError("register_agent requires a manifest with a name")
        caps = set(capabilities) if capabilities is not None else set(
            self._capability_fn(manifest)
        )
        self._capabilities[name] = caps
        return set(caps)

    def capabilities_for(self, name: str) -> Set[str]:
        """The registered capability labels for ``name`` (empty if unregistered)."""
        return set(self._capabilities.get(name, set()))

    # -- version tracking (READ-ONLY over the ledger) -----------------------
    def versions(self, name: str) -> List[AgentVersion]:
        """All standing versions of ``name``, oldest first, derived from the ledger.

        Each distinct fingerprint recorded for ``name`` is one version; a later
        row for a fingerprint already seen refreshes that version's live details
        (arn/deployed_at) without allocating a new version number.
        """
        caps = frozenset(self._capabilities.get(name, set()))
        by_fp: Dict[str, AgentVersion] = {}
        order: List[str] = []
        for row in self._ledger.rows():  # oldest first; a pure READ of the ledger
            if row.name != name:
                continue
            fp = row.fingerprint
            if fp not in by_fp:
                order.append(fp)
            # newest row for a fingerprint wins on live details (arn/deployed_at)
            by_fp[fp] = AgentVersion(
                name=row.name,
                fingerprint=fp,
                version=(order.index(fp) + 1),
                arn=row.arn,
                image_uri=row.image_uri,
                role_arn=row.role_arn,
                deployed_at=row.deployed_at,
                capabilities=caps,
            )
        return [by_fp[fp] for fp in order]

    def current(self, name: str) -> Optional[AgentVersion]:
        """The current (newest) standing version of ``name``, or ``None``.

        Newest-row-wins, mirroring :meth:`DeployLedger.lookup`.
        """
        versions = self.versions(name)
        return versions[-1] if versions else None

    def names(self) -> List[str]:
        """All agent names present in the ledger, in first-seen order."""
        seen: List[str] = []
        for row in self._ledger.rows():
            if row.name not in seen:
                seen.append(row.name)
        return seen

    def process_table(self) -> Dict[str, AgentVersion]:
        """The standing process table: ``name -> current version`` for every agent.

        A pure projection over the ledger — the scheduler matches against this and
        the cockpit monitors it. Read-only: nothing is scheduled or seeded.
        """
        table: Dict[str, AgentVersion] = {}
        for name in self.names():
            cur = self.current(name)
            if cur is not None:
                table[name] = cur
        return table

    # -- task matching ------------------------------------------------------
    def match_task(self, task: str) -> Optional[AgentVersion]:
        """The current version of a standing agent that serves ``task``, or ``None``.

        Considers only the *current* version of each agent (the process table);
        an older version is never dispatched to even if it once served the task.
        """
        for version in self.process_table().values():
            if version.serves(task):
                return version
        return None

    def match_all(self, task: str) -> List[AgentVersion]:
        """Every current-version agent that serves ``task`` (process-table scan)."""
        return [v for v in self.process_table().values() if v.serves(task)]

    # -- on-demand spawn / fork (routes through provision_agent) ------------
    def ensure_task(
        self,
        task: str,
        *,
        entry: Any,
        clients: Any,
        manifest: Any = None,
        capabilities: Optional[Set[str]] = None,
        provision_fn: Optional[Callable[..., Dict[str, Any]]] = None,
        **provision_kwargs: Any,
    ) -> AgentVersion:
        """Return the current version serving ``task``, spawning one on demand.

        If the process table already has a current version serving ``task``, that
        version is returned unchanged (no deploy). Otherwise this **spawns** the
        agent by delegating to the shipped :func:`provision_agent` actuator (never
        a new compiler path); ``provision_agent`` owns the ledger append, so the
        registry stays read-only over the ledger. After provisioning, the registry
        re-reads the ledger and returns the freshly-standing current version.
        """
        existing = self.match_task(task)
        if existing is not None:
            return existing
        self._provision(
            entry=entry,
            clients=clients,
            manifest=manifest,
            capabilities=capabilities,
            provision_fn=provision_fn,
            task=task,
            **provision_kwargs,
        )
        return self._resolve_after_provision(entry, task)

    def fork(
        self,
        name: str,
        *,
        entry: Any,
        clients: Any,
        manifest: Any = None,
        capabilities: Optional[Set[str]] = None,
        provision_fn: Optional[Callable[..., Dict[str, Any]]] = None,
        **provision_kwargs: Any,
    ) -> AgentVersion:
        """Stand up a **new version** of an existing agent ``name`` on demand.

        A fork is a same-name deploy with a changed hosting fingerprint. Like
        :meth:`ensure_task` it delegates to :func:`provision_agent` (which owns
        the ledger append) and then re-reads the ledger for the new current
        version. The registry itself never writes the ledger.
        """
        self._provision(
            entry=entry,
            clients=clients,
            manifest=manifest,
            capabilities=capabilities,
            provision_fn=provision_fn,
            task=None,
            **provision_kwargs,
        )
        cur = self.current(name)
        if cur is None:
            raise RegistryError(
                f"fork of {name!r} did not yield a standing version in the ledger"
            )
        return cur

    # -- internals ----------------------------------------------------------
    def _provision(
        self,
        *,
        entry: Any,
        clients: Any,
        manifest: Any,
        capabilities: Optional[Set[str]],
        provision_fn: Optional[Callable[..., Dict[str, Any]]],
        task: Optional[str],
        **provision_kwargs: Any,
    ) -> Dict[str, Any]:
        """Delegate a spawn/fork to the shipped actuator; register capabilities."""
        fn = provision_fn or _default_provision_fn()
        # Teach the registry this agent's capabilities before it becomes standing.
        if manifest is not None:
            self.register_agent(manifest, capabilities=capabilities)
        elif capabilities is not None:
            self._capabilities[str(getattr(entry, "name", ""))] = set(capabilities)
        elif task is not None:
            # Minimal capability so the just-spawned agent matches the task.
            self._capabilities.setdefault(str(getattr(entry, "name", "")), set()).add(task)
        return fn(
            entry,
            clients=clients,
            ledger=self._ledger,  # provision_agent owns the append; registry does not
            manifest=manifest,
            **provision_kwargs,
        )

    def _resolve_after_provision(self, entry: Any, task: str) -> AgentVersion:
        name = str(getattr(entry, "name", ""))
        cur = self.current(name)
        if cur is None or not cur.serves(task):
            raise RegistryError(
                f"spawn for task {task!r} did not yield a matching standing version"
            )
        return cur


class RegistryError(RuntimeError):
    """Raised when a spawn/fork does not resolve to a standing ledger version."""


def _default_provision_fn() -> Callable[..., Dict[str, Any]]:
    """Bind the shipped actuator lazily (keeps import cost/AWS surface off import)."""
    from concursus.build.provision import provision_agent

    return provision_agent
