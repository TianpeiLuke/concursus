# API Reference: `governor`

*`GovernorLoop`, `TrustLadderScheduler`, `AgentRegistry`, `ScopeAddress`, `KTLODaemon`, `DirectorCockpit` — the runtime-governance organ that wraps the compiler.*

The `governor` tier is the runtime-governance organ of `concursus`. It wraps the compiler ([`assemble`](assemble.md)/`recompile` → frozen [`ProvisioningPlan`](assemble.md)) and the single-static-pass [`Supervisor`](execute.md) in a strictly-outer, bounded control loop. Each round forms a *fresh* frozen plan at the compiler front, runs exactly one Supervisor episode over it, folds the outputs into an append-only [`StateStore`](state.md) log, then decides — bounded — whether to replan or synthesize.

> Load-bearing invariant, stated once: **Concursus is a compiler, not a runtime governor.** The governor loop is *strictly outer* — it never reaches inside a running `Supervisor`, never mutates a frozen plan, and never collapses the compiler into the loop. Frozen plans are values swapped by version; the append-only log is the sole structural anchor of the executed prefix; trust moves only GOV-side; and cockpit/registry/scope are read-only projections. Plan/execute is not a refusal to govern — it is *how* the governor governs.

This page is the terse per-module symbol catalog. For the narrative walkthrough — the mental model, when to reach for each seam, and worked end-to-end examples — read [Guide: The Governor](../guides/governor.md) first. For the create-time deploy gate this tier complements, see [`build` reference → Trust Ladder](build.md).

The tier is eight modules:

| Module | Source | Owns |
|---|---|---|
| `governor.state` | [`../../src/concursus/governor/state.py`](../../src/concursus/governor/state.py) | Persistent outer-loop state: the ordered sequence of frozen plan VALUEs by version + a log pointer. |
| `governor.scope` | [`../../src/concursus/governor/scope.py`](../../src/concursus/governor/scope.py) | The `org → portfolio → program → task` scope stack + read-only cross-program synthesis. |
| `governor.registry` | [`../../src/concursus/governor/registry.py`](../../src/concursus/governor/registry.py) | The versioned, read-only process table over the shipped `DeployLedger`. |
| `governor.authoring` | [`../../src/concursus/governor/authoring.py`](../../src/concursus/governor/authoring.py) | Net-new agent-manifest authoring (opt-in) — the deepest form of Create. |
| `governor.scheduler` | [`../../src/concursus/governor/scheduler.py`](../../src/concursus/governor/scheduler.py) | The per-decision Trust-Ladder router/matcher + candidate-set binder (opt-in). |
| `governor.loop` | [`../../src/concursus/governor/loop.py`](../../src/concursus/governor/loop.py) | The fixed cyclic outer driver `GovernorLoop` + resume seam. |
| `governor.ktlo` | [`../../src/concursus/governor/ktlo.py`](../../src/concursus/governor/ktlo.py) | The standing "keep-the-lights-on" daemon above the loop. |
| `governor.cockpit` | [`../../src/concursus/governor/cockpit.py`](../../src/concursus/governor/cockpit.py) | The read-only director surfaces (briefing / exception queue / runs monitor). |

The symbols below are re-exported from `concursus.governor` (see [`governor/__init__.py`](../../src/concursus/governor/__init__.py)); most are also on the package root `concursus`. A few symbols are **not** in that re-export set and are imported from their submodule directly — [`Binding`](#binding) (`from concursus.governor.scheduler import Binding`) and the [`governor.authoring`](#governorauthoring) symbols (`author_manifest`, `ManifestAuthorError`, `ManifestAuthorFn`, plus the staffing symbols [`staff_capability_dag`](#staff_capability_dag), [`staff_with_rebind`](#staff_with_rebind), [`RebindExhausted`](#rebindexhausted), [`CandidatesFn`](#candidatesfn)). The strictness / payload dials [`make_trust_strictness`](#make_trust_strictness), [`Tier`](#tier), [`make_payload_tier`](#make_payload_tier), [`project_context`](#project_context), and [`manifest_is_programmatic`](#manifest_is_programmatic) ARE re-exported from `concursus.governor` but, unlike most `governor` symbols, are **not** on the package root:

```python
from concursus.governor import (
    GovernorState,
    ScopeAddress, SCOPE_LEVELS, SCOPE_SEP, ScopeError,
    build_programs_index, programs_dir, render_programs_index, director_leverage_view,
    AgentRegistry, AgentVersion, RegistryError,
    TrustLadderScheduler, ScheduleDecision, FrontierProposal,
    DISPATCH, ESCALATE, UNMATCHED, SchedulerError,
    make_trust_strictness,
    Tier, make_payload_tier, project_context, manifest_is_programmatic,
    GovernorLoop, GovernorResult, GovernorLoopError,
    CheckpointStore, InProcessCheckpointStore, GOV_NODES,
    EventSink, NullEventSink, FanOutEventSink,
    KTLODaemon, KTLOResult, KTLODaemonError,
    EventSource, InProcessEventQueue, ScriptedEventSource,
    FireBudgetGate, ProvenanceGuard, EpisodeAdmissionGate, DetectionMode,
    LAUNCH, KTLO, TRIAGE_CLOSE, TRIAGE_INVESTIGATE, TRIAGE_ESCALATE,
)
# DirectorCockpit is exported from the package root:
from concursus import DirectorCockpit
```

A handful of the opt-in seams are **not** in the `governor` re-export set and are imported from their submodule directly: [`GOV_EVENT_KINDS`](#gov_event_kinds) (`from concursus.governor.loop import GOV_EVENT_KINDS`); the [`ControlSurface`](#controlsurface)/[`ControlSurfaceError`](#controlsurfaceerror) agent-facing surface and the [`NodeEventBus`](#nodeeventbus) (`from concursus.governor.cockpit import ControlSurface, ControlSurfaceError, NodeEventBus`); the control-verb taxonomy [`READ_VERBS`](#read_verbs--actuating_verbs)/[`ACTUATING_VERBS`](#read_verbs--actuating_verbs)/`RECURSIVE_VERBS` + the compiled [`ControlScope`](#controlscope) bound (`from concursus.governor.scope import READ_VERBS, ACTUATING_VERBS, ControlScope`); and the [`IdleRuntimeCuller`](#idleruntimeculler) + its cull-tier constants (`from concursus.governor.ktlo import IdleRuntimeCuller, CULL_TIER_STANDING, CULL_TIER_EPHEMERAL`). Every one of these is opt-in and default-off: constructing/wiring none of them leaves the default loop, daemon, and cockpit byte-for-byte unchanged.

> **The opt-in, default-off additions.** The flexibility & robustness layer completed in v0.6.0 — the Trust-Ladder scheduler, the frontier/decompose channels, auto-create, the payload-tier dials, the episode gate + event sink, the standing-fleet admission gates, and the agent-facing control surface — are all additive. Omitting them (or leaving the new keyword args at their `False` / `None` defaults) leaves the default `plan → deploy → run` **byte-for-byte unchanged**; Concursus stays a compiler that makes a single static pass over a frozen `plan.order`.

---

## `governor.state`

Source: [`../../src/concursus/governor/state.py`](../../src/concursus/governor/state.py)

The persistent outer-loop state for the governor cycle. It is deliberately **not** a mutable compiler plan: it holds the ordered SEQUENCE of frozen `ProvisioningPlan` VALUEs produced across rounds (by version), plus a POINTER to the append-only `StateStore` log. There is no `set_output`-style API and no method that edits a plan in place.

| Symbol | Kind | Summary |
|---|---|---|
| [`GovernorState`](#governorstate) | dataclass | Sequence of frozen plan VALUEs + a log pointer + round counters. |
| [`GovernorState.advance`](#governorstateadvance) | method | Swap in a newly-formed plan and bump the version. |

### `GovernorState`

```python
@dataclass
class GovernorState:
    current_frozen_plan: ProvisioningPlan
    store: StateStore
    plan_version: int = 0
    iteration: int = 0
    no_progress: int = 0
    replan_reason: Optional[str] = None
    plan_history: List[ProvisioningPlan] = field(default_factory=list)
```

Outer-loop state. A mutable `@dataclass` (not frozen), but the plan VALUEs it holds are never edited in place.

| Field | Meaning |
|---|---|
| `current_frozen_plan` | The frozen plan VALUE for the current round. Mirrors the tail of `plan_history`. |
| `store` | POINTER to the append-only `StateStore` log — the sole structural anchor of the executed prefix (held, not copied inline). |
| `plan_version` | Mirrors `current_frozen_plan.revision`. **Derived, not caller-set** — see below. |
| `iteration` | Number of governor rounds/episodes run so far. |
| `no_progress` | Consecutive rounds that made no forward progress (feeds the stall bound). |
| `replan_reason` | Why the most recent replan happened (`None` before any replan). |
| `plan_history` | The full ordered sequence of frozen plan VALUEs, oldest first; each entry is replayable in isolation. |

**`__post_init__`** (runs automatically at construction): pins `plan_version` to `current_frozen_plan.revision`, and — if `plan_history` is empty — seeds it with `[current_frozen_plan]` so the sequence is complete from round zero. A pre-populated `plan_history` (e.g. reconstructed) is left as-is.

```python
from concursus.governor import GovernorState

state = GovernorState(current_frozen_plan=plan, store=store)
# plan_version == plan.revision ; plan_history == [plan]
```

> **Gotcha.** `plan_version` is derived: passing `plan_version=` to the constructor is overwritten in `__post_init__` to `current_frozen_plan.revision`.

#### `GovernorState.advance`

```python
def advance(
    self,
    next_plan: ProvisioningPlan,
    *,
    reason: Optional[str] = None,
    progressed: bool = True,
) -> "GovernorState"
```

Swaps in a newly-assembled/recompiled plan and bumps the version. Appends `next_plan` to `plan_history`, re-points `current_frozen_plan`, sets `plan_version = next_plan.revision`, increments `iteration`, resets `no_progress` to `0` if `progressed` else increments it, and records `reason`.

- **Returns** `self` — it mutates the `GovernorState` *container* in place, but does **not** edit any plan object. The prior plan value stays byte-identical in `plan_history` (INV-3/INV-4). The `store` pointer is unchanged; the executed prefix stays re-derivable from the log (INV-5).

```python
state.advance(recompiled_plan, reason="replan", progressed=True)
# bumps version, appends to plan_history, resets no_progress
```

> **Gotchas.** `advance()` mutates and returns `self` — it is *not* a copy-on-write of the state container; only the held plan VALUEs are immutable. `no_progress` resets to `0` on any `progressed=True` advance, so a stall must be a *run* of `progressed=False` advances.

---

## `governor.scope`

Source: [`../../src/concursus/governor/scope.py`](../../src/concursus/governor/scope.py)

The program/portfolio scope stack *above* the single-run unit: an `org → portfolio → program → task` scope address, plus read-only cross-program memory synthesis at PROGRAM grain (a programs index) and a 1:N director-leverage view. Pure-Python, stdlib only.

> **INV-5 (memory seam).** Scope is a pure GOV aggregation over READ MODELS — no compiler impact. Everything is a read-only projection over the per-run precedent notes (loaded via `load_precedents`). It selects nothing, seeds nothing, drives no dispatch; it never calls `assemble()`/`recompile()`/`Supervisor.run()`/`StateStore.put()`. Regenerated from scratch each call: same notes → byte-identical output.

| Symbol | Kind | Summary |
|---|---|---|
| [`SCOPE_LEVELS`](#scope_levels) | constant | The ordered scope stack, coarsest → finest. |
| [`SCOPE_SEP`](#scope_sep) | constant | The `trail_id` addressing separator (`"."`). |
| [`READ_VERBS`](#read_verbs--actuating_verbs) / [`ACTUATING_VERBS`](#read_verbs--actuating_verbs) / `RECURSIVE_VERBS` | constants | The control-verb taxonomy the [`ControlSurface`](#controlsurface) is built on. |
| [`ControlScope`](#controlscope) | dataclass | The compiled authorization bound a `ControlSurface` reads from (opt-in). |
| [`ScopeError`](#scopeerror) | exception | Malformed scope operation (subclass of `ValueError`). |
| [`ScopeAddress`](#scopeaddress) | dataclass | A frozen point in the scope stack. |
| [`ScopeAddress.from_trail_id`](#scopeaddressfrom_trail_id) | classmethod | Parse a `trail_id` into an address (levels top-down). |
| [`ScopeAddress.push`](#scopeaddresspush) | method | Return a NEW address filling the next empty level. |
| [`ScopeAddress.to_trail_id`](#scopeaddressto_trail_id) | method | Join set levels into a `trail_id`. |
| [`ScopeAddress.program_key`](#scopeaddressprogram_key) | method | The `org.portfolio.program` PROGRAM-grain key. |
| [`ScopeAddress.depth`](#scopeaddressdepth) | method | How many levels are set (0..4). |
| [`ScopeAddress.to_dict`](#scopeaddressto_dict) | method | The address as a plain dict keyed by level. |
| [`build_programs_index`](#build_programs_index) | function | Aggregate per-run notes into a PROGRAM-grain projection. |
| [`programs_dir`](#programs_dir) | function | The dedicated `<vault>/programs/` tree path. |
| [`render_programs_index`](#render_programs_index) | function | Render the cross-program memory hub to a file; return its path. |
| [`director_leverage_view`](#director_leverage_view) | function | The 1:N leverage rollup over `build_programs_index`. |

### `SCOPE_LEVELS`

```python
SCOPE_LEVELS = ("org", "portfolio", "program", "task")
```

The ordered scope stack, coarsest → finest. A run/episode is a `"task"`. Drives `push()` ordering and `to_dict()` keys.

### `SCOPE_SEP`

```python
SCOPE_SEP = "."
```

The `trail_id` addressing separator. A `trail_id` is a scope address whose segments fill the levels top-down. (Duplicated as a literal in `loop.py` to avoid a module-top scope import — see [`loop.SCOPE_SEP`](#gov_nodes--supervisorfactory--scope_sep).)

### `READ_VERBS` / `ACTUATING_VERBS`

```python
READ_VERBS: FrozenSet[str]       = frozenset({"query_plan", "tail_log", "search_runs", "precedents"})
ACTUATING_VERBS: FrozenSet[str]  = frozenset({"deploy", "run", "recompile"})
RECURSIVE_VERBS: FrozenSet[str]  = frozenset({"recompile"})
```

The control-verb taxonomy the [`ControlSurface`](#controlsurface) is built on. `READ_VERBS` are pure, side-effect-free projections over the SSOT and are **always** exposed. `ACTUATING_VERBS` mutate through the real actuators and are exposed **only if the compiled [`ControlScope`](#controlscope) authorized them** (non-registration gating). `RECURSIVE_VERBS` (a subset — `recompile`) are the actuating verbs that can re-enter the compiler; they carry the same authorization + activation gates. Imported from the submodule directly (`from concursus.governor.scope import READ_VERBS, ACTUATING_VERBS`).

### `ControlScope`

```python
@dataclass(frozen=True)
class ControlScope:
    actuating: FrozenSet[str] = frozenset()   # the ACTUATING_VERBS the compiled scope authorized
    trust_ceiling: Optional[int] = None       # the compiled TrustGrade ceiling (0-3), or None
    revision: Optional[int] = None            # the frozen plan revision it was compiled from
    @classmethod
    def from_plan(cls, plan, *, authorize=None, trust_ceiling=None) -> "ControlScope"
    def available_verbs(self) -> FrozenSet[str]   # READ_VERBS | self.actuating
    def authorizes(self, verb: str) -> bool       # READ_VERBS always True; actuating iff authorized
```

The **compiled authorization bound** a [`ControlSurface`](#controlsurface) reads from — a frozen VALUE derived from the frozen plan (its `revision` + the authorized actuating subset + the [`TrustGrade`](build.md) ceiling), *not* an env var. By default a plan authorizes NO actuating verb (read-only, offline — the safe floor); an operator may pass `authorize` (an iterable of verb names, or a plan attribute read from `plan.control_verbs`) to bind specific actuating verbs (anything not in `ACTUATING_VERBS` is dropped). `available_verbs()` is exactly the verb set the surface will register; `authorizes()` is the non-registration predicate. Any actuating verb outside `actuating` is unrepresentable on the surface rather than runtime-rejected. Opt-in: constructed only when a `ControlSurface` is wired. Imported from `concursus.governor.scope`.

### `ScopeError`

```python
class ScopeError(ValueError)
```

Raised for a malformed scope operation (e.g. an over-deep `push` onto an already-full stack). Subclass of `ValueError`.

### `ScopeAddress`

```python
@dataclass(frozen=True)
class ScopeAddress:
    org: str = ""
    portfolio: str = ""
    program: str = ""
    task: str = ""
```

A frozen VALUE point in the `org → portfolio → program → task` scope stack. All four levels are strings defaulting to `""`. `push()` returns a NEW address (never mutates), giving stack semantics without shared mutable state. Levels fill top-down, so a partial address (only `org`/`portfolio` set) is a scope PREFIX many programs/tasks live under.

#### `ScopeAddress.from_trail_id`

```python
@classmethod
def from_trail_id(cls, trail_id: str, *, sep: str = SCOPE_SEP) -> "ScopeAddress"
```

Parses a `trail_id` scope address, filling levels top-down. The first three `sep`-segments map to `org`/`portfolio`/`program`; any remaining segments join back into `task` (so a task may itself contain `sep`). Fewer than four segments leave the deeper levels empty. Tolerant of `None`/empty input (`str(trail_id or "")`). Inverse of [`to_trail_id`](#scopeaddressto_trail_id) for a full four-level address.

```python
from concursus.governor import ScopeAddress

addr = ScopeAddress.from_trail_id("acme.retail.oncall.ticket-42")
# org="acme", portfolio="retail", program="oncall", task="ticket-42"
```

> **Gotcha.** A `task` segment can itself contain the separator: `from_trail_id` joins `parts[3:]` back into `task`, so `"a.b.c.d.e"` → `task="d.e"`.

#### `ScopeAddress.push`

```python
def push(self, value: str) -> "ScopeAddress"
```

Returns a NEW address with `value` filling the next empty level (`org → portfolio → program → task`). Never mutates `self`.

- **Raises** [`ScopeError`](#scopeerror) — if the stack is already full (`task` already set).

```python
root = ScopeAddress()
prefix = root.push("acme").push("retail")   # partial PREFIX: org+portfolio set
```

#### `ScopeAddress.to_trail_id`

```python
def to_trail_id(self, *, sep: str = SCOPE_SEP) -> str
```

Joins the set levels (dropping trailing empties) into a `trail_id` string. Inverse of [`from_trail_id`](#scopeaddressfrom_trail_id) for a full address.

#### `ScopeAddress.program_key`

```python
def program_key(self, *, sep: str = SCOPE_SEP) -> str
```

The PROGRAM-grain key: the `org.portfolio.program` prefix (trailing empties dropped). Runs sharing this key belong to the same program.

> **Gotcha.** Returns `""` (empty string) for an address with no `org` — an ungrouped run. Such runs collapse into a single `""` bucket in the programs index.

#### `ScopeAddress.depth`

```python
def depth(self) -> int
```

How many levels are set (0..4). Counts from the top, stopping at the first empty.

#### `ScopeAddress.to_dict`

```python
def to_dict(self) -> Dict[str, str]
```

The address as a plain, JSON-serializable dict keyed by level (`org`/`portfolio`/`program`/`task`). Unlike `to_trail_id`/`program_key`, this *includes* empty levels.

### `build_programs_index`

```python
def build_programs_index(vault_path, *, sep: str = SCOPE_SEP) -> Dict[str, dict]
```

Aggregates the per-run precedent notes (via `load_precedents` — the single source of truth) into a PROGRAM-grain projection. Maps each run's `trail_id` to a [`ScopeAddress`](#scopeaddress) and rolls the runs up by [`program_key`](#scopeaddressprogram_key).

- **Returns** a dict keyed by `program_key`; each value is `{"program_key", "org", "portfolio", "program", "runs" (sorted list), "run_count", "status_counts" (dict of status→count)}`.
- `trail_id` is read from `payload["trail_id"]` (the record's `output` if it is a dict), falling back to `record.node`; `status` from `payload["status"]`.
- Pure function; no I/O beyond reading the notes; deterministic (runs sorted, `run_count` finalized).

```python
from concursus.governor import build_programs_index

index = build_programs_index("/path/to/vault")
# {"acme.retail.oncall": {"runs": [...], "run_count": 3, "status_counts": {...}, ...}}
```

> **Gotcha.** `build_programs_index` prefers `payload["trail_id"]` over `record.node` — it falls back to `record.node` only when the note payload lacks a `trail_id`.

### `programs_dir`

```python
def programs_dir(vault_path) -> Path
```

Returns the dedicated `<vault>/programs/` tree (a sibling of `precedents/` and `runs/`) as a `pathlib.Path`. Does *not* create the directory ([`render_programs_index`](#render_programs_index) does).

### `render_programs_index`

```python
def render_programs_index(
    vault_path, *, sep: str = SCOPE_SEP, slipbox_form: bool = False, date: str = ""
) -> str
```

Renders the cross-program memory hub to `<vault>/programs/_index.md` and returns its path (`str`). The program-grain analogue of `render_precedent_hub`: one section per program (keyed + sorted by `program_key`), regenerated from scratch each call. `slipbox_form=True` prepends a YAML frontmatter block (tags/keywords/topics/date/status/etc.); `date` fills the "date of note" field. Each section shows `run_count` + a status digest and a bullet per run; the body reads `(no programs synthesized yet)` when empty.

```python
from concursus.governor import render_programs_index

path = render_programs_index("/path/to/vault", slipbox_form=True, date="2026-07-15")
# writes <vault>/programs/_index.md ; returns its path
```

> **Gotchas.** This function *does* perform file I/O — it `mkdir`s `<vault>/programs/` (`parents=True, exist_ok=True`) and atomically writes the index file. "Read-only" here means it never touches plan/store/dispatch, not that it writes nothing; it is still idempotent (same notes → byte-identical output).

### `director_leverage_view`

```python
def director_leverage_view(vault_path, *, sep: str = SCOPE_SEP) -> Dict[str, object]
```

The 1:N leverage view — one director over many programs, many episodes. A read-only synthesis over [`build_programs_index`](#build_programs_index).

- **Returns** `{"program_count", "run_count" (total across programs), "runs_per_program" (dict), "status_counts" (cross-program rollup dict), "programs" (sorted list of program_keys)}`.

```python
from concursus.governor import director_leverage_view

view = director_leverage_view("/path/to/vault")
# {"program_count": N, "run_count": M, "runs_per_program": {...}, ...}
```

---

## `governor.registry`

Source: [`../../src/concursus/governor/registry.py`](../../src/concursus/governor/registry.py)

The versioned agent registry — the governor's **process table**. A strictly-outer, read-only view built *on top of* the shipped [`DeployLedger`](build.md), answering the dispatch-time question *"which standing agent, at which version, can do task X right now?"* that the ledger's content-identity lookup does not.

> **Read-only over the ledger.** Every query re-reads `ledger.rows()` and never calls `ledger.record`/mutates a row. Capability metadata is registry-side only and is never written back to the ledger. Spawn/fork delegate to `provision_agent` (which owns the optional ledger append); the registry re-reads the ledger afterward. Version numbering: each distinct fingerprint for a name is one version, 1-based in first-appearance order; the newest version is *current* (mirroring `DeployLedger.lookup`'s newest-row-wins).

| Symbol | Kind | Summary |
|---|---|---|
| [`CapabilityFn`](#capabilityfn) | type alias | `Callable[[Any], Set[str]]` — manifest → served task labels. |
| [`AgentVersion`](#agentversion) | dataclass | One standing version of an agent, derived from the ledger. |
| [`AgentVersion.serves`](#agentversionserves) | method | Whether this version's capabilities cover a task. |
| [`AgentRegistry`](#agentregistry) | class | The read-only process-table view over a `DeployLedger`. |
| [`AgentRegistry.register_agent`](#agentregistryregister_agent) | method | Record which task labels an agent serves (registry-side only). |
| [`AgentRegistry.capabilities_for`](#agentregistrycapabilities_for) | method | The registered capability labels for a name. |
| [`AgentRegistry.versions`](#agentregistryversions) | method | All standing versions of a name, oldest first. |
| [`AgentRegistry.current`](#agentregistrycurrent) | method | The current (newest) standing version of a name. |
| [`AgentRegistry.names`](#agentregistrynames) | method | All agent names in the ledger, first-seen order. |
| [`AgentRegistry.process_table`](#agentregistryprocess_table) | method | `name → current version` for every agent. |
| [`AgentRegistry.match_task`](#agentregistrymatch_task) | method | The current version serving a task, or `None`. |
| [`AgentRegistry.match_all`](#agentregistrymatch_all) | method | Every current-version agent serving a task. |
| [`AgentRegistry.ensure_task`](#agentregistryensure_task) | method | Return the current version serving a task, spawning on demand. |
| [`AgentRegistry.fork`](#agentregistryfork) | method | Stand up a NEW version of an existing agent on demand. |
| [`RegistryError`](#registryerror) | exception | Spawn/fork did not resolve to a standing version. |

### `CapabilityFn`

```python
CapabilityFn = Callable[[Any], Set[str]]
```

Type alias for a capability-derivation hook: `manifest → the set of task labels it serves`. The registry default (`_default_capabilities`) gives an agent a capability equal to its own name, plus any `manifest.registry["capabilities"]` or `manifest.capabilities`.

### `AgentVersion`

```python
@dataclass(frozen=True)
class AgentVersion:
    name: str
    fingerprint: str
    version: int
    arn: Optional[str] = None
    image_uri: Optional[str] = None
    role_arn: Optional[str] = None
    deployed_at: Optional[Any] = None
    capabilities: frozenset = field(default_factory=frozenset)
```

One standing version of an agent, derived from the ledger. A distinct `fingerprint` recorded for a `name` is one version; `version` is 1-based in first-appearance order. The newest recorded version is the current one the scheduler dispatches to. `arn`/`image_uri`/`role_arn`/`deployed_at` are live details refreshed from the newest ledger row for that fingerprint; `capabilities` is a `frozenset` of task labels.

#### `AgentVersion.serves`

```python
def serves(self, task: str) -> bool
```

`True` iff this version's capabilities cover `task` (`task in self.capabilities`).

### `AgentRegistry`

```python
class AgentRegistry:
    def __init__(
        self,
        ledger: DeployLedger,
        *,
        capability_fn: Optional[CapabilityFn] = None,
    ) -> None
```

A versioned, read-only process-table view over a `DeployLedger`. Construct with the ledger the deploy path writes to; register manifests for per-agent capabilities, then query the process table / match tasks to current versions. Every query re-reads the ledger, so a version deployed by another process becomes visible on the next query with no registry mutation. `capability_fn` defaults to `_default_capabilities`.

```python
from concursus.governor import AgentRegistry

registry = AgentRegistry(ledger)
registry.register_agent(manifest, capabilities={"triage", "summarize"})
av = registry.match_task("triage")            # -> AgentVersion or None
table = registry.process_table()               # {"triager": AgentVersion(version=3, ...)}
```

#### `AgentRegistry.register_agent`

```python
def register_agent(
    self,
    manifest: Any,
    *,
    capabilities: Optional[Set[str]] = None,
) -> Set[str]
```

Records which task labels `manifest`'s agent serves and returns them. Registry-side metadata **only** — does not deploy and does not touch the ledger. Uses explicit `capabilities` if given, else `self._capability_fn(manifest)`. Keys by `str(manifest.name)`.

- **Raises** `ValueError` — if the manifest has no name (`"register_agent requires a manifest with a name"`).

#### `AgentRegistry.capabilities_for`

```python
def capabilities_for(self, name: str) -> Set[str]
```

The registered capability labels for `name` — a fresh copy (`set(...)`), safe to mutate; empty set if unregistered.

#### `AgentRegistry.versions`

```python
def versions(self, name: str) -> List[AgentVersion]
```

All standing versions of `name`, oldest first, derived from the ledger. Each distinct fingerprint is one version; a later row for a seen fingerprint refreshes live details (`arn`/`deployed_at`) without allocating a new version number. Pure READ of `ledger.rows()`.

> **Gotcha.** `versions()` re-stamps every returned `AgentVersion` with the *currently-registered* capability frozenset for that name — capability changes are not versioned; they apply retroactively to every version view.

#### `AgentRegistry.current`

```python
def current(self, name: str) -> Optional[AgentVersion]
```

The current (newest) standing version of `name`, or `None`. Newest-row-wins, mirroring `DeployLedger.lookup`. Returns `versions(name)[-1]` if any.

#### `AgentRegistry.names`

```python
def names(self) -> List[str]
```

All agent names present in the ledger, in first-seen order (deduplicated scan of `ledger.rows()`).

#### `AgentRegistry.process_table`

```python
def process_table(self) -> Dict[str, AgentVersion]
```

The standing process table: `name → current version` for every agent. A pure projection over the ledger — the scheduler matches against this and the cockpit monitors it. Omits names with no current version. Read-only: nothing is scheduled or seeded.

#### `AgentRegistry.match_task`

```python
def match_task(self, task: str) -> Optional[AgentVersion]
```

The current version of a standing agent that serves `task`, or `None`. Considers only current versions (the process table) — an older version is never dispatched to. Returns the FIRST current version whose `serves(task)` is `True` (process-table dict iteration order).

> **Gotcha.** With multiple agents serving the same task, `match_task` returns the first in dict iteration order — use [`match_all`](#agentregistrymatch_all) for determinism.

#### `AgentRegistry.match_all`

```python
def match_all(self, task: str) -> List[AgentVersion]
```

Every current-version agent that serves `task` (a full process-table scan).

#### `AgentRegistry.ensure_task`

```python
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
) -> AgentVersion
```

Returns the current version serving `task`, spawning one on demand. If a current version already serves `task` it is returned unchanged (no deploy). Otherwise it **spawns** via the shipped `provision_agent` actuator (never a new compiler path), then re-reads the ledger for the freshly-standing current version. Before provisioning, capabilities are taught: from `manifest`, else explicit `capabilities`, else a minimal capability `= {task}` on the entry's name so the spawned agent matches. `provision_agent` owns the ledger append (the registry stays read-only).

- **Raises** [`RegistryError`](#registryerror) — if the spawn does not yield a current version that serves `task`.

```python
av = registry.ensure_task("triage", entry=entry, clients=clients, manifest=manifest)
```

#### `AgentRegistry.fork`

```python
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
) -> AgentVersion
```

Stands up a NEW version of an existing agent `name` on demand — a same-name deploy with a changed hosting fingerprint. Delegates to `provision_agent` (passing `task=None`), then re-reads the ledger for the new current version. The registry itself never writes the ledger.

- **Raises** [`RegistryError`](#registryerror) — `"fork of <name> did not yield a standing version in the ledger"` if `current(name)` is `None` after provisioning.

### `RegistryError`

```python
class RegistryError(RuntimeError)
```

Raised when a spawn/fork does not resolve to a standing ledger version. Subclass of `RuntimeError`. (Defined *after* `AgentRegistry` in the module but fully importable and referenced by its methods.)

> **Gotcha.** The registry never writes the ledger — `provision_agent` (bound lazily) owns the append. If provisioning does not append a matching row, `ensure_task`/`fork` raise `RegistryError`.

---

## `governor.authoring`

Source: [`../../src/concursus/governor/authoring.py`](../../src/concursus/governor/authoring.py)

Net-new agent-manifest authoring — the DEEPEST form of Create. The [`registry`](#governorregistry) Create path (`ensure_task`/`fork` → `provision_agent` → `CreateAgentRuntime`) can *provision* a manifest that already exists, but it cannot AUTHOR a role that has never existed. Given a capability/task label with no matching manifest, this module authors a valid [`AgentManifest`](build.md) (name, registry stub, contract inputs + output schema, spec) so the role can then be provisioned and staffed. **Opt-in and offline by default**: the LLM is an INJECTED, OPTIONAL seam (`manifest_author_fn`); with none supplied, a deterministic skeleton manifest is authored LLM-free (no boto3, no model imported).

> **Identity guard (INV-1/INV-2/INV-3).** Authoring happens strictly BEFORE `assemble` and yields a plain manifest VALUE; it never touches `Supervisor.run` nor a running frozen plan. A freshly authored agent enters at a LOW create-time trust seed (`L0_SHADOW` by default) — it must EARN autonomy on the [Trust Ladder](#governorscheduler) before it can dispatch a side-effecting task.

These symbols are importable from `concursus.governor.authoring` (not re-exported from the `governor` package root).

| Symbol | Kind | Summary |
|---|---|---|
| [`ManifestAuthorFn`](#manifestauthorfn) | type alias | `Callable[[str, Mapping], AgentManifest \| dict]` — the injected author seam. |
| [`ManifestAuthorError`](#manifestauthorerror) | exception | Task could not be authored into a valid manifest (subclass of `ValueError`). |
| [`author_manifest`](#author_manifest) | function | Author a valid `AgentManifest` for a net-new role serving a task. |
| [`staff_capability_dag`](#staff_capability_dag) | function | Staff a capability `AgentDAG` into an assemblable `{node: AgentManifest}` set (bind or author per node). |
| [`staff_with_rebind`](#staff_with_rebind) | function | Bind each node to a type-ALIGNING agent, re-binding on an alignment failure — the compiler as regulator. |
| [`RebindExhausted`](#rebindexhausted) | exception | No candidate combination aligns within `max_rebinds` (subclass of `ValueError`). |
| [`CandidatesFn`](#candidatesfn) | type alias | `Callable[[str], list]` — the ranked-candidates seam (node → best-first manifests). |

### `ManifestAuthorFn`

```python
ManifestAuthorFn = Callable[[str, Mapping[str, Any]], Any]
```

Type alias for the injected manifest-author seam: `(task, context) -> AgentManifest | dict`. Where an LLM would synthesize a role's prompt/SOPs/tools/schema. NEVER imported or constructed inside the module — it is supplied by the caller.

### `ManifestAuthorError`

```python
class ManifestAuthorError(ValueError)
```

Raised when a task cannot be authored into a valid `AgentManifest` — an empty task label, or an author function that returns something that is neither an `AgentManifest` nor a coercible `from_dict` mapping, or whose output fails `AgentManifest.validate()`. Subclass of `ValueError`.

### `author_manifest`

```python
def author_manifest(
    task: str,
    *,
    inputs: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
    manifest_author_fn: Optional[ManifestAuthorFn] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> AgentManifest
```

Author a valid [`AgentManifest`](build.md) for a net-new role serving `task`.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `task` | `str` | — | The capability/task label the authored role serves. Empty/whitespace raises. |
| `inputs` | `Optional[Mapping]` | `None` | Optional contract inputs schema for the skeleton manifest. |
| `context` | `Optional[Mapping]` | `None` | Extra context passed through to `manifest_author_fn` (ignored by the default skeleton). |
| `manifest_author_fn` | `Optional[ManifestAuthorFn]` | `None` | The injected LLM seam. `None` → deterministic offline skeleton. |
| `trust_seed` | `TrustGrade` | `L0_SHADOW` | The create-time trust seed the authored role enters at. |

- **Returns** an `AgentManifest` that always passes `AgentManifest.validate()`.
- **Raises** [`ManifestAuthorError`](#manifestauthorerror) — empty `task`; or `manifest_author_fn` returns a non-`AgentManifest`/non-mapping value, a mapping that `from_dict` rejects, or a manifest that fails `validate()`.

**Behavior.**

1. Empty/whitespace `task` → `ManifestAuthorError`.
2. No `manifest_author_fn` (DEFAULT) → returns a deterministic skeleton: a valid, provisionable container-hosted HTTP `AgentManifest` with a placeholder `container_uri` `"<to-provision>/<slug>:latest"`, entry `"agents.<slug>:run"`, `capabilities=[task]`, a minimal but non-empty output schema (so the dependency-resolver/alignment gate has a type gate), `side_effecting=False`, at the low `trust_seed`. `<slug>` is a lowercase `[a-z0-9_]` slug of `task`.
3. With `manifest_author_fn` (INJECTED LLM seam) → calls `manifest_author_fn(task, context)`; its output (an `AgentManifest` OR a `from_dict` mapping) is coerced and VALIDATED before return.

A net-new role is thus CREATED (its prompt/tools/schema authored from a capability gap), not merely provisioned from a hand-declared manifest — and it must earn autonomy on the Trust Ladder before it can dispatch a side-effecting task.

```python
from concursus.governor.authoring import author_manifest

# offline default: a deterministic L0_SHADOW skeleton role for a capability gap
manifest = author_manifest("root_cause_analysis")
# manifest.name == "root_cause_analysis"; capabilities == {"root_cause_analysis"}; trust_seed == L0_SHADOW
```

### `staff_capability_dag`

```python
def staff_capability_dag(
    dag: "AgentDAG",
    *,
    bind_fn: Optional[Callable[[str], Optional[str]]] = None,
    manifest_author_fn: Optional[ManifestAuthorFn] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> Dict[str, AgentManifest]
```

Turn an agent-agnostic CAPABILITY [`AgentDAG`](assemble.md) — the nodes + edges but NO manifests and NO `depends_on` wiring produced by `plan_from_goal(..., decompose=True)` — into an ASSEMBLABLE `{node: AgentManifest}` set. The staffing step at the compiler front that un-collapses *binding* from *authoring*: since a raw capability DAG cannot be assembled directly (`assemble` requires a manifest per node and derives wiring from `depends_on`), this synthesizes, per node, both a manifest AND its data-wiring from the DAG edges. **Opt-in / default-off**: the loop only calls it under `decompose=True` (see [`GovernorLoop`](#governorloop)); nothing runs this on the legacy single-shot path.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `dag` | `AgentDAG` | — | The agent-agnostic capability DAG (task nodes + edges, no manifests, no `depends_on`). |
| `bind_fn` | `Optional[Callable[[str], Optional[str]]]` | `None` | Per-node binder: `node -> standing agent name` to bind, or `None` to author a skeleton. Default `None` authors every node. |
| `manifest_author_fn` | `Optional[ManifestAuthorFn]` | `None` | The injected LLM author seam, threaded into [`author_manifest`](#author_manifest) for unbound nodes. `None` → deterministic offline skeleton. |
| `trust_seed` | `TrustGrade` | `L0_SHADOW` | The create-time trust seed each staffed role enters at. |

- **Returns** a `{node: AgentManifest}` map ready for `OrchestrationAssembler.assemble(dag, …)`.

**Behavior.** Per node it synthesizes a manifest keyed by the node id — bound to a standing agent via `bind_fn(node)` (recorded as `registry["bound_agent"]` for provenance) when that returns a name, else an authored `L0_SHADOW` skeleton via [`author_manifest`](#author_manifest) — plus its data-wiring from the DAG edges: one contract input per upstream producer (named after the producer node, `{"type": "string"}`) fed by `"<producer>.result"`, with the matching `depends_on` edge. Keying by the node id keeps the frozen `plan.order` the capability topology. Makes the COLD-START path work end-to-end: `decompose → staff → assemble` freezes a real multi-node plan with ZERO hand-authored manifests. Pure + offline (INV-2): binds/authors VALUES, never dispatches, never mutates a running plan.

```python
from concursus.governor.authoring import staff_capability_dag
from concursus import plan_from_goal

cap_dag = plan_from_goal("resolve ticket 42", decompose=True)   # nodes + edges, no manifests
manifests = staff_capability_dag(cap_dag)                       # bind_fn=None → authors every node
# assemble freezes a real multi-node plan from a zero-bench cold start
```

### `staff_with_rebind`

```python
def staff_with_rebind(
    dag: "AgentDAG",
    candidates_fn: CandidatesFn,
    *,
    assembler: Any = None,
    max_rebinds: int = 8,
) -> Dict[str, AgentManifest]
```

Bind each capability node to a type-ALIGNING agent, RE-BINDING on an alignment failure — the compiler's *regulator* half. Instead of hard-erroring when a bound team fails the deep type gate, it SEARCHES per-node candidate lists for an assignment that assembles: starting from every node's first candidate it strict-assembles, and on an [`AlignmentError`](core.md#alignmenterror) that names an offending node (via its `.node`/`.producer` attributes) it advances the OFFENDING PRODUCER (falling back to the consumer) to its next candidate and retries. A bounded author-time search (INV-2: a pure author-time loop, never a compiler while-loop in the run; `max_rebinds` caps it). This is the "weak regulator → real regulator" fix: reject-and-rebind, not just validate.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `dag` | `AgentDAG` | — | The capability DAG to staff. |
| `candidates_fn` | [`CandidatesFn`](#candidatesfn) | — | `node -> [AgentManifest, ...]` best-first (e.g. the scheduler's trust-ranked candidate set). |
| `assembler` | `Any` | `None` | The `OrchestrationAssembler` to strict-assemble against; `None` → a fresh `OrchestrationAssembler(strict_types=True)`. |
| `max_rebinds` | `int` | `8` | The cap on re-bind advances before giving up. |

- **Returns** the aligning `{node: AgentManifest}` set (assemblable under `strict_types`). Each candidate manifest is re-keyed/renamed to its node id and given the edge-derived wiring (like [`staff_capability_dag`](#staff_capability_dag)), so the frozen `plan.order` stays the capability topology.
- **Raises** [`RebindExhausted`](#rebindexhausted) — if no candidate combination aligns within `max_rebinds`.

Author-time + offline; never dispatches, never mutates a running plan (INV-1/INV-3). Opt-in — a compiler-front helper the caller wires explicitly.

```python
from concursus.governor.authoring import staff_with_rebind

# candidates_fn(node) -> best-first manifests; re-binds the offending node on an AlignmentError
manifests = staff_with_rebind(cap_dag, candidates_fn, max_rebinds=8)
```

### `RebindExhausted`

```python
class RebindExhausted(ValueError)
```

Raised by [`staff_with_rebind`](#staff_with_rebind) when a bounded re-bind cannot find a type-aligning agent assignment within `max_rebinds`. Subclass of `ValueError`.

### `CandidatesFn`

```python
CandidatesFn = Callable[[str], list]
```

Type alias for the ranked-candidates seam consumed by [`staff_with_rebind`](#staff_with_rebind): `node -> [AgentManifest, ...]` best-first. The re-binder tries the first candidate, and on a type-alignment failure at that node advances to the next. Typically wraps the scheduler's trust-ranked candidate set (e.g. [`decide_ranked`](#trustladderschedulerdecide_ranked)).

---

## `governor.scheduler`

Source: [`../../src/concursus/governor/scheduler.py`](../../src/concursus/governor/scheduler.py)

The per-decision Trust-Ladder scheduler — the governor **router**'s matcher. At dispatch it matches each ready frontier step to a standing agent (via the read-only [`AgentRegistry`](#agentregistry)), reads that agent's *earned* trust off a GOV-side ladder, and decides per-decision whether the step is cleared to `DISPATCH` or must be `ESCALATE`d (or is `UNMATCHED`). This is **opt-in**: pass a `TrustLadderScheduler` to `GovernorLoop(scheduler=...)`; with no scheduler the router is a byte-for-byte pass-through.

> **INV-3/INV-4/INV-5.** `propose_frontier` returns a `FrontierProposal` VALUE — it never mutates a frozen plan, never calls `assemble`/`recompile`, never reaches into a running Supervisor. `update_trust` is the ONLY place trust is (re)earned, and it lives GOV-side only; the compiler never runs it. The create-time [`evaluate_deploy_gate`](build.md) is read at most ONCE per agent (in `seed_grade`), never per invocation.

Grades come from the [`TrustGrade`](build.md) `IntEnum` (`L0_SHADOW < L1_CANARY < L2_GUARDED < L3_AUTONOMOUS`).

| Symbol | Kind | Summary |
|---|---|---|
| [`DISPATCH`](#dispatch--escalate--unmatched) | constant | `"dispatch"` — cleared; earned trust meets the bar. |
| [`ESCALATE`](#dispatch--escalate--unmatched) | constant | `"escalate"` — below bar (or held for approval); not dispatched this round. |
| [`UNMATCHED`](#dispatch--escalate--unmatched) | constant | `"unmatched"` — no standing agent serves the step. |
| [`SchedulerError`](#schedulererror) | exception | Invalid scheduler config/decision (subclass of `RuntimeError`). |
| [`ScheduleDecision`](#scheduledecision) | dataclass | One per-decision outcome (a frozen VALUE). |
| [`ScheduleDecision.to_dict`](#scheduledecisionto_dict) | method | Plain-dict form (grades → `.name`). |
| [`FrontierProposal`](#frontierproposal) | dataclass | The router's frontier partition (a pure VALUE). |
| [`FrontierProposal.to_dict`](#frontierproposalto_dict) | method | Plain-dict form for the next recompile. |
| [`Binding`](#binding) | dataclass | One resolved task→agent binding chosen from the FULL candidate set (a frozen VALUE). |
| [`Binding.to_dict`](#bindingto_dict) | method | Plain-dict form (grades → `.name`). |
| [`TrustLadderScheduler`](#trustladderscheduler) | class | The per-decision matcher/binder; holds the earned ladder. |
| [`TrustLadderScheduler.seed_grade`](#trustladderschedulerseed_grade) | method | Lazily seed the earned grade from the create-time gate. |
| [`TrustLadderScheduler.earned_grade`](#trustladderschedulerearned_grade) | method | Re-fetch the authoritative earned grade. |
| [`TrustLadderScheduler.decide`](#trustladderschedulerdecide) | method | GATE ONE ready step (first-match). |
| [`TrustLadderScheduler.propose_frontier`](#trustladderschedulerpropose_frontier) | method | Partition the ready frontier into a `FrontierProposal`. |
| [`TrustLadderScheduler.decide_ranked`](#trustladderschedulerdecide_ranked) | method | BIND ONE step from the full candidate set by trust priority. |
| [`TrustLadderScheduler.propose_bindings`](#trustladderschedulerpropose_bindings) | method | Bind every ready frontier node → a `Binding`. |
| [`TrustLadderScheduler.update_trust`](#trustladderschedulerupdate_trust) | method | Re-earn trust GOV-side after an episode. |
| [`make_trust_strictness`](#make_trust_strictness) | function | The adaptive strictness dial: `node -> bool` = strict for below-bar (WEAK) agents, lean for proven. |
| [`Tier`](#tier) | enum | The payload-detail tier (`HIGH`/`GUARDED`/`LOW`/`PROGRAMMATIC`); detail ∝ 1/trust. |
| [`make_payload_tier`](#make_payload_tier) | function | The 4-value generalization of `make_trust_strictness`: `node -> Tier`. |
| [`project_context`](#project_context) | function | Pure monotone-lattice projection of a full context down to a `Tier`. |
| [`manifest_is_programmatic`](#manifest_is_programmatic) | function | `node -> bool` off the `registry.programmatic` manifest flag. |
| [`compute_schedule`](#compute_schedule) | function | The pure `state -> Decision` scheduling core (opt-in, orthogonal). |
| [`Decision`](#decision--declinednode) / [`DeclinedNode`](#decision--declinednode) | dataclasses | The pure decision VALUE + one declined node with a first-class reason. |

### `DISPATCH` / `ESCALATE` / `UNMATCHED`

```python
DISPATCH  = "dispatch"    # cleared: earned trust meets the bar — propose to compile next
ESCALATE  = "escalate"    # below bar (or held for approval) — NOT dispatched this round
UNMATCHED = "unmatched"   # no standing agent serves the step — needs provision
```

The three `ScheduleDecision.action` values. `ESCALATE` is also used when `require_approval` is set and the matched agent is side-effecting. `UNMATCHED` is distinct from `ESCALATE` — an unmatched node blocks the frontier forever.

### `SchedulerError`

```python
class SchedulerError(RuntimeError)
```

Raised on an invalid Trust-Ladder scheduler configuration or decision (e.g. a schedule-state node missing its `node` label). Subclass of `RuntimeError`.

### `ScheduleDecision`

```python
@dataclass(frozen=True)
class ScheduleDecision:
    node: str
    action: str
    agent: Optional[str] = None
    version: Optional[int] = None
    grade: Optional[TrustGrade] = None
    bar: Optional[TrustGrade] = None
    escalated_to: Optional[TrustGrade] = None
    reason: str = ""
```

One per-decision outcome (a frozen VALUE): how a single ready step was resolved this round. `action` is one of [`DISPATCH`/`ESCALATE`/`UNMATCHED`](#dispatch--escalate--unmatched); `grade` = the matched agent's authoritative earned trust; `bar` = required autonomy floor; `escalated_to` = the grade a below-bar decision escalates to.

#### `ScheduleDecision.to_dict`

```python
def to_dict(self) -> Dict[str, Any]
```

Plain-dict form; `TrustGrade` fields are serialized to their `.name` (or `None`). Keys: `node`, `action`, `agent`, `version`, `grade`, `bar`, `escalated_to`, `reason`.

### `FrontierProposal`

```python
@dataclass(frozen=True)
class FrontierProposal:
    compile_next: Tuple[str, ...] = ()
    escalated: Tuple[str, ...] = ()
    unmatched: Tuple[str, ...] = ()
    decisions: Tuple[ScheduleDecision, ...] = ()
```

A frontier proposal (a pure VALUE the router hands forward, INPUT to the next recompile): which ready nodes are cleared to compile next vs held. `compile_next` = cleared to dispatch, `escalated` = below-bar held, `unmatched` = no standing agent, `decisions` = the per-node [`ScheduleDecision`](#scheduledecision)s. Never mutates a plan; immutable tuples.

#### `FrontierProposal.to_dict`

```python
def to_dict(self) -> Dict[str, Any]
```

Plain dict suitable to hand to the next recompile round: lists for `compile_next`/`escalated`/`unmatched` and decision dicts (via `ScheduleDecision.to_dict`).

### `Binding`

```python
@dataclass(frozen=True)
class Binding:
    node: str
    action: str
    agent: Optional[str] = None
    version: Optional[int] = None
    grade: Optional[TrustGrade] = None
    bar: Optional[TrustGrade] = None
    load: Optional[int] = None
    candidates: Tuple[str, ...] = ()
    reason: str = ""
```

One resolved task→agent binding (a frozen VALUE) — the scheduler's *binder* output, distinct from a [`ScheduleDecision`](#scheduledecision). Where a `ScheduleDecision` is the GATE outcome of a *first-match* agent, a `Binding` is the chosen `(agent, version)` for a task, selected from the FULL capable-candidate set by trust PRIORITY then availability. `action` is one of [`DISPATCH`/`ESCALATE`/`UNMATCHED`](#dispatch--escalate--unmatched); `grade` = the chosen agent's authoritative earned trust; `bar` = the autonomy floor it cleared; `load` = the availability signal (`load_fn`) for the chosen agent (`None` when no `load_fn`); `candidates` = the tuple of ALL capable agent names considered (via `registry.match_all`); `reason` = a human-readable rationale. It is a pure VALUE — the input a post-bind compile consumes; the scheduler still never mutates a frozen plan. Unlike `ScheduleDecision`/`FrontierProposal`, `Binding` is **not** re-exported from the `concursus.governor` package root — import it from its submodule: `from concursus.governor.scheduler import Binding`.

#### `Binding.to_dict`

```python
def to_dict(self) -> Dict[str, Any]
```

Plain-dict form; `TrustGrade` fields (`grade`, `bar`) are serialized to their `.name` (or `None`) and `candidates` to a list. Keys: `node`, `action`, `agent`, `version`, `grade`, `bar`, `load`, `candidates`, `reason`.

### `TrustLadderScheduler`

```python
class TrustLadderScheduler:
    def __init__(
        self,
        registry: AgentRegistry,
        *,
        manifests: Optional[Mapping[str, Any]] = None,
        min_autonomy: TrustGrade = TrustGrade.L1_CANARY,
        escalation_grade: TrustGrade = TrustGrade.L3_AUTONOMOUS,
        require_approval: bool = False,
        load_fn: Optional[Any] = None,
    ) -> None
```

The per-decision Trust-Ladder scheduler. Holds the GOV-side *earned* trust ladder (the only mutable trust store; `build/trust` stays the create-time seed). Each decision re-reads the registry process table and the earned ladder; nothing structural is cached. `min_autonomy`/`escalation_grade` are parsed via `TrustGrade.parse`. `manifests` maps agent name → manifest (for `trust_seed` and `side_effecting`). `require_approval=True` forces `ESCALATE` for any side-effecting matched agent regardless of grade. `load_fn` (default `None`) is an OPTIONAL availability/load signal `load_fn(agent_name) -> int` (an in-flight/queued count) used only by the [binder path](#trustladderschedulerdecide_ranked) as a tie-break between equal-trust candidates; with `load_fn=None` ranking is pure trust-priority. It is read-only — a bad `load_fn` that raises is swallowed and treated as `0`, so it can never break scheduling.

```python
from concursus.governor import TrustLadderScheduler
from concursus.build.trust import TrustGrade

sched = TrustLadderScheduler(registry, manifests=manifests, min_autonomy=TrustGrade.L1_CANARY)
```

#### `TrustLadderScheduler.seed_grade`

```python
def seed_grade(self, name: str) -> TrustGrade
```

The authoritative earned grade for `name`, seeded lazily from the create-time gate. If `name` is already in the earned ladder, returns it. Else reads `manifest.trust_seed` (default `L0_SHADOW`), calls `evaluate_deploy_gate(side_effecting, trust_seed=seed, min_autonomy=None, require_approval=False)`; grade = `seed` if `decision.mode == "live"` else `L0_SHADOW`; caches it. The create-time gate is consulted at most ONCE per agent — never per invocation thereafter.

#### `TrustLadderScheduler.earned_grade`

```python
def earned_grade(self, name: str) -> TrustGrade
```

Re-fetch the authoritative earned grade for `name` (never cached structurally). Seeds lazily on first read (via [`seed_grade`](#trustladderschedulerseed_grade)), then returns the GOV-side ladder value. An `update_trust` between rounds is reflected immediately.

#### `TrustLadderScheduler.decide`

```python
def decide(self, node: str) -> ScheduleDecision
```

Resolve ONE ready step: match `node` (a task label) to the current version of a standing agent via the registry, read earned trust, then decide. Order of checks:

1. No match → `UNMATCHED`.
2. `require_approval` AND matched agent is side-effecting → `ESCALATE` (held for approval).
3. earned `grade < bar` → `ESCALATE`.
4. else → `DISPATCH`.

The `bar` is `L0_SHADOW` for non-side-effecting agents (always cleared) and `min_autonomy` for side-effecting agents. Reads the create-time gate not at all.

```python
decision = sched.decide("triage")   # ScheduleDecision(action=DISPATCH|ESCALATE|UNMATCHED, ...)
```

#### `TrustLadderScheduler.propose_frontier`

```python
def propose_frontier(
    self,
    plan: Any,
    *,
    completed: Iterable[str],
    ready: Optional[Iterable[str]] = None,
) -> FrontierProposal
```

PROPOSE which ready nodes are cleared to compile next — a VALUE, never a plan mutation. If `ready` is given, the frontier is `ready` minus `completed`; else it is `plan.order` minus `completed`. Decides each node via [`decide`](#trustladderschedulerdecide) and partitions into `compile_next`/`escalated`/`unmatched` with the per-node decisions. Never writes `plan.order`; the result is INPUT to the next recompile — the scheduler never calls `assemble`/`recompile` itself.

```python
proposal = sched.propose_frontier(plan, completed=store.completed())
held = set(proposal.escalated) | set(proposal.unmatched)   # nodes to withhold this round
```

> **Gate vs binder.** [`decide`](#trustladderschedulerdecide)/[`propose_frontier`](#trustladderschedulerpropose_frontier) are the first-match trust GATE — they take the ONE agent `registry.match_task` returns and gate it dispatch/escalate/unmatched, unchanged. [`decide_ranked`](#trustladderschedulerdecide_ranked)/[`propose_bindings`](#trustladderschedulerpropose_bindings) below are the additive BINDER path — they pull the FULL candidate set via `registry.match_all` and SELECT the best agent by trust priority then availability, returning a [`Binding`](#binding). Both are pure VALUEs; neither mutates a frozen plan or calls `assemble`/`recompile`.

#### `TrustLadderScheduler.decide_ranked`

```python
def decide_ranked(self, node: str) -> Binding
```

BIND ONE ready step by candidate-set × trust-PRIORITY × availability — the binder analogue of [`decide`](#trustladderschedulerdecide). Unlike `decide` (first-match then gate), this pulls the FULL candidate set via `registry.match_all(node)` (not `match_task`), keeps the candidates that clear their own bar, ranks them best-trust-first, and returns a [`Binding`](#binding). A pure VALUE — it reads the read-only registry process table + the earned ladder and mutates nothing (no plan, no `assemble`/`recompile`).

**Behavior.**

1. No capable candidate (`match_all` empty) → `UNMATCHED`.
2. Each candidate is judged against its OWN bar (`L0_SHADOW` for non-side-effecting, `min_autonomy` for side-effecting); with `require_approval` set, side-effecting candidates are dropped from the cleared set.
3. No candidate clears its bar → `ESCALATE` (the `Binding` reports the strictest bar over the candidate set and the full `candidates` tuple).
4. Otherwise → `DISPATCH` the best cleared candidate. Ranking is highest earned trust first, tie-broken by least `load_fn(name)` (or `0` when no `load_fn`), then agent name — fully deterministic.

```python
binding = sched.decide_ranked("triage")
# Binding(action=DISPATCH, agent="triager", version=3, grade=..., candidates=("triager", "analyst"), ...)
```

#### `TrustLadderScheduler.propose_bindings`

```python
def propose_bindings(
    self,
    plan: Any,
    *,
    completed: Iterable[str],
    ready: Optional[Iterable[str]] = None,
) -> Dict[str, Binding]
```

Bind every ready frontier node → a [`Binding`](#binding) — the binder analogue of [`propose_frontier`](#trustladderschedulerpropose_frontier), a VALUE and never a plan mutation. If `ready` is given, the frontier is `ready` minus `completed`; else it is `plan.order` minus `completed`. Reads `plan.order` (never writes it), skips `completed` nodes, and returns `{node: Binding}` over the ready frontier via [`decide_ranked`](#trustladderschedulerdecide_ranked). The result is INPUT to a post-bind recompile; the scheduler never calls `assemble`/`recompile` itself. `UNMATCHED` bindings are what an opt-in Create arrow (see [`loop` auto-create](#governorloop)) turns into an on-demand spawn.

```python
bindings = sched.propose_bindings(plan, completed=store.completed())
# {"triage": Binding(action=DISPATCH, ...), "remediate": Binding(action=UNMATCHED, ...)}
```

#### `TrustLadderScheduler.update_trust`

```python
def update_trust(self, name: str, outcome: Any) -> TrustGrade
```

Re-earn trust GOV-side after collect from an episode `outcome`; return the new grade. A clean outcome promotes by one rung (capped at `escalation_grade`); a failing outcome demotes by one rung (floored at `L0_SHADOW`). Writes the new grade to the GOV-side ladder so the next round's `decide` reads it. Cleanliness is read off the outcome: an outcome that is a `Mapping` with `ok is False`, a truthy `error`, or `status == "failed"` is a failure; anything else (including a non-`Mapping` or absent outcome) is clean.

```python
sched.update_trust("triager", {"ok": True})    # promote one rung after a clean episode
```

> **Gotchas.** `update_trust` and `decide`/`earned_grade` key the ladder by **agent name**, not the task-label node id. When `node != agent name`, resolve `node → agent` from the `FrontierProposal` decisions before calling `update_trust`, or the earned grade never moves. A held/shadowed create-time seed floors the earned grade at `L0_SHADOW` even if the manifest declared a higher `trust_seed`. `require_approval=True` escalates *every* side-effecting matched agent regardless of how high its earned grade is; non-side-effecting agents always clear their bar. `propose_frontier` with `ready=None` uses `plan.order` as the whole frontier (minus completed).

### `make_trust_strictness`

```python
def make_trust_strictness(
    scheduler: "TrustLadderScheduler",
    *,
    strict_below: TrustGrade = TrustGrade.L2_GUARDED,
) -> Callable[[str], bool]
```

The adaptive STRICTNESS DIAL: builds a `node -> bool` predicate that returns `True` (apply the strict deep contract) for a node whose serving agent's EARNED trust is BELOW `strict_below` — i.e. WEAK / unproven agents (default: below `L2_GUARDED`, so L0/L1) — and `False` (run the lean path) for STRONG / proven agents (`>= strict_below`). Realizes "strictness ∝ 1/strength, read off the SAME Trust Ladder that governs autonomy." Author/compile-time only, GOV-side. **Opt-in / default-off**: nothing calls it unless you wire the predicate into the compiler and/or supervisor.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `scheduler` | `TrustLadderScheduler` | — | The scheduler holding the earned ladder; grade is read live via `scheduler.earned_grade(node)`. |
| `strict_below` | `TrustGrade` | `L2_GUARDED` | The trust bar — a node whose earned grade is below this gets the strict contract. |

- **Returns** a `Callable[[str], bool]` node-predicate. An UNKNOWN / never-seeded node (no evidence yet), or a node whose grade cannot be resolved (the lookup raises), is treated as WEAK → returns `True` (the conservative default: an unproven role earns the strict contract until it proves otherwise).

Wire it as the `strict_fn` of the compiler and/or the `acceptance_fn` of the supervisor, so the deep type-align / single-writer / output-QA gates NARROW to the weak nodes and proven agents keep the lean path:

```python
from concursus.governor import make_trust_strictness, TrustLadderScheduler

sched = TrustLadderScheduler(registry, manifests=manifests)
is_strict = make_trust_strictness(sched)                       # default bar = L2_GUARDED
# compile-time: strict deep gates only for below-bar (weak) nodes
assembler = OrchestrationAssembler(strict_fn=is_strict)
# post-run QA: narrow the acceptance gate to weak nodes too
supervisor = Supervisor(..., check_acceptance=True, acceptance_fn=is_strict)
```

> Exported from `concursus.governor` (but NOT from the package root, unlike most `governor` symbols): `from concursus.governor import make_trust_strictness`.

### `Tier`

```python
class Tier(Enum):
    HIGH         = "high"          # proven agent — least payload detail
    GUARDED      = "guarded"       # mid-trust — guardrails only
    LOW          = "low"           # weak / unproven — full payload detail
    PROGRAMMATIC = "programmatic"  # deterministic tool-caller — tool_calls only (orthogonal to trust)
```

The payload-detail tier. Where [`make_trust_strictness`](#make_trust_strictness) is a binary strict/lean dial, `Tier` is its 4-value generalization: **payload detail is proportional to `1/trust`** — a proven agent (`HIGH`) gets the leanest payload, a weak agent (`LOW`) gets the fullest. `PROGRAMMATIC` is an orthogonal fourth value for deterministic tool-callers (a fixed `tool_calls` payload, independent of earned trust). Consumed by [`project_context`](#project_context) to pick which context keys survive. Author/compile-time only.

### `make_payload_tier`

```python
def make_payload_tier(
    scheduler: "TrustLadderScheduler",
    is_programmatic: Optional[Callable[[str], bool]] = None,
    *,
    strict_below: TrustGrade = TrustGrade.L2_GUARDED,
) -> Callable[[str], Tier]
```

The 4-value generalization of [`make_trust_strictness`](#make_trust_strictness): builds a `node -> Tier` selector off the SAME earned Trust Ladder. It maps earned trust to a tier — strictly above `strict_below` → `HIGH`, exactly at the bar → `GUARDED`, and below the bar (or unknown) → `LOW` — so a below-`strict_below` (weak) agent still gets the fullest payload, consistent with the binary dial. An `is_programmatic(node)` that returns truthy overrides the trust mapping and returns `PROGRAMMATIC` (the tool-caller value is orthogonal to trust). Author/compile-time only, GOV-side. **Opt-in / default-off**: nothing calls it unless you wire the selector into the compiler/supervisor.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `scheduler` | `TrustLadderScheduler` | — | The scheduler holding the earned ladder; grade is read live via `scheduler.earned_grade(node)`. |
| `is_programmatic` | `Optional[Callable[[str], bool]]` | `None` | Per-node override: truthy → `PROGRAMMATIC` regardless of trust. `None` → trust mapping only. Wire [`manifest_is_programmatic`](#manifest_is_programmatic) here. |
| `strict_below` | `TrustGrade` | `L2_GUARDED` | The trust bar mirrored from `make_trust_strictness`: below it lands in `LOW`. |

- **Returns** a `Callable[[str], Tier]`. An unknown / never-seeded node (or one whose grade cannot be resolved) is treated as WEAK → `LOW` (the conservative default: fullest payload until a role proves itself); a bad `is_programmatic` that raises is swallowed and falls through to the trust mapping.

```python
from concursus.governor import make_payload_tier, manifest_is_programmatic, TrustLadderScheduler

sched = TrustLadderScheduler(registry, manifests=manifests)
tier_of = make_payload_tier(sched, manifest_is_programmatic(manifests))   # node -> Tier
# wire tier_of as OrchestrationAssembler(payload_tier_fn=...) and/or Supervisor(payload_tier_fn=...)
```

### `project_context`

```python
def project_context(full_context: Mapping[str, Any], tier: Tier) -> Dict[str, Any]
```

The pure monotone-lattice projection: given a full free-form context (the manifest's `contract.context` coaching dimension — `{sop?, tools_available?, guardrails?, examples?, tool_calls?}`) and a [`Tier`](#tier), returns the subset of keys that survive at that tier. The lattice is monotone in detail — `LOW` keeps ALL keys, `GUARDED` keeps only `{guardrails}`, `HIGH` keeps `{}` (empty), and `PROGRAMMATIC` keeps only `{tool_calls}`. Absent/empty context or an unknown tier returns `{}`. Pure function of `(full_context, tier)`; no scheduler, no I/O. This is the coaching dimension — the inputs/outputs + acceptance dimension lives in `contract` itself and is NEVER tiered.

```python
from concursus.governor import project_context, Tier

project_context({"sop": ..., "guardrails": ..., "tool_calls": ...}, Tier.GUARDED)
# {"guardrails": ...}      -- HIGH -> {} ; LOW -> all keys ; PROGRAMMATIC -> {"tool_calls": ...}
```

### `manifest_is_programmatic`

```python
def manifest_is_programmatic(manifests: Mapping[str, Any]) -> Callable[[str], bool]
```

Builds the `node -> bool` programmatic predicate off the manifests' `registry.programmatic` flag. Reads each agent's `registry["programmatic"]` flag; the returned predicate is the `is_programmatic` selector [`make_payload_tier`](#make_payload_tier) consumes to route deterministic tool-callers to [`Tier.PROGRAMMATIC`](#tier). A node with no matching manifest (or a falsy flag) → `False` (trust mapping applies). Author/compile-time only.

```python
from concursus.governor import manifest_is_programmatic

is_prog = manifest_is_programmatic(manifests)     # node -> bool over registry.programmatic
```

> Exported from `concursus.governor` (`from concursus.governor import Tier, make_payload_tier, project_context, manifest_is_programmatic`). Like `make_trust_strictness`, these are opt-in author/compile-time seams: the payload contract they feed is [`assemble`](assemble.md)'s `payload_tier_fn` and [`Supervisor`](execute.md)'s `payload_tier_fn`; the default plan (neither wired) is byte-for-byte unchanged.

### `compute_schedule`

```python
def compute_schedule(state: Any) -> Decision
```

A TOTAL, DETERMINISTIC pure function `state -> Decision` with NO I/O — it never reads the registry, never reads the GOV-side trust ladder, never touches a plan. Every gate result the frontier depends on (are deps complete? does earned trust clear the bar? does the round budget admit the node?) is precomputed by the CALLER and handed in via `state`; this core only PARTITIONS the resolved frontier into dispatch vs first-class declines. That keeps "which nodes run" a pure value transform, separate from the impure matching that [`decide`](#trustladderschedulerdecide)/[`propose_frontier`](#trustladderschedulerpropose_frontier) perform against the live registry (whose `dispatch`/`escalate`/`unmatched` taxonomy is deliberately UNCHANGED — this core is an orthogonal, opt-in addition, not a replacement, so the default path is byte-for-byte the same).

`state` is a mapping OR a duck-typed object exposing `nodes` — an ordered iterable of node descriptors; each descriptor carries `node` (str, REQUIRED), `deps_met` (bool, default `True`), `trust_ok` (bool, default `True`), `budget_ok` (bool, default `True`), and optional `detail`. A node is DISPATCHed iff all three gates pass; otherwise it is DECLINED with exactly ONE structured reason, chosen by precedence `deps_unmet > trust_gate_failed > budget_exhausted` (the earliest failing gate wins). Input order is preserved. A missing/empty `nodes` yields an empty [`Decision`](#decision--declinednode). The reasons are the module constants `DECLINE_DEPS_UNMET` / `DECLINE_TRUST_GATE_FAILED` / `DECLINE_BUDGET_EXHAUSTED`.

- **Raises** [`SchedulerError`](#schedulererror) — a schedule-state node is missing its `node` label.

### `Decision` / `DeclinedNode`

```python
@dataclass(frozen=True)
class DeclinedNode:
    node: str
    reason: str        # always exactly one of the DECLINE_* constants
    detail: str = ""
    def to_dict(self) -> Dict[str, Any]

@dataclass(frozen=True)
class Decision:
    dispatch: Tuple[str, ...] = ()
    declined: Tuple[DeclinedNode, ...] = ()
    def to_dict(self) -> Dict[str, Any]
    def declined_by(self, reason: str) -> Tuple[str, ...]
```

The output of [`compute_schedule`](#compute_schedule): `dispatch` is the ordered tuple of node labels cleared to run this round; `declined` is the ordered tuple of `DeclinedNode` VALUEs, each carrying exactly one first-class structured `reason` a caller can branch on (`detail` is optional human-readable elaboration and is never load-bearing). `to_dict()` yields plain-dict forms suitable to log to the append-only StateStore; `declined_by(reason)` returns the node labels declined for one specific structured reason (order-preserving).

---

## `governor.loop`

Source: [`../../src/concursus/governor/loop.py`](../../src/concursus/governor/loop.py)

The governor's fixed cyclic control loop — the OUTER driver around the compiler. `GovernorLoop` forms a fresh frozen plan each round (`planner`), runs one static Supervisor episode (`run_episode`), folds outputs into the append-only log (`collect`), then decides bounded whether to replan or synthesize. The topology is FIXED and compiled once; it runs on an optional LangGraph backend or a pure-Python fallback.

> **Fixed topology.** `planner → router → run_episode → collect → route_after_collect → {planner | synthesize} → END`. All dynamism lives in `GovernorState` + the append-only log; the topology never changes. Termination is BOUNDED four ways so the loop MUST terminate: frontier-exhaustion, the `no_progress_n` stall bound, the `max_rounds` budget, and a hard structural `step_cap` analogue.

| Symbol | Kind | Summary |
|---|---|---|
| [`GOV_NODES`](#gov_nodes--supervisorfactory--scope_sep) | constant | The fixed governor cycle nodes. |
| [`SupervisorFactory`](#gov_nodes--supervisorfactory--scope_sep) | type alias | The supervisor-construction seam. |
| [`SCOPE_SEP`](#gov_nodes--supervisorfactory--scope_sep) | constant | The `trail_id` scope separator (`"."`). |
| [`GOV_EVENT_KINDS`](#gov_event_kinds) | constant | The closed set of episode-boundary event kinds the opt-in `EventSink` emits. |
| [`CheckpointStore`](#checkpointstore) | protocol | The outer-altitude resume seam. |
| [`InProcessCheckpointStore`](#inprocesscheckpointstore) | class | Zero-dependency in-process `CheckpointStore`. |
| [`EventSink`](#eventsink) | protocol | The opt-in episode-boundary observability seam (`emit`). |
| [`NullEventSink`](#nulleventsink) | class | The default no-op `EventSink` — drops every event. |
| [`FanOutEventSink`](#fanouteventsink) | class | *(opt-in)* A composite `EventSink` that fans one event out to several child sinks (the single `event_sink` slot). |
| [`GovernorLoopError`](#governorlooperror) | exception | Invalid loop config / unknown backend. |
| [`GovernorResult`](#governorresult) | dataclass | The outcome of a bounded `GovernorLoop.run`. |
| [`GovernorLoop`](#governorloop) | class | The fixed cyclic outer driver. |
| [`GovernorLoop.run`](#governorlooprun) | method | Drive the bounded cycle to termination. |
| [`GovernorLoop.cockpit`](#governorloopcockpit) | method | A read-only `DirectorCockpit` over the current run. |
| [`GovernorLoop.programs_index`](#governorloopprograms_index) | method | The PROGRAM-grain projection over the run's vault. |
| [`GovernorLoop.leverage_view`](#governorloopleverage_view) | method | The 1:N leverage view over the run's vault. |

### `GOV_NODES` / `SupervisorFactory` / `SCOPE_SEP`

```python
GOV_NODES = ("planner", "router", "run_episode", "collect")
SupervisorFactory = Callable[..., object]
SCOPE_SEP = "."
```

- `GOV_NODES` — the fixed governor cycle nodes (the linear chain); `synthesize` is the terminal node reached from the routing edge after `collect`. Used by both backends and by the internal `step_cap`.
- `SupervisorFactory` — the supervisor-construction seam: build a runnable episode supervisor over one frozen plan. Also re-used by [`ktlo`](#governorktlo).
- `SCOPE_SEP` — the `trail_id` scope-address separator, duplicated as a literal (mirrors [`scope.SCOPE_SEP`](#scope_sep)) so the read-only accessors need no module-top scope import. The default `sep` for the `programs_index`/`leverage_view` pass-throughs.

### `GOV_EVENT_KINDS`

```python
GOV_EVENT_KINDS = frozenset((
    RunEventKind.EPISODE_START.value,   # "episode_start"
    RunEventKind.EPISODE_END.value,     # "episode_end"
    RunEventKind.DECISION.value,        # "decision"
))
```

The CLOSED set of run-event kinds the loop's opt-in [`EventSink`](#eventsink) emits at episode boundaries — `episode_start` (before a Supervisor episode is dispatched), `episode_end` (after `collect` folds the episode's outputs into the log), and `decision` (the bounded routing verdict after `collect`). It is a subset of the single [`RunEventKind`](state.md) vocabulary shared with the readers: the build-time drift guard `check_run_event_alignment` (exercised in `tests/test_run_event_contract.py`) asserts every kind here is a member of `RUN_EVENT_KINDS`, so an emitter/reader mismatch fails at test/build time rather than silently at runtime. Imported from the submodule directly (`from concursus.governor.loop import GOV_EVENT_KINDS`) — not in the `governor` re-export set. The membership check is asserted only on the sink-wired path, so the default (no-sink) loop is byte-for-byte unchanged.

### `CheckpointStore`

```python
class CheckpointStore(Protocol):
    def save(self, run_id: str, checkpoint: Dict[str, Any]) -> None: ...
    def load(self, run_id: str) -> Optional[Dict[str, Any]]: ...
```

The OUTER-altitude resume seam: persist/load a plain-dict round checkpoint by run id. A checkpoint is a small pure-Python dict — `{plan_version, iteration, no_progress, round, prev_completed, replan_reason}` — a POINTER into the round sequence, NEVER a mutable plan snapshot. On restart the loop re-fetches the frozen plan BY VERSION (by replaying the compiler front against the surviving log). Implementations must be idempotent: `save` overwrites the single latest checkpoint for a `run_id`; `load` returns it or `None` before the first save.

### `InProcessCheckpointStore`

```python
class InProcessCheckpointStore:
    def __init__(self) -> None
    def save(self, run_id: str, checkpoint: Dict[str, Any]) -> None
    def load(self, run_id: str) -> Optional[Dict[str, Any]]
```

Zero-dependency in-process `CheckpointStore` — the offline default. Holds the latest checkpoint dict per `run_id` in a plain dict. Copies on BOTH `save` (`dict(checkpoint)`, keyed by `str(run_id)`) and `load` (returns `dict(ckpt)` or `None`) so a caller can never mutate the stored checkpoint in place (it is a VALUE).

### `EventSink`

```python
@runtime_checkable
class EventSink(Protocol):
    def emit(self, event: RunEvent) -> None: ...
```

The OPT-IN episode-BOUNDARY observability seam. When wired via [`GovernorLoop(event_sink=...)`](#governorloop), the loop hands the sink one small plain-dict [`RunEvent`](state.md) VALUE at each episode boundary — `episode_start` (before a Supervisor episode runs), `episode_end` (after `collect` folds the episode's outputs into the log), and `decision` (the bounded routing verdict after `collect`). Each event's `type` is a member of the closed [`GOV_EVENT_KINDS`](#gov_event_kinds) / [`RunEventKind`](state.md) vocabulary; the event is a frozen typed VALUE (never a live ctx/plan handle), so a sink can never reach inside a running Supervisor or mutate a frozen plan (INV-1/INV-3/INV-5). Any exception raised by `emit` is swallowed by the loop, so a misbehaving sink can never break a live episode or the bound.

**Opt-in / default-OFF.** The default `event_sink=None` is interpreted as no-op — nothing is emitted and no method is called — so the default loop is byte-for-byte unchanged and returns a byte-identical `GovernorResult`. Three concrete sinks ship in this tier: [`NullEventSink`](#nulleventsink) (the canonical explicit no-op) and [`FanOutEventSink`](#fanouteventsink) (compose several into the one slot), plus — from the state tier — [`TransferTriggerSink`](../guides/knowledge-transfer.md) (fire the session-end knowledge transfer at `synthesize`, `from concursus.state.transfer import TransferTriggerSink`). `NullEventSink` and `FanOutEventSink` are re-exported from `concursus.governor`.

### `NullEventSink`

```python
class NullEventSink:
    def emit(self, event: RunEvent) -> None: ...   # returns None
```

The default no-op `EventSink` — drops every event. Passing it is behaviorally identical to leaving `event_sink` unset (`None`): a run with a `NullEventSink` returns a byte-identical [`GovernorResult`](#governorresult), because emitting an event never touches ctx, the frozen plan, or the append-only log.

### `FanOutEventSink`

```python
class FanOutEventSink:
    def __init__(self, sinks): ...
    def emit(self, event) -> None: ...
```

*(opt-in)* A composite `EventSink` that fans one boundary event out to several child sinks — the loop has exactly ONE `event_sink` slot, so observers that must coexist (e.g. an operator sink AND a [`TransferTriggerSink`](../guides/knowledge-transfer.md) firing at `synthesize`) compose here: `event_sink=FanOutEventSink([observer, transfer])`. Each child's `emit` is called in order, INDIVIDUALLY guarded, so one misbehaving child can never starve the others; `None` children are dropped at construction; an empty list is a no-op — behaviorally identical to leaving `event_sink` unset. Observer-only (INV-1/INV-3/INV-5). Re-exported from `concursus.governor`.

### `GovernorLoopError`

```python
class GovernorLoopError(ValueError)
```

Raised on an invalid governor-loop configuration or an unknown backend. Subclass of `ValueError`.

### `GovernorResult`

```python
@dataclass
class GovernorResult:
    rounds: int
    terminated_by: str
    done: bool
    completed: List[str]
    frontier: List[str]
    outputs: Dict[str, dict]
    state: GovernorState
    trace: List[str]
    supervisor_runs: int
    backend: str
    escalated: List[str] = field(default_factory=list)
    unmatched: List[str] = field(default_factory=list)
```

The outcome of a bounded `GovernorLoop.run`.

| Field | Meaning |
|---|---|
| `rounds` | Number of completed episodes (`Supervisor.run` passes). |
| `terminated_by` | One of `frontier_exhaust` \| `no_progress` \| `unmatched_stall` \| `round_cap` \| `step_cap` \| `aborted` \| `paused`. |
| `done` | Whether the plan's frontier was exhausted (all nodes completed). |
| `completed` / `frontier` | Completed node ids (re-derived from the log) / the still-open frontier at termination. |
| `outputs` | The LAST episode's returned outputs. |
| `state` | The persistent [`GovernorState`](#governorstate) (holds the full plan-value sequence). |
| `trace` | The ordered node-visit trace. |
| `supervisor_runs` | How many times a Supervisor was run (one per round; INV-1). |
| `backend` | `"langgraph"` or `"python"`. |
| `escalated` / `unmatched` | The opt-in governance surfaces — always empty on the default (no-scheduler) path. |

> `unmatched_stall` is the specific `no_progress` case where an `UNMATCHED` held node blocked the frontier so it never advanced at all. `aborted`/`paused` are the two [episode-gate](#governorloop) bounded stops (see below).

### `GovernorLoop`

```python
class GovernorLoop:
    def __init__(
        self,
        goal: str,
        manifests: Dict[str, AgentManifest],
        *,
        store: Optional[StateStore] = None,
        checkpointer: Optional[CheckpointStore] = None,
        assembler: Optional[OrchestrationAssembler] = None,
        scheduler: Optional["TrustLadderScheduler"] = None,
        auto_create: bool = False,
        create_fn: Optional[Callable[[str], Any]] = None,
        supervisor_factory: Optional[SupervisorFactory] = None,
        invoke_fn: Optional[InvokeFn] = None,
        arns: Optional[Dict[str, str]] = None,
        plan_model_fn: Optional[PlanModelFn] = None,
        deliberate: bool = False,
        trail_factory: Optional[Callable[[], Any]] = None,
        investigator: Optional[Callable[[Any], Any]] = None,
        deliberate_retriever: Optional[Any] = None,
        deliberate_max_rounds: Optional[int] = None,
        deliberate_depth_cap: Optional[int] = None,
        deliberate_confidence_floor: Optional[float] = None,
        session_id: Optional[str] = None,
        memory_id: Optional[str] = None,
        actor_id: Optional[str] = None,
        memory_client: Any = None,
        max_rounds: int = 8,
        no_progress_n: int = 2,
        max_revisions: int = DEFAULT_MAX_REVISIONS,
        confidence_threshold: float = 0.5,
        backend: str = "auto",
        run_id: str = "governor",
        checkpoint_every: int = 0,
        record_frontier: bool = False,
        decompose: bool = False,
        bind_fn: Optional[Callable[[str], Optional[str]]] = None,
        episode_gate: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
        event_sink: Optional[EventSink] = None,
    ) -> None
```

The fixed cyclic outer driver around the compiler.

**Key parameters.** `goal` is the compiler-front goal for the first round; `manifests` is the agent set. Store precedence: an explicit `store` wins verbatim; else `memory_id` builds the shipped [`MemoryStateStore`](state.md) (requires both `session_id` AND `actor_id`); else the offline `InProcessStateStore`. The opt-in seams default to byte-for-byte legacy behavior: `scheduler=None` → the router is a pure pass-through (no node held, `update_trust` never called); `auto_create=False` → an `UNMATCHED` role stays held (unchanged); `deliberate=False` → single-shot `plan_from_goal` authoring; `checkpoint_every=0` → no auto-compaction; `record_frontier=False` → the cleared frontier is not threaded into the next recompile; `decompose=False` → single-shot `plan_from_goal` + manifest reconcile (no capability staffing); `episode_gate=None` → no boundary gate is consulted, every bounded round runs uninterrupted; `event_sink=None` → [`NullEventSink`](#nulleventsink) semantics (nothing emitted, no observer called). `checkpointer` enables outer-altitude resume; `backend` is `"auto"` \| `"python"` \| `"langgraph"`.

**Capability-decompose authoring + frontier channel (opt-in, default OFF).**

| Param | Type | Default | Meaning |
|---|---|---|---|
| `record_frontier` | `bool` | `False` | When `True` AND a `scheduler` is set, the router's cleared frontier ([`FrontierProposal.compile_next`](#frontierproposal)) is threaded into the NEXT round's `recompile(compile_next=…)` and recorded on the read-only `ProvisioningPlan.frontier`. Independent of the binder. |
| `decompose` | `bool` | `False` | When `True`, round-1 authoring runs `plan_from_goal(decompose=True)` + [`staff_capability_dag`](#staff_capability_dag)`(bind_fn)` instead of the single-node `plan_from_goal` + manifest reconcile — so `decompose → bind → assemble` is the loop's LIVE authoring path. |
| `bind_fn` | `Optional[Callable[[str], Optional[str]]]` | `None` | The per-capability binder passed to [`staff_capability_dag`](#staff_capability_dag): `node -> standing agent name`, or `None` to author a skeleton for that node. |

With `record_frontier=True`, the previously-dead scheduler→compiler channel is closed ON THE LIVE PATH: the router's cleared frontier is threaded forward and surfaced on the read-only `ProvisioningPlan.frontier` (emitted in `to_dict` only when non-empty; it never changes order/entries/wiring). With `record_frontier=False` (default) behavior is byte-for-byte unchanged. With `decompose=True` the loop runs cold-start with ZERO caller manifests: `plan_from_goal(decompose=True)` yields an agent-agnostic capability DAG, [`staff_capability_dag`](#staff_capability_dag)`(bind_fn)` staffs it (bind each capability via `bind_fn`, else author a low-trust skeleton), and the staffed set is MEMOIZED (one deterministic set shared by assemble/recompile + every episode; a resume re-derives it identically, INV-4). The single-node plan is still frozen by `assemble` exactly as before (INV-3). With `decompose=False` (default) the loop uses the byte-for-byte single-shot manifest path.

**Auto-Create seam (opt-in, default OFF).** With `auto_create=True` AND a `scheduler`, when the router finds `UNMATCHED` frontier roles (no standing agent serves them) it invokes the spawn seam `create_fn(task) -> bool`, records the spawned tasks on `ctx["created"]` (surfaced on the cockpit), then RE-PROPOSES the frontier so a now-standing agent binds. `create_fn` defaults to the internal `_default_create_fn`, which routes `registry.ensure_task(task)` → `provision_agent` → `CreateAgentRuntime` (the real Create actuator), returning whether an agent is now standing (or `None` when no registry is reachable, so auto-create no-ops and the node stays held); inject a fake `create_fn` for tests so nothing touches AWS/boto3. A failed or unconfirmed spawn leaves the node HELD (safe degradation, `UNMATCHED` this round). Spawns happen BETWEEN rounds — never a live-plan mutation (INV-3). With `auto_create=False` (default) an `UNMATCHED` role is simply held, unchanged.

**Episode-boundary approval/interrupt gate (`episode_gate`, opt-in, default OFF).** When a callable is supplied it is consulted ONLY at EPISODE BOUNDARIES — before each Supervisor episode is dispatched, never mid-episode (the supervisor still runs as a single static pass, INV-1). It receives a read-only VALUE view of the boundary (`{"type": "episode_boundary", "run_id", "round", "completed", "frontier"}`, with `completed`/`frontier` RE-DERIVED from the append-only log + the frozen plan's order, never a live handle) and returns a verdict: a falsy / `"approve"` / `"continue"` verdict dispatches the episode as normal; `"abort"` (or `"stop"`/`"halt"`) stops the loop BETWEEN episodes with no episode run, finalizing at `synthesize` with `terminated_by="aborted"`; `"pause"`/`"hold"` is the same bounded stop but labeled `"paused"` (a warm-resumable boundary, since nothing was dispatched). A stop verdict does not bump the round counter, construct a supervisor, invoke a node, or touch the frozen plan. Any exception the gate raises is swallowed (fail-open: the loop continues to the ungated bounded default). The gate can ONLY stop the bounded loop earlier — it can never extend it past the hard bounds, mutate a plan, or reach inside a running Supervisor (INV-1/INV-2/INV-3). With `episode_gate=None` (default) no gate is ever consulted and behavior is byte-for-byte unchanged.

**Episode-boundary event sink (`event_sink`, opt-in, default no-op).** When an [`EventSink`](#eventsink) is supplied the loop emits a plain-dict [`RunEvent`](state.md) VALUE at each episode boundary only — `episode_start` (in `run_episode`, before the supervisor is built), `episode_end` (in `collect`, after the log fold + progress bookkeeping, carrying `done`/`progressed`), and `decision` (on the routing edge, carrying `route` + the terminal `terminated_by` label). Each emitted `type` is asserted to be in the closed [`GOV_EVENT_KINDS`](#gov_event_kinds) vocabulary (on the sink-wired path only), and emission is wrapped so a misbehaving sink can never break a live episode. With `event_sink=None` (default) nothing is emitted, no observer is called, and the `GovernorResult` is byte-identical.

**Coordination-notice episode-start read (opt-in by ABSENCE).** At each episode START (never mid-episode) the loop does a PURE staleness read over the append-only log via [`list_pending_notices`](state.md): it hands `store.records()` + the freshly-sampled `store.completed()` terminal set to the reader, which returns only the coordination notices whose referenced node is NOT yet terminal (a notice about a finished node is stale and dropped). It selects nothing, dispatches nothing, and mutates neither the log nor the frozen plan (marking a notice consumed is a separate follow-up append, never done here). Opt-in by absence: when the log carries ZERO coordination records the pending list is empty, nothing is stashed on `ctx`, and the default loop (which never appends a notice) is byte-for-byte unchanged; only when notices exist is the surfaced referenced-node set recorded on `ctx["pending_notices"]` for read-only observability.

- **Raises** [`GovernorLoopError`](#governorlooperror) — `backend` not in `("auto", "python", "langgraph")`; empty/whitespace `goal`; `max_rounds < 1`; `no_progress_n < 1`; `checkpoint_every < 0`; `memory_id` set without a non-empty `session_id`; `memory_id` set without a non-empty `actor_id`.

```python
from concursus.governor import GovernorLoop

loop = GovernorLoop("resolve ticket 42", manifests, backend="python")
result = loop.run({"ticket_id": 42})
# -> GovernorResult(rounds=..., terminated_by="frontier_exhaust", ...)

# opt-in Trust-Ladder governance:
governed = GovernorLoop(goal, manifests, scheduler=TrustLadderScheduler(registry, manifests=manifests))

# durable MemoryStateStore backend:
durable = GovernorLoop(goal, manifests, memory_id="mem-1", actor_id="act-1", session_id="sess-1")

# opt-in episode-boundary observability (offline; no LLM/boto3 needed):
from concursus.governor import NullEventSink

class CollectingSink:                     # a trivial EventSink (structural typing)
    def __init__(self): self.events = []
    def emit(self, event): self.events.append(event)

observed = GovernorLoop(goal, manifests, backend="python", event_sink=CollectingSink())
# observed.run(...) drives episode_start/episode_end/decision events into the sink;
# swapping in NullEventSink() (or leaving event_sink unset) is byte-identical.

# opt-in episode-boundary approval/interrupt gate: stop between episodes:
gated = GovernorLoop(
    goal, manifests, backend="python",
    episode_gate=lambda b: "pause" if b["round"] >= 1 else "continue",
)
# gated.run(...) -> GovernorResult(terminated_by="paused", ...) once round 1 boundary is reached
```

#### `GovernorLoop.run`

```python
def run(self, inputs: Optional[dict] = None) -> GovernorResult
```

Drive the bounded outer cycle to termination and return a [`GovernorResult`](#governorresult). Tries the LangGraph backend on `"auto"` (falling back to pure Python if langgraph is not importable) or `"langgraph"` (raising if missing); `"python"` forces the fallback. Either backend runs the SAME node functions and routing. Restores from a surviving checkpoint (re-fetching the plan BY VERSION), then stashes the run's final frozen plan + read-only governance sets for the cockpit/scope accessors.

- **Raises** [`GovernorLoopError`](#governorlooperror) — `backend="langgraph"` requested but langgraph is not installed.

#### `GovernorLoop.cockpit`

```python
def cockpit(self, *, vault_path: Optional[str] = None) -> "DirectorCockpit"
```

Return a read-only [`DirectorCockpit`](#governorcockpit) over this loop's CURRENT run. Builds a `Supervisor` bound to the loop's OWN store and the run's final frozen plan VALUE, then hands it (plus the read-only escalated/unmatched sets) to the cockpit. A PURE read surface — it never calls `Supervisor.run`, never assembles/recompiles, never dispatches, never `put`s. Call **after** `run()`; before the first run the plan is `None` (uses an inert empty plan) and the cockpit's `revision` reads `None`. `vault_path` (optional) lets the cockpit render the idempotent precedent hub.

```python
cockpit = loop.cockpit(vault_path="/path/to/vault")   # read-only surface over the finished run
```

#### `GovernorLoop.programs_index`

```python
def programs_index(self, vault_path: str, *, sep: str = SCOPE_SEP) -> Dict[str, dict]
```

Return the PROGRAM-grain projection over the live run's vault (read-only). A thin pass-through to [`scope.build_programs_index`](#build_programs_index). `vault_path` is REQUIRED (the offline default store holds no vault dir). Regenerated from the notes each call.

#### `GovernorLoop.leverage_view`

```python
def leverage_view(self, vault_path: str, *, sep: str = SCOPE_SEP) -> Dict[str, object]
```

Return the 1:N director-leverage view over the live run's vault (read-only). A pass-through to [`scope.director_leverage_view`](#director_leverage_view). Selects/seeds nothing, drives no dispatch (INV-5).

> **Gotchas.** The `MemoryStateStore` backend (`memory_id` set) REQUIRES both `session_id` and `actor_id` or `__init__` raises. Trust-Ladder governance is entirely OPT-IN: with `scheduler=None` the router is a byte-for-byte pass-through and `escalated`/`unmatched` are empty. Auto-Create is also OPT-IN and needs BOTH `auto_create=True` and a `scheduler`; with `auto_create=False` (default) an `UNMATCHED` role stays held and no spawn is attempted, and a failed spawn safely leaves the node held. Call `cockpit()`/`programs_index()`/`leverage_view()` after `run()`. The replan SIGNAL (failure/contradiction/low_confidence) overrides frontier-exhaustion but never the hard bounds — `max_rounds` and `no_progress_n` are checked first so a persistently-failing signal can never run away. A langgraph runtime failure during invoke falls back to the pure-Python driver on a FRESH initial context (re-running from scratch).

---

## `governor.ktlo`

Source: [`../../src/concursus/governor/ktlo.py`](../../src/concursus/governor/ktlo.py)

A standing KTLO ("keep-the-lights-on") daemon that wraps [`GovernorLoop`](#governorloop) over a live event source. A strictly-outer layer *above* the loop: it monitors a queue + drift detector, triages each signal, auto-escalates, and — per triggered investigation — dispatches ONE fresh bounded `GovernorLoop` episode. Launch (one-shot) vs KTLO (standing) is a config on the same machinery.

> **INV-1/INV-4.** The standing cycle lives entirely in this outer daemon. It only ENQUEUES episodes; it never reaches inside a running Supervisor and never lengthens a single `Supervisor.run` into an unbounded loop. Each woken investigation is a FRESH `GovernorLoop` over a FRESH store (via `store_factory`) — N events → N independent, bounded, replayable-in-isolation episodes. A failing episode is recorded in `errors` and the daemon SURVIVES to the next signal.

| Symbol | Kind | Summary |
|---|---|---|
| [`LAUNCH`](#launch--ktlo) | constant | `"launch"` — one-shot: drain once, then stop. |
| [`KTLO`](#launch--ktlo) | constant | `"ktlo"` — standing cyclic monitor (the default mode). |
| [`TRIAGE_CLOSE`](#triage_close--triage_investigate--triage_escalate) | constant | `"close"` — noise; dropped, no episode. |
| [`TRIAGE_INVESTIGATE`](#triage_close--triage_investigate--triage_escalate) | constant | `"investigate"` — dispatch a bounded episode. |
| [`TRIAGE_ESCALATE`](#triage_close--triage_investigate--triage_escalate) | constant | `"escalate"` — flag + dispatch a bounded episode. |
| [`KTLODaemonError`](#ktlodaemonerror) | exception | Invalid daemon config (subclass of `ValueError`). |
| [`EventSource`](#eventsource) | protocol | The live signal seam (`poll` / `drained`). |
| [`InProcessEventQueue`](#inprocesseventqueue) | class | Zero-dependency in-process `EventSource`. |
| [`ScriptedEventSource`](#scriptedeventsource) | class | An `EventSource` yielding pre-scripted batches. |
| [`KTLOResult`](#ktloresult) | dataclass | The outcome of a bounded `KTLODaemon.run`. |
| [`KTLODaemon`](#ktlodaemon) | class | The standing daemon over an `EventSource`. |
| [`KTLODaemon.run`](#ktlodaemonrun) | method | Drive the bounded monitor loop to termination. |
| [`FireBudgetGate`](#firebudgetgate) | class | *(opt-in)* A persisted, pure per-`(source, entity)` fire-budget admission gate. |
| [`ProvenanceGuard`](#provenanceguard) | class | *(opt-in)* A pure self-trigger guard: drops events the fleet itself emitted. |
| [`DetectionMode`](#detectionmode) | enum | The `new_items` / `state_change` / `diff` episode-admission modes. |
| [`EpisodeAdmissionGate`](#episodeadmissiongate) | class | *(opt-in)* A persisted episode-admission gate over a `DetectionMode`. |
| [`IdleRuntimeCuller`](#idleruntimeculler) | class | *(opt-in)* A pure idle-runtime reaper (computes-only). |

The type aliases the daemon accepts as seams: `StoreFactory = Callable[[], StateStore]` (yields a fresh log per episode; default `InProcessStateStore`), `DriftDetector = Callable[[], List[dict]]` (synthetic drift signals, each tagged `source="drift"`), `GoalFn = Callable[[dict], str]` (signal → episode goal; default uses the signal's `goal`/`summary`/`title`/`id`, else `"investigate ktlo signal"`), and `TriageFn = Callable[[dict], str]` (signal → verdict; the default closes an explicit `noise` flag, escalates severity in `{sev1, sev2, high, critical, p0, p1}`, else investigates).

### `LAUNCH` / `KTLO`

```python
LAUNCH = "launch"   # one-shot scoped formation: drain once, then stop
KTLO   = "ktlo"     # standing cyclic monitor: keep polling across ticks until drained (default)
```

Daemon modes — a config on the SAME machinery. `LAUNCH` sets `terminated_by = "launch_complete"`; `KTLO` sets `"source_drained"` or `"tick_cap"`.

### `TRIAGE_CLOSE` / `TRIAGE_INVESTIGATE` / `TRIAGE_ESCALATE`

```python
TRIAGE_CLOSE       = "close"        # noise / below threshold — dropped (events_closed++)
TRIAGE_INVESTIGATE = "investigate"  # real work — dispatch (events_investigated++)
TRIAGE_ESCALATE    = "escalate"     # high severity — flag (escalations++) AND dispatch
```

The three triage verdicts. `TRIAGE_ESCALATE` is a superset of investigate for dispatch — it counts toward BOTH `escalations` and `events_investigated`.

### `KTLODaemonError`

```python
class KTLODaemonError(ValueError)
```

Raised on an invalid KTLO daemon configuration (bad mode, missing source, bad bound, empty manifests, negative idle floor). Subclass of `ValueError`.

### `EventSource`

```python
class EventSource(Protocol):
    def poll(self) -> List[dict]: ...
    def drained(self) -> bool: ...
```

The live signal seam the daemon monitors. `poll()` returns the batch of signals arrived since the last poll (`[]` on a quiet tick); `drained()` reports whether the source will yield no further signals. The standing loop uses `drained()` (with a quiet drift detector) to know when it may stop; a launch run drains it exactly once.

### `InProcessEventQueue`

```python
class InProcessEventQueue:
    def __init__(self, events: Optional[List[dict]] = None, *, closed: bool = True) -> None
    def enqueue(self, event: dict) -> None
    def close(self) -> None
    def poll(self) -> List[dict]
    def drained(self) -> bool
```

A zero-dependency in-process `EventSource` (offline default / test seam). Holds signals in a FIFO list; each `poll` drains and returns everything enqueued since the last poll. Copies each event dict on construction and `enqueue` (stores `dict(event)`). `drained()` is `True` once the queue is empty AND `close()`d (default `closed=True`).

### `ScriptedEventSource`

```python
class ScriptedEventSource:
    def __init__(self, batches: List[List[dict]]) -> None
    def poll(self) -> List[dict]
    def drained(self) -> bool
```

An `EventSource` that yields pre-scripted batches, one per `poll`. e.g. `batches=[[t1], [], [t2]]` delivers `t1`, then NOTHING (the daemon must survive the empty tick), then `t2`; `drained()` is `True` once all batches have been polled (`[]` thereafter). Used to prove the standing daemon persists between episodes.

### `KTLOResult`

```python
@dataclass
class KTLOResult:
    mode: str
    ticks: int
    terminated_by: str
    episodes: List[GovernorResult] = field(default_factory=list)
    episode_plans: List[ProvisioningPlan] = field(default_factory=list)
    events_seen: int = 0
    events_closed: int = 0
    events_investigated: int = 0
    escalations: int = 0
    drift_triggered: int = 0
    errors: List[str] = field(default_factory=list)
    alive: bool = False
```

The outcome of a bounded `KTLODaemon.run`. Tallies `mode`, monitor `ticks`, `terminated_by` (`source_drained` \| `launch_complete` \| `tick_cap`), the ordered [`GovernorResult`](#governorresult) per dispatched investigation (`episodes`), the first frozen plan each episode formed (`episode_plans` — one DISTINCT plan object per episode, INV-4), the event counters (`events_seen`/`events_closed`/`events_investigated`/`escalations`/`drift_triggered`), per-episode `errors`, and `alive` (`False` once `run()` returns).

### `KTLODaemon`

```python
class KTLODaemon:
    def __init__(
        self,
        manifests: Dict[str, AgentManifest],
        *,
        source: Optional[EventSource] = None,
        mode: str = KTLO,
        drift_detector: Optional[DriftDetector] = None,
        goal_fn: Optional[GoalFn] = None,
        triage_fn: Optional[TriageFn] = None,
        store_factory: Optional[StoreFactory] = None,
        assembler: Optional[OrchestrationAssembler] = None,
        supervisor_factory: Optional[SupervisorFactory] = None,
        invoke_fn: Optional[InvokeFn] = None,
        arns: Optional[Dict[str, str]] = None,
        plan_model_fn: Optional[PlanModelFn] = None,
        max_ticks: int = 64,
        episode_max_rounds: int = 8,
        episode_no_progress_n: int = 2,
        max_revisions: int = DEFAULT_MAX_REVISIONS,
        backend: str = "python",
        scheduler: Optional[Any] = None,
        deliberate: bool = False,
    ) -> None
```

A standing KTLO daemon wrapping `GovernorLoop` over a live `EventSource`. Runs the outer `monitor → triage → escalate → (replan | close)` loop, dispatching one fresh bounded `GovernorLoop` episode per investigate/escalate signal. `mode` is the ONLY difference between launch and ktlo. `store_factory` defaults to `InProcessStateStore`; `backend` defaults to `"python"` (unlike `GovernorLoop`'s `"auto"`). Optional `scheduler`/`deliberate` are forwarded per-episode.

- **Raises** [`KTLODaemonError`](#ktlodaemonerror) — `mode` not in `(LAUNCH, KTLO)`; `source is None`; `max_ticks < 1`; empty `manifests` map.

```python
from concursus.governor import KTLODaemon, InProcessEventQueue, ScriptedEventSource

src = InProcessEventQueue([{"id": "t1", "severity": "sev2"}], closed=True)
daemon = KTLODaemon(manifests, source=src, mode="launch")
result = daemon.run()   # -> KTLOResult(terminated_by="launch_complete", episodes=[GovernorResult(...)])

standing = KTLODaemon(manifests, source=ScriptedEventSource([[t1], [], [t2]]), mode="ktlo", max_ticks=8)
```

#### `KTLODaemon.run`

```python
def run(self) -> KTLOResult
```

Stand up the daemon and drive the bounded `monitor → triage → escalate → replan` loop to termination. In `launch` mode runs exactly ONE monitor tick (drain-once); in `ktlo` mode keeps ticking (surviving empty ticks) until the source is drained and drift is quiet, or the hard `max_ticks` cap trips. Each investigate/escalate signal spawns one fresh bounded episode. Returns a [`KTLOResult`](#ktloresult); sets `result.alive = False` on return.

> **Gotchas.** `KTLODaemon` `backend` defaults to `"python"`, NOT `"auto"` like `GovernorLoop`. A raising episode does not crash the daemon — the exception is caught, appended to `result.errors`, and the daemon proceeds. Episode inputs are derived from `signal["inputs"]` if present, else the signal's non-reserved fields (reserved: `inputs`/`id`/`source`/`signal`/`goal`); the whole signal is always threaded under `inputs["signal"]`. The episode `run_id` defaults to `str(signal.get("id", goal))` — signals without an id share a `run_id` derived from their goal (relevant if a checkpointer is ever wired).

### `FireBudgetGate`

```python
class FireBudgetGate:
    def __init__(self, store: Optional[StateStore] = None, *, clock: Optional[Callable[[], float]] = None) -> None
    def fires(self, source_id: str, entity_ref: str) -> int
    def can_fire(self, source_id, entity_ref, cooldown_s=0.0, max_fires=1, *, now=None) -> bool
    def commit_fire(self, source_id: str, entity_ref: str, *, now=None) -> None
```

*(opt-in)* A persisted, PURE per-`(source, entity)` fire-budget admission gate for a standing fleet. `can_fire` is a PURE read — it inspects the persisted seen/last-fired cell and returns whether a fire is admissible under a `cooldown_s` (minimum seconds between fires) and a `max_fires` cap; it NEVER mutates state and NEVER consumes budget, so it is idempotent and safe to call speculatively. Consumption is a SEPARATE step: call `can_fire`, do the durable work (dispatch an episode), and — only AFTER that durable commit succeeds — call `commit_fire` to record the consumption. So an episode that never durably happened never burns budget. The cell is persisted through the append-only [`StateStore`](state.md) (the SSOT), so the budget survives a resume via replay and is shared by any gate over the same store. `max_fires=None` disables the cap (cooldown-only); `cooldown_s=0` disables the cooldown. Default-off — nothing constructs one unless a caller opts in.

### `ProvenanceGuard`

```python
class ProvenanceGuard:
    def __init__(self, fleet_ids: Iterable[str], *, provenance_keys: Tuple[str, ...] = (...)) -> None
    def is_self_triggered(self, signal: dict) -> bool
    def admit(self, signals: Iterable[dict]) -> List[dict]
```

*(opt-in)* A PURE provenance self-trigger guard: drops events the fleet itself emitted. A standing fleet that reacts to a live source can trip over its OWN side effects — an episode writes a record the source re-surfaces as a fresh signal, spawning another episode ad infinitum. Given the set of provenance ids the fleet stamps on the events it emits, `is_self_triggered` reports whether a signal carries one (checked across a few conventional provenance keys, default `("emitted_by", "provenance", "source_fleet", "fleet_id")`), and `admit` filters a batch down to the externally-sourced signals. Pure — it holds only the id set, mutates nothing.

### `DetectionMode`

```python
class DetectionMode(str, Enum):
    NEW_ITEMS    = "new_items"      # admit only the FIRST time a signal's key is seen (identity dedup)
    STATE_CHANGE = "state_change"   # admit only when the signal's state hash DIFFERS from last seen
    DIFF         = "diff"           # admit only when the signal's item set has keys not seen before
```

How an [`EpisodeAdmissionGate`](#episodeadmissiongate) decides a signal is worth a fresh episode. A `str` subclass so `== "new_items"` and string projection keep working.

### `EpisodeAdmissionGate`

```python
class EpisodeAdmissionGate:
    def __init__(self, mode, store: Optional[StateStore] = None, *,
                 key_fn=None, state_fn=None, items_fn=None,
                 namespace: str = "ktlo_episode_seen") -> None
    @property
    def mode(self) -> DetectionMode
    def admit(self, signal: dict) -> bool
    def diff(self, signal: dict) -> List[str]
    def commit(self, signal: dict) -> None
```

*(opt-in)* A persisted episode-admission gate over a [`DetectionMode`](#detectionmode) + a seen-key set. `admit` is a PURE read (it never mutates the seen set) that answers whether `signal` warrants a fresh episode under the configured mode. As with [`FireBudgetGate`](#firebudgetgate), the seen-key set is committed SEPARATELY via `commit` — call it only AFTER a durable commit (the episode dispatched), so an un-dispatched signal never poisons the seen set. `diff` returns the item keys not yet seen (for `DIFF` mode). The seen set is persisted through the append-only [`StateStore`](state.md) (the SSOT), so it survives a resume via replay and is shared by any gate over the same store. Default key/state/item extractors are supplied; override via `key_fn`/`state_fn`/`items_fn`.

### `IdleRuntimeCuller`

```python
CULL_TIER_STANDING = "standing"     # held to the LONG idle floor
CULL_TIER_EPHEMERAL = "ephemeral"   # held to the SHORT idle floor

class IdleRuntimeCuller:
    def __init__(
        self,
        long_floor_s: float,
        short_floor_s: float,
        *,
        standing_tier: str = CULL_TIER_STANDING,
        protect_most_recent: bool = True,
    ) -> None

    def floor_for(self, runtime, tiers=None, *, most_recent=None) -> float
    def cull(
        self,
        last_active: Mapping[str, float],
        now_ts: float,
        *,
        active: Iterable[str] = (),
        tiers: Optional[Mapping[str, str]] = None,
    ) -> Set[str]
```

A **PURE, computes-only** idle-runtime reaper: given `{runtime -> last_active_ts}`, the wall-clock `now_ts`, the set of in-flight `active` runtimes, and a per-runtime `tiers` map, `cull` returns the `Set[str]` of runtimes **eligible** for teardown. It performs **no teardown**, holds no runtime handles, touches no registry / ledger / plan, and mutates nothing — the caller tears the returned set down and re-provisions from the durable ledger identity on the next invoke. It belongs to the **outer governance layer, never `Supervisor.run`** (INV-3).

Two idle floors:
- A runtime whose tier is `standing` (or the single most-recently-active runtime, when `protect_most_recent`) is held to the **long** floor; every other runtime to the **short** floor.
- **Never cull an `active` (in-flight) runtime**, regardless of its `last_active`.
- **Validate wall-clock `elapsed = now_ts - last_active >= floor` before culling; reschedule (keep) if `elapsed < floor`.** Drift-safe — a `last_active` stamped in the future (clock skew) yields a negative elapsed, which is below any non-negative floor, so it keeps rather than reclaims.

**Opt-in / additive.** Nothing constructs or calls the culler by default; it is a helper the governance layer may use to bound a standing fleet's idle-runtime accretion. The runtime is disposable — its identity persists in the [`DeployLedger`](build.md) and re-provisions on next invoke. `__init__` raises [`KTLODaemonError`](#ktlodaemonerror) on a negative floor.

```python
from concursus.governor.ktlo import IdleRuntimeCuller, CULL_TIER_STANDING

culler = IdleRuntimeCuller(long_floor_s=900, short_floor_s=300)
to_reclaim = culler.cull(
    {"agentA": now - 1200, "agentB": now - 120, "agentC": now - 600},
    now,
    active={"agentC"},                       # in-flight -> never culled
    tiers={"agentA": CULL_TIER_STANDING},    # standing -> long floor (1200s >= 900 -> eligible)
)
# -> {"agentA"}   (agentB under short floor: kept; agentC active: kept)
```

---

## `governor.cockpit`

Source: [`../../src/concursus/governor/cockpit.py`](../../src/concursus/governor/cockpit.py)

The read-only director cockpit — a thin PROJECTION layer over already-shipped read models that composes director surfaces from nothing but query/summary/render* calls: `briefing`, `exception_queue`, `runs_monitor`, plus a snapshot-then-follow tail and a live family tree over the frozen DAG.

> **INV-5 (memory seam).** The cockpit selects nothing, seeds nothing, schedules nothing, and holds no mutable executed-prefix cache. It never calls `assemble()`, `Supervisor.run()`, or `StateStore.put()` — it re-derives every view from the append-only log on each call via read-only surfaces. The opt-in governance sets (`escalated`/`unmatched`) are just VALUES passed in at construction; the cockpit never re-derives them.

Normally constructed via [`GovernorLoop.cockpit()`](#governorloopcockpit), which injects the loop's store-bound `Supervisor`, the final frozen plan, and the last run's escalated/unmatched sets.

| Symbol | Kind | Summary |
|---|---|---|
| [`DirectorCockpit`](#directorcockpit) | class | The read-only director view over one run's read models. |
| [`DirectorCockpit.briefing`](#directorcockpitbriefing) | method | Run summary + optional precedent-hub path. |
| [`DirectorCockpit.exception_queue`](#directorcockpitexception_queue) | method | Failed/blocked (+ escalated/unmatched) nodes awaiting judgment. |
| [`DirectorCockpit.runs_monitor`](#directorcockpitruns_monitor) | method | Plan version + progress over the log metadata. |
| [`DirectorCockpit.snapshot`](#directorcockpitsnapshot--follow) / [`.follow`](#directorcockpitsnapshot--follow) | methods | Point-in-time replay + replay-from-offset tail over the log. |
| [`DirectorCockpit.family_tree`](#directorcockpitfamily_tree) | method | The frozen `AgentDAG` rendered as a live-status lineage tree. |
| [`NodeEventBus`](#nodeeventbus) | class | A read-side per-node stream multiplexer. |
| [`ControlSurface`](#controlsurface) | class | *(opt-in)* Agent-facing surface over the SSOT — always-on read verbs; actuating verbs gated by non-registration + activation + monotonic trust clamp. |
| [`ControlSurfaceError`](#controlsurfaceerror) | exception | Raised on an absent/unauthorized/unarmed verb. |

### `DirectorCockpit`

```python
class DirectorCockpit:
    def __init__(self, *, supervisor: Any, vault_path: Optional[str] = None,
                 plan: Any = None,
                 escalated: Optional[List[str]] = None,
                 unmatched: Optional[List[str]] = None) -> None
```

A read-only director view over one run's shipped read models. Handed an already-executed (or resumable) `Supervisor` plus an optional `vault_path`, `plan` value, and the opt-in `escalated`/`unmatched` governance sets. It never drives the run; it only reads `supervisor.summary()`/`index()` and renders the idempotent precedent hub. All args are keyword-only. `escalated`/`unmatched` default to `[]` (copied on construction) → today's failed-only exception queue. Direct construction requires supplying a `Supervisor` exposing `summary()`/`summary_line()`/`index()`/`session_id`.

```python
from concursus import DirectorCockpit

cockpit = DirectorCockpit(supervisor=supervisor, vault_path="/vault", plan=plan)
```

#### `DirectorCockpit.briefing`

```python
def briefing(self, *, slipbox_form: bool = False, date: str = "") -> Dict[str, Any]
```

A director briefing: run summary + (optional) precedent-hub path. Renders the idempotent precedent hub only when `vault_path` is not `None`, and folds in the supervisor's read-only summary.

- **Returns** `{"summary" (supervisor.summary()), "summary_line" (supervisor.summary_line()), "precedent_hub" (path or None), "revision" (plan.revision or None)}`. No plan assembled, no node dispatched.

```python
b = cockpit.briefing(slipbox_form=True, date="2026-07-15")
# {"summary": ..., "summary_line": ..., "precedent_hub": "/vault/...", "revision": 3}
```

#### `DirectorCockpit.exception_queue`

```python
def exception_queue(self) -> List[Dict[str, Any]]
```

The failed/blocked nodes awaiting a director judgment. Driven by `Supervisor.summary()["failed"]`, enriched with the latest failed `Record` from `RunIndex.query(status="failed")` for `attempt`/`address`/`content_hash` metadata (latest chosen by max `seq` per node). Iterates `summary()["order"]`, emitting one row per failed node as `{node, reason (the failed reason), attempt, address, content_hash}`. When the opt-in governance sets were handed in, one row per escalated node (`reason="escalated"`) and per unmatched node (`reason="unmatched"`) is APPENDED — these carry `attempt`/`address`/`content_hash` as `None`. Default (no governance sets) → exactly the failed rows.

```python
q = cockpit.exception_queue()
# [{"node": "x", "reason": "timeout", "attempt": 2, ...}, {"node": "y", "reason": "escalated", ...}]
```

#### `DirectorCockpit.runs_monitor`

```python
def runs_monitor(self) -> Dict[str, Any]
```

A runs-index monitor: plan version + progress over the log metadata. Reads `RunIndex` metadata (node set, record count) and the supervisor summary's progress counters; reports the frozen plan's `revision`.

- **Returns** `{"session_id", "revision", "total", "completed", "failed_count", "completed_nodes", "indexed_nodes" (sorted index.nodes()), "record_count" (len index.query()), "order"}`. Read-only: never touches the plan or the store.

```python
m = cockpit.runs_monitor()
# {"revision": 3, "total": 5, "completed": 4, "failed_count": 1, ...}
```

#### `DirectorCockpit.snapshot` / `.follow`

```python
def snapshot(self) -> Dict[str, Any]
def follow(self, from_offset: int) -> Dict[str, Any]
```

Snapshot-then-follow over the append-only log. `snapshot()` replays every record to the current offset (ordered by the store-assigned strict-monotonic `seq`) and returns `{"offset" (max seq), "records", "count"}`; an observer passes that `offset` to `follow(from_offset)`, which returns only the newer slice (`seq > from_offset`), ordered by `seq`, plus a fresh `offset`. Because the log is append-only and single-writer, this is loss-free with no drift and no reconcile branch. Read-only: drives nothing, mutates nothing (INV-4/INV-5).

#### `DirectorCockpit.family_tree`

```python
def family_tree(self) -> Dict[str, Any]
```

The frozen `AgentDAG` rendered as a lineage tree annotated with live status. Because the full topology is known at compile time — `plan.order` for the node set, `plan.wiring` for the edges — the cockpit draws the whole tree up front and colors each node `done` | `failed` | `running` | `pending` from the append-only log (`running` = a node that has emitted at least one record but is neither completed nor failed).

- **Returns** `{"revision", "nodes" (each `{node, status, producers}`), "counts" (per-status tallies)}`. Read-only; no plan is touched.

### `NodeEventBus`

```python
class NodeEventBus:
    def __init__(self) -> None
    def subscribe(self, node_id: str, listener: Callable[[str, Any], None]) -> Callable[[], None]
    def emit(self, node_id: str, chunk: Any) -> None
```

A read-side per-node stream multiplexer: one ingest point, N per-node listeners. An observer `subscribe`s to a single node id and receives only that node's chunks (the call returns an unsubscribe thunk), so an operator can isolate one agent's output without threading callbacks through producers. Pure dispatch — it holds no run state and drives nothing (INV-5); producers are decoupled from consumers. Import from `concursus.governor.cockpit`.

### `ControlSurfaceError`

```python
class ControlSurfaceError(RuntimeError)
```

Raised by [`ControlSurface`](#controlsurface) on an illegal control operation: invoking an absent/unauthorized verb, invoking a dangerous verb that was not `activate`\ d, or activating a verb the compiled scope did not authorize. Subclass of `RuntimeError`.

### `ControlSurface`

```python
class ControlSurface:
    def __init__(self, *, supervisor: Any = None, scope: Any = None,
                 vault_path: Optional[str] = None, plan: Any = None,
                 actuators: Optional[Mapping[str, Callable[..., Any]]] = None) -> None
    def verbs(self) -> List[str]                       # the verbs this surface exposes
    def has_verb(self, verb: str) -> bool
    # READ verbs — always present, pure projections over the SSOT:
    def query_plan(self) -> Dict[str, Any]
    def tail_log(self, from_offset: int = 0) -> Dict[str, Any]
    def search_runs(self, text: str = "", *, key=None, limit=None) -> List[Dict[str, Any]]
    def precedents(self) -> List[dict]
    # ACTUATING verbs — gated:
    def activate(self, verb: str) -> None              # arm a dangerous verb (must be scope-authorized)
    def is_active(self, verb: str) -> bool
    def effective_trust(self, requested) -> Any        # clamp DOWN to the compiled ceiling
    def invoke(self, verb: str, /, *args, **kwargs) -> Any
```

An **OPT-IN, agent-facing control surface over the SSOT** — a thin, in-process seam (no HTTP) an agent can drive to observe and, when authorized, actuate a run. Its safety comes from three compile-anchored gates, all reading from the **compiled scope** ([`ControlScope`](#controlscope) / `plan.revision`), never an env var:

1. **Non-registration.** [`READ_VERBS`](#read_verbs--actuating_verbs) (`query_plan` / `tail_log` / `search_runs` / `precedents`) are **always** present — pure read projections over the append-only log, run-db, and precedent store. [`ACTUATING_VERBS`](#read_verbs--actuating_verbs) (`deploy` / `run` / `recompile`) appear on the surface **only if the compiled scope authorized them** — an unauthorized actuating verb is simply *absent* (`has_verb` → `False`, `invoke` raises `ControlSurfaceError`). Illegal states are unrepresentable rather than runtime-rejected.
2. **Activation.** A present actuating verb must be explicitly `activate`\ d before it can `invoke` — a two-step arm/fire guard against accidental actuation.
3. **Monotonic trust clamp.** `effective_trust(requested)` clamps the requested [`TrustGrade`](build.md) **down** to the compiled ceiling (never above it). Actuation routes through the **existing actuators** (`build.provision` / `execute.Supervisor` / `assemble`) — the surface owns no logic and **cannot mutate the frozen plan**.

**Opt-in / additive.** Nothing constructs a `ControlSurface` by default; it is a capability an operator/agent layer wires explicitly. Read verbs are side-effect-free; actuating verbs are absent unless the compiled scope authorized them, then arm-gated and trust-clamped. Import from `concursus.governor.cockpit`.

```python
from concursus.governor.cockpit import ControlSurface

surface = ControlSurface(supervisor=supervisor, scope=scope, actuators=actuators)
surface.query_plan()                 # always available (read verb)
surface.has_verb("deploy")           # True only if `scope` authorized it
surface.activate("deploy")           # arm it (raises if not scope-authorized)
surface.invoke("deploy", plan)       # routes through the real actuator; trust clamped to the ceiling
```

- **Raises** [`ControlSurfaceError`](#controlsurfaceerror) — invoking an absent/unauthorized verb, invoking a dangerous verb that was not `activate`\ d, or activating a verb the compiled scope did not authorize.

---

## See also

- [Guide: The Governor (Runtime Governance)](../guides/governor.md) — the narrative walkthrough of this tier: mental model, worked examples, and when to reach for each seam.
- [Guide: Durable Run State](../guides/durable-state.md) and the [`state` reference](state.md) — the append-only `StateStore` log the governor folds outputs into.
- [Guide: Knowledge Transfer](../guides/knowledge-transfer.md) — the session-end `TransferTriggerSink` that fires at `synthesize` through the event-sink seam.
- [`build` reference](build.md) — the `DeployLedger` the registry projects over and the `TrustGrade`/`evaluate_deploy_gate` create-time gate the scheduler seeds from.
- [`assemble` reference](assemble.md) and [`execute` reference](execute.md) — the compiler front (`ProvisioningPlan`, `plan_from_goal`) and the `Supervisor` the loop drives one episode at a time.
- [Core Concepts](../concepts.md) — the vocabulary and invariants (DAG, plan, state, trust, governor).
