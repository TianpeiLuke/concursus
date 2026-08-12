# Guide: The Governor (Runtime Governance)

*The strictly-outer standing loop: schedule, match by trust, run bounded episodes, escalate.*

The compiler is one organ — [`AgentDAG` → assemble → frozen `ProvisioningPlan` → `Supervisor.run`](compiling-and-running.md) — and a single run is one forward pass over an immutable plan. But a real operating system does not stop after one process. It keeps a standing loop that schedules the next unit of work, matches it to a capable server, decides whether to run or hold, and survives forever. That is the **governor**: the runtime-governance tier that wraps the compiler in a bounded cycle. The README sketches it; this guide documents it in full.

The load-bearing rule, stated plainly: **Concursus is a compiler, not a runtime governor.** The governor is a bounded cycle *around* the compiler. Each round forms a fresh frozen plan at the compiler front and dispatches a new bounded episode. It **never reaches inside a running `Supervisor` and never mutates a frozen plan** — the governor loop is strictly *outer*. Growth happens *between* episodes (a new `recompile`d plan value), never mid-flight. This is not a refusal to govern; it is *how* the governor governs at scale, safely and auditably.

All symbols in this guide are exported from `concursus.governor` (see [`governor/__init__.py`](../../src/concursus/governor/__init__.py)). For the terse symbol catalog see the [governor API reference](../reference/governor.md); for the create-time gate this tier complements see [build reference → Trust Ladder](../reference/build.md).

---

## The mental model: a bounded cycle around the compiler

The governor's control loop has a **fixed topology, compiled once**:

```
planner -> router -> run_episode -> collect -> route_after_collect
                                                 -> {planner | synthesize} -> END
```

All dynamism lives in the persistent `GovernorState` and the append-only `StateStore` log — the topology itself never changes. One trip around the cycle is one **round**:

| Node | What it does | Invariant |
|---|---|---|
| `planner` | Forms a **new** frozen `ProvisioningPlan` at the compiler front (`plan_from_goal` + `assemble` round 1, `recompile` after). Never edits a prior plan. | INV-3/INV-4 |
| `router` | (Opt-in) matches the ready frontier to standing agents by trust; partitions into dispatch vs held. Default: pure pass-through. | INV-3/INV-4 |
| `run_episode` | Calls `Supervisor.run` **once** to completion over the frozen plan — a single static pass. | INV-1 |
| `collect` | Folds the episode outputs into the append-only log; re-earns trust GOV-side. Executed prefix re-derived from `store.completed()`. | INV-5 |
| `route_after_collect` | Decides, *bounded*, whether to replan (another round) or synthesize (terminate). | bounded |

The loop is **bounded four ways so it must terminate**: frontier-exhaustion, a `no_progress_n` stall bound, a `max_rounds` budget, and a hard structural `step_cap`. A persistent replan signal can override frontier-exhaustion but never the hard bounds — `max_rounds`/`no_progress_n` are checked first, so a failing signal can never run away.

---

## `GovernorState` — persistent outer-loop state

[`governor/state.py`](../../src/concursus/governor/state.py) holds the cycle's persistent state. It is deliberately **not** a mutable compiler plan: it is the ordered *sequence* of frozen plan **values** (by version) plus a **pointer** to the append-only log.

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

- `plan_version` **always mirrors** `current_frozen_plan.revision` — `__post_init__` pins it (any value you pass to the constructor is overwritten), and `plan_history` is seeded with `[current_frozen_plan]` so the sequence is complete from round zero.
- `advance(next_plan, *, reason=None, progressed=True)` swaps in a newly assembled/recompiled plan: it **appends** `next_plan` to `plan_history`, re-points `current_frozen_plan`, sets `plan_version = next_plan.revision`, increments `iteration`, and resets `no_progress` to 0 (or increments it when `progressed=False`). It **never edits the prior plan** — the prior value stays byte-identical in `plan_history`.

```python
state = GovernorState(current_frozen_plan=plan, store=store)
# plan_version == plan.revision;  plan_history == [plan]
state.advance(recompiled_plan, reason="replan", progressed=True)
# version bumped, history extended, no_progress reset to 0
```

There is deliberately **no** `set_output`-style API and no method that edits a plan in place. `advance()` mutates and returns `self` (the *container*), but the held plan **values** are immutable; the executed prefix stays re-derivable from the log (INV-5). A stall is a *run* of `progressed=False` advances — any `progressed=True` resets the counter to 0.

---

## `GovernorLoop` — the fixed cyclic driver

[`governor/loop.py`](../../src/concursus/governor/loop.py) is the outer driver. The cycle nodes are the module constant `GOV_NODES = ("planner", "router", "run_episode", "collect")` (`synthesize` is the terminal node reached from the routing edge). It runs on an **optional LangGraph backend** (imported lazily) or a **pure-Python fallback** — the same node functions and routing either way. Concursus imports and its full suite passes with neither LangGraph nor any LLM installed.

### Construction

```python
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
    # ... deliberation seams (see the Reasoning guide) ...
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
    record_frontier: bool = False,   # opt-in scheduler->compiler channel (default off)
    decompose: bool = False,         # opt-in cold-start capability authoring (default off)
    bind_fn: Optional[Callable[[str], Optional[str]]] = None,  # per-capability binder
    episode_gate: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,  # opt-in boundary gate (default off)
    event_sink: Optional[EventSink] = None,   # opt-in episode-boundary observability (default off)
) -> None
```

Key options:

| Option | Effect |
|---|---|
| `store=` | Inject any [`StateStore`](durable-state.md) verbatim (highest precedence). |
| `memory_id=` (+ `session_id`, `actor_id`) | Select the shipped `MemoryStateStore` so the log survives micro-VM teardown. **Both `session_id` and `actor_id` are required** or `__init__` raises. |
| *(neither)* | Falls back to the offline `InProcessStateStore`. |
| `scheduler=` | **Opt-in.** Enables the Trust-Ladder router. `None` → `router` is a byte-for-byte pass-through, nothing held. |
| `deliberate=True` | **Opt-in.** Enables the pre-freeze [DKS deliberation](reasoning.md) authoring path for round 1. `False` → single-shot `plan_from_goal`. |
| `checkpointer=` | Enables outer-altitude resume. |
| `checkpoint_every=N` | **Auto-checkpoint cadence.** `0` (default) → no auto-compaction; `>0` → `collect` calls `store.checkpoint()` once every N rounds so a long-running log stays bounded for warm resume (a pure resume-cost optimization; raw events are never deleted). |
| `auto_create=` (+ `create_fn=`) | **Opt-in Create arrow.** `False` (default) → an UNMATCHED role stays held (unchanged). `True` (with a `scheduler`) → an UNMATCHED frontier role is spawned on demand *between rounds*, then re-proposed. See [The Create arrow](#the-create-arrow--auto-spawn-an-unmatched-role-between-rounds). |
| `decompose=` (+ `bind_fn=`) | **Opt-in cold-start authoring (default `False`).** `True` → round 1 authors a *capability* DAG (`plan_from_goal(decompose=True)`) and staffs it via `staff_capability_dag(bind_fn)` instead of the single-node plan + manifest reconcile — the decompose → bind → assemble front. `False` → byte-for-byte the single-shot manifest path. See [The decompose→bind path](#the-decomposebind-path--cold-start-with-zero-manifests). |
| `record_frontier=` | **Opt-in scheduler→compiler channel (default `False`).** `True` (with a `scheduler`) → the router's cleared frontier is threaded into the next `recompile(compile_next=…)` and recorded on `ProvisioningPlan.frontier`. Off → `recompile` is called without `compile_next` (unchanged). See [The record_frontier channel](#the-record_frontier-channel--closing-the-schedulercompiler-loop-on-the-live-path). |
| `episode_gate=` | **Opt-in episode-boundary approval/interrupt gate (default `None`).** A `callable(boundary) -> Optional[str]` consulted ONLY at episode boundaries — before each Supervisor episode is dispatched, never mid-episode. `None` → no gate is ever consulted (unchanged). See [The episode gate](#the-episode-gate--approve-pause-or-abort-between-episodes). |
| `event_sink=` | **Opt-in episode-boundary observability (default `None`).** An `EventSink` (`emit(event)`) that receives a typed `RunEvent` value at each boundary. `None` (or the shipped `NullEventSink`) → nothing is emitted; the `GovernorResult` and log are byte-identical. Compose several observers via `FanOutEventSink([my_observer, TransferTriggerSink])`. See [The event sink](#the-event-sink--episode-boundary-observability) and [Composing sinks](#composing-sinks--the-session-end-transfer-trigger). |
| `backend=` | `"auto"` (LangGraph, else Python), `"python"` (force fallback), or `"langgraph"` (raise if missing). |

`GovernorLoopError` (a `ValueError`) is raised on invalid config: unknown backend, empty/whitespace goal, `max_rounds < 1`, `no_progress_n < 1`, `checkpoint_every < 0`, or `memory_id` set without both a non-empty `session_id` and `actor_id`.

### Running

`run(inputs=None) -> GovernorResult` drives the cycle to termination. It restores from a surviving checkpoint (re-fetching the plan **by version**, never a stored snapshot), runs the graph or Python driver, then stashes the run's final frozen plan and read-only governance sets for the cockpit/scope accessors.

```python
@dataclass
class GovernorResult:
    rounds: int
    terminated_by: str
    done: bool
    completed: List[str]
    frontier: List[str]
    outputs: Dict[str, dict]        # the LAST episode's outputs
    state: GovernorState
    trace: List[str]
    supervisor_runs: int            # one per round (INV-1)
    backend: str
    escalated: List[str] = field(default_factory=list)
    unmatched: List[str] = field(default_factory=list)
```

`terminated_by` is one of `frontier_exhaust | no_progress | unmatched_stall | round_cap | step_cap` — plus, when the opt-in [episode gate](#the-episode-gate--approve-pause-or-abort-between-episodes) stops the loop between episodes, `aborted` or `paused`. `unmatched_stall` is the specific no-progress case where an UNMATCHED held node blocked the frontier so it never advanced at all. `escalated`/`unmatched` are **always empty on the default (no-scheduler) path**.

```python
loop = GovernorLoop("resolve ticket 42", manifests, backend="python")
result = loop.run({"ticket_id": 42})
result.terminated_by   # 'frontier_exhaust'
result.supervisor_runs # one per round
```

### The Create arrow — auto-spawn an UNMATCHED role between rounds

By default an `UNMATCHED` frontier role (no standing agent serves the task) blocks the frontier forever — the safe, unchanged behavior. `GovernorLoop(auto_create=True, create_fn=…)` (both **opt-in**, both default off) closes that arrow: when a `scheduler` is set and the router finds UNMATCHED roles, the loop invokes the spawn seam `create_fn(task) -> bool` **between rounds**, records the spawned tasks on `ctx["created"]` (surfaced on the cockpit), then **re-proposes** so a now-standing agent binds this round.

- The default seam is `_default_create_fn()` — it routes `registry.ensure_task(task)` → `provision_agent` → `CreateAgentRuntime` (the real Create actuator). Inject a fake `create_fn` for tests so nothing touches AWS/boto3.
- A failed or unconfirmed spawn leaves the node **HELD** (safe degradation) — the loop never crashes on a spawn failure.
- Spawns happen strictly **between rounds** — never a live-plan mutation, never reaching inside a running `Supervisor` (INV-1/INV-3). A freshly authored agent enters at a low trust grade and must earn autonomy on the Trust Ladder before it can dispatch a side-effecting task.

```python
def fake_create(task: str) -> bool:            # test seam: no AWS/boto3
    registry.register_agent(make_manifest(task), capabilities={task})
    return True

loop = GovernorLoop(
    "resolve ticket 42", manifests,
    scheduler=scheduler,          # required for the Create arrow to fire
    auto_create=True,             # opt-in; default False leaves the role held
    create_fn=fake_create,        # default seam -> registry.ensure_task -> CreateAgentRuntime
    backend="python",
)
result = loop.run({"ticket_id": 42})
# an UNMATCHED role was spawned between rounds, then bound on the re-proposal
```

### The decompose→bind path — cold-start with zero manifests

By default round-1 authoring runs the single-node `plan_from_goal` and reconciles it against the caller's `manifests` — you must hand it a real manifest set. `GovernorLoop(decompose=True, bind_fn=…)` (both **opt-in**, both default off) makes the loop's *live* authoring path the **decompose → bind → assemble** front instead. When `decompose=True`, round 1 calls `plan_from_goal(decompose=True)` to author an agent-agnostic **capability** DAG (task nodes + edges, no manifests, no `depends_on`), then `staff_capability_dag(dag, bind_fn=…)` staffs it into an assemblable `{node: AgentManifest}` set — one manifest per capability node, either bound to a standing agent via `bind_fn(node)` or authored as a low-trust skeleton (see [`staff_capability_dag`](#capability-staffing--staff_capability_dag--staff_with_rebind) below). This makes the **cold-start path** work end-to-end: you can run with **zero caller manifests** and the loop authors and staffs the whole multi-node plan itself.

The staffed set is authored **once** and memoized — the same deterministic manifest set is shared by `assemble`, every `recompile`, and every episode, so a resume re-derives it identically (INV-4). `assemble` still freezes the staffed plan exactly as it freezes a hand-authored one (INV-3): decompose converges to a frozen `AgentDAG` *before* the freeze, so the loop stays strictly outer. Default `decompose=False` is the single-shot manifest path, byte-for-byte unchanged.

```python
from concursus.governor import GovernorLoop, TrustLadderScheduler

# Cold start: no hand-authored manifests. decompose=True authors a capability DAG
# and staffs it; bind_fn reuses a standing agent where one exists, else authors a skeleton.
def bind_capability(node: str) -> str | None:
    av = registry.match_task(node)         # a standing agent that serves this capability?
    return av.name if av else None         # else None -> author an L0_SHADOW skeleton

loop = GovernorLoop(
    "resolve ticket 42", {},               # zero caller manifests
    decompose=True,                        # opt-in: decompose -> bind -> assemble front
    bind_fn=bind_capability,               # per-capability binder (node -> agent name or None)
    backend="python",
)
result = loop.run({"ticket_id": 42})       # a real multi-node plan, authored + staffed from the goal
```

### The record_frontier channel — closing the scheduler→compiler loop on the live path

The router computes a cleared frontier (`FrontierProposal.compile_next`) every round, but by default that partition is *advisory* — `recompile` is called without it and the scheduler→compiler channel is dead on the live path. `GovernorLoop(record_frontier=True)` (opt-in, default off) closes it: when `record_frontier=True` **and** a `scheduler` is set, the router's cleared frontier is threaded into the next round's `recompile(compile_next=…)` and recorded on the read-only [`ProvisioningPlan.frontier`](#trustladderscheduler--the-routers-matcher) field (emitted in `to_dict` only when non-empty).

This is independent of the binder and of `auto_create`. It **never** changes `order`/`entries`/wiring — the monotonic superset is untouched; the plan merely carries *which* frontier the scheduler cleared this revision (INV-3). Default off = `recompile` is called without `compile_next`, byte-for-byte unchanged.

```python
loop = GovernorLoop(
    "resolve ticket 42", manifests,
    scheduler=scheduler,        # required — record_frontier needs a router to produce the frontier
    record_frontier=True,       # opt-in: thread the cleared frontier into the next recompile
    backend="python",
)
result = loop.run({"ticket_id": 42})
# result.state.current_frozen_plan.frontier  # the scheduler's cleared set, recorded (never reorders)
```

### The episode gate — approve, pause, or abort between episodes

By default the loop runs every bounded round with no interruption. `GovernorLoop(episode_gate=…)` (opt-in, default `None`) installs a human-or-policy approval gate that is consulted **only at episode boundaries** — before each Supervisor episode is dispatched, never mid-episode (the supervisor still runs as a single static pass, INV-1). The gate is a `callable(boundary) -> Optional[str]`; it receives a read-only VALUE view of the boundary — `{"type": "episode_boundary", "run_id", "round", "completed", "frontier"}` — re-derived from the append-only log and the frozen plan's order, never a live `ctx`/plan handle, so it can neither reach inside a running Supervisor nor mutate a frozen plan (INV-1/INV-3/INV-5).

The verdict is normalized:

| Verdict (case-insensitive) | Effect |
|---|---|
| falsy / `"approve"` / `"continue"` | Dispatch the episode as normal. |
| `"abort"` / `"stop"` / `"halt"` | Stop the loop **between** episodes (no episode runs this round); finalize with `terminated_by="aborted"`. |
| `"pause"` / `"hold"` | Same bounded stop, labeled `terminated_by="paused"` (a warm-resumable boundary, since nothing was dispatched). |

A stop verdict runs **no** supervisor, invokes no node, does not bump the round counter, and writes nothing to the log — the frozen plan and the still-open frontier are untouched. The gate can only stop the bounded loop **earlier**; it can never extend it past the hard bounds, and a gate that raises **fails open** (the loop continues, degrading to the ungated bounded default) so a buggy gate can never break a run. An always-`"approve"` gate yields a run byte-for-byte identical to the ungated default.

```python
def approve_between_episodes(boundary: dict) -> str | None:
    # boundary is a read-only VALUE: type / run_id / round / completed / frontier
    if boundary["round"] >= 3:
        return "pause"          # warm-resumable stop -> terminated_by == "paused"
    return "approve"            # dispatch this episode as normal

loop = GovernorLoop(
    "resolve ticket 42", manifests,
    episode_gate=approve_between_episodes,   # opt-in; default None never consults a gate
    backend="python",
)
result = loop.run({"ticket_id": 42})
# result.terminated_by  # 'paused' if the gate held between episodes, else a normal bound
```

### The event sink — episode-boundary observability

By default nothing is emitted. `GovernorLoop(event_sink=…)` (opt-in, default `None`) wires an observability seam that receives a small typed event at each episode boundary. The sink satisfies the `EventSink` Protocol (`emit(event) -> None`); each event is a frozen typed `RunEvent` VALUE whose `type` is a member of the closed `RunEventKind` vocabulary — never a live `ctx`/plan handle — so a sink can never reach inside a running Supervisor or mutate a frozen plan (INV-1/INV-3/INV-5). Three kinds are emitted per round, in order:

| `type` | When | Extra fields |
|---|---|---|
| `episode_start` | Before a Supervisor episode runs. | — |
| `episode_end` | After `collect` folds the episode's outputs into the log. | `done`, `progressed` |
| `decision` | The bounded routing verdict after `collect`. | `route`, `terminated_by` |

Every event carries the same boundary scalars as the gate view — `run_id`, `round`, `completed`, `frontier`. Emitter and readers share that one closed vocabulary; the build-time drift guard `check_run_event_alignment` (exercised in `tests/test_run_event_contract.py`) fails if the emitter ever sends a kind the readers don't know. Wiring a sink is strictly opt-in: the default is `None` (no-op — nothing is emitted, no method is called), so the default loop is **byte-for-byte unchanged**; the shipped `NullEventSink` is the canonical explicit no-op and returns a byte-identical `GovernorResult`. Any exception raised by `emit` is swallowed so a misbehaving sink can never break a live episode.

```python
from concursus.governor import GovernorLoop, NullEventSink

class ListSink:                          # satisfies the EventSink Protocol
    def __init__(self): self.events = []
    def emit(self, event): self.events.append(dict(event))

sink = ListSink()
loop = GovernorLoop(
    "resolve ticket 42", manifests,
    event_sink=sink,                     # opt-in; None (or NullEventSink()) emits nothing
    backend="python",
)
result = loop.run({"ticket_id": 42})
[e["type"] for e in sink.events]         # ['episode_start', 'episode_end', 'decision', ...]
# NullEventSink() is behaviorally identical to leaving event_sink unset.
```

One domain observer ships — `TransferTriggerSink` (the session-end transfer, from the state tier). To run it alongside your own observer, compose them with `FanOutEventSink`; see below.

#### Composing sinks — the session-end transfer trigger

The loop has exactly ONE `event_sink` slot, so observers that must coexist compose through `FanOutEventSink(sinks)` — it fans each boundary event out to every child, individually guarded (one misbehaving child can't starve the others), and an empty list is a no-op identical to leaving `event_sink` unset. The shipped domain observer that rides this seam is:

- `TransferTriggerSink(run_dir, target_dir, *, admit_fn=None, trail_id="run", date="")` (from `concursus.state.transfer`) fires the **session-end knowledge transfer** when the loop reaches `synthesize`. It keys on the `decision` event whose `route == "synthesize"` — the true end of the run, **not** `episode_end.done` (a done round can still route back to the planner). It exports the run's episodic notes to `target_dir` (and admits them via the injected `admit_fn`), marks the run transferred (exactly-once), and stashes the result on `.last_result`. Its errors are swallowed by the emit guard, and [`sweep_untransferred_runs`](../reference/state.md#sweep_untransferred_runs) is the reaper/next-boot backstop for a run that never reached a graceful `synthesize`.

```python
from concursus.governor import GovernorLoop, FanOutEventSink
from concursus.state.transfer import TransferTriggerSink

loop = GovernorLoop(
    "resolve ticket 42", manifests,
    event_sink=FanOutEventSink([
        TransferTriggerSink(run_dir, inbox_dir, admit_fn=admit_bundle, trail_id="sess-42"),
        my_own_observer,                  # any other EventSink-shaped observer
    ]),
    backend="python",
)
```

The trigger is the runtime half; the compile-time half — authoring the `slipbox_transfer` terminal node + its fail-closed acceptance gate (`build_slipbox_transfer_manifest` / `wire_slipbox_transfer_terminal` / `slipbox_transfer_acceptance_fn`), registering the consolidation sub-agent (`register_slipbox_foundry`), and the transfer-inclusive `session_overall_ok` rollup — lives in `concursus.state.transfer`. See [Guide: Session-End Knowledge Transfer](knowledge-transfer.md).

### Checkpoint store

The `CheckpointStore` Protocol is the outer-altitude resume seam. A checkpoint is a small plain dict — `{plan_version, iteration, no_progress, round, prev_completed, replan_reason}` — a **pointer** into the round sequence, never a mutable plan snapshot. `InProcessCheckpointStore` is the zero-dependency offline default; it **copies on both `save` and `load`** so a caller can never mutate the stored checkpoint in place. On restart the loop re-derives the frozen plan by replaying the compiler front against the surviving log.

### Read-only accessors over the live run

Call these **after** `run()` (before the first run the plan is `None`, so `revision` reads `None`):

- `cockpit(*, vault_path=None) -> DirectorCockpit` — a pure read surface (see [DirectorCockpit](#directorcockpit--the-human-as-director-surface) below).
- `programs_index(vault_path, *, sep=".") -> Dict[str, dict]` — thin pass-through to `scope.build_programs_index`.
- `leverage_view(vault_path, *, sep=".") -> Dict[str, object]` — pass-through to `scope.director_leverage_view`.

---

## `TrustLadderScheduler` — the router's matcher

[`governor/scheduler.py`](../../src/concursus/governor/scheduler.py) supplies the `router` node's decision logic. At dispatch it matches each ready frontier step to a *standing* agent (via the read-only [`AgentRegistry`](#agentregistry--agentversion--the-process-table)), reads that agent's **earned** trust off a GOV-side ladder, and decides — per decision — one of three actions:

| Action (module constant) | Meaning |
|---|---|
| `DISPATCH` (`"dispatch"`) | Cleared: earned trust meets the bar — propose to compile next. |
| `ESCALATE` (`"escalate"`) | Below bar (or `require_approval` on a side-effecting agent) — held, escalated (e.g. L1→L3), not dispatched this round. |
| `UNMATCHED` (`"unmatched"`) | No standing agent serves the step — needs provision. An unmatched node blocks the frontier forever. |

```python
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

The two value types it produces are frozen:

- `ScheduleDecision(node, action, agent=None, version=None, grade=None, bar=None, escalated_to=None, reason="")` — one per-decision outcome. `.to_dict()` serializes `TrustGrade` fields to their `.name`.
- `FrontierProposal(compile_next=(), escalated=(), unmatched=(), decisions=())` — the frontier partition the router hands forward as **input to the next `recompile`**. It is a pure value; it never mutates a plan.

The router's cleared frontier is no longer a dead end: `OrchestrationAssembler.recompile(prior_plan, *, completed, …, compile_next=None)` accepts the cleared set (`FrontierProposal.compile_next`, or the DISPATCH nodes of `propose_bindings`) and **records** it onto the fresh plan's read-only `ProvisioningPlan.frontier` field (filtered to real topology nodes). This closes the previously-dead scheduler→compiler channel while preserving the strictly-outer invariant: `recompile(compile_next=…)` **never** changes `order`/`entries`/wiring — the monotonic superset is untouched, it merely carries *which* frontier the scheduler cleared this revision. `ProvisioningPlan.frontier` defaults to `[]` and is emitted in `to_dict()` **only when non-empty** (same pattern as `revision`), so a plan compiled with no scheduler frontier is byte-for-byte unchanged.

Core methods:

- `decide(node) -> ScheduleDecision` — resolve one ready step. Order of checks: no match → `UNMATCHED`; `require_approval` **and** side-effecting → `ESCALATE`; earned `grade < bar` → `ESCALATE`; else `DISPATCH`.
- `propose_frontier(plan, *, completed, ready=None) -> FrontierProposal` — reads `plan.order` (or an explicit `ready` set), skips `completed`, decides each node, and partitions. **Never writes `plan.order`**; never calls assemble/recompile.
- `update_trust(name, outcome) -> TrustGrade` — the **only** place trust is re-earned, and it lives GOV-side only. A clean outcome promotes one rung (capped at `escalation_grade`); a failing outcome demotes one rung (floored at `L0_SHADOW`).
- `seed_grade(name)` / `earned_grade(name)` — the earned grade, seeded lazily. The create-time [`evaluate_deploy_gate`](../reference/build.md) is consulted **at most once per agent** to establish live/shadow standing; thereafter the earned ladder is a GOV-side value, never the create-time gate.

The **bar**: non-side-effecting agents have a required bar of `L0_SHADOW` (always cleared) — escalation only bites side-effecting agents, whose bar is `min_autonomy`. `SchedulerError` is declared but not raised in the matcher itself.

```python
sched = TrustLadderScheduler(registry, manifests=manifests, min_autonomy=TrustGrade.L1_CANARY)
proposal = sched.propose_frontier(plan, completed=store.completed())
held = set(proposal.escalated) | set(proposal.unmatched)   # withheld this round
sched.update_trust("triager", {"ok": True})                # promote one rung after a clean episode
```

#### From a GATE to a BINDER

`decide`/`propose_frontier` are a first-match trust **gate**: they resolve a step to the *first* current version that serves it (`registry.match_task`), then dispatch-or-hold. Those methods are **unchanged**. An *additive* **binder** path picks from the **full** candidate set by trust priority — the evolution beyond first-match, off by default (you only reach it by calling the new methods; the gate path is untouched).

- `decide_ranked(node) -> Binding` — pulls the **full** candidate set via `registry.match_all(node)` (not first-match `match_task`), keeps the candidates that clear their own bar, and ranks them **best-earned-trust-first**, tie-breaking by least `load_fn(name)` (an optional availability/load signal) then agent name for determinism. Returns a `Binding`. If every capable agent is below bar → `ESCALATE`; if none serve → `UNMATCHED`. A pure **value**: it reads the read-only registry + earned ladder and mutates nothing.
- `propose_bindings(plan, *, completed, ready=None) -> Dict[node, Binding]` — the binder analogue of `propose_frontier`: reads `plan.order` (never writes it), skips `completed` nodes, and returns `{node: Binding}` over the ready frontier. Like `propose_frontier` it never calls assemble/recompile and never mutates a frozen plan (INV-3/INV-4).

The `TrustLadderScheduler.__init__(..., load_fn=None)` param wires the optional availability signal (`load_fn(agent_name) -> int`); a bad `load_fn` is swallowed and treated as `0`, so ranking stays deterministic. `Binding` is a `@dataclass(frozen=True)` — `node`, `action` (`DISPATCH | ESCALATE | UNMATCHED`), `agent`, `version`, `grade`, `bar`, `load`, `candidates` (the tuple of **all** capable agent names considered), and `reason`; `.to_dict()` renders `grade`/`bar` as `.name`.

```python
sched = TrustLadderScheduler(registry, manifests=manifests, load_fn=lambda name: inflight[name])
binding = sched.decide_ranked("triage")     # -> Binding over the FULL candidate set
binding.action                              # 'dispatch' — bound best-trust-first
binding.candidates                          # every capable agent name considered
bindings = sched.propose_bindings(plan, completed=store.completed())   # {node: Binding}
```

An `UNMATCHED` binding is exactly what the [Create arrow](#the-create-arrow--auto-spawn-an-unmatched-role-between-rounds) turns into an on-demand spawn.

#### The adaptive strictness dial — `make_trust_strictness`

The compiler and output-QA contract gates ship a `strict_fn`/`acceptance_fn` dial (`Optional[Callable[[str], bool]]`) that narrows a strict gate to just the nodes the predicate returns truthy for. `make_trust_strictness` builds that predicate off **the same Trust Ladder that governs autonomy** — realizing *strictness ∝ 1/strength*: weak/unproven agents get the strict deep gates, proven ones run the lean path.

```python
def make_trust_strictness(
    scheduler: "TrustLadderScheduler",
    *,
    strict_below: TrustGrade = TrustGrade.L2_GUARDED,
) -> Callable[[str], bool]
```

It returns a `node -> bool` predicate that is `True` (**apply** the strict contract) when the node's serving agent's **earned** trust is *below* `strict_below` — by default L0/L1, the WEAK rungs — and `False` (lean) for a proven agent at or above the bar. The grade is read live via `scheduler.earned_grade(node)` (author/compile-time, GOV-side). An **unknown / never-seeded** node — no evidence yet — is treated as WEAK and gets the strict contract (the conservative default). It is exported from `concursus.governor`. This is a pure predicate builder: it seeds nothing, dispatches nothing, and mutates no plan. Wire it into either contract gate; it is **opt-in** — a gate with no `strict_fn`/`acceptance_fn` applies uniformly, and a gate that is itself off (`strict_types=False`, `check_acceptance=False`) is unchanged.

```python
from concursus.governor import make_trust_strictness
from concursus import OrchestrationAssembler, Supervisor

is_strict = make_trust_strictness(scheduler)          # weak agents (< L2_GUARDED) -> strict
# Compile-time: deep type-align + single-writer, but only for weak/unproven nodes.
assembler = OrchestrationAssembler(strict_types=True, single_writer=True, strict_fn=is_strict)
# Run-time: the output-QA gate, likewise dialed to weak nodes only.
supervisor = Supervisor(..., check_acceptance=True, acceptance_fn=is_strict)
```

> **Gotcha — the ladder is keyed by agent NAME, not node id.** `decide`/`update_trust` key the earned ladder by the matched *agent name*. When `node != agent`, the caller (`GovernorLoop`'s collect step) resolves node→agent from the `FrontierProposal` decisions before calling `update_trust`, or the earned grade never moves. `require_approval=True` escalates *every* side-effecting matched agent regardless of how high its earned grade climbs.

> The scheduler module also ships a pure, opt-in `compute_schedule(state) -> Decision` core (`Decision`/`DeclinedNode` values with first-class `DECLINE_*` reasons) — a total, deterministic partition of an already-resolved frontier, with no registry/ladder/plan I/O. It is orthogonal to `decide`/`propose_frontier` (whose taxonomy is unchanged) and a payload-tier dial (`Tier`, `make_payload_tier`, `project_context`, `manifest_is_programmatic`) that maps earned trust to how much coaching context a node's payload carries. Both are default-off additions; see the [governor API reference](../reference/governor.md).

---

## Capability staffing — `staff_capability_dag` / `staff_with_rebind`

[`governor/authoring.py`](../../src/concursus/governor/authoring.py) is the **compiler front's** staffing step: it un-collapses *binding* from *authoring* so a bare capability DAG becomes an assemblable plan. Both functions are pure + offline (INV-2): they bind/author *values*, never dispatch, and never mutate a running plan. Import them from `concursus.governor.authoring`.

### `staff_capability_dag` — decompose → staff

```python
def staff_capability_dag(
    dag: "AgentDAG",
    *,
    bind_fn: Optional[Callable[[str], Optional[str]]] = None,
    manifest_author_fn: Optional[ManifestAuthorFn] = None,
    trust_seed: TrustGrade = TrustGrade.L0_SHADOW,
) -> Dict[str, AgentManifest]
```

A capability DAG (from `plan_from_goal(decompose=True)`) has agent-agnostic task nodes and edges but **no manifests and no `depends_on` wiring**, so it cannot be assembled directly. `staff_capability_dag` synthesizes, per node: a manifest keyed by the node id — bound to a standing agent via `bind_fn(node) -> agent-name-or-None`, else an authored `L0_SHADOW` skeleton — **plus** its data-wiring derived from the DAG edges (one input per upstream producer, fed by `"<producer>.result"`, with the matching `depends_on` edge). The result is a `{node: AgentManifest}` map ready for `OrchestrationAssembler.assemble(dag, …)`. Keying by node id keeps the frozen `plan.order` at the capability topology (the auditable artifact). `bind_fn=None` authors *every* node (via `author_manifest`) — the zero-bench cold-start path; a real binder reuses standing agents. This is the reusable core that `GovernorLoop(decompose=True)` calls on the [live path](#the-decomposebind-path--cold-start-with-zero-manifests).

```python
from concursus.governor.authoring import staff_capability_dag
from concursus import plan_from_goal, OrchestrationAssembler

dag = plan_from_goal("resolve ticket 42", decompose=True)   # capability DAG: nodes + edges, no manifests
manifests = staff_capability_dag(dag, bind_fn=bind_capability)  # -> {node: AgentManifest}
plan = OrchestrationAssembler().assemble(dag, manifests)     # freezes exactly like a hand-authored set
```

### `staff_with_rebind` — the compiler as a regulator

```python
def staff_with_rebind(
    dag: "AgentDAG",
    candidates_fn: CandidatesFn,
    *,
    assembler: Optional[OrchestrationAssembler] = None,
    max_rebinds: int = 8,
) -> Dict[str, AgentManifest]
```

Where `staff_capability_dag` binds once, `staff_with_rebind` **rejects-and-rebinds**: it is the compiler's *regulator* half. `candidates_fn(node) -> [AgentManifest, ...]` returns that node's candidates **best-first** (e.g. the scheduler's trust-ranked `decide_ranked` set). Starting from every node's first candidate it **strict-assembles** (`assembler` defaults to `OrchestrationAssembler(strict_types=True)`); on an `AlignmentError` it advances the **offending producer** (falling back to the consumer — the `AlignmentError.producer`/`.node` attributes let it target without message-parsing) to its next candidate and retries — a bounded author-time search capped by `max_rebinds`. Returns the type-aligning `{node: AgentManifest}` set, or raises `RebindExhausted` if no combination aligns within the bound. This is the "weak regulator → real regulator" fix: not just *validate* a bound team, but *search* for one that assembles. Author-time + offline: it is a bounded loop, never a compiler while-loop in the run (INV-2), and never dispatches or mutates a running plan (INV-1/INV-3).

- `RebindExhausted(ValueError)` — raised when no candidate combination aligns within `max_rebinds`.
- `CandidatesFn = Callable[[str], list]` — the ranked-candidates seam (node → best-first manifests).

```python
from concursus.governor.authoring import staff_with_rebind, RebindExhausted

def candidates(node: str) -> list:            # best-first AgentManifests per capability node
    return manifests_by_node[node]            # your ranked candidate list for this capability

try:
    manifests = staff_with_rebind(dag, candidates, max_rebinds=8)   # search for an aligning team
except RebindExhausted:
    ...                                        # no candidate combination type-aligns within the bound
```

---

## `AgentRegistry` & `AgentVersion` — the process table

[`governor/registry.py`](../../src/concursus/governor/registry.py) is the governor's **process table**: a strictly-outer, read-only view built *on top of* the shipped [`DeployLedger`](../reference/build.md). The ledger answers a create-time question ("have I already stood up this exact content?"); the registry answers the dispatch-time one: *"which standing agent, at which version, can do task X right now?"*

```python
@dataclass(frozen=True)
class AgentVersion:
    name: str
    fingerprint: str
    version: int                    # 1-based, first-appearance order
    arn: Optional[str] = None
    image_uri: Optional[str] = None
    role_arn: Optional[str] = None
    deployed_at: Optional[Any] = None
    capabilities: frozenset = field(default_factory=frozenset)

    def serves(self, task: str) -> bool: ...   # task in self.capabilities
```

`AgentRegistry(ledger, *, capability_fn=None)` is **read-only over the ledger**: every query re-reads `ledger.rows()` and never records. Capability metadata (which task labels an agent serves) is registry-side only and never written back into the ledger.

| Method | Returns |
|---|---|
| `register_agent(manifest, *, capabilities=None) -> Set[str]` | Teaches the registry which tasks a named agent serves (metadata only, no deploy). Raises `ValueError` if the manifest has no name. |
| `capabilities_for(name) -> Set[str]` | The registered labels for `name` (a fresh copy). |
| `versions(name) -> List[AgentVersion]` | All standing versions, oldest first. Each distinct fingerprint is one version. |
| `current(name) -> Optional[AgentVersion]` | The newest version (newest-row-wins), or `None`. |
| `process_table() -> Dict[str, AgentVersion]` | `name -> current version` for every agent. |
| `match_task(task) -> Optional[AgentVersion]` | The first **current** version that serves `task`. Older versions are never dispatched to. |
| `match_all(task) -> List[AgentVersion]` | Every current-version agent serving `task`. |
| `ensure_task(task, *, entry, clients, ...) -> AgentVersion` | Return the current version serving `task`, spawning one on demand via `provision_agent` if unmatched. |
| `fork(name, *, entry, clients, ...) -> AgentVersion` | Stand up a **new version** of an existing agent name on demand. |

Spawn/fork **do not write the ledger themselves** — they delegate to `provision_agent` (which owns the optional append); the registry re-reads afterward. `RegistryError` (a `RuntimeError`, defined after the class in the file but fully importable) is raised when a spawn/fork does not resolve to a standing ledger version.

```python
registry = AgentRegistry(DeployLedger(".concursus/deploy_ledger.json"))
registry.register_agent(manifest, capabilities={"triage", "summarize"})
av = registry.match_task("triage")     # -> AgentVersion or None
```

> **Gotcha.** The default capability of an agent is its own name (plus any `manifest.registry['capabilities']`), so a task named after the agent matches out of the box. `match_task` returns the first match in dict-iteration order — use `match_all` when several agents serve the same task and you need determinism. Capability changes are **not versioned**: `versions()` re-stamps every returned `AgentVersion` with the currently-registered capability set.

---

## `ScopeAddress` — the org→portfolio→program→task addressing scheme

[`governor/scope.py`](../../src/concursus/governor/scope.py) adds the scope stack *above* the single-run unit, plus read-only cross-program synthesis. The rest of the governor works at the grain of one run/episode (a `task`); this module rolls runs up by program.

```python
SCOPE_LEVELS = ("org", "portfolio", "program", "task")   # coarsest -> finest
SCOPE_SEP = "."                                          # trail_id separator
```

```python
@dataclass(frozen=True)
class ScopeAddress:
    org: str = ""
    portfolio: str = ""
    program: str = ""
    task: str = ""
```

`ScopeAddress` is a frozen value — `push(value)` returns a **new** address filling the next empty level (never mutates); it raises `ScopeError` (a `ValueError`) if the stack is already full. Levels fill top-down, so a partial address is a scope *prefix*.

| Method / function | Purpose |
|---|---|
| `ScopeAddress.from_trail_id(trail_id, *, sep=".")` | Parse a trail_id: first three segments → org/portfolio/program; the rest joins back into `task` (a task may itself contain `sep`). |
| `to_trail_id(*, sep=".")` | Join the set levels (trailing empties dropped) — inverse of `from_trail_id` for a full address. |
| `program_key(*, sep=".")` | The `org.portfolio.program` prefix (`""` for an ungrouped run with no org). |
| `depth()` / `to_dict()` | Levels set (0..4) / a JSON-serializable dict keyed by level. |
| `build_programs_index(vault_path, *, sep=".")` | Aggregate per-run precedent notes into a program-grain projection (pure, deterministic). |
| `render_programs_index(vault_path, *, sep=".", slipbox_form=False, date="")` | Render the cross-program hub to `<vault>/programs/_index.md`; returns the path. |
| `director_leverage_view(vault_path, *, sep=".")` | The 1:N leverage view: `{program_count, run_count, runs_per_program, status_counts, programs}`. |
| `programs_dir(vault_path)` | The `<vault>/programs/` tree (a sibling of `precedents/`). |

```python
addr = ScopeAddress.from_trail_id("acme.retail.oncall.ticket-42")
addr.program_key()   # 'acme.retail.oncall'
```

Everything here is a **read-only projection** over per-run precedent notes (INV-5): it selects nothing, seeds nothing, and drives no dispatch — it never calls `assemble()`/`recompile()`/`Supervisor.run()`/`StateStore.put()`, and regenerates from scratch each call (same notes → byte-identical output). `render_programs_index` *does* write the index file (`mkdir` + atomic write) — "read-only" means it never touches plan/store/dispatch, not that it never writes the index.

`scope.py` also declares the control-surface verb taxonomy (`READ_VERBS` / `ACTUATING_VERBS` / `RECURSIVE_VERBS`) and the compiled `ControlScope` authorization bound — see [`ControlSurface`](#controlsurface--the-opt-in-agent-facing-control-surface) below.

---

## `KTLODaemon` — the standing keep-the-lights-on loop

[`governor/ktlo.py`](../../src/concursus/governor/ktlo.py) is the strictly-outer layer *above* `GovernorLoop`: a continuous monitor that stays up, wakes on event arrival, triages each signal, auto-escalates, and — per triggered investigation — dispatches **one fresh bounded `GovernorLoop` episode**. Its conceptual loop is `monitor -> triage -> escalate -> (replan | close)`.

**Launch vs KTLO is a config, not two code paths:**

| Mode (constant) | Behavior | `terminated_by` |
|---|---|---|
| `LAUNCH` (`"launch"`) | One-shot: drain the source once, spawn episodes, stop. | `launch_complete` |
| `KTLO` (`"ktlo"`, default) | Standing: keep polling across ticks, surviving empty ticks, until drained and drift is quiet — bounded by `max_ticks`. | `source_drained` or `tick_cap` |

### Event sources

The `EventSource` Protocol is the live signal seam: `poll() -> List[dict]` (the batch since the last poll, `[]` on a quiet tick) and `drained() -> bool`. Two shipped implementations:

- `InProcessEventQueue(events=None, *, closed=True)` — a FIFO queue; `enqueue`, `close`. `drained()` is `True` once empty **and** closed.
- `ScriptedEventSource(batches)` — yields pre-scripted batches one per poll (e.g. `[[t1], [], [t2]]` proves the daemon survives the empty tick).

### Triage verbs

`triage_fn` maps a signal to one of:

| Verb (constant) | Meaning | Counter |
|---|---|---|
| `TRIAGE_CLOSE` (`"close"`) | Noise — dropped, no episode. | `events_closed` |
| `TRIAGE_INVESTIGATE` (`"investigate"`) | Real work — dispatch a bounded episode. | `events_investigated` |
| `TRIAGE_ESCALATE` (`"escalate"`) | High severity — flag **and** dispatch (a superset of investigate). | `escalations` + `events_investigated` |

### Construction & result

```python
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
    # ... assembler / supervisor / invoke seams ...
    max_ticks: int = 64,
    episode_max_rounds: int = 8,
    episode_no_progress_n: int = 2,
    max_revisions: int = DEFAULT_MAX_REVISIONS,
    backend: str = "python",
    scheduler: Optional[Any] = None,
    deliberate: bool = False,
) -> None
```

`run() -> KTLOResult` drives the bounded monitor loop. `KTLOResult` tallies `mode`, `ticks`, `terminated_by`, the ordered `episodes: List[GovernorResult]`, one distinct `episode_plans` entry per episode, the event counters (`events_seen/closed/investigated`, `escalations`, `drift_triggered`), per-episode `errors`, and `alive` (`False` once `run()` returns). `KTLODaemonError` (a `ValueError`) is raised on a bad mode, a missing source, `max_ticks < 1`, or an empty manifests map.

```python
from concursus.governor import KTLODaemon, InProcessEventQueue

source = InProcessEventQueue([{"id": "t1", "severity": "sev2"}], closed=True)
daemon = KTLODaemon(manifests, source=source, mode="launch")
result = daemon.run()          # terminated_by == 'launch_complete'
result.episodes                # [GovernorResult(...)]
```

Each investigation is a **fresh** `GovernorLoop` over a **fresh** store (`store_factory`), forming a brand-new frozen plan and terminating on its own bounds — N events → N independent, bounded, replayable-in-isolation episodes (INV-4). The daemon only *enqueues* episodes; it never reaches inside a running `Supervisor` (INV-1). A raising episode is caught, appended to `errors`, and the daemon **survives** to the next signal. Opt-in `scheduler`/`deliberate` are forwarded per-episode.

> **Gotcha.** `KTLODaemon`'s `backend` defaults to `"python"` (unlike `GovernorLoop`'s `"auto"`). `_default_triage_fn` checks the `noise` flag before severity; `severity` in `{sev1, sev2, high, critical, p0, p1}` → escalate, else investigate.

### The idle-runtime culler — reclaiming idle standing runtimes

A standing fleet accretes idle runtimes (a woken episode stands one up, the work drains, the runtime lingers). `IdleRuntimeCuller` (in [`governor/ktlo.py`](../../src/concursus/governor/ktlo.py)) answers **one** question, purely: given `{runtime -> last_active_ts}`, the wall clock `now_ts`, the in-flight `active` set, and a per-runtime `tiers` map, **which** idle runtimes are eligible to reclaim? It is **opt-in and default-OFF** — nothing on the standard daemon path constructs one, so the daemon is byte-for-byte unchanged unless a caller opts in — and it is **never** consulted inside `Supervisor.run` (INV-3): culling is a strictly-outer, between-episode housekeeping decision.

`cull(...)` **computes only** — it returns a `Set[str]`, performs **no teardown**, holds no runtime handles, and does no I/O. The caller tears the returned set down; a reclaimed runtime's identity persists in the durable `DeployLedger` (content-keyed), so it re-provisions on its next invoke. Two idle floors gate reclamation:

- **Two tiers, two floors.** A runtime whose tier is `CULL_TIER_STANDING` (`"standing"`) — or, when `protect_most_recent=True` (the default), the single most-recently-active runtime — is held to the **LONG** floor (`long_floor_s`); every other runtime (`CULL_TIER_EPHEMERAL`) is held to the **SHORT** floor (`short_floor_s`). `floor_for(runtime, tiers, *, most_recent=None)` is the pure read of which floor applies.
- **Defense 1 — never cull an in-flight runtime.** A runtime in the `active` set is untouchable regardless of its `last_active`.
- **Defense 2 — validate wall-clock elapsed, drift-safe.** A runtime is culled iff `now_ts - last_active_ts >= floor`; `elapsed < floor` **reschedules** it (kept, re-checked next sweep). A negative elapsed (a `last_active` stamped in the future by clock skew) is below any non-negative floor, so it **keeps** rather than reclaims.

`IdleRuntimeCuller(long_floor_s, short_floor_s, *, standing_tier="standing", protect_most_recent=True)` raises `KTLODaemonError` if either floor is negative.

```python
from concursus.governor.ktlo import (
    IdleRuntimeCuller, CULL_TIER_STANDING, CULL_TIER_EPHEMERAL,
)

culler = IdleRuntimeCuller(long_floor_s=3600.0, short_floor_s=300.0)
now = 10_000.0
last_active = {"std": now - 4000.0, "eph": now - 400.0, "busy": now - 5000.0}
tiers = {"std": CULL_TIER_STANDING, "eph": CULL_TIER_EPHEMERAL}

reclaim = culler.cull(last_active, now, active={"busy"}, tiers=tiers)
# 'busy' is in-flight -> never culled; 'std' (long floor) and 'eph' (short floor) both cleared theirs
for runtime in reclaim:
    ...   # caller tears it down; identity survives in the ledger for re-provision on next invoke
```

### Opt-in admission gates — `can_fire` fire budget

Alongside the culler, [`governor/ktlo.py`](../../src/concursus/governor/ktlo.py) ships a set of **standalone, default-OFF** admission gates a caller (or the daemon behind a default-`None` kwarg) can consult **before** dispatching an episode. None touches the default KTLO path: unless a caller constructs and passes one, `KTLODaemon` behaves byte-for-byte as before. Each gate that consumes budget persists its state through the append-only `StateStore` (the SSOT, offline by default), so it survives a resume via replay.

`FireBudgetGate` is a persisted, pure per-`(source, entity)` fire-budget gate that enforces a `cooldown_s` (minimum seconds between fires) and a `max_fires` cap:

- `can_fire(source_id, entity_ref, cooldown_s=0.0, max_fires=1, *, now=None) -> bool` is a **pure read** — it inspects the persisted cell and returns whether a fire is admissible; it **never** mutates state and **never** consumes budget, so it is idempotent and safe to call speculatively. It returns `False` when the cell has already committed `>= max_fires` fires, or when a positive `cooldown_s` has not elapsed since `last_fired`. `max_fires=None` disables the cap (cooldown-only); `cooldown_s=0` disables the cooldown.
- `commit_fire(source_id, entity_ref, *, now=None)` records **one** consumed fire — call it **only after** the caller's own durable commit (the episode actually dispatched), so an episode that never durably happened (raised, was rejected) never burns budget.
- `fires(source_id, entity_ref) -> int` is the pure read of how many fires the cell has committed.

```python
from concursus.governor.ktlo import FireBudgetGate
from concursus.state.statestore import InProcessStateStore

budget = FireBudgetGate(InProcessStateStore())   # offline default store; pass any StateStore
if budget.can_fire("pager", "ticket-42", cooldown_s=300.0, max_fires=3):
    # ... do the durable work (dispatch the episode) ...
    budget.commit_fire("pager", "ticket-42")      # ONLY after the durable commit succeeds
# a second can_fire now reflects the committed fire (cooldown / cap enforced)
```

The sibling gates — `ProvenanceGuard` (drop self-triggered events the fleet itself emitted) and `EpisodeAdmissionGate` (dedup by `DetectionMode`: `new_items` / `state_change` / `diff`) — follow the same **pure-read `admit` + separate `commit`** discipline; see the [governor API reference](../reference/governor.md).

---

## `DirectorCockpit` — the human-as-director surface

[`governor/cockpit.py`](../../src/concursus/governor/cockpit.py) is a thin, read-only projection over already-shipped read models. It composes several director surfaces from nothing but `query`/`summary`/`render*` calls — it never calls `assemble()`, `Supervisor.run()`, or `StateStore.put()`.

```python
def __init__(self, *, supervisor, vault_path=None, plan=None,
             escalated=None, unmatched=None) -> None
```

| Method | Returns |
|---|---|
| `briefing(*, slipbox_form=False, date="") -> Dict` | `{summary, summary_line, precedent_hub, revision}`. Renders the idempotent precedent hub only when `vault_path` is set. |
| `exception_queue() -> List[Dict]` | The failed nodes (from `summary()['failed']`, enriched with the latest failed `Record`), **plus** one row per `escalated` (`reason='escalated'`) and per `unmatched` (`reason='unmatched'`) node when governance sets were handed in. |
| `runs_monitor() -> Dict` | Plan `revision` + progress over run-index metadata (`total`, `completed`, `failed_count`, `completed_nodes`, `indexed_nodes`, `record_count`, `order`). |
| `snapshot()` / `follow(from_offset)` | A point-in-time replay of the append-only log ordered by the store `seq`, then a loss-free tail of records appended after an offset (snapshot-then-follow, no full reconcile). |
| `family_tree() -> Dict` | The frozen `AgentDAG` (`plan.order` + `plan.wiring`) rendered as a lineage tree, each node colored `done`/`failed`/`running`/`pending` from the log. |

The opt-in governance sets are just **values** passed in at construction — the cockpit never re-derives, assembles, or dispatches to obtain them. Normally you get the cockpit from `GovernorLoop.cockpit()`, which injects the loop's store-bound `Supervisor`, the final frozen plan, and the last run's escalated/unmatched sets. `revision` is `None` when no plan value was handed in (before the first `run`). With no governance sets (the default), `exception_queue()` is exactly today's failed-only queue.

---

## `ControlSurface` — the opt-in agent-facing control surface

Where `DirectorCockpit` gives a **human** director read-only projections, `ControlSurface` (also in [`governor/cockpit.py`](../../src/concursus/governor/cockpit.py)) gives a governed **agent** a narrow, **in-process** (not HTTP) handle over the same single source of truth. It is **opt-in, read-mostly, and offline-by-default**: it holds the same read-only `supervisor`/`plan` the cockpit does, re-derives every read from the log on each call, cannot mutate the frozen plan, and pulls no boto3/langgraph at import. Every actuating capability is **default-off**; the compiler-not-governor framing is preserved — actuation routes through existing actuators, never new side-effect logic on the surface.

Verbs split by blast radius, defined in [`governor/scope.py`](../../src/concursus/governor/scope.py):

- **Read verbs** (`READ_VERBS = {"query_plan", "tail_log", "search_runs", "precedents"}`) are **always on** — pure projections over the frozen plan, the append-only log, the run-db FTS, and the durable precedents. They mutate nothing and need no authorization.
- **Actuating verbs** (`ACTUATING_VERBS = {"deploy", "run", "recompile"}`) route **through the existing actuators only** (`deploy → provision`, `run → Supervisor.run`, `recompile → assembler`; `recompile` is additionally recursive). Their availability is resolved from the compiled `ControlScope` (from the frozen plan/scope), **not an env var**.

### Which verbs appear

`verbs()` returns the always-on read verbs plus whatever actuating verbs the compiled scope authorized; `has_verb(verb)` tests one. Authorization is by **non-registration**: a verb the compiled `ControlScope` did not authorize is simply **absent** — there is no disabled/deny stub to bypass. A default `ControlScope.from_plan(plan)` authorizes **no** actuating verb, so the surface is fully read-only.

```python
from concursus.governor.cockpit import ControlSurface, ControlSurfaceError
from concursus.governor.scope import ControlScope

# Authorize 'run' + 'recompile' but NOT 'deploy'; compile a trust ceiling of L1 (=1).
scope = ControlScope.from_plan(plan, authorize=["run", "recompile"], trust_ceiling=1)
surface = ControlSurface(supervisor=sup, scope=scope, plan=plan)   # no actuators wired -> read-only

surface.verbs()             # ['precedents','query_plan','recompile','run','search_runs','tail_log']
surface.has_verb("deploy")  # False — omitted by the scope, so absent (not a deny stub)
surface.query_plan()        # READ (always on): {'order': [...], 'revision': ...}
```

### Activating an actuating verb

A present actuating verb is guarded twice more. First, `activate(verb)` must **explicitly arm** it (default off); arming a verb the scope did not authorize — or a non-actuating verb — raises `ControlSurfaceError`. Second, the verb is only actually invocable when an **actuator thunk was injected** for it (`actuators={verb: callable}`); with none wired, `invoke` raises (offline-by-default). `invoke(verb, *args, **kwargs)` enforces, in order: non-registration → activation → trust clamp, then calls the injected thunk. Read verbs are not routed through `invoke` (they are pure methods).

```python
def deploy_actuator(*args, **kwargs):   # your existing actuator (offline test seam)
    return "deployed"

scope = ControlScope.from_plan(plan, authorize=["deploy"], trust_ceiling=1)   # ceiling L1
surface = ControlSurface(
    supervisor=sup, scope=scope, plan=plan,
    actuators={"deploy": deploy_actuator},   # inject the existing actuator thunk
)

surface.invoke("deploy")            # raises ControlSurfaceError — not activated yet
surface.activate("deploy")          # explicitly arm the dangerous verb
surface.is_active("deploy")         # True
surface.invoke("deploy", "node-x", trust="L3_AUTONOMOUS")   # routed through deploy_actuator
```

### Trust clamping

When an `invoke` carries a `trust` (or `requested_trust`) kwarg, the surface clamps it **down** to the compiled ceiling before the actuator sees it, via `effective_trust(requested)` → `clamp_trust_grade(ceiling, requested)`. The compiled `TrustGrade` is a **monotonic ceiling**: a surface may voluntarily opt down to a more cautious grade, but can **never** escalate above the compiled grade — the effective grade is `min(compiled, requested)`. With no compiled ceiling on the scope (`trust_ceiling=None`), the requested grade passes through and the activation gate is the sole guard.

```python
from concursus.build.trust import TrustGrade

# scope compiled with trust_ceiling=1 (L1_CANARY):
surface.effective_trust(TrustGrade.L3_AUTONOMOUS)   # -> TrustGrade.L1_CANARY (clamped down)
surface.effective_trust(TrustGrade.L0_SHADOW)       # -> TrustGrade.L0_SHADOW (already below)
# so invoke("deploy", ..., trust=L3_AUTONOMOUS) hands the actuator a CLAMPED L1_CANARY, never L3.
```

---

## End to end: one governed round

This sketch wires all the parts together — process table, trust ladder, the outer loop, and the read-only director surface — for a single bounded, governed episode. Every symbol below is real public API.

```python
from concursus.build.ledger import DeployLedger
from concursus.build.trust import TrustGrade
from concursus.governor import AgentRegistry, TrustLadderScheduler, GovernorLoop

# 1. Process table: a read-only, versioned view over the deploy ledger.
registry = AgentRegistry(DeployLedger(".concursus/deploy_ledger.json"))
for manifest in manifests.values():           # manifests: Dict[str, AgentManifest]
    registry.register_agent(manifest)

# 2. The GOV-side router matcher: match by trust x availability, escalate below-bar.
scheduler = TrustLadderScheduler(
    registry,
    manifests=manifests,
    min_autonomy=TrustGrade.L1_CANARY,
)

# 3. The strictly-outer bounded loop. Each round forms a FRESH frozen plan and
#    runs ONE Supervisor episode over it — never mutating a plan mid-flight.
loop = GovernorLoop(
    "resolve ticket 42",
    manifests,
    scheduler=scheduler,     # opt-in: without it, the router is a pass-through
    backend="python",
    max_rounds=8,
    no_progress_n=2,
)
result = loop.run({"ticket_id": 42})

print(result.terminated_by)  # e.g. 'frontier_exhaust'
print(result.rounds, result.supervisor_runs)   # one Supervisor.run per round
print(result.escalated)      # nodes the trust ladder HELD below-bar this run
print(result.unmatched)      # nodes with no standing agent (blocked the frontier)

# 4. Read the run as a director — a PURE read surface, no re-run, no dispatch.
cockpit = loop.cockpit()
for row in cockpit.exception_queue():
    print(row["node"], row["reason"])   # 'escalated' / 'unmatched' / a failure reason
```

What happened, round by round: `planner` formed a frozen `ProvisioningPlan`; `router` asked the scheduler to partition the ready frontier (dispatch vs held-below-bar vs unmatched); `run_episode` ran the `Supervisor` **once** over the frozen plan, skipping held nodes without invoking them or writing the log; `collect` folded outputs into the append-only log and re-earned trust GOV-side; `route_after_collect` decided — bounded — to loop again or synthesize. Across all of it the frozen plan was never mutated and the loop never reached inside the running `Supervisor`. That is the strictly-outer invariant in practice.

To keep the lights on continuously, wrap the same machinery in a [`KTLODaemon`](#ktlodaemon--the-standing-keep-the-lights-on-loop): it monitors a live `EventSource`, triages each signal, and dispatches one fresh bounded `GovernorLoop` episode per investigation — the loop above, once per triaged signal.

---

## Where this fits

- **Create-time gate vs runtime ladder.** The governor's `TrustLadderScheduler` re-earns trust GOV-side *between rounds*; it is not the create-time deploy gate. That gate — `evaluate_deploy_gate` grading a `TrustGrade` seed into live/shadow/hold — is a separate, once-per-node decision documented in the [build reference → Trust Ladder](../reference/build.md). The scheduler *reads* that gate exactly once per agent to seed the ladder, then never again.
- **Reasoning happens strictly before assemble.** When `deliberate=True`, round 1 authors the DAG via bounded DKS deliberation *before* the plan is frozen — see the [Reasoning guide](reasoning.md). The governor loop is still strictly outer: deliberation converges to a frozen `AgentDAG`, then `assemble` runs exactly as before.
- **Session-end memory transfer.** When the loop reaches `synthesize`, an opt-in `TransferTriggerSink` (composed via `FanOutEventSink`) flows the run's episodic memory out to the permanent Slipbox — strictly outer, at the boundary, never inside a running `Supervisor` (INV-1/INV-3). See [Composing sinks](#composing-sinks--the-session-end-transfer-trigger) and the [Knowledge Transfer guide](knowledge-transfer.md).
- **Full symbol catalog.** [governor API reference](../reference/governor.md).

See also: [Core Concepts](../concepts.md) · [Compiling & Running](compiling-and-running.md) · [Durable Run State](durable-state.md) · [Knowledge Transfer](knowledge-transfer.md) · [Overview](../overview.md) · [docs index](../README.md).
