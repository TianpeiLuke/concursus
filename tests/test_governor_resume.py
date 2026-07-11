"""Tests for the governor's DUAL RESUME at two altitudes (G-4).

The governor crashes and restarts at two altitudes that must compose:

* OUTER (round altitude): the loop persists a plain-dict CHECKPOINT of ``plan_version`` +
  ``iteration`` + ``no_progress`` + a POINTER to the current frozen plan (the shared append-only
  log), keyed by a run id.  The checkpoint NEVER holds a mutable plan snapshot — on restart the
  plan is RE-FETCHED by version by deterministically replaying the compiler front against the
  surviving log.  So a crashed governor resumes at the correct ROUND with the right frozen-plan
  revision (INV-3/INV-4/INV-5).
* INNER (node altitude): within a round :meth:`Supervisor.run` already resumes by replaying the
  append-only log against THAT round's frozen plan (``completed()``-skip) — reused verbatim, so
  nodes already committed to the log are NOT re-invoked (INV-1).

Everything is offline: a fake partial supervisor that mirrors the store-bound seam +
:class:`InProcessStateStore` + :class:`InProcessCheckpointStore`, no AWS touched.
"""

from concursus import (
    AgentManifest,
    GovernorLoop,
    GovernorResult,
)
from concursus.governor.loop import CheckpointStore, InProcessCheckpointStore
from concursus.state.statestore import InProcessStateStore


def _manifest(name, *, inputs=None, depends_on=None):
    return AgentManifest.from_dict(
        {
            "name": name,
            "registry": {
                "container_uri": "acct.dkr.ecr.us-east-1.amazonaws.com/agents:latest",
                "protocol": "HTTP",
                "entry": f"agents.{name}:run",
                "role_arn": "arn:aws:iam::123456789012:role/agent",
            },
            "contract": {
                "inputs": inputs or {},
                "outputs": {"doc": {"type": "string", "required": True}},
            },
            "spec": {"depends_on": depends_on or []},
        }
    )


def _plan_model_fn(goal, precedents, directives):
    """Inject a three-node linear topology so the manifests' depends_on edges type-align."""
    return {"nodes": ["a", "b", "c"], "edges": [["a", "b"], ["b", "c"]]}


def _three_node_manifests():
    return {
        "a": _manifest("a"),
        "b": _manifest(
            "b",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "a.doc", "to": "document"}],
        ),
        "c": _manifest(
            "c",
            inputs={"document": {"type": "string", "required": True}},
            depends_on=[{"from": "b.doc", "to": "document"}],
        ),
    }


class _PartialRecordingSupervisor:
    """A store-bound fake that completes exactly ONE new node per round and RECORDS every node it
    actually processes as new work.

    Mirrors the real store-bound seam: it INNER-replays by skipping nodes already in
    ``store.completed()`` (never re-invoking them) and writes through to the shared store, so
    ``invoked`` is the ground truth of which nodes did fresh work this process.
    """

    invoked = []  # class-level ledger of nodes processed as NEW work (never the skipped prefix)
    run_count = 0

    def __init__(self, *, plan, manifests, store, invoke_fn, arns, session_id):
        self._plan = plan
        self._store = store

    def run(self, inputs):
        type(self).run_count += 1
        already = set(self._store.completed())
        for node in self._plan.order:
            if node in already:
                continue  # INNER replay: a committed node is skipped, never re-invoked
            type(self).invoked.append(node)
            self._store.put(node, {"doc": f"{node}-out"})
            break  # complete exactly ONE new node this round
        return {n: self._store.get(n) for n in self._plan.order if n in self._store.completed()}


def _fresh_partial():
    _PartialRecordingSupervisor.invoked = []
    _PartialRecordingSupervisor.run_count = 0
    return _PartialRecordingSupervisor


def test_dual_resume_outer_version_inner_replay():
    """Crash after 1 node of round 2, restore from checkpoint: plan_version preserved and committed
    nodes are skipped on replay (no re-invoke of committed nodes)."""
    manifests = _three_node_manifests()
    store = InProcessStateStore()          # the SOLE structural anchor, survives the crash
    checkpointer = InProcessCheckpointStore()  # the OUTER round-altitude pointer, survives the crash

    # --- process 1: run two rounds, then "crash" (round_cap stands in for a mid-run kill). One new
    # node completes per round: 'a' in round 1, 'b' in round 2 => the crash lands after 1 node of
    # round 2, with 'a','b' committed to the log.
    fake1 = _fresh_partial()
    loop1 = GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        checkpointer=checkpointer,
        supervisor_factory=lambda **kw: fake1(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=2,          # forces the "crash" after round 2
        no_progress_n=3,
        run_id="run-1",
        backend="python",
    )
    r1 = loop1.run({"uri": "s3://doc"})
    assert r1.terminated_by == "round_cap"        # stopped mid-journey, frontier not exhausted
    assert r1.done is False
    assert set(store.completed()) == {"a", "b"}   # exactly two nodes committed before the crash
    assert fake1.invoked == ["a", "b"]            # each did fresh work in this process

    # The OUTER checkpoint captured the ROUND altitude: plan_version + iteration, NOT a plan object.
    ckpt = checkpointer.load("run-1")
    assert ckpt is not None
    assert ckpt["plan_version"] == 1              # revision 0 (assemble) + 1 recompile
    assert ckpt["iteration"] == 1
    assert ckpt["round"] == 2

    # --- process 2: a BRAND-NEW loop instance over the SAME store + checkpointer resumes.
    fake2 = _fresh_partial()                       # fresh ledger => proves nothing re-invokes here
    loop2 = GovernorLoop(
        "summarize the document",
        manifests,
        store=store,
        checkpointer=checkpointer,
        supervisor_factory=lambda **kw: fake2(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        run_id="run-1",
        backend="python",
    )
    r2 = loop2.run({"uri": "s3://doc"})

    # OUTER resume: the plan_version was PRESERVED across the crash (re-fetched by version, never
    # reset to 0) and continued monotonically for the one remaining round (+1 for 'c').
    assert isinstance(r2, GovernorResult)
    assert r2.state.plan_history[0].revision == 0
    assert r2.state.plan_version == ckpt["plan_version"] + 1 == 2
    assert [p.revision for p in r2.state.plan_history] == [0, 1, 2]

    # INNER replay: the already-committed nodes 'a','b' were SKIPPED — only the open node 'c' did
    # fresh work in the resumed process. No re-invoke of committed nodes.
    assert fake2.invoked == ["c"]

    # The journey completed: the frontier exhausted over the resumed rounds.
    assert r2.terminated_by == "frontier_exhaust"
    assert r2.done is True
    assert set(r2.completed) == {"a", "b", "c"}


def test_checkpoint_holds_version_and_log_pointer_not_plan_snapshot():
    """The persisted checkpoint holds plan_version + iteration + no_progress (+ round bookkeeping)
    but NEVER a ProvisioningPlan object — the plan is re-fetched by version (INV-3/INV-5)."""
    from concursus.assemble.assemble import ProvisioningPlan

    fake = _fresh_partial()
    checkpointer = InProcessCheckpointStore()
    loop = GovernorLoop(
        "summarize the document",
        _three_node_manifests(),
        store=InProcessStateStore(),
        checkpointer=checkpointer,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        run_id="rid",
        backend="python",
    )
    loop.run({"uri": "s3://doc"})

    ckpt = checkpointer.load("rid")
    assert ckpt is not None
    # It is a plain pure-python dict of scalars — a version + log POINTER, not a plan snapshot.
    assert set(ckpt) == {
        "plan_version",
        "iteration",
        "round",
        "no_progress",
        "prev_completed",
        "replan_reason",
    }
    assert isinstance(ckpt["plan_version"], int)
    assert isinstance(ckpt["iteration"], int)
    # No value is (or contains) a frozen ProvisioningPlan — the plan is re-fetched by version.
    for value in ckpt.values():
        assert not isinstance(value, ProvisioningPlan)


def test_no_checkpointer_no_resume_default():
    """Absent a checkpointer the loop neither persists nor resumes — default behavior is unchanged."""
    fake = _fresh_partial()
    store = InProcessStateStore()
    loop = GovernorLoop(
        "summarize the document",
        _three_node_manifests(),
        store=store,
        supervisor_factory=lambda **kw: fake(**kw),
        plan_model_fn=_plan_model_fn,
        max_rounds=8,
        no_progress_n=3,
        backend="python",
    )
    # _maybe_restore is a no-op with no checkpointer (a fresh ctx is untouched).
    ctx = loop._initial_ctx({})
    loop._maybe_restore(ctx)
    assert ctx["state"] is None and ctx["round"] == 0

    result = loop.run({"uri": "s3://doc"})
    assert result.done is True
    assert result.terminated_by == "frontier_exhaust"
    assert set(result.completed) == {"a", "b", "c"}
    assert fake.invoked == ["a", "b", "c"]  # a clean single run invokes each node exactly once
