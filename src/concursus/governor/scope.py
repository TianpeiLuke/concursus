"""Program/portfolio scope stack above the single-run unit (S12-G9).

The rest of the governor operates at the grain of ONE run/episode: a run has a
``trail_id``, distills into ONE precedent note, and
:func:`~concursus.state.distill.render_precedent_hub` rolls the SET of those
per-run notes into a cross-run RUNS index. This module adds the layer *above*
the single run: an ``org -> portfolio -> program -> task`` scope stack, a
cross-program memory synthesis at PROGRAM grain (a programs index — the
program-grain analogue of the runs-grain precedent hub), and a 1:N *leverage*
view (one director over many programs, each hosting many KTLO episodes).

INV-5 (memory seam): scope is PURE GOV aggregation over READ MODELS — no
compiler impact. Everything here is a READ-ONLY projection over the per-run
precedent notes (the single source of truth, loaded via
:func:`~concursus.state.distill.load_precedents`). Like ``render_precedent_hub``
it SELECTS nothing, SEEDS nothing, and DRIVES no dispatch: it never calls
``assemble()`` / ``recompile()`` / ``Supervisor.run()`` / ``StateStore.put()``,
holds no mutable executed-prefix cache, and is regenerated from scratch each
call (same notes -> byte-identical output). Deleting the programs index loses
nothing — this rebuilds it from the notes.

Pure-Python, stdlib only.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from concursus.state.distill import load_precedents
from concursus.state.filevault import FileVaultStateStore, _SLIPBOX_TOPICS

# The ordered scope stack, coarsest -> finest. A run/episode is a ``task``.
SCOPE_LEVELS = ("org", "portfolio", "program", "task")

# The trail_id addressing separator. A trail_id is a scope address: its
# segments fill the levels top-down (org first); a bare token is a task-less
# ``org``. ``to_trail_id`` / ``from_trail_id`` are inverse for a full address.
SCOPE_SEP = "."

# The programs index lives in its OWN tree — a sibling of ``<vault>/precedents/``
# and ``<vault>/runs/`` — so this program-grain projection never pollutes the
# per-run precedent read model that ``load_precedents`` globs.
_PROGRAMS_DIRNAME = "programs"
_PROGRAMS_INDEX_NAME = "_index.md"


class ScopeError(ValueError):
    """Raised for a malformed scope operation (e.g. an over-deep push)."""


@dataclass(frozen=True)
class ScopeAddress:
    """A point in the ``org -> portfolio -> program -> task`` scope stack.

    A frozen VALUE: :meth:`push` returns a NEW address (never mutates), giving
    stack semantics without shared mutable state. Levels fill top-down, so a
    partial address (e.g. only ``org``/``portfolio`` set) is a scope PREFIX that
    many programs/tasks live under.
    """

    org: str = ""
    portfolio: str = ""
    program: str = ""
    task: str = ""

    # ---- construction ------------------------------------------------
    @classmethod
    def from_trail_id(cls, trail_id: str, *, sep: str = SCOPE_SEP) -> "ScopeAddress":
        """Parse a ``trail_id`` scope address, filling levels top-down.

        The first three ``sep``-segments map to ``org``/``portfolio``/``program``;
        any remaining segments join back into ``task`` (so a task may itself
        contain ``sep``). Fewer than four segments leave the deeper levels empty.
        Inverse of :meth:`to_trail_id` for a full four-level address.
        """
        parts = str(trail_id or "").split(sep)
        org = parts[0] if len(parts) > 0 else ""
        portfolio = parts[1] if len(parts) > 1 else ""
        program = parts[2] if len(parts) > 2 else ""
        task = sep.join(parts[3:]) if len(parts) > 3 else ""
        return cls(org=org, portfolio=portfolio, program=program, task=task)

    def push(self, value: str) -> "ScopeAddress":
        """Return a NEW address with ``value`` filling the next empty level.

        org -> portfolio -> program -> task. Raises :class:`ScopeError` if the
        stack is already full (task set)."""
        for level in SCOPE_LEVELS:
            if not getattr(self, level):
                from dataclasses import replace

                return replace(self, **{level: value})
        raise ScopeError(
            f"scope stack is full ({SCOPE_LEVELS[-1]!r} already set); cannot push {value!r}"
        )

    # ---- keys / rendering --------------------------------------------
    def _levels(self) -> List[str]:
        """The set level values, top-down, stopping at the first empty (drops
        trailing empties so a partial address renders as a clean prefix)."""
        out: List[str] = []
        for level in SCOPE_LEVELS:
            value = getattr(self, level)
            if not value:
                break
            out.append(value)
        return out

    def to_trail_id(self, *, sep: str = SCOPE_SEP) -> str:
        """Join the set levels (dropping trailing empties) into a trail_id."""
        return sep.join(self._levels())

    def program_key(self, *, sep: str = SCOPE_SEP) -> str:
        """The PROGRAM-grain key: the ``org.portfolio.program`` prefix (trailing
        empties dropped). Runs sharing this key belong to the same program.
        ``""`` for an address with no ``org`` (an ungrouped run)."""
        return sep.join(self._levels()[:3])

    def depth(self) -> int:
        """How many levels are set (0..4)."""
        return len(self._levels())

    def to_dict(self) -> Dict[str, str]:
        """The address as a plain, JSON-serializable dict keyed by level."""
        return {level: getattr(self, level) for level in SCOPE_LEVELS}


# --------------------------------------------------------------------------- program-grain synthesis
def build_programs_index(vault_path, *, sep: str = SCOPE_SEP) -> Dict[str, dict]:
    """Aggregate the per-run precedent notes into a PROGRAM-grain projection.

    Reads the SET of per-run precedent notes (via :func:`load_precedents` — the
    single source of truth), maps each run's ``trail_id`` to a
    :class:`ScopeAddress`, and rolls the runs up by :meth:`ScopeAddress.program_key`.
    Returns a dict keyed by program_key; each value carries the program's scope
    coordinates, its status tallies, and its sorted member runs. Pure function;
    no I/O beyond reading the notes, no plan/store access, deterministic.
    """
    index: Dict[str, dict] = {}
    for record in load_precedents(vault_path):
        payload = record.output if isinstance(record.output, dict) else {}
        trail_id = str(payload.get("trail_id") or record.node)
        addr = ScopeAddress.from_trail_id(trail_id, sep=sep)
        key = addr.program_key(sep=sep)
        status = str(payload.get("status") or "")
        entry = index.get(key)
        if entry is None:
            entry = {
                "program_key": key,
                "org": addr.org,
                "portfolio": addr.portfolio,
                "program": addr.program,
                "runs": [],
                "run_count": 0,
                "status_counts": {},
            }
            index[key] = entry
        entry["runs"].append(trail_id)
        if status:
            entry["status_counts"][status] = entry["status_counts"].get(status, 0) + 1
    # Finalize: sort member runs + run_count deterministically.
    for entry in index.values():
        entry["runs"] = sorted(entry["runs"])
        entry["run_count"] = len(entry["runs"])
    return index


def programs_dir(vault_path) -> Path:
    """The dedicated ``<vault>/programs/`` tree (sibling of ``precedents/``)."""
    return Path(vault_path) / _PROGRAMS_DIRNAME


def render_programs_index(
    vault_path, *, sep: str = SCOPE_SEP, slipbox_form: bool = False, date: str = ""
) -> str:
    """Render the cross-program memory hub (``<vault>/programs/_index.md``); path.

    The program-grain analogue of
    :func:`~concursus.state.distill.render_precedent_hub`: a pure, idempotent
    READ-ONLY projection over the per-run precedent notes, one section per
    program (keyed + sorted by program_key), regenerated from scratch each call
    (same notes -> byte-identical output). It is a retrieval index, NOT a live
    router/scheduler: it selects no run and seeds nothing.
    """
    index = build_programs_index(vault_path, sep=sep)

    lines: List[str] = []
    if slipbox_form:
        fm = {
            "tags": ["resource", "concursus", "run_state", "entry_point"],
            "keywords": [
                "concursus program index",
                "cross-program synthesis",
                "portfolio scope stack",
            ],
            "topics": _SLIPBOX_TOPICS,
            "language": "markdown",
            "date of note": date,
            "status": "active",
            "building_block": "navigation",
            "folgezettel": "1",
            "lineage": ["concursus_programs:1"],
            "access_control_group": ["general"],
        }
        import json as _json

        lines.append("---")
        for key, value in fm.items():
            if isinstance(value, list):
                lines.append(f"{key}:")
                lines.extend(f"  - {_json.dumps(v)}" for v in value)
            else:
                lines.append(f"{key}: {_json.dumps(value)}")
        lines.append("---")
        lines.append("")

    lines.append("# Concursus Programs Index")
    lines.append("")
    lines.append(
        "Cross-program memory synthesis at PROGRAM grain — one section per program "
        "(``org.portfolio.program``), each rolling up the runs it hosts. A read-only "
        "projection regenerated from the per-run precedent notes under `precedents/` "
        "(the single source of truth); it selects and seeds nothing."
    )
    lines.append("")

    if index:
        for key in sorted(index):
            entry = index[key]
            counts = entry["status_counts"]
            digest = ", ".join(f"{s} {counts[s]}" for s in sorted(counts)) or "no status"
            lines.append(f"## {key}")
            lines.append("")
            lines.append(f"{entry['run_count']} run(s) — {digest}")
            for run in entry["runs"]:
                lines.append(f"- {run}")
            lines.append("")
    else:
        lines.append("- (no programs synthesized yet)")
        lines.append("")

    prog_dir = programs_dir(vault_path)
    prog_dir.mkdir(parents=True, exist_ok=True)
    path = prog_dir / _PROGRAMS_INDEX_NAME
    FileVaultStateStore._atomic_write(path, "\n".join(lines))
    return str(path)


def director_leverage_view(vault_path, *, sep: str = SCOPE_SEP) -> Dict[str, object]:
    """The 1:N leverage view — one director over many programs, many episodes.

    A read-only synthesis over :func:`build_programs_index`: the count of
    programs a director spans, the total runs/episodes hosted across them, the
    per-program run counts, and a cross-program status rollup. Selects nothing,
    seeds nothing, drives no dispatch.
    """
    index = build_programs_index(vault_path, sep=sep)
    runs_per_program: Dict[str, int] = {}
    status_counts: Dict[str, int] = {}
    run_count = 0
    for key in sorted(index):
        entry = index[key]
        runs_per_program[key] = entry["run_count"]
        run_count += entry["run_count"]
        for status, count in entry["status_counts"].items():
            status_counts[status] = status_counts.get(status, 0) + count
    return {
        "program_count": len(index),
        "run_count": run_count,
        "runs_per_program": runs_per_program,
        "status_counts": status_counts,
        "programs": sorted(index),
    }
