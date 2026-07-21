"""Build-enforced API-surface snapshot — any un-reviewed change to the public surface fails.

Mirrors KiRoom's ``api-surface.snapshot.json`` guard. The public surface of the compiler is
(1) the re-exported names in :data:`concursus.__all__` and (2) the public field names of
the :class:`~concursus.core.manifest.AgentManifest` dataclass (the ``.agent.yaml`` model).
We serialize that surface and assert it equals a COMMITTED snapshot
(``tests/api_surface.snapshot.json``), so adding/removing an export or a manifest field is a
diff a reviewer must consciously bless by regenerating the snapshot.

Regenerate the committed snapshot from the CURRENT surface with::

    PYTHONPATH=src python3.11 tests/test_api_surface.py

This is a pure introspection test — it imports the package (offline, no boto3/langgraph) and reads
its ``__all__`` / dataclass fields; it touches no source and changes no default code path.
"""

from __future__ import annotations

import dataclasses
import json
import os

import concursus
from concursus import AgentManifest

SNAPSHOT_PATH = os.path.join(os.path.dirname(__file__), "api_surface.snapshot.json")


def _public_manifest_fields():
    """Public (non-underscore) dataclass field names of :class:`AgentManifest`, sorted."""
    return sorted(
        f.name for f in dataclasses.fields(AgentManifest) if not f.name.startswith("_")
    )


def current_surface():
    """The live public surface, serialized as sorted lists (order-insensitive, membership-sensitive)."""
    return {
        "__all__": sorted(concursus.__all__),
        "AgentManifest_fields": _public_manifest_fields(),
    }


def _load_snapshot():
    with open(SNAPSHOT_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_snapshot():
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as fh:
        json.dump(current_surface(), fh, indent=2, sort_keys=True)
        fh.write("\n")


# -- the guard --------------------------------------------------------------
def test_snapshot_file_exists():
    assert os.path.exists(SNAPSHOT_PATH), (
        "committed api_surface.snapshot.json is missing — regenerate it with "
        "`PYTHONPATH=src python3.11 tests/test_api_surface.py`"
    )


def test_public_surface_matches_committed_snapshot():
    committed = _load_snapshot()
    current = current_surface()
    cur_all, com_all = set(current["__all__"]), set(committed["__all__"])
    cur_f, com_f = set(current["AgentManifest_fields"]), set(committed["AgentManifest_fields"])
    assert current == committed, (
        "public API surface changed without updating the committed snapshot. If this change is "
        "intentional AND reviewed, regenerate it with "
        "`PYTHONPATH=src python3.11 tests/test_api_surface.py`.\n"
        f"  __all__ added:            {sorted(cur_all - com_all)}\n"
        f"  __all__ removed:          {sorted(com_all - cur_all)}\n"
        f"  AgentManifest fields +:   {sorted(cur_f - com_f)}\n"
        f"  AgentManifest fields -:   {sorted(com_f - cur_f)}"
    )


def test_every_exported_name_is_importable():
    """A stale ``__all__`` entry (name listed but not bound) would break ``from pkg import *``."""
    missing = [n for n in concursus.__all__ if not hasattr(concursus, n)]
    assert missing == [], f"__all__ lists names that are not importable: {missing}"


if __name__ == "__main__":  # regenerate the committed snapshot from the current surface
    _write_snapshot()
    print(f"wrote {SNAPSHOT_PATH}")
