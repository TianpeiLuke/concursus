"""Operator UI surfaces.

Presentation only. These modules consume the record dict that ``tests/dag_render.py`` produces and
never import the record layer, so the dependency runs one way: shipped presentation code does not
reach into test support.

Not re-exported from :mod:`concursus`'s top-level ``__all__`` — the console is an operator
surface, not part of the compiler's public API, and ``tests/test_api_surface.py`` guards that line.
Import it explicitly::

    from concursus.ui.console import render_console
"""

from __future__ import annotations

from concursus.ui.console import (
    COLUMN_ORDER,
    LIVE_EXCLUDED,
    MARKS,
    TRANSIENT,
    board_columns,
    render_console,
)

__all__ = [
    "COLUMN_ORDER",
    "LIVE_EXCLUDED",
    "MARKS",
    "TRANSIENT",
    "board_columns",
    "render_console",
]
