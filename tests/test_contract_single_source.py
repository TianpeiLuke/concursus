"""Build-enforced SINGLE-SOURCE contract for the alignment type-gate.

The dependency-resolver type-gate — :func:`~concursus.core.resolve.check_alignment` and
its sibling :func:`~concursus.core.resolve.resolve_edges`, plus the
:class:`~concursus.core.resolve.AlignmentError` it raises — is the ONE authority that
decides whether a ``depends_on`` edge type-aligns. A second copy (a forked ``def check_alignment``
in another module, or an inline re-implementation of the alignment rule that raises its own
``AlignmentError`` outside the resolver) is the classic drift bug: two validators disagree, the
compiler passes what the runtime rejects (or vice-versa). This mirrors KiRoom's
``model-resolution-contract.test.ts`` intent — the resolution rule is defined once, and the build
fails the instant a duplicate appears.

These tests AST-parse the shipped package source (offline, no imports of AWS/langgraph) and assert:

- ``check_alignment`` is defined in exactly ONE module, and it is ``core/resolve.py``.
- ``resolve_edges`` is defined in exactly ONE module, and it is ``core/resolve.py``.
- ``class AlignmentError`` is declared in exactly ONE module, and it is ``core/resolve.py``.
- Every ``raise AlignmentError(...)`` site lives in ``core/resolve.py`` — no other module inlines
  the alignment rule to reject an edge on its own.

A pure structural guard: it reads no runtime state and changes no default code path.
"""

from __future__ import annotations

import ast
import os
from typing import Dict, List, Tuple

# tests/ -> ../src/concursus
_PKG_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "concursus")
)
#: The one module the alignment type-gate is allowed to live in (repo-relative to the package root).
_CANONICAL = os.path.join("core", "resolve.py")


def _package_py_files() -> List[str]:
    """Every ``.py`` file shipped in the package (skips ``__pycache__``)."""
    out: List[str] = []
    for root, _dirs, files in os.walk(_PKG_ROOT):
        if "__pycache__" in root:
            continue
        for fname in files:
            if fname.endswith(".py"):
                out.append(os.path.join(root, fname))
    return out


def _rel(path: str) -> str:
    """Path relative to the package root, for readable assertion messages."""
    return os.path.relpath(path, _PKG_ROOT)


def _defs_and_raises() -> Tuple[Dict[str, List[str]], Dict[str, List[str]], List[str]]:
    """Walk the AST of every package module.

    Returns ``(func_defs, class_defs, alignment_raise_files)`` where ``func_defs`` maps a
    function name -> the rel-paths that ``def`` it, ``class_defs`` maps a class name -> the
    rel-paths that declare it, and ``alignment_raise_files`` is every rel-path with a
    ``raise AlignmentError(...)`` statement.
    """
    func_defs: Dict[str, List[str]] = {"check_alignment": [], "resolve_edges": []}
    class_defs: Dict[str, List[str]] = {"AlignmentError": []}
    raise_files: List[str] = []

    for path in _package_py_files():
        rel = _rel(path)
        with open(path, "r", encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=path)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in func_defs:
                    func_defs[node.name].append(rel)
            elif isinstance(node, ast.ClassDef):
                if node.name in class_defs:
                    class_defs[node.name].append(rel)
            elif isinstance(node, ast.Raise):
                exc = node.exc
                # match `raise AlignmentError(...)` and bare `raise AlignmentError`
                callee = exc.func if isinstance(exc, ast.Call) else exc
                if isinstance(callee, ast.Name) and callee.id == "AlignmentError":
                    if rel not in raise_files:
                        raise_files.append(rel)
    return func_defs, class_defs, raise_files


def test_check_alignment_defined_exactly_once_in_core_resolve():
    func_defs, _classes, _raises = _defs_and_raises()
    sites = func_defs["check_alignment"]
    assert sites == [_CANONICAL], (
        "check_alignment must be defined in exactly ONE module (core/resolve.py); a second "
        f"definition is alignment-gate drift. Found def sites: {sites}"
    )


def test_resolve_edges_defined_exactly_once_in_core_resolve():
    func_defs, _classes, _raises = _defs_and_raises()
    sites = func_defs["resolve_edges"]
    assert sites == [_CANONICAL], (
        "resolve_edges must be defined in exactly ONE module (core/resolve.py); a second "
        f"definition is resolver drift. Found def sites: {sites}"
    )


def test_alignment_error_class_declared_exactly_once_in_core_resolve():
    _funcs, class_defs, _raises = _defs_and_raises()
    sites = class_defs["AlignmentError"]
    assert sites == [_CANONICAL], (
        "class AlignmentError must be declared in exactly ONE module (core/resolve.py); a second "
        f"class of the same name is a forked type-gate error. Found class sites: {sites}"
    )


def test_alignment_rule_is_only_enforced_in_core_resolve():
    """No module OTHER than the resolver may inline the alignment rule by raising AlignmentError."""
    _funcs, _classes, raise_files = _defs_and_raises()
    offenders = [p for p in raise_files if p != _CANONICAL]
    assert offenders == [], (
        "AlignmentError is raised outside core/resolve.py — a second, inline copy of the "
        f"alignment rule (drift risk). Offending modules: {offenders}"
    )
    # sanity: the canonical module IS where the rule is enforced (guards against a silent no-match).
    assert _CANONICAL in raise_files, (
        "expected core/resolve.py to raise AlignmentError; the scan matched nothing, which likely "
        "means the AST detection broke rather than that the rule moved"
    )
