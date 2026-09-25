"""Structural guard: ``browser/core/`` must stay pure — importable
without Textual at all. That's what makes every ``core.*`` model/update/
selector directly unit-testable with no ``App``/``Pilot`` (see
``ARCHITECTURE.md``'s layering discipline, applied here one level below
the SDK: ``core`` is this package's own "zero I/O, zero framework"
layer). Walks the real ``core/`` source tree via ``ast`` (same technique
as ``scripts/check_sdk_import_boundary.py``, scoped to this one
narrower rule) rather than trusting import discipline to hold by
convention alone."""

from __future__ import annotations

import ast
from pathlib import Path

_CORE_ROOT = (
    Path(__file__).resolve().parents[3] / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser/core"
)


def _textual_imports(tree: ast.Module) -> list[tuple[int, str]]:
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "textual":
            found.append((node.lineno, node.module))
        elif isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names if alias.name.split(".")[0] == "textual")
    return found


def test_core_root_exists() -> None:
    # A guard failing silently because the path itself is wrong (a future
    # rename/move) would be worse than no guard at all -- fail loudly.
    assert _CORE_ROOT.is_dir(), f"expected {_CORE_ROOT} to exist"


def test_textual_import_nested_inside_a_function_is_still_found() -> None:
    tree = ast.parse("def f():\n    from textual.widgets import Tree\n")
    assert _textual_imports(tree) == [(2, "textual.widgets")]


def test_non_textual_import_is_ignored() -> None:
    tree = ast.parse("import dataclasses\nfrom synology_apm_repo.sdk.api import Repository\n")
    assert _textual_imports(tree) == []


def test_bare_import_of_textual_itself_is_found() -> None:
    tree = ast.parse("import textual\n")
    assert _textual_imports(tree) == [(1, "textual")]


def test_core_has_no_textual_import() -> None:
    """The real guard — every ``.py`` file under the actual ``core/``
    source tree, not a synthetic one. No monkeypatching: this turns the
    actual invariant into a named, individually-runnable assertion."""
    violations: list[str] = []
    for path in sorted(_CORE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for lineno, imported in _textual_imports(tree):
            violations.append(f"{path.relative_to(_CORE_ROOT)}:{lineno}: imports {imported!r}")
    assert violations == [], "browser/core/ must never import textual:\n" + "\n".join(violations)
