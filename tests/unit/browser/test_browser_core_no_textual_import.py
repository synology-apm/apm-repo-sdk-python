"""Structural guard: ``browser/core/`` imports no Textual at all, nested
imports included, so every ``core.*`` model/update/selector is unit-testable
without an ``App``/``Pilot``."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

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
    # A wrong path would let the guard below pass vacuously.
    assert _CORE_ROOT.is_dir(), f"expected {_CORE_ROOT} to exist"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        pytest.param(
            "def f():\n    from textual.widgets import Tree\n",
            [(2, "textual.widgets")],
            id="textual_import_nested_inside_a_function_is_still_found",
        ),
        pytest.param(
            "import dataclasses\nfrom synology_apm_repo.sdk.api import Repository\n",
            [],
            id="non_textual_import_is_ignored",
        ),
        pytest.param("import textual\n", [(1, "textual")], id="bare_import_of_textual_itself_is_found"),
    ],
)
def test_textual_imports(source: str, expected: list[tuple[int, str]]) -> None:
    assert _textual_imports(ast.parse(source)) == expected


def test_core_has_no_textual_import() -> None:
    violations: list[str] = []
    for path in sorted(_CORE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for lineno, imported in _textual_imports(tree):
            violations.append(f"{path.relative_to(_CORE_ROOT)}:{lineno}: imports {imported!r}")
    assert violations == [], "browser/core/ must never import textual:\n" + "\n".join(violations)
