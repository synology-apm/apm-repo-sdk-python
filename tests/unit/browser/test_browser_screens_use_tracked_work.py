"""Structural guard: every ``screens/*.py`` worker method must go through
``widgets/worker_progress.py``'s own tracked ``work``, never ``textual``'s
directly — that's what makes an omitted busy-indicator wrap
(``busy=False``) a decoration-time, reviewable choice instead of a
silent gap. Walks the real ``screens/``
source tree via ``ast`` (same technique as
``test_browser_core_no_textual_import.py``), rather than trusting import
discipline to hold by convention alone.
"""

from __future__ import annotations

import ast
from pathlib import Path

_SCREENS_ROOT = (
    Path(__file__).resolve().parents[3] / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser/screens"
)


def _textual_work_imports(tree: ast.Module) -> list[int]:
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "textual" and any(a.name == "work" for a in node.names)
    ]


def test_screens_root_exists() -> None:
    # A guard failing silently because the path itself is wrong (a future
    # rename/move) would be worse than no guard at all -- fail loudly.
    assert _SCREENS_ROOT.is_dir(), f"expected {_SCREENS_ROOT} to exist"


def test_textual_work_import_is_found() -> None:
    tree = ast.parse("from textual import work\n")
    assert _textual_work_imports(tree) == [1]


def test_unrelated_textual_import_is_ignored() -> None:
    tree = ast.parse("from textual.app import ComposeResult\nfrom textual import events\n")
    assert _textual_work_imports(tree) == []


def test_screens_never_import_textual_work_directly() -> None:
    """The real guard — every ``.py`` file under the actual ``screens/``
    source tree, not a synthetic one."""
    violations: list[str] = []
    for path in sorted(_SCREENS_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(
            f"{path.relative_to(_SCREENS_ROOT)}:{lineno}: imports `work` from textual directly"
            for lineno in _textual_work_imports(tree)
        )
    assert violations == [], (
        "screens/*.py must import `work` from widgets/worker_progress.py, never textual directly:\n"
        + "\n".join(violations)
    )
