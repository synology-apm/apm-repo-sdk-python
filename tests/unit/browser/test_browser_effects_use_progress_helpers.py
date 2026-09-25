"""Structural guard: every ``runtime/*_effects.py`` ``Cmd`` dispatch must
go through ``widgets/worker_progress.py``'s ``run_worker_with_progress``/
``run_worker_no_progress``, never a bare ``.run_worker(...)`` call — same
enforcement technique (and the same reasoning) as
``test_browser_screens_use_tracked_work.py``'s own guard for
``screens/*.py`` workers. Scoped to the whole file, not just ``perform()``:
every worker this package's effects classes ever start is dispatched from
``perform()``, so a bare ``.run_worker(`` call appearing anywhere in one
of these files is already a violation.
"""

from __future__ import annotations

import ast
from pathlib import Path

_RUNTIME_ROOT = (
    Path(__file__).resolve().parents[3] / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser/runtime"
)


def _run_worker_calls(tree: ast.Module) -> list[int]:
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "run_worker"
    ]


def test_runtime_root_exists() -> None:
    # A guard failing silently because the path itself is wrong (a future
    # rename/move) would be worse than no guard at all -- fail loudly.
    assert _RUNTIME_ROOT.is_dir(), f"expected {_RUNTIME_ROOT} to exist"


def test_bare_run_worker_call_is_found() -> None:
    tree = ast.parse("self._screen.run_worker(foo)\n")
    assert _run_worker_calls(tree) == [1]


def test_tracked_helper_call_is_ignored() -> None:
    tree = ast.parse("run_worker_with_progress(self._screen, foo)\nrun_worker_no_progress(app, bar)\n")
    assert _run_worker_calls(tree) == []


def test_effects_never_call_run_worker_directly() -> None:
    """The real guard — every ``*_effects.py`` file under the actual
    ``runtime/`` source tree, not a synthetic one. Doesn't recurse into
    ``widgets/worker_progress.py`` itself (the one place a bare
    ``host.run_worker(...)`` call is exactly right) since that module
    lives outside ``runtime/`` entirely."""
    violations: list[str] = []
    for path in sorted(_RUNTIME_ROOT.glob("*_effects.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(
            f"{path.name}:{lineno}: calls .run_worker(...) directly" for lineno in _run_worker_calls(tree)
        )
    assert violations == [], (
        "runtime/*_effects.py must dispatch workers via run_worker_with_progress/"
        "run_worker_no_progress, never a bare .run_worker(...) call:\n" + "\n".join(violations)
    )
