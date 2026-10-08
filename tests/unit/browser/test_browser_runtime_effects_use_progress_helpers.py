"""Structural guard: every ``runtime/*_effects.py`` ``Cmd`` dispatch must
go through ``widgets/worker_progress.py``'s ``run_worker_with_progress``/
``run_worker_no_progress``, never a bare ``.run_worker(...)`` call (a method
call, which ruff's ``TID251`` ban on ``textual.work`` can't express). The
whole file is scanned, not just ``perform()``.
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
    # A wrong path would make the scan below pass vacuously.
    assert _RUNTIME_ROOT.is_dir(), f"expected {_RUNTIME_ROOT} to exist"


def test_bare_run_worker_call_is_found() -> None:
    tree = ast.parse("self._screen.run_worker(foo)\n")
    assert _run_worker_calls(tree) == [1]


def test_tracked_helper_call_is_ignored() -> None:
    tree = ast.parse("run_worker_with_progress(self._screen, foo)\nrun_worker_no_progress(app, bar)\n")
    assert _run_worker_calls(tree) == []


def test_effects_never_call_run_worker_directly() -> None:
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
