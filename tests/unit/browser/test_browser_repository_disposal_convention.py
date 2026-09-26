"""Structural guard: a ``Repository`` is never closed by calling
``.close()`` on it directly, anywhere in ``browser/`` — disposal always
goes through ``Session.close_repo()`` instead. A bare
``repo.close()`` leaves that repository's S3/Azure/SMB connector
referenced by the ``Session`` for the rest of the process.

Detection is a naming-convention heuristic, not real type inference
(mypy is the only thing in this toolchain that actually knows a given
expression's static type) — but ``repo``/``repository``/``self.repo``/
``self._repo`` is, without a single exception, the variable name this
codebase uses for a ``Repository``-typed value, so a ``.close()`` call
on any of those shapes is exactly the pattern worth catching."""

from __future__ import annotations

import ast
from pathlib import Path

_BROWSER_SRC = Path(__file__).resolve().parents[3] / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser"

#: ``Name`` ids and ``Attribute`` attrs this codebase actually uses,
#: without a single exception, for a ``Repository``-typed value.
_REPO_NAMES = frozenset({"repo", "repository"})
_REPO_ATTRS = frozenset({"repo", "_repo"})


def _bare_repo_close_calls(tree: ast.Module) -> list[int]:
    """Line numbers of every ``<repo-like>.close()`` call in ``tree`` —
    a plain ``Name`` (``repo.close()``, a loop variable named ``repo``)
    or an ``Attribute`` access (``self.repo.close()``,
    ``self._repo.close()``)."""
    found: list[int] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "close"):
            continue
        base = node.func.value
        is_repo_like = (isinstance(base, ast.Name) and base.id in _REPO_NAMES) or (
            isinstance(base, ast.Attribute) and base.attr in _REPO_ATTRS
        )
        if is_repo_like:
            found.append(node.lineno)
    return found


def test_name_based_repo_close_is_found() -> None:
    tree = ast.parse("async def f(repo):\n    await repo.close()\n")
    assert _bare_repo_close_calls(tree) == [2]


def test_self_dot_repo_close_is_found() -> None:
    tree = ast.parse("async def f(self):\n    await self.repo.close()\n")
    assert _bare_repo_close_calls(tree) == [2]


def test_self_dot_underscore_repo_close_is_found() -> None:
    tree = ast.parse("async def f(self):\n    await self._repo.close()\n")
    assert _bare_repo_close_calls(tree) == [2]


def test_session_close_repo_is_not_flagged() -> None:
    """The sanctioned replacement — a different method name entirely
    (``close_repo``, not ``close``), so this never collides with the
    check above."""
    tree = ast.parse("async def f(session, repo):\n    await session.close_repo(repo)\n")
    assert _bare_repo_close_calls(tree) == []


def test_close_on_an_unrelated_name_is_not_flagged() -> None:
    tree = ast.parse("async def f(provider):\n    await provider.close()\n")
    assert _bare_repo_close_calls(tree) == []


def test_repository_disposal_stays_on_session_close_repo() -> None:
    """The real guard — every ``.py`` file under the actual
    ``browser/`` source tree, not a synthetic one."""
    violations: list[str] = []
    for path in sorted(_BROWSER_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(f"{path.relative_to(_BROWSER_SRC)}:{lineno}" for lineno in _bare_repo_close_calls(tree))
    assert violations == [], (
        "found a bare repo.close() call -- repository disposal must go through "
        "Session.close_repo() (see runtime/resources.py):\n" + "\n".join(violations)
    )
