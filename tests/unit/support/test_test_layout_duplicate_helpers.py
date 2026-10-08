"""No two test files define the same helper: a top-level function or class
of ``MIN_STATEMENTS`` or more statements in one ``tests/**/test_*.py`` file
must not be structurally identical to one in another (``tests/smoke``
excluded). Identity is the AST with positions, the definition's own name and
its docstring left out, so a renamed or re-documented copy still counts. A
shared helper lives in an importable module (``tests/CLAUDE.md``)."""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

_TESTS = Path(__file__).resolve().parents[2]

MIN_STATEMENTS = 5

_Definition = ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef


def _body_without_docstring(node: _Definition) -> list[ast.stmt]:
    body = node.body
    first = body[0] if body else None
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
        return body[1:]
    return body


def _statement_count(node: _Definition) -> int:
    return sum(isinstance(sub, ast.stmt) for stmt in _body_without_docstring(node) for sub in ast.walk(stmt))


def _fingerprint(node: _Definition) -> str:
    parts: list[ast.AST] = [*_body_without_docstring(node), *node.decorator_list]
    if isinstance(node, ast.ClassDef):
        parts += [*node.bases, *node.keywords]
    else:
        parts.append(node.args)
        if node.returns is not None:
            parts.append(node.returns)
    return f"{type(node).__name__}:" + "|".join(ast.dump(part) for part in parts)


def duplicate_helpers(sources: dict[str, str]) -> list[str]:
    """``"<file>:<line> <name> == <file>:<line> <name>"`` for every pair of
    structurally identical top-level definitions in different files of
    ``sources`` (path -> source)."""
    seen: dict[str, list[tuple[str, int, str]]] = defaultdict(list)
    for path, source in sorted(sources.items()):
        for node in ast.parse(source).body:
            if isinstance(node, _Definition) and _statement_count(node) >= MIN_STATEMENTS:
                seen[_fingerprint(node)].append((path, node.lineno, node.name))
    found: list[str] = []
    for definitions in seen.values():
        first_path, first_line, first_name = definitions[0]
        found += [
            f"{first_path}:{first_line} {first_name} == {path}:{line} {name}"
            for path, line, name in definitions[1:]
            if path != first_path
        ]
    return found


def test_no_two_test_files_define_the_same_helper() -> None:
    sources = {
        str(path.relative_to(_TESTS)): path.read_text(encoding="utf-8")
        for path in sorted(_TESTS.rglob("test_*.py"))
        if "smoke" not in path.relative_to(_TESTS).parts
    }
    found = duplicate_helpers(sources)
    assert found == [], "move the shared helper into an importable module:\n" + "\n".join(found)


_HELPER = '''
def {name}(path, rows):
    """{doc}"""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    conn.execute("CREATE TABLE t(a)")
    conn.executemany("INSERT INTO t VALUES (?)", rows)
    conn.commit()
    conn.close()
'''


def test_a_renamed_redocumented_copy_in_another_file_is_reported() -> None:
    sources = {
        "a/test_one.py": _HELPER.format(name="_write_rows", doc="One doc."),
        "b/test_two.py": "import os\n" + _HELPER.format(name="_put_rows", doc="Another doc."),
    }
    assert duplicate_helpers(sources) == ["a/test_one.py:2 _write_rows == b/test_two.py:3 _put_rows"]


def test_a_copy_differing_in_one_literal_is_not_reported() -> None:
    sources = {
        "a/test_one.py": _HELPER.format(name="_write_rows", doc=""),
        "b/test_two.py": _HELPER.format(name="_write_rows", doc="").replace("t(a)", "t(b)"),
    }
    assert duplicate_helpers(sources) == []


def test_a_helper_below_the_size_floor_or_within_one_file_is_not_reported() -> None:
    small = "def _f(x):\n    y = x + 1\n    return y\n"
    sources = {
        "a/test_one.py": small + _HELPER.format(name="_w", doc="") + _HELPER.format(name="_v", doc=""),
        "b/test_two.py": small,
    }
    assert duplicate_helpers(sources) == []
