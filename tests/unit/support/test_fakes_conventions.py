"""Every hand-written fake under ``tests/`` declares what it stands in for:
``@faithful_to(Real)``, or ``@unchecked_fake("...")`` where no real class
can be compared against (``support/fakes.py``).

A class counts as a fake when it defines a public method, has no base class
of its own (a subclass inherits its base's check, or is the real class), and
is named like one (``Fake``/``Stub``, or a ``...Store``/``...Provider``/
``...Sink``/``...Writer``-style suffix) or defines three or more ``ObjectStore`` methods.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_TESTS = Path(__file__).resolve().parents[2]
_DECLARATIONS = ("faithful_to", "unchecked_fake")
_FAKE_NAME = re.compile(
    r"Fake|Stub|(Store|Provider|Repo|Session|Catalog|Sink|Source|Stream|Db|Tree|Unit|Content|Writer|Reader|Client)$"
)
_OBJECT_STORE_METHODS = {"read", "size", "exists", "listdir", "close"}
_NEUTRAL_BASES = {"object", "Protocol"}


def undeclared_fakes(source: str) -> list[str]:
    """The ``line: name`` of every class in ``source`` that looks like a fake
    and carries neither declaration."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.ClassDef) or node.name.startswith("Test"):
            continue
        decorators = {ast.unparse(d.func if isinstance(d, ast.Call) else d) for d in node.decorator_list}
        if decorators & set(_DECLARATIONS):
            continue
        if any(ast.unparse(base) not in _NEUTRAL_BASES for base in node.bases):
            continue
        methods = {n.name for n in node.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)}
        if not any(not m.startswith("_") for m in methods):
            continue
        if _FAKE_NAME.search(node.name) or len(methods & _OBJECT_STORE_METHODS) >= 3:
            found.append(f"{node.lineno}: {node.name}")
    return found


def test_every_fake_under_tests_declares_what_it_stands_in_for() -> None:
    problems = [
        f"{path.relative_to(_TESTS)}:{entry}"
        for path in sorted(_TESTS.rglob("*.py"))
        if "smoke" not in path.parts
        for entry in undeclared_fakes(path.read_text(encoding="utf-8"))
    ]
    assert problems == [], "add @faithful_to(Real) or @unchecked_fake(...):\n" + "\n".join(problems)


def test_an_undecorated_store_shaped_class_is_reported() -> None:
    source = """
class Anything:
    async def read(self, path): ...
    async def size(self, path): ...
    async def exists(self, path): ...
"""
    assert undeclared_fakes(source) == ["2: Anything"]


def test_a_declared_subclassed_data_only_or_test_class_is_not_reported() -> None:
    source = """
@faithful_to(ObjectStore)
class _FakeStore:
    async def read(self, path): ...

@unchecked_fake("botocore S3 client")
class _FakeS3Client:
    def get_object(self): ...

class _CountingStore(WrappingStore):
    async def read(self, path): ...

class _FakeRow:
    size = 3

class TestFakeStore:
    def test_x(self): ...
"""
    assert undeclared_fakes(source) == []
