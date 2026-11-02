"""``ex``: the ``examples/`` module a test file covers, picked by the file's
name (``test_<example>_<behaviour>.py``, the longest module stem it starts
with) and executed once per test file. Tests change its attributes only
through ``monkeypatch``, which restores them."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"


@pytest.fixture(scope="module")
def ex(request: pytest.FixtureRequest) -> ModuleType:
    test_stem = Path(request.path).stem.removeprefix("test_")
    matches = sorted(
        (p for p in _EXAMPLES.rglob("*.py") if test_stem.startswith(p.stem + "_")), key=lambda p: len(p.stem)
    )
    assert matches, f"{request.path.name} names no examples/ module"
    return load_module(matches[-1].stem, matches[-1])
