"""``ex``: the ``examples/*.py`` module a test file covers, picked by the
file's name (``test_<example>_<behaviour>.py``) and executed once per test
file. Tests change its attributes only through ``monkeypatch``, which
restores them."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"


@pytest.fixture(scope="module")
def ex(request: pytest.FixtureRequest) -> ModuleType:
    test_stem = Path(request.path).stem.removeprefix("test_")
    matches = [p for p in _EXAMPLES.glob("*.py") if test_stem.startswith(p.stem + "_")]
    assert len(matches) == 1, f"{request.path.name} names no single examples/*.py module: {matches}"
    return load_module(matches[0].stem, matches[0])
