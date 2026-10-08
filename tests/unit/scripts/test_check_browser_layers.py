"""Tests for scripts/check_browser_layers.py, each against a tiny synthetic
browser tree under ``tmp_path``."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_browser_layers.py"


@pytest.fixture
def check_browser_layers() -> ModuleType:
    return load_module("check_browser_layers", _SCRIPT_PATH)


def _tree(root: Path, files: dict[str, str]) -> Path:
    for layer in ("view", "content_preview", "widgets", "core", "runtime"):
        (root / layer).mkdir(parents=True, exist_ok=True)
    for relative, text in files.items():
        (root / relative).write_text(text)
    return root


def test_imports_a_layer_builds_on_are_allowed(check_browser_layers: ModuleType, tmp_path: Path) -> None:
    _tree(
        tmp_path,
        {
            "core/update.py": "from ..view import spec\nfrom synology_apm_repo.browser import strings\n",
            "runtime/effects.py": "from ..core import update\nfrom ..widgets import hint\n",
        },
    )
    assert check_browser_layers.find_violations(tmp_path) == []


@pytest.mark.parametrize(
    ("module", "source", "expected"),
    [
        ("view/spec.py", "from ..core import model\n", "view/spec.py:1: view/ imports 'core'"),
        ("core/update.py", "from ..runtime import effects\n", "core/update.py:1: core/ imports 'runtime'"),
        (
            "runtime/effects.py",
            "if True:\n    from synology_apm_repo.browser.screens import x\n",
            "runtime/effects.py:2: runtime/ imports 'screens'",
        ),
        ("widgets/hint.py", "import synology_apm_repo.browser.app\n", "widgets/hint.py:1: widgets/ imports 'app'"),
        ("core/update.py", "from .. import screens\n", "core/update.py:1: core/ imports 'screens'"),
        (
            "runtime/effects.py",
            "from synology_apm_repo.browser import app\n",
            "runtime/effects.py:1: runtime/ imports 'app'",
        ),
    ],
)
def test_an_import_a_layer_does_not_build_on_is_reported(
    check_browser_layers: ModuleType, tmp_path: Path, module: str, source: str, expected: str
) -> None:
    _tree(tmp_path, {module: source})
    assert check_browser_layers.find_violations(tmp_path) == [expected]


def test_a_missing_layer_directory_is_reported(check_browser_layers: ModuleType, tmp_path: Path) -> None:
    _tree(tmp_path, {})
    (tmp_path / "widgets").rmdir()
    assert check_browser_layers.find_violations(tmp_path) == ["widgets/: layer directory is missing"]
