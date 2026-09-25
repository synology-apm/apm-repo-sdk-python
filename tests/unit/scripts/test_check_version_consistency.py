"""Tests for scripts/check_version_consistency.py.

``scripts/`` isn't an installed package, so the module under test is loaded
by path via ``importlib`` through the ``check_version_consistency`` fixture.
This checker only ever reads local ``pyproject.toml`` files, never the
network, so every scenario here is offline, including a "regression guard
against the real repository files" test,
``TestMain.test_actual_repo_pyproject_files_are_consistent``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_version_consistency.py"

_SDK = "synology-apm-repo-sdk"
_CLI = "synology-apm-repo-cli"
_BROWSER = "synology-apm-repo-browser"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_version_consistency", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def check_version_consistency() -> ModuleType:
    return _load_module()


def _write_pyproject(tmp_path: Path, name: str, version: str, dependencies: tuple[str, ...] = ()) -> Path:
    deps_toml = ", ".join(f'"{d}"' for d in dependencies)
    path = tmp_path / f"{name}.toml"
    path.write_text(f'[project]\nname = "{name}"\nversion = "{version}"\ndependencies = [{deps_toml}]\n')
    return path


def _write_clean_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module: ModuleType, *, version: str = "1.0.0"
) -> None:
    """The three packages at a matching version, with correct dependency
    pins on each other — the baseline every ``TestMain`` scenario below
    mutates one piece of."""
    sdk_dep = f"{_SDK}=={version}"
    paths = {
        _SDK: _write_pyproject(tmp_path, _SDK, version),
        _CLI: _write_pyproject(tmp_path, _CLI, version, dependencies=(sdk_dep,)),
        _BROWSER: _write_pyproject(tmp_path, _BROWSER, version, dependencies=(sdk_dep,)),
    }
    monkeypatch.setattr(module, "PYPROJECT_PATHS", paths)


class TestPin:
    def test_matching_prefix_returns_the_pinned_version(self, check_version_consistency: ModuleType) -> None:
        pin = check_version_consistency._pin(["foo==1.0.0", f"{_SDK}==2.3.4"], _SDK)
        assert pin == "2.3.4"

    def test_no_matching_dependency_returns_none(self, check_version_consistency: ModuleType) -> None:
        assert check_version_consistency._pin(["foo==1.0.0"], _SDK) is None


class TestMain:
    def test_clean_state_passes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_version_consistency: ModuleType,
    ) -> None:
        _write_clean_repo(tmp_path, monkeypatch, check_version_consistency)
        assert check_version_consistency.main() == 0
        assert "OK: all packages at version '1.0.0'" in capsys.readouterr().out

    def test_cli_version_mismatch_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_version_consistency: ModuleType,
    ) -> None:
        _write_clean_repo(tmp_path, monkeypatch, check_version_consistency)
        _write_pyproject(tmp_path, _CLI, "1.0.1", dependencies=(f"{_SDK}==1.0.0",))

        assert check_version_consistency.main() == 1
        err = capsys.readouterr().err
        assert "version='1.0.1' does not match" in err

    def test_missing_dependency_pin_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_version_consistency: ModuleType,
    ) -> None:
        _write_clean_repo(tmp_path, monkeypatch, check_version_consistency)
        _write_pyproject(tmp_path, _BROWSER, "1.0.0")  # no dependencies at all

        assert check_version_consistency.main() == 1
        err = capsys.readouterr().err
        assert f"is missing a {_SDK}== dependency pin" in err

    def test_stale_dependency_pin_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_version_consistency: ModuleType,
    ) -> None:
        _write_clean_repo(tmp_path, monkeypatch, check_version_consistency)
        _write_pyproject(tmp_path, _CLI, "1.0.0", dependencies=(f"{_SDK}==0.9.0",))  # own version bumped, pin wasn't

        assert check_version_consistency.main() == 1
        err = capsys.readouterr().err
        assert f"pins {_SDK}==0.9.0" in err

    def test_actual_repo_pyproject_files_are_consistent(self, check_version_consistency: ModuleType) -> None:
        """No monkeypatching — this repository's real three ``pyproject.toml``
        files, via the script's own unmodified ``ROOT``/``PYPROJECT_PATHS``.
        Turns "did I bump all three lockstep versions/pins correctly" into
        a named, individually-runnable assertion instead of only a
        ``make test``-time signal."""
        assert check_version_consistency.main() == 0


__all__: list[str] = []
