"""Tests for scripts/check_lock_sources.py."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_lock_sources.py"


@pytest.fixture
def check_lock_sources() -> ModuleType:
    return load_module("check_lock_sources", _SCRIPT_PATH)


def _write_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module: ModuleType, *sources: str) -> None:
    entries = "".join(
        f'[[package]]\nname = "pkg{i}"\nversion = "1.0"\nsource = {source}\n\n' for i, source in enumerate(sources)
    )
    path = tmp_path / "uv.lock"
    path.write_text(f"version = 1\n\n{entries}")
    monkeypatch.setattr(module, "LOCK_PATH", path)


_PYPI = '{ registry = "https://pypi.org/simple" }'
_MEMBER = '{ editable = "packages/synology-apm-repo-sdk" }'


class TestMain:
    def test_pypi_and_workspace_members_pass(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_lock_sources: ModuleType,
    ) -> None:
        _write_lock(tmp_path, monkeypatch, check_lock_sources, _PYPI, _MEMBER)

        assert check_lock_sources.main() == 0
        assert "all 2 locked packages" in capsys.readouterr().out

    @pytest.mark.parametrize(
        "source",
        [
            '{ registry = "https://test.pypi.org/simple" }',
            '{ git = "https://github.com/example/pkg?rev=abc#abc" }',
            '{ url = "https://example.com/pkg-1.0.tar.gz" }',
            '{ path = "../pkg" }',
            '{ editable = "../elsewhere" }',
        ],
    )
    def test_any_other_source_fails_naming_the_package(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_lock_sources: ModuleType,
        source: str,
    ) -> None:
        _write_lock(tmp_path, monkeypatch, check_lock_sources, _PYPI, source)

        assert check_lock_sources.main() == 1
        err = capsys.readouterr().err
        assert "ERROR: pkg1 1.0: source" in err
        assert "pkg0" not in err

    def test_the_real_lock_passes(self, check_lock_sources: ModuleType) -> None:
        assert check_lock_sources.main() == 0
