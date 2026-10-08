"""Tests for scripts/check_sdk_import_boundary.py."""

from __future__ import annotations

import ast
from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_sdk_import_boundary.py"


@pytest.fixture
def check_sdk_import_boundary() -> ModuleType:
    return load_module("check_sdk_import_boundary", _SCRIPT_PATH)


def _checked_tree(monkeypatch: pytest.MonkeyPatch, module: ModuleType, tmp_path: Path, source: str) -> None:
    """Points the checker at ``tmp_path`` (also its ``ROOT``, so violations
    print relative to it), holding one file with ``source``."""
    path = tmp_path / "commands" / "other.py"
    path.parent.mkdir(parents=True)
    path.write_text(source)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "SOURCE_ROOTS", [tmp_path])


class TestSdkImports:
    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            pytest.param(
                "from synology_apm_repo.sdk.dedup import pool\n",
                [(1, "synology_apm_repo.sdk.dedup"), (1, "synology_apm_repo.sdk.dedup.pool")],
                id="from_import_is_found",
            ),
            pytest.param(
                "import synology_apm_repo.sdk.catalog.catalog\n",
                [(1, "synology_apm_repo.sdk.catalog.catalog")],
                id="bare_import_is_found",
            ),
            pytest.param(
                "def f():\n    from synology_apm_repo.sdk.dedup import pool\n",
                [(2, "synology_apm_repo.sdk.dedup"), (2, "synology_apm_repo.sdk.dedup.pool")],
                id="import_nested_inside_a_function_body_is_still_found",
            ),
            # Catalog is exported, not the catalog package (even on a
            # case-insensitive filesystem).
            pytest.param(
                "from synology_apm_repo.sdk import dedup, Session, Catalog\n",
                [(1, "synology_apm_repo.sdk"), (1, "synology_apm_repo.sdk.dedup")],
                id="a_submodule_imported_by_name_is_found_but_an_exported_name_is_not",
            ),
            pytest.param("import typer\nfrom pathlib import Path\n", [], id="non_sdk_import_is_ignored"),
        ],
    )
    def test_sdk_imports(
        self, check_sdk_import_boundary: ModuleType, source: str, expected: list[tuple[int, str]]
    ) -> None:
        assert check_sdk_import_boundary._sdk_imports(ast.parse(source)) == expected


class TestMain:
    @pytest.mark.parametrize(
        "source",
        [
            "from synology_apm_repo.sdk import Session\n",
            "from synology_apm_repo.sdk.presentation import format_bytes\n",
            "from synology_apm_repo.sdk.profiles import form_fields_for\n",
            "from synology_apm_repo.sdk.diagnostics import inspect_bucket\n",
            # A public module named by name is still public.
            "from synology_apm_repo.sdk import presentation\n",
        ],
    )
    def test_a_public_module_import_is_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, check_sdk_import_boundary: ModuleType, source: str
    ) -> None:
        _checked_tree(monkeypatch, check_sdk_import_boundary, tmp_path, source)

        assert check_sdk_import_boundary.main() == 0

    @pytest.mark.parametrize(
        ("source", "module"),
        [
            ("from synology_apm_repo.sdk.dedup import pool\n", "synology_apm_repo.sdk.dedup"),
            ("from synology_apm_repo.sdk.api import Session\n", "synology_apm_repo.sdk.api"),
            # A submodule of a public module is internal too.
            (
                "from synology_apm_repo.sdk.presentation.format import pluralize\n",
                "synology_apm_repo.sdk.presentation.format",
            ),
            ("import synology_apm_repo.sdk.units.base\n", "synology_apm_repo.sdk.units.base"),
            # An internal submodule pulled in by name through a public module.
            ("from synology_apm_repo.sdk import dedup\n", "synology_apm_repo.sdk.dedup"),
            (
                "from synology_apm_repo.sdk.presentation import format\n",
                "synology_apm_repo.sdk.presentation.format",
            ),
        ],
    )
    def test_an_internal_module_import_is_a_violation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
        source: str,
        module: str,
    ) -> None:
        _checked_tree(monkeypatch, check_sdk_import_boundary, tmp_path, source)

        assert check_sdk_import_boundary.main() == 1
        err = capsys.readouterr().err
        assert f"commands/other.py:1: imports {module!r}" in err

    @pytest.mark.parametrize(
        "source",
        [
            "from synology_apm_repo.browser.app import ApmRepoBrowserApp\n",
            "import synology_apm_repo.browser\n",
            "def f():\n    from synology_apm_repo.browser import strings\n",
            "from synology_apm_repo import browser\n",
        ],
    )
    def test_a_frontend_importing_another_is_a_violation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
        source: str,
    ) -> None:
        _checked_tree(monkeypatch, check_sdk_import_boundary, tmp_path, source)
        monkeypatch.setattr(
            check_sdk_import_boundary, "FORBIDDEN_FRONTENDS", {tmp_path: ("synology_apm_repo.browser",)}
        )

        assert check_sdk_import_boundary.main() == 1
        assert "a frontend imports another frontend" in capsys.readouterr().err

    def test_a_package_merely_sharing_a_frontends_prefix_is_not_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, check_sdk_import_boundary: ModuleType
    ) -> None:
        _checked_tree(monkeypatch, check_sdk_import_boundary, tmp_path, "import synology_apm_repo.browserlike\n")
        monkeypatch.setattr(
            check_sdk_import_boundary, "FORBIDDEN_FRONTENDS", {tmp_path: ("synology_apm_repo.browser",)}
        )

        assert check_sdk_import_boundary.main() == 0
