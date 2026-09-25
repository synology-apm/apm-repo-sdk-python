"""Tests for scripts/check_sdk_import_boundary.py.

``scripts/`` isn't an installed package, so the module under test is loaded
by path via ``importlib`` through the
``check_sdk_import_boundary`` fixture. ``TestMain``'s synthetic scenarios
monkeypatch ``SOURCE_ROOTS`` to a single tmp source tree mapped to the real
``synology_apm_repo.cli`` dotted prefix (rather than a made-up package
name) so a file written at the matching relative path
(``commands/dump.py``) resolves, via the script's own unmodified
``_module_name()``, to the exact dotted name a real ``EXCEPTIONS`` entry
would key on. ``_patch_source_roots`` also clears ``EXCEPTIONS`` to
``{}`` — the real dict's ~20 entries name modules nothing in a narrow
synthetic tmp tree ever resolves to, which the stale-``EXCEPTIONS``-key
check (``main()``'s own ``stale_keys``) would otherwise flag on every
synthetic scenario below; a test that needs one specific entry sets it
explicitly, after calling ``_patch_source_roots``.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_sdk_import_boundary.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_sdk_import_boundary", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def check_sdk_import_boundary() -> ModuleType:
    return _load_module()


def _write_source(root: Path, relative: str, text: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _patch_source_roots(monkeypatch: pytest.MonkeyPatch, module: ModuleType, tmp_path: Path) -> None:
    """Points the checker at ``tmp_path`` as both its one source root
    (mapped to the real ``synology_apm_repo.cli`` prefix) and its ``ROOT``
    (``main()``'s violation messages print paths relative to it, so it
    must also move for a synthetic tree outside the real repository
    root). Also clears ``EXCEPTIONS`` to ``{}``; a test that needs one
    specific entry sets it explicitly, after calling
    ``_patch_source_roots``."""
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "SOURCE_ROOTS", [(tmp_path, "synology_apm_repo.cli")])
    monkeypatch.setattr(module, "EXCEPTIONS", {})


class TestMatches:
    def test_exact_prefix_matches(self, check_sdk_import_boundary: ModuleType) -> None:
        assert check_sdk_import_boundary._matches("synology_apm_repo.sdk.api", ("synology_apm_repo.sdk.api",))

    def test_dotted_submodule_of_a_prefix_matches(self, check_sdk_import_boundary: ModuleType) -> None:
        assert check_sdk_import_boundary._matches(
            "synology_apm_repo.sdk.presentation.format", ("synology_apm_repo.sdk.presentation",)
        )

    def test_unrelated_module_does_not_match(self, check_sdk_import_boundary: ModuleType) -> None:
        assert not check_sdk_import_boundary._matches(
            "synology_apm_repo.sdk.dedup.pool", ("synology_apm_repo.sdk.api",)
        )

    def test_prefix_as_a_bare_string_prefix_but_not_a_dotted_submodule_does_not_match(
        self, check_sdk_import_boundary: ModuleType
    ) -> None:
        # "synology_apm_repo.sdk.apiextra" shares the literal string prefix
        # "synology_apm_repo.sdk.api" but isn't a dotted submodule of it --
        # _matches must require the "." boundary, not just str.startswith.
        assert not check_sdk_import_boundary._matches("synology_apm_repo.sdk.apiextra", ("synology_apm_repo.sdk.api",))


class TestModuleName:
    def test_plain_module_resolves_to_its_dotted_name(
        self, tmp_path: Path, check_sdk_import_boundary: ModuleType
    ) -> None:
        path = _write_source(tmp_path, "commands/dump.py", "")
        name = check_sdk_import_boundary._module_name(path, tmp_path, "synology_apm_repo.cli")
        assert name == "synology_apm_repo.cli.commands.dump"

    def test_dunder_init_resolves_to_its_package_name_not_a_dot_init_suffix(
        self, tmp_path: Path, check_sdk_import_boundary: ModuleType
    ) -> None:
        path = _write_source(tmp_path, "commands/__init__.py", "")
        name = check_sdk_import_boundary._module_name(path, tmp_path, "synology_apm_repo.cli")
        assert name == "synology_apm_repo.cli.commands"

    def test_root_dunder_init_resolves_to_the_bare_root_package(
        self, tmp_path: Path, check_sdk_import_boundary: ModuleType
    ) -> None:
        path = _write_source(tmp_path, "__init__.py", "")
        name = check_sdk_import_boundary._module_name(path, tmp_path, "synology_apm_repo.cli")
        assert name == "synology_apm_repo.cli"


class TestSdkImports:
    def test_from_import_is_found(self, check_sdk_import_boundary: ModuleType) -> None:
        tree = ast.parse("from synology_apm_repo.sdk.dedup import pool\n")
        found = check_sdk_import_boundary._sdk_imports(tree)
        assert found == [(1, "synology_apm_repo.sdk.dedup")]

    def test_bare_import_is_found(self, check_sdk_import_boundary: ModuleType) -> None:
        tree = ast.parse("import synology_apm_repo.sdk.catalog.catalog\n")
        found = check_sdk_import_boundary._sdk_imports(tree)
        assert found == [(1, "synology_apm_repo.sdk.catalog.catalog")]

    def test_import_nested_inside_a_function_body_is_still_found(self, check_sdk_import_boundary: ModuleType) -> None:
        tree = ast.parse("def f():\n    from synology_apm_repo.sdk.dedup import pool\n")
        found = check_sdk_import_boundary._sdk_imports(tree)
        assert found == [(2, "synology_apm_repo.sdk.dedup")]

    def test_non_sdk_import_is_ignored(self, check_sdk_import_boundary: ModuleType) -> None:
        tree = ast.parse("import typer\nfrom pathlib import Path\n")
        assert check_sdk_import_boundary._sdk_imports(tree) == []


class TestMain:
    def test_allowed_prefix_import_is_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, check_sdk_import_boundary: ModuleType
    ) -> None:
        _write_source(tmp_path, "commands/other.py", "from synology_apm_repo.sdk.api import Repository\n")
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)

        assert check_sdk_import_boundary.main() == 0

    def test_dotted_submodule_of_an_allowed_prefix_is_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, check_sdk_import_boundary: ModuleType
    ) -> None:
        _write_source(
            tmp_path, "commands/other.py", "from synology_apm_repo.sdk.presentation.format import format_bytes\n"
        )
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)

        assert check_sdk_import_boundary.main() == 0

    def test_disallowed_import_is_a_violation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
    ) -> None:
        _write_source(tmp_path, "commands/other.py", "from synology_apm_repo.sdk.dedup import pool\n")
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)

        assert check_sdk_import_boundary.main() == 1
        err = capsys.readouterr().err
        assert "commands/other.py:1" in err
        assert "synology_apm_repo.sdk.dedup" in err

    def test_named_exception_is_allowed_only_at_its_own_module(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
    ) -> None:
        # commands/dump.py resolves, via the script's own unmodified
        # _module_name(), to the same dotted name a real EXCEPTIONS entry
        # for it would key on -- set explicitly here rather than relying
        # on the real, unmodified dict (_patch_source_roots clears it to
        # {}, since the real dict's ~20 entries name modules nothing in
        # this synthetic tmp tree ever resolves to) -- the identical
        # import in a sibling file has no such allowance.
        text = "from synology_apm_repo.sdk.diagnostics import inspect_bucket\n"
        _write_source(tmp_path, "commands/dump.py", text)
        _write_source(tmp_path, "commands/other.py", text)
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)
        monkeypatch.setattr(
            check_sdk_import_boundary,
            "EXCEPTIONS",
            {"synology_apm_repo.cli.commands.dump": check_sdk_import_boundary._DIAGNOSTICS_MODULE},
        )

        assert check_sdk_import_boundary.main() == 1
        err = capsys.readouterr().err
        assert "commands/dump.py" not in err
        assert "commands/other.py:1" in err

    def test_bare_import_statement_is_also_checked(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
    ) -> None:
        _write_source(tmp_path, "commands/other.py", "import synology_apm_repo.sdk.catalog.catalog\n")
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)

        assert check_sdk_import_boundary.main() == 1
        err = capsys.readouterr().err
        assert "synology_apm_repo.sdk.catalog.catalog" in err

    def test_a_stale_exceptions_key_is_reported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        check_sdk_import_boundary: ModuleType,
    ) -> None:
        """A key that no longer names a real module (a rename/move/delete
        that forgot to update ``EXCEPTIONS`` alongside it) would otherwise
        pass silently forever -- nothing else in this script ever looks a
        stale key up."""
        _write_source(tmp_path, "commands/other.py", "")
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)
        monkeypatch.setattr(
            check_sdk_import_boundary,
            "EXCEPTIONS",
            {"synology_apm_repo.cli.commands.renamed_or_deleted": ("synology_apm_repo.sdk.diagnostics",)},
        )

        assert check_sdk_import_boundary.main() == 1
        err = capsys.readouterr().err
        assert "synology_apm_repo.cli.commands.renamed_or_deleted" in err

    def test_an_exceptions_key_matching_a_real_module_is_not_reported_as_stale(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        check_sdk_import_boundary: ModuleType,
    ) -> None:
        _write_source(tmp_path, "commands/dump.py", "")
        _patch_source_roots(monkeypatch, check_sdk_import_boundary, tmp_path)
        monkeypatch.setattr(
            check_sdk_import_boundary,
            "EXCEPTIONS",
            {"synology_apm_repo.cli.commands.dump": ("synology_apm_repo.sdk.diagnostics",)},
        )

        assert check_sdk_import_boundary.main() == 0

    def test_actual_cli_and_browser_source_has_no_violations(self, check_sdk_import_boundary: ModuleType) -> None:
        """No monkeypatching — this repository's real CLI/browser source trees,
        via the script's own unmodified ``ROOT``/``SOURCE_ROOTS``. Turns
        the actual invariant CI relies on into a named, individually-
        runnable assertion instead of only a ``make test``-time signal."""
        assert check_sdk_import_boundary.main() == 0


__all__: list[str] = []
