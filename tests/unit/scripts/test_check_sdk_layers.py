"""Tests for scripts/check_sdk_layers.py: the layer-direction rules and the
import-cycle rule, each against a tiny synthetic SDK tree under ``tmp_path``."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from support.modules import load_module

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "check_sdk_layers.py"


@pytest.fixture
def check_sdk_layers() -> ModuleType:
    return load_module("check_sdk_layers", _SCRIPT_PATH)


def _write(root: Path, relative: str, text: str = "") -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_downward_imports_are_allowed(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "format/codec.py")
    _write(tmp_path, "dedup/pool.py", "from ..format import codec\nfrom ..errors import SomeError\n")
    _write(tmp_path, "units/provider.py", "from ..dedup import pool\nfrom synology_apm_repo.sdk.format import codec\n")
    _write(tmp_path, "errors.py")

    assert check_sdk_layers.find_violations(tmp_path) == []


@pytest.mark.parametrize(
    ("importer", "source", "target", "expected"),
    [
        pytest.param(
            "dedup/pool.py",
            "from ..units import base\n",
            "units/base.py",
            "dedup/pool.py:1: 'dedup' layer imports higher layer 'units'",
            id="relative",
        ),
        pytest.param(
            "storage/local.py",
            "import synology_apm_repo.sdk.catalog.workload\n",
            "catalog/workload.py",
            "storage/local.py:1: 'storage' layer imports higher layer 'catalog.workload'",
            id="absolute",
        ),
        pytest.param(
            "format/codec.py",
            "from synology_apm_repo.sdk import api\n",
            "api/__init__.py",
            "format/codec.py:1: 'format' layer imports higher layer 'api'",
            id="from-the-root-package",
        ),
    ],
)
def test_upward_import_is_reported(
    check_sdk_layers: ModuleType, tmp_path: Path, importer: str, source: str, target: str, expected: str
) -> None:
    _write(tmp_path, importer, source)
    _write(tmp_path, target)

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == [expected]


def test_function_local_and_type_checking_imports_count(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(
        tmp_path,
        "dedup/export.py",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from ..api import Repository\n"
        "def lazy():\n"
        "    from ..catalog import workload\n",
    )
    _write(tmp_path, "api/__init__.py")
    _write(tmp_path, "catalog/workload.py")

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == [
        "dedup/export.py:3: 'dedup' layer imports higher layer 'api'",
        "dedup/export.py:5: 'dedup' layer imports higher layer 'catalog'",
    ]


def test_diagnostics_ranks_with_api(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "units/probe.py", "from .. import diagnostics\n")
    _write(tmp_path, "diagnostics.py", "from .api import session\n")
    _write(tmp_path, "api/session.py")

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == ["units/probe.py:1: 'units' layer imports higher layer 'diagnostics'"]


def test_concurrency_is_a_leaf(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "concurrency.py", "from ._util import closing\nfrom .dedup import pool\n")
    _write(tmp_path, "_util/closing.py")
    _write(tmp_path, "dedup/pool.py")

    assert check_sdk_layers.find_violations(tmp_path) == [
        "concurrency.py:2: leaf package 'concurrency' imports 'dedup'"
    ]


def test_profiles_may_import_only_storage_and_the_leaves(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(
        tmp_path,
        "profiles/__init__.py",
        "from ..storage import base\nfrom ..errors import SomeError\nfrom ..dedup import pool\n",
    )
    _write(tmp_path, "storage/base.py")
    _write(tmp_path, "dedup/pool.py")

    assert check_sdk_layers.find_violations(tmp_path) == ["profiles/__init__.py:3: 'profiles' imports 'dedup'"]


def test_an_unclassified_top_level_module_is_reported(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "newthing.py")

    assert check_sdk_layers.find_violations(tmp_path) == [
        "newthing.py: top-level module 'newthing' is in no layer, leaf or side package"
    ]


def test_leaf_package_importing_a_layer_is_reported(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "presentation/progress.py", "from ..dedup import pool\nfrom ..errors import SomeError\n")
    _write(tmp_path, "dedup/pool.py")
    _write(tmp_path, "errors.py")

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == ["presentation/progress.py:1: leaf package 'presentation' imports 'dedup'"]


def test_content_layer_may_import_only_units_base_and_content(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "units/base.py")
    _write(tmp_path, "units/device.py")
    _write(tmp_path, "units/content/a.py", "from ..base import ContentSource\nfrom . import b\n")
    _write(tmp_path, "units/content/b.py", "from ..device import DeviceProvider\n")

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == ["units/content/b.py:1: Content layer imports Unit layer module 'units.device'"]


def test_package_init_resolves_relative_imports_against_itself(check_sdk_layers: ModuleType, tmp_path: Path) -> None:
    _write(tmp_path, "storage/__init__.py", "from .local import LocalFsStore\nfrom ..dedup import pool\n")
    _write(tmp_path, "storage/local.py")
    _write(tmp_path, "dedup/pool.py")

    violations = check_sdk_layers.find_violations(tmp_path)

    assert violations == ["storage/__init__.py:2: 'storage' layer imports higher layer 'dedup'"]


def test_main_reports_violations_on_stderr(
    check_sdk_layers: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(tmp_path, "dedup/pool.py", "from ..units import base\n")
    _write(tmp_path, "units/base.py")
    monkeypatch.setattr(check_sdk_layers, "SDK_ROOT", tmp_path)

    assert check_sdk_layers.main() == 1
    captured = capsys.readouterr()
    assert "dedup/pool.py:1" in captured.err
    assert captured.out == ""


def test_main_reports_ok_on_clean_tree(
    check_sdk_layers: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _write(tmp_path, "format/codec.py")
    monkeypatch.setattr(check_sdk_layers, "SDK_ROOT", tmp_path)

    assert check_sdk_layers.main() == 0
    assert "OK" in capsys.readouterr().out


class TestImportCycles:
    """No group of modules may import itself back through module-level or
    function-local imports; ``TYPE_CHECKING`` imports never run and don't
    count."""

    def test_an_acyclic_tree_has_no_cycles(self, check_sdk_layers: ModuleType, tmp_path: Path) -> None:
        _write(tmp_path, "dedup/a.py", "from . import b\n")
        _write(tmp_path, "dedup/b.py", "from . import c\n")
        _write(tmp_path, "dedup/c.py")

        assert check_sdk_layers.find_import_cycles(tmp_path) == []

    def test_a_module_level_cycle_is_reported_with_where_each_edge_is(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        _write(tmp_path, "dedup/a.py", "from . import b\n")
        _write(tmp_path, "dedup/b.py", "x = 1\nfrom .a import thing\n")

        [report] = check_sdk_layers.find_import_cycles(tmp_path)

        assert report.startswith("import cycle among dedup.a, dedup.b: ")
        assert "dedup.a -> dedup.b (dedup/a.py:1, module-level)" in report
        assert "dedup.b -> dedup.a (dedup/b.py:2, module-level)" in report

    def test_a_cycle_closed_only_by_a_function_local_import_is_reported(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        """The import that dodges a cycle at import time is the one that
        would otherwise hide it."""
        _write(tmp_path, "dedup/a.py", "from .b import thing\n")
        _write(tmp_path, "dedup/b.py", "def later():\n    from . import a\n    return a\n")

        [report] = check_sdk_layers.find_import_cycles(tmp_path)

        assert "dedup.b -> dedup.a (dedup/b.py:2, function-local)" in report

    def test_a_cycle_closed_only_by_a_type_checking_import_is_allowed(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        _write(tmp_path, "dedup/a.py", "from .b import thing\n")
        _write(
            tmp_path,
            "dedup/b.py",
            "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    from .a import Thing\n",
        )

        assert check_sdk_layers.find_import_cycles(tmp_path) == []

    def test_the_else_branch_of_a_type_checking_guard_does_count(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        _write(tmp_path, "dedup/a.py", "from .b import thing\n")
        _write(
            tmp_path,
            "dedup/b.py",
            "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    pass\nelse:\n    from .a import Thing\n",
        )

        assert len(check_sdk_layers.find_import_cycles(tmp_path)) == 1

    def test_from_package_import_submodule_is_an_import_of_the_submodule(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        _write(tmp_path, "dedup/__init__.py")
        _write(tmp_path, "dedup/a.py", "from ..dedup import b\n")
        _write(tmp_path, "dedup/b.py", "from .a import thing\n")

        [report] = check_sdk_layers.find_import_cycles(tmp_path)

        assert "dedup.a" in report and "dedup.b" in report

    def test_absolute_imports_are_followed(self, check_sdk_layers: ModuleType, tmp_path: Path) -> None:
        _write(tmp_path, "dedup/a.py", "from synology_apm_repo.sdk.dedup import b\n")
        _write(tmp_path, "dedup/b.py", "import synology_apm_repo.sdk.dedup.a\n")

        assert len(check_sdk_layers.find_import_cycles(tmp_path)) == 1

    def test_importing_a_name_from_a_package_depends_on_its_init(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        """``from pkg import name`` runs ``pkg/__init__.py``; when that in turn
        imports the importing module, it is a real cycle."""
        _write(tmp_path, "dedup/__init__.py", "from .a import thing\n")
        _write(tmp_path, "dedup/a.py", "from . import helper\n")

        [report] = check_sdk_layers.find_import_cycles(tmp_path)

        assert "dedup -> dedup.a" in report and "dedup.a -> dedup" in report

    def test_imports_outside_the_sdk_are_ignored(self, check_sdk_layers: ModuleType, tmp_path: Path) -> None:
        _write(tmp_path, "dedup/a.py", "import os\nfrom pathlib import Path\nfrom rich import console\n")

        assert check_sdk_layers.find_import_cycles(tmp_path) == []

    def test_two_separate_cycles_are_reported_separately(self, check_sdk_layers: ModuleType, tmp_path: Path) -> None:
        _write(tmp_path, "dedup/a.py", "from . import b\n")
        _write(tmp_path, "dedup/b.py", "from . import a\n")
        _write(tmp_path, "units/c.py", "from . import d\n")
        _write(tmp_path, "units/d.py", "from . import c\n")

        reports = check_sdk_layers.find_import_cycles(tmp_path)

        assert [r.split(":")[0] for r in reports] == [
            "import cycle among dedup.a, dedup.b",
            "import cycle among units.c, units.d",
        ]

    def test_find_violations_includes_cycles_next_to_the_layer_rules(
        self, check_sdk_layers: ModuleType, tmp_path: Path
    ) -> None:
        _write(tmp_path, "dedup/a.py", "from ..units import base\nfrom . import b\n")
        _write(tmp_path, "dedup/b.py", "from . import a\n")
        _write(tmp_path, "units/base.py")

        violations = check_sdk_layers.find_violations(tmp_path)

        assert any("'dedup' layer imports higher layer" in v for v in violations)
        assert any(v.startswith("import cycle among dedup.a, dedup.b") for v in violations)

    def test_main_reports_a_cycle_on_stderr(
        self,
        check_sdk_layers: ModuleType,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _write(tmp_path, "dedup/a.py", "from . import b\n")
        _write(tmp_path, "dedup/b.py", "from . import a\n")
        monkeypatch.setattr(check_sdk_layers, "SDK_ROOT", tmp_path)

        assert check_sdk_layers.main() == 1
        captured = capsys.readouterr()
        assert "closes an import cycle" in captured.err
        assert "import cycle among dedup.a, dedup.b" in captured.err
        assert captured.out == ""
