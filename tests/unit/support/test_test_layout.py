"""The suite's layout, as tests/CLAUDE.md's "Layout and naming" lays it out:
no module under ``tests/`` imports a test file (shared test code lives in
an importable module: ``support.*``, ``<split>.<distribution>.*_fakes`` /
``*_drivers``), and every test file is named after the module it covers."""

from __future__ import annotations

import ast
from pathlib import Path

_TESTS = Path(__file__).resolve().parents[2]
_REPO = _TESTS.parent


def _package(distribution: str) -> Path:
    return _REPO / "packages" / f"synology-apm-repo-{distribution}" / "src" / "synology_apm_repo" / distribution


#: The name a ``tests/unit/support/`` file checking the suite's own layout
#: carries, in place of a ``tests/support`` module.
_SUITE_LAYOUT = "test_layout"


def imported_test_modules(source: str) -> list[str]:
    """Every ``test_*`` module ``source`` imports, by its dotted name."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            # ``from . import test_x`` has no module: the alias is the import.
            module = node.module or ""
            names = [module, *(f"{module}.{alias.name}".lstrip(".") for alias in node.names)]
        elif isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        else:
            continue
        found.extend(name for name in names if name.rsplit(".", 1)[-1].startswith("test_"))
    return found


def module_paths(root: Path) -> set[str]:
    """Every module and package below ``root`` as a test file names it: its
    path relative to ``root`` with each part's leading underscores dropped,
    joined by ``_``; a package is its directory. ``root`` itself is not
    included."""
    paths: set[str] = set()
    for path in root.rglob("*.py"):
        parts = path.relative_to(root).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        for end in range(1, len(parts) + 1):
            paths.add("_".join(part.lstrip("_") for part in parts[:end]))
    return paths


def names_a_module(stem: str, prefix: str, modules: set[str]) -> bool:
    """Whether ``stem`` is ``test_<prefix><module>`` or
    ``test_<prefix><module>_<behaviour>`` for one of ``modules``."""
    if not stem.startswith(f"test_{prefix}"):
        return False
    words = stem.removeprefix(f"test_{prefix}").split("_")
    ends = [end for end in range(1, len(words) + 1) if "_".join(words[:end]) in modules]
    if not ends:
        return False
    named, rest = "_".join(words[: ends[-1]]), words[ends[-1] :]
    # Past a package, the behaviour must not be a near-miss of one of its
    # modules (``commands_exporter`` beside ``commands/export.py``).
    children = {m.removeprefix(f"{named}_").split("_")[0] for m in modules if m.startswith(f"{named}_")}
    word = rest[0] if rest else ""
    return not rest or not any(
        word.startswith(child) or (len(word) >= 3 and child.startswith(word)) for child in children
    )


def _naming_rules() -> dict[Path, tuple[str, set[str]]]:
    """Each test directory's stem prefix and the module paths its files name."""
    rules: dict[Path, tuple[str, set[str]]] = {}
    for distribution in ("sdk", "cli", "browser"):
        prefix = "" if distribution == "sdk" else f"{distribution}_"
        modules = module_paths(_package(distribution))
        for split in ("unit", "integration"):
            rules[_TESTS / split / distribution] = (prefix, modules)
    rules[_TESTS / "unit" / "scripts"] = ("", module_paths(_REPO / "scripts"))
    rules[_TESTS / "unit" / "examples"] = ("", module_paths(_REPO / "examples"))
    rules[_TESTS / "unit" / "support"] = ("", module_paths(_TESTS / "support") | {_SUITE_LAYOUT})
    return rules


def test_no_module_under_tests_imports_a_test_file() -> None:
    problems = [
        f"{path.relative_to(_TESTS)} imports {name}"
        for path in sorted(_TESTS.rglob("*.py"))
        for name in imported_test_modules(path.read_text(encoding="utf-8"))
    ]
    assert problems == []


def test_an_import_of_a_test_file_is_reported() -> None:
    source = (
        "from unit.sdk.test_api_session import _repo\nimport unit.cli.test_cli_commands_ls\n"
        "from support import pilot\nfrom . import test_api_catalog\n"
    )
    assert imported_test_modules(source) == [
        "unit.sdk.test_api_session",
        "unit.cli.test_cli_commands_ls",
        "test_api_catalog",
    ]


def test_every_test_file_is_named_after_a_module_it_covers() -> None:
    rules = _naming_rules()
    problems = []
    for path in sorted([*(_TESTS / "unit").rglob("test_*.py"), *(_TESTS / "integration").rglob("test_*.py")]):
        rule = rules.get(path.parent)
        if rule is None:
            problems.append(f"{path.relative_to(_TESTS)}: no naming rule for its directory")
        elif not names_a_module(path.stem, *rule):
            problems.append(f"{path.relative_to(_TESTS)}: names no module of {path.parent.relative_to(_TESTS)}")
    assert problems == []


def test_module_paths_drop_leading_underscores_and_name_packages_by_directory(tmp_path: Path) -> None:
    for name in ("__init__.py", "main.py", "core/__init__.py", "core/_apfs.py", "_shared/store.py"):
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).touch()
    assert module_paths(tmp_path) == {"main", "core", "core_apfs", "shared", "shared_store"}


def test_a_stem_naming_a_module_and_a_behaviour_conforms() -> None:
    modules = {"commands", "commands_export"}
    assert names_a_module("test_cli_commands_export", "cli_", modules)
    assert names_a_module("test_cli_commands_export_cancel", "cli_", modules)
    assert names_a_module("test_cli_commands_canonical_ref_roundtrip", "cli_", modules)  # a package's behaviour


def test_a_stem_naming_no_module_is_reported() -> None:
    modules = {"commands", "commands_export"}
    assert not names_a_module("test_cli_export", "cli_", modules)
    assert not names_a_module("test_cli_commands_exporter", "cli_", {"commands_export"})
    assert not names_a_module("test_cli_commands_exporter", "cli_", modules)  # a near-miss past a package
    assert not names_a_module("test_cli_commands_exp", "cli_", modules)
    assert not names_a_module("test_commands_export", "cli_", modules)
