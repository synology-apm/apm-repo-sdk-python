"""``support/recording/manifest.py``: every committed recorded fixture has
exactly one owning test file and a recording target, every fixture a test
names exists, no test records against two targets, and no integration
``conftest.py`` names a fixture; every target names its sample by an alias,
and no committed file names a real sample."""

from __future__ import annotations

import ast
import gzip
import subprocess
import tomllib
from pathlib import Path

import pytest

from support.recording.manifest import (
    FIXTURE_SUFFIX,
    TARGETS,
    TARGETS_EXAMPLE_FILE,
    TARGETS_FILE,
    TESTS_DIR,
    _target_argument,
    committed_fixtures,
    fixture_uses,
    load_targets,
    real_names,
    sample_directory_renames,
)


def test_every_fixture_is_used_by_exactly_one_test_file() -> None:
    uses = fixture_uses()
    shared = {name: use.files for name, use in uses.items() if len(use.files) != 1}
    unused = set(committed_fixtures()) - set(uses)
    assert shared == {}
    assert unused == set()


def test_every_fixture_a_test_names_is_committed() -> None:
    assert set(fixture_uses()) - set(committed_fixtures()) == set()


def test_every_fixture_has_a_target_and_no_target_is_stale() -> None:
    committed = set(committed_fixtures())
    assert committed - set(TARGETS) == set()
    assert set(TARGETS) - committed == set()


def test_no_test_records_two_fixtures_against_different_targets() -> None:
    targets_by_test: dict[str, set[str]] = {}
    for name, use in fixture_uses().items():
        for node_id in use.node_ids:
            targets_by_test.setdefault(node_id, set()).add(TARGETS[name])
    assert {node_id: t for node_id, t in targets_by_test.items() if len(t) > 1} == {}


def test_a_fixture_is_traced_through_constants_helpers_and_fixtures_but_not_docstrings(tmp_path: Path) -> None:
    integration = tmp_path / "tests" / "integration"
    integration.mkdir(parents=True)
    (integration / "test_x.py").write_text(
        '"""Mentions ``doc_only.json.gz``."""\n'
        "import pytest\n"
        'CONSTANT = "via_constant.json.gz"\n'
        "def _helper():\n"
        "    return CONSTANT\n"
        "@pytest.fixture(autouse=True)\n"
        "def _everywhere():\n"
        '    return "via_autouse.json.gz"\n'
        "@pytest.fixture\n"
        "def requested():\n"
        '    return "via_fixture.json.gz"\n'
        "def test_helper():\n"
        "    _helper()\n"
        "def test_requests(requested):\n"
        '    """Also ``doc_only.json.gz``."""\n'
        "class TestGroup:\n"
        "    def test_literal(self):\n"
        '        return "via_literal.json.gz"\n'
    )
    uses = {name: use.node_ids for name, use in fixture_uses(integration).items()}
    file = "tests/integration/test_x.py"
    assert uses == {
        "via_constant.json.gz": (f"{file}::test_helper",),
        "via_fixture.json.gz": (f"{file}::test_requests",),
        "via_literal.json.gz": (f"{file}::TestGroup::test_literal",),
        "via_autouse.json.gz": (f"{file}::TestGroup::test_literal", f"{file}::test_helper", f"{file}::test_requests"),
    }


def test_every_target_names_a_sample_by_an_alias_in_the_example_map() -> None:
    example = tomllib.loads(TARGETS_EXAMPLE_FILE.read_text(encoding="utf-8"))
    assert [target for target in TARGETS.values() if not target.startswith("sample:")] == []
    aliases = {target.removeprefix("sample:").partition("/")[0] for target in TARGETS.values()}
    assert aliases - set(example) == set()


def test_a_local_sample_is_renamed_by_its_directory_name_and_its_container_is_not(tmp_path: Path) -> None:
    targets = {
        "all": f"local:{tmp_path}",
        "plain": f"local:{tmp_path}/alice-backup",
        "remote": "profile:bob-profile",
    }
    assert sample_directory_renames(targets) == {"alice-backup": "plain"}
    assert real_names(targets) == {"alice-backup", "bob-profile"}


def test_a_sample_target_resolves_through_the_map_with_its_subpath() -> None:
    targets = {"plain": "local:/data/alice-backup", "remote": "profile:bob-profile"}
    assert (
        _target_argument("sample:plain/@ActiveProtectVault", targets) == "local:/data/alice-backup/@ActiveProtectVault"
    )
    assert _target_argument("sample:remote", targets) == "profile:bob-profile"
    assert _target_argument("sample:missing/x", targets) == f"<missing in {TARGETS_FILE.name}>/x"


#: The pathspecs whose new, not-yet-added files the real-name check reads.
_SOURCE_DIRS = ("tests", "packages", "docs", "scripts", "examples", ".github", "*.md")


def test_no_committed_file_names_a_real_sample() -> None:
    """``targets.toml`` is untracked, so this runs only where it exists:
    every committed or addable file, fixtures decompressed, is free of each
    real name it maps an alias to."""
    names = real_names(load_targets())
    if not names:
        pytest.skip(f"no {TARGETS_FILE.name}: the real sample names are unknown here")
    root = TESTS_DIR.parent
    # Tracked files, plus new ones under a directory that ships or is
    # committed with the code -- not stray local output (a profile dump).
    git = ["git", "ls-files", "-z"]
    tracked = subprocess.run([*git, "--cached"], cwd=root, capture_output=True, check=True).stdout.decode()
    new = subprocess.run(
        [*git, "--others", "--exclude-standard", "--", *_SOURCE_DIRS], cwd=root, capture_output=True, check=True
    ).stdout.decode()
    hits = []
    for rel in sorted(set(filter(None, (tracked + new).split("\0")))):
        path = root / rel
        if not path.is_file():
            continue
        data = path.read_bytes()
        if rel.endswith(FIXTURE_SUFFIX):
            data = gzip.decompress(data)
        hits += [f"{rel}: {name}" for name in sorted(names) if name.encode() in data]
    assert hits == []


def test_a_fixture_named_in_a_test_class_fixture_method_counts_for_the_class(tmp_path: Path) -> None:
    test_file = tmp_path / "tests" / "integration" / "sdk" / "test_x.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "import pytest\n"
        "class TestX:\n"
        "    @pytest.fixture\n"
        "    def store(self):\n"
        "        return 'x.json.gz'\n"
        "    def test_a(self, store): ...\n"
    )
    uses = fixture_uses(tmp_path / "tests" / "integration")
    assert uses["x.json.gz"].node_ids == ("tests/integration/sdk/test_x.py::TestX::test_a",)


def test_no_integration_conftest_names_a_fixture() -> None:
    """A fixture's loader stays with the test file that owns it."""
    offenders = [
        path.relative_to(TESTS_DIR).as_posix()
        for path in sorted((TESTS_DIR / "integration").rglob("conftest.py"))
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith(".json.gz")
    ]
    assert offenders == []
