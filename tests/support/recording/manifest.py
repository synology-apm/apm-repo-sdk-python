"""Which test file owns each ``tests/fixtures/*.json.gz``, and how to
re-record it.

A fixture's owner and recipe are computed from ``tests/integration/``'s
source: a test function uses a fixture when its body (or a decorator) names
it as a string literal, names a module-level constant holding it, calls a
module-level helper that does, or requests a module-level pytest fixture
that does. Docstrings don't count. Every test using a fixture is in its
recipe, so one ``make record-fixture`` invocation records the union of
their calls.

``TARGETS`` is the one hand-maintained part: each fixture's
``--record-against`` root, as ``sample:<alias>`` or
``sample:<alias>/<subpath>``. An alias names a sample by what it holds;
the untracked ``targets.toml`` beside this module maps it to its real
``local:<path>`` or ``profile:<name>`` (see ``targets.toml.example``), so
no real sample directory, profile or bucket name is committed.

Usage (``PYTHONPATH=tests``)::

    python -m support.recording.manifest          # one make line per fixture
    python -m support.recording.manifest --check  # fixtures not yet in the current format
"""

from __future__ import annotations

import argparse
import ast
import json
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from support.recording.fixture_store import FORMAT_VERSION, load_fixture_text

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURES_DIR = TESTS_DIR / "fixtures"
INTEGRATION_DIR = TESTS_DIR / "integration"

FIXTURE_SUFFIX = ".json.gz"

#: The untracked alias -> real ``--record-against`` value map ``sample:``
#: targets resolve through.
TARGETS_FILE = Path(__file__).with_name("targets.toml")
#: Its committed template: every alias, each with a one-line description.
TARGETS_EXAMPLE_FILE = Path(__file__).with_name("targets.toml.example")

#: The directory holding every local sample side by side.
_ALL_LOCAL = "sample:all-local"
_VAULT_PLAIN = "sample:vault-plain"
_VAULT_PLAIN_VAULT = "sample:vault-plain/@ActiveProtectVault"
_VAULT_ENCRYPTED = "sample:vault-encrypted"
_VAULT_ENCRYPTED_VAULT = "sample:vault-encrypted/@ActiveProtectVault"
_VAULT_M365_VAULT = "sample:vault-m365/@ActiveProtectVault"
_OBJSTORE_ENCRYPTED = "sample:objstore-encrypted"
_OBJSTORE_M365_ENCRYPTED = "sample:objstore-m365-encrypted"
_PCPS = "sample:pcps"
_PCPS_ENCRYPTED = "sample:pcps-encrypted"

#: Each fixture's ``--record-against`` target; a new fixture adds its entry
#: in the same change.
TARGETS: dict[str, str] = {
    "api_is_encrypted_all_samples.json.gz": _ALL_LOCAL,
    "api_resolve_vault_plain_walk.json.gz": _VAULT_PLAIN,
    "api_vault_encrypted_key_status.json.gz": _VAULT_ENCRYPTED,
    "api_vault_plain_walk.json.gz": _VAULT_PLAIN,
    "catalog_catalog_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "cli_canonical_ref_roundtrip_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_cat_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_doctor_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "cli_doctor_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "cli_dump_bucket_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_dump_composition_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_export_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_key_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "cli_ls_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_trace_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "cli_tree_vault_plain.json.gz": _VAULT_PLAIN,
    "cli_verify_objstore_m365_encrypted.json.gz": _OBJSTORE_M365_ENCRYPTED,
    "cli_verify_vault_plain_clean.json.gz": _VAULT_PLAIN,
    "crypto_chunk0_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "crypto_vault_encrypted_target_db.json.gz": _VAULT_ENCRYPTED,
    "crypto_version_spec_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "dedup_composition_reader_c0_8_vault_plain.json.gz": _VAULT_PLAIN,
    "dedup_file_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "dedup_file_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "dedup_keys_objstore_encrypted.json.gz": _OBJSTORE_ENCRYPTED,
    "dedup_keys_vault_encrypted_verify.json.gz": _VAULT_ENCRYPTED_VAULT,
    "dedup_keys_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "dedup_repository_objstore_encrypted_generations.json.gz": _OBJSTORE_ENCRYPTED,
    "dedup_repository_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "dedup_repository_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "device_pcps_missing_fids_pc_encrypted.json.gz": _PCPS_ENCRYPTED,
    "device_pcps_pc_and_ps.json.gz": _PCPS,
    "device_vm_mbr_gpt_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "device_vm_repo_root_vault_plain.json.gz": _VAULT_PLAIN,
    "device_vm_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "disk_fs_ext4_btrfs_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "disk_fs_ntfs_fat32_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "disk_fs_ntfs_system32_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "file_map_tree_vault_m365.json.gz": _VAULT_M365_VAULT,
    "fingerprint_objstore_encrypted.json.gz": "sample:objstore-encrypted/@ActiveProtectData/BikXpRbFNGI1",
    "fingerprint_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "fingerprint_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "format_composition_c0_8_header_vault_plain.json.gz": _VAULT_PLAIN,
    "fs_config_json_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "fs_no_duplicate_children_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "fs_repo_root_vault_plain.json.gz": _VAULT_PLAIN,
    "object_db_id_cli_teams_chat_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "object_db_id_vault_plain_teams_chat.json.gz": _VAULT_PLAIN_VAULT,
    "pool_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "pool_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "repo_info_real_samples.json.gz": _ALL_LOCAL,
    "saas_content_calendar_alice_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "saas_content_mail_family_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "saas_content_mail_m365_grace_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "saas_stream_objectdb_discovery_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "storage_generations_objstore_encrypted_transactions.json.gz": _OBJSTORE_ENCRYPTED,
    "storage_generations_objstore_m365_encrypted_repo_info.json.gz": _OBJSTORE_M365_ENCRYPTED,
    "storage_layout_all_samples.json.gz": _ALL_LOCAL,
    "storage_layout_object_store_objstore_m365_encrypted_dedup_query.json.gz": _OBJSTORE_M365_ENCRYPTED,
    "storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz": _OBJSTORE_M365_ENCRYPTED,
    "storage_sqlite_source_envelopes_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
    "storage_sqlite_source_envelopes_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "tui_browse_happy_path_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "tui_hex_filter_refresh_objstore_encrypted_pilot.json.gz": _OBJSTORE_ENCRYPTED,
    "tui_hex_filter_refresh_vault_plain_pilot.json.gz": _VAULT_PLAIN,
    "tui_labels_vault_encrypted_pilot.json.gz": _VAULT_ENCRYPTED_VAULT,
    "tui_labels_vault_plain_pilot.json.gz": _VAULT_PLAIN_VAULT,
    "tui_objstore_encrypted_pilot_walk.json.gz": _OBJSTORE_ENCRYPTED,
    "tui_preview_vault_plain_pilot.json.gz": _VAULT_PLAIN,
    "tui_samples_dir_scan.json.gz": _ALL_LOCAL,
    "tui_tree_and_key_vault_plain_pilot.json.gz": _VAULT_PLAIN_VAULT,
    "tui_vault_encrypted_pilot_walk.json.gz": _VAULT_ENCRYPTED_VAULT,
    "tui_vault_plain_pilot_walk.json.gz": _VAULT_PLAIN,
    "tui_yg_vault_plain_pilot.json.gz": _VAULT_PLAIN,
    "units_pcps_disk_overlapping_fragments.json.gz": _PCPS,
    "units_saas_calendar_gws_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_calendar_m365_grace_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_contact_gws_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_contact_m365_empty_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_drive_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_mail_gws_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_mail_m365_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_raw_object_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_stream_agent_repo_vault_encrypted.json.gz": (
        "sample:vault-encrypted/@ActiveProtectVault/saas/agent_repo"
    ),
    "units_saas_stream_agent_repo_vault_m365.json.gz": "sample:vault-m365/@ActiveProtectVault/saas/agent_repo",
    "units_saas_stream_agent_repo_vault_plain.json.gz": "sample:vault-plain/@ActiveProtectVault/saas/agent_repo",
    "units_saas_stream_agent_repo_vault_vm_m365_encrypted.json.gz": "sample:vault-vm-m365-encrypted/@ActiveProtectVault/saas/agent_repo",
    "units_saas_stream_root_nonempty_vault_plain.json.gz": _VAULT_PLAIN,
    "units_saas_stream_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_teams_chat_second_stream_and_chats_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "units_saas_teams_chat_vault_plain.json.gz": _VAULT_PLAIN_VAULT,
    "vm_key_verification_vault_encrypted.json.gz": _VAULT_ENCRYPTED_VAULT,
}


@dataclass(frozen=True, slots=True)
class FixtureUse:
    """One fixture name as ``tests/integration/`` uses it.

    Attributes:
        name: The fixture's file name under ``tests/fixtures/``.
        files: Every test file using it, repository-relative; a well-formed
            suite has exactly one.
        node_ids: Every test (pytest node id) using it, across ``files``.
    """

    name: str
    files: tuple[str, ...]
    node_ids: tuple[str, ...]


def _docstring_nodes(tree: ast.Module) -> set[int]:
    """``id()`` of every docstring constant in ``tree``."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                ids.add(id(first.value))
    return ids


def _fixture_decorator(func: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.expr | None:
    """``func``'s ``@pytest.fixture``/``@pytest.fixture(...)`` decorator, if any."""
    for decorator in func.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute) and target.attr == "fixture":
            return decorator
    return None


def _is_autouse(decorator: ast.expr) -> bool:
    return isinstance(decorator, ast.Call) and any(
        kw.arg == "autouse" and isinstance(kw.value, ast.Constant) and kw.value.value is True
        for kw in decorator.keywords
    )


def _file_uses(path: Path, rel: str) -> dict[str, set[str]]:
    """Fixture name -> node ids of the tests in ``path`` that use it."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    docstrings = _docstring_nodes(tree)

    def literals_and_names(node: ast.AST) -> tuple[set[str], set[str]]:
        literals: set[str] = set()
        names: set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and id(sub) not in docstrings:
                if sub.value.endswith(FIXTURE_SUFFIX):
                    literals.add(sub.value)
            elif isinstance(sub, ast.Name):
                names.add(sub.id)
        return literals, names

    # Every module-level definition a test can reach by name: a constant, a
    # helper function, or a pytest fixture (reached through a parameter, or
    # by every test when autouse).
    direct: dict[str, set[str]] = {}
    edges: dict[str, set[str]] = {}
    autouse: set[str] = set()
    tests: list[str] = []

    def add_function(key: str, func: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        direct[key], edges[key] = literals_and_names(func)
        edges[key] |= {arg.arg for arg in (*func.args.posonlyargs, *func.args.args, *func.args.kwonlyargs)}

    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            add_function(stmt.name, stmt)
            decorator = _fixture_decorator(stmt)
            if decorator is not None and _is_autouse(decorator):
                autouse.add(stmt.name)
            elif decorator is None and stmt.name.startswith("test"):
                tests.append(stmt.name)
        elif isinstance(stmt, ast.ClassDef) and stmt.name.startswith("Test"):
            methods = [m for m in stmt.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))]
            # A fixture or helper method of the class counts for every test in it.
            shared_literals: set[str] = set()
            shared_names: set[str] = set()
            for method in methods:
                if not method.name.startswith("test"):
                    literals, names = literals_and_names(method)
                    shared_literals |= literals
                    shared_names |= names
            for method in methods:
                if method.name.startswith("test"):
                    key = f"{stmt.name}::{method.name}"
                    add_function(key, method)
                    direct[key] |= shared_literals
                    edges[key] |= shared_names
                    tests.append(key)
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value is not None:
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    direct[target.id], edges[target.id] = literals_and_names(stmt.value)

    def reach(start: str) -> set[str]:
        seen = {start, *autouse}
        stack = list(seen)
        found: set[str] = set()
        while stack:
            current = stack.pop()
            found |= direct.get(current, set())
            for name in edges.get(current, set()):
                if name in direct and name not in seen and name not in tests:
                    seen.add(name)
                    stack.append(name)
        return found

    uses: dict[str, set[str]] = defaultdict(set)
    for key in tests:
        for name in reach(key):
            uses[name].add(f"{rel}::{key}")
    return uses


def fixture_uses(integration_dir: Path = INTEGRATION_DIR) -> dict[str, FixtureUse]:
    """Every fixture name ``integration_dir``'s tests use, by name.

    Args:
        integration_dir: The ``tests/integration`` tree to scan; paths and
            node ids are relative to the directory two levels above it.

    Returns:
        Each used fixture name's ``FixtureUse``.
    """
    root = integration_dir.parents[1]
    files: dict[str, set[str]] = defaultdict(set)
    node_ids: dict[str, set[str]] = defaultdict(set)
    for path in sorted(integration_dir.rglob("test_*.py")):
        rel = path.relative_to(root).as_posix()
        for name, ids in _file_uses(path, rel).items():
            files[name].add(rel)
            node_ids[name] |= ids
    return {
        name: FixtureUse(name=name, files=tuple(sorted(files[name])), node_ids=tuple(sorted(node_ids[name])))
        for name in sorted(files)
    }


def committed_fixtures(fixtures_dir: Path = FIXTURES_DIR) -> list[str]:
    """Every committed recorded fixture's file name, sorted."""
    return sorted(path.name for path in fixtures_dir.glob(f"*{FIXTURE_SUFFIX}"))


def fixture_format(path: Path) -> int:
    """The ``"format"`` a fixture file was written in (1 when it has none)."""
    format_version: int = json.loads(load_fixture_text(path)).get("format", 1)
    return format_version


def load_targets(path: Path = TARGETS_FILE) -> dict[str, str]:
    """Alias -> real ``--record-against`` value (``local:<path>`` or
    ``profile:<name>``), from ``path``; empty when it doesn't exist."""
    if not path.is_file():
        return {}
    with path.open("rb") as f:
        return {str(alias): str(value) for alias, value in tomllib.load(f).items()}


def sample_directory_renames(targets: dict[str, str]) -> dict[str, str]:
    """Real directory name -> alias, for every local sample in ``targets``.

    A local target holding other local targets (the directory of every
    sample) is the container they are renamed within, not a sample, so it
    is left out.

    Args:
        targets: ``load_targets()``'s map.

    Returns:
        Each local sample directory's basename, mapped to its alias.
    """
    paths = {
        alias: Path(value.removeprefix("local:")).resolve()
        for alias, value in targets.items()
        if value.startswith("local:")
    }
    return {
        path.name: alias for alias, path in paths.items() if not any(path in other.parents for other in paths.values())
    }


def real_names(targets: dict[str, str]) -> set[str]:
    """Every real name ``targets`` maps an alias to: each local sample's
    directory name and each profile name."""
    profiles = {value.removeprefix("profile:") for value in targets.values() if value.startswith("profile:")}
    return set(sample_directory_renames(targets)) | profiles


def _target_argument(target: str, targets: dict[str, str]) -> str:
    alias, _, subpath = target.removeprefix("sample:").partition("/")
    value = targets.get(alias, f"<{alias} in {TARGETS_FILE.name}>")
    if subpath and not value.startswith("profile:"):
        return f"{value.rstrip('/')}/{subpath}"
    return value


def record_commands(targets: dict[str, str]) -> list[str]:
    """One ``make record-fixture`` command per committed fixture, sorted by
    target so a sample's fixtures are recorded together.

    Args:
        targets: ``load_targets()``'s map; an alias missing from it prints
            as a placeholder.

    Returns:
        The commands, one per fixture.
    """
    uses = fixture_uses()
    names = sorted(committed_fixtures(), key=lambda name: (TARGETS.get(name, ""), name))
    return [
        f'make record-fixture TARGET={_target_argument(TARGETS[name], targets)} TEST="{" ".join(uses[name].node_ids)}"'
        for name in names
    ]


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--check", action="store_true", help=f"list fixtures not in format {FORMAT_VERSION}; exit 1 if any"
    )
    args = parser.parse_args()
    if args.check:
        stale = [name for name in committed_fixtures() if fixture_format(FIXTURES_DIR / name) != FORMAT_VERSION]
        for name in stale:
            print(f"{name}: format {fixture_format(FIXTURES_DIR / name)}, record with {TARGETS.get(name)}")  # noqa: T201
        raise SystemExit(1 if stale else 0)
    for command in record_commands(load_targets()):
        print(command)  # noqa: T201


if __name__ == "__main__":
    _main()
