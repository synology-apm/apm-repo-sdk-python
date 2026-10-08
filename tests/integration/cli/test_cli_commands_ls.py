"""Regression tests for ``synology-apm-repo-cli ls``. With ``--profile``
supplying the store, a ref's ``fs_path`` half is empty, so every ref is
written ``#<fragment>``. Human-ref resolution is covered synthetically by
``tests/unit/cli/test_cli_commands_tree.py`` and ``tests/unit/sdk/test_units_node_ref.py``.

Fixture: ``cli_ls_vault_plain.json.gz``, recorded against ``vault-plain``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_VM_REF, VAULT_PLAIN_VM_VERSION_UID


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_ls_vault_plain.json.gz")


def _rows(output: str) -> list[str]:
    """``ls``'s output lines minus its "... elapsed ..." progress line."""
    return [line for line in output.strip().splitlines() if "elapsed" not in line]


def test_bare_path_lists_backup_sources_replayed() -> None:
    result = invoke(["ls", "#", "--profile", "anything"])
    assert len(_rows(result.output)) == 2  # vault-plain's real 2 connections
    assert "connection_config_id" not in result.output


def test_version_lists_devices_replayed() -> None:
    result = invoke(["ls", VAULT_PLAIN_VM_REF, "--profile", "anything"])
    assert len(_rows(result.output)) == 1  # exactly one real device under this version


def test_device_lists_disk_and_delta_replayed() -> None:
    result = invoke(["ls", f"{VAULT_PLAIN_VM_REF}/device:241", "--profile", "anything"])
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img" in result.output
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img.delta" in result.output


def test_json_output_is_valid_replayed() -> None:
    result = invoke(["--json", "ls", "#", "--profile", "anything"])
    rows = json.loads(result.stdout)
    assert len(rows) == 2  # vault-plain's real 2 connections
    assert all(isinstance(r["name"], str) and r["name"] for r in rows)


def test_verbose_shows_canonical_ref_replayed() -> None:
    result = invoke(["--verbose", "ls", f"{VAULT_PLAIN_VM_REF}/device:241", "--profile", "anything"])
    assert f"cat:1/wl:2/ver:{VAULT_PLAIN_VM_VERSION_UID}" in result.output


def test_bare_ref_flag_shows_canonical_ref_without_verbose_replayed() -> None:
    result = invoke(["ls", f"{VAULT_PLAIN_VM_REF}/device:241", "--profile", "anything", "--ref"])
    assert f"cat:1/wl:2/ver:{VAULT_PLAIN_VM_VERSION_UID}" in result.output


def test_unknown_backup_source_fails_cleanly_replayed() -> None:
    result = invoke(["ls", "#NoSuchSource", "--profile", "anything"], exit_code=None)
    assert result.exit_code != 0
    assert "no backup source named" in result.output
