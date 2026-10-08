"""Regression tests for the canonical-ref round trip: a ref printed by
``ls``/``tree`` resolves when pasted into ``cat``/``export``. Under
``--profile`` a printed ref's ``fs_path`` half is empty, so it starts with
``#``.

Fixture: ``cli_canonical_ref_roundtrip_vault_plain.json.gz``, recorded
against ``vault-plain``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

import synology_apm_repo.cli.repo_session as repo_session_mod
from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_VM_REF

_DISK_REF = f"{VAULT_PLAIN_VM_REF}/device:241/object:1"


# ``tree --depth`` would expand the disk image's "(filesystem)" sibling, whose
# disk reads this fixture doesn't record.
pytestmark = pytest.mark.usefixtures("no_disk_fs_sibling")


def test_ref_printed_by_ls_can_be_pasted_straight_into_cat_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_canonical_ref_roundtrip_vault_plain.json.gz", allow_content=True)

    listed = invoke(["--verbose", "--json", "ls", _DISK_REF, "--profile", "anything"])
    rows = json.loads(listed.stdout)
    assert len(rows) == 1
    canonical_ref = rows[0]["ref"]
    assert canonical_ref.startswith("#")

    result = invoke(["cat", canonical_ref, "--offset", "0", "--length", "520", "--profile", "anything"])
    data = result.stdout_bytes
    assert data[510:512] == bytes.fromhex("55aa")
    assert data[512:520] == b"EFI PART"


def test_ref_printed_by_tree_can_be_pasted_straight_into_export_replayed(
    patch_profile_store: Callable[..., None], tmp_path: Path
) -> None:
    fixture_name = "cli_canonical_ref_roundtrip_vault_plain.json.gz"
    patch_profile_store(fixture_name, repo_session_mod, allow_content=True)

    result = invoke(["--json", "tree", VAULT_PLAIN_VM_REF, "--depth", "2", "--ref", "--profile", "anything"])
    # A version ref lands on the provider's placeholder root, which tree
    # lists by its children: --json is a bare array of devices.
    entries = json.loads(result.stdout)
    delta_entry = next(c for device in entries for c in device["children"] if c["name"].endswith(".img.delta"))
    canonical_ref = delta_entry["ref"]
    assert canonical_ref.startswith("#")

    dst = tmp_path / "roundtrip.delta"
    invoke(["export", canonical_ref, "-o", str(dst), "--profile", "anything"])
    assert dst.read_bytes()[:4] == b"CbTT"
