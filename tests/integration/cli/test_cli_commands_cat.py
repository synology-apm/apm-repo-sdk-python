"""Regression tests for ``synology-apm-repo-cli cat``: reading a real VM
disk image's first 520 bytes, and refusing the version's device folder.

Fixture: ``cli_cat_vault_plain.json.gz``, recorded against ``vault-plain``.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_VM_REF

_DISK_REF = f"{VAULT_PLAIN_VM_REF}/device:241/object:1"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_cat_vault_plain.json.gz", allow_content=True)


def test_reads_a_real_mbr_gpt_header_replayed() -> None:
    result = invoke(["cat", _DISK_REF, "--offset", "0", "--length", "520", "--profile", "anything"])
    data = result.stdout_bytes
    assert data[510:512] == bytes.fromhex("55aa")
    assert data[512:520] == b"EFI PART"


def test_a_folder_ref_fails_cleanly_replayed() -> None:
    result = invoke(["cat", f"{VAULT_PLAIN_VM_REF}/device:241", "--profile", "anything"], exit_code=None)
    assert result.exit_code != 0
    assert "folder" in result.output
