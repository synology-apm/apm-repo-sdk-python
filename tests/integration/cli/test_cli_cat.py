"""Regression test for ``synology-apm-repo-cli cat`` — replayed from a committed
fixture recorded against real bytes, with **no external dependency** —
same ``patch_profile_store`` fixture (``tests/integration/cli/conftest.py``)
every sibling in this directory uses. ``cat`` resolves its ref via ``Repository.resolve()``, not
``walk()``/``walk_ref()`` the way ``ls``/``tree`` do — a different
internal call sequence even against the same real repository.

Fixture: ``cli_cat_apv1_vault.json.gz``, this file's own dedicated
recording against ``apv-sample-1/@ActiveProtectVault`` — ``repo.resolve()``
against the real VM disk image's first 520 bytes
(``test_reads_a_real_mbr_gpt_header_replayed``), plus the same version's
device folder node, resolved but never opened, for the folder-ref
failure case (``test_a_folder_ref_fails_cleanly_replayed``) — both tests
are this fixture's recording recipe, neither is a subset of the other.
Both refs are canonical — internal catalog identifiers, immune to
catalog-metadata anonymization.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.repo_session as repo_session_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

_VM_REF = "#cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"
_DISK_REF = f"{_VM_REF}/device:241/object:1"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_cat_apv1_vault.json.gz", repo_session_mod, allow_content=True)


def test_reads_a_real_mbr_gpt_header_replayed() -> None:
    result = runner.invoke(app, ["cat", _DISK_REF, "--offset", "0", "--length", "520", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    data = result.stdout_bytes
    assert data[510:512] == bytes.fromhex("55aa")
    assert data[512:520] == b"EFI PART"


def test_a_folder_ref_fails_cleanly_replayed() -> None:
    result = runner.invoke(app, ["cat", f"{_VM_REF}/device:241", "--profile", "anything"])
    assert result.exit_code != 0
    assert "folder" in result.output


__all__: list[str] = []
