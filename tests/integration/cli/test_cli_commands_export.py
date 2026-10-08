"""Regression tests for ``synology-apm-repo-cli export``.

Fixture: ``cli_export_vault_plain.json.gz``, recorded against
``vault-plain``: a real non-dedup ``.img.delta`` sidecar.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_VM_REF

_DELTA_REF = f"{VAULT_PLAIN_VM_REF}/device:241/object:2"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_export_vault_plain.json.gz", allow_content=True)


def test_exports_a_small_non_dedup_file_replayed(tmp_path: Path) -> None:
    dst = tmp_path / "out.delta"
    invoke(["export", _DELTA_REF, "-o", str(dst), "--profile", "anything"])
    assert dst.read_bytes()[:4] == b"CbTT"
    assert not dst.with_name(dst.name + ".part").exists()


def test_failed_export_leaves_no_final_file_replayed(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    # A canonical ref naming a version UUID that doesn't exist.
    bogus_ref = "#cat:1/wl:2/ver:00000000-0000-0000-0000-000000000000"
    result = invoke(["export", bogus_ref, "-o", str(dst), "--profile", "anything"], exit_code=None)
    assert result.exit_code != 0
    assert not dst.exists()
