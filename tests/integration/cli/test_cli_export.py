"""Regression test for ``synology-apm-repo-cli export`` — replayed from a committed
fixture recorded against real bytes, with **no external dependency**.

``export.py`` doesn't route through ``cli.repo_session.opened_repo()`` the
way every other command here does — it calls ``resolve_profile_store``
directly, owning its own ``Session``/``open_single_repo`` lifecycle for
clean Ctrl-C cancellation rather than handing that off to a shared helper,
so the monkeypatch target is
``cli.commands.export.resolve_profile_store``, not
``cli.repo_session.resolve_profile_store`` — the ``patch_profile_store``
fixture (``tests/integration/cli/conftest.py``) takes the target module
as its own argument for exactly this reason.

Fixture: ``cli_export_apv1_vault.json.gz``, this file's own dedicated
recording against ``apv-sample-1/@ActiveProtectVault`` — covers the real
``.img.delta`` sidecar file's content (a tiny, 64-byte non-dedup file —
the whole point of this scenario), actually run through
``content.export_to()`` during recording (not just read).
``test_exports_a_small_non_dedup_file_replayed`` is this fixture's
recording recipe; its ref is canonical — an internal catalog
identifier, immune to catalog-metadata anonymization.
``test_failed_export_leaves_no_final_file_replayed`` needs no recording
at all — a canonical ref naming a version UUID that doesn't exist fails
to resolve without ever touching the real backend.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.commands.export as export_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

_VM_REF = "#cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"
_DELTA_REF = f"{_VM_REF}/device:241/object:2"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_export_apv1_vault.json.gz", export_mod, allow_content=True)


def test_exports_a_small_non_dedup_file_replayed(tmp_path: Path) -> None:
    dst = tmp_path / "out.delta"
    result = runner.invoke(app, ["export", _DELTA_REF, "-o", str(dst), "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert dst.read_bytes()[:4] == b"CbTT"
    assert not dst.with_name(dst.name + ".part").exists()


def test_failed_export_leaves_no_final_file_replayed(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    # A canonical ref naming a version UUID that doesn't exist -- resolution
    # fails before ever touching the real backend, so this needs no
    # recording of its own (record_target isn't even used here).
    bogus_ref = "#cat:1/wl:2/ver:00000000-0000-0000-0000-000000000000"
    result = runner.invoke(app, ["export", bogus_ref, "-o", str(dst), "--profile", "anything"])
    assert result.exit_code != 0
    assert not dst.exists()


__all__: list[str] = []
