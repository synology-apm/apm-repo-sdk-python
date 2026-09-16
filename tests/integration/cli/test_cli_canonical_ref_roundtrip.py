"""Regression test for the canonical-ref round-trip contract — a ref
printed by ``ls``/``tree`` must resolve when pasted into ``cat``/
``export`` — replayed from committed fixtures recorded against real
bytes, with **no external dependency**.

Each round trip is genuinely two CLI invocations against the *same*
underlying real data, resolved two different ways (an ``ls``/``tree``
walk to print the canonical form, then a canonical-ref resolve to read
it back) — ``Repository.resolve()`` against a *canonical* ref is yet
another distinct call shape from ``walk()``/``walk_ref()``, on top of
the already-established "different consumer methods need their own
recording" lesson. Both entry refs below are themselves already
canonical (immune to catalog-metadata anonymization — see
``test_cli_ls.py``'s own docstring) rather than human display-name
paths: the round-trip property under test — a ref ``ls``/``tree`` prints
resolves cleanly when pasted into ``cat``/``export`` — doesn't depend on
how the CLI reached that node in the first place, only on what gets
printed and pasted back.

Fixture: ``cli_canonical_ref_roundtrip_apv1_vault.json.gz``, this file's
own dedicated recording against ``apv-sample-1/@ActiveProtectVault`` —
both tests below are this fixture's recording recipe (neither's call
shape is a subset of the other's):

- The ls-to-cat round trip's own recording: walk to the real VM disk
  image, print its canonical ref, resolve *that* ref and read its
  first 520 bytes.
- The tree-to-export round trip's own recording: walk to the device
  tree, print the ``.img.delta`` sidecar's canonical ref, resolve
  *that* ref and export it.

The real test asserts the printed ref starts with ``fs_path + "#"``
where ``fs_path`` is the real filesystem path the CLI was invoked with;
under ``--profile``, ``fs_path`` is always ``""``, so the equivalent
assertion here is just "starts with ``#``".
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
import synology_apm_repo.cli.commands.export as export_mod
import synology_apm_repo.sdk.units.device as device_module
from synology_apm_repo.cli.main import app

runner = CliRunner()

_VM_REF = "#cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"
_DISK_REF = f"{_VM_REF}/device:241/object:1"


@pytest.fixture(autouse=True)
def _no_disk_fs_sibling(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tree-to-export round trip below recurses via ``tree
    --depth``, which eagerly expands every non-leaf child within that
    depth, including units/device.py's own additive "(filesystem)"
    sibling — needing real pytsk3 reads this fixture predates. This
    file is about the canonical-ref round-trip contract, not
    units/content/disk_fs.py — see tests/unit/sdk/test_units_disk_fs.py for that
    feature's own tests."""
    monkeypatch.setattr(device_module, "disk_fs_available", lambda: False)


def test_ref_printed_by_ls_can_be_pasted_straight_into_cat_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_canonical_ref_roundtrip_apv1_vault.json.gz", browse_mod, allow_content=True)

    listed = runner.invoke(app, ["--verbose", "--json", "ls", _DISK_REF, "--profile", "anything"])
    assert listed.exit_code == 0, listed.output
    rows = json.loads(listed.stdout)
    assert len(rows) == 1
    canonical_ref = rows[0]["ref"]
    assert canonical_ref.startswith("#")

    result = runner.invoke(app, ["cat", canonical_ref, "--offset", "0", "--length", "520", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    data = result.stdout_bytes
    assert data[510:512] == bytes.fromhex("55aa")
    assert data[512:520] == b"EFI PART"


def test_ref_printed_by_tree_can_be_pasted_straight_into_export_replayed(
    patch_profile_store: Callable[..., None], tmp_path: Path
) -> None:
    fixture_name = "cli_canonical_ref_roundtrip_apv1_vault.json.gz"
    patch_profile_store(fixture_name, browse_mod, allow_content=True)
    patch_profile_store(fixture_name, export_mod, allow_content=True)

    result = runner.invoke(app, ["--json", "tree", _VM_REF, "--depth", "2", "--ref", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    # _VM_REF names no item-level segments, landing tree()'s own walk
    # right at the provider's root (units/device.py's "Devices" node) --
    # a synthetic placeholder that no longer wraps its children in a
    # printed heading (see tree.py's own module-level comments), so
    # --json here is a bare array of that root's own children directly,
    # not {"children": [...]}.
    entries = json.loads(result.stdout)
    delta_entry = next(c for device in entries for c in device["children"] if c["name"].endswith(".img.delta"))
    canonical_ref = delta_entry["ref"]
    assert canonical_ref.startswith("#")

    dst = tmp_path / "roundtrip.delta"
    export_result = runner.invoke(app, ["export", canonical_ref, "-o", str(dst), "--profile", "anything"])
    assert export_result.exit_code == 0, export_result.output
    assert dst.read_bytes()[:4] == b"CbTT"


__all__: list[str] = []
