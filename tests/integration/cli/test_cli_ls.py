"""Regression test for ``synology-apm-repo-cli ls`` — replayed from a committed
fixture recorded against real bytes, with **no external dependency** —
same ``patch_profile_store`` fixture (``tests/conftest.py``) every
sibling in this directory uses.

A CLI ``<ref>`` argument is ``<fs_path>[#<fragment>]`` — with
``--profile`` supplying the store (instead of a literal filesystem
path), the real ``fs_path`` half is simply empty, so every ref below is
written as ``#<fragment>`` rather than ``<samples_dir>/apv-sample-1#<fragment>``.
Every ref here is canonical (``cat:``/``wl:``/``ver:``/``device:``/
``object:``-prefixed) rather than a human display-name path — canonical
segments are internal catalog identifiers, immune to catalog-metadata
anonymization, so this file (unlike a human ref built from real display
names) is directly re-recordable against a real backend. Human-ref
parsing/walking itself (connection → workloads, workload → versions, name
collision disambiguation) is covered synthetically, with fake data, by
``tests/unit/cli/test_cli_tree.py`` and ``tests/unit/sdk/test_units_node_ref.py``
— this file's job is proving ``ls``'s own real CLI wiring end to end, not
re-proving human-ref resolution.

Fixture: ``cli_ls_apv1_vault.json.gz``, this file's own dedicated
recording against ``apv-sample-1/@ActiveProtectVault`` — the walk goes
all the way from the root down through connection 1, workload 2, its
``2026-08-07 09:00:10`` version, and that version's own device + disk/
delta tree; every shallower ref this file's tests use is a strict prefix
of that one walk. ``test_device_lists_disk_and_delta_replayed`` is this
file's own recording recipe (its call sequence is a superset of every
other test here) — ``make record-fixture
TARGET=local:apv-sample-1/@ActiveProtectVault TEST=<its node id>``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import ModuleType

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

_VM_REF = "#cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[[str, ModuleType], None]) -> None:
    patch_profile_store("cli_ls_apv1_vault.json.gz", browse_mod)


def _rows(output: str) -> list[str]:
    """``ls``'s own progress reporting prints a human-readable
    "discovering ... elapsed ..." line before the listing itself —
    strip it so a row count reflects only real listed items."""
    return [line for line in output.strip().splitlines() if "elapsed" not in line]


def test_bare_path_lists_backup_sources_replayed() -> None:
    result = runner.invoke(app, ["ls", "#", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert len(_rows(result.output)) == 2  # apv-sample-1's real 2 connections
    assert "connection_config_id" not in result.output


def test_version_lists_devices_replayed() -> None:
    result = runner.invoke(app, ["ls", _VM_REF, "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert len(_rows(result.output)) == 1  # exactly one real device under this version


def test_device_lists_disk_and_delta_replayed() -> None:
    result = runner.invoke(app, ["ls", f"{_VM_REF}/device:241", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img" in result.output
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img.delta" in result.output


def test_json_output_is_valid_replayed() -> None:
    result = runner.invoke(app, ["--json", "ls", "#", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert len(rows) == 2  # apv-sample-1's real 2 connections
    assert all(isinstance(r["name"], str) and r["name"] for r in rows)


def test_verbose_shows_canonical_ref_replayed() -> None:
    result = runner.invoke(app, ["--verbose", "ls", f"{_VM_REF}/device:241", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7" in result.output


def test_bare_ref_flag_shows_canonical_ref_without_verbose_replayed() -> None:
    # ls.py's own ``show_ref = state.verbose or show_ref`` -- every other
    # test in this file either passes --verbose or neither flag; this
    # is the only one proving --ref alone (no --verbose) also works.
    result = runner.invoke(app, ["ls", f"{_VM_REF}/device:241", "--profile", "anything", "--ref"])
    assert result.exit_code == 0, result.output
    assert "cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7" in result.output


def test_unknown_backup_source_fails_cleanly_replayed() -> None:
    result = runner.invoke(app, ["ls", "#NoSuchSource", "--profile", "anything"])
    assert result.exit_code != 0
    assert "no backup source named" in result.output


__all__: list[str] = []
