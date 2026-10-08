"""Regression tests for ``synology-apm-repo-cli tree``. With ``--profile``
supplying the store, a ref's ``fs_path`` half is empty, so every ref is
written ``#<fragment>``. A disambiguated human ref resolving back is
covered synthetically by ``tests/unit/sdk/test_units_node_ref.py``.

Fixture: ``cli_tree_vault_plain.json.gz``, recorded against
``vault-plain``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

import synology_apm_repo.sdk.units.device as device_module
from support.cli import invoke
from support.recording.sample_constants import VAULT_PLAIN_VM_REF, VAULT_PLAIN_VM_VERSION_UID


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[..., None], monkeypatch: pytest.MonkeyPatch) -> None:
    patch_profile_store("cli_tree_vault_plain.json.gz")
    # tree --depth would expand the disk image's "(filesystem)" sibling,
    # whose disk reads this fixture doesn't record.
    monkeypatch.setattr(device_module, "disk_fs_available", lambda: False)


def test_depth_limits_the_catalog_tree_replayed() -> None:
    result = invoke(["tree", "#", "--depth", "1", "--profile", "anything"])
    lines = [line for line in result.output.splitlines() if "elapsed" not in line]
    # Connections print at any depth; --depth 1 adds their workloads but
    # no versions: vault-plain's 2 connections plus 25 workloads.
    assert len(lines) == 27


_SUB_TYPE_SUFFIXES = tuple(f"· {sub_type}" for sub_type in ("MAIL", "CALENDAR", "CONTACT", "DRIVE"))


def test_disambiguates_colliding_workload_names_replayed() -> None:
    result = invoke(["tree", "#", "--depth", "2", "--profile", "anything"])
    # Matched by the "· <SUB_TYPE>" suffix disambiguate() appends, not by an
    # anonymized display name; more than one real persona collides, so
    # lines are grouped by the prefix before the suffix.
    colliding_lines = [
        line.strip().rstrip("/")
        for line in result.output.splitlines()
        if line.strip().rstrip("/").endswith(_SUB_TYPE_SUFFIXES)
    ]
    assert not any("#" in line for line in colliding_lines), colliding_lines
    by_prefix: dict[str, set[str]] = {}
    for line in colliding_lines:
        prefix, _, suffix = line.rpartition(" · ")
        by_prefix.setdefault(prefix, set()).add(suffix)
    # At least one real persona collides across exactly these 4 sub_types.
    assert {"MAIL", "CALENDAR", "CONTACT", "DRIVE"} in by_prefix.values(), by_prefix


def test_json_tree_of_device_items_replayed() -> None:
    result = invoke(["--json", "tree", f"{VAULT_PLAIN_VM_REF}/device:241", "--profile", "anything"])
    (entry,) = json.loads(result.stdout)
    names = {c["name"] for c in entry["children"]}
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img" in names


def test_human_output_actually_prints_the_ref_replayed() -> None:
    result = invoke(["tree", f"{VAULT_PLAIN_VM_REF}/device:241", "--ref", "--profile", "anything"])
    assert f"cat:1/wl:2/ver:{VAULT_PLAIN_VM_VERSION_UID}" in result.output
