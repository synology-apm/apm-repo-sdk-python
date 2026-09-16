"""Regression test for ``synology-apm-repo-cli tree`` (and one ``ls``-based
disambiguation-round-trip scenario) — replayed from a committed fixture
recorded against real bytes, with **no external dependency** — same
``patch_profile_store`` fixture (``tests/conftest.py``) every sibling
in this directory uses. See ``test_cli_ls.py``'s own docstring
for why refs are written ``"#<fragment>"`` under ``--profile``.

Fixture: ``cli_tree_apv1_vault.json.gz``, this file's own dedicated
recording against ``apv-sample-1/@ActiveProtectVault`` — covers both real
connections' full workload lists (``tree --depth 2`` walks the whole
catalog, not just one connection the way ``test_cli_ls.py``'s own
narrower recording does), at least one real persona whose 4 workloads
(MAIL/CALENDAR/CONTACT/DRIVE) collide on one display name, and the same
VM device tree (navigated via a canonical ref, immune to
catalog-metadata anonymization — see ``test_cli_ls.py``'s own
docstring). Recording it needs both the depth-2 catalog walk
(``test_disambiguates_colliding_workload_names_replayed``) and the
device-tree walk (``test_json_tree_of_device_items_replayed``) — no
single test here covers both.

The disambiguated-name-resolves-back round trip itself (does a hash-
suffixed, sub_type-hinted human ref resolve to the right one of several
colliding real workloads) isn't covered here: it's already proven
synthetically, with fake colliding names, by
``tests/unit/sdk/test_units_node_ref.py``'s own
``test_hints_resolve_a_collision_the_same_way_disambiguate_does``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from types import ModuleType

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
import synology_apm_repo.sdk.units.device as device_module
from synology_apm_repo.cli.main import app

runner = CliRunner()

_VM_REF = "#cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"


@pytest.fixture(autouse=True)
def _replay(patch_profile_store: Callable[[str, ModuleType], None], monkeypatch: pytest.MonkeyPatch) -> None:
    patch_profile_store("cli_tree_apv1_vault.json.gz", browse_mod)
    # tree's own --depth recursion eagerly expands every non-leaf child
    # it finds within that depth, including units/device.py's own
    # additive "(filesystem)" sibling node next to the VM disk image —
    # which needs real pytsk3 reads this fixture was never recorded
    # against (predating that feature). This file is about tree's own
    # CLI wiring, not units/content/disk_fs.py — see
    # tests/unit/sdk/test_units_disk_fs.py for that feature's own tests.
    monkeypatch.setattr(device_module, "disk_fs_available", lambda: False)


def test_depth_limits_the_catalog_tree_replayed() -> None:
    result = runner.invoke(app, ["tree", "#", "--depth", "1", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if "elapsed" not in line]
    # Connections print unconditionally at the bare root (no synthetic "/"
    # wrapper line consuming a depth unit -- see tree.py's own
    # _root_connection_entries), so --depth 1 here means "connections plus
    # one further level, their workloads" -- apv-sample-1's real 2
    # connections plus their real 25 workloads combined, nothing deeper
    # still (no version lines, which need --depth 2). A regression that
    # ignored --depth and recursed into versions too would print more
    # than this. Checking a *count* rather than the absence of one
    # specific placeholder string avoids a trap: a stale "not in" check
    # against text that later drifts would silently stop catching
    # anything, since the (now-wrong) string was never going to appear
    # either way.
    assert len(lines) == 27


_SUB_TYPE_SUFFIXES = tuple(f"· {sub_type}" for sub_type in ("MAIL", "CALENDAR", "CONTACT", "DRIVE"))


def test_disambiguates_colliding_workload_names_replayed() -> None:
    result = runner.invoke(app, ["tree", "#", "--depth", "2", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    # Matched by the stable "· <SUB_TYPE>" disambiguation suffix
    # disambiguate() itself produces, not by any colliding real persona's
    # own (anonymized, drift-prone) display name -- the disambiguation
    # *logic* is unit-tested with fake colliding names by
    # tests/unit/cli/test_cli_tree.py's own
    # test_workload_entries_disambiguates_colliding_display_names_by_sub_type_hint;
    # this only needs to prove apv-sample-1's real data actually triggers
    # that path end to end through the CLI. apv-sample-1 has more than one
    # colliding persona, so group by the shared prefix before "· <SUB_TYPE>"
    # rather than assuming a single fixed total count.
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
    result = runner.invoke(app, ["--json", "tree", f"{_VM_REF}/device:241", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    entry = json.loads(result.stdout)
    names = {c["name"] for c in entry["children"]}
    assert "0bab024f-b2c8-4fe4-90e0-877b9a9974fb.img" in names


def test_human_output_actually_prints_the_ref_replayed() -> None:
    result = runner.invoke(app, ["tree", f"{_VM_REF}/device:241", "--ref", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert "cat:1/wl:2/ver:06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7" in result.output


__all__: list[str] = []
