"""``textual`` ``Pilot``-driven label-formatting coverage — end-to-end
against committed fixtures recorded from real sample data, with **no real
``samples_dir`` dependency**.

Every test but one shares this file's own dedicated
``tui_labels_apv1_pilot.json.gz``/``tui_labels_apv2_encrypted_pilot.json.gz``.
The one exception,
``test_scanning_the_parent_of_several_repos_still_hides_the_internal_marker``,
scans the whole real ``samples_dir`` itself (several sibling repositories, one
directory level deeper than every other fixture's own single-repository root),
so it gets its own small ``tui_samples_dir_scan.json.gz`` fixture —
discovery and connection labels only, no workload/version/content read
anywhere, so it stays tiny despite spanning every real sample repository.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import Input, Tree
from textual.widgets.tree import TreeNode

import synology_apm_repo.browser.screens.connect_dialog as connect_dialog_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen, CatalogEntry
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.sdk.storage.base import ObjectStore


async def _patch_local_store(
    monkeypatch: pytest.MonkeyPatch,
    record_target: Callable[[str], Awaitable[ObjectStore]],
    name: str,
    label: str,
) -> None:
    store = await record_target(name)

    def _fake(self: ConnectDialog) -> tuple[ObjectStore, str]:
        return store, label

    monkeypatch.setattr(connect_dialog_module.ConnectDialog, "_build_local_store", _fake)


def test_root_is_labeled_catalogs_and_repo_label_has_no_internal_marker_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[str, str]:
        await _patch_local_store(monkeypatch, record_target, "tui_labels_apv1_pilot.json.gz", "apv-sample-1")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return str(tree.root.label), str(tree.root.children[0].label)

    root_label, repo_label = asyncio.run(scenario())
    assert root_label == "Catalogs"
    assert "@ActiveProtectVault" not in repo_label
    assert "@ActiveProtectData" not in repo_label
    assert "apv-sample-1" in repo_label


def test_scanning_the_parent_of_several_repos_still_hides_the_internal_marker_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> list[str]:
        await _patch_local_store(monkeypatch, record_target, "tui_samples_dir_scan.json.gz", "samples")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return [str(node.label) for node in tree.root.children]

    labels = asyncio.run(scenario())
    assert len(labels) >= 2, "test invariant: samples_dir must hold more than one repository for this bug to show"
    for label in labels:
        assert "@ActiveProtectVault" not in label, label
        assert "@ActiveProtectData" not in label, label


def test_key_needed_hint_shown_before_any_key_is_tried_for_a_real_encrypted_repo_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> str:
        await _patch_local_store(
            monkeypatch, record_target, "tui_labels_apv2_encrypted_pilot.json.gz", "apv-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return str(tree.root.children[0].label)

    label = asyncio.run(scenario())
    assert "· key needed" in label, label


def test_selecting_a_connection_on_an_encrypted_repo_blocks_workloads_and_auto_prompts_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> bool:
        await _patch_local_store(
            monkeypatch, record_target, "tui_labels_apv2_encrypted_pilot.json.gz", "apv-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            cat_tree.focus()
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, KeyDialog), timeout=0.4, interval=0.02)
            return isinstance(app.screen, KeyDialog)

    assert asyncio.run(scenario()), "selecting an encrypted connection never auto-popped KeyDialog"


def test_label_updates_to_key_verified_after_a_real_key_is_provided_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    #: apv-sample-2-encrypted's real vault key — see
    #: ``tests/integration/sdk/test_units_disk_fs.py``'s own
    #: ``_ENCRYPTED_KEY_STRING`` for the same value/precedent (a replay
    #: test must never read the real ``samples_dir`` at test time).
    key_string = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="

    async def scenario() -> tuple[str, str, int]:
        await _patch_local_store(
            monkeypatch, record_target, "tui_labels_apv2_encrypted_pilot.json.gz", "apv-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            browse_screen = app.screen
            assert isinstance(browse_screen, BrowseScreen), browse_screen
            tree = browse_screen.query_one("#col-catalogs", Tree)
            before = str(tree.root.children[0].label)

            cat_tree = browse_screen.query_one("#col-catalogs", Tree)
            cat_tree.focus()
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, KeyDialog), timeout=0.4, interval=0.02)
            assert isinstance(app.screen, KeyDialog), app.screen

            wl_tree_before = browse_screen.query_one("#col-workloads", Tree)
            blocked_child_count = len(wl_tree_before.root.children)

            key_input = app.screen.query_one("#key-input", Input)
            key_input.value = key_string
            key_input.focus()
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=0.9, interval=0.03)
            assert isinstance(app.screen, BrowseScreen), app.screen
            assert app.screen is browse_screen

            wl_tree_after = browse_screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree_after.root.children, timeout=0.9, interval=0.03)

            after = str(tree.root.children[0].label)
            return before, after, blocked_child_count

    before, after, blocked_child_count = asyncio.run(scenario())
    assert before.endswith("· key needed"), before
    assert after.endswith("· key verified"), after
    assert blocked_child_count == 0, "workload list was populated before the key was ever verified"


def test_cancelling_the_key_dialog_leaves_the_workload_list_blocked_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[bool, int]:
        await _patch_local_store(
            monkeypatch, record_target, "tui_labels_apv2_encrypted_pilot.json.gz", "apv-sample-2-encrypted"
        )
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)
            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            cat_tree.focus()
            await pilot.press("enter")
            await wait_until(pilot, lambda: isinstance(app.screen, KeyDialog), timeout=0.4, interval=0.02)
            assert isinstance(app.screen, KeyDialog), app.screen

            await pilot.press("escape")
            await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=0.4, interval=0.02)
            assert isinstance(app.screen, BrowseScreen), app.screen

            wl_tree = app.screen.query_one("#col-workloads", Tree)
            return isinstance(app.screen, BrowseScreen), len(wl_tree.root.children)

    is_browse_screen, child_count = asyncio.run(scenario())
    assert is_browse_screen
    assert child_count == 0, "workload list was populated despite the key dialog being cancelled"


def _all_node_labels(node: TreeNode[object]) -> set[str]:
    labels: set[str] = set()
    for child in node.children:
        labels.add(str(child.label))
        labels |= _all_node_labels(child)
    return labels


#: A GW workload's own domain is catalog-metadata-anonymized (unlike an
#: M365 tenant_id) -- every real domain collapses to the same fixed
#: ``gws_domain`` placeholder, so asserting the exact literal would only
#: ever hold when replaying the anonymized fixture, never when recording
#: fresh against the real backend (see ``tests/CLAUDE.md``'s "never
#: hardcode an anonymized value" rule and ``test_catalog_workload.py``'s
#: equivalent fix). A domain-shape check holds either way -- requires at
#: least one letter so a dotted-quad IP label (a real ``config_pc``/
#: ``config_ps`` private IP elsewhere in this same tree, itself already
#: anonymized separately) never satisfies this by accident.
_DOMAIN_RE = re.compile(
    r"^(?=[a-z0-9.-]*[a-z])[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)


def test_workload_type_groups_use_proper_gws_and_m365_vendor_names_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    open_browser_pilot: Any,
    wait_until: Any,
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async def scenario() -> tuple[set[str], set[str]]:
        await _patch_local_store(monkeypatch, record_target, "tui_labels_apv1_pilot.json.gz", "apv-sample-1")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await open_browser_pilot(app, pilot, tmp_path)

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            repo_node = cat_tree.root.children[0]
            # connection_config_id 1 -- an internal catalog identifier,
            # stable and non-identifying (never touched by anonymization).
            jy_node = next(
                n
                for n in repo_node.children
                if isinstance(n.data, CatalogEntry) and n.data.catalog.connection.connection_config_id == 1
            )
            cat_tree.move_cursor(jy_node)
            cat_tree.focus()
            await pilot.press("enter")
            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=0.9, interval=0.03)
            root_labels = {str(g.label) for g in wl_tree.root.children}
            return root_labels, _all_node_labels(wl_tree.root)

    root_labels, all_labels = asyncio.run(scenario())
    assert {"FS", "VM", "Google Workspace", "Microsoft 365"} <= root_labels, root_labels
    assert not ({"Mail", "Chat", "SharePoint"} & root_labels), root_labels

    assert "87c467dd-ac00-45d8-babb-e2b0787e2d13" in all_labels, all_labels
    assert any(_DOMAIN_RE.match(label) for label in all_labels), all_labels

    assert {"Mail", "Shared Drives", "Contacts", "Calendars", "Drives"} <= all_labels, all_labels
    assert {"Groups", "Chat", "OneDrive", "Exchange", "Teams", "SharePoint"} <= all_labels, all_labels
    assert not (
        {
            "TEAM_DRIVE",
            "CONTACT",
            "CALENDAR",
            "DRIVE",
            "USER_CHAT",
            "USER_DRIVE",
            "USER_EXCHANGE",
            "GROUP_EXCHANGE",
            "SITE",
        }
        & all_labels
    ), all_labels


__all__: list[str] = []
