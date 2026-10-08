"""``Pilot`` tests for the browser's repository, connection and workload
labels.

Fixtures and the root each is recorded against:

- ``tui_labels_vault_plain_pilot.json.gz`` — ``vault-plain/@ActiveProtectVault``.
- ``tui_labels_vault_encrypted_pilot.json.gz`` —
  ``vault-encrypted/@ActiveProtectVault``.
- ``tui_samples_dir_scan.json.gz`` — ``all-local``, the directory holding
  every local sample; its test reads discovery and repository labels only.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

from textual.widgets import Input, Tree
from textual.widgets.tree import TreeNode

from integration.browser.pilot_drivers import ReplayLocalStore, open_browser_pilot
from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, UI_TIMEOUT, focus_widget, wait_until
from support.recording.sample_constants import VAULT_ENCRYPTED_KEY_STRING
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.sdk.api import Catalog


def test_root_is_labeled_catalogs_and_repo_label_has_no_internal_marker_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[str, str]:
        await replay_local_store("tui_labels_vault_plain_pilot.json.gz", label="vault-plain")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return str(tree.root.label), str(tree.root.children[0].label)

    root_label, repo_label = asyncio.run(scenario())
    assert root_label == "Catalogs"
    assert "@ActiveProtectVault" not in repo_label
    assert "@ActiveProtectData" not in repo_label
    assert "vault-plain" in repo_label


def test_scanning_the_parent_of_several_repos_still_hides_the_internal_marker_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> list[str]:
        await replay_local_store("tui_samples_dir_scan.json.gz", label="samples")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return [str(node.label) for node in tree.root.children]

    labels = asyncio.run(scenario())
    assert len(labels) >= 2, "test invariant: all-local must hold more than one repository for this bug to show"
    for label in labels:
        assert "@ActiveProtectVault" not in label, label
        assert "@ActiveProtectData" not in label, label


def test_key_needed_hint_shown_before_any_key_is_tried_for_a_real_encrypted_repo_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> str:
        await replay_local_store("tui_labels_vault_encrypted_pilot.json.gz", label="vault-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            tree = app.screen.query_one("#col-catalogs", Tree)
            return str(tree.root.children[0].label)

    label = asyncio.run(scenario())
    assert "· key needed" in label, label


def test_selecting_a_connection_on_an_encrypted_repo_blocks_workloads_and_auto_prompts_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> bool:
        await replay_local_store("tui_labels_vault_encrypted_pilot.json.gz", label="vault-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, KeyDialog) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            return isinstance(app.screen, KeyDialog)

    assert asyncio.run(scenario()), "selecting an encrypted connection never auto-popped KeyDialog"


def test_label_updates_to_key_verified_after_a_real_key_is_provided_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    key_string = VAULT_ENCRYPTED_KEY_STRING

    async def scenario() -> tuple[str, str, int]:
        await replay_local_store("tui_labels_vault_encrypted_pilot.json.gz", label="vault-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            browse_screen = app.screen
            assert isinstance(browse_screen, BrowseScreen), browse_screen
            tree = browse_screen.query_one("#col-catalogs", Tree)
            before = str(tree.root.children[0].label)

            cat_tree = browse_screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, KeyDialog) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            assert isinstance(app.screen, KeyDialog), app.screen

            wl_tree_before = browse_screen.query_one("#col-workloads", Tree)
            blocked_child_count = len(wl_tree_before.root.children)

            key_input = app.screen.query_one("#key-input", Input)
            key_input.value = key_string
            await focus_widget(pilot, key_input)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.03,
            )
            assert isinstance(app.screen, BrowseScreen), app.screen
            assert app.screen is browse_screen

            wl_tree_after = browse_screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree_after.root.children, timeout=SDK_TIMEOUT, interval=0.03)

            after = str(tree.root.children[0].label)
            return before, after, blocked_child_count

    before, after, blocked_child_count = asyncio.run(scenario())
    assert before.endswith("· key needed"), before
    assert after.endswith("· key verified"), after
    assert blocked_child_count == 0, "workload list was populated before the key was ever verified"


def test_cancelling_the_key_dialog_leaves_the_workload_list_blocked_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[bool, int]:
        await replay_local_store("tui_labels_vault_encrypted_pilot.json.gz", label="vault-encrypted")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)
            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, KeyDialog) and app.screen.is_mounted,
                timeout=SDK_TIMEOUT,
                interval=0.02,
            )
            assert isinstance(app.screen, KeyDialog), app.screen

            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
            )
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


#: A GWS domain label, by shape: the domain is anonymized (unlike an M365
#: tenant_id), so the shape holds both replaying and recording fresh. At
#: least one letter is required so a dotted-quad IP label elsewhere in the
#: tree never matches.
_DOMAIN_RE = re.compile(
    r"^(?=[a-z0-9.-]*[a-z])[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)


def test_workload_type_groups_use_proper_gws_and_m365_vendor_names_replayed(
    replay_local_store: ReplayLocalStore,
    tmp_path: Path,
) -> None:
    async def scenario() -> tuple[set[str], set[str]]:
        await replay_local_store("tui_labels_vault_plain_pilot.json.gz", label="vault-plain")
        app = ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await open_browser_pilot(app, pilot, tmp_path)

            cat_tree = app.screen.query_one("#col-catalogs", Tree)
            repo_node = cat_tree.root.children[0]
            connection_node = next(
                n
                for n in repo_node.children
                if n.data is not None
                and isinstance(n.data.payload, Catalog)
                and n.data.payload.connection.connection_config_id == 1
            )
            _ = cat_tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
            cat_tree.move_cursor(connection_node)
            await focus_widget(pilot, cat_tree)
            await pilot.press("enter")
            wl_tree = app.screen.query_one("#col-workloads", Tree)
            await wait_until(pilot, lambda: wl_tree.root.children, timeout=SDK_TIMEOUT, interval=0.03)
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
