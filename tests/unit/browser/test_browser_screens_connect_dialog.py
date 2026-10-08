"""``Pilot`` tests for ``ConnectDialog``'s core mechanics -- the dialog
``BrowseScreen`` opens on launch (and again on ``c``): mounting/tabs, the
local backend's directory tree, and the scan-and-submit flow every backend
shares. S3/Azure/SMB field validation and bucket/container browsing live in
``test_browser_screens_connect_dialog_remote_backends.py``; saved profiles in
``test_browser_screens_profile_manager.py``.

The repository scan runs inside the dialog (``_scan`` ->
``scan_repositories`` -> ``Session.discover``); every test that needs a scan
to succeed fakes ``Session.discover``. Constructing an
``S3Store``/``AzureStore``/``SmbStore`` does no I/O, so field handling needs
no fake.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from textual.widgets import Button, Checkbox, DirectoryTree, Input, OptionList, Select, Static, Tabs, Tree
from textual.worker import Worker

import synology_apm_repo.sdk.api as api
from support.fakes import faithful_to
from support.pilot import (
    RUN_TEST_SIZE,
    SDK_TIMEOUT,
    UI_TIMEOUT,
    focus_widget,
    move_cursor_to,
    settle,
    wait_for_screen,
    wait_until,
)
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog, ConnectResult
from synology_apm_repo.browser.strings import CONNECT_CANCELLING_STATUS
from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree
from synology_apm_repo.sdk.api import Catalog, Repository
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.profiles import BackendKind, form_fields_for
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore
from unit.browser.browse_screen_fakes import fake_repository
from unit.browser.connect_dialog_drivers import (
    activate_backend_and_settle,
    open_connect_dialog,
    wait_for_status_containing,
)


def _fake_discover(
    monkeypatch: pytest.MonkeyPatch,
    *,
    found: Sequence[Repository] = (),
    error: Exception | None = None,
    gate: asyncio.Event | None = None,
) -> list[object]:
    """Replace ``Session.discover``: wait for ``gate`` if given, then raise
    ``error`` if given, else yield ``found``. Returns the stores it is called
    with."""
    stores: list[object] = []

    async def discover(
        self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
    ) -> AsyncIterator[Repository]:
        stores.append(store)
        if gate is not None:
            await gate.wait()
        if error is not None:
            raise error
        for repo in found:
            yield repo

    monkeypatch.setattr(api.Session, "discover", discover)
    return stores


def _form_state(app: ApmRepoBrowserApp, dialog: ConnectDialog) -> tuple[object, ...]:
    """Every input's value, every button's enabled state, every option list's
    classes, and the focused widget: what a dialog event handler can change."""
    return (
        {i.id: i.value for i in dialog.query(Input)},
        {b.id: b.disabled for b in dialog.query(Button)},
        {o.id: sorted(o.classes) for o in dialog.query(OptionList)},
        app.focused.id if app.focused is not None else None,
    )


def _set_s3_fields(dialog: ConnectDialog) -> None:
    dialog.query_one("#connect-s3-bucket", Input).value = "test-1"


def _set_azure_fields(dialog: ConnectDialog) -> None:
    dialog.query_one("#connect-azure-container", Input).value = "test-container"
    dialog.query_one("#connect-azure-account-url", Input).value = "https://example.blob.core.windows.net"


def _set_smb_fields(dialog: ConnectDialog) -> None:
    dialog.query_one("#connect-smb-server", Input).value = "nas.example.com"
    dialog.query_one("#connect-smb-share", Input).value = "test-share"


class TestDialogMechanics:
    def test_connect_dialog_opens_automatically_on_launch(self) -> None:

        async def scenario() -> None:
            app = ApmRepoBrowserApp()
            async with app.run_test(size=RUN_TEST_SIZE) as pilot:
                await wait_for_screen(pilot, ConnectDialog)

        asyncio.run(scenario())

    def test_connect_dialog_defaults_to_local_fields_visible(self) -> None:
        """Local is the default backend, and its submit button reads "Open"
        ("Connect" for the remote backends)."""

        async def scenario() -> tuple[bool, bool, bool, bool, str]:
            async with open_connect_dialog() as (_app, _pilot, dialog):
                local_active = dialog.query_one("#connect-local-fields").has_class("active")
                s3_active = dialog.query_one("#connect-s3-fields").has_class("active")
                azure_active = dialog.query_one("#connect-azure-fields").has_class("active")
                smb_active = dialog.query_one("#connect-smb-fields").has_class("active")
                submit_label = str(dialog.query_one("#connect-submit", Button).label)
                return local_active, s3_active, azure_active, smb_active, submit_label

        local_active, s3_active, azure_active, smb_active, submit_label = asyncio.run(scenario())
        assert local_active is True
        assert s3_active is False
        assert azure_active is False
        assert smb_active is False
        assert submit_label == "Open"

    def test_connect_dialog_switching_backends_toggles_fields_and_submit_label(
        self,
    ) -> None:
        async def scenario() -> list[tuple[bool, bool, bool, bool, str]]:
            snapshots: list[tuple[bool, bool, bool, bool, str]] = []
            async with open_connect_dialog() as (_app, pilot, dialog):

                def snapshot() -> tuple[bool, bool, bool, bool, str]:
                    return (
                        dialog.query_one("#connect-local-fields").has_class("active"),
                        dialog.query_one("#connect-s3-fields").has_class("active"),
                        dialog.query_one("#connect-azure-fields").has_class("active"),
                        dialog.query_one("#connect-smb-fields").has_class("active"),
                        str(dialog.query_one("#connect-submit", Button).label),
                    )

                await activate_backend_and_settle(dialog, "s3", pilot)
                snapshots.append(snapshot())

                await activate_backend_and_settle(dialog, "azure", pilot)
                snapshots.append(snapshot())

                await activate_backend_and_settle(dialog, "smb", pilot)
                snapshots.append(snapshot())

                await activate_backend_and_settle(dialog, "local", pilot)
                snapshots.append(snapshot())
            return snapshots

        after_s3, after_azure, after_smb, after_local = asyncio.run(scenario())
        assert after_s3 == (False, True, False, False, "Connect")
        assert after_azure == (False, False, True, False, "Connect")
        assert after_smb == (False, False, False, True, "Connect")
        assert after_local == (True, False, False, False, "Open")

    def test_connect_dialog_arrow_keys_switch_backend_without_tab_key(
        self,
    ) -> None:
        """With focus on ``#connect-backend-tabs``, left/right alone switch
        the active backend."""

        async def scenario() -> list[str]:
            active_ids: list[str] = []
            async with open_connect_dialog() as (_app, pilot, dialog):
                tabs = dialog.query_one("#connect-backend-tabs", Tabs)
                await focus_widget(pilot, tabs)

                await pilot.press("right")
                await wait_until(pilot, lambda: tabs.active == "smb", timeout=UI_TIMEOUT, interval=0.05)
                active_ids.append(tabs.active)

                await pilot.press("right")
                await wait_until(pilot, lambda: tabs.active == "s3", timeout=UI_TIMEOUT, interval=0.05)
                active_ids.append(tabs.active)

                await pilot.press("left")
                await wait_until(pilot, lambda: tabs.active == "smb", timeout=UI_TIMEOUT, interval=0.05)
                active_ids.append(tabs.active)
            return active_ids

        # Tab order is Local, SMB, S3, Azure.
        active_ids = asyncio.run(scenario())
        assert active_ids == ["smb", "s3", "smb"], active_ids

    def test_connect_dialog_tab_key_moves_from_backend_tabs_into_active_fields(
        self,
    ) -> None:
        """Tab moves from the backend picker into the active fields group in
        one hop, rather than cycling within the picker."""

        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await focus_widget(pilot, dialog.query_one("#connect-backend-tabs", Tabs))

                await pilot.press("tab")
                tree = dialog.query_one("#connect-local-tree", DirectoryTree)
                await wait_until(pilot, lambda: dialog.focused is tree, timeout=UI_TIMEOUT, interval=0.05)
                return dialog.focused is tree

        landed_on_tree = asyncio.run(scenario())
        assert landed_on_tree

    def test_browse_screen_c_binding_pushes_connect_dialog(
        self,
    ) -> None:

        async def scenario() -> bool:
            async with open_connect_dialog() as (app, pilot, _dialog):
                await pilot.press("escape")
                await wait_until(
                    pilot,
                    lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                    timeout=UI_TIMEOUT,
                    interval=0.05,
                )
                assert isinstance(app.screen, BrowseScreen), app.screen
                app.screen.query_one("#col-catalogs", Tree).focus()
                await pilot.press("c")
                await wait_until(
                    pilot,
                    lambda: isinstance(app.screen, ConnectDialog) and app.screen.is_mounted,
                    timeout=UI_TIMEOUT,
                    interval=0.05,
                )
                return isinstance(app.screen, ConnectDialog)

        pushed = asyncio.run(scenario())
        assert pushed

    def test_every_profile_field_has_its_widget(self) -> None:
        """``SavedProfileManager`` reads and refills each tab through
        ``form_fields_for`` by widget id (``#connect-<backend>-<field>``),
        so a field the SDK adds needs a widget of the matching type here."""

        async def scenario() -> list[str]:
            missing: list[str] = []
            async with open_connect_dialog() as (_app, _pilot, dialog):
                for backend in BackendKind:
                    for field in form_fields_for(backend):
                        widget_type = Checkbox if field.is_checkbox else Input
                        selector = f"{widget_type.__name__}#connect-{backend}-{field.name.replace('_', '-')}"
                        if not dialog.query(selector):
                            missing.append(selector)
            return missing

        assert asyncio.run(scenario()) == []

    def test_on_select_changed_ignores_a_foreign_select(self) -> None:
        async def scenario() -> tuple[object, object]:
            async with open_connect_dialog() as (app, pilot, dialog):
                before = _form_state(app, dialog)
                foreign: Select[str] = Select([("a", "a")], id="not-a-profile-select")
                dialog.on_select_changed(Select.Changed(foreign, "a"))  # must not raise
                await settle(pilot)
                return before, _form_state(app, dialog)

        before, after = asyncio.run(scenario())
        assert after == before

    def test_on_option_list_option_selected_ignores_a_foreign_option_list(self) -> None:
        from textual.widgets.option_list import Option

        async def scenario() -> tuple[object, object]:
            async with open_connect_dialog() as (app, pilot, dialog):
                before = _form_state(app, dialog)
                foreign = OptionList(id="not-a-connect-option-list")
                # Must not raise.
                dialog.on_option_list_option_selected(OptionList.OptionSelected(foreign, Option("x"), 0))
                await settle(pilot)
                return before, _form_state(app, dialog)

        before, after = asyncio.run(scenario())
        assert after == before


class TestLocalBackend:
    def test_connect_dialog_local_path_validation_errors(
        self,
        tmp_path: Path,
    ) -> None:
        """Both are construction-time checks (``_build_local_store()``),
        shown inline before any scan starts."""

        async def scenario() -> tuple[str, str]:
            async with open_connect_dialog() as (_app, pilot, dialog):
                dialog.query_one("#connect-local-path", Input).value = ""
                dialog.query_one("#connect-submit", Button).press()
                empty_status = await wait_for_status_containing(pilot, dialog, "directory path")

                not_a_dir = tmp_path / "not-a-directory.txt"
                not_a_dir.write_text("x")
                dialog.query_one("#connect-local-path", Input).value = str(not_a_dir)
                dialog.query_one("#connect-submit", Button).press()
                not_dir_status = await wait_for_status_containing(pilot, dialog, "not a directory")
                return empty_status, not_dir_status

        empty_status, not_dir_status = asyncio.run(scenario())
        assert "directory path" in empty_status.lower(), empty_status
        assert "not a directory" in not_dir_status.lower(), not_dir_status

    def test_local_directory_tree_hides_files(
        self,
        tmp_path: Path,
    ) -> None:
        """``DirsOnlyTree`` shows only subdirectories; Textual's
        ``DirectoryTree`` shows files too."""
        (tmp_path / "a-file.txt").write_text("x")
        (tmp_path / "a-subdir").mkdir()

        async def scenario() -> list[str]:
            async with open_connect_dialog() as (app, pilot, _dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=UI_TIMEOUT, interval=0.1)
                return [str(child.label) for child in tree.root.children]

        labels = asyncio.run(scenario())
        assert any("a-subdir" in label for label in labels), labels
        assert not any("a-file" in label for label in labels), labels

    def test_local_directory_tree_dotdot_entry_goes_up_a_level(
        self,
        tmp_path: Path,
    ) -> None:
        """Selecting the ``".."`` entry re-roots the tree at the parent and
        syncs the path Input."""
        child = tmp_path / "child"
        child.mkdir()

        async def scenario() -> tuple[str, str]:
            async with open_connect_dialog() as (app, pilot, _dialog):
                assert len(app.screen.query("#connect-local-up")) == 0, "no separate Up button"
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(child)
                await wait_until(pilot, lambda: tree.root.children, timeout=UI_TIMEOUT, interval=0.1)
                assert str(tree.root.children[0].label) == "..", [str(c.label) for c in tree.root.children]

                tree.focus()
                await move_cursor_to(pilot, tree, tree.root.children[0])
                await pilot.press("enter")
                await wait_until(pilot, lambda: str(tree.path) == str(tmp_path), timeout=UI_TIMEOUT, interval=0.1)
                return str(tree.path), app.screen.query_one("#connect-local-path", Input).value

        tree_path, input_value = asyncio.run(scenario())
        assert tree_path == str(tmp_path)
        assert input_value == str(tmp_path)

    def test_local_directory_tree_backspace_jumps_to_parent_node_and_collapses_it(
        self,
        tmp_path: Path,
    ) -> None:
        """Unlike ``".."``, Backspace (``move_cursor_to_parent``) moves within
        the current root and never re-roots the tree."""
        (tmp_path / "parent" / "child").mkdir(parents=True)

        async def scenario() -> tuple[bool, bool, str, str]:
            async with open_connect_dialog() as (app, pilot, _dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=UI_TIMEOUT, interval=0.1)
                parent_node = next(c for c in tree.root.children if str(c.label) == "parent")
                await move_cursor_to(pilot, tree, parent_node)
                await pilot.press("enter")
                await wait_until(pilot, lambda: parent_node.children, timeout=UI_TIMEOUT, interval=0.1)
                child_node = parent_node.children[0]
                await move_cursor_to(pilot, tree, child_node)
                path_before = str(tree.path)

                await pilot.press("backspace")
                await wait_until(pilot, lambda: tree.cursor_node is parent_node, timeout=UI_TIMEOUT, interval=0.1)
                landed_on_parent = tree.cursor_node is parent_node
                return landed_on_parent, not parent_node.is_expanded, str(tree.path), path_before

        landed_on_parent, collapsed, path_after, path_before = asyncio.run(scenario())
        assert landed_on_parent
        assert collapsed
        assert path_after == path_before, 'Backspace must never re-root the tree — only ".." does that'

    def test_local_directory_tree_typeahead_jumps_to_matching_sibling(
        self,
        tmp_path: Path,
    ) -> None:
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()
        (tmp_path / "gamma").mkdir()

        async def scenario() -> tuple[str | None, str | None]:
            async with open_connect_dialog() as (app, pilot, _dialog):
                tree = app.screen.query_one("#connect-local-tree", DirsOnlyTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=UI_TIMEOUT, interval=0.1)
                await focus_widget(pilot, tree)

                await pilot.press("b")
                await wait_until(
                    pilot,
                    lambda: tree.cursor_node is not None and str(tree.cursor_node.label) == "beta",
                    timeout=UI_TIMEOUT,
                    interval=0.1,
                )
                matched = str(tree.cursor_node.label) if tree.cursor_node is not None else None

                before = tree.cursor_node
                # No sibling starts with "z": the handler empties the buffer
                # once it has found no match, so the keystroke is done then.
                await pilot.press("z")
                await wait_until(pilot, lambda: tree._typeahead_buffer == "", timeout=UI_TIMEOUT)
                unmatched_stayed = str(before.label) if tree.cursor_node is before and before is not None else None
                return matched, unmatched_stayed

        matched, unmatched_stayed = asyncio.run(scenario())
        assert matched == "beta", matched
        assert unmatched_stayed == "beta", "an unmatched keystroke must not move the cursor at all"

    def test_local_directory_tree_typeahead_resets_to_a_single_char_match(
        self,
        tmp_path: Path,
    ) -> None:
        """ "b" then "g": "bg" matches no sibling, so the buffer resets to
        "g" and the cursor jumps to "gamma"."""
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()
        (tmp_path / "gamma").mkdir()

        async def scenario() -> str | None:
            async with open_connect_dialog() as (app, pilot, _dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=UI_TIMEOUT, interval=0.1)
                await focus_widget(pilot, tree)

                await pilot.press("b")
                await wait_until(
                    pilot,
                    lambda: tree.cursor_node is not None and str(tree.cursor_node.label) == "beta",
                    timeout=UI_TIMEOUT,
                    interval=0.1,
                )
                await pilot.press("g")
                await wait_until(
                    pilot,
                    lambda: tree.cursor_node is not None and str(tree.cursor_node.label) == "gamma",
                    timeout=UI_TIMEOUT,
                    interval=0.1,
                )
                return str(tree.cursor_node.label) if tree.cursor_node is not None else None

        assert asyncio.run(scenario()) == "gamma"

    def test_build_local_store_with_a_real_directory_returns_a_real_local_fs_store(self, tmp_path: Path) -> None:
        from synology_apm_repo.sdk.storage import LocalFsStore

        async def scenario() -> tuple[object, str]:
            async with open_connect_dialog() as (_app, _pilot, dialog):
                dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
                return dialog._build_local_store()

        store, label = asyncio.run(scenario())
        assert isinstance(store, LocalFsStore)
        assert label == str(tmp_path)

    def test_pressing_enter_in_the_local_path_field_submits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spies on ``_submit``: a scan of an empty ``tmp_path`` can finish
        before polling ever observes ``scanning``."""

        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                monkeypatch.setattr(dialog, "_submit", fake_submit)
                path_input = dialog.query_one("#connect-local-path", Input)
                path_input.value = str(tmp_path)
                await focus_widget(pilot, path_input)
                await pilot.press("enter")
                await wait_until(pilot, lambda: submitted)
                return submitted

        assert asyncio.run(scenario())


class TestScanAndSubmit:
    @pytest.mark.parametrize(
        ("backend", "set_fields", "expected_store_type", "expected_label", "repo_root"),
        [
            pytest.param("s3", _set_s3_fields, S3Store, "s3://test-1", "@ActiveProtectData/repo-1", id="s3"),
            pytest.param(
                "azure",
                _set_azure_fields,
                AzureStore,
                "azure://test-container",
                "@ActiveProtectData/repo-2",
                id="azure",
            ),
            pytest.param(
                "smb",
                _set_smb_fields,
                SmbStore,
                "smb://nas.example.com/test-share",
                "@ActiveProtectData/repo-3",
                id="smb",
            ),
        ],
    )
    def test_connect_dialog_valid_fields_scan_and_dismiss(
        self,
        monkeypatch: pytest.MonkeyPatch,
        backend: str,
        set_fields: Callable[[ConnectDialog], None],
        expected_store_type: type,
        expected_label: str,
        repo_root: str,
    ) -> None:
        """Valid fields build the backend's real store, and a (faked) scan
        finding one repository dismisses with a ``ConnectResult``."""
        fake_repo = fake_repository(repo_root)

        async def fake_catalogs() -> list[Catalog]:
            return []

        monkeypatch.setattr(fake_repo, "catalogs", fake_catalogs)

        stores = _fake_discover(monkeypatch, found=[fake_repo])

        async def scenario() -> ConnectResult:
            result: ConnectResult | None = None

            async def on_dismiss(r: ConnectResult | None) -> None:
                nonlocal result
                result = r

            async with open_connect_dialog() as (app, pilot, _first_dialog):
                # A second dialog, so this scenario owns its dismiss callback.
                app.push_screen(ConnectDialog(), on_dismiss)
                await wait_until(
                    pilot,
                    lambda: (
                        isinstance(app.screen, ConnectDialog)
                        and app.screen.is_mounted
                        and app.screen is not _first_dialog
                    ),
                    timeout=UI_TIMEOUT,
                    interval=0.05,
                    message="second ConnectDialog never became active",
                )
                dialog = app.screen
                assert isinstance(dialog, ConnectDialog), dialog
                await activate_backend_and_settle(dialog, backend, pilot)
                set_fields(dialog)
                dialog.query_one("#connect-submit", Button).press()
                await wait_until(pilot, lambda: result is not None, timeout=SDK_TIMEOUT, interval=0.05)
                assert result is not None, "ConnectDialog never dismissed"
            return result

        repos, label = asyncio.run(scenario())
        (store,) = stores
        assert isinstance(store, expected_store_type)
        assert label == expected_label
        assert len(repos) == 1
        assert repos[0] is fake_repo

    def test_connecting_via_s3_backend_populates_browse_screen(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A successful scan leaves ``BrowseScreen`` on screen with the
        repository in ``#col-catalogs`` and ``#open-status`` reporting it."""
        fake_repo = fake_repository("@ActiveProtectData/repo-1")

        async def fake_catalogs() -> list[Catalog]:
            return []

        monkeypatch.setattr(fake_repo, "catalogs", fake_catalogs)

        _fake_discover(monkeypatch, found=[fake_repo])

        async def scenario() -> tuple[int, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-bucket"
                dialog.query_one("#connect-submit", Button).press()
                # The repo reaches #col-catalogs through its own dispatch,
                # which can land after the screen swap.
                await wait_until(
                    pilot,
                    lambda: (
                        isinstance(app.screen, BrowseScreen)
                        and app.screen.query_one("#col-catalogs", Tree).root.children
                    ),
                    timeout=SDK_TIMEOUT,
                    interval=0.1,
                )
                assert isinstance(app.screen, BrowseScreen), app.screen
                status = str(app.screen.query_one("#open-status", Static).render())
                tree = app.screen.query_one("#col-catalogs", Tree)
                return len(tree.root.children), status

        repo_count, status = asyncio.run(scenario())
        assert repo_count == 1, status
        assert "found" in status.lower(), status

    def test_connect_dialog_scan_apm_repo_error_shows_inline_and_stays_open(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _fake_discover(monkeypatch, error=ApmRepoError("repo is locked"))

        async def scenario() -> tuple[bool, str, bool]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                submit = dialog.query_one("#connect-submit", Button)
                submit.press()
                status = await wait_for_status_containing(pilot, dialog, "error")
                return isinstance(app.screen, ConnectDialog), status, submit.disabled

        still_open, status, submit_disabled = asyncio.run(scenario())
        assert still_open
        assert "repo is locked" in status
        assert submit_disabled is False  # the submit/cancel button is never disabled, even mid-scan

    def test_connect_dialog_scan_unexpected_exception_shows_inline_and_stays_open(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _FakeConnectionRefused(Exception):
            pass

        _fake_discover(monkeypatch, error=_FakeConnectionRefused("connection refused"))

        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open
        assert "connection refused" in status

    def test_connect_dialog_scan_finding_nothing_shows_inline_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _fake_discover(monkeypatch)

        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open
        assert "no repository found" in status

    def test_submit_is_a_no_op_while_already_scanning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                # `scanning` is `self._scan_worker is not None`.
                dialog._scan_worker = cast(Any, object())

                called = False

                def fake_build_store() -> tuple[object, str]:
                    nonlocal called
                    called = True
                    raise AssertionError("_build_store must not run while a scan is already underway")

                monkeypatch.setattr(dialog, "_build_store", fake_build_store)
                dialog._submit()
                await settle(pilot)
                return called

        assert asyncio.run(scenario()) is False


class TestScanningDisablesFieldsAndRelabelsSubmit:
    """Each fakes ``Session.discover`` to block on an ``asyncio.Event``, so
    the dialog's state is observed while a scan is in flight."""

    def test_scan_disables_fields_and_relabels_submit_to_cancel(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        block = asyncio.Event()
        fake_repo = fake_repository("@ActiveProtectData/repo-1")

        _fake_discover(monkeypatch, found=[fake_repo], gate=block)

        async def scenario() -> tuple[bool, bool, str, bool]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                submit = dialog.query_one("#connect-submit", Button)
                submit.press()
                await wait_until(pilot, lambda: dialog.scanning, timeout=SDK_TIMEOUT, interval=0.02)
                tabs_disabled = dialog.query_one("#connect-backend-tabs").disabled
                fields_disabled = dialog.query_one("#connect-s3-fields").disabled
                label = str(submit.label)
                submit_disabled = submit.disabled
                block.set()
                await wait_until(
                    pilot,
                    lambda: isinstance(app.screen, BrowseScreen) and app.screen.is_mounted,
                    timeout=SDK_TIMEOUT,
                    interval=0.02,
                )
                return tabs_disabled, fields_disabled, label, submit_disabled

        tabs_disabled, fields_disabled, label, submit_disabled = asyncio.run(scenario())
        assert tabs_disabled
        assert fields_disabled
        assert label == "Cancel"
        assert submit_disabled is False  # stays clickable -- it's what cancels the scan

    def test_cancel_button_click_mid_scan_stops_worker_and_restores_editable_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        never = asyncio.Event()  # never set -- the scan is cancelled, not let to finish

        _fake_discover(monkeypatch, gate=never)

        async def scenario() -> tuple[bool, bool, str, bool, str, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                submit = dialog.query_one("#connect-submit", Button)
                submit.press()
                await wait_until(pilot, lambda: dialog.scanning, timeout=SDK_TIMEOUT, interval=0.02)
                status = dialog.query_one("#connect-status", Static)
                scanning_status = str(status.render())
                submit.press()  # mid-scan, the submit button cancels
                await wait_until(pilot, lambda: not dialog.scanning, timeout=SDK_TIMEOUT, interval=0.02)
                await wait_until(pilot, lambda: not dialog.query_one("#connect-s3-fields").disabled)
                return (
                    isinstance(app.screen, ConnectDialog),
                    dialog.query_one("#connect-s3-fields").disabled,
                    str(submit.label),
                    submit.disabled,
                    scanning_status,
                    str(status.render()),
                )

        still_open, fields_disabled, label, submit_disabled, scanning_status, final_status = asyncio.run(scenario())
        assert still_open
        assert fields_disabled is False
        assert label == "Connect"
        assert submit_disabled is False
        assert "scanning" in scanning_status.lower()
        # The transient "cancelling..." text is too brief to observe here;
        # test_cancel_scan_writes_cancelling_status_immediately covers it.
        assert final_status == ""

    def test_cancel_scan_writes_cancelling_status_immediately(self) -> None:
        """``_cancel_scan`` writes ``CONNECT_CANCELLING_STATUS``
        synchronously, before the worker's cancellation lands."""

        @faithful_to(Worker)
        class _FakeWorker:
            def cancel(self) -> None:
                pass

        async def scenario() -> str:
            async with open_connect_dialog() as (_app, _pilot, dialog):
                dialog._scan_worker = cast(Any, _FakeWorker())
                dialog._cancel_scan()
                return str(dialog.query_one("#connect-status", Static).render())

        assert asyncio.run(scenario()) == CONNECT_CANCELLING_STATUS

    def test_escape_mid_scan_cancels_the_worker_and_dismisses_the_dialog(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Escape (``action_cancel``) dismisses before ``_scan``'s
        ``CancelledError`` cleanup runs, so only the dismiss is asserted."""
        never = asyncio.Event()

        _fake_discover(monkeypatch, gate=never)

        async def scenario() -> bool:
            dismissed = False
            result: ConnectResult | None = None

            async def on_dismiss(r: ConnectResult | None) -> None:
                nonlocal dismissed, result
                dismissed = True
                result = r

            async with open_connect_dialog() as (app, pilot, _first_dialog):
                app.push_screen(ConnectDialog(), on_dismiss)
                await wait_until(
                    pilot,
                    lambda: (
                        isinstance(app.screen, ConnectDialog)
                        and app.screen.is_mounted
                        and app.screen is not _first_dialog
                    ),
                    timeout=UI_TIMEOUT,
                    interval=0.05,
                    message="second ConnectDialog never became active",
                )
                dialog = app.screen
                assert isinstance(dialog, ConnectDialog), dialog
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                dialog.query_one("#connect-submit", Button).press()
                await wait_until(pilot, lambda: dialog.scanning, timeout=SDK_TIMEOUT, interval=0.02)
                await pilot.press("escape")
                await wait_until(pilot, lambda: dismissed, timeout=SDK_TIMEOUT, interval=0.02)
            return dismissed and result is None

        assert asyncio.run(scenario())
