"""``textual`` ``Pilot``-driven coverage for ``ConnectDialog``'s core
mechanics — the screen auto-opened on top of ``BrowseScreen`` the
instant the app mounts (``c`` reopens it later the same way): dialog
mounting/tabs, the local backend's directory-tree fields, and the
scan-and-submit flow shared by every backend. See
``test_browser_pilot_remote_browser.py`` for S3/Azure/SMB field
validation and remote bucket/container browsing, and
``test_browser_profile_manager.py`` for profile save/load/delete —
both duplicate this file's own ``_open_connect_dialog``/
``_activate_backend`` helpers verbatim rather than importing them (see
``tests/CLAUDE.md``'s "no test module ever imports from another").
Mirrors ``test_browser_pilot_yg.py``'s own conventions (synchronous
``def test_...()`` wrapping ``asyncio.run(...)``).

The real connection test/repository scan runs *inside* this dialog (see its
own module docstring for why) — every test below that needs a scan to
actually *succeed* (rather than just checking field-toggling/
validation, which stays fully offline: constructing an
``S3Store``/``AzureStore``/``SmbStore`` does no real
I/O) fakes ``Session.discover_remote`` to yield a fake ``Repository``
instead of doing real network I/O — the same boundary
``tests/unit/browser/test_browser_browse_screen_labels.py`` already fakes.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, DirectoryTree, Input, OptionList, Select, Static, Tabs, Tree

import synology_apm_repo.sdk.api as api
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog, ConnectResult
from synology_apm_repo.sdk.api import Catalog, Repository
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore


@asynccontextmanager
async def _open_connect_dialog() -> AsyncIterator[tuple[ApmRepoBrowserApp, Pilot[None], ConnectDialog]]:
    """Mounts a fresh app, waits for the auto-opened ``ConnectDialog``
    to appear, and yields ``(app, pilot, dialog)`` — the boilerplate every
    ``scenario()`` closure below starts with, replacing a hand-rolled
    ``app = ApmRepoBrowserApp()`` + ``async with app.run_test(...) as
    pilot:`` + pause + isinstance-assert block. Still an ``async with
    app.run_test(...)`` underneath — nothing changes about ``app``'s own
    lifecycle, this just factors out the mount-and-wait every caller did
    identically."""
    app = ApmRepoBrowserApp()
    async with app.run_test(size=(140, 45)) as pilot:
        await pilot.pause()
        assert isinstance(app.screen, ConnectDialog), app.screen
        yield app, pilot, app.screen


def _activate_backend(dialog: ConnectDialog, backend: str) -> None:
    """Drives the backend ``Tabs`` strip the same way
    a real activation does — setting ``active`` posts the same
    ``Tabs.TabActivated`` message ``ConnectDialog.on_tabs_tab_activated``
    reacts to, so this is equivalent to a user pressing/arrowing onto the
    given tab, not a backdoor into ``_switch_backend``."""
    dialog.query_one("#connect-backend-tabs", Tabs).active = backend


async def _activate_backend_and_settle(
    dialog: ConnectDialog, backend: str, pilot: Pilot[None], wait_until: Any
) -> None:
    """``_activate_backend`` plus the wait its callers all need.

    Setting ``Tabs.active`` only posts ``TabActivated``; the pane swap happens
    when ``ConnectDialog`` handles it, which is a later event-loop turn. The
    pane carrying the ``active`` class is that swap having happened — waiting
    on it beats guessing a duration before touching this backend's widgets.
    """
    _activate_backend(dialog, backend)
    await wait_until(
        pilot,
        lambda: dialog.query_one(f"#connect-{backend}-fields").has_class("active"),
        timeout=0.6,
        interval=0.02,
        message=f"{backend} pane never became active",
    )


async def _wait_for_status_containing(dialog: ConnectDialog, pilot: Pilot[None], wait_until: Any, needle: str) -> str:
    """Polls ``#connect-status`` until its rendered text contains
    ``needle`` (case-insensitive), returning that final text — same
    contract as ``_wait_for_detail_text`` in
    ``test_browser_pilot_preview.py``, but for this dialog's own
    status line."""
    status = ""

    def _matches() -> bool:
        nonlocal status
        status = str(dialog.query_one("#connect-status", Static).render())
        return needle in status.lower()

    await wait_until(pilot, _matches, timeout=1.5, interval=0.05)
    return status


def _fake_repository(repo_root: str) -> Repository:
    """A real ``Repository``, backed by a placeholder store/layout —
    ``Repository.__init__`` does no I/O itself (``catalog_repo_layouts()``
    is a pure function of ``layout``), so nothing here ever touches a
    real ``DedupRepo``."""
    return Repository(
        cast(ObjectStore, object()),
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root),
        None,
        None,
        encrypted=False,
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
        """There is nothing to do on launch except pick a source, so
        ``ConnectDialog`` is what the user actually sees first — not an
        empty three-column ``BrowseScreen`` with nothing in it."""

        async def scenario() -> bool:
            app = ApmRepoBrowserApp()
            async with app.run_test(size=(140, 45)) as pilot:
                await pilot.pause()
                return isinstance(app.screen, ConnectDialog)

        assert asyncio.run(scenario())

    def test_connect_dialog_defaults_to_local_fields_visible(self) -> None:
        """ "Local" is the default backend (the common case, and the one
        that needs no credentials at all) — its directory tree/path fields
        start visible, S3/Azure/SMB's don't, and the submit button reads
        "Open" (vs. "Connect" for the other backends — see
        ``_switch_backend``'s own docstring)."""

        async def scenario() -> tuple[bool, bool, bool, bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
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

    def test_connect_dialog_switching_backends_toggles_fields_and_submit_label(self, wait_until: Any) -> None:
        async def scenario() -> list[tuple[bool, bool, bool, bool, str]]:
            snapshots: list[tuple[bool, bool, bool, bool, str]] = []
            async with _open_connect_dialog() as (app, pilot, dialog):

                def snapshot() -> tuple[bool, bool, bool, bool, str]:
                    return (
                        dialog.query_one("#connect-local-fields").has_class("active"),
                        dialog.query_one("#connect-s3-fields").has_class("active"),
                        dialog.query_one("#connect-azure-fields").has_class("active"),
                        dialog.query_one("#connect-smb-fields").has_class("active"),
                        str(dialog.query_one("#connect-submit", Button).label),
                    )

                await _activate_backend_and_settle(dialog, "s3", pilot, wait_until)
                snapshots.append(snapshot())

                await _activate_backend_and_settle(dialog, "azure", pilot, wait_until)
                snapshots.append(snapshot())

                await _activate_backend_and_settle(dialog, "smb", pilot, wait_until)
                snapshots.append(snapshot())

                await _activate_backend_and_settle(dialog, "local", pilot, wait_until)
                snapshots.append(snapshot())
            return snapshots

        after_s3, after_azure, after_smb, after_local = asyncio.run(scenario())
        assert after_s3 == (False, True, False, False, "Connect")
        assert after_azure == (False, False, True, False, "Connect")
        assert after_smb == (False, False, False, True, "Connect")
        assert after_local == (True, False, False, False, "Open")

    def test_connect_dialog_arrow_keys_switch_backend_without_tab_key(self) -> None:
        """The whole point of the backend picker being one ``Tabs`` strip
        instead of separate ``Button``s: with focus on
        ``#connect-backend-tabs``, left/right alone must switch the active
        backend — no ``tab`` keypress involved at all."""

        async def scenario() -> list[str]:
            active_ids: list[str] = []
            async with _open_connect_dialog() as (app, pilot, dialog):
                tabs = dialog.query_one("#connect-backend-tabs", Tabs)
                tabs.focus()
                await pilot.pause(0.05)

                await pilot.press("right")
                await pilot.pause(0.1)
                active_ids.append(tabs.active)

                await pilot.press("right")
                await pilot.pause(0.1)
                active_ids.append(tabs.active)

                await pilot.press("left")
                await pilot.pause(0.1)
                active_ids.append(tabs.active)
            return active_ids

        # Tab order is Local, SMB, S3, Azure — starting on Local (the
        # default), right/right/left lands on SMB, S3, SMB.
        active_ids = asyncio.run(scenario())
        assert active_ids == ["smb", "s3", "smb"], active_ids

    def test_connect_dialog_tab_key_moves_from_backend_tabs_into_active_fields(self) -> None:
        """Tab's job now is to move *between* the dialog's big components
        (backend picker -> active fields group), in one hop -- not to cycle
        within the backend picker itself, which the arrow-key test above
        already covers."""

        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                dialog.query_one("#connect-backend-tabs", Tabs).focus()
                await pilot.pause(0.05)

                await pilot.press("tab")
                await pilot.pause(0.1)
                tree = dialog.query_one("#connect-local-tree", DirectoryTree)
                return dialog.focused is tree

        landed_on_tree = asyncio.run(scenario())
        assert landed_on_tree

    def test_browse_screen_c_binding_pushes_connect_dialog(self) -> None:
        """``c`` reopens ``ConnectDialog`` from ``BrowseScreen`` — checked here
        by first Esc-ing out of the dialog auto-opened on launch (landing on
        an empty ``BrowseScreen``, per ``app.py``'s own ``on_mount``), then
        pressing ``c``."""

        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await pilot.press("escape")
                await pilot.pause(0.2)
                assert isinstance(app.screen, BrowseScreen), app.screen
                app.screen.query_one("#col-catalogs", Tree).focus()
                await pilot.press("c")
                await pilot.pause(0.2)
                return isinstance(app.screen, ConnectDialog)

        pushed = asyncio.run(scenario())
        assert pushed

    def test_on_select_changed_ignores_a_foreign_select(self) -> None:
        async def scenario() -> None:
            async with _open_connect_dialog() as (app, pilot, dialog):
                foreign: Select[str] = Select([("a", "a")], id="not-a-profile-select")
                dialog.on_select_changed(Select.Changed(foreign, "a"))  # must not raise
                await pilot.pause()

        asyncio.run(scenario())

    def test_on_option_list_option_selected_ignores_a_foreign_option_list(self) -> None:
        from textual.widgets.option_list import Option

        async def scenario() -> None:
            async with _open_connect_dialog() as (app, pilot, dialog):
                foreign = OptionList(id="not-a-connect-option-list")
                dialog.on_option_list_option_selected(OptionList.OptionSelected(foreign, Option("x"), 0))
                await pilot.pause()  # must not raise

        asyncio.run(scenario())


class TestLocalBackend:
    def test_connect_dialog_local_path_validation_errors(self, tmp_path: Path) -> None:
        """Empty path and "not a directory" are both construction-time
        checks ``_build_local_store()`` raises before any real scan starts —
        the same "cheap check, no I/O, shown inline" contract
        ``_build_s3_store()``/``_build_azure_store()`` already have for
        their own required fields."""

        async def scenario() -> tuple[str, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                dialog.query_one("#connect-local-path", Input).value = ""
                dialog.query_one("#connect-submit", Button).press()
                await pilot.pause(0.1)
                empty_status = str(dialog.query_one("#connect-status", Static).render())

                not_a_dir = tmp_path / "not-a-directory.txt"
                not_a_dir.write_text("x")
                dialog.query_one("#connect-local-path", Input).value = str(not_a_dir)
                dialog.query_one("#connect-submit", Button).press()
                await pilot.pause(0.1)
                not_dir_status = str(dialog.query_one("#connect-status", Static).render())
                return empty_status, not_dir_status

        empty_status, not_dir_status = asyncio.run(scenario())
        assert "directory path" in empty_status.lower(), empty_status
        assert "not a directory" in not_dir_status.lower(), not_dir_status

    def test_local_directory_tree_hides_files(self, tmp_path: Path, wait_until: Any) -> None:
        """``DirsOnlyTree`` (the local backend's browser) must never show a
        plain file, only subdirectories — Textual's own ``DirectoryTree``
        shows both by default; ``filter_paths()`` is the override this
        project's own subclass uses to hide files entirely (see
        ``connect_dialog.py``'s own docstring)."""
        (tmp_path / "a-file.txt").write_text("x")
        (tmp_path / "a-subdir").mkdir()

        async def scenario() -> list[str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=2.0, interval=0.1)
                return [str(child.label) for child in tree.root.children]

        labels = asyncio.run(scenario())
        assert any("a-subdir" in label for label in labels), labels
        assert not any("a-file" in label for label in labels), labels

    def test_local_directory_tree_dotdot_entry_goes_up_a_level(self, tmp_path: Path, wait_until: Any) -> None:
        """Going up is a ``".."`` entry inside the tree itself, matching the
        classic file-manager convention. Selecting it must re-root the whole
        tree at the parent (and sync the path Input), not merely land the
        cursor on an inert leaf."""
        child = tmp_path / "child"
        child.mkdir()

        async def scenario() -> tuple[str, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                assert len(app.screen.query("#connect-local-up")) == 0, "the old Up button must be gone"
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(child)
                await wait_until(pilot, lambda: tree.root.children, timeout=2.0, interval=0.1)
                assert str(tree.root.children[0].label) == "..", [str(c.label) for c in tree.root.children]

                tree.focus()
                tree.move_cursor(tree.root.children[0])
                await pilot.pause(0.05)
                await pilot.press("enter")
                await wait_until(pilot, lambda: str(tree.path) == str(tmp_path), timeout=2.0, interval=0.1)
                return str(tree.path), app.screen.query_one("#connect-local-path", Input).value

        tree_path, input_value = asyncio.run(scenario())
        assert tree_path == str(tmp_path)
        assert input_value == str(tmp_path)

    def test_local_directory_tree_backspace_jumps_to_parent_node_and_collapses_it(
        self, tmp_path: Path, wait_until: Any
    ) -> None:
        """Backspace is a *different* thing from the ".." entry: it moves
        within the currently-rooted tree (cursor to the current node's
        parent, collapsing it), it never changes what the tree is rooted
        at — the shared behavior every tree in this app gets via
        ``move_cursor_to_parent`` (see that function's own docstring)."""
        (tmp_path / "parent" / "child").mkdir(parents=True)

        async def scenario() -> tuple[bool, bool, str, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=2.0, interval=0.1)
                parent_node = next(c for c in tree.root.children if str(c.label) == "parent")
                tree.move_cursor(parent_node)
                await pilot.pause(0.05)
                await pilot.press("enter")
                await wait_until(pilot, lambda: parent_node.children, timeout=2.0, interval=0.1)
                child_node = parent_node.children[0]
                tree.move_cursor(child_node)
                await pilot.pause(0.1)
                path_before = str(tree.path)

                await pilot.press("backspace")
                await pilot.pause(0.2)
                landed_on_parent = tree.cursor_node is parent_node
                return landed_on_parent, not parent_node.is_expanded, str(tree.path), path_before

        landed_on_parent, collapsed, path_after, path_before = asyncio.run(scenario())
        assert landed_on_parent
        assert collapsed
        assert path_after == path_before, 'Backspace must never re-root the tree — only ".." does that'

    def test_local_directory_tree_typeahead_jumps_to_matching_sibling(self, tmp_path: Path, wait_until: Any) -> None:
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()
        (tmp_path / "gamma").mkdir()

        async def scenario() -> tuple[str | None, str | None]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=2.0, interval=0.1)
                tree.focus()
                await pilot.pause(0.05)

                await pilot.press("b")
                await pilot.pause(0.1)
                matched = str(tree.cursor_node.label) if tree.cursor_node is not None else None

                before = tree.cursor_node
                await pilot.press("z")  # no sibling starts with "z" — must not move
                await pilot.pause(0.1)
                unmatched_stayed = str(before.label) if tree.cursor_node is before and before is not None else None
                return matched, unmatched_stayed

        matched, unmatched_stayed = asyncio.run(scenario())
        assert matched == "beta", matched
        assert unmatched_stayed == "beta", "an unmatched keystroke must not move the cursor at all"

    def test_local_directory_tree_typeahead_resets_to_a_single_char_match(
        self, tmp_path: Path, wait_until: Any
    ) -> None:
        """ "b" then "g": the extended buffer "bg" matches no sibling, but
        "g" alone matches "gamma" — the buffer must reset to just "g"
        (and the cursor must actually jump), not stay stuck on the
        now-dead "bg" prefix for the rest of the timeout window."""
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()
        (tmp_path / "gamma").mkdir()

        async def scenario() -> str | None:
            async with _open_connect_dialog() as (app, pilot, dialog):
                tree = app.screen.query_one("#connect-local-tree", DirectoryTree)
                tree.path = str(tmp_path)
                await wait_until(pilot, lambda: tree.root.children, timeout=2.0, interval=0.1)
                tree.focus()
                await pilot.pause(0.05)

                await pilot.press("b")
                await pilot.pause(0.1)
                await pilot.press("g")
                await pilot.pause(0.1)
                return str(tree.cursor_node.label) if tree.cursor_node is not None else None

        assert asyncio.run(scenario()) == "gamma"

    def test_build_local_store_with_a_real_directory_returns_a_real_local_fs_store(self, tmp_path: Path) -> None:
        """Every other "local" backend test in this file drives a scan via
        ``open_browser_pilot``, which monkeypatches ``_build_local_store``
        entirely (see that fixture's own docstring) — so the real method's
        own success path, constructing an actual ``LocalFsStore``, is never
        otherwise exercised at all."""
        from synology_apm_repo.sdk.storage import LocalFsStore

        async def scenario() -> tuple[object, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                dialog.query_one("#connect-local-path", Input).value = str(tmp_path)
                return dialog._build_local_store()

        store, label = asyncio.run(scenario())
        assert isinstance(store, LocalFsStore)
        assert label == str(tmp_path)

    def test_pressing_enter_in_the_local_path_field_submits(self, tmp_path: Path) -> None:
        """A real scan of an empty ``tmp_path`` can complete (finding
        nothing) faster than polling could ever observe ``_scanning`` go
        True and back — a spy on ``_submit`` itself, rather than watching
        ``_scanning``, is the only race-free way to confirm Enter reached
        it."""

        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                dialog._submit = fake_submit  # type: ignore[method-assign, assignment]
                path_input = dialog.query_one("#connect-local-path", Input)
                path_input.value = str(tmp_path)
                path_input.focus()
                await pilot.press("enter")
                await pilot.pause()
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
        wait_until: Any,
    ) -> None:
        """Valid fields build a real ``S3Store``/``AzureStore``, a
        (faked) successful scan finds one repository, and the dialog dismisses with
        ``(repos, label)`` — the shape ``BrowseScreen._apply_discovered``
        expects (see ``connect_dialog.py``'s own ``ConnectResult``)."""
        fake_repo = _fake_repository(repo_root)

        async def fake_catalogs() -> list[Catalog]:
            return []

        fake_repo.catalogs = fake_catalogs  # type: ignore[method-assign]

        captured: dict[str, object] = {}

        async def fake_discover_remote(
            self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
        ) -> AsyncIterator[Repository]:
            captured["store"] = store
            yield fake_repo

        monkeypatch.setattr(api.Session, "discover_remote", fake_discover_remote)

        async def scenario() -> ConnectResult:
            result: ConnectResult | None = None

            async def on_dismiss(r: ConnectResult | None) -> None:
                nonlocal result
                result = r

            async with _open_connect_dialog() as (app, pilot, _first_dialog):
                # Stacks a second ConnectDialog on top of the one auto-opened
                # on launch, purely so this scenario gets its own dismiss
                # callback to inspect.
                app.push_screen(ConnectDialog(), on_dismiss)
                await pilot.pause(0.2)
                dialog = app.screen
                assert isinstance(dialog, ConnectDialog), dialog
                await _activate_backend_and_settle(dialog, backend, pilot, wait_until)
                set_fields(dialog)
                dialog.query_one("#connect-submit", Button).press()
                await wait_until(pilot, lambda: result is not None, timeout=1.5, interval=0.05)
                assert result is not None, "ConnectDialog never dismissed"
            return result

        repos, label = asyncio.run(scenario())
        assert isinstance(captured["store"], expected_store_type)
        assert label == expected_label
        assert len(repos) == 1
        assert repos[0] is fake_repo

    def test_connecting_via_s3_backend_populates_browse_screen(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        """A successful ``ConnectDialog`` scan (here: S3, with
        ``Session.discover_remote`` faked to yield one repository, so this needs
        no live endpoint) must leave ``BrowseScreen`` — not ``ConnectDialog``
        — on screen, with that repository already rendered in ``#col-catalogs``
        and ``#open-status`` reporting it found. The discovery loop lives
        entirely in ``ConnectDialog`` (see ``connect_dialog.py``'s own
        module docstring for why), so this drives the real dialog UI rather
        than calling a ``BrowseScreen`` method directly."""
        fake_repo = _fake_repository("@ActiveProtectData/repo-1")

        async def fake_catalogs() -> list[Catalog]:
            return []

        fake_repo.catalogs = fake_catalogs  # type: ignore[method-assign]

        async def fake_discover_remote(
            self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
        ) -> AsyncIterator[Repository]:
            yield fake_repo

        monkeypatch.setattr(api.Session, "discover_remote", fake_discover_remote)

        async def scenario() -> tuple[int, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await _activate_backend_and_settle(dialog, "s3", pilot, wait_until)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-bucket"
                dialog.query_one("#connect-submit", Button).press()
                await wait_until(pilot, lambda: isinstance(app.screen, BrowseScreen), timeout=3.0, interval=0.1)
                assert isinstance(app.screen, BrowseScreen), app.screen
                status = str(app.screen.query_one("#open-status", Static).render())
                tree = app.screen.query_one("#col-catalogs", Tree)
                return len(tree.root.children), status

        repo_count, status = asyncio.run(scenario())
        assert repo_count == 1, status
        assert "found" in status.lower(), status

    def test_connect_dialog_scan_apm_repo_error_shows_inline_and_stays_open(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        async def fake_discover_remote(
            self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
        ) -> AsyncIterator[Repository]:
            raise ApmRepoError("repo is locked")
            yield  # pragma: no cover - unreachable, makes this an async generator

        monkeypatch.setattr(api.Session, "discover_remote", fake_discover_remote)

        async def scenario() -> tuple[bool, str, bool]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await _activate_backend_and_settle(dialog, "s3", pilot, wait_until)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                submit = dialog.query_one("#connect-submit", Button)
                submit.press()
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                return isinstance(app.screen, ConnectDialog), status, submit.disabled

        still_open, status, submit_disabled = asyncio.run(scenario())
        assert still_open
        assert "repo is locked" in status
        assert submit_disabled is False  # _fail_scan re-enables it

    def test_connect_dialog_scan_unexpected_exception_shows_inline_and_stays_open(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        class _FakeConnectionRefused(Exception):
            pass

        async def fake_discover_remote(
            self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
        ) -> AsyncIterator[Repository]:
            raise _FakeConnectionRefused("connection refused")
            yield  # pragma: no cover - unreachable, makes this an async generator

        monkeypatch.setattr(api.Session, "discover_remote", fake_discover_remote)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await _activate_backend_and_settle(dialog, "s3", pilot, wait_until)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                dialog.query_one("#connect-submit", Button).press()
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open
        assert "connection refused" in status

    def test_connect_dialog_scan_finding_nothing_shows_inline_error(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        async def fake_discover_remote(
            self: object, store: object, key: str | None = None, *, progress: object = None, trace: object = None
        ) -> AsyncIterator[Repository]:
            return
            yield  # pragma: no cover - unreachable, makes this an async generator

        monkeypatch.setattr(api.Session, "discover_remote", fake_discover_remote)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await _activate_backend_and_settle(dialog, "s3", pilot, wait_until)
                dialog.query_one("#connect-s3-bucket", Input).value = "test-1"
                dialog.query_one("#connect-submit", Button).press()
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open
        assert "no repository found" in status

    def test_submit_is_a_no_op_while_already_scanning(self) -> None:
        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                dialog._scanning = True

                called = False

                def fake_build_store() -> tuple[object, str]:
                    nonlocal called
                    called = True
                    raise AssertionError("_build_store must not run while a scan is already underway")

                dialog._build_store = fake_build_store  # type: ignore[method-assign,assignment]
                dialog._submit()
                await pilot.pause()
                return called

        assert asyncio.run(scenario()) is False


__all__: list[str] = []
