"""``textual`` ``Pilot``-driven coverage for ``ConnectDialog``'s saved-
profile management, backed by ``browser/screens/profile_manager.py``:
per-tab profile listing/filtering, selecting a saved profile to refill
its fields (secrets included), and the save/delete flows and their
error paths — all against an in-memory fake standing in for
``profiles.json`` + the OS keyring. See
``test_browser_pilot_connect_dialog.py`` for the dialog's core
mounting/tabs/local-backend/scan-and-submit mechanics, and
``test_browser_pilot_remote_browser.py`` for S3/Azure/SMB field
validation and remote bucket/container browsing — both duplicate this
file's own ``_open_connect_dialog``/``_activate_backend`` helpers
verbatim rather than importing them (see ``tests/CLAUDE.md``'s "no test
module ever imports from another").
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, Checkbox, Input, Select, Static, Tabs

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens import profile_manager as profile_manager_module
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.profile_manager import _ProfileBackend
from synology_apm_repo.sdk.profiles import BackendKind, ProfileSummary


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


def _select_option_values(select: Select[str]) -> list[str]:
    """The real (non-blank) option values currently loaded into a
    ``Select`` — there is no public listing accessor, so this reads the
    same ``_options`` list ``Select`` itself builds from ``set_options()``,
    filtering out the blank entry ``allow_blank=True`` always prepends."""
    return [value for _, value in select._options if isinstance(value, str)]


async def _wait_for_profile_options(dialog: ConnectDialog, backend: str, pilot: Pilot[None], wait_until: Any) -> None:
    select = dialog.query_one(f"#connect-{backend}-profile-select", Select)
    await wait_until(pilot, lambda: bool(_select_option_values(select)), timeout=1.5, interval=0.05)


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


def _make_fake_profile_store(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, tuple[BackendKind, dict[str, str | bool]]]:
    """A simple in-memory dict standing in for ``profiles.json`` + the OS
    keyring, keyed by profile name -> ``(kind, fields)`` — patched in for
    ``profile_manager_module``'s ``list_profiles``/``load_profile``/
    ``save_profile``/``delete_profile`` exactly like ``list_buckets`` is
    faked above."""
    store: dict[str, tuple[BackendKind, dict[str, str | bool]]] = {}

    async def fake_list_profiles(*, config_dir: Path | None = None) -> list[ProfileSummary]:
        return [ProfileSummary(name=name, kind=kind) for name, (kind, _fields) in sorted(store.items())]

    async def fake_load_profile(name: str, *, config_dir: Path | None = None) -> dict[str, str | bool]:
        return dict(store[name][1])

    async def fake_save_profile(
        name: str, kind: BackendKind, fields: dict[str, str | bool], *, config_dir: Path | None = None
    ) -> None:
        store[name] = (kind, dict(fields))

    async def fake_delete_profile(name: str, *, config_dir: Path | None = None) -> None:
        del store[name]

    monkeypatch.setattr(profile_manager_module, "list_profiles", fake_list_profiles)
    monkeypatch.setattr(profile_manager_module, "load_profile", fake_load_profile)
    monkeypatch.setattr(profile_manager_module, "save_profile", fake_save_profile)
    monkeypatch.setattr(profile_manager_module, "delete_profile", fake_delete_profile)
    return store


class TestProfileManagement:
    def test_connect_dialog_profile_select_filters_by_tab(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        """Each tab's picker only ever offers profiles of its own backend
        kind — a saved profile of one kind must never show up in another
        tab's ``Select``."""
        store = _make_fake_profile_store(monkeypatch)
        store["my-s3"] = (BackendKind.S3, {"bucket": "bucket-a"})
        store["my-azure"] = (BackendKind.AZURE, {"container": "container-a"})
        store["my-smb"] = (BackendKind.SMB, {"server": "nas.example.com", "share": "share-a"})

        async def scenario() -> tuple[list[str], list[str], list[str]]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await _wait_for_profile_options(dialog, "s3", pilot, wait_until)
                await _wait_for_profile_options(dialog, "azure", pilot, wait_until)
                await _wait_for_profile_options(dialog, "smb", pilot, wait_until)
                s3_select = dialog.query_one("#connect-s3-profile-select", Select)
                azure_select = dialog.query_one("#connect-azure-profile-select", Select)
                smb_select = dialog.query_one("#connect-smb-profile-select", Select)
                return (
                    _select_option_values(s3_select),
                    _select_option_values(azure_select),
                    _select_option_values(smb_select),
                )

        s3_names, azure_names, smb_names = asyncio.run(scenario())
        assert s3_names == ["my-s3"], s3_names
        assert azure_names == ["my-azure"], azure_names
        assert smb_names == ["my-smb"], smb_names

    def test_connect_dialog_selecting_s3_profile_refills_fields_including_secret(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        """Selecting a saved profile refills every field of that tab —
        including the secret landing, unmasked, in the ``password=True``
        secret-key ``Input``'s ``.value`` — without ever auto-submitting
        (the dialog must still be open, on the S3 tab's fields, afterwards)."""
        store = _make_fake_profile_store(monkeypatch)
        store["my-s3"] = (
            BackendKind.S3,
            {
                "bucket": "bucket-a",
                "endpoint": "https://example.com",
                "region": "us-east-1",
                "verify_tls": False,
                "access_key": "AKIAEXAMPLE",
                "secret_key": "super-secret-value",
            },
        )

        async def scenario() -> tuple[str, str, str, str, str, bool]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "s3", pilot, wait_until)
                select = dialog.query_one("#connect-s3-profile-select", Select)
                select.value = "my-s3"
                await pilot.pause(0.2)
                return (
                    dialog.query_one("#connect-s3-bucket", Input).value,
                    dialog.query_one("#connect-s3-endpoint", Input).value,
                    dialog.query_one("#connect-s3-region", Input).value,
                    dialog.query_one("#connect-s3-access-key", Input).value,
                    dialog.query_one("#connect-s3-secret-key", Input).value,
                    dialog.query_one("#connect-s3-verify-tls", Checkbox).value,
                )

        bucket, endpoint, region, access_key, secret_key, verify_tls = asyncio.run(scenario())
        assert bucket == "bucket-a"
        assert endpoint == "https://example.com"
        assert region == "us-east-1"
        assert access_key == "AKIAEXAMPLE"
        assert secret_key == "super-secret-value"
        assert verify_tls is False

    def test_connect_dialog_save_profile_validates_fields_before_showing_name_row(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ "Save as profile..." validates the tab's fields the same no-I/O
        way ``_build_s3_store()`` does before ever showing the name prompt —
        an empty bucket must show the usual inline warning instead, with the
        name row staying hidden and nothing saved."""
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                row_visible = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")
                status = str(dialog.query_one("#connect-status", Static).render())
                return row_visible, status

        row_visible, status = asyncio.run(scenario())
        assert not row_visible, "the name row must not appear when required fields are missing"
        assert "bucket" in status.lower(), status
        assert not store

    def test_connect_dialog_save_profile_flow(self, monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
        """ "Save as profile...": reveals the inline name row; confirming
        calls ``save_profile()`` with the tab's current fields and hides the
        row again, without ever requiring a live connection first."""
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> tuple[bool, bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-bucket", Input).value = "bucket-a"
                dialog.query_one("#connect-s3-access-key", Input).value = "AKIAEXAMPLE"
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                row_visible_before = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")

                dialog.query_one("#connect-s3-profile-name-input", Input).value = "new-profile"
                dialog.query_one("#connect-s3-profile-name-confirm", Button).press()
                await wait_until(pilot, lambda: "new-profile" in store, timeout=1.5, interval=0.05)
                row_visible_after = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")
                status = str(dialog.query_one("#connect-status", Static).render())
                return row_visible_before, row_visible_after, status

        row_visible_before, row_visible_after, status = asyncio.run(scenario())
        assert row_visible_before, "the name row must appear after Save as profile..."
        assert not row_visible_after, "confirming must hide the name row again"
        assert "new-profile" in status.lower()
        kind, fields = store["new-profile"]
        assert kind is BackendKind.S3
        assert fields["bucket"] == "bucket-a"
        assert fields["access_key"] == "AKIAEXAMPLE"

    def test_connect_dialog_delete_profile_flow(self, monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
        """Deleting is immediate, no confirmation dialog — matches
        ``WorklistScreen``'s own precedent for a destructive action."""
        store = _make_fake_profile_store(monkeypatch)
        store["doomed"] = (BackendKind.S3, {"bucket": "bucket-a"})

        async def scenario() -> tuple[bool, list[str]]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "s3", pilot, wait_until)
                select = dialog.query_one("#connect-s3-profile-select", Select)
                select.value = "doomed"
                await pilot.pause(0.1)
                delete_button = dialog.query_one("#connect-s3-delete-profile-button", Button)
                delete_button_enabled = not delete_button.disabled
                delete_button.press()
                await wait_until(pilot, lambda: "doomed" not in store, timeout=1.5, interval=0.05)
                return delete_button_enabled, _select_option_values(select)

        delete_button_enabled, remaining_names = asyncio.run(scenario())
        assert delete_button_enabled, "the delete button must enable once a profile is selected"
        assert "doomed" not in store
        assert remaining_names == []

    def test_connect_dialog_profile_load_failure_shows_inline_error(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        """A corrupt config file or locked keyring surfacing from
        ``load_profile()`` must show inline in ``#connect-status``, not crash
        the dialog — manual field entry + Connect stays usable regardless."""
        store = _make_fake_profile_store(monkeypatch)
        store["broken"] = (BackendKind.S3, {"bucket": "bucket-a"})

        async def fake_load_profile_raises(name: str, *, config_dir: Path | None = None) -> dict[str, str | bool]:
            raise RuntimeError("keyring backend unavailable")

        monkeypatch.setattr(profile_manager_module, "load_profile", fake_load_profile_raises)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "s3", pilot, wait_until)
                select = dialog.query_one("#connect-s3-profile-select", Select)
                select.value = "broken"
                await pilot.pause(0.2)
                status = str(dialog.query_one("#connect-status", Static).render())
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "a failed profile load must not crash/dismiss the dialog"
        assert "error" in status.lower(), status

    def test_connect_dialog_escape_while_naming_profile_closes_only_the_name_row(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Esc while the inline name row is open must close only that row —
        the dialog itself (and whatever was already typed in the other
        fields) must stay exactly as it was."""
        _make_fake_profile_store(monkeypatch)

        async def scenario() -> tuple[bool, bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-bucket", Input).value = "bucket-a"
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                row_visible_before = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")

                await pilot.press("escape")
                await pilot.pause(0.1)
                row_visible_after = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")
                still_open = isinstance(app.screen, ConnectDialog)
                bucket_value = dialog.query_one("#connect-s3-bucket", Input).value if still_open else ""
                return row_visible_before, still_open and not row_visible_after, bucket_value

        row_visible_before, closed_row_only, bucket_value = asyncio.run(scenario())
        assert row_visible_before, "the name row must appear after Save as profile..."
        assert closed_row_only, "Esc must close only the name row, leaving the dialog open"
        assert bucket_value == "bucket-a", "the rest of the form must be untouched by that Esc"

    def test_connect_dialog_save_profile_empty_name_warns_and_does_not_save(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> str:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-bucket", Input).value = "bucket-a"
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-profile-name-input", Input).value = ""
                dialog.query_one("#connect-s3-profile-name-confirm", Button).press()
                await pilot.pause(0.1)
                return str(dialog.query_one("#connect-status", Static).render())

        status = asyncio.run(scenario())
        assert store == {}
        assert "name" in status.lower()

    def test_connect_dialog_save_profile_failure_shows_inline_error(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        _make_fake_profile_store(monkeypatch)

        async def fake_save_profile_raises(
            name: str, kind: BackendKind, fields: dict[str, str | bool], *, config_dir: Path | None = None
        ) -> None:
            raise RuntimeError("disk full")

        monkeypatch.setattr(profile_manager_module, "save_profile", fake_save_profile_raises)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-bucket", Input).value = "bucket-a"
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-profile-name-input", Input).value = "new-profile"
                dialog.query_one("#connect-s3-profile-name-confirm", Button).press()
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                row_still_open = dialog.query_one("#connect-s3-profile-name-row").has_class("-visible")
                return row_still_open, status

        row_still_open, status = asyncio.run(scenario())
        assert row_still_open, "a failed save must leave the name row open, not hide it as if it succeeded"
        assert "disk full" in status

    def test_connect_dialog_pressing_enter_in_the_profile_name_field_confirms(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> None:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-bucket", Input).value = "bucket-a"
                dialog.query_one("#connect-s3-save-profile-button", Button).press()
                await pilot.pause(0.1)
                name_input = dialog.query_one("#connect-s3-profile-name-input", Input)
                name_input.value = "enter-confirmed"
                name_input.focus()
                await pilot.press("enter")
                await wait_until(pilot, lambda: "enter-confirmed" in store, timeout=1.5, interval=0.05)

        asyncio.run(scenario())
        assert "enter-confirmed" in store

    def test_connect_dialog_delete_profile_failure_shows_inline_error(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        store = _make_fake_profile_store(monkeypatch)
        store["doomed"] = (BackendKind.S3, {"bucket": "bucket-a"})

        async def fake_delete_profile_raises(name: str, *, config_dir: Path | None = None) -> None:
            raise RuntimeError("keyring locked")

        monkeypatch.setattr(profile_manager_module, "delete_profile", fake_delete_profile_raises)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "s3")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "s3", pilot, wait_until)
                select = dialog.query_one("#connect-s3-profile-select", Select)
                select.value = "doomed"
                await pilot.pause(0.1)
                dialog.query_one("#connect-s3-delete-profile-button", Button).press()
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open
        assert "keyring locked" in status
        assert "doomed" in store  # the failed delete never removed it

    def test_connect_dialog_delete_selected_profile_with_nothing_selected_is_a_no_op(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The delete button is disabled with nothing selected (see
        ``on_select_changed``), so this guard is otherwise unreachable
        through the UI — called directly here, the same defensive-code
        rationale as ``BrowseScreen``'s foreign-DataTable guard test."""
        _make_fake_profile_store(monkeypatch)

        async def scenario() -> None:
            async with _open_connect_dialog() as (app, pilot, dialog):
                dialog._delete_selected_profile(_ProfileBackend.S3)  # nothing selected -- Select.value is BLANK
                await pilot.pause()

        asyncio.run(scenario())  # must not raise

    def test_connect_dialog_profile_list_refresh_failure_shows_inline_error(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        async def fake_list_profiles_raises() -> list[ProfileSummary]:
            raise RuntimeError("config file corrupt")

        monkeypatch.setattr(profile_manager_module, "list_profiles", fake_list_profiles_raises)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                status = await _wait_for_status_containing(dialog, pilot, wait_until, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "a corrupt config file must never crash the dialog itself"
        assert "config file corrupt" in status

    def test_connect_dialog_selecting_an_azure_profile_refills_its_own_fields(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        store = _make_fake_profile_store(monkeypatch)
        store["my-azure"] = (
            BackendKind.AZURE,
            {"container": "container-a", "account_url": "https://x.blob.core.windows.net"},
        )

        async def scenario() -> tuple[str, bool]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "azure")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "azure", pilot, wait_until)
                select = dialog.query_one("#connect-azure-profile-select", Select)
                select.value = "my-azure"
                await wait_until(
                    pilot, lambda: dialog.query_one("#connect-azure-container", Input).value, timeout=1.5, interval=0.05
                )
                container_value = dialog.query_one("#connect-azure-container", Input).value
                delete_enabled = not dialog.query_one("#connect-azure-delete-profile-button", Button).disabled
                return container_value, delete_enabled

        container_value, delete_enabled = asyncio.run(scenario())
        assert container_value == "container-a"
        assert delete_enabled

    def test_connect_dialog_selecting_an_smb_profile_refills_fields_including_secret(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any
    ) -> None:
        store = _make_fake_profile_store(monkeypatch)
        store["my-smb"] = (
            BackendKind.SMB,
            {
                "server": "nas.example.com",
                "share": "backups",
                "port": "1445",
                "username": "admin",
                "password": "hunter2",
            },
        )

        async def scenario() -> tuple[str, str, str, str, str, bool]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "smb")
                await pilot.pause(0.1)
                await _wait_for_profile_options(dialog, "smb", pilot, wait_until)
                select = dialog.query_one("#connect-smb-profile-select", Select)
                select.value = "my-smb"
                await wait_until(
                    pilot, lambda: dialog.query_one("#connect-smb-server", Input).value, timeout=1.5, interval=0.05
                )
                return (
                    dialog.query_one("#connect-smb-server", Input).value,
                    dialog.query_one("#connect-smb-share", Input).value,
                    dialog.query_one("#connect-smb-port", Input).value,
                    dialog.query_one("#connect-smb-username", Input).value,
                    dialog.query_one("#connect-smb-password", Input).value,
                    not dialog.query_one("#connect-smb-delete-profile-button", Button).disabled,
                )

        server, share, port, username, password, delete_enabled = asyncio.run(scenario())
        assert server == "nas.example.com"
        assert share == "backups"
        assert port == "1445"
        assert username == "admin"
        assert password == "hunter2"
        assert delete_enabled

    def test_connect_dialog_azure_save_profile_flow(self, monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
        """Azure's own counterpart to ``test_connect_dialog_save_profile_flow``
        (S3's) — the "Save as profile..." button's own flow (as opposed to
        selecting an already-saved profile, which
        ``test_connect_dialog_selecting_an_azure_profile_refills_its_own_fields``
        already covers) had no direct Azure-tab coverage before this."""
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> tuple[bool, bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "azure")
                await pilot.pause(0.1)
                dialog.query_one("#connect-azure-container", Input).value = "container-a"
                # AzureStore's constructor (unlike S3Store's fully-lazy one)
                # validates account_url synchronously, so an empty one would
                # fail this no-I/O validation step before ever showing the
                # name row - see _build_azure_store's own real construction.
                dialog.query_one("#connect-azure-account-url", Input).value = "https://example.blob.core.windows.net"
                dialog.query_one("#connect-azure-save-profile-button", Button).press()
                await pilot.pause(0.1)
                row_visible_before = dialog.query_one("#connect-azure-profile-name-row").has_class("-visible")

                dialog.query_one("#connect-azure-profile-name-input", Input).value = "new-azure-profile"
                dialog.query_one("#connect-azure-profile-name-confirm", Button).press()
                await wait_until(pilot, lambda: "new-azure-profile" in store, timeout=1.5, interval=0.05)
                row_visible_after = dialog.query_one("#connect-azure-profile-name-row").has_class("-visible")
                status = str(dialog.query_one("#connect-status", Static).render())
                return row_visible_before, row_visible_after, status

        row_visible_before, row_visible_after, status = asyncio.run(scenario())
        assert row_visible_before, "the name row must appear after Save as profile..."
        assert not row_visible_after, "confirming must hide the name row again"
        assert "new-azure-profile" in status.lower()
        kind, fields = store["new-azure-profile"]
        assert kind is BackendKind.AZURE
        assert fields["container"] == "container-a"

    def test_connect_dialog_smb_save_profile_flow(self, monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
        """SMB's own counterpart to ``test_connect_dialog_save_profile_flow``
        (S3's) — confirms the save flow works identically for the third
        backend, not just the two the mechanism was originally built for."""
        store = _make_fake_profile_store(monkeypatch)

        async def scenario() -> tuple[bool, bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                _activate_backend(dialog, "smb")
                await pilot.pause(0.1)
                dialog.query_one("#connect-smb-server", Input).value = "nas.example.com"
                dialog.query_one("#connect-smb-share", Input).value = "backups"
                dialog.query_one("#connect-smb-save-profile-button", Button).press()
                await pilot.pause(0.1)
                row_visible_before = dialog.query_one("#connect-smb-profile-name-row").has_class("-visible")

                dialog.query_one("#connect-smb-profile-name-input", Input).value = "new-smb-profile"
                dialog.query_one("#connect-smb-profile-name-confirm", Button).press()
                await wait_until(pilot, lambda: "new-smb-profile" in store, timeout=1.5, interval=0.05)
                row_visible_after = dialog.query_one("#connect-smb-profile-name-row").has_class("-visible")
                status = str(dialog.query_one("#connect-status", Static).render())
                return row_visible_before, row_visible_after, status

        row_visible_before, row_visible_after, status = asyncio.run(scenario())
        assert row_visible_before, "the name row must appear after Save as profile..."
        assert not row_visible_after, "confirming must hide the name row again"
        assert "new-smb-profile" in status.lower()
        kind, fields = store["new-smb-profile"]
        assert kind is BackendKind.SMB
        assert fields["server"] == "nas.example.com"
        assert fields["share"] == "backups"


__all__: list[str] = []
