"""``textual`` ``Pilot``-driven coverage for ``ConnectDialog``'s S3/Azure/
SMB backend tabs — field validation (empty required fields, an invalid
SMB port, an unresolvable Azure account URL) and the "Browse" bucket/
container picker, which calls ``browser/screens/remote_browser.py``'s
``list_remote_items()`` (faked here, no real endpoint needed). The
dialog's core mounting/tabs/local-backend/scan-and-submit mechanics are
covered in ``test_browser_pilot_connect_dialog.py``, and profile
save/load/delete in ``test_browser_profile_manager.py`` — both duplicate
this file's own ``_open_connect_dialog`` helper verbatim rather than
importing it, since backend activation itself is shared through
``tests/unit/browser/conftest.py``'s ``activate_backend_and_settle``
fixture instead.

The real connection test/repository scan runs *inside* this dialog, not in
``BrowseScreen`` — every test below stays fully offline (field-toggling/
validation only; constructing an ``S3Store``/``AzureStore``/``SmbStore``
does no real I/O).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, Checkbox, Input, OptionList, Static

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens import remote_browser as remote_browser_module
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.profile_manager import _ProfileBackend
from synology_apm_repo.sdk.profiles import BackendKind
from synology_apm_repo.sdk.storage.s3 import S3Store


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


_BROWSE_PICKER_BACKENDS = [
    pytest.param("s3", "bucket", id="s3"),
    pytest.param("azure", "container", id="azure"),
]


class TestRemoteFields:
    def test_connect_dialog_empty_bucket_shows_warning_and_does_not_dismiss(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "bucket")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an empty bucket name must not dismiss the dialog"
        assert "bucket" in status.lower(), status

    @pytest.mark.parametrize(("backend", "noun"), _BROWSE_PICKER_BACKENDS)
    def test_connect_dialog_browse_field_populates_from_a_real_list_call(
        self,
        monkeypatch: pytest.MonkeyPatch,
        backend: str,
        noun: str,
        wait_until: Any,
        activate_backend_and_settle: Any,
        ui_timeout: float,
        sdk_timeout: float,
    ) -> None:
        """The "Browse" button next to the bucket/container field calls the
        SDK's bucket-/container-less ``list_remote_items()`` (faked here, no
        real endpoint needed) and shows the result in
        ``#connect-{backend}-{noun}-list``; selecting one fills the field
        ``Input`` the same way ``DirsOnlyTree`` selection fills the local path
        ``Input``."""

        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            return [f"{noun}-a", f"{noun}-b"]

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> tuple[str, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one(f"#connect-{backend}-browse-{noun}s", Button).press()
                option_list = dialog.query_one(f"#connect-{backend}-{noun}-list", OptionList)
                await wait_until(pilot, lambda: option_list.option_count, timeout=sdk_timeout, interval=0.05)
                await pilot.press("enter")
                field_input = dialog.query_one(f"#connect-{backend}-{noun}", Input)
                await wait_until(pilot, lambda: field_input.value == f"{noun}-a", timeout=ui_timeout, interval=0.05)
                field_value = field_input.value
                status = str(dialog.query_one("#connect-status", Static).render())
                return field_value, status

        field_value, status = asyncio.run(scenario())
        assert field_value == f"{noun}-a"
        assert noun in status.lower(), status

    @pytest.mark.parametrize(("backend", "noun"), _BROWSE_PICKER_BACKENDS)
    def test_connect_dialog_browse_field_shows_error_without_list_permission(
        self,
        monkeypatch: pytest.MonkeyPatch,
        backend: str,
        noun: str,
        wait_until: Any,
        activate_backend_and_settle: Any,
        wait_for_status_containing: Any,
    ) -> None:
        """A credential without account-level list permission (or any other
        backend-specific failure) must show an inline error, not crash or
        silently leave the picker empty — same broad-catch idiom ``_scan()``
        already uses for exactly this class of failure."""

        class _FakeDenied(Exception):
            pass

        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            raise _FakeDenied("Access Denied")

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one(f"#connect-{backend}-browse-{noun}s", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "denied")
                option_list = dialog.query_one(f"#connect-{backend}-{noun}-list", OptionList)
                return option_list.has_class("-visible"), status

        list_visible, status = asyncio.run(scenario())
        assert not list_visible, f"no {noun}s were ever returned - the picker must stay hidden"
        assert "error" in status.lower(), status
        assert "denied" in status.lower(), status

    @pytest.mark.parametrize(("backend", "noun"), _BROWSE_PICKER_BACKENDS)
    def test_connect_dialog_browse_field_shows_timeout_error_instead_of_hanging(
        self,
        monkeypatch: pytest.MonkeyPatch,
        backend: str,
        noun: str,
        wait_until: Any,
        activate_backend_and_settle: Any,
        wait_for_status_containing: Any,
    ) -> None:
        """An endpoint that never responds must surface a clear timeout error
        within the dialog's own network timeout, not hang indefinitely — the
        ``list_remote_items()`` fake here never returns, standing in for an
        unreachable endpoint whose SDK-level connect/read timeouts
        (``storage/s3.py``/``storage/azure.py``) didn't fire for some reason;
        the dialog's own ``asyncio.wait_for`` wrapper is the backstop under
        test. The dialog's network timeout is monkeypatched down so this test
        doesn't itself take ``_NETWORK_TIMEOUT_SECONDS`` real seconds to run."""
        monkeypatch.setattr(remote_browser_module, "_NETWORK_TIMEOUT_SECONDS", 0.05)

        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            await asyncio.sleep(10)
            return []  # pragma: no cover - never reached, the wait_for wrapper cancels first

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> str:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one(f"#connect-{backend}-browse-{noun}s", Button).press()
                # "listing ..." shows first and is replaced by the timeout
                # error; waiting a fixed 0.3s could read either one.
                status: str = await wait_for_status_containing(pilot, dialog, "timed out")
                return status

        status = asyncio.run(scenario())
        assert "error" in status.lower(), status
        assert "timed out" in status.lower(), status

    def test_connect_dialog_azure_unresolvable_account_url_shows_inline_error_instead_of_crashing(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        """Unlike ``S3Store``'s fully-lazy constructor, ``AzureStore(...)``
        (``BlobServiceClient(...)``) validates its account_url/credential shape
        synchronously and can raise ``ValueError`` for an account URL with no
        path and no recognizable ``.blob.core.<...>`` subdomain — the account
        name is genuinely unresolvable. ``_submit()`` must catch this the same
        way it catches every other construction-time failure and show it
        inline, not let it propagate out of the button-press handler and crash
        the whole app."""

        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "azure", pilot)
                dialog.query_one("#connect-azure-container", Input).value = "my-container"
                dialog.query_one("#connect-azure-account-url", Input).value = "http://192.0.2.10:10000"
                dialog.query_one("#connect-azure-credential", Input).value = "some-key"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "a construction-time ValueError must show inline, not crash the dialog"
        assert "error" in status.lower(), status

    def test_s3_verify_tls_false_actually_reaches_the_constructed_stores_client_kwargs(
        self, wait_until: Any, activate_backend_and_settle: Any
    ) -> None:
        # A profile-refill test elsewhere only proves verify_tls round-trips
        # through the Checkbox widget itself -- never that it actually
        # reaches a constructed S3Store's real client kwargs.
        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "my-bucket"
                dialog.query_one("#connect-s3-verify-tls", Checkbox).value = False

                store = (await dialog._build_s3_store())[0]
                assert isinstance(store, S3Store)
                return bool(store._client_kwargs["verify"])

        assert asyncio.run(scenario()) is False

    def test_browse_buckets_is_a_no_op_while_already_browsing_or_scanning(
        self, monkeypatch: pytest.MonkeyPatch, wait_until: Any, activate_backend_and_settle: Any
    ) -> None:
        calls = 0

        async def fake_list_buckets(kind: BackendKind, **kwargs: object) -> list[str]:
            nonlocal calls
            calls += 1
            return ["bucket-a"]

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_buckets)

        async def scenario() -> int:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog._remote_browser.browsing[_ProfileBackend.S3] = True  # simulate an in-flight browse
                dialog._browse_buckets()
                # Asserting an absence (no list call while already browsing) --
                # there is no readiness signal to wait on instead.
                await pilot.pause(0.1)
                return calls

        assert asyncio.run(scenario()) == 0

    def test_browse_buckets_reports_when_none_are_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
        wait_until: Any,
        activate_backend_and_settle: Any,
        wait_for_status_containing: Any,
    ) -> None:
        async def fake_list_buckets(kind: BackendKind, **kwargs: object) -> list[str]:
            return []

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_buckets)

        async def scenario() -> tuple[str, bool]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-browse-buckets", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "no bucket")
                option_list = dialog.query_one("#connect-s3-bucket-list", OptionList)
                return status, option_list.has_class("-visible")

        status, list_visible = asyncio.run(scenario())
        assert "no buckets found" in status.lower()
        assert not list_visible

    def test_pressing_enter_in_an_s3_field_submits(self, wait_until: Any, activate_backend_and_settle: Any) -> None:
        # Only the local-path field's Enter-submits path was ever exercised
        # above -- on_input_submitted's "any other field" fallthrough
        # (``self._submit()``) for the S3/Azure tabs was untested.
        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                dialog._submit = fake_submit  # type: ignore[method-assign, assignment]
                bucket_input = dialog.query_one("#connect-s3-bucket", Input)
                bucket_input.value = "my-bucket"
                bucket_input.focus()
                await pilot.press("enter")
                await pilot.pause()
                return submitted

        assert asyncio.run(scenario())

    def test_pressing_enter_in_an_azure_field_submits(self, wait_until: Any, activate_backend_and_settle: Any) -> None:
        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "azure", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                dialog._submit = fake_submit  # type: ignore[method-assign, assignment]
                container_input = dialog.query_one("#connect-azure-container", Input)
                container_input.value = "my-container"
                container_input.focus()
                await pilot.press("enter")
                await pilot.pause()
                return submitted

        assert asyncio.run(scenario())

    def test_connect_dialog_empty_azure_container_shows_warning_and_does_not_dismiss(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "azure", pilot)
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "container")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an empty container name must not dismiss the dialog"
        assert "container" in status.lower(), status

    def test_connect_dialog_empty_smb_server_shows_warning_and_does_not_dismiss(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "server")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an empty server name must not dismiss the dialog"
        assert "server" in status.lower(), status

    def test_connect_dialog_empty_smb_share_shows_warning_and_does_not_dismiss(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                dialog.query_one("#connect-smb-server", Input).value = "nas.example.com"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "share")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an empty share name must not dismiss the dialog"
        assert "share" in status.lower(), status

    def test_connect_dialog_non_numeric_smb_port_shows_warning_instead_of_a_raw_exception(
        self, wait_until: Any, activate_backend_and_settle: Any, wait_for_status_containing: Any
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                dialog.query_one("#connect-smb-server", Input).value = "nas.example.com"
                dialog.query_one("#connect-smb-share", Input).value = "backups"
                dialog.query_one("#connect-smb-port", Input).value = "not-a-number"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "port")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an invalid port must not dismiss the dialog"
        assert "port" in status.lower(), status
        assert "invalid literal" not in status.lower(), "must show a clean message, not the raw ValueError text"

    def test_pressing_enter_in_an_smb_field_submits(self, wait_until: Any, activate_backend_and_settle: Any) -> None:
        async def scenario() -> bool:
            async with _open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                dialog._submit = fake_submit  # type: ignore[method-assign, assignment]
                share_input = dialog.query_one("#connect-smb-share", Input)
                share_input.value = "my-share"
                share_input.focus()
                await pilot.press("enter")
                await pilot.pause()
                return submitted

        assert asyncio.run(scenario())


__all__: list[str] = []
