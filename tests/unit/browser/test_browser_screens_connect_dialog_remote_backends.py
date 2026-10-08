"""``Pilot`` tests for ``ConnectDialog``'s S3/Azure/SMB tabs: field
validation (empty required fields, an invalid SMB port, an unresolvable
Azure account URL) and the "Browse" bucket/container picker
(``RemoteOptionsBrowser``, whose ``list_remote_items()`` call is faked
here). Fully offline: constructing an ``S3Store``/``AzureStore``/``SmbStore``
does no I/O. The dialog's core mechanics are covered in
``test_browser_screens_connect_dialog.py``; saved profiles in
``test_browser_screens_profile_manager.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from textual.widgets import Button, Checkbox, Input, OptionList, Static

from support.pilot import SDK_TIMEOUT, UI_TIMEOUT, focus_widget, wait_until
from synology_apm_repo.browser.screens import remote_browser as remote_browser_module
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.sdk.profiles import BackendKind
from synology_apm_repo.sdk.storage.s3 import S3Store
from unit.browser.connect_dialog_drivers import (
    activate_backend_and_settle,
    open_connect_dialog,
    wait_for_status_containing,
)

_BROWSE_PICKER_BACKENDS = [
    pytest.param("s3", "bucket", id="s3"),
    pytest.param("azure", "container", id="azure"),
]


class TestRemoteFields:
    @pytest.mark.parametrize(
        ("backend", "field"),
        [
            pytest.param("s3", "bucket", id="empty_bucket"),
            pytest.param("azure", "container", id="empty_azure_container"),
            pytest.param("smb", "server", id="empty_smb_server"),
        ],
    )
    def test_connect_dialog_empty_required_field_shows_warning_and_does_not_dismiss(
        self, backend: str, field: str
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, field)
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, f"an empty {field} name must not dismiss the dialog"
        assert field in status.lower(), status

    @pytest.mark.parametrize(("backend", "noun"), _BROWSE_PICKER_BACKENDS)
    def test_connect_dialog_browse_field_populates_from_a_real_list_call(
        self,
        monkeypatch: pytest.MonkeyPatch,
        backend: str,
        noun: str,
    ) -> None:
        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            return [f"{noun}-a", f"{noun}-b"]

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> tuple[str, str]:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one(f"#connect-{backend}-browse-{noun}s", Button).press()
                option_list = dialog.query_one(f"#connect-{backend}-{noun}-list", OptionList)
                await wait_until(pilot, lambda: option_list.option_count, timeout=SDK_TIMEOUT, interval=0.05)
                await pilot.press("enter")
                field_input = dialog.query_one(f"#connect-{backend}-{noun}", Input)
                await wait_until(pilot, lambda: field_input.value == f"{noun}-a", timeout=UI_TIMEOUT, interval=0.05)
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
    ) -> None:
        """Any listing failure (e.g. a credential without account-level list permission)."""

        class _FakeDenied(Exception):
            pass

        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            raise _FakeDenied("Access Denied")

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (_app, pilot, dialog):
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
    ) -> None:
        monkeypatch.setattr(remote_browser_module, "_NETWORK_TIMEOUT_SECONDS", 0.05)

        async def fake_list(kind: BackendKind, **kwargs: object) -> list[str]:
            await asyncio.Event().wait()  # never set: only the timeout ends this
            return []  # pragma: no cover - never reached, the wait_for wrapper cancels first

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list)

        async def scenario() -> str:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, backend, pilot)
                dialog.query_one(f"#connect-{backend}-browse-{noun}s", Button).press()
                # "listing ..." shows first, then the timeout error replaces it.
                status: str = await wait_for_status_containing(pilot, dialog, "timed out")
                return status

        status = asyncio.run(scenario())
        assert "error" in status.lower(), status
        assert "timed out" in status.lower(), status

    def test_connect_dialog_azure_unresolvable_account_url_shows_inline_error_instead_of_crashing(
        self,
    ) -> None:
        """Building the store rejects the account URL (``ProfileFieldError``) before any I/O."""

        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "azure", pilot)
                dialog.query_one("#connect-azure-container", Input).value = "my-container"
                dialog.query_one("#connect-azure-account-url", Input).value = "http://192.0.2.10:10000"
                dialog.query_one("#connect-azure-credential", Input).value = "some-key"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "error")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "a construction-time ProfileFieldError must show inline, not crash the dialog"
        assert "error" in status.lower(), status

    def test_s3_verify_tls_false_actually_reaches_the_constructed_stores_client_kwargs(
        self,
    ) -> None:
        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-bucket", Input).value = "my-bucket"
                dialog.query_one("#connect-s3-verify-tls", Checkbox).value = False

                store = (await dialog._build_remote_store(BackendKind.S3))[0]
                assert isinstance(store, S3Store)
                return bool(store._client_kwargs["verify"])

        assert asyncio.run(scenario()) is False

    def test_browse_buckets_is_a_no_op_while_already_browsing_or_scanning(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls = 0

        async def fake_list_buckets(kind: BackendKind, **kwargs: object) -> list[str]:
            nonlocal calls
            calls += 1
            return ["bucket-a"]

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_buckets)

        async def scenario() -> int:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog._remote_browser.browsing[BackendKind.S3] = True  # simulate an in-flight browse
                worker = dialog._browse_buckets()
                await wait_until(pilot, lambda: worker.is_finished, timeout=UI_TIMEOUT)
                return calls

        assert asyncio.run(scenario()) == 0

    def test_browse_buckets_reports_when_none_are_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def fake_list_buckets(kind: BackendKind, **kwargs: object) -> list[str]:
            return []

        monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_buckets)

        async def scenario() -> tuple[str, bool]:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                dialog.query_one("#connect-s3-browse-buckets", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "no bucket")
                option_list = dialog.query_one("#connect-s3-bucket-list", OptionList)
                return status, option_list.has_class("-visible")

        status, list_visible = asyncio.run(scenario())
        assert "no buckets found" in status.lower()
        assert not list_visible

    def test_pressing_enter_in_an_s3_field_submits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Enter in any field other than a profile-name input calls _submit().
        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "s3", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                monkeypatch.setattr(dialog, "_submit", fake_submit)
                bucket_input = dialog.query_one("#connect-s3-bucket", Input)
                bucket_input.value = "my-bucket"
                await focus_widget(pilot, bucket_input)
                await pilot.press("enter")
                await wait_until(pilot, lambda: submitted)
                return submitted

        assert asyncio.run(scenario())

    def test_pressing_enter_in_an_azure_field_submits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "azure", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                monkeypatch.setattr(dialog, "_submit", fake_submit)
                container_input = dialog.query_one("#connect-azure-container", Input)
                container_input.value = "my-container"
                await focus_widget(pilot, container_input)
                await pilot.press("enter")
                await wait_until(pilot, lambda: submitted)
                return submitted

        assert asyncio.run(scenario())

    def test_connect_dialog_empty_smb_share_shows_warning_and_does_not_dismiss(
        self,
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                dialog.query_one("#connect-smb-server", Input).value = "nas.example.com"
                dialog.query_one("#connect-submit", Button).press()
                status = await wait_for_status_containing(pilot, dialog, "share")
                return isinstance(app.screen, ConnectDialog), status

        still_open, status = asyncio.run(scenario())
        assert still_open, "an empty share name must not dismiss the dialog"
        assert "share" in status.lower(), status

    def test_connect_dialog_non_numeric_smb_port_shows_warning_instead_of_a_raw_exception(
        self,
    ) -> None:
        async def scenario() -> tuple[bool, str]:
            async with open_connect_dialog() as (app, pilot, dialog):
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

    def test_pressing_enter_in_an_smb_field_submits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def scenario() -> bool:
            async with open_connect_dialog() as (_app, pilot, dialog):
                await activate_backend_and_settle(dialog, "smb", pilot)
                submitted = False

                def fake_submit() -> None:
                    nonlocal submitted
                    submitted = True

                monkeypatch.setattr(dialog, "_submit", fake_submit)
                share_input = dialog.query_one("#connect-smb-share", Input)
                share_input.value = "my-share"
                await focus_widget(pilot, share_input)
                await pilot.press("enter")
                await wait_until(pilot, lambda: submitted)
                return submitted

        assert asyncio.run(scenario())
