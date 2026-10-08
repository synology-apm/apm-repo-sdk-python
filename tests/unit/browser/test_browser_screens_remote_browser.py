"""Unit tests for ``RemoteOptionsBrowser.browse`` against a minimal fake
``ConnectDialog`` host (``test_browser_screens_connect_dialog_remote_backends.py`` drives it
through the real dialog). ``_FakeDialog.post_message`` captures the
``ItemsListed`` message ``browse()`` posts instead of delivering it."""

from __future__ import annotations

from typing import Any

import pytest
from textual.app import App, ComposeResult
from textual.widgets import OptionList, Static

from synology_apm_repo.browser.screens import remote_browser as remote_browser_module
from synology_apm_repo.browser.screens.remote_browser import RemoteOptionsBrowser
from synology_apm_repo.browser.strings import CONNECT_NETWORK_TIMEOUT_WARNING
from synology_apm_repo.sdk.profiles import BackendKind


class _FakeDialog(App[None]):
    def __init__(self) -> None:
        super().__init__()
        self.scanning = False
        self.posted: list[RemoteOptionsBrowser.ItemsListed] = []

    def account_client_kwargs(self, backend: BackendKind) -> dict[str, object]:
        return {}

    def compose(self) -> ComposeResult:
        yield Static(id="connect-status")
        yield OptionList(id="connect-s3-bucket-list")
        yield OptionList(id="connect-azure-container-list")

    def post_message(self, message: Any) -> bool:
        if isinstance(message, RemoteOptionsBrowser.ItemsListed):
            self.posted.append(message)
            return True
        return super().post_message(message)


async def test_browse_posts_the_real_items_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        return ["bucket-a", "bucket-b"]

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

        assert len(app.posted) == 1
        message = app.posted[0]
        assert message.items == ["bucket-a", "bucket-b"]
        assert message.error is None
        assert message.option_list_id == "#connect-s3-bucket-list"
        assert message.noun == "bucket"
        assert browser.browsing[BackendKind.S3] is False  # reset after completion


async def test_browse_removes_the_visible_class_from_the_target_option_list(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        return []

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        option_list = app.query_one("#connect-s3-bucket-list", OptionList)
        option_list.add_class("-visible")
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

        assert not option_list.has_class("-visible")


async def test_browse_reports_a_timeout_as_the_network_timeout_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        raise TimeoutError

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

        message = app.posted[0]
        assert message.items == []
        assert message.error == CONNECT_NETWORK_TIMEOUT_WARNING
        assert browser.browsing[BackendKind.S3] is False


async def test_browse_reports_a_real_backend_failure_as_the_exception_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    boom = RuntimeError("no list permission")

    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        raise boom

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse(
            BackendKind.AZURE,
            option_list_id="#connect-azure-container-list",
            noun="container",
        )

        message = app.posted[0]
        assert message.error is boom
        assert browser.browsing[BackendKind.AZURE] is False


async def test_browse_is_a_no_op_while_the_same_backend_is_already_browsing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]
        browser.browsing[BackendKind.S3] = True  # simulates a browse already in flight

        await browser.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

        assert calls == 0
        assert app.posted == []


async def test_browse_is_a_no_op_while_the_dialog_itself_is_scanning(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    app.scanning = True
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse(
            BackendKind.S3,
            option_list_id="#connect-s3-bucket-list",
            noun="bucket",
        )

        assert calls == 0
        assert app.posted == []


async def test_browse_buckets_targets_the_s3_backend_and_option_list(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_kind: BackendKind | None = None

    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        nonlocal seen_kind
        seen_kind = kind
        return ["bucket-a"]

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse_buckets()

        assert seen_kind is BackendKind.S3
        assert app.posted[0].option_list_id == "#connect-s3-bucket-list"
        assert app.posted[0].noun == "bucket"


async def test_browse_containers_targets_the_azure_backend_and_option_list(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_kind: BackendKind | None = None

    async def fake_list_remote_items(kind: BackendKind, **kwargs: object) -> list[str]:
        nonlocal seen_kind
        seen_kind = kind
        return ["container-a"]

    monkeypatch.setattr(remote_browser_module, "list_remote_items", fake_list_remote_items)
    app = _FakeDialog()
    async with app.run_test():
        browser = RemoteOptionsBrowser(app)  # type: ignore[arg-type]

        await browser.browse_containers()

        assert seen_kind is BackendKind.AZURE
        assert app.posted[0].option_list_id == "#connect-azure-container-list"
        assert app.posted[0].noun == "container"
