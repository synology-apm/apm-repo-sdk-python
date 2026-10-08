"""``Pilot`` tests for ``KeyDialog`` against a fake ``Repository`` whose
``set_key()`` result is canned."""

from __future__ import annotations

from typing import Any

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Input, Static

import synology_apm_repo.sdk.api as _sdk_api
from support.fakes import faithful_to
from support.pilot import SDK_TIMEOUT, focus_widget, settle, wait_until
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.sdk.api import KeyVerification, SetKeyResult
from synology_apm_repo.sdk.errors import ApmRepoError


@faithful_to(_sdk_api.Repository)
class _FakeRepo:
    def __init__(self, result: SetKeyResult | ApmRepoError) -> None:
        self._result = result
        self.received_keys: list[str] = []

    async def set_key(self, key_string: str) -> SetKeyResult:
        self.received_keys.append(key_string)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class _FakeApp(App[None]):
    """Pushes a ``KeyDialog`` over ``repo``, recording each result it is
    dismissed with in ``dismissed``."""

    def __init__(self, repo: _FakeRepo) -> None:
        super().__init__()
        self._repo = repo
        self.dismissed: list[bool | None] = []

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(KeyDialog(self._repo), callback=self.dismissed.append)  # type: ignore[arg-type]


async def test_action_verify_with_a_typed_but_unsubmitted_key() -> None:
    repo = _FakeRepo(SetKeyResult(KeyVerification(gcm_ok=True, vault_key=b"vault")))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        await wait_until(pilot, lambda: repo.received_keys != [], timeout=SDK_TIMEOUT, interval=0.02)
        assert repo.received_keys == ["userKeyID@dGVzdA=="]


async def test_action_verify_with_an_empty_field_does_nothing() -> None:
    repo = _FakeRepo(SetKeyResult(KeyVerification(gcm_ok=True, vault_key=b"vault")))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.action_verify()
        await settle(pilot)
        assert repo.received_keys == []


async def test_wrong_key_shows_invalid_and_does_not_dismiss() -> None:
    repo = _FakeRepo(SetKeyResult(KeyVerification(gcm_ok=False, vault_key=None)))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@wrong=="
        screen.action_verify()
        status = screen.query_one("#key-status", Static)
        await wait_until(pilot, lambda: "invalid" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "gcm_ok=False" in str(status.render())
        assert isinstance(app.screen, KeyDialog)  # still open — not dismissed


async def test_action_cancel_dismisses_with_false() -> None:
    repo = _FakeRepo(SetKeyResult(KeyVerification(gcm_ok=True, vault_key=b"vault")))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.action_cancel()
        await wait_until(pilot, lambda: app.screen is not screen)
        assert app.dismissed == [False]
        assert repo.received_keys == []  # cancelled without ever verifying


async def test_on_input_submitted_via_a_real_enter_keypress_verifies() -> None:
    # Covers the on_input_submitted path that direct action_verify() calls skip.
    repo = _FakeRepo(SetKeyResult(KeyVerification(gcm_ok=True, vault_key=b"vault")))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        key_input = screen.query_one("#key-input", Input)
        await focus_widget(pilot, key_input)
        for char in "userKeyID@dGVzdA==":
            await pilot.press(char)
        await pilot.press("enter")
        await wait_until(pilot, lambda: repo.received_keys != [], timeout=SDK_TIMEOUT, interval=0.02)
        assert repo.received_keys == ["userKeyID@dGVzdA=="]


async def test_partial_reopen_failure_notifies_but_still_dismisses_with_true(monkeypatch: pytest.MonkeyPatch) -> None:
    good_verification = KeyVerification(gcm_ok=True, vault_key=b"vault")
    repo = _FakeRepo(SetKeyResult(good_verification, (ApmRepoError("boom"),)))
    app = _FakeApp(repo)
    notifications: list[tuple[str, str]] = []
    async with app.run_test() as pilot:
        real_notify = app.notify

        def _recording_notify(message: str, *args: object, severity: str = "information", **kwargs: Any) -> None:
            notifications.append((message, severity))
            real_notify(message, *args, severity=severity, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(app, "notify", _recording_notify)
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        await wait_until(pilot, lambda: app.screen is not screen, timeout=SDK_TIMEOUT, interval=0.02)
        assert app.dismissed == [True]
        assert repo.received_keys == ["userKeyID@dGVzdA=="]
    assert len(notifications) == 1
    message, severity = notifications[0]
    assert severity == "warning"
    assert "boom" in message


async def test_set_key_error_shows_in_the_status_line() -> None:
    repo = _FakeRepo(ApmRepoError("vault key not found"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        status = screen.query_one("#key-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=SDK_TIMEOUT, interval=0.02)
        assert "vault key not found" in str(status.render())
        assert isinstance(app.screen, KeyDialog)
