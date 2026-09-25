"""Unit tests for ``KeyDialog`` — driven through a real Textual ``Pilot``
against a fake ``Repository`` whose ``set_key()`` is
fully controllable, so this needs no real repository/key material and lives in
``tests/unit/``. Real end-to-end coverage (the dialog auto-opened by
``BrowseScreen`` against a real encrypted sample) lives in
``tests/integration/browser/test_browser_pilot.py``/
``test_browser_pilot_labels.py``."""

from __future__ import annotations

from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import Input, Static

from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.sdk.api import KeyVerification
from synology_apm_repo.sdk.errors import ApmRepoError


class _FakeRepo:
    def __init__(
        self,
        result: KeyVerification | ApmRepoError | ExceptionGroup[ApmRepoError],
        *,
        key_verification: KeyVerification | None = None,
    ) -> None:
        self._result = result
        self.received_keys: list[str] = []
        # Mirrors Repository.set_key()'s own real behavior: key_verification
        # (and key_status) are committed *before* an ExceptionGroup is
        # raised for a partial reopen/close failure, so a caller sees the
        # correct key_status even alongside a partial-failure report. Only
        # meaningful for the ExceptionGroup scenario; unused for the plain
        # KeyVerification/ApmRepoError ones above.
        self.key_verification = key_verification

    async def set_key(self, key_string: str) -> KeyVerification:
        self.received_keys.append(key_string)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class _FakeApp(App[None]):
    def __init__(self, repo: _FakeRepo) -> None:
        super().__init__()
        self._repo = repo

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(KeyDialog(self._repo))  # type: ignore[arg-type]


async def test_action_verify_with_a_typed_but_unsubmitted_key(wait_until: Any, sdk_timeout: float) -> None:
    """``on_input_submitted`` (Enter inside the Input) already covers
    submission — this is ``action_verify``'s own binding (also reachable
    via Enter, but dispatched independently of the Input's own
    ``Submitted`` message when a screen-level binding wins), called
    directly here to exercise its own body rather than relying on which
    of the two Textual dispatches the real keypress happens to take."""
    repo = _FakeRepo(KeyVerification(gcm_ok=True, vault_key=b"vault"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        await wait_until(pilot, lambda: repo.received_keys != [], timeout=sdk_timeout, interval=0.02)
        assert repo.received_keys == ["userKeyID@dGVzdA=="]


async def test_action_verify_with_an_empty_field_does_nothing() -> None:
    repo = _FakeRepo(KeyVerification(gcm_ok=True, vault_key=b"vault"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.action_verify()
        await pilot.pause()
        assert repo.received_keys == []


async def test_wrong_key_shows_invalid_and_does_not_dismiss(wait_until: Any, sdk_timeout: float) -> None:
    repo = _FakeRepo(KeyVerification(gcm_ok=False, vault_key=None))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@wrong=="
        screen.action_verify()
        status = screen.query_one("#key-status", Static)
        await wait_until(pilot, lambda: "invalid" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "gcm_ok=False" in str(status.render())
        assert isinstance(app.screen, KeyDialog)  # still open — not dismissed


async def test_action_cancel_dismisses_with_false() -> None:
    # Calls action_cancel() directly -- no test in this file drives it
    # through a real Esc keypress.
    repo = _FakeRepo(KeyVerification(gcm_ok=True, vault_key=b"vault"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.action_cancel()
        await pilot.pause()
        assert app.screen is not screen
        assert repo.received_keys == []  # cancelled without ever verifying


async def test_on_input_submitted_via_a_real_enter_keypress_verifies(wait_until: Any, sdk_timeout: float) -> None:
    # Distinct from test_action_verify_with_a_typed_but_unsubmitted_key
    # above (which calls action_verify() directly) -- this drives the
    # dialog through a real Enter keypress inside the focused Input,
    # dispatching on_input_submitted instead.
    repo = _FakeRepo(KeyVerification(gcm_ok=True, vault_key=b"vault"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        key_input = screen.query_one("#key-input", Input)
        key_input.focus()
        await pilot.pause()
        for char in "userKeyID@dGVzdA==":
            await pilot.press(char)
        await pilot.press("enter")
        await wait_until(pilot, lambda: repo.received_keys != [], timeout=sdk_timeout, interval=0.02)
        assert repo.received_keys == ["userKeyID@dGVzdA=="]


async def test_partial_reopen_failure_notifies_but_still_dismisses_with_true(
    wait_until: Any, sdk_timeout: float
) -> None:
    """``Repository.set_key()`` raises ``ExceptionGroup`` (not
    ``ApmRepoError``) when the key itself verifies fine but reopening/
    closing one already-opened sibling catalog independently fails —
    ``key_status`` is already ``VERIFIED`` at that point, so the dialog
    must still dismiss with ``True`` rather than looping the user back to
    re-enter a key that already worked."""
    good_verification = KeyVerification(gcm_ok=True, vault_key=b"vault")
    repo = _FakeRepo(ExceptionGroup("partial failure", [ApmRepoError("boom")]), key_verification=good_verification)
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        await wait_until(pilot, lambda: app.screen is not screen, timeout=sdk_timeout, interval=0.02)
        assert repo.received_keys == ["userKeyID@dGVzdA=="]


async def test_set_key_error_shows_in_the_status_line(wait_until: Any, sdk_timeout: float) -> None:
    repo = _FakeRepo(ApmRepoError("vault key not found"))
    app = _FakeApp(repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, KeyDialog)
        screen.query_one("#key-input", Input).value = "userKeyID@dGVzdA=="
        screen.action_verify()
        status = screen.query_one("#key-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=sdk_timeout, interval=0.02)
        assert "vault key not found" in str(status.render())
        assert isinstance(app.screen, KeyDialog)


__all__: list[str] = []
