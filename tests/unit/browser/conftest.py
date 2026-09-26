"""Fixtures scoped to ``tests/unit/browser/`` only -- a fixture belonging
to one slice of the sdk/cli/browser split, not the whole suite, stays out
of root ``tests/conftest.py``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest
from textual.widgets import Static, Tabs

from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog


@pytest.fixture(autouse=True)
def _apply_fast_browser_debounce(fast_browser_debounce: None) -> None:
    """Auto-activates ``tests/conftest.py``'s ``fast_browser_debounce`` for
    every test in this directory."""


@pytest.fixture
def wait_for_status_containing(
    wait_until: Callable[..., Awaitable[None]], sdk_timeout: float
) -> Callable[..., Awaitable[str]]:
    """``await wait_for_status_containing(pilot, dialog, needle, *,
    timeout=sdk_timeout)`` polls ``#connect-status`` until its rendered text
    contains ``needle`` (case-insensitive), returning that final text.
    Defaults to ``sdk_timeout`` since a caller may be waiting on a real
    (faked) dispatch through ``Session.discover_remote``/``list_remote_items``,
    not just UI state."""

    async def _wait(
        pilot: object,
        dialog: ConnectDialog,
        needle: str,
        *,
        timeout: float | None = None,  # noqa: ASYNC109 - a poll budget, not a cancellation scope; see wait_until's docstring
    ) -> str:
        # A fixture value can't be a plain parameter default; see
        # tests/conftest.py's wait_for_detail_content's identical sentinel.
        timeout = sdk_timeout if timeout is None else timeout
        status = ""

        def _matches() -> bool:
            nonlocal status
            status = str(dialog.query_one("#connect-status", Static).render())
            return needle in status.lower()

        await wait_until(pilot, _matches, timeout=timeout, interval=0.05)
        return status

    return _wait


@pytest.fixture
def activate_backend_and_settle(
    wait_until: Callable[..., Awaitable[None]], ui_timeout: float
) -> Callable[[ConnectDialog, str, object], Awaitable[None]]:
    """``await activate_backend_and_settle(dialog, backend, pilot)`` sets
    the backend ``Tabs`` strip's ``active`` value and waits for the
    resulting pane swap to land — setting ``active`` only posts
    ``TabActivated``; ``ConnectDialog`` handles the swap a later
    event-loop turn."""

    async def _activate(dialog: ConnectDialog, backend: str, pilot: object) -> None:
        dialog.query_one("#connect-backend-tabs", Tabs).active = backend
        await wait_until(
            pilot,
            lambda: dialog.query_one(f"#connect-{backend}-fields").has_class("active"),
            timeout=ui_timeout,
            interval=0.02,
            message=f"{backend} pane never became active",
        )

    return _activate
