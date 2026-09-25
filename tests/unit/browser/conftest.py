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
    every test in this directory. That fixture itself lives in the shared
    root conftest, not here, because it costs nothing for a test that
    never asks for it by name (the same reason ``open_browser_pilot``/
    ``wait_for_detail_content`` live there despite being browser-only);
    only its *autouse* activation is scoped per directory."""


@pytest.fixture
def wait_for_status_containing(
    wait_until: Callable[..., Awaitable[None]], sdk_timeout: float
) -> Callable[..., Awaitable[str]]:
    """``await wait_for_status_containing(pilot, dialog, needle, *,
    timeout=sdk_timeout)`` polls ``#connect-status`` until its rendered text
    contains ``needle`` (case-insensitive), returning that final text —
    shared by every ``ConnectDialog``-driving test under this directory.
    Defaults to ``sdk_timeout`` since callers span both pure
    construction-time validation and a real (faked) dispatch through
    ``Session.discover_remote``/``list_remote_items`` — the wider ceiling
    costs nothing once the condition is already true, same reasoning as
    ``tests/conftest.py``'s ``wait_for_detail_content``."""

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
    """``await activate_backend_and_settle(dialog, backend, pilot)`` drives
    the backend ``Tabs`` strip the same way a real activation does (setting
    ``active`` posts the same ``Tabs.TabActivated`` message
    ``ConnectDialog.on_tabs_tab_activated`` reacts to) and waits for the
    resulting pane swap to actually land — shared by every
    ``ConnectDialog``-driving test under this directory. Setting
    ``Tabs.active`` only posts ``TabActivated``; the pane swap happens
    when ``ConnectDialog`` handles it, a later event-loop turn — waiting
    on the pane carrying the ``active`` class beats guessing a duration
    before touching that backend's widgets."""

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
