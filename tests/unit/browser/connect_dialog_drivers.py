"""``ConnectDialog`` drives shared by the ``test_browser_screens_connect_dialog``,
``test_browser_screens_connect_dialog_remote_backends`` and
``test_browser_screens_profile_manager`` tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from textual.pilot import Pilot
from textual.widgets import Static, Tabs

from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, wait_for_screen, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog


@asynccontextmanager
async def open_connect_dialog() -> AsyncIterator[tuple[ApmRepoBrowserApp, Pilot[None], ConnectDialog]]:
    """Run a fresh ``ApmRepoBrowserApp`` and yield ``(app, pilot, dialog)``
    once the ``ConnectDialog`` it opens on launch is the active, mounted screen."""
    app = ApmRepoBrowserApp()
    async with app.run_test(size=RUN_TEST_SIZE) as pilot:
        yield app, pilot, await wait_for_screen(pilot, ConnectDialog)


async def wait_for_status_containing(
    pilot: Pilot[Any],
    dialog: ConnectDialog,
    needle: str,
    *,
    timeout: float = SDK_TIMEOUT,  # noqa: ASYNC109 - a poll budget, not a cancellation scope
) -> str:
    """Poll ``#connect-status`` until its text contains ``needle``
    (case-insensitive) and return that text."""
    status = ""

    def matches() -> bool:
        nonlocal status
        status = str(dialog.query_one("#connect-status", Static).render())
        return needle in status.lower()

    await wait_until(pilot, matches, timeout=timeout, interval=0.05)
    return status


async def activate_backend_and_settle(dialog: ConnectDialog, backend: str, pilot: Pilot[Any]) -> None:
    """Switch the backend ``Tabs`` strip to ``backend`` and wait for the pane
    swap, which ``ConnectDialog`` handles a later event-loop turn."""
    dialog.query_one("#connect-backend-tabs", Tabs).active = backend
    await wait_until(
        pilot,
        lambda: dialog.query_one(f"#connect-{backend}-fields").has_class("active"),
        message=f"{backend} pane never became active",
    )
