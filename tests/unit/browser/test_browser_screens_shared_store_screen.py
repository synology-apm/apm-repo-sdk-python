"""Unit tests for ``screens/_shared/store_screen.py``'s ``StoreScreen``
teardown and ``NavigableScreen``'s breadcrumb loading suffix, on a minimal
screen mounted on ``ScreenHostApp``."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Static

from support.pilot import wait_until
from synology_apm_repo.browser.screens._shared import StoreScreen
from unit.browser.screen_host_fakes import ScreenHostApp


def _update(model: int, msg: int) -> tuple[int, tuple[str, ...]]:
    return model + msg, ()


class _CountingScreen(StoreScreen[int, int, str]):
    """Counts dispatched ints; hosts one worker that blocks until cancelled,
    and records what its teardown saw."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []
        self.worker_cancelled = False

    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")

    def on_mount(self) -> None:
        super().on_mount()
        self._update_breadcrumb_text("base")
        self._open_store(0, _update, lambda cmd: None)
        self.run_worker(self._block(), name="blocking")

    async def _block(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.worker_cancelled = True
            # A result published past cancellation must find the store closed.
            self.store.dispatch(100)
            self.events.append("worker exited")
            raise

    async def _after_store_closed(self) -> None:
        self.events.append("after store closed")


class _App(ScreenHostApp):
    def __init__(self) -> None:
        super().__init__()
        self.screen_under_test = _CountingScreen()

    def screens(self) -> Sequence[Screen[Any]]:
        return (self.screen_under_test,)


async def test_unmount_closes_the_store_then_drains_the_workers_then_runs_the_hook() -> None:
    app = _App()
    async with app.run_test() as pilot:
        screen = app.screen_under_test
        await wait_until(pilot, lambda: app.screen is screen)
        screen.store.dispatch(2)
        assert screen.store.model == 2

        app.pop_screen()
        await wait_until(pilot, lambda: "after store closed" in screen.events)

        assert screen.worker_cancelled
        assert screen.events == ["worker exited", "after store closed"]
        assert screen.store.model == 2  # the worker's late dispatch found the store closed


async def test_the_loading_indicator_follows_the_breadcrumb_until_cleared() -> None:
    app = _App()
    async with app.run_test() as pilot:
        screen = app.screen_under_test
        await wait_until(pilot, lambda: app.screen is screen)

        def breadcrumb() -> str:
            return str(screen.query_one("#breadcrumb", Static).render())

        screen._set_loading_indicator("spinning")
        assert "base  spinning" in breadcrumb()
        screen._update_breadcrumb_text("moved on")
        assert "moved on  spinning" in breadcrumb()  # a new breadcrumb keeps the running indicator
        screen._set_loading_indicator(None)
        assert "spinning" not in breadcrumb()
        assert "moved on" in breadcrumb()


async def test_unmount_without_an_opened_store_drains_and_skips_the_hook() -> None:
    """A screen whose on_mount failed before ``_open_store`` still tears
    down cleanly instead of raising AttributeError over the original error."""
    app = _App()
    async with app.run_test():
        never_opened = _CountingScreen()  # never mounted, so _open_store never ran

        await never_opened.on_unmount()

        assert never_opened.events == []  # the hook only follows an opened store
