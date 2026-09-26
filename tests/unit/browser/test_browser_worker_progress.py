"""Unit tests for ``widgets/worker_progress.py``'s ``work``/
``run_worker_with_progress``/``run_worker_no_progress`` — these make
showing a ``DebouncedProgress`` while a worker runs the default,
opt-out behavior instead of something a call site has to remember to
add. Driven against a bare ``App`` (any ``Widget``/``Screen``/
``App`` works as the timer host ``DebouncedProgress`` needs). Every
debounce-timing assertion uses a short, explicit ``delay=`` plus
``wait_until`` (poll for the state, never a bare sleep), not the real
300ms default.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Static

from synology_apm_repo.browser.widgets.progress_hint import StaticTextSink
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress, run_worker_with_progress, work

_SHORT_DELAY = 0.02


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        yield Static("", id="status")


class _RecordingSink:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def show(self, frame: str) -> None:
        self.calls.append(frame)

    def hide(self) -> None:
        self.calls.append("hide")


async def test_work_bare_shows_the_default_breadcrumb_sink_after_the_debounce_delay(
    wait_until: Any, ui_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        @work(sink=lambda self: recorded, delay=_SHORT_DELAY)
        async def _slow(self: App[None]) -> None:
            await asyncio.Event().wait()  # never resolves on its own -- cancelled below

        worker = _slow(app)
        await wait_until(pilot, lambda: recorded.calls, timeout=ui_timeout, message="sink was never shown")
        worker.cancel()


async def test_work_busy_false_never_shows_anything_even_past_the_delay(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        @work(busy=False, sink=lambda self: recorded, delay=_SHORT_DELAY)
        async def _fast(self: App[None]) -> None:
            await asyncio.Event().wait()

        worker = _fast(app)
        # A generous wait, well past _SHORT_DELAY, proving an absence --
        # not a precise timing assertion, so this isn't the flaky kind of
        # sleep tests/CLAUDE.md warns against.
        await pilot.pause(_SHORT_DELAY * 10)
        assert recorded.calls == []
        worker.cancel()


async def test_work_uses_the_given_sink_and_hides_it_once_the_worker_finishes(
    wait_until: Any, ui_timeout: float
) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        release = asyncio.Event()

        # A real asyncio.sleep() here would race the debounce delay's own
        # timer, risking "Loading" flashing and reverting between two
        # wait_until polls. Gating completion on an Event this test
        # controls keeps "Loading" shown until released instead.
        @work(sink=lambda self: StaticTextSink(self, "#status", base=lambda: "base"), delay=_SHORT_DELAY)
        async def _slow(self: App[None]) -> None:
            await release.wait()

        _slow(app)
        await wait_until(
            pilot,
            lambda: "Loading" in str(app.query_one("#status", Static).render()),
            timeout=ui_timeout,
            message="the sink never showed",
        )
        release.set()
        await wait_until(
            pilot,
            lambda: str(app.query_one("#status", Static).render()) == "base",
            timeout=ui_timeout,
            message="the sink never hid once the worker finished",
        )


def test_work_rejects_thread_true_at_decoration_time() -> None:
    with pytest.raises(ValueError, match="thread=True"):

        @work(thread=True)
        async def _threaded(self: App[None]) -> None:
            pass


async def test_work_forwards_kwargs_to_textual_work() -> None:
    app = _FakeApp()
    async with app.run_test():

        @work(group="my-group", name="my-worker", busy=False)
        async def _slow(self: App[None]) -> None:
            await asyncio.Event().wait()

        worker = _slow(app)
        assert worker.group == "my-group"
        assert worker.name == "my-worker"
        worker.cancel()


async def test_run_worker_with_progress_shows_the_given_sink(wait_until: Any, ui_timeout: float) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        recorded = _RecordingSink()

        async def _slow() -> None:
            await asyncio.Event().wait()

        worker = run_worker_with_progress(app, _slow, sink=recorded, delay=_SHORT_DELAY)
        await wait_until(pilot, lambda: recorded.calls, timeout=ui_timeout, message="sink was never shown")
        worker.cancel()


async def test_run_worker_no_progress_never_shows_anything() -> None:
    app = _FakeApp()
    async with app.run_test():
        ran = False

        async def _fast() -> None:
            nonlocal ran
            ran = True

        worker = run_worker_no_progress(app, _fast)
        await worker.wait()
        assert ran
