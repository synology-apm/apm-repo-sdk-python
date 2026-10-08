"""Waiting helpers for Textual ``Pilot`` tests (see ``tests/CLAUDE.md``'s
"Driving a ``Pilot`` test"): wait for the state a step needs, never for a
duration.

``UI_TIMEOUT`` bounds a condition that settles with no SDK/Store dispatch in
between (``push_screen``, a widget attribute); ``SDK_TIMEOUT`` one gated on a
dispatch through the SDK/Store/provider layer, even a fast one. A wait
returns the instant its condition holds, so neither budget costs anything on
a healthy run.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from textual.css.query import NoMatches
from textual.dom import DOMNode
from textual.pilot import Pilot
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.widgets import filter_debounce, progress_hint

UI_TIMEOUT = 3.0
SDK_TIMEOUT = 10.0

#: ``App.run_test(size=...)`` for a test that drives the whole app: room for
#: every dialog and ``BrowseScreen``'s three columns.
RUN_TEST_SIZE = (140, 45)


async def wait_until(
    pilot: Pilot[Any],
    condition: Callable[[], object],
    *,
    timeout: float = UI_TIMEOUT,  # noqa: ASYNC109 - a poll budget, not a cancellation scope
    interval: float = 0.02,
    message: str = "condition not met before timeout",
) -> None:
    """Poll ``condition()`` between ``pilot.pause(interval)`` turns until it is
    truthy; raise ``TimeoutError(message)`` once ``timeout`` seconds' worth of
    intervals have passed without that.

    A ``NoMatches`` from ``condition`` counts as "not yet": a just-pushed
    screen mounts its children a turn or more later.
    """
    elapsed = 0.0
    while elapsed < timeout:
        await pilot.pause(interval)
        try:
            met = condition()
        except NoMatches:
            met = False
        if met:
            return
        elapsed += interval
    raise TimeoutError(message)


async def settle(pilot: Pilot[Any]) -> None:
    """Let the messages already queued, and those they post in turn, run:
    the wait before asserting that something never happens."""
    await pilot.pause()
    await pilot.pause()


async def wait_for_workers(
    pilot: Pilot[Any],
    *,
    group: str | None = None,
    node: DOMNode | None = None,
    timeout: float = SDK_TIMEOUT,  # noqa: ASYNC109
) -> None:
    """Wait until every worker of the app (only ``group``'s and/or ``node``'s,
    when given) has finished: the point after which a fetch it ran can no
    longer change what is on screen, so a test asserting that it changed
    nothing can assert then."""

    def pending() -> list[str]:
        return [
            w.name or repr(w)
            for w in pilot.app.workers
            if not w.is_finished and (group is None or w.group == group) and (node is None or w.node is node)
        ]

    await wait_until(pilot, lambda: not pending(), timeout=timeout, message="a worker never finished")


def count_progress_ticks(monkeypatch: pytest.MonkeyPatch) -> Callable[[], int]:
    """Count every ``DebouncedProgress`` frame tick from now on (patch before
    the progress starts), so a test asserting that a tick leaves the screen
    unchanged can wait for one to have run; returns the running count."""
    ticks = 0
    original = progress_hint.DebouncedProgress._tick

    def counting_tick(self: progress_hint.DebouncedProgress) -> None:
        nonlocal ticks
        original(self)
        ticks += 1

    monkeypatch.setattr(progress_hint.DebouncedProgress, "_tick", counting_tick)
    return lambda: ticks


async def wait_for_screen[ScreenT: Screen[Any]](
    pilot: Pilot[Any],
    screen_type: type[ScreenT],
    *,
    timeout: float = UI_TIMEOUT,  # noqa: ASYNC109
) -> ScreenT:
    """Wait until the active screen is a mounted ``screen_type`` and return it."""
    app = pilot.app
    await wait_until(
        pilot,
        lambda: isinstance(app.screen, screen_type) and app.screen.is_mounted,
        timeout=timeout,
        message=f"{screen_type.__name__} never became the active screen",
    )
    screen = app.screen
    assert isinstance(screen, screen_type)
    return screen


async def focus_widget(pilot: Pilot[Any], widget: Widget, *, timeout: float = UI_TIMEOUT) -> None:  # noqa: ASYNC109
    """Focus ``widget`` and wait until it has focus: ``focus()`` is a request
    the app grants a turn or two later, and a key pressed before that goes to
    the previous holder."""
    widget.focus()
    await wait_until(pilot, lambda: widget.has_focus, timeout=timeout, message=f"{widget!r} never took focus")


async def move_cursor_to(
    pilot: Pilot[Any],
    tree: Tree[Any],
    node: TreeNode[Any],
    *,
    timeout: float = UI_TIMEOUT,  # noqa: ASYNC109
) -> None:
    """Move ``tree``'s cursor onto ``node`` and wait until it is there.

    ``move_cursor()`` resolves the node through the tree's line map and does
    nothing against a stale one, so the map is rebuilt first.
    """
    _ = tree._tree_lines
    tree.move_cursor(node)
    await wait_until(
        pilot, lambda: tree.cursor_node is node, timeout=timeout, message=f"cursor never landed on {node!r}"
    )


async def wait_for_detail_content(
    pilot: Pilot[Any],
    screen: Widget,
    *,
    contains: str | None = None,
    timeout: float = SDK_TIMEOUT,  # noqa: ASYNC109
) -> None:
    """Wait for a ``UnitScreen``'s ``#detail`` pane to show real content rather
    than the transient ``"(⠋ loading)"`` cue (``DetailPane.show_loading``,
    driven by ``DebouncedProgress``).

    With ``contains``, wait for that substring (which can never match the cue);
    without it, for any non-empty text that isn't the cue.
    """

    def text() -> str:
        return str(screen.query_one("#detail", Static).render())

    if contains is not None:
        await wait_until(
            pilot, lambda: contains in text(), timeout=timeout, message=f"{contains!r} never appeared in #detail"
        )
        return
    await wait_until(
        pilot,
        lambda: text().strip() != "" and not text().rstrip().endswith("loading)"),
        timeout=timeout,
        interval=0.03,
        message="#detail never showed real content past the loading cue",
    )


async def wait_for_filter_closed(pilot: Pilot[Any], screen: Widget, *, timeout: float = UI_TIMEOUT) -> None:  # noqa: ASYNC109
    """Wait for the shared ``#filter-input`` to lose its ``active`` class after
    a close (``escape``/``enter``); every ``BrowseScreen``/``UnitScreen``
    filter closes through the same ``FilterFieldController.close()``."""
    await wait_until(
        pilot,
        lambda: not screen.query_one("#filter-input", Input).has_class("active"),
        timeout=timeout,
        message="#filter-input never closed",
    )


@pytest.fixture(autouse=True)
def fast_browser_debounce(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten ``Debouncer``/``DebouncedProgress``'s 0.3 s timers to 0.02 s,
    for every test under a ``conftest.py`` that imports this fixture (both
    ``browser/`` directories)."""
    monkeypatch.setattr(filter_debounce, "_DELAY", 0.02)
    monkeypatch.setattr(progress_hint, "_DELAY", 0.02)
