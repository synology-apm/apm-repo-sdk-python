"""Fixtures shared across every distribution's tests (sdk/cli/browser) and
across both the unit/integration split -- narrower-scoped fixtures live in
``tests/unit/conftest.py``, ``tests/integration/conftest.py``, and
``tests/integration/cli/conftest.py`` instead."""

from __future__ import annotations

import os
import time
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Self

import pytest
from textual.widgets import Button, Input, Tree

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog

_FIXED_TZ = "Asia/Taipei"

#: Asia/Taipei has observed no DST since 1979, so a bare +08:00 renders every
#: backup timestamp exactly as the named zone does — and unlike
#: ``ZoneInfo(_FIXED_TZ)`` it needs no ``tzdata``, which Windows ships no system
#: copy of and this workspace only pulls in transitively.
_FIXED_TZ_OFFSET = timezone(timedelta(hours=8))


class _FixedLocalDatetime(datetime):
    """``datetime`` whose *bare* ``astimezone()`` resolves to ``_FIXED_TZ_OFFSET``
    rather than to whatever zone the running machine is in. Only used where
    ``time.tzset()`` is unavailable — see ``_fixed_timezone``."""

    def astimezone(self, tz: tzinfo | None = None) -> Self:
        return super().astimezone(_FIXED_TZ_OFFSET if tz is None else tz)


@pytest.fixture(scope="session", autouse=True)
def _fixed_timezone() -> Iterator[None]:
    """Pins the timezone behind local-time rendering to Asia/Taipei for the whole
    test run.

    ``catalog/version.py``'s ``_version_display_name()`` deliberately renders a
    backup version's epoch in the *local* timezone for display, so any
    replay fixture asserting on that rendered string is only reproducible
    when replayed in the same timezone it was recorded in. Pinning here —
    rather than leaving it to whatever timezone the running machine
    happens to be in — makes every ``tests/fixtures/*.json.gz`` replay
    assertion built around a rendered timestamp reproducible on any
    machine, including CI.

    Two mechanisms, because ``TZ``/``tzset`` is a POSIX interface: where
    ``time.tzset()`` exists, pinning the C library covers every local-time
    call in the process at once. Windows has no ``time.tzset()`` and its CRT
    ignores ``TZ``, so there the single call site above is pinned directly.
    """
    if hasattr(time, "tzset"):
        previous = os.environ.get("TZ")
        os.environ["TZ"] = _FIXED_TZ
        time.tzset()
        try:
            yield
        finally:
            if previous is None:
                del os.environ["TZ"]
            else:
                os.environ["TZ"] = previous
            time.tzset()
    else:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr("synology_apm_repo.sdk.catalog.version.datetime", _FixedLocalDatetime)
            yield


def pytest_configure(config: pytest.Config) -> None:
    """Strips ``FORCE_COLOR`` from the environment before test collection.

    Every CLI command module builds its own module-level ``rich.Console()``
    at *import* time (``cli/commands/verify.py``'s ``console = Console()``
    and its siblings) with the default ``color_system="auto"`` — resolved
    and cached once, in ``Console.__init__``, by checking
    ``Console.is_terminal`` right then, never re-checked afterward. Rich's
    ``is_terminal`` treats *any* non-empty ``FORCE_COLOR`` value — even
    ``"0"`` — as "force ANSI on" regardless of whether the underlying
    stream is a real terminal (https://force-color.org/, checked ahead of
    an ``isatty()`` probe), so an ambient ``FORCE_COLOR`` in the outer
    shell (set by some terminal/tooling this test run happens to be
    launched from) permanently bakes real ANSI escapes into these
    singletons the moment pytest's collection phase imports them —
    breaking every CLI test that asserts on exact rendered text (including
    parsing ``--json`` output as JSON) even though Click's
    ``CliRunner.invoke()`` itself captures stdout to a plain, non-tty
    stream specifically so those assertions can rely on plain text.
    A fixture runs too late to fix this (collection, and so every
    module-level ``Console()``, already happened before any fixture's own
    setup); ``pytest_configure`` is the one hook that runs before
    collection starts, while there's still time for a fresh ``Console()``
    to resolve ``color_system="auto"`` against a clean environment.
    """
    os.environ.pop("FORCE_COLOR", None)


@pytest.fixture
def wait_until() -> Callable[..., Awaitable[None]]:
    """``await wait_until(pilot, condition, *, timeout=1.0, interval=0.02,
    message="...")`` polls ``condition()`` via repeated
    ``pilot.pause(interval)`` calls until it returns truthy, raising
    ``TimeoutError(message)`` once ``timeout`` seconds have elapsed without
    that happening — replacing the ``for _ in range(N): await
    pilot.pause(x); if cond: break`` loop duplicated across
    ``tests/integration/browser/test_browser_pilot_*.py`` (and
    ``open_browser_pilot``'s own two loops below), which silently fell
    through to whatever assertion came next instead of failing with a
    clear cause."""

    async def _wait(
        pilot: object,
        condition: Callable[[], object],
        *,
        timeout: float = 1.0,  # noqa: ASYNC109 - a poll budget, not a cancellation scope; see docstring
        interval: float = 0.02,
        message: str = "condition not met before timeout",
    ) -> None:
        elapsed = 0.0
        while elapsed < timeout:
            await pilot.pause(interval)  # type: ignore[attr-defined]
            if condition():
                return
            elapsed += interval
        raise TimeoutError(message)

    return _wait


@pytest.fixture
def focus_widget(wait_until: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """``await focus_widget(pilot, widget)`` — focus it, then wait until it
    genuinely has focus.

    A key goes to whatever *actually* has focus, and ``focus()`` is a request
    the app grants a turn or two later. A test that focuses and presses in the
    same breath sends its key to the previous holder instead, which looks like
    the action silently doing nothing (see ``tests/CLAUDE.md``'s "Driving a
    ``Pilot`` test").
    """

    async def _focus(pilot: object, widget: Any) -> None:
        widget.focus()
        await wait_until(
            pilot,
            lambda: widget.has_focus,
            timeout=0.6,
            interval=0.02,
            message=f"{widget!r} never took focus",
        )

    return _focus


@pytest.fixture
def move_cursor_to(wait_until: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """``await move_cursor_to(pilot, tree, node)`` — move a ``Tree``'s cursor
    onto ``node`` and wait until it is actually there.

    Reading ``_tree_lines`` first is load-bearing, not defensive:
    ``move_cursor()`` resolves the node through the tree's line map, so against
    a stale one it simply does nothing and the cursor stays where it was. Every
    caller needs all three steps, so they live together here rather than being
    re-assembled (and half-remembered) per call site.
    """

    async def _move(pilot: object, tree: Any, node: Any) -> None:
        _ = tree._tree_lines  # forces the line map to rebuild; see docstring
        tree.move_cursor(node)
        await wait_until(
            pilot,
            lambda: tree.cursor_node is node,
            timeout=0.6,
            interval=0.02,
            message=f"cursor never landed on {node!r}",
        )

    return _move


@pytest.fixture
def open_browser_pilot(
    wait_until: Callable[..., Awaitable[None]],
) -> Callable[[ApmRepoBrowserApp, object, Path], Awaitable[None]]:
    """``await open_browser_pilot(app, pilot, path)`` — drives the
    auto-opened ``ConnectDialog`` to open a real local repository at
    ``path`` and waits for ``BrowseScreen``'s ``#col-catalogs`` to
    be fully populated (cursor already parked on the first connection),
    shared by the 6 ``tests/integration/browser/test_browser_pilot*.py``
    files. Not
    a context manager: unlike ``open_repo``, there is nothing here
    to close —
    ``app``/``pilot``'s own lifecycle is already owned by each test's own
    ``async with app.run_test() as pilot:`` block."""

    async def _open(app: ApmRepoBrowserApp, pilot: object, path: Path) -> None:
        # ConnectDialog is auto-opened on top of BrowseScreen the instant the
        # app mounts (see app.py's own on_mount) — this drives its "local"
        # backend (the default), since picking a source is entirely
        # ConnectDialog's job.
        assert isinstance(app.screen, ConnectDialog), app.screen
        dialog = app.screen
        dialog.query_one("#connect-local-path", Input).value = str(path)
        dialog.query_one("#connect-submit", Button).press()
        await wait_until(
            pilot,
            lambda: isinstance(app.screen, BrowseScreen),
            timeout=10.0,
            interval=0.2,
            message="BrowseScreen never appeared",
        )
        # BrowseScreen itself never auto-expands any repository (see
        # browse_screen.py's own _add_repo docstring) — do the obvious next
        # real-user action here instead, so every caller can still rely on
        # #col-catalogs being fully populated (cursor already parked on the
        # first connection) the moment this returns.
        tree = app.screen.query_one("#col-catalogs", Tree)
        tree.focus()
        # BrowseScreen's own transition (waited for above) only means the
        # screen itself has mounted, not that the background worker
        # populating this tree has finished -- wait for the real condition
        # before indexing into it, rather than assuming the two always land
        # together (observed to race under real scheduling contention).
        await wait_until(
            pilot,
            lambda: tree.root.children,
            timeout=10.0,
            interval=0.2,
            message="#col-catalogs never populated",
        )
        tree.move_cursor(tree.root.children[0])
        await pilot.press("enter")  # type: ignore[attr-defined]
        await wait_until(
            pilot,
            lambda: tree.root.children[0].children,
            timeout=10.0,
            interval=0.2,
            message="first connection's children never populated",
        )

    return _open
