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
from textual.widgets import Button, DataTable, Input, Static, Tree

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.widgets import filter_debounce, progress_hint
from synology_apm_repo.sdk.api import Catalog

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
def ui_timeout() -> float:
    """Poll-loop ceiling for a ``wait_until`` condition that settles with no
    SDK/Store dispatch in between (see ``tests/CLAUDE.md``'s "Driving a
    ``Pilot`` test" for the ``ui_timeout``/``sdk_timeout`` split)."""
    return 3.0


@pytest.fixture
def sdk_timeout() -> float:
    """Poll-loop ceiling for a ``wait_until`` condition gated on a real
    dispatch through the SDK/Store/provider layer (see ``tests/CLAUDE.md``'s
    "Driving a ``Pilot`` test" for the ``ui_timeout``/``sdk_timeout`` split)."""
    return 10.0


@pytest.fixture
def focus_widget(wait_until: Callable[..., Awaitable[None]], ui_timeout: float) -> Callable[..., Awaitable[None]]:
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
            timeout=ui_timeout,
            interval=0.02,
            message=f"{widget!r} never took focus",
        )

    return _focus


@pytest.fixture
def move_cursor_to(wait_until: Callable[..., Awaitable[None]], ui_timeout: float) -> Callable[..., Awaitable[None]]:
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
            timeout=ui_timeout,
            interval=0.02,
            message=f"cursor never landed on {node!r}",
        )

    return _move


@pytest.fixture
def open_browser_pilot(
    wait_until: Callable[..., Awaitable[None]], sdk_timeout: float
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
            timeout=sdk_timeout,
            interval=0.2,
            message="BrowseScreen never appeared",
        )
        # BrowseScreen itself never auto-expands any repository -- catalog
        # fetching is deferred to avoid a real network cost on every
        # repository the user may never look at -- do the obvious next
        # real-user action here instead, so every caller can still rely on
        # #col-catalogs being fully populated (cursor already parked on
        # the first connection) the moment this returns.
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
            timeout=sdk_timeout,
            interval=0.2,
            message="#col-catalogs never populated",
        )
        _ = tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
        tree.move_cursor(tree.root.children[0])
        await pilot.press("enter")  # type: ignore[attr-defined]
        await wait_until(
            pilot,
            lambda: tree.root.children[0].children,
            timeout=sdk_timeout,
            interval=0.2,
            message="first connection's children never populated",
        )

    return _open


@pytest.fixture
def wait_for_detail_content(
    wait_until: Callable[..., Awaitable[None]], sdk_timeout: float
) -> Callable[..., Awaitable[None]]:
    """``await wait_for_detail_content(pilot, screen, *, contains=None,
    timeout=sdk_timeout)`` waits for a ``UnitScreen``'s ``#detail`` pane to show
    real content, not ``DebouncedProgress``'s own transient
    ``"(⠋ loading)"`` cue (``screens/detail_pane.py``'s ``show_loading()``,
    always ending in the literal ``"loading)"``) — a bare
    ``text.strip() != ""`` check can pass on that cue alone for a
    content-only-preview node, whose header is empty (see
    ``core/unit/select.py``'s ``is_content_only_preview``).

    Pass ``contains=`` when the expected real text is already known — waits
    for that substring directly, which by construction can never match the
    loading cue. Omit it for real, unpredictable fixture data — waits for
    any non-empty text that isn't the loading cue itself instead."""

    async def _wait(
        pilot: object,
        screen: Any,
        *,
        contains: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 - a poll budget, not a cancellation scope; see wait_until's docstring
    ) -> None:
        # A fixture value can't be a plain parameter default, hence the
        # sentinel: `None` means "use sdk_timeout", not "no timeout".
        timeout = sdk_timeout if timeout is None else timeout

        def _text() -> str:
            return str(screen.query_one("#detail", Static).render())

        if contains is not None:
            await wait_until(
                pilot,
                lambda: contains in _text(),
                timeout=timeout,
                interval=0.02,
                message=f"{contains!r} never appeared in #detail",
            )
            return
        await wait_until(
            pilot,
            lambda: _text().strip() != "" and not _text().rstrip().endswith("loading)"),
            timeout=timeout,
            interval=0.03,
            message="#detail never showed real content past the loading cue",
        )

    return _wait


@pytest.fixture
def wait_for_filter_closed(
    wait_until: Callable[..., Awaitable[None]], ui_timeout: float
) -> Callable[..., Awaitable[None]]:
    """``await wait_for_filter_closed(pilot, screen, *, timeout=ui_timeout)``
    waits for the shared ``#filter-input`` widget
    (``screens/_shared/filter.py``'s ``close_filter_debounce``) to have
    actually lost its ``active`` CSS class after a close (``escape``/
    ``enter``) — every ``BrowseScreen``/``UnitScreen`` filter session closes
    through the identical ``FilterFieldController.close()`` mechanics, so
    this one wait covers all of them instead of each call site re-deriving
    its own fixed pause."""

    async def _wait(
        pilot: object,
        screen: Any,
        *,
        timeout: float | None = None,  # noqa: ASYNC109 - a poll budget, not a cancellation scope; see wait_until's docstring
    ) -> None:
        # A fixture value can't be a plain parameter default; see
        # wait_for_detail_content's identical sentinel.
        timeout = ui_timeout if timeout is None else timeout
        await wait_until(
            pilot,
            lambda: not screen.query_one("#filter-input", Input).has_class("active"),
            timeout=timeout,
            interval=0.02,
            message="#filter-input never closed",
        )

    return _wait


@pytest.fixture
def drill_to_unit_screen_via_fs_device(
    wait_until: Callable[..., Awaitable[None]],
    focus_widget: Callable[..., Awaitable[None]],
    ui_timeout: float,
    sdk_timeout: float,
) -> Callable[[ApmRepoBrowserApp, object], Awaitable[None]]:
    """``await drill_to_unit_screen_via_fs_device(app, pilot)`` — drills
    from an already-populated ``BrowseScreen`` (see ``open_browser_pilot``)
    into a specific, confirmed-good workload: the connection with
    ``connection_config_id`` 1's FS device, rather than whatever a
    cursor-default "press enter" lands on first. A VM device under this
    same connection has a real version whose ``target.db`` was never
    captured (a genuine gap in the real sample data itself, not a
    recording or anonymization bug), and which device sorts/groups first
    shifts every time display names are re-anonymized. FS content is
    structurally immune to that whole failure mode — it never goes through
    ``target.db`` at all — so it's the more durable choice, not just a
    currently-lucky pick. Shared by every
    ``tests/integration/browser/test_browser_pilot*.py`` file that needs
    this exact real workload."""

    async def _drill(app: ApmRepoBrowserApp, pilot: object) -> None:
        assert isinstance(app.screen, BrowseScreen), app.screen
        cat_tree = app.screen.query_one("#col-catalogs", Tree)
        repo_node = cat_tree.root.children[0]
        # connection_config_id 1 -- an internal catalog identifier, stable
        # and non-identifying (never touched by anonymization).
        connection_node = next(
            n
            for n in repo_node.children
            if n.data is not None
            and isinstance(n.data.payload, Catalog)
            and n.data.payload.connection.connection_config_id == 1
        )
        _ = cat_tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
        cat_tree.move_cursor(connection_node)
        await focus_widget(pilot, cat_tree)
        await pilot.press("enter")  # type: ignore[attr-defined]

        wl_tree = app.screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: wl_tree.root.children, timeout=sdk_timeout, interval=0.02)
        fs_group = next(n for n in wl_tree.root.children if str(n.label) == "FS")
        # Only the *first* group auto-expands (BrowseScreen._on_workloads_populated) --
        # "FS" isn't always that one, so its own children aren't visible/
        # selectable via move_cursor until expanded explicitly.
        fs_group.expand()
        # fs_group.children lands in the same real workloads dispatch as
        # wl_tree.root.children above -- SDK tier, not a UI-only wait.
        await wait_until(pilot, lambda: fs_group.children, timeout=sdk_timeout, interval=0.02)
        _ = wl_tree._tree_lines  # forces the line map to rebuild; see move_cursor_to's docstring
        wl_tree.move_cursor(fs_group.children[0])
        await focus_widget(pilot, wl_tree)
        await pilot.press("enter")  # type: ignore[attr-defined]

        versions_table = app.screen.query_one("#col-versions", DataTable)
        # row_count alone can still reflect a *previous* workload's rows,
        # left over until _render_versions() clears/repopulates the table for
        # this one -- on_data_table_row_selected's own bounds check reads
        # _visible_version_indices, which is what actually gates a real
        # selection, so wait for that specifically rather than row_count
        # (proven by direct capture: row_count already 2 from a stale render
        # while _visible_version_indices was still `[]` and workload_versions
        # was still `Loading`, silently no-opping a real keypress).
        await wait_until(
            pilot,
            lambda: versions_table.row_count and app.screen._visible_version_indices,
            timeout=sdk_timeout,
            interval=0.02,
        )
        await focus_widget(pilot, versions_table)
        await pilot.press("enter")  # type: ignore[attr-defined]
        await wait_until(pilot, lambda: isinstance(app.screen, UnitScreen), timeout=ui_timeout, interval=0.03)
        assert isinstance(app.screen, UnitScreen), app.screen

    return _drill


@pytest.fixture
def fast_browser_debounce(monkeypatch: pytest.MonkeyPatch) -> None:
    """Speeds up ``Debouncer``/``DebouncedProgress``'s real timers (0.3s each
    in production) to 0.02s for whichever test depends on this, by
    overriding the module constant each resolves at construction time --
    a live lookup, not a ``delay: float = _DEFAULT_DELAY`` bound default
    fixed at import time.

    Deliberately not ``autouse`` here: living in the shared root
    ``tests/conftest.py`` costs nothing for a test that never asks for it
    by name, the same reason ``open_browser_pilot``/``wait_for_detail_content``
    etc. already live here despite being browser-only. ``tests/unit/browser/
    conftest.py`` and ``tests/integration/browser/conftest.py`` each make
    this autouse for their own directory instead, by depending on it from
    an autouse fixture of their own; pytest resolves it by name up the
    conftest hierarchy, no import needed."""
    monkeypatch.setattr(filter_debounce, "_DEFAULT_DELAY", 0.02)
    monkeypatch.setattr(progress_hint, "_DEFAULT_DELAY", 0.02)
