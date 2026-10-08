"""``Pilot`` tests for ``UnitScreen``'s ``children()`` pagination ("load
more" and the partial-load filter hint) against a fake provider that
slices by ``offset``/``limit``.

Every child here is a leaf, so the file table (not the folder tree) shows
the pages. ``action_load_more``/``action_filter`` target ``model.selected``,
which is root as soon as it loads, so no cursor movement is needed.
"""

from __future__ import annotations

import pytest
from textual.coordinate import Coordinate
from textual.widgets import DataTable

from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, wait_until
from synology_apm_repo.browser.core.unit.msg import GotoRequested
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    FakeApp,
    FakeRepo,
    PagedTreeProvider,
)

_TOTAL_CHILDREN = CHILDREN_PAGE_SIZE + 10


def _build_root_and_children() -> tuple[Node, dict[str, list[Node]]]:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False, details={"key": ()})
    children = [
        Node(ref=NodeRef("repo", ("item", f"{i:04d}")), name=f"item-{i:04d}", is_leaf=True)
        for i in range(_TOTAL_CHILDREN)
    ]
    return root, {str(root_ref): children}


async def test_first_expand_loads_only_one_page() -> None:
    root, children_by_ref = _build_root_and_children()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.05)
        assert len(screen.store.model.loaded[root.ref].children) == CHILDREN_PAGE_SIZE
        assert len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE


async def test_load_more_fetches_the_rest_and_then_reports_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    root, children_by_ref = _build_root_and_children()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE

        notifications: list[tuple[str, str]] = []
        monkeypatch.setattr(
            unit_screen,
            "notify",
            lambda message, *, severity="information", **kw: notifications.append((message, severity)),
        )

        await pilot.press("plus")
        await wait_until(
            pilot, lambda: len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN, timeout=SDK_TIMEOUT, interval=0.05
        )
        assert len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN
        assert any("loaded" in msg for msg, _sev in notifications)

        notifications.clear()
        await pilot.press("plus")
        # The warning is what says the keypress was handled; without it there
        # is nothing to assert about yet.
        await wait_until(
            pilot,
            lambda: notifications,
            timeout=SDK_TIMEOUT,
            interval=0.05,
            message="no notification for a second plus",
        )
        assert len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN  # nothing more to add
        assert any("already loaded" in msg for msg, sev in notifications if sev == "warning")


async def test_load_more_appends_in_place_without_resetting_scroll_position() -> None:
    """``FileTableView.render`` appends a new page in place rather than
    ``clear()``-and-rebuild, which would reset the scroll position."""
    root, children_by_ref = _build_root_and_children()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.05)

        table = unit_screen.file_table
        table.scroll_y = 5.0  # simulate the user having scrolled down the first page
        first_page = list(unit_screen._file_table._nodes)

        await pilot.press("plus")
        await wait_until(
            pilot, lambda: len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN, timeout=SDK_TIMEOUT, interval=0.05
        )

        assert unit_screen._file_table._nodes[: len(first_page)] == first_page
        assert table.scroll_y == 5.0, "load-more must not reset the table's own scroll position"


async def test_filter_on_a_partially_loaded_level_warns_it_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    root, children_by_ref = _build_root_and_children()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE  # not exhausted yet

        notifications: list[tuple[str, str]] = []
        monkeypatch.setattr(
            unit_screen,
            "notify",
            lambda message, *, severity="information", **kw: notifications.append((message, severity)),
        )

        await pilot.press("slash")
        await wait_until(
            pilot, lambda: any(sev == "warning" and str(CHILDREN_PAGE_SIZE) in msg for msg, sev in notifications)
        )


async def test_goto_ref_past_the_root_s_already_loaded_first_page_does_not_crash() -> None:
    """The goto loads root's complete child list over the partial first page the mount's auto-expand loaded."""
    root, children_by_ref = _build_root_and_children()
    provider = PagedTreeProvider(root, children_by_ref)
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=SDK_TIMEOUT, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE  # root "loaded", one page

        target = children_by_ref[str(root.ref)][-1]  # past the first page's own last item
        unit_screen.store.dispatch(GotoRequested(target=target.ref))
        await wait_until(
            pilot,
            lambda: target in unit_screen._file_table._nodes,
            timeout=SDK_TIMEOUT,
            interval=0.05,
        )
        row_index = unit_screen._file_table._nodes.index(target)
        table = unit_screen.query_one("#file-table", DataTable)
        assert table.cursor_coordinate == Coordinate(row_index, 0)
        assert len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN  # topped up to the full, exhaustive list
