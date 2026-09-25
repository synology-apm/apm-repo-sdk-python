"""Unit tests for ``UnitScreen``'s ``children()`` pagination — driven
through a real Textual ``Pilot``
(``app.run_test()``), against a fake, sample-independent provider that
actually slices by ``offset``/``limit`` (unlike ``tests/unit/
test_cli_browse.py``'s own ``_FakeProvider``, which ignores them —
this one exists specifically to exercise the "load more" keybinding and
the partial-load filter hint). Real end-to-end coverage against real recorded sample data lives in
``tests/integration/browser/test_browser_pilot_hex_filter_refresh.py``'s own ``/``
filter test.

Every child fixture here is an ordinary leaf, so it's the *file table* --
never the folder tree, which excludes leaves entirely -- that renders the
paginated results. ``action_load_more``/``action_filter`` resolve their
target via ``model.selected`` directly (not a tree cursor), so no cursor
navigation is needed before pressing ``+``/``/``: root is already
``model.selected`` the moment it loads."""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.widgets import DataTable

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.core.unit.update import CHILDREN_PAGE_SIZE
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.sdk.api import Repository, Session, Version
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

_TOTAL_CHILDREN = CHILDREN_PAGE_SIZE + 10


def _version() -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type="VM",
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class _PaginatingFakeProvider:
    """Unlike ``test_cli_browse.py``'s own ``_FakeProvider`` (which
    accepts ``offset``/``limit`` only to satisfy the ``UnitProvider``
    signature, then ignores them), this one genuinely slices — the same
    real behavior every SDK provider now has, so ``UnitScreen``'s own
    "load more" logic has something real to page through."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]]) -> None:
        self._root = root
        self._children_by_ref = children_by_ref

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        all_children = self._children_by_ref.get(str(node.ref), [])
        stop = offset + limit if limit is not None else None
        return all_children[offset:stop]

    async def unit(self, node: Node) -> Node:
        return node


class _FakeCatalog:
    def __init__(self, provider: _PaginatingFakeProvider) -> None:
        self._provider = provider

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _PaginatingFakeProvider:
        return self._provider


class _FakeApp(App[None]):
    """A minimal host for ``UnitScreen`` — deliberately not
    ``ApmRepoBrowserApp`` itself, which auto-pushes ``BrowseScreen`` +
    ``ConnectDialog`` on mount and owns a real ``Session``; ``UnitScreen``
    only ever reads ``app_state.repo_handle``/``app_state.verbose``/
    ``app_state.jobs`` (``NavigableScreen.app_state`` is just
    ``self.app``, duck-typed, no runtime type check), so any ``App``
    subclass exposing those attributes satisfies it. ``self.repo_handle``
    only needs to resolve to something non-``None`` here — none of this
    file's tests exercise ``action_refresh``'s
    ``invalidate_directory_cache()`` call, the one thing ``_load_root``
    itself still reads off it."""

    def __init__(self, version: Version, catalog: _FakeCatalog) -> None:
        super().__init__()
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.resources = ResourceTable(cast(Session, object()))
        self.repo_handle: RepoHandle | None = self.resources.put_repo(cast(Repository, object()))
        self._version = version
        self._catalog = catalog

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(UnitScreen(self._catalog, self._version))  # type: ignore[arg-type]


def _build_root_and_children() -> tuple[Node, dict[str, list[Node]]]:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False, attrs={"key": ()})
    children = [
        Node(ref=NodeRef("repo", ("item", f"{i:04d}")), name=f"item-{i:04d}", is_leaf=True)
        for i in range(_TOTAL_CHILDREN)
    ]
    return root, {str(root_ref): children}


async def test_first_expand_loads_only_one_page(wait_until: Any, sdk_timeout: float) -> None:
    root, children_by_ref = _build_root_and_children()
    provider = _PaginatingFakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in screen.store.model.loaded, timeout=sdk_timeout, interval=0.05)
        assert len(screen.store.model.loaded[root.ref].children) == CHILDREN_PAGE_SIZE
        assert len(screen._file_table._nodes) == CHILDREN_PAGE_SIZE


async def test_load_more_fetches_the_rest_and_then_reports_exhausted(wait_until: Any, sdk_timeout: float) -> None:
    root, children_by_ref = _build_root_and_children()
    provider = _PaginatingFakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE

        notifications: list[tuple[str, str]] = []
        unit_screen.notify = lambda message, *, severity="information", **kw: notifications.append(  # type: ignore[method-assign]
            (message, severity)
        )

        # model.selected is already root.ref the moment it loads -- no
        # cursor navigation needed before pressing "+".
        await pilot.press("plus")
        await wait_until(
            pilot, lambda: len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN, timeout=sdk_timeout, interval=0.05
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
            timeout=sdk_timeout,
            interval=0.05,
            message="no notification for a second plus",
        )
        assert len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN  # nothing more to add
        assert any("already loaded" in msg for msg, sev in notifications if sev == "warning")


async def test_load_more_appends_in_place_without_resetting_scroll_position(
    wait_until: Any, sdk_timeout: float
) -> None:
    """The new page is a pure append onto what's already on screen --
    ``FileTableView.render`` must extend the table in place rather than
    ``clear()``+rebuild every prior row, since ``clear()`` unconditionally
    resets scroll position too. Without this, pressing ``+`` on a large,
    already-scrolled-down folder would snap the view back to the top on
    every further page."""
    root, children_by_ref = _build_root_and_children()
    provider = _PaginatingFakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.05)

        table = unit_screen.file_table
        table.scroll_y = 5.0  # simulate the user having scrolled down the first page
        first_page = list(unit_screen._file_table._nodes)

        await pilot.press("plus")
        await wait_until(
            pilot, lambda: len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN, timeout=sdk_timeout, interval=0.05
        )

        assert unit_screen._file_table._nodes[: len(first_page)] == first_page
        assert table.scroll_y == 5.0, "load-more must not reset the table's own scroll position"


async def test_filter_on_a_partially_loaded_level_warns_it_is_incomplete(wait_until: Any, sdk_timeout: float) -> None:
    root, children_by_ref = _build_root_and_children()
    provider = _PaginatingFakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE  # not exhausted yet

        notifications: list[tuple[str, str]] = []
        unit_screen.notify = lambda message, *, severity="information", **kw: notifications.append(  # type: ignore[method-assign]
            (message, severity)
        )

        await pilot.press("slash")
        await pilot.pause(0.05)
        assert any(sev == "warning" and str(CHILDREN_PAGE_SIZE) in msg for msg, sev in notifications)


async def test_goto_ref_past_the_root_s_already_loaded_first_page_does_not_crash(
    wait_until: Any, sdk_timeout: float
) -> None:
    """The root gets marked loaded by the ordinary auto-expand on mount,
    but only up to one ``CHILDREN_PAGE_SIZE`` page — a goto-ref target
    beyond that page must still be found, not raise ``StopIteration``
    out of the ``_walk_to_target`` worker's own
    ``GotoChainWalker.expand_to_chain`` call. The target is a leaf, so it
    never becomes a tree node at all -- ``_walk_to_target`` parks the
    file table's own cursor on it instead (the folder tree's cursor stays
    on root, its parent)."""
    root, children_by_ref = _build_root_and_children()
    provider = _PaginatingFakeProvider(root, children_by_ref)
    app = _FakeApp(_version(), _FakeCatalog(provider))
    async with app.run_test() as pilot:
        unit_screen = app.screen
        assert isinstance(unit_screen, UnitScreen)
        await wait_until(pilot, lambda: root.ref in unit_screen.store.model.loaded, timeout=sdk_timeout, interval=0.05)
        assert len(unit_screen._file_table._nodes) == CHILDREN_PAGE_SIZE  # root "loaded", one page

        target = children_by_ref[str(root.ref)][-1]  # past the first page's own last item
        unit_screen._walk_to_target(unit_screen._current_provider(), target.ref)
        await wait_until(
            pilot,
            lambda: target in unit_screen._file_table._nodes,
            timeout=sdk_timeout,
            interval=0.05,
        )
        row_index = unit_screen._file_table._nodes.index(target)
        table = unit_screen.query_one("#file-table", DataTable)
        assert table.cursor_coordinate == Coordinate(row_index, 0)
        assert len(unit_screen._file_table._nodes) == _TOTAL_CHILDREN  # topped up to the full, exhaustive list


__all__: list[str] = []
