"""Unit tests for ``BrowseScreen`` covering branches none of this
package's other ``browse_screen``-focused test files reach: the
loading-indicator markup, the version/tree filter Enter-to-close paths,
``action_refresh``'s dispatch branches, ``refresh_for_verbose_mode``, the
"nothing selected" tree guard, ``action_go_back``'s filter/goto branches,
``action_show_diagnostics``, the tree-filter "nothing cached" guard and
its workload-level re-render, goto-ref's parse-failure/error branches,
and races 1-3 the ``core/browse/*`` MVU treatment closes. Driven through
a real Textual ``Pilot`` against ``BrowseScreen`` pushed directly
(bypassing ``ApmRepoBrowserApp``'s own auto-opened ``ConnectDialog`` —
this screen's own ``on_mount`` only builds an empty ``Store``/``Effects``
pair from a blank ``BrowseModel()``, needing neither a repository nor a
session; real data only arrives later, via ``_apply_discovered()`` once
that dialog's scan finds something). Most tests here drive real ``Msg``
dispatches through ``screen.store`` and observe the resulting rendered
``Tree``/``DataTable`` state, rather than poking private screen fields
directly."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Input, Static, Tree

from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.browse.model import catalog_key, workload_key
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    CatalogsRequested,
    KeyVerified,
    RescanStarted,
    TreeFilterClosed,
    TreeFilterOpened,
    TreeFilterTextChanged,
    VersionFilterOpened,
    VersionFilterTextChanged,
    WorkloadSelected,
)
from synology_apm_repo.browser.core.browse.select import WorkloadGroupKey
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.core.remote_data import Success
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.strings import BROWSE_VERSIONS_EMPTY_LABEL, GOTO_REF_NOT_CANONICAL_WARNING
from synology_apm_repo.sdk.api import Catalog, Connection, Repository, Session, Version, Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    ConnectionConfigId,
    ConnectionId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout, RepositoryLayout
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


def _fake_repository(repo_root: str = "@ActiveProtectData/repo-1") -> Repository:
    """A real ``Repository``, backed by placeholder store/layout —
    ``Repository.__init__`` does no I/O itself (``catalog_repo_layouts()``
    is a pure function of ``layout``), so nothing here ever touches a
    real ``DedupRepo``."""
    return Repository(
        cast(ObjectStore, object()),
        RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root),
        None,
        None,
        encrypted=False,
    )


def _connection(connection_id: str = "cc") -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(1),
        connection_id=ConnectionId(connection_id),
        display_name="Source",
        namespaces=(),
        workload_count=1,
        version_count=1,
    )


def _fake_catalog(connection: Connection) -> Catalog:
    """A minimal ``Catalog`` wrapping ``connection`` — none of this file's
    tests call any of its I/O methods (``workloads``/``versions``/
    ``provider``), so a placeholder ``dedup_repo``/``track``/
    ``require_key_verified`` is enough. The placeholder still carries a
    working ``.layout`` (``repo_id=None``, matching a vault) since
    ``Catalog.catalog_id`` — reached by ``disambiguate_catalogs()`` when
    rendering a filtered catalog list — dereferences it."""
    import types

    dedup_repo = cast(DedupRepo, types.SimpleNamespace(layout=RepoLayout(kind=RepoKind.VAULT, repo_root="")))
    return Catalog(
        dedup_repo,
        connection,
        saas_streams=SaasStreamCache(dedup_repo),
        track=lambda provider: provider,
        require_key_verified=lambda: None,
    )


class _FakeApp(App[None]):
    """``BrowseScreen`` only ever reads ``app_state.current_repo``/
    ``.verbose`` directly; ``resources``/``session`` are needed because
    this screen's own ``Store`` routes every ``Repository`` through
    ``ResourceTable`` as an opaque ``RepoHandle``/``Session.close_repo()``
    now -- a real, empty ``Session()`` costs no I/O to construct and
    correctly no-ops its own bookkeeping for a repo it never tracked via
    ``session.discover()``."""

    def __init__(self) -> None:
        super().__init__()
        self.repo_handle: RepoHandle | None = None
        self.verbose = False
        self.jobs: dict[JobId, Job] = {}
        self.session = Session()
        self.resources = ResourceTable(self.session)

    @property
    def current_repo(self) -> Repository | None:
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(BrowseScreen())


async def _discover(screen: BrowseScreen, repo: Repository, *, label: str = "/scan") -> RepoHandle:
    """``_apply_discovered``'s own real path -- mints and returns the
    fresh ``RepoHandle`` so a test can address this repository by handle
    afterward, the same way ``BrowseEffects`` itself does."""
    screen._apply_discovered([repo], label)
    handles = list(screen.store.model.repos)
    return handles[-1]


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


def _workload(workload_id: int, display_name: str) -> Workload:
    return Workload(
        workload_id=workload_id,  # type: ignore[arg-type]
        workload_uid=f"wl-{workload_id}",  # type: ignore[arg-type]
        workload_type="VM",
        sub_type=None,
        display_name=display_name,
        subtitle=None,
        spec={},
    )


class _CountingCatalog:
    """A ``Catalog``-shaped fake whose own ``workloads()``/``versions()``
    count their own calls -- shared by the skip-refetch tests below to
    prove a reselect hits (or, on a cache hit, doesn't hit) the fetch,
    not just that the rendered result looks right either way."""

    def __init__(self, catalog_id: CatalogId, workloads: list[Workload]) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"
        self._workloads = workloads
        self.workloads_calls = 0
        self.versions_calls = 0

    async def workloads(self) -> list[Workload]:
        self.workloads_calls += 1
        return self._workloads

    async def versions(self, workload: Workload) -> list[Version]:
        self.versions_calls += 1
        return [_version()]


class _GatedCatalog:
    """A ``Catalog``-shaped fake whose ``workloads()``/``versions()`` each
    block on their own ``asyncio.Event`` until the test releases it --
    lets a test control exactly when a slow fetch resolves, relative to
    a second, faster selection made in the meantime. Ungated by default
    (both gates start set), so a test only pays for the control it
    actually asks for."""

    def __init__(self, catalog_id: CatalogId, workloads: list[Workload] | None = None) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"
        self._workloads = workloads or []
        self.workloads_gate = asyncio.Event()
        self.workloads_gate.set()
        self.workloads_calls = 0
        self.versions_calls: list[Workload] = []
        self._versions_gates: dict[int, asyncio.Event] = {}

    def gate_versions_for(self, workload_id: int) -> asyncio.Event:
        gate = asyncio.Event()
        self._versions_gates[workload_id] = gate
        return gate

    async def workloads(self) -> list[Workload]:
        self.workloads_calls += 1
        await self.workloads_gate.wait()
        return self._workloads

    async def versions(self, workload: Workload) -> list[Version]:
        self.versions_calls.append(workload)
        gate = self._versions_gates.get(workload.workload_id)
        if gate is not None:
            await gate.wait()
        return [_version()]


async def test_set_loading_indicator_appends_markup_to_the_breadcrumb() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen._set_loading_indicator("[dim]spinning[/dim]")
        breadcrumb = str(screen.query_one("#breadcrumb", Static).render())
        assert "spinning" in breadcrumb


async def test_action_refresh_dispatches_when_something_is_selected_else_reopens_connect(wait_until: Any) -> None:
    """``update()``'s own ``RefreshRequested`` case does all the actual
    branch-picking (workload over catalog over neither) -- proven
    directly in ``test_browser_core_browse_update.py``. This only proves
    ``action_refresh`` dispatches it when something's selected, and falls
    through to ``action_connect_remote`` otherwise."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        catalog = _CountingCatalog(CatalogId("cat-1"), [_workload(1, "Workload-1")])
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.action_refresh()
        await wait_until(pilot, lambda: catalog.workloads_calls == 2)

        connect_calls: list[bool] = []
        screen.action_connect_remote = lambda: connect_calls.append(True)  # type: ignore[method-assign]
        screen.store.dispatch(RescanStarted(scan_path=""))
        screen.action_refresh()
        assert connect_calls == [True]


async def test_refresh_for_verbose_mode_relabels_repo_nodes(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        await _discover(screen, repo)
        await pilot.pause()
        tree = screen.query_one("#col-catalogs", Tree)
        before = str(tree.root.children[0].label)

        app.verbose = True
        screen.refresh_for_verbose_mode()
        after = str(tree.root.children[0].label)
        assert after != before
        # No per-repository uuid any more -- that's genuinely per-catalog now
        # (see Catalog.info) -- only the layout-kind suffix is added here.
        assert "layout: object_store" in after


async def test_on_tree_node_selected_with_no_data_is_a_no_op() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        tree.root.add_leaf("(error)", data=None)
        await pilot.pause()
        tree.focus()
        tree.move_cursor(tree.root.children[0])
        await pilot.pause()
        await pilot.press("enter")  # must not raise
        await pilot.pause()


async def test_action_go_back_closes_version_filter_before_falling_through() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen.store.dispatch(VersionFilterOpened())
        screen.query_one("#filter-input", Input).add_class("active")

        screen.action_go_back()
        await pilot.pause()
        assert screen.store.model.version_filter is None
        assert isinstance(app.screen, BrowseScreen)


async def test_action_go_back_closes_an_open_goto_box() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen.action_goto_ref()
        await pilot.pause()
        assert screen.query_one("#goto-input", Input).has_class("active")

        screen.action_go_back()
        await pilot.pause()
        assert not screen.query_one("#goto-input", Input).has_class("active")
        assert isinstance(app.screen, BrowseScreen)  # h/Esc is otherwise a no-op here, never pops


async def test_action_show_diagnostics_pushes_the_diagnostics_screen() -> None:
    from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        async def fake_verify(level: object, **kwargs: object) -> list[object]:
            return []

        repo = _fake_repository()
        repo.verify = fake_verify  # type: ignore[method-assign,assignment]
        app.repo_handle = app.resources.put_repo(repo)
        screen.action_show_diagnostics()
        await pilot.pause()
        assert isinstance(app.screen, DiagnosticsScreen)


async def test_open_tree_filter_is_a_no_op_for_an_uncached_level() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        tree.focus()
        screen.action_filter()  # nothing cached under the tree's own top container
        await pilot.pause()
        assert not screen.query_one("#filter-input", Input).has_class("active")


async def test_tree_filter_rerenders_workload_level_via_the_group_cache(wait_until: Any, move_cursor_to: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        workloads = [_workload(1, "Workload-1"), _workload(2, "Workload-2")]
        catalog = _CountingCatalog(CatalogId("cat-1"), workloads)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children))
        group_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, group_node.children[0])
        screen.action_filter()
        await pilot.pause()
        assert screen.store.model.tree_filter is not None
        screen._tree_filter.pending_text = "Workload-1"
        screen._tree_filter._commit()
        await pilot.pause()

        assert [c.data.payload.display_name for c in group_node.children if c.data is not None] == ["Workload-1"]


async def test_tree_filter_debounces_before_rerendering(wait_until: Any, move_cursor_to: Any) -> None:
    """``on_input_changed``'s tree-filter branch goes through the same
    ``Debouncer`` the version filter already used (matching
    browser/README.md's loading-feedback convention) — a keystroke must
    not rebuild the tree synchronously, only once the debounce settles."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        connection_a = _connection("cc-a")
        connection_b = _connection("cc-b")
        catalog_a = _fake_catalog(connection_a)
        catalog_b = _fake_catalog(connection_b)

        class _StaticCatalogsRepo:
            async def catalogs(self) -> list[Catalog]:
                return [catalog_a, catalog_b]

        repo.catalogs = _StaticCatalogsRepo().catalogs  # type: ignore[method-assign]
        await _discover(screen, repo)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        repo_node.expand()  # the real UI flow: expanding is what fires on_tree_node_expanded below
        await wait_until(pilot, lambda: len(repo_node.children) == 2)

        await move_cursor_to(pilot, tree, repo_node.children[0])
        screen.action_filter()
        await pilot.pause()
        original_children = list(repo_node.children)
        filter_input = screen.query_one("#filter-input", Input)
        filter_input.value = connection_a.display_name  # "Source" -- both share this display_name in these fakes
        screen.on_input_changed(Input.Changed(filter_input, filter_input.value))
        # Not rebuilt yet -- the keystroke only (re)armed the debounce.
        assert list(repo_node.children) == original_children

        await wait_until(
            pilot,
            lambda: (
                screen.store.model.tree_filter is not None
                and screen.store.model.tree_filter.text == connection_a.display_name
            ),
            timeout=1.0,
            interval=0.02,
            message="debounced tree filter never settled",
        )


async def test_submit_goto_parse_failure_returns_without_resolving() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen._submit_goto("/some/path#not-canonical")
        assert warnings == [GOTO_REF_NOT_CANONICAL_WARNING]


async def test_submit_goto_version_lookup_failure_notifies(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        repo = _fake_repository()

        async def failing_version_for_ref(node_ref: object) -> object:
            raise ApmRepoError("ref not found")

        repo.version_for_ref = failing_version_for_ref  # type: ignore[method-assign,assignment]
        app.repo_handle = app.resources.put_repo(repo)

        ref = NodeRef.canonical(
            "repo", catalog_id=CatalogId("1"), workload_id=WorkloadId(1), version_uid=VersionUid("v1")
        )
        screen._submit_goto(str(ref))
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == ["ref not found"]


async def test_enter_on_the_filter_input_closes_an_open_tree_filter(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        catalog = _fake_catalog(_connection())

        async def _catalogs() -> list[Catalog]:
            return [catalog]

        repo.catalogs = _catalogs  # type: ignore[method-assign]
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        tree = screen.query_one("#col-catalogs", Tree)
        await wait_until(pilot, lambda: tree.root.children[0].children)
        screen.store.dispatch(TreeFilterOpened(tree="catalogs", parent_key=handle))
        filter_input = screen.query_one("#filter-input", Input)
        filter_input.add_class("active")

        screen.on_input_submitted(Input.Submitted(filter_input, ""))
        await pilot.pause()
        assert screen.store.model.tree_filter is None
        assert not filter_input.has_class("active")


async def test_saas_only_workloads_land_the_cursor_on_a_leaf_not_root(wait_until: Any) -> None:
    """With no device-type workloads at all, the first leaf-holding group
    can only ever be found inside the SaaS platform/tenant/sub_type
    nesting -- ``test_browser_browse_screen_grouping.py`` covers
    ``_group_workloads`` itself; this is the rendering layer directly
    above it that actually reaches this specific case."""
    workload = Workload(
        workload_id=1,  # type: ignore[arg-type]
        workload_uid="wl-uid",  # type: ignore[arg-type]
        workload_type="M365",
        sub_type="USER_MAILBOX",
        display_name="mailbox",
        subtitle=None,
        spec={"spec": {"tenant_id": "tenant-1"}},
    )
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        catalog = _CountingCatalog(CatalogId("cat-1"), [workload])
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: tree.cursor_node is not None and tree.cursor_node.data is not None)
        cursor_node = tree.cursor_node
        assert cursor_node is not None and cursor_node.data is not None
        assert cursor_node.data.payload is workload


async def test_render_versions_filter_excludes_non_matching_names(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        catalog = _CountingCatalog(CatalogId("cat-1"), [])
        workload = _workload(1, "Workload-1")
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        catalog.versions = _named_versions_fn(["Monday backup", "Tuesday backup"])  # type: ignore[method-assign]
        screen.store.dispatch(WorkloadSelected(workload=workload))
        table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2)

        screen.store.dispatch(VersionFilterOpened())
        screen.store.dispatch(VersionFilterTextChanged(text="monday"))
        # A real, dispatch-driven render -- not a direct _render_versions()
        # call -- proves the Store subscription itself actually reacts to
        # a filter-only change: model.version_filter is part of the
        # subscribed slice precisely because filtering happens inside
        # _render_versions() itself, so version_rows()/version_load_error()
        # alone never change on a filter-only keystroke.
        await wait_until(pilot, lambda: table.row_count == 1)
        assert screen._visible_version_indices == [0]


def _named_versions_fn(names: list[str]) -> Any:
    async def _versions(workload: Workload) -> list[Version]:
        return [
            Version(
                version_id=VersionId(i),
                version_uid=VersionUid(f"vuid-{i}"),
                workload_id=WorkloadId(1),
                connection_config_id=ConnectionConfigId(1),
                target_type="VM",
                target_id=TargetId("target"),
                saas_stream_uuid=StreamUuid(""),
                saas_snapshot_uuid=SnapshotUuid(""),
                saas_version_id=SaasVersionId(0),
                deleted=False,
                display_name=name,
                meta=None,
            )
            for i, name in enumerate(names)
        ]

    return _versions


async def test_render_versions_restores_cursor_to_the_same_version_after_filtering(wait_until: Any) -> None:
    """``_render_versions`` runs on every settled keystroke of the
    (debounced) version filter, and ``DataTable.clear()`` unconditionally
    resets ``cursor_coordinate`` to (0, 0) — without the restore this
    covers, filtering while the cursor sits on a version other than the
    first would silently yank it back to row 0 on every keystroke."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        catalog = _CountingCatalog(CatalogId("cat-1"), [])
        workload = _workload(1, "Workload-1")
        catalog.versions = _named_versions_fn(["Apple backup", "Banana backup", "Cherry backup"])  # type: ignore[method-assign]
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        screen.store.dispatch(WorkloadSelected(workload=workload))
        table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: table.row_count == 3)
        table.cursor_coordinate = Coordinate(2, 0)  # parked on "Cherry backup"
        await pilot.pause()

        screen.store.dispatch(VersionFilterOpened())
        screen.store.dispatch(VersionFilterTextChanged(text="e"))  # matches Apple/Cherry (not Banana)
        # A real, dispatch-driven render, same reason as the filter test
        # above: a direct _render_versions() call would skip proving the
        # dispatch path that actually delivers a filter keystroke here.
        await wait_until(pilot, lambda: table.row_count == 2)

        assert table.row_count == 2
        assert screen._visible_version_indices == [0, 2]
        assert table.cursor_row == 1  # "Cherry backup", now the second visible row


async def test_on_data_table_row_selected_ignores_a_foreign_table() -> None:
    from textual.widgets.data_table import RowKey

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        foreign_table: DataTable[str] = DataTable(id="not-col-versions")
        event = DataTable.RowSelected(data_table=foreign_table, cursor_row=0, row_key=RowKey("x"))
        screen.on_data_table_row_selected(event)  # must not raise or push a screen
        await pilot.pause()
        assert isinstance(app.screen, BrowseScreen)


async def test_catalogs_fetch_error_shows_an_error_leaf_under_that_repo_node(
    wait_until: Any, sdk_timeout: float
) -> None:
    # _load_catalogs_for's own ApmRepoError branch -- every other repository
    # stays fully usable regardless of this one repository's own failure,
    # but no test exercised the error leaf itself.
    class _FailingRepo:
        async def catalogs(self) -> list[Catalog]:
            raise ApmRepoError("boom")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        repo.catalogs = _FailingRepo().catalogs  # type: ignore[method-assign]
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))

        tree = screen.query_one("#col-catalogs", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children[0].children), timeout=sdk_timeout, interval=0.02)
        assert len(tree.root.children[0].children) == 1
        assert "error: boom" in str(tree.root.children[0].children[0].label)


async def test_reexpanding_a_repo_with_zero_catalogs_does_not_refetch(wait_until: Any, move_cursor_to: Any) -> None:
    """A repository with genuinely zero catalogs still gets a resolved
    ``Success(())`` in ``model.repos[handle].catalogs`` once its own
    fetch succeeds -- ``select.py``'s own ``_catalog_children_spec`` then
    renders 0 widget children, the exact same shape a *never-requested*
    repo node also has. ``on_tree_node_expanded`` must tell the two apart
    via ``state.catalogs`` being a ``Success`` already, not
    ``event.node.children`` alone, or every collapse/re-expand re-issues
    a real ``repo.catalogs()`` round-trip (a genuine S3/Azure network
    cost) for a repository that will only ever come back empty."""
    calls = 0

    async def _counting_catalogs() -> list[Catalog]:
        nonlocal calls
        calls += 1
        return []

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        repo.catalogs = _counting_catalogs  # type: ignore[method-assign]
        await _discover(screen, repo)

        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_node)
        await pilot.press("space")  # expand -- dispatches CatalogsRequested
        await wait_until(pilot, lambda: calls == 1)
        assert list(repo_node.children) == []

        await pilot.press("space")  # collapse
        await pilot.pause()
        await pilot.press("space")  # re-expand -- must not re-fetch
        await pilot.pause()

        assert calls == 1, "re-expanding a zero-catalog repo must not re-fetch"


async def test_reexpanding_a_repo_while_its_catalogs_fetch_is_still_loading_does_not_refetch(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """Textual's own ``auto_expand`` toggles the node on every Enter/space
    press -- pressing it again while the first ``CatalogsRequested`` is
    still ``Loading`` (a slow S3/Azure fetch) must not dispatch a second
    one, or each duplicate spawns its own worker and its own
    ``TreeNodeLoadingSink`` on this same node (the reported bug: the node's
    label ends up with several loading indicators stacked on it)."""
    calls = 0
    gate = asyncio.Event()

    async def _gated_catalogs() -> list[Catalog]:
        nonlocal calls
        calls += 1
        await gate.wait()
        return [_fake_catalog(_connection())]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        repo.catalogs = _gated_catalogs  # type: ignore[method-assign]
        await _discover(screen, repo)

        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_node)
        await pilot.press("space")  # expand -- starts the gated fetch
        await wait_until(pilot, lambda: calls == 1)

        await pilot.press("space")  # collapse
        await pilot.press("space")  # re-expand while still loading -- must not refetch
        await pilot.pause()
        assert calls == 1, "re-expanding while still loading must not refetch"

        gate.set()
        await wait_until(pilot, lambda: repo_node.children)
        assert calls == 1
        assert "Loading" not in str(repo_node.label)


async def test_workloads_fetch_error_shows_an_error_leaf_in_column_2(wait_until: Any, sdk_timeout: float) -> None:
    # _load_workloads's own generic ApmRepoError branch -- distinct from
    # its KeyRequiredError/KeyMismatchError branch (already covered by
    # tests/integration/browser/test_browser_pilot.py's own
    # encrypted-repository scenario) -- no test exercised this error leaf itself.
    class _FailingCatalog:
        catalog_id = CatalogId("cat-1")
        display_name = "catalog"

        async def workloads(self) -> list[Workload]:
            raise ApmRepoError("boom")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, _FailingCatalog())))

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=sdk_timeout, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


class _FakeCatalogWithId:
    """Shared by the key-verification-reload tests below -- a
    ``Catalog`` duck-type carrying only what ``_load_workloads``/
    ``BrowseEffects._reload_catalogs_after_key_verified`` actually touch."""

    def __init__(self, catalog_id: CatalogId) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"
        self.workloads_calls = 0

    async def workloads(self) -> list[Workload]:
        self.workloads_calls += 1
        return []


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_is_gone(
    wait_until: Any, sdk_timeout: float
) -> None:
    """A narrow race — the catalog just key-verified disappears before
    ``repo.catalog_by_id()`` is re-fetched (a repeat scan/refresh
    mid-flight) — must report it via the same ``FailureInfo``-driven error
    leaf ``_load_workloads``'s own ``ApmRepoError`` branch renders, not
    silently leave column 2/3 on stale data with no indication anything
    went wrong."""
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        return None

    repo.catalog_by_id = _catalog_by_id  # type: ignore[method-assign]
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("gone")))
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=sdk_timeout, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error:" in str(tree.root.children[0].label) and "gone" in str(tree.root.children[0].label)


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_fails_to_open(
    wait_until: Any, sdk_timeout: float
) -> None:
    """A narrower race than the "gone" case above — the catalog just
    key-verified now raises instead of resolving to ``None`` (e.g. a
    repeat scan found it newly corrupt) — must report it too, not
    propagate out of this worker uncaught."""
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        raise ApmRepoError("boom")

    repo.catalog_by_id = _catalog_by_id  # type: ignore[method-assign]
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("anything")))
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=sdk_timeout, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


async def test_reselecting_the_same_catalog_after_a_key_reload_uses_the_fresh_one(wait_until: Any) -> None:
    """``Repository.set_key()`` closes and replaces every already-opened
    ``DedupRepo`` on success — ``KeyVerified``'s own
    ``ReloadCatalogsAfterKeyVerified`` effect exists specifically so this
    screen stops using the resulting stale ``Catalog`` reference. Nor is
    the catalog that triggered ``KeyDialog`` the only stale reference:
    for ``RepoKind.OBJECT_STORE``, ``catalogs()`` already opened every
    sibling's own ``DedupRepo`` eagerly, unkeyed, before any key was ever
    entered — so ``model.repos[repo].catalogs`` (what a later ``/``
    filter rebuilds column 1's leaves from) must get every sibling's own
    fresh entry too, not just the one that triggered ``KeyDialog``."""
    stale = _FakeCatalogWithId(CatalogId("cat-1"))
    fresh = _FakeCatalogWithId(CatalogId("cat-1"))
    stale_other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh_other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh_by_id = {CatalogId("cat-1"): fresh, CatalogId("cat-other"): fresh_other}
    repo = _fake_repository()

    async def _catalogs() -> list[Catalog]:
        return [cast(Catalog, stale_other), cast(Catalog, stale)]

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        return cast(Catalog, fresh_by_id[catalog_id])

    repo.catalogs = _catalogs  # type: ignore[method-assign]
    repo.catalog_by_id = _catalog_by_id  # type: ignore[method-assign]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: bool(screen.store.model.repos[handle].catalogs.value))  # type: ignore[union-attr]

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: screen.store.model.selected_catalog is not None)

        assert screen.store.model.selected_catalog is not None
        assert screen.store.model.selected_catalog.catalog is cast(Catalog, fresh)
        refreshed = screen.store.model.repos[handle].catalogs.value  # type: ignore[union-attr]
        assert set(refreshed) == {cast(Catalog, fresh), cast(Catalog, fresh_other)}

        # Re-selecting the identical catalog again (the user clicking the
        # same node again later) must now see the fresh catalog, not the
        # stale one -- and its own cache entry is a FailureInfo (never a
        # cached Success, since a key-required catalog's own workloads()
        # could never have succeeded before the key was verified), so the
        # reselect genuinely re-fetches.
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, fresh)))
        await wait_until(pilot, lambda: fresh.workloads_calls > 0)


async def test_reselecting_the_same_catalog_skips_a_redundant_workloads_fetch(wait_until: Any) -> None:
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), [_workload(1, "Workload-1")])

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)

        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        # Reselecting the exact same catalog later must serve the cached
        # workload list rather than re-dispatching workloads().
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await pilot.pause()
        assert catalog.workloads_calls == 1
        workloads_tree = screen.query_one("#col-workloads", Tree)
        assert workloads_tree.root.children  # the cached list still rendered


async def test_reselecting_the_same_workload_skips_a_redundant_versions_fetch(wait_until: Any) -> None:
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), [])
    workload = _workload(1, "Workload-1")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        # Reselecting the exact same workload later must serve the
        # cached version list rather than re-dispatching versions().
        screen.store.dispatch(WorkloadSelected(workload=workload))
        await pilot.pause()
        assert catalog.versions_calls == 1
        versions_table = screen.query_one("#col-versions", DataTable)
        assert versions_table.row_count == 1


async def test_reselecting_the_same_catalog_while_its_workloads_fetch_is_still_loading_does_not_refetch(
    wait_until: Any,
) -> None:
    """The column-2 analogue of the catalogs-column race
    (``test_reexpanding_a_repo_while_its_catalogs_fetch_is_still_loading_does_not_refetch``):
    reselecting the same catalog while its own ``LoadWorkloads`` is still
    ``Loading`` must not dispatch a second one -- a duplicate dispatch
    here, while data-safe (an idempotent ``workloads()`` re-read), still
    duplicates the worker and its loading indicator."""
    repo = _fake_repository()
    catalog = _GatedCatalog(CatalogId("cat-1"), [_workload(1, "Workload-1")])
    catalog.workloads_gate.clear()

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)

        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await pilot.pause()
        assert catalog.workloads_calls == 1, "reselecting while still loading must not refetch"

        catalog.workloads_gate.set()
        workloads_tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: workloads_tree.root.children)
        assert catalog.workloads_calls == 1


async def test_reselecting_the_same_workload_while_its_versions_fetch_is_still_loading_does_not_refetch(
    wait_until: Any,
) -> None:
    repo = _fake_repository()
    workload = _workload(1, "Workload-1")
    catalog = _GatedCatalog(CatalogId("cat-1"))
    gate = catalog.gate_versions_for(1)

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await pilot.pause()

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == [workload])

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await pilot.pause()
        assert catalog.versions_calls == [workload], "reselecting while still loading must not refetch"

        gate.set()
        versions_table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: versions_table.row_count == 1)
        assert catalog.versions_calls == [workload]


async def test_versions_placeholder_never_shows_while_the_first_fetch_is_still_pending(
    wait_until: Any,
) -> None:
    """The "(no available versions)" placeholder must never appear before
    ``catalog.versions()`` has actually resolved -- showing it during a
    still-in-flight first fetch is exactly the race ``has_ever_resolved``
    (``core/remote_data.py``) now guards against."""
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), [])
    workload = _workload(1, "Workload-1")
    gate = asyncio.Event()

    async def _versions(w: Workload) -> list[Version]:
        catalog.versions_calls += 1
        await gate.wait()
        return []

    catalog.versions = _versions  # type: ignore[method-assign, assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        versions_table = screen.query_one("#col-versions", DataTable)
        # Same real-timer caveat as
        # test_refreshing_twice_while_the_versions_refetch_is_still_loading_does_not_redispatch
        # above: DataTableLoadingRowSink's own debounced row may or may not
        # have appeared by now, so assert the invariant that matters here
        # (no placeholder text) rather than an exact row count.
        assert versions_table.row_count <= 1
        if versions_table.row_count == 1:
            assert str(versions_table.get_row_at(0)[0]) != BROWSE_VERSIONS_EMPTY_LABEL

        gate.set()
        # Waits on the placeholder text itself, not just row_count == 1 --
        # the debounced loading row asserted-tolerated above could already
        # be sitting at row 0 before gate.set(), which would make a bare
        # row_count == 1 check trivially true without ever waiting for
        # _render_versions()'s clear()-and-repopulate to actually run.
        await wait_until(
            pilot,
            lambda: (
                versions_table.row_count == 1 and str(versions_table.get_row_at(0)[0]) == BROWSE_VERSIONS_EMPTY_LABEL
            ),
        )


async def test_a_versions_fetch_failure_shows_an_error_row_and_a_reselect_retries(wait_until: Any) -> None:
    """A ``catalog.versions()`` failure renders as column 3's own single
    error row, is never cached as a skip-refetch hit, and a later
    reselect of the exact same workload genuinely retries."""
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), [])
    workload = _workload(1, "Workload-1")
    should_fail = True

    async def _versions(w: Workload) -> list[Version]:
        catalog.versions_calls += 1
        if should_fail:
            raise ApmRepoError("versions boom")
        return [_version()]

    catalog.versions = _versions  # type: ignore[method-assign, assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        versions_table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: versions_table.row_count == 1)
        assert "error: versions boom" in str(versions_table.get_row_at(0)[0])

        # Never cached as a hit -- a reselect of the identical workload
        # always retries rather than replaying the failure forever.
        should_fail = False
        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 2)
        await wait_until(
            pilot, lambda: versions_table.row_count == 1 and "error" not in str(versions_table.get_row_at(0)[0])
        )


async def test_refresh_re_fetches_the_currently_selected_workload_even_when_cached(wait_until: Any) -> None:
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), [])
    workload = _workload(1, "Workload-1")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        # ``r`` on an already-cached selection must not be short-circuited
        # by the skip-refetch cache -- an explicit refresh always re-fetches.
        screen.action_refresh()
        await wait_until(pilot, lambda: catalog.versions_calls == 2)


async def test_refreshing_twice_while_the_versions_refetch_is_still_loading_does_not_redispatch(
    wait_until: Any,
) -> None:
    """``RefreshRequested`` unconditionally transitions to ``Loading`` via
    ``loading_preserving`` -- pressing ``r`` again before the first refresh
    resolves must not dispatch a second ``LoadVersions`` (duplicate
    worker/indicator), and must not blank the stale-but-real rows still on
    screen (a second ``loading_preserving()`` call on an already-``Loading``
    value would drop its own carried-forward ``previous``)."""
    repo = _fake_repository()
    catalog = _GatedCatalog(CatalogId("cat-1"))
    workload = _workload(1, "Workload-1")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await pilot.pause()
        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == [workload])

        versions_table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: versions_table.row_count == 1)
        stale_row = versions_table.get_row_at(0)

        catalog.gate_versions_for(1)  # a fresh Event() starts cleared -- gates the refresh below
        screen.action_refresh()
        await wait_until(pilot, lambda: catalog.versions_calls == [workload, workload])

        screen.action_refresh()  # a second refresh while the first is still in flight
        await pilot.pause()
        assert catalog.versions_calls == [workload, workload], "must not re-dispatch while already refreshing"
        # The stale-but-real row must still be there. Whether
        # DataTableLoadingRowSink's own debounced indicator row has also
        # appended by now depends on real scheduling (it's a real timer,
        # not tied to anything this test controls) -- checking the actual
        # invariant (the real row survives, in its original place) instead
        # of an exact row count keeps this deterministic either way.
        assert versions_table.get_row_at(0) == stale_row
        assert versions_table.row_count in (1, 2)


async def test_refreshing_twice_while_the_workloads_refetch_is_still_loading_does_not_redispatch(
    wait_until: Any,
) -> None:
    repo = _fake_repository()
    catalog = _GatedCatalog(CatalogId("cat-1"), [_workload(1, "Workload-1")])

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        workloads_tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: workloads_tree.root.children)

        catalog.workloads_gate.clear()
        screen.action_refresh()
        await wait_until(pilot, lambda: catalog.workloads_calls == 2)

        screen.action_refresh()  # a second refresh while the first is still in flight
        await pilot.pause()
        assert catalog.workloads_calls == 2, "must not re-dispatch while already refreshing"
        assert workloads_tree.root.children  # the stale-but-real tree stayed on screen

        catalog.workloads_gate.set()
        await wait_until(pilot, lambda: catalog.workloads_calls == 2)


# -- races 1-3 (refactor plan's own numbering) -----------------------------


async def test_selecting_catalog_b_before_catalog_as_slow_workloads_fetch_resolves_wins(wait_until: Any) -> None:
    """Race 1: selecting catalog A then quickly reselecting catalog B
    must not let A's slower ``workloads()`` fetch overwrite B's already
    -rendered tree once it finally resolves. Closed structurally by
    ``model.catalog_workloads`` being keyed by each catalog's own
    ``CatalogKey`` -- A's late write lands in A's own dict entry, which
    ``select.py`` only ever reads when A is the *currently selected*
    catalog, never B's."""
    repo = _fake_repository()
    catalog_a = _GatedCatalog(CatalogId("a"), [_workload(1, "A-workload")])
    catalog_a.workloads_gate.clear()  # A's own fetch stays in flight until released below
    catalog_b = _CountingCatalog(CatalogId("b"), [_workload(2, "B-workload")])

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)

        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog_a)))
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog_b)))
        await wait_until(pilot, lambda: catalog_b.workloads_calls == 1)

        workloads_tree = screen.query_one("#col-workloads", Tree)
        b_children = list(workloads_tree.root.children)
        assert b_children, "setup: B's own workloads never rendered"

        catalog_a.workloads_gate.set()
        await pilot.pause()
        # A's own late resolution must not have rebuilt the tree over B's.
        assert list(workloads_tree.root.children) == b_children


async def test_selecting_workload_b_before_workload_as_slow_versions_fetch_resolves_wins(wait_until: Any) -> None:
    """The version-selection analogue of race 1, one level down --
    ``model.workload_versions`` is keyed by each workload's own
    ``WorkloadKey``, the same structural isolation."""
    repo = _fake_repository()
    workload_a = _workload(1, "Workload-A")
    workload_b = _workload(2, "Workload-B")
    catalog = _GatedCatalog(CatalogId("cat-1"))
    gate_a = catalog.gate_versions_for(1)

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await pilot.pause()
        wk_b = workload_key(catalog_key(handle, cast(Catalog, catalog)), workload_b)

        screen.store.dispatch(WorkloadSelected(workload=workload_a))
        await wait_until(pilot, lambda: catalog.versions_calls == [workload_a])

        screen.store.dispatch(WorkloadSelected(workload=workload_b))
        # catalog.versions_calls only proves both fetches have *started* --
        # versions_calls.append() runs synchronously at the top of
        # _GatedCatalog.versions(), before any await. B's own fetch is
        # ungated (only workload_a's id has a gate), so it still needs a
        # real dispatch through the Store to land in model.workload_versions
        # and reach _render_versions() -- wait for that settled state
        # rather than the row_count it produces, or a stale row_count left
        # over from _discover()'s own earlier selection can get captured
        # as b_row_count instead.
        await wait_until(pilot, lambda: isinstance(screen.store.model.workload_versions.get(wk_b), Success))
        table = screen.query_one("#col-versions", DataTable)
        b_row_count = table.row_count
        assert b_row_count

        gate_a.set()
        await pilot.pause()
        # A's own late resolution must not have replaced B's already-shown rows.
        assert table.row_count == b_row_count


async def test_switching_catalogs_mid_flight_does_not_make_load_versions_query_the_wrong_catalog(
    wait_until: Any,
) -> None:
    """Race 2: the effect performing ``LoadVersions`` must query the
    catalog the workload was actually selected under, captured on the
    ``Cmd`` at dispatch time -- never re-read from live
    ``model.selected_catalog``, which a concurrent catalog switch could
    already have moved on by the time this fetch's own ``await`` returns."""
    repo = _fake_repository()
    workload_a = _workload(1, "Workload-A")
    catalog_a = _GatedCatalog(CatalogId("a"))
    gate_a = catalog_a.gate_versions_for(1)
    catalog_b = _GatedCatalog(CatalogId("b"))

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog_a)))
        await pilot.pause()
        screen.store.dispatch(WorkloadSelected(workload=workload_a))
        await wait_until(pilot, lambda: catalog_a.versions_calls == [workload_a])

        # The user switches to a different catalog before A's own fetch
        # resolves.
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog_b)))
        await pilot.pause()

        gate_a.set()
        await pilot.pause()
        # A's fetch ran against the catalog it was dispatched with, never
        # the one the selection moved on to in the meantime.
        assert catalog_a.versions_calls == [workload_a]
        assert catalog_b.versions_calls == []


async def test_a_late_catalogs_fetch_does_not_steal_the_cursor_from_elsewhere(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """Race 3: ``Repository.catalogs()`` is real, unbounded I/O -- if the
    user has already moved the cursor away from the repository node they
    expanded by the time it resolves,
    ``_maybe_auto_park_catalog_cursor``'s own first-successful-load
    auto-park must not yank the cursor back to it."""
    repo = _fake_repository()
    catalog = _fake_catalog(_connection())
    gate = asyncio.Event()

    async def _gated_catalogs() -> list[Catalog]:
        await gate.wait()
        return [catalog]

    repo.catalogs = _gated_catalogs  # type: ignore[method-assign]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_node)

        screen.store.dispatch(CatalogsRequested(repo=handle))
        await pilot.pause()  # let the dispatch actually start the gated fetch

        # The user moves on before the fetch resolves -- e.g. back to root.
        await move_cursor_to(pilot, tree, tree.root)

        gate.set()
        await wait_until(pilot, lambda: repo_node.children)
        await pilot.pause()

        assert tree.cursor_node is tree.root  # not stolen back onto repo_node's first catalog


async def test_auto_park_fires_again_after_a_rescan_with_prior_navigation(wait_until: Any, move_cursor_to: Any) -> None:
    """``_maybe_auto_park_catalog_cursor``'s own guard
    (``tree.cursor_node is not node``) depends on ``tree.cursor_node``
    being a real, current reference -- a rescan that leaves the cursor
    pointing at a stale, already-removed ``TreeNode`` from before it
    (rather than resetting it to something real, e.g. the tree's own
    root) would make that guard always true, permanently breaking
    auto-park for every scan after the first one in a session with
    prior navigation. Drills into repo A first (auto-park already
    parks the cursor several levels deep, under a real catalog), then
    rescans to a different repo B and confirms auto-park still fires
    for B too."""
    repo_a = _fake_repository("@ActiveProtectData/repo-a")
    catalog_a = _fake_catalog(_connection())

    async def _catalogs_a() -> list[Catalog]:
        return [catalog_a]

    repo_a.catalogs = _catalogs_a  # type: ignore[method-assign]

    repo_b = _fake_repository("@ActiveProtectData/repo-b")
    catalog_b = _fake_catalog(_connection())

    async def _catalogs_b() -> list[Catalog]:
        return [catalog_b]

    repo_b.catalogs = _catalogs_b  # type: ignore[method-assign]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)

        await _discover(screen, repo_a)
        repo_a_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_a_node)
        # A real expand (not a direct dispatch) -- on_tree_node_expanded
        # is what dispatches CatalogsRequested for a real user interaction,
        # and it's the expand itself that makes the node's own children
        # reachable for move_cursor once they arrive.
        repo_a_node.expand()
        await wait_until(pilot, lambda: repo_a_node.children)
        await pilot.pause()
        assert tree.cursor_node is repo_a_node.children[0]  # auto-park fired for A

        await _discover(screen, repo_b)
        await pilot.pause()
        assert tree.cursor_node is tree.root  # reset by the rescan, not left dangling on A's own removed node
        repo_b_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, repo_b_node)
        repo_b_node.expand()
        await wait_until(pilot, lambda: repo_b_node.children)
        await pilot.pause()

        assert tree.cursor_node is repo_b_node.children[0]  # auto-park fired again for B


async def test_filtering_column_two_preserves_a_surviving_leafs_own_widget_and_its_cache(
    wait_until: Any, move_cursor_to: Any
) -> None:
    """The keyed reconciler keeps a surviving workload leaf's own
    ``TreeNode`` object across a ``/`` filter round-trip on column 2
    (see ``view/reconcile.py``'s own core guarantee) -- unlike the
    pre-MVU screen's own ``id(TreeNode)``-keyed
    ``_workload_versions_by_id``, which needed the workload's own stable
    id to survive a *destroy-and-recreate* cycle, a survivor here is
    never destroyed at all, so reselecting it after a filter round-trip
    both reuses the exact same widget and serves the cached version list
    without a redundant ``versions()`` call."""
    workloads = [_workload(1, "Workload-1"), _workload(2, "Workload-2")]
    repo = _fake_repository()
    catalog = _CountingCatalog(CatalogId("cat-1"), workloads)

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children))
        group_node = tree.root.children[0]
        original_leaf = next(
            c for c in group_node.children if c.data is not None and c.data.payload.display_name == "Workload-1"
        )

        screen.store.dispatch(WorkloadSelected(workload=workloads[0]))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        assert group_node.data is not None
        group_key = group_node.data.key
        assert isinstance(group_key, WorkloadGroupKey)
        screen.store.dispatch(TreeFilterOpened(tree="workloads", parent_key=group_key))
        screen.store.dispatch(TreeFilterTextChanged(text="Workload-1"))
        await pilot.pause()
        screen.store.dispatch(TreeFilterClosed())  # empty filter text -> full list restored
        await pilot.pause()

        rebuilt_leaf = next(
            c for c in group_node.children if c.data is not None and c.data.payload.display_name == "Workload-1"
        )
        assert rebuilt_leaf is original_leaf, "the reconciler must keep a surviving leaf's own TreeNode"

        screen.store.dispatch(WorkloadSelected(workload=workloads[0]))
        await pilot.pause()
        assert catalog.versions_calls == 1  # served from cache, not re-fetched
        assert screen.query_one("#col-versions", DataTable).row_count == 1


async def test_reload_refreshes_the_target_even_when_a_sibling_fails_to_refetch(wait_until: Any) -> None:
    """A sibling catalog's own ``catalog_by_id()`` re-fetch raising (a
    narrow race — found newly corrupt mid-flight) is best-effort: it must
    not abort refreshing the catalog that actually triggered this reload,
    unlike ``catalog_id``'s own failure (the strict "reports an error"
    tests above) — and the failed sibling's own stale entry must stay in
    the list at its own position rather than disappearing from column 1
    entirely."""
    stale_other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh = _FakeCatalogWithId(CatalogId("cat-1"))
    repo = _fake_repository()

    async def _catalogs() -> list[Catalog]:
        return [cast(Catalog, stale_other), cast(Catalog, fresh)]

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        if catalog_id == CatalogId("cat-other"):
            raise ApmRepoError("sibling boom")
        return cast(Catalog, fresh)

    repo.catalogs = _catalogs  # type: ignore[method-assign]
    repo.catalog_by_id = _catalog_by_id  # type: ignore[method-assign]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: bool(screen.store.model.repos[handle].catalogs.value))  # type: ignore[union-attr]

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: screen.store.model.selected_catalog is not None)

        selected = screen.store.model.selected_catalog
        assert selected is not None
        assert selected.catalog is cast(Catalog, fresh)
        # The sibling that failed to re-fetch keeps its own stale entry --
        # never dropped from the list just because its own refresh failed.
        refreshed = screen.store.model.repos[handle].catalogs.value  # type: ignore[union-attr]
        assert set(refreshed) == {cast(Catalog, fresh), cast(Catalog, stale_other)}


async def test_reload_is_a_noop_when_the_triggering_catalog_is_no_longer_in_the_cached_list(wait_until: Any) -> None:
    """A narrower race still: by the time this reload runs, the catalog
    that actually triggered it is no longer among this repository's own
    cached sibling ids at all (e.g. a concurrent rescan replaced the
    list) -- every sibling still gets its own best-effort refresh, but
    there is no catalog left to select, so this quietly does nothing
    further rather than selecting a stale/wrong catalog."""
    other = _FakeCatalogWithId(CatalogId("cat-other"))
    repo = _fake_repository()

    async def _catalogs() -> list[Catalog]:
        return [cast(Catalog, other)]

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        return cast(Catalog, other)

    repo.catalogs = _catalogs  # type: ignore[method-assign]
    repo.catalog_by_id = _catalog_by_id  # type: ignore[method-assign]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        handle = await _discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: bool(screen.store.model.repos[handle].catalogs.value))  # type: ignore[union-attr]

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: screen.store.model.repos[handle].catalogs.value == (cast(Catalog, other),))  # type: ignore[union-attr]

        # The sibling was still refreshed, but nothing was ever selected.
        assert screen.store.model.selected_catalog is None


async def test_second_repo_does_not_steal_app_state_repo_from_the_first(wait_until: Any) -> None:
    # RepoAdded's own first-repository-wins guard -- every other test in
    # this package only ever adds one repository.
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        first = _fake_repository("@ActiveProtectData/repo-1")
        second = _fake_repository("@ActiveProtectData/repo-2")

        screen._apply_discovered([first, second], "/scan")
        await wait_until(pilot, lambda: app.repo_handle is not None)
        assert app.repo_handle is not None
        assert app.resources.repo(app.repo_handle) is first  # still the first repo, not stolen back by the second
        assert len(screen.store.model.repos) == 2


async def test_footer_shows_only_connect_goto_ref_quit_and_help() -> None:
    """``BrowseScreen``'s own footer is deliberately trimmed to
    ``c``/``g``/``q``/``?`` -- every other binding (``d``/``r``/``/``/``t``
    and every nav key) stays fully functional (dispatch is unaffected,
    only ``show`` differs), just not printed here; ``?``'s own help
    screen lists every one of them regardless of ``show``."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        shown = {key for key, active in screen.active_bindings.items() if active.binding.show}
        assert shown == {"c", "g", "q", "question_mark"}


async def test_footer_hidden_bindings_still_dispatch() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        refresh_calls = [0]
        filter_calls = [0]
        screen.action_refresh = lambda: refresh_calls.__setitem__(0, refresh_calls[0] + 1)  # type: ignore[method-assign]
        screen.action_filter = lambda: filter_calls.__setitem__(0, filter_calls[0] + 1)  # type: ignore[method-assign]

        await pilot.press("r")
        await pilot.press("slash")
        assert refresh_calls[0] == 1
        assert filter_calls[0] == 1

        # "d"/"t" resolve on the App (ApmRepoBrowserApp), not this
        # screen -- _FakeApp deliberately doesn't implement them, so
        # dispatch here just needs to not raise (same fallback-through
        # mechanism every screen already relies on for these two keys).
        await pilot.press("d")
        await pilot.press("t")


__all__: list[str] = []
