"""Unit tests for ``BrowseScreen`` covering branches none of this
package's other ``browse_screen``-focused
test files reach: the loading-indicator markup, the version/tree filter
Enter-to-close paths, ``action_refresh``'s two dispatch branches,
``refresh_for_verbose_mode``, the "nothing selected" tree guard,
``action_go_back``'s filter/goto branches, ``action_show_diagnostics``,
the tree-filter "nothing cached" guard and its workload-level re-render,
and goto-ref's parse-failure/error branches. Driven through a real
Textual ``Pilot`` against ``BrowseScreen`` pushed directly (bypassing
``ApmRepoBrowserApp``'s own auto-opened ``ConnectDialog`` — this
screen's own ``on_mount`` needs neither a repository nor a session, see its
own docstring), same ``_FakeApp`` minimal-host convention as this
package's ``unit_screen`` gap tests."""

from __future__ import annotations

from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Input, Static, Tree

from synology_apm_repo.browser.screens.browse_screen import BrowseScreen, CatalogEntry
from synology_apm_repo.browser.strings import GOTO_REF_NOT_CANONICAL_WARNING
from synology_apm_repo.sdk.api import Catalog, Connection, Repository, Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import CatalogId, ConnectionConfigId, ConnectionId, VersionUid, WorkloadId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.node_ref import NodeRef


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


def _connection() -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(1),
        connection_id=ConnectionId("cc"),
        display_name="Source",
        namespaces=(),
        workload_count=1,
        version_count=1,
    )


def _fake_catalog(connection: Connection) -> Catalog:
    """A minimal ``Catalog`` wrapping ``connection`` — none of this file's
    tests call any of its I/O methods (``workloads``/``versions``/
    ``provider``), so a placeholder ``dedup_repo``/``track``/
    ``require_key_verified`` is enough."""
    return Catalog(
        cast(DedupRepo, object()),
        connection,
        track=lambda provider: provider,
        require_key_verified=lambda: None,
    )


class _FakeApp(App[None]):
    """See ``test_browser_unit_screen_pagination.py``'s own identical
    class for why a bare ``App`` (not ``ApmRepoBrowserApp``) is enough
    here: ``BrowseScreen`` only ever reads ``app_state.repo``/``.verbose``,
    and its own ``on_mount`` needs neither."""

    def __init__(self) -> None:
        super().__init__()
        self.repo: Repository | None = None
        self.verbose = False

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(BrowseScreen())


async def test_set_loading_indicator_appends_markup_to_the_breadcrumb() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen._set_loading_indicator("[dim]spinning[/dim]")
        breadcrumb = str(screen.query_one("#breadcrumb", Static).render())
        assert "spinning" in breadcrumb


async def test_action_refresh_dispatches_to_the_deepest_selected_level() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        calls: list[tuple[str, object]] = []
        screen._load_versions = lambda workload: calls.append(("versions", workload))  # type: ignore[method-assign,assignment,return-value]
        screen._load_workloads = lambda repo, catalog: calls.append(("workloads", catalog))  # type: ignore[method-assign,assignment,return-value]

        repo = _fake_repository()
        catalog = _fake_catalog(_connection())
        workload = Workload(
            workload_id=1,  # type: ignore[arg-type]
            workload_uid="wl-uid",  # type: ignore[arg-type]
            workload_type="VM",
            sub_type=None,
            display_name="Workload",
            subtitle=None,
            spec={},
        )

        # Deepest level selected: a workload -> _load_versions.
        screen._selected_workload = workload
        screen._selected_catalog = CatalogEntry(repo=repo, catalog=catalog)
        screen.action_refresh()
        assert calls == [("versions", workload)]

        # Only a catalog selected -> _load_workloads.
        calls.clear()
        screen._selected_workload = None
        screen.action_refresh()
        assert calls == [("workloads", catalog)]

        # Nothing selected at all -> the third branch, action_connect_remote()
        # (never exercised by this test before) -- the exact same flow ``c`` triggers.
        connect_calls: list[bool] = []
        screen.action_connect_remote = lambda: connect_calls.append(True)  # type: ignore[method-assign]
        calls.clear()
        screen._selected_catalog = None
        screen.action_refresh()
        assert calls == []
        assert connect_calls == [True]


async def test_refresh_for_verbose_mode_relabels_repo_nodes() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        repo = _fake_repository()
        screen._add_repo(repo)
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
        screen._version_filter_active = True
        screen.query_one("#filter-input", Input).add_class("active")

        screen.action_go_back()
        await pilot.pause()
        assert screen._version_filter_active is False
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

        app.repo = _fake_repository()
        app.repo.verify = fake_verify  # type: ignore[method-assign,assignment]
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


async def test_tree_filter_rerenders_workload_level_via_the_group_cache() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-workloads", Tree)
        group_node = tree.root.add("VM", data="VM")
        workloads = [
            Workload(
                workload_id=i,  # type: ignore[arg-type]
                workload_uid=f"wl-{i}",  # type: ignore[arg-type]
                workload_type="VM",
                sub_type=None,
                display_name=f"Workload-{i}",
                subtitle=None,
                spec={},
            )
            for i in range(2)
        ]
        screen._workloads_by_group_node[id(group_node)] = workloads

        screen._tree_filter_parent = group_node
        screen._tree_filter_text = "Workload-1"
        screen._rerender_tree_filter(group_node)
        await pilot.pause()
        assert [c.data.display_name for c in group_node.children] == ["Workload-1"]  # type: ignore[union-attr]


async def test_submit_goto_parse_failure_returns_without_resolving() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen._submit_goto("/some/path#not-canonical")
        assert warnings == [GOTO_REF_NOT_CANONICAL_WARNING]


async def test_submit_goto_version_lookup_failure_notifies() -> None:
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
        app.repo = repo

        ref = NodeRef.canonical(
            "repo", catalog_id=CatalogId("1"), workload_id=WorkloadId(1), version_uid=VersionUid("v1")
        )
        await screen._submit_goto(str(ref))
        assert warnings == ["ref not found"]


async def test_enter_on_the_filter_input_closes_an_open_tree_filter() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.add("repo", data=_fake_repository())
        screen._catalogs_by_repo_node[id(repo_node)] = []
        screen._tree_filter_parent = repo_node
        filter_input = screen.query_one("#filter-input", Input)
        filter_input.add_class("active")

        await screen.on_input_submitted(Input.Submitted(filter_input, ""))
        await pilot.pause()
        assert screen._tree_filter_parent is None
        assert not filter_input.has_class("active")


async def test_set_workloads_saas_only_sets_first_group_node_in_the_tenant_loop() -> None:
    """With no device-type workloads at all, ``first_group_node`` can
    only ever get set inside the SaaS platform/tenant nested loop —
    ``test_browser_browse_screen_grouping.py`` covers ``_group_workloads``
    itself; this is the rendering layer directly above it that actually
    reaches this specific assignment."""
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
        screen._set_workloads([workload])
        await pilot.pause()
        tree = screen.query_one("#col-workloads", Tree)
        assert tree.cursor_node is not None
        assert tree.cursor_node.data is not None  # landed on the workload leaf, not left on root


async def test_render_versions_filter_excludes_non_matching_names() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        screen._version_names = [(0, "Monday backup"), (1, "Tuesday backup")]
        screen._version_filter_active = True
        screen._version_filter_text = "monday"
        screen._render_versions()
        table = screen.query_one("#col-versions", DataTable)
        assert table.row_count == 1
        assert screen._visible_version_indices == [0]


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


async def test_catalogs_fetch_error_shows_an_error_leaf_under_that_repo_node(wait_until: Any) -> None:
    # _load_catalogs_for's own ApmRepoError branch -- every other repository
    # stays fully usable regardless of this one repository's own failure
    # (module comment), but no test exercised the error leaf itself.
    class _FailingRepo:
        async def catalogs(self) -> list[Catalog]:
            raise ApmRepoError("boom")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.add("repo-1", data=_FailingRepo())

        screen._load_catalogs_for(repo_node, _FailingRepo())  # type: ignore[arg-type]
        await wait_until(pilot, lambda: bool(repo_node.children), timeout=0.6, interval=0.02)
        assert len(repo_node.children) == 1
        assert "error: boom" in str(repo_node.children[0].label)


async def test_workloads_fetch_error_shows_an_error_leaf_in_column_2(wait_until: Any) -> None:
    # _load_workloads's own generic ApmRepoError branch -- distinct from
    # its KeyRequiredError/KeyMismatchError branch (already covered by
    # tests/integration/browser/test_browser_pilot.py's own
    # encrypted-repository scenario) -- no test exercised this error leaf itself.
    class _FailingCatalog:
        async def workloads(self) -> list[Workload]:
            raise ApmRepoError("boom")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        screen._load_workloads(_fake_repository(), _FailingCatalog())  # type: ignore[arg-type]
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=0.6, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_is_gone(
    wait_until: Any,
) -> None:
    """A narrow race — the catalog just key-verified disappears before
    ``repo.catalog_by_id()`` is re-fetched (a repeat scan/refresh
    mid-flight) — must report it the same way ``_load_workloads``'s own
    ``ApmRepoError`` branch does, not silently leave column 2/3 on stale
    data with no indication anything went wrong."""

    class _EmptyCatalogsRepo:
        async def catalog_by_id(self, catalog_id: CatalogId) -> Catalog | None:
            return None

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        screen._reload_workloads_with_fresh_catalog(_EmptyCatalogsRepo(), CatalogId("gone"))  # type: ignore[arg-type]
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=0.6, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error:" in str(tree.root.children[0].label) and "gone" in str(tree.root.children[0].label)


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_fails_to_open(
    wait_until: Any,
) -> None:
    """A narrower race than the "gone" case above — the catalog just
    key-verified now raises instead of resolving to ``None`` (e.g. a
    repeat scan found it newly corrupt) — must report it too, not
    propagate out of this ``@work`` worker uncaught."""

    class _FailingCatalogById:
        async def catalog_by_id(self, catalog_id: CatalogId) -> Catalog | None:
            raise ApmRepoError("boom")

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)

        screen._reload_workloads_with_fresh_catalog(_FailingCatalogById(), CatalogId("anything"))  # type: ignore[arg-type]
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=0.6, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


async def test_reselecting_the_same_catalog_after_a_key_reload_uses_the_fresh_one(wait_until: Any) -> None:
    """``Repository.set_key()`` closes and replaces every already-opened
    ``DedupRepo`` on success — ``_reload_workloads_with_fresh_catalog``
    exists specifically so this screen stops using the resulting stale
    ``Catalog`` reference, but ``self._selected_catalog`` isn't the only
    place one was cached: ``_catalogs_by_repo_node`` (what a later ``/``
    filter rebuilds column 1's leaves from) and the currently-rendered
    leaf's own ``TreeNode.data`` (what re-selecting that exact node reads)
    both need the same fix. Nor is the catalog that triggered ``KeyDialog``
    the only stale reference: for ``RepoKind.OBJECT_STORE``, ``catalogs()``
    already opened every sibling's own ``DedupRepo`` eagerly before any
    key was entered, so ``set_key()`` closed and replaced every sibling's
    too — this confirms a *second* catalog under the same repository node, never
    itself selected, still gets its own leaf/cache entry refreshed."""

    class _FakeCatalogWithId:
        def __init__(self, catalog_id: CatalogId) -> None:
            self.catalog_id = catalog_id
            self.display_name = "catalog"

        async def workloads(self) -> list[Workload]:
            return []

    stale = _FakeCatalogWithId(CatalogId("cat-1"))
    fresh = _FakeCatalogWithId(CatalogId("cat-1"))
    stale_other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh_other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh_by_id = {CatalogId("cat-1"): fresh, CatalogId("cat-other"): fresh_other}
    # A real Repository (breadcrumb rendering needs a real .layout), with
    # catalog_by_id overridden to return each id's own fresh fake -- the
    # same override style test_reload_workloads_with_fresh_catalog_reports_
    # an_error_when_the_catalog_is_gone's own duck-typed repository uses, just on
    # a real Repository instance instead of a bare duck-typed class, since
    # this test also exercises the (real) breadcrumb path.
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> _FakeCatalogWithId:
        return fresh_by_id[catalog_id]

    repo.catalog_by_id = _catalog_by_id  # type: ignore[assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        # A second repository node with nothing cached at all yet -- exercises
        # _replace_cached_catalog's own "skip a repository node with no cached
        # catalogs" branch.
        tree.root.add("repo-uncached", data=_fake_repository("@ActiveProtectData/repo-2"))
        repo_node = tree.root.add("repo-1", data=repo)
        # A sibling catalog with a *different* id under the same repository node
        # -- never itself selected, but still opened (unkeyed) by the same
        # earlier catalogs() sweep and so equally closed-and-replaced by
        # set_key() -- confirms it gets refreshed too, not left stale.
        other_leaf = repo_node.add_leaf(
            "catalog-other", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale_other))
        )
        leaf = repo_node.add_leaf("catalog-1", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale)))
        screen._catalogs_by_repo_node[id(repo_node)] = [cast(Catalog, stale_other), cast(Catalog, stale)]

        screen._reload_workloads_with_fresh_catalog(repo, CatalogId("cat-1"))
        await wait_until(pilot, lambda: screen._catalogs_by_repo_node[id(repo_node)][1] is cast(Catalog, fresh))

        # The tree leaf itself was fixed, not just self._selected_catalog.
        assert leaf.data is not None
        assert leaf.data.catalog is cast(Catalog, fresh)
        # The sibling that never triggered KeyDialog was refreshed too --
        # set_key() closed its DedupRepo exactly as it did the
        # selected one's, so leaving it stale would leak that closed
        # connection's own aiosqlite thread the next time it's selected.
        assert screen._catalogs_by_repo_node[id(repo_node)][0] is cast(Catalog, fresh_other)
        assert other_leaf.data is not None
        assert other_leaf.data.catalog is cast(Catalog, fresh_other)

        # Re-selecting the same leaf (the user clicking the same catalog
        # node again later) must now see the fresh catalog, not the stale
        # one the leaf originally carried.
        await screen._on_catalog_tree_selected(leaf.data)
        assert screen._selected_catalog is not None
        assert screen._selected_catalog.catalog is cast(Catalog, fresh)


async def test_replace_cached_catalog_does_not_cross_repo_boundaries_on_a_catalog_id_collision() -> None:
    """``CatalogId`` falls back to ``str(connection_config_id)`` for a
    ``RepoKind.VAULT`` (``repo_id`` is always ``None`` there, see
    ``resolve_catalog_id``) — a per-repository local autoincrement id with no
    cross-repository uniqueness guarantee, so two independently opened repositories can
    legitimately share the same ``catalog_id`` string. Matching by
    ``catalog_id`` alone (the bug this fix closes) would silently overwrite
    an unrelated repository's own cached ``Catalog`` on a collision and return
    before the intended repository's own stale entry was ever reached — this
    confirms neither happens, with the colliding, unrelated repository node
    ordered *first* in the tree (the exact ordering that would have
    triggered the bug)."""

    class _FakeCatalogWithId:
        def __init__(self, catalog_id: CatalogId) -> None:
            self.catalog_id = catalog_id

    repo_a = _fake_repository("@ActiveProtectData/repo-a")
    repo_b = _fake_repository("@ActiveProtectData/repo-b")
    stale_a = _FakeCatalogWithId(CatalogId("1"))
    fresh_a = _FakeCatalogWithId(CatalogId("1"))
    catalog_b = _FakeCatalogWithId(CatalogId("1"))  # same id as repo_a's, different repository -- a genuine collision

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        # repo_b's node comes first in iteration order -- if the fix
        # didn't check repo_node.data is repo, its colliding catalog is
        # the one that would get silently overwritten instead of repo_a's.
        b_node = tree.root.add("repo-b", data=repo_b)
        b_leaf = b_node.add_leaf("catalog-b", data=CatalogEntry(repo=repo_b, catalog=cast(Catalog, catalog_b)))
        screen._catalogs_by_repo_node[id(b_node)] = [cast(Catalog, catalog_b)]

        a_node = tree.root.add("repo-a", data=repo_a)
        a_leaf = a_node.add_leaf("catalog-a", data=CatalogEntry(repo=repo_a, catalog=cast(Catalog, stale_a)))
        screen._catalogs_by_repo_node[id(a_node)] = [cast(Catalog, stale_a)]

        screen._replace_cached_catalog(repo_a, CatalogId("1"), cast(Catalog, fresh_a))

        # repo_a's own entry was actually fixed.
        assert screen._catalogs_by_repo_node[id(a_node)][0] is cast(Catalog, fresh_a)
        assert a_leaf.data is not None
        assert a_leaf.data.catalog is cast(Catalog, fresh_a)
        # repo_b's own, unrelated entry is completely untouched.
        assert screen._catalogs_by_repo_node[id(b_node)][0] is cast(Catalog, catalog_b)
        assert b_leaf.data is not None
        assert b_leaf.data.catalog is cast(Catalog, catalog_b)


async def test_replace_cached_catalog_skips_a_matching_repo_node_with_nothing_cached_yet() -> None:
    """A repository node whose ``.data is repo`` matches, but which has no
    ``_catalogs_by_repo_node`` entry at all yet (column 1 expanded but its
    own ``catalogs()`` load never finished, or hasn't been expanded) --
    must be skipped without raising, same as a non-matching repository node."""
    repo = _fake_repository()
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        tree.root.add("repo-1", data=repo)  # matches repo, but nothing cached under it

        screen._replace_cached_catalog(repo, CatalogId("cat-1"), cast(Catalog, object()))  # must not raise


class _FakeCatalogWithId:
    """Shared by the ``_reload_workloads_with_fresh_catalog`` sibling
    tests below -- a ``Catalog`` duck-type carrying only what
    ``_load_workloads``/``_replace_cached_catalog`` actually touch."""

    def __init__(self, catalog_id: CatalogId) -> None:
        self.catalog_id = catalog_id
        self.display_name = "catalog"

    async def workloads(self) -> list[Workload]:
        return []


async def test_reload_refreshes_the_target_even_when_a_sibling_fails_to_refetch(wait_until: Any) -> None:
    """A sibling catalog's own ``catalog_by_id()`` re-fetch raising (a
    narrow race — found newly corrupt mid-flight) is best-effort: it must
    not abort refreshing the catalog that actually triggered this reload,
    unlike ``catalog_id``'s own failure (the strict "reports an error"
    tests above)."""
    stale = _FakeCatalogWithId(CatalogId("cat-1"))
    fresh = _FakeCatalogWithId(CatalogId("cat-1"))
    stale_other = _FakeCatalogWithId(CatalogId("cat-other"))
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> _FakeCatalogWithId:
        if catalog_id == CatalogId("cat-other"):
            raise ApmRepoError("sibling boom")
        return fresh

    repo.catalog_by_id = _catalog_by_id  # type: ignore[assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.add("repo-1", data=repo)
        other_leaf = repo_node.add_leaf(
            "catalog-other", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale_other))
        )
        repo_node.add_leaf("catalog-1", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale)))
        screen._catalogs_by_repo_node[id(repo_node)] = [cast(Catalog, stale_other), cast(Catalog, stale)]

        screen._reload_workloads_with_fresh_catalog(repo, CatalogId("cat-1"))
        await wait_until(pilot, lambda: screen._selected_catalog is not None)

        assert screen._selected_catalog is not None
        assert screen._selected_catalog.catalog is cast(Catalog, fresh)
        # the sibling's own failure left its cache entry/leaf untouched, not crashed.
        assert screen._catalogs_by_repo_node[id(repo_node)][0] is cast(Catalog, stale_other)
        assert other_leaf.data is not None
        assert other_leaf.data.catalog is cast(Catalog, stale_other)


async def test_reload_refreshes_the_target_even_when_a_sibling_is_gone(wait_until: Any) -> None:
    """Same best-effort posture, the ``catalog_by_id()`` returns ``None``
    ("gone already") shape instead of raising."""
    stale = _FakeCatalogWithId(CatalogId("cat-1"))
    fresh = _FakeCatalogWithId(CatalogId("cat-1"))
    stale_other = _FakeCatalogWithId(CatalogId("cat-other"))
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> _FakeCatalogWithId | None:
        if catalog_id == CatalogId("cat-other"):
            return None
        return fresh

    repo.catalog_by_id = _catalog_by_id  # type: ignore[assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.add("repo-1", data=repo)
        other_leaf = repo_node.add_leaf(
            "catalog-other", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale_other))
        )
        repo_node.add_leaf("catalog-1", data=CatalogEntry(repo=repo, catalog=cast(Catalog, stale)))
        screen._catalogs_by_repo_node[id(repo_node)] = [cast(Catalog, stale_other), cast(Catalog, stale)]

        screen._reload_workloads_with_fresh_catalog(repo, CatalogId("cat-1"))
        await wait_until(pilot, lambda: screen._selected_catalog is not None)

        assert screen._selected_catalog is not None
        assert screen._selected_catalog.catalog is cast(Catalog, fresh)
        assert screen._catalogs_by_repo_node[id(repo_node)][0] is cast(Catalog, stale_other)
        assert other_leaf.data is not None
        assert other_leaf.data.catalog is cast(Catalog, stale_other)


async def test_reload_is_a_noop_when_the_triggering_catalog_is_no_longer_in_the_cached_list(
    wait_until: Any,
) -> None:
    """A narrower race still: by the time this reload runs, the catalog
    that actually triggered it is no longer among this repository node's own
    cached catalogs at all (e.g. a concurrent ``/`` filter or rescan
    replaced the list) -- every sibling still gets its own best-effort
    refresh, but there is no catalog left to load workloads for, so this
    quietly does nothing further rather than loading a stale/wrong
    catalog's workloads."""
    other = _FakeCatalogWithId(CatalogId("cat-other"))
    fresh_other = _FakeCatalogWithId(CatalogId("cat-other"))
    repo = _fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> _FakeCatalogWithId:
        return fresh_other

    repo.catalog_by_id = _catalog_by_id  # type: ignore[assignment]

    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.add("repo-1", data=repo)
        repo_node.add_leaf("catalog-other", data=CatalogEntry(repo=repo, catalog=cast(Catalog, other)))
        screen._catalogs_by_repo_node[id(repo_node)] = [cast(Catalog, other)]  # no "cat-1" entry at all

        screen._reload_workloads_with_fresh_catalog(repo, CatalogId("cat-1"))
        await wait_until(pilot, lambda: screen._catalogs_by_repo_node[id(repo_node)][0] is cast(Catalog, fresh_other))

        # the sibling was still refreshed, but nothing was ever selected/loaded.
        assert screen._selected_catalog is None


async def test_second_repo_does_not_steal_app_state_repo_from_the_first() -> None:
    # _add_repo's own first-repository-wins guard (_auto_selected_repo) --
    # every other test in this package only ever adds one repository.
    app = _FakeApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, BrowseScreen)
        first = _fake_repository("@ActiveProtectData/repo-1")
        second = _fake_repository("@ActiveProtectData/repo-2")

        screen._add_repo(first)
        assert app.repo is first

        screen._add_repo(second)
        assert app.repo is first  # still the first repository, not stolen back by the second
        assert len(screen._repos) == 2


__all__: list[str] = []
