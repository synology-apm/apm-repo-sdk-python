"""Unit tests for ``browser.runtime.unit_effects.UnitEffects`` — driven
against a bare ``App`` hosting a real ``#file-table``/``#folder-tree``
pair (``DataTableLoadingRowSink``/``TreeNodeLoadingSink`` each need a
real, mounted widget; nothing here needs ``UnitScreen`` specifically) and
a real ``Store`` running the real ``core.unit.update``, so this proves
the whole round-trip (dispatch -> update -> perform -> real worker ->
dispatch back) works when ``UnitEffects`` is addressed directly by name,
not just indirectly through a screen's own ``on_mount()``. Same
convention as ``test_browser_runtime_app_effects.py``."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Tree

from synology_apm_repo.browser.core.unit.cmd import CloseProvider, Notify, UnitCmd
from synology_apm_repo.browser.core.unit.model import UnitModel
from synology_apm_repo.browser.core.unit.msg import ChildrenRequested, FolderSelected, RootRequested, UnitMsg
from synology_apm_repo.browser.core.unit.update import update
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.runtime.unit_effects import UnitEffects
from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.browser.widgets import progress_hint
from synology_apm_repo.sdk.api import Catalog, Repository, Session, Version
from synology_apm_repo.sdk.errors import ApmRepoError
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


class _FakeRepo:
    async def invalidate_directory_cache(self) -> None:
        pass


class _FakeProvider:
    """A ``ClosableUnitProvider`` -- ``__aenter__``/``__aexit__`` are part
    of that protocol too (not just ``root``/``children``/``unit``/
    ``close``), and ``ResourceTable.release_provider``'s own
    ``isinstance`` check silently treats a provider missing any one of
    them as a *non*-closable ``UnitProvider`` instead of raising, so an
    incomplete fake here would fail its own test by never calling
    ``close()`` at all rather than by erroring loudly."""

    def __init__(self, root: Node, children_by_ref: dict[str, list[Node]] | None = None) -> None:
        self._root = root
        self._children_by_ref = children_by_ref or {}
        self.close_calls = 0

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        items = self._children_by_ref.get(str(node.ref), [])
        return items[offset : offset + limit] if limit is not None else items[offset:]

    async def unit(self, node: Node) -> Any:  # pragma: no cover - never exercised here
        raise NotImplementedError

    async def close(self) -> None:
        self.close_calls += 1

    async def __aenter__(self) -> _FakeProvider:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class _FakeCatalog:
    def __init__(self, provider: _FakeProvider | None = None, error: ApmRepoError | None = None) -> None:
        self._provider = provider
        self._error = error

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _FakeProvider:
        if self._error is not None:
            raise self._error
        assert self._provider is not None
        return self._provider


class _FakeApp(App[None]):
    """Hosts a real ``#file-table``/``#folder-tree`` -- ``_load_children``'s
    own ``DataTableLoadingRowSink``/``TreeNodeLoadingSink`` each need a
    real, mounted widget, and ``DebouncedProgress``'s own ``run_worker``
    call needs a real, running App regardless."""

    def compose(self) -> ComposeResult:
        yield DataTable(id="file-table")
        yield Tree("root", id="folder-tree")


def _make_store(
    app: App[None], catalog: _FakeCatalog, repo: _FakeRepo | None, version: Version
) -> Store[UnitModel, UnitMsg, UnitCmd]:
    """Ties a real ``Store`` to a real ``UnitEffects`` -- resolved via
    ordinary closure late-binding, the same trick
    ``test_browser_runtime_app_effects.py``'s own ``_make_store`` uses
    for ``AppEffects``."""
    effects: UnitEffects

    def _perform(cmd: UnitCmd) -> None:
        effects.perform(cmd)

    store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, _perform)
    effects = UnitEffects(
        cast(Widget, app),
        ResourceTable(cast(Session, object())),
        store,
        cast(Catalog, catalog),
        version,
        repo=lambda: cast(Repository, repo) if repo is not None else None,
        file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
        unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
    )
    return store


async def test_perform_notify_calls_screen_notify() -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[str, str]] = []
        app.notify = lambda message, *, severity="information", **kw: calls.append((message, severity))  # type: ignore[method-assign]
        store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, lambda cmd: None)
        effects = UnitEffects(
            cast(Widget, app),
            ResourceTable(cast(Session, object())),
            store,
            cast(Catalog, _FakeCatalog()),
            _version(),
            repo=lambda: None,
            file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
            unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
        )

        effects.perform(Notify(message="hello", severity="warning"))

        assert calls == [("hello", "warning")]


async def test_perform_close_provider_releases_the_real_handle(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        provider = _FakeProvider(root=Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False))
        handle = resources.put_provider(provider)
        store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, lambda cmd: None)
        effects = UnitEffects(
            cast(Widget, app),
            resources,
            store,
            cast(Catalog, _FakeCatalog()),
            _version(),
            repo=lambda: None,
            file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
            unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
        )

        effects.perform(CloseProvider(provider=handle))
        await wait_until(pilot, lambda: provider.close_calls > 0)

        assert resources.provider(handle) is None


async def test_load_root_success_dispatches_root_loaded(wait_until: Any) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), _version())

        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)

        assert store.model.root is root
        assert store.model.provider is not None


async def test_load_root_invalidates_the_directory_cache_when_asked() -> None:
    calls = 0

    class _CountingRepo:
        async def invalidate_directory_cache(self) -> None:
            nonlocal calls
            calls += 1

    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, cast(_FakeRepo, _CountingRepo()), _version())

        store.dispatch(RootRequested(invalidate=True, force_raw=False))
        await pilot.pause()
        await pilot.pause()

        assert calls == 1


async def test_load_root_failure_dispatches_root_load_failed(wait_until: Any) -> None:
    catalog = _FakeCatalog(error=ApmRepoError("boom"))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), _version())

        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root_error is not None)

        assert store.model.root_error == "boom"


async def test_load_children_success_dispatches_children_loaded(wait_until: Any) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "leaf")), name="leaf", is_leaf=True)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root, children_by_ref={str(root.ref): [leaf]}))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), _version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)

        store.dispatch(ChildrenRequested(node=root))
        await wait_until(pilot, lambda: root.ref in store.model.loaded)

        assert [c.name for c in store.model.loaded[root.ref].children] == ["leaf"]


async def test_load_children_failure_dispatches_children_load_failed(wait_until: Any) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    provider = _FakeProvider(root=root)

    async def _raising_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        raise ApmRepoError("children boom")

    provider.children = _raising_children  # type: ignore[method-assign]
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), _version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)

        store.dispatch(ChildrenRequested(node=root))
        await wait_until(pilot, lambda: root.ref in store.model.errors)

        assert store.model.errors[root.ref] == "children boom"


async def test_load_children_for_an_unselected_sibling_anchors_the_tree_node_not_the_file_table(
    wait_until: Any, sdk_timeout: float
) -> None:
    """The loading indicator for a ``LoadChildren`` fetch must anchor on
    whichever surface is actually about to show its own result -- the
    file table only when the node being expanded is the currently
    *selected* folder. Expanding a different, unselected node (e.g. via
    ``space`` on a sibling in the folder tree, which dispatches
    ``ChildrenRequested`` keyed on the expanded tree node itself and never
    touches ``model.selected`` at all) must anchor on that tree node's own
    label instead, never the file table showing an unrelated folder's own
    contents."""
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="folder", is_leaf=False)
    gate = asyncio.Event()

    class _GatedProvider(_FakeProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == folder.ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root=root, children_by_ref={str(root.ref): [folder]})
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), _version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)
        assert store.model.selected == root.ref  # "folder" itself is not selected

        tree = app.query_one("#folder-tree", Tree)
        folder_node = tree.root.add(
            "folder", data=Binding(key=folder.ref, label="folder", payload=folder), allow_expand=True
        )

        store.dispatch(ChildrenRequested(node=folder))
        # DebouncedProgress only starts animating past its own 300ms arm
        # delay -- this waits out that real delay via polling.
        await wait_until(pilot, lambda: "Loading" in str(folder_node.label), timeout=sdk_timeout)

        table = app.query_one("#file-table", DataTable)
        assert table.row_count == 0, "an unselected sibling's own fetch must never touch the file table"

        gate.set()
        await wait_until(pilot, lambda: folder.ref in store.model.loaded)
        assert "Loading" not in str(folder_node.label)


async def test_load_children_for_the_selected_folder_anchors_the_file_table_not_the_tree_node(
    wait_until: Any,
) -> None:
    """The common-case counterpart of the sibling test above: expanding
    the currently *selected* folder itself (the ordinary "select a
    folder, wait for its own contents to populate the table" path) must
    anchor the loading indicator on the file table, never the tree
    node's own label."""
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    gate = asyncio.Event()

    class _GatedProvider(_FakeProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == root.ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root=root, children_by_ref={str(root.ref): []})
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        table = app.query_one("#file-table", DataTable)
        table.add_column("Name")
        store = _make_store(app, catalog, _FakeRepo(), _version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)
        assert store.model.selected == root.ref

        root_tree_node = app.query_one("#folder-tree", Tree).root

        store.dispatch(ChildrenRequested(node=root))
        # DebouncedProgress only starts animating past its own 300ms arm
        # delay -- this waits out that real delay via polling.
        await wait_until(pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]))
        assert "Loading" not in str(root_tree_node.label), "the selected folder's own fetch must not spinner the tree"

        gate.set()
        await wait_until(pilot, lambda: root.ref in store.model.loaded)


async def test_switching_to_an_already_loaded_folder_mid_fetch_leaves_no_stray_loading_row(
    wait_until: Any, sdk_timeout: float, monkeypatch: Any
) -> None:
    """Regression test for the stale-loading-row bug: folder A's own
    ``ChildrenRequested`` fetch is still in flight (and, by design, left
    running rather than cancelled -- its result is still valid, cacheable
    model data) when the user switches to folder B, whose own contents
    are already loaded. A's next animation tick must not re-append a
    stray "Loading" row onto B's now-displayed table."""
    monkeypatch.setattr(progress_hint, "_FRAME_INTERVAL", 0.02)
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    folder_a = Node(ref=NodeRef("repo", ("root", "a")), name="a", is_leaf=False)
    folder_b = Node(ref=NodeRef("repo", ("root", "b")), name="b", is_leaf=False)
    gate = asyncio.Event()

    class _GatedProvider(_FakeProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == folder_a.ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root=root, children_by_ref={str(root.ref): [folder_a, folder_b]})
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        table = app.query_one("#file-table", DataTable)
        table.add_column("Name")
        store = _make_store(app, catalog, _FakeRepo(), _version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None)

        store.dispatch(FolderSelected(ref=folder_a.ref))
        store.dispatch(ChildrenRequested(node=folder_a))
        await wait_until(
            pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]), timeout=sdk_timeout
        )

        # Switch to folder B, already loaded elsewhere -- simulates the
        # real FileTableView's own render reaction to model.selected
        # changing, which this bare-Effects test doesn't wire up itself.
        store.dispatch(FolderSelected(ref=folder_b.ref))
        table.clear()
        table.add_row("b-item.txt")

        # A's own fetch is still gated, so no readiness signal exists for
        # "no more ticks will ever touch this table again" -- this waits a
        # fixed, short interval (several sped-up _FRAME_INTERVAL ticks) to
        # prove the absence of a stray row.
        await asyncio.sleep(0.1)
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["b-item.txt"], "A's late tick must not leak a stray Loading row onto B's own table"

        gate.set()
        await wait_until(pilot, lambda: folder_a.ref in store.model.loaded)


__all__: list[str] = []
