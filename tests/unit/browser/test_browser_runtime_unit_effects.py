"""Unit tests for ``browser.runtime.unit_effects.UnitEffects``, driven
through a bare ``App`` hosting ``#file-table``/``#folder-tree`` and a real
``Store`` running ``core.unit.update``: the round-trip is dispatch -> update
-> perform -> real worker -> dispatch back."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Tree

import synology_apm_repo.sdk.api as _sdk_api
from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, count_progress_ticks, settle, wait_until
from synology_apm_repo.browser.core.unit.cmd import CloseProvider, Notify, UnitCmd
from synology_apm_repo.browser.core.unit.model import (
    DetailError,
    DetailOverview,
    DetailPreview,
    UnitModel,
    UnitPurpose,
)
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenRequested,
    DetailRequested,
    FolderSelected,
    RootRequested,
    UnitMsg,
    UnitOpenRequested,
)
from synology_apm_repo.browser.core.unit.update import update
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.runtime.unit_effects import UnitEffects
from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.browser.widgets import progress_hint
from synology_apm_repo.browser.widgets.progress_hint import LoadingSink
from synology_apm_repo.sdk.api import Catalog, Repository, Session, Version
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import (
    ClosableUnitProvider,
    ContentSource,
    Node,
    NodeRole,
    RestorableUnit,
    UnitProvider,
)
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact
from synology_apm_repo.sdk.units.node_ref import NodeRef


async def _unread_content() -> bytes:
    raise AssertionError("this test never reads a unit's content")


@faithful_to(_sdk_api.Repository)
class _FakeRepo:
    async def invalidate_caches(self, *names: str) -> None:
        pass

    async def release_provider(self, provider: object) -> None:
        if isinstance(provider, ClosableUnitProvider):
            await provider.close()


@faithful_to(ClosableUnitProvider)
class _FakeProvider:
    """A ``ClosableUnitProvider``; it must implement the whole protocol
    (``__aenter__``/``__aexit__`` included), or ``_FakeRepo.release_provider``'s
    ``isinstance`` check silently skips ``close()``."""

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


@faithful_to(_sdk_api.Catalog)
class _FakeCatalog:
    def __init__(self, provider: _FakeProvider | None = None, error: Exception | None = None) -> None:
        self._provider = provider
        self._error = error

    async def provider(self, version: Version, *, raw: object = None) -> _FakeProvider:
        if self._error is not None:
            raise self._error
        assert self._provider is not None
        return self._provider


class _FakeApp(App[None]):
    """Hosts the mounted ``#file-table``/``#folder-tree`` the loading sinks attach to."""

    def compose(self) -> ComposeResult:
        yield DataTable(id="file-table")
        yield Tree("root", id="folder-tree")

    def _set_loading_indicator(self, markup: str | None) -> None:
        """No-op stand-in for ``NavigableScreen._set_loading_indicator``, where a
        root load slow enough to show a spinner (one parked on the load gate) draws it."""


@faithful_to(LoadingSink)
class _NullSink:
    def show(self, frame: str) -> None: ...

    def hide(self) -> None: ...


def _make_store(
    app: App[None],
    catalog: _FakeCatalog,
    repo: _FakeRepo | None,
    version: Version,
    detail_sink: Callable[[Callable[[], bool]], Any] | None = None,
    resources: ResourceTable | None = None,
    show_unit: Callable[[RestorableUnit, UnitPurpose], None] | None = None,
) -> Store[UnitModel, UnitMsg, UnitCmd]:
    """Ties a real ``Store`` to a real ``UnitEffects``; ``effects`` is late-bound because it needs the store."""
    effects: UnitEffects

    def _perform(cmd: UnitCmd) -> None:
        effects.perform(cmd)

    store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, _perform)
    effects = UnitEffects(
        cast(Widget, app),
        resources if resources is not None else ResourceTable(cast(Session, object())),
        store,
        cast(Catalog, catalog),
        version,
        repo=lambda: cast(Repository, repo) if repo is not None else None,
        file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
        unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
        detail_sink=detail_sink or (lambda is_current: _NullSink()),
        show_unit=show_unit or (lambda unit, purpose: None),
    )
    return store


async def test_perform_notify_calls_screen_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            app, "notify", lambda message, *, severity="information", **kw: calls.append((message, severity))
        )
        store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, lambda cmd: None)
        effects = UnitEffects(
            cast(Widget, app),
            ResourceTable(cast(Session, object())),
            store,
            cast(Catalog, _FakeCatalog()),
            make_version(),
            repo=lambda: None,
            file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
            unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
            detail_sink=lambda is_current: _NullSink(),
            show_unit=lambda unit, purpose: None,
        )

        effects.perform(Notify(message="hello", severity="warning"))

        assert calls == [("hello", "warning")]


async def test_perform_close_provider_releases_the_real_handle() -> None:
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
            make_version(),
            repo=lambda: None,
            file_table=lambda: cast("DataTable[object]", app.query_one("#file-table", DataTable)),
            unit_tree=lambda: cast("Tree[Binding[NodeRef]]", app.query_one("#folder-tree", Tree)),
            detail_sink=lambda is_current: _NullSink(),
            show_unit=lambda unit, purpose: None,
        )

        effects.perform(CloseProvider(provider=handle))
        await wait_until(pilot, lambda: provider.close_calls > 0, timeout=SDK_TIMEOUT)

        assert resources.provider(handle) is None


async def test_load_root_success_dispatches_root_loaded() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())

        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        assert store.model.root is root
        assert store.model.provider is not None


async def test_load_root_invalidates_the_repository_caches_when_asked() -> None:
    calls = 0

    @faithful_to(Repository)
    class _CountingRepo:
        async def invalidate_caches(self, *names: str) -> None:
            nonlocal calls
            calls += 1

        async def release_provider(self, provider: object) -> None:
            pass

    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, cast(_FakeRepo, _CountingRepo()), make_version())

        store.dispatch(RootRequested(invalidate=True, force_raw=False))
        await wait_until(pilot, lambda: calls == 1, timeout=SDK_TIMEOUT)


async def test_load_root_failure_dispatches_root_load_failed() -> None:
    catalog = _FakeCatalog(error=ApmRepoError("boom"))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())

        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root_error is not None, timeout=SDK_TIMEOUT)

        assert store.model.root_error == "boom"


async def test_load_root_surfaces_a_non_sdk_error_instead_of_staying_loading() -> None:
    catalog = _FakeCatalog(error=ValueError("raw parser failure"))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())

        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root_error is not None, timeout=SDK_TIMEOUT)

        assert store.model.root_error == "raw parser failure"


async def test_load_root_surfaces_a_failed_cache_invalidation() -> None:
    @faithful_to(Repository)
    class _FailingInvalidateRepo(_FakeRepo):
        async def invalidate_caches(self, *names: str) -> None:
            raise ApmRepoError("invalidate failed")

    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FailingInvalidateRepo(), make_version())

        store.dispatch(RootRequested(invalidate=True, force_raw=False))
        await wait_until(pilot, lambda: store.model.root_error is not None, timeout=SDK_TIMEOUT)

        assert store.model.root_error == "invalidate failed"


async def test_load_children_success_dispatches_children_loaded() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "leaf")), name="leaf", is_leaf=True)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root, children_by_ref={str(root.ref): [leaf]}))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        store.dispatch(ChildrenRequested(node=root))
        await wait_until(pilot, lambda: root.ref in store.model.loaded, timeout=SDK_TIMEOUT)

        assert [c.name for c in store.model.loaded[root.ref].children] == ["leaf"]


async def test_load_children_failure_dispatches_children_load_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    provider = _FakeProvider(root=root)

    async def _raising_children(node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        raise ApmRepoError("children boom")

    monkeypatch.setattr(provider, "children", _raising_children)
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        store.dispatch(ChildrenRequested(node=root))
        await wait_until(pilot, lambda: root.ref in store.model.errors, timeout=SDK_TIMEOUT)

        assert store.model.errors[root.ref] == "children boom"


async def test_an_opened_unit_reaches_the_screen_with_its_purpose(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "file")), name="file", is_leaf=True)
    unit = RestorableUnit(ref=leaf.ref, name="file", is_leaf=True, content=LazyArtifact(_unread_content))
    provider = _FakeProvider(root=root)

    async def _unit(node: Node) -> RestorableUnit:
        return unit

    monkeypatch.setattr(provider, "unit", _unit)
    shown: list[tuple[RestorableUnit, UnitPurpose]] = []
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(
            app,
            _FakeCatalog(provider=provider),
            _FakeRepo(),
            make_version(),
            show_unit=lambda u, p: shown.append((u, p)),
        )
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        store.dispatch(UnitOpenRequested(node=leaf, purpose=UnitPurpose.HEX_PREVIEW))
        await wait_until(pilot, lambda: bool(shown), timeout=SDK_TIMEOUT)

        assert shown == [(unit, UnitPurpose.HEX_PREVIEW)]


async def test_a_unit_that_cannot_be_opened_is_a_warning_not_a_screen(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "file")), name="file", is_leaf=True)
    provider = _FakeProvider(root=root)

    async def _unit(node: Node) -> RestorableUnit:
        raise ValueError("a parser's own error")

    monkeypatch.setattr(provider, "unit", _unit)
    shown: list[object] = []
    warnings: list[str] = []
    app = _FakeApp()
    async with app.run_test() as pilot:
        monkeypatch.setattr(app, "notify", lambda message, **kw: warnings.append(message))
        store = _make_store(
            app, _FakeCatalog(provider=provider), _FakeRepo(), make_version(), show_unit=lambda u, p: shown.append(u)
        )
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        store.dispatch(UnitOpenRequested(node=leaf, purpose=UnitPurpose.EXPORT))
        await wait_until(pilot, lambda: bool(warnings), timeout=SDK_TIMEOUT)

        assert warnings == ["a parser's own error"]
        assert shown == []


async def test_load_children_for_an_unselected_sibling_anchors_the_tree_node_not_the_file_table() -> None:
    """Expanding an unselected node shows its loading indicator on that tree node's label, not the file table."""
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="folder", is_leaf=False)
    gate = asyncio.Event()

    @faithful_to(UnitProvider)
    class _GatedProvider(_FakeProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node.ref == folder.ref:
                await gate.wait()
            return await super().children(node, offset=offset, limit=limit)

    provider = _GatedProvider(root=root, children_by_ref={str(root.ref): [folder]})
    catalog = _FakeCatalog(provider=provider)
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, _FakeRepo(), make_version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)
        assert store.model.selected == root.ref

        tree = app.query_one("#folder-tree", Tree)
        folder_node = tree.root.add(
            "folder", data=Binding(key=folder.ref, label="folder", payload=folder), allow_expand=True
        )

        store.dispatch(ChildrenRequested(node=folder))
        # The indicator appears only after DebouncedProgress's arm delay.
        await wait_until(pilot, lambda: "Loading" in str(folder_node.label), timeout=SDK_TIMEOUT)

        table = app.query_one("#file-table", DataTable)
        assert table.row_count == 0, "an unselected sibling's own fetch must never touch the file table"

        gate.set()
        await wait_until(pilot, lambda: folder.ref in store.model.loaded, timeout=SDK_TIMEOUT)
        assert "Loading" not in str(folder_node.label)


async def test_load_children_for_the_selected_folder_anchors_the_file_table_not_the_tree_node() -> None:
    """Expanding the selected folder shows its loading indicator in the file table, not on the tree node."""
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    gate = asyncio.Event()

    @faithful_to(UnitProvider)
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
        store = _make_store(app, catalog, _FakeRepo(), make_version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)
        assert store.model.selected == root.ref

        root_tree_node = app.query_one("#folder-tree", Tree).root

        store.dispatch(ChildrenRequested(node=root))
        # The indicator appears only after DebouncedProgress's arm delay.
        await wait_until(
            pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]), timeout=SDK_TIMEOUT
        )
        assert "Loading" not in str(root_tree_node.label), "the selected folder's own fetch must not spinner the tree"

        gate.set()
        await wait_until(pilot, lambda: root.ref in store.model.loaded, timeout=SDK_TIMEOUT)


async def test_switching_to_an_already_loaded_folder_mid_fetch_leaves_no_stray_loading_row(monkeypatch: Any) -> None:
    """Folder A's in-flight fetch adds no stray "Loading" row once the user switched to an already-loaded folder B."""
    monkeypatch.setattr(progress_hint, "_FRAME_INTERVAL", 0.02)
    ticks = count_progress_ticks(monkeypatch)
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    folder_a = Node(ref=NodeRef("repo", ("root", "a")), name="a", is_leaf=False)
    folder_b = Node(ref=NodeRef("repo", ("root", "b")), name="b", is_leaf=False)
    gate = asyncio.Event()

    @faithful_to(UnitProvider)
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
        store = _make_store(app, catalog, _FakeRepo(), make_version())
        store.dispatch(RootRequested(invalidate=False, force_raw=False))
        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)

        store.dispatch(FolderSelected(folder=folder_a))
        store.dispatch(ChildrenRequested(node=folder_a))
        await wait_until(
            pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]), timeout=SDK_TIMEOUT
        )

        # Stand in for FileTableView's re-render on selection change.
        store.dispatch(FolderSelected(folder=folder_b))
        table.clear()
        table.add_row("b-item.txt")

        # A's next tick, the one that could re-add its row, has run.
        switched_at = ticks()
        await wait_until(pilot, lambda: ticks() > switched_at, timeout=SDK_TIMEOUT)
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["b-item.txt"], "A's late tick must not leak a stray Loading row onto B's own table"

        gate.set()
        await wait_until(pilot, lambda: folder_a.ref in store.model.loaded, timeout=SDK_TIMEOUT)


# -- detail pane fetches ------------------------------------------------------


@faithful_to(ContentSource)
class _Content:
    """Serves ``data``; given an ``Event``, ``read`` records ``"reading"`` and then
    ``"cancelled"`` in ``cancelled`` as it parks and is cancelled."""

    size = None

    def __init__(self, data: bytes | BaseException | asyncio.Event, cancelled: list[str] | None = None) -> None:
        self._data = data
        self._cancelled = cancelled

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if isinstance(self._data, asyncio.Event):
            if self._cancelled is not None:
                self._cancelled.append("reading")
            try:
                await self._data.wait()  # never set: blocks until cancelled
            except asyncio.CancelledError:
                if self._cancelled is not None:
                    self._cancelled.append("cancelled")
                raise
            return b""
        if isinstance(self._data, BaseException):
            raise self._data
        return self._data


@faithful_to(UnitProvider)
class _ContentProvider(_FakeProvider):
    """A ``_FakeProvider`` whose ``unit()`` serves canned content per ref."""

    def __init__(
        self,
        root: Node,
        children_by_ref: dict[str, list[Node]],
        content: dict[str, bytes | BaseException | asyncio.Event],
        cancelled: list[str] | None = None,
    ):
        super().__init__(root, children_by_ref)
        self._content = content
        self._cancelled = cancelled

    async def unit(self, node: Node) -> Any:
        content = cast(ContentSource, _Content(self._content[str(node.ref)], self._cancelled))
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, content=content)


async def _selected_store(app: App[None], provider: _FakeProvider, **kwargs: Any) -> Store[UnitModel, UnitMsg, UnitCmd]:
    store = _make_store(app, _FakeCatalog(provider=provider), _FakeRepo(), make_version(), **kwargs)
    store.dispatch(RootRequested(invalidate=False, force_raw=False))
    return store


async def test_selecting_a_leaf_resolves_to_its_rendered_preview() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "a")), name="a", is_leaf=True)
    provider = _ContentProvider(
        root, {str(root.ref): [leaf]}, {str(leaf.ref): b"<html><body><p>hello world</p></body></html>"}
    )
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=leaf))
        await wait_until(pilot, lambda: isinstance(store.model.detail.body, DetailPreview), timeout=SDK_TIMEOUT)  # type: ignore[union-attr]

        assert "hello world" in store.model.detail.body.text  # type: ignore[union-attr]


async def test_a_failing_preview_resolves_to_an_error_body() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "a")), name="a", is_leaf=True)
    provider = _ContentProvider(root, {str(root.ref): [leaf]}, {str(leaf.ref): ValueError("unreadable")})
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=leaf))
        await wait_until(pilot, lambda: isinstance(store.model.detail.body, DetailError), timeout=SDK_TIMEOUT)  # type: ignore[union-attr]

        assert store.model.detail.body == DetailError("unreadable")  # type: ignore[union-attr]


async def test_selecting_a_list_group_resolves_to_its_overview_rows() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    group = Node(ref=NodeRef("repo", ("root", "list")), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
    item = Node(ref=NodeRef("repo", ("root", "list", "1")), name="1", is_leaf=True)
    provider = _ContentProvider(
        root,
        {str(root.ref): [group], str(group.ref): [item]},
        {str(item.ref): json.dumps({"Title": "alpha"}).encode()},
    )
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=group))
        await wait_until(pilot, lambda: isinstance(store.model.detail.body, DetailOverview), timeout=SDK_TIMEOUT)  # type: ignore[union-attr]

        body = store.model.detail.body  # type: ignore[union-attr]
        assert isinstance(body, DetailOverview)
        assert len(body.rows) == 1
        assert "alpha" in str(body.rows[0])
        assert body.truncated is False


async def test_a_list_overview_listing_failure_resolves_to_an_error_body() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    group = Node(ref=NodeRef("repo", ("root", "list")), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)

    class _Failing(_ContentProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node is group:
                raise ApmRepoError("listing failed")
            return await super().children(node, offset, limit)

    provider = _Failing(root, {str(root.ref): [group]}, {})
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=group))
        await wait_until(pilot, lambda: isinstance(store.model.detail.body, DetailError), timeout=SDK_TIMEOUT)  # type: ignore[union-attr]

        assert store.model.detail.body == DetailError("listing failed")  # type: ignore[union-attr]


async def test_a_non_sdk_list_overview_failure_also_resolves_to_an_error_body() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    group = Node(ref=NodeRef("repo", ("root", "list")), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)

    class _Failing(_ContentProvider):
        async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
            if node is group:
                raise ValueError("parser exploded")
            return await super().children(node, offset, limit)

    provider = _Failing(root, {str(root.ref): [group]}, {})
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=group))
        await wait_until(pilot, lambda: isinstance(store.model.detail.body, DetailError), timeout=SDK_TIMEOUT)  # type: ignore[union-attr]

        assert store.model.detail.body == DetailError("parser exploded")  # type: ignore[union-attr]


async def test_the_detail_loading_sink_is_current_only_while_its_fetch_is_awaited() -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    leaf = Node(ref=NodeRef("repo", ("root", "a")), name="a", is_leaf=True)
    provider = _ContentProvider(root, {str(root.ref): [leaf]}, {str(leaf.ref): b"<html><body>x</body></html>"})
    currents: list[Callable[[], bool]] = []

    def _sink(is_current: Callable[[], bool]) -> _NullSink:
        currents.append(is_current)
        return _NullSink()

    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider, detail_sink=_sink)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=leaf))
        assert [c() for c in currents] == [True]

        await wait_until(pilot, lambda: not currents[0](), timeout=SDK_TIMEOUT)
        assert store.model.detail is not None


@pytest.mark.parametrize(
    "second",
    [
        pytest.param("leaf", id="a_new_fetching_selection_cancels_the_superseded_fetch"),
        pytest.param("folder", id="a_selection_with_no_fetch_cancels_the_running_fetch"),
    ],
)
async def test_superseded_fetch_is_cancelled(second: str) -> None:
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    slow = Node(ref=NodeRef("repo", ("root", "slow")), name="slow", is_leaf=True)
    other = Node(ref=NodeRef("repo", ("root", "other")), name="other", is_leaf=True)
    folder = Node(ref=NodeRef("repo", ("root", "dir")), name="dir", is_leaf=False)
    cancelled: list[str] = []
    provider = _ContentProvider(
        root,
        {str(root.ref): [slow, other, folder]},
        {str(slow.ref): asyncio.Event(), str(other.ref): b"<html><body>x</body></html>"},
        cancelled,
    )
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = await _selected_store(app, provider)
        await wait_until(pilot, lambda: store.model.provider is not None, timeout=SDK_TIMEOUT)
        store.dispatch(DetailRequested(node=slow))
        await wait_until(pilot, lambda: cancelled == ["reading"], timeout=SDK_TIMEOUT)

        store.dispatch(DetailRequested(node=other if second == "leaf" else folder))
        await wait_until(pilot, lambda: cancelled == ["reading", "cancelled"], timeout=SDK_TIMEOUT)


async def test_a_refresh_waits_for_a_load_another_screen_holds_on_the_shared_gate() -> None:
    calls = 0

    @faithful_to(Repository)
    class _CountingRepo:
        async def invalidate_caches(self, *names: str) -> None:
            nonlocal calls
            calls += 1

        async def release_provider(self, provider: object) -> None:
            pass

    resources = ResourceTable(cast(Session, object()))
    root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False)
    catalog = _FakeCatalog(provider=_FakeProvider(root=root))
    app = _FakeApp()
    async with app.run_test() as pilot:
        store = _make_store(app, catalog, cast(_FakeRepo, _CountingRepo()), make_version(), resources=resources)

        async with resources.load_gate.shared():  # e.g. a browse load still running
            store.dispatch(RootRequested(invalidate=True, force_raw=False))
            await settle(pilot)
            assert calls == 0

        await wait_until(pilot, lambda: store.model.root is not None, timeout=SDK_TIMEOUT)
        assert calls == 1
