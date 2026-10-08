"""``UnitEffects``: a ``UnitScreen``'s store's ``perform`` -- loads the
root/provider, a page of children or the detail pane's preview/overview,
resolves a goto target, opens a leaf as a unit and hands it to the screen,
closes a discarded provider, or shows a toast.

It takes a plain ``Widget`` plus narrow callables rather than ``UnitScreen``,
which ``runtime/`` can't import (``browser/README.md``).

Provider closes run on the App in ``UNIT_PROVIDER_CLOSE_GROUP`` (a
screen-hosted close would be cancelled when the screen unmounts), which
``ApmRepoBrowserApp.on_unmount`` drains before closing the session.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import assert_never

from textual.widget import Widget
from textual.widgets import DataTable, Tree
from textual.worker import Worker

from synology_apm_repo.browser.core.unit.cmd import (
    CancelDetailFetch,
    CloseProvider,
    LoadChildren,
    LoadListOverview,
    LoadPreview,
    LoadRoot,
    Notify,
    OpenUnit,
    ResolveGoto,
    ShowUnit,
    UnitCmd,
)
from synology_apm_repo.browser.core.unit.model import (
    DETAIL_GROUP,
    GOTO_GROUP,
    UNIT_OPEN_GROUP,
    DetailBody,
    DetailError,
    DetailIdle,
    DetailNote,
    DetailOverview,
    DetailPreview,
    UnitModel,
    UnitPurpose,
    detail_loading,
)
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenLoaded,
    ChildrenLoadFailed,
    DetailResolved,
    GotoFailed,
    GotoNotFound,
    GotoResolved,
    MoreChildrenLoaded,
    MoreChildrenLoadFailed,
    RootLoaded,
    RootLoadFailed,
    UnitMsg,
    UnitOpened,
    UnitOpenFailed,
)
from synology_apm_repo.browser.runtime.list_overview import load_list_overview
from synology_apm_repo.browser.runtime.preview import PreviewError, PreviewNote, PreviewText, load_preview
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.view.reconcile import Binding, find_node
from synology_apm_repo.browser.widgets.progress_hint import DataTableLoadingRowSink, LoadingSink, TreeNodeLoadingSink
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress, run_worker_with_progress
from synology_apm_repo.sdk import (
    Catalog,
    Node,
    NodeRef,
    RawView,
    Repository,
    RestorableUnit,
    Version,
    find_path_with_children,
)

#: The App-hosted worker group of every provider close.
UNIT_PROVIDER_CLOSE_GROUP = "unit-provider-close"


class UnitEffects:
    def __init__(
        self,
        screen: Widget,
        resources: ResourceTable,
        store: Store[UnitModel, UnitMsg, UnitCmd],
        catalog: Catalog,
        version: Version,
        *,
        repo: Callable[[], Repository | None],
        file_table: Callable[[], DataTable[object]],
        unit_tree: Callable[[], Tree[Binding[NodeRef]]],
        detail_sink: Callable[[Callable[[], bool]], LoadingSink],
        show_unit: Callable[[RestorableUnit, UnitPurpose], None],
    ) -> None:
        self._screen = screen
        self._resources = resources
        self._store = store
        self._catalog = catalog
        self._version = version
        self._repo = repo
        self._file_table = file_table
        self._unit_tree = unit_tree
        self._detail_sink = detail_sink
        self._show_unit = show_unit

    def perform(self, cmd: UnitCmd) -> None:
        match cmd:
            case LoadRoot():
                # The default breadcrumb sink: the screen's identity is what
                # is loading. It needs self._screen to be a NavigableScreen.
                _worker: Worker[None] = run_worker_with_progress(self._screen, functools.partial(self._load_root, cmd))
            case LoadChildren():
                # On the file table when the node is the selected folder,
                # else on its tree node: expanding a tree node doesn't
                # select it.
                sink: LoadingSink

                def _is_selected() -> bool:
                    return cmd.node.ref == self._store.model.selected

                if _is_selected():
                    sink = DataTableLoadingRowSink(self._file_table(), is_current=_is_selected)
                else:
                    tree = self._unit_tree()
                    sink = TreeNodeLoadingSink(find_node(tree.root, cmd.node.ref) or tree.root)
                _worker = run_worker_with_progress(self._screen, functools.partial(self._load_children, cmd), sink=sink)
            case LoadPreview():
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_preview, cmd),
                    sink=self._detail_sink(lambda: detail_loading(self._store.model, cmd.request)),
                    group=DETAIL_GROUP,
                    exclusive=True,
                )
            case LoadListOverview():
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_list_overview, cmd),
                    sink=self._detail_sink(lambda: detail_loading(self._store.model, cmd.request)),
                    group=DETAIL_GROUP,
                    exclusive=True,
                )
            case ResolveGoto():
                _worker = run_worker_with_progress(
                    self._screen, functools.partial(self._resolve_goto, cmd), group=GOTO_GROUP, exclusive=True
                )
            case OpenUnit():
                _worker = run_worker_with_progress(
                    self._screen, functools.partial(self._open_unit, cmd), group=UNIT_OPEN_GROUP, exclusive=True
                )
            case ShowUnit(unit=unit, purpose=purpose):
                self._show_unit(unit, purpose)
            case CancelDetailFetch():
                self._screen.workers.cancel_group(self._screen, DETAIL_GROUP)
            case CloseProvider(provider=handle):
                app = self._screen.app
                _worker = run_worker_no_progress(
                    app,
                    functools.partial(self._resources.release_provider, handle),
                    group=UNIT_PROVIDER_CLOSE_GROUP,
                    name=f"close-provider-{handle}",
                )
            case Notify(message=message, severity=severity, title=title):
                self._screen.notify(message, severity=severity, title=title or "")
            case _:
                assert_never(cmd)

    async def _load_root(self, cmd: LoadRoot) -> None:
        repo = self._repo()
        assert repo is not None
        try:
            if cmd.invalidate:
                # Exclusive: the gate waits for the browse screen's loads; this
                # screen's own workers were drained before RootRequested.
                async with self._resources.load_gate.exclusive():
                    await repo.invalidate_caches()
            async with self._resources.load_gate.shared():
                provider = await self._catalog.provider(self._version, raw=RawView() if cmd.force_raw else None)
        except Exception as exc:  # noqa: BLE001
            # Any exception: an uncaught one would leave the root loading.
            self._store.dispatch(RootLoadFailed(epoch=cmd.epoch, request=cmd.request, message=str(exc)))
            return
        handle = self._resources.put_provider(provider, repo)
        self._store.dispatch(RootLoaded(epoch=cmd.epoch, request=cmd.request, provider=handle, root=provider.root()))

    async def _load_children(self, cmd: LoadChildren) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        try:
            children = await provider.children(cmd.node, offset=cmd.offset, limit=cmd.limit)
        except Exception as exc:  # noqa: BLE001
            # Any exception: a provider can raise a third-party parser's own.
            self._dispatch_failure(cmd, str(exc))
            return
        self._dispatch_success(cmd, children)

    async def _load_preview(self, cmd: LoadPreview) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        result = await load_preview(provider, cmd.node, read_limit=cmd.read_limit)
        body: DetailBody
        match result:
            case PreviewText(text=text):
                body = DetailPreview(text)
            case PreviewNote(message=message):
                body = DetailNote(message)
            case PreviewError(message=message):
                body = DetailError(message)
            case None:
                body = DetailIdle()
        self._store.dispatch(DetailResolved(epoch=cmd.epoch, request=cmd.request, body=body))

    async def _load_list_overview(self, cmd: LoadListOverview) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        body: DetailBody
        try:
            overview = await load_list_overview(
                provider,
                cmd.node,
                item_cap=cmd.item_cap,
                read_limit=cmd.read_limit,
                max_concurrent=cmd.max_concurrent,
            )
        except Exception as exc:  # noqa: BLE001
            # Any exception, so the pane never stays loading.
            body = DetailError(str(exc))
        else:
            body = DetailOverview(rows=tuple(overview.rows), truncated=overview.truncated)
        self._store.dispatch(DetailResolved(epoch=cmd.epoch, request=cmd.request, body=body))

    async def _resolve_goto(self, cmd: ResolveGoto) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        try:
            found = await find_path_with_children(provider, cmd.target)
        except Exception as exc:  # noqa: BLE001
            # Any exception: a provider can raise a third-party parser's own.
            self._store.dispatch(GotoFailed(epoch=cmd.epoch, request=cmd.request, message=str(exc)))
            return
        if found is None:
            self._store.dispatch(GotoNotFound(epoch=cmd.epoch, request=cmd.request))
            return
        chain, children_by_step = found
        self._store.dispatch(
            GotoResolved(
                epoch=cmd.epoch,
                request=cmd.request,
                chain=tuple(chain),
                children_by_step=tuple(tuple(children) for children in children_by_step),
            )
        )

    async def _open_unit(self, cmd: OpenUnit) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        try:
            unit = await provider.unit(cmd.node)
        except Exception as exc:  # noqa: BLE001
            # Any exception: unit() can raise a third-party parser's own.
            self._store.dispatch(UnitOpenFailed(epoch=cmd.epoch, request=cmd.request, message=str(exc)))
            return
        self._store.dispatch(UnitOpened(epoch=cmd.epoch, request=cmd.request, unit=unit, purpose=cmd.purpose))

    def _dispatch_failure(self, cmd: LoadChildren, message: str) -> None:
        if cmd.offset:
            self._store.dispatch(
                MoreChildrenLoadFailed(epoch=cmd.epoch, request=cmd.request, ref=cmd.node.ref, message=message)
            )
        else:
            self._store.dispatch(
                ChildrenLoadFailed(epoch=cmd.epoch, request=cmd.request, ref=cmd.node.ref, message=message)
            )

    def _dispatch_success(self, cmd: LoadChildren, children: list[Node]) -> None:
        exhausted = len(children) < cmd.limit
        if cmd.offset:
            self._store.dispatch(
                MoreChildrenLoaded(
                    epoch=cmd.epoch, request=cmd.request, ref=cmd.node.ref, more=tuple(children), exhausted=exhausted
                )
            )
        else:
            self._store.dispatch(
                ChildrenLoaded(
                    epoch=cmd.epoch,
                    request=cmd.request,
                    ref=cmd.node.ref,
                    children=tuple(children),
                    exhausted=exhausted,
                )
            )
