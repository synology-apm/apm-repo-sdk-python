"""``UnitEffects``: the one place a ``UnitCmd`` actually does anything --
fetches a root/provider, fetches a page of children, closes a
discarded provider, or shows a toast. Constructed once per
``UnitScreen`` instance (unlike ``AppEffects``, which is app-wide), and
handed to that screen's own ``Store`` as its ``perform`` callback.

Takes a plain ``Widget`` plus three narrow callables (``repo``,
``file_table``, ``unit_tree``) rather than the real ``UnitScreen``/
``ApmRepoBrowserApp`` types -- ``runtime/`` must never import from
``screens/`` (the package's own one-directional ``core`` -> ``runtime`` ->
``view`` -> ``screens`` layering), and importing either concrete class
here would be circular (``unit_screen.py`` is what constructs this
class).

Provider close is hosted on the *App*, never the screen: ``Widget.
_on_unmount`` cancels every worker on a node once that node unmounts,
regardless of its own group, so a close worker hosted on the screen
could be cancelled out from under itself the instant the screen
unmounts, mid-close -- exactly the leaked-connection failure mode
closing a provider exists to prevent. ``ApmRepoBrowserApp.on_unmount``
drains (never cancels) ``UNIT_PROVIDER_CLOSE_GROUP`` before closing the
session.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import assert_never

from textual.widget import Widget
from textual.widgets import DataTable, Tree
from textual.worker import Worker

from synology_apm_repo.browser.core.unit.cmd import CloseProvider, LoadChildren, LoadRoot, Notify, UnitCmd
from synology_apm_repo.browser.core.unit.model import UnitModel
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenLoaded,
    ChildrenLoadFailed,
    MoreChildrenLoaded,
    MoreChildrenLoadFailed,
    RootLoaded,
    RootLoadFailed,
    UnitMsg,
)
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.view.reconcile import Binding, find_node
from synology_apm_repo.browser.widgets.progress_hint import DataTableLoadingRowSink, TreeNodeLoadingSink, _LoadingSink
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress, run_worker_with_progress
from synology_apm_repo.sdk.api import Catalog, Repository, Version
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

#: Shared group for every UnitScreen instance's provider-close worker,
#: hosted on the App rather than the screen.
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
    ) -> None:
        self._screen = screen
        self._resources = resources
        self._store = store
        self._catalog = catalog
        self._version = version
        self._repo = repo
        self._file_table = file_table
        self._unit_tree = unit_tree

    def perform(self, cmd: UnitCmd) -> None:
        match cmd:
            case LoadRoot():
                # No explicit sink -- this is the one call site in this
                # package relying on run_worker_with_progress's default
                # breadcrumb sink, which needs self._screen to actually be
                # a NavigableScreen at runtime even though it's typed here
                # as a bare Widget (the runtime -> screens layering ban
                # forbids proving that statically). True for every real
                # UnitScreen; not something the type system can check.
                _worker: Worker[None] = run_worker_with_progress(self._screen, functools.partial(self._load_root, cmd))
            case LoadChildren():
                # Anchored on whichever surface is actually about to show
                # this fetch's own result: the file table when the node
                # being expanded is the currently-selected folder (the
                # common case -- select a folder, wait for its own contents
                # to populate the table) -- but *not* every LoadChildren is for that
                # folder. Expanding a sibling node via `space`/its own
                # disclosure triangle dispatches ChildrenRequested without
                # ever touching model.selected at all -- Textual's
                # NodeExpanded fires for any node whose disclosure state
                # changes, independently of NodeSelected (which only fires
                # on Enter/click and is what actually drives
                # model.selected), so anchoring unconditionally on the file
                # table would show the spinner inside an unrelated folder's
                # own listing while the node actually being expanded shows
                # no feedback at all. Falls back to the expanding tree node
                # itself in that case.
                sink: _LoadingSink

                def _is_selected() -> bool:
                    return cmd.node.ref == self._store.model.selected

                if _is_selected():
                    sink = DataTableLoadingRowSink(self._file_table(), is_current=_is_selected)
                else:
                    tree = self._unit_tree()
                    sink = TreeNodeLoadingSink(find_node(tree.root, cmd.node.ref) or tree.root)
                _worker = run_worker_with_progress(self._screen, functools.partial(self._load_children, cmd), sink=sink)
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
            case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
                assert_never(cmd)

    async def _load_root(self, cmd: LoadRoot) -> None:
        repo = self._repo()
        assert repo is not None
        if cmd.invalidate:
            # A real re-scan, not just re-querying already-cached
            # directory listings: a Session holds one DirCache per open
            # repository for its whole lifetime, and only an explicit
            # invalidate() forces it to re-scan rather than serve the
            # same stale listing forever.
            await repo.invalidate_directory_cache()
        try:
            provider = await self._catalog.provider(self._version, force_raw=cmd.force_raw)
        except ApmRepoError as exc:
            self._store.dispatch(RootLoadFailed(epoch=cmd.epoch, request=cmd.request, message=str(exc)))
            return
        handle = self._resources.put_provider(provider)
        self._store.dispatch(RootLoaded(epoch=cmd.epoch, request=cmd.request, provider=handle, root=provider.root()))

    async def _load_children(self, cmd: LoadChildren) -> None:
        provider = self._resources.provider(cmd.provider)
        if provider is None:  # pragma: no cover - defensive; a handle only exists while its own provider is live
            return
        try:
            children = await provider.children(cmd.node, offset=cmd.offset, limit=cmd.limit)
        except Exception as exc:
            # Broader than ApmRepoError on purpose: a provider can also
            # surface an unexpected failure from a third-party parser it
            # depends on that isn't an ApmRepoError at all.
            self._dispatch_failure(cmd, str(exc))
            return
        self._dispatch_success(cmd, children)

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
