"""``BrowseEffects``: the one place a ``BrowseCmd`` actually does
anything -- fetches catalogs/workloads/versions, closes discarded
repositories, sets the current repository, prompts for a key, or shows a
toast. Constructed once (there is exactly one ``BrowseScreen``), handed
to its ``Store`` as its ``perform`` callback.

Takes a plain ``Widget`` plus narrow callables rather than the real
``BrowseScreen``/``ApmRepoBrowserApp`` types, per this package's
``core``->``runtime``->``view``->``screens`` import layering (see
``browser/README.md``); importing either concrete class here would also
be circular.

Repository close is hosted on the App, never the screen, so a rescan's
``CloseRepos`` always has a host that outlives the dispatch even though
this root screen never actually unmounts. ``ApmRepoBrowserApp.on_unmount``
drains (never cancels) ``BROWSE_REPO_CLOSE_GROUP`` before closing the
session.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from typing import Any, assert_never

from textual.widget import Widget
from textual.widgets import DataTable, Tree
from textual.worker import Worker

from synology_apm_repo.browser.core.browse.cmd import (
    BrowseCmd,
    CloseRepos,
    LoadCatalogsFor,
    LoadVersions,
    LoadWorkloads,
    Notify,
    PromptForKey,
    ReloadCatalogsAfterKeyVerified,
    SetCurrentRepo,
)
from synology_apm_repo.browser.core.browse.model import BrowseModel, catalog_key, workload_key
from synology_apm_repo.browser.core.browse.msg import (
    BrowseMsg,
    CatalogSelected,
    CatalogsLoaded,
    CatalogsLoadFailed,
    CatalogsRefreshed,
    CatalogsRefreshFailed,
    KeyVerified,
    RepoKeyStatusRefreshed,
    VersionsLoaded,
    VersionsLoadFailed,
    WorkloadsLoaded,
    WorkloadsLoadFailed,
)
from synology_apm_repo.browser.core.browse.select import is_workload_current
from synology_apm_repo.browser.core.keys import CatalogKey, RepoHandle, WorkloadKey
from synology_apm_repo.browser.core.remote_data import FailureInfo, FailureKind, Success
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.view.reconcile import Binding, find_node
from synology_apm_repo.browser.widgets.progress_hint import DataTableLoadingRowSink, TreeNodeLoadingSink
from synology_apm_repo.browser.widgets.worker_progress import run_worker_no_progress, run_worker_with_progress
from synology_apm_repo.sdk.api import Catalog
from synology_apm_repo.sdk.errors import ApmRepoError, KeyMismatchError, KeyRequiredError

#: Shared group for every BrowseScreen repository-close worker, hosted on
#: the App rather than the screen.
BROWSE_REPO_CLOSE_GROUP = "browse-repo-close"


def _load_versions_key(cmd: LoadVersions) -> WorkloadKey:
    """Shared by ``perform()``'s ``is_current`` predicate and
    ``_load_versions()`` so both derive the same ``WorkloadKey``."""
    return workload_key(catalog_key(cmd.repo, cmd.catalog), cmd.workload)


class BrowseEffects:
    def __init__(
        self,
        screen: Widget,
        resources: ResourceTable,
        store: Store[BrowseModel, BrowseMsg, BrowseCmd],
        *,
        catalog_tree: Callable[[], Tree[Binding[object]]],
        workload_tree: Callable[[], Tree[Binding[object]]],
        version_table: Callable[[], DataTable[Any]],
        set_current_repo: Callable[[RepoHandle | None], None],
        maybe_auto_park_catalog_cursor: Callable[[RepoHandle], None],
    ) -> None:
        self._screen = screen
        self._resources = resources
        self._store = store
        self._catalog_tree = catalog_tree
        self._workload_tree = workload_tree
        self._version_table = version_table
        self._set_current_repo = set_current_repo
        self._maybe_auto_park_catalog_cursor = maybe_auto_park_catalog_cursor

    def perform(self, cmd: BrowseCmd) -> None:
        match cmd:
            case CloseRepos(repos=repos):
                app = self._screen.app
                _worker: Worker[None] = run_worker_no_progress(
                    app, functools.partial(self._close_repos, repos), group=BROWSE_REPO_CLOSE_GROUP, name="close-repos"
                )
            case SetCurrentRepo(repo=repo):
                self._set_current_repo(repo)  # stays a handle; the screen dereferences via ResourceTable
            case LoadCatalogsFor():
                # Anchored on the repo node: expanding it populates its
                # own children, in the same tree.
                catalog_tree = self._catalog_tree()
                repo_node = find_node(catalog_tree.root, cmd.repo) or catalog_tree.root
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_catalogs_for, cmd),
                    sink=TreeNodeLoadingSink(repo_node),
                )
            case LoadWorkloads():
                # Anchored on column 2's permanent "Workloads" tree root,
                # since this fetch populates that whole column, not a
                # child of the column-1 node clicked to trigger it.
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_workloads, cmd),
                    sink=TreeNodeLoadingSink(self._workload_tree().root),
                )
            case PromptForKey():
                self._prompt_for_key(cmd)
            case ReloadCatalogsAfterKeyVerified():
                # Anchored on the catalog the user just unlocked, not the
                # whole repository, even though every sibling is refetched
                # too. Falls back to the repo node, then the tree root, if
                # the catalog's node can't be found.
                catalog_tree = self._catalog_tree()
                target_node = (
                    find_node(catalog_tree.root, CatalogKey(repo=cmd.repo, catalog_id=cmd.catalog_id))
                    or find_node(catalog_tree.root, cmd.repo)
                    or catalog_tree.root
                )
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._reload_catalogs_after_key_verified, cmd),
                    sink=TreeNodeLoadingSink(target_node),
                )
            case LoadVersions():
                # Column 3 is a flat DataTable with no per-node loading sink,
                # so this appends a trailing loading row instead.
                dispatched_key = _load_versions_key(cmd)
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_versions, cmd),
                    sink=DataTableLoadingRowSink(
                        self._version_table(),
                        is_current=lambda: is_workload_current(self._store.model, dispatched_key),
                    ),
                )
            case Notify(message=message, severity=severity, title=title):
                self._screen.notify(message, severity=severity, title=title or "")
            case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
                assert_never(cmd)

    async def _close_repos(self, repos: tuple[RepoHandle, ...]) -> None:
        """Releases every discarded repository concurrently, in one
        worker -- no ordering dependency between them."""
        await asyncio.gather(*(self._resources.release_repo(handle) for handle in repos))

    async def _load_catalogs_for(self, cmd: LoadCatalogsFor) -> None:
        repo = self._resources.repo(cmd.repo)
        if repo is None:  # pragma: no cover - defensive; a handle only exists while its own repository is live
            return
        try:
            catalogs = await repo.catalogs()
        except Exception as exc:
            # Broader than ApmRepoError: a repository can surface a raw
            # third-party failure too. Load-bearing: an uncaught exception
            # would leave this slot stuck Loading forever.
            self._store.dispatch(
                CatalogsLoadFailed(epoch=cmd.epoch, request=cmd.request, repo=cmd.repo, message=str(exc))
            )
            return
        self._store.dispatch(
            CatalogsLoaded(epoch=cmd.epoch, request=cmd.request, repo=cmd.repo, catalogs=tuple(catalogs))
        )
        self._maybe_auto_park_catalog_cursor(cmd.repo)

    async def _load_workloads(self, cmd: LoadWorkloads) -> None:
        key = catalog_key(cmd.repo, cmd.catalog)
        try:
            workloads = await cmd.catalog.workloads()
        except (KeyRequiredError, KeyMismatchError) as exc:
            info = FailureInfo(message=str(exc), kind=FailureKind.KEY_REQUIRED)
            self._store.dispatch(WorkloadsLoadFailed(catalog=key, real_catalog=cmd.catalog, info=info))
            return
        except Exception as exc:
            # Broader than ApmRepoError, load-bearing here too -- same
            # reasoning as _load_catalogs_for above.
            self._store.dispatch(
                WorkloadsLoadFailed(catalog=key, real_catalog=cmd.catalog, info=FailureInfo(message=str(exc)))
            )
            return
        self._store.dispatch(WorkloadsLoaded(catalog=key, workloads=tuple(workloads)))

    def _prompt_for_key(self, cmd: PromptForKey) -> None:
        from synology_apm_repo.browser.screens.key_dialog import KeyDialog

        repo = self._resources.repo(cmd.repo)
        if repo is None:  # pragma: no cover - defensive; a handle only exists while its own repository is live
            return
        catalog_id = cmd.catalog.catalog_id

        def on_dismiss(verified: bool | None) -> None:
            # Dispatched unconditionally: key_status can change even on a
            # failed verify attempt, so labels need refreshing regardless.
            self._store.dispatch(RepoKeyStatusRefreshed(repo=cmd.repo, key_status=repo.key_status))
            if verified:
                self._store.dispatch(KeyVerified(repo=cmd.repo, catalog_id=catalog_id))

        self._screen.app.push_screen(KeyDialog(repo), on_dismiss)

    async def _reload_catalogs_after_key_verified(self, cmd: ReloadCatalogsAfterKeyVerified) -> None:
        """``Repository.set_key()`` closes and replaces every already-opened
        ``DedupRepo`` this repository holds, not just the one ``catalog_id``
        names (``RepoKind.OBJECT_STORE`` eagerly opens every sibling up
        front), so every sibling catalog is re-resolved via
        ``repo.catalog_by_id()``, concurrently since there's no ordering
        dependency between them. Dispatches ``CatalogsRefreshed`` (a
        sibling whose re-fetch failed keeps its stale entry rather than
        dropping out) then ``CatalogSelected`` for the one that triggered
        this reload, reusing the ordinary selection path's cache-check --
        safe since a key-required catalog's entry is always a
        ``FailureInfo``, never a cached ``Success``, so the reselect
        always misses the cache and genuinely re-fetches."""
        repo = self._resources.repo(cmd.repo)
        if repo is None:  # pragma: no cover - defensive; a handle only exists while its own repository is live
            return
        repo_state = self._store.model.repos.get(cmd.repo)
        old_catalogs = (
            list(repo_state.catalogs.value)
            if repo_state is not None and isinstance(repo_state.catalogs, Success)
            else []
        )
        old_by_id = {c.catalog_id: c for c in old_catalogs}
        sibling_ids = [c.catalog_id for c in old_catalogs] if old_catalogs else [cmd.catalog_id]

        results = await asyncio.gather(
            *(repo.catalog_by_id(sibling_id) for sibling_id in sibling_ids), return_exceptions=True
        )

        fresh_catalogs: list[Catalog] = []
        target: Catalog | None = None
        for sibling_id, result in zip(sibling_ids, results, strict=True):
            if isinstance(result, BaseException):
                if not isinstance(result, ApmRepoError):
                    raise result  # pragma: no cover - defensive; catalog_by_id only ever raises ApmRepoError
                if sibling_id != cmd.catalog_id:
                    # Best-effort for a sibling: its stale entry stays in
                    # the list rather than dropping out.
                    stale = old_by_id.get(sibling_id)
                    if stale is not None:
                        fresh_catalogs.append(stale)
                    continue
                self._store.dispatch(CatalogsRefreshFailed(repo=cmd.repo, message=str(result)))
                return
            if result is None:
                if sibling_id != cmd.catalog_id:
                    stale = old_by_id.get(sibling_id)
                    if stale is not None:
                        fresh_catalogs.append(stale)
                    continue
                self._store.dispatch(
                    CatalogsRefreshFailed(repo=cmd.repo, message=f"catalog {cmd.catalog_id!r} no longer found")
                )
                return
            fresh_catalogs.append(result)
            if sibling_id == cmd.catalog_id:
                target = result
        self._store.dispatch(CatalogsRefreshed(repo=cmd.repo, catalogs=tuple(fresh_catalogs)))
        if target is not None:
            self._store.dispatch(CatalogSelected(repo=cmd.repo, catalog=target))

    async def _load_versions(self, cmd: LoadVersions) -> None:
        key = _load_versions_key(cmd)
        try:
            versions = await cmd.catalog.versions(cmd.workload)
        except Exception as exc:
            # Broader than ApmRepoError, load-bearing here too -- same
            # reasoning as _load_catalogs_for above.
            self._store.dispatch(VersionsLoadFailed(workload=key, message=str(exc)))
            return
        self._store.dispatch(VersionsLoaded(workload=key, versions=tuple(versions)))
