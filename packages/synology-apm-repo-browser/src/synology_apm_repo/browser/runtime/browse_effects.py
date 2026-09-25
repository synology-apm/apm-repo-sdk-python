"""``BrowseEffects``: the one place a ``BrowseCmd`` actually does
anything -- fetches a repository's own catalogs, a catalog's own
workloads, a workload's own versions, closes discarded repositories,
sets the app-wide "current repository", prompts for a key, or shows a
toast. Constructed once (there is exactly one ``BrowseScreen``, the
app's own root screen), and handed to its ``Store`` as its ``perform``
callback.

Takes a plain ``Widget`` plus narrow callables (``catalog_tree``,
``workload_tree``, ``set_current_repo``) rather than the real
``BrowseScreen``/``ApmRepoBrowserApp`` types: ``runtime/`` must never
import from ``screens/`` (``core`` -> ``runtime`` -> ``view`` ->
``screens``, one-directional), and importing either concrete class here
would be circular (``browse_screen.py`` constructs this class, and
``app.py`` imports ``browse_screen.py``).

Repository close is hosted on the *App*, never the screen -- same
posture as ``UnitEffects``'s own ``CloseProvider`` handler, even though
this screen never actually unmounts in practice (it's the app's own root
screen, pushed once, never popped): a rescan's own ``CloseRepos`` can be
dispatched at any point while this screen is very much still alive and
already displaying a fresh scan's results, so the close still needs a
host that outlives the individual dispatch that triggered it.
``ApmRepoBrowserApp.on_unmount`` drains (never cancels)
``BROWSE_REPO_CLOSE_GROUP`` before closing the session, the same as it
already does for ``UNIT_PROVIDER_CLOSE_GROUP``.
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
    """The one place ``LoadVersions``'s own ``WorkloadKey`` is derived --
    both ``perform()``'s ``is_current`` predicate and ``_load_versions()``
    itself need it from the same ``cmd``, so a shared derivation is the
    single source of truth rather than two independent copies drifting
    apart."""
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
                # `repo` stays a handle all the way through -- the screen
                # itself dereferences it, at the point of use, through
                # ResourceTable, the sole owner of the live object.
                self._set_current_repo(repo)
            case LoadCatalogsFor():
                # Anchored on the repo node itself: expanding it is what
                # populates its own children, in the same tree.
                catalog_tree = self._catalog_tree()
                repo_node = find_node(catalog_tree.root, cmd.repo) or catalog_tree.root
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_catalogs_for, cmd),
                    sink=TreeNodeLoadingSink(repo_node),
                )
            case LoadWorkloads():
                # Anchored on column 2's own permanent "Workloads" tree
                # root -- this fetch populates that whole column, not a
                # child of whatever column-1 node was clicked to trigger
                # it, so the breadcrumb-two-columns-away default would
                # be the wrong place.
                _worker = run_worker_with_progress(
                    self._screen,
                    functools.partial(self._load_workloads, cmd),
                    sink=TreeNodeLoadingSink(self._workload_tree().root),
                )
            case PromptForKey():
                self._prompt_for_key(cmd)
            case ReloadCatalogsAfterKeyVerified():
                # Anchored on the specific catalog the user just unlocked
                # via KeyDialog, not the whole repository -- every sibling
                # catalog is refetched too (same reasoning as
                # _reload_catalogs_after_key_verified below), but this one
                # is what the user's attention is actually on.
                # Falls back to the repo node itself if the catalog's own
                # node can't be found (e.g. a rescan mid-flight), and to
                # the tree's root if even that's gone.
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
                # Anchored on column 3's own table: no per-node equivalent
                # of TreeNodeLoadingSink exists for a flat DataTable, so
                # this appends a trailing "<frame> Loading" row instead of
                # the screen-wide breadcrumb.
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
        worker -- a rescan closing several repos at once has no
        ordering dependency between them, so gathering avoids spawning
        one ``Worker`` per handle for closes that could all run at
        once."""
        await asyncio.gather(*(self._resources.release_repo(handle) for handle in repos))

    async def _load_catalogs_for(self, cmd: LoadCatalogsFor) -> None:
        repo = self._resources.repo(cmd.repo)
        if repo is None:  # pragma: no cover - defensive; a handle only exists while its own repository is live
            return
        try:
            catalogs = await repo.catalogs()
        except Exception as exc:
            # Broader than ApmRepoError on purpose: a repository can also
            # surface an unexpected failure from a third-party parser/driver
            # it depends on that isn't an ApmRepoError at all. Load-bearing
            # now that BrowseScreen.on_tree_node_expanded's is_pending_or_done guard
            # blocks re-dispatch while this slot stays Loading -- an
            # exception this doesn't catch would leave it stuck Loading
            # forever, with no way to retry short of a full reconnect.
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
            # Broader than ApmRepoError on purpose, same reasoning as
            # _load_catalogs_for above -- load-bearing here too, since
            # CatalogSelected's is_pending_or_done guard would otherwise
            # leave this slot stuck Loading forever.
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
            # Read live and dispatched unconditionally -- key_status can
            # change even on a failed verify attempt (NO_KEY_PROVIDED ->
            # INVALID) or stay the same on a bare cancel, so both
            # repository and catalog labels need refreshing on every
            # KeyDialog dismissal regardless of verified's own value.
            self._store.dispatch(RepoKeyStatusRefreshed(repo=cmd.repo, key_status=repo.key_status))
            if verified:
                self._store.dispatch(KeyVerified(repo=cmd.repo, catalog_id=catalog_id))

        self._screen.app.push_screen(KeyDialog(repo), on_dismiss)

    async def _reload_catalogs_after_key_verified(self, cmd: ReloadCatalogsAfterKeyVerified) -> None:
        """``Repository.set_key()`` (just run, successfully, by the
        ``KeyDialog`` this resumes from) closes and replaces every
        already-opened ``DedupRepo`` this repository holds -- not just
        the one ``catalog_id`` names, since ``RepoKind.OBJECT_STORE``
        eagerly opens every sibling catalog up front -- so every sibling
        catalog under this repository, not just the one that triggered
        ``KeyDialog``, needs its own fresh ``Catalog`` re-resolved via
        ``repo.catalog_by_id()``.
        Fetched concurrently (``asyncio.gather``), not one at a time --
        each sibling is an independent round-trip, and ``OBJECT_STORE``'s
        own eager-open-every-sibling posture means this can be more than
        a couple of catalogs; there's no ordering dependency between
        them; only the tuple order of the *result* (matching
        ``sibling_ids``' own order, so a caller-facing catalog list stays
        stable) needs to survive the fan-out. Dispatches
        ``CatalogsRefreshed`` (every sibling's own fresh entry, with a
        sibling whose own re-fetch failed keeping its stale entry in
        place at its own position rather than dropping out of the list
        entirely) followed by ``CatalogSelected`` for the one that
        actually triggered this reload -- reusing the ordinary selection path's own
        cache-check/``LoadWorkloads`` dispatch rather than duplicating
        it, which is safe here because a key-required catalog's own
        ``catalog_workloads`` entry is always a ``FailureInfo`` (never a
        cached ``Success``, since ``workloads()`` could never have
        succeeded before the key was verified) -- so the reselect always
        misses the skip-refetch cache and genuinely re-fetches."""
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
                    # Best-effort for a sibling -- its own stale entry
                    # (when this repo's catalogs were ever fetched at
                    # all) stays in the list at its own position rather
                    # than dropping out of it.
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
            # Broader than ApmRepoError on purpose, same reasoning as
            # _load_catalogs_for above -- load-bearing here too, since
            # WorkloadSelected's is_pending_or_done guard would otherwise
            # leave this slot stuck Loading forever.
            self._store.dispatch(VersionsLoadFailed(workload=key, message=str(exc)))
            return
        self._store.dispatch(VersionsLoaded(workload=key, versions=tuple(versions)))
