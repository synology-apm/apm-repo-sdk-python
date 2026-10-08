"""``update(model, msg) -> (model, cmds)`` for ``BrowseScreen``'s store.

Every repo-scoped result is dropped once its ``RepoHandle`` is no longer
in ``model.repos``: handles are never reused and ``RescanStarted`` empties
``repos``, so a result from before a rescan can't touch the new scan's
state. ``CatalogsLoaded``/``CatalogsLoadFailed`` additionally check
``epoch``/``request``. A workload/version result needs neither: it is
stored under its own catalog/workload key, so a late one can't overwrite
another selection's data."""

from __future__ import annotations

import dataclasses
from typing import assert_never

from synology_apm_repo.browser.core.browse.cmd import (
    BrowseCmd,
    CloseRepos,
    LoadCatalogsFor,
    LoadVersions,
    LoadWorkloads,
    PromptForKey,
    ReloadCatalogsAfterKeyVerified,
    SetCurrentRepo,
)
from synology_apm_repo.browser.core.browse.model import (
    BrowseModel,
    RepoState,
    SelectedCatalog,
    TreeFilterState,
    VersionFilterState,
    catalog_key,
    catalogs_slot,
    workload_key,
)
from synology_apm_repo.browser.core.browse.msg import (
    BrowseMsg,
    CatalogSelected,
    CatalogsLoaded,
    CatalogsLoadFailed,
    CatalogsRefreshed,
    CatalogsRefreshFailed,
    CatalogsRequested,
    KeyVerified,
    RefreshRequested,
    RepoAdded,
    RepoKeyStatusRefreshed,
    RepoSelected,
    RescanStarted,
    TreeFilterClosed,
    TreeFilterOpened,
    TreeFilterTextChanged,
    VerboseSet,
    VersionFilterClosed,
    VersionFilterOpened,
    VersionFilterTextChanged,
    VersionsLoaded,
    VersionsLoadFailed,
    WorkloadSelected,
    WorkloadsLoaded,
    WorkloadsLoadFailed,
)
from synology_apm_repo.browser.core.keys import Epoch, filter_closed, filter_text_changed, is_stale
from synology_apm_repo.browser.core.keys import next_request as _next_request
from synology_apm_repo.browser.core.remote_data import (
    FailureInfo,
    FailureKind,
    Loading,
    NotAsked,
    Success,
    is_pending_or_done,
    loading_preserving,
)

_Result = tuple[BrowseModel, tuple[BrowseCmd, ...]]
"""What ``update`` and each ``_on_*`` handler return: the next model and the commands to run."""


def _on_rescan_started(model: BrowseModel, msg: RescanStarted) -> _Result:
    old_handles = tuple(model.repos)
    new_model = dataclasses.replace(
        model,
        epoch=Epoch(model.epoch + 1),
        scan_path=msg.scan_path,
        repos={},
        catalog_workloads={},
        workload_versions={},
        selected_catalog=None,
        selected_workload=None,
        reload_failure=None,
        tree_filter=None,
        version_filter=None,
    )
    close_cmds = (CloseRepos(repos=old_handles),) if old_handles else ()
    return new_model, close_cmds


def _on_repo_added(model: BrowseModel, msg: RepoAdded) -> _Result:
    is_first = not model.repos  # computed before inserting -- the "adopt as current repo" trigger
    new_model = dataclasses.replace(
        model, repos={**model.repos, msg.repo: RepoState(layout=msg.layout, key_status=msg.key_status)}
    )
    cmds = (SetCurrentRepo(repo=msg.repo),) if is_first else ()
    return new_model, cmds


def _on_catalogs_requested(model: BrowseModel, msg: CatalogsRequested) -> _Result:
    if msg.repo not in model.repos:
        # A Tree.NodeExpanded queued before a RescanStarted.
        return model, ()
    if is_pending_or_done(model.repos[msg.repo].catalogs):
        # Already fetched or fetching: an expanded repo node can't tell
        # "empty but loaded" from "never requested".
        return model, ()
    request, new_model = _next_request(model)
    new_model = dataclasses.replace(new_model, inflight={**new_model.inflight, catalogs_slot(msg.repo): request})
    requested_repo = new_model.repos[msg.repo]
    new_model = dataclasses.replace(
        new_model, repos={**new_model.repos, msg.repo: dataclasses.replace(requested_repo, catalogs=Loading())}
    )
    catalogs_cmd = LoadCatalogsFor(repo=msg.repo, epoch=new_model.epoch, request=request)
    return new_model, (catalogs_cmd,)


def _on_catalogs_loaded(model: BrowseModel, msg: CatalogsLoaded) -> _Result:
    if is_stale(model.epoch, model.inflight, catalogs_slot(msg.repo), msg.epoch, msg.request):
        return model, ()
    loaded_repo = model.repos.get(msg.repo)
    if loaded_repo is None:  # pragma: no cover - defensive; RescanStarted already bumps epoch above
        return model, ()
    new_repo_state = dataclasses.replace(loaded_repo, catalogs=Success(msg.catalogs))
    return dataclasses.replace(model, repos={**model.repos, msg.repo: new_repo_state}), ()


def _on_catalogs_load_failed(model: BrowseModel, msg: CatalogsLoadFailed) -> _Result:
    if is_stale(model.epoch, model.inflight, catalogs_slot(msg.repo), msg.epoch, msg.request):
        return model, ()
    failed_repo = model.repos.get(msg.repo)
    if failed_repo is None:  # pragma: no cover - defensive; same as CatalogsLoaded above
        return model, ()
    new_repo_state = dataclasses.replace(failed_repo, catalogs=FailureInfo(message=msg.message))
    return dataclasses.replace(model, repos={**model.repos, msg.repo: new_repo_state}), ()


def _on_catalog_selected(model: BrowseModel, msg: CatalogSelected) -> _Result:
    if msg.repo not in model.repos:
        return model, ()
    key = catalog_key(msg.repo, msg.catalog)
    new_model = dataclasses.replace(
        model,
        selected_catalog=SelectedCatalog(repo=msg.repo, catalog=msg.catalog),
        selected_workload=None,
        reload_failure=None,
    )
    existing_workloads = new_model.catalog_workloads.get(key, NotAsked())
    if is_pending_or_done(existing_workloads):
        return new_model, ()  # skip-refetch cache hit, or already fetching
    new_model = dataclasses.replace(new_model, catalog_workloads={**new_model.catalog_workloads, key: Loading()})
    return new_model, (LoadWorkloads(repo=msg.repo, catalog=msg.catalog),)


def _on_workloads_load_failed(model: BrowseModel, msg: WorkloadsLoadFailed) -> _Result:
    if msg.catalog.repo not in model.repos:
        return model, ()
    if msg.info.kind == FailureKind.KEY_REQUIRED:
        # Reset to NotAsked, never cached as a hit, so a reselect
        # after providing the key always retries.
        new_model = dataclasses.replace(model, catalog_workloads={**model.catalog_workloads, msg.catalog: NotAsked()})
        return new_model, (PromptForKey(repo=msg.catalog.repo, catalog=msg.real_catalog),)
    new_model = dataclasses.replace(model, catalog_workloads={**model.catalog_workloads, msg.catalog: msg.info})
    return new_model, ()


def _on_catalogs_refresh_failed(model: BrowseModel, msg: CatalogsRefreshFailed) -> _Result:
    if msg.repo not in model.repos:
        return model, ()
    # Columns 2/3 mean nothing once the catalog list failed to refresh.
    return dataclasses.replace(model, selected_catalog=None, selected_workload=None, reload_failure=msg.message), ()


def _on_workload_selected(model: BrowseModel, msg: WorkloadSelected) -> _Result:
    assert model.selected_catalog is not None  # a workload is only ever selected under a selected catalog
    wk_key = workload_key(model.selected_catalog.key, msg.workload)
    new_model = dataclasses.replace(model, selected_workload=msg.workload)
    existing_versions = new_model.workload_versions.get(wk_key, NotAsked())
    if is_pending_or_done(existing_versions):
        return new_model, ()  # skip-refetch cache hit, or already fetching
    new_model = dataclasses.replace(new_model, workload_versions={**new_model.workload_versions, wk_key: Loading()})
    versions_cmd = LoadVersions(
        repo=model.selected_catalog.repo, catalog=model.selected_catalog.catalog, workload=msg.workload
    )
    return new_model, (versions_cmd,)


def _on_versions_load_failed(model: BrowseModel, msg: VersionsLoadFailed) -> _Result:
    if msg.workload.catalog.repo not in model.repos:
        return model, ()
    new_model = dataclasses.replace(
        model, workload_versions={**model.workload_versions, msg.workload: FailureInfo(message=msg.message)}
    )
    return new_model, ()


def _on_refresh_requested(model: BrowseModel, _msg: RefreshRequested) -> _Result:
    if model.selected_workload is not None:
        assert model.selected_catalog is not None
        wk_key = workload_key(model.selected_catalog.key, model.selected_workload)
        stale_versions = model.workload_versions.get(wk_key, NotAsked())
        if isinstance(stale_versions, Loading):
            # A refresh is already in flight; a second loading_preserving()
            # would drop the carried-forward previous value.
            return model, ()
        new_model = dataclasses.replace(
            model, workload_versions={**model.workload_versions, wk_key: loading_preserving(stale_versions)}
        )
        refresh_versions_cmd = LoadVersions(
            repo=model.selected_catalog.repo,
            catalog=model.selected_catalog.catalog,
            workload=model.selected_workload,
            invalidate=True,
        )
        return new_model, (refresh_versions_cmd,)
    if model.selected_catalog is not None:
        ck = model.selected_catalog.key
        stale_workloads = model.catalog_workloads.get(ck, NotAsked())
        if isinstance(stale_workloads, Loading):
            return model, ()  # a refresh is already in flight -- see the versions branch above
        new_model = dataclasses.replace(
            model, catalog_workloads={**model.catalog_workloads, ck: loading_preserving(stale_workloads)}
        )
        refresh_workloads_cmd = LoadWorkloads(
            repo=model.selected_catalog.repo, catalog=model.selected_catalog.catalog, invalidate=True
        )
        return new_model, (refresh_workloads_cmd,)
    return model, ()  # pragma: no cover - defensive; the screen's own action_refresh guards this case first


def _on_tree_filter_text_changed(model: BrowseModel, msg: TreeFilterTextChanged) -> _Result:
    return (
        filter_text_changed(
            model, lambda m: m.tree_filter, lambda m, s: dataclasses.replace(m, tree_filter=s), msg.text
        ),
        (),
    )


def _on_version_filter_text_changed(model: BrowseModel, msg: VersionFilterTextChanged) -> _Result:
    return (
        filter_text_changed(
            model, lambda m: m.version_filter, lambda m, s: dataclasses.replace(m, version_filter=s), msg.text
        ),
        (),
    )


def update(model: BrowseModel, msg: BrowseMsg) -> _Result:
    match msg:
        case RescanStarted():
            return _on_rescan_started(model, msg)
        case RepoAdded():
            return _on_repo_added(model, msg)
        case RepoSelected(repo=repo):
            return model, (SetCurrentRepo(repo=repo),)
        case CatalogsRequested():
            return _on_catalogs_requested(model, msg)
        case CatalogsLoaded():
            return _on_catalogs_loaded(model, msg)
        case CatalogsLoadFailed():
            return _on_catalogs_load_failed(model, msg)
        case CatalogSelected():
            return _on_catalog_selected(model, msg)
        case WorkloadsLoaded(catalog=key, workloads=workloads):
            if key.repo not in model.repos:
                return model, ()
            return dataclasses.replace(
                model, catalog_workloads={**model.catalog_workloads, key: Success(workloads)}
            ), ()
        case WorkloadsLoadFailed():
            return _on_workloads_load_failed(model, msg)
        case RepoKeyStatusRefreshed(repo=repo, key_status=key_status):
            state = model.repos.get(repo)
            if state is None:  # pragma: no cover - defensive; a rescan mid-flight already discarded repo
                return model, ()
            new_state = dataclasses.replace(state, key_status=key_status)
            return dataclasses.replace(model, repos={**model.repos, repo: new_state}), ()
        case KeyVerified(repo=repo, catalog_id=catalog_id):
            return model, (ReloadCatalogsAfterKeyVerified(repo=repo, catalog_id=catalog_id),)
        case CatalogsRefreshed(repo=repo, catalogs=catalogs):
            refreshed_repo = model.repos.get(repo)
            if refreshed_repo is None:  # a rescan mid-flight already discarded repo
                return model, ()
            new_repo_state = dataclasses.replace(refreshed_repo, catalogs=Success(catalogs))
            return dataclasses.replace(model, repos={**model.repos, repo: new_repo_state}), ()
        case CatalogsRefreshFailed():
            return _on_catalogs_refresh_failed(model, msg)
        case WorkloadSelected():
            return _on_workload_selected(model, msg)
        case VersionsLoaded(workload=key, versions=versions):
            if key.catalog.repo not in model.repos:
                return model, ()
            return dataclasses.replace(model, workload_versions={**model.workload_versions, key: Success(versions)}), ()
        case VersionsLoadFailed():
            return _on_versions_load_failed(model, msg)
        case RefreshRequested():
            return _on_refresh_requested(model, msg)
        case TreeFilterOpened(tree=tree, parent_key=parent_key):
            return dataclasses.replace(model, tree_filter=TreeFilterState(tree=tree, parent_key=parent_key)), ()
        case TreeFilterTextChanged():
            return _on_tree_filter_text_changed(model, msg)
        case TreeFilterClosed():
            return (
                filter_closed(model, lambda m: m.tree_filter, lambda m: dataclasses.replace(m, tree_filter=None)),
                (),
            )
        case VersionFilterOpened():
            return dataclasses.replace(model, version_filter=VersionFilterState()), ()
        case VersionFilterTextChanged():
            return _on_version_filter_text_changed(model, msg)
        case VersionFilterClosed():
            return (
                filter_closed(model, lambda m: m.version_filter, lambda m: dataclasses.replace(m, version_filter=None)),
                (),
            )
        case VerboseSet(verbose=verbose):
            return dataclasses.replace(model, verbose=verbose), ()
        case _:
            assert_never(msg)
