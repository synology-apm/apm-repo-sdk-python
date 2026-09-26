"""``update(model, msg) -> (model, cmds)`` for ``BrowseScreen``'s store --
pure, synchronous, exhaustive (``case _: assert_never(msg)``).

Only ``CatalogsLoaded``/``CatalogsLoadFailed`` (column 1) check
``epoch``/``request`` against the model's current ones, since a
widget-level collapse/re-expand can dispatch a second ``CatalogsRequested``
while the first fetch is still in flight. A catalog/workload selection's
fetch needs no such check: it's keyed by the selected object itself, so a
differently-selected fetch's late result can never overwrite what's
rendered. That data-level safety still doesn't excuse a duplicate,
overlapping fetch for the same key -- ``core/remote_data.py``'s
``is_pending_or_done`` guards ``CatalogSelected``/``WorkloadSelected``/
``RefreshRequested`` against spawning a redundant worker/loading-indicator."""

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


def update(model: BrowseModel, msg: BrowseMsg) -> tuple[BrowseModel, tuple[BrowseCmd, ...]]:
    match msg:
        case RescanStarted(scan_path=scan_path):
            old_handles = tuple(model.repos)
            new_model = dataclasses.replace(
                model,
                epoch=Epoch(model.epoch + 1),
                scan_path=scan_path,
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

        case RepoAdded(repo=repo, layout=layout, key_status=key_status):
            is_first = not model.repos  # computed before inserting -- the "adopt as current repo" trigger
            new_model = dataclasses.replace(
                model, repos={**model.repos, repo: RepoState(layout=layout, key_status=key_status)}
            )
            cmds = (SetCurrentRepo(repo=repo),) if is_first else ()
            return new_model, cmds

        case RepoSelected(repo=repo):
            return model, (SetCurrentRepo(repo=repo),)

        case CatalogsRequested(repo=repo):
            if repo not in model.repos:
                # A stale Tree.NodeExpanded event, queued before a
                # RescanStarted wiped model.repos out from under it.
                return model, ()
            if is_pending_or_done(model.repos[repo].catalogs):
                return model, ()  # defense in depth; the screen already guards this before dispatching
            request, new_model = _next_request(model)
            new_model = dataclasses.replace(new_model, inflight={**new_model.inflight, catalogs_slot(repo): request})
            requested_repo = new_model.repos[repo]
            new_model = dataclasses.replace(
                new_model, repos={**new_model.repos, repo: dataclasses.replace(requested_repo, catalogs=Loading())}
            )
            catalogs_cmd = LoadCatalogsFor(repo=repo, epoch=new_model.epoch, request=request)
            return new_model, (catalogs_cmd,)

        case CatalogsLoaded(epoch=epoch, request=request, repo=repo, catalogs=catalogs):
            if is_stale(model.epoch, model.inflight, catalogs_slot(repo), epoch, request):
                return model, ()
            loaded_repo = model.repos.get(repo)
            if loaded_repo is None:  # pragma: no cover - defensive; RescanStarted already bumps epoch above
                return model, ()
            new_repo_state = dataclasses.replace(loaded_repo, catalogs=Success(catalogs))
            return dataclasses.replace(model, repos={**model.repos, repo: new_repo_state}), ()

        case CatalogsLoadFailed(epoch=epoch, request=request, repo=repo, message=message):
            if is_stale(model.epoch, model.inflight, catalogs_slot(repo), epoch, request):
                return model, ()
            failed_repo = model.repos.get(repo)
            if failed_repo is None:  # pragma: no cover - defensive; same as CatalogsLoaded above
                return model, ()
            new_repo_state = dataclasses.replace(failed_repo, catalogs=FailureInfo(message=message))
            return dataclasses.replace(model, repos={**model.repos, repo: new_repo_state}), ()

        case CatalogSelected(repo=repo, catalog=catalog):
            key = catalog_key(repo, catalog)
            new_model = dataclasses.replace(
                model,
                selected_catalog=SelectedCatalog(repo=repo, catalog=catalog),
                selected_workload=None,
                reload_failure=None,
            )
            existing_workloads = new_model.catalog_workloads.get(key, NotAsked())
            if is_pending_or_done(existing_workloads):
                return new_model, ()  # skip-refetch cache hit, or already fetching
            new_model = dataclasses.replace(
                new_model, catalog_workloads={**new_model.catalog_workloads, key: Loading()}
            )
            return new_model, (LoadWorkloads(repo=repo, catalog=catalog),)

        case WorkloadsLoaded(catalog=key, workloads=workloads):
            return dataclasses.replace(
                model, catalog_workloads={**model.catalog_workloads, key: Success(workloads)}
            ), ()

        case WorkloadsLoadFailed(catalog=key, real_catalog=catalog, info=info):
            if info.kind == FailureKind.KEY_REQUIRED:
                # Reset to NotAsked, never cached as a hit, so a reselect
                # after providing the key always retries.
                new_model = dataclasses.replace(model, catalog_workloads={**model.catalog_workloads, key: NotAsked()})
                return new_model, (PromptForKey(repo=key.repo, catalog=catalog),)
            new_model = dataclasses.replace(model, catalog_workloads={**model.catalog_workloads, key: info})
            return new_model, ()

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
            if refreshed_repo is None:  # pragma: no cover - defensive; a rescan mid-flight already discarded repo
                return model, ()
            new_repo_state = dataclasses.replace(refreshed_repo, catalogs=Success(catalogs))
            return dataclasses.replace(model, repos={**model.repos, repo: new_repo_state}), ()

        case CatalogsRefreshFailed(repo=repo, message=message):
            # No CatalogKey yet to hang a per-catalog FailureInfo off of;
            # blanks column 2/3 since neither is meaningful once the
            # catalog list itself failed to refresh.
            return dataclasses.replace(model, selected_catalog=None, selected_workload=None, reload_failure=message), ()

        case WorkloadSelected(workload=workload):
            assert model.selected_catalog is not None  # a workload is only ever selected under a selected catalog
            ck = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
            wk_key = workload_key(ck, workload)
            new_model = dataclasses.replace(model, selected_workload=workload)
            existing_versions = new_model.workload_versions.get(wk_key, NotAsked())
            if is_pending_or_done(existing_versions):
                return new_model, ()  # skip-refetch cache hit, or already fetching
            new_model = dataclasses.replace(
                new_model, workload_versions={**new_model.workload_versions, wk_key: Loading()}
            )
            versions_cmd = LoadVersions(
                repo=model.selected_catalog.repo, catalog=model.selected_catalog.catalog, workload=workload
            )
            return new_model, (versions_cmd,)

        case VersionsLoaded(workload=key, versions=versions):
            return dataclasses.replace(model, workload_versions={**model.workload_versions, key: Success(versions)}), ()

        case VersionsLoadFailed(workload=key, message=message):
            # Never cached as a hit, so a later reselect always retries
            # rather than replaying the failure forever.
            new_model = dataclasses.replace(
                model, workload_versions={**model.workload_versions, key: FailureInfo(message=message)}
            )
            return new_model, ()

        case RefreshRequested():
            if model.selected_workload is not None:
                assert model.selected_catalog is not None
                ck = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
                wk_key = workload_key(ck, model.selected_workload)
                stale_versions = model.workload_versions.get(wk_key, NotAsked())
                if isinstance(stale_versions, Loading):
                    # A refresh is already in flight; a second
                    # loading_preserving() call here would drop the first
                    # call's carried-forward previous, below.
                    return model, ()
                # loading_preserving keeps the stale-but-real version list
                # on screen while the refetch is in flight.
                new_model = dataclasses.replace(
                    model, workload_versions={**model.workload_versions, wk_key: loading_preserving(stale_versions)}
                )
                refresh_versions_cmd = LoadVersions(
                    repo=model.selected_catalog.repo,
                    catalog=model.selected_catalog.catalog,
                    workload=model.selected_workload,
                )
                return new_model, (refresh_versions_cmd,)
            if model.selected_catalog is not None:
                ck = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
                stale_workloads = model.catalog_workloads.get(ck, NotAsked())
                if isinstance(stale_workloads, Loading):
                    return model, ()  # a refresh is already in flight -- see the versions branch above
                new_model = dataclasses.replace(
                    model, catalog_workloads={**model.catalog_workloads, ck: loading_preserving(stale_workloads)}
                )
                refresh_workloads_cmd = LoadWorkloads(
                    repo=model.selected_catalog.repo, catalog=model.selected_catalog.catalog
                )
                return new_model, (refresh_workloads_cmd,)
            return model, ()  # pragma: no cover - defensive; the screen's own action_refresh guards this case first

        case TreeFilterOpened(tree=tree, parent_key=parent_key):
            return dataclasses.replace(model, tree_filter=TreeFilterState(tree=tree, parent_key=parent_key)), ()

        case TreeFilterTextChanged(text=text):
            return (
                filter_text_changed(
                    model, lambda m: m.tree_filter, lambda m, s: dataclasses.replace(m, tree_filter=s), text
                ),
                (),
            )

        case TreeFilterClosed():
            return (
                filter_closed(model, lambda m: m.tree_filter, lambda m: dataclasses.replace(m, tree_filter=None)),
                (),
            )

        case VersionFilterOpened():
            return dataclasses.replace(model, version_filter=VersionFilterState()), ()

        case VersionFilterTextChanged(text=text):
            return (
                filter_text_changed(
                    model, lambda m: m.version_filter, lambda m, s: dataclasses.replace(m, version_filter=s), text
                ),
                (),
            )

        case VersionFilterClosed():
            return (
                filter_closed(model, lambda m: m.version_filter, lambda m: dataclasses.replace(m, version_filter=None)),
                (),
            )

        case _:  # pragma: no cover - exhaustiveness fallback; mypy proves this unreachable
            assert_never(msg)
