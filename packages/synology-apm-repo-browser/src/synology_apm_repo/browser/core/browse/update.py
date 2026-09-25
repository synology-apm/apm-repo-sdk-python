"""``update(model, msg) -> (model, cmds)`` for ``BrowseScreen``'s own
store -- pure, synchronous, exhaustive (``case _: assert_never(msg)``,
so a new unhandled ``BrowseMsg`` case is a mypy error, not a silent
no-op).

Only ``CatalogsLoaded``/``CatalogsLoadFailed`` (column 1) check their own
``epoch``/``request`` against the model's current ones before applying --
a widget-level collapse/re-expand of the same repository node can
dispatch a second ``CatalogsRequested`` while the first is still in
flight (the node has no children yet either way, so the screen's own
``if event.node.children: return`` guard doesn't catch it), the same
per-``Slot`` overlap ``core/unit/update.py`` closes for a tree node's own
children fetch. A catalog/workload selection's own fetch
(``WorkloadsLoaded``/``VersionsLoaded``) needs no such check: it's keyed
by the *selected* catalog/workload object itself
(``catalog_workloads``/``workload_versions``), so a differently-selected
fetch's late result can never overwrite what's currently rendered
regardless of arrival order -- only a reselect of the exact same,
already-in-flight catalog/workload could race with itself, and
``catalog.workloads()``/``catalog.versions()`` are idempotent reads of
already-recorded backup data, so whichever overlapping call resolves
last simply overwrites its own key with an equivalent result. The cache
write and the render are the same operation here: ``select.py``
re-derives column 2/3 from ``model.selected_catalog``/
``model.catalog_workloads``/``model.workload_versions`` on every dispatch
regardless of which field changed.

That data-level safety is still not a reason to let ``CatalogSelected``/
``WorkloadSelected``/``RefreshRequested`` dispatch a second, overlapping
fetch for the same key: each duplicate still spawns its own worker and its
own loading-indicator sink on the same widget, the same worker/indicator
duplication ``CatalogsRequested``'s guard closes for column 1 -- see
``core/remote_data.py``'s own ``is_pending_or_done``, used by all three
cases below for exactly this."""

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
            is_first = not model.repos  # computed before inserting -- the sole "adopt as app_state.repo_handle" trigger
            new_model = dataclasses.replace(
                model, repos={**model.repos, repo: RepoState(layout=layout, key_status=key_status)}
            )
            cmds = (SetCurrentRepo(repo=repo),) if is_first else ()
            return new_model, cmds

        case RepoSelected(repo=repo):
            return model, (SetCurrentRepo(repo=repo),)

        case CatalogsRequested(repo=repo):
            if repo not in model.repos:
                # The dispatching Tree.NodeExpanded event can be stale --
                # queued before a RescanStarted wiped model.repos out from
                # under it. The same kind of race CatalogsLoaded/
                # CatalogsLoadFailed's own is_stale check covers for a
                # fetch already in flight; this is the equivalent guard
                # for one that hasn't started yet.
                return model, ()
            if is_pending_or_done(model.repos[repo].catalogs):
                # Defense in depth: BrowseScreen.on_tree_node_expanded
                # already guards this before dispatching, but update()
                # shouldn't rely on every future caller remembering to check
                # first.
                return model, ()
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
                # Skip-refetch cache hit, or already fetching: a duplicate
                # dispatch would still spawn its own worker/loading-
                # indicator for no benefit.
                return new_model, ()
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
                # Reset to NotAsked, never cached as a FailureInfo/rendered
                # as an error leaf -- column 2 stays blank and this only
                # prompts for the key. NotAsked also misses CatalogSelected's
                # own skip-refetch cache check on a later reselect (only
                # Success counts as a hit), so a reselect after this always
                # retries.
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
            # Found *before* any catalog was ever (re)selected, so there's
            # no CatalogKey yet to hang a normal per-catalog
            # catalog_workloads FailureInfo entry off of, the way
            # CatalogsLoadFailed/WorkloadsLoadFailed do. Blanks column 2/3,
            # since neither is meaningful once the catalog list itself
            # failed to refresh.
            return dataclasses.replace(model, selected_catalog=None, selected_workload=None, reload_failure=message), ()

        case WorkloadSelected(workload=workload):
            assert model.selected_catalog is not None  # a workload is only ever selected under a selected catalog
            ck = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
            wk_key = workload_key(ck, workload)
            new_model = dataclasses.replace(model, selected_workload=workload)
            existing_versions = new_model.workload_versions.get(wk_key, NotAsked())
            if is_pending_or_done(existing_versions):
                # Skip-refetch cache hit, or already fetching: a duplicate
                # dispatch would still spawn its own worker/loading-
                # indicator for no benefit.
                return new_model, ()
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
            # Rendered as column 3's own single error row (select.py's
            # version_load_error), never cached as a hit -- same
            # "FailureInfo is never a skip-refetch cache hit" rule as
            # catalog_workloads/repos above, so a later reselect of this
            # exact workload always retries rather than replaying the
            # failure forever.
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
                    # A refresh is already in flight -- unlike
                    # CatalogSelected/WorkloadSelected's own skip-refetch
                    # guard (is_pending_or_done), Success must still fall
                    # through below: refresh's whole point is to re-fetch
                    # a Success. Only re-triggering a Loading refresh needs
                    # blocking, both to avoid a second worker/loading-
                    # indicator and because a second loading_preserving()
                    # call here would drop the first call's own
                    # carried-forward previous, below.
                    return model, ()
                # loading_preserving: a refresh's own Success -> Loading
                # transition (unlike CatalogSelected/WorkloadSelected, which
                # skip re-fetching entirely once a slot already holds a
                # Success, so they never reach a Success -> Loading
                # transition at all) keeps the stale-but-real version list
                # on screen while the refetch is in flight, per
                # RemoteData.Loading.previous's own documented intent.
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
