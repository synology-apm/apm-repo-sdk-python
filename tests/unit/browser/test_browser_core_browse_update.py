"""Unit tests for ``browser.core.browse.update``; each test asserts on the
returned ``(model, cmds)`` pair, with no Textual or effects layer."""

from __future__ import annotations

from typing import cast

import pytest

from support.model_factories import make_version, make_workload
from synology_apm_repo.browser.core.browse.cmd import (
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
    FilterTree,
    RepoState,
    SelectedCatalog,
    TreeFilterState,
    VersionFilterState,
    catalog_key,
    catalogs_slot,
    workload_key,
)
from synology_apm_repo.browser.core.browse.msg import (
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
from synology_apm_repo.browser.core.browse.select import version_rows, workload_tree_spec
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, WorkloadKey
from synology_apm_repo.browser.core.remote_data import (
    FailureInfo,
    FailureKind,
    Loading,
    NotAsked,
    RemoteData,
    Success,
)
from synology_apm_repo.sdk.api import Catalog, KeyStatus
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout


def _layout(repo_root: str = "repo-1") -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root)


class _FakeCatalog:
    """A ``Catalog`` duck-type; ``update()`` reads only ``catalog_id``."""

    def __init__(self, catalog_id: str, display_name: str = "catalog") -> None:
        self.catalog_id = CatalogId(catalog_id)
        self.display_name = display_name


def _catalog(catalog_id: str = "cat-1") -> Catalog:
    return cast(Catalog, _FakeCatalog(catalog_id))


def _live(handle: int = 1) -> dict[RepoHandle, RepoState]:
    """``BrowseModel.repos`` holding just ``handle``: every repo-scoped result
    for a handle outside ``repos`` is dropped as stale."""
    return {RepoHandle(handle): RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)}


def test_rescan_started_with_nothing_open_resets_with_no_close_command() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, RescanStarted(scan_path="/scan"))

    assert new_model.scan_path == "/scan"
    assert new_model.epoch == Epoch(1)
    assert cmds == ()


def test_rescan_started_closes_every_previously_discovered_repo() -> None:
    handle_a, handle_b = RepoHandle(1), RepoHandle(2)
    model = BrowseModel(
        repos={
            handle_a: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED),
            handle_b: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED),
        },
        selected_catalog=SelectedCatalog(repo=handle_a, catalog=_catalog()),
        selected_workload=make_workload(workload_id=1),
    )
    new_model, cmds = update(model, RescanStarted(scan_path="/new-scan"))

    assert new_model.repos == {}
    assert new_model.catalog_workloads == {}
    assert new_model.workload_versions == {}
    assert new_model.selected_catalog is None
    assert new_model.selected_workload is None
    assert cmds == (CloseRepos(repos=(handle_a, handle_b)),)


def test_rescan_started_bumps_epoch_so_a_stale_in_flight_fetch_is_dropped() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(epoch=Epoch(3), inflight={catalogs_slot(handle): RequestId(5)})
    new_model, _cmds = update(model, RescanStarted(scan_path=""))

    assert new_model.epoch == Epoch(4)
    stale_result, _ = update(new_model, CatalogsLoaded(epoch=Epoch(3), request=RequestId(5), repo=handle, catalogs=()))
    assert stale_result is new_model


def test_repo_added_first_repo_sets_current_repo() -> None:
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, RepoAdded(repo=handle, layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED))

    assert new_model.repos[handle] == RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)
    assert cmds == (SetCurrentRepo(repo=handle),)


def test_repo_added_second_repo_does_not_re_adopt_current_repo() -> None:
    first = RepoHandle(1)
    model = BrowseModel(repos={first: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)})
    second = RepoHandle(2)
    new_model, cmds = update(model, RepoAdded(repo=second, layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED))

    assert set(new_model.repos) == {first, second}
    assert cmds == ()
    assert new_model.repos[first] is model.repos[first]


def test_repo_selected_only_sets_current_repo_leaving_the_model_untouched() -> None:
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, RepoSelected(repo=handle))

    assert new_model is model
    assert cmds == (SetCurrentRepo(repo=handle),)


def test_catalogs_requested_marks_loading_and_mints_a_load_catalogs_command() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)})
    new_model, cmds = update(model, CatalogsRequested(repo=handle))

    assert new_model.repos[handle].catalogs == Loading()
    assert new_model.inflight == {catalogs_slot(handle): RequestId(1)}
    assert cmds == (LoadCatalogsFor(repo=handle, epoch=Epoch(0), request=RequestId(1)),)


def test_catalogs_requested_is_a_no_op_for_a_repo_no_longer_in_the_model() -> None:
    """A stale ``Tree.NodeExpanded`` queued before a ``RescanStarted``
    cleared ``model.repos`` is a no-op, not a ``KeyError``."""
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, CatalogsRequested(repo=handle))

    assert new_model is model
    assert cmds == ()


@pytest.mark.parametrize("catalogs", [Success(()), Loading()], ids=["loaded-empty", "loading"])
def test_catalogs_requested_for_a_repo_already_loaded_or_loading_is_a_no_op(
    catalogs: RemoteData[tuple[Catalog, ...]],
) -> None:
    """Re-expanding a repository node fetches nothing, even when its loaded
    catalog list is empty and so has no child nodes."""
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=catalogs)}
    )
    new_model, cmds = update(model, CatalogsRequested(repo=handle))

    assert new_model is model
    assert cmds == ()


def test_catalogs_loaded_publishes_when_current() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Loading())},
        inflight={catalogs_slot(handle): RequestId(1)},
    )
    catalogs = (_catalog("a"), _catalog("b"))
    new_model, cmds = update(
        model, CatalogsLoaded(epoch=Epoch(0), request=RequestId(1), repo=handle, catalogs=catalogs)
    )

    assert new_model.repos[handle].catalogs == Success(catalogs)
    assert cmds == ()


def test_catalogs_loaded_dropped_on_a_request_mismatch() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)},
        inflight={catalogs_slot(handle): RequestId(2)},
    )
    new_model, cmds = update(model, CatalogsLoaded(epoch=Epoch(0), request=RequestId(1), repo=handle, catalogs=()))

    assert new_model is model
    assert cmds == ()


def test_catalogs_load_failed_records_a_failure_when_current() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED, catalogs=Loading())},
        inflight={catalogs_slot(handle): RequestId(1)},
    )
    new_model, cmds = update(
        model, CatalogsLoadFailed(epoch=Epoch(0), request=RequestId(1), repo=handle, message="boom")
    )

    assert new_model.repos[handle].catalogs == FailureInfo(message="boom")
    assert cmds == ()


def test_catalogs_load_failed_dropped_when_superseded() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)},
        inflight={catalogs_slot(handle): RequestId(2)},
    )
    new_model, cmds = update(
        model, CatalogsLoadFailed(epoch=Epoch(0), request=RequestId(1), repo=handle, message="boom")
    )

    assert new_model is model
    assert cmds == ()


def test_catalog_selected_on_a_cache_miss_marks_loading_and_dispatches_load_workloads() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    model = BrowseModel(repos=_live())
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog)
    key = catalog_key(handle, catalog)
    assert new_model.catalog_workloads[key] == Loading()
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog),)


def test_catalog_selected_on_a_cache_hit_serves_the_cached_workloads_with_no_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    workloads = (make_workload(workload_id=1),)
    model = BrowseModel(repos=_live(), catalog_workloads={key: Success(workloads)})
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog)
    assert new_model.catalog_workloads[key] == Success(workloads)
    assert cmds == ()


def test_catalog_selected_while_its_workloads_are_still_loading_selects_it_with_no_second_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(repos=_live(), catalog_workloads={key: Loading()})
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog)
    assert new_model.catalog_workloads[key] == Loading()
    assert cmds == ()


def test_catalog_selected_resets_selected_workload_and_reload_failure() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    model = BrowseModel(repos=_live(), selected_workload=make_workload(workload_id=9), reload_failure="stale failure")
    new_model, _cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_workload is None
    assert new_model.reload_failure is None


def test_catalog_selected_a_prior_key_required_failure_is_never_a_cache_hit() -> None:
    """A ``KEY_REQUIRED`` failure is cached as ``NotAsked``, so reselecting
    the catalog always retries."""
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(repos=_live(), catalog_workloads={key: NotAsked()})
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.catalog_workloads[key] == Loading()
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog),)


def test_workloads_loaded_writes_the_cache_for_a_live_repo() -> None:
    key = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    workloads = (make_workload(workload_id=1), make_workload(workload_id=2))
    model = BrowseModel(repos=_live())
    new_model, cmds = update(model, WorkloadsLoaded(catalog=key, workloads=workloads))

    assert new_model.catalog_workloads[key] == Success(workloads)
    assert cmds == ()


def test_workloads_loaded_for_an_unrelated_catalog_leaves_repos_untouched() -> None:
    handle = RepoHandle(1)
    repos = {handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)}
    model = BrowseModel(repos=repos)
    key = CatalogKey(repo=handle, catalog_id=CatalogId("cat-1"))
    new_model, _cmds = update(model, WorkloadsLoaded(catalog=key, workloads=()))

    assert new_model.repos is model.repos


def test_workloads_load_failed_with_key_required_caches_not_asked_and_prompts() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(repos=_live())
    info = FailureInfo(message="key needed", kind=FailureKind.KEY_REQUIRED)
    new_model, cmds = update(model, WorkloadsLoadFailed(catalog=key, real_catalog=catalog, info=info))

    assert new_model.catalog_workloads[key] == NotAsked()
    assert cmds == (PromptForKey(repo=handle, catalog=catalog),)


def test_workloads_load_failed_with_a_plain_error_caches_the_failure_with_no_prompt() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(repos=_live())
    info = FailureInfo(message="boom")
    new_model, cmds = update(model, WorkloadsLoadFailed(catalog=key, real_catalog=catalog, info=info))

    assert new_model.catalog_workloads[key] == info
    assert cmds == ()


def test_repo_key_status_refreshed_updates_only_that_repos_key_status() -> None:
    handle = RepoHandle(1)
    layout = _layout()
    model = BrowseModel(repos={handle: RepoState(layout=layout, key_status=KeyStatus.NO_KEY_PROVIDED)})
    new_model, cmds = update(model, RepoKeyStatusRefreshed(repo=handle, key_status=KeyStatus.VERIFIED))

    assert new_model.repos[handle] == RepoState(layout=layout, key_status=KeyStatus.VERIFIED)
    assert cmds == ()


def test_key_verified_dispatches_the_reload_effect_with_no_model_change() -> None:
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))

    assert new_model is model
    assert cmds == (ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")),)


def test_catalogs_refreshed_replaces_the_whole_tuple() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.VERIFIED, catalogs=FailureInfo(message="boom"))}
    )
    fresh = (_catalog("a"), _catalog("b"))
    new_model, cmds = update(model, CatalogsRefreshed(repo=handle, catalogs=fresh))

    assert new_model.repos[handle].catalogs == Success(fresh)
    assert cmds == ()


def test_catalogs_refresh_failed_blanks_the_selection_and_sets_reload_failure() -> None:
    handle = RepoHandle(1)
    model = BrowseModel(
        repos=_live(),
        selected_catalog=SelectedCatalog(repo=handle, catalog=_catalog()),
        selected_workload=make_workload(workload_id=1),
    )
    new_model, cmds = update(model, CatalogsRefreshFailed(repo=handle, message="gone"))

    assert new_model.selected_catalog is None
    assert new_model.selected_workload is None
    assert new_model.reload_failure == "gone"
    assert cmds == ()


def _rescanned_model() -> BrowseModel:
    """A fresh scan's state, holding only ``RepoHandle(2)`` with a catalog
    selected -- what a result for the pre-rescan ``RepoHandle(1)`` arrives into."""
    live = RepoHandle(2)
    return BrowseModel(
        epoch=Epoch(1), repos=_live(2), selected_catalog=SelectedCatalog(repo=live, catalog=_catalog("new"))
    )


def test_catalogs_refresh_results_for_a_repo_a_rescan_discarded_leave_the_new_scan_untouched() -> None:
    model = _rescanned_model()

    for msg in (
        CatalogsRefreshed(repo=RepoHandle(1), catalogs=(_catalog("old"),)),
        CatalogsRefreshFailed(repo=RepoHandle(1), message="gone"),
    ):
        new_model, cmds = update(model, msg)
        assert new_model is model
        assert cmds == ()


def test_catalog_selected_for_a_repo_a_rescan_discarded_is_dropped() -> None:
    model = _rescanned_model()
    new_model, cmds = update(model, CatalogSelected(repo=RepoHandle(1), catalog=_catalog("old")))

    assert new_model is model
    assert cmds == ()


def test_workloads_results_for_a_repo_a_rescan_discarded_are_dropped_without_a_key_prompt() -> None:
    model = _rescanned_model()
    catalog = _catalog("old")
    key = catalog_key(RepoHandle(1), catalog)
    info = FailureInfo(message="key needed", kind=FailureKind.KEY_REQUIRED)

    for msg in (
        WorkloadsLoaded(catalog=key, workloads=(make_workload(workload_id=1),)),
        WorkloadsLoadFailed(catalog=key, real_catalog=catalog, info=info),
    ):
        new_model, cmds = update(model, msg)
        assert new_model is model
        assert cmds == ()


def test_versions_results_for_a_repo_a_rescan_discarded_are_dropped() -> None:
    model = _rescanned_model()
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("old")), workload_uid="wl-1")  # type: ignore[arg-type]

    for msg in (
        VersionsLoaded(workload=key, versions=(make_version(),)),
        VersionsLoadFailed(workload=key, message="boom"),
    ):
        new_model, cmds = update(model, msg)
        assert new_model is model
        assert cmds == ()


def test_workload_selected_on_a_cache_miss_marks_loading_and_dispatches_load_versions() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1)
    model = BrowseModel(selected_catalog=SelectedCatalog(repo=handle, catalog=catalog))
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert new_model.selected_workload is workload
    key = workload_key(catalog_key(handle, catalog), workload)
    assert new_model.workload_versions[key] == Loading()
    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload),)


def test_workload_selected_on_a_cache_hit_serves_the_cached_versions_with_no_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1)
    key = workload_key(catalog_key(handle, catalog), workload)
    versions = (make_version(),)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog), workload_versions={key: Success(versions)}
    )
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert new_model.workload_versions[key] == Success(versions)
    assert cmds == ()


def test_workload_selected_while_its_versions_are_still_loading_dispatches_no_second_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1)
    key = workload_key(catalog_key(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog), workload_versions={key: Loading()}
    )
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert new_model.selected_workload is workload
    assert new_model.workload_versions[key] == Loading()
    assert cmds == ()


def test_versions_loaded_writes_the_cache_for_a_live_repo() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    versions = (make_version(),)
    model = BrowseModel(repos=_live())
    new_model, cmds = update(model, VersionsLoaded(workload=key, versions=versions))

    assert new_model.workload_versions[key] == Success(versions)
    assert cmds == ()


def test_versions_load_failed_caches_a_failure_info() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    model = BrowseModel(repos=_live())
    new_model, cmds = update(model, VersionsLoadFailed(workload=key, message="versions boom"))

    assert new_model.workload_versions[key] == FailureInfo(message="versions boom")
    assert cmds == ()


def test_versions_load_failed_is_never_a_skip_refetch_cache_hit() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1, display_name="wl-1")
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        workload_versions={key: FailureInfo(message="boom")},
    )
    _new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload),)


def test_refresh_requested_with_a_selected_workload_takes_priority_over_the_catalog() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1)
    key = workload_key(catalog_key(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        selected_workload=workload,
        workload_versions={key: Success((make_version(),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    # A refresh re-fetches even a cache hit, keeping the old rows as
    # Loading.previous so column 3 stays populated meanwhile.
    assert new_model.workload_versions[key] == Loading(previous=(make_version(),))
    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload, invalidate=True),)


def test_refresh_requested_with_only_a_selected_catalog_re_fetches_workloads() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        catalog_workloads={key: Success((make_workload(workload_id=1),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    # Same stale-while-revalidate carry-forward as the versions refresh above.
    assert new_model.catalog_workloads[key] == Loading(previous=(make_workload(workload_id=1),))
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog, invalidate=True),)


def test_refresh_requested_while_a_versions_refresh_is_still_loading_is_a_no_op() -> None:
    """A second ``r`` keeps the first refresh's carried-forward rows."""
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = make_workload(workload_id=1)
    key = workload_key(catalog_key(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        selected_workload=workload,
        workload_versions={key: Loading(previous=(make_version(),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    assert new_model is model
    assert cmds == ()


def test_refresh_requested_while_a_workloads_refresh_is_still_loading_is_a_no_op() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        catalog_workloads={key: Loading(previous=(make_workload(workload_id=1),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    assert new_model is model
    assert cmds == ()


def test_refresh_requested_with_nothing_selected_is_a_no_op() -> None:
    """``update()`` ignores a refresh with nothing selected, independently
    of ``action_refresh``'s own guard."""
    model = BrowseModel()
    new_model, cmds = update(model, RefreshRequested())

    assert new_model is model
    assert cmds == ()


# -- a late result for a selection the user already left ----------------


def test_a_late_workloads_result_for_a_catalog_the_user_left_leaves_column_two_alone() -> None:
    handle = RepoHandle(1)
    catalog_a, catalog_b = _catalog("a"), _catalog("b")
    model = BrowseModel(repos=_live())
    model, _ = update(model, CatalogSelected(repo=handle, catalog=catalog_a))
    model, _ = update(model, CatalogSelected(repo=handle, catalog=catalog_b))
    model, _ = update(
        model,
        WorkloadsLoaded(
            catalog=catalog_key(handle, catalog_b), workloads=(make_workload(workload_id=2, display_name="B"),)
        ),
    )
    shown = workload_tree_spec(model)
    assert shown

    model, cmds = update(
        model,
        WorkloadsLoaded(
            catalog=catalog_key(handle, catalog_a), workloads=(make_workload(workload_id=1, display_name="A"),)
        ),
    )

    assert model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog_b)
    assert workload_tree_spec(model) == shown
    assert cmds == ()


def test_a_late_versions_result_for_a_workload_the_user_left_leaves_column_three_alone() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload_a, workload_b = (
        make_workload(workload_id=1, display_name="A"),
        make_workload(workload_id=2, display_name="B"),
    )
    ck = catalog_key(handle, catalog)
    model = BrowseModel(repos=_live(), selected_catalog=SelectedCatalog(repo=handle, catalog=catalog))
    model, _ = update(model, WorkloadSelected(workload=workload_a))
    model, _ = update(model, WorkloadSelected(workload=workload_b))
    model, _ = update(model, VersionsLoaded(workload=workload_key(ck, workload_b), versions=(make_version(),)))
    shown = version_rows(model)
    assert len(shown) == 1

    model, _ = update(
        model, VersionsLoaded(workload=workload_key(ck, workload_a), versions=(make_version(), make_version()))
    )

    assert model.selected_workload is workload_b
    assert version_rows(model) == shown


def test_switching_catalogs_mid_versions_fetch_requests_nothing_from_the_new_catalog() -> None:
    """The in-flight ``LoadVersions`` carries the catalog it was minted for;
    selecting another catalog clears the workload and asks only for its
    workloads, and the late result lands under the old catalog's key."""
    handle = RepoHandle(1)
    catalog_a, catalog_b = _catalog("a"), _catalog("b")
    workload = make_workload(workload_id=1)
    model = BrowseModel(repos=_live(), selected_catalog=SelectedCatalog(repo=handle, catalog=catalog_a))
    model, cmds = update(model, WorkloadSelected(workload=workload))
    assert cmds == (LoadVersions(repo=handle, catalog=catalog_a, workload=workload),)

    model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog_b))
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog_b),)
    assert model.selected_workload is None

    model, _ = update(
        model,
        VersionsLoaded(workload=workload_key(catalog_key(handle, catalog_a), workload), versions=(make_version(),)),
    )
    assert version_rows(model) == ()
    assert model.workload_versions[workload_key(catalog_key(handle, catalog_a), workload)] == Success((make_version(),))


def test_tree_filter_opened_sets_the_filter_state() -> None:
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, TreeFilterOpened(tree=FilterTree.CATALOGS, parent_key=handle))

    assert new_model.tree_filter == TreeFilterState(tree=FilterTree.CATALOGS, parent_key=handle)
    assert cmds == ()


def test_tree_filter_text_changed_updates_the_open_filters_text() -> None:
    handle = RepoHandle(1)
    model = update(BrowseModel(), TreeFilterOpened(tree=FilterTree.CATALOGS, parent_key=handle))[0]
    new_model, cmds = update(model, TreeFilterTextChanged(text="needle"))

    assert new_model.tree_filter == TreeFilterState(tree=FilterTree.CATALOGS, parent_key=handle, text="needle")
    assert cmds == ()


def test_tree_filter_text_changed_is_a_no_op_with_no_filter_open() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, TreeFilterTextChanged(text="needle"))

    assert new_model is model
    assert cmds == ()


def test_tree_filter_closed_clears_the_filter_state() -> None:
    model = update(BrowseModel(), TreeFilterOpened(tree=FilterTree.WORKLOADS, parent_key="group"))[0]
    new_model, cmds = update(model, TreeFilterClosed())

    assert new_model.tree_filter is None
    assert cmds == ()


def test_tree_filter_closed_is_a_no_op_with_no_filter_open() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, TreeFilterClosed())

    assert new_model is model
    assert cmds == ()


def test_version_filter_opened_sets_the_filter_state() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, VersionFilterOpened())

    assert new_model.version_filter == VersionFilterState()
    assert cmds == ()


def test_version_filter_text_changed_updates_the_open_filters_text() -> None:
    model = update(BrowseModel(), VersionFilterOpened())[0]
    new_model, cmds = update(model, VersionFilterTextChanged(text="v2"))

    assert new_model.version_filter == VersionFilterState(text="v2")
    assert cmds == ()


def test_version_filter_text_changed_is_a_no_op_with_no_filter_open() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, VersionFilterTextChanged(text="v2"))

    assert new_model is model
    assert cmds == ()


def test_version_filter_closed_clears_the_filter_state() -> None:
    model = update(BrowseModel(), VersionFilterOpened())[0]
    new_model, cmds = update(model, VersionFilterClosed())

    assert new_model.version_filter is None
    assert cmds == ()


def test_version_filter_closed_is_a_no_op_with_no_filter_open() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, VersionFilterClosed())

    assert new_model is model
    assert cmds == ()


def test_verbose_set_stores_the_flag_and_survives_a_rescan() -> None:
    model, cmds = update(BrowseModel(), VerboseSet(verbose=True))
    assert model.verbose is True
    assert cmds == ()
    assert update(model, RescanStarted(scan_path="/x"))[0].verbose is True
