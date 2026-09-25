"""Unit tests for ``browser.core.browse.update`` -- every branch, no
Textual/App/Pilot involved at all. Each test asserts on the returned
``(model, cmds)`` pair directly -- ``Cmd`` is data, never a callable, so
``assert cmds == (LoadWorkloads(...),)`` is a one-line test with no
effects layer involved. Same convention as
``test_browser_core_unit_update.py``."""

from __future__ import annotations

from typing import cast

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
    VersionFilterClosed,
    VersionFilterOpened,
    VersionFilterTextChanged,
    VersionsLoaded,
    VersionsLoadFailed,
    WorkloadSelected,
    WorkloadsLoaded,
    WorkloadsLoadFailed,
)
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import CatalogKey, Epoch, RepoHandle, RequestId, WorkloadKey
from synology_apm_repo.browser.core.remote_data import FailureInfo, FailureKind, Loading, NotAsked, Success
from synology_apm_repo.sdk.api import Catalog, KeyStatus, Version, Workload
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout


def _layout(repo_root: str = "repo-1") -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=repo_root)


class _FakeCatalog:
    """A ``Catalog``-shaped duck-type carrying only ``catalog_id``/
    ``display_name`` -- everything ``update.py``'s own pure logic
    touches on a ``Catalog`` it's handed; no real ``DedupRepo``/
    ``Connection`` needed for a pure test (same convention as
    ``test_browser_browse_screen_gaps.py``'s own ``_FakeCatalogWithId``)."""

    def __init__(self, catalog_id: str, display_name: str = "catalog") -> None:
        self.catalog_id = CatalogId(catalog_id)
        self.display_name = display_name


def _catalog(catalog_id: str = "cat-1") -> Catalog:
    return cast(Catalog, _FakeCatalog(catalog_id))


def _workload(workload_id: int, display_name: str = "Workload") -> Workload:
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=f"wl-{workload_id}",  # type: ignore[arg-type]
        workload_type="VM",
        sub_type=None,
        display_name=display_name,
        subtitle=None,
        spec={},
    )


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
        selected_workload=_workload(1),
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
    # The stale fetch's own result, arriving after this rescan, is
    # checked against the model's *current* epoch by CatalogsLoaded --
    # proven directly below, not just implied by the bump here.
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
    # The first repo's own already-stored state is untouched by adding
    # a second one -- proves RepoAdded's dict-merge doesn't disturb an
    # unrelated key's own identity.
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
    """The dispatching ``Tree.NodeExpanded`` event can be stale -- queued
    before a ``RescanStarted`` wiped ``model.repos`` out from under it.
    Must no-op like every other repo-keyed case, not raise ``KeyError``
    indexing a handle that's no longer there."""
    handle = RepoHandle(1)
    model = BrowseModel()
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
    model = BrowseModel()
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog)
    key = catalog_key(handle, catalog)
    assert new_model.catalog_workloads[key] == Loading()
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog),)


def test_catalog_selected_on_a_cache_hit_serves_the_cached_workloads_with_no_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    workloads = (_workload(1),)
    model = BrowseModel(catalog_workloads={key: Success(workloads)})
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_catalog == SelectedCatalog(repo=handle, catalog=catalog)
    assert new_model.catalog_workloads[key] == Success(workloads)
    assert cmds == ()


def test_catalog_selected_resets_selected_workload_and_reload_failure() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    model = BrowseModel(selected_workload=_workload(9), reload_failure="stale failure")
    new_model, _cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.selected_workload is None
    assert new_model.reload_failure is None


def test_catalog_selected_a_prior_key_required_failure_is_never_a_cache_hit() -> None:
    """A ``KEY_REQUIRED`` failure is cached as ``NotAsked`` (see
    ``WorkloadsLoadFailed``'s own case below), never a ``Success`` --
    reselecting must always retry (a key-required catalog can never
    have a real cached workload list from before the key was
    verified)."""
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(catalog_workloads={key: NotAsked()})
    new_model, cmds = update(model, CatalogSelected(repo=handle, catalog=catalog))

    assert new_model.catalog_workloads[key] == Loading()
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog),)


def test_workloads_loaded_writes_the_cache_unconditionally() -> None:
    key = CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1"))
    workloads = (_workload(1), _workload(2))
    model = BrowseModel()
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
    model = BrowseModel()
    info = FailureInfo(message="key needed", kind=FailureKind.KEY_REQUIRED)
    new_model, cmds = update(model, WorkloadsLoadFailed(catalog=key, real_catalog=catalog, info=info))

    assert new_model.catalog_workloads[key] == NotAsked()
    assert cmds == (PromptForKey(repo=handle, catalog=catalog),)


def test_workloads_load_failed_with_a_plain_error_caches_the_failure_with_no_prompt() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel()
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
        selected_catalog=SelectedCatalog(repo=handle, catalog=_catalog()), selected_workload=_workload(1)
    )
    new_model, cmds = update(model, CatalogsRefreshFailed(repo=handle, message="gone"))

    assert new_model.selected_catalog is None
    assert new_model.selected_workload is None
    assert new_model.reload_failure == "gone"
    assert cmds == ()


def test_workload_selected_on_a_cache_miss_marks_loading_and_dispatches_load_versions() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = _workload(1)
    model = BrowseModel(selected_catalog=SelectedCatalog(repo=handle, catalog=catalog))
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert new_model.selected_workload is workload
    key = workload_key(catalog_key(handle, catalog), workload)
    assert new_model.workload_versions[key] == Loading()
    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload),)


def test_workload_selected_on_a_cache_hit_serves_the_cached_versions_with_no_fetch() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = _workload(1)
    key = workload_key(catalog_key(handle, catalog), workload)
    versions = (_version(),)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog), workload_versions={key: Success(versions)}
    )
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    assert new_model.workload_versions[key] == Success(versions)
    assert cmds == ()


def test_versions_loaded_writes_the_cache_unconditionally() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    versions = (_version(),)
    model = BrowseModel()
    new_model, cmds = update(model, VersionsLoaded(workload=key, versions=versions))

    assert new_model.workload_versions[key] == Success(versions)
    assert cmds == ()


def test_versions_load_failed_caches_a_failure_info() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    model = BrowseModel()
    new_model, cmds = update(model, VersionsLoadFailed(workload=key, message="versions boom"))

    assert new_model.workload_versions[key] == FailureInfo(message="versions boom")
    assert cmds == ()


def test_versions_load_failed_is_never_a_skip_refetch_cache_hit() -> None:
    key = WorkloadKey(catalog=CatalogKey(repo=RepoHandle(1), catalog_id=CatalogId("cat-1")), workload_uid="wl-1")  # type: ignore[arg-type]
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = _workload(1, display_name="wl-1")
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        workload_versions={key: FailureInfo(message="boom")},
    )
    new_model, cmds = update(model, WorkloadSelected(workload=workload))

    # A reselect after a failure always retries -- a FailureInfo is
    # never treated as a Success cache hit.
    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload),)


def test_refresh_requested_with_a_selected_workload_takes_priority_over_the_catalog() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    workload = _workload(1)
    key = workload_key(catalog_key(handle, catalog), workload)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        selected_workload=workload,
        workload_versions={key: Success((_version(),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    # Even though it was a cache hit -- a refresh always re-fetches. The
    # stale Success value is carried forward as Loading.previous so
    # column 3 stays populated while the refetch is in flight, instead of
    # blanking to empty.
    assert new_model.workload_versions[key] == Loading(previous=(_version(),))
    assert cmds == (LoadVersions(repo=handle, catalog=catalog, workload=workload),)


def test_refresh_requested_with_only_a_selected_catalog_re_fetches_workloads() -> None:
    handle = RepoHandle(1)
    catalog = _catalog("cat-1")
    key = catalog_key(handle, catalog)
    model = BrowseModel(
        selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
        catalog_workloads={key: Success((_workload(1),))},
    )
    new_model, cmds = update(model, RefreshRequested())

    # Same stale-while-revalidate carry-forward as the versions refresh above.
    assert new_model.catalog_workloads[key] == Loading(previous=(_workload(1),))
    assert cmds == (LoadWorkloads(repo=handle, catalog=catalog),)


def test_refresh_requested_with_nothing_selected_is_a_no_op() -> None:
    """The screen's own ``action_refresh`` guards this case before ever
    dispatching (falling back to reopening ``ConnectDialog`` instead) --
    proven here directly anyway, since nothing about ``update()`` itself
    depends on that screen-level guard actually running first."""
    model = BrowseModel()
    new_model, cmds = update(model, RefreshRequested())

    assert new_model is model
    assert cmds == ()


def test_tree_filter_opened_sets_the_filter_state() -> None:
    handle = RepoHandle(1)
    model = BrowseModel()
    new_model, cmds = update(model, TreeFilterOpened(tree="catalogs", parent_key=handle))

    assert new_model.tree_filter == TreeFilterState(tree="catalogs", parent_key=handle)
    assert cmds == ()


def test_tree_filter_text_changed_updates_the_open_filters_text() -> None:
    handle = RepoHandle(1)
    model = update(BrowseModel(), TreeFilterOpened(tree="catalogs", parent_key=handle))[0]
    new_model, cmds = update(model, TreeFilterTextChanged(text="needle"))

    assert new_model.tree_filter == TreeFilterState(tree="catalogs", parent_key=handle, text="needle")
    assert cmds == ()


def test_tree_filter_text_changed_is_a_no_op_with_no_filter_open() -> None:
    model = BrowseModel()
    new_model, cmds = update(model, TreeFilterTextChanged(text="needle"))

    assert new_model is model
    assert cmds == ()


def test_tree_filter_closed_clears_the_filter_state() -> None:
    model = update(BrowseModel(), TreeFilterOpened(tree="workloads", parent_key="group"))[0]
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


__all__: list[str] = []
