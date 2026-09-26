"""Unit tests for ``browser.runtime.browse_effects.BrowseEffects`` —
driven against a bare ``App`` hosting real ``#col-catalogs``/
``#col-workloads`` ``Tree`` widgets and a real ``Store`` running the real
``core.browse.update``, so this proves the whole round-trip (dispatch ->
update -> perform -> real worker -> dispatch back) works when
``BrowseEffects`` is addressed directly by name, not just indirectly
through ``BrowseScreen``'s own ``on_mount()``. Same convention as
``test_browser_runtime_app_effects.py``/``test_browser_runtime_unit_effects.py``."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from textual.app import App, ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Tree

from synology_apm_repo.browser.core.browse.cmd import (
    BrowseCmd,
    CloseRepos,
    LoadVersions,
    LoadWorkloads,
    Notify,
    PromptForKey,
    ReloadCatalogsAfterKeyVerified,
    SetCurrentRepo,
)
from synology_apm_repo.browser.core.browse.model import BrowseModel, SelectedCatalog, catalog_key, workload_key
from synology_apm_repo.browser.core.browse.msg import BrowseMsg, CatalogsRequested, RepoAdded, WorkloadSelected
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import RepoHandle
from synology_apm_repo.browser.core.remote_data import FailureInfo, Success
from synology_apm_repo.browser.runtime.browse_effects import BROWSE_REPO_CLOSE_GROUP, BrowseEffects
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.view.reconcile import Binding
from synology_apm_repo.browser.widgets import progress_hint
from synology_apm_repo.sdk.api import Catalog, KeyStatus, Repository, Session, Version, Workload
from synology_apm_repo.sdk.errors import ApmRepoError, KeyRequiredError
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


def _layout() -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="repo-1")


def _workload(workload_id: int, display_name: str = "W") -> Workload:
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


class _FakeCatalog:
    def __init__(
        self, catalog_id: str, workloads: list[Workload] | None = None, workloads_error: Exception | None = None
    ) -> None:
        self.catalog_id = CatalogId(catalog_id)
        self.display_name = "catalog"
        self._workloads = workloads or []
        self._workloads_error = workloads_error
        self.versions_result: list[Version] = []
        self.versions_error: Exception | None = None

    async def workloads(self) -> list[Workload]:
        if self._workloads_error is not None:
            raise self._workloads_error
        return self._workloads

    async def versions(self, workload: Workload) -> list[Version]:
        if self.versions_error is not None:
            raise self.versions_error
        return self.versions_result


class _FakeRepo:
    def __init__(
        self,
        key_status: KeyStatus = KeyStatus.NOT_ENCRYPTED,
        catalogs: list[_FakeCatalog] | None = None,
        catalogs_error: Exception | None = None,
        catalog_by_id_result: dict[str, _FakeCatalog | None] | None = None,
        catalog_by_id_error: dict[str, Exception] | None = None,
    ) -> None:
        self.key_status = key_status
        self._catalogs = catalogs or []
        self._catalogs_error = catalogs_error
        self._catalog_by_id_result = catalog_by_id_result or {}
        self._catalog_by_id_error = catalog_by_id_error or {}
        self.layout = _layout()

    async def catalogs(self) -> list[_FakeCatalog]:
        if self._catalogs_error is not None:
            raise self._catalogs_error
        return self._catalogs

    async def catalog_by_id(self, catalog_id: CatalogId) -> _FakeCatalog | None:
        key = str(catalog_id)
        if key in self._catalog_by_id_error:
            raise self._catalog_by_id_error[key]
        return self._catalog_by_id_result.get(key)


class _FakeSession:
    """A working ``Session.close_repo`` -- the ``cast(Session, object())``
    placeholder every other test file in this package uses is only safe
    when nothing actually dispatches ``CloseRepos``; this file's own
    close test genuinely does."""

    def __init__(self) -> None:
        self.closed: list[object] = []

    async def close_repo(self, repo: object) -> None:
        self.closed.append(repo)


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        yield Tree("Catalogs", id="col-catalogs")
        yield Tree("Workloads", id="col-workloads")
        yield DataTable(id="col-versions")


def _make_store_and_effects(
    app: App[None], resources: ResourceTable, *, set_current_repo: Any = None, maybe_auto_park: Any = None
) -> tuple[Store[BrowseModel, BrowseMsg, BrowseCmd], BrowseEffects]:
    effects: BrowseEffects

    def _perform(cmd: BrowseCmd) -> None:
        effects.perform(cmd)

    store: Store[BrowseModel, BrowseMsg, BrowseCmd] = Store(BrowseModel(), update, _perform)
    effects = BrowseEffects(
        cast(Widget, app),
        resources,
        store,
        catalog_tree=lambda: cast(Tree[Binding[object]], app.query_one("#col-catalogs", Tree)),
        workload_tree=lambda: cast(Tree[Binding[object]], app.query_one("#col-workloads", Tree)),
        version_table=lambda: app.query_one("#col-versions", DataTable),
        set_current_repo=set_current_repo or (lambda repo: None),
        maybe_auto_park_catalog_cursor=maybe_auto_park or (lambda repo: None),
    )
    return store, effects


async def test_perform_notify_calls_screen_notify() -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[str, str]] = []
        app.notify = lambda message, *, severity="information", **kw: calls.append((message, severity))  # type: ignore[method-assign]
        resources = ResourceTable(cast(Session, object()))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(Notify(message="hi", severity="error"))

        assert calls == [("hi", "error")]


async def test_perform_set_current_repo_passes_the_handle_through_unresolved() -> None:
    """``SetCurrentRepo`` stays handle-only all the way to
    ``set_current_repo`` -- the screen dereferences it, at the point of
    use, through ``ResourceTable`` rather than caching the live object."""
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        calls: list[object] = []
        _store, effects = _make_store_and_effects(app, resources, set_current_repo=calls.append)

        effects.perform(SetCurrentRepo(repo=handle))

        assert calls == [handle]


async def test_perform_close_repos_releases_every_handle(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        session = _FakeSession()
        resources = ResourceTable(cast(Session, session))
        repo_a, repo_b = _FakeRepo(), _FakeRepo()
        handle_a = resources.put_repo(cast(Repository, repo_a))
        handle_b = resources.put_repo(cast(Repository, repo_b))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(CloseRepos(repos=(handle_a, handle_b)))
        # The close worker is hosted on the App, on BROWSE_REPO_CLOSE_GROUP
        # (confirmed live, before it finishes and Textual drops it from
        # app.workers) -- the same App-hosted-worker posture UnitEffects's
        # own CloseProvider uses, not just "eventually gets closed somehow".
        assert any(w.group == BROWSE_REPO_CLOSE_GROUP for w in app.workers)
        await wait_until(pilot, lambda: len(session.closed) == 2)

        assert set(session.closed) == {repo_a, repo_b}
        assert resources.repo(handle_a) is None
        assert resources.repo(handle_b) is None


async def test_perform_close_repos_uses_one_worker_for_every_handle(wait_until: Any) -> None:
    """A rescan discarding several repositories at once has no ordering
    dependency between their closes -- gathered into a single worker
    rather than spawning one ``Worker`` per handle."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        session = _FakeSession()
        resources = ResourceTable(cast(Session, session))
        repo_a, repo_b, repo_c = _FakeRepo(), _FakeRepo(), _FakeRepo()
        handle_a = resources.put_repo(cast(Repository, repo_a))
        handle_b = resources.put_repo(cast(Repository, repo_b))
        handle_c = resources.put_repo(cast(Repository, repo_c))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(CloseRepos(repos=(handle_a, handle_b, handle_c)))

        assert len([w for w in app.workers if w.group == BROWSE_REPO_CLOSE_GROUP]) == 1
        await wait_until(pilot, lambda: len(session.closed) == 3)


async def test_load_catalogs_for_success_dispatches_catalogs_loaded_and_auto_parks(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog = _FakeCatalog("cat-1")
        repo = _FakeRepo(catalogs=[catalog])
        handle = resources.put_repo(cast(Repository, repo))
        parked: list[RepoHandle] = []
        store, _effects = _make_store_and_effects(app, resources, maybe_auto_park=parked.append)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success))

        assert store.model.repos[handle].catalogs == Success((cast(Catalog, catalog),))
        assert parked == [handle]


async def test_load_catalogs_for_failure_dispatches_catalogs_load_failed(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(catalogs_error=ApmRepoError("boom"))
        handle = resources.put_repo(cast(Repository, repo))
        store, _effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, FailureInfo))

        assert store.model.repos[handle].catalogs == FailureInfo(message="boom")


async def test_load_workloads_success_dispatches_workloads_loaded(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        workload = _workload(1)
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads=[workload]))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        key = catalog_key(handle, catalog)
        await wait_until(pilot, lambda: isinstance(store.model.catalog_workloads.get(key), Success))

        assert store.model.catalog_workloads[key] == Success((workload,))


async def test_load_workloads_key_required_dispatches_a_key_required_failure(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads_error=KeyRequiredError("needs key")))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        pushed: list[object] = []
        app.push_screen = lambda screen, callback=None, **kw: pushed.append(screen)  # type: ignore[method-assign, assignment]
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        await wait_until(pilot, lambda: bool(pushed))

        assert isinstance(pushed[0], KeyDialog)


async def test_load_workloads_generic_error_dispatches_a_plain_failure(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads_error=ApmRepoError("boom")))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        key = catalog_key(handle, catalog)
        await wait_until(pilot, lambda: isinstance(store.model.catalog_workloads.get(key), FailureInfo))

        assert store.model.catalog_workloads[key] == FailureInfo(message="boom")


async def test_load_versions_success_dispatches_versions_loaded(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_result = [_version()]
        catalog = cast(Catalog, catalog_fake)
        workload = _workload(1)
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=workload))
        key = workload_key(catalog_key(handle, catalog), workload)
        await wait_until(pilot, lambda: isinstance(store.model.workload_versions.get(key), Success))

        assert store.model.workload_versions[key] == Success((catalog_fake.versions_result[0],))


async def test_load_versions_failure_dispatches_versions_load_failed(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_error = ApmRepoError("versions boom")
        catalog = cast(Catalog, catalog_fake)
        workload = _workload(1)
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=workload))
        key = workload_key(catalog_key(handle, catalog), workload)
        await wait_until(pilot, lambda: isinstance(store.model.workload_versions.get(key), FailureInfo))

        assert store.model.workload_versions[key] == FailureInfo(message="versions boom")


async def test_reload_catalogs_after_key_verified_refreshes_and_selects_the_target(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        stale = _FakeCatalog("cat-1")
        fresh = _FakeCatalog("cat-1")
        repo = _FakeRepo(catalogs=[stale], catalog_by_id_result={"cat-1": fresh})
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success))

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: store.model.selected_catalog is not None)

        assert store.model.selected_catalog is not None
        assert store.model.selected_catalog.catalog is cast(Catalog, fresh)
        assert store.model.repos[handle].catalogs == Success((cast(Catalog, fresh),))


async def test_reload_catalogs_after_key_verified_fetches_every_sibling_concurrently(wait_until: Any) -> None:
    """Each sibling's own ``catalog_by_id()`` is an independent
    round-trip -- fetched via ``asyncio.gather``, not one at a time.
    Proven by a repo whose ``catalog_by_id`` records a call the instant
    it's invoked, then blocks on one shared gate every sibling waits on
    together: a concurrent fan-out records every sibling's call before
    any can resolve, since nothing releases the gate until this test
    does; a sequential implementation would only have recorded the
    first."""
    started: list[str] = []
    gate = asyncio.Event()

    class _ConcurrentRepo:
        def __init__(self) -> None:
            self.key_status = KeyStatus.NOT_ENCRYPTED
            self.layout = _layout()

        async def catalog_by_id(self, catalog_id: CatalogId) -> _FakeCatalog:
            started.append(str(catalog_id))
            await gate.wait()
            return _FakeCatalog(str(catalog_id))

    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        siblings = [_FakeCatalog("cat-1"), _FakeCatalog("cat-2"), _FakeCatalog("cat-3")]
        repo = _FakeRepo(catalogs=siblings)
        repo.catalog_by_id = _ConcurrentRepo().catalog_by_id  # type: ignore[method-assign]
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success))

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: len(started) == 3)
        assert set(started) == {"cat-1", "cat-2", "cat-3"}

        # Cleanup: release the gate so the still-in-flight reload actually
        # finishes rather than outliving the test.
        gate.set()
        await wait_until(pilot, lambda: store.model.selected_catalog is not None)


async def test_reload_catalogs_after_key_verified_reports_a_gone_catalog(wait_until: Any) -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(catalog_by_id_result={"cat-1": None})
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: store.model.reload_failure is not None)

        assert store.model.reload_failure is not None
        assert "cat-1" in store.model.reload_failure


async def test_prompt_for_key_pushes_a_key_dialog_for_the_real_repo() -> None:
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        pushed: list[object] = []
        app.push_screen = lambda screen, callback=None, **kw: pushed.append(screen)  # type: ignore[method-assign, assignment]
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(PromptForKey(repo=handle, catalog=cast(Catalog, _FakeCatalog("cat-1"))))

        assert len(pushed) == 1
        assert isinstance(pushed[0], KeyDialog)


async def test_prompt_for_key_on_dismiss_verified_refreshes_key_status_and_dispatches_key_verified() -> None:
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(key_status=KeyStatus.NO_KEY_PROVIDED)
        handle = resources.put_repo(cast(Repository, repo))
        dismiss_callbacks: list[Any] = []
        app.push_screen = lambda screen, callback=None, **kw: dismiss_callbacks.append(callback)  # type: ignore[method-assign, assignment]
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(PromptForKey(repo=handle, catalog=cast(Catalog, _FakeCatalog("cat-1"))))
        assert len(dismiss_callbacks) == 1

        # KeyDialog's own set_key() call already ran by the time it
        # dismisses -- key_status is read live off the real repo, not
        # captured at PromptForKey dispatch time.
        repo.key_status = KeyStatus.VERIFIED
        dismiss_callbacks[0](True)

        assert store.model.repos[handle].key_status == KeyStatus.VERIFIED
        # KeyVerified's own effect (ReloadCatalogsAfterKeyVerified) needs
        # a real Repository to resolve -- reaching that far is already
        # covered by test_reload_catalogs_after_key_verified_* above;
        # this test's own job is only proving on_dismiss's dispatch.


async def test_prompt_for_key_on_dismiss_unverified_still_refreshes_key_status_only() -> None:
    """A wrong-key retry can still move ``key_status`` from
    ``NO_KEY_PROVIDED`` to ``INVALID`` without ``verified`` ever being
    ``True``: ``KeyVerified`` (and so the whole sibling-reload effect)
    must never fire for this case."""
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(key_status=KeyStatus.NO_KEY_PROVIDED)
        handle = resources.put_repo(cast(Repository, repo))
        dismiss_callbacks: list[Any] = []
        app.push_screen = lambda screen, callback=None, **kw: dismiss_callbacks.append(callback)  # type: ignore[method-assign, assignment]
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(PromptForKey(repo=handle, catalog=cast(Catalog, _FakeCatalog("cat-1"))))
        repo.key_status = KeyStatus.INVALID
        dismiss_callbacks[0](False)

        assert store.model.repos[handle].key_status == KeyStatus.INVALID
        assert store.model.selected_catalog is None  # KeyVerified never dispatched


async def test_switching_to_an_already_cached_workload_mid_fetch_leaves_no_stray_loading_row(
    wait_until: Any, sdk_timeout: float, monkeypatch: Any
) -> None:
    """Regression test for the stale-loading-row bug, column-3's own
    counterpart of ``test_browser_runtime_unit_effects.py``'s file-table
    fix: workload A's own ``LoadVersions`` fetch is still in flight when
    the user switches to workload B, whose own version list is already
    cached (``WorkloadSelected``'s own skip-refetch cache hit returns
    ``(new_model, ())`` -- no further ``Cmd`` dispatched at all). A's next
    animation tick must not re-append a stray
    "Loading" row onto B's now-displayed table."""
    monkeypatch.setattr(progress_hint, "_FRAME_INTERVAL", 0.02)
    workload_a, workload_b = _workload(1, "A"), _workload(2, "B")
    gate = asyncio.Event()

    class _GatedCatalog(_FakeCatalog):
        async def versions(self, workload: Workload) -> list[Version]:
            if workload.workload_id == workload_a.workload_id:
                await gate.wait()
            return await super().versions(workload)

    gated_catalog = _GatedCatalog("cat-1")
    gated_catalog.versions_result = [_version()]
    catalog = cast(Catalog, gated_catalog)

    app = _FakeApp()
    async with app.run_test() as pilot:
        table = app.query_one("#col-versions", DataTable)
        table.add_column("Date")
        resources = ResourceTable(cast(Session, object()))
        handle = resources.put_repo(cast(Repository, _FakeRepo()))
        ck = catalog_key(handle, catalog)
        b_key = workload_key(ck, workload_b)
        # Workload B is already cached (a prior fetch's own Success) --
        # WorkloadSelected's own skip-refetch guard means reselecting it
        # dispatches no Cmd, simulating "already-loaded" without a second
        # real fetch.
        initial_model = BrowseModel(
            selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
            workload_versions={b_key: Success((_version(),))},
        )

        effects: BrowseEffects

        def _perform(cmd: BrowseCmd) -> None:
            effects.perform(cmd)

        store: Store[BrowseModel, BrowseMsg, BrowseCmd] = Store(initial_model, update, _perform)
        effects = BrowseEffects(
            cast(Widget, app),
            resources,
            store,
            catalog_tree=lambda: cast(Tree[Binding[object]], app.query_one("#col-catalogs", Tree)),
            workload_tree=lambda: cast(Tree[Binding[object]], app.query_one("#col-workloads", Tree)),
            version_table=lambda: table,
            set_current_repo=lambda repo: None,
            maybe_auto_park_catalog_cursor=lambda repo: None,
        )

        store.dispatch(WorkloadSelected(workload=workload_a))
        await wait_until(
            pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]), timeout=sdk_timeout
        )

        store.dispatch(WorkloadSelected(workload=workload_b))
        # This bare-Effects test wires up no render subscription to
        # repaint the table on its own (and the skip-refetch cache hit
        # above dispatches no Cmd that could trigger one either) --
        # simulate what the real BrowseScreen's own version-table render
        # reaction would already have done.
        table.clear()
        table.add_row("2026-01-01 00:00")

        # A's own fetch is still gated, so no readiness signal exists for
        # "no more ticks will ever touch this table again" -- this waits a
        # fixed, short interval (several sped-up _FRAME_INTERVAL ticks) to
        # prove the absence of a stray row.
        await asyncio.sleep(0.1)
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"], "A's late tick must not leak a stray Loading row onto B's own table"

        gate.set()
        a_key = workload_key(ck, workload_a)
        await wait_until(pilot, lambda: isinstance(store.model.workload_versions.get(a_key), Success))


__all__: list[str] = []
