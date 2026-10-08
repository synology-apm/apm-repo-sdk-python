"""Unit tests for ``browser.runtime.browse_effects.BrowseEffects``, driven
against a bare ``App`` hosting a plain ``Tree``/``DataTable`` under each of
``BrowseScreen``'s column ids and a real ``Store``
running the real ``core.browse.update``, so each test covers the whole
dispatch -> update -> perform -> worker -> dispatch round-trip."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Tree

import synology_apm_repo.sdk.api as _sdk_api
from support.fakes import faithful_to
from support.model_factories import make_version, make_workload
from support.pilot import SDK_TIMEOUT, count_progress_ticks, settle, wait_until
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
from synology_apm_repo.browser.core.browse.model import (
    BrowseModel,
    RepoState,
    SelectedCatalog,
    catalog_key,
    workload_key,
)
from synology_apm_repo.browser.core.browse.msg import (
    BrowseMsg,
    CatalogsRequested,
    RepoAdded,
    WorkloadSelected,
    WorkloadsLoadFailed,
)
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import RepoHandle
from synology_apm_repo.browser.core.remote_data import FailureInfo, FailureKind, NotAsked, Success
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
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout


def _layout() -> RepositoryLayout:
    return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="repo-1")


@faithful_to(_sdk_api.Catalog)
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

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        if self.versions_error is not None:
            raise self.versions_error
        return self.versions_result


@faithful_to(_sdk_api.Repository)
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
        self.invalidate_caches_calls = 0
        self.invalidate_caches_error: Exception | None = None

    async def invalidate_caches(self, *names: str) -> None:
        self.invalidate_caches_calls += 1
        if self.invalidate_caches_error is not None:
            raise self.invalidate_caches_error

    async def catalogs(self) -> list[_FakeCatalog]:
        if self._catalogs_error is not None:
            raise self._catalogs_error
        return self._catalogs

    async def catalog_by_id(self, catalog_id: CatalogId) -> _FakeCatalog | None:
        key = str(catalog_id)
        if key in self._catalog_by_id_error:
            raise self._catalog_by_id_error[key]
        return self._catalog_by_id_result.get(key)


@faithful_to(Session)
class _FakeSession:
    """A ``Session.close_repo`` that records its argument, for the tests
    that dispatch ``CloseRepos`` (a ``cast(Session, object())`` placeholder
    would fail there)."""

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
        # What BrowseScreen passes.
        prompt_for_key=lambda repo, on_dismiss: app.push_screen(KeyDialog(repo), on_dismiss),
    )
    return store, effects


async def test_perform_notify_calls_screen_notify(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _FakeApp()
    async with app.run_test():
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            app, "notify", lambda message, *, severity="information", **kw: calls.append((message, severity))
        )
        resources = ResourceTable(cast(Session, object()))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(Notify(message="hi", severity="error"))

        assert calls == [("hi", "error")]


async def test_perform_set_current_repo_passes_the_handle_through_unresolved() -> None:
    """The screen dereferences the handle through ``ResourceTable`` at the
    point of use, rather than caching the live repository."""
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        calls: list[object] = []
        _store, effects = _make_store_and_effects(app, resources, set_current_repo=calls.append)

        effects.perform(SetCurrentRepo(repo=handle))

        assert calls == [handle]


async def test_perform_close_repos_releases_every_handle() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        session = _FakeSession()
        resources = ResourceTable(cast(Session, session))
        repo_a, repo_b = _FakeRepo(), _FakeRepo()
        handle_a = resources.put_repo(cast(Repository, repo_a))
        handle_b = resources.put_repo(cast(Repository, repo_b))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(CloseRepos(repos=(handle_a, handle_b)))
        # Checked before the worker finishes: Textual drops finished
        # workers from app.workers.
        assert any(w.group == BROWSE_REPO_CLOSE_GROUP for w in app.workers)
        await wait_until(pilot, lambda: len(session.closed) == 2, timeout=SDK_TIMEOUT)

        assert set(session.closed) == {repo_a, repo_b}
        assert resources.repo(handle_a) is None
        assert resources.repo(handle_b) is None


async def test_perform_close_repos_uses_one_worker_for_every_handle() -> None:
    """The closes have no ordering dependency, so one worker gathers them."""
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
        await wait_until(pilot, lambda: len(session.closed) == 3, timeout=SDK_TIMEOUT)


async def test_load_catalogs_for_success_dispatches_catalogs_loaded_and_auto_parks() -> None:
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
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success), timeout=SDK_TIMEOUT)

        assert store.model.repos[handle].catalogs == Success((cast(Catalog, catalog),))
        assert parked == [handle]


async def test_load_catalogs_for_failure_dispatches_catalogs_load_failed() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(catalogs_error=ApmRepoError("boom"))
        handle = resources.put_repo(cast(Repository, repo))
        store, _effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(
            pilot, lambda: isinstance(store.model.repos[handle].catalogs, FailureInfo), timeout=SDK_TIMEOUT
        )

        assert store.model.repos[handle].catalogs == FailureInfo(message="boom")


async def test_load_workloads_success_dispatches_workloads_loaded() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        workload = make_workload(workload_id=1)
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads=[workload]))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        key = catalog_key(handle, catalog)
        await wait_until(
            pilot, lambda: isinstance(store.model.catalog_workloads.get(key), Success), timeout=SDK_TIMEOUT
        )

        assert store.model.catalog_workloads[key] == Success((workload,))


async def test_load_workloads_key_required_dispatches_a_key_required_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``KeyRequiredError`` is dispatched as a ``KEY_REQUIRED`` failure, the
    only kind ``core.browse.update`` answers with a ``KeyDialog`` push and a
    ``NotAsked`` (never-cached) entry."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads_error=KeyRequiredError("needs key")))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        pushed: list[object] = []
        monkeypatch.setattr(app, "push_screen", lambda screen, callback=None, **kw: pushed.append(screen))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        dispatched: list[BrowseMsg] = []
        real_dispatch = store.dispatch

        def recording_dispatch(msg: BrowseMsg) -> None:
            dispatched.append(msg)
            real_dispatch(msg)

        monkeypatch.setattr(store, "dispatch", recording_dispatch)

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        await wait_until(pilot, lambda: bool(pushed), timeout=SDK_TIMEOUT)

        key = catalog_key(handle, catalog)
        assert dispatched == [
            WorkloadsLoadFailed(
                catalog=key,
                real_catalog=catalog,
                info=FailureInfo(message="needs key", kind=FailureKind.KEY_REQUIRED),
            )
        ]
        assert isinstance(pushed[0], KeyDialog)
        assert store.model.catalog_workloads[key] == NotAsked()


async def test_load_workloads_generic_error_dispatches_a_plain_failure() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads_error=ApmRepoError("boom")))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog))
        key = catalog_key(handle, catalog)
        await wait_until(
            pilot, lambda: isinstance(store.model.catalog_workloads.get(key), FailureInfo), timeout=SDK_TIMEOUT
        )

        assert store.model.catalog_workloads[key] == FailureInfo(message="boom")


async def test_load_versions_success_dispatches_versions_loaded() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_result = [make_version()]
        catalog = cast(Catalog, catalog_fake)
        workload = make_workload(workload_id=1)
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=workload))
        key = workload_key(catalog_key(handle, catalog), workload)
        await wait_until(
            pilot, lambda: isinstance(store.model.workload_versions.get(key), Success), timeout=SDK_TIMEOUT
        )

        assert store.model.workload_versions[key] == Success((catalog_fake.versions_result[0],))


async def test_load_versions_failure_dispatches_versions_load_failed() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_error = ApmRepoError("versions boom")
        catalog = cast(Catalog, catalog_fake)
        workload = make_workload(workload_id=1)
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=workload))
        key = workload_key(catalog_key(handle, catalog), workload)
        await wait_until(
            pilot, lambda: isinstance(store.model.workload_versions.get(key), FailureInfo), timeout=SDK_TIMEOUT
        )

        assert store.model.workload_versions[key] == FailureInfo(message="versions boom")


async def test_reload_catalogs_after_key_verified_refreshes_and_selects_the_target() -> None:
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
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success), timeout=SDK_TIMEOUT)

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: store.model.selected_catalog is not None, timeout=SDK_TIMEOUT)

        assert store.model.selected_catalog is not None
        assert store.model.selected_catalog.catalog is cast(Catalog, fresh)
        assert store.model.repos[handle].catalogs == Success((cast(Catalog, fresh),))


async def test_reload_catalogs_after_key_verified_fetches_every_sibling_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every sibling's ``catalog_by_id()`` is in flight at once: each blocks
    on one shared gate, so a sequential fetch would record only the first
    call."""
    started: list[str] = []
    gate = asyncio.Event()

    @faithful_to(Repository)
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
        monkeypatch.setattr(repo, "catalog_by_id", _ConcurrentRepo().catalog_by_id)
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success), timeout=SDK_TIMEOUT)

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: len(started) == 3, timeout=SDK_TIMEOUT)
        assert set(started) == {"cat-1", "cat-2", "cat-3"}

        # Release the gate so the in-flight reload doesn't outlive the test.
        gate.set()
        await wait_until(pilot, lambda: store.model.selected_catalog is not None, timeout=SDK_TIMEOUT)


async def test_reload_catalogs_after_key_verified_selects_nothing_when_the_target_left_the_cached_list() -> None:
    """The siblings still refresh."""
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        stale_other = _FakeCatalog("cat-other")
        fresh_other = _FakeCatalog("cat-other")
        repo = _FakeRepo(catalogs=[stale_other], catalog_by_id_result={"cat-other": fresh_other})
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)

        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: isinstance(store.model.repos[handle].catalogs, Success), timeout=SDK_TIMEOUT)

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(
            pilot,
            lambda: store.model.repos[handle].catalogs == Success((cast(Catalog, fresh_other),)),
            timeout=SDK_TIMEOUT,
        )

        assert store.model.selected_catalog is None


async def test_reload_catalogs_after_key_verified_reports_a_gone_catalog() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(catalog_by_id_result={"cat-1": None})
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(ReloadCatalogsAfterKeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: store.model.reload_failure is not None, timeout=SDK_TIMEOUT)

        assert store.model.reload_failure is not None
        assert "cat-1" in store.model.reload_failure


async def test_prompt_for_key_pushes_a_key_dialog_for_the_real_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        pushed: list[object] = []
        monkeypatch.setattr(app, "push_screen", lambda screen, callback=None, **kw: pushed.append(screen))
        _store, effects = _make_store_and_effects(app, resources)

        effects.perform(PromptForKey(repo=handle, catalog=cast(Catalog, _FakeCatalog("cat-1"))))

        assert len(pushed) == 1
        assert isinstance(pushed[0], KeyDialog)


async def test_prompt_for_key_on_dismiss_verified_refreshes_key_status_and_dispatches_key_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeApp()
    async with app.run_test():
        resources = ResourceTable(cast(Session, object()))
        repo = _FakeRepo(key_status=KeyStatus.NO_KEY_PROVIDED)
        handle = resources.put_repo(cast(Repository, repo))
        dismiss_callbacks: list[Any] = []
        monkeypatch.setattr(app, "push_screen", lambda screen, callback=None, **kw: dismiss_callbacks.append(callback))
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
        # KeyVerified's own effect is covered by the
        # test_reload_catalogs_after_key_verified_* tests.


async def test_prompt_for_key_on_dismiss_unverified_still_refreshes_key_status_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        monkeypatch.setattr(app, "push_screen", lambda screen, callback=None, **kw: dismiss_callbacks.append(callback))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(PromptForKey(repo=handle, catalog=cast(Catalog, _FakeCatalog("cat-1"))))
        repo.key_status = KeyStatus.INVALID
        dismiss_callbacks[0](False)

        assert store.model.repos[handle].key_status == KeyStatus.INVALID
        assert store.model.selected_catalog is None  # KeyVerified never dispatched


async def test_switching_to_an_already_cached_workload_mid_fetch_leaves_no_stray_loading_row(monkeypatch: Any) -> None:
    """Workload A's ``LoadVersions`` is still in flight when the user
    switches to B, whose versions are cached (so no ``Cmd`` is dispatched):
    A's next animation tick must not re-append a "Loading" row onto B's
    table."""
    monkeypatch.setattr(progress_hint, "_FRAME_INTERVAL", 0.02)
    ticks = count_progress_ticks(monkeypatch)
    workload_a, workload_b = (
        make_workload(workload_id=1, display_name="A"),
        make_workload(workload_id=2, display_name="B"),
    )
    gate = asyncio.Event()

    @faithful_to(Catalog)
    class _GatedCatalog(_FakeCatalog):
        async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
            if workload.workload_id == workload_a.workload_id:
                await gate.wait()
            return await super().versions(workload)

    gated_catalog = _GatedCatalog("cat-1")
    gated_catalog.versions_result = [make_version()]
    catalog = cast(Catalog, gated_catalog)

    app = _FakeApp()
    async with app.run_test() as pilot:
        table = app.query_one("#col-versions", DataTable)
        table.add_column("Date")
        resources = ResourceTable(cast(Session, object()))
        handle = resources.put_repo(cast(Repository, _FakeRepo()))
        ck = catalog_key(handle, catalog)
        b_key = workload_key(ck, workload_b)
        initial_model = BrowseModel(
            repos={handle: RepoState(layout=_layout(), key_status=KeyStatus.NOT_ENCRYPTED)},
            selected_catalog=SelectedCatalog(repo=handle, catalog=catalog),
            workload_versions={b_key: Success((make_version(),))},
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
            prompt_for_key=lambda repo, on_dismiss: None,
        )

        store.dispatch(WorkloadSelected(workload=workload_a))
        await wait_until(
            pilot, lambda: table.row_count > 0 and "Loading" in str(table.get_row_at(0)[0]), timeout=SDK_TIMEOUT
        )

        store.dispatch(WorkloadSelected(workload=workload_b))
        # No render subscription here: repaint B's table the way
        # BrowseScreen's render would.
        table.clear()
        table.add_row("2026-01-01 00:00")

        # A's next tick, the one that could re-add its row, has run while its fetch is still gated.
        switched_at = ticks()
        await wait_until(pilot, lambda: ticks() > switched_at, timeout=SDK_TIMEOUT)
        rows = [str(table.get_row_at(i)[0]) for i in range(table.row_count)]
        assert rows == ["2026-01-01 00:00"], "A's late tick must not leak a stray Loading row onto B's own table"

        gate.set()
        a_key = workload_key(ck, workload_a)
        await wait_until(
            pilot, lambda: isinstance(store.model.workload_versions.get(a_key), Success), timeout=SDK_TIMEOUT
        )


async def test_a_refresh_of_workloads_invalidates_the_repository_caches_before_fetching() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        workload = make_workload(workload_id=1)
        catalog = cast(Catalog, _FakeCatalog("cat-1", workloads=[workload]))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadWorkloads(repo=handle, catalog=catalog, invalidate=True))
        key = catalog_key(handle, catalog)
        await wait_until(
            pilot, lambda: isinstance(store.model.catalog_workloads.get(key), Success), timeout=SDK_TIMEOUT
        )

        assert repo.invalidate_caches_calls == 1


async def test_an_ordinary_load_does_not_invalidate_anything() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_result = [make_version()]
        catalog = cast(Catalog, catalog_fake)
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=make_workload(workload_id=1)))
        key = workload_key(catalog_key(handle, catalog), make_workload(workload_id=1))
        await wait_until(
            pilot, lambda: isinstance(store.model.workload_versions.get(key), Success), timeout=SDK_TIMEOUT
        )

        assert repo.invalidate_caches_calls == 0


async def test_a_refresh_of_versions_invalidates_first_and_a_failed_invalidation_is_that_loads_failure() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        catalog_fake = _FakeCatalog("cat-1")
        catalog_fake.versions_result = [make_version()]
        catalog = cast(Catalog, catalog_fake)
        workload = make_workload(workload_id=1)
        repo = _FakeRepo()
        repo.invalidate_caches_error = RuntimeError("cannot drop caches")
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadVersions(repo=handle, catalog=catalog, workload=workload, invalidate=True))
        key = workload_key(catalog_key(handle, catalog), workload)
        await wait_until(
            pilot, lambda: isinstance(store.model.workload_versions.get(key), FailureInfo), timeout=SDK_TIMEOUT
        )

        assert repo.invalidate_caches_calls == 1
        assert store.model.workload_versions[key] == FailureInfo(message="cannot drop caches")


@faithful_to(Catalog)
class _VersionsGatedCatalog(_FakeCatalog):
    """A catalog whose ``versions()`` blocks until released, standing in for a
    slow fetch still running when a refresh asks to drop the caches."""

    def __init__(self, catalog_id: str) -> None:
        super().__init__(catalog_id)
        self.versions_started = asyncio.Event()
        self.release = asyncio.Event()

    async def versions(self, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
        self.versions_started.set()
        await self.release.wait()
        return self.versions_result


async def test_a_refresh_waits_for_a_load_already_running_before_dropping_the_caches() -> None:
    app = _FakeApp()
    async with app.run_test() as pilot:
        resources = ResourceTable(cast(Session, object()))
        slow = _VersionsGatedCatalog("cat-slow")
        slow.versions_result = [make_version()]
        refreshed = cast(Catalog, _FakeCatalog("cat-1", workloads=[make_workload(workload_id=1)]))
        repo = _FakeRepo()
        handle = resources.put_repo(cast(Repository, repo))
        store, effects = _make_store_and_effects(app, resources)
        store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))

        effects.perform(LoadVersions(repo=handle, catalog=cast(Catalog, slow), workload=make_workload(workload_id=2)))
        await wait_until(pilot, lambda: slow.versions_started.is_set(), timeout=SDK_TIMEOUT)
        effects.perform(LoadWorkloads(repo=handle, catalog=refreshed, invalidate=True))
        await settle(pilot)

        assert repo.invalidate_caches_calls == 0  # the running load still holds the gate

        slow.release.set()
        key = catalog_key(handle, refreshed)
        await wait_until(
            pilot, lambda: isinstance(store.model.catalog_workloads.get(key), Success), timeout=SDK_TIMEOUT
        )
        assert repo.invalidate_caches_calls == 1
        slow_key = workload_key(catalog_key(handle, cast(Catalog, slow)), make_workload(workload_id=2))
        assert isinstance(store.model.workload_versions.get(slow_key), Success)  # the slow load still delivered
