"""BrowseScreen catalog/workload/version fetching: error leaves and rows, the pending-versions placeholder, and reloading after a key change."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest
from textual.widgets import DataTable, Tree

from support.fakes import faithful_to
from support.model_factories import make_version, make_workload
from support.pilot import SDK_TIMEOUT, wait_until
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    CatalogsRequested,
    KeyVerified,
    WorkloadSelected,
)
from synology_apm_repo.browser.strings import (
    BROWSE_VERSIONS_EMPTY_LABEL,
)
from synology_apm_repo.sdk.api import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from unit.browser.browse_screen_fakes import (
    CountingCatalog,
    FakeCatalogWithId,
    discover,
    fake_repository,
    open_browse_screen,
)


async def test_catalogs_fetch_error_shows_an_error_leaf_under_that_repo_node(monkeypatch: pytest.MonkeyPatch) -> None:
    @faithful_to(Repository)
    class _FailingRepo:
        async def catalogs(self) -> list[Catalog]:
            raise ApmRepoError("boom")

    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        monkeypatch.setattr(repo, "catalogs", _FailingRepo().catalogs)
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))

        tree = screen.query_one("#col-catalogs", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children[0].children), timeout=SDK_TIMEOUT, interval=0.02)
        assert len(tree.root.children[0].children) == 1
        assert "error: boom" in str(tree.root.children[0].children[0].label)


async def test_workloads_fetch_error_shows_an_error_leaf_in_column_2() -> None:
    # Generic ApmRepoError only; the key-error branch is in tests/integration/browser/test_browser_app.py.
    @faithful_to(Catalog)
    class _FailingCatalog:
        catalog_id = CatalogId("cat-1")
        display_name = "catalog"

        async def workloads(self) -> list[Workload]:
            raise ApmRepoError("boom")

    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, _FailingCatalog())))

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=SDK_TIMEOUT, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A catalog missing on the post-key re-fetch renders an error leaf in column 2."""
    repo = fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        return None

    monkeypatch.setattr(repo, "catalog_by_id", _catalog_by_id)
    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("gone")))
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=SDK_TIMEOUT, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error:" in str(tree.root.children[0].label) and "gone" in str(tree.root.children[0].label)


async def test_reload_workloads_with_fresh_catalog_reports_an_error_when_the_catalog_fails_to_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising ``catalog_by_id()`` is reported as an error leaf, not propagated."""
    repo = fake_repository()

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        raise ApmRepoError("boom")

    monkeypatch.setattr(repo, "catalog_by_id", _catalog_by_id)
    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("anything")))
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children), timeout=SDK_TIMEOUT, interval=0.02)
        assert len(tree.root.children) == 1
        assert "error: boom" in str(tree.root.children[0].label)


async def test_reselecting_the_same_catalog_after_a_key_reload_uses_the_fresh_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ReloadCatalogsAfterKeyVerified`` swaps in fresh ``Catalog`` objects for the selected catalog and its siblings."""
    stale = FakeCatalogWithId(CatalogId("cat-1"))
    fresh = FakeCatalogWithId(CatalogId("cat-1"))
    stale_other = FakeCatalogWithId(CatalogId("cat-other"))
    fresh_other = FakeCatalogWithId(CatalogId("cat-other"))
    fresh_by_id = {CatalogId("cat-1"): fresh, CatalogId("cat-other"): fresh_other}
    repo = fake_repository()

    async def _catalogs() -> list[Catalog]:
        return [cast(Catalog, stale_other), cast(Catalog, stale)]

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        return cast(Catalog, fresh_by_id[catalog_id])

    monkeypatch.setattr(repo, "catalogs", _catalogs)
    monkeypatch.setattr(repo, "catalog_by_id", _catalog_by_id)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, lambda: bool(screen.store.model.repos[handle].catalogs.value))  # type: ignore[union-attr]

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: screen.store.model.selected_catalog is not None)

        assert screen.store.model.selected_catalog is not None
        assert screen.store.model.selected_catalog.catalog is cast(Catalog, fresh)
        refreshed = screen.store.model.repos[handle].catalogs.value  # type: ignore[union-attr]
        assert set(refreshed) == {cast(Catalog, fresh), cast(Catalog, fresh_other)}

        # Selecting it fetches workloads through the fresh object.
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, fresh)))
        await wait_until(pilot, lambda: fresh.workloads_calls > 0)


async def test_versions_placeholder_never_shows_while_the_first_fetch_is_still_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The "(no available versions)" placeholder must not appear before
    the first ``catalog.versions()`` resolves (``has_ever_resolved``)."""
    repo = fake_repository()
    catalog = CountingCatalog(CatalogId("cat-1"), [])
    workload = make_workload(workload_id=1, display_name="Workload-1")
    gate = asyncio.Event()

    async def _versions(w: Workload) -> list[Version]:
        catalog.versions_calls += 1
        await gate.wait()
        return []

    monkeypatch.setattr(catalog, "versions", _versions)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        versions_table = screen.query_one("#col-versions", DataTable)
        # DataTableLoadingRowSink's debounced row may or may not have
        # appeared yet, so assert only that the placeholder is absent.
        assert versions_table.row_count <= 1
        if versions_table.row_count == 1:
            assert str(versions_table.get_row_at(0)[0]) != BROWSE_VERSIONS_EMPTY_LABEL

        gate.set()
        # Wait on the placeholder text: a bare row_count == 1 could
        # already be satisfied by the loading row.
        await wait_until(
            pilot,
            lambda: (
                versions_table.row_count == 1 and str(versions_table.get_row_at(0)[0]) == BROWSE_VERSIONS_EMPTY_LABEL
            ),
        )


async def test_a_versions_fetch_failure_shows_an_error_row_and_a_reselect_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``catalog.versions()`` failure renders as a single error row in
    column 3, is not cached, and a reselect retries."""
    repo = fake_repository()
    catalog = CountingCatalog(CatalogId("cat-1"), [])
    workload = make_workload(workload_id=1, display_name="Workload-1")
    should_fail = True

    async def _versions(w: Workload) -> list[Version]:
        catalog.versions_calls += 1
        if should_fail:
            raise ApmRepoError("versions boom")
        return [make_version()]

    monkeypatch.setattr(catalog, "versions", _versions)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        versions_table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: versions_table.row_count == 1)
        assert "error: versions boom" in str(versions_table.get_row_at(0)[0])

        should_fail = False
        screen.store.dispatch(WorkloadSelected(workload=workload))
        await wait_until(pilot, lambda: catalog.versions_calls == 2)
        await wait_until(
            pilot, lambda: versions_table.row_count == 1 and "error" not in str(versions_table.get_row_at(0)[0])
        )
