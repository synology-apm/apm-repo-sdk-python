"""BrowseScreen stale-fetch races on its widgets: a late catalogs fetch and the cursor, auto-park across a rescan, a filter keeping a surviving leaf, and a partial reload."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest
from textual.widgets import DataTable, Tree

from support.model_factories import make_catalog, make_connection, make_workload
from support.pilot import SDK_TIMEOUT, move_cursor_to, settle, wait_until
from synology_apm_repo.browser.core.browse.model import FilterTree
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    CatalogsRequested,
    KeyVerified,
    TreeFilterClosed,
    TreeFilterOpened,
    TreeFilterTextChanged,
    WorkloadSelected,
)
from synology_apm_repo.browser.core.browse.select import WorkloadGroupKey
from synology_apm_repo.sdk.api import Catalog
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


async def test_a_late_catalogs_fetch_does_not_steal_the_cursor_from_elsewhere(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = fake_repository()
    catalog = make_catalog(connection=make_connection())
    started, gate = asyncio.Event(), asyncio.Event()

    async def _gated_catalogs() -> list[Catalog]:
        started.set()
        await gate.wait()
        return [catalog]

    monkeypatch.setattr(repo, "catalogs", _gated_catalogs)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_node)

        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(pilot, started.is_set)

        # Move away before the fetch resolves.
        await move_cursor_to(pilot, tree, tree.root)

        gate.set()
        await wait_until(pilot, lambda: repo_node.children)
        await settle(pilot)

        assert tree.cursor_node is tree.root  # not stolen back onto repo_node's first catalog


async def test_auto_park_fires_again_after_a_rescan_with_prior_navigation(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_a = fake_repository("@ActiveProtectData/repo-a")
    catalog_a = make_catalog(connection=make_connection())

    async def _catalogs_a() -> list[Catalog]:
        return [catalog_a]

    monkeypatch.setattr(repo_a, "catalogs", _catalogs_a)

    repo_b = fake_repository("@ActiveProtectData/repo-b")
    catalog_b = make_catalog(connection=make_connection())

    async def _catalogs_b() -> list[Catalog]:
        return [catalog_b]

    monkeypatch.setattr(repo_b, "catalogs", _catalogs_b)

    async with open_browse_screen() as (_app, pilot, screen):
        tree = screen.query_one("#col-catalogs", Tree)

        await discover(screen, repo_a)
        repo_a_node = tree.root.children[0]
        await move_cursor_to(pilot, tree, repo_a_node)
        # A real expand: on_tree_node_expanded dispatches CatalogsRequested.
        repo_a_node.expand()
        await wait_until(pilot, lambda: repo_a_node.children)
        await wait_until(pilot, lambda: tree.cursor_node is repo_a_node.children[0])  # auto-park fired for A

        await discover(screen, repo_b)
        # Reset by the rescan, not left dangling on A's own removed node.
        await wait_until(pilot, lambda: tree.cursor_node is tree.root)
        repo_b_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, repo_b_node)
        repo_b_node.expand()
        await wait_until(pilot, lambda: repo_b_node.children)
        await wait_until(pilot, lambda: tree.cursor_node is repo_b_node.children[0])  # auto-park fired again for B


async def test_filtering_column_two_preserves_a_surviving_leafs_own_widget_and_its_cache() -> None:
    workloads = [
        make_workload(workload_id=1, display_name="Workload-1"),
        make_workload(workload_id=2, display_name="Workload-2"),
    ]
    repo = fake_repository()
    catalog = CountingCatalog(CatalogId("cat-1"), workloads)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children))
        group_node = tree.root.children[0]
        original_leaf = next(
            c for c in group_node.children if c.data is not None and c.data.payload.display_name == "Workload-1"
        )

        screen.store.dispatch(WorkloadSelected(workload=workloads[0]))
        await wait_until(pilot, lambda: catalog.versions_calls == 1)

        assert group_node.data is not None
        group_key = group_node.data.key
        assert isinstance(group_key, WorkloadGroupKey)
        screen.store.dispatch(TreeFilterOpened(tree=FilterTree.WORKLOADS, parent_key=group_key))
        screen.store.dispatch(TreeFilterTextChanged(text="Workload-1"))
        await wait_until(pilot, lambda: len(group_node.children) == 1)
        screen.store.dispatch(TreeFilterClosed())  # empty filter text -> full list restored
        await wait_until(pilot, lambda: len(group_node.children) == 2)

        rebuilt_leaf = next(
            c for c in group_node.children if c.data is not None and c.data.payload.display_name == "Workload-1"
        )
        assert rebuilt_leaf is original_leaf, "the reconciler must keep a surviving leaf's own TreeNode"

        screen.store.dispatch(WorkloadSelected(workload=workloads[0]))
        await settle(pilot)
        assert catalog.versions_calls == 1  # served from cache, not re-fetched
        assert screen.query_one("#col-versions", DataTable).row_count == 1


async def test_reload_refreshes_the_target_even_when_a_sibling_fails_to_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The target's entry is replaced by the re-fetched object (a distinct
    one, so a reload that kept the stale entry is visible); the sibling,
    whose re-fetch fails, keeps its stale entry."""
    stale_other = FakeCatalogWithId(CatalogId("cat-other"))
    stale_target = FakeCatalogWithId(CatalogId("cat-1"))
    fresh = FakeCatalogWithId(CatalogId("cat-1"))
    repo = fake_repository()

    async def _catalogs() -> list[Catalog]:
        return [cast(Catalog, stale_other), cast(Catalog, stale_target)]

    async def _catalog_by_id(catalog_id: CatalogId) -> Catalog | None:
        if catalog_id == CatalogId("cat-other"):
            raise ApmRepoError("sibling boom")
        return cast(Catalog, fresh)

    monkeypatch.setattr(repo, "catalogs", _catalogs)
    monkeypatch.setattr(repo, "catalog_by_id", _catalog_by_id)

    async with open_browse_screen() as (_app, pilot, screen):
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        await wait_until(
            pilot,
            lambda: bool(screen.store.model.repos[handle].catalogs.value),  # type: ignore[union-attr]
            timeout=SDK_TIMEOUT,
        )
        before = screen.store.model.repos[handle].catalogs.value  # type: ignore[union-attr]
        assert set(before) == {cast(Catalog, stale_target), cast(Catalog, stale_other)}

        screen.store.dispatch(KeyVerified(repo=handle, catalog_id=CatalogId("cat-1")))
        await wait_until(pilot, lambda: screen.store.model.selected_catalog is not None, timeout=SDK_TIMEOUT)

        selected = screen.store.model.selected_catalog
        assert selected is not None
        assert selected.catalog is cast(Catalog, fresh)
        refreshed = screen.store.model.repos[handle].catalogs.value  # type: ignore[union-attr]
        assert set(refreshed) == {cast(Catalog, fresh), cast(Catalog, stale_other)}
