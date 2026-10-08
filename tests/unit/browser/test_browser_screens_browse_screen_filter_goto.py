"""BrowseScreen tree filter (when it opens, debouncing, closing) and goto-ref failures."""

from __future__ import annotations

from typing import cast

import pytest
from textual.widgets import Input, Tree

from support.fakes import faithful_to
from support.model_factories import make_catalog, make_connection, make_workload
from support.pilot import UI_TIMEOUT, move_cursor_to, wait_until
from synology_apm_repo.browser.core.browse.model import FilterTree
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    CatalogsRequested,
    TreeFilterOpened,
)
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_CANONICAL_WARNING,
)
from synology_apm_repo.sdk.api import Catalog, Repository
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.browse_screen_fakes import (
    CountingCatalog,
    discover,
    fake_repository,
    open_browse_screen,
)


async def test_tree_filter_rerenders_workload_level_via_the_group_cache() -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        workloads = [
            make_workload(workload_id=1, display_name="Workload-1"),
            make_workload(workload_id=2, display_name="Workload-2"),
        ]
        catalog = CountingCatalog(CatalogId("cat-1"), workloads)
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: bool(tree.root.children))
        group_node = tree.root.children[0]

        await move_cursor_to(pilot, tree, group_node.children[0])
        screen.action_filter()
        await wait_until(pilot, lambda: screen.store.model.tree_filter is not None)
        screen._tree_filter.pending_text = "Workload-1"
        screen._tree_filter._commit()

        await wait_until(
            pilot,
            lambda: [c.data.payload.display_name for c in group_node.children if c.data is not None] == ["Workload-1"],
        )


async def test_tree_filter_debounces_before_rerendering(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        connection_a = make_connection(connection_id="cc-a")
        connection_b = make_connection(connection_id="cc-b")
        catalog_a = make_catalog(connection=connection_a)
        catalog_b = make_catalog(connection=connection_b)

        @faithful_to(Repository)
        class _StaticCatalogsRepo:
            async def catalogs(self) -> list[Catalog]:
                return [catalog_a, catalog_b]

        monkeypatch.setattr(repo, "catalogs", _StaticCatalogsRepo().catalogs)
        await discover(screen, repo)
        tree = screen.query_one("#col-catalogs", Tree)
        repo_node = tree.root.children[0]
        repo_node.expand()  # fires on_tree_node_expanded, as in the real UI
        await wait_until(pilot, lambda: len(repo_node.children) == 2)

        await move_cursor_to(pilot, tree, repo_node.children[0])
        screen.action_filter()
        await wait_until(pilot, lambda: screen.store.model.tree_filter is not None)
        original_children = list(repo_node.children)
        filter_input = screen.query_one("#filter-input", Input)
        filter_input.value = connection_a.display_name  # both fakes share this display_name
        screen.on_input_changed(Input.Changed(filter_input, filter_input.value))
        # Not rebuilt yet: the keystroke only armed the debounce.
        assert list(repo_node.children) == original_children

        await wait_until(
            pilot,
            lambda: (
                screen.store.model.tree_filter is not None
                and screen.store.model.tree_filter.text == connection_a.display_name
            ),
            timeout=UI_TIMEOUT,
            interval=0.02,
            message="debounced tree filter never settled",
        )


async def test_submit_goto_parse_failure_returns_without_resolving(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen._submit_goto("/some/path#not-canonical")
        assert warnings == [GOTO_REF_NOT_CANONICAL_WARNING]


async def test_submit_goto_version_lookup_failure_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (app, pilot, screen):
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        repo = fake_repository()

        async def failing_version_for_ref(node_ref: object) -> object:
            raise ApmRepoError("ref not found")

        monkeypatch.setattr(repo, "version_for_ref", failing_version_for_ref)
        app.repo_handle = app.resources.put_repo(repo)

        ref = NodeRef.canonical(
            "repo", catalog_id=CatalogId("1"), workload_id=WorkloadId(1), version_uid=VersionUid("v1")
        )
        screen._submit_goto(str(ref))
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == ["ref not found"]


async def test_enter_on_the_filter_input_closes_an_open_tree_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        catalog = make_catalog(connection=make_connection())

        async def _catalogs() -> list[Catalog]:
            return [catalog]

        monkeypatch.setattr(repo, "catalogs", _catalogs)
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogsRequested(repo=handle))
        tree = screen.query_one("#col-catalogs", Tree)
        await wait_until(pilot, lambda: tree.root.children[0].children)
        screen.store.dispatch(TreeFilterOpened(tree=FilterTree.CATALOGS, parent_key=handle))
        filter_input = screen.query_one("#filter-input", Input)
        filter_input.add_class("active")

        screen.on_input_submitted(Input.Submitted(filter_input, ""))
        await wait_until(pilot, lambda: screen.store.model.tree_filter is None)
        assert not filter_input.has_class("active")
