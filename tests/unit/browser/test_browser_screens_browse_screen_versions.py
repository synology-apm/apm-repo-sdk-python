"""BrowseScreen version column: where the cursor lands, filtering, and restoring the cursor afterwards."""

from __future__ import annotations

from typing import Any, cast

import pytest
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Tree

from support.model_factories import make_version, make_workload
from support.pilot import settle, wait_until
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    VersionFilterOpened,
    VersionFilterTextChanged,
    WorkloadSelected,
)
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.sdk.api import Catalog, Version, Workload
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from unit.browser.browse_screen_fakes import (
    CountingCatalog,
    discover,
    fake_repository,
    open_browse_screen,
)


async def test_saas_only_workloads_land_the_cursor_on_a_leaf_not_root() -> None:
    """The leaf sits inside the platform/tenant/sub_type nesting."""
    workload = make_workload(
        workload_uid="wl-uid",
        workload_type="M365",
        sub_type="USER_MAILBOX",
        display_name="mailbox",
        spec={"spec": {"tenant_id": "tenant-1"}},
    )
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        catalog = CountingCatalog(CatalogId("cat-1"), [workload])
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        tree = screen.query_one("#col-workloads", Tree)
        await wait_until(pilot, lambda: tree.cursor_node is not None and tree.cursor_node.data is not None)
        cursor_node = tree.cursor_node
        assert cursor_node is not None and cursor_node.data is not None
        assert cursor_node.data.payload is workload


async def test_render_versions_filter_excludes_non_matching_names(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        catalog = CountingCatalog(CatalogId("cat-1"), [])
        workload = make_workload(workload_id=1, display_name="Workload-1")
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        monkeypatch.setattr(catalog, "versions", _named_versions_fn(["Monday backup", "Tuesday backup"]))
        screen.store.dispatch(WorkloadSelected(workload=workload))
        table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: table.row_count == 2)

        screen.store.dispatch(VersionFilterOpened())
        screen.store.dispatch(VersionFilterTextChanged(text="monday"))
        # Dispatch-driven, so this also proves the Store subscription
        # reacts to a filter-only change.
        await wait_until(pilot, lambda: table.row_count == 1)
        assert screen._visible_version_indices == [0]


def _named_versions_fn(names: list[str]) -> Any:
    async def _versions(workload: Workload) -> list[Version]:
        return [make_version(version_id=i, version_uid=f"vuid-{i}", display_name=name) for i, name in enumerate(names)]

    return _versions


async def test_render_versions_restores_cursor_to_the_same_version_after_filtering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Filtering rebuilds the table with ``DataTable.clear()``."""
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        catalog = CountingCatalog(CatalogId("cat-1"), [])
        workload = make_workload(workload_id=1, display_name="Workload-1")
        monkeypatch.setattr(catalog, "versions", _named_versions_fn(["Apple backup", "Banana backup", "Cherry backup"]))
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        screen.store.dispatch(WorkloadSelected(workload=workload))
        table = screen.query_one("#col-versions", DataTable)
        await wait_until(pilot, lambda: table.row_count == 3)
        table.cursor_coordinate = Coordinate(2, 0)  # parked on "Cherry backup"
        await wait_until(pilot, lambda: table.cursor_row == 2)

        screen.store.dispatch(VersionFilterOpened())
        screen.store.dispatch(VersionFilterTextChanged(text="e"))  # matches Apple/Cherry (not Banana)
        await wait_until(pilot, lambda: table.row_count == 2)

        assert table.row_count == 2
        assert screen._visible_version_indices == [0, 2]
        assert table.cursor_row == 1  # "Cherry backup", now the second visible row


async def test_on_data_table_row_selected_ignores_a_foreign_table() -> None:
    from textual.widgets.data_table import RowKey

    async with open_browse_screen() as (app, pilot, screen):
        foreign_table: DataTable[str] = DataTable(id="not-col-versions")
        event = DataTable.RowSelected(data_table=foreign_table, cursor_row=0, row_key=RowKey("x"))
        screen.on_data_table_row_selected(event)  # must not raise or push a screen
        await settle(pilot)
        assert isinstance(app.screen, BrowseScreen)
