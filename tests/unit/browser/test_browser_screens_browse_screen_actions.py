"""BrowseScreen key actions: refresh and reconnect (refused while an export runs), verbose relabelling, go-back, diagnostics, the loading indicator; and the footer bindings."""

from __future__ import annotations

from typing import cast

import pytest
from textual.widgets import Input, Static, Tree

from support.model_factories import make_workload
from support.pilot import focus_widget, move_cursor_to, settle, wait_for_screen, wait_until
from synology_apm_repo.browser.core.app.model import Job, JobStatus
from synology_apm_repo.browser.core.browse.msg import (
    CatalogSelected,
    RescanStarted,
    VersionFilterOpened,
)
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.strings import (
    RECONNECT_EXPORT_BUSY_WARNING,
    REFRESH_EXPORT_BUSY_WARNING,
)
from synology_apm_repo.sdk.api import Catalog
from synology_apm_repo.sdk.identifiers import (
    CatalogId,
)
from unit.browser.browse_screen_fakes import (
    CountingCatalog,
    discover,
    fake_repository,
    open_browse_screen,
)


async def test_set_loading_indicator_appends_markup_to_the_breadcrumb() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        screen._set_loading_indicator("[dim]spinning[/dim]")
        breadcrumb = str(screen.query_one("#breadcrumb", Static).render())
        assert "spinning" in breadcrumb


async def test_action_refresh_dispatches_when_something_is_selected_else_reopens_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        repo = fake_repository()
        catalog = CountingCatalog(CatalogId("cat-1"), [make_workload(workload_id=1, display_name="Workload-1")])
        handle = await discover(screen, repo)
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)

        screen.action_refresh()
        await wait_until(pilot, lambda: catalog.workloads_calls == 2)

        connect_calls: list[bool] = []
        monkeypatch.setattr(screen, "action_connect_remote", lambda: connect_calls.append(True))
        screen.store.dispatch(RescanStarted(scan_path=""))
        screen.action_refresh()
        assert connect_calls == [True]


async def test_action_refresh_is_refused_while_an_export_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (app, pilot, screen):
        catalog = CountingCatalog(CatalogId("cat-1"), [make_workload(workload_id=1, display_name="Workload-1")])
        handle = await discover(screen, fake_repository())
        screen.store.dispatch(CatalogSelected(repo=handle, catalog=cast(Catalog, catalog)))
        await wait_until(pilot, lambda: catalog.workloads_calls == 1)
        app.jobs = {JobId(1): Job(id=JobId(1), label="export x", group="job-1")}
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_refresh()

        assert warnings == [REFRESH_EXPORT_BUSY_WARNING]
        assert catalog.workloads_calls == 1  # no re-fetch


@pytest.mark.parametrize("status", [JobStatus.RUNNING, JobStatus.QUEUED])
async def test_reconnecting_is_refused_while_an_export_is_pending(
    status: JobStatus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``c`` and ``Esc`` on the root both end by closing every open repository, which an export reads from."""
    async with open_browse_screen() as (app, _pilot, screen):
        handle = await discover(screen, fake_repository())
        app.jobs = {JobId(1): Job(id=JobId(1), label="export x", group="job-1", status=status)}
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_connect_remote()
        screen.action_go_back()

        assert warnings == [RECONNECT_EXPORT_BUSY_WARNING] * 2
        assert isinstance(app.screen, BrowseScreen)  # no ConnectDialog was pushed
        assert handle in screen.store.model.repos  # nothing was closed


async def test_refresh_for_verbose_mode_relabels_repo_nodes() -> None:
    async with open_browse_screen() as (app, pilot, screen):
        repo = fake_repository()
        await discover(screen, repo)
        tree = screen.query_one("#col-catalogs", Tree)
        await wait_until(pilot, lambda: tree.root.children)
        before = str(tree.root.children[0].label)

        app.verbose = True
        screen.refresh_for_verbose_mode()
        after = str(tree.root.children[0].label)
        assert after != before
        assert "layout: object_store" in after


async def test_on_tree_node_selected_with_no_data_is_a_no_op() -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        tree = screen.query_one("#col-catalogs", Tree)
        error_leaf = tree.root.add_leaf("(error)", data=None)
        await focus_widget(pilot, tree)
        await move_cursor_to(pilot, tree, error_leaf)
        await pilot.press("enter")  # must not raise
        await settle(pilot)


async def test_action_go_back_closes_version_filter_before_falling_through() -> None:
    async with open_browse_screen() as (app, pilot, screen):
        screen.store.dispatch(VersionFilterOpened())
        screen.query_one("#filter-input", Input).add_class("active")

        screen.action_go_back()
        await wait_until(pilot, lambda: screen.store.model.version_filter is None)
        assert isinstance(app.screen, BrowseScreen)


async def test_action_go_back_closes_an_open_goto_box() -> None:
    async with open_browse_screen() as (app, pilot, screen):
        screen.action_goto_ref()
        await wait_until(pilot, lambda: screen.query_one("#goto-input", Input).has_class("active"))

        screen.action_go_back()
        await wait_until(pilot, lambda: not screen.query_one("#goto-input", Input).has_class("active"))
        assert isinstance(app.screen, BrowseScreen)  # Esc closed only the box: no reset, no ConnectDialog


async def test_action_show_diagnostics_pushes_the_diagnostics_screen(monkeypatch: pytest.MonkeyPatch) -> None:
    from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

    async with open_browse_screen() as (app, pilot, screen):

        async def fake_verify(level: object, **kwargs: object) -> list[object]:
            return []

        repo = fake_repository()
        monkeypatch.setattr(repo, "verify", fake_verify)
        app.repo_handle = app.resources.put_repo(repo)
        screen.action_show_diagnostics()
        await wait_for_screen(pilot, DiagnosticsScreen)


async def test_footer_shows_only_connect_goto_ref_quit_and_help() -> None:
    async with open_browse_screen() as (_app, _pilot, screen):
        shown = {key for key, active in screen.active_bindings.items() if active.binding.show}
        assert shown == {"c", "g", "q", "question_mark"}


async def test_footer_hidden_bindings_still_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    async with open_browse_screen() as (_app, pilot, screen):
        refresh_calls = [0]
        filter_calls = [0]
        monkeypatch.setattr(screen, "action_refresh", lambda: refresh_calls.__setitem__(0, refresh_calls[0] + 1))
        monkeypatch.setattr(screen, "action_filter", lambda: filter_calls.__setitem__(0, filter_calls[0] + 1))

        await pilot.press("r")
        await pilot.press("slash")
        assert refresh_calls[0] == 1
        assert filter_calls[0] == 1

        # "d"/"t" resolve on ApmRepoBrowserApp, which FakeApp does not
        # implement; they only need to not raise.
        await pilot.press("d")
        await pilot.press("t")
