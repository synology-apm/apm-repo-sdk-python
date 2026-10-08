"""UnitScreen key actions: export, copy ref, hex preview, refresh, the verbose
reload, diagnostics and go-back, including their nothing-selected guards and
failure toasts; and the footer bindings."""

from __future__ import annotations

import asyncio

import pytest
from textual.widgets import Input, Static, Tree

from support.fakes import faithful_to
from support.model_factories import make_version
from support.pilot import SDK_TIMEOUT, settle, wait_for_screen, wait_until
from synology_apm_repo.browser.core.app.model import Job
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.strings import (
    REFRESH_EXPORT_BUSY_WARNING,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_NOTHING_SELECTED_WARNING,
)
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import Node, NodeRole, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.browser.unit_screen_fakes import (
    ConfigurableProvider,
    FakeApp,
    FakeContentSource,
    FakeRepo,
    leaf_node,
)


async def test_export_and_copy_ref_and_hex_preview_warn_when_nothing_is_selected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        monkeypatch.setattr(screen, "_selected_node", lambda: None)

        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_export_selected()
        assert warnings == [UNIT_NOTHING_SELECTED_WARNING]

        warnings.clear()
        screen.action_copy_ref()
        assert warnings == [UNIT_COPY_REF_NOTHING_SELECTED_WARNING]

        warnings.clear()
        app.verbose = True
        screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_hex_preview_outside_verbose_mode_is_a_no_op_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_hex_preview()
        assert warnings == ["press d to enable verbose mode first"]


async def test_copy_ref_with_a_real_selection_copies_and_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: leaf)

        copied: list[str] = []
        monkeypatch.setattr(app, "copy_to_clipboard", lambda text: copied.append(text))
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_copy_ref()
        assert copied == [str(leaf.ref)]
        assert warnings == ["copied ref to clipboard"]


async def test_export_selected_with_a_real_leaf_pushes_the_export_screen(monkeypatch: pytest.MonkeyPatch) -> None:
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=FakeContentSource(b"data"),  # type: ignore[arg-type]
    )
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: leaf)

        screen.action_export_selected()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted)


async def test_export_selected_on_a_folder_pushes_the_export_screen_for_the_whole_folder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from synology_apm_repo.browser.core.app.model import FolderExport
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "docs")), name="docs", is_leaf=False)
    provider = ConfigurableProvider(root, {str(root_ref): [folder]})
    repo = FakeRepo(provider)
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: folder)
        app.verbose = True

        screen.action_export_selected()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted)

        target = app.screen._target  # type: ignore[attr-defined]
        assert isinstance(target, FolderExport)
        assert (target.node, target.name, target.force_raw) == (folder, "docs", True)
        assert target.repo is repo  # type: ignore[comparison-overlap]


@pytest.mark.parametrize(
    "role",
    [NodeRole.ORDINARY, NodeRole.LIST_OVERVIEW, NodeRole.FLAT_CATEGORY],
    ids=["plain folder", "site list group", "site list category"],
)
async def test_e_on_a_subfolder_row_of_the_file_table_exports_that_folder(role: NodeRole) -> None:
    """The SharePoint List group and List category are non-leaf nodes the browser treats specially elsewhere (no
    tree expansion, an overview pane); for an export they are folders like any other."""
    from synology_apm_repo.browser.core.app.model import FolderExport
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "docs")), name="docs", is_leaf=False, role=role)
    provider = ConfigurableProvider(root, {str(root_ref): [folder]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        await wait_until(pilot, lambda: len(screen._file_table._nodes) == 1)
        table = screen.file_table
        table.focus()
        await wait_until(pilot, lambda: table.has_focus)
        table.move_cursor(row=0)
        await wait_until(pilot, lambda: screen._selected_node() == folder)

        screen.action_export_selected()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted)

        target = app.screen._target  # type: ignore[attr-defined]
        assert isinstance(target, FolderExport)
        assert (target.node, target.name) == (folder, "docs")


async def test_export_selected_shows_the_loading_indicator_while_the_fetch_is_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fetch ends while ``ExportScreen`` covers the screen, which skips
    breadcrumb renders; the cleared indicator shows once it resumes
    (``on_screen_resume``)."""
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    gate = asyncio.Event()

    @faithful_to(UnitProvider)
    class _SlowProvider(ConfigurableProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            await gate.wait()
            return await super().unit(node)

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=FakeContentSource(b"data"),  # type: ignore[arg-type]
    )
    provider = _SlowProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: leaf)

        screen.action_export_selected()
        breadcrumb = screen.query_one("#breadcrumb", Static)
        await wait_until(pilot, lambda: "Loading" in str(breadcrumb.render()), timeout=SDK_TIMEOUT)

        gate.set()
        await wait_until(pilot, lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted)

        app.pop_screen()
        await wait_until(pilot, lambda: app.screen is screen)
        await wait_until(pilot, lambda: "Loading" not in str(breadcrumb.render()))


async def test_export_selected_degrades_to_a_toast_instead_of_crashing_the_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-``ApmRepoError`` from ``provider.unit()`` becomes a toast, not a crash."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})  # no units_by_ref entry for `leaf`
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: leaf)

        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_export_selected()
        await wait_until(pilot, lambda: bool(warnings))

        assert isinstance(app.screen, UnitScreen)  # still here -- no crash, no screen pushed
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_degrades_to_a_toast_instead_of_crashing_the_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-``ApmRepoError`` from ``provider.unit()`` becomes a toast, not a crash."""
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        monkeypatch.setattr(screen, "_selected_node", lambda: leaf)

        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_hex_preview()
        await wait_until(pilot, lambda: bool(warnings))

        assert isinstance(app.screen, UnitScreen)
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_selected_but_not_a_leaf_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="folder", is_leaf=False)
    provider = ConfigurableProvider(root, {str(root_ref): [folder]})
    app = FakeApp(make_version(), FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        monkeypatch.setattr(screen, "_selected_node", lambda: folder)

        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_action_refresh_reloads_the_tree_from_the_root() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = leaf_node("item.bin", "item")
    provider = ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        provider_calls_before = app._repo.catalog.provider_calls

        screen.action_refresh()
        await wait_until(pilot, lambda: app._repo.catalog.provider_calls > provider_calls_before)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        assert len(screen._file_table._nodes) == 1
        assert app._repo.catalog.provider_calls > provider_calls_before  # re-fetched, not just re-rendered
        assert app._repo.invalidate_caches_calls == 1  # a real re-scan, not stale cached listings


async def test_action_refresh_is_refused_while_an_export_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    app = FakeApp(make_version(), FakeRepo(ConfigurableProvider(root, {})))
    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        await wait_until(pilot, lambda: root_ref in screen.store.model.loaded)
        app.jobs = {JobId(1): Job(id=JobId(1), label="export x", group="job-1")}
        warnings: list[str] = []
        monkeypatch.setattr(screen, "notify", lambda message, **kwargs: warnings.append(message))

        screen.action_refresh()

        assert warnings == [REFRESH_EXPORT_BUSY_WARNING]
        assert app._repo.invalidate_caches_calls == 0


async def test_action_show_diagnostics_pushes_the_diagnostics_screen() -> None:
    from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_show_diagnostics()
        await wait_for_screen(pilot, DiagnosticsScreen)


async def test_go_back_closes_an_open_goto_box_before_popping() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    app = FakeApp(make_version(), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_goto_ref()
        await wait_until(pilot, lambda: screen.query_one("#goto-input", Input).has_class("active"))

        screen.action_go_back()
        await wait_until(pilot, lambda: not screen.query_one("#goto-input", Input).has_class("active"))
        assert isinstance(app.screen, UnitScreen)  # the box closed; the screen itself is still open

        # With nothing open, Esc pops the screen.
        screen.action_go_back()
        await wait_until(pilot, lambda: app.screen is not screen)


async def test_refresh_for_verbose_mode_reloads_for_a_saas_version() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    repo = FakeRepo(provider)
    app = FakeApp(make_version(target_type="M365"), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert repo.catalog.provider_calls == 1  # the initial LoadRoot on mount

        screen.refresh_for_verbose_mode()
        # The fake returns the same provider either way, so count calls.
        await wait_until(pilot, lambda: repo.catalog.provider_calls == 2)


async def test_refresh_for_verbose_mode_is_a_no_op_for_a_non_saas_version() -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = ConfigurableProvider(root, {})
    app = FakeApp(make_version(target_type="VM"), FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#folder-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        handle_before = screen.store.model.provider

        screen.refresh_for_verbose_mode()
        await settle(pilot)
        assert screen.store.model.provider is handle_before  # untouched — never reset/reloaded


async def test_footer_shows_only_back_export_detail_and_help() -> None:
    repo = FakeRepo(None, provider_error=ApmRepoError("irrelevant to this test"))
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, UnitScreen)
        shown = {key for key, active in screen.active_bindings.items() if active.binding.show}
        assert shown == {"escape", "e", "i", "question_mark"}


async def test_footer_hidden_bindings_still_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = FakeRepo(None, provider_error=ApmRepoError("irrelevant to this test"))
    app = FakeApp(make_version(), repo)
    async with app.run_test() as pilot:
        screen = await wait_for_screen(pilot, UnitScreen)

        refresh_calls = [0]
        filter_calls = [0]
        monkeypatch.setattr(screen, "action_refresh", lambda: refresh_calls.__setitem__(0, refresh_calls[0] + 1))
        monkeypatch.setattr(screen, "action_filter", lambda: filter_calls.__setitem__(0, filter_calls[0] + 1))

        await pilot.press("r")
        await pilot.press("slash")
        assert refresh_calls[0] == 1
        assert filter_calls[0] == 1

        # "d"/"t" are ApmRepoBrowserApp actions that FakeApp lacks; they only need to not raise.
        await pilot.press("d")
        await pilot.press("t")
