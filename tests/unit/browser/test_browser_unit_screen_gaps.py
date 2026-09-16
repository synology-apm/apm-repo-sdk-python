"""Unit tests for ``UnitScreen`` covering the branches none of this
package's other ``unit_screen``-focused
test files reach: repository/provider-level errors, the detail/preview/List-
overview edge cases, the "nothing selected" guards, load-more/filter
error and edge paths, and goto-ref's error/not-found handling. Driven
through a real Textual ``Pilot`` against a configurable fake provider,
same ``_FakeApp``/``_FakeRepo`` convention as
``test_browser_unit_screen_pagination.py``/``test_browser_unit_screen_children_error_handling.py``
(duplicated here rather than imported — see ``tests/CLAUDE.md``'s "no
test module ever imports from another")."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Input, Static, Tree

from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
    UNIT_NOTHING_SELECTED_WARNING,
)
from synology_apm_repo.sdk.api import Version
from synology_apm_repo.sdk.errors import ApmRepoError
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
from synology_apm_repo.sdk.units.base import Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.site import SITE_LIST_OVERVIEW_ATTR


def _version(target_type: str = "VM") -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid-1"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type=target_type,
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


class _ContentSource:
    supports_concurrent_export = False

    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return self._data


class _ConfigurableProvider:
    """A plain ``UnitProvider`` over a hand-built ``ref -> children`` map,
    with a few knobs the other unit_screen test files' fakes don't need:
    ``raise_children_for`` (a set of ref strings whose ``children()``
    raises ``ApmRepoError``) and ``units_by_ref`` (for
    ``provider.unit()``, needed by preview/List-overview/export/hex)."""

    def __init__(
        self,
        root: Node,
        children_by_ref: dict[str, list[Node]] | None = None,
        *,
        raise_children_for: set[str] = frozenset(),  # type: ignore[assignment]
        units_by_ref: dict[str, RestorableUnit] | None = None,
    ) -> None:
        self._root = root
        self._children_by_ref = children_by_ref or {}
        self._raise_children_for = raise_children_for
        self._units_by_ref = units_by_ref or {}

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if str(node.ref) in self._raise_children_for:
            raise ApmRepoError(f"boom at {node.ref}")
        items = self._children_by_ref.get(str(node.ref), [])
        return items[offset : offset + limit] if limit is not None else items[offset:]

    async def unit(self, node: Node) -> RestorableUnit:
        unit = self._units_by_ref.get(str(node.ref))
        if unit is None:
            raise NotImplementedError(f"no fake unit registered for {node.ref}")
        return unit


class _FakeCatalog:
    """Stands in for ``api.Catalog`` — ``UnitScreen`` now dispatches its
    root provider through the catalog, not the repository directly (see
    ``unit_screen.py``'s own ``_load_root``)."""

    def __init__(self, provider: _ConfigurableProvider | None, *, provider_error: ApmRepoError | None = None) -> None:
        self._provider = provider
        self._provider_error = provider_error
        self.provider_calls = 0

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId("catalog-1")

    async def provider(
        self, version: Version, *, object_db_id: str | None = None, force_raw: bool = False
    ) -> _ConfigurableProvider:
        self.provider_calls += 1
        if self._provider_error is not None:
            raise self._provider_error
        assert self._provider is not None
        return self._provider


class _FakeRepo:
    def __init__(
        self,
        provider: _ConfigurableProvider | None,
        *,
        provider_error: ApmRepoError | None = None,
        version_for_ref_error: ApmRepoError | None = None,
        version_for_ref_result: Version | None = None,
    ) -> None:
        self.catalog = _FakeCatalog(provider, provider_error=provider_error)
        self._version_for_ref_error = version_for_ref_error
        self._version_for_ref_result = version_for_ref_result
        self.invalidate_directory_cache_calls = 0

    async def version_for_ref(self, node_ref: NodeRef) -> tuple[_FakeCatalog, Version]:
        if self._version_for_ref_error is not None:
            raise self._version_for_ref_error
        assert self._version_for_ref_result is not None
        return self.catalog, self._version_for_ref_result

    async def verify(self, level: object, **kwargs: object) -> list[object]:
        # UnitScreen itself never calls this — only here because ``d``'s
        # own action_toggle_verbose -> action_show_diagnostics test below
        # pushes a real DiagnosticsScreen, which does.
        return []

    async def invalidate_directory_cache(self) -> None:
        self.invalidate_directory_cache_calls += 1


class _FakeApp(App[None]):
    """See ``test_browser_unit_screen_pagination.py``'s own identical
    class for why a bare ``App`` (not ``ApmRepoBrowserApp``) is enough
    here: ``UnitScreen`` only ever reads ``app_state.repo``/``.verbose``."""

    def __init__(self, version: Version, repo: _FakeRepo, *, target_ref: NodeRef | None = None) -> None:
        super().__init__()
        self.repo = repo
        self.verbose = False
        self._version = version
        self._target_ref = target_ref

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(UnitScreen(self.repo.catalog, self._version, target_ref=self._target_ref))  # type: ignore[arg-type]


def _leaf(name: str, ref_segment: str, **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", ref_segment)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


# -- _load_root error path -----------------------------------------------


async def test_load_root_provider_error_shows_in_the_detail_pane(wait_until: Any) -> None:
    repo = _FakeRepo(None, provider_error=ApmRepoError("repo is locked"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        detail = app.screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert "repo is locked" in str(detail.render())


# -- detail / preview -------------------------------------------------


async def test_detail_shows_ref_and_attrs_only_in_verbose_mode(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item", attrs={"custom": "value"})
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(leaf)
        detail_non_verbose = str(screen.query_one("#detail", Static).render())

        app.verbose = True
        screen._show_detail(leaf)
        detail_verbose = str(screen.query_one("#detail", Static).render())

        assert "ref:" not in detail_non_verbose
        assert "custom" not in detail_non_verbose
        assert "ref:" in detail_verbose
        assert "custom: value" in detail_verbose


async def test_show_detail_and_load_preview_are_no_ops_without_a_provider(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._reset_tree()  # provider -> None
        assert screen._provider is None
        leaf = _leaf("item.bin", "item")
        screen._show_detail(leaf)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._load_preview(leaf)  # must not raise
        await pilot.pause()
        # No provider -> _load_preview returns immediately; the detail
        # pane still shows only the header _show_detail() wrote.
        assert str(screen.query_one("#detail", Static).render()) == header_only


async def test_late_preview_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(b"<html><body><p>hello world</p></body></html>"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(leaf)  # sets _detail_pane._node = leaf, schedules a preview worker
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)
        # The preview must never have been appended — the detail pane
        # still shows only the header _show_detail() itself wrote.
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "hello world" not in header_only


# -- SharePoint List overview -------------------------------------------


def _list_overview_node() -> Node:
    return Node(
        ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, attrs={SITE_LIST_OVERVIEW_ATTR: True}
    )


async def test_list_overview_is_a_no_op_without_a_provider(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._reset_tree()
        detail_before = str(screen.query_one("#detail", Static).render())
        screen._load_list_overview(_list_overview_node())  # must not raise
        await pilot.pause()
        # No provider -> _load_list_overview returns immediately, before
        # ever touching the detail pane.
        assert str(screen.query_one("#detail", Static).render()) == detail_before


async def test_list_overview_children_error_shows_in_detail_pane(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(
        root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "error:" in str(detail.render()))
        assert f"boom at {overview_node.ref}" in str(detail.render())


async def test_list_overview_error_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(
        root, {str(root_ref): [overview_node]}, raise_children_for={str(overview_node.ref)}
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "error:" not in header_only


async def test_list_overview_success_for_a_node_the_user_moved_away_from_is_discarded(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    provider = _ConfigurableProvider(root, {str(root_ref): [overview_node]})  # empty children -> "(no items)"
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        header_only = str(screen.query_one("#detail", Static).render())
        screen._detail_pane._node = None  # simulate having moved on before the worker resolves
        await pilot.pause(0.1)
        assert str(screen.query_one("#detail", Static).render()) == header_only
        assert "(no items)" not in header_only


async def test_list_overview_skips_nested_folders_and_malformed_items_and_reports_empty(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    overview_node = _list_overview_node()
    nested_folder = Node(ref=NodeRef("repo", ("root", "list", "folder")), name="folder", is_leaf=False)
    malformed_item = _leaf("bad", "bad")
    malformed_unit = RestorableUnit(
        ref=malformed_item.ref,
        name=malformed_item.name,
        is_leaf=True,
        content=_ContentSource(b"not json"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(
        root,
        {str(root_ref): [overview_node], str(overview_node.ref): [nested_folder, malformed_item]},
        units_by_ref={str(malformed_item.ref): malformed_unit},
    )
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen._show_detail(overview_node)
        detail = screen.query_one("#detail", Static)
        await wait_until(pilot, lambda: "(no items)" in str(detail.render()))
        assert "(no items)" in str(detail.render())  # the folder was skipped, the malformed item discarded


# -- "nothing selected" guards -------------------------------------------


async def test_export_and_copy_ref_and_hex_preview_warn_when_nothing_is_selected(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: None  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen.action_export_selected()
        assert warnings == [UNIT_NOTHING_SELECTED_WARNING]

        warnings.clear()
        screen.action_copy_ref()
        assert warnings == [UNIT_COPY_REF_NOTHING_SELECTED_WARNING]

        warnings.clear()
        app.verbose = True
        await screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_hex_preview_outside_verbose_mode_is_a_no_op_warning(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen.action_hex_preview()
        assert warnings == ["press d to enable verbose mode first"]


async def test_copy_ref_with_a_real_selection_copies_and_notifies(wait_until: Any) -> None:
    # The "nothing selected" warning branches above are the only ones
    # this file covers for copy_ref/export_selected/hex_preview -- their
    # real success paths (something actually selected) were untested.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        copied: list[str] = []
        app.copy_to_clipboard = lambda text: copied.append(text)  # type: ignore[method-assign]
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        screen.action_copy_ref()
        assert copied == [str(leaf.ref)]
        assert warnings == ["copied ref to clipboard"]


async def test_export_selected_with_a_real_leaf_pushes_the_export_screen(wait_until: Any) -> None:
    from synology_apm_repo.browser.screens.export_screen import ExportScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    unit = RestorableUnit(
        ref=leaf.ref,
        name=leaf.name,
        is_leaf=True,
        content=_ContentSource(b"data"),  # type: ignore[arg-type]
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]}, units_by_ref={str(leaf.ref): unit})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        await screen.action_export_selected()
        await pilot.pause()
        assert isinstance(app.screen, ExportScreen)


async def test_export_selected_degrades_to_a_toast_instead_of_crashing_the_app(wait_until: Any) -> None:
    # provider.unit() raising here (a leaf with no fake unit registered,
    # via _ConfigurableProvider.unit()'s own NotImplementedError -- a
    # deliberately non-ApmRepoError exception, exercising the broad
    # ``except Exception``) must degrade to a toast, not crash the whole
    # Textual app the way an unguarded await once did.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})  # no units_by_ref entry for `leaf`
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen.action_export_selected()
        await pilot.pause()

        assert isinstance(app.screen, UnitScreen)  # still here -- no crash, no screen pushed
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_degrades_to_a_toast_instead_of_crashing_the_app(wait_until: Any) -> None:
    # Same reasoning as the export_selected test above.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: leaf  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen.action_hex_preview()
        await pilot.pause()

        assert isinstance(app.screen, UnitScreen)
        assert warnings and "no fake unit registered" in warnings[0]


async def test_hex_preview_selected_but_not_a_leaf_warns(wait_until: Any) -> None:
    # Distinct from "nothing selected" (None) -- a real, non-leaf node
    # (a folder) is selected instead.
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    folder = Node(ref=NodeRef("repo", ("root", "folder")), name="folder", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): [folder]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    app.verbose = True
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen._selected_node = lambda: folder  # type: ignore[method-assign]

        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        await screen.action_hex_preview()
        assert warnings == [UNIT_HEX_NOTHING_SELECTED_WARNING]


async def test_action_refresh_reloads_the_tree_from_the_root(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        provider_calls_before = app.repo.catalog.provider_calls

        screen.action_refresh()
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        assert len(tree.root.children) == 1
        assert app.repo.catalog.provider_calls > provider_calls_before  # re-fetched, not just re-rendered
        assert app.repo.invalidate_directory_cache_calls == 1  # a real re-scan, not stale cached listings


async def test_action_show_diagnostics_pushes_the_diagnostics_screen(wait_until: Any) -> None:
    from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_show_diagnostics()
        await pilot.pause()
        assert isinstance(app.screen, DiagnosticsScreen)


# -- go back closes an open goto box first --------------------------------


async def test_go_back_closes_an_open_goto_box_before_popping(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        screen.action_goto_ref()
        await pilot.pause()
        assert screen.query_one("#goto-input", Input).has_class("active")

        screen.action_go_back()
        await pilot.pause()
        assert not screen.query_one("#goto-input", Input).has_class("active")
        assert isinstance(app.screen, UnitScreen)  # the box closed; the screen itself is still open

        # Nothing open this time: Esc pops the screen itself.
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


# -- refresh_for_verbose_mode --------------------------------------------


async def test_refresh_for_verbose_mode_reloads_for_a_saas_version(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    repo = _FakeRepo(provider)
    app = _FakeApp(_version(target_type="M365"), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        assert repo.catalog.provider_calls == 1  # the initial _load_root on mount

        screen.refresh_for_verbose_mode()
        await pilot.pause()
        # A real reload happened -- catalog.provider() was dispatched a
        # second time -- not just "provider is still non-None" (the fake
        # always returns the same object either way).
        assert repo.catalog.provider_calls == 2


async def test_refresh_for_verbose_mode_is_a_no_op_for_a_non_saas_version(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(target_type="VM"), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        provider_before = screen._provider

        screen.refresh_for_verbose_mode()
        await pilot.pause()
        assert screen._provider is provider_before  # untouched — never reset/reloaded


# -- load more / filter ---------------------------------------------------


async def test_load_more_is_a_no_op_without_a_provider_or_tree_data(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        from synology_apm_repo.browser.screens.unit_screen import _LoadedChildren

        screen._reset_tree()
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
        screen._load_more(tree.root, _LoadedChildren(children=[], next_offset=0, exhausted=False))
        await pilot.pause()
        # No provider -> _load_more returns immediately: no children
        # appended, no notification fired.
        assert list(tree.root.children) == []
        assert warnings == []


async def test_action_load_more_warnings_for_unloaded_and_already_complete_levels(wait_until: Any) -> None:
    """Drives ``_children_by_node_id`` directly rather than through the
    tree's own real expand lifecycle: root (non-leaf, no ``target_ref``)
    auto-expands and auto-loads the instant it mounts (see
    ``_populate_root``'s own docstring), so by the time any test code
    runs, the level the cursor starts on is already loaded — the only
    reliable way to force each of ``action_load_more``'s two guard states
    is to set this bookkeeping dict to exactly what each state needs."""
    from synology_apm_repo.browser.screens.unit_screen import _LoadedChildren

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        # Nothing loaded under the cursor's current level at all.
        screen._children_by_node_id.clear()
        screen.action_load_more()
        assert warnings == [UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING]

        # That same level, now loaded but already exhausted.
        listing_node = screen._current_listing_node()
        screen._children_by_node_id[id(listing_node)] = _LoadedChildren(children=[], next_offset=0, exhausted=True)
        warnings.clear()
        screen.action_load_more()
        assert warnings == [UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING]


async def test_load_more_error_notifies_and_load_more_with_active_filter_rerenders(wait_until: Any) -> None:
    from synology_apm_repo.browser.screens.unit_screen import _LoadedChildren

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        # A children() failure during load-more must notify, not crash.
        provider._raise_children_for = {str(root_ref)}
        loaded = _LoadedChildren(children=[], next_offset=0, exhausted=False)
        screen._children_by_node_id[id(tree.root)] = loaded
        screen._load_more(tree.root, loaded)
        await wait_until(pilot, lambda: bool(warnings))
        assert f"boom at {root_ref}" in warnings[0]

        # A subsequent successful load-more, with an active filter on the
        # same level, must re-render through the filtered path rather than
        # appending unconditionally.
        provider._raise_children_for = set()
        more_items = [_leaf(f"item-{i}", f"item-{i}") for i in range(3)]
        provider._children_by_ref[str(root_ref)] = more_items
        screen._filter_parent = tree.root
        screen._filter_text = "item-1"
        warnings.clear()
        screen._load_more(tree.root, loaded)
        await wait_until(pilot, lambda: bool(warnings))
        assert [str(c.label) for c in tree.root.children] == ["item-1"]


async def test_action_filter_is_a_no_op_for_an_unloaded_level(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        # Root auto-expands/auto-loads the instant it mounts (see
        # test_action_load_more_warnings_...'s own comment) — force the
        # "nothing loaded here" state directly rather than relying on a
        # narrow pre-expand race window.
        screen._children_by_node_id.clear()
        screen.action_filter()
        await pilot.pause()
        assert not screen.query_one("#filter-input", Input).has_class("active")


async def test_enter_on_the_filter_input_closes_it(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    leaf = _leaf("item.bin", "item")
    provider = _ConfigurableProvider(root, {str(root_ref): [leaf]})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        screen.action_filter()
        await pilot.pause()
        assert screen.query_one("#filter-input", Input).has_class("active")

        await pilot.press("enter")
        await pilot.pause()
        assert not screen.query_one("#filter-input", Input).has_class("active")


# -- goto ref (``g``) ---------------------------------------------------


async def test_submit_goto_version_lookup_failure_notifies(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    repo = _FakeRepo(provider, version_for_ref_error=ApmRepoError("unknown version"))
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        await screen._submit_goto(str(other_ref))
        assert warnings == ["unknown version"]


async def test_submit_goto_a_different_version_pushes_a_new_unit_screen(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    other_version = dataclasses.replace(
        _version(), version_uid=VersionUid("some-other-version"), version_id=VersionId(2)
    )
    repo = _FakeRepo(provider, version_for_ref_result=other_version)
    app = _FakeApp(_version(), repo)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)

        other_ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("catalog-1"),
            workload_id=WorkloadId(1),
            version_uid=VersionUid("some-other-version"),
        )
        await screen._submit_goto(str(other_ref))
        await pilot.pause()
        assert app.screen is not screen
        assert isinstance(app.screen, UnitScreen)


async def test_walk_to_target_is_a_no_op_with_no_provider(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        was_expanded = tree.root.is_expanded
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
        screen._walk_to_target(None, root_ref)
        await pilot.pause()
        # No provider -> _walk_to_target returns immediately: no
        # notification fired, no expansion-state change triggered by it.
        assert warnings == []
        assert tree.root.is_expanded == was_expanded


async def test_goto_target_not_found_notifies_and_expands_root(
    monkeypatch: pytest.MonkeyPatch, wait_until: Any
) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    missing_ref = NodeRef("repo", ("root", "missing"))
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    # Patched on the class, before the app even runs: with target_ref set,
    # on_mount's _load_root() dispatches straight into _walk_to_target as
    # a background worker with nothing real to await, so it can finish
    # inside run_test()'s own startup pump -- patching screen.notify only
    # after entering the pilot context can already be too late to observe
    # the call.
    warnings: list[str] = []
    monkeypatch.setattr(UnitScreen, "notify", lambda self, message, **kwargs: warnings.append(message))
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=missing_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: bool(warnings) and tree.root.is_expanded)
        assert warnings == [GOTO_REF_NOT_FOUND_WARNING]
        assert tree.root.is_expanded  # _ensure_root_expanded() actually expanded it


async def test_goto_children_error_notifies_and_expands_root(monkeypatch: pytest.MonkeyPatch, wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    target_ref = NodeRef("repo", ("root", "target"))
    provider = _ConfigurableProvider(root, {}, raise_children_for={str(root_ref)})
    # Patched on the class before the app runs -- see the sibling
    # not-found test's own comment for why.
    warnings: list[str] = []
    monkeypatch.setattr(UnitScreen, "notify", lambda self, message, **kwargs: warnings.append(message))
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=target_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: bool(warnings) and tree.root.is_expanded)
        assert warnings == [f"boom at {root_ref}"]
        assert tree.root.is_expanded


async def test_goto_not_found_warning_text(wait_until: Any) -> None:
    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    provider = _ConfigurableProvider(root, {str(root_ref): []})
    app = _FakeApp(_version(), _FakeRepo(provider))
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: tree.root.data is not None)
        screen = app.screen
        assert isinstance(screen, UnitScreen)
        warnings: list[str] = []
        screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
        missing_ref = NodeRef("repo", ("root", "missing"))
        screen._walk_to_target(screen._provider, missing_ref)
        await wait_until(pilot, lambda: bool(warnings))
        assert warnings == [GOTO_REF_NOT_FOUND_WARNING]


async def test_goto_a_disk_fs_sibling_finds_it_nested_under_its_image_node(wait_until: Any) -> None:
    """``GotoChainWalker._find_chain_child``'s own fallback: the goto
    target is a provider-level *sibling* of its disk-image node
    (``chain``, from ``find_path_with_children``, is provider-shaped and
    doesn't know about widget nesting at all), but ``UnitScreen.add_child_nodes``
    already nested its ``TreeNode`` one level under the image node in the
    actual tree widget (see ``test_browser_unit_screen_disk_fs_nesting.py``'s
    own module docstring for the real shape this mirrors) — so a direct
    children-of-root search for it must fail and fall back to checking
    one level deeper before giving up."""
    from synology_apm_repo.sdk.units.device_disk_fs import DISK_FS_SIBLING_REF_ATTR

    root_ref = NodeRef("repo", ("root",))
    root = Node(ref=root_ref, name="root", is_leaf=False)
    image_ref = NodeRef("repo", ("object", "5"))
    image_node = Node(ref=image_ref, name="disk-1.img", is_leaf=True)
    fs_ref = image_ref.child("fs")
    fs_node = Node(
        ref=fs_ref, name="disk-1.img (filesystem)", is_leaf=False, attrs={DISK_FS_SIBLING_REF_ATTR: image_ref}
    )
    provider = _ConfigurableProvider(root, {str(root_ref): [image_node, fs_node]})
    app = _FakeApp(_version(), _FakeRepo(provider), target_ref=fs_ref)
    async with app.run_test() as pilot:
        tree = app.screen.query_one("#unit-tree", Tree)
        await wait_until(pilot, lambda: len(tree.root.children) > 0)

        assert len(tree.root.children) == 1  # only the image node is a direct child
        image_tree_node = tree.root.children[0]
        await wait_until(pilot, lambda: image_tree_node.is_expanded and len(image_tree_node.children) > 0)
        fs_tree_node = image_tree_node.children[0]
        assert fs_tree_node.data is not None
        assert fs_tree_node.data.ref == fs_ref
        assert tree.cursor_node is fs_tree_node  # goto landed the cursor on it


__all__: list[str] = []
