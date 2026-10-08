"""``UnitScreen``: one version's item browser -- a folder ``Tree`` (containers
only) beside a ``DataTable`` of the selected folder's children, and a
detail pane with the selection's header and a best-effort content preview
(a SharePoint List group's overview table included). Verbose mode adds
internal identifiers to the pane and switches a SaaS version to the raw
index-entry provider (``refresh_for_verbose_mode``).

State lives in a ``Store`` (``core/unit/``, effects in
``runtime/unit_effects.py``), goto targets and units being opened included;
the screen keeps only the goto it seeds the store with and its view
collaborators (filter, detail pane, folder tree, file table), and applies
what the store decides: a goto's ``Landing``, an opened unit's screen."""

from __future__ import annotations

import dataclasses
from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import BindingType
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import DataTable, Footer, Input, Static, Tree
from textual.worker import get_current_worker

from synology_apm_repo.browser.core.app.model import FolderExport, jobs_occupy_slot
from synology_apm_repo.browser.core.unit.cmd import CloseProvider, UnitCmd
from synology_apm_repo.browser.core.unit.detail import detail_view
from synology_apm_repo.browser.core.unit.model import GotoState, Landing, UnitModel, UnitPurpose
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenRequested,
    DetailRequested,
    FilterClosed,
    FilterOpened,
    FilterTextChanged,
    FolderSelected,
    GotoRequested,
    LoadMoreRequested,
    RootRequested,
    UnitMsg,
    UnitOpenRequested,
    VerboseSet,
)
from synology_apm_repo.browser.core.unit.select import (
    column_headers_for,
    column_widths_for,
    file_table_rows,
    find_node_in_model,
    folder_tree_spec,
)
from synology_apm_repo.browser.core.unit.update import update
from synology_apm_repo.browser.keymap import (
    COMMON_BINDINGS,
    COPY_REF_BINDING,
    FILTER_BINDING,
    GOTO_REF_BINDING,
    NAV_BINDINGS,
    REFRESH_BINDING,
    UNIT_BINDINGS,
    VERIFY_BINDING,
    WORKLIST_BINDING,
    hidden,
    shown,
)
from synology_apm_repo.browser.runtime.unit_effects import UnitEffects
from synology_apm_repo.browser.screens._shared import (
    FilterFieldController,
    StoreScreen,
)
from synology_apm_repo.browser.screens.detail_pane import DetailLoadingSink, DetailPane
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen
from synology_apm_repo.browser.screens.unit_file_table import FileTable, FileTableView
from synology_apm_repo.browser.screens.unit_folder_tree import FolderTreeView
from synology_apm_repo.browser.strings import (
    GOTO_REF_PLACEHOLDER,
    REFRESH_EXPORT_BUSY_WARNING,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_COPY_REF_NOTIFY,
    UNIT_FILTER_PARTIAL_LOAD_WARNING,
    UNIT_FILTER_PLACEHOLDER,
    UNIT_HEX_NEEDS_VERBOSE_WARNING,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_NOTHING_SELECTED_WARNING,
)
from synology_apm_repo.browser.view.reconcile import Binding, find_node, move_cursor_keyed
from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.browser.worker_drain import drain
from synology_apm_repo.sdk import (
    Catalog,
    Node,
    NodeRef,
    RestorableUnit,
    Version,
)
from synology_apm_repo.sdk.presentation import safe


class UnitScreen(StoreScreen[UnitModel, UnitMsg, UnitCmd]):
    BINDINGS: ClassVar[list[BindingType]] = [
        # Quit/verbose stay bound (and in ?'s help) but leave the footer to Esc.
        *hidden(COMMON_BINDINGS, "q", "d"),
        *shown(NAV_BINDINGS, "escape"),
        *UNIT_BINDINGS,
        VERIFY_BINDING,
        dataclasses.replace(REFRESH_BINDING, show=False),
        dataclasses.replace(FILTER_BINDING, show=False),
        COPY_REF_BINDING,
        GOTO_REF_BINDING,
        dataclasses.replace(WORKLIST_BINDING, show=False),
    ]

    def __init__(self, catalog: Catalog, version: Version, *, target_ref: NodeRef | None = None) -> None:
        super().__init__()
        self._catalog = catalog
        self._version = version
        # Seeds the store's goto, resolved once the root loads.
        self._initial_goto = GotoState(target=target_ref) if target_ref is not None else None
        self._filter = FilterFieldController(
            self,
            lambda text: self.store.dispatch(FilterTextChanged(text=text)),
            lambda: self.store.dispatch(FilterClosed()),
        )
        self._detail_pane = DetailPane(self)
        self._folder_tree = FolderTreeView(self)
        self._file_table = FileTableView(self)
        # self.store/self.effects are built in on_mount(): self.app isn't
        # reachable before mount.

    @property
    def unit_tree(self) -> Tree[Binding[NodeRef]]:
        # Not ``tree``: ``DOMNode.tree`` already exists in Textual.
        return self.query_one("#folder-tree", Tree)

    @property
    def file_table(self) -> FileTable:
        return self.query_one("#file-table", FileTable)

    @override
    def compose(self) -> ComposeResult:
        # Escaped: Tree labels are parsed as markup. The breadcrumb is
        # filled in on_mount.
        display_name = safe(self._version.display_name)
        yield Static("", id="breadcrumb")
        with Horizontal(id="browser"):
            yield FastLabelTree(display_name, id="folder-tree")
            yield FileTable(id="file-table")
        with VerticalScroll(id="detail-scroll", can_maximize=True):
            yield Static("", id="detail")
        yield Input(placeholder=UNIT_FILTER_PLACEHOLDER, id="filter-input")
        yield Input(placeholder=GOTO_REF_PLACEHOLDER, id="goto-input")
        yield Footer(show_command_palette=False)

    @override
    def on_mount(self) -> None:
        super().on_mount()
        self._update_breadcrumb_text(safe(self._version.display_name))
        self._open_store(
            UnitModel(verbose=self.app_state.verbose, goto=self._initial_goto),
            update,
            lambda cmd: self.effects.perform(cmd),
        )
        self.effects = UnitEffects(
            self,
            self.app_state.resources,
            self.store,
            self._catalog,
            self._version,
            repo=lambda: self.app_state.current_repo,
            file_table=lambda: self.file_table,
            unit_tree=lambda: self.unit_tree,
            detail_sink=lambda is_current: DetailLoadingSink(self._detail_pane, is_current),
            show_unit=self._show_unit,
        )
        self.file_table.cursor_type = "row"
        self.store.subscribe(lambda model: model.root, self._on_root_changed, init=False)
        self.store.subscribe(detail_view, self._detail_pane.render, init=True)
        self.store.subscribe(folder_tree_spec, self._folder_tree.render, init=True)
        # Order matters (Store._notify() runs subscribers in registration
        # order): columns, then widths, then rows.
        self.store.subscribe(
            lambda model: column_headers_for(model, model.selected), self._file_table.configure_columns, init=True
        )
        self.store.subscribe(
            lambda model: column_widths_for(model, model.selected),
            self._file_table.configure_column_widths,
            init=True,
        )
        # Scoped to model.selected so a sibling folder's load doesn't
        # rebuild the table.
        self.store.subscribe(lambda model: file_table_rows(model, model.selected), self._file_table.render, init=True)
        # Last: a landing moves the cursor onto the tree nodes and file-table
        # rows the subscriptions above just rendered.
        self.store.subscribe(lambda model: model.landing, self._land, init=False)
        # init=False: the dispatch below already uses the current verbose
        # state; init=True would reload a SaaS tree twice on mount.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        self.store.dispatch(RootRequested(invalidate=False, force_raw=self.app_state.verbose))

    @override
    async def _after_store_closed(self) -> None:
        """Closes this screen's provider on pop, now that no worker of this
        screen can use it. The close runs via ``UnitEffects`` hosted on the
        App so unmounting can't cancel it (``ApmRepoBrowserApp.on_unmount``
        waits for it)."""
        provider = self.store.model.provider
        if provider is not None:
            self.effects.perform(CloseProvider(provider=provider))

    # -- tree rendering (Store subscriptions) -----------------------------

    def _on_root_changed(self, root: Node | None) -> None:
        """Fires only when the root itself changes (fresh load, refresh,
        verbose reload), not on descendant changes. ``root is None`` marks
        a reset in flight (which also clears the model's detail)."""
        if root is None:
            return
        # A goto being resolved expands the tree itself (its Landing).
        self._folder_tree.focus_and_maybe_autoexpand(root, skip_autoexpand=self.store.model.goto is not None)

    # -- expand-to-load (lazy children) ------------------------------------

    def on_tree_node_expanded(self, event: Tree.NodeExpanded[Binding[NodeRef]]) -> None:
        node = self._folder_tree.node_of(event.node)
        if node is None:
            return
        if event.node.children:
            # Loaded children, or the synthetic error leaf of a failed fetch
            # (which a re-expand must not retry).
            return
        self.store.dispatch(ChildrenRequested(node=node))

    def on_tree_node_selected(self, event: Tree.NodeSelected[Binding[NodeRef]]) -> None:
        # Don't toggle non-leaf nodes: Textual's Tree already does so on
        # this message, and toggling again would re-collapse them.
        node = self._folder_tree.node_of(event.node)
        if node is not None:
            self._navigate_to_folder(node)

    def _select_folder_ref(self, node: Node) -> None:
        """Dispatches ``FolderSelected`` without a detail-pane write."""
        self.store.dispatch(FolderSelected(folder=node))

    def _navigate_to_folder(self, node: Node) -> None:
        """Points the file table at ``node`` and shows its header in the
        detail pane. For a List-overview group the file table keeps its
        previous folder (see ``FolderSelected``)."""
        self._select_folder_ref(node)
        self._show_detail(node)

    # -- file table (Store subscription) -----------------------------

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "file-table":
            return
        node = self._file_table.node_at(event.cursor_row)
        if node is None:
            return  # out of range (defensive), or the synthetic error row -- nothing to do
        if node.is_leaf:
            self._show_detail(node)
            return
        # Sync the folder tree's cursor to this row.
        self._navigate_to_folder(node)
        tree_node = find_node(self.unit_tree.root, node.ref)
        if tree_node is not None:
            self._folder_tree.expand_ancestors(tree_node)
            # TreeNode.expand() ignores allow_expand (False for List-overview
            # groups), which would misroute into ChildrenRequested.
            if tree_node.allow_expand and not tree_node.is_expanded:
                tree_node.expand()
            move_cursor_keyed(self.unit_tree, tree_node)

    def _selected_node(self) -> Node | None:
        """The focused folder tree's or file table's selection, else the
        detail pane's node. ``None`` if nothing was ever selected or the
        cursor is on the synthetic error row."""
        focused = self.focused
        if isinstance(focused, Tree):
            return self._folder_tree.node_of(focused.cursor_node)
        if isinstance(focused, DataTable):
            return self._file_table.node_at(focused.cursor_row)
        detail = self.store.model.detail
        return detail.node if detail is not None else None

    def action_show_detail(self) -> None:
        node = self._selected_node()
        if node is not None:
            self._show_detail(node)

    def _show_detail(self, node: Node) -> None:
        self.store.dispatch(DetailRequested(node=node))

    def action_export_selected(self) -> None:
        """Exports the selected file, or everything below the selected folder."""
        node = self._selected_node()
        repo = self.app_state.current_repo
        if node is None or self.store.model.provider is None:
            self.notify(UNIT_NOTHING_SELECTED_WARNING, severity="warning")
            return
        if node.is_leaf:
            self.store.dispatch(UnitOpenRequested(node=node, purpose=UnitPurpose.EXPORT))
        elif repo is not None:
            folder = FolderExport(repo, self._catalog, self._version, self.app_state.verbose, node, node.name)
            self.app.push_screen(ExportScreen(folder))
        else:
            self.notify(UNIT_NOTHING_SELECTED_WARNING, severity="warning")

    def _show_unit(self, unit: RestorableUnit, purpose: UnitPurpose) -> None:
        """``ShowUnit``: pushes the screen ``purpose`` names for ``unit``."""
        screen = ExportScreen(unit) if purpose is UnitPurpose.EXPORT else HexPreviewScreen(unit.content, unit.name)
        self.app.push_screen(screen)

    @override
    def _close_open_filter(self) -> bool:
        if self.store.model.filter is None:
            return False
        self._close_filter()
        return True

    # -- hex preview (``x``, verbose-mode only) -------------------------

    def action_hex_preview(self) -> None:
        if not self.app_state.verbose:
            self.notify(UNIT_HEX_NEEDS_VERBOSE_WARNING, severity="warning")
            return
        node = self._selected_node()
        if node is None or not node.is_leaf or self.store.model.provider is None:
            self.notify(UNIT_HEX_NOTHING_SELECTED_WARNING, severity="warning")
            return
        self.store.dispatch(UnitOpenRequested(node=node, purpose=UnitPurpose.HEX_PREVIEW))

    # -- refresh (``r``) -----------------------------------------------

    def action_refresh(self) -> None:
        """Re-queries the whole tree from the root after dropping the
        repository's caches (``Repository.invalidate_caches``). Refused while
        an export runs: it reads through the connections this replaces."""
        if jobs_occupy_slot(self.app_state.jobs):
            self.notify(REFRESH_EXPORT_BUSY_WARNING, severity="warning")
            return
        self._refresh(invalidate=True)

    def refresh_for_verbose_mode(self) -> None:
        """Watch callback on the app's ``verbose`` reactive: sets the model's
        flag, and for a SaaS version reloads the tree, switching between
        decoded content and ``RawObjectProvider``'s raw index entries."""
        self.store.dispatch(VerboseSet(verbose=self.app_state.verbose))
        if not self._version.is_saas:
            return
        self._refresh(invalidate=False)

    # busy=False: UnitEffects.perform's worker already shows progress.
    @work(busy=False)
    async def _refresh(self, *, invalidate: bool) -> None:
        """Cancels and drains this screen's other workers before dispatching
        ``RootRequested``, so none reads through the provider it closes."""
        current = get_current_worker()
        stale = [w for w in self._own_workers(cancel=False) if w is not current]
        for worker in stale:
            worker.cancel()
        await drain(stale)
        self.store.dispatch(RootRequested(invalidate=invalidate, force_raw=self.app_state.verbose))

    # -- load more (``+``) -----------------------------------------------

    def action_load_more(self) -> None:
        """Dispatches ``LoadMoreRequested`` for ``model.selected`` (not the
        tree cursor, which can lag it); ``update()`` handles the guards."""
        ref = self.store.model.selected
        node = find_node_in_model(self.store.model, ref) if ref is not None else None
        if node is None:
            return
        self.store.dispatch(LoadMoreRequested(node=node))

    # -- filter (``/``) -------------------------------------------------

    def action_filter(self) -> None:
        ref = self.store.model.selected
        if ref is None:
            return
        level = self.store.model.loaded.get(ref)
        if level is None:
            return  # nothing loaded under this level yet — nothing to filter
        if not level.exhausted:
            self.notify(UNIT_FILTER_PARTIAL_LOAD_WARNING.format(loaded=len(level.children)), severity="warning")
        self.store.dispatch(FilterOpened(ref=ref))
        self._filter.open()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "filter-input" or self.store.model.filter is None:
            return
        self._filter.on_text_changed(event.value)

    def _close_filter(self) -> None:
        # Cancels the pending debounce and drops the filter: the full list returns at once.
        self._filter.close()

    # -- copy ref (``y``) ------------------------------------------------

    def action_copy_ref(self) -> None:
        node = self._selected_node()
        if node is None:
            self.notify(UNIT_COPY_REF_NOTHING_SELECTED_WARNING, severity="warning")
            return
        self.app.copy_to_clipboard(str(node.ref))
        self.notify(UNIT_COPY_REF_NOTIFY)

    # -- goto ref (``g``) -------------------------------------------------
    # A ref in this same version is resolved by the store; any other opens
    # through NavigableScreen._goto_elsewhere.

    @override
    def _goto(self, node_ref: NodeRef) -> None:
        canonical_ids = node_ref.canonical_ids
        assert canonical_ids is not None  # parse_canonical_ref already checked node_ref.kind
        catalog_id, _workload_id, version_uid = canonical_ids
        if catalog_id == self._catalog.catalog_id and version_uid == self._version.version_uid:
            self.store.dispatch(GotoRequested(target=node_ref))
            return
        self._goto_elsewhere(node_ref)

    def _land(self, landing: Landing | None) -> None:
        """Applies a goto's ``Landing``: reveals and selects its folder's tree
        node, then focuses a leaf target's file-table row (a no-op if the row
        isn't rendered, e.g. filtered out)."""
        if landing is None:
            return
        tree = self.unit_tree
        tree_node = find_node(tree.root, landing.folder)
        if tree_node is None:
            return
        self._folder_tree.expand_ancestors(tree_node)
        if landing.expand and tree_node.allow_expand and not tree_node.is_expanded:
            tree_node.expand()
        move_cursor_keyed(tree, tree_node)
        tree.scroll_to_node(tree_node)
        if landing.leaf is not None:
            self._file_table.locate_and_focus_row(landing.leaf)
