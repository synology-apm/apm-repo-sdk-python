"""``UnitScreen``: the selected version's own item browser -- a
File-Browser-style split between a folder ``Tree`` (left, containers
only) and a ``DataTable`` (right) listing the *selected* folder's own
children. Internal identifiers (``stream_id``, canonical ``NodeRef``,
...) only show in the detail pane in verbose mode. Every leaf also gets
an inline, best-effort content preview in that pane (see
``_load_preview``); a SharePoint List *group* node is a special case
handled outside the file table (see ``_load_list_overview``); ``d``
forces the raw index-entry provider for SaaS versions (see
``refresh_for_verbose_mode``).

Navigational state lives in a real MVU ``Store`` (``core/unit/model.py``'s
``UnitModel``, ``core/unit/update.py``'s ``update()``, ``select.py``'s
``folder_tree_spec``/``file_table_rows``, ``runtime/unit_effects.py``'s
``UnitEffects``). This screen only dispatches ``Msg``s and keeps a few
view-local pieces the ``Store`` doesn't need: ``_pending_target_ref`` (a
one-shot constructor argument), and the filter/detail-pane/goto-walk/
folder-tree/file-table collaborators."""

from __future__ import annotations

import asyncio
import dataclasses
import json

from textual.app import ComposeResult
from textual.binding import Binding as KeyBinding
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import DataTable, Footer, Input, Static, Tree
from textual.worker import Worker, get_current_worker

from synology_apm_repo.browser.content_preview import visible_site_fields
from synology_apm_repo.browser.core.unit.cmd import CloseProvider, UnitCmd
from synology_apm_repo.browser.core.unit.model import UnitModel, has_pending_children
from synology_apm_repo.browser.core.unit.msg import (
    ChildrenRequested,
    FilterClosed,
    FilterOpened,
    FilterTextChanged,
    FolderSelected,
    LoadMoreRequested,
    RootRequested,
    UnitMsg,
)
from synology_apm_repo.browser.core.unit.select import (
    column_headers_for,
    column_widths_for,
    file_table_rows,
    find_node_in_model,
    folder_tree_spec,
    prefers_recent_content,
    preview_renderer_for,
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
)
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.runtime.unit_effects import UnitEffects
from synology_apm_repo.browser.screens._shared import (
    FilterFieldController,
    NavigableScreen,
    notify_warning,
    parse_canonical_ref,
    resolve_and_open_goto_target,
    show_error,
)
from synology_apm_repo.browser.screens.detail_pane import DetailPane, DetailPaneLoadingSink
from synology_apm_repo.browser.screens.goto_walker import GotoChainWalker
from synology_apm_repo.browser.screens.unit_file_table import FileTable, FileTableView
from synology_apm_repo.browser.screens.unit_folder_tree import FolderTreeView
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    GOTO_REF_PLACEHOLDER,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_COPY_REF_NOTIFY,
    UNIT_FILTER_PARTIAL_LOAD_WARNING,
    UNIT_FILTER_PLACEHOLDER,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_NOTHING_SELECTED_WARNING,
)
from synology_apm_repo.browser.view.reconcile import Binding, find_node, move_cursor_keyed
from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.browser.worker_drain import drain
from synology_apm_repo.sdk.api import Catalog, Version
from synology_apm_repo.sdk.concurrency import bounded_gather
from synology_apm_repo.sdk.errors import ApmRepoError, ContentUnavailableError
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.resolve import find_path_with_children
from synology_apm_repo.sdk.units.saas.site import is_flat_category, is_list_overview

#: Caps a preview read regardless of the unit's own declared size. Never a
#: *silent* cap: ``content_preview``'s own truncation note is always shown
#: when a preview is actually cut short.
_PREVIEW_READ_LIMIT = 256 * 1024

#: How many items a SharePoint List's spreadsheet-style overview
#: (``_load_list_overview``) fetches -- smaller than
#: ``core.unit.update.CHILDREN_PAGE_SIZE`` since building the overview
#: reads each item's own content, not just its listing ``Node``. Never a
#: *silent* cap — the rendered table says so when this limit was hit.
_LIST_OVERVIEW_ITEM_CAP = 50

#: How many of those items' own content fetches run concurrently, via
#: ``concurrency.bounded_gather()`` -- deliberately the same bound as
#: ``verify_reachable.py``'s ``_MAX_CONCURRENT_BUCKET_CHECKS``.
_LIST_OVERVIEW_MAX_CONCURRENT = 8


#: This screen's footer shows ``escape`` (Esc always pops back to
#: ``BrowseScreen`` here), built via ``dataclasses.replace()`` on the
#: shared ``NAV_BINDINGS`` entry other screens use unmodified.
_BACK_BINDING = next(b for b in NAV_BINDINGS if isinstance(b, KeyBinding) and b.key == "escape")

#: Quit/verbose-mode stay functional but drop out of this screen's footer.
#: Extracted from ``COMMON_BINDINGS`` rather than redeclared, so a future
#: change to either binding can't drift out of sync between the two copies.
_QUIT_BINDING = next(b for b in COMMON_BINDINGS if isinstance(b, KeyBinding) and b.key == "q")
_VERBOSE_BINDING = next(b for b in COMMON_BINDINGS if isinstance(b, KeyBinding) and b.key == "d")


class UnitScreen(NavigableScreen):
    BINDINGS = [
        *[b for b in COMMON_BINDINGS if not (isinstance(b, KeyBinding) and b.key in ("q", "d"))],
        dataclasses.replace(_QUIT_BINDING, show=False),
        dataclasses.replace(_VERBOSE_BINDING, show=False),
        *[b for b in NAV_BINDINGS if not (isinstance(b, KeyBinding) and b.key == "escape")],
        dataclasses.replace(_BACK_BINDING, show=True),
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
        # Once the root/provider load, walk straight to this node and
        # expand its ancestors (a ``g`` jump landing here) -- see
        # _on_root_changed(). A one-shot constructor argument, consumed
        # exactly once.
        self._pending_target_ref = target_ref
        self._filter = FilterFieldController(
            self,
            lambda text: self.store.dispatch(FilterTextChanged(text=text)),
            lambda: self.store.dispatch(FilterClosed()),
        )
        self._detail_pane = DetailPane(self)
        self._goto_walker = GotoChainWalker(self)
        self._folder_tree = FolderTreeView(self)
        self._file_table = FileTableView(self)
        # self.store/self.effects are constructed in on_mount(), not here:
        # Textual gives a screen no App to reach (self.app/self.app_state)
        # until it actually mounts.

    @property
    def unit_tree(self) -> Tree[Binding[NodeRef]]:
        # Named ``unit_tree``, not ``tree`` -- ``DOMNode.tree`` already exists
        # (Textual's own debug rich.tree.Tree representation).
        return self.query_one("#folder-tree", Tree)

    @property
    def file_table(self) -> FileTable:
        return self.query_one("#file-table", FileTable)

    def compose(self) -> ComposeResult:
        # display_name is escaped: Tree.process_label re-parses a plain
        # str as Rich markup too, like Static.update()'s default
        # formatter. Breadcrumb starts empty; on_mount fills it below.
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

    def on_mount(self) -> None:
        super().on_mount()
        self._update_breadcrumb_text(safe(self._version.display_name))
        self.store: Store[UnitModel, UnitMsg, UnitCmd] = Store(UnitModel(), update, self._perform)
        self.effects = UnitEffects(
            self,
            self.app_state.resources,
            self.store,
            self._catalog,
            self._version,
            repo=lambda: self.app_state.current_repo,
            file_table=lambda: self.file_table,
            unit_tree=lambda: self.unit_tree,
        )
        self.file_table.cursor_type = "row"
        self.store.subscribe(lambda model: model.root, self._on_root_changed, init=False)
        self.store.subscribe(lambda model: model.root_error, self._on_root_error, init=False)
        self.store.subscribe(folder_tree_spec, self._folder_tree.render, init=True)
        # Columns, then widths, then rows -- Store._notify() calls
        # subscribers in registration order, so a folder switch rebuilds
        # the table's columns and sizes them before rows fill them in.
        self.store.subscribe(
            lambda model: column_headers_for(model, model.selected), self._file_table.configure_columns, init=True
        )
        self.store.subscribe(
            lambda model: column_widths_for(model, model.selected),
            self._file_table.configure_column_widths,
            init=True,
        )
        # Subscribing on this derived value (scoped to model.selected)
        # rather than the raw model.loaded/model.errors dicts means a
        # sibling folder's own background load/error doesn't force a
        # redundant rebuild here.
        self.store.subscribe(lambda model: file_table_rows(model, model.selected), self._file_table.render, init=True)
        # init=False: the initial dispatch below already reflects the
        # current verbose state; an init=True watch would additionally
        # re-run refresh_for_verbose_mode() immediately, reloading a SaaS
        # tree a second time on every mount.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        self.store.dispatch(RootRequested(invalidate=False, force_raw=self.app_state.verbose))

    def _perform(self, cmd: UnitCmd) -> None:
        self.effects.perform(cmd)

    async def on_unmount(self) -> None:
        """Closes this screen's provider once it's popped/replaced
        (otherwise its ``aiosqlite`` connection's background thread
        stays open for the session): ``store.close()`` first, then
        drains this screen's workers (a cancelled worker must reach its
        next ``await`` before it stops using the provider) before
        dispatching the close via ``UnitEffects``, hosted on the *App*
        so unmounting the screen can't cancel it mid-close —
        ``ApmRepoBrowserApp.on_unmount`` waits for it."""
        self.store.close()
        await drain(self._screen_workers(cancel=True))
        provider = self.store.model.provider
        if provider is not None:
            self.effects.perform(CloseProvider(provider=provider))

    def _set_loading_indicator(self, markup: str | None) -> None:
        text = safe(self._version.display_name)
        if markup is not None:
            text = f"{text}  {markup}"
        self._update_breadcrumb_text(text)

    def _current_provider(self) -> UnitProvider | None:
        handle = self.store.model.provider
        return self.app_state.resources.provider(handle) if handle is not None else None

    def _on_root_error(self, message: str | None) -> None:
        if message is not None:
            show_error(self, "#detail", message)

    # -- tree rendering (Store subscriptions) -----------------------------

    def _on_root_changed(self, root: Node | None) -> None:
        """Fires only when the root itself changes (a fresh load, a
        refresh, a verbose-mode reload) -- never on a descendant-only
        change, which ``FolderTreeView.render`` already handles.
        Focus/auto-expand/goto-walk run exactly once per real root
        change.

        ``root is None`` marks a reset already in flight; ``DetailPane.
        clear()`` runs here so a node from the just-closed provider
        generation can't resurface via ``_selected_node()``'s fallback."""
        if root is None:
            self._detail_pane.clear()
            return
        target_ref = self._pending_target_ref
        # Skip the normal auto-expand when a ``g`` jump is about to walk
        # this same root itself: auto-expanding here would populate the
        # root's own children independently of (and racing with)
        # _walk_to_target's own GotoChainWalker.expand_to_chain call for
        # the same level.
        self._folder_tree.focus_and_maybe_autoexpand(root, skip_autoexpand=target_ref is not None)
        if target_ref is not None:
            self._pending_target_ref = None
            provider = self._current_provider()
            # Not awaited: _walk_to_target is itself a @work method, so
            # calling it schedules a separate worker and returns
            # immediately.
            self._walk_to_target(provider, target_ref)

    # -- expand-to-load (lazy children) ------------------------------------

    def on_tree_node_expanded(self, event: Tree.NodeExpanded[Binding[NodeRef]]) -> None:
        node = self._folder_tree.node_of(event.node)
        if node is None:
            return
        if node.ref in self.store.model.loaded:
            # Already fetched -- a genuinely empty container must not
            # re-fetch on every collapse/re-expand.
            return
        if event.node.children:
            # Already has widget children with no model.loaded entry --
            # the synthetic error leaf a failed fetch shows -- nothing to
            # fetch.
            return
        if has_pending_children(self.store.model, node.ref):
            # Already fetching -- auto_expand can re-fire this handler
            # while a fetch is still in flight.
            return
        self.store.dispatch(ChildrenRequested(node=node))

    def on_tree_node_selected(self, event: Tree.NodeSelected[Binding[NodeRef]]) -> None:
        # Non-leaf nodes are deliberately *not* toggled here: Textual's own
        # Tree already expands/collapses on this same message via a
        # node-local handler that runs first -- toggling again here would
        # make every Enter press expand then immediately re-collapse.
        # Arrow-key movement doesn't reach here (Textual fires a separate
        # NodeHighlighted for that); only Enter/click does.
        node = self._folder_tree.node_of(event.node)
        if node is not None:
            self._navigate_to_folder(node)

    def _select_folder_ref(self, node: Node) -> None:
        """Just the ``FolderSelected`` dispatch, with no detail-pane write
        -- narrow enough for ``GotoChainWalker`` to call through
        ``self._screen`` too.

        A no-op for a SharePoint List-overview group: such a group never
        has real file-table contents, so selecting it would leave
        load-more/filter silently targeting a level that can't load."""
        if is_list_overview(node):
            return
        self.store.dispatch(FolderSelected(ref=node.ref))
        if (
            is_flat_category(node)
            and node.ref not in self.store.model.loaded
            and not has_pending_children(self.store.model, node.ref)
        ):
            # is_flat_category nodes never get a tree expand arrow, so
            # on_tree_node_expanded's dispatch never fires for them --
            # fetch explicitly here instead, guarded the same way.
            self.store.dispatch(ChildrenRequested(node=node))

    def _navigate_to_folder(self, node: Node) -> None:
        """Points the file table at ``node`` and shows its header in the
        detail pane. The file table is left showing whichever folder was
        selected before when ``node`` is a SharePoint List-overview group
        (``_select_folder_ref``'s no-op for one); only the detail pane
        reflects the group itself."""
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
        # Sync the folder tree's cursor onto this row too, so the two
        # panes never disagree about "where am I" after this.
        self._navigate_to_folder(node)
        tree_node = find_node(self.unit_tree.root, node.ref)
        if tree_node is not None:
            self._folder_tree.expand_ancestors(tree_node)
            # allow_expand is False for a SharePoint List-overview group;
            # TreeNode.expand() doesn't check that flag itself, so calling
            # it unconditionally would misroute a resulting NodeExpanded
            # into the ordinary paginated ChildrenRequested path instead
            # of _load_list_overview.
            if tree_node.allow_expand and not tree_node.is_expanded:
                tree_node.expand()
            move_cursor_keyed(self.unit_tree, tree_node)

    def _selected_node(self) -> Node | None:
        """Whichever of the folder tree or file table has focus, its
        current selection. Falls back to the detail pane's own tracked
        node when focus is on neither. ``None`` only when nothing has
        ever been selected, or the file table's cursor sits on the
        synthetic error row."""
        focused = self.focused
        if isinstance(focused, Tree):
            return self._folder_tree.node_of(focused.cursor_node)
        if isinstance(focused, DataTable):
            return self._file_table.node_at(focused.cursor_row)
        return self._detail_pane.node

    def action_show_detail(self) -> None:
        node = self._selected_node()
        if node is not None:
            self._show_detail(node)

    def _show_detail(self, node: Node) -> None:
        self._detail_pane.show(node)
        if self._current_provider() is None:
            return
        if node.is_leaf:
            self._detail_pane.set_wide(False)
            self._load_preview(node)
        elif is_list_overview(node):
            self._detail_pane.set_wide(True)
            self._load_list_overview(node)

    # -- inline content preview ------------------------------------------

    @work(sink=lambda self, node: DetailPaneLoadingSink(self._detail_pane, node))
    async def _load_preview(self, node: Node) -> None:
        """Best-effort: reads up to ``_PREVIEW_READ_LIMIT`` bytes and
        renders a preview appended below ``_show_detail``'s header.
        Never raises to the worker: ``ContentUnavailableError`` renders
        as an inline note and any other exception as an inline error,
        except ``asyncio.CancelledError`` (a ``BaseException``, not
        ``Exception``), which still propagates.

        A ``prefers_recent_content`` node (e.g. a chronological
        Teams/Chat transcript) gets a tail-anchored read once its
        content size exceeds the limit, so the newest messages show;
        rendering runs in ``asyncio.to_thread()`` since that renderer is
        a CPU-bound ``HTMLParser`` subclass."""
        provider = self._current_provider()
        if provider is None:
            return
        try:
            unit = await provider.unit(node)
            content = unit.open()
            offset = 0
            if prefers_recent_content(node):
                # LazyArtifact content stays size=None until built once; a
                # zero-length read forces that build so the size check
                # below sees the real value.
                await content.read(0, 0)
                if content.size is not None and content.size > _PREVIEW_READ_LIMIT:
                    offset = content.size - _PREVIEW_READ_LIMIT
            data = await content.read(offset, _PREVIEW_READ_LIMIT)
            preview = await asyncio.to_thread(preview_renderer_for(node), data)
        except ContentUnavailableError as exc:
            self._detail_pane.append_preview_note(node, exc)
            return
        except Exception as exc:
            self._detail_pane.append_preview_error(node, exc)
            return
        if preview:
            self._detail_pane.append_preview(node, preview)

    # -- SharePoint List overview (selecting a List group, not one item) --

    @work(sink=lambda self, node: DetailPaneLoadingSink(self._detail_pane, node))
    async def _load_list_overview(self, node: Node) -> None:
        """Spreadsheet-style preview of a SharePoint List's own items,
        read directly through ``provider.children()``/``provider.unit()``
        since a List's items are never tree-navigable. Only reachable for
        a ``site_list_overview``-flagged group node. Best-effort like
        ``_load_preview``: a failure past the initial ``children()`` call
        is caught per item, never for the whole batch."""
        provider = self._current_provider()
        if provider is None:
            return
        try:
            children = await provider.children(node, offset=0, limit=_LIST_OVERVIEW_ITEM_CAP)
        except ApmRepoError as exc:
            message = str(exc)
            self._detail_pane.append_list_overview_error(node, message)
            return
        # a nested folder isn't expected under a plain List; skipped
        # defensively rather than fetched.
        leaves = [child for child in children if child.is_leaf]
        rows_by_index: list[dict[str, object] | None] = [None] * len(leaves)

        async def _load_one(item: tuple[int, Node]) -> None:
            index, child = item
            try:
                unit = await provider.unit(child)
                data = await unit.open().read(0, _PREVIEW_READ_LIMIT)
                values = json.loads(data)
            except Exception:
                return  # one malformed item must never blank the whole overview
            if isinstance(values, dict):
                rows_by_index[index] = visible_site_fields(values)

        await bounded_gather(enumerate(leaves), _load_one, max_concurrent=_LIST_OVERVIEW_MAX_CONCURRENT)
        rows = [row for row in rows_by_index if row is not None]
        self._detail_pane.append_list_overview(node, rows, truncated=len(children) == _LIST_OVERVIEW_ITEM_CAP)

    def action_export_selected(self) -> None:
        node = self._selected_node()
        if node is None or not node.is_leaf or self._current_provider() is None:
            self.notify(UNIT_NOTHING_SELECTED_WARNING, severity="warning")
            return
        self._export_selected(node)

    # ``provider.unit()`` is real SDK I/O (a raw-object open, a SaaS
    # index lookup, ...), so this can't stay inline in the synchronous
    # action above without blocking the UI with no feedback past the
    # 300ms budget the README documents.
    @work
    async def _export_selected(self, node: Node) -> None:
        from synology_apm_repo.browser.screens.export_screen import ExportScreen

        provider = self._current_provider()
        if provider is None:
            return
        try:
            unit = await provider.unit(node)
        except Exception as exc:
            # Broader than ApmRepoError on purpose: a provider's unit()
            # can raise a third-party parser's own exception too.
            notify_warning(self, exc)
            return
        self.app.push_screen(ExportScreen(unit))

    def action_go_back(self) -> None:
        # Esc closes an open filter (``/``) or goto (``g``) box first, same
        # reasoning as BrowseScreen.action_go_back.
        if self.store.model.filter is not None:
            self._close_filter()
            return
        if self.query_one("#goto-input", Input).has_class("active"):
            self._close_goto()
            return
        self.app.pop_screen()

    # -- hex preview (``x``, verbose-mode only) -------------------------

    def action_hex_preview(self) -> None:
        if not self.app_state.verbose:
            self.notify("press d to enable verbose mode first", severity="warning")
            return
        node = self._selected_node()
        if node is None or not node.is_leaf or self._current_provider() is None:
            self.notify(UNIT_HEX_NOTHING_SELECTED_WARNING, severity="warning")
            return
        self._hex_preview(node)

    @work
    async def _hex_preview(self, node: Node) -> None:
        from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen

        provider = self._current_provider()
        if provider is None:
            return
        try:
            content = (await provider.unit(node)).open()
        except Exception as exc:
            notify_warning(self, exc)
            return
        self.app.push_screen(HexPreviewScreen(content, node.name))

    # -- refresh (``r``) -----------------------------------------------

    def action_refresh(self) -> None:
        """Re-queries from the root, discarding cached directory listings
        first (``DirCache`` is an unbounded, session-wide shared cache).
        Unlike ``BrowseScreen``'s three independent levels, there is one
        tree here, so "refresh" means the whole tree."""
        self._refresh(invalidate=True)

    def refresh_for_verbose_mode(self) -> None:
        """Watch callback on the app's ``verbose`` reactive (see
        ``on_mount``) -- re-loads the whole tree, switching between
        decoded content and ``RawObjectProvider``'s raw index entries."""
        # force_raw is SaaS-only: a Device/FS version's provider and tree
        # are identical either way, so only a real {M365, GW} version
        # actually reloads.
        if self._version.target_type not in ("M365", "GW"):
            return
        self._refresh(invalidate=False)

    # busy=False: the real fetch runs on UnitEffects.perform's own
    # already-wrapped worker -- wrapping this one too would double the
    # breadcrumb's progress animation.
    @work(busy=False)
    async def _refresh(self, *, invalidate: bool) -> None:
        """Drains this screen's in-flight workers before dispatching
        ``RootRequested``, so a still-running worker reading through the
        old provider can't race the close it triggers. Excludes this
        exact worker from its own cancel/drain sweep."""
        current = get_current_worker()
        stale = [w for w in self._screen_workers(cancel=False) if w is not current]
        for worker in stale:
            worker.cancel()
        await drain(stale)
        self.store.dispatch(RootRequested(invalidate=invalidate, force_raw=self.app_state.verbose))

    # -- load more (``+``) -----------------------------------------------

    def action_load_more(self) -> None:
        """``update()``'s ``LoadMoreRequested`` case handles both guard
        states. Resolved via ``model.selected``, never the folder tree's
        own cursor -- the file table can move ``model.selected`` one step
        ahead of the tree's cursor-sync."""
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

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter-input":
            self._close_filter()
        elif event.input.id == "goto-input":
            self._submit_goto(event.value)

    def _screen_workers(self, *, cancel: bool) -> list[Worker[None]]:
        """This screen's own workers — a provider-close worker is never
        among them, since it's hosted on the App, not this screen.
        ``cancel=True`` cancels the ones it returns, so a caller never
        has to remember to do both."""
        workers = [w for w in self.workers if w.node is self]
        if cancel:
            for worker in workers:
                worker.cancel()
        return workers

    def _close_filter(self) -> None:
        # empty filter text -> full list restored, applied immediately
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
    # action_goto_ref/_close_goto live on NavigableScreen — only this
    # screen's own _submit_goto stays here.

    def _submit_goto(self, text: str) -> None:
        self._close_goto()
        node_ref = parse_canonical_ref(text, notify=self.notify)
        if node_ref is None:
            return
        canonical_ids = node_ref.canonical_ids
        assert canonical_ids is not None  # parse_canonical_ref already checked node_ref.kind
        catalog_id, _workload_id, version_uid = canonical_ids
        if catalog_id == self._catalog.catalog_id and version_uid == self._version.version_uid:
            self._walk_to_target(self._current_provider(), node_ref)
            return
        self._goto_cross_version(node_ref)

    # busy=False: resolve_and_open_goto_target already wraps its own fetch
    # in DebouncedProgress -- wrapping again here would double it.
    @work(busy=False)
    async def _goto_cross_version(self, node_ref: NodeRef) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        await resolve_and_open_goto_target(self, repo, node_ref)

    @work
    async def _walk_to_target(self, provider: UnitProvider | None, target_ref: NodeRef) -> None:
        if provider is None:
            return
        try:
            found = await find_path_with_children(provider, target_ref)
        except ApmRepoError as exc:
            # find_path_with_children() has no catch of its own for a
            # non-leaf node's children() raising (e.g. a VM version whose
            # target.db never landed).
            notify_warning(self, exc)
            self._folder_tree.ensure_root_expanded()
            return
        if found is None:
            self.notify(GOTO_REF_NOT_FOUND_WARNING, severity="warning")
            # _on_root_changed() skipped its own auto-expand while this
            # walk was pending; since the walk failed, fall back here.
            self._folder_tree.ensure_root_expanded()
            return
        chain, children_by_step = found
        target_node = self._goto_walker.expand_to_chain(chain, children_by_step)
        if not target_node.is_leaf:
            # A folder -- expand_to_chain already selected it and parked
            # the folder tree's cursor there.
            self._show_detail(target_node)
            return
        # A leaf -- expand_to_chain parked the cursor on its parent folder
        # instead; locate_and_focus_row is a silent no-op if the target
        # isn't among the rendered rows (e.g. filtered out).
        self._file_table.locate_and_focus_row(target_node.ref)
        self._show_detail(target_node)
