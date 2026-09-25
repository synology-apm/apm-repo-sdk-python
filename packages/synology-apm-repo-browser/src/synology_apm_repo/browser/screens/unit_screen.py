"""``UnitScreen``: the selected version's own item browser -- a
File-Browser-style split between a lazily-expanded folder ``Tree`` (left,
containers only) and a ``DataTable`` (right) listing the *selected*
folder's own children -- both files and subfolders, one row per
restorable unit (disk / file / mail / contact / event / Drive item / raw
object) or subfolder. Internal identifiers (``stream_id``, canonical
``NodeRef``, ...) only ever show in the detail pane when verbose mode is
on. Every leaf additionally gets an inline, best-effort readable-content
preview in that same pane (see ``_load_preview``, dispatched via
``core/unit/select.py``'s ``preview_renderer_for`` to
``browser/content_preview/``) — a parsed Organizer/Title/Location/
Start Time/End Time/Recurrence block for a calendar event, a Full Name/
Email-and-more block for a contact, a chat-transcript rendering for a
Teams/Chat message page, not the raw ``.ics``/CSV/JSON/HTML bytes. For a
content-only kind, ``DetailPane``'s own generic header is dropped
entirely -- see ``select.is_content_only_preview``.

A SharePoint List *group* node is a special case, handled outside the
file table entirely — see ``_load_list_overview`` for why and how.

``d`` forces the raw index-entry provider for SaaS versions — see
``refresh_for_verbose_mode``.

The screen's own navigational state (provider lifecycle, per-node loaded
children, the active filter, which folder is currently selected) lives in
a real MVU ``Store`` -- ``core/unit/model.py``'s own ``UnitModel``,
``update()`` in ``core/unit/update.py``, ``select.py``'s
``folder_tree_spec``/``file_table_rows`` translating the model into,
respectively, ``view/reconcile.py``'s ``NodeSpec`` tree and this screen's
own file-table rows, and ``runtime/unit_effects.py``'s ``UnitEffects``
doing the actual provider/children I/O. This screen's own job is thin:
dispatch a ``Msg`` in response to user action, and keep a few small,
purely view-local pieces of state a ``Store`` doesn't need to know about
-- ``_pending_target_ref`` (a one-shot constructor argument, consumed
exactly once), and the filter debounce/detail-pane/goto-walk/folder-tree/
file-table collaborators (``FilterFieldController``/``DetailPane``/
``GotoChainWalker``/``FolderTreeView``/``FileTableView`` -- the latter two
own the folder ``Tree``'s and file ``DataTable``'s own rendering/lookup
mechanics, including the file table's own row -> ``Node`` index), none of
which have the id()/race hazards the store's own epoch/inflight machinery
targets."""

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

#: Preview reads are capped regardless of the unit's own declared size —
#: a bound on worst-case I/O/memory for a display convenience, not a
#: correctness requirement. Never a *silent* cap: ``content_preview``'s
#: own truncation note is always shown when a preview is actually cut
#: short.
_PREVIEW_READ_LIMIT = 256 * 1024

#: How many items a SharePoint List's spreadsheet-style overview
#: (``_load_list_overview``) fetches and reads — deliberately smaller than
#: ``core.unit.update.CHILDREN_PAGE_SIZE``: building the overview reads
#: every fetched item's own content (one ``provider.unit()`` + bounded
#: read each), not just its listing ``Node``, so this bounds real
#: I/O/memory, not just widget count. Never a *silent* cap — the
#: rendered table says so when this limit was hit.
_LIST_OVERVIEW_ITEM_CAP = 50

#: How many of those items' own content fetches run concurrently --
#: each is an independent round-trip with no ordering dependency on any
#: other, so sequential-one-at-a-time would sum every item's own latency
#: instead of bounding it by the slowest one. Dispatched via the SDK's
#: own ``concurrency.bounded_gather()`` -- the same shared bounded-fan-out
#: helper ``verify_reachable.py``'s ``_MAX_CONCURRENT_BUCKET_CHECKS`` sites
#: use, not just a matching bound by coincidence. A real remote API this
#: could hit still needs *some* ceiling, not unbounded fan-out for a page
#: of up to ``_LIST_OVERVIEW_ITEM_CAP`` items at once.
_LIST_OVERVIEW_MAX_CONCURRENT = 8


#: This screen's own footer additionally shows ``escape`` — unlike
#: ``BrowseScreen`` (the app's own root screen, where Esc means something
#: different — see its own ``action_go_back``), Esc here always pops back
#: to ``BrowseScreen``, so it's worth printing. Built via
#: ``dataclasses.replace()`` on the same shared ``NAV_BINDINGS`` entry
#: other screens use unmodified.
_BACK_BINDING = next(b for b in NAV_BINDINGS if isinstance(b, KeyBinding) and b.key == "escape")

#: Quit/verbose-mode stay fully functional but drop out of *this*
#: screen's own footer -- kept curated to the handful of actions someone
#: actively browsing a version's own content cares about (Back/Help/
#: Export/Detail), the same "hide, don't remove" treatment
#: ``BrowseScreen`` already gives its own ``d`` binding. Extracted from
#: ``COMMON_BINDINGS`` rather than redeclared, so a future change to
#: either binding's own key/action can't silently drift out of sync
#: between the two copies.
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
        # expand its ancestors (a ``g`` jump, landing here either from
        # BrowseScreen's own ``g`` or from this same screen's ``g`` jumping to
        # a different version) — see _on_root_changed(). A one-shot
        # constructor argument, consumed exactly once -- not Store state,
        # since nothing about it needs race-proofing against a concurrent
        # dispatch.
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
        # display_name is normally the backup's real timestamp formatted
        # for local time, but degrades to the raw version_uid when the
        # version's status can't be decrypted/parsed -- escaped
        # at both call sites below, since Tree.process_label re-parses a
        # plain str as Rich markup too, exactly like Static.update()/
        # DataTable's own default_cell_formatter. The breadcrumb
        # itself starts empty here, like BrowseScreen's own -- on_mount
        # below fills it via _update_breadcrumb_text, so
        # NavigableScreen's own tasks-hint cache/render machinery has a
        # single write path to go through rather than two.
        display_name = safe(self._version.display_name)
        yield Static("", id="breadcrumb")
        with Horizontal(id="browser"):
            yield FastLabelTree(display_name, id="folder-tree")
            yield FileTable(id="file-table")
        with VerticalScroll(id="detail-scroll"):
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
        # Columns first, widths second, rows third -- Store._notify()
        # calls every subscriber in registration order, so a folder
        # switch that changes all three (a different UnitKind of folder)
        # always rebuilds the table's own columns, then sizes them,
        # before the row render fills them in. column_headers_for/
        # column_widths_for/file_table_rows all resolve the very same
        # selected folder's ColumnSpec, so none of the three ever
        # disagree about the current shape.
        self.store.subscribe(
            lambda model: column_headers_for(model, model.selected), self._file_table.configure_columns, init=True
        )
        self.store.subscribe(
            lambda model: column_widths_for(model, model.selected),
            self._file_table.configure_column_widths,
            init=True,
        )
        # file_table_rows() is already scoped to model.selected -- each
        # FileRow's own Node carries its ref, so a switch to a different
        # folder can never compare equal to the previous one unless both
        # are genuinely empty (nothing to redraw either way). Subscribing
        # on this derived value directly, rather than the raw model.loaded/
        # model.errors dicts, means a sibling folder's own background
        # load/error -- which mutates those dicts without touching what
        # this table is actually showing -- doesn't force a redundant
        # table.clear()+rebuild here. Same reasoning behind BrowseScreen's
        # own version_rows(model) selector, one element of its own
        # version-column subscription -- scoping to a derived value
        # instead of the raw model.workload_versions there too.
        self.store.subscribe(lambda model: file_table_rows(model, model.selected), self._file_table.render, init=True)
        # init=False: the initial dispatch below already reflects the
        # current verbose state itself (force_raw=self.app_state.verbose)
        # -- an init=True watch would additionally re-run
        # refresh_for_verbose_mode() immediately, which for a SaaS
        # version reloads the whole tree a second, wasted time on every
        # mount. The watch only needs to fire on a genuine later toggle.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        self.store.dispatch(RootRequested(invalidate=False, force_raw=self.app_state.verbose))

    def _perform(self, cmd: UnitCmd) -> None:
        self.effects.perform(cmd)

    async def on_unmount(self) -> None:
        """Closes this screen's own provider once it's popped/replaced.

        Without this, simply browsing into a version and back leaves its
        provider's ``SqliteSource``/``aiosqlite`` connection(s) — and the
        real background thread each one owns — open for the rest of the
        session: ``Repository.close()``'s own end-of-session sweep --
        releasing every tracked provider's own sqlite connections, since
        ``aiosqlite`` dedicates a non-daemon background thread to each
        one's whole lifetime that would otherwise block interpreter exit
        forever -- is a safety net for a provider nobody got around to
        closing, not meant to be the only thing that ever does.
        ``async def`` is safe here — Textual's own callback-invocation
        machinery awaits a coroutine-returning handler like ``on_unmount``
        to completion rather than leaving it as an un-awaited coroutine.

        ``self.store.close()`` first, matching every other screen's own
        Store-backed ``on_unmount`` (``ExportScreen``, ``app.py``): a
        worker whose result lands after this point must not be able to
        mutate a store that's mid-teardown. This screen's own workers are
        then drained *before* the close is dispatched -- Textual's
        ``Widget._on_unmount`` only requests their cancellation, and a
        cancelled task still has to reach its next ``await`` before it
        stops using the provider. The close itself is performed directly
        (bypassing ``dispatch()``/``update()`` -- there's no new request
        to make here, only cleanup) via ``UnitEffects``, which hosts it
        on the *App*, not this screen: ``Widget._on_unmount`` cancels
        every worker on a node once that node unmounts regardless of its
        own group, so a close worker hosted on the screen could be
        cancelled out from under itself the instant the screen unmounts,
        mid-close -- exactly the leaked-connection failure mode closing a
        provider exists to prevent. ``ApmRepoBrowserApp.on_unmount``
        waits for it now."""
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
        """Fires only when the root itself actually changes (a fresh
        load, a refresh, a verbose-mode reload) -- never on a
        descendant-only change (a child's own children loading, a
        filter keystroke), which ``FolderTreeView.render`` already handles
        on its own. Focus/auto-expand/goto-walk-trigger all belong here,
        not there, specifically so they run exactly once per real root
        change instead of on every structural update anywhere in the
        tree.

        ``root is None`` marks a reset already in flight (``RootRequested``
        sets ``model.root``/``model.provider`` to ``None`` before its own
        fresh load even starts) -- ``DetailPane.clear()`` is called right
        here for that case so a node from the just-closed provider
        generation can never resurface via ``_selected_node()``'s own
        fallback to the pane once nothing has been selected again."""
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
            # Already fetched -- even a genuinely empty, successfully
            # loaded container (0 real children) must not re-fetch on
            # every collapse/re-expand. `event.node.children` alone can't
            # tell "empty but loaded" apart from "never requested" (both
            # are an empty widget child list), which is exactly the
            # regression this check exists to close.
            return
        if event.node.children:
            # Already has widget children with no model.loaded entry of
            # its own -- the synthetic error leaf a failed fetch already
            # shows under it (see error_leaf_ref) -- nothing to fetch.
            return
        if has_pending_children(self.store.model, node.ref):
            # Already fetching -- Textual's own auto_expand toggles this
            # node on every Enter press, so a slow fetch left Loading
            # when the user presses Enter again re-fires this same
            # handler; ChildrenLoaded's own epoch/request check only
            # arbitrates which result wins once fetches finish, it does
            # nothing to stop a second worker from being created here.
            return
        self.store.dispatch(ChildrenRequested(node=node))

    def on_tree_node_selected(self, event: Tree.NodeSelected[Binding[NodeRef]]) -> None:
        # Non-leaf nodes are deliberately *not* toggled here: Textual's
        # own Tree already has a ``@on(NodeSelected)`` handler
        # (``_expand_node_on_select``, gated on ``auto_expand`` — ``True`` by
        # default, never overridden in this codebase) that
        # expands/collapses the node in response to this exact same
        # message, and node-local handlers run before the message bubbles
        # up to a screen's own ``on_tree_node_selected``. Toggling again
        # here would make every ``l``/Enter press expand the node and then
        # immediately re-collapse it — level-2+ nodes could never stay
        # open via the keyboard (only ``space``/a mouse click on the ▶
        # triangle works, since ``action_toggle_node`` doesn't go through
        # ``NodeSelected`` at all).
        #
        # Arrow-key movement alone does not reach here at all (Textual
        # fires a separate NodeHighlighted for that) -- only Enter/click
        # does, matching BrowseScreen's own column-to-column behavior:
        # arrow keys move within the folder tree, Enter reveals the
        # folder's own contents in the file table.
        node = self._folder_tree.node_of(event.node)
        if node is not None:
            self._navigate_to_folder(node)

    def _select_folder_ref(self, node: Node) -> None:
        """Just the ``FolderSelected`` dispatch, with no detail-pane write
        -- narrow enough for ``GotoChainWalker`` to call through
        ``self._screen`` too (it only ever wants the file table pointed at
        a folder, never a synchronous detail write of its own).

        A no-op when ``node`` is a SharePoint List-overview group: such a
        group never has real file-table contents of its own
        (``core/unit/select.py``'s own ``file_table_rows`` always renders
        ``()`` for one), so selecting it would only make the load-more/
        filter keys silently target a level that can never be loaded.
        Enforced here, the one choke point both callers (an ordinary tree
        click, a goto-ref landing) funnel through, rather than at each
        call site -- a future third caller can't forget it."""
        if is_list_overview(node):
            return
        self.store.dispatch(FolderSelected(ref=node.ref))
        if (
            is_flat_category(node)
            and node.ref not in self.store.model.loaded
            and not has_pending_children(self.store.model, node.ref)
        ):
            # This node never gets a tree expand arrow (is_flat_category
            # -- see core/unit/select.py's own _is_container), so
            # on_tree_node_expanded's own ChildrenRequested dispatch
            # never fires for it. Fetch explicitly here instead -- the
            # only other place a folder's own contents are ever
            # requested -- guarded the same way that handler guards
            # itself, so reselecting an already-loaded (or still-loading)
            # category doesn't refetch.
            self.store.dispatch(ChildrenRequested(node=node))

    def _navigate_to_folder(self, node: Node) -> None:
        """Points the file table at ``node`` and shows its header in the
        detail pane -- what both the folder tree's own selection and the
        file table's own subfolder-row activation want. ``_show_detail``'s
        existing ``is_leaf``/``is_list_overview`` branches already no-op
        past the header write for a plain folder, so nothing further is
        needed here for that case. The file table is left showing
        whichever real folder was selected before when ``node`` is a
        SharePoint List-overview group (``_select_folder_ref``'s own
        no-op for one); only the detail pane (via ``_show_detail`` below)
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
        # A subfolder row (a SharePoint List-overview group included --
        # _show_detail's own is_list_overview branch fires _load_list_overview
        # for it exactly as a folder-tree selection would): sync the
        # folder tree's own cursor onto it too, so the two panes never
        # disagree about "where am I" after this.
        self._navigate_to_folder(node)
        tree_node = find_node(self.unit_tree.root, node.ref)
        if tree_node is not None:
            self._folder_tree.expand_ancestors(tree_node)
            # allow_expand is False for a SharePoint List-overview group
            # (folder_tree_spec's own is_container decision, reconciled
            # onto the widget) -- TreeNode.expand() doesn't check that
            # flag itself the way the keyboard/mouse path
            # (Tree._toggle_node) does, so calling it unconditionally
            # here would post a real NodeExpanded for a node
            # on_tree_node_expanded then can't tell apart from an
            # ordinary never-requested container, dispatching a
            # ChildrenRequested through the wrong (ordinary paginated)
            # path for a node whose contents are only ever read via
            # _load_list_overview.
            if tree_node.allow_expand and not tree_node.is_expanded:
                tree_node.expand()
            move_cursor_keyed(self.unit_tree, tree_node)

    def _selected_node(self) -> Node | None:
        """Whichever of the folder tree or the file table currently has
        focus, its own current selection's data. Falls back to whatever
        the detail pane is currently showing when focus is on neither
        (e.g. ``#detail-scroll`` itself, a focusable ``VerticalScroll`` a
        mouse click or Tab can land on) -- every navigation path
        (``_navigate_to_folder``, a leaf row's own selection) always
        calls ``DetailPane.show()`` in lockstep with the real selection,
        so its own tracked node is never stale relative to either widget.
        ``None`` only when nothing has ever been selected, or when the
        file table's cursor sits on the synthetic error row (no real
        ``Node`` behind it). Shared by every action here that acts on
        "whatever's currently selected"
        (``action_show_detail``/``action_export_selected``/
        ``action_hex_preview``/``action_copy_ref``)."""
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
        """Best-effort: reads up to ``_PREVIEW_READ_LIMIT`` bytes and tries
        to render a human-readable preview, appended below the header
        ``_show_detail`` already shows. Never raises out to the worker —
        a preview is a display convenience, not a correctness
        requirement, so one unit's malformed/unusual bytes must never
        crash the whole screen -- but the failure still reaches the
        pane rather than being silently swallowed, since a content-only
        node's own empty header (``is_content_only_preview``) would
        otherwise leave the pane completely blank with no indication
        anything was even selected. ``ContentUnavailableError`` (a
        cloud-sync placeholder, an EFS-encrypted file -- both expected,
        not a rendering bug) renders as an inline note
        (``append_preview_note``); any other exception renders as an
        inline error (``append_preview_error``). The broad ``except
        Exception`` still doesn't swallow cancellation:
        ``asyncio.CancelledError`` derives from ``BaseException``, not
        ``Exception``.

        For a node ``prefers_recent_content`` flags, a tail-anchored read
        is used directly instead of a head read once the content's own
        known ``size`` exceeds ``_PREVIEW_READ_LIMIT``: a Teams/Chat
        transcript is chronological (oldest message first), so the
        newest messages -- not its first page -- are what a user
        actually wants visible, the same reason a real chat client opens
        scrolled to the bottom.

        The render itself runs in ``asyncio.to_thread()`` -- a Teams/Chat
        transcript's own renderer is a real ``HTMLParser`` subclass over
        up to ``_PREVIEW_READ_LIMIT`` bytes, a genuinely CPU-bound parse
        that would otherwise block the whole TUI (this worker's real
        ``await``s above it are ordinary async I/O, still on the event
        loop) for its full duration."""
        provider = self._current_provider()
        if provider is None:
            return
        try:
            # ``provider.unit()``/``ContentSource.read()`` are both async;
            # ``RestorableUnit.open()`` between them is synchronous.
            unit = await provider.unit(node)
            content = unit.open()
            offset = 0
            if prefers_recent_content(node):
                # A Teams/Chat page's own ContentSource is a LazyArtifact:
                # size is a synchronous property but assembling the
                # artifact requires await, so (unlike a DedupFile-backed
                # source) it stays None until read/stream/export_to has
                # actually built it once -- a zero-length read forces the
                # one real build eagerly, with no bytes materialized to
                # this caller, so the size check below sees the real
                # value. The real read below is then just an in-memory
                # slice of the now-cached bytes, not a second real read.
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
        since a List's items are never tree-navigable at all (see
        ``core/unit/select.py``'s own ``_is_container``) — the only
        place they're ever fetched. Only reachable for a
        ``site_list_overview``-flagged group node (a plain List, never a
        document-library folder). Best-effort like ``_load_preview``: a
        failure past the initial ``children()`` call is caught per item,
        never for the whole batch. Fetches concurrently, bounded by
        ``_LIST_OVERVIEW_MAX_CONCURRENT``: a real remote API needs some
        ceiling, not unbounded fan-out for a page of up to
        ``_LIST_OVERVIEW_ITEM_CAP`` items at once."""
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
            # Broader than ApmRepoError on purpose, same reasoning as
            # UnitEffects._load_children's identical catch: a provider's
            # unit() can raise more than ApmRepoError (a third-party
            # parser's own exception, say) -- degrade to a toast either
            # way rather than crashing the whole app.
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
            # Same positioning as ``d`` itself: raw bytes are
            # exactly what ordinary mode hides, so this is a no-op
            # (not an error) outside verbose mode rather than a key
            # that silently does nothing with no explanation at all.
            self.notify("press d to enable verbose mode first", severity="warning")
            return
        node = self._selected_node()
        if node is None or not node.is_leaf or self._current_provider() is None:
            self.notify(UNIT_HEX_NOTHING_SELECTED_WARNING, severity="warning")
            return
        self._hex_preview(node)

    # Async ``@work`` — see ``_export_selected``'s identical reasoning;
    # ``provider.unit()`` is the same real SDK I/O either way.
    @work
    async def _hex_preview(self, node: Node) -> None:
        from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen

        provider = self._current_provider()
        if provider is None:
            return
        try:
            content = (await provider.unit(node)).open()
        except Exception as exc:
            # Same reasoning as action_export_selected's identical catch.
            notify_warning(self, exc)
            return
        self.app.push_screen(HexPreviewScreen(content, node.name))

    # -- refresh (``r``) -----------------------------------------------

    def action_refresh(self) -> None:
        """Re-queries from the root, discarding cached directory listings
        first: ``DirCache`` is an unbounded, session-wide shared cache, so
        without invalidating it, this would just re-serve whatever was
        listed earlier this session instead of actually re-scanning the
        store. Unlike ``BrowseScreen``'s three independent levels there
        is one tree here, so "refresh" means the whole tree, not just the
        cursor's current level. ``update()``'s own ``RootRequested`` case
        does all the actual reset work (bumping the epoch, clearing
        ``loaded``/``errors``/``filter``, closing the old provider) --
        this action just drains this screen's own in-flight workers first
        and dispatches."""
        self._refresh(invalidate=True)

    def refresh_for_verbose_mode(self) -> None:
        """Registered as a watch callback on the app's ``verbose`` reactive
        (see ``on_mount``) -- re-loads the whole tree from a freshly
        dispatched provider, switching between an application-layer
        provider's decoded content and ``RawObjectProvider``'s raw index
        entries for the same version. Unlike ``BrowseScreen``'s own
        version, which only re-renders already-fetched labels, this one
        reloads because a SaaS version's raw/decoded tree shapes genuinely
        differ, not just their labels."""
        # force_raw is SaaS-only: VM/PC/PS/FS dispatches straight to
        # DeviceProvider/FsProvider before force_raw is ever checked, so
        # a Device/FS version's provider and tree are byte-for-byte
        # identical either way -- reloading would only discard the
        # user's cursor position/expansion state for zero visible
        # change. Only a real {M365, GW} version actually reloads.
        if self._version.target_type not in ("M365", "GW"):
            return
        self._refresh(invalidate=False)

    # busy=False: this body only cancels/drains stale workers and
    # dispatches RootRequested -- the real fetch it triggers runs on a
    # separate, already-wrapped worker (UnitEffects.perform's LoadRoot
    # case), so wrapping this one too would risk two DebouncedProgress
    # instances animating the same breadcrumb at once.
    @work(busy=False)
    async def _refresh(self, *, invalidate: bool) -> None:
        """Drains this screen's own in-flight workers *before* dispatching
        ``RootRequested`` -- ``update()``'s own ``RootRequested`` case
        returns a ``CloseProvider`` cmd closing the current provider on a
        worker hosted on the App, not this screen, so a real screen
        unmount can't cancel it mid-close out from under itself, and a
        still-running ``LoadChildren`` worker reading through that exact
        same provider must not be left racing it, the same reasoning
        ``on_unmount``'s own drain-before-close already established there.
        Excludes this exact worker (``get_current_worker()``) from its own
        cancel/drain sweep -- ``@work`` already registered it among
        ``self.workers`` by the time this body starts running, so without
        the exclusion this would cancel itself. A second refresh fired
        before the first finishes draining correctly cancels that first,
        now-stale ``_refresh`` worker too -- newest reset wins."""
        current = get_current_worker()
        stale = [w for w in self._screen_workers(cancel=False) if w is not current]
        for worker in stale:
            worker.cancel()
        await drain(stale)
        self.store.dispatch(RootRequested(invalidate=invalidate, force_raw=self.app_state.verbose))

    # -- load more (``+``) -----------------------------------------------

    def action_load_more(self) -> None:
        """``update()``'s own ``LoadMoreRequested`` case handles both
        guard states ("nothing loaded here yet", "already exhausted") --
        this action is just the dispatch, same reasoning as
        ``action_refresh`` above. Resolved via ``model.selected`` +
        ``find_node_in_model`` (a pure model lookup), never the folder
        tree's own cursor -- the file table can move ``model.selected``
        one step ahead of the tree's own cursor-sync (see
        ``on_data_table_row_selected``), so depending on tree-cursor
        state here would be a real, if rare, correctness bug."""
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

    # Async ``@work`` — the cross-version branch above needs a real
    # ``resolve_goto_version`` fetch; the same-version fast path
    # (``_walk_to_target``, already its own ``@work``) never reaches here.
    # busy=False: delegates its entire body to resolve_and_open_goto_target,
    # which already wraps its own fetch in DebouncedProgress -- wrapping
    # again here would double the same breadcrumb's debounce/animation
    # state for no benefit.
    @work(busy=False)
    async def _goto_cross_version(self, node_ref: NodeRef) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        await resolve_and_open_goto_target(self, repo, node_ref)

    # Real fetch (find_path_with_children walks provider.children() down
    # to the target, expanding levels along the way) with no wrap of its
    # own until now -- @work's default busy=True closes that gap, using
    # the same breadcrumb sink as its sibling _goto_cross_version above
    # for a consistent "g" experience regardless of which path resolves it.
    @work
    async def _walk_to_target(self, provider: UnitProvider | None, target_ref: NodeRef) -> None:
        if provider is None:
            return
        try:
            found = await find_path_with_children(provider, target_ref)
        except ApmRepoError as exc:
            # find_path_with_children() calls provider.children() on every
            # non-leaf node along the way with no catch of its own (unlike
            # UnitEffects._load_children's identical call, which shows a
            # per-node error leaf instead of crashing) — a node whose
            # children() raises (e.g. a VM version whose target.db never
            # landed) would otherwise leave this worker's exception
            # uncaught. Same ApmRepoError -> notify + fall back posture as
            # the version_for_ref() catch just above.
            notify_warning(self, exc)
            self._folder_tree.ensure_root_expanded()
            return
        if found is None:
            self.notify(GOTO_REF_NOT_FOUND_WARNING, severity="warning")
            # _on_root_changed() deliberately skipped its own auto-expand
            # while this walk was pending, to avoid racing this same
            # GotoChainWalker.expand_to_chain call for the same level --
            # since the walk itself just failed, nothing else will ever
            # expand the root, so do the plain fallback here instead of
            # leaving the user stuck looking at a permanently collapsed
            # tree.
            self._folder_tree.ensure_root_expanded()
            return
        chain, children_by_step = found
        target_node = self._goto_walker.expand_to_chain(chain, children_by_step)
        if not target_node.is_leaf:
            # A folder (or SharePoint List-overview group) -- expand_to_chain
            # already selected it and parked the folder tree's own cursor
            # there.
            self._show_detail(target_node)
            return
        # A leaf -- expand_to_chain parked the tree cursor on (and
        # selected) its parent folder instead, so the target itself has
        # to be located in the now-populated file table (populated
        # synchronously by that same FolderSelected dispatch --
        # Store.dispatch fully drains before expand_to_chain returns).
        # locate_and_focus_row is a silent no-op when the target isn't
        # among the currently-rendered rows (filtered out by a stale
        # active filter, or otherwise not listed -- e.g. an item nested
        # under a SharePoint List group, never tree/table-navigable to
        # begin with) -- same fallback posture either way: leave the
        # cursor on the parent folder rather than leaving the user stuck
        # looking at nothing explained.
        self._file_table.locate_and_focus_row(target_node.ref)
        self._show_detail(target_node)
