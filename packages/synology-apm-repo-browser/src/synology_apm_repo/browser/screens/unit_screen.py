"""``UnitScreen``: the selected version's own item tree — a
lazily-expanded ``Tree``, one leaf per restorable unit (disk / file /
mail / contact / event / Drive item / raw object). Internal identifiers
(``stream_id``, canonical ``NodeRef``, ...) only ever show in the detail
pane when verbose mode is on. Mail/Calendar-event/Contact/
self-contained-HTML leaves additionally get an inline, best-effort
readable-content preview in that same pane (see
``_load_preview``/``browser/content_preview.py``) — a parsed
Organizer/Title/Location/Start Time/End Time/Recurrence block for a
calendar event, a Full Name/Email block for a contact, not the raw
``.ics``/CSV/JSON bytes.

A SharePoint List *group* node is a special case, handled outside this
tree entirely — see ``_load_list_overview`` for why and how.

``d`` forces the raw index-entry provider for SaaS versions — see
``refresh_for_verbose_mode``."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable

from textual import work
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.content_preview import (
    render_calendar_event_preview,
    render_contact_preview,
    render_html_preview,
    render_mail_preview,
    visible_site_fields,
)
from synology_apm_repo.browser.keymap import (
    COMMON_BINDINGS,
    COPY_REF_BINDING,
    FILTER_BINDING,
    GOTO_REF_BINDING,
    NAV_BINDINGS,
    REFRESH_BINDING,
    UNIT_BINDINGS,
    VERIFY_BINDING,
)
from synology_apm_repo.browser.screens._shared import (
    NavigableScreen,
    current_listing_tree_node,
    parse_canonical_ref,
    resolve_goto_version,
    show_error,
    show_filter_input,
)
from synology_apm_repo.browser.screens.detail_pane import DetailPane
from synology_apm_repo.browser.screens.goto_walker import GotoChainWalker
from synology_apm_repo.browser.strings import (
    GOTO_REF_NOT_FOUND_WARNING,
    GOTO_REF_PLACEHOLDER,
    UNIT_COPY_REF_NOTHING_SELECTED_WARNING,
    UNIT_COPY_REF_NOTIFY,
    UNIT_FILTER_PARTIAL_LOAD_WARNING,
    UNIT_FILTER_PLACEHOLDER,
    UNIT_HEX_NOTHING_SELECTED_WARNING,
    UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING,
    UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING,
    UNIT_LOAD_MORE_NOTIFY,
    UNIT_NOTHING_SELECTED_WARNING,
    UNIT_STATUS_BAR,
)
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress
from synology_apm_repo.sdk.api import Catalog, Version
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import ClosableUnitProvider, Node, UnitKind, UnitProvider
from synology_apm_repo.sdk.units.device_disk_fs import disk_fs_sibling_ref
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.resolve import find_path_with_children
from synology_apm_repo.sdk.units.saas.site import is_list_overview

#: Preview reads are capped regardless of the unit's own declared size —
#: a bound on worst-case I/O/memory for a display convenience, not a
#: correctness requirement. Never a *silent* cap: ``content_preview``'s
#: own truncation note is always shown when a preview is actually cut
#: short.
_PREVIEW_READ_LIMIT = 256 * 1024

#: ``_load_preview``'s dispatch — a leaf ``UnitKind`` not listed here
#: (self-contained HTML included) falls back to ``render_html_preview``.
_PREVIEW_RENDERERS: dict[UnitKind, Callable[[bytes], str | None]] = {
    UnitKind.MAIL: render_mail_preview,
    UnitKind.CALENDAR_EVENT: render_calendar_event_preview,
    UnitKind.CONTACT: render_contact_preview,
}

#: One tree-node expansion loads at most this many children up front;
#: ``+`` (action_load_more) fetches the next page of the same size. Bounds
#: how many ``TreeNode`` widgets get built per expand for a level with an
#: enormous fan-out — real SDK-side pagination pushdown (``Table.select``'s
#: own ``order_by``/``limit``/``offset``, ``storage/sqlite.py``'s
#: ``apply_index_hint``) means this genuinely reduces I/O/memory too, not
#: just deferred widget construction, for every provider except
#: ``RawObjectProvider``/``TeamsChatProvider`` (already bounded by index/
#: channel count, never item count — see ``tree_strategy.py``'s own module
#: docstring).
_CHILDREN_PAGE_SIZE = 500

#: How many items a SharePoint List's spreadsheet-style overview
#: (``_load_list_overview``) fetches and reads — deliberately smaller than
#: ``_CHILDREN_PAGE_SIZE``: building the overview reads every fetched item's
#: own content (one ``provider.unit()`` + bounded read each), not just its
#: listing ``Node``, so this bounds real I/O, not just widget count. Never a
#: *silent* cap — the rendered table says so when this limit was hit.
_LIST_OVERVIEW_ITEM_CAP = 50

#: Tree widget label for a disk-image node's nested "(filesystem)" sibling
#: (see ``add_child_nodes``). ``child.name`` itself is the disk's own full
#: name plus a "(filesystem)" suffix (``units/device_disk_fs.py``'s
#: ``f"{name} (filesystem)"`` convention, right for the CLI's flat,
#: non-nested listing of the same ``Node``) — nesting already puts this
#: label directly under its own disk-image node, so the widget-only label
#: omits that repeated name instead.
_DISK_FS_SIBLING_LABEL = "Filesystem"


@dataclasses.dataclass(frozen=True)
class _LoadedChildren:
    """Everything loaded so far for one expanded tree node. ``children``
    and ``exhausted`` genuinely diverge, since a level loads one page at
    a time: a node can be loaded (this exists at all) without being
    loaded *in full* (``exhausted`` is ``False``). Frozen: ``_load_more``
    replaces the whole instance in ``_children_by_node_id`` on every
    page fetched, rather than extending ``children``/bumping
    ``next_offset`` in place."""

    children: list[Node]
    next_offset: int
    exhausted: bool


class UnitScreen(NavigableScreen):
    BINDINGS = [
        *COMMON_BINDINGS,
        *NAV_BINDINGS,
        *UNIT_BINDINGS,
        VERIFY_BINDING,
        REFRESH_BINDING,
        FILTER_BINDING,
        COPY_REF_BINDING,
        GOTO_REF_BINDING,
    ]

    def __init__(self, catalog: Catalog, version: Version, *, target_ref: NodeRef | None = None) -> None:
        super().__init__()
        self._catalog = catalog
        self._version = version
        # Once the root/provider load, walk straight to this node and
        # expand its ancestors (a ``g`` jump, landing here either from
        # BrowseScreen's own ``g`` or from this same screen's ``g`` jumping to
        # a different version) — see on_mount()/_load_root().
        self._pending_target_ref = target_ref
        self._provider: UnitProvider | None = None
        self._loaded_tree_node_ids: set[int] = set()
        # Full, unfiltered children loaded so far per already-loaded tree
        # node, plus pagination state (filtering with ``/`` is purely
        # client-side over what's already loaded — never a fresh
        # provider.children() call, and never triggers a "load more"
        # itself either) — keyed by id(tree_node) since TreeNode itself
        # isn't hashable/stable across Textual's own internal bookkeeping
        # the way plain ids are (matching this file's own
        # _loaded_tree_node_ids convention).
        self._children_by_node_id: dict[int, _LoadedChildren] = {}
        self._filter_parent: TreeNode[Node] | None = None
        self._filter_text = ""
        self._detail_pane = DetailPane(self)
        self._goto_walker = GotoChainWalker(self)

    @property
    def unit_tree(self) -> Tree[Node]:
        # Named ``unit_tree``, not ``tree`` -- ``DOMNode.tree`` already exists
        # (Textual's own debug rich.tree.Tree representation).
        return self.query_one("#unit-tree", Tree)

    def compose(self) -> ComposeResult:
        # Tree's own label rendering doesn't parse markup (see
        # sdk/presentation/markup.py's docstring), so only the Static breadcrumb
        # needs escaping here, not the Tree() argument.
        yield Static(safe(self._version.display_name), id="breadcrumb")
        yield Tree(self._version.display_name, id="unit-tree")
        with VerticalScroll(id="detail-scroll"):
            yield Static("", id="detail")
        yield Static(UNIT_STATUS_BAR, id="status-bar")
        yield Input(placeholder=UNIT_FILTER_PLACEHOLDER, id="filter-input")
        yield Input(placeholder=GOTO_REF_PLACEHOLDER, id="goto-input")

    def on_mount(self) -> None:
        self._load_root()

    async def on_unmount(self) -> None:
        """Closes this screen's own provider once it's popped/replaced.

        Without this, simply browsing into a version and back leaves its
        provider's ``SqliteSource``/``aiosqlite`` connection(s) — and the
        real background thread each one owns — open for the rest of the
        session: ``Repository.close()``'s own end-of-session sweep
        (``ClosableUnitProvider``'s own docstring) is a safety net for a
        provider nobody got around to closing, not meant to be the only
        thing that ever does, and a session spent browsing many versions
        one after another would otherwise accumulate one leaked
        connection/thread per version visited. ``async def`` is safe
        here — Textual awaits an ``on_unmount`` coroutine to completion
        (see ``keymap.py``'s own module docstring)."""
        if isinstance(self._provider, ClosableUnitProvider):
            await self._provider.close()

    def _set_loading_indicator(self, markup: str | None) -> None:
        text = safe(self._version.display_name)
        if markup is not None:
            text = f"{text}  {markup}"
        self._update_breadcrumb_text(text)

    # Async ``@work`` (never ``thread=True``) — see browser/README.md.
    @work
    async def _load_root(self, *, invalidate: bool = False) -> None:
        with DebouncedProgress(self):
            repo = self.app_state.repo
            assert repo is not None
            if invalidate:
                # A real re-scan, not just re-querying already-cached
                # directory listings — see DirCache's own docstring on why
                # "refresh" is meaningless without this.
                await repo.invalidate_directory_cache()
            try:
                # ``force_raw`` is SaaS-only and ignored for VM/PC/PS/FS
                # versions (see ``Catalog.provider()``'s own docstring) —
                # passed unconditionally here rather than branching on
                # ``self._version.target_type``, since that ignoring is
                # already the contract, not something this call site
                # needs to duplicate. Diagnostic mode forces the raw
                # index-entry tree instead of whatever application-layer
                # provider would otherwise decode this version's content
                # — see ``refresh_for_verbose_mode``'s own docstring for
                # why toggling ``d`` re-runs this from scratch.
                provider = await self._catalog.provider(self._version, force_raw=self.app_state.verbose)
            except ApmRepoError as exc:
                self._show_error(str(exc))
                return
            self._provider = provider
            root_node = provider.root()
            self._populate_root(root_node)
            target_ref = self._pending_target_ref
            if target_ref is not None:
                self._pending_target_ref = None
                # Not awaited: ``_walk_to_target`` is itself a ``@work``
                # method, so calling it schedules a separate worker and
                # returns immediately.
                self._walk_to_target(provider, target_ref)

    def _show_error(self, message: str) -> None:
        show_error(self, "#detail", message)

    def _populate_root(self, node: Node) -> None:
        tree = self.unit_tree
        tree.root.data = node
        tree.root.set_label(node.name)
        tree.root.allow_expand = not node.is_leaf
        tree.focus()
        # Skip the normal auto-expand when a ``g`` jump is about to walk
        # this same root itself: auto-expanding here fires
        # on_tree_node_expanded -> _ensure_loaded -> a *second*,
        # independently-scheduled _load_children() worker that races with
        # _walk_to_target's own GotoChainWalker.expand_to_chain for who
        # adds the root's TreeNode children first — if the two orderings
        # race, expand_to_chain finds no children yet and raises
        # StopIteration. _walk_to_target (called right after this via
        # _load_root, see below) does its own equivalent expand +
        # add-children for every step of the chain including the root, so
        # there is nothing for the auto-expand to usefully race for.
        if not node.is_leaf and self._pending_target_ref is None:
            tree.root.expand()

    def on_tree_node_expanded(self, event: Tree.NodeExpanded[Node]) -> None:
        self._ensure_loaded(event.node)

    def _ensure_loaded(self, tree_node: TreeNode[Node]) -> None:
        if id(tree_node) in self._loaded_tree_node_ids or tree_node.data is None:
            return
        self._loaded_tree_node_ids.add(id(tree_node))
        self._load_children(tree_node, tree_node.data)

    @work
    async def _load_children(self, tree_node: TreeNode[Node], node: Node) -> None:
        with DebouncedProgress(self):
            provider = self._provider
            assert provider is not None
            try:
                children = await provider.children(node, offset=0, limit=_CHILDREN_PAGE_SIZE)
            except Exception as exc:
                # Broader than ApmRepoError on purpose: a provider can also
                # surface an unexpected failure from a third-party parser
                # it depends on that isn't an ApmRepoError at all (e.g.
                # KeyRequiredError for an encrypted-but-no-key-yet repository). A
                # single error leaf (no ``data``, so selecting it is a
                # no-op — see on_tree_node_selected's ``if node is None:
                # return``) shows the user something instead of a
                # silently-empty or crashed tree.
                self._show_children_error(tree_node, str(exc))
                return
            self._populate_children(tree_node, children)

    def _show_children_error(self, tree_node: TreeNode[Node], message: str) -> None:
        # Tree labels don't parse markup (unlike Static — see
        # sdk/presentation/markup.py), so no [red]...[/red] tag here; plain text.
        tree_node.add_leaf(f"error: {message}")

    def _populate_children(self, tree_node: TreeNode[Node], children: list[Node]) -> None:
        self._children_by_node_id[id(tree_node)] = _LoadedChildren(
            children=list(children), next_offset=len(children), exhausted=len(children) < _CHILDREN_PAGE_SIZE
        )
        self.add_child_nodes(tree_node, children)

    def add_child_nodes(self, tree_node: TreeNode[Node], children: list[Node]) -> None:
        """Public (not ``_``-prefixed): ``GotoChainWalker`` calls back into
        this from its own module to add nodes exactly the way ordinary
        tree expansion does."""
        # ref -> TreeNode, for children added directly under ``tree_node`` in
        # *this* call only — a disk-image node and its "(filesystem)"
        # sibling always arrive together in the same provider.children()
        # page (units/device.py's own _object_nodes / units/device_pcps.py's
        # own _disk_nodes comment), so a same-call lookup is enough to find
        # the pairing.
        added_by_ref: dict[NodeRef, TreeNode[Node]] = {}
        for child in children:
            sibling_ref = disk_fs_sibling_ref(child)
            if sibling_ref is not None and sibling_ref in added_by_ref:
                # Nest the "(filesystem)" sibling one level under its own
                # disk-image node instead of beside it, so ``XXXX.img`` and
                # ``XXXX.img (filesystem)`` read as one grouped pair rather
                # than two unrelated, duplicate-looking entries. The image
                # node's own ``data``
                # (used for export/hex-preview/detail/ref-copy) and the
                # sibling's own ``data`` (its real, unchanged Node/ref) are
                # both untouched — only where the sibling's TreeNode lives
                # in the widget, and what label it shows, change.
                image_tree_node = added_by_ref[sibling_ref]
                image_tree_node.add(_DISK_FS_SIBLING_LABEL, data=child)
                image_tree_node.allow_expand = True
                self._loaded_tree_node_ids.add(id(image_tree_node))
                self._children_by_node_id[id(image_tree_node)] = _LoadedChildren(
                    children=[child], next_offset=1, exhausted=True
                )
                continue
            # A SharePoint List group (``site_list_overview``-flagged, see
            # ``units/saas/site.py``) is genuinely ``is_leaf=False`` at the SDK
            # level — it's not a single restorable unit — but its own
            # items are deliberately never shown in this tree: the
            # spreadsheet-style overview ``_load_list_overview`` builds
            # already reads them directly via ``provider.children()``, so
            # there is nothing left for tree navigation to usefully add.
            # ``add_leaf()`` (no expand arrow) reflects that at the widget
            # level without changing the ``Node``'s own ``is_leaf`` — export/
            # hex-preview (both gated on ``node.is_leaf``) correctly still
            # refuse it, since it still isn't a real restorable unit.
            if child.is_leaf or is_list_overview(child):
                added_by_ref[child.ref] = tree_node.add_leaf(child.name, data=child)
            else:
                added_by_ref[child.ref] = tree_node.add(child.name, data=child)

    def on_tree_node_selected(self, event: Tree.NodeSelected[Node]) -> None:
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
        # ``NodeSelected`` at all). Level 1 would look unaffected only
        # because ``_populate_root()`` expands the root programmatically
        # (``tree.root.expand()``), which never posts ``NodeSelected``.
        node = event.node.data
        if node is not None and (node.is_leaf or is_list_overview(node)):
            self._show_detail(node)

    def _selected_node(self) -> Node | None:
        """``#unit-tree``'s current cursor node's own data — ``None``
        both when nothing is selected at all and when the cursor sits on
        a node Textual hasn't assigned data to (shouldn't happen for a
        real node, but ``TreeNode.data`` is itself optional). Shared by
        every action here that acts on "whatever's currently selected"
        (``action_show_detail``/``action_export_selected``/
        ``action_hex_preview``/``action_copy_ref``)."""
        tree = self.unit_tree
        return tree.cursor_node.data if tree.cursor_node is not None else None

    def action_show_detail(self) -> None:
        node = self._selected_node()
        if node is not None:
            self._show_detail(node)

    def _show_detail(self, node: Node) -> None:
        self._detail_pane.show(node)
        if self._provider is None:
            return
        if node.is_leaf:
            self._detail_pane.set_wide(False)
            self._load_preview(node)
        elif is_list_overview(node):
            self._detail_pane.set_wide(True)
            self._load_list_overview(node)

    # -- inline content preview ------------------------------------------

    @work
    async def _load_preview(self, node: Node) -> None:
        """Best-effort: reads up to ``_PREVIEW_READ_LIMIT`` bytes and tries
        to render a human-readable preview, appended below the header
        ``_show_detail`` already shows. Never raises out to the worker —
        a preview is a display convenience, not a correctness
        requirement, so one unit's malformed/unusual bytes must never
        crash the whole screen. The broad ``except Exception`` still
        doesn't swallow cancellation: ``asyncio.CancelledError`` derives
        from ``BaseException``, not ``Exception``."""
        provider = self._provider
        if provider is None:
            return
        try:
            # ``provider.unit()``/``ContentSource.read()`` are both async;
            # ``RestorableUnit.open()`` between them is synchronous.
            unit = await provider.unit(node)
            data = await unit.open().read(0, _PREVIEW_READ_LIMIT)
            kind = node.kind
            renderer = _PREVIEW_RENDERERS.get(kind, render_html_preview) if kind is not None else render_html_preview
            preview = renderer(data)
        except Exception:
            return
        if preview:
            self._detail_pane.append_preview(node, preview)

    # -- SharePoint List overview (selecting a List group, not one item) --

    @work
    async def _load_list_overview(self, node: Node) -> None:
        """Spreadsheet-style preview of a SharePoint List's own items,
        read directly through ``provider.children()``/``provider.unit()``
        since a List's items are never tree-navigable at all (see
        ``add_child_nodes``) — the only place they're ever fetched.
        Only reachable for a ``site_list_overview``-flagged group node
        (a plain List, never a document-library folder). Best-effort
        like ``_load_preview``: a failure past the initial ``children()``
        call is caught per item, never for the whole batch."""
        provider = self._provider
        if provider is None:
            return
        with DebouncedProgress(self):
            try:
                children = await provider.children(node, offset=0, limit=_LIST_OVERVIEW_ITEM_CAP)
            except ApmRepoError as exc:
                message = str(exc)
                self._detail_pane.append_list_overview_error(node, message)
                return
            rows: list[dict[str, object]] = []
            for child in children:
                if not child.is_leaf:
                    continue  # a nested folder — not expected under a plain List, skip defensively
                try:
                    unit = await provider.unit(child)
                    data = await unit.open().read(0, _PREVIEW_READ_LIMIT)
                    values = json.loads(data)
                except Exception:
                    continue  # one malformed item must never blank the whole overview
                if isinstance(values, dict):
                    rows.append(visible_site_fields(values))
            self._detail_pane.append_list_overview(node, rows, truncated=len(children) == _LIST_OVERVIEW_ITEM_CAP)

    # ``async def`` action — see keymap.py's module docstring for why
    # Textual allows this; needed here because ``provider.unit()`` is async.
    async def action_export_selected(self) -> None:
        node = self._selected_node()
        if node is None or not node.is_leaf or self._provider is None:
            self.notify(UNIT_NOTHING_SELECTED_WARNING, severity="warning")
            return
        from synology_apm_repo.browser.screens.export_screen import ExportScreen

        try:
            unit = await self._provider.unit(node)
        except Exception as exc:
            # Broader than ApmRepoError on purpose, same reasoning as
            # _load_children's identical catch: a provider's unit() can
            # raise more than ApmRepoError (a third-party parser's own
            # exception, say) -- degrade to a toast either way rather than
            # crashing the whole app.
            self.notify(str(exc), severity="warning")
            return
        self.app.push_screen(ExportScreen(unit))

    def action_go_back(self) -> None:
        # Esc closes an open filter (``/``) or goto (``g``) box first, same
        # reasoning as BrowseScreen.action_go_back.
        if self._filter_parent is not None:
            self._close_filter()
            return
        if self.query_one("#goto-input", Input).has_class("active"):
            self._close_goto()
            return
        self.app.pop_screen()

    # -- hex preview (``x``, verbose-mode only) -------------------------

    async def action_hex_preview(self) -> None:
        if not self.app_state.verbose:
            # Same positioning as ``d`` itself: raw bytes are
            # exactly what ordinary mode hides, so this is a no-op
            # (not an error) outside verbose mode rather than a key
            # that silently does nothing with no explanation at all.
            self.notify("press d to enable verbose mode first", severity="warning")
            return
        node = self._selected_node()
        if node is None or not node.is_leaf or self._provider is None:
            self.notify(UNIT_HEX_NOTHING_SELECTED_WARNING, severity="warning")
            return
        from synology_apm_repo.browser.screens.hex_preview_screen import HexPreviewScreen

        try:
            content = (await self._provider.unit(node)).open()
        except Exception as exc:
            # Same reasoning as action_export_selected's identical catch.
            self.notify(str(exc), severity="warning")
            return
        self.app.push_screen(HexPreviewScreen(content, node.name))

    # -- refresh (``r``) -----------------------------------------------

    def _reset_tree(self) -> None:
        """Discards every cached expansion and the provider itself —
        shared by ``action_refresh`` (an explicit user request) and
        ``refresh_for_verbose_mode`` (verbose mode just toggled). Closes
        the discarded provider in the background (see ``_close_provider``)
        rather than just dropping the reference — same leaked-connection
        concern ``on_unmount``'s own docstring explains, just reached via
        refresh/verbose-toggle instead of navigating away entirely. This
        method itself stays sync (both callers are plain, un-awaited
        action/hook methods), so the actual close is a fire-and-forget
        worker rather than awaited here directly."""
        tree = self.unit_tree
        tree.root.remove_children()
        self._loaded_tree_node_ids.clear()
        self._children_by_node_id.clear()
        old_provider, self._provider = self._provider, None
        if old_provider is not None:
            self._close_provider(old_provider)

    @work
    async def _close_provider(self, provider: UnitProvider) -> None:
        """Fire-and-forget close for a provider this screen no longer
        needs, called from the sync ``_reset_tree`` — safe to run
        concurrently with a fresh ``_load_root()`` fetching this screen's
        *next* provider, since the two never share a connection."""
        if isinstance(provider, ClosableUnitProvider):
            await provider.close()

    def action_refresh(self) -> None:
        """Re-queries from the root, discarding cached directory listings
        first (see ``DirCache``'s own docstring) so this actually re-scans
        the store instead of re-serving whatever was listed earlier this
        session. Unlike ``BrowseScreen``'s three independent levels there
        is one tree here, so "refresh" means the whole tree, not just the
        cursor's current level."""
        self._reset_tree()
        self._load_root(invalidate=True)

    def refresh_for_verbose_mode(self) -> None:
        """Duck-typed hook the App's ``d`` action calls after toggling
        verbose mode (see ``browse_screen.py``'s own version for the
        mechanism). Unlike that screen's version, which only re-renders
        already-fetched labels, this one re-loads the whole tree from a
        freshly dispatched provider, switching between an
        application-layer provider's decoded content and
        ``RawObjectProvider``'s raw index entries for the same version."""
        # force_raw is SaaS-only and ignored for VM/PC/PS/FS versions (see
        # Catalog.provider()'s own docstring), so a Device/FS version's
        # provider and tree are byte-for-byte identical either way --
        # reloading would only discard the user's cursor position/expansion
        # state for zero visible change. Only a real {M365, GW} version
        # actually reloads.
        if self._version.target_type not in ("M365", "GW"):
            return
        self._reset_tree()
        self._load_root()

    # -- load more (``+``) -----------------------------------------------

    def _current_listing_node(self) -> TreeNode[Node]:
        return current_listing_tree_node(self.unit_tree)

    def action_load_more(self) -> None:
        tree_node = self._current_listing_node()
        loaded = self._children_by_node_id.get(id(tree_node))
        if loaded is None:
            self.notify(UNIT_LOAD_MORE_NOTHING_TO_LOAD_WARNING, severity="warning")
            return
        if loaded.exhausted:
            self.notify(UNIT_LOAD_MORE_ALREADY_COMPLETE_WARNING, severity="warning")
            return
        self._load_more(tree_node, loaded)

    @work
    async def _load_more(self, tree_node: TreeNode[Node], loaded: _LoadedChildren) -> None:
        provider = self._provider
        if provider is None or tree_node.data is None:
            return
        with DebouncedProgress(self):
            try:
                more = await provider.children(tree_node.data, offset=loaded.next_offset, limit=_CHILDREN_PAGE_SIZE)
            except ApmRepoError as exc:
                self.notify(str(exc), severity="warning")
                return
            loaded = dataclasses.replace(
                loaded,
                children=[*loaded.children, *more],
                next_offset=loaded.next_offset + len(more),
                exhausted=len(more) < _CHILDREN_PAGE_SIZE,
            )
            self._children_by_node_id[id(tree_node)] = loaded
            if self._filter_parent is tree_node and self._filter_text:
                # An active filter on this same level must keep excluding
                # non-matches among the newly loaded children too, not
                # just the ones loaded before — re-render from the full,
                # now-extended list rather than appending unconditionally.
                self._rerender_filtered(tree_node)
            else:
                self.add_child_nodes(tree_node, more)
            self.notify(UNIT_LOAD_MORE_NOTIFY.format(loaded=len(more), total=len(loaded.children)))

    # -- filter (``/``) -------------------------------------------------

    def action_filter(self) -> None:
        tree_node = self._current_listing_node()
        loaded = self._children_by_node_id.get(id(tree_node))
        if loaded is None:
            return  # nothing loaded under this level yet — nothing to filter
        if not loaded.exhausted:
            self.notify(UNIT_FILTER_PARTIAL_LOAD_WARNING.format(loaded=len(loaded.children)), severity="warning")
        self._filter_parent = tree_node
        self._filter_text = ""
        show_filter_input(self)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "filter-input" or self._filter_parent is None:
            return
        self._filter_text = event.value
        self._rerender_filtered(self._filter_parent)

    # ``async def`` handler — see ``action_export_selected``'s own comment;
    # ``_submit_goto`` awaits an SDK call.
    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter-input":
            self._close_filter()
        elif event.input.id == "goto-input":
            await self._submit_goto(event.value)

    def _purge_loaded_ids(self, tree_node: TreeNode[Node]) -> None:
        """Recursively discards every ``id(tree_node)`` bookkeeping entry
        under (and including) ``tree_node``. Must run *before*
        ``remove_children()`` frees these ``TreeNode`` objects: CPython
        may reuse a garbage-collected node's memory address for a later
        ``.add()``ed node, and an unpurged, recycled id would make
        ``_ensure_loaded`` believe that unrelated new node was already
        loaded -- silently, permanently refusing to expand it."""
        for child in tree_node.children:
            self._purge_loaded_ids(child)
        self._loaded_tree_node_ids.discard(id(tree_node))
        self._children_by_node_id.pop(id(tree_node), None)

    def _rerender_filtered(self, tree_node: TreeNode[Node]) -> None:
        loaded = self._children_by_node_id.get(id(tree_node))
        children = loaded.children if loaded is not None else []
        needle = self._filter_text.lower()
        for child in tree_node.children:
            self._purge_loaded_ids(child)
        tree_node.remove_children()
        visible = [c for c in children if not needle or needle in c.name.lower()] if needle else children
        self.add_child_nodes(tree_node, visible)

    def _close_filter(self) -> None:
        parent = self._filter_parent
        self._filter_parent = None
        self._filter_text = ""
        self.query_one("#filter-input", Input).remove_class("active")
        if parent is not None:
            self._rerender_filtered(parent)  # empty filter text -> full list restored

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

    async def _submit_goto(self, text: str) -> None:
        self._close_goto()
        node_ref = parse_canonical_ref(text, notify=self.notify)
        if node_ref is None:
            return
        repo = self.app_state.repo
        assert repo is not None
        canonical_ids = node_ref.canonical_ids
        assert canonical_ids is not None  # parse_canonical_ref already checked node_ref.kind
        catalog_id, _workload_id, version_uid = canonical_ids
        if catalog_id == self._catalog.catalog_id and version_uid == self._version.version_uid:
            self._walk_to_target(self._provider, node_ref)
            return
        resolved = await resolve_goto_version(self.notify, repo, node_ref)
        if resolved is None:
            return
        catalog, version = resolved
        self.app.push_screen(UnitScreen(catalog, version, target_ref=node_ref))

    @work
    async def _walk_to_target(self, provider: UnitProvider | None, target_ref: NodeRef) -> None:
        if provider is None:
            return
        try:
            found = await find_path_with_children(provider, target_ref)
        except ApmRepoError as exc:
            # find_path_with_children() calls provider.children() on every
            # non-leaf node along the way with no catch of its own (unlike
            # _load_children's identical call, which shows a per-node error
            # leaf instead of crashing) — a node whose children() raises
            # (e.g. a VM version whose target.db never landed) would
            # otherwise leave this worker's exception uncaught. Same
            # ApmRepoError -> notify + fall back posture as the
            # version_for_ref() catch just above.
            self.notify(str(exc), severity="warning")
            self._ensure_root_expanded()
            return
        if found is None:
            self.notify(GOTO_REF_NOT_FOUND_WARNING, severity="warning")
            # _populate_root() deliberately skipped its own auto-expand
            # while this walk was pending (see its docstring) — since the
            # walk itself just failed, nothing else will ever expand the
            # root, so do the plain fallback here instead of leaving the
            # user stuck looking at a permanently collapsed tree.
            self._ensure_root_expanded()
            return
        chain, children_by_step = found
        target_node = self._goto_walker.expand_to_chain(chain, children_by_step)
        if target_node is not None:
            self._show_detail(target_node)

    def _ensure_root_expanded(self) -> None:
        tree = self.unit_tree
        if tree.root.data is not None and not tree.root.data.is_leaf and not tree.root.is_expanded:
            tree.root.expand()

    # -- collaborator accessors for GotoChainWalker ----------------------
    # Public (not ``_``-prefixed): reached from ``goto_walker.py``, a separate
    # module -- see that module's own docstring for the convention this
    # follows.

    def is_loaded(self, tree_node: TreeNode[Node]) -> bool:
        return id(tree_node) in self._loaded_tree_node_ids

    def mark_loaded_exhaustive(self, tree_node: TreeNode[Node], children: list[Node]) -> None:
        """Marks ``tree_node`` as loaded with exactly ``children``, in full --
        used only by goto-ref chain-walking, which always fetches a step's
        complete sibling list via ``find_path_with_children`` rather than a
        page-sized guess."""
        self._loaded_tree_node_ids.add(id(tree_node))
        self._children_by_node_id[id(tree_node)] = _LoadedChildren(
            children=list(children), next_offset=len(children), exhausted=True
        )

    def loaded_children(self, tree_node: TreeNode[Node]) -> _LoadedChildren | None:
        return self._children_by_node_id.get(id(tree_node))
