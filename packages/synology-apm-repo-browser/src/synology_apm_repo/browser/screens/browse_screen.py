"""``BrowseScreen`` is the persistent three-column view users land in
once a source has been picked: catalog(s) (column 1) -> workload type ->
workload (column 2) -> version (column 3).

Picking *where* to browse, and the connection test/scan itself, both live
entirely in ``ConnectDialog`` (auto-opened on top of this screen at app
start; ``c`` reopens it — see ``action_connect_remote``). This screen only
ever *renders* an already-completed scan's results (``_apply_discovered``),
never runs one itself.

Column 1's catalogs load lazily, on ``on_tree_node_expanded`` — a repository's
``catalogs()`` is a real network cost on S3/Azure (several full
SQLite-file downloads; cheap only for local storage), so eagerly
fetching it for every discovered repository before this screen even appears
would mean waiting on repositories the user may never look at (see the
``core/browse/update.py``'s own ``RepoAdded`` case for why no repository,
including the first, auto-expands). Column 2 loads a catalog's workloads
all at once, on selection, rather than per-node, since a catalog the user
has already committed to browsing is a real, already-open repository's
already-open db. Column 3 is a flat ``DataTable`` of versions.

This screen's own tree/selection state lives in a real MVU ``Store`` --
``core/browse/model.py``'s own ``BrowseModel``, ``update()`` in
``core/browse/update.py``, ``select.py``'s ``catalog_tree_spec``/
``workload_tree_spec``/``version_rows`` translating the model into
``view/reconcile.py``'s ``NodeSpec`` tree (columns 1/2) or plain rows
(column 3, which has no reconciler -- ``DataTable`` has no incremental
diff API the way ``Tree`` does, so column 3 is instead a plain
``clear()`` + ``add_row`` loop with the cursor position saved/restored
around it), and ``runtime/browse_effects.py``'s ``BrowseEffects`` doing the actual
repository/catalog I/O. This screen's own job is thin: dispatch a
``Msg`` in response to user action, and keep the handful of purely
view-local pieces of state a ``Store`` doesn't need to know about
(``_cursor_auto_parked``, the filter debounces, ``_visible_version_indices``)."""

from __future__ import annotations

import dataclasses
from typing import cast

from textual.app import ComposeResult
from textual.binding import Binding as KeyBinding
from textual.containers import Horizontal
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Footer, Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.core.browse.cmd import BrowseCmd
from synology_apm_repo.browser.core.browse.model import BrowseModel, VersionFilterState, catalog_key, workload_key
from synology_apm_repo.browser.core.browse.msg import (
    BrowseMsg,
    CatalogSelected,
    CatalogsRequested,
    RefreshRequested,
    RepoAdded,
    RepoSelected,
    RescanStarted,
    TreeFilterClosed,
    TreeFilterOpened,
    TreeFilterTextChanged,
    VersionFilterClosed,
    VersionFilterOpened,
    VersionFilterTextChanged,
    WorkloadSelected,
)
from synology_apm_repo.browser.core.browse.select import (
    CatalogTreeKey,
    RepoErrorKey,
    WorkloadGroupKey,
    WorkloadTreeKey,
    catalog_tree_spec,
    version_fetch_pending,
    version_load_error,
    version_rows,
    workload_tree_spec,
)
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import CatalogKey, RepoHandle
from synology_apm_repo.browser.core.remote_data import Success, is_pending_or_done
from synology_apm_repo.browser.keymap import (
    COMMON_BINDINGS,
    FILTER_BINDING,
    GOTO_REF_BINDING,
    NAV_BINDINGS,
    REFRESH_BINDING,
    VERIFY_BINDING,
    WORKLIST_BINDING,
)
from synology_apm_repo.browser.repo_labels import _repo_path_component
from synology_apm_repo.browser.runtime.browse_effects import BrowseEffects
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens._shared import (
    FilterFieldController,
    NavigableScreen,
    current_listing_tree_node,
    parse_canonical_ref,
    resolve_and_open_goto_target,
)
from synology_apm_repo.browser.strings import (
    BROWSE_FILTER_PLACEHOLDER,
    BROWSE_VERSIONS_EMPTY_LABEL,
    GOTO_REF_PLACEHOLDER,
)
from synology_apm_repo.browser.view.reconcile import (
    Binding,
    NodeSpec,
    find_node,
    force_tree_line_cache,
    reconcile_and_restore_cursor,
)
from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.browser.workload_grouping import _SAAS_PLATFORM_TYPES, _humanize_type, _saas_group_key
from synology_apm_repo.sdk.api import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.node_ref import NodeRef

#: This screen's own footer shows only ``c``/``g``/``q``/``?`` — every
#: other action below stays fully bound and reachable via ``?``'s help
#: screen (which lists every active binding regardless of ``show``), just
#: not printed in the footer. Built via ``dataclasses.replace()`` on the
#: same shared ``keymap.py`` constants other screens use unmodified, so
#: only *this* screen's own visibility differs — never edit the shared
#: constants themselves for this.
_VERBOSE_BINDING = next(b for b in COMMON_BINDINGS if isinstance(b, KeyBinding) and b.key == "d")


def _workload_of(tree_node: TreeNode[Binding[WorkloadTreeKey]] | None) -> Workload | None:
    if tree_node is None or tree_node.data is None or not isinstance(tree_node.data.payload, Workload):
        return None
    return tree_node.data.payload


def _find_first_leaf_holding_group(
    node: TreeNode[Binding[WorkloadTreeKey]],
) -> TreeNode[Binding[WorkloadTreeKey]] | None:
    """Descends from ``node`` (column 2's first top-level node) to the
    first node whose own children are real workload leaves -- a device
    group already is one; a SaaS platform header isn't (its children are
    tenant nodes, whose own children are sub_type groups, whose children
    finally are leaves). Recurses however deep this particular workload
    happens to nest."""
    if not node.children:
        return None
    if _workload_of(node.children[0]) is not None:
        return node
    return _find_first_leaf_holding_group(node.children[0])


class BrowseScreen(NavigableScreen):
    BINDINGS = [
        *[b for b in COMMON_BINDINGS if not (isinstance(b, KeyBinding) and b.key == "d")],
        dataclasses.replace(_VERBOSE_BINDING, show=False),
        *NAV_BINDINGS,
        VERIFY_BINDING,
        dataclasses.replace(REFRESH_BINDING, show=False),
        dataclasses.replace(FILTER_BINDING, show=False),
        dataclasses.replace(GOTO_REF_BINDING, show=True),
        dataclasses.replace(WORKLIST_BINDING, show=False),
        KeyBinding("c", "connect_remote", "Connect"),
    ]

    def __init__(self) -> None:
        super().__init__()
        # A one-time guard: parks the cursor on the first catalog once
        # whichever repository the user expands first actually finishes
        # loading its catalogs() call -- purely a widget/cursor mechanic
        # (reads live cursor position), so it stays here rather than in
        # the Store's own Model (see browser/README.md's "View-local
        # state never needs a Cmd"). Reset in _apply_discovered, once per
        # scan.
        self._cursor_auto_parked = False
        self._tree_filter = FilterFieldController(
            self,
            lambda text: self.store.dispatch(TreeFilterTextChanged(text=text)),
            lambda: self.store.dispatch(TreeFilterClosed()),
        )
        self._version_filter = FilterFieldController(
            self,
            lambda text: self.store.dispatch(VersionFilterTextChanged(text=text)),
            lambda: self.store.dispatch(VersionFilterClosed()),
            post_close=self._focus_versions_column,
        )
        self._visible_version_indices: list[int] = []
        # self.store/self.effects are constructed in on_mount(), not
        # here: Textual gives a screen no App to reach (self.app/
        # self.app_state) until it actually mounts.

    @property
    def _catalog_tree(self) -> Tree[Binding[CatalogTreeKey]]:
        return self.query_one("#col-catalogs", Tree)

    @property
    def _workload_tree(self) -> Tree[Binding[WorkloadTreeKey]]:
        return self.query_one("#col-workloads", Tree)

    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")
        yield Static("", id="open-status")
        with Horizontal(id="columns"):
            yield FastLabelTree("Catalogs", id="col-catalogs")
            yield FastLabelTree("Workloads", id="col-workloads")
            yield DataTable(id="col-versions")
        yield Input(placeholder=BROWSE_FILTER_PLACEHOLDER, id="filter-input")
        yield Input(placeholder=GOTO_REF_PLACEHOLDER, id="goto-input")
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        super().on_mount()
        self.store: Store[BrowseModel, BrowseMsg, BrowseCmd] = Store(BrowseModel(), update, self._perform)
        self.effects = BrowseEffects(
            self,
            self.app_state.resources,
            self.store,
            catalog_tree=lambda: cast("Tree[Binding[object]]", self._catalog_tree),
            workload_tree=lambda: cast("Tree[Binding[object]]", self._workload_tree),
            version_table=lambda: self.query_one("#col-versions", DataTable),
            set_current_repo=self._set_current_repo,
            maybe_auto_park_catalog_cursor=self._maybe_auto_park_catalog_cursor,
        )
        # init=False: nothing verbose-dependent is rendered yet at this
        # point (no repos discovered until later, via _apply_discovered)
        # -- the watch only needs to fire on a genuine later toggle.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        table = self.query_one("#col-versions", DataTable)
        table.add_column("Version")
        table.cursor_type = "row"
        # Both trees' roots are permanent containers (repository(s); workload
        # type groups), never meant to be collapsed by the user -- unlike
        # UnitScreen's own root (a real, leaf-or-not restorable-unit
        # node), Textual's Tree defaults every root to *collapsed*, so
        # without this, nothing added under either root ever becomes
        # visible.
        self._catalog_tree.root.expand()
        self._workload_tree.root.expand()
        # Nothing is populated yet at this point -- ConnectDialog is
        # auto-opened right on top of this screen the instant it mounts
        # (see app.py's own on_mount) and _apply_discovered() only ever
        # runs after that dialog's scan actually finds something, so
        # there's no meaningful widget to focus here until then; the
        # catalog tree is still a reasonable default landing spot for
        # the (rare) case the user Esc's out of that first dialog
        # without connecting anything.
        self._catalog_tree.focus()
        self.store.subscribe(
            lambda model: catalog_tree_spec(model, scan_path=model.scan_path, verbose=self.app_state.verbose),
            self._render_catalog_tree,
            init=True,
        )
        # Registered before the auto-park subscription below, in that
        # order deliberately: Store._notify() calls subscriptions in
        # registration order, and the auto-park reaction needs this
        # render's own reconcile_and_restore_cursor() to have already run
        # so the TreeNodes it navigates actually exist.
        self.store.subscribe(
            lambda model: workload_tree_spec(model, verbose=self.app_state.verbose),
            self._render_workload_tree,
            init=True,
        )
        self.store.subscribe(self._current_workloads_success, self._on_workloads_populated, init=False)
        # (rows, error, version_filter, fetch_pending) together --
        # version_rows() alone returns () for both Loading and
        # FailureInfo, so a bare Loading -> FailureInfo transition
        # wouldn't otherwise register as a slice change and this screen
        # would never actually render the error row. model.version_filter
        # is in this same slice for the identical reason: the filter text
        # itself is applied inside _render_versions() (view-local, per
        # browser/README.md's own "View-local state never needs a Cmd"
        # section -- substring matching plus cursor-position restore, not
        # a NodeSpec-tree concern), so a filter-only change (open/type/
        # close) never touches workload_versions at all and would
        # otherwise never reach _on_versions_changed, leaving a keystroke
        # into the open filter box with no visible effect.
        # version_fetch_pending(model) is here for the same
        # reason again: version_rows() alone also stays () across a first
        # Loading() -> a genuinely-empty Success(()) transition, so
        # without watching this flag too, _render_versions() would never
        # be asked to add the "(no available versions)" placeholder once
        # that fetch actually resolves.
        self.store.subscribe(
            lambda model: (
                version_rows(model),
                version_load_error(model),
                model.version_filter,
                version_fetch_pending(model),
            ),
            self._on_versions_changed,
            init=True,
        )
        self.store.subscribe(
            lambda model: (model.selected_catalog, model.selected_workload), self._on_selection_changed, init=False
        )

    def _perform(self, cmd: BrowseCmd) -> None:
        self.effects.perform(cmd)

    def _set_current_repo(self, repo: RepoHandle | None) -> None:
        self.app_state.repo_handle = repo

    @staticmethod
    def _current_workloads_success(model: BrowseModel) -> tuple[Workload, ...] | None:
        if model.selected_catalog is None:
            return None
        key = catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog)
        state = model.catalog_workloads.get(key)
        return state.value if isinstance(state, Success) else None

    # -- breadcrumb ---------------------------------------------------

    def _breadcrumb(self) -> str:
        # display_name/path are real backup-derived or filesystem text —
        # escaped before reaching Static, per sdk/presentation/markup.py's docstring.
        parts = []
        model = self.store.model
        if model.selected_catalog is not None:
            repo_state = model.repos.get(model.selected_catalog.repo)
            if repo_state is not None:
                parts.append(safe(_repo_path_component(repo_state.layout, model.scan_path)))
            parts.append(safe(model.selected_catalog.catalog.display_name))
        if model.selected_workload is not None:
            workload = model.selected_workload
            if workload.workload_type in _SAAS_PLATFORM_TYPES:
                parts.append(safe(_humanize_type(workload.workload_type)))
                parts.append(safe(_saas_group_key(workload)))
            parts.append(safe(_humanize_type(workload.type_hint)))
            parts.append(safe(workload.display_name))
        return " › ".join(parts) if parts else "/"

    def _on_selection_changed(self, _selection: object) -> None:
        self._update_breadcrumb()

    def _update_breadcrumb(self) -> None:
        self._set_loading_indicator(None)

    def _set_loading_indicator(self, markup: str | None) -> None:
        text = self._breadcrumb()
        if markup is not None:
            text = f"{text}  {markup}"
        self._update_breadcrumb_text(text)

    # -- discovery (column 1) --------------------------------------------
    #
    # There is no scan/connect logic left on this screen at all — every
    # backend (local directory, S3, Azure) is picked *and* scanned inside
    # ConnectDialog itself, which only ever dismisses once it already has
    # a completed, non-empty result. Everything here only renders that
    # result.

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter-input":
            # Same two-mode branch as ``action_go_back`` (Esc) — the tree
            # filter and the version filter (column 3) are mutually
            # exclusive and share this one input, so Enter has to close
            # whichever one is actually open rather than always assuming
            # the tree filter.
            if self.store.model.tree_filter is not None:
                self._close_tree_filter()
            elif self.store.model.version_filter is not None:
                self._close_version_filter()
        elif event.input.id == "goto-input":
            self._submit_goto(event.value)

    def action_refresh(self) -> None:
        """Re-queries whichever level is currently the deepest selected
        one -- ``update()``'s own ``RefreshRequested`` case does the
        actual reset/re-dispatch work, this is just the guard-and-dispatch
        (same shape ``UnitScreen.action_refresh`` uses). With nothing
        selected, this reopens ``ConnectDialog`` instead -- the exact
        same flow ``c`` triggers -- since the store/credentials behind
        the current results live only inside that dialog, not on this
        screen, so there's nothing here to re-scan directly."""
        model = self.store.model
        if model.selected_workload is not None or model.selected_catalog is not None:
            self.store.dispatch(RefreshRequested())
        else:
            self.action_connect_remote()

    def action_connect_remote(self) -> None:
        from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog, ConnectResult

        def on_dismiss(result: ConnectResult | None) -> None:
            if result is not None:
                repos, label = result
                self._apply_discovered(repos, label)

        self.app.push_screen(ConnectDialog(), on_dismiss)

    def _apply_discovered(self, repos: list[Repository], label: str) -> None:
        """Renders an already-completed ``ConnectDialog`` scan. ``repos``
        is always non-empty -- the dialog only ever dismisses successfully
        once at least one repository was found -- and carries only
        repository handles, not their catalogs/connections: those are
        loaded lazily by ``BrowseScreen``, on demand, once a catalog node
        is actually expanded, since fetching from S3/Azure is a real
        network cost (several full SQLite-file downloads)."""
        assert repos, "ConnectDialog only ever dismisses once at least one repository was found"
        self._cursor_auto_parked = False
        self.store.dispatch(RescanStarted(scan_path=label))
        for repo in repos:
            handle = self.app_state.resources.put_repo(repo)
            self.store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        self._finish_discovery(len(repos))
        self._catalog_tree.focus()

    def _finish_discovery(self, count: int) -> None:
        # count is always > 0 here — asserted in _apply_discovered (this
        # method's one caller), not re-checked here.
        repos = pluralize(count, "repository", "repositories")
        self.query_one("#open-status", Static).update(f"found {count} {repos}")

    # -- tree rendering (Store subscriptions) -----------------------------

    def _render_catalog_tree(self, specs: tuple[NodeSpec[CatalogTreeKey], ...]) -> None:
        """The root of ``#col-catalogs`` is a permanent, non-domain
        container ("Catalogs") -- unlike ``UnitScreen``'s own single
        tree, there's nothing to ``Binding``-wrap at the root itself,
        so this reconciles straight onto ``tree.root``'s own children via
        ``reconcile_and_restore_cursor``, which relocates the cursor onto
        its surviving domain key after reconcile."""
        reconcile_and_restore_cursor(self._catalog_tree, specs)

    def _render_workload_tree(self, specs: tuple[NodeSpec[WorkloadTreeKey], ...]) -> None:
        reconcile_and_restore_cursor(self._workload_tree, specs)

    def _maybe_auto_park_catalog_cursor(self, repo: RepoHandle) -> None:
        """Whichever repository the user expands first this scan, once
        its catalogs actually arrive: park the cursor on its first
        catalog rather than leaving it sitting on the now-expanded but
        otherwise unhelpful repository node. Fires at most once per scan
        (``_cursor_auto_parked``, reset in ``_apply_discovered``).
        ``tree.cursor_node is node``: a repository's own ``catalogs()``
        call is real, unbounded network I/O, so the user may well have
        moved the cursor elsewhere by the time it resolves -- without
        this check a late-arriving load would yank the cursor back here
        regardless. Called by ``BrowseEffects`` right after a successful
        ``LoadCatalogsFor`` dispatch -- see that class's own
        ``_load_catalogs_for``."""
        if self._cursor_auto_parked:
            return
        tree = self._catalog_tree
        node = find_node(tree.root, repo)
        if node is None or not node.children or tree.cursor_node is not node:
            return
        force_tree_line_cache(tree)
        tree.move_cursor(node.children[0])
        self._cursor_auto_parked = True

    def _on_workloads_populated(self, workloads: tuple[Workload, ...] | None) -> None:
        """Expand the whole chain up to root and park the cursor on the
        first workload leaf, the first time (or every time a refresh/
        reselect re-fetches) column 2 actually shows some real workload.
        Never fires from a ``/`` filter re-render (that only touches
        ``model.tree_filter``, not ``model.catalog_workloads``, so this
        subscription's own slice doesn't change)."""
        if not workloads:
            return
        tree = self._workload_tree
        if not tree.root.children:
            return  # pragma: no cover - defensive; workloads truthy implies a rendered group exists
        group_node = _find_first_leaf_holding_group(tree.root.children[0])
        if group_node is None or not group_node.children:
            return  # pragma: no cover - defensive; same as above
        ancestor: TreeNode[Binding[WorkloadTreeKey]] | None = group_node
        while ancestor is not None:
            ancestor.expand()
            ancestor = ancestor.parent
        force_tree_line_cache(tree)
        tree.move_cursor(group_node.children[0])
        tree.focus()

    # -- column 1: sources (repository -> catalog) -------------------------------

    def on_tree_node_expanded(self, event: Tree.NodeExpanded[Binding[object]]) -> None:
        if event.control.id != "col-catalogs":
            return
        if event.node.data is None or not isinstance(event.node.data.payload, int):
            return  # not a repo node (the tree's own root, whose .data stays None)
        repo = cast(RepoHandle, event.node.data.payload)
        state = self.store.model.repos.get(repo)
        if state is not None and is_pending_or_done(state.catalogs):
            # Already fetched, or already fetching -- even a repository with
            # genuinely zero catalogs (0 real children either way) must not
            # re-fetch on every collapse/re-expand: that's a real S3/Azure
            # network round-trip (several full SQLite-file downloads; cheap
            # only for local storage), and
            # `event.node.children` alone can't tell "empty but loaded"
            # apart from "never requested". A `Loading` fetch must not be
            # re-triggered either: Textual's own `auto_expand` toggles this
            # node on every Enter press, so a slow fetch left `Loading` when
            # the user presses Enter again re-fires this same handler --
            # `CatalogsRequested`'s epoch/request check only arbitrates which
            # *result* wins once fetches finish, it does nothing to stop a
            # second worker (and a second loading-indicator sink on this
            # same node) from being created here.
            return
        if event.node.children:
            return  # already loaded (or errored -- both leave real children behind)
        self.store.dispatch(CatalogsRequested(repo=repo))

    async def on_tree_node_selected(self, event: Tree.NodeSelected[Binding[object]]) -> None:
        tree_id = event.control.id
        if tree_id == "col-catalogs":
            self._on_catalog_tree_selected(cast(TreeNode[Binding[CatalogTreeKey]], event.node))
        elif tree_id == "col-workloads":
            self._on_workload_tree_selected(cast(TreeNode[Binding[WorkloadTreeKey]], event.node))
        # Non-leaf nodes need no further handling — Textual's own
        # Tree.auto_expand already expands/collapses in response to this
        # exact message (see unit_screen.py's on_tree_node_selected for
        # the full explanation of why re-toggling here would be wrong).

    def _on_catalog_tree_selected(self, tree_node: TreeNode[Binding[CatalogTreeKey]]) -> None:
        if tree_node.data is None:
            return
        payload = tree_node.data.payload
        if isinstance(payload, Catalog):
            repo = tree_node.data.key
            assert isinstance(repo, CatalogKey)
            self.store.dispatch(CatalogSelected(repo=repo.repo, catalog=payload))
        elif isinstance(payload, int):
            self.store.dispatch(RepoSelected(repo=cast(RepoHandle, payload)))
        # payload is None for the synthetic "error: ..." leaf -- nothing to do.

    def _on_workload_tree_selected(self, tree_node: TreeNode[Binding[WorkloadTreeKey]]) -> None:
        workload = _workload_of(tree_node)
        if workload is not None:
            self.store.dispatch(WorkloadSelected(workload=workload))
        # A bare str-payloaded group node (device type_hint, SaaS platform,
        # tenant key) — nothing further to do for any of them.

    # -- column 3: version (unchanged DataTable) --------------------------

    def _on_versions_changed(
        self, _slice: tuple[tuple[tuple[int, str], ...], str | None, VersionFilterState | None, bool]
    ) -> None:
        self._render_versions()

    def _render_versions(self) -> None:
        table = self.query_one("#col-versions", DataTable)
        # clear() below unconditionally resets cursor_coordinate to (0, 0),
        # which is jarring when this runs mid-filter (a settled keystroke of
        # the version filter, not just a fresh load) -- remember which
        # version the cursor was actually on so it can be restored below if
        # that version is still visible afterwards.
        cursor_index: int | None = None
        if 0 <= table.cursor_row < len(self._visible_version_indices):
            cursor_index = self._visible_version_indices[table.cursor_row]
        table.clear()
        model = self.store.model
        error = version_load_error(model)
        if error is not None:
            # Bypasses the version filter entirely -- the same posture
            # catalog_tree_spec's/workload_tree_spec's own synthetic error
            # leaves already take (an error replaces the level, it isn't
            # itself filterable).
            table.add_row(f"error: {error}")
            self._visible_version_indices = []
            return
        version_filter_active = model.version_filter is not None
        needle = model.version_filter.text.lower() if model.version_filter is not None else ""
        rows = version_rows(model)
        visible: list[int] = []
        cursor_row = 0
        for index, name in rows:
            if needle and needle not in name.lower():
                continue
            if index == cursor_index:
                cursor_row = len(visible)
            table.add_row(name)
            visible.append(index)
        self._visible_version_indices = visible
        if not rows and not version_filter_active and not version_fetch_pending(model):
            # A workload with every version rotated/expired out returns a
            # genuinely empty list (no error -- see catalog/version.py's own
            # filtering). version_fetch_pending guards against showing this
            # before the fetch has resolved even once -- rows is already ()
            # for that case too, but showing the placeholder there would
            # coexist with DataTableLoadingRowSink's own debounced loading
            # row instead of the fetch's first resolution actually deciding
            # which one belongs on screen. Not selectable: it's the only
            # row, so ``_visible_version_indices`` stays empty and
            # on_data_table_row_selected's own bounds check is already a
            # no-op against it.
            table.add_row(BROWSE_VERSIONS_EMPTY_LABEL)
        if visible and not version_filter_active:
            table.focus()
        if cursor_row:  # already (0, 0) from clear() above otherwise
            table.cursor_coordinate = Coordinate(cursor_row, 0)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "col-versions":
            return
        if event.cursor_row >= len(self._visible_version_indices):  # pragma: no cover - defensive
            return
        model = self.store.model
        if model.selected_catalog is None or model.selected_workload is None:
            return  # pragma: no cover - defensive; a visible version row implies both are already selected
        original_index = self._visible_version_indices[event.cursor_row]
        key = workload_key(
            catalog_key(model.selected_catalog.repo, model.selected_catalog.catalog), model.selected_workload
        )
        versions_state = model.workload_versions.get(key)
        if not isinstance(versions_state, Success):  # pragma: no cover - defensive
            return
        self._open_version(versions_state.value[original_index])

    def _open_version(self, version: Version) -> None:
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        assert self.store.model.selected_catalog is not None
        self.app.push_screen(UnitScreen(self.store.model.selected_catalog.catalog, version))

    def action_go_back(self) -> None:
        if self.store.model.tree_filter is not None:
            self._close_tree_filter()
            return
        if self.store.model.version_filter is not None:
            self._close_version_filter()
            return
        if self.query_one("#goto-input", Input).has_class("active"):
            self._close_goto()
            return
        # BrowseScreen is the app's own root/base screen — pushed exactly
        # once, in ApmRepoBrowserApp.on_mount, never popped by anything
        # else. Nothing else to close: closes whatever's currently
        # connected (a no-op when nothing is) and reopens ConnectDialog on
        # top — the same flow ``c``/app startup already trigger — so Esc
        # here never falls through to the App's own bare placeholder
        # screen underneath.
        self._close_everything()
        self.action_connect_remote()

    def _close_everything(self) -> None:
        """Closes every currently-open repository and returns this screen
        to the same blank state ``__init__`` starts from. A no-op close
        when nothing is open."""
        self._cursor_auto_parked = False
        self.store.dispatch(RescanStarted(scan_path=""))
        self.query_one("#open-status", Static).update("")
        self.app_state.repo_handle = None

    # No key-entry binding/action: entering a key is triggered
    # automatically — see BrowseEffects._prompt_for_key.

    # -- filter (``/``) ------------------------------------------------
    #
    # Branches on which column currently has focus: a Tree (columns 1/2,
    # the tree filter) or the DataTable (column 3's table-filter). Both
    # (and UnitScreen's own filter) share their commit/close mechanics
    # via FilterFieldController (screens/_shared.py) -- only *opening*
    # differs enough per call site to stay here.

    def action_filter(self) -> None:
        focused = self.focused
        if isinstance(focused, Tree) and focused.id in ("col-catalogs", "col-workloads"):
            self._open_tree_filter(cast(Tree[Binding[object]], focused))
        elif isinstance(focused, DataTable) and focused.id == "col-versions":
            self._open_version_filter()

    def _open_tree_filter(self, tree: Tree[Binding[object]]) -> None:
        parent = current_listing_tree_node(tree)
        if parent.data is None:
            return  # the tree's own root -- nothing cached to filter
        key = parent.data.key
        if tree.id == "col-catalogs":
            if isinstance(key, CatalogKey | RepoErrorKey):
                return  # cursor is one level too deep -- filtering only ever narrows a repo's own catalog children
            repo = cast(RepoHandle, key)
            state = self.store.model.repos.get(repo)
            if state is None or not isinstance(state.catalogs, Success):
                return  # nothing cached under this repo to filter
            self.store.dispatch(TreeFilterOpened(tree="catalogs", parent_key=repo))
        else:
            if not isinstance(key, WorkloadGroupKey):
                return  # a workload leaf, not a group -- nothing under it to filter
            self.store.dispatch(TreeFilterOpened(tree="workloads", parent_key=key))
        self._tree_filter.open()

    def _open_version_filter(self) -> None:
        self.store.dispatch(VersionFilterOpened())
        self._version_filter.open()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "filter-input":
            return
        if self.store.model.tree_filter is not None:
            self._tree_filter.on_text_changed(event.value)
        elif self.store.model.version_filter is not None:
            self._version_filter.on_text_changed(event.value)

    def _close_tree_filter(self) -> None:
        # empty filter text -> full list restored, applied immediately
        self._tree_filter.close()

    def _close_version_filter(self) -> None:
        self._version_filter.close()  # post_close refocuses #col-versions

    def _focus_versions_column(self) -> None:
        self.query_one("#col-versions", DataTable).focus()

    # -- goto ref (``g``) -------------------------------------------------
    # action_goto_ref/_close_goto live on NavigableScreen — only this
    # screen's own _submit_goto stays here.

    def _submit_goto(self, text: str) -> None:
        self._close_goto()
        node_ref = parse_canonical_ref(text, notify=self.notify)
        if node_ref is None:
            return
        self._goto_resolve(node_ref)

    # ``resolve_goto_version`` is real SDK I/O, so this can't stay inline
    # in the synchronous ``_submit_goto`` above without blocking the UI.
    # busy=False: delegates its entire body to resolve_and_open_goto_target,
    # which already wraps its own fetch in DebouncedProgress.
    @work(busy=False)
    async def _goto_resolve(self, node_ref: NodeRef) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        await resolve_and_open_goto_target(self, repo, node_ref)

    def refresh_for_verbose_mode(self) -> None:
        """Registered as a watch callback on the app's ``verbose``
        reactive (see ``on_mount``). Unlike ``UnitScreen``'s own version
        (which re-dispatches a real fetch, since a SaaS version's raw/
        decoded tree shapes genuinely differ), toggling ``d`` here only
        changes label *text* -- verbose-mode catalog/repo labels append a
        uuid/layout-kind suffix, nothing about which repositories/
        catalogs/workloads exist. There is no ``Msg`` for "nothing in the
        model changed, only re-render the same data with a different
        ``verbose`` flag" -- ``model.repos``/``model.catalog_workloads``
        are exactly as they already were -- so this re-invokes the same
        pure selectors the ``Store`` subscriptions in ``on_mount`` use,
        directly, bypassing ``dispatch()`` entirely."""
        self._render_catalog_tree(
            catalog_tree_spec(self.store.model, scan_path=self.store.model.scan_path, verbose=self.app_state.verbose)
        )
        self._render_workload_tree(workload_tree_spec(self.store.model, verbose=self.app_state.verbose))
