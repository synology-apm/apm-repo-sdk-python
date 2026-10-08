"""``BrowseScreen``: the root, three-column view -- repositories and
catalogs (column 1), workloads grouped by type (column 2), versions
(column 3).

``ConnectDialog`` (opened at launch, and by ``c``) picks the source and
scans it; this screen renders the result (``_apply_discovered``). A
repository's catalogs load when it is expanded, since ``catalogs()`` costs
network round trips on S3/Azure; a catalog's workloads load on selection.
Column 3 is a ``DataTable`` rebuilt with ``clear()``, its cursor restored
by version.

State lives in a ``Store`` (``core/browse/``, effects in
``runtime/browse_effects.py``); the screen keeps only view-local state
(``_cursor_auto_parked``, the filter controllers,
``_visible_version_indices``)."""

from __future__ import annotations

import dataclasses
from typing import ClassVar, cast, override

from textual.app import ComposeResult
from textual.binding import Binding as KeyBinding
from textual.binding import BindingType
from textual.containers import Horizontal
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Footer, Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.core.app.model import jobs_occupy_slot
from synology_apm_repo.browser.core.browse.cmd import BrowseCmd
from synology_apm_repo.browser.core.browse.model import BrowseModel, FilterTree, VersionFilterState
from synology_apm_repo.browser.core.browse.msg import (
    BrowseMsg,
    CatalogSelected,
    CatalogsRequested,
    RefreshRequested,
    RepoAdded,
    RepoSelected,
    RescanStarted,
    TreeFilterClosed,
    TreeFilterTextChanged,
    VerboseSet,
    VersionFilterClosed,
    VersionFilterOpened,
    VersionFilterTextChanged,
    WorkloadSelected,
)
from synology_apm_repo.browser.core.browse.select import (
    CatalogTreeKey,
    WorkloadTreeKey,
    breadcrumb,
    catalog_tree_spec,
    current_workloads,
    tree_filter_target,
    version_fetch_pending,
    version_load_error,
    version_rows,
    visible_version_rows,
    workload_tree_spec,
)
from synology_apm_repo.browser.core.browse.update import update
from synology_apm_repo.browser.core.keys import CatalogKey, RepoHandle
from synology_apm_repo.browser.core.remote_data import Success
from synology_apm_repo.browser.keymap import (
    COMMON_BINDINGS,
    FILTER_BINDING,
    GOTO_REF_BINDING,
    NAV_BINDINGS,
    REFRESH_BINDING,
    VERIFY_BINDING,
    WORKLIST_BINDING,
    hidden,
)
from synology_apm_repo.browser.runtime.browse_effects import BrowseEffects
from synology_apm_repo.browser.screens._shared import (
    FilterFieldController,
    StoreScreen,
    current_listing_tree_node,
)
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog, ConnectResult
from synology_apm_repo.browser.screens.key_dialog import KeyDialog
from synology_apm_repo.browser.screens.unit_screen import UnitScreen
from synology_apm_repo.browser.strings import (
    BROWSE_FILTER_PLACEHOLDER,
    BROWSE_VERSIONS_EMPTY_LABEL,
    GOTO_REF_PLACEHOLDER,
    RECONNECT_EXPORT_BUSY_WARNING,
    REFRESH_EXPORT_BUSY_WARNING,
)
from synology_apm_repo.browser.view.reconcile import (
    Binding,
    NodeSpec,
    find_node,
    force_tree_line_cache,
    reconcile_and_restore_cursor,
)
from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.sdk import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.presentation import pluralize


def _workload_of(tree_node: TreeNode[Binding[WorkloadTreeKey]] | None) -> Workload | None:
    if tree_node is None or tree_node.data is None or not isinstance(tree_node.data.payload, Workload):
        return None
    return tree_node.data.payload


def _find_first_leaf_holding_group(
    node: TreeNode[Binding[WorkloadTreeKey]],
) -> TreeNode[Binding[WorkloadTreeKey]] | None:
    """Descends from ``node`` (column 2's first top-level node) to the
    first node whose children are real workload leaves -- a device group
    already is one; a SaaS platform header isn't (tenant -> sub_type ->
    leaf)."""
    if not node.children:
        return None
    if _workload_of(node.children[0]) is not None:
        return node
    return _find_first_leaf_holding_group(node.children[0])


class BrowseScreen(StoreScreen[BrowseModel, BrowseMsg, BrowseCmd]):
    BINDINGS: ClassVar[list[BindingType]] = [
        # The footer shows only c/g/q/?; d stays bound and in ?'s help.
        *hidden(COMMON_BINDINGS, "d"),
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
        # Once-per-scan guard for parking the cursor on the first catalog
        # (reset in _apply_discovered); see browser/README.md's "View-local
        # state never needs a Cmd".
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
        # self.store/self.effects are built in on_mount(): self.app isn't
        # reachable before mount.

    @property
    def _catalog_tree(self) -> Tree[Binding[CatalogTreeKey]]:
        return self.query_one("#col-catalogs", Tree)

    @property
    def _workload_tree(self) -> Tree[Binding[WorkloadTreeKey]]:
        return self.query_one("#col-workloads", Tree)

    @override
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

    @override
    def on_mount(self) -> None:
        super().on_mount()
        self._open_store(BrowseModel(verbose=self.app_state.verbose), update, lambda cmd: self.effects.perform(cmd))
        self.effects = BrowseEffects(
            self,
            self.app_state.resources,
            self.store,
            catalog_tree=lambda: cast("Tree[Binding[object]]", self._catalog_tree),
            workload_tree=lambda: cast("Tree[Binding[object]]", self._workload_tree),
            version_table=lambda: self.query_one("#col-versions", DataTable),
            set_current_repo=self._set_current_repo,
            maybe_auto_park_catalog_cursor=self._maybe_auto_park_catalog_cursor,
            prompt_for_key=lambda repo, on_dismiss: self.app.push_screen(KeyDialog(repo), on_dismiss),
        )
        # init=False: the model above already starts with the current flag.
        self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)
        table = self.query_one("#col-versions", DataTable)
        table.add_column("Version")
        table.cursor_type = "row"
        # Textual's Tree roots default to collapsed, which would hide
        # everything under these permanent containers.
        self._catalog_tree.root.expand()
        self._workload_tree.root.expand()
        # Default focus if the user Escapes out of the auto-opened
        # ConnectDialog without connecting.
        self._catalog_tree.focus()
        self.store.subscribe(
            catalog_tree_spec,
            self._render_catalog_tree,
            init=True,
        )
        # Must precede the current_workloads subscription, which moves the
        # cursor onto this tree: Store._notify() runs subscriptions in
        # registration order.
        self.store.subscribe(
            workload_tree_spec,
            self._render_workload_tree,
            init=True,
        )
        self.store.subscribe(current_workloads, self._on_workloads_populated, init=False)
        # version_rows() alone stays () across Loading -> FailureInfo and
        # Loading -> empty Success, and the filter text is applied inside
        # _render_versions(), so the slice also carries the error, filter
        # and fetch_pending to make each of those transitions a change.
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

    def _set_current_repo(self, repo: RepoHandle | None) -> None:
        self.app_state.repo_handle = repo

    # -- breadcrumb ---------------------------------------------------

    def _breadcrumb(self) -> str:
        return breadcrumb(self.store.model)

    def _on_selection_changed(self, _selection: object) -> None:
        self._update_breadcrumb()

    def _update_breadcrumb(self) -> None:
        self._update_breadcrumb_text(self._breadcrumb())

    # -- discovery (column 1) --------------------------------------------

    @override
    def _close_open_filter(self) -> bool:
        # The tree and version filters share one input; close whichever is open.
        if self.store.model.tree_filter is not None:
            self._close_tree_filter()
            return True
        if self.store.model.version_filter is not None:
            self._close_version_filter()
            return True
        return False

    def action_refresh(self) -> None:
        """Re-queries the deepest selected level (``update()``'s
        ``RefreshRequested``). With nothing selected, reopens
        ``ConnectDialog``, which holds the credentials behind the results."""
        model = self.store.model
        if model.selected_workload is not None or model.selected_catalog is not None:
            # Dropping the caches replaces connections a running export reads through.
            if jobs_occupy_slot(self.app_state.jobs):
                self.notify(REFRESH_EXPORT_BUSY_WARNING, severity="warning")
                return
            self.store.dispatch(RefreshRequested())
        else:
            self.action_connect_remote()

    def _refuse_reconnect_during_export(self) -> bool:
        """Warns and returns ``True`` while an export is running or queued: reconnecting closes the
        repository it reads from."""
        if self.app_state.jobs:
            self.notify(RECONNECT_EXPORT_BUSY_WARNING, severity="warning")
            return True
        return False

    def action_connect_remote(self) -> None:
        if self._refuse_reconnect_during_export():
            return

        def on_dismiss(result: ConnectResult | None) -> None:
            if result is not None:
                repos, label = result
                self._apply_discovered(repos, label)

        self.app.push_screen(ConnectDialog(), on_dismiss)

    def _apply_discovered(self, repos: list[Repository], label: str) -> None:
        """Renders a completed ``ConnectDialog`` scan. ``repos`` is non-empty;
        catalogs load lazily per repository."""
        assert repos, "ConnectDialog only ever dismisses once at least one repository was found"
        self._cursor_auto_parked = False
        self.store.dispatch(RescanStarted(scan_path=label))
        for repo in repos:
            handle = self.app_state.resources.put_repo(repo)
            self.store.dispatch(RepoAdded(repo=handle, layout=repo.layout, key_status=repo.key_status))
        self._finish_discovery(len(repos))
        self._catalog_tree.focus()

    def _finish_discovery(self, count: int) -> None:
        repos = pluralize(count, "repository", "repositories")
        self.query_one("#open-status", Static).update(f"found {count} {repos}")

    # -- tree rendering (Store subscriptions) -----------------------------

    def _render_catalog_tree(self, specs: tuple[NodeSpec[CatalogTreeKey], ...]) -> None:
        """Reconciles ``specs`` onto the fixed root's children."""
        reconcile_and_restore_cursor(self._catalog_tree, specs)

    def _render_workload_tree(self, specs: tuple[NodeSpec[WorkloadTreeKey], ...]) -> None:
        reconcile_and_restore_cursor(self._workload_tree, specs)

    def _maybe_auto_park_catalog_cursor(self, repo: RepoHandle) -> None:
        """Parks the cursor on the first catalog once the first-expanded
        repository's catalogs arrive; at most once per scan. The
        ``cursor_node is node`` check keeps a late load from yanking the
        cursor back. Called by ``BrowseEffects`` after a successful
        ``LoadCatalogsFor``."""
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
        """Expands the chain to root and parks the cursor on the first
        workload leaf when column 2 first shows workloads (or after a
        refresh). A ``/`` filter re-render doesn't trigger it."""
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
        if event.node.data is None or not isinstance(repo := event.node.data.payload, RepoHandle):
            return  # not a repo node (the tree's own root, whose .data stays None)
        if event.node.children:
            return  # already loaded (or errored -- both leave real children behind)
        self.store.dispatch(CatalogsRequested(repo=repo))

    async def on_tree_node_selected(self, event: Tree.NodeSelected[Binding[object]]) -> None:
        tree_id = event.control.id
        if tree_id == "col-catalogs":
            self._on_catalog_tree_selected(cast(TreeNode[Binding[CatalogTreeKey]], event.node))
        elif tree_id == "col-workloads":
            self._on_workload_tree_selected(cast(TreeNode[Binding[WorkloadTreeKey]], event.node))
        # Tree itself already toggles a non-leaf node's expansion.

    def _on_catalog_tree_selected(self, tree_node: TreeNode[Binding[CatalogTreeKey]]) -> None:
        if tree_node.data is None:
            return
        payload = tree_node.data.payload
        if isinstance(payload, Catalog):
            repo = tree_node.data.key
            assert isinstance(repo, CatalogKey)
            self.store.dispatch(CatalogSelected(repo=repo.repo, catalog=payload))
        elif isinstance(payload, RepoHandle):
            self.store.dispatch(RepoSelected(repo=payload))
        # payload is None for the synthetic "error: ..." leaf -- nothing to do.

    def _on_workload_tree_selected(self, tree_node: TreeNode[Binding[WorkloadTreeKey]]) -> None:
        workload = _workload_of(tree_node)
        if workload is not None:
            self.store.dispatch(WorkloadSelected(workload=workload))
        # A bare str-payloaded group node (device type_hint, SaaS platform,
        # tenant key) — nothing further to do for any of them.

    # -- column 3: version (flat DataTable) -----------------------------

    def _on_versions_changed(
        self, _slice: tuple[tuple[tuple[int, str], ...], str | None, VersionFilterState | None, bool]
    ) -> None:
        self._render_versions()

    def _render_versions(self) -> None:
        table = self.query_one("#col-versions", DataTable)
        # clear() resets the cursor; remember its version to restore it.
        cursor_index: int | None = None
        if 0 <= table.cursor_row < len(self._visible_version_indices):
            cursor_index = self._visible_version_indices[table.cursor_row]
        table.clear()
        model = self.store.model
        error = version_load_error(model)
        if error is not None:
            # An error replaces the level and isn't filtered.
            table.add_row(f"error: {error}")
            self._visible_version_indices = []
            return
        version_filter_active = model.version_filter is not None
        rows = visible_version_rows(model)
        visible: list[int] = []
        cursor_row = 0
        for index, name in rows:
            if index == cursor_index:
                cursor_row = len(visible)
            table.add_row(name)
            visible.append(index)
        self._visible_version_indices = visible
        if not rows and not version_filter_active and not version_fetch_pending(model):
            # Genuinely empty list (e.g. every version expired). Not
            # selectable: _visible_version_indices stays empty.
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
        key = model.selected_workload_key
        if key is None:
            return  # pragma: no cover - defensive; a visible version row implies both are already selected
        original_index = self._visible_version_indices[event.cursor_row]
        versions_state = model.workload_versions.get(key)
        if not isinstance(versions_state, Success):  # pragma: no cover - defensive
            return
        self._open_version(versions_state.value[original_index])

    def _open_version(self, version: Version) -> None:
        assert self.store.model.selected_catalog is not None
        self.app.push_screen(UnitScreen(self.store.model.selected_catalog.catalog, version))

    @override
    def _go_back(self) -> None:
        # Root screen, never popped: reset and reopen ConnectDialog instead.
        if self._refuse_reconnect_during_export():
            return
        self._close_everything()
        self.action_connect_remote()

    def _close_everything(self) -> None:
        """Closes every open repository and returns this screen to its
        initial blank state."""
        self._cursor_auto_parked = False
        self.store.dispatch(RescanStarted(scan_path=""))
        self.query_one("#open-status", Static).update("")
        self.app_state.repo_handle = None

    # No key-entry binding/action: entering a key is triggered
    # automatically — see BrowseEffects._prompt_for_key.

    # -- filter (``/``) ------------------------------------------------
    # Filters whichever column has focus.

    def action_filter(self) -> None:
        focused = self.focused
        if isinstance(focused, Tree) and focused.id in ("col-catalogs", "col-workloads"):
            self._open_tree_filter(cast(Tree[Binding[object]], focused))
        elif isinstance(focused, DataTable) and focused.id == "col-versions":
            self._open_version_filter()

    def _open_tree_filter(self, tree: Tree[Binding[object]]) -> None:
        parent = current_listing_tree_node(tree)
        key = parent.data.key if parent.data is not None else None  # None: the tree's own root
        which = FilterTree.CATALOGS if tree.id == "col-catalogs" else FilterTree.WORKLOADS
        if (opened := tree_filter_target(self.store.model, which, key)) is None:
            return
        self.store.dispatch(opened)
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
        # Cancels the pending debounce and drops the filter: the full list returns at once.
        self._tree_filter.close()

    def _close_version_filter(self) -> None:
        self._version_filter.close()  # post_close refocuses #col-versions

    def _focus_versions_column(self) -> None:
        self.query_one("#col-versions", DataTable).focus()

    def refresh_for_verbose_mode(self) -> None:
        """Watch callback for the app's ``verbose``: the trees' labels
        re-render through their store subscriptions."""
        self.store.dispatch(VerboseSet(verbose=self.app_state.verbose))
