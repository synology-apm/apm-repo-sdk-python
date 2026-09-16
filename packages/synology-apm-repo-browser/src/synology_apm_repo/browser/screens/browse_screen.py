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
would mean waiting on repositories the user may never look at (see ``_add_repo``
for why no repository, including the first, auto-expands). Column 2 loads a
catalog's workloads all at once, on selection (``_load_workloads``),
rather than per-node, since a catalog the user has already committed to
browsing is a real, already-open repository's already-open db. Column 3 is a
flat ``DataTable`` of versions.
"""

from __future__ import annotations

import dataclasses

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import DataTable, Input, Static, Tree
from textual.widgets.tree import TreeNode

from synology_apm_repo.browser.keymap import (
    COMMON_BINDINGS,
    FILTER_BINDING,
    GOTO_REF_BINDING,
    NAV_BINDINGS,
    REFRESH_BINDING,
    VERIFY_BINDING,
)
from synology_apm_repo.browser.repo_labels import _catalog_label, _repo_label, _repo_path_component
from synology_apm_repo.browser.screens._shared import (
    NavigableScreen,
    current_listing_tree_node,
    force_tree_line_cache,
    parse_canonical_ref,
    resolve_goto_version,
    show_filter_input,
)
from synology_apm_repo.browser.strings import (
    BROWSE_FILTER_PLACEHOLDER,
    BROWSE_STATUS_BAR,
    BROWSE_VERSIONS_EMPTY_LABEL,
    GOTO_REF_PLACEHOLDER,
)
from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress
from synology_apm_repo.browser.workload_grouping import (
    _SAAS_PLATFORM_TYPES,
    _group_workloads,
    _humanize_type,
    _saas_group_key,
)
from synology_apm_repo.sdk.api import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.errors import ApmRepoError, KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.identifiers import CatalogId
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.node_ref import catalog_pairs, disambiguate, version_pairs, workload_pairs


@dataclasses.dataclass(frozen=True)
class CatalogEntry:
    """A ``col-catalogs`` leaf's ``TreeNode.data`` — pairs a
    ``Catalog`` with the ``Repository`` it belongs to. Column 1 may show
    more than one repository's tree at once (multi-repository discovery under one
    scanned path), so anything downstream (loading that catalog's
    workloads, keeping ``ApmRepoBrowserApp.repo`` in sync) always needs to
    know which repository a given catalog actually came from. Not
    underscore-prefixed (unlike this module's other private helpers)
    since integration tests legitimately need to import it for white-box
    tree-node-data-type checks — the same precedent as this codebase's
    existing ``# noqa: SLF001`` white-box test access elsewhere."""

    repo: Repository
    catalog: Catalog


class BrowseScreen(NavigableScreen):
    BINDINGS = [
        *COMMON_BINDINGS,
        *NAV_BINDINGS,
        VERIFY_BINDING,
        REFRESH_BINDING,
        FILTER_BINDING,
        GOTO_REF_BINDING,
        Binding("c", "connect_remote", "Connect"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._repos: list[Repository] = []
        self._selected_catalog: CatalogEntry | None = None
        self._selected_workload: Workload | None = None
        self._versions: list[Version] = []
        # The guard for _add_repo()'s one-time "which repository is the
        # default" behavior (adopting the first-discovered repository as
        # app_state.repo — see _add_repo()'s own docstring). Fires once
        # per scan regardless of how many repositories stream in, independent of
        # _cursor_auto_parked below.
        self._auto_selected_repo = False
        # A separate one-time guard: parks the cursor on the first
        # catalog once whichever repository the user expands first actually
        # finishes loading its catalogs() call (see _load_catalogs_for) —
        # independent of _auto_selected_repo above, since nothing expands
        # a repository automatically.
        self._cursor_auto_parked = False
        # Lazy-loading bookkeeping for column 1 — matches UnitScreen's
        # own ``_loaded_tree_node_ids``: marked *before* the fetch starts
        # (in on_tree_node_expanded), not after it succeeds, so a repository
        # whose catalogs() call fails doesn't retry (and re-add a
        # second error leaf) every time it's re-expanded.
        self._catalogs_requested_repo_nodes: set[int] = set()
        # Filter (``/``) caches — filtering is purely front-end and never
        # re-calls a provider: every entry here is already-fetched domain
        # data (a repository's catalogs, a catalog's workloads), never
        # re-queried on a filter keystroke. ``_catalogs_by_repo_node``
        # is only ever written once a load actually *succeeds* (see
        # above), so ``_open_tree_filter``'s own ``id(parent) not in cache``
        # check already means "nothing loaded (yet, or ever, if it
        # errored) to filter" — no separate bookkeeping needed for that.
        # ``_workloads_by_group_node`` stays exactly the simpler "written
        # once, read on every filter keystroke" cache it always was:
        # column 2 loads a catalog's workloads all at once, on
        # selection (see module docstring), not per-node.
        self._catalogs_by_repo_node: dict[int, list[Catalog]] = {}
        self._workloads_by_group_node: dict[int, list[Workload]] = {}
        self._tree_filter_parent: TreeNode[object] | None = None
        self._tree_filter_text = ""
        self._version_names: list[tuple[int, str]] = []
        self._visible_version_indices: list[int] = []
        self._version_filter_text = ""
        self._version_filter_active = False
        # The scanned path/label ConnectDialog's last successful scan
        # dismissed with — needed by _repo_label() (via
        # _refresh_repo_labels()) for as long as this scan's results are
        # on screen, not just at the moment each repository was first added (a
        # key-status hint can only be attached *after* the fact, once
        # the user has actually tried one).
        self._scan_path = ""

    def compose(self) -> ComposeResult:
        yield Static("", id="breadcrumb")
        yield Static("", id="open-status")
        with Horizontal(id="columns"):
            yield Tree("Catalogs", id="col-catalogs")
            yield Tree("Workloads", id="col-workloads")
            yield DataTable(id="col-versions")
        yield Static(BROWSE_STATUS_BAR, id="status-bar")
        yield Input(placeholder=BROWSE_FILTER_PLACEHOLDER, id="filter-input")
        yield Input(placeholder=GOTO_REF_PLACEHOLDER, id="goto-input")

    def on_mount(self) -> None:
        table = self.query_one("#col-versions", DataTable)
        table.add_column("Version")
        table.cursor_type = "row"
        # Both trees' roots are permanent containers (repository(s); workload
        # type groups), never meant to be collapsed by the user — unlike
        # UnitScreen's own root (a real, leaf-or-not restorable-unit
        # node), Textual's Tree defaults every root to *collapsed*, so
        # without this, nothing added under either root ever becomes
        # visible: move_cursor() onto a freshly-added grandchild silently
        # clamps back to the root itself, the exact same ``_line == -1``
        # symptom ``goto_walker.py``'s own ``expand_to_chain`` comment
        # documents.
        self.query_one("#col-catalogs", Tree).root.expand()
        self.query_one("#col-workloads", Tree).root.expand()
        # Nothing is populated yet at this point — ConnectDialog is
        # auto-opened right on top of this screen the instant it mounts
        # (see app.py's own on_mount) and _apply_discovered() only ever
        # runs after that dialog's scan actually finds something, so
        # there's no meaningful widget to focus here until then; the
        # catalog tree is still a reasonable default landing spot for
        # the (rare) case the user Esc's out of that first dialog
        # without connecting anything.
        self.query_one("#col-catalogs", Tree).focus()

    def _breadcrumb(self) -> str:
        # display_name/path are real backup-derived or filesystem text —
        # escaped before reaching Static, per sdk/presentation/markup.py's docstring.
        parts = []
        if self._selected_catalog is not None:
            repo = self._selected_catalog.repo
            parts.append(safe(_repo_path_component(repo, self._scan_path)))
            parts.append(safe(self._selected_catalog.catalog.display_name))
        if self._selected_workload is not None:
            workload = self._selected_workload
            if workload.workload_type in _SAAS_PLATFORM_TYPES:
                parts.append(safe(_humanize_type(workload.workload_type)))
                parts.append(safe(_saas_group_key(workload)))
            parts.append(safe(_humanize_type(workload.type_hint)))
            parts.append(safe(workload.display_name))
        return " › ".join(parts) if parts else "/"

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
    # ConnectDialog itself (see that module's own docstring for why),
    # which only ever dismisses once it already has a completed,
    # non-empty result. Everything here only renders that result.

    # ``async def`` handler — see keymap.py's module docstring for why
    # Textual allows this; needed here because ``_submit_goto`` awaits an
    # SDK call.
    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter-input":
            # Same two-mode branch as ``action_go_back`` (Esc) — the tree
            # filter and the version filter (column 3) are mutually
            # exclusive and share this one input, so Enter has to close
            # whichever one is actually open rather than always assuming
            # the tree filter.
            if self._tree_filter_parent is not None:
                self._close_tree_filter()
            elif self._version_filter_active:
                self._close_version_filter()
        elif event.input.id == "goto-input":
            await self._submit_goto(event.value)

    def action_refresh(self) -> None:
        """Re-queries whichever level is currently the deepest selected
        one, same "discard the cached expansion, requery" semantics every
        screen's own refresh follows — scaled to this screen's three
        levels. With nothing selected, this reopens ``ConnectDialog``
        instead — the exact same flow ``c`` triggers — since the
        store/credentials behind the current results live only inside
        that dialog, not on this screen, so there's nothing here to
        re-scan directly."""
        if self._selected_workload is not None:
            self._load_versions(self._selected_workload)
        elif self._selected_catalog is not None:
            self._load_workloads(self._selected_catalog.repo, self._selected_catalog.catalog)
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
        """Renders an already-completed ``ConnectDialog`` scan. See
        ``ConnectResult``'s own declaration comment (``connect_dialog.py``)
        for the always-≥1-repository invariant and why catalogs aren't
        part of ``repos``."""
        assert repos, "ConnectDialog only ever dismisses once at least one repository was found"
        self._reset_for_new_scan(label)
        for repo in repos:
            self._add_repo(repo)
        self._finish_discovery(len(repos))

    def _reset_for_new_scan(self, path: str) -> None:
        self._scan_path = path
        tree = self.query_one("#col-catalogs", Tree)
        tree.root.remove_children()
        tree.root.set_label("Catalogs")
        tree.root.data = None
        old_repos, self._repos = self._repos, []
        if old_repos:
            self._close_repos(old_repos)
        self._catalogs_by_repo_node.clear()
        self._catalogs_requested_repo_nodes.clear()
        self._selected_catalog = None
        self._selected_workload = None
        self._auto_selected_repo = False
        self._cursor_auto_parked = False
        self._reset_workloads_tree()
        self._clear_versions()
        self._update_breadcrumb()

    @work
    async def _close_repos(self, repos: list[Repository]) -> None:
        """Fire-and-forget close for repositories discarded by a rescan — same
        leaked-connection concern as ``UnitScreen._close_provider``'s own
        docstring, just reached via reconnecting to a new source instead
        of navigating away. ``Repository.close()`` itself already settles
        any in-flight ``catalogs()``/``verify()`` fetch and attempts every
        tracked resource before reporting, so no extra coordination with
        a still-running catalog-load worker is needed here — only
        aggregating failures across the (possibly several, one scan can
        discover more than one repository) repositories being closed at once."""
        errors: list[Exception] = []
        for repo in repos:
            try:
                await repo.close()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise ExceptionGroup("BrowseScreen._close_repos() failed to close every discarded repository", errors)

    def _add_repo(self, repo: Repository) -> None:
        self._repos.append(repo)
        tree = self.query_one("#col-catalogs", Tree)
        label = _repo_label(repo, self._scan_path, verbose=self.app_state.verbose)
        tree.root.add(label, data=repo)
        if not self._auto_selected_repo:
            # First repository to arrive this scan — adopt it as the default
            # "current repository" so ``app_state.repo`` always has a sane target
            # for ``g`` even before the user explicitly clicks anything. A
            # plain pointer assignment, no I/O — deliberately does *not*
            # also expand this node: that would trigger its catalog load
            # (on_tree_node_expanded below) and land the cursor two levels
            # deep before the user asked for anything, an inconsistent
            # halfway auto-drill (column 2's workload list was never part
            # of it either). The cursor stays on column 1's root — every
            # repository, including this one, loads its catalogs only once the
            # user actually expands it. Fires as soon as something's here,
            # not only when there's exactly one repository in total. Guarded on
            # _auto_selected_repo, not _selected_catalog (see that flag's
            # own comment), so a later-discovered repository can't steal this
            # back.
            self.app_state.repo = repo
            self._auto_selected_repo = True
        tree.focus()

    def on_tree_node_expanded(self, event: Tree.NodeExpanded[object]) -> None:
        if event.control.id != "col-catalogs":
            return
        repo = event.node.data
        if not isinstance(repo, Repository) or id(event.node) in self._catalogs_requested_repo_nodes:
            return
        self._catalogs_requested_repo_nodes.add(id(event.node))
        self._load_catalogs_for(event.node, repo)

    # Async ``@work`` (never ``thread=True``) — see browser/README.md. Same
    # on-demand shape as UnitScreen._load_children: a DebouncedProgress
    # hint (nothing shown for a call fast enough not to need it), the
    # real fetch, then populate — except a fetch failure here shows an
    # error *leaf* under the repository node rather than replacing the whole
    # tree, since every other already-loaded repository stays fully usable
    # regardless of this one's outcome.
    @work
    async def _load_catalogs_for(self, repo_node: TreeNode[object], repo: Repository) -> None:
        with DebouncedProgress(self):
            try:
                catalogs = await repo.catalogs()
            except ApmRepoError as exc:
                repo_node.add_leaf(f"error: {exc}")
                return
        self._catalogs_by_repo_node[id(repo_node)] = catalogs
        self._add_catalog_children(repo_node, repo, catalogs)
        if not self._cursor_auto_parked and repo_node.children:
            # Whichever repository the user expands first this scan, once its
            # catalogs actually arrive: park the cursor on its first
            # catalog rather than leaving it sitting on the now-expanded
            # but otherwise unhelpful repository node. Fires at most once per
            # scan (_cursor_auto_parked), same as _add_repo()'s own
            # first-repository guard, but independently of it — every repository,
            # including the default one, only expands off a real user
            # action (see _add_repo()'s own docstring), so this only ever
            # runs there too.
            tree = self.query_one("#col-catalogs", Tree)
            force_tree_line_cache(tree)
            tree.move_cursor(repo_node.children[0])
            self._cursor_auto_parked = True

    def _refresh_repo_labels(self) -> None:
        """Re-labels every already-added repository node with whatever's
        currently known: each label was first computed in ``_add_repo``,
        but ``key_status``/``verbose`` may have changed since. Called
        from ``on_screen_resume`` (a ``KeyDialog`` may just have verified
        a key), directly from ``_prompt_for_key``'s own callback (needed
        before deciding whether to load workloads), and from
        ``refresh_for_verbose_mode``."""
        tree = self.query_one("#col-catalogs", Tree)
        for repo_node in tree.root.children:
            if isinstance(repo_node.data, Repository):
                repo_node.set_label(_repo_label(repo_node.data, self._scan_path, verbose=self.app_state.verbose))

    def _refresh_catalog_labels(self) -> None:
        """The per-catalog-leaf counterpart to ``_refresh_repo_labels`` —
        ``verbose`` toggling changes ``_catalog_label``'s own uuid/id
        suffix too, not just the repository-level ``layout:`` one. Looks up
        each already-rendered leaf's own name by identity in a freshly
        recomputed ``{catalog: name}`` mapping (rather than zipping
        ``repo_node.children`` against a plain list) so this stays
        correct even when a ``/`` filter is currently showing only a
        subset of one repository's own catalogs."""
        tree = self.query_one("#col-catalogs", Tree)
        for repo_node in tree.root.children:
            catalogs = self._catalogs_by_repo_node.get(id(repo_node))
            if catalogs is None:
                continue
            names_by_catalog = dict(zip(catalogs, disambiguate(catalog_pairs(catalogs)), strict=True))
            for catalog_node in repo_node.children:
                data = catalog_node.data
                if isinstance(data, CatalogEntry) and data.catalog in names_by_catalog:
                    catalog_node.set_label(
                        _catalog_label(data.catalog, names_by_catalog[data.catalog], verbose=self.app_state.verbose)
                    )

    def on_screen_resume(self) -> None:
        self._refresh_repo_labels()
        self._refresh_catalog_labels()

    def refresh_for_verbose_mode(self) -> None:
        self._refresh_repo_labels()
        self._refresh_catalog_labels()

    def _add_catalog_children(self, repo_node: TreeNode[object], repo: Repository, catalogs: list[Catalog]) -> None:
        names = disambiguate(catalog_pairs(catalogs))
        for catalog, name in zip(catalogs, names, strict=True):
            label = _catalog_label(catalog, name, verbose=self.app_state.verbose)
            repo_node.add_leaf(label, data=CatalogEntry(repo=repo, catalog=catalog))

    def _finish_discovery(self, count: int) -> None:
        # count is always > 0 here — asserted in _apply_discovered (this
        # method's one caller), not re-checked here.
        repos = pluralize(count, "repository", "repositories")
        self.query_one("#open-status", Static).update(f"found {count} {repos}")

    # -- column 1: sources (repository -> catalog) -------------------------------

    async def on_tree_node_selected(self, event: Tree.NodeSelected[object]) -> None:
        tree_id = event.control.id
        data = event.node.data
        if data is None:
            return
        if tree_id == "col-catalogs":
            await self._on_catalog_tree_selected(data)
        elif tree_id == "col-workloads":
            self._on_workload_tree_selected(data)
        # Non-leaf nodes need no further handling — Textual's own
        # Tree.auto_expand already expands/collapses in response to this
        # exact message (see unit_screen.py's on_tree_node_selected for
        # the full explanation of why re-toggling here would be wrong).

    async def _on_catalog_tree_selected(self, data: object) -> None:
        if isinstance(data, Repository):
            self.app_state.repo = data
            return
        if isinstance(data, CatalogEntry):
            self.app_state.repo = data.repo
            self._selected_catalog = data
            self._selected_workload = None
            self._update_breadcrumb()
            # Cleared *before* the fetch starts (matching _clear_versions()
            # just below, and _on_workload_tree_selected's own identical
            # reasoning for column 3) — otherwise switching catalogs
            # leaves the previous catalog's workload tree on screen for
            # the whole fetch.
            self._reset_workloads_tree()
            self._clear_versions()
            self._load_workloads(data.repo, data.catalog)

    def _prompt_for_key(self, repo: Repository, catalog: Catalog) -> None:
        """Pushes the centered ``KeyDialog`` modal. Triggered by
        ``_load_workloads`` catching ``KeyRequiredError``/``KeyMismatchError`` from
        ``catalog.workloads()`` itself — the SDK's own gate
        (``api/repository.py``'s ``_require_key_verified``), not a
        client-side pre-check: attempting the call and catching the
        specific exception costs nothing extra over checking ``key_status``
        first (the gate itself is a no-I/O check, raised before any real
        catalog I/O), so there is no reason to duplicate that logic here
        too. Proceeds to the workload list on a verified key, otherwise
        leaves it blocked (empty column 2/3) — either way the repository label
        is refreshed to reflect whatever just became known about
        ``key_status``.
        """
        from synology_apm_repo.browser.screens.key_dialog import KeyDialog

        def on_dismiss(verified: bool | None) -> None:
            self._refresh_repo_labels()
            self._refresh_catalog_labels()
            if verified:
                self._reload_workloads_with_fresh_catalog(repo, catalog.catalog_id)

        self.app.push_screen(KeyDialog(repo), on_dismiss)

    # Async ``@work`` — ``Repository.set_key()`` (just run, successfully,
    # by the ``KeyDialog`` this resumes from) closes and replaces every
    # already-opened ``DedupRepo`` this repository holds -- not just the one
    # ``catalog_id`` names. For ``RepoKind.OBJECT_STORE``,
    # ``repo.catalogs()`` (column 1's own load, back when this repository node
    # was first expanded) already opened *every* sibling's own
    # ``DedupRepo`` eagerly, unkeyed, before any key was ever entered
    # (see its own docstring) -- so every sibling catalog leaf under this
    # repository node, not just the one that triggered ``KeyDialog``, is holding
    # a ``Catalog`` whose ``_dedup_repo`` ``set_key()`` just closed and
    # replaced. Refreshing only ``catalog_id`` would leave every other
    # sibling's leaf pointing at a closed ``DedupRepo`` -- invisible to
    # ``Repository.close()`` (which only ever walks whatever is *currently*
    # cached at each index) the moment that catalog is next selected and
    # happily reopens its own new, forever-untracked connection instead of
    # raising on the closed one. So this refreshes every catalog already
    # listed under this repository node via ``repo.catalog_by_id()`` (cheap: for
    # ``OBJECT_STORE`` each id skips straight to its own one matching
    # index, no full ``repo.catalogs()`` listing), then updates both
    # ``self._selected_catalog`` (read by ``_load_versions``) and each
    # leaf's own cached entry rather than leaving any of them pointing at
    # a closed connection.
    @work
    async def _reload_workloads_with_fresh_catalog(self, repo: Repository, catalog_id: CatalogId) -> None:
        tree = self.query_one("#col-catalogs", Tree)
        repo_node = next((node for node in tree.root.children if node.data is repo), None)
        sibling_ids = (
            [entry.catalog_id for entry in self._catalogs_by_repo_node[id(repo_node)]]
            if repo_node is not None and id(repo_node) in self._catalogs_by_repo_node
            else [catalog_id]
        )

        target: Catalog | None = None
        for sibling_id in sibling_ids:
            try:
                fresh = await repo.catalog_by_id(sibling_id)
            except ApmRepoError as exc:
                if sibling_id != catalog_id:
                    continue  # best-effort for a sibling -- its own leaf simply stays stale, see below
                # A narrow race (a repeat scan/refresh mid-flight found this
                # catalog newly corrupt) now surfaces as a raised error
                # instead of the "gone already" None below — reported the
                # same way, rather than left to propagate out of this worker.
                self._reset_workloads_tree()
                self.query_one("#col-workloads", Tree).root.add_leaf(f"error: {exc}")
                return
            if fresh is None:
                if sibling_id != catalog_id:
                    continue
                # The catalog verified a moment ago is gone already (a narrow
                # race — a repeat scan/refresh mid-flight) — report it the
                # same way _load_workloads' own ApmRepoError branch does,
                # rather than silently leaving column 2/3 on stale data with
                # no indication anything went wrong.
                self._reset_workloads_tree()
                self.query_one("#col-workloads", Tree).root.add_leaf(f"error: catalog {catalog_id!r} no longer found")
                return
            self._replace_cached_catalog(repo, sibling_id, fresh)
            if sibling_id == catalog_id:
                target = fresh
        if target is None:
            return
        self._selected_catalog = CatalogEntry(repo=repo, catalog=target)
        self._load_workloads(repo, target)

    def _replace_cached_catalog(self, repo: Repository, catalog_id: CatalogId, fresh: Catalog) -> None:
        """``self._selected_catalog`` isn't the only place this screen
        itself cached the old, now-closed ``Catalog`` — ``_catalogs_by_repo_node``
        (what a later ``/`` filter keystroke rebuilds column 1's leaves
        from, via ``_rerender_tree_filter``/``_add_catalog_children``) and
        the currently-rendered leaf's own ``TreeNode.data`` (what
        re-selecting that exact node reads, in ``_on_catalog_tree_selected``)
        both still point at it otherwise — re-selecting the same catalog
        later, or just filtering column 1, would reinstate a reference to
        a closed connection, whose next query raises a plain (non
        ``ApmRepoError``) error this screen doesn't catch, silently
        leaving the version list empty with no indication why.

        Every repository node is checked by ``repo_node.data is repo`` before its
        own catalog list is even considered — ``CatalogId`` falls back to
        ``str(connection_config_id)`` for a ``RepoKind.VAULT`` (whose
        ``repo_id`` is always ``None``, see ``resolve_catalog_id``), a
        per-repository local autoincrement id with no cross-repository
        uniqueness guarantee, and column 1 can hold several independently
        opened repositories at once (this module's own docstring). Matching by
        ``catalog_id`` alone, the way the sibling leaf-loop below already
        avoids by checking ``data.repo is repo``, would risk overwriting
        an unrelated repository's own cached ``Catalog`` with this one's fresh
        instance on a collision, and returning before ``repo``'s actual
        stale entry is ever reached."""
        tree = self.query_one("#col-catalogs", Tree)
        for repo_node in tree.root.children:
            if repo_node.data is not repo:
                continue
            catalogs = self._catalogs_by_repo_node.get(id(repo_node))
            if catalogs is None:
                continue
            for index, cat in enumerate(catalogs):
                if cat.catalog_id != catalog_id:
                    continue
                catalogs[index] = fresh
                for leaf in repo_node.children:
                    data = leaf.data
                    if isinstance(data, CatalogEntry) and data.repo is repo and data.catalog.catalog_id == catalog_id:
                        leaf.data = CatalogEntry(repo=repo, catalog=fresh)
                return

    # -- column 2: workload type -> workload ------------------------------

    def _reset_workloads_tree(self) -> None:
        tree = self.query_one("#col-workloads", Tree)
        tree.root.remove_children()
        tree.root.set_label("Workloads")
        self._workloads_by_group_node.clear()

    # Async ``@work`` (never ``thread=True``) — see browser/README.md. A
    # DebouncedProgress hint for the same reason _load_catalogs_for's
    # and _load_versions's own have one — workloads() is real I/O against
    # the repository's own db (S3/Azure-backed, per this module's own
    # docstring), so without it a slow call left column 2 sitting on
    # whatever it last showed for however long the fetch took, with
    # nothing distinguishing "still loading" from "genuinely empty".
    @work
    async def _load_workloads(self, repo: Repository, catalog: Catalog) -> None:
        with DebouncedProgress(self):
            try:
                workloads = await catalog.workloads()
            except (KeyRequiredError, KeyMismatchError):
                self._prompt_for_key(repo, catalog)
                return
            except ApmRepoError as exc:
                self._reset_workloads_tree()
                self.query_one("#col-workloads", Tree).root.add_leaf(f"error: {exc}")
                return
        self._set_workloads(workloads)

    def _add_sub_type_group(self, parent: TreeNode[object], group_workloads: list[Workload]) -> TreeNode[object]:
        """Adds one sub_type-level group node under ``parent`` (a device
        type-group directly under root, or a SaaS sub_type group under a
        tenant/domain node) and registers it into
        ``_workloads_by_group_node`` — filtering (``/``) keys off exactly
        this level, one above the workload leaves themselves, regardless
        of how deep it sits under root."""
        group_node = parent.add(_humanize_type(group_workloads[0].type_hint), data=group_workloads[0].type_hint)
        self._workloads_by_group_node[id(group_node)] = group_workloads
        self._add_workload_leaves(group_node, group_workloads)
        return group_node

    @staticmethod
    def _add_workload_leaves(node: TreeNode[object], workloads: list[Workload]) -> None:
        """Adds one disambiguated leaf per workload directly under
        ``node`` — shared by ``_add_sub_type_group`` (building a fresh
        group) and ``_rerender_tree_filter`` (re-populating an existing
        group node after a ``/`` filter), which independently rebuilt the
        identical ``disambiguate()`` + ``add_leaf()`` loop. ``workloads``
        are already one sub_type group's own siblings here (grouped by
        ``_add_sub_type_group``'s caller), so ``workload_pairs``'s own
        ``type_hint`` half would never actually differentiate anything at
        this level and is discarded."""
        pairs, _hints = workload_pairs(workloads)
        names = disambiguate(pairs)
        for workload, name in zip(workloads, names, strict=True):
            node.add_leaf(name, data=workload)

    def _set_workloads(self, workloads: list[Workload]) -> None:
        """Builds column 2's tree from ``_group_workloads``'s own pure
        grouping — this method is just the ``Tree``-rendering layer on
        top of it (widget construction, expand-to-first-match,
        cursor/focus), not where the actual grouping logic lives."""
        self._reset_workloads_tree()
        tree = self.query_one("#col-workloads", Tree)
        grouping = _group_workloads(workloads)

        first_group_node: TreeNode[object] | None = None

        for group_workloads in grouping.device_groups.values():
            group_node = self._add_sub_type_group(tree.root, group_workloads)
            if first_group_node is None:
                first_group_node = group_node

        for platform_type, tenant_groups in grouping.saas_groups.items():
            platform_node = tree.root.add(_humanize_type(platform_type), data=platform_type)
            for tenant_key, sub_groups in tenant_groups.items():
                tenant_node = platform_node.add(tenant_key, data=tenant_key)
                for group_workloads in sub_groups.values():
                    group_node = self._add_sub_type_group(tenant_node, group_workloads)
                    if first_group_node is None:
                        first_group_node = group_node

        if first_group_node is not None:
            # Expand the whole chain up to root, not just this node
            # itself — a SaaS sub_type group can now sit 3 levels below
            # root (platform -> tenant/domain -> sub_type), and Textual
            # only shows a node's children once every ancestor between
            # it and the tree's own root is individually expanded.
            ancestor: TreeNode[object] | None = first_group_node
            while ancestor is not None:
                ancestor.expand()
                ancestor = ancestor.parent
            if first_group_node.children:
                force_tree_line_cache(tree)
                tree.move_cursor(first_group_node.children[0])
        tree.focus()

    def _on_workload_tree_selected(self, data: object) -> None:
        if isinstance(data, Workload):
            self._selected_workload = data
            self._update_breadcrumb()
            # Cleared *before* the fetch starts (matching
            # _on_catalog_tree_selected's own _clear_versions() call), not
            # only in _set_versions() once it lands — otherwise switching
            # from a workload with versions to one still loading leaves the
            # previous workload's rows on screen for the whole fetch,
            # indistinguishable from them belonging to the new selection.
            self._clear_versions()
            self._load_versions(data)
        # A bare ``str`` here is a non-leaf node — a device/SaaS-sub_type
        # group, a SaaS platform header, or a tenant/domain group —
        # nothing further to do for any of them.

    # -- column 3: version (unchanged DataTable) --------------------------

    def _clear_versions(self) -> None:
        table = self.query_one("#col-versions", DataTable)
        table.clear()
        self._versions = []
        self._version_names = []
        self._visible_version_indices = []

    # Async ``@work`` (never ``thread=True``) — see browser/README.md. A
    # DebouncedProgress hint here for the same reason
    # _load_catalogs_for's own has one: without it, a slow versions()
    # call (a rotated-out workload still has to hit the store to find that
    # out) leaves column 3 blank for however long the fetch takes, with
    # nothing to tell "still loading" apart from "loaded, genuinely empty"
    # — exactly the ambiguity _render_versions()'s own empty-state row
    # exists to resolve for the *other* half of that same symptom.
    @work
    async def _load_versions(self, workload: Workload) -> None:
        with DebouncedProgress(self):
            # The selected catalog, not self.app_state.repo -- a repository can
            # hold several catalogs now, and app_state.repo alone doesn't
            # say which one workload actually came from.
            assert self._selected_catalog is not None
            versions = await self._selected_catalog.catalog.versions(workload)
        names = disambiguate(version_pairs(versions))
        self._set_versions(versions, names)

    def _set_versions(self, versions: list[Version], names: list[str]) -> None:
        self._versions = versions
        self._version_names = list(enumerate(names))
        self._render_versions()

    def _render_versions(self) -> None:
        table = self.query_one("#col-versions", DataTable)
        table.clear()
        needle = self._version_filter_text.lower() if self._version_filter_active else ""
        visible: list[int] = []
        for index, name in self._version_names:
            if needle and needle not in name.lower():
                continue
            table.add_row(name)
            visible.append(index)
        self._visible_version_indices = visible
        if not self._version_names and not self._version_filter_active:
            # A workload with every version rotated/expired out returns a
            # genuinely empty list from Repository.versions() (no error —
            # see catalog/version.py's own filtering) — without this row the table
            # is just blank, indistinguishable from a fetch still in
            # flight (see _load_versions's own DebouncedProgress for the
            # other half of that ambiguity). Not selectable: it's the only
            # row, so ``_visible_version_indices`` stays empty and
            # on_data_table_row_selected's own bounds check is already a
            # no-op against it.
            table.add_row(BROWSE_VERSIONS_EMPTY_LABEL)
        if visible and not self._version_filter_active:
            table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "col-versions":
            return
        if event.cursor_row >= len(self._visible_version_indices):  # pragma: no cover - defensive
            return
        original_index = self._visible_version_indices[event.cursor_row]
        self._open_version(self._versions[original_index])

    def _open_version(self, version: Version) -> None:
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        assert self._selected_catalog is not None
        self.app.push_screen(UnitScreen(self._selected_catalog.catalog, version))

    def action_go_back(self) -> None:
        if self._tree_filter_parent is not None:
            self._close_tree_filter()
            return
        if self._version_filter_active:
            self._close_version_filter()
            return
        if self.query_one("#goto-input", Input).has_class("active"):
            self._close_goto()
            return
        # BrowseScreen is the app's own root/base screen — pushed exactly
        # once, in ApmRepoBrowserApp.on_mount, and never popped by
        # anything else — so there's nowhere further to go "back" to.
        # Falling through to self.app.pop_screen() here would pop *this*
        # screen and reveal the App's own bare default screen underneath
        # (just Header/Footer, no content), turning Esc on the main
        # screen into a blank window. h/Esc are simply no-ops here.

    # No key-entry binding/action: entering a key is triggered
    # automatically — see ``_load_workloads``/``_prompt_for_key`` above.

    # -- filter (``/``) ------------------------------------------------
    #
    # Branches on which column currently has focus: a Tree (columns 1/2,
    # a tree-filter mirroring UnitScreen's) or the DataTable (column 3's
    # table-filter).

    def action_filter(self) -> None:
        focused = self.focused
        if isinstance(focused, Tree) and focused.id in ("col-catalogs", "col-workloads"):
            self._open_tree_filter(focused)
        elif isinstance(focused, DataTable) and focused.id == "col-versions":
            self._open_version_filter()

    def _open_tree_filter(self, tree: Tree[object]) -> None:
        parent = current_listing_tree_node(tree)
        cache = self._catalogs_by_repo_node if tree.id == "col-catalogs" else self._workloads_by_group_node
        if id(parent) not in cache:
            return  # this level has no cached children to filter (e.g. filtering the tree's own top container)
        self._tree_filter_parent = parent
        self._tree_filter_text = ""
        show_filter_input(self)

    def _open_version_filter(self) -> None:
        self._version_filter_active = True
        self._version_filter_text = ""
        show_filter_input(self)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "filter-input":
            return
        if self._tree_filter_parent is not None:
            self._tree_filter_text = event.value
            self._rerender_tree_filter(self._tree_filter_parent)
        elif self._version_filter_active:
            self._version_filter_text = event.value
            self._render_versions()

    def _rerender_tree_filter(self, parent: TreeNode[object]) -> None:
        needle = self._tree_filter_text.lower()
        is_catalogs = parent.tree.id == "col-catalogs"
        if is_catalogs:
            catalogs = self._catalogs_by_repo_node.get(id(parent), [])
            repo = parent.data
            assert isinstance(repo, Repository)  # guaranteed by _open_tree_filter's own cache-membership check
            parent.remove_children()
            visible_catalogs = [c for c in catalogs if not needle or needle in c.display_name.lower()]
            self._add_catalog_children(parent, repo, visible_catalogs)
        else:
            workloads = self._workloads_by_group_node.get(id(parent), [])
            parent.remove_children()
            visible_workloads = [w for w in workloads if not needle or needle in w.display_name.lower()]
            self._add_workload_leaves(parent, visible_workloads)

    def _close_tree_filter(self) -> None:
        parent = self._tree_filter_parent
        self._tree_filter_parent = None
        self._tree_filter_text = ""
        self.query_one("#filter-input", Input).remove_class("active")
        if parent is not None:
            self._rerender_tree_filter(parent)  # empty filter text -> full list restored

    def _close_version_filter(self) -> None:
        self._version_filter_active = False
        self._version_filter_text = ""
        self.query_one("#filter-input", Input).remove_class("active")
        self._render_versions()
        self.query_one("#col-versions", DataTable).focus()

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
        resolved = await resolve_goto_version(self.notify, repo, node_ref)
        if resolved is None:
            return
        catalog, version = resolved
        from synology_apm_repo.browser.screens.unit_screen import UnitScreen

        self.app.push_screen(UnitScreen(catalog, version, target_ref=node_ref))
