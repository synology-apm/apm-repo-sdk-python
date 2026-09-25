"""Pure ``UnitModel`` -> ``NodeSpec``/``FileRow`` translation for
``view/reconcile.py`` (the folder tree) and ``UnitScreen``'s own file
table. The SharePoint-List-overview ``allow_expand=False`` decision is
purely presentational and lives here, not in ``update.py``, so a goto-ref
chain step's own exhaustive sibling list (``ChainStepResolved``) renders
identically to an ordinary ``provider.children()`` page (``ChildrenLoaded``)
without ``update()`` needing to know or care about the difference.

The folder tree only ever shows containers (a plain leaf, or a SharePoint
List-overview group's own items, never appear there) -- an ordinary leaf
and a subfolder alike only ever appear as a ``FileRow`` in whichever
folder's file table they belong to. The whole tree, root included, is
uniformly keyed by ``NodeRef`` and ``Binding``-wrapped (see
``view/reconcile.py``'s own ``Binding``) -- never a bare ``Node`` on the
root alone -- so ``reconcile_children`` can be called the same way at
every level with no special-cased root handling in the screen.

**Per-context file-table columns** (``ColumnSpec``/``column_headers_for``/
``file_table_rows``): the selected folder's own ``Node`` resolves which
column set applies, entirely via ``node_leaf_kind`` (set on every
container a ``SaasWorkloadProvider``-based provider builds, root and
every group alike, from that provider's own ``SaasWorkloadConfig.leaf_kind``
-- each provider's own ``group_attrs``/``leaf_kind`` can still override
that value for a specific container). This resolves per *folder*, not
per child row, and needs no per-child inspection or fallback for a
folder with zero children -- the folder's own ``Node`` always carries
the answer. A leaf-specific spec (Mail/Contact/Calendar Event) also
hides a selected folder's own non-leaf children from the file table,
since they're already reachable as ordinary tree nodes and have nothing
meaningful to show in those columns.

**Detail-pane preview dispatch** (``preview_renderer_for``/
``is_content_only_preview``/``prefers_recent_content``): the same
per-node/per-kind presentation split applies to which ``content_preview``
renderer a leaf's own content gets, whether ``DetailPane``'s generic
header is worth showing before it at all, and which end of a long
content stream ``UnitScreen._load_preview`` reads from when it exceeds
the read cap -- kept here rather than in ``unit_screen.py``/
``detail_pane.py`` for the same reason ``_column_spec_for`` is. All three
dispatch purely off ``Node.kind``, with no provider-specific ``attrs``
side-channel."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime

from rich.cells import cell_len
from rich.text import Text

from synology_apm_repo.browser.content_preview import (
    render_calendar_event_preview,
    render_contact_preview,
    render_html_preview,
    render_mail_preview,
    render_teams_chat_preview,
)
from synology_apm_repo.browser.core.unit.model import FilterState, UnitModel
from synology_apm_repo.browser.view.reconcile import NodeSpec
from synology_apm_repo.sdk.presentation.format import format_bytes, format_timestamp
from synology_apm_repo.sdk.presentation.icons import FILE_STATE_ICON, diagnostic_suffix, file_state_suffix
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import (
    Node,
    UnitKind,
    node_file_state,
    node_is_diagnostic,
    node_leaf_kind,
    node_modified_time,
)
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.site import is_flat_category, is_list_overview

#: A file-table cell's own value -- a plain string (re-parsed as Rich
#: markup by DataTable's default cell formatter, same as every other
#: string this module hands the tree/table) or a ``Text`` for a cell
#: needing real formatting Textual's ``add_column`` itself has no option
#: for (``_DEFAULT_COLUMN_SPEC``'s own right-aligned Size column).
CellValue = str | Text


def node_label(node: Node) -> str:
    """This node's own tree/table-row label, with the matching glyph
    appended when ``Node.attrs["file_state"]`` says the SDK believes
    it's a cloud-sync placeholder or EFS-encrypted, or when the node is
    a ``diagnostic_node()`` placeholder standing in for content the
    provider couldn't resolve. Shown in the default view, not gated
    behind verbose mode — user-facing backup-completeness information,
    not an internal identifier (ARCHITECTURE.md's Presentation section).
    ``node.name`` is real backup-derived content -- escaped via
    ``safe()`` before reaching either widget, both of which re-parse a
    plain ``str`` as Rich markup (``Tree.process_label``/``DataTable``'s
    own ``default_cell_formatter`` each call ``Text.from_markup``), so a
    literal ``[`` in it can't be misread as a markup tag."""
    return (
        f"{safe(node.name)}{file_state_suffix(node_file_state(node).value)}"
        f"{diagnostic_suffix(node_is_diagnostic(node))}"
    )


def _is_container(node: Node) -> bool:
    """A SharePoint List *group* node is genuinely ``is_leaf=False`` at
    the SDK level — it's not a single restorable unit — but its own
    items are deliberately never shown in this tree (the spreadsheet-
    style overview reads them directly instead), so it renders with no
    expand arrow despite not being a leaf either. The site root's own
    "List" *category* node (``is_flat_category``) gets the same
    treatment for a different reason: its own children (the site's
    individual Lists) are meant to be browsed as ordinary file-table
    rows, never as further tree nodes -- the same ``node_leaf_kind``-based
    per-folder column resolution ("Per-context file-table columns" above)
    already gives those rows the right columns without this tree builder
    needing to special-case them."""
    return not node.is_leaf and not is_list_overview(node) and not is_flat_category(node)


def _needle_for(model: UnitModel, ref: NodeRef) -> str:
    filter_state: FilterState | None = model.filter
    if filter_state is not None and filter_state.ref == ref:
        return filter_state.text.lower()
    return ""


def _matches_filter(needle: str, name: str) -> bool:
    """The one active-filter substring predicate this module's own
    folder tree and file table narrow their children by, against the
    same ``needle`` (``_needle_for``'s own return value) -- shared
    between those two call sites so a future change to filter matching
    (normalization, case-sensitivity, ...) can't be applied to one and
    missed in the other. ``core/browse/select.py`` inlines its own,
    independently-evolving copy of the identical shape for its catalog/
    workload trees -- not unified with this one, since the two screens'
    own filtering already diverge in every other respect (``FilterState``
    vs. ``_tree_needle``'s own per-tree-kind keying)."""
    return not needle or needle in name.lower()


def error_leaf_ref(ref: NodeRef) -> NodeRef:
    """Synthetic key for the one error leaf a failed initial children
    fetch renders under ``ref`` -- an extra segment no real child of
    ``ref`` would ever carry, so it can never collide with one."""
    return NodeRef(repo_path=ref.repo_path, segments=(*ref.segments, "__error__"))


def _folder_node_spec(model: UnitModel, node: Node) -> NodeSpec[NodeRef]:
    is_container = _is_container(node)
    children: tuple[NodeSpec[NodeRef], ...] | None = None
    if is_container:
        level = model.loaded.get(node.ref)
        if level is not None:
            # not child.is_leaf admits a SharePoint List-overview group
            # too (also is_leaf=False) -- an ordinary leaf is never
            # tree-shown at all, only ever appearing as a FileRow in
            # whichever folder's file table it belongs to.
            needle = _needle_for(model, node.ref)
            children = tuple(
                _folder_node_spec(model, child)
                for child in level.children
                if not child.is_leaf and _matches_filter(needle, child.name)
            )
        else:
            error = model.errors.get(node.ref)
            if error is not None:
                # error is an exception's own str() -- arbitrary,
                # provider-dependent text, escaped for the same reason
                # node_label() escapes node.name above.
                error_spec = NodeSpec(
                    key=error_leaf_ref(node.ref),
                    label=f"error: {safe(error)}",
                    payload=None,
                    allow_expand=False,
                )
                children = (error_spec,)
    return NodeSpec(key=node.ref, label=node_label(node), payload=node, allow_expand=is_container, children=children)


def folder_tree_spec(model: UnitModel) -> NodeSpec[NodeRef] | None:
    """``None`` before the root has ever loaded -- the screen leaves
    the tree's own root widget alone in that case (nothing to
    reconcile onto it yet)."""
    if model.root is None:
        return None
    return _folder_node_spec(model, model.root)


def _modified_text(node: Node) -> str:
    dt = node_modified_time(node)
    return format_timestamp(dt) if dt is not None else ""


def _attr_text(node: Node, key: str) -> str:
    # Real backup-derived content (Mail's sender, Contact's email) can
    # contain anything, including a literal "[" -- escaped for the same
    # reason node_label() escapes node.name: so it can't be misread as
    # Rich markup.
    value = node.attrs.get(key)
    return safe(value) if value else ""


def _attr_timestamp_text(node: Node, key: str) -> str:
    value = node.attrs.get(key)
    return format_timestamp(value) if isinstance(value, datetime) else ""


@dataclasses.dataclass(frozen=True)
class FixedColumnWidth:
    """A column whose real values are a fixed format or a bounded,
    enumerable vocabulary (a formatted size/timestamp, a closed glyph
    set, a short recurrence label, ...) -- never needs more room than
    its own longest possible rendering, so its width never needs to
    track what's currently on screen the way a flexible column's does."""

    cells: int


@dataclasses.dataclass(frozen=True)
class FlexibleColumnWidth:
    """A column whose real values are open-ended (a name, a subject
    line, an event title, ...) -- shares whatever width its own
    ``ColumnSpec``'s fixed-width columns don't already use, weighted
    against any sibling ``FlexibleColumnWidth`` in the same spec (every
    spec today has only one such column except Mail's Sender/Subject, so
    the weight's absolute value is inert everywhere but there)."""

    weight: int = 1

    def __post_init__(self) -> None:
        if self.weight <= 0:
            raise ValueError(f"FlexibleColumnWidth.weight must be positive, got {self.weight!r}")


#: ``None`` keeps ``DataTable``'s own plain content-driven auto width --
#: every ``ColumnSpec`` below opts into an explicit policy for every
#: column today, but omitting ``widths`` entirely (``ColumnSpec.widths``'s
#: own default) falls back to ``None`` for each header.
ColumnWidth = FixedColumnWidth | FlexibleColumnWidth | None


@dataclasses.dataclass(frozen=True)
class ColumnSpec:
    """One selected folder's own file-table column headers and per-child
    cell values -- resolved once per folder by ``_column_spec_for``, not
    per row: every row within one folder shares the same columns."""

    headers: tuple[str, ...]
    cells: Callable[[Node], tuple[CellValue, ...]]
    #: True for a leaf-only kind whose columns only make sense for a leaf
    #: (a mail's Sender/Subject/Date, an event's Title/Start/End/
    #: Recurrence, a contact's Full Name/Email) -- ``file_table_rows``
    #: drops a selected folder's own non-leaf children from the file
    #: table under this spec, since they're already reachable as
    #: ordinary tree nodes and have nothing meaningful to show in these
    #: columns. Only Mail's own subfolders and an M365 Contact's own
    #: folders ever exercise this drop today -- a Calendar Event leaf's
    #: own folder (one individual calendar) never has a non-leaf child to
    #: begin with, so ``True`` there is inert, not wrong. Left ``False``
    #: (the default) for every kind whose children can legitimately mix
    #: a subfolder with a leaf in the same file-table listing today
    #: (File/Drive/Document-Library items) and for
    #: ``UnitKind.CATEGORY_GROUP``/``TEAMS_CHAT_MESSAGE`` (a SharePoint
    #: List category's, a Calendar category's, and Teams/Chat's own
    #: children are exactly the non-leaf group/category nodes
    #: ``_NAME_CREATED_COLUMN_SPEC`` is meant to show, never filtered).
    leaves_only: bool = False
    #: Column width policy, positionally aligned with ``headers`` --
    #: normalized in ``__post_init__`` to always match ``headers``'s own
    #: length (``()`` at construction means "every column auto", same as
    #: leaving it off entirely). Living on the spec itself, not a
    #: header-text-keyed lookup in the TUI's own widget layer
    #: (``unit_file_table.py``), is deliberate: two different specs that
    #: happen to share a header string (a future kind's own "Date", say)
    #: can never accidentally inherit each other's width intent this way
    #: -- each spec's own ``widths`` only ever describes its own columns.
    widths: tuple[ColumnWidth, ...] = ()

    def __post_init__(self) -> None:
        if not self.widths:
            object.__setattr__(self, "widths", (None,) * len(self.headers))
        elif len(self.widths) != len(self.headers):
            raise ValueError(f"ColumnSpec.widths must match headers 1:1 ({self.widths!r} vs {self.headers!r})")


def _bare_name(node: Node) -> str:
    """``node_label()``'s own name+diagnostic-marker half, without the
    file-state glyph -- ``_default_cells``'s own Name cell uses this
    instead of ``node_label()`` directly, since its file-state glyph
    moves to its own dedicated column there."""
    return f"{safe(node.name)}{diagnostic_suffix(node_is_diagnostic(node))}"


def _default_cells(node: Node) -> tuple[CellValue, ...]:
    size_text = format_bytes(node.size) if node.size is not None else ""
    return (
        _bare_name(node),
        FILE_STATE_ICON[node_file_state(node).value],
        Text(size_text, justify="right"),
        _modified_text(node),
    )


#: A real value (``format_bytes``'s own longest possible rendering) needs
#: no more room than that -- the one extra leading space is deliberate
#: headroom: ``format_bytes``'s own ``f"{value:.1f}"`` rounds a value
#: just under a unit boundary up to that unit's next whole number
#: (``"1024.0 MiB"`` is one character longer than ``"999.9 MiB"``'s own
#: 9). For a timestamp column (Modified/Created/Date/Start Time/End
#: Time, all rendered by ``_modified_text``/``_attr_timestamp_text``),
#: ``format_timestamp``'s own ``strftime`` pattern is always exactly 19
#: characters, so the margin there is pure headroom.
_SIZE_COLUMN_WIDTH = len(" 999.9 MiB")
_TIMESTAMP_COLUMN_WIDTH = len(" 2024-05-30 12:06:31")

#: Computed from the real glyphs (mirrors ``widgets/fast_tree.py``'s own
#: ``_icon_collapsed_width``/``_icon_expanded_width`` pattern) rather
#: than a hardcoded number, so a future ``FileState`` addition with a
#: wider glyph widens this automatically. No safety margin needed --
#: unlike Size/timestamp columns' own open-ended formatted numbers, this
#: is an exhaustive enumeration of every value this column will ever hold.
_FILE_STATE_COLUMN_WIDTH = max(cell_len(icon) for icon in FILE_STATE_ICON.values())

#: The header itself, not any real value, is the binding constraint here:
#: ``sdk/units/saas/calendar.py``'s own ``_recurrence_label`` only ever
#: returns "", a capitalized ``M365_RECURRENCE_FREQ`` entry (Daily/
#: Weekly/Monthly/Yearly), or its own documented fallback "Recurring" for
#: an unrecognized pattern -- every one of those is shorter than
#: "Recurrence" itself. No safety margin needed, same reasoning as
#: ``_FILE_STATE_COLUMN_WIDTH``'s own exhaustive vocabulary.
_RECURRENCE_COLUMN_WIDTH = cell_len("Recurrence")

#: Unlike every other ``FixedColumnWidth`` above, a contact's own real
#: name has no format/vocabulary to derive a bound from -- this is a
#: deliberate display cap (fits the large majority of real full names
#: without truncation, leaving Email a comfortably wide flexible column
#: instead of both splitting an open-ended budget), not a measured
#: worst case. An unusually long name still just clips instead of
#: pushing Email around.
_CONTACT_FULL_NAME_COLUMN_WIDTH = 30

#: Every disk/device kind, Drive/OneDrive, and a SharePoint Document
#: Library's own items -- none of these carry a ``node_leaf_kind`` this
#: module recognizes below, so this is also the fallback for any
#: unlisted/unresolvable kind, not just File's own literal columns.
#:
#: The untitled second column holds only the cloud-sync/EFS-encrypted
#: glyph a bare Name column would otherwise carry inline (``node_label()``'s
#: own suffix) -- split out into its own fixed-width column so the glyph
#: lines up instead of shifting Size/Modified sideways by however long the
#: current row's own name happens to be. Only this spec gets the split:
#: ``file_state`` is currently produced only by the NTFS/APFS disk-fs
#: backends (``units/content/disk_fs/_ntfs.py``/``_apfs.py``), not a
#: concept every provider kind has, so every other ``ColumnSpec`` below
#: (Mail/Contact/Calendar Event/CategoryGroup/TeamsChatMessage, none
#: disk-backed) would only ever render this column empty -- those keep
#: ``node_label()``'s inline suffix instead of adding a column that could
#: never show anything.
_DEFAULT_COLUMN_SPEC = ColumnSpec(
    headers=("Name", "", "Size", "Modified"),
    cells=_default_cells,
    widths=(
        FlexibleColumnWidth(),
        FixedColumnWidth(_FILE_STATE_COLUMN_WIDTH),
        FixedColumnWidth(_SIZE_COLUMN_WIDTH),
        FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH),
    ),
)

#: "Name, Created" -- see ``_COLUMN_SPECS``' own ``CATEGORY_GROUP``/
#: ``TEAMS_CHAT_MESSAGE`` entries below for which containers share this.
_NAME_CREATED_COLUMN_SPEC = ColumnSpec(
    headers=("Name", "Created"),
    cells=lambda node: (node_label(node), _modified_text(node)),
    widths=(FlexibleColumnWidth(), FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH)),
)

_COLUMN_SPECS: dict[UnitKind, ColumnSpec] = {
    UnitKind.MAIL: ColumnSpec(
        headers=("Sender", "Subject", "Date"),
        cells=lambda node: (_attr_text(node, "sender"), node_label(node), _modified_text(node)),
        leaves_only=True,
        # Sender:Subject 1:3 -- a sender is typically a short name/email,
        # a subject often much longer.
        widths=(FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH)),
    ),
    UnitKind.CONTACT: ColumnSpec(
        headers=("Full Name", "Email"),
        cells=lambda node: (node_label(node), _attr_text(node, "email")),
        leaves_only=True,
        widths=(FixedColumnWidth(_CONTACT_FULL_NAME_COLUMN_WIDTH), FlexibleColumnWidth()),
    ),
    UnitKind.CALENDAR_EVENT: ColumnSpec(
        headers=("Title", "Start Time", "End Time", "Recurrence"),
        cells=lambda node: (
            node_label(node),
            _attr_timestamp_text(node, "event_start"),
            _attr_timestamp_text(node, "event_end"),
            _attr_text(node, "recurrence"),
        ),
        leaves_only=True,
        widths=(
            FlexibleColumnWidth(),
            FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH),
            FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH),
            FixedColumnWidth(_RECURRENCE_COLUMN_WIDTH),
        ),
    ),
    # Both kinds mark a container whose own children have a real creation
    # time but no size (the opposite of the default spec's own File-style
    # shape) -- a SharePoint site's "List" category (each child a List's
    # own group node) and a Calendar's My/Other Calendars category (each
    # child an individual calendar's own group node) share
    # UnitKind.CATEGORY_GROUP since the shape is identical; Teams/Chat's
    # own root/channel-category containers use UnitKind.TEAMS_CHAT_MESSAGE
    # instead, since that value has to double as both a leaf's own kind
    # and those containers' leaf_kind, and CATEGORY_GROUP is never a
    # leaf's own kind, only ever a container's leaf_kind. "Created" reuses
    # _modified_text against the
    # same attrs["mtime"] every other Modified/Date column in this module
    # reads -- each provider's own group_attrs/leaf-attrs construction is
    # what populates it from that container's own real creation-time
    # column, not an item's.
    UnitKind.CATEGORY_GROUP: _NAME_CREATED_COLUMN_SPEC,
    UnitKind.TEAMS_CHAT_MESSAGE: _NAME_CREATED_COLUMN_SPEC,
}


def _column_spec_for(node: Node | None) -> ColumnSpec:
    kind = node_leaf_kind(node) if node is not None else None
    if kind is None:
        return _DEFAULT_COLUMN_SPEC
    return _COLUMN_SPECS.get(kind, _DEFAULT_COLUMN_SPEC)


def column_headers_for(model: UnitModel, ref: NodeRef | None) -> tuple[str, ...]:
    """The file-table column headers for whichever folder ``ref`` names
    -- ``UnitScreen`` wires this straight into ``FileTableView.
    configure_columns`` as a ``Store`` subscription, always in lockstep
    with ``file_table_rows`` below and ``column_widths_for`` since all
    three resolve the same folder's ``ColumnSpec``."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).headers


def column_widths_for(model: UnitModel, ref: NodeRef | None) -> tuple[ColumnWidth, ...]:
    """``column_headers_for``'s own width-policy counterpart, positionally
    aligned with its return for the same folder (both resolve the same
    ``ColumnSpec``, so the two can never disagree) -- ``UnitScreen``
    applies this to the real ``DataTable`` widget via
    ``FileTableView.configure_column_widths``, registered immediately
    after ``configure_columns`` in the same notify batch."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).widths


@dataclasses.dataclass(frozen=True, slots=True)
class FileRow:
    """One file-table row's own domain data for the currently selected
    folder -- ``node`` is ``None`` only for the synthetic error row
    (mirrors ``error_leaf_ref``'s own tree-side convention: "``None``
    means synthetic" on both widgets). ``cells`` is already ordered to
    match ``column_headers_for``'s own return for the same folder --
    built by that same folder's ``ColumnSpec``, never per-row."""

    node: Node | None
    cells: tuple[CellValue, ...]


def _error_row(message: str, num_columns: int) -> FileRow:
    """The synthetic error row's own cells, padded to whatever column
    count the current folder's own ``ColumnSpec`` declares -- the real
    message always in the first cell, every other column blank."""
    cells: tuple[CellValue, ...] = (f"error: {safe(message)}", *([""] * max(num_columns - 1, 0)))
    return FileRow(node=None, cells=cells)


def file_table_rows(model: UnitModel, ref: NodeRef | None) -> tuple[FileRow, ...]:
    """Every row the file table shows for ``ref``'s own children -- both
    files and subfolders, unlike the folder tree, which only ever shows
    subfolders, except under a ``leaves_only`` spec, where a subfolder is
    dropped here too. A SharePoint
    List-overview node's own ``ref`` yields ``()`` unconditionally: its
    items are read directly by ``UnitScreen._load_list_overview`` into
    the detail pane, never listed here. Applies the same active-filter
    substring narrowing the folder tree uses (``_needle_for``), against
    the same ``FilterState.ref``."""
    if ref is None:
        return ()
    node = find_node_in_model(model, ref)
    if node is not None and is_list_overview(node):
        return ()
    spec = _column_spec_for(node)
    # loaded wins over a stale error entry, same ordering _folder_node_spec
    # uses -- update.py's own ChildrenLoaded case always clears errors[ref]
    # on success, so the two should never coexist for the same ref in
    # practice, but the selector stays defensively correct either way.
    level = model.loaded.get(ref)
    if level is None:
        error = model.errors.get(ref)
        if error is not None:
            return (_error_row(error, len(spec.headers)),)
        return ()
    needle = _needle_for(model, ref)
    rows: list[FileRow] = []
    for child in level.children:
        if spec.leaves_only and not child.is_leaf:
            continue
        if not _matches_filter(needle, child.name):
            continue
        rows.append(FileRow(node=child, cells=spec.cells(child)))
    return tuple(rows)


#: ``preview_renderer_for``'s per-kind dispatch -- a leaf ``UnitKind`` not
#: listed here (self-contained HTML included) falls back to
#: ``render_html_preview``.
_PREVIEW_RENDERERS: dict[UnitKind, Callable[[bytes], str | None]] = {
    UnitKind.MAIL: render_mail_preview,
    UnitKind.CALENDAR_EVENT: render_calendar_event_preview,
    UnitKind.CONTACT: render_contact_preview,
    UnitKind.TEAMS_CHAT_MESSAGE: render_teams_chat_preview,
}

#: Every kind whose own preview already states the identity a generic
#: header (Name/kind/size/modified) would otherwise just repeat -- every
#: key ``_PREVIEW_RENDERERS`` declares, since each of those renderers
#: exists for exactly this reason.
_CONTENT_ONLY_KINDS = frozenset(_PREVIEW_RENDERERS)


def is_content_only_preview(node: Node) -> bool:
    """Whether ``DetailPane``'s own header (Name/kind/size/modified, and
    -- outside verbose mode -- nothing else) should be dropped entirely
    for ``node``, so its detail pane starts directly at its own parsed
    preview's first line instead. True for every kind whose own preview
    already states the identity that generic header would otherwise just
    repeat -- every kind ``_PREVIEW_RENDERERS`` declares a renderer for."""
    return node.kind in _CONTENT_ONLY_KINDS


def preview_renderer_for(node: Node) -> Callable[[bytes], str | None]:
    """Which ``content_preview`` renderer applies to ``node``'s own
    content bytes -- ``UnitScreen._load_preview``'s own dispatch, moved
    here to keep every per-node/per-kind presentation decision in one
    module alongside ``_column_spec_for``."""
    kind = node.kind
    return _PREVIEW_RENDERERS.get(kind, render_html_preview) if kind is not None else render_html_preview


def prefers_recent_content(node: Node) -> bool:
    """Whether ``UnitScreen._load_preview`` should read ``node``'s own
    content from the *end* when it exceeds the per-preview read cap,
    not the start -- true only for a Teams/Chat message page
    (``UnitKind.TEAMS_CHAT_MESSAGE``), whose content is a chronological
    transcript (oldest message first): the newest messages are what a
    user actually wants visible, the same reason a real chat client
    opens scrolled to the bottom rather than the top. Every other kind
    keeps the existing head-first read -- an ordinary file/mail/event's
    own most useful content is at its start, not its end.

    Cheap to act on for a Teams/Chat page specifically even though the
    read is tail-anchored rather than starting at 0: its own
    ``LazyArtifact`` content source has already materialized the whole
    channel's HTML in memory by the time any ``read()`` happens, so the
    offset costs nothing beyond a cheap slice."""
    return node.kind is UnitKind.TEAMS_CHAT_MESSAGE


def find_node_in_model(model: UnitModel, ref: NodeRef) -> Node | None:
    """Looks up ``ref``'s own ``Node`` in the already-loaded domain tree
    (``model.root``/``model.node_index``) -- never the widget. An O(1)
    lookup, not a walk: ``model.node_index`` is kept in sync with
    ``model.loaded`` by every fetch-result case in ``update.py``,
    specifically because this function backs two ``Store``
    subscriptions (``column_headers_for``/``file_table_rows``, below),
    which run on *every* dispatch, not just a folder switch -- a linear
    scan here would rescan the whole session's loaded nodes on every
    unrelated background fetch too. Used directly by
    ``UnitScreen.action_load_more`` instead of reading the folder tree's
    own cursor, since the file table can move ``model.selected`` one step
    ahead of the tree's own cursor-sync. ``action_filter`` doesn't need
    this: it only checks whether ``model.selected`` has a loaded level at
    all (``model.loaded.get(ref)``), never the ``Node`` itself."""
    if model.root is not None and model.root.ref == ref:
        return model.root
    return model.node_index.get(ref)
