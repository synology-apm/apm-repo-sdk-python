"""Pure ``UnitModel`` -> ``NodeSpec``/``FileRow`` translation for
``view/reconcile.py`` (the folder tree) and ``UnitScreen``'s file table.

The folder tree only ever shows containers -- an ordinary leaf and a
subfolder alike only ever appear as a ``FileRow`` in whichever folder's
file table they belong to. The whole tree, root included, is uniformly
keyed by ``NodeRef`` and ``Binding``-wrapped, so ``reconcile_children``
needs no special-cased root handling.

**Per-context file-table columns** (``ColumnSpec``/``column_headers_for``/
``file_table_rows``): the selected folder's ``Node`` resolves which column
set applies via ``node_leaf_kind``, resolved per folder, not per child
row. A leaf-specific spec (Mail/Contact/Calendar Event) also hides a
folder's non-leaf children from the file table.

**Detail-pane preview dispatch** (``preview_renderer_for``/
``is_content_only_preview``/``prefers_recent_content``): the same
per-kind dispatch decides which ``content_preview`` renderer a leaf's
content gets, whether ``DetailPane``'s generic header is worth showing,
and which end of a long content stream to read from -- all off
``Node.kind`` alone."""

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
    """This node's tree/table-row label, with the matching glyph appended
    for a cloud-sync placeholder/EFS-encrypted state or a diagnostic
    placeholder. Shown in the default view, not gated behind verbose mode.
    ``node.name`` is real backup-derived content, escaped via ``safe()``
    since both widgets re-parse a plain ``str`` as Rich markup."""
    return (
        f"{safe(node.name)}{file_state_suffix(node_file_state(node).value)}"
        f"{diagnostic_suffix(node_is_diagnostic(node))}"
    )


def _is_container(node: Node) -> bool:
    """A SharePoint List group node is ``is_leaf=False`` at the SDK level
    but its items are never shown in this tree (the spreadsheet-style
    overview reads them directly), so it renders with no expand arrow. A
    site's "List" category node (``is_flat_category``) gets the same
    treatment: its children are meant to be browsed as file-table rows,
    never further tree nodes."""
    return not node.is_leaf and not is_list_overview(node) and not is_flat_category(node)


def _needle_for(model: UnitModel, ref: NodeRef) -> str:
    filter_state: FilterState | None = model.filter
    if filter_state is not None and filter_state.ref == ref:
        return filter_state.text.lower()
    return ""


def _matches_filter(needle: str, name: str) -> bool:
    """The active-filter substring predicate the folder tree and file
    table both narrow children by, against ``_needle_for``'s ``needle``."""
    return not needle or needle in name.lower()


def error_leaf_ref(ref: NodeRef) -> NodeRef:
    """Synthetic key for the error leaf a failed initial children fetch
    renders under ``ref`` -- a segment no real child would carry."""
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
    enumerable vocabulary -- never needs more room than its longest
    possible rendering."""

    cells: int


@dataclasses.dataclass(frozen=True)
class FlexibleColumnWidth:
    """A column whose real values are open-ended -- shares whatever width
    its ``ColumnSpec``'s fixed-width columns don't already use, weighted
    against any sibling ``FlexibleColumnWidth`` in the same spec."""

    weight: int = 1

    def __post_init__(self) -> None:
        if self.weight <= 0:
            raise ValueError(f"FlexibleColumnWidth.weight must be positive, got {self.weight!r}")


#: ``None`` keeps ``DataTable``'s plain content-driven auto width.
ColumnWidth = FixedColumnWidth | FlexibleColumnWidth | None


@dataclasses.dataclass(frozen=True)
class ColumnSpec:
    """One selected folder's file-table column headers and per-child cell
    values -- resolved once per folder by ``_column_spec_for``, not per row."""

    headers: tuple[str, ...]
    cells: Callable[[Node], tuple[CellValue, ...]]
    #: True for a leaf-only kind (Mail's Sender/Subject/Date, an event's
    #: Title/Start/End/Recurrence, a contact's Full Name/Email) --
    #: ``file_table_rows`` drops a folder's non-leaf children under this
    #: spec, since they have nothing meaningful to show in these columns.
    leaves_only: bool = False
    #: Column width policy, positionally aligned with ``headers`` --
    #: normalized in ``__post_init__`` to match ``headers``'s length.
    #: Lives on the spec itself, not a header-text-keyed lookup, so two
    #: specs sharing a header string never inherit each other's width.
    widths: tuple[ColumnWidth, ...] = ()

    def __post_init__(self) -> None:
        if not self.widths:
            object.__setattr__(self, "widths", (None,) * len(self.headers))
        elif len(self.widths) != len(self.headers):
            raise ValueError(f"ColumnSpec.widths must match headers 1:1 ({self.widths!r} vs {self.headers!r})")


def _bare_name(node: Node) -> str:
    """``node_label()``'s name+diagnostic-marker half, without the
    file-state glyph -- used for ``_default_cells``'s Name cell, since the
    glyph moves to its own dedicated column there."""
    return f"{safe(node.name)}{diagnostic_suffix(node_is_diagnostic(node))}"


def _default_cells(node: Node) -> tuple[CellValue, ...]:
    size_text = format_bytes(node.size) if node.size is not None else ""
    return (
        _bare_name(node),
        FILE_STATE_ICON[node_file_state(node).value],
        Text(size_text, justify="right"),
        _modified_text(node),
    )


#: A real value (``format_bytes``'s longest rendering) needs no more room
#: than that plus one leading space: a value just under a unit boundary
#: rounds up to the next whole number (``"1024.0 MiB"`` is one char
#: longer than ``"999.9 MiB"``). ``format_timestamp``'s ``strftime``
#: pattern is always exactly 19 characters.
_SIZE_COLUMN_WIDTH = len(" 999.9 MiB")
_TIMESTAMP_COLUMN_WIDTH = len(" 2024-05-30 12:06:31")

#: Computed from the real glyphs so a future ``FileState`` addition with
#: a wider glyph widens this automatically. No safety margin needed: an
#: exhaustive enumeration of every value this column will ever hold.
_FILE_STATE_COLUMN_WIDTH = max(cell_len(icon) for icon in FILE_STATE_ICON.values())

#: The header itself is the binding constraint: every real recurrence
#: label ("", Daily/Weekly/Monthly/Yearly, or "Recurring") is shorter
#: than "Recurrence" itself. No safety margin needed.
_RECURRENCE_COLUMN_WIDTH = cell_len("Recurrence")

#: A deliberate display cap, not a measured worst case: fits the large
#: majority of real full names, leaving Email a comfortably wide flexible
#: column. An unusually long name just clips instead of pushing Email around.
_CONTACT_FULL_NAME_COLUMN_WIDTH = 30

#: Every disk/device kind, Drive/OneDrive, and Document Library items;
#: also the fallback for any unlisted/unresolvable kind.
#:
#: The untitled second column holds only the cloud-sync/EFS-encrypted
#: glyph a bare Name column would otherwise carry inline, split out so the
#: glyph lines up instead of shifting Size/Modified sideways. Only this
#: spec gets the split: ``file_state`` is produced only by the NTFS/APFS
#: disk-fs backends, so every other spec below would render it empty and
#: keeps ``node_label()``'s inline suffix instead.
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
    # Both kinds mark a container with a real creation time but no size:
    # SharePoint's "List" category and a Calendar's My/Other Calendars
    # category share CATEGORY_GROUP; Teams/Chat's containers use
    # TEAMS_CHAT_MESSAGE instead, since CATEGORY_GROUP is never a leaf's
    # own kind, only ever a container's leaf_kind.
    UnitKind.CATEGORY_GROUP: _NAME_CREATED_COLUMN_SPEC,
    UnitKind.TEAMS_CHAT_MESSAGE: _NAME_CREATED_COLUMN_SPEC,
}


def _column_spec_for(node: Node | None) -> ColumnSpec:
    kind = node_leaf_kind(node) if node is not None else None
    if kind is None:
        return _DEFAULT_COLUMN_SPEC
    return _COLUMN_SPECS.get(kind, _DEFAULT_COLUMN_SPEC)


def column_headers_for(model: UnitModel, ref: NodeRef | None) -> tuple[str, ...]:
    """The file-table column headers for the folder ``ref`` names --
    always in lockstep with ``file_table_rows``/``column_widths_for``,
    since all three resolve the same folder's ``ColumnSpec``."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).headers


def column_widths_for(model: UnitModel, ref: NodeRef | None) -> tuple[ColumnWidth, ...]:
    """``column_headers_for``'s width-policy counterpart, positionally
    aligned with its return for the same folder."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).widths


@dataclasses.dataclass(frozen=True, slots=True)
class FileRow:
    """One file-table row's domain data for the selected folder --
    ``node`` is ``None`` only for the synthetic error row. ``cells`` is
    already ordered to match ``column_headers_for``'s return for the
    same folder."""

    node: Node | None
    cells: tuple[CellValue, ...]


def _error_row(message: str, num_columns: int) -> FileRow:
    """The synthetic error row's cells, padded to the current folder's
    ``ColumnSpec`` column count -- the message in the first cell, every
    other column blank."""
    cells: tuple[CellValue, ...] = (f"error: {safe(message)}", *([""] * max(num_columns - 1, 0)))
    return FileRow(node=None, cells=cells)


def file_table_rows(model: UnitModel, ref: NodeRef | None) -> tuple[FileRow, ...]:
    """Every row the file table shows for ``ref``'s children -- both
    files and subfolders, unlike the folder tree, except under a
    ``leaves_only`` spec, where a subfolder is dropped here too. A
    SharePoint List-overview node's ``ref`` yields ``()`` unconditionally:
    its items are read directly into the detail pane, never listed here."""
    if ref is None:
        return ()
    node = find_node_in_model(model, ref)
    if node is not None and is_list_overview(node):
        return ()
    spec = _column_spec_for(node)
    # loaded wins over a stale error entry -- update.py's ChildrenLoaded
    # clears errors[ref] on success, but the selector stays defensively
    # correct either way.
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
    """Whether ``DetailPane``'s generic header should be dropped entirely
    for ``node``, so its detail pane starts directly at its parsed
    preview's first line -- true for every kind whose preview already
    states the identity that header would otherwise repeat."""
    return node.kind in _CONTENT_ONLY_KINDS


def preview_renderer_for(node: Node) -> Callable[[bytes], str | None]:
    """Which ``content_preview`` renderer applies to ``node``'s content bytes."""
    kind = node.kind
    return _PREVIEW_RENDERERS.get(kind, render_html_preview) if kind is not None else render_html_preview


def prefers_recent_content(node: Node) -> bool:
    """Whether ``UnitScreen._load_preview`` should read ``node``'s content
    from the end rather than the start when it exceeds the read cap --
    true only for a Teams/Chat message page, whose chronological
    transcript makes the newest messages the useful ones. Cheap to act on
    there specifically: its ``LazyArtifact`` content source has already
    materialized the whole channel's HTML in memory by the time any
    ``read()`` happens."""
    return node.kind is UnitKind.TEAMS_CHAT_MESSAGE


def find_node_in_model(model: UnitModel, ref: NodeRef) -> Node | None:
    """Looks up ``ref``'s ``Node`` in the already-loaded domain tree, never
    the widget. An O(1) lookup: ``model.node_index`` is kept in sync with
    ``model.loaded`` since this backs two ``Store`` subscriptions that run
    on every dispatch, not just a folder switch."""
    if model.root is not None and model.root.ref == ref:
        return model.root
    return model.node_index.get(ref)
