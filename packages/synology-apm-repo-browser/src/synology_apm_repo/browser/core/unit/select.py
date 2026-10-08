"""Selectors over ``UnitModel``: the folder tree's ``NodeSpec``s (for
``view/reconcile.py``), the file table's columns and ``FileRow``s, and the
per-kind preview dispatch.

The folder tree shows containers only; every child, leaf or subfolder,
appears as a ``FileRow`` in its folder's file table. The whole tree, root
included, is keyed by ``NodeRef``.

**File-table columns** (``ColumnSpec``): the selected folder's
``Node.leaf_kind`` picks one column set per folder.

**Preview dispatch** (``preview_renderer_for``/``is_content_only_preview``/
``prefers_recent_content``): ``Node.kind`` picks a leaf's
``content_preview`` renderer, whether the detail pane's generic header is
shown, and which end of long content is read."""

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
from synology_apm_repo.browser.core.text_filter import matches_filter
from synology_apm_repo.browser.core.unit.model import FilterState, UnitModel
from synology_apm_repo.browser.view.reconcile import NodeSpec
from synology_apm_repo.sdk import (
    Node,
    NodeRef,
    NodeRole,
    UnitKind,
)
from synology_apm_repo.sdk.presentation import (
    FILE_STATE_ICON,
    diagnostic_suffix,
    file_state_suffix,
    format_bytes,
    format_timestamp,
    safe,
)

#: A file-table cell: a ``str``, which ``DataTable`` parses as Rich markup,
#: or a ``Text`` for formatting ``add_column`` can't express (the
#: right-aligned Size column).
CellValue = str | Text


def node_label(node: Node) -> str:
    """This node's tree/table-row label: its escaped name plus the
    file-state and diagnostic glyphs, shown outside verbose mode too."""
    return f"{safe(node.name)}{file_state_suffix(node.file_state.value)}{diagnostic_suffix(node.is_diagnostic)}"


def _is_container(node: Node) -> bool:
    """Whether ``node`` is expandable in the folder tree: a non-leaf other
    than a ``LIST_OVERVIEW`` node (its items show as one overview) or a
    ``FLAT_CATEGORY`` node (its children show as file-table rows only)."""
    return not node.is_leaf and node.role is not NodeRole.LIST_OVERVIEW and node.role is not NodeRole.FLAT_CATEGORY


def _needle_for(model: UnitModel, ref: NodeRef) -> str:
    filter_state: FilterState | None = model.filter
    if filter_state is not None and filter_state.ref == ref:
        return filter_state.text
    return ""


def error_leaf_ref(ref: NodeRef) -> NodeRef:
    """Key of the error leaf under ``ref`` after its first children fetch
    failed; no real child carries that segment."""
    return NodeRef(repo_path=ref.repo_path, segments=(*ref.segments, "__error__"))


def _folder_node_spec(model: UnitModel, node: Node) -> NodeSpec[NodeRef]:
    is_container = _is_container(node)
    children: tuple[NodeSpec[NodeRef], ...] | None = None
    if is_container:
        level = model.loaded.get(node.ref)
        if level is not None:
            needle = _needle_for(model, node.ref)
            children = tuple(
                _folder_node_spec(model, child)
                for child in level.children
                if not child.is_leaf and matches_filter(needle, child.name)
            )
        else:
            error = model.errors.get(node.ref)
            if error is not None:
                # Exception text is arbitrary: escape it.
                error_spec = NodeSpec(
                    key=error_leaf_ref(node.ref),
                    label=f"error: {safe(error)}",
                    payload=None,
                    allow_expand=False,
                )
                children = (error_spec,)
    return NodeSpec(key=node.ref, label=node_label(node), payload=node, allow_expand=is_container, children=children)


def folder_tree_spec(model: UnitModel) -> NodeSpec[NodeRef] | None:
    """The folder tree's root ``NodeSpec``, or ``None`` before the root
    loads."""
    if model.root is None:
        return None
    return _folder_node_spec(model, model.root)


def _modified_text(node: Node) -> str:
    dt = node.mtime
    return format_timestamp(dt) if dt is not None else ""


def _attr_text(value: str | None) -> str:
    # Backup content: escape it.
    return safe(value) if value else ""


def _attr_timestamp_text(value: datetime | None) -> str:
    return format_timestamp(value) if value is not None else ""


@dataclasses.dataclass(frozen=True, slots=True)
class FixedColumnWidth:
    """A column whose values have a bounded width (a fixed format or a
    closed vocabulary)."""

    cells: int


@dataclasses.dataclass(frozen=True, slots=True)
class FlexibleColumnWidth:
    """A column with open-ended values, sharing the width the fixed
    columns leave, by ``weight``."""

    weight: int = 1

    def __post_init__(self) -> None:
        if self.weight <= 0:
            raise ValueError(f"FlexibleColumnWidth.weight must be positive, got {self.weight!r}")


#: ``None`` keeps ``DataTable``'s plain content-driven auto width.
ColumnWidth = FixedColumnWidth | FlexibleColumnWidth | None


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One folder's file-table column headers and per-child cell values."""

    headers: tuple[str, ...]
    cells: Callable[[Node], tuple[CellValue, ...]]
    #: Leaf-only columns (Mail, Contact, Calendar Event): ``file_table_rows``
    #: drops the folder's non-leaf children.
    leaves_only: bool = False
    #: Width policy per header; empty means ``None`` for each.
    widths: tuple[ColumnWidth, ...] = ()

    def __post_init__(self) -> None:
        if not self.widths:
            object.__setattr__(self, "widths", (None,) * len(self.headers))
        elif len(self.widths) != len(self.headers):
            raise ValueError(f"ColumnSpec.widths must match headers 1:1 ({self.widths!r} vs {self.headers!r})")


def _bare_name(node: Node) -> str:
    """``node_label()`` without the file-state glyph, which
    ``_DEFAULT_COLUMN_SPEC`` shows in its own column."""
    return f"{safe(node.name)}{diagnostic_suffix(node.is_diagnostic)}"


def _default_cells(node: Node) -> tuple[CellValue, ...]:
    size_text = format_bytes(node.size) if node.size is not None else ""
    return (
        _bare_name(node),
        FILE_STATE_ICON[node.file_state.value],
        Text(size_text, justify="right"),
        _modified_text(node),
    )


#: ``format_bytes``'s longest rendering is ``"1024.0 MiB"`` (a value just
#: under a unit boundary rounds up), the same width as ``" 999.9 MiB"``.
#: ``format_timestamp``'s ``strftime`` pattern is always exactly 19
#: characters.
_SIZE_COLUMN_WIDTH = len(" 999.9 MiB")
_TIMESTAMP_COLUMN_WIDTH = len(" 2024-05-30 12:06:31")

#: The widest file-state glyph.
_FILE_STATE_COLUMN_WIDTH = max(cell_len(icon) for icon in FILE_STATE_ICON.values())

#: Every recurrence label is shorter than the header.
_RECURRENCE_COLUMN_WIDTH = cell_len("Recurrence")

#: A display cap, not a worst case: a longer name clips, leaving Email the
#: flexible width.
_CONTACT_FULL_NAME_COLUMN_WIDTH = 30

#: Every disk/device kind, Drive/OneDrive and Document Library items, and
#: the fallback for any other kind. The untitled second column holds the
#: file-state glyph, so it lines up; only disk filesystems produce a
#: non-normal ``file_state``, so the other specs keep it inline.
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

#: "Name, Created", for the containers ``_COLUMN_SPECS`` maps to it.
_NAME_CREATED_COLUMN_SPEC = ColumnSpec(
    headers=("Name", "Created"),
    cells=lambda node: (node_label(node), _modified_text(node)),
    widths=(FlexibleColumnWidth(), FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH)),
)

_COLUMN_SPECS: dict[UnitKind, ColumnSpec] = {
    UnitKind.MAIL: ColumnSpec(
        headers=("Sender", "Subject", "Date"),
        cells=lambda node: (_attr_text(node.columns.sender), node_label(node), _modified_text(node)),
        leaves_only=True,
        # Sender:Subject 1:3 -- a sender is typically a short name/email,
        # a subject often much longer.
        widths=(FlexibleColumnWidth(1), FlexibleColumnWidth(3), FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH)),
    ),
    UnitKind.CONTACT: ColumnSpec(
        headers=("Full Name", "Email"),
        cells=lambda node: (node_label(node), _attr_text(node.columns.email)),
        leaves_only=True,
        widths=(FixedColumnWidth(_CONTACT_FULL_NAME_COLUMN_WIDTH), FlexibleColumnWidth()),
    ),
    UnitKind.CALENDAR_EVENT: ColumnSpec(
        headers=("Title", "Start Time", "End Time", "Recurrence"),
        cells=lambda node: (
            node_label(node),
            _attr_timestamp_text(node.columns.event_start),
            _attr_timestamp_text(node.columns.event_end),
            _attr_text(node.columns.recurrence),
        ),
        leaves_only=True,
        widths=(
            FlexibleColumnWidth(),
            FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH),
            FixedColumnWidth(_TIMESTAMP_COLUMN_WIDTH),
            FixedColumnWidth(_RECURRENCE_COLUMN_WIDTH),
        ),
    ),
    # Containers with a creation time but no size: SharePoint's "List" and
    # Calendar's My/Other Calendars categories (CATEGORY_GROUP), and
    # Teams/Chat's containers.
    UnitKind.CATEGORY_GROUP: _NAME_CREATED_COLUMN_SPEC,
    UnitKind.TEAMS_CHAT_MESSAGE: _NAME_CREATED_COLUMN_SPEC,
}


def _column_spec_for(node: Node | None) -> ColumnSpec:
    kind = node.leaf_kind if node is not None else None
    if kind is None:
        return _DEFAULT_COLUMN_SPEC
    return _COLUMN_SPECS.get(kind, _DEFAULT_COLUMN_SPEC)


def column_headers_for(model: UnitModel, ref: NodeRef | None) -> tuple[str, ...]:
    """The file-table column headers for the folder ``ref`` names."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).headers


def column_widths_for(model: UnitModel, ref: NodeRef | None) -> tuple[ColumnWidth, ...]:
    """The width policy for each of ``column_headers_for``'s headers."""
    node = find_node_in_model(model, ref) if ref is not None else None
    return _column_spec_for(node).widths


@dataclasses.dataclass(frozen=True, slots=True)
class FileRow:
    """One file-table row; ``node`` is ``None`` only for the error row.
    ``cells`` match ``column_headers_for``."""

    node: Node | None
    cells: tuple[CellValue, ...]


def _error_row(message: str, num_columns: int) -> FileRow:
    """The error row: the message in the first of ``num_columns`` cells."""
    cells: tuple[CellValue, ...] = (f"error: {safe(message)}", *([""] * max(num_columns - 1, 0)))
    return FileRow(node=None, cells=cells)


def file_table_rows(model: UnitModel, ref: NodeRef | None) -> tuple[FileRow, ...]:
    """The file table's rows for ``ref``'s children, files and subfolders
    (files only under a ``leaves_only`` spec). Empty for a
    ``LIST_OVERVIEW`` node, whose items show in the detail pane."""
    if ref is None:
        return ()
    node = find_node_in_model(model, ref)
    if node is not None and (node.role is NodeRole.LIST_OVERVIEW):
        return ()
    spec = _column_spec_for(node)
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
        if not matches_filter(needle, child.name):
            continue
        rows.append(FileRow(node=child, cells=spec.cells(child)))
    return tuple(rows)


#: ``preview_renderer_for``'s per-kind dispatch; any other kind gets
#: ``render_html_preview``.
_PREVIEW_RENDERERS: dict[UnitKind, Callable[[bytes], str | None]] = {
    UnitKind.MAIL: render_mail_preview,
    UnitKind.CALENDAR_EVENT: render_calendar_event_preview,
    UnitKind.CONTACT: render_contact_preview,
    UnitKind.TEAMS_CHAT_MESSAGE: render_teams_chat_preview,
}

#: Kinds whose preview already states what the generic header would.
_CONTENT_ONLY_KINDS = frozenset(_PREVIEW_RENDERERS)


def is_content_only_preview(node: Node) -> bool:
    """Whether the detail pane drops its generic header for ``node``,
    whose preview already states the same."""
    return node.kind in _CONTENT_ONLY_KINDS


def preview_renderer_for(node: Node) -> Callable[[bytes], str | None]:
    """Which ``content_preview`` renderer applies to ``node``'s content bytes."""
    kind = node.kind
    return _PREVIEW_RENDERERS.get(kind, render_html_preview) if kind is not None else render_html_preview


def prefers_recent_content(node: Node) -> bool:
    """Whether ``load_preview`` reads the end of ``node``'s content rather
    than the start when it exceeds the read cap: true for a Teams/Chat
    transcript, whose newest messages come last."""
    return node.kind is UnitKind.TEAMS_CHAT_MESSAGE


def find_node_in_model(model: UnitModel, ref: NodeRef) -> Node | None:
    """``ref``'s ``Node`` among the loaded nodes, by ``model.node_index``
    (it runs in subscriptions on every dispatch)."""
    if model.root is not None and model.root.ref == ref:
        return model.root
    return model.node_index.get(ref)
