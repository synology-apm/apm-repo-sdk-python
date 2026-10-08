"""``detail_view``: what ``UnitScreen``'s detail pane shows -- the selected
node's header (``header_text``), the body under it, and whether the pane
needs the wide layout. ``DetailPane`` turns a ``DetailView`` into widget
text."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from synology_apm_repo.browser.core.unit.model import DetailBody, DetailError, DetailIdle
from synology_apm_repo.browser.core.unit.select import is_content_only_preview
from synology_apm_repo.sdk import (
    FileState,
    Node,
    NodeRole,
    node_kind_label,
)
from synology_apm_repo.sdk.presentation import format_bytes, format_timestamp, safe

if TYPE_CHECKING:
    from synology_apm_repo.browser.core.unit.model import UnitModel


@dataclasses.dataclass(frozen=True, slots=True)
class DetailView:
    """``node`` is ``None`` when there is no selection (or a root-load
    failure is being shown in ``body``)."""

    node: Node | None
    body: DetailBody
    verbose: bool

    @property
    def wide(self) -> bool:
        """A List overview is a pre-rendered table that must pan
        horizontally instead of wrapping."""
        return self.node is not None and not self.node.is_leaf and self.node.role is NodeRole.LIST_OVERVIEW


def detail_view(model: UnitModel) -> DetailView:
    if model.root_error is not None:
        return DetailView(node=None, body=DetailError(model.root_error), verbose=model.verbose)
    if model.detail is None:
        return DetailView(node=None, body=DetailIdle(), verbose=model.verbose)
    return DetailView(node=model.detail.node, body=model.detail.body, verbose=model.verbose)


def header_text(node: Node, *, verbose: bool) -> str:
    """The detail pane's header for ``node``. Backup content (name,
    diagnostic, degraded, ``details``) is escaped for markup; ``node.ref`` is
    percent-encoded (never contains ``[]``) and left as is."""
    if is_content_only_preview(node):
        # Mail/calendar/contact/Teams-chat previews already state their own
        # identity, so a generic header would repeat it. Verbose mode still
        # gets ref/details, just without the redundant header lines.
        if not verbose:
            return ""
        return "\n".join(_verbose_lines(node))
    lines = [f"[b]{safe(node.name)}[/b]", f"kind: {node_kind_label(node)}"]
    if node.size is not None:
        size_line = f"size: {format_bytes(node.size)}"
        if node.file_state is FileState.CLOUD_ONLY:
            # A cloud-sync placeholder's size is declared, not on disk.
            size_line += " (0 Byte on disk)"
        lines.append(size_line)
    modified = node.mtime
    if modified is not None:
        # Shown outside verbose mode too, for when the file table isn't in
        # view (e.g. a goto landing on a leaf).
        lines.append(f"modified: {format_timestamp(modified)}")
    if node.degraded is not None:
        lines.append(f"note: {safe(node.degraded)}")
    if verbose:
        lines.extend(_verbose_lines(node))
    return "\n".join(lines)


def _verbose_lines(node: Node) -> list[str]:
    """``ref``, a placeholder's ``diagnostic`` reason, every set ``columns``
    field and every ``details`` entry, escaped like the rest of the header."""
    lines = [f"ref: {node.ref}"]
    if node.diagnostic is not None:
        lines.append(f"diagnostic: {safe(node.diagnostic)}")
    columns = node.columns
    lines.extend(
        f"{field.name}: {safe(value)}"
        for field in dataclasses.fields(columns)
        if (value := getattr(columns, field.name)) is not None
    )
    lines.extend(f"{safe(key)}: {safe(value)}" for key, value in node.details.items())
    return lines
