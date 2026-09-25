"""Shared error/warning display helpers."""

from __future__ import annotations

from typing import Any

from textual.screen import Screen
from textual.widgets import Static

from synology_apm_repo.sdk.presentation.markup import safe


def show_error(screen: Screen[Any], widget_id: str, message: object) -> None:
    """Write ``[red]error:[/red] {message}`` into ``screen``'s
    ``widget_id`` ``Static`` — shared by ``DiagnosticsScreen``/
    ``UnitScreen`` (``#diag-status`` and ``#detail`` respectively) and
    ``KeyDialog``/``ConnectDialog``.
    ``screen: Screen[Any]``, not ``Screen[None]``: a ``ModalScreen``
    (``KeyDialog``/``ConnectDialog``) is generic over its own dismiss
    result, not ``None`` — this function only ever calls ``query_one``
    on it, so the dismiss-result type is irrelevant here. Deliberately
    not the *only* way an error reaches a screen: ``notify`` toasts,
    tree-leaf errors, and ``DetailPane.append_list_overview_error``'s
    own list-overview format are distinct UI contexts on purpose (see
    ``sdk/presentation/markup.py``) and stay separate from this."""
    screen.query_one(widget_id, Static).update(f"[red]error:[/red] {safe(message)}")


def notify_warning(screen: Screen[Any], exc: BaseException) -> None:
    """``self.notify(str(exc), severity="warning")`` -- shared by
    ``KeyDialog``/``UnitScreen`` (several call sites) for "degrade to a
    toast rather than crash" exception handling."""
    screen.notify(str(exc), severity="warning")
