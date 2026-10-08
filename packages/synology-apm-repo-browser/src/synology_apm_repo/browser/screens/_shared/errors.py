"""Shared error/warning display helpers."""

from __future__ import annotations

from typing import Any

from textual.screen import Screen
from textual.widgets import Static

from synology_apm_repo.sdk.presentation import safe


def show_error(screen: Screen[Any], widget_id: str, message: object) -> None:
    """Write ``[red]error:[/red] {message}`` into ``screen``'s ``widget_id``
    ``Static``, escaping ``message``."""
    screen.query_one(widget_id, Static).update(f"[red]error:[/red] {safe(message)}")


def notify_warning(screen: Screen[Any], exc: BaseException) -> None:
    """Toast ``str(exc)`` as a warning."""
    screen.notify(str(exc), severity="warning")
