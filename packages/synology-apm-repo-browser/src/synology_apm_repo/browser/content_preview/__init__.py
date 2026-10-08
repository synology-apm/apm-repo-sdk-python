"""Plain-text previews of a unit's content bytes for ``UnitScreen``'s detail
pane, one module per content type: mail (``.eml``), a calendar event
(``.ics``), a contact (M365 CSV or GWS People-API JSON), a Teams/Chat
transcript (its exported HTML), and any other self-contained HTML, sniffed
from the bytes.

Pure functions with no I/O. ``render_html_preview`` never raises and
returns ``None`` for "no preview"; the other renderers can raise on
malformed bytes, which ``runtime/preview.py``'s ``load_preview`` catches.
``visible_site_fields`` instead filters a SharePoint List item's fields for
the List overview.
"""

from __future__ import annotations

from .calendar import render_calendar_event_preview
from .contact import render_contact_preview
from .html import render_html_preview
from .mail import render_mail_preview
from .site import visible_site_fields
from .teams_chat import render_teams_chat_preview

__all__ = [
    "render_calendar_event_preview",
    "render_contact_preview",
    "render_html_preview",
    "render_mail_preview",
    "render_teams_chat_preview",
    "visible_site_fields",
]
