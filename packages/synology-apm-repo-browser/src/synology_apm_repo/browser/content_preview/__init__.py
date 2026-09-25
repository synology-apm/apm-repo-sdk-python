"""Turns a restorable unit's raw content bytes into a short,
human-readable text preview for ``UnitScreen``'s detail pane: mail
(``.eml``, via the stdlib ``email`` package), a Calendar event (``.ics``,
via ``icalendar``), a Contact (M365's Outlook-compatible CSV or GWS's
raw People-API JSON), a Teams/Chat channel/chat (its exported HTML page,
parsed back into a chat-transcript-style rendering), and any other
self-contained HTML — sniffed from the bytes, not from any
caller-supplied type flag.

Pure functions, no Textual/Widget imports, no I/O. ``render_html_preview``
is fully defensive — it never raises, returning ``None`` for "no preview
available" instead. ``render_mail_preview``/``render_calendar_event_preview``/
``render_contact_preview``/``render_teams_chat_preview`` each parse a
specific format and can raise on a genuinely malformed unit's bytes;
that's deliberately not wrapped in a try/except here —
``core/unit/select.py``'s ``preview_renderer_for`` picks which of these
applies to a given node, and ``unit_screen.py``'s ``_load_preview`` is
the one place a preview failure is caught, so one malformed unit still
can't crash the whole screen.

``visible_site_fields`` is the one export here that isn't bytes-in/text-out:
a SharePoint List item needs a filtered field *dict* for
``unit_screen.py``'s table builder, not pre-rendered text.

Split by content type into sibling modules (``html.py``, ``mail.py``,
``calendar.py``, ``contact.py``, ``teams_chat.py``, ``site.py``), with
``_common.py`` holding the truncation/HTML-to-text plumbing more than one
of them builds on.
"""

from __future__ import annotations

from ._common import _drop_trailing_unterminated_tag
from .calendar import render_calendar_event_preview
from .contact import render_contact_preview
from .html import render_html_preview
from .mail import render_mail_preview
from .site import visible_site_fields
from .teams_chat import render_teams_chat_preview

__all__ = [
    "_drop_trailing_unterminated_tag",
    "render_calendar_event_preview",
    "render_contact_preview",
    "render_html_preview",
    "render_mail_preview",
    "render_teams_chat_preview",
    "visible_site_fields",
]
