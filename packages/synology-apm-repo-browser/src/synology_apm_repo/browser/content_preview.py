"""Turns a restorable unit's raw content bytes into a short,
human-readable text preview for ``UnitScreen``'s detail pane: mail
(``.eml``, via the stdlib ``email`` package), a Calendar event (``.ics``,
via ``icalendar``), a Contact (M365's Outlook-compatible CSV or GWS's
raw People-API JSON), and anything self-contained HTML (e.g. Teams/
Chat's whole-channel export) — sniffed from the bytes, not from any
caller-supplied type flag.

Pure functions, no Textual/Widget imports, no I/O. ``render_html_preview``
is fully defensive — it never raises, returning ``None`` for "no preview
available" instead. ``render_mail_preview``/``render_calendar_event_preview``/
``render_contact_preview`` each parse a specific format and can raise on a
genuinely malformed unit's bytes; that's deliberately not wrapped in a
try/except here — ``unit_screen.py``'s ``_load_preview`` is the one place
a preview failure is caught, so one malformed unit still can't crash the
whole screen.

``visible_site_fields`` is the one export here that isn't bytes-in/text-out:
a SharePoint List item needs a filtered field *dict* for
``unit_screen.py``'s table builder, not pre-rendered text.
"""

from __future__ import annotations

import csv
import email
import io
import json
import re
from datetime import date, datetime
from email import policy
from html.parser import HTMLParser

import icalendar

from synology_apm_repo.sdk.presentation.format import format_bytes

#: Shared placeholder for a real field this specific unit genuinely
#: doesn't have a value for (an event with no location, a contact with
#: no email, ...) — never a raw internal id or a blank line standing in
#: for one. ``_NO_TITLE_LABEL``/``_NO_SUBJECT_LABEL`` exist separately so a
#: missing event title or mail subject never falls back to showing the
#: item's raw Graph id.
_NONE_LABEL = "(none)"
_NO_TITLE_LABEL = "(no title)"
_NO_SUBJECT_LABEL = "(no subject)"

# Block-level tags where a plain-text rendering wants a line break — this
# is a display heuristic, not an HTML-correctness parser: unknown/inline
# tags are simply dropped without affecting line breaks.
_BLOCK_TAGS = frozenset({"p", "div", "br", "h1", "h2", "h3", "h4", "tr", "li", "blockquote"})

_HTML_SNIFF_RE = re.compile(rb"<!doctype\s+html|<html[\s>]", re.IGNORECASE)
_HTML_SNIFF_WINDOW = 1024
"""How many leading bytes to check for an HTML doctype/tag — real pages
here (render_channel_html's own output) always open with
``<!doctype html>`` in the first ~40 bytes; a generous window just
tolerates a stray leading BOM/whitespace without reading the whole
(possibly large) buffer just to decide "is this HTML at all"."""

_TRUNCATION_NOTE = "\n\n[…preview truncated — press e to export the full item…]"


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + _TRUNCATION_NOTE


class _HTMLToText(HTMLParser):
    """Best-effort HTML -> plain text: drops all markup, keeps
    ``<style>``/``<script>`` contents out entirely, and inserts a line
    break at block-level tag boundaries so paragraphs/messages don't run
    together into one wall of text. Not a general-purpose HTML renderer
    — this is a *preview*, not a faithful re-layout."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("style", "script"):
            self._skip_depth += 1
        elif tag in _BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("style", "script"):
            self._skip_depth = max(0, self._skip_depth - 1)
        elif tag in _BLOCK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            self._parts.append(data)

    def text(self) -> str:
        raw = "".join(self._parts)
        # Collapse to at most one blank line between non-empty lines —
        # real HTML (this project's own render_channel_html included)
        # nests enough block tags per line of actual content to otherwise
        # produce mostly-blank output.
        lines = [line.strip() for line in raw.splitlines()]
        collapsed: list[str] = []
        for line in lines:
            if line or (collapsed and collapsed[-1]):
                collapsed.append(line)
        return "\n".join(collapsed).strip()


def _html_to_text(html_text: str) -> str:
    parser = _HTMLToText()
    parser.feed(html_text)
    parser.close()
    return parser.text()


_TRUNCATED_TAG_NAME_RE = re.compile(rb"<\s*([a-zA-Z][a-zA-Z0-9]*)")
# Labels for the one real case (an <img>'s own base64 payload — a Teams
# sticker) plus a generic fallback for any other tag this same
# truncation could in principle land inside.
_TRUNCATED_TAG_LABELS = {b"img": "image"}
_TRUNCATED_TAG_DEFAULT_LABEL = "content"


def _drop_trailing_unterminated_tag(data: bytes) -> tuple[bytes, str | None]:
    """``data`` is only ever a bounded prefix of a unit's real bytes
    (``UnitScreen``'s ``_PREVIEW_READ_LIMIT``, 256 KiB), never the whole
    thing — a long base64 attribute value (e.g. a Teams sticker's inline
    ``<img src="data:image/jpeg;base64,...">``) can span past that
    window, leaving ``data`` ending mid-attribute with no closing
    ``"``/``>`` anywhere in it. Drops back to the last real ``>`` in the
    window before ever handing bytes to ``HTMLParser``, so it never sees
    an unterminated tag.

    Returns:
        ``(trimmed_data, label)`` — ``label`` (e.g. ``"image"``) is
        ``None`` when nothing was dropped (the common case), so a real,
        truncated tag is never silently lost: the caller shows a
        placeholder built from it instead of the content just vanishing
        with no trace.
    """
    # CPython's HTMLParser.close() dumps an unterminated tag's whole
    # remaining buffer as literal text when it finds no closing ``>``/``<``
    # anywhere in it — guaranteed for base64, whose alphabet has neither
    # character — so raw base64 would otherwise fill the whole detail
    # pane. Dropping back to the last real ``>`` is safe by construction
    # for base64 (``>`` never appears in it); it merely bounds, rather
    # than eliminates, the residual risk for some other very-long
    # attribute value that happens to contain a literal ``>`` before its
    # own closing quote.
    safe_end = data.rfind(b">")
    if safe_end < 0:
        return data, None
    dropped = data[safe_end + 1 :]
    tag_match = _TRUNCATED_TAG_NAME_RE.match(dropped)
    if tag_match is None:
        return data, None  # trailing plain text/whitespace, not a truncated tag — nothing to flag
    label = _TRUNCATED_TAG_LABELS.get(tag_match.group(1).lower(), _TRUNCATED_TAG_DEFAULT_LABEL)
    return data[: safe_end + 1], label


def render_html_preview(data: bytes, *, max_chars: int = 4000) -> str | None:
    """``None`` if ``data`` doesn't look like a self-contained HTML
    document at all — the signal callers use to fall back to "no
    preview available" rather than treating non-HTML content as an
    error. Otherwise a plain-text rendering, truncated to ``max_chars``;
    ``data`` is first trimmed to end at its own last real ``>`` (see
    ``_drop_trailing_unterminated_tag``)."""
    probe = data[:_HTML_SNIFF_WINDOW].lstrip(b"\xef\xbb\xbf \t\r\n")
    if not _HTML_SNIFF_RE.match(probe):
        return None
    trimmed, dropped_label = _drop_trailing_unterminated_tag(data)
    text = _html_to_text(trimmed.decode("utf-8", errors="replace"))
    if dropped_label is not None:
        # ">=": the read window itself is size-capped, so a real
        # attribute this large may well continue past it — this is a
        # lower bound on what was cut, verified as such, never claimed
        # to be the attachment's real total size.
        placeholder = f"[{dropped_label}, ≥{format_bytes(len(data) - len(trimmed))}, not shown in preview]"
        text = f"{text}\n\n{placeholder}" if text else placeholder
    if not text:
        return None
    return _truncate(text, max_chars)


def render_mail_preview(data: bytes, *, max_chars: int = 4000) -> str:
    """Parse ``.eml`` bytes (RFC822/MIME — the shape
    ``units/saas/mail.py::build_eml`` always produces) into a
    From/To/Subject/Date header block plus a plain-text body — the
    ``text/html`` body case reuses ``_html_to_text`` rather than showing raw
    markup. From/To/Date are dropped when absent; Subject is never dropped
    — an empty/missing one shows ``_NO_SUBJECT_LABEL`` instead, the same
    always-shown-with-a-placeholder treatment ``render_calendar_event_preview``
    gives Title. Genuine parse failures propagate — deliberately not
    wrapped in a try/except here; see the module docstring for who catches
    them."""
    msg = email.message_from_bytes(data, policy=policy.default)
    header_lines = [f"{name}: {value}" for name in ("From", "To") if (value := msg.get(name))]
    header_lines.append(f"Subject: {msg.get('Subject') or _NO_SUBJECT_LABEL}")
    if date := msg.get("Date"):
        header_lines.append(f"Date: {date}")

    body_text = ""
    body_part = msg.get_body(preferencelist=("plain", "html"))
    if body_part is not None:
        content = body_part.get_content()
        if isinstance(content, str):
            body_text = _html_to_text(content) if body_part.get_content_type() == "text/html" else content

    text = "\n".join(header_lines)
    if body_text.strip():
        text = f"{text}\n\n{body_text}" if text else body_text
    return _truncate(text, max_chars)


def _format_when(when: date | datetime) -> str:
    return when.isoformat(sep=" ") if isinstance(when, datetime) else when.isoformat()


def render_calendar_event_preview(data: bytes) -> str:
    """Parse one exported ``.ics`` (a single ``VEVENT`` —
    ``units/saas/calendar.py::build_ics``'s own output shape) into
    Organizer/Title/Location/Start Time/End Time/Recurrence lines, one
    per line, always in that order. A field the real event genuinely
    doesn't have shows ``_NONE_LABEL`` (``_NO_TITLE_LABEL`` for Title
    specifically) — never dropped, never a raw internal id. Same
    let-it-propagate posture as ``render_mail_preview``."""
    cal = icalendar.Calendar.from_ical(data)
    events = cal.walk("VEVENT")
    if not events:
        return _NONE_LABEL
    event = events[0]

    organizer = event.get("organizer")
    organizer_text = str(organizer).removeprefix("mailto:") if organizer else _NONE_LABEL
    title = event.get("summary")
    title_text = str(title) if title else _NO_TITLE_LABEL
    location = event.get("location")
    location_text = str(location) if location else _NONE_LABEL
    dtstart = event.get("dtstart")
    start_text = _format_when(dtstart.dt) if dtstart is not None else _NONE_LABEL
    dtend = event.get("dtend")
    end_text = _format_when(dtend.dt) if dtend is not None else _NONE_LABEL
    rrule = event.get("rrule")
    recurrence_text = rrule.to_ical().decode("ascii") if rrule is not None else _NONE_LABEL

    return "\n".join(
        [
            f"Organizer: {organizer_text}",
            f"Title: {title_text}",
            f"Location: {location_text}",
            f"Start Time: {start_text}",
            f"End Time: {end_text}",
            f"Recurrence: {recurrence_text}",
        ]
    )


def _format_contact_summary(full_name: str, email_address: str) -> str:
    return f"Full Name: {full_name}\nEmail: {email_address}"


def _render_m365_contact_csv(data: bytes) -> str:
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig")))
    header = next(reader, [])
    row = next(reader, [])
    fields = dict(zip(header, row, strict=False))
    parts = [fields.get("First Name", ""), fields.get("Middle Name", ""), fields.get("Last Name", "")]
    full_name = " ".join(p for p in parts if p) or _NONE_LABEL
    email_address = fields.get("E-mail Address") or _NONE_LABEL
    return _format_contact_summary(full_name, email_address)


def _render_gws_contact_json(data: bytes) -> str:
    client_metadata = (json.loads(data).get("client_metadata")) or {}
    names = client_metadata.get("names") or []
    full_name = _NONE_LABEL
    if names and isinstance(names[0], dict):
        full_name = names[0].get("displayName") or _NONE_LABEL
    emails = client_metadata.get("emailAddresses") or []
    email_address = _NONE_LABEL
    if emails and isinstance(emails[0], dict):
        email_address = emails[0].get("value") or _NONE_LABEL
    return _format_contact_summary(full_name, email_address)


def visible_site_fields(values: dict[str, object]) -> dict[str, object]:
    """Drops SharePoint's own OData/id plumbing from one Site List
    item's field dict: any key starting with ``odata``/``OData``
    (a single case-insensitive prefix check catches both the
    lowercase-dotted REST convention, e.g. ``odata.type``, and the
    double-underscore "unspeakable property name" convention, e.g.
    ``OData__UIVersionString``), anything ending in ``Id``
    (case-sensitive, so a plain ``ID`` field survives), and ``GUID``.
    This is SharePoint's own protocol noise, not this project's internal
    repository identifiers, but a preview exists to be scannable for the same
    reason a repository's own ids stay hidden. Order is preserved (dict
    insertion order mirrors the SharePoint field's own order).

    Public (not ``_``-prefixed): a SharePoint List's own items are never
    individually browsable in the TUI's tree (see ``unit_screen.py``'s
    ``add_child_nodes``) — this is only ever called from
    ``unit_screen.py``'s own ``_load_list_overview``, which needs each
    item's *filtered field dict*, not pre-rendered text."""
    return {
        key: value
        for key, value in values.items()
        if not (key.lower().startswith("odata") or key.endswith("Id") or key == "GUID")
    }


def render_contact_preview(data: bytes) -> str:
    """Full Name + Email from one exported contact: CSV (M365,
    Outlook-compatible, UTF-8 BOM — ``units/saas/contact.py::
    build_contact_csv``'s own output shape) or raw JSON (GWS, Google
    People API ``client_metadata``'s ``names[0].displayName``/
    ``emailAddresses[0].value``). Which shape ``data`` actually is gets
    sniffed from the bytes (a leading UTF-8 BOM vs. a JSON object), the
    same "no caller-supplied flag" posture as ``render_html_preview``'s
    doctype sniff. Unlike that one, a genuinely malformed contact still
    propagates — same let-it-propagate posture as ``render_mail_preview``."""
    if data.startswith(b"\xef\xbb\xbf"):
        return _render_m365_contact_csv(data)
    return _render_gws_contact_json(data)
