"""Shared plumbing used by more than one content-type preview renderer —
truncation, HTML-to-text, and the truncated-trailing-tag safety net.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser

from synology_apm_repo.sdk.presentation.format import format_bytes

#: Shared placeholder for a real field this specific unit genuinely
#: doesn't have a value for (an event with no location, a contact with
#: no email, ...) — never a raw internal id or a blank line standing in
#: for one. ``_NO_TITLE_LABEL``/``_NO_SUBJECT_LABEL`` exist separately so a
#: missing event title or mail subject never falls back to showing the
#: item's raw Graph id.
_NONE_LABEL = "(none)"

# Block-level tags where a plain-text rendering wants a line break — this
# is a display heuristic, not an HTML-correctness parser: unknown/inline
# tags are simply dropped without affecting line breaks.
_BLOCK_TAGS = frozenset({"p", "div", "br", "h1", "h2", "h3", "h4", "tr", "li", "blockquote"})

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
        return _collapse_blank_lines("".join(self._parts))


def _collapse_blank_lines(raw: str) -> str:
    """Strips each line, then collapses to at most one blank line between
    non-empty ones and drops any leading/trailing blank lines entirely —
    shared by ``_HTMLToText`` and ``_ChannelTranscriptParser`` (both parse
    real HTML that nests enough block tags per line of actual content to
    otherwise produce mostly-blank output)."""
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
    # CPython's HTMLParser silently drops an unterminated tag (and anything
    # read into it, e.g. a large base64 payload) rather than emitting it as
    # text. Without this guard a real trailing tag (an image, a cut-off
    # field) simply vanishes from the preview with no indication anything
    # was cut; this turns that silent loss into an explicit, labeled
    # placeholder.
    safe_end = data.rfind(b">")
    if safe_end < 0:
        return data, None
    dropped = data[safe_end + 1 :]
    tag_match = _TRUNCATED_TAG_NAME_RE.match(dropped)
    if tag_match is None:
        return data, None  # trailing plain text/whitespace, not a truncated tag — nothing to flag
    label = _TRUNCATED_TAG_LABELS.get(tag_match.group(1).lower(), _TRUNCATED_TAG_DEFAULT_LABEL)
    return data[: safe_end + 1], label


def _truncation_placeholder(label: str, dropped_bytes: int) -> str:
    """The note shown in place of whatever ``_drop_trailing_unterminated_tag``
    dropped — shared by every renderer that can hit that guard. ``≥``:
    the read window itself is size-capped, so a real attribute this large
    may well continue past it — ``dropped_bytes`` is a lower bound on what
    was cut, never claimed to be the attachment's real total size."""
    return f"[{label}, ≥{format_bytes(dropped_bytes)}, not shown in preview]"


def _html_bytes_to_text_with_truncation_note(data: bytes) -> tuple[str, bool]:
    """``_drop_trailing_unterminated_tag`` + ``_html_to_text`` + an explicit
    note for whatever the trim dropped — shared by every renderer that
    converts a self-contained HTML byte string straight to plain text.
    ``teams_chat.py``'s own transcript parser needs its own conversion
    step, so it calls ``_drop_trailing_unterminated_tag``/
    ``_truncation_placeholder`` directly instead of this one.

    Returns ``(text, has_real_content)`` — ``has_real_content`` is whether
    ``_html_to_text``'s own converted output was non-blank *before* any
    truncation note got appended, so a caller with somewhere else to fall
    back to (``mail.py``) can tell "this alternative had real content plus
    a note about what got cut" apart from "this alternative had nothing
    but the note" — a placeholder-only result isn't the alternative
    actually working."""
    trimmed, dropped_label = _drop_trailing_unterminated_tag(data)
    real_text = _html_to_text(trimmed.decode("utf-8", errors="replace"))
    text = real_text
    if dropped_label is not None:
        placeholder = _truncation_placeholder(dropped_label, len(data) - len(trimmed))
        text = f"{text}\n\n{placeholder}" if text else placeholder
    return text, bool(real_text.strip())
