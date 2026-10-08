"""Plumbing the preview renderers share: truncation, HTML-to-text, and
handling a tag cut off by the read window.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import override

from synology_apm_repo.sdk.presentation import format_bytes

#: Placeholder for a field this unit has no value for (an event with no
#: location, a contact with no email, ...). A missing event title or mail
#: subject uses ``_NO_TITLE_LABEL``/``_NO_SUBJECT_LABEL`` instead.
_NONE_LABEL = "(none)"

# Tags that become a line break in the plain-text rendering; others are
# dropped.
_BLOCK_TAGS = frozenset({"p", "div", "br", "h1", "h2", "h3", "h4", "tr", "li", "blockquote"})

#: The most text a preview shows; the rest is left to an export.
_MAX_PREVIEW_CHARS = 4000

_TRUNCATION_NOTE = "\n\n[…preview truncated — press e to export the full item…]"


def _truncate(text: str) -> str:
    if len(text) <= _MAX_PREVIEW_CHARS:
        return text
    return text[:_MAX_PREVIEW_CHARS].rstrip() + _TRUNCATION_NOTE


class _HTMLToText(HTMLParser):
    """Best-effort HTML -> plain text: drops markup and
    ``<style>``/``<script>`` contents, and breaks lines at block-level
    tags."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("style", "script"):
            self._skip_depth += 1
        elif tag in _BLOCK_TAGS:
            self._parts.append("\n")

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag in ("style", "script"):
            self._skip_depth = max(0, self._skip_depth - 1)
        elif tag in _BLOCK_TAGS:
            self._parts.append("\n")

    @override
    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            self._parts.append(data)

    def text(self) -> str:
        return _collapse_blank_lines("".join(self._parts))


def _collapse_blank_lines(raw: str) -> str:
    """Strips each line, collapses runs of blank lines to one, and drops
    leading/trailing blank lines. Shared by ``_HTMLToText`` and
    ``_ChannelTranscriptParser``, whose nested block tags otherwise yield
    mostly-blank output."""
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
# The note's label for a cut-off tag (an <img>'s base64 payload, e.g. a
# Teams sticker), with a fallback for any other.
_TRUNCATED_TAG_LABELS = {b"img": "image"}
_TRUNCATED_TAG_DEFAULT_LABEL = "content"


def _drop_trailing_unterminated_tag(data: bytes) -> tuple[bytes, str | None]:
    """Trims ``data`` (a bounded prefix of a unit's bytes) back to its last
    ``>`` when it ends inside a tag, e.g. a long base64 ``<img
    src="data:...">`` cut by the read window, which ``HTMLParser`` would
    silently drop.

    Returns:
        ``(trimmed_data, label)`` — ``label`` (e.g. ``"image"``) names the
        dropped tag for ``_truncation_placeholder``, or is ``None`` when
        nothing was dropped.
    """
    safe_end = data.rfind(b">")
    if safe_end < 0:
        return data, None
    dropped = data[safe_end + 1 :]
    tag_match = _TRUNCATED_TAG_NAME_RE.match(dropped)
    if tag_match is None:
        return data, None  # trailing text, not a cut-off tag
    label = _TRUNCATED_TAG_LABELS.get(tag_match.group(1).lower(), _TRUNCATED_TAG_DEFAULT_LABEL)
    return data[: safe_end + 1], label


def _truncation_placeholder(label: str, dropped_bytes: int) -> str:
    """The note shown for what ``_drop_trailing_unterminated_tag`` dropped;
    ``≥`` since the read window cut the tag short."""
    return f"[{label}, ≥{format_bytes(dropped_bytes)}, not shown in preview]"


def _html_bytes_to_text_with_truncation_note(data: bytes) -> tuple[str, bool]:
    """``data`` as plain text, with a note for a cut-off trailing tag.

    Returns ``(text, has_real_content)``: whether the text is non-blank
    without the note, so a caller with a fallback can tell a note-only
    result from a real one."""
    trimmed, dropped_label = _drop_trailing_unterminated_tag(data)
    real_text = _html_to_text(trimmed.decode("utf-8", errors="replace"))
    text = real_text
    if dropped_label is not None:
        placeholder = _truncation_placeholder(dropped_label, len(data) - len(trimmed))
        text = f"{text}\n\n{placeholder}" if text else placeholder
    return text, bool(real_text.strip())
