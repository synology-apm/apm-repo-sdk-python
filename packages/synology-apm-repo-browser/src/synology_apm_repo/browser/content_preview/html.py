"""Generic self-contained-HTML preview, sniffed from the bytes rather than
a caller-supplied type flag. Unlike the other content-type renderers in
this package, ``render_html_preview`` never raises on malformed input.
"""

from __future__ import annotations

import re

from ._common import _html_bytes_to_text_with_truncation_note, _truncate

_HTML_SNIFF_RE = re.compile(rb"<!doctype\s+html|<html[\s>]", re.IGNORECASE)
_HTML_SNIFF_WINDOW = 1024
"""How many leading bytes to check for an HTML doctype/tag — real pages
here (render_channel_html's own output) always open with
``<!doctype html>`` in the first ~40 bytes; a generous window just
tolerates a stray leading BOM/whitespace without reading the whole
(possibly large) buffer just to decide "is this HTML at all"."""


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
    text, _ = _html_bytes_to_text_with_truncation_note(data)
    if not text:
        return None
    return _truncate(text, max_chars)
