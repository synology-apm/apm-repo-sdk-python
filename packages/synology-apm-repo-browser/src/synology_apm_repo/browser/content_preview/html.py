"""Preview of any self-contained HTML document, sniffed from the bytes."""

from __future__ import annotations

import re

from ._common import _html_bytes_to_text_with_truncation_note, _truncate

_HTML_SNIFF_RE = re.compile(rb"<!doctype\s+html|<html[\s>]", re.IGNORECASE)
_HTML_SNIFF_WINDOW = 1024
"""Leading bytes searched for a doctype or ``<html>`` tag, allowing for a BOM
or whitespace before it."""


def render_html_preview(data: bytes) -> str | None:
    """A plain-text rendering of ``data``, truncated, or
    ``None`` if it isn't an HTML document (no preview). Never raises."""
    probe = data[:_HTML_SNIFF_WINDOW].lstrip(b"\xef\xbb\xbf \t\r\n")
    if not _HTML_SNIFF_RE.match(probe):
        return None
    text, _ = _html_bytes_to_text_with_truncation_note(data)
    if not text:
        return None
    return _truncate(text)
