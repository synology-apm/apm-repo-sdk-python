"""Generic self-contained-HTML preview, sniffed from the bytes rather than
a caller-supplied type flag. Unlike the other content-type renderers in
this package, ``render_html_preview`` never raises on malformed input.
"""

from __future__ import annotations

import re

from synology_apm_repo.sdk.presentation.format import format_bytes

from ._common import _drop_trailing_unterminated_tag, _html_to_text, _truncate

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
