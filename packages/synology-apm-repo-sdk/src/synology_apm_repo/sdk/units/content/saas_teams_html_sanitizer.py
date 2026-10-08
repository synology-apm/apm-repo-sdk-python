"""Content Layer — the allowlist sanitizer behind Teams/Chat's HTML export:
an "html" contentType message body is re-emitted with only the tags below
and no source attributes but an http(s) link's ``href``, so backup content
can't inject script or style into the exported page. Pure ``str -> str``.
"""

from __future__ import annotations

import html as html_module
import re
from collections.abc import Mapping
from html.parser import HTMLParser
from typing import override

# Structural/inline tags kept from an "html" contentType body, rendered
# without any attributes (see _MessageBodyRenderer).
_ALLOWED_STRUCTURAL_TAGS = frozenset(
    {
        "div",
        "p",
        "blockquote",
        "h1",
        "h2",
        "h3",
        "h4",
        "ul",
        "ol",
        "li",
        "table",
        "thead",
        "tbody",
        "tr",
        "td",
        "th",
        "colgroup",
        "col",
        "hr",
        "br",
        "b",
        "strong",
        "i",
        "em",
        "u",
        "s",
        "sup",
        "sub",
        "span",
    }
)
# Void: rendered from the start tag alone, never pushed onto the open stack.
_VOID_STRUCTURAL_TAGS = frozenset({"br", "hr", "col"})
_LINK_SCHEMES = ("http://", "https://")
# A sticker's cached bytes are embedded in a ``src`` attribute only when they
# are plain base64, so backup content can't close the attribute.
_BASE64 = re.compile(r"[A-Za-z0-9+/]*={0,2}")


class _MessageBodyRenderer(HTMLParser):
    """Renders one message's raw ``"html"`` contentType body through a
    fixed, closed allowlist of safe tags (``_ALLOWED_STRUCTURAL_TAGS``
    plus ``img``/``emoji``/``at``/``a``) — arbitrary source markup is
    never interpreted or re-emitted verbatim, and every attribute except
    a scheme-validated ``<a href>`` is dropped so this page's own CSS,
    not per-message inline styling, controls the look."""

    def __init__(self, stickers: Mapping[str, str]) -> None:
        super().__init__(convert_charrefs=True)
        self._stickers = stickers
        self.parts: list[str] = []
        # Tags opened so far; an end tag emits only when it matches the
        # top, so a dropped or mismatched one is a no-op.
        self._open: list[str] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "img":
            self._handle_img(dict(attrs))
            return
        if tag == "emoji":
            # Teams' <emoji alt="🙂">: its alt is the character itself.
            alt = dict(attrs).get("alt")
            if alt:
                self.parts.append(html_module.escape(alt))
            return
        if tag == "at":
            # <at id="0">display name</at>: a Graph @mention.
            self.parts.append('<span class="mention">@')
            self._open.append("at")
            return
        if tag == "a":
            href = dict(attrs).get("href") or ""
            if href.lower().startswith(_LINK_SCHEMES):
                escaped_href = html_module.escape(href, quote=True)
                self.parts.append(f'<a href="{escaped_href}" target="_blank" rel="noopener noreferrer">')
            else:
                self.parts.append("<a>")  # href missing/unsafe: keep the text, drop the link
            self._open.append("a")
            return
        if tag in _VOID_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
            return
        if tag in _ALLOWED_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
            self._open.append(tag)
            return
        # Any other tag (e.g. <attachment>) is dropped; its text still
        # comes through handle_data.

    @override
    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # A self-closed tag (``<br/>``, ``<attachment id="..."/>``) never
        # gets handle_endtag, so nothing may be pushed onto the open stack.
        if tag == "img":
            self._handle_img(dict(attrs))
        elif tag == "emoji":
            alt = dict(attrs).get("alt")
            if alt:
                self.parts.append(html_module.escape(alt))
        elif tag in _VOID_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
        # A self-closed <at>/<a>/other tag has no text to keep; dropped.

    @override
    def handle_endtag(self, tag: str) -> None:
        if not self._open or self._open[-1] != tag:
            return
        self._open.pop()
        self.parts.append("</span>" if tag == "at" else f"</{tag}>")

    def close_open_tags(self) -> None:
        """End every tag the body left open, so its formatting stops at the
        end of this message."""
        while self._open:
            self.handle_endtag(self._open[-1])

    def _handle_img(self, attrs: Mapping[str, str | None]) -> None:
        # A sticker (src matching one of this message's sticker_info_table
        # URLs, its content plain base64) is embedded; any other <img> can't
        # be fetched offline and becomes a placeholder from its alt.
        src = attrs.get("src")
        cached = self._stickers.get(src) if src is not None else None
        # Line-wrapped base64 is still base64.
        base64_content = "".join(cached.split()) if cached is not None else None
        if base64_content is not None and _BASE64.fullmatch(base64_content):
            # Teams stickers are JPEG (base64 prefix "/9j/").
            self.parts.append(f'<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,{base64_content}">')
            return
        alt = attrs.get("alt")
        label = html_module.escape(alt) if alt else "image"
        self.parts.append(f'<span class="inline-media">🖼 {label}</span>')

    @override
    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(html_module.escape(data))


def render_message_body_html(raw_content: str, stickers: Mapping[str, str]) -> str:
    """One message's raw body as safe HTML: text with no tags is escaped;
    anything else goes through ``_MessageBodyRenderer``'s allowlist, every
    tag it opens closed by the end of the body."""
    if "<" not in raw_content:
        return html_module.escape(raw_content)
    renderer = _MessageBodyRenderer(stickers)
    renderer.feed(raw_content)
    renderer.close()
    renderer.close_open_tags()
    return "".join(renderer.parts)
