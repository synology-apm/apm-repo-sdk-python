"""Content Layer — Teams/Chat's HTML rendering. Pure ``rows -> HTML string``
formatting, no I/O; the tree-navigation and message/sticker-fetching logic
that calls this lives in ``units/saas/teams_chat.py``.
"""

from __future__ import annotations

import dataclasses
import html as html_module
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from json import loads as json_loads

_SYSTEM_EVENT_LABEL = "(system event)"
# A deleted message's content_preview is literally "<The message is
# deleted>", with metadata.body.content emptied to "" by the connector.
_DELETED_MESSAGE_LABEL = "(this message has been deleted)"


@dataclasses.dataclass(frozen=True)
class _RenderedMessage:
    created: str | None  # already a display string (ISO or formatted) — no further parsing needed downstream
    sender: str
    is_system: bool
    is_deleted: bool
    msg_id: str | None
    reply_to_id: str | None
    content: str
    preview: str  # plain-text (never HTML), for the reply-quote line — never the already-rendered ``content``
    attachments: tuple[Mapping[str, object], ...]


def _parse_json_object(value: object) -> dict[str, object]:
    """``value`` decoded as a JSON object, or ``{}`` for anything that
    isn't valid JSON or isn't an object at the top level. Never raises:
    ``metadata``/``author`` are themselves JSON strings inside
    ``msg_info_table`` rows, but a future connector version drifting
    their shape shouldn't crash rendering."""
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json_loads(value)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _sender_from_row(metadata: Mapping[str, object], row: Mapping[str, object]) -> str:
    # Primary: metadata.from.user.displayName (Microsoft Graph chatMessage
    # shape). Fallback: the row's own ``author`` JSON column (populated
    # even for some rows where ``from`` is present too — kept as a
    # fallback for schema drift, not redundancy).
    from_field = metadata.get("from")
    if isinstance(from_field, dict):
        user = from_field.get("user")
        if isinstance(user, dict):
            name = user.get("displayName")
            if isinstance(name, str) and name:
                return name
    author = _parse_json_object(row.get("author"))
    name = author.get("name")
    if isinstance(name, str) and name:
        return name
    return "(unknown sender)"


def _created_from_row(metadata: Mapping[str, object], row: Mapping[str, object]) -> str | None:
    # Primary: metadata.createdDateTime (ISO 8601 UTC — used as-is, no
    # re-parsing/re-formatting, to stay maximally faithful to the
    # source). Fallback: the row's own create_time (Unix epoch seconds),
    # for a metadata shape that ever drifts.
    created = metadata.get("createdDateTime")
    if isinstance(created, str) and created:
        return created
    create_time = row.get("create_time")
    if isinstance(create_time, int):
        return datetime.fromtimestamp(create_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return None


def _content_from_row(metadata: Mapping[str, object], row: Mapping[str, object], *, is_system: bool) -> str:
    if is_system:
        # A system-message body is the literal, content-free XML-ish tag
        # "<systemEventMessage/>" — showing that verbatim to a human
        # reader is noise, not information, so it's replaced with a
        # generic label instead of escaped and displayed as-is.
        return _SYSTEM_EVENT_LABEL
    body = metadata.get("body")
    if isinstance(body, dict):
        content = body.get("content")
        if isinstance(content, str):
            return content
    preview = row.get("content_preview")
    return preview if isinstance(preview, str) else ""


def _attachments_from_row(metadata: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    attachments = metadata.get("attachments")
    if not isinstance(attachments, list):
        return ()
    return tuple(a for a in attachments if isinstance(a, dict))


# Structural/inline markup that appears inside an "html" contentType
# body. Rendered bare —
# every attribute (including Teams' own inline ``style=``) is dropped, not
# just whitelisted, so this page's own CSS controls the look consistently
# rather than trusting per-message inline styling; only ``<a href>`` (see
# _MessageBodyRenderer.handle_starttag) keeps one attribute, and even
# that only after scheme-validating it.
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
# Void — rendered fully from their own start tag (or dropped), never
# pushed onto the open-tag stack and never paired with a close tag.
_VOID_STRUCTURAL_TAGS = frozenset({"br", "hr", "col"})
_LINK_SCHEMES = ("http://", "https://")


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
        # Tags this instance actually emitted an opening for, in order —
        # a matching close only fires (and only emits ``</tag>``) when it's
        # the top of this stack; anything else (an end tag for a tag we
        # dropped, or genuinely mismatched source markup) is a no-op,
        # never an exception. Exact top-of-stack matching is a safety
        # margin against malformed markup, not a mechanism real
        # Teams-generated HTML is expected to exercise.
        self._open: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "img":
            self._handle_img(dict(attrs))
            return
        if tag == "emoji":
            # <emoji id="..." alt="🙂"> is how Teams' own emoji picker
            # inserts an emoji — never a plain Unicode character in the
            # raw body. Render its alt (the actual character) directly.
            alt = dict(attrs).get("alt")
            if alt:
                self.parts.append(html_module.escape(alt))
            return
        if tag == "at":
            # <at id="0">display name</at> is a genuine Microsoft Graph
            # @mention; render as a styled @name span. metadata.mentions[]
            # is redundant with this tag's own inner text and isn't
            # consulted separately.
            self.parts.append('<span class="mention">@')
            self._open.append("at")
            return
        if tag == "a":
            href = dict(attrs).get("href") or ""
            if href.lower().startswith(_LINK_SCHEMES):
                escaped_href = html_module.escape(href, quote=True)
                self.parts.append(f'<a href="{escaped_href}" target="_blank" rel="noopener noreferrer">')
            else:
                self.parts.append("<a>")  # real href missing/unsafe — keep the text, drop the link
            self._open.append("a")
            return
        if tag in _VOID_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
            return
        if tag in _ALLOWED_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
            self._open.append(tag)
            return
        # Unrecognized tag (includes the real <attachment> marker) —
        # dropped structurally; its own text content still comes through
        # handle_data.

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # A self-closed void tag (e.g. real ``<attachment id="..."/>``-
        # style or ``<br/>``) — the parser wouldn't otherwise call
        # handle_endtag for these, so route through the same start-tag
        # logic and skip pushing anything onto the open stack.
        if tag == "img":
            self._handle_img(dict(attrs))
        elif tag == "emoji":
            alt = dict(attrs).get("alt")
            if alt:
                self.parts.append(html_module.escape(alt))
        elif tag in _VOID_STRUCTURAL_TAGS:
            self.parts.append(f"<{tag}>")
        # <at>/<a>/other allowed tags never appear genuinely self-closed
        # in real data (they always have real inner text); a self-closed
        # occurrence of one has no text to preserve either way, so it's
        # simply dropped rather than opening a tag with no matching close.

    def handle_endtag(self, tag: str) -> None:
        if not self._open or self._open[-1] != tag:
            return
        self._open.pop()
        self.parts.append("</span>" if tag == "at" else f"</{tag}>")

    def _handle_img(self, attrs: Mapping[str, str | None]) -> None:
        # A sticker's <img src> matches one of this message's own
        # sticker_info_table rows by URL and becomes a real,
        # base64-embedded <img>. Any other <img> (e.g. a live Graph/giphy
        # URL this offline tool can't fetch) becomes a text placeholder
        # built from its own alt instead of silently vanishing.
        src = attrs.get("src")
        base64_content = self._stickers.get(src) if src is not None else None
        if base64_content is not None:
            # Hardcoded to JPEG, not a general image-type sniff — Teams
            # stickers are JPEG, identifiable by their "/9j/" base64 prefix.
            self.parts.append(f'<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,{base64_content}">')
            return
        alt = attrs.get("alt")
        label = html_module.escape(alt) if alt else "image"
        self.parts.append(f'<span class="inline-media">🖼 {label}</span>')

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(html_module.escape(data))


def _render_message_body_html(raw_content: str, stickers: Mapping[str, str]) -> str:
    """Renders one message's raw body content safely: plain ``"text"``
    contentType (no tags) returns escaped text unchanged; a real
    ``"html"`` contentType body is parsed through
    ``_MessageBodyRenderer``'s closed tag allowlist, never re-emitting
    arbitrary source markup verbatim."""
    if "<" not in raw_content:
        return html_module.escape(raw_content)
    renderer = _MessageBodyRenderer(stickers)
    renderer.feed(raw_content)
    renderer.close()
    return "".join(renderer.parts)


def _message_from_row(
    row: Mapping[str, object], stickers_by_msg_id: Mapping[str, Mapping[str, str]]
) -> _RenderedMessage:
    metadata = _parse_json_object(row.get("metadata"))
    is_system = bool(row.get("is_sys_message"))
    is_deleted = bool(row.get("is_deleted"))
    reply_to = row.get("reply_to_id")
    msg_id = row.get("msg_id")
    preview_raw = row.get("content_preview")
    preview = preview_raw if isinstance(preview_raw, str) else ""
    raw_content = _content_from_row(metadata, row, is_system=is_system)
    # System messages stay raw here — _render_message_html's own system
    # branch does its own escaping of message.content directly (a
    # separate code path from the regular-message body below, entirely
    # unchanged); pre-rendering them here too would double-escape.
    # Deleted messages show the placeholder instead of the emptied real
    # body — metadata.body.content is emptied to "" by the connector for
    # a deleted row, which would otherwise render as a blank message with
    # no indication anything was ever there.
    if is_deleted:
        content = _DELETED_MESSAGE_LABEL
    elif is_system:
        content = raw_content
    else:
        content = _render_message_body_html(raw_content, stickers_by_msg_id.get(str(msg_id), {}))
    return _RenderedMessage(
        created=_created_from_row(metadata, row),
        sender=_sender_from_row(metadata, row),
        is_system=is_system,
        is_deleted=is_deleted,
        msg_id=str(msg_id) if msg_id is not None else None,
        reply_to_id=str(reply_to) if reply_to else None,
        content=content,
        preview=preview,
        attachments=() if is_deleted else _attachments_from_row(metadata),
    )


def _render_attachment_html(attachment: Mapping[str, object]) -> str:
    name = attachment.get("name")
    content_type = attachment.get("contentType")
    label = html_module.escape(name) if isinstance(name, str) and name else "attachment"
    ctype = html_module.escape(content_type) if isinstance(content_type, str) and content_type else "unknown type"
    content = attachment.get("content")
    # An Adaptive Card attachment's content is a JSON string sitting
    # directly in this same metadata — no separate object fetch needed,
    # so it's shown in full, not treated as unavailable. The *other*
    # real Microsoft Graph shape is an ``attachmentType: "reference"``
    # attachment (content left out, only ``contentUrl`` pointing at a live
    # web resource this offline tool cannot follow).
    if isinstance(content, str) and content:
        return (
            f'<div class="attachment"><div class="attachment-hdr">📎 {label} ({ctype})</div>'
            f'<pre class="attachment-body">{html_module.escape(content)}</pre></div>'
        )
    return f'<div class="attachment">📎 {label} ({ctype}) — content not included in this offline export</div>'


_AVATAR_COLOR_COUNT = 8  # matches the .avatar-0 .. .avatar-7 classes in _PAGE_CSS
_REPLY_PREVIEW_LENGTH = 80


def _avatar_class(sender: str) -> str:
    # Deterministic across runs (unlike the builtin hash() on str, which
    # is randomized per-process) — a cosmetic per-sender color pick, not
    # a reproduction of any real avatar the source data carries.
    bucket = sum(ord(c) for c in sender) % _AVATAR_COLOR_COUNT
    return f"avatar-{bucket}"


def _avatar_html(sender: str) -> str:
    initial = html_module.escape(sender.strip()[:1].upper()) if sender.strip() else "?"
    return f'<div class="avatar {_avatar_class(sender)}">{initial}</div>'


def _truncate(text: str, limit: int) -> str:
    stripped = text.strip()
    return stripped if len(stripped) <= limit else stripped[: limit - 1].rstrip() + "…"


def _reply_note_html(message: _RenderedMessage, by_msg_id: Mapping[str, _RenderedMessage]) -> str:
    if not message.reply_to_id:
        return ""
    parent = by_msg_id.get(message.reply_to_id)
    if parent is None:
        # The parent isn't in this same export (a real, if rare,
        # possibility — e.g. it belongs to a version this page wasn't
        # built from) — reply_to_id itself is a real link, just not one
        # this page can resolve.
        return '<div class="reply">&#8618; replying to a message not included in this export</div>'
    if parent.is_deleted:
        snippet = html_module.escape(_DELETED_MESSAGE_LABEL)
    elif parent.is_system:
        snippet = html_module.escape(parent.content)
    else:
        snippet = html_module.escape(_truncate(parent.preview, _REPLY_PREVIEW_LENGTH)) if parent.preview else "…"
    sender = html_module.escape(parent.sender)
    return f'<div class="reply">&#8618; replying to <b>{sender}</b>: {snippet}</div>'


def _render_message_html(message: _RenderedMessage, by_msg_id: Mapping[str, _RenderedMessage]) -> str:
    if message.is_system:
        return f'<div class="msg sys">{html_module.escape(message.content)}</div>'
    header = f"<span>{html_module.escape(message.sender)}</span>"
    if message.created:
        header += f" <span>&middot; {html_module.escape(message.created)}</span>"
    reply_note = _reply_note_html(message, by_msg_id)
    css_class = "msg deleted" if message.is_deleted else "msg"
    if message.is_deleted:
        # The placeholder text itself (_DELETED_MESSAGE_LABEL) is plain,
        # trusted text this module wrote, not source content — still
        # escaped for uniformity/defense in depth, not because it could
        # ever actually contain markup.
        body = f'<div class="body">{html_module.escape(message.content)}</div>'
        attachments_html = ""
    else:
        # message.content is already safe HTML by the time it gets
        # here — _message_from_row built it via _render_message_body_html,
        # which either escaped the raw content outright (plain "text"
        # contentType) or ran it through _MessageBodyRenderer's own safe
        # tag allowlist. Escaping it *again* here would turn every real
        # structural tag/sticker/mention/emoji it already rendered back
        # into visible, useless markup text.
        body = f'<div class="body">{message.content}</div>'
        attachments_html = "".join(_render_attachment_html(a) for a in message.attachments)
    return (
        f'<div class="{css_class}">{_avatar_html(message.sender)}'
        f'<div class="content">{reply_note}<div class="hdr">{header}</div>{body}{attachments_html}</div></div>'
    )


_PAGE_CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;max-width:820px;
  margin:2rem auto;padding:0 1rem;color:#1f2328;background:#fff;line-height:1.5}
h1{font-size:1.35rem;font-weight:600;border-bottom:1px solid #e2e2e2;padding-bottom:.6rem;margin-bottom:1rem}
.date-sep{position:relative;text-align:center;margin:1.2rem 0 .6rem;color:#6b7280;font-size:.78rem}
.date-sep span{position:relative;display:inline-block;padding:0 .8rem;background:#fff}
.date-sep::before{content:"";position:absolute;left:0;right:0;top:50%;border-top:1px solid #e5e7eb}
.msg{display:flex;gap:.7rem;padding:.55rem 0;border-bottom:1px solid #f2f2f3}
.msg.sys{display:block;color:#9aa1a9;font-size:.8rem;text-align:center;padding:.35rem 0;
  font-style:italic;border-bottom:none}
.avatar{flex:none;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-size:.85rem;font-weight:600;color:#fff}
.avatar-0{background:#5b8def}.avatar-1{background:#2f9e6e}.avatar-2{background:#e08a3c}
.avatar-3{background:#c65b8c}.avatar-4{background:#7c6bd6}.avatar-5{background:#3aa6a6}
.avatar-6{background:#c2564a}.avatar-7{background:#8a924f}
.content{flex:1;min-width:0}
.hdr{font-weight:600;color:#3a3f45;font-size:.85rem;margin-bottom:.15rem}
.hdr :nth-child(2){font-weight:400;color:#8a8f98}
.body{white-space:pre-wrap;word-break:break-word;font-size:.92rem}
.msg.deleted .body{font-style:italic;color:#8a8f98}
.reply{color:#6b7280;font-size:.78rem;margin-bottom:.2rem;padding-left:.5rem;border-left:2px solid #d8dbe0;
  overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.mention{color:#2f7a52;font-weight:600}
.inline-media{display:inline-block;color:#6b7280;font-style:italic;background:#f2f3f5;border-radius:3px;
  padding:0 .3rem}
.attachment{margin-top:.4rem;padding:.5rem .6rem;background:#f6f7f8;border-radius:6px;font-size:.85rem}
.attachment-hdr{font-weight:600;margin-bottom:.3rem}
.attachment-body{white-space:pre-wrap;word-break:break-word;font-size:.8rem;max-height:20rem;overflow:auto;margin:0}
.sticker{max-width:280px;max-height:280px;display:block;margin:.3rem 0;border-radius:4px}
.body blockquote{margin:.4rem 0;padding:.4rem .8rem;border-left:3px solid #d8dbe0;color:#4b5157;background:#f8f9fa}
.body table{border-collapse:collapse;margin:.4rem 0;font-size:.88rem}
.body td,.body th{border:1px solid #e2e2e2;padding:.3rem .5rem}
.body ul,.body ol{margin:.3rem 0;padding-left:1.4rem}
.body h1,.body h2,.body h3,.body h4{margin:.5rem 0 .3rem;line-height:1.3}
.body h1{font-size:1.25rem}.body h2{font-size:1.15rem}.body h3{font-size:1.05rem}.body h4{font-size:1rem}
.body a{color:#2f7a52;text-decoration:underline}
.body hr{border:none;border-top:1px solid #e2e2e2;margin:.6rem 0}
"""


def render_channel_html(
    rows: Sequence[Mapping[str, object]],
    *,
    channel_name: str,
    stickers_by_msg_id: Mapping[str, Mapping[str, str]] | None = None,
) -> str:
    """One self-contained HTML page (no external resources/JS) for a
    Teams channel/chat's messages. ``rows`` are ``msg_info_table``
    records already read from the decompressed message DB; this
    function itself does no I/O. ``stickers_by_msg_id`` —
    ``{msg_id: {sticker_url: base64_content}}`` from
    ``_read_stickers`` — defaults to ``{}``. Messages sort by
    timestamp (undated rows first), group under a date separator, and
    resolve a real ``reply_to_id`` to its parent's sender/preview
    rather than a bare id.

    Known limitation: ``rows`` is the caller's entire channel/chat
    history (no cap), held in memory at once as one string inside a
    ``LazyArtifact`` — not the "small" blob that class assumes, for a
    genuinely large channel's transcript."""
    stickers_by_msg_id = stickers_by_msg_id or {}
    messages = sorted((_message_from_row(row, stickers_by_msg_id) for row in rows), key=lambda m: m.created or "")
    by_msg_id = {m.msg_id: m for m in messages if m.msg_id is not None}

    parts: list[str] = []
    current_date: str | None = None
    for message in messages:
        date = (message.created or "")[:10]  # YYYY-MM-DD prefix, true of both the ISO and epoch-seconds formats
        if date and date != current_date:
            current_date = date
            parts.append(f'<div class="date-sep"><span>{html_module.escape(date)}</span></div>')
        parts.append(_render_message_html(message, by_msg_id))
    body_html = "".join(parts)

    title = html_module.escape(channel_name)
    return (
        "<!doctype html>\n"
        f'<html><head><meta charset="utf-8"><title>{title}</title>'
        f"<style>{_PAGE_CSS}</style></head>"
        f"<body><h1>{title}</h1>{body_html}</body></html>"
    )
