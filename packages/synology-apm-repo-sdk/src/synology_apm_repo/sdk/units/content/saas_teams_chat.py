"""Content Layer — Teams/Chat's HTML rendering: pure ``rows -> HTML
string`` formatting, no I/O. ``units/saas/teams_chat.py`` calls it.
"""

from __future__ import annotations

import dataclasses
import html as html_module
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from ..._util.jsonparse import try_parse_json_object
from .saas_teams_html_sanitizer import render_message_body_html

_SYSTEM_EVENT_LABEL = "(system event)"
# A deleted message's content_preview is literally "<The message is
# deleted>", with metadata.body.content emptied to "" by the connector.
_DELETED_MESSAGE_LABEL = "(this message has been deleted)"


@dataclasses.dataclass(frozen=True, slots=True)
class _RenderedMessage:
    created: str | None  # a display string (ISO or formatted)
    sender: str
    is_system: bool
    is_deleted: bool
    msg_id: str | None
    reply_to_id: str | None
    content: str
    preview: str  # plain text, for the reply-quote line
    attachments: tuple[Mapping[str, object], ...]


def _sender_from_row(metadata: Mapping[str, object], row: Mapping[str, object]) -> str:
    # metadata.from.user.displayName (Graph chatMessage), else the row's
    # ``author`` JSON column.
    from_field = metadata.get("from")
    if isinstance(from_field, dict):
        user = from_field.get("user")
        if isinstance(user, dict):
            name = user.get("displayName")
            if isinstance(name, str) and name:
                return name
    # metadata/author are JSON strings inside msg_info_table rows; a drifted shape renders as empty.
    author = try_parse_json_object(row.get("author")) or {}
    name = author.get("name")
    if isinstance(name, str) and name:
        return name
    return "(unknown sender)"


def _created_from_row(metadata: Mapping[str, object], row: Mapping[str, object]) -> str | None:
    # metadata.createdDateTime (ISO 8601 UTC) as-is, else the row's
    # create_time (epoch seconds) formatted.
    created = metadata.get("createdDateTime")
    if isinstance(created, str) and created:
        return created
    create_time = row.get("create_time")
    if isinstance(create_time, int):
        return datetime.fromtimestamp(create_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return None


def _content_from_row(metadata: Mapping[str, object], row: Mapping[str, object], *, is_system: bool) -> str:
    if is_system:
        # The body is a content-free "<systemEventMessage/>" tag.
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


def _message_from_row(
    row: Mapping[str, object], stickers_by_msg_id: Mapping[str, Mapping[str, str]]
) -> _RenderedMessage:
    metadata = try_parse_json_object(row.get("metadata")) or {}
    is_system = bool(row.get("is_sys_message"))
    is_deleted = bool(row.get("is_deleted"))
    reply_to = row.get("reply_to_id")
    msg_id = row.get("msg_id")
    preview_raw = row.get("content_preview")
    preview = preview_raw if isinstance(preview_raw, str) else ""
    raw_content = _content_from_row(metadata, row, is_system=is_system)
    # A system message's content stays raw: _render_message_html escapes
    # it. A deleted message's body is emptied by the connector, so it
    # shows the placeholder.
    if is_deleted:
        content = _DELETED_MESSAGE_LABEL
    elif is_system:
        content = raw_content
    else:
        content = render_message_body_html(raw_content, stickers_by_msg_id.get(str(msg_id), {}))
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
    # Inline content (e.g. an Adaptive Card's JSON) is shown in full; a
    # "reference" attachment only has a live contentUrl, unreachable offline.
    if isinstance(content, str) and content:
        return (
            f'<div class="attachment"><div class="attachment-hdr">📎 {label} ({ctype})</div>'
            f'<pre class="attachment-body">{html_module.escape(content)}</pre></div>'
        )
    return f'<div class="attachment">📎 {label} ({ctype}) — content not included in this offline export</div>'


_AVATAR_COLOR_COUNT = 8  # matches the .avatar-0 .. .avatar-7 classes in _PAGE_CSS
_REPLY_PREVIEW_LENGTH = 80


def _avatar_class(sender: str) -> str:
    # Not hash(): str hashing is randomized per process.
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
        body = f'<div class="body">{html_module.escape(message.content)}</div>'
        attachments_html = ""
    else:
        # Already safe HTML from render_message_body_html; not re-escaped.
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
    """One self-contained HTML page (no external resources or JS) for a
    Teams channel/chat's ``msg_info_table`` rows. ``stickers_by_msg_id``
    is ``{msg_id: {sticker_url: base64_content}}``. Messages sort by
    timestamp (undated first), group under date separators, and show a
    reply's parent sender and preview. The whole history is rendered into
    one string."""
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
