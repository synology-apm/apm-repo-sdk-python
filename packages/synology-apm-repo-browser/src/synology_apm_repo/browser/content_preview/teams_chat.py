"""Teams/Chat channel/chat transcript preview: parses the unit's exported
HTML page back into a chat-transcript-style rendering.
"""

from __future__ import annotations

import dataclasses
from html.parser import HTMLParser
from typing import override

from ._common import (
    _BLOCK_TAGS,
    _MAX_PREVIEW_CHARS,
    _collapse_blank_lines,
    _drop_trailing_unterminated_tag,
    _truncation_placeholder,
)

_NO_MESSAGES_LABEL = "(no messages)"
_SYSTEM_EVENT_FALLBACK_LABEL = "(system event)"

#: The CSS classes that give an element a role; any other element inherits
#: the role around it.
_CHAT_ROLE_CLASSES = frozenset(
    {"date-sep", "msg", "avatar", "hdr", "reply", "body", "attachment", "attachment-hdr", "attachment-body"}
)

#: The tags a role class appears on. Every one of these is tracked on a
#: stack, role-bearing or not, since a plain ``<div>`` shares the tag name.
_ROLE_HOST_TAGS = frozenset({"div", "pre"})


@dataclasses.dataclass(frozen=True, slots=True)
class _ParsedChatMessage:
    is_sys: bool
    sys_parts: list[str] = dataclasses.field(default_factory=list)
    reply_parts: list[str] = dataclasses.field(default_factory=list)
    hdr_parts: list[str] = dataclasses.field(default_factory=list)
    body_parts: list[str] = dataclasses.field(default_factory=list)
    attachments: list[str] = dataclasses.field(default_factory=list)


def _render_chat_message(message: _ParsedChatMessage) -> str:
    if message.is_sys:
        text = "".join(message.sys_parts).strip() or _SYSTEM_EVENT_FALLBACK_LABEL
        return f"— {text} —"
    lines = []
    reply = "".join(message.reply_parts).strip()
    if reply:
        lines.append(reply)
    hdr = "".join(message.hdr_parts).strip()
    if hdr:
        lines.append(hdr)
    body = _collapse_blank_lines("".join(message.body_parts))
    if body:
        lines.append(body)
    lines.extend(a for a in message.attachments if a)
    return "\n".join(lines)


class _ChannelTranscriptParser(HTMLParser):
    """Parses one exported channel page, keyed off the SDK generator's CSS
    class names, into transcript text: one block per date separator or
    message, a reply quote and the sender/time line above a message's body,
    the avatar dropped. A block is finished when its element closes."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._role: str | None = None
        # (role_before, changed_role) per open _ROLE_HOST_TAGS tag;
        # changed_role is False for a plain wrapper tag (inherits the
        # surrounding role, restores nothing on close).
        self._role_stack: list[tuple[str | None, bool]] = []
        self._blocks: list[str] = []
        self._date_buf: list[str] = []
        self._current: _ParsedChatMessage | None = None
        self._attachment_hdr: list[str] = []
        self._attachment_body: list[str] = []
        self._attachment_plain: list[str] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("style", "script"):
            self._skip_depth += 1
            return
        if tag == "img":
            # <img> is a void tag; its placeholder lives in the alt attribute.
            self._emit(dict(attrs).get("alt") or "[image]")
            return
        if tag in _BLOCK_TAGS:
            self._emit("\n")
        if tag not in _ROLE_HOST_TAGS:
            return
        classes = (dict(attrs).get("class") or "").split()
        new_role = next((c for c in classes if c in _CHAT_ROLE_CLASSES), None)
        self._role_stack.append((self._role, new_role is not None))
        if new_role is None:
            return
        if new_role == "msg":
            self._current = _ParsedChatMessage(is_sys="sys" in classes)
        elif new_role == "attachment":
            self._attachment_hdr = []
            self._attachment_body = []
            self._attachment_plain = []
        self._role = new_role

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag in ("style", "script"):
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if tag in _BLOCK_TAGS:
            self._emit("\n")
        if tag not in _ROLE_HOST_TAGS or not self._role_stack:
            return
        role_before, changed_role = self._role_stack.pop()
        if not changed_role:
            return  # a plain wrapper tag's own close -- nothing to finalize/restore
        closing_role = self._role
        if closing_role == "msg" and self._current is not None:
            self._blocks.append(_render_chat_message(self._current))
            self._current = None
        elif closing_role == "date-sep":
            text = "".join(self._date_buf).strip()
            self._date_buf = []
            if text:
                self._blocks.append(f"── {text} ──")
        elif closing_role == "attachment" and self._current is not None:
            self._current.attachments.append(self._finalize_attachment())
        self._role = role_before

    @override
    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        self._emit(data)

    def _emit(self, text: str) -> None:
        role = self._role
        if role == "msg":
            # Direct text at the msg-wrapper level is real only for a system
            # message; otherwise it is inter-tag whitespace.
            if self._current is not None and self._current.is_sys:
                self._current.sys_parts.append(text)
            return
        if role is None or role == "avatar":
            return
        if role == "date-sep":
            self._date_buf.append(text)
            return
        if self._current is None:  # pragma: no cover - defensive: not reachable from real generator output
            return
        if role == "hdr":
            self._current.hdr_parts.append(text)
        elif role == "reply":
            self._current.reply_parts.append(text)
        elif role == "body":
            self._current.body_parts.append(text)
        elif role == "attachment":
            self._attachment_plain.append(text)
        elif role == "attachment-hdr":
            self._attachment_hdr.append(text)
        elif role == "attachment-body":
            self._attachment_body.append(text)

    def _finalize_attachment(self) -> str:
        hdr = "".join(self._attachment_hdr).strip()
        body = "".join(self._attachment_body).strip()
        if hdr or body:
            return "\n".join(part for part in (hdr, body) if part)
        return "".join(self._attachment_plain).strip()

    @property
    def blocks(self) -> list[str]:
        return self._blocks

    @property
    def pending(self) -> str | None:
        """The message still open when parsing stopped (a truncated read
        window can end before its closing ``</div>``s), so a partial last
        message is shown rather than lost."""
        return _render_chat_message(self._current) if self._current is not None else None


#: Prepended (never appended) when at least one whole leading (older)
#: block had to be dropped to fit ``_MAX_PREVIEW_CHARS``.
_OLDER_MESSAGES_TRUNCATED_NOTE = "[…earlier messages truncated — press e to export the full item…]\n\n"


def _join_keeping_the_newest(blocks: list[str]) -> tuple[str, bool]:
    """Joins ``blocks`` (oldest first, ``"\\n\\n"``-separated), dropping whole
    leading blocks until the text fits ``_MAX_PREVIEW_CHARS``; always keeps at
    least the newest block, however long. Returns ``(text,
    any_older_block_dropped)``."""
    kept: list[str] = []
    total = 0
    for block in reversed(blocks):
        added = len(block) + (2 if kept else 0)  # "\n\n" joiner, once there's already a newer block kept
        if kept and total + added > _MAX_PREVIEW_CHARS:
            break
        kept.append(block)
        total += added
    kept.reverse()
    return "\n\n".join(kept), len(kept) < len(blocks)


def render_teams_chat_preview(data: bytes) -> str:
    """A Teams/Chat channel or chat's exported HTML page as transcript
    text. Truncation drops the oldest blocks, matching
    ``prefers_recent_content``'s read from the end."""
    trimmed, dropped_label = _drop_trailing_unterminated_tag(data)
    parser = _ChannelTranscriptParser()
    parser.feed(trimmed.decode("utf-8", errors="replace"))
    parser.close()
    blocks = parser.blocks
    pending = parser.pending
    if pending:
        blocks = [*blocks, pending]
    if not blocks:
        return _NO_MESSAGES_LABEL
    text, dropped_older = _join_keeping_the_newest(blocks)
    if dropped_label is not None:
        placeholder = _truncation_placeholder(dropped_label, len(data) - len(trimmed))
        text = f"{text}\n\n{placeholder}"
    if dropped_older:
        text = f"{_OLDER_MESSAGES_TRUNCATED_NOTE}{text}"
    return text
