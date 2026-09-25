"""Teams/Chat channel/chat transcript preview: parses the unit's exported
HTML page back into a chat-transcript-style rendering.
"""

from __future__ import annotations

import dataclasses
from html.parser import HTMLParser

from synology_apm_repo.sdk.presentation.format import format_bytes

from ._common import _BLOCK_TAGS, _collapse_blank_lines, _drop_trailing_unterminated_tag

_NO_MESSAGES_LABEL = "(no messages)"
_SYSTEM_EVENT_FALLBACK_LABEL = "(system event)"

#: The one class token on a ``render_channel_html`` ``<div>`` this parser
#: assigns a distinct role to -- every other ``<div>`` (the "content"
#: wrapper, an inner ``blockquote``/``table`` inside a message body, ...)
#: simply inherits whichever role was already active when it opened, so
#: its own text still lands in the right section without needing its own
#: entry here.
_CHAT_ROLE_CLASSES = frozenset(
    {"date-sep", "msg", "avatar", "hdr", "reply", "body", "attachment", "attachment-hdr", "attachment-body"}
)

#: Every tag a ``_CHAT_ROLE_CLASSES`` class can appear on -- ``attachment-
#: body`` is a ``<pre>``, everything else a ``<div>``. ``_ChannelTranscriptParser``
#: tracks every open/close of these two tags on its own stack (role-bearing or
#: not), never by matching a closing tag's name against the stack top: a
#: plain nested ``<div>`` inside a role-bearing one shares its tag name, so
#: name-matching alone can't tell the two apart.
_ROLE_HOST_TAGS = frozenset({"div", "pre"})


@dataclasses.dataclass(frozen=True)
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
    """Parses one ``render_channel_html`` page (``units/content/
    saas_teams_chat.py`` — the exact, and only, HTML shape this class
    understands, coupled to its CSS class names by necessity since
    there's no other structure to key off) back into a chat-transcript
    -style plain text: one block per date separator or message, a
    message's own sender+timestamp line immediately above its body (no
    gap between them, unlike the generic ``_HTMLToText`` fallback this
    replaces), a reply quote directly above the message it belongs to,
    and the avatar's own single-letter placeholder dropped entirely as
    pure visual noise in a text rendering.

    Tracks one ``role`` (``None`` outside any recognized section) plus a
    stack of every open ``_ROLE_HOST_TAGS`` tag (role-bearing or not) it's
    nested within; every other tag's own text is routed by whichever role
    is already active. Finalizes a message/date-separator/attachment
    block the moment its own enclosing ``_ROLE_HOST_TAGS`` tag closes,
    rather than building one combined tree first."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._role: str | None = None
        # (role_before, changed_role) for every open _ROLE_HOST_TAGS tag,
        # role-bearing or not -- popped by stack position on that tag's
        # own close, never by matching the closing tag's name against
        # the stack top: a plain nested <div> inside a role-bearing one
        # shares its tag name, so name-matching alone can't tell the two
        # apart. ``changed_role`` is False for a
        # plain wrapper tag (inherits the surrounding role, restores
        # nothing of its own on close).
        self._role_stack: list[tuple[str | None, bool]] = []
        self._blocks: list[str] = []
        self._date_buf: list[str] = []
        self._current: _ParsedChatMessage | None = None
        self._attachment_hdr: list[str] = []
        self._attachment_body: list[str] = []
        self._attachment_plain: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("style", "script"):
            self._skip_depth += 1
            return
        if tag == "img":
            # A real sticker's own <img> has no inner text at all (it's a
            # void tag) -- its "[sticker]" placeholder lives only in its
            # own alt attribute (see saas_teams_chat.py's _handle_img).
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

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        self._emit(data)

    def _emit(self, text: str) -> None:
        role = self._role
        if role == "msg":
            # Direct text at the msg-wrapper level itself is only ever
            # real for a system message (the regular/deleted shape
            # always nests its text one level deeper, inside "content");
            # anything else here is inconsequential inter-tag whitespace.
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
            # Joins only whichever of the two is actually non-empty --
            # unlike f"{hdr}\n{body}", never a stray leading blank line
            # when one of the pair is empty.
            return "\n".join(part for part in (hdr, body) if part)
        return "".join(self._attachment_plain).strip()

    @property
    def blocks(self) -> list[str]:
        return self._blocks

    @property
    def pending(self) -> str | None:
        """Whatever message was still open when parsing stopped -- a
        truncated read window (``UnitScreen``'s size cap) can cut off
        before this message's own closing ``</div>``s ever arrive, and
        without this, that last, real (if partial) message would simply
        vanish rather than showing whatever of it was actually read."""
        return _render_chat_message(self._current) if self._current is not None else None


#: Prepended (never appended) when at least one whole leading (older)
#: block had to be dropped to fit ``max_chars``.
_OLDER_MESSAGES_TRUNCATED_NOTE = "[…earlier messages truncated — press e to export the full item…]\n\n"


def _join_keeping_the_newest(blocks: list[str], max_chars: int) -> tuple[str, bool]:
    """Joins ``blocks`` (oldest first, ``"\\n\\n"``-separated) into one
    text, dropping whole *leading* blocks -- never cutting one in half,
    unlike a raw character-count ``_truncate`` -- until the remainder
    fits ``max_chars``. Always keeps at least the single newest block
    regardless of its own length: a long last message is still more
    useful shown in full than dropped entirely. Returns ``(text,
    any_older_block_dropped)`` -- the caller prepends
    ``_OLDER_MESSAGES_TRUNCATED_NOTE`` when that flag is set, since
    dropping from the *front* means whatever's missing is always older
    messages, never newer ones."""
    kept: list[str] = []
    total = 0
    for block in reversed(blocks):
        added = len(block) + (2 if kept else 0)  # "\n\n" joiner, once there's already a newer block kept
        if kept and total + added > max_chars:
            break
        kept.append(block)
        total += added
    kept.reverse()
    return "\n\n".join(kept), len(kept) < len(blocks)


def render_teams_chat_preview(data: bytes, *, max_chars: int = 4000) -> str:
    """Parses a Teams/Chat channel/chat's exported HTML page
    (``units/content/saas_teams_chat.py::render_channel_html``'s own
    output shape — the unit's only exported form) into a chat-transcript
    -style plain text via ``_ChannelTranscriptParser``, rather than the
    generic ``render_html_preview``'s tag-stripped wall of text. ``data``
    is only ever a bounded prefix of the real bytes, trimmed to its own
    last real ``>`` first (see ``_drop_trailing_unterminated_tag``) —
    same truncation-safety treatment ``render_html_preview`` gives
    arbitrary HTML, since a long inline base64 sticker can just as
    easily span past the read window here.

    Truncation, when needed, always drops from the *oldest* end
    (``_join_keeping_the_newest``) — a channel's newest messages are
    what a user actually wants visible, the same reason
    ``core/unit/select.py``'s ``prefers_recent_content`` has
    ``UnitScreen._load_preview`` read ``data`` itself from the *end* of
    a long channel's content in the first place, rather than its start;
    this second truncation exists because that byte-level window can
    still hold more transcript text than ``max_chars``."""
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
    text, dropped_older = _join_keeping_the_newest(blocks, max_chars)
    if dropped_label is not None:
        placeholder = f"[{dropped_label}, ≥{format_bytes(len(data) - len(trimmed))}, not shown in preview]"
        text = f"{text}\n\n{placeholder}"
    if dropped_older:
        text = f"{_OLDER_MESSAGES_TRUNCATED_NOTE}{text}"
    return text
