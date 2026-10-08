"""``safe()``: escape arbitrary text before it reaches a Rich-markup-parsing
call (the CLI's ``Console.print()``, the TUI's ``Static``/``Tree``/
``DataTable``). Unescaped dynamic text can crash a Textual widget or be
silently mangled by Rich's parser.

Every ``[`` is escaped, not just tag-shaped ones as ``rich.markup.escape()``
does: Textual's tokenizer treats any unescaped ``[`` as an open tag and
crashes on an ordinary bracketed reference like ``"[TICKET-1234]"``.

C0 control characters and DEL (except tab/newline/CR) are stripped, so an
escape sequence embedded in repository-derived text (a filename, a chat
message) never reaches the terminal.

A value containing a strong right-to-left character is wrapped in a
zero-width bidi isolate (``FSI``/``PDI``): unisolated, the terminal's bidi
reordering can pull neighbouring table borders into the RTL run and desync
the rendered layout from what Rich/Textual drew.
"""

from __future__ import annotations

import re
import unicodedata
from re import Match

#: U+2068 FIRST STRONG ISOLATE / U+2069 POP DIRECTIONAL ISOLATE, wrapped
#: around a value containing a strong RTL character (rationale above).
_RTL_ISOLATE_START = "\u2068"
_RTL_ISOLATE_END = "\u2069"

#: A backslash run, a literal ``[``, and optionally the rest of a tag shape
#: (``rich.markup.escape()``'s pattern). Every bracket is escaped; the
#: second group only tells ``_escape_bracket`` which case applies.
_BRACKET_RE = re.compile(r"(\\*)\[([a-z#/@][^[]*?\])?")

#: C0 control characters and DEL, excluding tab/newline/CR.
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _escape_bracket(match: Match[str]) -> str:
    """Escapes one ``[``. Rich and Textual read a tag-shaped ``[`` as
    literal only after an odd backslash run, so that run is doubled plus
    one; for any other ``[`` their fallback strips exactly one backslash,
    so exactly one is added."""
    backslashes, tag_shape = match.groups()
    if tag_shape:
        return f"{backslashes}{backslashes}\\[{tag_shape}"
    return f"{backslashes}\\["


def _contains_rtl(text: str) -> bool:
    """Whether ``text`` has a strong right-to-left character
    (bidirectional category ``R``/``AL``)."""
    if text.isascii():
        return False
    return any(unicodedata.bidirectional(char) in ("R", "AL") for char in text)


def safe(value: object) -> str:
    """``str(value)`` made safe to interpolate into Rich markup beside
    literal ``[color]`` tags: every ``[`` escaped, control characters
    stripped, and an RTL-containing value wrapped in a bidi isolate."""
    text = _CONTROL_CHAR_RE.sub("", str(value))
    escaped = _BRACKET_RE.sub(_escape_bracket, text)
    # A lone trailing backslash would escape whatever markup follows the
    # interpolated value; rich.markup.escape() doubles it too.
    if escaped.endswith("\\") and not escaped.endswith("\\\\"):
        escaped += "\\"
    if _contains_rtl(text):
        escaped = f"{_RTL_ISOLATE_START}{escaped}{_RTL_ISOLATE_END}"
    return escaped
