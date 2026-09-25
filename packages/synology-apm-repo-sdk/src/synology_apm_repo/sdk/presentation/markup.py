"""``safe()`` — escape arbitrary text before it reaches a Rich-markup-parsing
call, shared by the CLI's ``rich.console.Console.print()`` calls, the TUI's
``Static.update()``/``Static(...)`` calls (``markup=True`` is the default
for both), and a TUI ``Tree``/``DataTable`` label or cell value (each
re-parses a plain ``str`` as markup too — ``Tree.process_label``/
``DataTable``'s own ``default_cell_formatter`` both call
``Text.from_markup``). Any genuinely dynamic text reaching one of those
call sites (an exception's ``str()``, a real backup-derived display name,
a user-typed path) risks breaking whichever entry point receives it
unescaped: a TUI widget crashes outright with a markup error on an
unmatched closing tag (``"a[/]b"``, say) or an unresolvable tag body,
while the CLI's ``Console.print()`` sometimes instead silently swallows a
bracketed suffix as an unclosed style span.

Deliberately not a bare ``rich.markup.escape()`` call — that function's
own regex only escapes a ``[`` immediately followed by a lowercase
letter/``#``/``/``/``@`` (a real Rich/Textual tag's own naming
convention), leaving any *other* ``[`` (followed by an uppercase letter,
a digit, punctuation, ...) untouched on the assumption that no real
parser would ever treat it as a tag either. Textual's own markup
tokenizer (``textual.markup``'s ``expect_markup``) doesn't share that
assumption: its ``open_tag`` token matches *any* unescaped ``[``,
regardless of what follows it, and raises ``textual.markup.MarkupError``
once it fails to resolve the rest as a real tag body — an ordinary
uppercase bracketed reference like ``"[MVP-5002577]"``, which
``rich.markup.escape()`` leaves untouched, still crashes
``Static.update()`` this way. Plain Rich (the
CLI's own entry point) is the more lenient parser matching ``escape()``'s
own heuristic, so escaping every ``[`` here — not just the ones that
look like a real tag name — is strictly safe for the CLI side too: a
bracket Rich's own parser wouldn't have treated as a tag renders
identically whether escaped or not.

Never renders as a real, unintended tag either way — the one narrow shape
where that safety costs an exact backslash-count round trip is a
tag-shaped bracket, whose existing backslash run gets doubled so both
parsers still read it as the literal, escaped form (they only do so when
the run is odd).

A value containing a strong right-to-left character (Hebrew, Arabic, ...)
additionally comes back wrapped in a Unicode bidi isolate (``FSI``/``PDI``,
zero-width): placed unisolated next to a fixed-width table's own column
borders, the terminal's own bidi reordering (Unicode UAX #9) can pull
those neutral border/padding characters into the RTL run's reordering
scope, desyncing the terminal's rendered layout from what Rich/Textual's
compositor believes it drew. ``FSI`` (not ``LRI``/``RLI``) keeps the
isolate's own internal direction auto-detected from its first strong
character, so the wrapped text still reads in its natural order — only
the surrounding context is shielded from it.
"""

from __future__ import annotations

import re
import unicodedata
from re import Match

#: U+2068 FIRST STRONG ISOLATE / U+2069 POP DIRECTIONAL ISOLATE, wrapped
#: around a value containing a strong RTL character (rationale above).
_RTL_ISOLATE_START = "\u2068"
_RTL_ISOLATE_END = "\u2069"

#: Any run of backslashes immediately followed by a literal ``[``, plus
#: an optional second group -- ``rich.markup.escape()``'s own tag-shape
#: pattern, reused here as a lookahead, not a gate: every bracket still
#: gets escaped either way, so this just lets ``_escape_bracket`` tell
#: which of its two cases applies.
_BRACKET_RE = re.compile(r"(\\*)\[([a-z#/@][^[]*?\])?")


def _escape_bracket(match: Match[str]) -> str:
    """Rich's/Textual's own parsers unescape a backslash-escaped ``[``
    two different ways depending on whether the text right after it
    still resolves to a real, closed tag shape once unescaped
    (``tag_shape`` matched or not): a tag-shaped match gets its existing
    backslash run doubled to stay odd-counted, the only form both parsers
    read as an escaped literal; a non-tag-shaped match gets exactly one
    backslash added, since neither parser's tag grammar could ever read
    it as a tag either way."""
    backslashes, tag_shape = match.groups()
    if tag_shape:
        # Both parsers only ever recognize this as "the escaped,
        # literal form of that tag" when the backslash run immediately
        # before it is *odd* -- an *even* run is instead read as a
        # real, unescaped tag opener. Doubling any backslashes already
        # present (Rich's own convention for this exact shape) always
        # yields an odd count, so this is never misread as a real tag
        # regardless of how many backslashes came before it.
        return f"{backslashes}{backslashes}\\[{tag_shape}"
    # Unclosed, or the content after "[" could never look like a tag at
    # all -- the common case for arbitrary dynamic text, e.g. a
    # Teams/Chat message body. Neither parser's own tag grammar can
    # ever match here no matter the backslash count, so the doubling
    # above is unnecessary -- worse, actively wrong, since both
    # parsers' own fallback for *this* shape only ever strips exactly
    # one backslash total from the run, not half of it. One extra
    # backslash (not doubled) is what round-trips correctly.
    return f"{backslashes}\\["


def _contains_rtl(text: str) -> bool:
    """Whether ``text`` has at least one strong right-to-left character
    (bidirectional category ``R``/``AL``) -- the property ``safe()``
    wraps in a bidi isolate."""
    # ASCII can never contain one -- str.isascii() is a cheap, exact
    # pre-filter that skips the per-character unicodedata lookup below
    # for the common case (``safe()`` runs on every dynamic string
    # reaching a rendered widget, most of it plain ASCII).
    if text.isascii():
        return False
    return any(unicodedata.bidirectional(char) in ("R", "AL") for char in text)


def safe(value: object) -> str:
    """``str(value)`` with every literal ``[`` escaped, so it can be
    interpolated into an f-string alongside literal ``[color]`` tags (or
    passed alone) without risking a markup-parse failure — the literal
    tags written by the calling code are untouched since this only
    affects the *argument*, applied before the f-string is built via
    ``_escape_bracket``. A value containing a strong right-to-left
    character comes back additionally wrapped in a bidi isolate too."""
    text = str(value)
    escaped = _BRACKET_RE.sub(_escape_bracket, text)
    # A trailing single backslash (one this substitution never touches,
    # since it's not immediately followed by ``[``) still needs doubling
    # here — same defensive trailing-backslash handling
    # ``rich.markup.escape()`` itself applies, kept for parity since it's
    # a property of the whole string's own ending, not of this
    # function's particular bracket regex.
    if escaped.endswith("\\") and not escaped.endswith("\\\\"):
        escaped += "\\"
    if _contains_rtl(text):
        escaped = f"{_RTL_ISOLATE_START}{escaped}{_RTL_ISOLATE_END}"
    return escaped
