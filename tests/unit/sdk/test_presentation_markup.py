"""Unit tests for ``synology_apm_repo.sdk.presentation.markup.safe`` — its
own contract (``str(value)`` with every literal ``[`` escaped), owned at
the SDK layer that actually implements it since the CLI and TUI must
never disagree on it (``ARCHITECTURE.md``'s Presentation section).
``tests/unit/cli/test_cli_markup.py`` and
``tests/unit/browser/test_browser_markup.py`` each additionally prove the
actual entry-point-specific failure mode ``safe()`` fixes (``Console.
print()`` silently truncating vs. ``Static.update()`` raising a markup
error) — that needs a real ``Console``/``Static`` and stays there; this
file needs neither."""

from __future__ import annotations

from synology_apm_repo.sdk.presentation.markup import safe


def test_plain_text_round_trips_unchanged() -> None:
    assert safe("no special chars") == "no special chars"


def test_bracketed_text_is_escaped_so_it_no_longer_parses_as_a_tag() -> None:
    assert safe("[not a tag]") == r"\[not a tag]"


def test_an_uppercase_starting_bracket_is_escaped_too() -> None:
    # rich.markup.escape() itself leaves this one untouched -- Textual's
    # own tokenizer is stricter and still crashes Static.update() on it.
    assert safe("[MVP-5002577]") == r"\[MVP-5002577]"


def test_a_digit_starting_bracket_is_escaped_too() -> None:
    assert safe("[1] first item") == r"\[1] first item"


def test_a_preexisting_backslash_before_a_bracket_is_doubled_not_left_ambiguous() -> None:
    # A literal single backslash right before the bracket must become
    # two (the original, preserved as a literal backslash) plus the new
    # escaping one -- never left as a single backslash, which would read
    # as "this backslash escapes the bracket" on its own, silently
    # dropping the user's own real backslash from the visible text.
    assert safe("a\\[b]") == "a\\\\\\[b]"


def test_a_trailing_lone_backslash_is_doubled() -> None:
    assert safe("a\\") == "a\\\\"


def test_a_preexisting_backslash_before_an_unclosed_bracket_is_not_doubled() -> None:
    """A ``[`` that never closes can never be mistaken for a real tag
    regardless of backslash count, so a pre-existing backslash right
    before it only needs one more backslash added, not doubled -- both
    parsers' own fallback for this shape only ever strips exactly one
    backslash total from the run, not half of it, so doubling here (as
    for a tag-shaped bracket) would round-trip back with one extra
    backslash instead."""
    assert safe("back\\[slash") == "back\\\\[slash"


def test_a_preexisting_backslash_before_an_uppercase_starting_bracket_is_not_doubled() -> None:
    """Same fix as the test above, for an uppercase-starting (also
    never tag-shaped) bracket."""
    assert safe("back\\[MVP-5002577]") == "back\\\\[MVP-5002577]"


def test_a_preexisting_backslash_before_a_tag_shaped_bracket_still_doubles() -> None:
    """Unlike the two cases above, a ``[`` that *does* go on to look
    like a real, closed tag (lowercase-starting, with an eventual real
    ``]``) still needs its own preceding backslash run doubled --
    that's the one shape where Rich's/Textual's own parsers decode a
    backslash run by halving it, so anything less than double would
    risk an *even* total being misread as a real, unescaped tag opener."""
    assert safe("back\\[red]slash[/red]") == "back\\\\\\[red]slash\\[/red]"


def test_non_str_value_is_stringified_then_escaped() -> None:
    assert safe(42) == "42"
    assert safe(None) == "None"


def test_rtl_text_is_wrapped_in_a_bidi_isolate() -> None:
    assert safe("הזמנה לאירוע") == "\u2068הזמנה לאירוע\u2069"


def test_mixed_ltr_and_rtl_text_is_still_wrapped() -> None:
    # Detection isn't first-character-only.
    assert safe("Re: הזמנה לאירוע") == "\u2068Re: הזמנה לאירוע\u2069"


def test_rtl_text_with_brackets_is_escaped_then_wrapped() -> None:
    # Escaping runs before the isolate wraps the result, not the reverse.
    assert safe("[1] הזמנה לאירוע") == "\u2068\\[1] הזמנה לאירוע\u2069"


def test_safe_of_repr_keeps_the_isolate_marks_invisible() -> None:
    """A caller quoting a ``safe()`` result with ``!r`` (Python's own
    ``repr()``) gets the isolate marks back as visible ``"\\u2068"``
    text -- ``repr()`` escapes any non-printable character, and the
    isolate marks are non-printable by design. ``safe(repr(value))``
    (``repr()`` first, to quote/escape the *raw* value; ``safe()`` last,
    so its own isolate marks are never re-escaped) is the correct order
    for a call site that wants both."""
    name = "הזמנה"
    assert "\\u2068" in f"{safe(name)!r}"  # the trap
    assert "\\u2068" not in safe(repr(name))  # the fix


__all__: list[str] = []
