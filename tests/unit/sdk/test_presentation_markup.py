"""Unit tests for ``synology_apm_repo.sdk.presentation.markup.safe``'s own
contract. The failure modes it fixes in a real ``Console``/``Static`` are
proven in ``tests/unit/sdk/test_presentation_markup_rich_console.py`` and
``tests/unit/sdk/test_presentation_markup_textual_static.py``."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.presentation.markup import safe


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("no special chars", "no special chars", id="plain_text_round_trips_unchanged"),
        pytest.param("[not a tag]", r"\[not a tag]", id="bracketed_text_is_escaped_so_it_no_longer_parses_as_a_tag"),
        # rich.markup.escape() leaves this one untouched; Textual's own
        # tokenizer is stricter and still crashes Static.update() on it.
        pytest.param("[TICKET-1234]", r"\[TICKET-1234]", id="an_uppercase_starting_bracket_is_escaped_too"),
        pytest.param("[1] first item", r"\[1] first item", id="a_digit_starting_bracket_is_escaped_too"),
        # The user's own backslash is preserved as a literal plus the new
        # escaping one -- a single backslash would read as escaping the
        # bracket and silently drop it from the visible text.
        pytest.param(
            "a\\[b]", "a\\\\\\[b]", id="a_preexisting_backslash_before_a_bracket_is_doubled_not_left_ambiguous"
        ),
        pytest.param("a\\", "a\\\\", id="a_trailing_lone_backslash_is_doubled"),
        # A "[" that never closes is never tag-shaped, and both parsers
        # strip exactly one backslash from the run for this shape, so one
        # more backslash is added, not a doubling.
        pytest.param(
            "back\\[slash", "back\\\\[slash", id="a_preexisting_backslash_before_an_unclosed_bracket_is_not_doubled"
        ),
        # Same, for an uppercase-starting (also never tag-shaped) bracket.
        pytest.param(
            "back\\[TICKET-1234]",
            "back\\\\[TICKET-1234]",
            id="a_preexisting_backslash_before_an_uppercase_starting_bracket_is_not_doubled",
        ),
        # A tag-shaped "[" is where Rich/Textual decode a backslash run by
        # halving it, so its preceding run is doubled.
        pytest.param(
            "back\\[red]slash[/red]",
            "back\\\\\\[red]slash\\[/red]",
            id="a_preexisting_backslash_before_a_tag_shaped_bracket_still_doubles",
        ),
    ],
)
def test_brackets_and_backslashes_are_escaped(value: str, expected: str) -> None:
    assert safe(value) == expected


def test_non_str_value_is_stringified_then_escaped() -> None:
    assert safe(42) == "42"
    assert safe(None) == "None"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("הזמנה לאירוע", "⁨הזמנה לאירוע⁩", id="rtl_text_is_wrapped_in_a_bidi_isolate"),
        # Detection isn't first-character-only.
        pytest.param("Re: הזמנה לאירוע", "⁨Re: הזמנה לאירוע⁩", id="mixed_ltr_and_rtl_text_is_still_wrapped"),
        # Escaping runs before the isolate wraps the result, not the reverse.
        pytest.param("[1] הזמנה לאירוע", "⁨\\[1] הזמנה לאירוע⁩", id="rtl_text_with_brackets_is_escaped_then_wrapped"),
    ],
)
def test_rtl_text_is_isolated(value: str, expected: str) -> None:
    assert safe(value) == expected


def test_safe_of_repr_keeps_the_isolate_marks_invisible() -> None:
    """``repr()`` escapes the non-printable isolate marks into visible
    ``"\\u2068"`` text, so a call site wanting both uses ``safe(repr(value))``,
    never ``f"{safe(value)!r}"``."""
    name = "הזמנה"
    assert "\\u2068" in f"{safe(name)!r}"  # the trap
    assert "\\u2068" not in safe(repr(name))  # the fix


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # Stripping the ESC byte defangs the sequence; the leftover "[31m"
        # is not tag-shaped, so bracket escaping renders it literally.
        pytest.param("\x1b[31mred\x1b[0m", r"\[31mred\[0m", id="an_ansi_escape_sequence_loses_its_esc_byte"),
        pytest.param("ding\x07!", "ding!", id="a_bell_character_is_stripped"),
        pytest.param("abc\x08def", "abcdef", id="a_backspace_character_is_stripped"),
        pytest.param("a\x00b", "ab", id="a_null_byte_is_stripped"),
        pytest.param("a\x7fb", "ab", id="a_del_character_is_stripped"),
        pytest.param("a\tb\nc\rd", "a\tb\nc\rd", id="tab_newline_and_carriage_return_are_left_alone"),
        # An ESC byte right next to a bracket doesn't interfere with its escaping.
        pytest.param(
            "\x1b[not a tag]", r"\[not a tag]", id="control_characters_are_stripped_before_bracket_escaping_runs"
        ),
    ],
)
def test_control_characters_are_stripped(value: str, expected: str) -> None:
    assert safe(value) == expected
