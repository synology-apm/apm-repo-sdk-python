"""Unit tests for ``synology_apm_repo.sdk.presentation.markup.safe`` — its
own contract (``str(value)`` with Rich markup escaped), owned at the SDK
layer that actually implements it, per ``ARCHITECTURE.md``'s Presentation
section ("CLI and TUI disagree is a bug by definition for anything in
this module"). ``tests/unit/cli/test_cli_markup.py`` and
``tests/unit/browser/test_browser_markup.py`` each additionally prove the
actual entry-point-specific failure mode ``safe()`` fixes (``Console.
print()`` silently truncating vs. ``Static.update()`` raising
``MarkupError``) — that needs a real ``Console``/``Static`` and stays
there; this file needs neither."""

from __future__ import annotations

from synology_apm_repo.sdk.presentation.markup import safe


def test_plain_text_round_trips_unchanged() -> None:
    assert safe("no special chars") == "no special chars"


def test_bracketed_text_is_escaped_so_it_no_longer_parses_as_a_tag() -> None:
    assert safe("[not a tag]") == r"\[not a tag]"


def test_non_str_value_is_stringified_then_escaped() -> None:
    assert safe(42) == "42"
    assert safe(None) == "None"


__all__: list[str] = []
