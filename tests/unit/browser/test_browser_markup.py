"""Unit tests for ``safe()`` against the TUI's own ``Static`` entry point —
proves the actual failure mode ``safe()`` fixes directly against
``Static``, faster and more targeted than routing through a full Pilot
walkthrough for the same fact."""

from __future__ import annotations

import asyncio

import pytest
from textual.app import App, ComposeResult

# Textual has its own markup parser/exception, distinct from
# rich.errors.MarkupError — Static.update() raises *this* one, not
# Rich's; the two are unrelated exception classes, not sub/superclass
# of each other.
from textual.markup import MarkupError
from textual.widgets import Static

from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.markup import safe

# A real shape (dedup/pool.py's KeyRequiredError): the ref contains ``/``,
# which Rich's markup grammar cannot parse as a tag parameter.
_REAL_SHAPE_MESSAGE = (
    "'@ActiveProtectVault/@data/Pool/45/0.buk.99' is encrypted but no vault key was provided "
    "[ref=@ActiveProtectVault/@data/Pool/45/0.buk.99]"
)


class _OneStatic(App[None]):
    def compose(self) -> ComposeResult:
        yield Static("", id="s")


def _update(text: str) -> None:
    async def scenario() -> None:
        app = _OneStatic()
        async with app.run_test() as pilot:
            await pilot.pause()
            app.query_one("#s", Static).update(text)

    asyncio.run(scenario())


def _rendered(text: str) -> str:
    async def scenario() -> str:
        app = _OneStatic()
        async with app.run_test() as pilot:
            await pilot.pause()
            static = app.query_one("#s", Static)
            static.update(text)
            return str(static.render())

    return asyncio.run(scenario())


def test_unescaped_exception_text_crashes_static_update() -> None:
    """Establishes the failure this module exists to fix: an
    ``ApmRepoError``'s ``[ref=...]`` suffix isn't valid Rich markup once
    the ref contains ``/``, which every real path does."""
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    with pytest.raises(MarkupError):
        _update(f"[red]error:[/red] {exc}")


def test_safe_prevents_the_crash() -> None:
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    _update(f"[red]error:[/red] {safe(exc)}")  # must not raise


def test_safe_handles_the_exact_real_sample_shape() -> None:
    _update(f"[red]error:[/red] {safe(_REAL_SHAPE_MESSAGE)}")  # must not raise


# An uppercase-starting "[Key=" -- Textual's own markup grammar reads
# this as a real key=value tag attribute, then fails to resolve the
# value (TOKEN/VARIABLE_REF need a leading letter/"$", COLOR needs
# "#"/"rgb"/"hsl", and PERCENT's own leading "-" still needs a digit
# right after it). rich.markup.escape() itself leaves the leading "["
# untouched too (its own regex only escapes one immediately followed by
# a lowercase letter, "#", "/", or "@", so the uppercase "K" here
# defeats it) -- this is exactly why safe() escapes every "[" rather
# than reusing rich.markup.escape()'s narrower heuristic.
_KEYVALUE_BRACKET_MESSAGE = 'see [Ticket=-MVP-5002577"\nfor details'


def test_unescaped_uppercase_key_bracketed_text_crashes_static_update() -> None:
    """Establishes the second, distinct failure this module's ``safe()``
    fixes -- not the ``[ref=...]``-shaped one above (already escaped
    correctly by plain ``rich.markup.escape()`` too, since ``ref`` is
    lowercase), but an uppercase-starting ``key=value``-shaped bracket a
    real chat message can just as easily contain."""
    with pytest.raises(MarkupError):
        _update(_KEYVALUE_BRACKET_MESSAGE)


def test_safe_prevents_the_uppercase_key_bracket_crash() -> None:
    _update(safe(_KEYVALUE_BRACKET_MESSAGE))  # must not raise


def test_safe_preserves_a_literal_backslash_right_before_an_unclosed_bracket() -> None:
    """A literal backslash immediately before a ``[`` that never closes
    must come back through ``Static.update()`` unchanged, not with an
    extra backslash inserted: Rich's/Textual's fallback un-escaping for
    a non-tag-shaped bracket strips exactly one backslash from the run,
    not half of it, so doubling here (as a tag-shaped match needs) would
    round-trip wrong."""
    original = "a Windows-style path fragment \\[not-a-real-tag"
    assert _rendered(safe(original)) == original


def test_safe_is_a_plain_str_transform() -> None:
    assert safe("no special chars") == "no special chars"
    assert safe(42) == "42"
    assert safe("[not a tag]") == r"\[not a tag]"


__all__: list[str] = []
