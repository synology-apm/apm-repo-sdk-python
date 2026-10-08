"""Unit tests for ``safe()`` against the TUI's own ``Static.update()``."""

from __future__ import annotations

import asyncio

import pytest
from textual.app import App, ComposeResult

# Static.update() raises Textual's own MarkupError, unrelated to Rich's.
from textual.markup import MarkupError
from textual.widgets import Static

from support.pilot import wait_until
from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.markup import safe

# A real shape (dedup/pool/_bucket_reader.py's KeyRequiredError): the ref contains ``/``,
# which Textual's markup grammar cannot parse as a tag parameter.
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
            await wait_until(pilot, lambda: app.query("#s"))
            app.query_one("#s", Static).update(text)

    asyncio.run(scenario())


def _rendered(text: str) -> str:
    async def scenario() -> str:
        app = _OneStatic()
        async with app.run_test() as pilot:
            await wait_until(pilot, lambda: app.query("#s"))
            static = app.query_one("#s", Static)
            static.update(text)
            return str(static.render())

    return asyncio.run(scenario())


def test_unescaped_exception_text_crashes_static_update() -> None:
    """An ``ApmRepoError``'s ``[ref=...]`` suffix isn't valid markup once the
    ref contains ``/``, which every real path does."""
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    with pytest.raises(MarkupError, match="Expected markup value"):
        _update(f"[red]error:[/red] {exc}")


def test_safe_prevents_the_crash() -> None:
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    assert _rendered(f"[red]error:[/red] {safe(exc)}") == f"error: {exc}"


def test_safe_handles_the_exact_real_sample_shape() -> None:
    assert _rendered(f"[red]error:[/red] {safe(_REAL_SHAPE_MESSAGE)}") == f"error: {_REAL_SHAPE_MESSAGE}"


# Textual's grammar reads an uppercase-starting "[Key=" as a key=value tag,
# and rich.markup.escape() doesn't escape it (it only escapes "[" before a
# lowercase letter, "#", "/" or "@") -- why safe() escapes every "[".
_KEYVALUE_BRACKET_MESSAGE = 'see [Ticket=-TICKET-1234"\nfor details'


def test_unescaped_uppercase_key_bracketed_text_crashes_static_update() -> None:
    with pytest.raises(MarkupError, match="Expected markup value"):
        _update(_KEYVALUE_BRACKET_MESSAGE)


def test_safe_prevents_the_uppercase_key_bracket_crash() -> None:
    assert _rendered(safe(_KEYVALUE_BRACKET_MESSAGE)) == _KEYVALUE_BRACKET_MESSAGE


def test_safe_preserves_a_literal_backslash_right_before_an_unclosed_bracket() -> None:
    """The un-escaping of a non-tag-shaped bracket strips exactly one
    backslash, so doubling it here (as a tag-shaped match needs) would
    round-trip wrong."""
    original = "a Windows-style path fragment \\[not-a-real-tag"
    assert _rendered(safe(original)) == original
