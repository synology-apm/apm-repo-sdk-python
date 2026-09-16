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


def test_unescaped_exception_text_crashes_static_update() -> None:
    """Establishes the failure this module exists to fix — see the
    module's own docstring for the full explanation (an ``ApmRepoError``'s
    ``[ref=...]`` suffix isn't valid Rich markup once the ref
    contains ``/``, which every real path does)."""
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


def test_safe_is_a_plain_str_transform() -> None:
    assert safe("no special chars") == "no special chars"
    assert safe(42) == "42"
    assert safe("[not a tag]") == r"\[not a tag]"


__all__: list[str] = []
