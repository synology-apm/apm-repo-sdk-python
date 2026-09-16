"""Unit tests for ``synology_apm_repo.sdk.presentation.markup.safe``
against the CLI's own ``Console.print()`` entry point — proves the actual
failure mode ``safe()`` fixes (mirrors
``tests/unit/browser/test_browser_markup.py`` in spirit; the CLI and TUI
hit the identical root cause, the bracketed ``[ref=...]``/``[spec=...]``
exception-message suffix, through two different Rich entry points —
``Console.print()`` here, ``Static.update()`` there)."""

from __future__ import annotations

import io

from rich.console import Console
from rich.errors import MarkupError

from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.markup import safe

# See test_browser_markup.py's own comment on this same constant for
# where this real shape comes from.
_REAL_SHAPE_MESSAGE = (
    "'@ActiveProtectVault/@data/Pool/45/0.buk.99' is encrypted but no vault key was provided "
    "[ref=@ActiveProtectVault/@data/Pool/45/0.buk.99]"
)


def test_unescaped_exception_text_breaks_console_print() -> None:
    """Establishes the failure this module exists to fix: printing raw,
    unescaped exception text through markup-enabled ``Console.print``
    either raises or silently truncates it, never shows it cleanly."""
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    buf = io.StringIO()
    console = Console(file=buf, width=200)
    try:
        console.print(f"[red]error:[/red] {exc}")
    except MarkupError:
        return  # the crash this module exists to prevent
    # Didn't crash — must have silently dropped the ref suffix instead
    # (rich/markup.py's own docstring documents this exact failure mode).
    assert str(exc) not in buf.getvalue()


def test_safe_preserves_the_full_message_through_console_print() -> None:
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    buf = io.StringIO()
    console = Console(file=buf, width=200)
    console.print(f"[red]error:[/red] {safe(exc)}")  # must not raise
    output = buf.getvalue()
    assert str(exc) in output
    assert "ref=@ActiveProtectVault/@data/Pool/45/0" in output


def test_safe_handles_the_exact_real_sample_shape() -> None:
    buf = io.StringIO()
    console = Console(file=buf, width=200)
    console.print(f"[red]error:[/red] {safe(_REAL_SHAPE_MESSAGE)}")  # must not raise
    assert _REAL_SHAPE_MESSAGE in buf.getvalue()


def test_safe_is_a_plain_str_transform() -> None:
    assert safe("no special chars") == "no special chars"
    assert safe(42) == "42"
    assert safe("[not a tag]") == r"\[not a tag]"


__all__: list[str] = []
