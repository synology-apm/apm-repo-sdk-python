"""Unit tests for ``synology_apm_repo.sdk.presentation.markup.safe`` through
the CLI's Rich ``Console.print()``: an exception message's bracketed
``[ref=...]`` suffix must print verbatim."""

from __future__ import annotations

import io

from rich.console import Console
from rich.errors import MarkupError

from synology_apm_repo.sdk.errors import KeyRequiredError
from synology_apm_repo.sdk.presentation.markup import safe

# A real shape (dedup/pool/_bucket_reader.py's KeyRequiredError): the ref contains "/",
# which Rich's markup grammar cannot parse as a tag parameter.
_REAL_SHAPE_MESSAGE = (
    "'@ActiveProtectVault/@data/Pool/45/0.buk.99' is encrypted but no vault key was provided "
    "[ref=@ActiveProtectVault/@data/Pool/45/0.buk.99]"
)


def test_unescaped_exception_text_breaks_console_print() -> None:
    """Raw exception text through markup-enabled ``Console.print`` either
    raises or loses its bracketed suffix."""
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    buf = io.StringIO()
    console = Console(file=buf, width=200)
    try:
        console.print(f"[red]error:[/red] {exc}")
    except MarkupError:
        return  # the crash this module exists to prevent
    # Didn't crash, so Rich swallowed the suffix as an unclosed style span.
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


def test_safe_preserves_a_literal_backslash_right_before_an_unclosed_bracket() -> None:
    """Rich strips exactly one backslash before a non-tag ``[``, so ``safe()``
    adds one there rather than doubling the run as it does for a tag."""
    original = "a Windows-style path fragment \\[not-a-real-tag"
    buf = io.StringIO()
    console = Console(file=buf, width=200)
    console.print(safe(original))
    assert original in buf.getvalue()
