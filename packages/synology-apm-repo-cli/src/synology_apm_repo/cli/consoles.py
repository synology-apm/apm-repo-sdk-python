"""The CLI's two Rich consoles, shared by every module: ``console`` for a
command's own report on stdout, ``err_console`` for errors, warnings and
progress on stderr (``progress_render.py``'s stdout/stderr rule). One
instance each, so the terminal is probed once. ``main()`` makes both
streams escape what their encoding can't represent."""

from __future__ import annotations

import io
import sys

from rich.console import Console

console = Console()
err_console = Console(stderr=True)


def escape_unencodable_output() -> None:
    """Makes stdout and stderr write a character their encoding can't
    represent (an emoji in a Windows pipe's ANSI code page) as a
    ``\\U...`` escape instead of raising ``UnicodeEncodeError``. The
    encoding is kept: the shell decodes piped output with it."""
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper):
            stream.reconfigure(errors="backslashreplace")
