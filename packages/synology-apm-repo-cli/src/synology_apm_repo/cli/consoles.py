"""The CLI's two Rich consoles, shared by every module: ``console`` for a
command's own report on stdout, ``err_console`` for errors, warnings and
progress on stderr (``progress_render.py``'s stdout/stderr rule). One
instance each, so the terminal is probed once."""

from __future__ import annotations

from rich.console import Console

console = Console()
err_console = Console(stderr=True)
