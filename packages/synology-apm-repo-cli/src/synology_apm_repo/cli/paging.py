"""Pipes a command's human-mode console output through a pager when
stdout is a real terminal — never for ``--json`` output (machine-consumable)
or a non-tty stdout (piped/redirected — there's nothing to page). Used by
``tree``/``dump``, the two command families whose output genuinely scales
unbounded (see each module's own docstring); ``ls``/``doctor``/``key``/
``verify`` stay one-shot prints, deliberately — their output is normally
short.
"""

from __future__ import annotations

import contextlib
import os
import shlex
import subprocess
from collections.abc import Iterator

from rich.console import Console
from rich.pager import Pager

_DEFAULT_PAGER = "less -FIRX"
"""``-F``: don't page at all if the content already fits on one screen —
the reason this needs no separate "is the output actually long" check of
its own. ``-R``: pass through the ANSI color codes Rich already wrote
instead of showing them as literal escape junk. ``-I``: case-insensitive
search. ``-X``: leave the output in scrollback after the pager exits."""


def pager_argv() -> list[str] | None:
    """argv for the pager to shell out to, honoring ``$PAGER`` with a
    sensible default when it's unset. ``None`` means "don't page" — only
    when ``$PAGER`` is explicitly set to an empty string, the standard way
    a user opts out of paging entirely."""
    raw = os.environ.get("PAGER", _DEFAULT_PAGER)
    return shlex.split(raw) if raw else None


class _SubprocessPager(Pager):
    """Rich's own ``Pager`` protocol is just a ``show(content: str) ->
    None`` method — this shells ``content`` out to ``argv`` the same way
    ``git``/``man`` do. Falls back to printing directly (no pager) if
    ``argv[0]`` isn't an executable found on ``$PATH`` — a missing pager
    must never crash the command it's wrapping."""

    def __init__(self, argv: list[str]) -> None:
        self._argv = argv

    def show(self, content: str) -> None:
        try:
            subprocess.run(self._argv, input=content, text=True, check=False)
        except FileNotFoundError:
            print(content, end="")


@contextlib.contextmanager
def paged(console: Console) -> Iterator[None]:
    """Entered around a command's whole human-mode render block —
    ``with paged(console): _render_human(...)``. A no-op (prints
    normally) unless stdout is a real terminal and a pager is actually
    resolved."""
    if not console.is_terminal:
        yield
        return
    argv = pager_argv()
    if argv is None:
        yield
        return
    with console.pager(pager=_SubprocessPager(argv), styles=True):
        yield
