"""``render()``, every command's ``--json``-or-human final output, and the
pager it optionally pipes human-mode output through when stdout is a
terminal. Commands whose output scales with item-tree content
(``ls``/``tree``/``dump``/``verify``) page; ``doctor``/``key``, whose output
is bounded, don't.
"""

from __future__ import annotations

import contextlib
import os
import shlex
import subprocess
from collections.abc import Callable, Iterator
from typing import override

from rich.console import Console
from rich.pager import Pager

from synology_apm_repo.cli.state import CliState

_DEFAULT_PAGER = "less -FIRX"
"""``-F``: don't page content that fits on one screen. ``-I``:
case-insensitive search. ``-R``: pass Rich's ANSI colors through. ``-X``:
leave the output in scrollback after the pager exits."""


def pager_argv() -> list[str] | None:
    """The pager's argv from ``$PAGER`` (default ``_DEFAULT_PAGER``), or
    ``None`` ("don't page") when ``$PAGER`` is set to an empty string."""
    raw = os.environ.get("PAGER", _DEFAULT_PAGER)
    return shlex.split(raw) if raw else None


class _SubprocessPager(Pager):
    """A Rich ``Pager`` that pipes ``content`` to ``argv``, printing it
    directly instead when ``argv[0]`` isn't found."""

    def __init__(self, argv: list[str]) -> None:
        self._argv = argv

    @override
    def show(self, content: str) -> None:
        try:
            subprocess.run(self._argv, input=content, text=True, check=False)
        except FileNotFoundError:
            print(content, end="")  # noqa: T201 - plain text, past Rich, for the pager


@contextlib.contextmanager
def paged(console: Console) -> Iterator[None]:
    """Page everything printed inside the block; a no-op unless stdout is a
    terminal and ``pager_argv()`` resolves a pager."""
    if not console.is_terminal:
        yield
        return
    argv = pager_argv()
    if argv is None:
        yield
        return
    with console.pager(pager=_SubprocessPager(argv), styles=True):
        yield


def render(console: Console, state: CliState, *, json: object, human: Callable[[], None], page: bool = False) -> None:
    """Print ``json`` under ``--json``, else call ``human()`` — inside
    ``paged(console)`` when ``page`` is true."""
    if state.json:
        console.print_json(data=json)
        return
    if page:
        with paged(console):
            human()
    else:
        human()
