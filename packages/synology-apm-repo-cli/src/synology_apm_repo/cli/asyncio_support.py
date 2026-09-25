"""``typer_async`` — the one place every CLI command's ``asyncio.run()``
boilerplate lives, instead of each command module defining its own
``async def _run(): ...; asyncio.run(_run())`` wrapper; Typer itself has
no native async support (it inspects a command's signature to build the
CLI, then calls it as a plain sync function), so every ``async def``
command callback still needs a synchronous entry point. It's also the
one chokepoint every command callback passes through.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

import typer

from synology_apm_repo.cli.errors import fail_unexpected

_P = ParamSpec("_P")
_T = TypeVar("_T")


def typer_async(func: Callable[_P, Coroutine[Any, Any, _T]]) -> Callable[_P, _T]:
    """Wraps an ``async def`` Typer command callback so Typer can register
    and call it like any other command. ``functools.wraps`` preserves the
    original function's signature (via ``__wrapped__``), which is what
    Typer actually inspects to build ``--options``/arguments — the
    wrapper itself only ever needs ``*args``/``**kwargs``.

    An exception that reaches here without becoming a ``typer.Exit``
    (every expected failure already does, via ``fail()``/``unwrap()``) is
    treated as a bug and handed to ``fail_unexpected`` instead of reaching
    the terminal as a raw traceback. ``KeyboardInterrupt``/
    ``asyncio.CancelledError`` aren't ``Exception`` subclasses, so Ctrl-C
    handling (``export.py``'s own two-press cancel, or the plain default
    for every other command) is unaffected by this."""

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        try:
            return asyncio.run(func(*args, **kwargs))
        except typer.Exit:
            raise  # fail()/fail_unexpected() itself, or a plain clean exit — not a bug
        except Exception as exc:
            fail_unexpected(exc)

    return wrapper
