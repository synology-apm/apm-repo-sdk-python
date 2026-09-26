"""``typer_async`` — the one place every CLI command's ``asyncio.run()``
boilerplate lives. Typer calls each callback as a plain sync function, so
every ``async def`` command needs a synchronous entry point; this is also
the one chokepoint every command callback passes through.
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
    and call it like a plain sync function. Any exception that escapes
    without becoming a ``typer.Exit`` is treated as a bug and handed to
    ``fail_unexpected``; ``KeyboardInterrupt``/``asyncio.CancelledError``
    aren't ``Exception`` subclasses, so Ctrl-C handling elsewhere is
    unaffected."""

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        try:
            return asyncio.run(func(*args, **kwargs))
        except typer.Exit:
            raise  # fail()/fail_unexpected() itself, or a plain clean exit — not a bug
        except Exception as exc:
            fail_unexpected(exc)

    return wrapper
