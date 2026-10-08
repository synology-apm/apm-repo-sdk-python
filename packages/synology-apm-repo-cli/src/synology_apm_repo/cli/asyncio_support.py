"""``typer_async``: the synchronous entry point every ``async def`` command
callback runs through, since Typer calls callbacks as plain functions."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable, Coroutine
from typing import Any

import typer

from synology_apm_repo.cli.errors import ExitCode, err_console, fail_unexpected


def typer_async[**P, T](func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, T]:
    """Wraps an ``async def`` Typer command callback so Typer can register
    and call it like a plain sync function. Any exception that escapes
    without becoming a ``typer.Exit`` is treated as a bug and handed to
    ``fail_unexpected``. A Ctrl-C a command doesn't handle itself exits
    ``ExitCode.CANCELLED`` (``export`` handles its first one, to clean up)."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return asyncio.run(func(*args, **kwargs))
        except typer.Exit:
            raise  # fail()/fail_unexpected() itself, or a plain clean exit — not a bug
        except KeyboardInterrupt:
            err_console.print("[yellow]cancelled[/yellow]")
            raise typer.Exit(code=ExitCode.CANCELLED) from None
        except Exception as exc:  # noqa: BLE001
            fail_unexpected(exc)

    return wrapper
