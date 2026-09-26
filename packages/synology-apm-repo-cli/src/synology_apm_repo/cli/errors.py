"""Small cross-cutting CLI-command guards and error-reporting helpers,
shared by every command module rather than each defining its own
``err_console``/``fail()`` pair."""

from __future__ import annotations

import traceback
from collections.abc import Awaitable
from typing import NoReturn, TypeVar

import typer
from rich.console import Console

from synology_apm_repo.cli.progress_render import finish_live_progress
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk.errors import ApmRepoError, KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.presentation.markup import safe

err_console = Console(stderr=True)
_T = TypeVar("_T")

#: Mirrors pyproject.toml's own "Issue Tracker" Project-URL — duplicated
#: here rather than read back from package metadata since fail_unexpected()
#: must work even when whatever crashed is the metadata-reading machinery
#: itself.
_ISSUE_TRACKER_URL = "https://github.com/synology-apm/apm-repo-sdk-python/issues"


def fail(message: object, *, cause: BaseException | None = None) -> NoReturn:
    """Print ``[red]error:[/red] {message}`` to stderr and exit(1).

    ``cause``, when given the exception a caller just caught, is chained
    via ``raise ... from cause`` so the original exception/traceback
    survives to an uncaught-exception dump. Omit ``cause`` for a
    validation failure with no exception to chain (e.g. a bad CLI
    argument combination)."""
    err_console.print(f"[red]error:[/red] {safe(message)}")
    raise typer.Exit(code=1) from cause


def friendly_message(exc: ApmRepoError, *, verbose: bool = False) -> str:
    """Rephrases ``Repository._require_key_verified()``'s messages into CLI
    language (``--key``, or the ``key`` subcommand) — only when ``ref`` is
    unset (the whole-repository gate, not a specific file); a
    ``KeyRequiredError``/``KeyMismatchError`` with its own ``ref`` falls
    through to the general case. Every other ``ApmRepoError`` renders
    ``exc.safe_message`` by default, or ``str(exc)`` under ``--verbose``."""
    if isinstance(exc, (KeyRequiredError, KeyMismatchError)) and exc.ref is None:
        if isinstance(exc, KeyRequiredError):
            return "this repository is encrypted — pass --key, or verify one with `synology-apm-repo-cli key`"
        return "the key given was rejected — check --key, or verify one with `synology-apm-repo-cli key`"
    return str(exc) if verbose else exc.safe_message


def fail_unexpected(exc: Exception) -> NoReturn:
    """Last-resort handler for an exception no command's own error handling
    caught — a bug, not an expected ``ApmRepoError``. Prints the exception,
    its full traceback (dim, so a bug report has something to paste), and a
    pointer to file one, then exits 1."""
    err_console.print(f"[red]internal error:[/red] {exc.__class__.__name__}: {safe(str(exc))}")
    err_console.print("".join(traceback.format_exception(exc)), style="dim", highlight=False)
    err_console.print(f"[dim]this looks like a bug — please file it at {_ISSUE_TRACKER_URL}[/dim]")
    raise typer.Exit(code=1) from exc


def fail_from_apm_error(exc: ApmRepoError, state: CliState, *, prefix: str = "") -> NoReturn:
    """The shared tail every Session/store-open skeleton
    (``repo_session.opened_repo``, ``export.py``, ``dump.py``'s
    ``_resolved_store``, ``profile.py``'s ``_verify_connectivity``) runs
    once an ``ApmRepoError`` ends it: clear any live progress line, then
    ``fail()`` with ``friendly_message``'s verbose-gated rendering.
    ``prefix`` exists only for ``profile.py``'s "connectivity check
    failed: " wording; every other caller leaves it blank."""
    finish_live_progress(state)
    fail(f"{prefix}{friendly_message(exc, verbose=state.verbose)}", cause=exc)


async def unwrap(awaitable: Awaitable[_T], *, verbose: bool = False) -> _T:
    """``await awaitable``, translating any ``ApmRepoError`` it raises into
    ``fail``'s standard CLI error exit — the "await one SDK call, fail() on
    ApmRepoError" shape several commands repeat verbatim for a single
    call whose failure should end the command immediately. ``verbose``
    (each caller's own ``state.verbose``) is forwarded to
    ``friendly_message`` unchanged."""
    try:
        return await awaitable
    except ApmRepoError as exc:
        fail(friendly_message(exc, verbose=verbose), cause=exc)


def require_one_of(repo: str | None, profile: str | None) -> None:
    """Refuse to continue unless exactly one of REPO/``--profile`` is
    given — the guard shared by ``doctor``/``key``/``verify`` (no command
    varies its own argument name here, so this doesn't take one)."""
    if (repo is None) == (profile is None):
        fail("give exactly one of REPO or --profile")
