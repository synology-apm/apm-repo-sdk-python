"""Error-reporting helpers and argument guards shared by every command
module."""

from __future__ import annotations

import enum
import traceback
from collections.abc import Awaitable
from typing import NoReturn

import typer

from synology_apm_repo.cli.consoles import err_console as err_console
from synology_apm_repo.cli.progress_render import finish_live_progress
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk import ApmRepoError, KeyMismatchError, KeyRequiredError
from synology_apm_repo.sdk.presentation import safe

#: This package's pyproject.toml "Issue Tracker" Project-URL, as a literal:
#: fail_unexpected() must work even when reading package metadata is what
#: crashed.
_ISSUE_TRACKER_URL = "https://github.com/synology-apm/apm-repo-sdk-python/issues"


class ExitCode(enum.IntEnum):
    """Every exit status a command ends with; a usage error is Click's own 2."""

    OK = 0
    ERROR = 1
    FINDINGS = 3
    """``verify`` ran to completion and found at least one unresolved problem
    (``VerifySummary.problem_count``)."""
    CANCELLED = 130
    """Ctrl-C cancelled the command (128 + SIGINT, the shell convention)."""


def fail(message: object, *, cause: BaseException | None = None) -> NoReturn:
    """Print ``[red]error:[/red] {message}`` to stderr and exit with ``ExitCode.ERROR``.

    ``cause``, when given the exception a caller just caught, is chained
    via ``raise ... from cause`` so the original exception/traceback
    survives to an uncaught-exception dump. Omit ``cause`` for a
    validation failure with no exception to chain (e.g. a bad CLI
    argument combination)."""
    err_console.print(f"[red]error:[/red] {safe(message)}")
    raise typer.Exit(code=ExitCode.ERROR) from cause


def friendly_message(exc: ApmRepoError, *, verbose: bool = False) -> str:
    """Rephrases ``KeyManager.require_verified()``'s messages into CLI
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
    pointer to file one, then exits with ``ExitCode.ERROR``."""
    err_console.print(f"[red]internal error:[/red] {exc.__class__.__name__}: {safe(str(exc))}")
    err_console.print("".join(traceback.format_exception(exc)), style="dim", highlight=False)
    err_console.print(f"[dim]this looks like a bug — please file it at {_ISSUE_TRACKER_URL}[/dim]")
    raise typer.Exit(code=ExitCode.ERROR) from exc


def fail_from_apm_error(exc: ApmRepoError, state: CliState, *, prefix: str = "") -> NoReturn:
    """The shared tail ``repo_session.cli_session`` and ``dump.py``'s
    ``_resolved_store`` run once an ``ApmRepoError`` ends them: clear any
    live progress line, then ``fail()`` with ``friendly_message``'s
    verbose-gated rendering, led by ``prefix``."""
    finish_live_progress(state)
    fail(f"{prefix}{friendly_message(exc, verbose=state.verbose)}", cause=exc)


async def unwrap[T](awaitable: Awaitable[T], *, verbose: bool = False) -> T:
    """``await awaitable``, turning an ``ApmRepoError`` it raises into
    ``fail``'s CLI error exit, worded by ``friendly_message(exc, verbose=verbose)``."""
    try:
        return await awaitable
    except ApmRepoError as exc:
        fail(friendly_message(exc, verbose=verbose), cause=exc)


def require_one_of(repo: str | None, profile: str | None) -> None:
    """Refuse to continue unless exactly one of REPO/``--profile`` is
    given — the guard shared by ``doctor``/``key``/``verify``."""
    if (repo is None) == (profile is None):
        fail("give exactly one of REPO or --profile")
