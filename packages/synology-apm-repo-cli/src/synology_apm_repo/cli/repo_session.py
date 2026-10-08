"""The CLI's ``Session`` lifecycle: ``cli_session`` (shared by every
command that opens one) and, built on it, ``open_repo``/``opened_repo``,
which discover exactly one repository at a filesystem path or in a
``--profile``-resolved store.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator, Callable

from synology_apm_repo.cli.errors import fail_from_apm_error, require_one_of
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk import ApmRepoError, NotFoundError, ObjectStore, Repository, Session, TraceEvent
from synology_apm_repo.sdk.presentation import ProgressCallback
from synology_apm_repo.sdk.profiles import store_from_profile


@dataclasses.dataclass(frozen=True, slots=True)
class CliSession:
    """A ``Session`` with the progress and trace callbacks the CLI's flags select."""

    session: Session
    progress: ProgressCallback
    trace: Callable[[TraceEvent], None] | None


@contextlib.asynccontextmanager
async def cli_session(state: CliState, *, error_prefix: Callable[[], str] | None = None) -> AsyncIterator[CliSession]:
    """Yield a ``CliSession``; an ``ApmRepoError`` from the body becomes
    ``fail_from_apm_error``'s CLI error exit (``error_prefix()``, read at
    failure time, leads its message), and the live progress line is cleared
    and the session closed however the body ends."""
    session = Session()
    try:
        yield CliSession(session, build_progress_meter(state).update, build_trace_callback(state))
    except ApmRepoError as exc:
        fail_from_apm_error(exc, state, prefix=error_prefix() if error_prefix is not None else "")
    finally:
        finish_live_progress(state)
        await session.close()


async def open_single_repo(
    session: Session,
    fs_path: str,
    key: str | None,
    *,
    store: ObjectStore | None = None,
    progress: ProgressCallback | None = None,
    trace: Callable[[TraceEvent], None] | None = None,
) -> Repository:
    """Discover exactly one repository at ``fs_path``.

    ``store``, when given (a ``--profile``-resolved ``ObjectStore``), is
    scanned with ``fs_path`` as the store-relative ``root``; otherwise
    ``fs_path`` is a local filesystem path.

    Raises:
        NotFoundError: No repository, or more than one, is found under
            ``fs_path`` (sibling repo-ids in one bucket are one
            ``Repository`` with several catalogs, not several).
    """
    if store is not None:
        repos = await session.open(store, key, root=fs_path, progress=progress, trace=trace)
    else:
        repos = await session.open(fs_path, key, progress=progress, trace=trace)
    if not repos:
        raise NotFoundError(f"no repository found at {fs_path!r}", ref=fs_path)
    if len(repos) > 1:
        roots = ", ".join(repo.layout.repo_root for repo in repos)
        raise NotFoundError(
            f"{len(repos)} repositories found under {fs_path!r} ({roots}) — point at one specifically",
            ref=fs_path,
        )
    return repos[0]


async def open_repo(cli: CliSession, fs_path: str, key: str | None, *, profile: str | None) -> Repository:
    """``open_single_repo`` through ``cli``'s session and callbacks: ``fs_path``
    is a local path, or with ``profile`` a root within that profile's store."""
    store = await store_from_profile(profile) if profile is not None else None
    return await open_single_repo(cli.session, fs_path, key, store=store, progress=cli.progress, trace=cli.trace)


@contextlib.asynccontextmanager
async def opened_repo(
    fs_path: str, key: str | None, *, profile: str | None, state: CliState
) -> AsyncIterator[Repository]:
    """Open exactly one repository at ``fs_path`` via ``open_repo``
    inside a ``cli_session`` and yield it; an ``ApmRepoError`` from the open
    or from the caller's own ``async with`` body is the CLI error exit.

    Callers needing ``require_one_of(repo, profile)``-style validation of
    REPO/``--profile`` run it themselves before entering this context
    manager — a bare argument mismatch isn't an ``ApmRepoError``."""
    async with cli_session(state) as cli:
        yield await open_repo(cli, fs_path, key, profile=profile)


def opened_repo_or_profile(
    repo: str | None, key: str | None, *, profile: str | None, state: CliState
) -> contextlib.AbstractAsyncContextManager[Repository]:
    """``require_one_of`` (REPO XOR ``--profile``), checked eagerly before
    any repository is opened, then ``opened_repo``; for
    ``doctor``/``key``/``verify``."""
    require_one_of(repo, profile)
    return opened_repo(repo or "", key, profile=profile, state=state)
