"""Repository-open lifecycle shared by ``ls``/``tree``/``cat``/``key``/
``verify``/``doctor``: discover exactly one repository at a filesystem
path or ``--profile``-resolved store, translate any ``ApmRepoError`` into
``fail()``'s standard CLI error exit, and always close the ``Session``
after. ``export.py`` isn't built on this — its own SIGINT/Task cancellation
machinery needs a different shape.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

from synology_apm_repo.cli.errors import fail_from_apm_error, require_one_of
from synology_apm_repo.cli.profile_store import resolve_profile_store
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk.api import Repository, Session, TraceEvent
from synology_apm_repo.sdk.errors import ApmRepoError, NotFoundError
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage import ObjectStore


async def open_single_repo(
    session: Session,
    fs_path: str,
    key: str | None,
    *,
    store: ObjectStore | None = None,
    progress: Callable[[Progress], Awaitable[None]] | None = None,
    trace: Callable[[TraceEvent], None] | None = None,
) -> Repository:
    """Discover exactly one repository at ``fs_path``.

    ``store``, when given (a ``--profile``-resolved ``ObjectStore``), scans
    via ``Session.open_remote`` with ``fs_path`` as the store-relative
    ``root``, instead of ``Session.open`` treating ``fs_path`` as a local
    filesystem path.

    Raises:
        NotFoundError: For zero or more than one repository — several sibling
            repo-ids sharing one bucket collapse into a single
            ``Repository`` with several ``catalogs()`` (not an error
            here); this only fires for genuinely separate repositories
            (distinct buckets/vaults) found under ``fs_path``, which
            needs a more specific path pointed at just one; picking one
            silently on the caller's behalf would be surprising for a
            forensics tool.
    """
    if store is not None:
        repos = await session.open_remote(store, key, root=fs_path, progress=progress, trace=trace)
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


@contextlib.asynccontextmanager
async def opened_repo(
    fs_path: str, key: str | None, *, profile: str | None, state: CliState
) -> AsyncIterator[Repository]:
    """Open exactly one repository at ``fs_path`` via ``open_single_repo`` and
    yield it, sharing this skeleton across
    ``ls``/``tree``/``cat``/``key``/``verify``/``doctor``. Any
    ``ApmRepoError`` — from the open itself or from the caller's own
    ``async with`` body — is translated to ``fail``'s standard CLI error
    exit; the session is always closed after, success or failure.

    Callers that need ``require_one_of(repo, profile)``-style validation
    of REPO/``--profile`` run it themselves *before* entering this
    context manager — a bare CLI argument mismatch isn't an
    ``ApmRepoError`` and isn't this function's job to check.

    ``finish_live_progress`` runs in ``finally`` — after the caller's own
    ``async with`` body (which may itself have rendered further progress
    via its own separate meter, e.g. ``verify``'s) has finished, whether
    it finished cleanly or not, so every command sharing this skeleton
    gets a clean line for its own next output without repeating the call
    itself. ``fail_from_apm_error`` (the ``except ApmRepoError`` branch)
    also calls it explicitly, *before* ``fail()``'s own print — ``finally``
    alone would run only after that print, too late to prevent this same
    error message from landing on top of a dangling progress line; calling
    it again from ``finally`` afterwards is a harmless no-op there, and is
    what covers a non-``ApmRepoError`` exception this function doesn't
    otherwise catch at all (a bug, not an expected failure — still
    deserves a clean line under whatever traceback ``fail_unexpected``
    goes on to print)."""
    session = Session()
    meter = build_progress_meter(state)
    trace = build_trace_callback(state)
    try:
        store = await resolve_profile_store(profile) if profile is not None else None
        repo = await open_single_repo(session, fs_path, key, store=store, progress=meter.update, trace=trace)
        yield repo
    except ApmRepoError as exc:
        fail_from_apm_error(exc, state)
    finally:
        finish_live_progress(state)
        await session.close()


def opened_repo_or_profile(
    repo: str | None, key: str | None, *, profile: str | None, state: CliState
) -> contextlib.AbstractAsyncContextManager[Repository]:
    """``require_one_of`` (REPO XOR ``--profile``) followed by ``opened_repo``
    — the two-line combo ``doctor``/``key``/``verify`` each
    repeat verbatim. Validation happens eagerly, before the context
    manager is even entered, matching every one of those call sites' own
    control flow (a bad argument combination exits before any repository is
    opened)."""
    require_one_of(repo, profile)
    return opened_repo(repo or "", key, profile=profile, state=state)
