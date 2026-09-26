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
        NotFoundError: For zero or more than one repository — several
            sibling repo-ids sharing one bucket collapse into a single
            ``Repository`` with several ``catalogs()`` (not an error here);
            this only fires for genuinely separate repositories (distinct
            buckets/vaults) found under ``fs_path``, which needs a more
            specific path pointed at just one.
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

    Callers needing ``require_one_of(repo, profile)``-style validation of
    REPO/``--profile`` run it themselves before entering this context
    manager — a bare argument mismatch isn't an ``ApmRepoError``.

    ``finish_live_progress`` runs both in the ``except ApmRepoError``
    branch (before ``fail()``'s own print, so the error doesn't land on a
    dangling progress line) and in ``finally`` (covering every other exit
    path); the second call is a harmless no-op when the first already
    ran."""
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
