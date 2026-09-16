"""Shared navigation for ``ls``/``tree``/``cat``/``export``.

A CLI ``<ref>`` argument is ``<filesystem-path>[#<fragment>]`` — the part
before ``#`` tells this module which directory to ``Session.open``, the
part after (if any) says where to navigate once there. ``walk_ref`` drives
``Repository.walk_human_ref`` for a human ref and ``Repository.resolve`` for
a canonical/raw one, wrapping either into the same ``Frame`` shape so
``ls``/``tree`` never need to branch on ref kind.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import TypeVar

from synology_apm_repo.cli.errors import fail, friendly_message, require_one_of
from synology_apm_repo.cli.profile_store import resolve_profile_store
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk.api import Frame as Frame
from synology_apm_repo.sdk.api import Repository, Session, TraceEvent
from synology_apm_repo.sdk.errors import ApmRepoError, NotFoundError
from synology_apm_repo.sdk.presentation.progress import Progress
from synology_apm_repo.sdk.storage import ObjectStore
from synology_apm_repo.sdk.units.base import Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef, RefKind
from synology_apm_repo.sdk.units.node_ref import catalog_pairs as catalog_pairs
from synology_apm_repo.sdk.units.node_ref import disambiguate as disambiguate
from synology_apm_repo.sdk.units.node_ref import version_pairs as version_pairs
from synology_apm_repo.sdk.units.node_ref import workload_pairs as workload_pairs

_T = TypeVar("_T")


@dataclasses.dataclass(frozen=True)
class ParsedRef:
    """A CLI ``<ref>`` argument split into the filesystem path (``Session.open``
    target) and the parsed navigation fragment."""

    fs_path: str
    node_ref: NodeRef


def parse_ref_argument(value: str) -> ParsedRef:
    """A bare path (no ``#``) is a human ref with zero segments — "browse
    from the top of whatever's discovered there"."""
    if "#" not in value:
        return ParsedRef(fs_path=value, node_ref=NodeRef.human(value))
    node_ref = NodeRef.parse(value)
    return ParsedRef(fs_path=node_ref.repo_path, node_ref=node_ref)


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
    itself. The ``except ApmRepoError`` branch also calls it explicitly,
    *before* ``fail()``'s own print — ``finally`` alone would run only
    after that print, too late to prevent this same error message from
    landing on top of a dangling progress line; calling it again from
    ``finally`` afterwards is a harmless no-op there, and is what covers
    a non-``ApmRepoError`` exception this function doesn't otherwise catch
    at all (a bug, not an expected failure — still deserves a clean line
    under whatever traceback ``fail_unexpected`` goes on to print).

    ``export.py`` isn't built on this: its own SIGINT/Task cancellation
    machinery needs a different shape."""
    session = Session()
    meter = build_progress_meter(state)
    trace = build_trace_callback(state)
    try:
        store = await resolve_profile_store(profile) if profile is not None else None
        repo = await open_single_repo(session, fs_path, key, store=store, progress=meter.update, trace=trace)
        yield repo
    except ApmRepoError as exc:
        finish_live_progress(state)
        fail(friendly_message(exc, verbose=state.verbose), cause=exc)
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


async def walk_ref(repo: Repository, node_ref: NodeRef, *, object_db_id: str | None = None) -> Frame:
    """``Repository.walk_human_ref`` for a human ref; canonical/raw refs
    (already fully-qualified) resolve straight to a node via
    ``Repository.resolve`` instead, wrapped in the same ``Frame`` shape so
    callers don't need to branch on ref kind."""
    if node_ref.kind is RefKind.HUMAN:
        return await repo.walk_human_ref(node_ref.segments, object_db_id=object_db_id)
    resolved = await repo.resolve(node_ref, object_db_id=object_db_id)
    return Frame(level="node", node=resolved, provider=await _provider_for(repo, node_ref, object_db_id=object_db_id))


async def _provider_for(repo: Repository, node_ref: NodeRef, *, object_db_id: str | None = None) -> UnitProvider | None:
    """The children-listing provider for a resolved canonical/raw node —
    needed by ``walk_ref``'s callers (``ls``/``tree``) to list a resolved
    node's own children, something ``Repository.resolve()`` itself has no
    reason to return (``cat``/``export`` never need it)."""
    if node_ref.kind is RefKind.RAW:
        return await repo.file_map_tree()
    if node_ref.kind is RefKind.CANONICAL:
        catalog, version = await repo.version_for_ref(node_ref)
        return await catalog.provider(version, object_db_id=object_db_id)
    return None  # pragma: no cover - defensive: walk_ref() only reaches here for RAW/CANONICAL kinds


async def resolve_restorable(
    repo: Repository, node_ref: NodeRef, *, ref: str, hint: str, object_db_id: str | None = None
) -> RestorableUnit:
    """``Repository.resolve`` narrowed to a single restorable item —
    ``fail()``s with ``hint`` appended when REF instead names a folder.
    Shared by ``cat``/``export``, whose entire output *is* one item's
    content and so have nothing meaningful to do with a folder ref."""
    resolved = await repo.resolve(node_ref, object_db_id=object_db_id)
    if not isinstance(resolved, RestorableUnit):
        fail(f"{ref!r} names a folder, not a single item — {hint}")
    return resolved


def display_ref(node: Node, fs_path: str) -> str:
    """The printed ref must be directly reusable, as printed, as a fresh
    CLI argument from the same working directory.

    ``node.ref.repo_path`` is the SDK's own ``layout.repo_root`` (a
    short, store-relative fragment like ``""`` or
    ``"@ActiveProtectVault"``), never the filesystem path REF was
    actually invoked with, so printing ``str(node.ref)`` verbatim would
    hand back something that only round-trips by coincidence. Substitute
    the real ``fs_path`` this command was given before printing. Shared
    by ``ls``/``tree`` — both print refs from the same kind of resolved
    ``Node``."""
    return str(dataclasses.replace(node.ref, repo_path=fs_path))


def disambiguated_names(
    objects: Sequence[_T], pairs: list[tuple[str, str]], *, hints: list[str | None] | None = None
) -> list[tuple[str, _T]]:
    """One disambiguated display name per object in ``objects``, paired up
    — the "``disambiguate(pairs, hints=hints)``, then zip with the objects
    those ``pairs`` were built from" step ``ls``'s own row-builder and two
    of ``tree``'s own entry-builders each repeat. ``pairs`` must be built
    from ``objects`` in the same order (``catalog_pairs``/``workload_pairs``
    already guarantee this)."""
    return list(zip(disambiguate(pairs, hints=hints), objects, strict=True))
