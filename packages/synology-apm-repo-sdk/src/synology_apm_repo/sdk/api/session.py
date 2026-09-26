"""``Session``: repository discovery, key material, and the store/repository
lifetime it owns, part of the Repository Layer (see ``api/__init__.py``).
Hands out
``Repository`` instances (``api.repository``) it discovered/opened;
never the other way around.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Self

from ..dedup.keys import KeyMaterial, KeyVerification
from ..dedup.keys import probe_encrypted as _probe_encrypted
from ..errors import ApmRepoError, NotFoundError
from ..presentation.progress import Progress
from ..storage.base import ObjectStore, aclose_if_possible
from ..storage.layout import (
    RepoLayout,
    RepositoryLayout,
    iter_repository_layouts,
    key_probe_layout,
)
from ..storage.local import LocalFsStore
from ..storage.recording import TraceEvent as TraceEvent
from ..storage.recording import TracingStore
from ..units.base import Node, RestorableUnit
from ..units.node_ref import NodeRef
from .repository import Repository


async def _resolve_key_verification(
    keys: KeyMaterial | None, store: ObjectStore, layout: RepoLayout
) -> KeyVerification | None:
    if keys is None:
        return None
    return await keys.verify(store, layout)


def _backing_of(store: ObjectStore) -> object:
    """The real store identity behind a possible ``TracingStore`` wrap —
    two separate ``discover()``/``discover_remote()`` calls each get their
    own ``TracingStore`` wrapper even for the same backing store, so
    comparing wrapper identity alone would miss the shared connector."""
    return store.backing if isinstance(store, TracingStore) else store


class Session:
    """Use as a context manager, or call ``close`` explicitly when done --
    or ``close_repo`` to release just one repository (and its store, if
    unshared) ahead of the rest of a longer-running session.
    """

    def __init__(self) -> None:
        self._repos: list[Repository] = []
        # Every store this session constructed, so close() can ``aclose()``
        # the ones that own an aiohttp connector (S3/Azure). Tracked here
        # rather than reached through each Repository because one discover()
        # call's store is shared by every repository it yields.
        self._stores: list[ObjectStore] = []

    async def discover(
        self,
        path: Path | str,
        key: str | None = None,
        *,
        progress: Callable[[Progress], Awaitable[None]] | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> AsyncIterator[Repository]:
        """Walk ``path`` for repositories, yielding each as it's found —
        an indeterminate-progress generator, not a blocking call; see
        ``open`` for that convenience wrapper. A layout that fails
        ``Repository._confirm_real()``'s check is skipped rather than
        aborting the whole scan. Local paths only — see ``discover_remote``
        for an already-constructed ``ObjectStore`` (S3/Azure/...).

        Args:
            key: When omitted, each yielded repository's encryption status
                is still resolved eagerly via ``dedup.keys.probe_encrypted``;
                skipped when a key *is* given.
            trace: When given, every ``ObjectStore`` call any repository
                found here ever makes, for its entire lifetime, is reported
                as a ``TraceEvent``.
        """
        store: ObjectStore = LocalFsStore(Path(path))
        async for repo in self._discover_from_store(store, key, progress=progress, trace=trace):
            yield repo

    async def discover_remote(
        self,
        store: ObjectStore,
        key: str | None = None,
        *,
        root: str = "",
        progress: Callable[[Progress], Awaitable[None]] | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> AsyncIterator[Repository]:
        """Same as ``discover``, against an already-constructed
        ``ObjectStore`` (S3/Azure/...) instead of building a ``LocalFsStore``
        from a filesystem path. This session takes ownership of ``store``,
        released by ``close``.

        ``root`` narrows the scan to a store-relative sub-path — needed
        because an S3/Azure store is scoped to a whole bucket/container
        with no "sub-root" constructor argument of its own.
        """
        async for repo in self._discover_from_store(store, key, root=root, progress=progress, trace=trace):
            yield repo

    async def _discover_from_store(
        self,
        store: ObjectStore,
        key: str | None,
        *,
        root: str = "",
        progress: Callable[[Progress], Awaitable[None]] | None,
        trace: Callable[[TraceEvent], None] | None,
    ) -> AsyncIterator[Repository]:
        """The shared body ``discover``/``discover_remote`` delegate to once
        each has settled on which concrete ``ObjectStore`` to scan.

        "Look for the next layout" and "finish opening an already-found
        one" race via ``asyncio.wait(..., FIRST_COMPLETED)`` rather than
        collecting every layout first: that would let a slow/hung later
        layout block yielding repositories already opened. Completed opens
        are yielded in the order they were started, as soon as they're
        ready.
        """
        if trace is not None:
            store = TracingStore(store, trace)
        self._stores.append(store)
        keys = KeyMaterial.from_key_string(key) if key else None

        found = 0
        layout_iter = iter_repository_layouts(store, root)
        pending_open: list[asyncio.Task[Repository | None]] = []
        next_layout: asyncio.Task[RepositoryLayout] | None = asyncio.ensure_future(anext(layout_iter))
        try:
            while next_layout is not None or pending_open:
                waiting: set[asyncio.Task[object]] = set(pending_open)
                if next_layout is not None:
                    waiting.add(next_layout)
                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

                if next_layout is not None and next_layout in done:
                    try:
                        layout = next_layout.result()
                    except StopAsyncIteration:
                        next_layout = None
                    else:
                        found += 1
                        if progress is not None:
                            await progress(
                                Progress(
                                    phase="discovering",
                                    determinate=False,
                                    unit="items",
                                    found=found,
                                    detail=layout.repo_root,
                                )
                            )
                        pending_open.append(asyncio.ensure_future(_open_repository(store, keys, layout)))
                        next_layout = asyncio.ensure_future(anext(layout_iter))

                while pending_open and pending_open[0].done():
                    repo = pending_open.pop(0).result()
                    if repo is not None:
                        self._repos.append(repo)
                        yield repo
        finally:
            await _drain_and_close(next_layout, pending_open, layout_iter)

    async def open(
        self,
        path: Path | str,
        key: str | None = None,
        *,
        progress: Callable[[Progress], Awaitable[None]] | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> list[Repository]:
        """``discover``, fully drained — the blocking convenience form for
        callers that don't need incremental results but still want
        progress/cancel support during a possibly slow scan."""
        return [repo async for repo in self.discover(path, key, progress=progress, trace=trace)]

    async def open_remote(
        self,
        store: ObjectStore,
        key: str | None = None,
        *,
        root: str = "",
        progress: Callable[[Progress], Awaitable[None]] | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> list[Repository]:
        """``discover_remote``, fully drained — the blocking convenience
        form for a caller that wants the final list but still needs
        progress/cancel support during a possibly slow scan."""
        return [repo async for repo in self.discover_remote(store, key, root=root, progress=progress, trace=trace)]

    async def resolve(self, ref: str | NodeRef) -> Node | RestorableUnit:
        """Turn a ``NodeRef`` (or its string form) back into a live node,
        re-derived from cheap catalog lookups and provider tree calls
        rather than stored anywhere.

        Two jobs stacked: which open ``Repository`` ``ref`` belongs to
        (matches ``ref.repo_path`` via ``Repository.owns_repo_path``,
        relevant once several repositories are open in one long-lived
        ``Session``), then what node inside it names (``Repository.
        resolve``'s job). A caller that already has the right
        ``Repository`` in hand should call ``Repository.resolve`` directly
        and skip this matching step.
        """
        node_ref = ref if isinstance(ref, NodeRef) else NodeRef.parse(ref)
        return await self._repo_for_ref(node_ref).resolve(node_ref)

    def _repo_for_ref(self, node_ref: NodeRef) -> Repository:
        for repo in self._repos:
            if repo.owns_repo_path(node_ref.repo_path):
                return repo
        raise NotFoundError(f"no open repository matches ref: {node_ref}", ref=str(node_ref))

    async def close_repo(self, repo: Repository) -> None:
        """Close and forget one repository this session tracks, releasing
        its own store too if no other still-tracked repository shares it —
        for a caller that discards one repository at a time from a
        longer-running session rather than tearing the whole session down.

        The caller must have finished draining the ``discover()``/
        ``discover_remote()`` call that yielded ``repo`` first: a
        not-yet-yielded sibling sharing the same store isn't in
        ``self._repos`` yet, so the shared-store check below would
        false-negative and close the store out from under it.
        """
        errors: list[Exception] = []
        try:
            await repo.close()
        except Exception as exc:
            errors.append(exc)
        if repo in self._repos:
            self._repos.remove(repo)
        backing = _backing_of(repo._store)  # noqa: SLF001 - Session constructs and owns every Repository it yields, so reaching into its own _store here is an accepted crossing of the normal encapsulation boundary
        if not any(_backing_of(r._store) is backing for r in self._repos):  # noqa: SLF001
            for store in [s for s in self._stores if _backing_of(s) is backing]:
                try:
                    await aclose_if_possible(store)
                except Exception as exc:
                    errors.append(exc)
                # Removed only after aclose_if_possible returns or raises,
                # so a cancellation mid-close leaves it tracked for
                # Session.close() to pick up later instead of orphaned.
                self._stores.remove(store)
        if errors:
            raise ExceptionGroup("Session.close_repo() failed to close every tracked resource", errors)

    async def close(self) -> None:
        """Close every repository this session opened, then release any
        backend client it owns.

        ``S3Store``/``AzureStore`` own an ``aiohttp`` connector that must be
        released; local stores have no ``aclose()`` and are skipped.
        """
        # Same "attempt every item, then report" posture as
        # Repository.close().
        errors: list[Exception] = []
        for repo in self._repos:
            try:
                await repo.close()
            except Exception as exc:
                errors.append(exc)
        self._repos.clear()
        for store in self._stores:
            try:
                await aclose_if_possible(store)
            except Exception as exc:
                errors.append(exc)
        self._stores.clear()
        if errors:
            raise ExceptionGroup("Session.close() failed to close every tracked resource", errors)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


async def _open_repository(store: ObjectStore, keys: KeyMaterial | None, layout: RepositoryLayout) -> Repository | None:
    """``Session._discover_from_store``'s per-layout open step — ``None``
    for a false-positive layout match (``iter_repository_layouts`` found
    something that doesn't actually open as a valid repository, or whose
    key/encryption record can't be read), a real ``Repository`` otherwise.
    The key/encryption probe is wrapped in the same ``ApmRepoError`` skip
    as ``_confirm_real()``: a half-written layout can pass the cheap
    marker check while its key/encryption record is genuinely unreadable.
    """
    key_layout = key_probe_layout(layout)
    try:
        key_verification = await _resolve_key_verification(keys, store, key_layout)
        encrypted = None if keys is not None else await _probe_encrypted(store, key_layout)
    except ApmRepoError:
        return None
    repo = Repository(store, layout, keys, key_verification, encrypted=encrypted)
    if not await repo._confirm_real():  # noqa: SLF001 - Session constructs every Repository via this factory, so calling its own _confirm_real() here before yielding it is an accepted crossing of the normal encapsulation boundary
        return None
    return repo


async def _drain_and_close(
    next_layout: asyncio.Task[RepositoryLayout] | None,
    pending_open: list[asyncio.Task[Repository | None]],
    layout_iter: AsyncIterator[RepositoryLayout],
) -> None:
    """``Session._discover_from_store``'s teardown — cancel and *await*
    every still-in-flight task before closing ``layout_iter``: calling
    ``aclose()`` while its ``__anext__()`` is still technically in flight
    raises "already running"."""
    if next_layout is not None:
        next_layout.cancel()
        with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
            await next_layout
    for task in pending_open:
        task.cancel()
    for task in pending_open:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    # iter_repository_layouts() is declared AsyncIterator[RepositoryLayout]
    # (the narrow interface callers need), but is always implemented as a
    # real async generator (every concrete body uses ``yield``) —
    # aclose() exists at runtime even though the declared type doesn't
    # expose it.
    await layout_iter.aclose()  # type: ignore[attr-defined]
