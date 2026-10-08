"""``Session``: repository discovery, and the lifetime of the stores and
``Repository`` instances it hands out."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import override

from .._util.closing import RESOURCE_CLOSE_TIMEOUT, AsyncClosing, close_each
from ..dedup.keys import KeyMaterial, KeyVerification
from ..dedup.keys import probe_encrypted as _probe_encrypted
from ..errors import ApmRepoError, NotFoundError, StorageBackendError
from ..presentation.progress import Progress, ProgressCallback
from ..storage.base import ObjectStore
from ..storage.layout import (
    RepoKind,
    RepoLayout,
    RepositoryLayout,
    iter_repository_layouts,
    key_probe_layout,
)
from ..storage.local import LocalFsStore
from ..storage.recording import TraceEvent as TraceEvent
from ..storage.recording import TracingStore
from ..units.node_ref import NodeRef
from .catalog import NodeFrame, RawView
from .repository import Repository


async def _resolve_key_verification(
    keys: KeyMaterial | None, store: ObjectStore, layout: RepoLayout
) -> KeyVerification | None:
    if keys is None:
        return None
    return await keys.verify(store, layout)


def _backing_of(store: ObjectStore) -> object:
    """The store behind a possible ``TracingStore`` wrapper: two traced
    discoveries over one store get separate wrappers."""
    return store.backing if isinstance(store, TracingStore) else store


class Session(AsyncClosing):
    """Discovers repositories and owns every store and ``Repository`` it
    hands out. Use as an async context manager or call ``close``;
    ``close_repo`` releases one repository early.
    """

    def __init__(self) -> None:
        # Every repository handed out, with the store it reads through.
        self._repos: dict[Repository, ObjectStore] = {}
        # Tracked here, not per Repository: one discovery's store is shared
        # by every repository it yields.
        self._stores: list[ObjectStore] = []
        # How many discoveries are still running over each backing store, by
        # identity: close_repo() leaves such a store open for the siblings
        # those discoveries have yet to yield, and marks it in
        # _close_when_idle for the last of them to close.
        self._discovering: dict[int, int] = {}
        self._close_when_idle: set[int] = set()

    async def discover(
        self,
        source: Path | str | ObjectStore,
        key: str | None = None,
        *,
        root: str = "",
        progress: ProgressCallback | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> AsyncGenerator[Repository]:
        """Walk ``source`` for repositories, yielding each as it's opened
        (``open`` collects them). A candidate with nothing to browse, or
        whose key record is unreadable, is skipped.

        Args:
            source: A local directory, or an ``ObjectStore`` (S3/Azure/SMB/...)
                this session takes ownership of.
            key: ``"<userKeyID>@<base64 userKey>"``, verified against each
                repository found. When omitted, each repository's
                ``is_encrypted`` is probed instead.
            root: The path within ``source`` to scan from.
            progress: Receives an indeterminate ``Progress`` per candidate
                found.
            trace: Receives a ``TraceEvent`` for every ``ObjectStore`` call
                the yielded repositories make, for their whole lifetime.

        Raises:
            KeyMaterialError: ``key`` is malformed.
            StorageBackendError: The store failed while scanning.
            ExceptionGroup: Closing the store failed after ``close_repo``
                released its last repository mid-discovery.
        """
        store = LocalFsStore(Path(source)) if isinstance(source, (Path, str)) else source
        # aclosing: closing this generator early must also close the inner
        # one now, not whenever it is garbage-collected.
        async with contextlib.aclosing(
            self._discover_from_store(store, key, root=root, progress=progress, trace=trace)
        ) as repos:
            async for repo in repos:
                yield repo

    async def _discover_from_store(
        self,
        store: ObjectStore,
        key: str | None,
        *,
        root: str = "",
        progress: ProgressCallback | None,
        trace: Callable[[TraceEvent], None] | None,
    ) -> AsyncGenerator[Repository]:
        """``discover``'s body. Finding the next layout and opening found
        ones run concurrently, so a slow later layout never holds back a
        repository already opened; repositories are yielded in the order
        they were found."""
        if trace is not None:
            store = TracingStore(store, trace)
        self._stores.append(store)
        keys = KeyMaterial.from_key_string(key) if key else None

        found = 0
        layout_iter = iter_repository_layouts(store, root)
        pending_open: list[asyncio.Task[Repository | None]] = []
        next_layout: asyncio.Task[RepositoryLayout] | None = None
        backing = _backing_of(store)
        self._discovering[id(backing)] = self._discovering.get(id(backing), 0) + 1
        try:
            next_layout = asyncio.ensure_future(anext(layout_iter))
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
                        self._repos[repo] = store
                        yield repo
        finally:
            await _drain_and_close(next_layout, pending_open, layout_iter)
            self._discovering[id(backing)] -= 1
            if not self._discovering[id(backing)]:
                del self._discovering[id(backing)]
                if id(backing) in self._close_when_idle:
                    self._close_when_idle.discard(id(backing))
                    if errors := await self._close_unshared_store(backing):
                        raise ExceptionGroup("closing a store close_repo() deferred failed", errors)

    async def open(
        self,
        source: Path | str | ObjectStore,
        key: str | None = None,
        *,
        root: str = "",
        progress: ProgressCallback | None = None,
        trace: Callable[[TraceEvent], None] | None = None,
    ) -> list[Repository]:
        """``discover``, collected into a list."""
        return [repo async for repo in self.discover(source, key, root=root, progress=progress, trace=trace)]

    async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
        """``Repository.resolve`` on whichever open repository owns
        ``ref.repo_path``; a caller holding the repository already can call
        that directly.

        Raises:
            NotFoundError: No open repository owns the ref, or no node
                matches it.
            KeyRequiredError: See ``Repository.resolve``.
            KeyMismatchError: See ``Repository.resolve``.
            ValueError: The string is not a parseable ref.
        """
        node_ref = NodeRef.coerce(ref)
        return await self._repo_for_ref(node_ref).resolve(node_ref, raw=raw)

    def _repo_for_ref(self, node_ref: NodeRef) -> Repository:
        for repo in self._repos:
            if repo._owns_repo_path(node_ref.repo_path):  # noqa: SLF001 -- Session dispatches refs among its own repositories
                return repo
        raise NotFoundError(f"no open repository matches ref: {node_ref}", ref=str(node_ref))

    async def close_repo(self, repo: Repository) -> None:
        """Close and forget one repository, and its store when no other
        tracked repository shares it and no discovery over it is still
        running.

        Raises:
            ExceptionGroup: The repository or its store failed to close.
        """
        errors: list[Exception] = []
        try:
            await repo._close()  # noqa: SLF001 -- Session owns every Repository it hands out
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        repo_store = self._repos.pop(repo, None)
        if repo_store is not None:
            backing = _backing_of(repo_store)
            if id(backing) in self._discovering:
                # A running discovery may still yield siblings over this
                # store; the last one to finish closes it.
                self._close_when_idle.add(id(backing))
            else:
                errors.extend(await self._close_unshared_store(backing))
        if errors:
            raise ExceptionGroup("Session.close_repo() failed to close every tracked resource", errors)

    async def _close_unshared_store(self, backing: object) -> list[Exception]:
        """Close every tracked wrap of ``backing`` unless a tracked repository
        still reads through it; returns the close failures."""
        if any(_backing_of(s) is backing for s in self._repos.values()):
            return []
        errors: list[Exception] = []
        for store in [s for s in self._stores if _backing_of(s) is backing]:
            try:
                await store.close()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            # Removed only after the close attempt, so a cancellation
            # mid-close leaves it for Session.close().
            self._stores.remove(store)
        return errors

    @override
    async def close(self) -> None:
        """Close every repository this session handed out, then every store
        it owns (the network clients of S3/Azure/SMB stores).

        Raises:
            ExceptionGroup: One or more resources failed to close (every one
                was still attempted).
        """
        repos, self._repos = list(self._repos), {}
        stores, self._stores = self._stores, []
        # Repositories bound each of their own closes; a store's close is one network call.
        errors = await close_each(r._close for r in repos)  # noqa: SLF001 -- as in close_repo
        errors += await close_each((s.close for s in stores), per_close_timeout=RESOURCE_CLOSE_TIMEOUT)
        if errors:
            raise ExceptionGroup("Session.close() failed to close every tracked resource", errors)


async def _open_repository(store: ObjectStore, keys: KeyMaterial | None, layout: RepositoryLayout) -> Repository | None:
    """One found layout as a ``Repository``, or ``None`` to skip it: its
    key/encryption record is unreadable (a half-written layout can pass
    the marker check) or it has nothing to browse."""
    if layout.kind is not RepoKind.VAULT and layout.catalog_ids == []:
        # None (unenumerable) or non-empty is real; [] means nothing to browse.
        # A catalog's corrupt repo_info surfaces later, when catalogs() opens it.
        return None
    key_layout = key_probe_layout(layout)
    try:
        key_verification = await _resolve_key_verification(keys, store, key_layout)
        encrypted = None if keys is not None else await _probe_encrypted(store, key_layout)
    except StorageBackendError:
        raise
    except ApmRepoError:
        return None
    return Repository(store, layout, keys, key_verification, encrypted=encrypted)


async def _drain_and_close(
    next_layout: asyncio.Task[RepositoryLayout] | None,
    pending_open: list[asyncio.Task[Repository | None]],
    layout_iter: AsyncGenerator[RepositoryLayout, None],
) -> None:
    """Cancel and await every in-flight task, then close ``layout_iter``:
    ``aclose()`` during a pending ``__anext__()`` raises "already running"."""
    if next_layout is not None:
        next_layout.cancel()
        with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
            await next_layout
    for task in pending_open:
        task.cancel()
    for task in pending_open:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await layout_iter.aclose()
