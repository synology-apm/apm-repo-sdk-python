"""``Session``: repository discovery, key material, and the store/repository
lifetime it owns, part of the Repository Layer (the ``Session``/
``Repository``/``Catalog`` split CLI/TUI code imports directly). Hands out
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
    """The real store identity behind a possible ``TracingStore`` wrap --
    two separate ``discover()``/``discover_remote()`` calls against the same
    backing store each get their own ``TracingStore`` instance when
    ``trace=`` is given, so comparing wrapper identity alone would miss that
    they still share one live connector underneath (see
    ``Session.close_repo()``)."""
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
        an indeterminate-progress generator (discovery has an unknowable
        total), not a blocking call; see ``open`` for that convenience
        wrapper.

        A layout ``iter_repository_layouts`` finds that then fails
        ``Repository._confirm_real()``'s own cheap, marker-only check is
        skipped rather than aborting the whole scan — for
        ``OBJECT_STORE`` this only catches a bucket with zero valid
        catalog ids; for ``VAULT`` this check never actually fails (its
        one marker check already ran when the layout was built). Neither
        kind's own corrupt ``repo_info`` is caught here — that surfaces
        later instead, scoped to the one broken catalog, the first time
        ``Repository.catalogs()`` actually opens it.

        Local paths only — see ``discover_remote`` for an
        already-constructed ``ObjectStore`` (S3/Azure/...).

        Args:
            key: When omitted, each yielded repository's encryption status is
                still resolved eagerly via
                ``dedup.keys.probe_encrypted`` (see ``KeyStatus``);
                skipped when a key *is* given, since
                ``Repository.key_verification`` already answers it.
            trace: When given, every ``ObjectStore`` call any repository
                found here ever makes, for that repository's entire
                lifetime, is reported as a ``TraceEvent``.
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
        from a filesystem path — for a caller that already knows which
        backend and bucket/container it wants (the TUI's connect
        dialog). This session takes ownership of ``store`` the same way
        ``discover`` does its own ``LocalFsStore``, released by ``close``.

        ``root`` narrows the scan to a store-relative sub-path, the same
        role a deeper local directory plays for ``discover`` — needed
        because S3/Azure stores are scoped to a whole bucket/container
        with no "sub-root" constructor argument, so a bucket holding
        several sibling repositories otherwise has no way to be narrowed to one.
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
        each has settled on which concrete ``ObjectStore`` to scan —
        nothing below here cares which concrete store it is.

        "Look for the next layout" and "finish opening an already-found
        one" race via ``asyncio.wait(..., FIRST_COMPLETED)`` rather than
        collecting every layout first and gathering the opens afterwards:
        that would let a slow/hung *later* layout (``iter_repository_layouts``
        can be arbitrarily slow on a misdirected directory tree) block
        yielding repositories already opened. Completed opens are yielded in the
        order they were *started*, the moment they're ready — every repository
        here shares one already-open store/connection, not a separate one
        each.
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
        callers that don't need incremental results but still
        want progress/cancel support: every CLI command that opens a
        repository needs the *final* list, but discovery can still take a
        while on a directory tree with many candidate layouts, so there's
        no reason to give up incremental feedback just because the caller
        is going to wait for the whole list anyway."""
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

        This is two jobs stacked: **which open** ``Repository`` ``ref``
        belongs to (this method's own job — matches ``ref.repo_path``
        against each open repository's own catalogs, via
        ``Repository.owns_repo_path``, relevant once several repositories are
        open in one long-lived ``Session``, e.g. a TUI), then **what node
        inside it** ``ref`` names
        (``Repository.resolve``'s job — see its docstring for the
        canonical/raw/human dispatch). A
        caller that already has the right ``Repository`` in hand (the
        common case for a one-shot CLI invocation, which opens exactly one
        repository per run) should call ``Repository.resolve`` directly and skip
        this matching step entirely.
        """
        node_ref = ref if isinstance(ref, NodeRef) else NodeRef.parse(ref)
        return await self._repo_for_ref(node_ref).resolve(node_ref)

    def _repo_for_ref(self, node_ref: NodeRef) -> Repository:
        for repo in self._repos:
            if repo.owns_repo_path(node_ref.repo_path):
                return repo
        raise NotFoundError(f"no open repository matches ref: {node_ref}", ref=str(node_ref))

    async def close_repo(self, repo: Repository) -> None:
        """Close and forget one repository this session tracks, releasing its
        own store too if no other still-tracked repository shares it.

        Unlike a bare ``repo.close()`` (idempotent, safe ahead of
        ``Session.close()``), this also removes
        ``repo`` from this session's bookkeeping and, when its store isn't
        shared with another repository this session still tracks, actually
        releases that store's connector/session instead of waiting for
        ``Session.close()`` at process end. For a caller that discards one
        repository at a time from a longer-running session — the TUI
        reconnecting to a different profile, or a tool walking many
        repositories sequentially — rather than tearing the whole session
        down at once.

        The caller must have finished draining the ``discover()``/
        ``discover_remote()`` call that yielded ``repo`` before calling this
        — a not-yet-yielded sibling repository sharing the same store isn't
        in ``self._repos`` yet, so the shared-store check below would
        false-negative and close the store out from under it. Both of this
        session's real callers (a fully-drained sample loop, a fully-drained
        reconnect scan) already satisfy this.

        Every tracked store sharing ``repo``'s own backing store (there can
        be more than one distinct ``TracingStore`` wrapper around the same
        backing, from separate ``discover()``/``discover_remote()`` calls)
        is released together once
        nothing in ``self._repos`` still references it, not just the one
        wrapper ``repo`` itself used — otherwise a sibling wrapper from an
        earlier call would sit in ``self._stores`` forever, never reachable
        by identity again.
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
                # Removed only after aclose_if_possible returns or raises, never
                # before it starts, so a cancellation arriving mid-close leaves
                # the store still tracked for Session.close() to pick up later
                # instead of orphaned.
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
        # Repository.close() — one repository's or store's failure must not
        # abandon closing the rest.
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
    key/encryption record itself can't be read), a real ``Repository``
    otherwise.

    Deliberately identical in shape for ``VAULT``/``OBJECT_STORE`` — key/
    encryption status is resolved from ``layout`` alone (``key_probe_layout``
    only needs ``repo_root`` for a ``VAULT`` or ``key_root`` for
    ``OBJECT_STORE``, shared by every sibling catalog, so neither backend
    needs a specific opened catalog for that), and
    ``Repository._confirm_real()`` trusts each kind's own marker check
    (already performed by ``iter_repository_layouts``) rather than
    opening a real ``DedupRepo`` here to double-check it, for either
    kind alike.

    The key/encryption probe below is wrapped in the same ``ApmRepoError``
    skip as ``_confirm_real()`` itself: a half-written layout can have
    ``iter_repository_layouts``'s own cheap marker check pass while its
    key/encryption record (e.g. a truncated ``db/vault_encryption_key``)
    is genuinely unreadable — the same false-positive-layout case described
    above, just caught here instead of in ``_confirm_real()``."""
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
    every still-in-flight task before closing ``layout_iter`` —
    ``aclose()`` on an async generator whose own ``__anext__()``
    coroutine is still technically in flight (task cancelled but not yet
    actually unwound) raises "already running"; awaiting the
    cancellation through first guarantees ``layout_iter`` is genuinely
    suspended, not mid-step. ``next_layout.cancel()`` is a no-op if it
    already finished (a benign race: the last layout was found right as
    this cancellation landed) — ``StopAsyncIteration`` is as harmless an
    outcome to discard here as a genuine ``CancelledError``, and letting
    either escape uncaught would corrupt the caller's own cancellation
    into a ``RuntimeError`` ("async generator raised
    StopAsyncIteration")."""
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
