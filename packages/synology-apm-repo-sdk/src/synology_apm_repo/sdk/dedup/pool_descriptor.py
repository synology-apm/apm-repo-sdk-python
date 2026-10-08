"""``PoolDescriptor``: a picklable recipe for rebuilding one ``Pool`` fresh
inside a worker process — the ``dedup``-layer counterpart to
``storage.store_descriptor``. Built from a live ``Pool`` (``from_pool``).
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import Callable
from typing import Any, Protocol, override, runtime_checkable

from .._util.closing import AsyncClosing
from ..cachemanager import DEFAULT_LIMITS, CacheLimits
from ..concurrency import close_worker_loop, new_process_pool, run_in_worker_loop, shutdown_pool
from ..storage.base import ObjectStore
from ..storage.dircache import DirCache
from ..storage.store_descriptor import StoreDescriptor, describe_store, rebuild_store
from .pool import NO_VERIFY, Pool, VerifyPolicy


@dataclasses.dataclass(frozen=True, slots=True)
class PoolDescriptor:
    """Picklable recipe for rebuilding an equivalent ``Pool`` in a worker
    process (``build_worker_pool``)."""

    store_descriptor: StoreDescriptor
    pool_root: str
    vault_key: bytes | None
    verify: VerifyPolicy = NO_VERIFY
    limits: CacheLimits = DEFAULT_LIMITS

    @classmethod
    def from_pool(cls, pool: Pool) -> PoolDescriptor | None:
        """``pool``'s recipe, ``verify`` policy and cache limits included. ``None`` when its
        store can't be reconstructed in a fresh process (an unrecognized
        store wrapper, or a backend built from an already-live client with
        no picklable recipe)."""
        store_descriptor = describe_store(pool.store)
        if store_descriptor is None:
            return None
        return cls(
            store_descriptor=store_descriptor,
            pool_root=pool.pool_root,
            vault_key=pool.vault_key,
            verify=pool.verify,
            limits=pool.limits,
        )


def build_worker_pool(descriptor: PoolDescriptor) -> tuple[ObjectStore, Pool]:
    """Rebuilds a fresh ``ObjectStore``/``DirCache``/``Pool`` from
    ``descriptor``; call it once per worker process, not per task, so the
    ``Pool``'s caches persist across tasks (``WorkerContext.init``).
    """
    store = rebuild_store(descriptor.store_descriptor)
    dir_cache = DirCache(store, maxsize=descriptor.limits.dir_scan)
    pool = Pool(
        store,
        descriptor.pool_root,
        dir_cache,
        vault_key=descriptor.vault_key,
        limits=descriptor.limits,
        verify=descriptor.verify,
    )
    return store, pool


async def close_worker_store(store: ObjectStore | None) -> None:
    """Best-effort release of a worker's ``ObjectStore`` at shutdown: a close
    failure is swallowed, since a shutdown hook has nowhere to report it."""
    if store is not None:
        with contextlib.suppress(Exception):
            await store.close()


@runtime_checkable
class SupportsWorkerPool(Protocol):
    """Content an export may decode in worker processes: it can describe
    the ``Pool`` a worker rebuilds to read it."""

    def pool_descriptor(self) -> PoolDescriptor | None:
        """How a worker rebuilds this content's ``Pool``, or ``None`` when
        it can't be rebuilt in another process."""
        ...


@dataclasses.dataclass
class WorkerContext:
    """One worker process's ``ObjectStore``/``Pool`` pair, kept warm across its
    tasks: built once by ``init()``, released once by ``shutdown()``. Used by
    ``dedup.verify_bucket_check`` and ``dedup.export_workers``.

    Each entry point keeps its own top-level ``_*_worker_init``/
    ``_*_worker_shutdown`` pair (``spawn`` pickles the initializer) that
    delegates the store/pool half here and adds any extra per-worker state.

    Not ``frozen``: ``init()`` assigns in place.
    """

    store: ObjectStore | None = None
    pool: Pool | None = None

    def init(self, descriptor: PoolDescriptor) -> None:
        self.store, self.pool = build_worker_pool(descriptor)

    def shutdown(self) -> None:
        run_in_worker_loop(close_worker_store(self.store))
        close_worker_loop()


class BoundProcessPool(AsyncClosing):
    """Worker processes bound for their lifetime to one repository
    (``pool_descriptor``): their initializer builds that repository's
    ``Pool`` once, and nothing re-initializes them per task. Async context
    manager; ``close()`` shuts them down.

    Attributes:
        pool_descriptor: The repository the workers serve.
        process_pool: The underlying executor, for ``concurrency.dispatch_to_pool``.
    """

    def __init__(
        self, pool_descriptor: PoolDescriptor, *, initializer: Callable[..., None], initargs: tuple[Any, ...]
    ) -> None:
        self.pool_descriptor = pool_descriptor
        self.process_pool = new_process_pool(initializer=initializer, initargs=initargs)

    def accepts_pool(self, pool_descriptor: PoolDescriptor | None) -> bool:
        """Whether work on ``pool_descriptor``'s repository may run here."""
        return pool_descriptor == self.pool_descriptor

    @override
    async def close(self) -> None:
        """Shuts the workers down, waiting for in-flight tasks (off the event loop)."""
        await shutdown_pool(self.process_pool)
