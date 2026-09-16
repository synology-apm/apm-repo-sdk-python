"""``PoolDescriptor``: a picklable recipe for rebuilding one ``Pool`` fresh
inside a worker process — the ``dedup``-layer counterpart to
``storage.store_descriptor``, named to match it (both are "a picklable
descriptor + a worker-side rebuild factory for X"). Neither module depends
on the other's caller; this one only needs a ``StoreDescriptor`` already in
hand, from whichever ``DedupRepo``/``Pool`` a caller is dispatching work
for — ``verify_reachable.py`` has a ``DedupRepo`` (``from_repo``),
``export_scheduler.py`` only ever has a bare ``Pool`` (``from_pool``), since
a ``DedupFile``/``ByteRangeView`` exposes its own ``.pool``, never a whole
``DedupRepo``.
"""

from __future__ import annotations

import contextlib
import dataclasses

from ..storage.base import AsyncCloseable, ObjectStore
from ..storage.dircache import DirCache
from ..storage.store_descriptor import StoreDescriptor, describe_store, rebuild_store
from .pool import Pool
from .repository import DedupRepo


@dataclasses.dataclass(frozen=True)
class PoolDescriptor:
    """Picklable recipe for rebuilding an equivalent ``Pool`` in a worker process."""

    store_descriptor: StoreDescriptor
    pool_root: str
    vault_key: bytes | None
    verify_fingerprint: bool = False
    verify_ciphertext_crc: bool = False

    @classmethod
    def from_repo(
        cls,
        repo: DedupRepo,
        *,
        verify_fingerprint: bool = False,
        verify_ciphertext_crc: bool = False,
    ) -> PoolDescriptor | None:
        """``None`` when ``repo.store`` can't be reconstructed in a fresh
        process (see ``describe_store()``'s own docstring) — the one place
        a caller assembles the other four fields alongside that probe,
        instead of each repeating the same "call ``describe_store``, check
        ``None``, then assemble" sequence inline."""
        store_descriptor = describe_store(repo.store)
        if store_descriptor is None:
            return None
        return cls(
            store_descriptor=store_descriptor,
            pool_root=repo.pool_root,
            vault_key=repo.vault_key,
            verify_fingerprint=verify_fingerprint,
            verify_ciphertext_crc=verify_ciphertext_crc,
        )

    @classmethod
    def from_pool(cls, pool: Pool) -> PoolDescriptor | None:
        """Same ``None``-on-undescribable-store contract as
        ``from_repo``, for a caller (``export_scheduler.py``) that only
        ever has a bare, already-configured ``Pool`` in hand, never a
        whole ``DedupRepo`` to build a fresh one from — so, unlike
        ``from_repo``, this mirrors ``pool``'s own current
        ``verify_fingerprint``/``verify_ciphertext_crc`` settings rather
        than taking an explicit override for either: a worker rebuilding
        this pool should behave identically to it, not silently reset
        either flag."""
        store_descriptor = describe_store(pool.store)
        if store_descriptor is None:
            return None
        return cls(
            store_descriptor=store_descriptor,
            pool_root=pool.pool_root,
            vault_key=pool.vault_key,
            verify_fingerprint=pool.verify_fingerprint,
            verify_ciphertext_crc=pool.verify_ciphertext_crc,
        )


def build_worker_pool(descriptor: PoolDescriptor) -> tuple[ObjectStore, Pool]:
    """Rebuilds a fresh ``ObjectStore``/``DirCache``/``Pool`` from
    ``descriptor`` — called once per worker process (from a
    ``ProcessPoolExecutor``'s synchronous ``initializer=``, since
    ``rebuild_store()``'s own docstring confirms every real backend's
    construction is synchronous and I/O-free), never once per task: the
    resulting ``Pool``'s own caches (bucket readers, and — when
    ``verify_fingerprint`` — its ``AllocationTableCache``) are only worth
    anything if they persist across every task that worker ever runs.
    """
    store = rebuild_store(descriptor.store_descriptor)
    dir_cache = DirCache(store)
    pool = Pool(
        store,
        descriptor.pool_root,
        dir_cache,
        vault_key=descriptor.vault_key,
        verify_fingerprint=descriptor.verify_fingerprint,
        verify_ciphertext_crc=descriptor.verify_ciphertext_crc,
    )
    return store, pool


async def aclose_worker_store(store: ObjectStore | None) -> None:
    """The natural counterpart to ``build_worker_pool()`` above: best-effort
    release of a worker's own ``ObjectStore`` at shutdown — its ``aiohttp``
    connector, for ``S3Store``/``AzureStore``; a no-op for
    ``LocalFsStore``/``SmbStore``, or ``None`` (nothing was ever built).
    The third ``isinstance(store, AsyncCloseable)`` guard in this codebase
    (``api/session.py``'s ``Session.close()``, ``storage/recording.py``'s
    ``TracingStore``/``RecordingStore.aclose()``) — each with its own
    failure policy suited to its own caller (collect-and-report, propagate,
    and here, swallow) rather than one shared, parameterized version, since
    a worker's own shutdown hook (``_export_worker_shutdown``/
    ``_verify_worker_shutdown``) has nothing left to report a failure to."""
    if isinstance(store, AsyncCloseable):
        with contextlib.suppress(Exception):
            await store.aclose()
