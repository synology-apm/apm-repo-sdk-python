"""``PoolDescriptor``: a picklable recipe for rebuilding one ``Pool`` fresh
inside a worker process — the ``dedup``-layer counterpart to
``storage.store_descriptor``. This one only needs a ``StoreDescriptor``
already in hand: ``verify_reachable.py`` has a ``DedupRepo``
(``from_repo``), ``export_scheduler.py`` only ever has a bare ``Pool``
(``from_pool``), since a ``DedupFile``/``ByteRangeView`` exposes its own
``.pool``, never a whole ``DedupRepo``.
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
        process (an unrecognized store wrapper, or a backend built from
        an already-live client with no picklable recipe)."""
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
        """Same ``None``-on-undescribable-store contract as ``from_repo``,
        for a caller that only has a bare ``Pool``, never a ``DedupRepo``
        to build from — mirrors ``pool``'s own current
        ``verify_fingerprint``/``verify_ciphertext_crc`` settings rather
        than an explicit override, so a worker rebuild behaves
        identically to it."""
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
    ``descriptor`` — called once per worker process, never once per
    task: the resulting ``Pool``'s caches only pay off if they persist
    across every task the worker runs.
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
    """The natural counterpart to ``build_worker_pool()``: best-effort
    release of a worker's ``ObjectStore`` at shutdown. Uses a direct
    ``isinstance(store, AsyncCloseable)`` check rather than
    ``storage.base.aclose_if_possible`` — that helper propagates a close
    failure, but a worker shutdown hook has nowhere left to report one,
    so this swallows it instead."""
    if isinstance(store, AsyncCloseable):
        with contextlib.suppress(Exception):
            await store.aclose()
