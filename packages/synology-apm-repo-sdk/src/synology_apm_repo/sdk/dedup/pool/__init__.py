"""Chunk pool: resolves a ``ChunkAddress`` to plaintext bytes.

The only place chunk decrypt+decompress happens. Three collaborating
classes: ``BucketReader`` (``_bucket_reader.py``; one ``.buk`` file's
header/SizeStore/locators plus the per-chunk read), ``Pool`` (this module;
the repository-wide entry point, which resolves ``(streamID, bucketID)`` to a
``.buk`` path and caches ``BucketReader``\\ s and decoded chunks), and
``BucketReaderCache`` (``_cache.py``; the private cache a bulk sweep uses
instead of ``Pool``'s own).
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Iterable, Mapping

from cryptography.hazmat.primitives.ciphers import algorithms

from ...asynccache import AsyncKeyedCache, CacheStats
from ...cachemanager import DEFAULT_LIMITS, CacheLimits
from ...errors import DataCorruptError
from ...format.addressing import ChunkAddress, pool_layer_path, split_layer_leaf
from ...format.crypto import build_aes_algorithm
from ...identifiers import BucketId, ChunkIdx, StreamId
from ...storage.base import ObjectStore, join_path
from ...storage.dircache import DirCache
from ...storage.seqid import resolve_seq_path, resolve_seq_size
from ..fingerprint import FingerprintIndex
from ._bucket_reader import BucketReader
from ._cache import BucketReaderCache

__all__ = [
    "FULL_VERIFY",
    "NO_VERIFY",
    "BucketReader",
    "BucketReaderCache",
    "Pool",
    "VerifyPolicy",
]

_SPEC = "FORMAT-SPEC.md: Sidecar files; Chunk pool encryption"


@dataclasses.dataclass(frozen=True, slots=True)
class VerifyPolicy:
    """The per-chunk checks every read through a ``Pool`` runs. Browsing and
    export run neither (``NO_VERIFY``); ``VerifyLevel.FULL`` runs both
    (``FULL_VERIFY``) — see ``ARCHITECTURE.md``'s validation table."""

    ciphertext_crc: bool = False
    """Check each chunk's stored bytes against its bucket's ChunkCrcStore,
    before decrypting."""
    fingerprint: bool = False
    """Check each decoded chunk's SHA-256 against its stored ``.fgp``
    fingerprint (one extra ``.inf``/``.fgp`` read per bucket group)."""


NO_VERIFY = VerifyPolicy()
FULL_VERIFY = VerifyPolicy(ciphertext_crc=True, fingerprint=True)


class Pool:
    """Repository-wide chunk pool entry point.

    Resolves any ``ChunkAddress`` to its 4096-byte plaintext, with
    two-level LRU caching: ``BucketReader``\\ s (cheap, kept many) and decoded
    plaintext chunks (kept fewer). Both are shared ``AsyncKeyedCache``
    instances, so concurrent callers missing the same key share one fetch;
    ``backfill_chunks`` is the one direct insert that bypasses that.

    Args:
        store: The repository's ``ObjectStore``.
        pool_root: Path of the ``@data/Pool`` directory.
        dir_cache: Shared ``DirCache`` for ``.<seqId>`` resolution.
        vault_key: The VaultKey for an encrypted repository, else ``None``.
        limits: Bounds of the bucket-reader (``bucket_readers``), plaintext
            chunk (``chunks``) and allocation-table (``allocation_tables``)
            caches.
        verify: The per-chunk checks every read runs.
    """

    def __init__(
        self,
        store: ObjectStore,
        pool_root: str,
        dir_cache: DirCache,
        *,
        vault_key: bytes | None = None,
        limits: CacheLimits = DEFAULT_LIMITS,
        verify: VerifyPolicy = NO_VERIFY,
    ) -> None:
        self._store = store
        self._pool_root = pool_root
        self._dir_cache = dir_cache
        self._vault_key = vault_key
        self._verify = verify
        self._limits = limits
        self._buckets = BucketReaderCache(maxsize=limits.bucket_readers)
        # Keyed by a plain int chunk index: building a ChunkIdx per chunk on
        # the batch paths would cost more than the lookup it labels.
        self._chunks: AsyncKeyedCache[tuple[StreamId, BucketId, int], bytes] = AsyncKeyedCache(maxsize=limits.chunks)
        # Shared by every fingerprint lookup, so a .inf group's table is read once.
        self._fingerprints = FingerprintIndex(store, dir_cache, pool_root, maxsize=limits.allocation_tables)
        self._release_epoch = 0
        self._aes_algorithm: algorithms.AES | None = None

    def release_caches(self) -> None:
        """Drop everything this pool holds in memory: decoded chunks, open
        bucket readers, and allocation tables. ``DirCache`` is untouched; it
        belongs to the store.
        """
        self._buckets.invalidate()
        self._chunks.invalidate()
        self._fingerprints.clear()
        self._release_epoch += 1

    def cache_stats(self) -> dict[str, CacheStats]:
        """Counters of this pool's three caches, keyed ``pool.buckets``,
        ``pool.chunks`` and ``pool.allocation_tables``."""
        return {
            "pool.buckets": self._buckets.stats(),
            "pool.chunks": self._chunks.stats(),
            "pool.allocation_tables": self._fingerprints.stats(),
        }

    @property
    def release_epoch(self) -> int:
        """Bumped once per ``release_caches()`` call. A caller fetching
        plaintext outside ``read_chunk()`` (``dedup_file.py``'s
        ``_resolve_bucket_group``) captures it first and skips
        ``backfill_chunks`` if it changed."""
        return self._release_epoch

    @property
    def store(self) -> ObjectStore:
        """The ``ObjectStore`` this pool reads from (used by
        ``PoolDescriptor.from_pool``)."""
        return self._store

    @property
    def pool_root(self) -> str:
        return self._pool_root

    @property
    def vault_key(self) -> bytes | None:
        return self._vault_key

    @property
    def limits(self) -> CacheLimits:
        """The bounds this pool's caches were built with."""
        return self._limits

    @property
    def verify(self) -> VerifyPolicy:
        """The per-chunk checks every read through this pool runs."""
        return self._verify

    async def bucket_path(self, stream_id: StreamId, bucket_id: BucketId) -> str:
        """Resolve ``(stream_id, bucket_id)`` to its store-relative physical
        ``.buk`` path, including any ``.<seqId>`` generation suffix.

        Raises:
            NotFoundError: No such bucket file.
        """
        return await resolve_seq_path(self._dir_cache, *self._bucket_location(stream_id, bucket_id))

    async def bucket_size(self, stream_id: StreamId, bucket_id: BucketId) -> int | None:
        """The on-disk size of ``bucket_path(stream_id, bucket_id)`` as its
        directory listing reported it, with no ``size`` request; ``None`` if
        the backend lists no sizes (``store.size`` is then the way to ask).
        The listing is cached, so a store that changed after it was read needs
        ``Repository.invalidate_caches()``.

        Raises:
            NotFoundError: No such bucket file.
        """
        return await resolve_seq_size(self._dir_cache, *self._bucket_location(stream_id, bucket_id))

    def _bucket_location(self, stream_id: StreamId, bucket_id: BucketId) -> tuple[str, str]:
        """``(directory, logical .buk name)`` of a bucket, before seq-id resolution."""
        dir_part, leaf = split_layer_leaf(pool_layer_path(stream_id, bucket_id))
        return join_path(self._pool_root, dir_part), f"{leaf}.buk"

    async def bucket(
        self, stream_id: StreamId, bucket_id: BucketId, *, cache: BucketReaderCache | None = None
    ) -> BucketReader:
        """The ``BucketReader`` for ``(stream_id, bucket_id)``, opened on first
        access and cached in ``cache`` — a bulk sweep's private one — or, when
        ``None``, in this pool's own."""
        target = self._buckets if cache is None else cache
        return await target.resolve((stream_id, bucket_id), self._open_bucket_by_key)

    async def open_bucket_uncached(self, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        """Open ``(stream_id, bucket_id)`` fresh, without touching this pool's
        bucket cache (no lookup, insert or eviction).

        Every ``bucket()`` cache miss, this pool's own cache or a sweep's
        ``BucketReaderCache``, opens through it.
        """
        path = await self.bucket_path(stream_id, bucket_id)
        return await BucketReader.open(
            self._store,
            path,
            vault_key=self._vault_key,
            algorithm=self._cached_aes_algorithm(),
            verify_ciphertext_crc=self._verify.ciphertext_crc,
        )

    def _cached_aes_algorithm(self) -> algorithms.AES | None:
        """The AES-256 key schedule for ``vault_key``, built on first use and
        shared by every ``BucketReader`` this pool opens; ``None`` without a
        ``vault_key``. Held per pool, so it never outlives or crosses
        repositories.
        """
        if self._vault_key is None:
            return None
        if self._aes_algorithm is None:
            self._aes_algorithm = build_aes_algorithm(self._vault_key)
        return self._aes_algorithm

    async def _open_bucket_by_key(self, key: tuple[StreamId, BucketId]) -> BucketReader:
        return await self.open_bucket_uncached(*key)

    async def read_chunk(self, addr: ChunkAddress) -> bytes:
        """Resolve ``addr`` to 4096 bytes of plaintext, cached, with this
        pool's ``verify`` checks (a cache hit's fingerprint is checked too).
        Bucket-major export (``chunk_walk.exec_one_bucket_group``) calls
        ``BucketReader.read_chunks`` directly instead.

        Returns:
            The plaintext chunk.

        Raises:
            NotFoundError: The bucket file is missing.
            ChunkCompactedError: The chunk was reclaimed by compaction.
            DataCorruptError: The bucket fails to decode, or a fingerprint or
                CRC check fails.
        """
        key = (addr.stream_id, addr.bucket_id, addr.chunk_idx)

        async def fetch_chunk(_key: tuple[StreamId, BucketId, int]) -> bytes:
            reader = await self.bucket(addr.stream_id, addr.bucket_id)
            return await reader.read_chunk(addr.chunk_idx, addr)

        plain = await self._chunks.resolve(key, fetch_chunk)
        await self.verify_fingerprints(addr.stream_id, addr.bucket_id, {int(addr.chunk_idx): plain})
        return plain

    def cached_chunks(
        self, stream_id: StreamId, bucket_id: BucketId, chunk_indices: Iterable[int]
    ) -> tuple[dict[int, bytes], list[int]]:
        """``(hits, misses)`` for ``chunk_indices`` of one bucket: the
        plaintext already cached, by index, and the indices that are not;
        never fetches. Each lookup counts and refreshes recency as
        ``read_chunk()``'s would. For batch callers (``dedup_file.py``) that
        skip ``read_chunk()``."""
        hits: dict[int, bytes] = {}
        misses: list[int] = []
        lookup = self._chunks.lookup
        for chunk_idx in chunk_indices:
            plain = lookup((stream_id, bucket_id, chunk_idx), None)
            if plain is None:
                misses.append(chunk_idx)
            else:
                hits[chunk_idx] = plain
        return hits, misses

    def backfill_chunks(
        self, stream_id: StreamId, bucket_id: BucketId, plaintexts: Mapping[int, bytes | memoryview]
    ) -> None:
        """Cache decoded ``plaintexts`` (by ``chunk_idx``) of one bucket as if
        ``read_chunk()`` had fetched them; a ``memoryview`` is copied, so the
        cache never pins the buffer it views. Verifies no fingerprint; that
        is the caller's job."""
        self._chunks.put_many(
            ((stream_id, bucket_id, chunk_idx), bytes(plain)) for chunk_idx, plain in plaintexts.items()
        )

    async def verify_fingerprints(
        self,
        stream_id: StreamId,
        bucket_id: BucketId,
        chunks: Mapping[int, bytes | memoryview],
    ) -> None:
        """Check every entry in ``chunks`` (decoded plaintext keyed by
        ``chunk_idx``) against its stored ``.fgp`` digest when this pool's
        ``verify.fingerprint`` is on: ``read_chunk``'s policy, for callers of
        ``BucketReader.read_chunks``.

        Args:
            stream_id: Stream of the bucket.
            bucket_id: The bucket the chunks belong to.
            chunks: Plaintext by ``chunk_idx``.

        Raises:
            DataCorruptError: Any chunk's plaintext SHA-256 doesn't
                match its stored fingerprint, or a sidecar is corrupt.
            NotFoundError: The group's ``.inf`` or ``.fgp`` file is missing.
            FormatError: The ``.inf`` or ``.fgp`` file is truncated.
        """
        if not self._verify.fingerprint:
            return
        expected_by_chunk = await self._fingerprints.digests(stream_id, bucket_id, chunks)
        sha256 = hashlib.sha256
        for chunk_idx, plain in chunks.items():
            if sha256(plain).digest() != expected_by_chunk[chunk_idx]:
                addr = ChunkAddress(stream_id, bucket_id, ChunkIdx(chunk_idx))
                raise DataCorruptError(f"chunk fingerprint mismatch at {addr}", ref=self._pool_root, spec=_SPEC)
