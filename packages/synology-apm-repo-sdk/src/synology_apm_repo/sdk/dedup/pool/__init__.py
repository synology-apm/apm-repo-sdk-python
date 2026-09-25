"""Chunk pool: resolves a ``ChunkAddress`` to plaintext bytes.

The only place chunk decrypt+decompress happens. Three collaborating
classes: ``BucketReader`` (``_bucket_reader.py`` — one ``.buk`` file's
header/SizeStore/locators plus the actual per-chunk read), ``Pool`` (this
module — repository-wide entry point — resolves ``(streamID, bucketID)``
to the right ``.buk`` path and caches both ``BucketReader``\\ s and
decoded plaintext chunks), and ``BucketReaderCache`` (``_cache.py`` — the
private cache shape a bulk sweep needs instead of ``Pool``'s own
session-wide bounded one — unbounded by default, since export's own
repeat-visit pattern needs it, but a caller whose own access pattern
doesn't benefit from unbounded growth passes its own ``maxsize``).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

from ...asynccache import AsyncKeyedCache
from ...errors import DataCorruptError
from ...format.addressing import ChunkAddress, pool_layer_path, split_layer_leaf
from ...identifiers import BucketId, ChunkIdx, StreamId
from ...storage.base import ObjectStore, join_path
from ...storage.dircache import DirCache
from ...storage.seqid import resolve_seq_path
from ..fingerprint import AllocationTableCache, fingerprints
from ._bucket_reader import BucketReader
from ._cache import BucketReaderCache

__all__ = [
    "DEFAULT_BUCKET_CACHE_SIZE",
    "DEFAULT_CHUNK_CACHE_SIZE",
    "INTERACTIVE_BUCKET_CACHE_SIZE",
    "BucketReader",
    "BucketReaderCache",
    "Pool",
]

_SPEC = "FORMAT-SPEC.md: sidecar-files/chunk-pool-encryption"

DEFAULT_BUCKET_CACHE_SIZE = 16
"""``Pool``'s own default ``bucket_cache_size`` -- also the value every
``BucketReaderCache(maxsize=...)`` construction site outside this module
(``chunk_walk.py``, ``export_scheduler.py``, ``units.content.pcps_disk``)
reuses for its own default bound, so those three stay tied to this one
number instead of each separately hardcoding a copy that could silently
drift from it."""

DEFAULT_CHUNK_CACHE_SIZE = 4096
"""``Pool``'s own default ``chunk_cache_size`` -- also reused by
``dedup.repository.DedupRepo``'s own matching default, so the two stay
tied to this one number instead of each separately hardcoding a copy that
could silently drift from it."""

INTERACTIVE_BUCKET_CACHE_SIZE = 64
"""``bucket_cache_size`` for the one ``Pool`` every non-bulk consumer of
a repository's own catalog shares (``api.repository.Repository.
_open_catalog_resources``, reached by both TUI/SDK browsing and one-shot CLI
commands like ``ls``/``tree``/``doctor`` via ``Repository.catalogs()``)
-- bigger than ``DEFAULT_BUCKET_CACHE_SIZE`` because a single real
directory listing under a PC/VM device's disk image can touch several
dozen distinct ``.buk`` files at once, which ``DEFAULT_BUCKET_CACHE_SIZE``'s
16 slots can't hold without evicting and later re-opening some of them
mid-listing. Deliberately a new constant rather than raising
``DEFAULT_BUCKET_CACHE_SIZE`` itself, which bulk-export's own
``BucketReaderCache``\\ s are tuned to for a different access pattern
(each bucket touched once, sequentially across a whole sweep) that gains
nothing from a bigger bound the way one deep tree walk does."""


class Pool:
    """Repository-wide chunk pool entry point.

    Resolves any ``ChunkAddress`` to its 4096-byte plaintext, with
    two-level LRU caching: ``BucketReader``\\ s (header + locators, cheap,
    kept many) and decoded plaintext chunks (more expensive, kept fewer).
    Both are ``AsyncKeyedCache`` instances — session-wide, shared across
    every interactive caller —
    which already gives concurrency-safety (an internal lock, held only
    around bookkeeping, never around the I/O of opening a bucket or
    reading/decrypting/decompressing a chunk) and in-flight fetch
    de-duplication (two concurrent callers missing the same key pay for
    one fetch, not two) for free. ``backfill_chunk`` is the one
    direct-insert exception to that de-duplication guarantee.
    """

    def __init__(
        self,
        store: ObjectStore,
        pool_root: str,
        dir_cache: DirCache,
        *,
        vault_key: bytes | None = None,
        bucket_cache_size: int = DEFAULT_BUCKET_CACHE_SIZE,
        chunk_cache_size: int = DEFAULT_CHUNK_CACHE_SIZE,
        verify_fingerprint: bool = False,
        verify_ciphertext_crc: bool = False,
    ) -> None:
        self._store = store
        self._pool_root = pool_root
        self._dir_cache = dir_cache
        self._vault_key = vault_key
        # This is the session-wide default; a per-call ``verify_fingerprint=``
        # argument to read_chunk()/verify_fingerprints() overrides it.
        self._verify_fingerprint = verify_fingerprint
        # Threaded into every BucketReader this Pool opens (open_bucket_uncached)
        # as that reader's own read_chunk/read_chunks default.
        # Unlike verify_fingerprint, there's no separate Pool-level
        # verify_fingerprints()-style method for this: ChunkCrcStore lives
        # inside the bucket file BucketReader already owns, not a separate
        # sidecar Pool has to locate.
        self._verify_ciphertext_crc = verify_ciphertext_crc
        self._buckets: AsyncKeyedCache[tuple[StreamId, BucketId], BucketReader] = AsyncKeyedCache(
            self.open_bucket_uncached_by_key, maxsize=bucket_cache_size
        )
        self._chunks: AsyncKeyedCache[tuple[StreamId, BucketId, ChunkIdx], bytes] = AsyncKeyedCache(
            maxsize=chunk_cache_size
        )
        # Shared by every fingerprint lookup this Pool ever does (below,
        # and via verify_fingerprints()) — up to GROUP_BUCKET_NUM buckets
        # sharing one .inf group only ever pay for its own header
        # validation/allocation-table read once, not once per bucket.
        self._allocation_cache = AllocationTableCache()
        # Read via the release_epoch property below.
        self._release_epoch = 0

    def release_caches(self) -> None:
        """Drop everything this pool holds in memory: decoded chunks, open
        bucket readers, and allocation tables.

        Closing a repository does not, on its own, free any of this — the
        caches live on the ``Pool``, which a caller can still be holding a
        reference to. Anything walking several repositories in one process
        (a smoke run, a TUI session browsing one after another) would
        otherwise keep every pool it ever opened fully populated. The
        ``DirCache`` is deliberately untouched: it belongs to the store,
        not to this pool.
        """
        self._buckets.invalidate()
        self._chunks.invalidate()
        self._allocation_cache.clear()
        self._release_epoch += 1

    @property
    def release_epoch(self) -> int:
        """Bumped once per ``release_caches()`` call -- a caller
        fetching a chunk's plaintext some other way than
        ``read_chunk()``/``resolve()`` (``dedup_file.py``'s
        ``_resolve_bucket_group``) captures this before starting, then
        skips ``backfill_chunk`` if it's changed by the time the fetch
        finishes."""
        return self._release_epoch

    @property
    def store(self) -> ObjectStore:
        """Needed by ``dedup.pool_descriptor.PoolDescriptor.from_pool`` to
        describe an equivalent ``Pool`` for a multiprocess worker to
        rebuild — the same reason ``dedup.repository.DedupRepo`` already
        exposes its own ``store``."""
        return self._store

    @property
    def pool_root(self) -> str:
        return self._pool_root

    @property
    def vault_key(self) -> bytes | None:
        return self._vault_key

    @property
    def verify_fingerprint(self) -> bool:
        """This ``Pool``'s own session-wide default — needed alongside
        ``store``/``pool_root``/``vault_key`` by
        ``dedup.pool_descriptor.PoolDescriptor.from_pool`` so a
        multiprocess worker's own rebuilt ``Pool`` mirrors this one's
        actual configuration instead of silently resetting it."""
        return self._verify_fingerprint

    @property
    def verify_ciphertext_crc(self) -> bool:
        return self._verify_ciphertext_crc

    async def bucket_path(self, stream_id: StreamId, bucket_id: BucketId) -> str:
        """Resolve ``(stream_id, bucket_id)`` to its physical ``.buk`` path
        (including any ``.<seqId>`` generation suffix), relative to the
        repository root."""
        layer_path = pool_layer_path(stream_id, bucket_id)
        dir_part, leaf = split_layer_leaf(layer_path)
        logical_name = f"{leaf}.buk"
        full_dir = join_path(self._pool_root, dir_part)
        return await resolve_seq_path(self._dir_cache, full_dir, logical_name)

    async def bucket(self, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        """Return the (cached) ``BucketReader`` for ``(stream_id,
        bucket_id)``, opening and caching it on first access."""
        return await self._buckets.resolve((stream_id, bucket_id))

    async def open_bucket_uncached(self, stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        """Open ``(stream_id, bucket_id)`` fresh, **without** touching
        ``_buckets`` at all — no lookup, no insert, no eviction.

        Exists for callers that need their own private, separately-scoped
        ``BucketReader`` cache instead of this shared one (a bulk sweep like
        ``export_scheduler.export_to``'s bucket-major path, or ``verify``'s
        Bucket-and-key stage — so a sweep touching every bucket once
        doesn't evict this ``Pool``'s own genuinely-hot interactive
        entries). ``_buckets`` itself is bound to call this for its
        own miss path (see ``Pool.__init__``); the two never diverge in
        how a bucket actually gets opened, only in whether the result is
        remembered in the shared cache afterward.
        """
        path = await self.bucket_path(stream_id, bucket_id)
        return await BucketReader.open(
            self._store, path, vault_key=self._vault_key, verify_ciphertext_crc=self._verify_ciphertext_crc
        )

    async def open_bucket_uncached_by_key(self, key: tuple[StreamId, BucketId]) -> BucketReader:
        """``open_bucket_uncached``, taking its ``(stream_id, bucket_id)``
        as one tuple — the single-arg shape ``AsyncKeyedCache.resolve``'s
        ``fetch`` callback needs, so every caller building a
        ``BucketReader`` cache keyed this way (this class's own
        ``_buckets``, ``chunk_walk.py``'s bucket-major export, ``verify``'s
        Bucket stage) can pass this bound method directly instead of each
        writing its own ``lambda key: pool.open_bucket_uncached(*key)``."""
        return await self.open_bucket_uncached(*key)

    async def read_chunk(
        self,
        addr: ChunkAddress,
        *,
        cache: bool = True,
        verify_fingerprint: bool | None = None,
        verify_ciphertext_crc: bool | None = None,
    ) -> bytes:
        """Resolve ``addr`` — using *its own* embedded ``stream_id``/
        ``bucket_id``,
        not any caller-assumed values — to 4096 bytes of plaintext. Not
        used by the bucket-major export scheduler
        (``chunk_walk.py``'s ``_exec_one_bucket_group``), which calls
        ``BucketReader.read_chunks`` directly, bypassing this class
        entirely.

        ``cache=False`` bypasses the plaintext chunk cache entirely — for
        a one-off integrity check of a single chunk per bucket, which has
        nothing to gain from caching it and would only evict genuinely-hot
        interactive data for no benefit.

        ``verify_fingerprint`` (``None``: defer to whatever this
        ``Pool`` was constructed with; an explicit ``True``/``False``
        overrides it for this one call) compares this chunk's plaintext
        SHA-256 against its stored ``.fgp`` fingerprint and raises
        ``DataCorruptError`` on a mismatch —
        a general data-integrity check independent of encryption, off by
        default since it costs one extra ``.inf``/``.fgp`` read per
        chunk. A plaintext-cache hit is checked too, not skipped, since
        the point is verifying the bytes about to be handed back.

        ``verify_ciphertext_crc`` — forwarded to
        ``BucketReader.read_chunk`` as-is (``None`` defers to whatever
        that reader was opened with). Checked
        before decrypting, so it still requires a vault key for an
        encrypted bucket to get as far as returning plaintext — a caller
        that wants the ChunkCrcStore check in isolation, independent of
        whether a key is even available, wants
        ``BucketReader.verify_chunk_ciphertext_crc`` instead.
        """
        key = (addr.stream_id, addr.bucket_id, addr.chunk_idx)

        async def fetch_chunk(_key: tuple[StreamId, BucketId, ChunkIdx]) -> bytes:
            reader = await self.bucket(addr.stream_id, addr.bucket_id)
            return await reader.read_chunk(addr.chunk_idx, addr, verify_ciphertext_crc=verify_ciphertext_crc)

        plain = await self._chunks.resolve(key, fetch_chunk) if cache else await fetch_chunk(key)
        await self.verify_fingerprints(
            addr.stream_id, addr.bucket_id, {int(addr.chunk_idx): plain}, verify_fingerprint=verify_fingerprint
        )
        return plain

    def cached_chunk(self, addr: ChunkAddress) -> bytes | None:
        """``addr``'s already-decoded plaintext if it's currently in
        ``_chunks``, else ``None`` -- never fetches, never blocks (backed
        by ``AsyncKeyedCache``'s own read-only ``Mapping.get``). For a
        caller like ``dedup_file.py``'s multi-chunk batch path that wants
        to skip re-fetching a chunk another read already populated,
        without going through ``read_chunk()``'s own single-key fetch
        shape."""
        return self._chunks.get((addr.stream_id, addr.bucket_id, addr.chunk_idx))

    def backfill_chunk(self, addr: ChunkAddress, plain: bytes) -> None:
        """The write half of ``cached_chunk``'s peek: remembers ``addr``'s
        already-decoded ``plain`` bytes in ``_chunks``, as if a
        ``read_chunk(addr)`` call had fetched them. Does not itself
        verify a fingerprint -- the caller is responsible for that before
        backfilling."""
        self._chunks.put((addr.stream_id, addr.bucket_id, addr.chunk_idx), plain)

    async def verify_fingerprints(
        self,
        stream_id: StreamId,
        bucket_id: BucketId,
        chunks: Mapping[int, bytes | memoryview],
        *,
        verify_fingerprint: bool | None = None,
    ) -> None:
        """Check every entry in ``chunks`` (already-decoded plaintext, keyed
        by its own ``chunk_idx`` within ``(stream_id, bucket_id)``) against
        its stored ``.fgp`` digest — the same policy/lookup ``read_chunk``
        applies to its own single chunk, factored out here so
        ``BucketReader.read_chunks``'s multi-chunk batch result (fetched
        by ``dedup_file.py``'s ``_fill_data_extent`` and
        ``chunk_walk.py``'s ``_exec_one_bucket_group``, both of which call
        ``read_chunks`` directly and so bypass ``read_chunk`` entirely) can
        honor it too, instead of silently skipping verification whenever a
        read/export spans more than one distinct chunk.

        ``verify_fingerprint`` — same meaning as ``read_chunk``'s own
        parameter: ``None`` defers to this ``Pool``'s session-wide default.

        Raises:
            DataCorruptError: Any chunk's plaintext SHA-256 doesn't match its
                stored fingerprint.
        """
        should_verify = self._verify_fingerprint if verify_fingerprint is None else verify_fingerprint
        if not should_verify:
            return
        # One batched fingerprints() call resolves the .inf header/
        # allocation-table entry shared by every chunk in this bucket
        # exactly once, instead of once per chunk -- real for a
        # multi-chunk read/export with verify_fingerprint enabled.
        # self._allocation_cache additionally shares that resolution
        # across separate calls/buckets in the same .inf group, since
        # up to GROUP_BUCKET_NUM buckets in it share identical bytes.
        chunk_indices = [ChunkIdx(raw_chunk_idx) for raw_chunk_idx in chunks]
        expected_by_chunk = await self.fingerprints_for(stream_id, bucket_id, chunk_indices)
        for raw_chunk_idx, plain in chunks.items():
            chunk_idx = ChunkIdx(raw_chunk_idx)
            if hashlib.sha256(plain).digest() != expected_by_chunk[chunk_idx]:
                addr = ChunkAddress(stream_id, bucket_id, chunk_idx)
                raise DataCorruptError(f"chunk fingerprint mismatch at {addr}", ref=self._pool_root, spec=_SPEC)

    async def fingerprints_for(
        self, stream_id: StreamId, bucket_id: BucketId, chunk_indices: Sequence[ChunkIdx]
    ) -> dict[ChunkIdx, bytes]:
        """Batched ``dedup.fingerprint.fingerprints()``, using this Pool's
        own ``AllocationTableCache`` — ``verify_fingerprints``'s own
        lookup half (resolves the stored digests only, no plaintext
        comparison of its own), factored out so its own ``.inf``/``.fgp``
        resolution shares this ``Pool``'s cache the same way every other
        lookup here does."""
        return await fingerprints(
            self._store,
            self._dir_cache,
            self._pool_root,
            stream_id,
            bucket_id,
            chunk_indices,
            cache=self._allocation_cache,
        )
