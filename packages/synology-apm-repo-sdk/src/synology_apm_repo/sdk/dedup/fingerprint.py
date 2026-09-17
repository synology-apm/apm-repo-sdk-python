"""Fingerprint lookup: locates the stored SHA-256 digest for one
chunk in its group's ``.inf``/``.fgp`` files. Used by ``verify`` and by
``Pool.read_chunk`` when ``verify_fingerprint`` is enabled.

Correctly handles a chunk whose fingerprint straddles a ``.fgp`` 4 MiB
segment boundary, across both plaintext and encrypted, vault- and
object-store-backed repositories.

**Group path indirection**: ``.inf``/``.fgp`` are
shared by an entire 1024-bucket *group*, keyed by the group's *starting*
bucket id (``bucket_id & ~1023``), not any individual bucket's own id —
the same 10-bit layering scheme as ``.buk`` paths, just applied to the
group id instead. ``.fgp`` is additionally split into 4 MiB segments
(``<prefix>_<segIdx>.fgp``) since a full group's fingerprint data
(1024 buckets * up to 8192 chunks * 32 bytes) would otherwise be a single
multi-hundred-MB file.

**``AllocationTableCache``**: ``fingerprint()``/``fingerprints()`` already
resolve a bucket's own ``.inf`` header + allocation-table entry once per
*call* regardless of how many chunks that call checks — but every one of
up to ``GROUP_BUCKET_NUM`` (1024) distinct *buckets* sharing one group
still pays for the same header validation and entry lookup again, once
per bucket, since nothing persists that resolution across separate calls,
even though the underlying bytes are identical for every bucket in the
group. An optional ``AllocationTableCache`` (kept by ``Pool``, so every
caller reading fingerprints through one — browsing, export, and both
``verify`` orchestrators alike — shares it automatically) resolves the
whole group's allocation table once, the first time any of its buckets is
touched, and answers every later bucket in that same group from memory.
"""

from __future__ import annotations

from collections.abc import Iterable

from ..asynccache import AsyncKeyedCache
from ..errors import DataCorruptError, FormatError
from ..format.addressing import group_start_bucket_id, pool_layer_path, split_layer_leaf
from ..format.const import GROUP_BUCKET_NUM
from ..format.headers import HEADER_LEN, MAGIC, parse_index_header
from ..identifiers import BucketId, ChunkIdx, StreamId
from ..storage.base import ObjectStore, join_path
from ..storage.dircache import DirCache
from ..storage.seqid import resolve_seq_path

_SPEC = "FORMAT-SPEC.md: sidecar-files/chunk-pool-encryption"

# .inf's allocation table: 1024 8-byte entries starting at a
# fixed offset within the (otherwise per-bucket-record) .inf file.
_ALLOC_TABLE_OFFSET = 12288
_ALLOC_ENTRY_LENGTH = 8
_ALLOC_TABLE_LENGTH = GROUP_BUCKET_NUM * _ALLOC_ENTRY_LENGTH
_OFFSET4K_SHIFT = 15
_RECNUM_MASK = (1 << 15) - 1

_FGP_RECORD_LENGTH = 32  # one SHA-256 digest per chunk
_FGP_SEGMENT_SIZE = 4 << 20  # 4 MiB, per-segment .fgp file split


def _group_dir_and_prefix(pool_root: str, stream_id: StreamId, bucket_id: BucketId) -> tuple[str, str]:
    """The directory and filename prefix shared by ``bucket_id``'s
    ``.inf``/``.fgp`` group."""
    group_start = group_start_bucket_id(bucket_id)
    layer_path = pool_layer_path(stream_id, group_start)
    dir_part, leaf = split_layer_leaf(layer_path)
    return join_path(pool_root, dir_part), leaf


async def _read_full_allocation_table(
    store: ObjectStore, dir_cache: DirCache, pool_root: str, stream_id: StreamId, bucket_id: BucketId
) -> tuple[str, str, str, bytes]:
    """Header-validate, then read the *whole* ``GROUP_BUCKET_NUM``-entry
    allocation table for ``bucket_id``'s own group in one shot — the
    once-per-group counterpart to ``_resolve_bucket_allocation``'s own
    once-per-call single-entry read. Returns ``(full_dir, prefix, inf_path,
    table)``, ``table`` being the tight ``_ALLOC_TABLE_LENGTH``-byte blob
    every bucket in the group slices its own 8-byte entry out of."""
    full_dir, prefix = _group_dir_and_prefix(pool_root, stream_id, bucket_id)
    inf_path = await resolve_seq_path(dir_cache, full_dir, f"{prefix}.inf")
    header_bytes = await store.read(inf_path, 0, HEADER_LEN)
    parse_index_header(header_bytes, expect_magic=MAGIC["bucket_meta"], spec=_SPEC)
    table = await store.read(inf_path, _ALLOC_TABLE_OFFSET, _ALLOC_TABLE_LENGTH)
    return full_dir, prefix, inf_path, table


class AllocationTableCache:
    """Caches each distinct ``(stream_id, group's starting bucket id)``'s
    whole, header-validated ``.inf`` allocation table for this cache
    instance's whole lifetime — see this module's own docstring for why.
    Pure in-memory bookkeeping on top of ``AsyncKeyedCache``; owns no
    store/dir_cache/pool_root of its own, since every call already has
    those in hand (mirrors ``dedup.pool.BucketReaderCache``'s own reason
    for the same shape — see that class's docstring)."""

    def __init__(self) -> None:
        self._tables: AsyncKeyedCache[tuple[int, int], tuple[str, str, str, bytes]] = AsyncKeyedCache()

    def clear(self) -> None:
        """Drop every cached allocation table — for a caller releasing a
        whole ``Pool``'s memory (see ``Pool.release_caches``)."""
        self._tables.invalidate()

    async def resolve_entry(
        self, store: ObjectStore, dir_cache: DirCache, pool_root: str, stream_id: StreamId, bucket_id: BucketId
    ) -> tuple[str, str, str, int, int]:
        """``_resolve_bucket_allocation``'s exact return shape
        (``full_dir, prefix, inf_path, byte_off, rec_num``), backed by
        this cache instead of a fresh per-call resolution."""
        group_start = group_start_bucket_id(bucket_id)
        key = (int(stream_id), int(group_start))

        async def _load(_key: tuple[int, int]) -> tuple[str, str, str, bytes]:
            return await _read_full_allocation_table(store, dir_cache, pool_root, stream_id, bucket_id)

        full_dir, prefix, inf_path, table = await self._tables.resolve(key, _load)
        entry_index = bucket_id & (GROUP_BUCKET_NUM - 1)
        entry_off = entry_index * _ALLOC_ENTRY_LENGTH
        entry = table[entry_off : entry_off + _ALLOC_ENTRY_LENGTH]
        if len(entry) < 4:
            raise FormatError(f"{inf_path!r} truncated: no allocation entry for bucket {bucket_id}", ref=inf_path)
        raw_pos = int.from_bytes(entry[0:4], "big")
        byte_off = (raw_pos >> _OFFSET4K_SHIFT) * 4096
        rec_num = raw_pos & _RECNUM_MASK
        return full_dir, prefix, inf_path, byte_off, rec_num


async def _resolve_bucket_allocation(
    store: ObjectStore,
    dir_cache: DirCache,
    pool_root: str,
    stream_id: StreamId,
    bucket_id: BucketId,
    *,
    cache: AllocationTableCache | None = None,
) -> tuple[str, str, str, int, int]:
    """The ``.inf`` header/allocation-table half of a fingerprint lookup —
    shared by every chunk in ``bucket_id``, so a caller checking more than
    one chunk in the same bucket (``fingerprints()`` below) resolves this
    exactly once instead of once per chunk. Returns ``(full_dir, prefix,
    inf_path, byte_off, rec_num)`` — everything ``fingerprint()``'s own
    per-chunk half (fingerprint offset, segment file, final read) needs.

    ``cache`` (default ``None``: every call resolves fresh, the original
    behavior, unchanged) delegates to an ``AllocationTableCache`` instead —
    see its own docstring for what that shares across calls.

    Raises ``FormatError`` if the ``.inf`` file is truncated, or whatever
    ``parse_index_header`` raises if its header fails validation.
    """
    if cache is not None:
        return await cache.resolve_entry(store, dir_cache, pool_root, stream_id, bucket_id)

    full_dir, prefix = _group_dir_and_prefix(pool_root, stream_id, bucket_id)
    inf_path = await resolve_seq_path(dir_cache, full_dir, f"{prefix}.inf")

    header_bytes = await store.read(inf_path, 0, HEADER_LEN)
    parse_index_header(header_bytes, expect_magic=MAGIC["bucket_meta"], spec=_SPEC)

    entry_index = bucket_id & (GROUP_BUCKET_NUM - 1)
    entry_off = _ALLOC_TABLE_OFFSET + entry_index * _ALLOC_ENTRY_LENGTH
    entry = await store.read(inf_path, entry_off, _ALLOC_ENTRY_LENGTH)
    if len(entry) < 4:
        raise FormatError(f"{inf_path!r} truncated: no allocation entry for bucket {bucket_id}", ref=inf_path)
    raw_pos = int.from_bytes(entry[0:4], "big")
    byte_off = (raw_pos >> _OFFSET4K_SHIFT) * 4096
    rec_num = raw_pos & _RECNUM_MASK
    return full_dir, prefix, inf_path, byte_off, rec_num


async def _read_fingerprint(
    store: ObjectStore,
    dir_cache: DirCache,
    *,
    full_dir: str,
    prefix: str,
    inf_path: str,
    byte_off: int,
    rec_num: int,
    bucket_id: BucketId,
    chunk_idx: ChunkIdx,
) -> bytes:
    """The per-chunk half of a fingerprint lookup, given an already-resolved
    bucket allocation (``_resolve_bucket_allocation``)."""
    if chunk_idx >= rec_num:
        raise DataCorruptError(
            f"chunk_idx {chunk_idx} has no fingerprint recorded for bucket {bucket_id} (recNum={rec_num})",
            ref=inf_path,
            spec=_SPEC,
        )

    fp_offset = byte_off + chunk_idx * _FGP_RECORD_LENGTH
    seg_idx = fp_offset // _FGP_SEGMENT_SIZE
    sub_offset = fp_offset % _FGP_SEGMENT_SIZE
    fgp_path = await resolve_seq_path(dir_cache, full_dir, f"{prefix}_{seg_idx}.fgp")
    digest = await store.read(fgp_path, sub_offset, _FGP_RECORD_LENGTH)
    if len(digest) != _FGP_RECORD_LENGTH:
        raise FormatError(
            f"{fgp_path!r} truncated: expected {_FGP_RECORD_LENGTH} bytes at offset {sub_offset}, got {len(digest)}",
            ref=fgp_path,
        )
    return digest


def _fgp_segment_for(byte_off: int, chunk_idx: ChunkIdx) -> int:
    return (byte_off + chunk_idx * _FGP_RECORD_LENGTH) // _FGP_SEGMENT_SIZE


def _group_contiguous_runs(chunk_indices: Iterable[ChunkIdx], byte_off: int) -> list[list[ChunkIdx]]:
    """Splits ``chunk_indices`` into maximal runs of consecutive integers
    that also stay within one ``.fgp`` segment file — the grouping
    ``fingerprints()`` reads one run at a time instead of one chunk at a
    time. Kept free of any I/O so it's trivial to reason about/test on
    its own.

    A deliberately different merge rule from ``dedup.pool.BucketReader
    ._fits_in_run``, not a duplicate of it: that one merges by *byte-gap
    tolerance* between arbitrary offsets (a real gap up to
    ``_GAP_TOLERANCE`` still merges), since ``.buk`` chunk data can be
    fragmented by reclaimed space; every fixed-length ``.fgp`` record
    is always tightly packed with zero gap, so what matters here instead
    is index adjacency plus never crossing a segment file's own boundary
    — a constraint pool.py's own runs have no equivalent of.
    """
    ordered = sorted(set(chunk_indices))
    runs: list[list[ChunkIdx]] = []
    for idx in ordered:
        if (
            runs
            and runs[-1][-1] == idx - 1
            and _fgp_segment_for(byte_off, runs[-1][-1]) == _fgp_segment_for(byte_off, idx)
        ):
            runs[-1].append(idx)
        else:
            runs.append([idx])
    return runs


async def _read_fingerprint_run(
    store: ObjectStore,
    dir_cache: DirCache,
    *,
    full_dir: str,
    prefix: str,
    inf_path: str,
    byte_off: int,
    rec_num: int,
    bucket_id: BucketId,
    run: list[ChunkIdx],
) -> dict[ChunkIdx, bytes]:
    """One contiguous, same-segment run of ``chunk_idx``\\ s, satisfied by
    a single ``store.read()`` spanning the whole run rather than one
    32-byte read per chunk — see ``fingerprints()``'s own docstring for
    why this matters."""
    last = run[-1]
    if last >= rec_num:
        raise DataCorruptError(
            f"chunk_idx {last} has no fingerprint recorded for bucket {bucket_id} (recNum={rec_num})",
            ref=inf_path,
            spec=_SPEC,
        )

    fp_start = byte_off + run[0] * _FGP_RECORD_LENGTH
    seg_idx = fp_start // _FGP_SEGMENT_SIZE
    sub_offset = fp_start % _FGP_SEGMENT_SIZE
    length = len(run) * _FGP_RECORD_LENGTH
    fgp_path = await resolve_seq_path(dir_cache, full_dir, f"{prefix}_{seg_idx}.fgp")
    blob = await store.read(fgp_path, sub_offset, length)
    if len(blob) != length:
        raise FormatError(
            f"{fgp_path!r} truncated: expected {length} bytes at offset {sub_offset}, got {len(blob)}",
            ref=fgp_path,
        )
    return {idx: blob[i * _FGP_RECORD_LENGTH : (i + 1) * _FGP_RECORD_LENGTH] for i, idx in enumerate(run)}


async def fingerprint(
    store: ObjectStore,
    dir_cache: DirCache,
    pool_root: str,
    stream_id: StreamId,
    bucket_id: BucketId,
    chunk_idx: ChunkIdx,
    *,
    cache: AllocationTableCache | None = None,
) -> bytes:
    """Look up the stored 32-byte SHA-256 fingerprint for one chunk.

    ``cache``: see ``_resolve_bucket_allocation``'s own docstring —
    default ``None`` preserves this function's original per-call
    resolution unchanged.

    Raises ``DataCorruptError`` if ``chunk_idx`` is beyond the group's
    recorded fingerprint count for this bucket, or if the ``.inf`` header
    fails validation; ``FormatError`` if either file is truncated.
    """
    full_dir, prefix, inf_path, byte_off, rec_num = await _resolve_bucket_allocation(
        store, dir_cache, pool_root, stream_id, bucket_id, cache=cache
    )
    return await _read_fingerprint(
        store,
        dir_cache,
        full_dir=full_dir,
        prefix=prefix,
        inf_path=inf_path,
        byte_off=byte_off,
        rec_num=rec_num,
        bucket_id=bucket_id,
        chunk_idx=chunk_idx,
    )


async def fingerprints(
    store: ObjectStore,
    dir_cache: DirCache,
    pool_root: str,
    stream_id: StreamId,
    bucket_id: BucketId,
    chunk_indices: Iterable[ChunkIdx],
    *,
    cache: AllocationTableCache | None = None,
) -> dict[ChunkIdx, bytes]:
    """Batched ``fingerprint()`` for every ``chunk_idx`` in
    ``chunk_indices``, all within the same ``(stream_id, bucket_id)`` —
    resolves the shared ``.inf`` header/allocation-table entry exactly
    once regardless of how many chunks are checked, instead of once per
    chunk the way calling ``fingerprint()`` in a loop would (a real gap
    for ``Pool.verify_fingerprints``'s multi-chunk case: every chunk in
    one bucket needs the identical two reads).

    The digests themselves are batched too: ``chunk_indices`` is grouped
    into maximal runs of consecutive integers that also stay within one
    ``.fgp`` segment (``_group_contiguous_runs``), and each run costs one
    ``store.read()`` spanning the whole run rather than one 32-byte read
    per chunk. This is the common case for a bucket-wide sweep (FULL-level
    verify's own "every non-``COMPACTED`` chunk" walk): a large bucket's
    fingerprints fit in a single ``.fgp`` segment, so an unbatched
    per-chunk read would otherwise turn one bucket's worth of checking
    into tens of thousands of separate 32-byte reads. A caller passing genuinely scattered indices
    (no two adjacent) still costs one read per index, same as before —
    grouping never makes an unmergeable case worse.

    ``cache``: see ``_resolve_bucket_allocation``'s own docstring —
    default ``None`` preserves this function's original per-call
    resolution unchanged.

    Raises the same exceptions ``fingerprint()`` does, for the same
    reasons.
    """
    full_dir, prefix, inf_path, byte_off, rec_num = await _resolve_bucket_allocation(
        store, dir_cache, pool_root, stream_id, bucket_id, cache=cache
    )
    result: dict[ChunkIdx, bytes] = {}
    for run in _group_contiguous_runs(chunk_indices, byte_off):
        result.update(
            await _read_fingerprint_run(
                store,
                dir_cache,
                full_dir=full_dir,
                prefix=prefix,
                inf_path=inf_path,
                byte_off=byte_off,
                rec_num=rec_num,
                bucket_id=bucket_id,
                run=run,
            )
        )
    return result
