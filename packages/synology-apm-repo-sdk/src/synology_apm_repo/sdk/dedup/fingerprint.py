"""Fingerprint lookup: locates the stored SHA-256 digests of a bucket's chunks in
its group's ``.inf``/``.fgp`` files (FORMAT-SPEC.md: Sidecar files), for a
``Pool`` whose ``VerifyPolicy.fingerprint`` is on.

**Group path indirection**: ``.inf``/``.fgp`` are shared by an entire
1024-bucket *group*, keyed by the group's starting bucket id
(``bucket_id & ~1023``) — the same 10-bit layering scheme as ``.buk``
paths. ``.fgp`` is further split into 4 MiB segments
(``<prefix>_<segIdx>.fgp``) since a full group's fingerprint data (1024
buckets * up to 8192 chunks * 32 bytes) would otherwise be one
multi-hundred-MB file.

**``FingerprintIndex``** reads a group's whole ``.inf`` allocation table
once and answers every later bucket in it from memory: up to
``GROUP_BUCKET_NUM`` (1024) buckets share one group's table.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable

from ..asynccache import AsyncKeyedCache, CacheStats
from ..cachemanager import DEFAULT_LIMITS
from ..errors import DataCorruptError, FormatError
from ..format.addressing import group_start_bucket_id, pool_layer_path, split_layer_leaf
from ..format.const import GROUP_BUCKET_NUM
from ..format.headers import HEADER_LEN, MAGIC, parse_index_header
from ..identifiers import BucketId, StreamId
from ..storage.base import ObjectStore, join_path
from ..storage.dircache import DirCache
from ..storage.seqid import resolve_seq_path

_SPEC = "FORMAT-SPEC.md: Sidecar files; Chunk pool encryption"

# .inf's allocation table: 1024 8-byte entries starting at a
# fixed offset within the (otherwise per-bucket-record) .inf file.
_ALLOC_TABLE_OFFSET = 12288
_ALLOC_ENTRY_LENGTH = 8
_ALLOC_TABLE_LENGTH = GROUP_BUCKET_NUM * _ALLOC_ENTRY_LENGTH
_OFFSET4K_SHIFT = 15
_OFFSET4K_UNIT = 4096  # the allocation entry's byte offset is in 4 KiB units
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


@dataclasses.dataclass(frozen=True, slots=True)
class _GroupTable:
    """One group's header-validated ``.inf`` and its whole allocation table
    (``_ALLOC_TABLE_LENGTH`` bytes, an 8-byte entry per bucket)."""

    full_dir: str
    prefix: str
    inf_path: str
    table: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class _BucketAllocation:
    """Where one bucket's fingerprint records sit in its group's ``.fgp``
    segments: ``rec_num`` records starting at byte ``byte_off``."""

    group: _GroupTable
    byte_off: int
    rec_num: int


def _group_contiguous_runs(chunk_indices: Iterable[int], byte_off: int) -> list[tuple[int, int]]:
    """Splits ``chunk_indices`` into maximal ``(chunk_idx_start, length)``
    runs of consecutive integers that also stay within one ``.fgp`` segment
    file. Does no I/O.

    Unlike ``BucketReader``'s run merging, tolerates no gap: ``.fgp``
    records are tightly packed.
    """
    runs: list[tuple[int, int]] = []
    start = prev = segment_end = -1
    for idx in sorted(set(chunk_indices)):
        if idx == prev + 1 and idx < segment_end:
            prev = idx
            continue
        if start >= 0:
            runs.append((start, prev - start + 1))
        start = prev = idx
        # First index whose record lies in the next segment.
        segment = (byte_off + idx * _FGP_RECORD_LENGTH) // _FGP_SEGMENT_SIZE
        segment_end = -(-((segment + 1) * _FGP_SEGMENT_SIZE - byte_off) // _FGP_RECORD_LENGTH)
    if start >= 0:
        runs.append((start, prev - start + 1))
    return runs


class FingerprintIndex:
    """The stored SHA-256 fingerprints of one pool's chunks, looked up
    through each group's ``.inf`` allocation table, cached per
    ``(stream_id, group's starting bucket id)`` and LRU-bounded by
    ``maxsize`` tables (8 KiB each)."""

    def __init__(
        self,
        store: ObjectStore,
        dir_cache: DirCache,
        pool_root: str,
        *,
        maxsize: int = DEFAULT_LIMITS.allocation_tables,
    ) -> None:
        self._store = store
        self._dir_cache = dir_cache
        self._pool_root = pool_root
        self._tables: AsyncKeyedCache[tuple[int, int], _GroupTable] = AsyncKeyedCache(maxsize=maxsize)

    def stats(self) -> CacheStats:
        """Counters of the allocation-table cache."""
        return self._tables.stats()

    def clear(self) -> None:
        """Drop every cached allocation table (see ``Pool.release_caches``)."""
        self._tables.invalidate()

    async def digests(self, stream_id: StreamId, bucket_id: BucketId, chunk_indices: Iterable[int]) -> dict[int, bytes]:
        """The stored 32-byte digest of every ``chunk_idx`` in
        ``chunk_indices`` of one bucket. Reads each maximal run of
        consecutive indices within one ``.fgp`` segment with one
        ``store.read()``; scattered indices cost one read each.

        Raises:
            NotFoundError: The group's ``.inf`` or ``.fgp`` file is missing.
            DataCorruptError: A chunk index is beyond this bucket's recorded
                fingerprint count, or the ``.inf`` header fails validation.
            FormatError: The ``.inf`` or ``.fgp`` file is truncated.
        """
        allocation = await self._allocation(stream_id, bucket_id)
        result: dict[int, bytes] = {}
        for run in _group_contiguous_runs(chunk_indices, allocation.byte_off):
            result.update(await self._read_run(allocation, bucket_id, run))
        return result

    async def _allocation(self, stream_id: StreamId, bucket_id: BucketId) -> _BucketAllocation:
        group_start = group_start_bucket_id(bucket_id)

        async def _load(_key: tuple[int, int]) -> _GroupTable:
            full_dir, prefix = _group_dir_and_prefix(self._pool_root, stream_id, bucket_id)
            inf_path = await resolve_seq_path(self._dir_cache, full_dir, f"{prefix}.inf")
            header_bytes = await self._store.read(inf_path, 0, HEADER_LEN)
            parse_index_header(header_bytes, expect_magic=MAGIC["bucket_meta"], spec=_SPEC)
            table = await self._store.read(inf_path, _ALLOC_TABLE_OFFSET, _ALLOC_TABLE_LENGTH)
            return _GroupTable(full_dir, prefix, inf_path, table)

        group = await self._tables.resolve((int(stream_id), int(group_start)), _load)
        entry_off = (bucket_id & (GROUP_BUCKET_NUM - 1)) * _ALLOC_ENTRY_LENGTH
        entry = group.table[entry_off : entry_off + _ALLOC_ENTRY_LENGTH]
        if len(entry) < 4:
            raise FormatError(
                f"{group.inf_path!r} truncated: no allocation entry for bucket {bucket_id}", ref=group.inf_path
            )
        raw_pos = int.from_bytes(entry[0:4], "big")
        return _BucketAllocation(group, (raw_pos >> _OFFSET4K_SHIFT) * _OFFSET4K_UNIT, raw_pos & _RECNUM_MASK)

    async def _read_run(
        self, allocation: _BucketAllocation, bucket_id: BucketId, run: tuple[int, int]
    ) -> dict[int, bytes]:
        """One contiguous, same-segment ``(chunk_idx_start, length)`` run,
        read with a single ``store.read()``."""
        group = allocation.group
        run_start, run_length = run
        last = run_start + run_length - 1
        if last >= allocation.rec_num:
            raise DataCorruptError(
                f"chunk_idx {last} has no fingerprint recorded for bucket {bucket_id} (recNum={allocation.rec_num})",
                ref=group.inf_path,
                spec=_SPEC,
            )
        fp_start = allocation.byte_off + run_start * _FGP_RECORD_LENGTH
        seg_idx = fp_start // _FGP_SEGMENT_SIZE
        sub_offset = fp_start % _FGP_SEGMENT_SIZE
        length = run_length * _FGP_RECORD_LENGTH
        fgp_path = await resolve_seq_path(self._dir_cache, group.full_dir, f"{group.prefix}_{seg_idx}.fgp")
        blob = await self._store.read(fgp_path, sub_offset, length)
        if len(blob) != length:
            raise FormatError(
                f"{fgp_path!r} truncated: expected {length} bytes at offset {sub_offset}, got {len(blob)}",
                ref=fgp_path,
            )
        return {
            run_start + i: blob[offset : offset + _FGP_RECORD_LENGTH]
            for i, offset in enumerate(range(0, length, _FGP_RECORD_LENGTH))
        }
