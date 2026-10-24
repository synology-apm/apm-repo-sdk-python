"""``DedupFile``: the sole cross-layer read contract.

Any workload — VM disk image, FS file, SaaS raw object — ultimately
reduces to a ``(stream_id, session_id, comp_offset)`` triple plus a size.
Everything above this module (catalog, units, CLI, TUI) reads content
through ``read()``/``stream()``/``export_range()``, never a bucket or
chunk. ``ByteRangeView`` is the second shared
primitive: FS's ``content_dedup_id`` + ``file_size`` and SaaS's
``object_table.(offset, length)`` are both a named sub-range of a bigger
dedup file.

Every ``read()`` locates its starting chunk-map record by
``CompositionRecord.entries``'s binary search, never a linear scan.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Iterable
from typing import Protocol, override, runtime_checkable

from ..concurrency import bounded_gather_first_error
from ..errors import ResourceLimitExceededError
from ..format.addressing import ChunkAddress
from ..format.chunkmap import ChunkMapKind
from ..format.composition import RecordHead
from ..format.const import FIXED_CHUNK_LENGTH
from ..identifiers import BucketId, ChunkIdx, SessionId, StreamId
from . import export_scheduler
from .chunk_walk import (
    ascending_runs,
    count_planned_bytes,
    data_extent_k_bounds,
    decode_bucket_chunks,
    iter_chunk_runs,
    merge_overlapping_ranges,
)
from .composition_reader import CompositionReader, CompositionRecord
from .export_sink import ExportWriter, WrittenBytesCallback
from .extent import DataExtent, ExportResult, Extent, ExtentKind, GapExtent
from .pool import Pool
from .pool_descriptor import PoolDescriptor

DEFAULT_STREAM_BLOCK = 8 << 20  # 8 MiB
"""Default block size of every ``ContentSource`` implementer's ``stream()``."""

#: Ceiling on one ``DedupFile.read()``/``ByteRangeView.read()`` call's
#: ``bytearray(length)`` allocation, since ``length`` traces back to
#: repository-declared metadata with no bound of its own. A caller needing
#: more uses ``stream()``, which allocates at most one
#: ``DEFAULT_STREAM_BLOCK`` at a time.
MAX_SINGLE_READ_SIZE = 1 << 30  # 1 GiB

#: Buckets one ``read()`` fetches from at once, so a read spanning several
#: costs about one storage round trip rather than one per bucket. Times
#: ``VirtualDiskContentSource``'s 4 fragments, a PC/PS disk read has up to
#: 32 store reads in flight, S3's whole connection pool; on SMB's 16
#: sessions the rest queue.
_READ_BUCKETS_IN_FLIGHT = 8

#: Chunks (16 MiB) one ``read()`` plans before fetching them, so a large
#: read holds the plan and tasks of one batch at a time.
_READ_BATCH_CHUNKS = 4096


@runtime_checkable
class _Readable[B: bytes | bytearray](Protocol):
    """The structural subset of ``ContentSource`` ``stream_via_read`` needs
    (redeclared because ``dedup/`` sits below ``units/``), generic in what
    ``read`` returns so a ``bytes`` source streams ``bytes``."""

    @property
    def size(self) -> int | None: ...

    async def read(self, offset: int = 0, length: int | None = None) -> B: ...


async def stream_via_read[B: bytes | bytearray](
    source: _Readable[B], block: int = DEFAULT_STREAM_BLOCK
) -> AsyncIterator[tuple[int, B]]:
    """Yield ``(offset, bytes)`` blocks front-to-back via repeated
    ``source.read(offset, block)`` calls.

    Raises:
        ValueError: ``source.size`` is ``None``.
    """
    if source.size is None:
        raise ValueError("stream() requires a known size")
    async for item in read_blocks(source, 0, source.size, block):
        yield item


def validate_read_args(offset: int, length: int | None) -> None:
    """Rejects a negative ``offset``/``length`` — the ``ValueError`` half of
    ``ContentSource.read``'s contract, shared by every implementer that
    doesn't need ``DedupFile``'s own per-argument messages.

    Raises:
        ValueError: ``offset`` or ``length`` is negative.
    """
    if offset < 0 or (length is not None and length < 0):
        raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")


def clamp_read_length(offset: int, length: int | None, size: int) -> int:
    """Resolve a ``read(offset, length)`` request against a known
    ``size``: fills in the default length (``offset`` to ``size``) and
    clamps a request extending past ``size`` down to what's actually
    there. Every ``ContentSource`` implementer shares this contract:
    reading past the end returns fewer bytes, never an error — only a
    negative ``offset``/``length`` is left for the caller to reject before
    calling this.

    Returns:
        The byte count to read, ``>= 0``.
    """
    available = size - offset
    if length is None:
        length = available
    return max(0, min(length, available))


def validate_export_range(start: int, end: int, size: int) -> None:
    """Checks ``0 <= start <= end <= size`` — the range a
    ``ContentSource.export_range`` call may ask for.

    Raises:
        ValueError: The range is not inside ``[0, size]``.
    """
    if not 0 <= start <= end <= size:
        raise ValueError(f"export range [{start}, {end}) is not inside [0, {size})")


def _require_chunk_aligned(offset: int) -> None:
    if offset % FIXED_CHUNK_LENGTH:
        raise ValueError(f"export range must start at a multiple of {FIXED_CHUNK_LENGTH}, got {offset}")


async def read_blocks[B: bytes | bytearray](
    source: _Readable[B], start: int, end: int, block: int = DEFAULT_STREAM_BLOCK
) -> AsyncIterator[tuple[int, B]]:
    """Yield ``(offset, bytes)`` blocks of ``source``'s ``[start, end)`` via
    repeated ``source.read(start + offset, block)`` calls, with ``offset``
    counted from ``start``. The offset advances by the requested length, not
    the returned one, so a short read cannot stall the loop."""
    offset = 0
    while start + offset < end:
        n = min(block, end - start - offset)
        yield offset, await source.read(start + offset, n)
        offset += n


async def _resolve_bucket_group(
    pool: Pool, stream_id: StreamId, bucket_id: BucketId, chunk_indices: Iterable[int]
) -> dict[int, bytes | memoryview]:
    """Resolve every ``chunk_idx`` in ``chunk_indices`` (one ``_BucketGroup``'s
    chunks) to its plaintext.

    Cached chunks (``Pool.cached_chunks``) are reused; the rest are decoded
    through ``Pool``'s own bucket cache (``chunk_walk.decode_bucket_chunks``)
    and backfilled (``Pool.backfill_chunks``) unless ``Pool.release_epoch``
    moved mid-fetch, all under ``Pool.batch_fetch``, so a concurrent batch of
    the same bucket is waited for rather than repeated. All are
    fingerprint-verified, cache hits included, as in ``Pool.read_chunk()``.
    """
    async with pool.batch_fetch(stream_id, bucket_id):
        already_cached, still_needed = pool.cached_chunks(stream_id, bucket_id, chunk_indices)
        decoded: dict[int, bytes | memoryview] = {}
        if still_needed:
            release_epoch = pool.release_epoch
            reader = await pool.bucket(stream_id, bucket_id)
            decoded = await decode_bucket_chunks(reader, stream_id, bucket_id, ascending_runs(still_needed), pool=pool)
            # The epoch check stops a backfill from refilling a Pool that a
            # concurrent release_caches() just emptied.
            if pool.release_epoch == release_epoch:
                pool.backfill_chunks(stream_id, bucket_id, decoded)
    if already_cached:
        # decode_bucket_chunks() verified what it decoded; cache hits are checked here.
        await pool.verify_fingerprints(stream_id, bucket_id, already_cached)
    return {**already_cached, **decoded}


@dataclasses.dataclass(frozen=True, slots=True)
class _BucketGroup:
    """The chunks one DATA extent needs from one bucket to fill
    ``[seg_start, seg_end)``, a sub-range of the extent's span."""

    stream_id: StreamId
    bucket_id: BucketId
    #: ``(chunk_idx_start, length, k_start)`` runs; a repeat region maps
    #: several k's onto one physical chunk.
    runs: tuple[tuple[int, int, int], ...]
    #: ``runs``' chunk indices merged into ascending, non-overlapping ranges.
    chunk_ranges: tuple[tuple[int, int], ...]
    extent_offset: int
    seg_start: int
    seg_end: int
    #: The extent needs exactly this one chunk, from this one bucket.
    single_chunk: bool

    @property
    def chunks(self) -> int:
        """Chunk slots the group fills, repeats counted."""
        return sum(length for _, length, _ in self.runs)


def _bucket_groups(extent: DataExtent, seg_start: int, seg_end: int) -> list[_BucketGroup]:
    """``extent``'s chunks for ``[seg_start, seg_end)``, grouped by
    ``(stream_id, bucket_id)``. No I/O."""
    assert extent.map_num > 0
    first_k, last_k = data_extent_k_bounds(extent, seg_start, seg_end)
    runs_by_bucket: dict[tuple[StreamId, BucketId], list[tuple[int, int, int]]] = {}
    for start_addr, run_len, k_start in iter_chunk_runs(extent.addr, extent.map_num, first_k, last_k):
        runs_by_bucket.setdefault((start_addr.stream_id, start_addr.bucket_id), []).append(
            (start_addr.chunk_idx, run_len, k_start)
        )
    groups = []
    for (stream_id, bucket_id), runs in runs_by_bucket.items():
        merged = merge_overlapping_ranges((start, length) for start, length, _ in runs)
        single_chunk = len(runs_by_bucket) == 1 and len(merged) == 1 and merged[0][1] == 1
        groups.append(
            _BucketGroup(
                stream_id, bucket_id, tuple(runs), tuple(merged), extent.offset, seg_start, seg_end, single_chunk
            )
        )
    return groups


async def _fetch_group(pool: Pool, group: _BucketGroup) -> dict[int, bytes | memoryview]:
    """``group``'s plaintext by ``chunk_idx``. A single chunk goes through
    ``Pool.read_chunk``; several through ``_resolve_bucket_group``. Both
    paths use ``Pool``'s chunk cache."""
    if group.single_chunk:
        chunk_idx = group.chunk_ranges[0][0]
        return {chunk_idx: await pool.read_chunk(ChunkAddress(group.stream_id, group.bucket_id, ChunkIdx(chunk_idx)))}
    indices = [idx for start, length in group.chunk_ranges for idx in range(start, start + length)]
    return await _resolve_bucket_group(pool, group.stream_id, group.bucket_id, indices)


def _copy_group(group: _BucketGroup, plain: dict[int, bytes | memoryview], out: memoryview, out_base: int) -> None:
    """Copy ``group``'s chunks into ``out`` (file offset ``out_base`` at
    index 0), clipped to ``[seg_start, seg_end)``; a chunk shorter than
    ``FIXED_CHUNK_LENGTH`` leaves the rest of its slot zero."""
    seg_start, seg_end = group.seg_start, group.seg_end
    for chunk_idx_start, length, k_start in group.runs:
        chunk_start = group.extent_offset + k_start * FIXED_CHUNK_LENGTH
        for chunk_idx in range(chunk_idx_start, chunk_idx_start + length):
            chunk = plain[chunk_idx]
            chunk_end = chunk_start + len(chunk)
            if seg_start <= chunk_start and chunk_end <= seg_end:
                # The whole chunk lies inside the segment: the common case.
                dest = chunk_start - out_base
                out[dest : dest + len(chunk)] = chunk
            else:
                lo = max(seg_start - chunk_start, 0)
                hi = min(seg_end - chunk_start, len(chunk))
                if hi > lo:
                    dest = chunk_start + lo - out_base
                    out[dest : dest + hi - lo] = chunk[lo:hi]
            chunk_start += FIXED_CHUNK_LENGTH


async def _fill_groups(pool: Pool, groups: list[_BucketGroup], out: memoryview, out_base: int) -> None:
    """Fetch every group and copy it into ``out`` as soon as it arrives.

    One bucket's groups are fetched in order, so a later one finds the
    chunks an earlier one cached; different buckets are fetched side by
    side, at most ``_READ_BUCKETS_IN_FLIGHT`` at once. Groups fill
    disjoint parts of ``out``. A failure is raised as a serial loop would
    raise it: the error of the first failing bucket in file order, bare.
    """
    by_bucket: dict[tuple[StreamId, BucketId], list[_BucketGroup]] = {}
    for group in groups:
        by_bucket.setdefault((group.stream_id, group.bucket_id), []).append(group)

    async def fill_bucket(bucket_groups: list[_BucketGroup]) -> None:
        for group in bucket_groups:
            _copy_group(group, await _fetch_group(pool, group), out, out_base)

    if len(by_bucket) <= 1:
        for bucket_groups in by_bucket.values():
            await fill_bucket(bucket_groups)
        return
    await bounded_gather_first_error(by_bucket.values(), fill_bucket, max_concurrent=_READ_BUCKETS_IN_FLIGHT)


class DedupContent:
    """The content methods ``DedupFile`` and ``ByteRangeView`` share, in
    the coordinates of the window ``_window()`` names within its base file:
    the whole file for a ``DedupFile``, the view's range for a
    ``ByteRangeView``."""

    size: int | None

    def _window(self) -> tuple[DedupFile, int]:
        """``(base file, window start)``."""
        raise NotImplementedError

    async def read(self, offset: int = 0, length: int | None = None) -> bytes | bytearray:
        raise NotImplementedError

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes | bytearray]]:
        """Yield ``(offset, bytes)`` blocks front-to-back via ``read``; needs
        a known ``size``."""
        return stream_via_read(self, block)

    def export_window(self) -> tuple[DedupFile, int, int]:
        """``(base file, window start, size)`` — the range an export of this
        content covers.

        Raises:
            ValueError: The size is unknown.
        """
        if self.size is None:
            raise ValueError("exporting requires a known size")
        base, window_start = self._window()
        return base, window_start, self.size

    async def export_range(
        self,
        writer: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: WrittenBytesCallback | None = None,
        tuning: export_scheduler.ExportTuning | None = None,
    ) -> ExportResult:
        """Write this content's ``[start, end)`` into ``writer``, at offsets
        relative to ``start``; ``start`` must fall on a 4096-byte boundary of
        the base file. Keywords and errors: see
        ``export_scheduler.export_to_writer``.

        Raises:
            ValueError: The range is not inside this content, ``start`` is
                not chunk-aligned, or the size is unknown.
        """
        base, window_start, size = self.export_window()
        validate_export_range(start, end, size)
        _require_chunk_aligned(window_start + start)
        target = self if (start, end) == (0, size) else base.view(window_start + start, end - start)
        return await export_scheduler.export_to_writer(target, writer, sparse=sparse, progress=progress, tuning=tuning)

    async def planned_bytes(self, start: int, end: int) -> int:
        """The real ``DATA`` bytes an export of ``[start, end)`` writes, which
        is what its progress counts."""
        base, window_start = self._window()
        return await count_planned_bytes(base, window_start + start, window_start + end)

    def pool_descriptor(self) -> PoolDescriptor | None:
        """How a worker process rebuilds the base file's ``Pool``, or ``None``
        when its store cannot be rebuilt in another process."""
        base, _ = self._window()
        return PoolDescriptor.from_pool(base.pool)


class DedupFile(DedupContent):
    """A logical, byte-addressable file backed by one composition record.

    ``size`` is supplied by the caller (``file_meta.file_size``,
    ``object_table.file_size``, ...), never inferred from the composition
    record, which may end before the file does (trailing ``HOLE``). ``None``
    means unknown: ``read`` then needs an explicit ``length`` and exports
    raise ``ValueError``.
    """

    def __init__(
        self,
        comp_reader: CompositionReader,
        pool: Pool,
        comp_offset: int,
        *,
        size: int | None = None,
    ) -> None:
        self._comp_reader = comp_reader
        self._pool = pool
        self._comp_offset = comp_offset
        self.size = size
        self._record: CompositionRecord | None = None

    @property
    def stream_id(self) -> StreamId:
        return self._comp_reader.stream_id

    @property
    def session_id(self) -> SessionId:
        return self._comp_reader.session_id

    @property
    def comp_offset(self) -> int:
        return self._comp_offset

    @property
    def pool(self) -> Pool:
        """The ``Pool`` this file's chunks resolve through."""
        return self._pool

    async def cached_record(self, *, record_head: RecordHead | None = None) -> CompositionRecord:
        """This file's own ``CompositionRecord``, fetched on first call and
        cached for this file's life; a given ``record_head`` (the
        already-parsed head) saves that first fetch reading it."""
        if self._record is None:
            self._record = await self._comp_reader.record(self._comp_offset, record_head=record_head)
        return self._record

    async def _extents(self, start: int = 0, end: int | None = None) -> AsyncIterator[Extent]:
        """Yield ``Extent``\\ s covering ``[start, end)`` (default: the
        whole file), converting gaps between chunk-map records into
        explicit ``HOLE`` extents and, if ``size`` extends past the last
        record, a trailing ``HOLE``.

        Extents are **not truncated** to ``[start, end)``: a boundary entry
        may start before ``start`` or run past ``end``, so callers intersect
        it with their window.

        Private; ``chunk_walk``'s module docstring covers its one outside
        caller.
        """
        record = await self.cached_record()
        cursor = start
        async for entry in record.entries(start, end):
            if entry.length == 0:
                continue
            if entry.file_offset > cursor:
                yield GapExtent(cursor, entry.file_offset - cursor, ExtentKind.HOLE)
            if entry.kind is ChunkMapKind.ZERO:
                yield GapExtent(entry.file_offset, entry.length, ExtentKind.ZERO)
            else:
                assert entry.addr is not None, "a non-ZERO chunk-map entry always carries its address"
                yield DataExtent(entry.file_offset, entry.length, entry.addr, entry.map_num, entry.repeat)
            cursor = max(cursor, entry.end_offset)

        stop = end if end is not None else self.size
        if stop is not None and stop > cursor:
            yield GapExtent(cursor, stop - cursor, ExtentKind.HOLE)

    @override
    async def read(self, offset: int = 0, length: int | None = None) -> bytes | bytearray:
        """Read ``length`` bytes starting at ``offset`` (default: from
        ``offset`` to ``size``). ``ZERO``/``HOLE`` ranges read as zero
        bytes. A request extending past ``size`` is clamped to the bytes
        that exist (see ``clamp_read_length``), never zero-padded.

        Args:
            offset: Start offset in the file.
            length: Byte count; ``None`` reads to ``size`` (required when
                ``size`` is unknown).

        Returns:
            The bytes read, possibly fewer than ``length``.

        Raises:
            ValueError: ``offset`` or ``length`` is negative, or ``length``
                is ``None`` while ``size`` is unknown.
            ResourceLimitExceededError: the resolved read size exceeds
                ``MAX_SINGLE_READ_SIZE`` — use ``stream()`` instead for a
                legitimately large read.
            NotFoundError: A composition sub-file or bucket is missing.
            ChunkCompactedError: A chunk in range was reclaimed by compaction.
            DataCorruptError: The record or a chunk fails to decode or verify.
        """
        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        if length is not None and length < 0:
            raise ValueError(f"length must be non-negative, got {length}")
        if length is None and self.size is None:
            raise ValueError("length must be given when this DedupFile's size is unknown")
        if self.size is not None:
            length = clamp_read_length(offset, length, self.size)
        assert length is not None  # size is known whenever length was None (checked above)
        if length <= 0:
            return b""
        if length > MAX_SINGLE_READ_SIZE:
            raise ResourceLimitExceededError(
                f"read() resolved to {length} bytes, exceeding the {MAX_SINGLE_READ_SIZE}-byte "
                "single-read safety ceiling — use stream() instead for a legitimately large read"
            )
        end = offset + length
        out = bytearray(length)
        view = memoryview(out)
        groups: list[_BucketGroup] = []
        planned = 0
        async for extent in self._extents(offset, end):
            seg_start = max(extent.offset, offset)
            seg_end = min(extent.end, end)
            if seg_end <= seg_start:  # pragma: no cover - defensive: _extents() invariants rule this out
                continue
            if extent.kind is ExtentKind.DATA:
                for group in _bucket_groups(extent, seg_start, seg_end):
                    groups.append(group)
                    planned += group.chunks
                if planned >= _READ_BATCH_CHUNKS:
                    await _fill_groups(self._pool, groups, view, offset)
                    groups, planned = [], 0
        await _fill_groups(self._pool, groups, view, offset)
        # Handed over, not copied: nothing else holds ``out``.
        return out

    @override
    def _window(self) -> tuple[DedupFile, int]:
        return self, 0

    def view(self, offset: int, length: int) -> ByteRangeView:
        """A ``ByteRangeView`` of ``length`` bytes at ``offset``."""
        return ByteRangeView(self, offset, length)


class ByteRangeView(DedupContent):
    """An offset+length window into a ``DedupFile``, with the same read
    contract (``size``/``read``/``stream``/``export_range``) in window-relative
    coordinates, for a workload whose "file" has no composition record of its own.
    """

    size: int

    def __init__(self, base: DedupFile, offset: int, length: int) -> None:
        self._base = base
        self._offset = offset
        self.size = length

    @property
    def base(self) -> DedupFile:
        """The ``DedupFile`` this view windows into."""
        return self._base

    @property
    def offset(self) -> int:
        """This view's start offset within ``base``."""
        return self._offset

    @override
    def _window(self) -> tuple[DedupFile, int]:
        return self._base, self._offset

    @override
    async def read(self, offset: int = 0, length: int | None = None) -> bytes | bytearray:
        """See ``units.base.ContentSource.read``: a request past this view's
        ``size`` is clamped, never an error."""
        validate_read_args(offset, length)
        length = clamp_read_length(offset, length, self.size)
        return await self._base.read(self._offset + offset, length)
