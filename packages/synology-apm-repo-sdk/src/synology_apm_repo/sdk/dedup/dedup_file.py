"""``DedupFile``: the sole cross-layer read contract.

Any workload — VM disk image, FS file, SaaS raw object — ultimately
reduces to a ``(stream_id, session_id, comp_offset)`` triple plus a size.
Once you have that, everything above this module (catalog, units, CLI,
TUI) only ever calls ``read()``/``stream()``/``export_to()`` — never
touches a bucket, chunk, or composition record directly. ``ByteRangeView``
is the second shared primitive: FS's ``content_dedup_id`` + ``file_size``
and SaaS's ``object_table.(offset, length)`` are the same concept (a named
sub-range of a bigger dedup file), so they share this one class instead of
each workload reinventing it.

Performance-critical property: every ``read()``/``extents()`` call
resolves its starting chunk-map record via ``CompositionRecord.entries``'s
binary search, never a linear scan — see that method's own docstring.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Protocol, runtime_checkable

from ..format.addressing import ChunkAddress
from ..format.chunkmap import ChunkMapKind
from ..format.const import FIXED_CHUNK_LENGTH
from ..identifiers import BucketId, ChunkIdx, StreamId
from .composition_reader import CompositionReader, CompositionRecord
from .pool import BucketReaderCache, Pool

DEFAULT_STREAM_BLOCK = 8 << 20  # 8 MiB
"""Shared by every real ``stream_via_read`` caller across this
project's own ``ContentSource`` implementers (``DedupFile``,
``ByteRangeView``, and — one layer up, in ``units/`` —
``VirtualDiskContentSource``/``DissectFileContentSource``/
``_LocalFileContentSource``) as their own ``stream()``'s default block
size, so the literal is defined once instead of independently redeclared
per module."""


@runtime_checkable
class _Readable(Protocol):
    """The two members ``stream_via_read`` actually needs — a subset
    of ``ContentSource`` deliberately
    redeclared here rather than imported: this module (``dedup/``) sits
    *below* ``units/`` in this project's own layering (every
    ``units/*.py`` file imports from here, never the reverse), so
    importing ``ContentSource`` from ``units.base`` would create a real
    circular import. Every real
    ``ContentSource`` implementer already satisfies this narrower shape
    structurally."""

    @property
    def size(self) -> int | None: ...

    async def read(self, offset: int = 0, length: int | None = None) -> bytes: ...


async def stream_via_read(source: _Readable, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
    """Yield ``(offset, bytes)`` blocks front-to-back via repeated
    ``source.read(offset, block)`` calls — just a loop, so any
    implementer whose own ``read()`` already handles its internal
    chunking/extents/fragments (``DedupFile``'s own bucket-merged
    reads via ``_fill_data_extent``, ``VirtualDiskContentSource``'s
    fragment stitching, ...) gets a correct ``stream()`` for free, with
    no separate bulk-read path to keep in sync. ``ContentSource`` is
    deliberately a structural ``Protocol``, not an ABC, so implementers
    don't need a common base class (see that Protocol's own docstring)
    — but nothing then forced them to share this identical loop either;
    this closes that gap without adding one. Not used by every
    ``ContentSource`` implementer: ``LazyArtifact`` (assembled
    ``.eml``/``.ics`` content) has its own, different ``stream()`` that
    slices an already-materialized in-memory buffer instead, since its
    own ``size`` is ``None`` until that buffer is built.

    Raises ``ValueError`` if ``source.size`` is ``None`` — every real
    caller here always has a known size by the time streaming starts.
    """
    if source.size is None:
        raise ValueError("stream() requires a known size")
    size = source.size
    offset = 0
    while offset < size:
        n = min(block, size - offset)
        yield offset, await source.read(offset, n)
        offset += n


def clamp_read_length(offset: int, length: int | None, size: int) -> int:
    """Resolve a ``read(offset, length)`` request against a known
    ``size``: fills in the default length (``offset`` to ``size``) and
    clamps a request extending past ``size`` down to what's actually
    there. Every ``ContentSource`` implementer shares this contract (see
    ``units.base.ContentSource.read``'s own docstring): reading past the
    end returns fewer bytes, never an error — only a negative
    ``offset``/``length`` is left for the caller to reject before calling
    this.
    """
    available = size - offset
    if length is None:
        length = available
    return max(0, min(length, available))


class ExtentKind(enum.Enum):
    """A file, as seen through ``DedupFile.extents()``, is a sequence of
    these three kinds. ``ZERO`` (an explicit ``Type::Zero``
    chunk-map record) and ``HOLE`` (no record at all, i.e. a gap between
    records) are deliberately distinct even though both read back as
    zero bytes — a ``HOLE`` is a real sparse gap worth preserving as one
    on export; a ``ZERO`` record is zero data explicitly recorded as
    such, not merely absent.
    """

    DATA = 1
    ZERO = 2
    HOLE = 3


@dataclasses.dataclass(frozen=True)
class Extent:
    """One contiguous span of a ``DedupFile``. ``addr``/``map_num``/
    ``repeat`` are populated only for ``ExtentKind.DATA`` — the
    template starting address and its ``1 + repeat`` repetitions the
    span's chunks are drawn from (``ChunkAddress.advance``'s carry
    semantics)."""

    offset: int
    length: int
    kind: ExtentKind
    addr: ChunkAddress | None = None
    map_num: int = 0
    repeat: int = 0

    @property
    def end(self) -> int:
        return self.offset + self.length


@dataclasses.dataclass(frozen=True)
class ExportResult:
    """The outcome of one ``export_to()`` call.

    Attributes:
        bytes_written: Actual bytes written to the destination.
        logical_size: The exported file's full logical size.
        holes: Byte count skipped via a sparse hole instead of writing zeros.
        zeros: Byte count written as explicit zero bytes (not sparse).
    """

    bytes_written: int
    logical_size: int
    holes: int
    zeros: int


async def _fill_data_extent(
    pool: Pool,
    extent: Extent,
    seg_start: int,
    seg_end: int,
    out: bytearray,
    out_base: int,
) -> None:
    """Fill ``out[seg_start - out_base : seg_end - out_base]`` with the
    plaintext bytes ``extent`` describes for ``[seg_start, seg_end)`` — a
    sub-range of ``extent``'s own span, never assumed to be the whole
    thing (callers intersect with their own requested window first).

    Exactly one distinct chunk needed goes through ``Pool.read_chunk``
    unchanged, staying covered by ``Pool``'s own cross-call cache. More
    than one distinct chunk is grouped by ``(stream_id, bucket_id)`` (a
    single extent can still straddle a bucket boundary) and
    fetched via one ``BucketReader.read_chunks``
    call per bucket instead of one ``Pool.read_chunk`` per chunk —
    deliberately bypassing ``Pool``'s own cache in that case, the same
    reasoning ``Pool.read_chunk``'s ``cache=False`` documents.

    Implements this bucket-grouped fetch independently rather than calling
    into ``chunk_walk.py``'s plan/execute engine: this function's own range
    is already bounded by its caller (never a whole-file sweep the way
    export/verify FULL are), so reusing ``chunk_walk.py``'s windowed
    planning and export-scoped ``BucketReaderCache`` would cost this path
    the single-chunk case's ``Pool.read_chunk`` cache reuse for no benefit.

    Free function (not a method) so both ``DedupFile`` and
    ``ByteRangeView`` can share it.
    """
    assert extent.kind is ExtentKind.DATA
    assert extent.addr is not None and extent.map_num > 0
    first_k = (seg_start - extent.offset) // FIXED_CHUNK_LENGTH
    last_k = (seg_end - 1 - extent.offset) // FIXED_CHUNK_LENGTH

    # Grouped by the *physical* chunk each k resolves to — a
    # repeat/template region can have several k's collapse onto the same
    # (stream_id, bucket_id, chunk_idx).
    fetched: dict[int, bytes | memoryview] = {}
    needed: dict[tuple[StreamId, BucketId, ChunkIdx], list[int]] = {}
    for k in range(first_k, last_k + 1):
        addr_k = extent.addr.advance(k % extent.map_num)
        needed.setdefault((addr_k.stream_id, addr_k.bucket_id, addr_k.chunk_idx), []).append(k)

    if len(needed) == 1:
        (stream_id, bucket_id, chunk_idx), ks = next(iter(needed.items()))
        addr = ChunkAddress(stream_id, bucket_id, chunk_idx)
        plain: bytes | memoryview = await pool.read_chunk(addr)
        for k in ks:
            fetched[k] = plain
    else:
        by_bucket: dict[tuple[StreamId, BucketId], list[tuple[ChunkIdx, list[int]]]] = {}
        for (stream_id, bucket_id, chunk_idx), ks in needed.items():
            by_bucket.setdefault((stream_id, bucket_id), []).append((chunk_idx, ks))
        for (stream_id, bucket_id), items in by_bucket.items():
            reader = await pool.bucket(stream_id, bucket_id)
            requests = sorted((chunk_idx, ChunkAddress(stream_id, bucket_id, chunk_idx)) for chunk_idx, _ks in items)
            decoded = await reader.read_chunks(requests)
            # read_chunks() bypasses Pool.read_chunk() entirely -- its own
            # fingerprint-verification policy doesn't apply here unless
            # applied explicitly (see Pool.verify_fingerprints's own
            # docstring for why this call site needs it).
            await pool.verify_fingerprints(stream_id, bucket_id, decoded)
            for chunk_idx, ks in items:
                plain = decoded[chunk_idx]
                for k in ks:
                    fetched[k] = plain

    for k in range(first_k, last_k + 1):
        chunk = fetched[k]
        chunk_start = extent.offset + k * FIXED_CHUNK_LENGTH
        lo = max(seg_start, chunk_start) - chunk_start
        hi = min(seg_end, chunk_start + FIXED_CHUNK_LENGTH) - chunk_start
        dest = max(seg_start, chunk_start) - out_base
        out[dest : dest + (hi - lo)] = chunk[lo:hi]


async def _run_export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool,
    progress: Callable[[int, int], Awaitable[None]] | None,
    window_entries: int | None,
    max_concurrent_opens: int | None,
    max_concurrent_reads: int,
    export_cache: BucketReaderCache | None,
    dst_offset: int,
    create: bool,
    executor: ProcessPoolExecutor | None,
) -> ExportResult:
    """Shared body of ``DedupFile.export_to()``/``ByteRangeView.export_to()``
    — both are already the same call into ``export_to``
    (which itself branches on ``isinstance(file_like, ByteRangeView)``),
    so only one class's own ``export_to()`` docstring needs to state what
    each keyword does."""
    # Deferred, not module-level: chunk_walk.py/export_scheduler.py both
    # import DedupFile from this module at their own module level, so a
    # module-level import here would be a real circular import -- the
    # same kind of crossing _Readable's own docstring above explains for
    # units.base.ContentSource.
    from .chunk_walk import DEFAULT_WINDOW_ENTRIES
    from .export_scheduler import export_to as _export_to

    return await _export_to(
        file_like,
        dst,
        sparse=sparse,
        progress=progress,
        window_entries=DEFAULT_WINDOW_ENTRIES if window_entries is None else window_entries,
        max_concurrent_opens=max_concurrent_opens,
        max_concurrent_reads=max_concurrent_reads,
        export_cache=export_cache,
        dst_offset=dst_offset,
        create=create,
        executor=executor,
    )


class DedupFile:
    """The sole cross-layer contract: a logical,
    byte-addressable file backed by one composition record.

    ``size`` is supplied by the caller (``file_meta.file_size``,
    ``object_table.file_size``, ...) — this layer never infers it from the
    composition record's own coverage, since a record's last byte is not
    necessarily the file's declared end (trailing ``HOLE``).
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
    def stream_id(self) -> int:
        return self._comp_reader.stream_id

    @property
    def session_id(self) -> int:
        return self._comp_reader.session_id

    @property
    def comp_offset(self) -> int:
        return self._comp_offset

    @property
    def pool(self) -> Pool:
        """The ``Pool`` this file's
        chunks resolve through — needed by ``export_scheduler.py``'s
        bucket-major planning, which schedules against a shared ``Pool``/
        ``BucketReaderCache`` rather than one read at a time."""
        return self._pool

    async def _get_record(self) -> CompositionRecord:
        if self._record is None:
            self._record = await self._comp_reader.record(self._comp_offset)
        return self._record

    async def _extents(self, start: int = 0, end: int | None = None) -> AsyncIterator[Extent]:
        """Yield ``Extent``\\ s covering ``[start, end)`` (default:
        the whole file), converting gaps between chunk-map records into
        explicit ``HOLE`` extents and, if ``size`` extends past the last
        record, a trailing ``HOLE`` up to that point.

        Yielded extents are **not truncated** to ``[start, end)`` at their
        own edges — a boundary entry may start slightly before ``start``
        or run past ``end``; callers needing an exact window (e.g.
        ``read``) intersect it themselves. This mirrors how chunk-map
        records work natively (each is atomic, anchored at its own
        ``file_offset``) and avoids re-deriving a truncated address.

        **Private**: the un-truncated boundary contract above is easy to
        misuse, and structurally impossible to hit if a caller never sees
        a raw ``Extent``'s boundary fields. The only legitimate reason to
        walk records directly is needing ``addr``/``map_num``/``repeat``
        for physical-chunk selection (``chunk_walk.py``); everything else
        wants ``read``/``stream``/``export_to``.
        """
        record = await self._get_record()
        cursor = start
        async for entry in record.entries(start, end):
            if entry.length == 0:
                continue
            if entry.file_offset > cursor:
                yield Extent(offset=cursor, length=entry.file_offset - cursor, kind=ExtentKind.HOLE)
            kind = ExtentKind.ZERO if entry.kind is ChunkMapKind.ZERO else ExtentKind.DATA
            yield Extent(
                offset=entry.file_offset,
                length=entry.length,
                kind=kind,
                addr=entry.addr if kind is ExtentKind.DATA else None,
                map_num=entry.map_num if kind is ExtentKind.DATA else 0,
                repeat=entry.repeat if kind is ExtentKind.DATA else 0,
            )
            cursor = max(cursor, entry.end_offset)

        stop = end if end is not None else self.size
        if stop is not None and stop > cursor:
            yield Extent(offset=cursor, length=stop - cursor, kind=ExtentKind.HOLE)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """Read ``length`` bytes starting at ``offset`` (default: from
        ``offset`` to ``size``). ``ZERO``/``HOLE`` extents read back as
        zero bytes for free (``bytearray``'s own zero-fill). A request
        extending past ``size`` is clamped to the bytes that actually
        exist (see ``clamp_read_length``), never zero-padded past it.

        Raises:
            ValueError: ``offset`` or ``length`` is negative.
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
        end = offset + length
        out = bytearray(length)
        async for extent in self._extents(offset, end):
            seg_start = max(extent.offset, offset)
            seg_end = min(extent.end, end)
            if seg_end <= seg_start:  # pragma: no cover - defensive: _extents() invariants rule this out
                continue
            if extent.kind is ExtentKind.DATA:
                await _fill_data_extent(self._pool, extent, seg_start, seg_end, out, offset)
        return bytes(out)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        """Yield ``(offset, bytes)`` blocks front-to-back — see
        ``stream_via_read``'s own docstring for why this is just a
        ``read(offset, block)`` loop (it inherits ``_fill_data_extent``'s
        own bucket-merged reads for free; no separate bulk-read path to
        keep in sync)."""
        return stream_via_read(self, block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
        window_entries: int | None = None,
        max_concurrent_opens: int | None = None,
        max_concurrent_reads: int = 1,
        export_cache: BucketReaderCache | None = None,
        dst_offset: int = 0,
        create: bool = True,
        executor: ProcessPoolExecutor | None = None,
    ) -> ExportResult:
        """Write this file to ``dst``. ``sparse=True`` (the default) never
        writes ``ZERO``/``HOLE`` bytes at all — the output file reads back
        as zero there via the filesystem's own sparse-hole support, and
        the two kinds are indistinguishable to a reader either way; use
        ``sparse=False`` when the caller needs the on-disk allocation to
        actually match the logical size. See ``export_scheduler.export_to``
        for what ``window_entries``/``max_concurrent_opens``/
        ``max_concurrent_reads``/``export_cache``/``dst_offset``/
        ``create``/``executor`` do.

        No ``verify_map_crc`` option here (or anywhere in the export
        path) — chunk-map CRC validation is ``verify``'s job specifically
        (``synology-apm-repo-cli verify --level full``), not export's; see
        ``units/verify_reachable.py``'s own module docstring."""
        return await _run_export_to(
            self,
            dst,
            sparse=sparse,
            progress=progress,
            window_entries=window_entries,
            max_concurrent_opens=max_concurrent_opens,
            max_concurrent_reads=max_concurrent_reads,
            export_cache=export_cache,
            dst_offset=dst_offset,
            create=create,
            executor=executor,
        )

    def view(self, offset: int, length: int) -> ByteRangeView:
        return ByteRangeView(self, offset, length)

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``True`` — a bucket-backed read has a real bucket concept
        to spread reads across (see
        ``ContentSource.supports_concurrent_export``)."""
        return True


class ByteRangeView:
    """A named sub-range of a ``DedupFile`` — the same read-contract
    shape (``size``/``read``/``stream``/``export_to``), coordinates
    translated. Shared by every workload whose "file" is really just an
    offset+length window into a bigger dedup file (FS's
    ``content_dedup_id``, SaaS's ``object_table`` rows) rather than its
    own composition record.
    """

    def __init__(self, base: DedupFile, offset: int, length: int) -> None:
        self._base = base
        self._offset = offset
        self.size = length

    @property
    def base(self) -> DedupFile:
        """The ``DedupFile`` this view windows into — needed by
        ``export_scheduler.py``'s bucket-major planning, which schedules
        against the base file's own ``DedupFile.pool``."""
        return self._base

    @property
    def offset(self) -> int:
        """This view's start offset within ``base``."""
        return self._offset

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """See ``units.base.ContentSource.read``'s EOF contract — a
        request extending past this view's own ``size`` is clamped, never
        an error."""
        if offset < 0 or (length is not None and length < 0):
            raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")
        length = clamp_read_length(offset, length, self.size)
        return await self._base.read(self._offset + offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
        window_entries: int | None = None,
        max_concurrent_opens: int | None = None,
        max_concurrent_reads: int = 1,
        export_cache: BucketReaderCache | None = None,
        dst_offset: int = 0,
        create: bool = True,
        executor: ProcessPoolExecutor | None = None,
    ) -> ExportResult:
        """Same as ``DedupFile.export_to``, windowed to this view's
        own range."""
        return await _run_export_to(
            self,
            dst,
            sparse=sparse,
            progress=progress,
            window_entries=window_entries,
            max_concurrent_opens=max_concurrent_opens,
            max_concurrent_reads=max_concurrent_reads,
            export_cache=export_cache,
            dst_offset=dst_offset,
            create=create,
            executor=executor,
        )

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``True`` — same bucket-backed shape as the base
        ``DedupFile`` it windows into."""
        return True
