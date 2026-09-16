"""``VirtualDiskContentSource`` reassembles a PC/PS physical disk's
independently-registered per-region dedup objects ("fragments") into one
``ContentSource`` that reads/exports/streams like a single disk image,
matching VM's own ``disk_image`` model.

Unlike VM (one disk = one ``object_table`` row = one composition),
PC/PS's disk-analyzer segments one physical disk into several regions
at backup time, each its own independently-registered dedup object
(FORMAT-SPEC.md: pcps-fragments). Two facts this class depends on: a fragment's
chunk-map offsets are disk-absolute, not relative to its own start, so
opening its ``DedupFile`` with the whole disk's size already gives a
complete, correctly-sparse view of the entire disk on its own; and
``db/file_meta.file_size`` is the whole disk's capacity (identical across
every sibling fragment), never one fragment's own real length — that has
to be asked of its own composition directly
(``CompositionRecord.extent``).

This class's only real job is routing a read to whichever fragment
covers it, and driving a whole-disk export fragment-by-fragment through
``export_scheduler.export_to``'s ``dst_offset`` mechanism — never through
``VirtualDiskContentSource.read``, which stays a browsing/preview path
only.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

from ...dedup.chunk_walk import build_export_executor
from ...dedup.dedup_file import (
    DEFAULT_STREAM_BLOCK,
    DedupFile,
    ExportResult,
    clamp_read_length,
    stream_via_read,
)
from ...dedup.pool import BucketReaderCache
from ...dedup.pool_descriptor import PoolDescriptor

_ZERO_FILL_BLOCK = 1 << 20


def _create_truncated(dst: Path, size: int) -> None:
    """Create/replace ``dst`` at exactly ``size`` bytes — same one-line
    shape as ``export_scheduler.py``'s own private helper of the same
    name, duplicated rather than imported across module boundaries (that
    module's ``__all__`` is deliberately just ``export_to``)."""
    with Path(dst).open("wb") as f:
        f.truncate(size)


_HAS_PWRITE = hasattr(os, "pwrite")


def _pwrite(fd: int, data: bytes, offset: int) -> None:
    """Same shape as ``export_scheduler.py``'s own private helper —
    ``os.pwrite()`` where available (POSIX); Windows has no positional
    write, so this falls back to ``lseek``+``write``, safe here because
    every call is awaited sequentially against this one fd (see the
    caller below)."""
    if _HAS_PWRITE:
        os.pwrite(fd, data, offset)
    else:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


def _write_zeros_at(fd: int, offset: int, length: int) -> None:
    """Same shape as ``export_scheduler.py``'s own private helper — used
    here only for the gaps *between*/around fragments (never covered by
    any fragment's own composition at all), which no per-fragment
    ``export_to`` call ever writes to."""
    block = bytes(_ZERO_FILL_BLOCK)
    remaining = length
    pos = offset
    while remaining > 0:
        take = min(remaining, len(block))
        _pwrite(fd, block[:take], pos)
        pos += take
        remaining -= take


def _gaps(fragments: list[DiskFragment], size: int) -> list[tuple[int, int]]:
    """``[start, end)`` ranges of the disk not covered by any fragment's
    own extent — a leading gap, any gap between fragments, and a
    trailing gap. ``fragments`` must already be sorted by ``start`` (as
    ``VirtualDiskContentSource`` always keeps its own list). Correct
    even when fragments overlap (real ones do — see that class's own
    docstring)."""
    gaps = []
    pos = 0
    for frag in fragments:
        if frag.start > pos:
            gaps.append((pos, frag.start))
        # max(), not frag.end directly: an overlapping fragment's start
        # can fall inside territory an earlier fragment already covered,
        # and a gap must never be re-opened there once it's covered.
        pos = max(pos, frag.end)
    if pos < size:
        gaps.append((pos, size))
    return gaps


@dataclasses.dataclass(frozen=True)
class DiskFragment:
    """One PC/PS per-region object, already resolved down to a
    ready-to-read ``DedupFile`` and the disk-absolute ``[start, end)``
    range its own composition actually covers
    (``CompositionRecord.extent`` — never ``file_meta.file_size``, see
    this module's own docstring)."""

    fid: int
    start: int
    end: int
    dedup_file: DedupFile
    src_file_path: str
    """Kept for diagnostics/``attrs`` display only — never consulted for
    addressing (that's what ``start``/``end`` are for)."""


def _make_progress(
    progress: Callable[[int, int], Awaitable[None]], base: int, total: int
) -> Callable[[int, int], Awaitable[None]]:
    """A fragment-scoped ``progress`` callback that reports against the
    combined whole-disk total, offset by every earlier fragment's own
    already-completed share (``base``). A plain closure over a loop
    variable would suffer the usual late-binding bug across fragments —
    this factory binds ``base``/``total`` at construction time instead."""

    async def _inner(done: int, _fragment_total: int) -> None:
        await progress(base + done, total)

    return _inner


class VirtualDiskContentSource:
    """Reassembles a PC/PS physical disk's independently-addressed
    fragments into one ``ContentSource`` spanning the whole disk
    (``[0, size)``), with the same ``size``/``read``/``stream``/
    ``export_to`` shape as a VM's single-composition ``disk_image`` —
    so nothing above this layer needs a PC/PS-specific branch.

    Fragments genuinely overlap in real data — a fragment's own
    composition covers whatever whole 4096-byte chunks its capture
    touched, and the chunk straddling two regions' boundary can carry
    real data in the later region while the earlier region pads the
    same range with a ``ZERO`` declaration (its capture stopped at the
    true, unaligned boundary). The rule this class applies: when more
    than one fragment covers a byte, the fragment with the higher
    ``start`` wins — a later region's real capture takes precedence
    over an earlier region's boundary-padding zero declaration.
    ``read``/``export_to`` both get this "for free" by writing/exporting
    fragments in ascending-``start`` order, so a later fragment's write
    naturally overwrites an earlier one's in any shared range.
    """

    def __init__(self, size: int, fragments: list[DiskFragment]) -> None:
        self.size = size
        self.fragments = sorted(fragments, key=lambda f: f.start)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """See ``units.base.ContentSource.read``'s EOF contract — a
        request extending past this disk's own ``size`` is clamped, never
        an error."""
        if offset < 0 or (length is not None and length < 0):
            raise ValueError(f"read(offset={offset}, length={length}): offset/length must be non-negative")
        length = clamp_read_length(offset, length, self.size)
        end = offset + length
        if end == offset:
            return b""
        touching = [f for f in self.fragments if f.start < end and f.end > offset]
        if not touching:
            # Fast path: nothing registered here at all -- a pure hole.
            return bytes(end - offset)
        if len(touching) == 1 and touching[0].start <= offset and end <= touching[0].end:
            # Fast path: exactly one fragment overlaps this request at
            # all, and it fully covers it -- delegate straight through,
            # no bytearray/copy of our own. Only safe when nothing else
            # touches this range: fragments can genuinely overlap in real
            # data (see this class's own docstring), so a fragment fully
            # covering the request is *not* enough on its own if a
            # second, higher-precedence fragment also overlaps some
            # sub-range of it -- that case must go through the assembly
            # path below to apply the real precedence rule correctly.
            return await touching[0].dedup_file.read(offset, end - offset)
        # No memoryview-based zero-copy path here despite the temptation:
        # DedupFile.read() already returns plain bytes, so the one copy
        # that matters (Pool decompressing a chunk into a buffer) already
        # happened upstream -- slicing into this bytearray is only a
        # second, unavoidable copy, and only for a read that straddles
        # more than one fragment (the single-fragment fast path above
        # skips it entirely otherwise).
        out = bytearray(end - offset)
        # ``touching`` inherits self.fragments' own ascending-start order --
        # deliberately not re-sorted, so a later fragment's write below
        # naturally overwrites an earlier one's in whatever range they
        # share ("higher start wins" -- see this class's own docstring).
        for frag in touching:
            seg_start, seg_end = max(frag.start, offset), min(frag.end, end)
            chunk = await frag.dedup_file.read(seg_start, seg_end - seg_start)
            out[seg_start - offset : seg_end - offset] = chunk
        return bytes(out)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``True`` — ``export_to()`` accepts and forwards
        ``max_concurrent_reads``/``max_concurrent_opens`` to each of its
        own fragments (see that method's own docstring)."""
        return True

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
        max_concurrent_opens: int | None = None,
        max_concurrent_reads: int = 1,
    ) -> ExportResult:
        """Export the whole disk by walking fragments in order and handing
        each to ``export_scheduler.export_to``'s own bucket-major writer,
        landing at its disk-absolute ``dst_offset`` inside one pre-sized
        ``dst`` file — never through ``read``. Fragments export
        sequentially, one at a time; ``max_concurrent_opens``/
        ``max_concurrent_reads`` only add concurrency *within* each
        fragment's own bucket-major walk (the same two knobs a VM's
        ``disk_image`` exposes — see ``chunk_walk.py``). Gaps
        between/around fragments (``_gaps``) are real holes: left to
        ``ftruncate()`` when ``sparse=True``, explicitly zero-filled
        otherwise. Every fragment shares one ``BucketReaderCache`` for
        this call, so bucket-locality reuse persists across them.

        ``ExportResult.holes``/``zeros``/``bytes_written`` are summed
        across fragments independently, so a byte range more than one
        fragment declares (adjacent regions' ``ZERO`` tails agreeing) is
        counted more than once — the written file is still byte-correct,
        only the reported totals can run slightly ahead of the disk's
        true size.

        Shares **one** multiprocess executor across every fragment's own
        ``export_to()`` call instead of letting each spin one up
        independently — the same "one executor per logical export, not
        per inner call" contract ``Repository.verify()``'s own
        multi-catalog fan-out follows, for the same reason (a fragment's
        own pool-spawn cost would otherwise be paid once per fragment).
        Built only when every fragment actually resolves to the identical
        ``PoolDescriptor`` (same store/``pool_root``/vault key) — true for
        every real disk today (every fragment belongs to the same
        repository/pool), checked rather than assumed.
        """
        await asyncio.to_thread(_create_truncated, dst, self.size)
        gaps = _gaps(self.fragments, self.size)
        gap_total = sum(end - start for start, end in gaps)

        export_cache = BucketReaderCache()
        planned_total = sum(f.end - f.start for f in self.fragments) if progress else 0
        bytes_written = holes = zeros = 0
        done_before = 0

        descriptors = [PoolDescriptor.from_pool(frag.dedup_file.pool) for frag in self.fragments]
        first = descriptors[0] if descriptors else None
        share_one_executor = first is not None and all(d == first for d in descriptors)
        executor = build_export_executor(first, str(dst)) if share_one_executor and first is not None else None
        try:
            # Fragments are walked sequentially, one fully finishing before
            # the next starts -- parallelizing across fragments would need
            # to preserve the "higher start wins" overlap precedence (class
            # docstring) under concurrent writes, not attempted here (each
            # fragment's own internal bucket-group dispatch is where the
            # real parallelism happens once `executor` is set).
            # max_concurrent_opens matters more here than for a comparable VM
            # range, since PC/PS's Pool data is typically spread across
            # smaller, more numerous buckets (left None, it auto-derives from
            # max_concurrent_reads the same way export_scheduler.export_to()
            # does for every other caller) -- only relevant on the fallback
            # path anyway, when `executor` is None.
            for frag in self.fragments:
                frag_progress = _make_progress(progress, done_before, planned_total) if progress is not None else None
                view = frag.dedup_file.view(frag.start, frag.end - frag.start)
                result = await view.export_to(
                    dst,
                    sparse=sparse,
                    progress=frag_progress,
                    export_cache=export_cache,
                    dst_offset=frag.start,
                    create=False,
                    max_concurrent_opens=max_concurrent_opens,
                    max_concurrent_reads=max_concurrent_reads,
                    executor=executor,
                )
                bytes_written += result.bytes_written
                holes += result.holes
                zeros += result.zeros
                done_before += frag.end - frag.start
        finally:
            if executor is not None:
                # A plain blocking call -- see export_scheduler.py's own
                # equivalent teardown for why this must go through
                # to_thread() rather than freeze this whole process's
                # event loop for however long a still-running worker takes.
                await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)

        if not sparse and gap_total:
            fd = await asyncio.to_thread(os.open, dst, os.O_WRONLY)
            try:
                for start, end in gaps:
                    await asyncio.to_thread(_write_zeros_at, fd, start, end - start)
            finally:
                await asyncio.to_thread(os.close, fd)
        holes += gap_total

        return ExportResult(bytes_written=bytes_written, logical_size=self.size, holes=holes, zeros=zeros)
