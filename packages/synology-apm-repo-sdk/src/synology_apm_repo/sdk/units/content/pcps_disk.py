"""``VirtualDiskContentSource`` reassembles a PC/PS physical disk's
per-region dedup objects ("fragments", FORMAT-SPEC.md: PC/PS disk fragments) into one
``ContentSource`` that behaves like a single disk image.

Two facts it depends on: a fragment's chunk-map offsets are
disk-absolute, so its ``DedupFile`` opened with the whole disk's size is
already a correctly-sparse whole-disk view; and ``db/file_meta.file_size``
is the whole disk's capacity, not one fragment's length (that comes from
``CompositionRecord.extent``).
"""

from __future__ import annotations

import dataclasses
import heapq
import itertools
from collections.abc import AsyncIterator

from ...dedup import export_scheduler
from ...dedup.chunk_walk import count_planned_bytes
from ...dedup.dedup_file import (
    DEFAULT_STREAM_BLOCK,
    DedupFile,
    clamp_read_length,
    stream_via_read,
    validate_export_range,
    validate_read_args,
)
from ...dedup.export_scheduler import ExportTuning
from ...dedup.export_sink import ExportWriter, WrittenBytesCallback
from ...dedup.extent import ExportResult
from ...dedup.pool_descriptor import PoolDescriptor


@dataclasses.dataclass(frozen=True, slots=True)
class DiskFragment:
    """One PC/PS per-region object, resolved to a ready-to-read
    ``DedupFile`` and the disk-absolute ``[start, end)`` its composition
    covers (``CompositionRecord.extent``)."""

    fid: int
    start: int
    end: int
    dedup_file: DedupFile
    src_file_path: str
    """For diagnostics only, never for addressing."""


def _winning_segments(fragments: list[DiskFragment]) -> list[tuple[DiskFragment, int, int]]:
    """Splits the disk into disjoint ``(fragment, start, end)`` segments,
    ascending, each owned by the fragment that wins there: where fragments
    overlap, the one with the higher ``start`` (a later one wins a tie).
    Empty fragments own nothing. ``fragments`` must be sorted by ``start``."""
    bounds = sorted({point for f in fragments for point in (f.start, f.end)})
    # Fragments starting at or before the current interval, the winner on top;
    # one that ended (an empty one included) is discarded when it surfaces.
    active: list[tuple[int, int]] = []
    upcoming = 0
    segments: list[tuple[DiskFragment, int, int]] = []
    for start, end in itertools.pairwise(bounds):
        while upcoming < len(fragments) and fragments[upcoming].start <= start:
            heapq.heappush(active, (-fragments[upcoming].start, -upcoming))
            upcoming += 1
        while active and fragments[-active[0][1]].end <= start:
            heapq.heappop(active)
        if not active:
            continue
        winner = fragments[-active[0][1]]
        if segments and segments[-1][0] is winner and segments[-1][2] == start:
            segments[-1] = (winner, segments[-1][1], end)
        else:
            segments.append((winner, start, end))
    return segments


class VirtualDiskContentSource:
    """A PC/PS physical disk's fragments as one ``ContentSource`` spanning
    ``[0, size)``.

    Fragments overlap in real data: a fragment's composition covers whole
    4096-byte chunks, so the chunk straddling a region boundary can hold
    real data in the later region while the earlier one pads it with a
    ``ZERO`` declaration. Where more than one fragment covers a byte, the
    one with the higher ``start`` wins. The disk is split once into
    disjoint winning segments (``_winning_segments``) that ``read`` and
    ``export_range`` both use, so neither depends on write order.
    """

    def __init__(self, size: int, fragments: list[DiskFragment]) -> None:
        self.size = size
        self.fragments = sorted(fragments, key=lambda f: f.start)
        self._segments = _winning_segments(self.fragments)

    async def read(self, offset: int = 0, length: int | None = None) -> bytes | bytearray:
        """``ContentSource.read``; a request past ``size`` is clamped."""
        validate_read_args(offset, length)
        length = clamp_read_length(offset, length, self.size)
        end = offset + length
        if end == offset:
            return b""
        touching = [segment for segment in self._segments if segment[1] < end and segment[2] > offset]
        if not touching:
            # Fast path: nothing registered here at all -- a pure hole.
            return bytes(end - offset)
        frag, seg_start, seg_end = touching[0]
        if len(touching) == 1 and seg_start <= offset and end <= seg_end:
            # Fast path: one segment covers the whole range, so skip assembly.
            return await frag.dedup_file.read(offset, end - offset)
        out = bytearray(end - offset)
        for frag, seg_start, seg_end in touching:
            start, stop = max(seg_start, offset), min(seg_end, end)
            out[start - offset : stop - offset] = await frag.dedup_file.read(start, stop - start)
        return out

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes | bytearray]]:
        return stream_via_read(self, block)

    async def export_range(
        self,
        writer: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: WrittenBytesCallback | None = None,
        tuning: ExportTuning | None = None,
    ) -> ExportResult:
        """Exports the disk's ``[start, end)`` into ``writer`` at offsets
        relative to ``start``, one winning segment at a time, so overlaps
        are neither written nor counted twice. Gaps between fragments are
        holes: left unwritten when ``sparse=True`` and the writer allows
        it, zero-filled otherwise.

        Raises:
            ValueError: The range is not inside the disk.
        """
        validate_export_range(start, end, self.size)
        segments = [
            (frag.dedup_file, max(seg_start, start), min(seg_end, end))
            for frag, seg_start, seg_end in self._segments
            if seg_start < end and seg_end > start
        ]
        return await export_scheduler.export_fragments_to_writer(
            segments, writer, span=(start, end), sparse=sparse, progress=progress, tuning=tuning
        )

    def pool_descriptor(self) -> PoolDescriptor | None:
        """How a worker process rebuilds the fragments' ``Pool``: ``None``
        unless every fragment's file is backed by the same rebuildable one."""
        descriptors = {frag.dedup_file.pool_descriptor() for frag in self.fragments}
        return descriptors.pop() if len(descriptors) == 1 else None

    async def planned_bytes(self, start: int, end: int) -> int:
        validate_export_range(start, end, self.size)
        return sum(
            [
                await count_planned_bytes(frag.dedup_file, max(seg_start, start), min(seg_end, end))
                for frag, seg_start, seg_end in self._segments
                if seg_start < end and seg_end > start
            ]
        )
