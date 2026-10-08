"""Unit tests for ``synology_apm_repo.sdk.units.content.pcps_disk``, using
synthetic composition and Pool data written to real files.

Each fragment is its own composition over a shared ``Pool``, and its
``DedupFile`` is opened with the whole disk's size, as a real fragment's
``file_meta.file_size`` is. ``two_fragment_disk`` (``_DISK_SIZE``):

- fragment A covers ``[0, 8192)``: a 4096-byte chunk of ``0xAA``, then ``0xAB``.
- fragment B covers ``[16384, 24576)``: a chunk of ``0xBB``, then ``0xBC``.
- ``[8192, 16384)`` and ``[24576, 32768)`` are gaps no fragment covers (unlike
  a composition's own ``ZERO``/``HOLE`` extents, which lie inside a fragment).
"""

from __future__ import annotations

import dataclasses
import random
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import pytest

from support.format_builders import (
    mapping_record,
    zero_record,
)
from support.repo_builders import write_bucket, write_composition_entries
from synology_apm_repo.sdk.dedup import chunk_walk as chunk_walk_mod
from synology_apm_repo.sdk.dedup import dedup_file as dedup_file_mod
from synology_apm_repo.sdk.dedup import export_scheduler as export_scheduler_mod
from synology_apm_repo.sdk.dedup import local_file_sink as local_file_sink_mod
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning
from synology_apm_repo.sdk.dedup.export_sink import run_sink_export
from synology_apm_repo.sdk.dedup.extent import ExportResult
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.export import LocalFileSink, run_export
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.content import pcps_disk as pcps_disk_mod
from synology_apm_repo.sdk.units.content.pcps_disk import (
    DiskFragment,
    VirtualDiskContentSource,
    _winning_segments,
)
from unit.sdk.dedup_export_fakes import SegmentCollector


async def _run_local_file_export(
    dst: Path, logical_size: int, *, sparse: bool, body: Callable[[LocalFileSink], Awaitable[ExportResult]]
) -> ExportResult:
    """``run_sink_export`` into an unstaged ``LocalFileSink`` at ``dst``; ``body`` receives the open sink."""
    sink = LocalFileSink(dst, staged=False)
    return await run_sink_export(sink, logical_size, sparse=sparse, body=lambda: body(sink))


_HEAD_OFF = 64
_DISK_SIZE = 8 * 4096  # 32768 -- 8 chunks' worth
_FRAG_A_START, _FRAG_A_END = 0, 8192
_FRAG_B_START, _FRAG_B_END = 16384, 24576
_PLAINTEXT_A = [bytes([0xAA]) * 4096, bytes([0xAB]) * 4096]
_PLAINTEXT_B = [bytes([0xBB]) * 4096, bytes([0xBC]) * 4096]


@pytest.fixture
def two_fragment_disk(tmp_path: Path) -> VirtualDiskContentSource:
    write_composition_entries(
        tmp_path / "Composition", mapping_record(_FRAG_A_START, 0, 0, map_num=2), stream_id=10, session_id=1
    )
    write_composition_entries(
        tmp_path / "Composition", mapping_record(_FRAG_B_START, 1, 0, map_num=2), stream_id=11, session_id=1
    )
    write_bucket(tmp_path / "Pool" / "0" / "0.buk", _PLAINTEXT_A)
    write_bucket(tmp_path / "Pool" / "0" / "1.buk", _PLAINTEXT_B)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    pool = Pool(store, "Pool", dir_cache)
    comp_reader_a = CompositionReader(store, dir_cache, "Composition", StreamId(10), SessionId(1))
    comp_reader_b = CompositionReader(store, dir_cache, "Composition", StreamId(11), SessionId(1))
    file_a = DedupFile(comp_reader_a, pool, _HEAD_OFF, size=_DISK_SIZE)
    file_b = DedupFile(comp_reader_b, pool, _HEAD_OFF, size=_DISK_SIZE)
    frag_a = DiskFragment(
        fid=1, start=_FRAG_A_START, end=_FRAG_A_END, dedup_file=file_a, src_file_path="D(x)O(0)S(0).img"
    )
    frag_b = DiskFragment(
        fid=2, start=_FRAG_B_START, end=_FRAG_B_END, dedup_file=file_b, src_file_path="D(x)O(16384)S(0).img"
    )
    # Deliberately passed out of order -- constructor must sort by start.
    return VirtualDiskContentSource(size=_DISK_SIZE, fragments=[frag_b, frag_a])


# Overlap fixture: fragment "early" declares [0, 12288) all ZERO (its capture
# stopped at an unaligned boundary and padded the last chunk); fragment "late"
# starts at 4096 with real MAPPING data across the overlap, and must win.
_OVERLAP_DISK_SIZE = 12288
_OVERLAP_ZERO_END = 12288
_OVERLAP_LATE_START = 4096
_OVERLAP_PLAINTEXT = [bytes([0xDD]) * 4096, bytes([0xDE]) * 4096]  # "late" fragment's real 2-chunk data


@pytest.fixture
def overlapping_fragment_disk(tmp_path: Path) -> VirtualDiskContentSource:
    write_composition_entries(
        tmp_path / "Composition", zero_record(0, zero_num=_OVERLAP_ZERO_END // 4096), stream_id=30, session_id=1
    )
    write_composition_entries(
        tmp_path / "Composition", mapping_record(_OVERLAP_LATE_START, 0, 0, map_num=2), stream_id=31, session_id=1
    )
    write_bucket(tmp_path / "Pool" / "0" / "0.buk", _OVERLAP_PLAINTEXT)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    pool = Pool(store, "Pool", dir_cache)
    comp_reader_early = CompositionReader(store, dir_cache, "Composition", StreamId(30), SessionId(1))
    comp_reader_late = CompositionReader(store, dir_cache, "Composition", StreamId(31), SessionId(1))
    file_early = DedupFile(comp_reader_early, pool, _HEAD_OFF, size=_OVERLAP_DISK_SIZE)
    file_late = DedupFile(comp_reader_late, pool, _HEAD_OFF, size=_OVERLAP_DISK_SIZE)
    frag_early = DiskFragment(
        fid=1, start=0, end=_OVERLAP_ZERO_END, dedup_file=file_early, src_file_path="D(x)O(0)S(0).img"
    )
    frag_late = DiskFragment(
        fid=2,
        start=_OVERLAP_LATE_START,
        end=_OVERLAP_LATE_START + 8192,
        dedup_file=file_late,
        src_file_path="D(x)O(4096)S(0).img",
    )
    return VirtualDiskContentSource(size=_OVERLAP_DISK_SIZE, fragments=[frag_early, frag_late])


_LATE_ZERO_DISK_SIZE = 12288
_LATE_ZERO_PLAINTEXT = [bytes([0xE1]) * 4096, bytes([0xE2]) * 4096, bytes([0xE3]) * 4096]


@pytest.fixture
def late_zero_over_early_data_disk(tmp_path: Path) -> VirtualDiskContentSource:
    """The reverse of ``overlapping_fragment_disk``: the earlier fragment has
    real data across [0, 12288) and the later one declares [4096, 12288) ZERO."""
    write_composition_entries(tmp_path / "Composition", mapping_record(0, 0, 0, map_num=3), stream_id=40, session_id=1)
    write_composition_entries(tmp_path / "Composition", zero_record(4096, zero_num=2), stream_id=41, session_id=1)
    write_bucket(tmp_path / "Pool" / "0" / "0.buk", _LATE_ZERO_PLAINTEXT)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    pool = Pool(store, "Pool", dir_cache)
    early = DedupFile(
        CompositionReader(store, dir_cache, "Composition", StreamId(40), SessionId(1)),
        pool,
        _HEAD_OFF,
        size=_LATE_ZERO_DISK_SIZE,
    )
    late = DedupFile(
        CompositionReader(store, dir_cache, "Composition", StreamId(41), SessionId(1)),
        pool,
        _HEAD_OFF,
        size=_LATE_ZERO_DISK_SIZE,
    )
    return VirtualDiskContentSource(
        size=_LATE_ZERO_DISK_SIZE,
        fragments=[
            DiskFragment(fid=1, start=0, end=12288, dedup_file=early, src_file_path="D(x)O(0)S(0).img"),
            DiskFragment(fid=2, start=4096, end=12288, dedup_file=late, src_file_path="D(x)O(4096)S(0).img"),
        ],
    )


@dataclasses.dataclass(frozen=True)
class _Range:
    start: int
    end: int


class TestOverlappingFragments:
    """Real PC/PS fragments can overlap; the later-starting fragment's data
    wins over an earlier fragment's boundary-padding ``ZERO`` in the shared
    range."""

    async def test_the_non_overlapping_prefix_reads_the_early_fragment(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await overlapping_fragment_disk.read(0, 4096) == bytes(4096)  # only "early" covers this, real zero

    async def test_the_overlap_reads_the_later_fragments_real_data_not_the_earlier_zero(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        result = await overlapping_fragment_disk.read(4096, 4096)
        assert result == _OVERLAP_PLAINTEXT[0]
        assert result != bytes(4096)

    async def test_the_late_only_tail_reads_the_late_fragment(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await overlapping_fragment_disk.read(8192, 4096) == _OVERLAP_PLAINTEXT[1]

    async def test_a_request_fully_inside_the_overlap_also_prefers_the_later_fragment(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        result = await overlapping_fragment_disk.read(5000, 1000)
        assert result == _OVERLAP_PLAINTEXT[0][5000 - 4096 : 6000 - 4096]

    async def test_whole_disk_read_matches_the_precedence_rule_throughout(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        expected = bytes(4096) + _OVERLAP_PLAINTEXT[0] + _OVERLAP_PLAINTEXT[1]
        assert await overlapping_fragment_disk.read(0, _OVERLAP_DISK_SIZE) == expected

    async def test_export_to_also_lands_the_later_fragments_data_in_the_overlap(
        self, overlapping_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        dst = tmp_path / "disk.img"
        result = await run_export(overlapping_fragment_disk, LocalFileSink(dst, staged=False), sparse=False)
        assert dst.read_bytes() == await overlapping_fragment_disk.read(0, _OVERLAP_DISK_SIZE)
        # frag_early's ZERO extent is counted for [0, 4096), which frag_late
        # (starting at 4096) never overrides.
        assert result.zeros >= 4096

    @pytest.mark.parametrize("sparse", [True, False])
    async def test_export_writes_a_later_zero_declaration_over_an_earlier_fragments_data(
        self,
        late_zero_over_early_data_disk: VirtualDiskContentSource,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sparse: bool,
    ) -> None:
        """The later fragment's ZERO wins over earlier data even when the file is preallocated."""
        monkeypatch.setattr(local_file_sink_mod, "preallocate", lambda fd, size: True)
        dst = tmp_path / "disk.img"
        await run_export(late_zero_over_early_data_disk, LocalFileSink(dst, staged=False), sparse=sparse)
        expected = _LATE_ZERO_PLAINTEXT[0] + bytes(8192)
        assert dst.read_bytes() == expected
        assert await late_zero_over_early_data_disk.read(0, _LATE_ZERO_DISK_SIZE) == expected

    async def test_an_overlap_is_neither_exported_nor_counted_twice(
        self, overlapping_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        result = await run_export(
            overlapping_fragment_disk, LocalFileSink(tmp_path / "disk.img", staged=False), sparse=False
        )
        # early fragment: winning [0, 4096) of zeros; late fragment: 2 chunks of data.
        assert result.zeros == 4096
        assert result.bytes_written == 8192


class TestWinningSegments:
    @staticmethod
    def _frags(*ranges: tuple[int, int]) -> list[DiskFragment]:
        return [cast(DiskFragment, _Range(start, end)) for start, end in ranges]

    def test_disjoint_fragments_keep_their_whole_ranges(self) -> None:
        frags = self._frags((0, 100), (200, 300))
        assert [(s, e) for _, s, e in _winning_segments(frags)] == [(0, 100), (200, 300)]

    def test_the_higher_start_wins_the_shared_boundary(self) -> None:
        frags = self._frags((0, 120), (100, 300))
        segments = _winning_segments(frags)
        assert [(s, e) for _, s, e in segments] == [(0, 100), (100, 300)]
        assert [f for f, _, _ in segments] == frags

    def test_a_fragment_inside_another_splits_it_into_two_segments(self) -> None:
        frags = self._frags((0, 300), (100, 200))
        segments = _winning_segments(frags)
        assert [(s, e) for _, s, e in segments] == [(0, 100), (100, 200), (200, 300)]
        assert [f for f, _, _ in segments] == [frags[0], frags[1], frags[0]]

    def test_empty_fragments_own_nothing(self) -> None:
        frags = self._frags((0, 100), (50, 50), (100, 100))
        assert [(s, e) for _, s, e in _winning_segments(frags)] == [(0, 100)]

    def test_matches_a_per_byte_oracle_on_random_layouts(self) -> None:
        rng = random.Random(1234)
        for _ in range(200):
            ranges = sorted((s, s + rng.randint(0, 20)) for s in (rng.randint(0, 40) for _ in range(rng.randint(0, 8))))
            frags = self._frags(*ranges)
            expected = {}
            for index, (start, end) in enumerate(ranges):
                for byte in range(start, end):
                    expected[byte] = index  # a higher start (a later one on a tie) overwrites
            got = {}
            previous_end = -1
            for frag, start, end in _winning_segments(frags):
                assert start < end and start >= previous_end  # ascending and disjoint
                previous_end = end
                for byte in range(start, end):
                    got[byte] = next(i for i, f in enumerate(frags) if f is frag)  # equal ranges are equal objects
            assert got == expected

    def test_a_fully_covered_fragment_disappears(self) -> None:
        frags = self._frags((100, 200), (100, 300))
        assert [(s, e) for _, s, e in _winning_segments(frags)] == [(100, 300)]


class TestConstruction:
    def test_fragments_are_sorted_by_start_regardless_of_input_order(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert [f.start for f in two_fragment_disk.fragments] == [_FRAG_A_START, _FRAG_B_START]

    def test_size_is_the_whole_disk_capacity(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        assert two_fragment_disk.size == _DISK_SIZE


class TestRead:
    async def test_reads_entirely_within_fragment_a(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        assert await two_fragment_disk.read(0, 4096) == _PLAINTEXT_A[0]
        assert await two_fragment_disk.read(4096, 4096) == _PLAINTEXT_A[1]

    async def test_reads_entirely_within_fragment_b(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        assert await two_fragment_disk.read(16384, 4096) == _PLAINTEXT_B[0]
        assert await two_fragment_disk.read(20480, 4096) == _PLAINTEXT_B[1]

    async def test_reads_entirely_within_a_gap_are_all_zero(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        assert await two_fragment_disk.read(8192, 8192) == bytes(8192)
        assert await two_fragment_disk.read(24576, 8192) == bytes(8192)

    async def test_read_spanning_fragment_a_into_the_gap(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        # [4096, 12288): second half of fragment A's data, then 4096 bytes of gap.
        result = await two_fragment_disk.read(4096, 8192)
        assert result == _PLAINTEXT_A[1] + bytes(4096)

    async def test_read_spanning_the_gap_into_fragment_b(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        # [12288, 20480): 4096 bytes of gap, then fragment B's first chunk.
        result = await two_fragment_disk.read(12288, 8192)
        assert result == bytes(4096) + _PLAINTEXT_B[0]

    async def test_whole_disk_read_matches_concatenation_of_every_region(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        expected = _PLAINTEXT_A[0] + _PLAINTEXT_A[1] + bytes(8192) + _PLAINTEXT_B[0] + _PLAINTEXT_B[1] + bytes(8192)
        assert len(expected) == _DISK_SIZE
        assert await two_fragment_disk.read(0, _DISK_SIZE) == expected

    async def test_default_length_reads_to_the_end_of_the_disk(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        result = await two_fragment_disk.read(20480)
        assert result == _PLAINTEXT_B[1] + bytes(8192)

    async def test_over_length_read_clamps_instead_of_raising(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await two_fragment_disk.read(0, _DISK_SIZE + 1) == await two_fragment_disk.read(0, _DISK_SIZE)

    async def test_read_starting_at_or_past_disk_end_returns_empty(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await two_fragment_disk.read(_DISK_SIZE, 10) == b""
        assert await two_fragment_disk.read(_DISK_SIZE + 100, 10) == b""

    async def test_negative_offset_raises(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            await two_fragment_disk.read(-1, 10)

    async def test_negative_length_raises(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            await two_fragment_disk.read(100, -1)

    async def test_zero_length_read_returns_empty_bytes(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        assert await two_fragment_disk.read(4096, 0) == b""


class TestStream:
    async def test_stream_reassembles_the_whole_disk_in_order(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        chunks = [chunk async for _offset, chunk in two_fragment_disk.stream(block=4096)]
        assert b"".join(chunks) == await two_fragment_disk.read(0, _DISK_SIZE)
        offsets = [offset async for offset, _chunk in two_fragment_disk.stream(block=4096)]
        assert offsets == list(range(0, _DISK_SIZE, 4096))


class TestExportTo:
    async def test_export_to_produces_a_byte_correct_whole_disk_image(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        dst = tmp_path / "disk.img"
        result = await run_export(two_fragment_disk, LocalFileSink(dst, staged=False), sparse=True)
        assert dst.stat().st_size == _DISK_SIZE
        assert dst.read_bytes() == await two_fragment_disk.read(0, _DISK_SIZE)
        assert result.logical_size == _DISK_SIZE
        assert result.bytes_written == 4 * 4096  # only the two fragments' own real DATA chunks
        assert result.holes == 4 * 4096  # the two gaps

    async def test_export_to_non_sparse_still_writes_real_zero_bytes_into_the_gaps(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        dst = tmp_path / "disk.img"
        await run_export(two_fragment_disk, LocalFileSink(dst, staged=False), sparse=False)
        content = dst.read_bytes()
        assert content[_FRAG_A_END:_FRAG_B_START] == bytes(_FRAG_B_START - _FRAG_A_END)
        assert content[_FRAG_B_END:_DISK_SIZE] == bytes(_DISK_SIZE - _FRAG_B_END)
        assert content == await two_fragment_disk.read(0, _DISK_SIZE)

    async def test_export_to_reports_progress_against_the_combined_total(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        await run_export(two_fragment_disk, LocalFileSink(tmp_path / "disk.img", staged=False), progress=_progress)

        # One report per fragment, against the combined DATA total across both.
        assert calls == [(2 * 4096, 4 * 4096), (4 * 4096, 4 * 4096)]

    async def test_export_range_reports_each_fragments_written_bytes(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        calls: list[int] = []

        async def _progress(written: int) -> None:
            calls.append(written)

        sink = LocalFileSink(tmp_path / "disk.img", staged=False)
        await run_sink_export(
            sink,
            _DISK_SIZE,
            sparse=True,
            body=lambda: two_fragment_disk.export_range(sink, 0, _DISK_SIZE, progress=_progress),
        )

        assert sum(calls) == 4 * 4096

    async def test_progress_walks_each_segments_extents_once(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        walks: list[str] = []
        real = chunk_walk_mod.count_planned_bytes

        async def _counting(base: DedupFile, start: int, end: int) -> int:
            walks.append("walk")
            return await real(base, start, end)

        monkeypatch.setattr(pcps_disk_mod, "count_planned_bytes", _counting)
        monkeypatch.setattr(dedup_file_mod, "count_planned_bytes", _counting)

        async def _progress(done: int, total: int) -> None:
            pass

        await run_export(two_fragment_disk, LocalFileSink(tmp_path / "disk.img", staged=False), progress=_progress)

        assert len(walks) == 2  # one per winning segment, for the total only: the export itself never walks

    async def test_progress_reaches_its_total_when_a_winning_segment_holds_zero_extents(
        self, late_zero_over_early_data_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        await run_export(
            late_zero_over_early_data_disk, LocalFileSink(tmp_path / "disk.img", staged=False), progress=_progress
        )

        assert calls == [(4096, 4096)]  # ZERO extents count toward neither side

    async def test_max_concurrent_reads_is_forwarded_to_every_fragment(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        received: list[int | None] = []
        real_export_to = export_scheduler_mod.export_to_writer

        async def _spy_export_to(*args: Any, **kwargs: Any) -> object:
            received.append(cast(ExportTuning, kwargs["tuning"]).max_concurrent_reads)
            return await real_export_to(*args, **kwargs)

        monkeypatch.setattr(export_scheduler_mod, "export_to_writer", _spy_export_to)

        await _run_local_file_export(
            tmp_path / "disk.img",
            two_fragment_disk.size,
            sparse=True,
            body=lambda sink: two_fragment_disk.export_range(
                sink, 0, two_fragment_disk.size, tuning=ExportTuning(max_concurrent_reads=8)
            ),
        )

        assert received == [8, 8]  # once per fragment

    async def test_max_concurrent_opens_is_forwarded_to_every_fragment(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        received: list[int | None] = []
        real_export_to = export_scheduler_mod.export_to_writer

        async def _spy_export_to(*args: Any, **kwargs: Any) -> object:
            received.append(cast(ExportTuning, kwargs["tuning"]).max_concurrent_opens)
            return await real_export_to(*args, **kwargs)

        monkeypatch.setattr(export_scheduler_mod, "export_to_writer", _spy_export_to)

        await _run_local_file_export(
            tmp_path / "disk.img",
            two_fragment_disk.size,
            sparse=True,
            body=lambda sink: two_fragment_disk.export_range(
                sink, 0, two_fragment_disk.size, tuning=ExportTuning(max_concurrent_opens=8)
            ),
        )

        assert received == [8, 8]  # once per fragment

    @pytest.mark.parametrize("sparse", [True, False])
    @pytest.mark.parametrize("segment_size", [4096, 8192, 12288])
    async def test_a_segmented_sink_gets_the_same_bytes_and_totals_as_a_local_file(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, sparse: bool, segment_size: int
    ) -> None:
        """Segments cut across the fragments and the gaps between and around them."""
        local = tmp_path / "local.img"
        expected = await run_export(two_fragment_disk, LocalFileSink(local, staged=False), sparse=sparse)

        sink = SegmentCollector(segment_size)
        result = await run_export(two_fragment_disk, sink, sparse=sparse)

        assert bytes(sink.output) == local.read_bytes()
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (
            expected.bytes_written,
            expected.logical_size,
            expected.holes,
            expected.zeros,
        )

    async def test_a_sub_range_spanning_both_fragments_and_the_gap_between_them(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        """Offsets are relative to the range start; the gap inside the range is zero-filled."""
        dst = tmp_path / "range.img"

        result = await _run_local_file_export(
            dst,
            16384,
            sparse=False,
            body=lambda sink: two_fragment_disk.export_range(sink, 4096, 20480, sparse=False),
        )

        assert dst.read_bytes() == await two_fragment_disk.read(4096, 16384)
        assert (result.bytes_written, result.logical_size, result.holes) == (8192, 16384, 8192)

    async def test_a_sub_range_inside_a_gap_is_all_hole(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        result = await _run_local_file_export(
            tmp_path / "gap.img",
            4096,
            sparse=True,
            body=lambda sink: two_fragment_disk.export_range(sink, 10240, 14336),
        )

        assert (result.bytes_written, result.logical_size, result.holes) == (0, 4096, 4096)

    async def test_planned_bytes_counts_only_the_data_inside_the_range(
        self, two_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await two_fragment_disk.planned_bytes(0, _DISK_SIZE) == 4 * 4096
        assert await two_fragment_disk.planned_bytes(4096, 20480) == 2 * 4096
        assert await two_fragment_disk.planned_bytes(10240, 14336) == 0

    async def test_a_range_outside_the_disk_is_rejected(self, two_fragment_disk: VirtualDiskContentSource) -> None:
        with pytest.raises(ValueError, match="is not inside"):
            await two_fragment_disk.planned_bytes(0, _DISK_SIZE + 1)

    async def test_export_to_with_no_gaps_at_all(self, tmp_path: Path) -> None:
        """Fragments tiling [0, size) exactly leave no gaps to zero-fill."""
        size = 8192
        write_composition_entries(
            tmp_path / "Composition", mapping_record(0, 0, 0, map_num=2), stream_id=20, session_id=1
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", _PLAINTEXT_A)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        comp_reader = CompositionReader(store, dir_cache, "Composition", StreamId(20), SessionId(1))
        file_a = DedupFile(comp_reader, pool, _HEAD_OFF, size=size)
        frag = DiskFragment(fid=1, start=0, end=size, dedup_file=file_a, src_file_path="D(x)O(0)S(0).img")
        disk = VirtualDiskContentSource(size=size, fragments=[frag])

        dst = tmp_path / "disk.img"
        result = await run_export(disk, LocalFileSink(dst, staged=False), sparse=False)
        assert dst.read_bytes() == _PLAINTEXT_A[0] + _PLAINTEXT_A[1]
        assert result.holes == 0


class TestDenseExportWithPreallocation:
    """A preallocated destination already reads as zero, so gaps need no
    zero-fill; the dense export's bytes must be the same either way."""

    async def test_dense_export_is_identical_with_and_without_preallocation(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from synology_apm_repo.sdk.dedup import local_file_sink

        results: dict[str, bytes] = {}
        for label, reserved in (("written", False), ("reserved", True)):
            monkeypatch.setattr(local_file_sink, "preallocate", lambda fd, size, answer=reserved: answer)
            dst = tmp_path / f"{label}.img"
            await run_export(two_fragment_disk, LocalFileSink(dst, staged=False), sparse=False)
            results[label] = dst.read_bytes()

        assert results["reserved"] == results["written"]
