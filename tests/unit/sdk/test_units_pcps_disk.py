"""Unit tests for ``synology_apm_repo.sdk.units.content.pcps_disk`` —
synthetic composition + Pool data written to real files, no sample
repositories required (mirrors ``tests/unit/sdk/test_dedup_export_scheduler.py``'s
own synthetic-fixture conventions, duplicated rather than imported —
this file's own established house style).

Two independent fragments, each its own composition (disk-absolute
addressing — every ``DedupFile`` here is opened with ``size=_DISK_SIZE``,
the *whole disk's* capacity, exactly like a real PC/PS fragment's own
``file_meta.file_size``), sharing one ``Pool``:

- fragment A: covers ``[0, 8192)`` — two 4096-byte chunks of ``0xAA``.
- fragment B: covers ``[16384, 24576)`` — two 4096-byte chunks of ``0xBB``.
- ``[8192, 16384)`` and ``[24576, 32768)`` are real gaps — no fragment's
  own composition covers them at all, unlike a composition's own
  ``ZERO``/``HOLE`` extents (which stay *inside* one composition).
"""

from __future__ import annotations

import os
import sys
import zlib
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard

from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.content import pcps_disk as pcps_disk_mod
from synology_apm_repo.sdk.units.content.pcps_disk import DiskFragment, VirtualDiskContentSource, _gaps

_O_BINARY = getattr(os, "O_BINARY", 0)

_HEAD_OFF = 64
_DISK_SIZE = 8 * 4096  # 32768 -- 8 chunks' worth
_FRAG_A_START, _FRAG_A_END = 0, 8192
_FRAG_B_START, _FRAG_B_END = 16384, 24576
_PLAINTEXT_A = [bytes([0xAA]) * 4096, bytes([0xAB]) * 4096]
_PLAINTEXT_B = [bytes([0xBB]) * 4096, bytes([0xBC]) * 4096]


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int) -> bytes:
    addr_int = ChunkAddress(StreamId(0), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=map_num << 16,
    )


def _zero_record(file_offset: int, zero_num: int) -> bytes:
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.ZERO.value, file_chunk_idx=file_offset >> 12, addr_int=0, tail_u32=zero_num
    )


def _composition_header_bytes() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _record_head_bytes(*, map_num: int, map_crc: int = 0, mode: int = 0x0001) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_composition(comp_root: Path, stream_id: int, session_id: int, entries: bytes) -> None:
    map_num = len(entries) // 20
    map_crc = zlib.crc32(entries) & 0xFFFFFFFF
    record_bytes = _record_head_bytes(map_num=map_num, map_crc=map_crc) + entries
    path = comp_root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_composition_header_bytes() + record_bytes)


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _write_bucket(path: Path, plaintexts: list[bytes]) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads = [compressor.compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(payload)) for payload in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = (MODE_COMPRESS | MODE_CHUNK_CRC).to_bytes(4, "big")
    header[12:16] = len(plaintexts).to_bytes(4, "big")
    header[16:20] = chunk_size_crc.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


@pytest.fixture
def two_fragment_disk(tmp_path: Path) -> VirtualDiskContentSource:
    _write_composition(
        tmp_path / "Composition",
        10,
        1,
        _mapping_record(_FRAG_A_START, 0, 0, map_num=2),
    )
    _write_composition(
        tmp_path / "Composition",
        11,
        1,
        _mapping_record(_FRAG_B_START, 1, 0, map_num=2),
    )
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _PLAINTEXT_A)
    _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _PLAINTEXT_B)
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


# Overlap fixture -- a real, observed condition (see VirtualDiskContentSource's
# own docstring): fragment "early" declares [4096, 12288) all ZERO (its own
# capture stopped at an unaligned true boundary and padded the rest of its
# last chunk); fragment "late" starts at 4096 too but has real, non-zero
# MAPPING data across the whole overlap -- the later fragment's real data
# must win, not the earlier one's zero padding.
_OVERLAP_DISK_SIZE = 12288
_OVERLAP_ZERO_END = 12288  # "early" fragment's own declared ZERO extent: [0, 12288)
_OVERLAP_LATE_START = 4096
_OVERLAP_PLAINTEXT = [bytes([0xDD]) * 4096, bytes([0xDE]) * 4096]  # "late" fragment's real 2-chunk data


@pytest.fixture
def overlapping_fragment_disk(tmp_path: Path) -> VirtualDiskContentSource:
    _write_composition(tmp_path / "Composition", 30, 1, _zero_record(0, zero_num=_OVERLAP_ZERO_END // 4096))
    _write_composition(tmp_path / "Composition", 31, 1, _mapping_record(_OVERLAP_LATE_START, 0, 0, map_num=2))
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _OVERLAP_PLAINTEXT)
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


class TestOverlappingFragments:
    """Real PC/PS fragments can genuinely overlap — the
    later-starting fragment's real data must win over an earlier
    fragment's own boundary-padding ``ZERO`` declaration in whatever
    range they share. See ``VirtualDiskContentSource``'s own docstring
    for the full story and the precedence rule this locks in."""

    async def test_the_non_overlapping_prefix_reads_the_early_fragment(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        assert await overlapping_fragment_disk.read(0, 4096) == bytes(4096)  # only "early" covers this, real zero

    async def test_the_overlap_reads_the_later_fragments_real_data_not_the_earlier_zero(
        self, overlapping_fragment_disk: VirtualDiskContentSource
    ) -> None:
        # [4096, 8192) -- "early" declares ZERO here too, but "late" has
        # real, non-zero data and starts later: "late" must win.
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
        """Both fragments' own extents fully cover [5000, 6000) -- the
        single-fragment fast path (checked from the highest start down)
        must still land on "late", not just the assembly path above."""
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
        """The physical file ends up byte-correct via plain write order
        (later fragments pwrite after, and therefore overwrite, earlier
        ones at the same destination offsets) -- exercised here through
        the real export_to() path, not just read()."""
        dst = tmp_path / "disk.img"
        result = await overlapping_fragment_disk.export_to(dst, sparse=False)
        assert dst.read_bytes() == await overlapping_fragment_disk.read(0, _OVERLAP_DISK_SIZE)
        # frag_early's own declared ZERO extent contributes real
        # accounting for the [0, 4096) slice frag_late never overrides
        # (frag_late only starts at 4096) -- ExportResult.zeros was never
        # asserted on by any test in this file despite a ZERO-record
        # fragment already existing in this fixture.
        assert result.zeros >= 4096


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
        # The combined guard's other arm (negative length) is covered
        # separately below -- only a genuinely negative offset/length
        # still raises; an over-length or past-the-end request clamps.
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


def _fake_fragment(start: int, end: int) -> DiskFragment:
    """A ``DiskFragment`` whose only real fields are ``start``/``end`` --
    _gaps() never touches ``dedup_file``/``src_file_path``."""
    return DiskFragment(fid=0, start=start, end=end, dedup_file=cast(Any, None), src_file_path="")


class TestGaps:
    def test_a_fragment_nested_inside_an_earlier_wider_one_does_not_reopen_a_gap(self) -> None:
        # _gaps()'s own ``pos = max(pos, frag.end)`` guard: frag2 here ends
        # *before* frag1 already did (fully nested), so a plain
        # ``pos = frag.end`` would walk pos backwards and wrongly reopen
        # [30, 100) as a gap even though frag1 already covers it.
        fragments = [_fake_fragment(0, 100), _fake_fragment(10, 30), _fake_fragment(150, 200)]
        assert _gaps(fragments, size=200) == [(100, 150)]

    def test_no_fragments_is_one_whole_gap(self) -> None:
        assert _gaps([], size=100) == [(0, 100)]

    def test_one_fragment_covering_the_whole_disk_has_no_gaps(self) -> None:
        assert _gaps([_fake_fragment(0, 100)], size=100) == []


class TestExportTo:
    async def test_export_to_produces_a_byte_correct_whole_disk_image(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        dst = tmp_path / "disk.img"
        result = await two_fragment_disk.export_to(dst, sparse=True)
        assert dst.stat().st_size == _DISK_SIZE
        assert dst.read_bytes() == await two_fragment_disk.read(0, _DISK_SIZE)
        assert result.logical_size == _DISK_SIZE
        assert result.bytes_written == 4 * 4096  # only the two fragments' own real DATA chunks
        assert result.holes == 4 * 4096  # the two gaps

    async def test_export_to_non_sparse_still_writes_real_zero_bytes_into_the_gaps(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path
    ) -> None:
        dst = tmp_path / "disk.img"
        await two_fragment_disk.export_to(dst, sparse=False)
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

        await two_fragment_disk.export_to(tmp_path / "disk.img", progress=_progress)

        assert calls
        totals = {total for _done, total in calls}
        assert totals == {4 * 4096}  # combined DATA total across both fragments, constant throughout
        dones = [done for done, _total in calls]
        assert dones == sorted(dones)  # monotonically non-decreasing across the fragment boundary
        assert dones[-1] == 4 * 4096

    async def test_max_concurrent_reads_is_forwarded_to_every_fragment(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Added after a real PC/PS disk export was found running well
        under 10 MiB/s with no way to opt into the same read
        parallelism a VM disk_image export already has (see this
        class's own docstring) -- pins down that the knob actually
        reaches each fragment's own ``export_to()`` call, not just that
        the whole-disk export still produces correct bytes."""
        from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView

        received: list[int | None] = []
        real_export_to = ByteRangeView.export_to

        async def _spy_export_to(self: ByteRangeView, *args: object, **kwargs: object) -> object:
            received.append(kwargs.get("max_concurrent_reads"))  # type: ignore[arg-type]
            return await real_export_to(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(ByteRangeView, "export_to", _spy_export_to)

        await two_fragment_disk.export_to(tmp_path / "disk.img", max_concurrent_reads=8)

        assert received == [8, 8]  # once per fragment

    async def test_max_concurrent_opens_is_forwarded_to_every_fragment(
        self, two_fragment_disk: VirtualDiskContentSource, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same forwarding contract as ``max_concurrent_reads`` above,
        for the independent bucket-open prefetch knob (this module's own
        docstring, ``chunk_walk._prefetch_bucket_opens``)."""
        from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView

        received: list[int | None] = []
        real_export_to = ByteRangeView.export_to

        async def _spy_export_to(self: ByteRangeView, *args: object, **kwargs: object) -> object:
            received.append(kwargs.get("max_concurrent_opens"))  # type: ignore[arg-type]
            return await real_export_to(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(ByteRangeView, "export_to", _spy_export_to)

        await two_fragment_disk.export_to(tmp_path / "disk.img", max_concurrent_opens=8)

        assert received == [8, 8]  # once per fragment

    async def test_export_to_with_no_gaps_at_all(self, tmp_path: Path) -> None:
        """A disk whose fragments already tile [0, size) exactly --
        _gaps() must return nothing, and the non-sparse zero-fill pass
        must not run (or run harmlessly) when there's nothing to fill."""
        size = 8192
        _write_composition(tmp_path / "Composition", 20, 1, _mapping_record(0, 0, 0, map_num=2))
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _PLAINTEXT_A)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        pool = Pool(store, "Pool", dir_cache)
        comp_reader = CompositionReader(store, dir_cache, "Composition", StreamId(20), SessionId(1))
        file_a = DedupFile(comp_reader, pool, _HEAD_OFF, size=size)
        frag = DiskFragment(fid=1, start=0, end=size, dedup_file=file_a, src_file_path="D(x)O(0)S(0).img")
        disk = VirtualDiskContentSource(size=size, fragments=[frag])

        dst = tmp_path / "disk.img"
        result = await disk.export_to(dst, sparse=False)
        assert dst.read_bytes() == _PLAINTEXT_A[0] + _PLAINTEXT_A[1]
        assert result.holes == 0


class TestPositionalWriteFallback:
    """``os.pwrite`` doesn't exist on Windows — this project's own CI runs
    on Linux/macOS, where the ``sys.platform != "win32"`` branch is always
    taken; here the ``lseek``+``write`` fallback is exercised directly by
    forcing ``sys.platform`` to ``"win32"``, proving it lands bytes at the
    same offset ``os.pwrite`` would."""

    def test_fallback_lands_bytes_at_the_given_offset(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "win32")
        dst = tmp_path / "out.bin"
        dst.write_bytes(bytes(10))
        fd = os.open(dst, os.O_WRONLY | _O_BINARY)
        try:
            pcps_disk_mod._pwrite(fd, b"hello", 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"hello" + bytes(2)
