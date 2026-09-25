"""Unit tests for ``synology_apm_repo.sdk.dedup.composition_reader`` —
synthetic composition sub-files written to real files, no sample
repositories required."""

from __future__ import annotations

import zlib
from pathlib import Path
from typing import cast

import pytest

from synology_apm_repo.sdk.asynccache import AsyncKeyedCache
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader, CompositionRecord
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.composition import CompositionStatus, RecordHead
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _mapping_record(file_offset: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=(map_num << 16) | repeat,
    )


def _zero_record(file_offset: int, zero_num: int) -> bytes:
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.ZERO.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=0,
        tail_u32=zero_num,
    )


def _composition_header_bytes(*, major: int = 1, minor: int = 1, sub_file_size: int = SUB_FILE_SIZE) -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = sub_file_size.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _record_head_bytes(*, status: int, map_num: int, mode: int = 0x0001, attr_leng: int = 0) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[2:4] = status.to_bytes(2, "big")
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = (0).to_bytes(4, "big")  # mapCrc — never auto-verified by CompositionReader
    head[18:20] = mode.to_bytes(2, "big")
    head[20:24] = attr_leng.to_bytes(4, "big")
    head[24:28] = (0).to_bytes(4, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_composition_subfile(path: Path, records_bytes: bytes, *, is_sub_zero: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    prefix = _composition_header_bytes() if is_sub_zero else b""
    path.write_bytes(prefix + records_bytes)


@pytest.fixture
def reader(tmp_path: Path) -> CompositionReader:
    store = LocalFsStore(tmp_path)
    return CompositionReader(store, DirCache(store), "Composition", stream_id=StreamId(7), session_id=SessionId(3))


class TestReadAt:
    async def test_reads_within_a_single_subfile(self, tmp_path: Path, reader: CompositionReader) -> None:
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", b"hello world")
        assert await reader.read_at(64, 5) == b"hello"
        assert await reader.read_at(69, 6) == b" world"

    async def test_zero_length_read_returns_empty_without_touching_storage(self, reader: CompositionReader) -> None:
        # no sub-file exists at all yet — a truthful demonstration that
        # n=0 short-circuits before any path resolution/read happens.
        assert await reader.read_at(64, 0) == b""

    async def test_truncated_subfile_raises_format_error(self, tmp_path: Path, reader: CompositionReader) -> None:
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", b"short")
        with pytest.raises(FormatError):
            await reader.read_at(64, 100)

    async def test_resolves_sequence_suffixed_subfile(self, tmp_path: Path, reader: CompositionReader) -> None:
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0.42", b"payload-data")
        assert await reader.read_at(64, 7) == b"payload"

    async def test_crosses_16mib_subfile_boundary(self, tmp_path: Path, reader: CompositionReader) -> None:
        sub0 = tmp_path / "Composition" / "7" / "3.com" / "c0"
        sub1 = tmp_path / "Composition" / "7" / "3.com" / "c1"
        sub0.parent.mkdir(parents=True)

        marker_end = b"TAIL0123"  # 8 bytes, ends exactly at the 16 MiB boundary
        with sub0.open("wb") as f:
            f.seek(SUB_FILE_SIZE - len(marker_end) - 1)
            f.write(b"\x00" + marker_end)  # sparse file; only the tail is materialized

        marker_start = b"HEAD4567"
        sub1.write_bytes(marker_start)

        # global offset SUB_FILE_SIZE - 8 spans the last 8 bytes of sub0
        # and the first 8 bytes of sub1.
        result = await reader.read_at(SUB_FILE_SIZE - len(marker_end), len(marker_end) + len(marker_start))
        assert result == marker_end + marker_start


class TestVerifyHeader:
    async def test_valid_header(self, tmp_path: Path, reader: CompositionReader) -> None:
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", b"")
        header = await reader.verify_header()
        assert header.major == 1
        assert header.minor == 1

    async def test_wrong_sub_file_size_raises(self, tmp_path: Path, reader: CompositionReader) -> None:
        path = tmp_path / "Composition" / "7" / "3.com" / "c0"
        path.parent.mkdir(parents=True)
        path.write_bytes(_composition_header_bytes(sub_file_size=1234))
        with pytest.raises(DataCorruptError):
            await reader.verify_header()


class TestRecord:
    async def test_open_record_and_read_head_fields(self, tmp_path: Path, reader: CompositionReader) -> None:
        entries = _mapping_record(0, (1 << 56) | (0 << 16) | 0, map_num=1)
        record_bytes = _record_head_bytes(status=0, map_num=1) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        record = await reader.record(64)
        assert record.map_num == 1
        assert record.attr_leng == 0

    async def test_missing_redundancy_bit_raises(self, tmp_path: Path, reader: CompositionReader) -> None:
        record_bytes = _record_head_bytes(status=0, map_num=0, mode=0x0000)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        with pytest.raises(UnsupportedVersionError):
            await reader.record(64)


def _fake_page(start_entry: int, count: int) -> bytes:
    """``count`` consecutive, valid 4096-byte-apart MAPPING records
    starting at entry index ``start_entry`` — real enough for
    ``_resolve_page`` to parse the last one for its own bookkeeping,
    and for ``entries()``/``_locate()`` to walk meaningfully, without
    needing a real on-disk fixture (``_PAGE_SIZE``'s 2048 entries/page
    would need a multi-hundred-KB real fixture to reach a second page)."""
    return b"".join(_mapping_record(i * 4096, addr_int=1, map_num=1) for i in range(start_entry, start_entry + count))


class TestGetPageContiguousScanBookkeeping:
    """``_get_page``'s ``_contiguous_scanned`` prefix only ever advances
    past a page fetched *in order* — a page
    fetched out of order still gets its own end-offset recorded for exact
    reuse, but doesn't extend the prefix until the in-between pages are
    filled in too. Exercised directly against a bare ``CompositionRecord``
    with a faked ``_fetch_page`` (no real bytes/store needed — a real
    3-page fixture would need over 4096 real chunk-map entries just to
    reach a third page, per ``_PAGE_SIZE``'s own value) rather than
    through ``entries()``'s public, always-sequential access pattern,
    which never triggers an out-of-order fetch on its own."""

    async def test_fetching_pages_out_of_order_then_catches_the_prefix_up_in_one_call(self) -> None:
        record = CompositionRecord(
            head_off=0,
            record_head=RecordHead(
                status=CompositionStatus.COMPLETE, map_num=3 * 2048 + 1, map_crc=0, mode=0, attr_leng=0, attr_crc=0
            ),
            reader=cast(CompositionReader, object()),
        )

        async def fake_fetch_page(page_idx: int) -> bytes:
            start_entry, count = record._page_bounds(page_idx)
            return _fake_page(start_entry, count)

        record._fetch_page = fake_fetch_page  # type: ignore[method-assign]

        # Pages 2 then 1 fetched first: neither matches _contiguous_scanned
        # (0), so both get their own end-offset recorded without advancing
        # the prefix.
        await record._get_page(2)
        await record._get_page(1)
        assert record._contiguous_scanned == 0
        assert 1 in record._page_end_offsets
        assert 2 in record._page_end_offsets

        # Page 0 finally arrives: the prefix catches up past all three
        # pages already known, in one call — the while loop's own
        # second (and further) iteration.
        await record._get_page(0)
        assert record._contiguous_scanned == 3


class TestPageCacheEviction:
    """``_pages`` is a bounded LRU of raw page bytes (``_DEFAULT_PAGE_CACHE_MAXSIZE``,
    128 by default) with an always-resident ``_page_end_offsets`` boundary
    index alongside it — eviction from the former must never affect the
    correctness of ``_locate()``/``entries()``, only whether a page's
    content needs a real re-fetch. Uses an explicit small ``maxsize``
    (a regular, constructor-overridable dataclass field) rather than a
    real multi-hundred-page fixture, mirroring ``test_dedup_pool.py``'s
    own ``TestLruEviction``."""

    def _record_with_fetch_tracking(self) -> tuple[CompositionRecord, dict[int, int]]:
        fetch_calls: dict[int, int] = {}
        record = CompositionRecord(
            head_off=0,
            record_head=RecordHead(
                status=CompositionStatus.COMPLETE, map_num=2 * 2048 + 5, map_crc=0, mode=0, attr_leng=0, attr_crc=0
            ),
            reader=cast(CompositionReader, object()),
            _pages=AsyncKeyedCache(maxsize=2),
        )

        async def fake_fetch_page(page_idx: int) -> bytes:
            fetch_calls[page_idx] = fetch_calls.get(page_idx, 0) + 1
            start_entry, count = record._page_bounds(page_idx)
            return _fake_page(start_entry, count)

        record._fetch_page = fake_fetch_page  # type: ignore[method-assign]
        return record, fetch_calls

    async def test_evicted_page_is_dropped_but_its_end_offset_survives(self) -> None:
        record, fetch_calls = self._record_with_fetch_tracking()

        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # maxsize=2: evicts page 0 (oldest)

        assert 0 not in record._pages
        assert set(record._pages) == {1, 2}
        # Never evicted, unlike _pages -- this is what keeps _locate's
        # binary search free of I/O regardless of what's still resident.
        assert record._page_end_offsets.keys() == {0, 1, 2}
        assert fetch_calls == {0: 1, 1: 1, 2: 1}

    async def test_locate_finds_a_later_page_without_touching_an_evicted_earlier_one(self) -> None:
        record, fetch_calls = self._record_with_fetch_tracking()
        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # evicts page 0
        assert 0 not in record._pages

        # Entry index 2*2048 + 1 = 4097 is the second entry of page 2,
        # covering [4097*4096, 4098*4096).
        target_offset = 4097 * 4096
        page_idx, idx_in_page = await record._locate(target_offset)
        assert (page_idx, idx_in_page) == (2, 1)
        # Finding a page beyond the evicted one never needed to re-fetch it.
        assert fetch_calls[0] == 1

    async def test_entries_transparently_refetches_an_evicted_page_with_correct_content(self) -> None:
        record, fetch_calls = self._record_with_fetch_tracking()
        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # evicts page 0
        assert 0 not in record._pages

        # Entry index 1 (of the now-evicted page 0) covers [4096, 8192).
        result = [e async for e in record.entries(start=4096, end=8192)]
        assert len(result) == 1
        assert result[0].file_offset == 4096
        # One real re-fetch happened to serve this -- content, not just
        # the page index, was genuinely needed.
        assert fetch_calls[0] == 2


class TestEntries:
    def _build_multi_entry_record(self, tmp_path: Path, reader: CompositionReader) -> None:
        # 4 entries: MAPPING [0,8192), MAPPING [8192,16384), ZERO [16384,20480),
        # then a HOLE (gap) [20480, 24576), then MAPPING [24576, 28672).
        addr0 = (1 << 56) | (10 << 16) | 0
        addr1 = (1 << 56) | (11 << 16) | 0
        addr4 = (1 << 56) | (12 << 16) | 0
        entries = (
            _mapping_record(0, addr0, map_num=2)  # covers [0, 8192)
            + _mapping_record(8192, addr1, map_num=2)  # covers [8192, 16384)
            + _zero_record(16384, zero_num=1)  # covers [16384, 20480)
            + _mapping_record(24576, addr4, map_num=1)  # covers [24576, 28672)
        )
        record_bytes = _record_head_bytes(status=0, map_num=4) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

    async def test_full_sequential_iteration(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries()]
        assert len(entries) == 4
        assert entries[0].kind is ChunkMapKind.MAPPING
        assert entries[0].file_offset == 0
        assert entries[2].kind is ChunkMapKind.ZERO
        assert entries[2].file_offset == 16384
        assert entries[3].file_offset == 24576

    async def test_start_exactly_at_an_entry_boundary(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=8192)]
        assert len(entries) == 3
        assert entries[0].file_offset == 8192

    async def test_start_in_the_middle_of_an_entry(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=9000)]  # inside the [8192,16384) entry
        assert len(entries) == 3
        assert entries[0].file_offset == 8192  # the entry itself, not skipped

    async def test_start_inside_a_hole_lands_on_next_entry(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=22000)]  # inside the [20480,24576) hole
        assert len(entries) == 1
        assert entries[0].file_offset == 24576

    async def test_end_excludes_entries_starting_at_or_after_it(
        self, tmp_path: Path, reader: CompositionReader
    ) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(end=16384)]
        assert len(entries) == 2
        assert entries[-1].file_offset == 8192

    async def test_start_and_end_bound_a_middle_range(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=9000, end=17000)]
        assert [e.file_offset for e in entries] == [8192, 16384]

    async def test_start_past_every_entry_yields_nothing(self, tmp_path: Path, reader: CompositionReader) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=999_999)]
        assert entries == []

    async def test_zero_map_num_yields_nothing(self, tmp_path: Path, reader: CompositionReader) -> None:
        record_bytes = _record_head_bytes(status=0, map_num=0)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert [e async for e in record.entries()] == []

    async def test_binary_search_touches_far_fewer_entries_than_a_linear_scan_would(self, tmp_path: Path) -> None:
        # 1000 sequential MAPPING entries, each covering one 4096-byte chunk,
        # all within one page (_PAGE_SIZE=2048) — locating the *last* one
        # must not degrade to O(n) or even to one read per entry.
        n = 1000
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = _record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = _CountingStore(LocalFsStore(tmp_path))
        counting_reader = CompositionReader(
            counting_store, DirCache(counting_store), "Composition", StreamId(7), SessionId(3)
        )
        record = await counting_reader.record(64)
        target_offset = (n - 1) * 4096

        reads_before = counting_store.read_count
        entries = [e async for e in record.entries(start=target_offset)]
        reads_after = counting_store.read_count

        assert len(entries) == 1
        assert entries[0].file_offset == target_offset
        # the whole 1000-entry array fits in one page: one merged read
        # fetches it all, in-memory binary search does the rest — nowhere
        # near a 1000-read linear scan or a per-entry probe.
        assert reads_after - reads_before == 1

    async def test_page_boundary_is_crossed_transparently(self, tmp_path: Path) -> None:
        # _PAGE_SIZE=2048 entries per page; 2500 entries means the target
        # (near the end) lives in page 1, and entries() must walk from
        # wherever ``start`` lands straight through to the true end,
        # correctly crossing from page 1 into... (there is no page 2 here,
        # but the walk must not stop short at the first page's boundary
        # when ``start`` itself resolves into a later page).
        n = 2500
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = _record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        record = await reader.record(64)

        # start squarely inside page 1 (entries 2048..2499)
        start_offset = 2200 * 4096
        entries = [e async for e in record.entries(start=start_offset)]
        assert len(entries) == n - 2200
        assert entries[0].file_offset == start_offset
        assert entries[-1].file_offset == (n - 1) * 4096

        # a full walk from the very start must still cross the page 0/1
        # boundary and reach the same last entry.
        full = [e async for e in record.entries()]
        assert len(full) == n
        assert full[-1].file_offset == (n - 1) * 4096
        assert full[2048].file_offset == 2048 * 4096  # first entry of page 1

    async def test_second_lookup_in_an_already_cached_page_does_no_further_io(self, tmp_path: Path) -> None:
        n = 500  # comfortably within one page
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = _record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = _CountingStore(LocalFsStore(tmp_path))
        counting_reader = CompositionReader(
            counting_store, DirCache(counting_store), "Composition", StreamId(7), SessionId(3)
        )
        record = await counting_reader.record(64)

        # first lookup warms the page cache
        first = [e async for e in record.entries(start=100 * 4096)]
        assert len(first) == n - 100

        reads_before = counting_store.read_count
        # a second, different lookup against the *same* record must reuse
        # the already-cached page — zero further storage reads.
        second = [e async for e in record.entries(start=250 * 4096)]
        reads_after = counting_store.read_count

        assert len(second) == n - 250
        assert reads_after == reads_before


class TestExtent:
    """``CompositionRecord.extent()`` — needed by PC/PS's per-region disk
    fragments, whose registered ``file_meta.file_size`` is the whole
    disk's capacity, never the fragment's own real length."""

    async def test_empty_record_is_zero_to_zero(self, tmp_path: Path, reader: CompositionReader) -> None:
        record_bytes = _record_head_bytes(status=0, map_num=0)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (0, 0)

    async def test_single_entry_extent_matches_its_own_span(self, tmp_path: Path, reader: CompositionReader) -> None:
        # A fragment starting well off zero (a PC/PS disk-region fragment
        # whose own composition doesn't begin at byte 0 of the disk) --
        # file_offset is always chunk-aligned (fileChunkIdx << 12,
        # format/chunkmap.py), so 4 chunks in (16384), not an arbitrary
        # byte count.
        addr = (1 << 56) | (10 << 16) | 0
        entries = _mapping_record(16384, addr, map_num=1)
        record_bytes = _record_head_bytes(status=0, map_num=1) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (16384, 16384 + 4096)

    async def test_multi_entry_extent_spans_first_to_last_ignoring_the_gap(
        self, tmp_path: Path, reader: CompositionReader
    ) -> None:
        # Same layout as TestEntries._build_multi_entry_record: a HOLE gap
        # sits between [20480,24576) — extent() must span start-to-end
        # across it, not stop short or double-count it.
        addr0 = (1 << 56) | (10 << 16) | 0
        addr1 = (1 << 56) | (11 << 16) | 0
        addr4 = (1 << 56) | (12 << 16) | 0
        entries = (
            _mapping_record(0, addr0, map_num=2)  # covers [0, 8192)
            + _mapping_record(8192, addr1, map_num=2)  # covers [8192, 16384)
            + _zero_record(16384, zero_num=1)  # covers [16384, 20480)
            + _mapping_record(24576, addr4, map_num=1)  # covers [24576, 28672)
        )
        record_bytes = _record_head_bytes(status=0, map_num=4) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (0, 28672)

    async def test_extent_of_a_multi_page_record_only_reads_first_and_last_pages(self, tmp_path: Path) -> None:
        # 2500 entries -> spans page 0 and page 1 (_PAGE_SIZE=2048).
        # extent() must resolve via exactly two page reads, never a full
        # scan across every intervening page.
        n = 2500
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = _record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = _CountingStore(LocalFsStore(tmp_path))
        counting_reader = CompositionReader(
            counting_store, DirCache(counting_store), "Composition", StreamId(7), SessionId(3)
        )
        record = await counting_reader.record(64)

        reads_before = counting_store.read_count
        extent = await record.extent()
        reads_after = counting_store.read_count

        assert extent == (0, n * 4096)
        assert reads_after - reads_before == 2  # first page + last page, nothing in between


class _CountingStore:
    """Wraps a real ``ObjectStore``, tallying ``read()`` calls — used to
    make the binary-search claim in ``entries()`` an actually-checked fact
    rather than an assumption."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self.read_count = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.read_count += 1
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)
