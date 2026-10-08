"""Unit tests for ``synology_apm_repo.sdk.dedup.composition_reader``."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

import pytest

from support.format_builders import (
    chunk_map_record_bytes,
    composition_header_bytes,
    record_head_bytes,
    zero_record,
)
from support.store_fakes import CountingStore
from synology_apm_repo.sdk.asynccache import AsyncKeyedCache
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader, CompositionRecord
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.composition import CompositionStatus, RecordHead
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _mapping_record(file_offset: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    return chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=(map_num << 16) | repeat,
    )


def _write_composition_subfile(path: Path, records_bytes: bytes, *, is_sub_zero: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    prefix = composition_header_bytes() if is_sub_zero else b""
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
        # No sub-file exists: n=0 returns before any path resolution.
        assert await reader.read_at(64, 0) == b""

    async def test_truncated_subfile_raises_format_error(self, tmp_path: Path, reader: CompositionReader) -> None:
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", b"short")
        with pytest.raises(FormatError, match="composition sub-file"):
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
        path.write_bytes(composition_header_bytes(sub_file_size=1234))
        with pytest.raises(DataCorruptError, match="subFileSize"):
            await reader.verify_header()


class TestRecord:
    async def test_open_record_and_read_head_fields(self, tmp_path: Path, reader: CompositionReader) -> None:
        entries = _mapping_record(0, (1 << 56) | (0 << 16) | 0, map_num=1)
        record_bytes = record_head_bytes(status=0, map_num=1) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        record = await reader.record(64)
        assert record.map_num == 1
        assert record.attr_leng == 0

    async def test_missing_redundancy_bit_raises(self, tmp_path: Path, reader: CompositionReader) -> None:
        record_bytes = record_head_bytes(status=0, map_num=0, mode=0x0000)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        with pytest.raises(UnsupportedVersionError, match="RecordHead lacks the Redundancy mode bit"):
            await reader.record(64)


class TestCompositionCache:
    """``record()``'s optional shared ``composition_cache`` (in production,
    the ``DedupRepo``'s repo-wide one)."""

    async def test_no_cache_means_every_call_is_a_fresh_fetch(self, tmp_path: Path) -> None:
        record_bytes = record_head_bytes(status=0, map_num=1) + _mapping_record(0, (1 << 56) | (0 << 16) | 0, 1)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))

        first = await reader.record(64)
        second = await reader.record(64)
        assert first is not second

    async def test_shared_cache_returns_the_identical_record_across_readers(self, tmp_path: Path) -> None:
        record_bytes = record_head_bytes(status=0, map_num=1) + _mapping_record(0, (1 << 56) | (0 << 16) | 0, 1)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = AsyncKeyedCache()

        first_reader = CompositionReader(
            store, dir_cache, "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )
        second_reader = CompositionReader(
            store, dir_cache, "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )

        first = await first_reader.record(64)
        second = await second_reader.record(64)
        assert first is second

    async def test_shared_cache_fetches_the_underlying_bytes_only_once(self, tmp_path: Path) -> None:
        record_bytes = record_head_bytes(status=0, map_num=1) + _mapping_record(0, (1 << 56) | (0 << 16) | 0, 1)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        counting_store = CountingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(counting_store)
        cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = AsyncKeyedCache()

        first_reader = CompositionReader(
            counting_store, dir_cache, "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )
        second_reader = CompositionReader(
            counting_store, dir_cache, "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )

        await first_reader.record(64)
        reads_before = counting_store.read_count
        await second_reader.record(64)
        reads_after = counting_store.read_count

        assert reads_after == reads_before  # the second reader's fetch never touched storage

    async def test_concurrent_resolves_for_the_same_key_fetch_only_once(self, tmp_path: Path) -> None:
        record_bytes = record_head_bytes(status=0, map_num=1) + _mapping_record(0, (1 << 56) | (0 << 16) | 0, 1)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        counting_store = CountingStore(LocalFsStore(tmp_path))
        dir_cache = DirCache(counting_store)
        cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = AsyncKeyedCache()
        readers = [
            CompositionReader(
                counting_store, dir_cache, "Composition", StreamId(7), SessionId(3), composition_cache=cache
            )
            for _ in range(5)
        ]

        results = await asyncio.gather(*(r.record(64) for r in readers))

        assert all(r is results[0] for r in results)
        # One RecordHead read for all five concurrent resolvers.
        assert counting_store.read_count == 1

    async def test_bounded_to_the_configured_maxsize(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        store = LocalFsStore(tmp_path)
        cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = AsyncKeyedCache(maxsize=64)
        reader = CompositionReader(
            store, DirCache(store), "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )

        async def fake_build_record(head_off: int) -> CompositionRecord:
            return CompositionRecord(
                head_off=head_off,
                record_head=RecordHead(
                    status=CompositionStatus.COMPLETE, map_num=0, map_crc=0, mode=0, attr_leng=0, attr_crc=0
                ),
                reader=reader,
            )

        monkeypatch.setattr(reader, "_build_record", fake_build_record)
        for head_off in range(65):
            await reader.record(head_off)

        assert len(cache) == 64
        assert (StreamId(7), SessionId(3), 0) not in cache  # the oldest was evicted
        assert (StreamId(7), SessionId(3), 64) in cache

    async def test_an_evicted_key_is_genuinely_refetched_through_the_real_key_construction(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``record()``'s ``(stream_id, session_id, head_off)`` key takes
        part in the cache's LRU eviction (the LRU itself is tested in
        ``test_asynccache.py``)."""
        store = LocalFsStore(tmp_path)
        cache: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = AsyncKeyedCache(maxsize=2)
        reader = CompositionReader(
            store, DirCache(store), "Composition", StreamId(7), SessionId(3), composition_cache=cache
        )
        built: list[int] = []

        async def fake_build_record(head_off: int) -> CompositionRecord:
            built.append(head_off)
            return CompositionRecord(
                head_off=head_off,
                record_head=RecordHead(
                    status=CompositionStatus.COMPLETE, map_num=0, map_crc=0, mode=0, attr_leng=0, attr_crc=0
                ),
                reader=reader,
            )

        monkeypatch.setattr(reader, "_build_record", fake_build_record)

        first = await reader.record(64)
        await reader.record(128)
        await reader.record(192)  # evicts (7, 3, 64), the oldest of 3 keys under maxsize=2

        assert (StreamId(7), SessionId(3), 64) not in cache
        refetched = await reader.record(64)
        assert refetched is not first
        assert built == [64, 128, 192, 64]


def _fake_page(start_entry: int, count: int) -> bytes:
    """``count`` consecutive MAPPING records, 4096 bytes apart, starting at
    entry index ``start_entry`` -- a page's bytes for a faked
    ``_fetch_page``, with no on-disk record."""
    return b"".join(_mapping_record(i * 4096, addr_int=1, map_num=1) for i in range(start_entry, start_entry + count))


class TestGetPageContiguousScanBookkeeping:
    """``_contiguous_scanned`` advances only past pages fetched in order; an
    out-of-order page records its end offset but extends the prefix only
    once the pages before it arrive. Driven through ``_get_page`` directly,
    since ``entries()`` always fetches in order."""

    async def test_fetching_pages_out_of_order_then_catches_the_prefix_up_in_one_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        monkeypatch.setattr(record, "_fetch_page", fake_fetch_page)

        await record._get_page(2)
        await record._get_page(1)
        assert record._contiguous_scanned == 0
        assert 1 in record._page_end_offsets
        assert 2 in record._page_end_offsets

        # Page 0 catches the prefix up past all three pages in one call.
        await record._get_page(0)
        assert record._contiguous_scanned == 3


class TestPageCacheEviction:
    """``_pages`` is a bounded LRU of page bytes beside the never-evicted
    ``_page_end_offsets`` index: eviction only costs a re-fetch, never
    ``_locate()``/``entries()`` correctness. Uses ``maxsize=2``."""

    def _record_with_fetch_tracking(self, monkeypatch: pytest.MonkeyPatch) -> tuple[CompositionRecord, dict[int, int]]:
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

        monkeypatch.setattr(record, "_fetch_page", fake_fetch_page)
        return record, fetch_calls

    async def test_evicted_page_is_dropped_but_its_end_offset_survives(self, monkeypatch: pytest.MonkeyPatch) -> None:
        record, fetch_calls = self._record_with_fetch_tracking(monkeypatch)

        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # maxsize=2: evicts page 0 (oldest)

        assert 0 not in record._pages
        assert set(record._pages) == {1, 2}
        assert record._page_end_offsets.keys() == {0, 1, 2}
        assert fetch_calls == {0: 1, 1: 1, 2: 1}

    async def test_locate_finds_a_later_page_without_touching_an_evicted_earlier_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        record, fetch_calls = self._record_with_fetch_tracking(monkeypatch)
        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # evicts page 0
        assert 0 not in record._pages

        # Entry 4097 = page 2, index 1.
        target_offset = 4097 * 4096
        page_idx, idx_in_page = await record._locate(target_offset)
        assert (page_idx, idx_in_page) == (2, 1)
        assert fetch_calls[0] == 1

    async def test_entries_transparently_refetches_an_evicted_page_with_correct_content(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        record, fetch_calls = self._record_with_fetch_tracking(monkeypatch)
        await record._get_page(0)
        await record._get_page(1)
        await record._get_page(2)  # evicts page 0
        assert 0 not in record._pages

        # Entry index 1 (of the now-evicted page 0) covers [4096, 8192).
        result = [e async for e in record.entries(start=4096, end=8192)]
        assert len(result) == 1
        assert result[0].file_offset == 4096
        assert fetch_calls[0] == 2


class TestConcurrentReseedRace:
    """A ``seed_pages_from_array()`` reseed racing an in-flight
    ``_get_page()`` of the same page waits on ``_lock_for_page`` for the
    fetch to finish, so the stale fetch never overwrites the reseed. A
    ``CompositionRecord`` is shared repo-wide, so the race is reachable."""

    async def test_reseed_waits_for_an_in_flight_fetch_then_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        record = CompositionRecord(
            head_off=0,
            record_head=RecordHead(
                status=CompositionStatus.COMPLETE, map_num=1, map_crc=0, mode=0, attr_leng=0, attr_crc=0
            ),
            reader=cast(CompositionReader, object()),
        )
        stale_page = _mapping_record(0, 1, 1)
        repaired_page = _mapping_record(4096, 1, 1)

        fetch_started = asyncio.Event()
        release_fetch = asyncio.Event()

        async def slow_fetch_page(page_idx: int) -> bytes:
            fetch_started.set()
            await release_fetch.wait()
            return stale_page

        monkeypatch.setattr(record, "_fetch_page", slow_fetch_page)

        ordinary_task = asyncio.create_task(record._get_page(0))
        await fetch_started.wait()  # the ordinary fetch is in flight, holding _lock_for_page(0)

        reseed_task = asyncio.create_task(record.seed_pages_from_array(repaired_page))
        await asyncio.sleep(0)  # let the reseed reach _lock_for_page(0) and block on it

        release_fetch.set()  # let the ordinary fetch complete and release the lock

        ordinary_result = await ordinary_task
        await reseed_task

        assert ordinary_result == stale_page  # the in-flight reader's own call is unaffected
        assert record._pages.get(0) == repaired_page
        assert record._page_end_offsets[0] == 4096 + 4096  # derived from repaired_page, not stale_page


class TestRepairedPagesSurviveEviction:
    async def test_an_evicted_seeded_page_comes_back_from_the_repaired_array(self) -> None:
        """A page ``seed_pages_from_array`` served, once evicted from the
        page LRU, is cut from the repaired array again, never re-read from
        the corrupted on-disk copy."""
        map_num = 2048 + 1  # two pages
        repaired = b"".join(_mapping_record(i * 4096, addr_int=1, map_num=1) for i in range(map_num))
        record = CompositionRecord(
            head_off=0,
            record_head=RecordHead(
                status=CompositionStatus.COMPLETE, map_num=map_num, map_crc=0, mode=0, attr_leng=0, attr_crc=0
            ),
            reader=cast(CompositionReader, object()),  # any disk read would fail
        )
        record._pages.maxsize = 1
        await record.seed_pages_from_array(repaired)

        await record._get_page(1)  # evicts page 0
        assert 0 not in record._pages
        assert await record._get_page(0) == repaired[: 2048 * 20]


class TestEntries:
    def _build_multi_entry_record(self, tmp_path: Path, reader: CompositionReader) -> None:
        # MAPPING [0,8192), MAPPING [8192,16384), ZERO [16384,20480),
        # HOLE (no entry) [20480,24576), MAPPING [24576,28672).
        addr0 = (1 << 56) | (10 << 16) | 0
        addr1 = (1 << 56) | (11 << 16) | 0
        addr4 = (1 << 56) | (12 << 16) | 0
        entries = (
            _mapping_record(0, addr0, map_num=2)
            + _mapping_record(8192, addr1, map_num=2)
            + zero_record(16384, zero_num=1)
            + _mapping_record(24576, addr4, map_num=1)
        )
        record_bytes = record_head_bytes(status=0, map_num=4) + entries
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

    @pytest.mark.parametrize(
        ("start", "expected_count", "expected_first_offset"),
        [
            pytest.param(8192, 3, 8192, id="exactly_at_an_entry_boundary"),
            # Inside the [8192,16384) entry: the entry itself, not skipped.
            pytest.param(9000, 3, 8192, id="in_the_middle_of_an_entry"),
            # Inside the [20480,24576) hole.
            pytest.param(22000, 1, 24576, id="inside_a_hole_lands_on_next_entry"),
        ],
    )
    async def test_entries_from_a_start_offset(
        self,
        tmp_path: Path,
        reader: CompositionReader,
        start: int,
        expected_count: int,
        expected_first_offset: int,
    ) -> None:
        self._build_multi_entry_record(tmp_path, reader)
        record = await reader.record(64)
        entries = [e async for e in record.entries(start=start)]
        assert len(entries) == expected_count
        assert entries[0].file_offset == expected_first_offset

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
        record_bytes = record_head_bytes(status=0, map_num=0)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert [e async for e in record.entries()] == []

    async def test_locating_the_last_entry_of_a_page_reads_that_page_once(self, tmp_path: Path) -> None:
        # 1000 one-chunk MAPPING entries, all within one page (_PAGE_SIZE=2048).
        n = 1000
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = CountingStore(LocalFsStore(tmp_path))
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
        assert reads_after - reads_before == 1

    async def test_page_boundary_is_crossed_transparently(self, tmp_path: Path) -> None:
        # 2500 entries span pages 0 and 1 (_PAGE_SIZE=2048).
        n = 2500
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        record = await reader.record(64)

        # Inside page 1 (entries 2048..2499).
        start_offset = 2200 * 4096
        entries = [e async for e in record.entries(start=start_offset)]
        assert len(entries) == n - 2200
        assert entries[0].file_offset == start_offset
        assert entries[-1].file_offset == (n - 1) * 4096

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
        record_bytes = record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = CountingStore(LocalFsStore(tmp_path))
        counting_reader = CompositionReader(
            counting_store, DirCache(counting_store), "Composition", StreamId(7), SessionId(3)
        )
        record = await counting_reader.record(64)

        first = [e async for e in record.entries(start=100 * 4096)]
        assert len(first) == n - 100

        reads_before = counting_store.read_count
        second = [e async for e in record.entries(start=250 * 4096)]
        reads_after = counting_store.read_count

        assert len(second) == n - 250
        assert reads_after == reads_before


class TestExtent:
    """``CompositionRecord.extent()`` -- needed by PC/PS disk fragments,
    whose ``file_meta.file_size`` is the whole disk's capacity, not the
    fragment's length."""

    async def test_empty_record_is_zero_to_zero(self, tmp_path: Path, reader: CompositionReader) -> None:
        record_bytes = record_head_bytes(status=0, map_num=0)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (0, 0)

    async def test_single_entry_extent_matches_its_own_span(self, tmp_path: Path, reader: CompositionReader) -> None:
        # A fragment starting 4 chunks into the disk (file_offset is
        # always chunk-aligned).
        addr = (1 << 56) | (10 << 16) | 0
        entries = _mapping_record(16384, addr, map_num=1)
        record_bytes = record_head_bytes(status=0, map_num=1) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (16384, 16384 + 4096)

    async def test_multi_entry_extent_spans_first_to_last_ignoring_the_gap(
        self, tmp_path: Path, reader: CompositionReader
    ) -> None:
        # TestEntries._build_multi_entry_record's layout, HOLE at [20480,24576) included.
        addr0 = (1 << 56) | (10 << 16) | 0
        addr1 = (1 << 56) | (11 << 16) | 0
        addr4 = (1 << 56) | (12 << 16) | 0
        entries = (
            _mapping_record(0, addr0, map_num=2)
            + _mapping_record(8192, addr1, map_num=2)
            + zero_record(16384, zero_num=1)
            + _mapping_record(24576, addr4, map_num=1)
        )
        record_bytes = record_head_bytes(status=0, map_num=4) + entries
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)
        record = await reader.record(64)
        assert await record.extent() == (0, 28672)

    async def test_extent_of_a_multi_page_record_only_reads_first_and_last_pages(self, tmp_path: Path) -> None:
        # 2500 entries span pages 0 and 1 (_PAGE_SIZE=2048).
        n = 2500
        parts = []
        for i in range(n):
            addr = (1 << 56) | (i << 16) | 0
            parts.append(_mapping_record(i * 4096, addr, map_num=1))
        record_bytes = record_head_bytes(status=0, map_num=n) + b"".join(parts)
        _write_composition_subfile(tmp_path / "Composition" / "7" / "3.com" / "c0", record_bytes)

        counting_store = CountingStore(LocalFsStore(tmp_path))
        counting_reader = CompositionReader(
            counting_store, DirCache(counting_store), "Composition", StreamId(7), SessionId(3)
        )
        record = await counting_reader.record(64)

        reads_before = counting_store.read_count
        extent = await record.extent()
        reads_after = counting_store.read_count

        assert extent == (0, n * 4096)
        assert reads_after - reads_before == 2  # first page + last page, nothing in between
