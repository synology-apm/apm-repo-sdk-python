"""Unit tests for ``synology_apm_repo.sdk.dedup.dedup_file``. Most tests read
``unit.sdk.dedup_export_fakes``'s standard file, whose layout
``standard_entries()`` documents."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

import pytest

from support.fakes import faithful_to
from support.format_builders import (
    chunk_map_record_bytes,
    composition_header_bytes,
    mapping_record,
    record_head_bytes,
    zero_record,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
)
from support.store_fakes import CountingStore
from synology_apm_repo.sdk.dedup import chunk_walk
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import (
    MAX_SINGLE_READ_SIZE,
    ByteRangeView,
    DedupFile,
    read_blocks,
    validate_export_range,
    validate_read_args,
)
from synology_apm_repo.sdk.dedup.export_sink import run_sink_export
from synology_apm_repo.sdk.dedup.extent import DataExtent, ExportResult, ExtentKind, GapExtent
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink
from synology_apm_repo.sdk.dedup.pool import BucketReader, Pool
from synology_apm_repo.sdk.errors import ResourceLimitExceededError
from synology_apm_repo.sdk.format import addressing
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import ContentSource
from unit.sdk.dedup_export_fakes import (
    CHUNK_PLAINTEXTS,
    HEAD_OFF,
    SESSION_ID,
    SIZE,
    STREAM_ID,
    build_blocking_dedup_file,
    dedup_file_at,
    standard_entries,
)
from unit.sdk.pool_fakes import chunk_address


async def _export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool = True,
    progress: Callable[[int], Awaitable[None]] | None = None,
) -> ExportResult:
    """``export_range`` into an unstaged ``LocalFileSink`` at ``dst``, driven by ``run_sink_export``."""
    _, _, size = file_like.export_window()
    sink = LocalFileSink(dst, staged=False)
    return await run_sink_export(
        sink, size, sparse=sparse, body=lambda: file_like.export_range(sink, 0, size, sparse=sparse, progress=progress)
    )


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    return dedup_file_at(tmp_path)


class TestExtents:
    """Calls the private ``_extents()`` directly: chunk-map parsing (kind
    classification, HOLE synthesis, template fields) needs the raw ``Extent``s."""

    async def test_full_walk_kinds_and_offsets(self, dedup_file: DedupFile) -> None:
        extents = [e async for e in dedup_file._extents()]
        kinds_and_offsets = [(e.kind, e.offset, e.length) for e in extents]
        assert kinds_and_offsets == [
            (ExtentKind.DATA, 0, 3 * 4096),
            (ExtentKind.ZERO, 12288, 2 * 4096),
            (ExtentKind.HOLE, 20480, 4096),
            (ExtentKind.DATA, 24576, 4 * 4096),
            (ExtentKind.HOLE, 40960, 4096),
        ]

    async def test_data_extent_carries_template_fields(self, dedup_file: DedupFile) -> None:
        first = await anext(dedup_file._extents())
        assert isinstance(first, DataExtent)
        assert first.map_num == 3
        assert first.repeat == 0
        assert first.addr == chunk_address(0, 0, 0)

    async def test_zero_and_hole_extents_are_gaps(self, dedup_file: DedupFile) -> None:
        extents = [e async for e in dedup_file._extents()]
        assert [type(e) for e in extents[1:3]] == [GapExtent, GapExtent]

    async def test_no_trailing_hole_when_size_matches_last_record_end(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        exact_size_file = DedupFile(comp_reader, pool, HEAD_OFF, size=40960)
        extents = [e async for e in exact_size_file._extents()]
        assert extents[-1].kind is not ExtentKind.HOLE or extents[-1].offset != 40960

    async def test_zero_length_entry_is_skipped_not_yielded_as_an_empty_extent(self, tmp_path: Path) -> None:
        # A ZERO record with zero_num=0 covers nothing: no empty Extent, and
        # the following record is unaffected.
        entries = (
            mapping_record(0, 0, 0, map_num=1) + zero_record(4096, zero_num=0) + mapping_record(4096, 0, 1, map_num=1)
        )
        record_bytes = record_head_bytes(map_num=3) + entries
        comp_path = tmp_path / "Composition" / str(STREAM_ID) / f"{SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(composition_header_bytes() + record_bytes)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=8192)

        extents = [e async for e in file._extents()]
        assert [(e.kind, e.offset, e.length) for e in extents] == [
            (ExtentKind.DATA, 0, 4096),
            (ExtentKind.DATA, 4096, 4096),
        ]
        assert await file.read(0, 8192) == CHUNK_PLAINTEXTS[0] + CHUNK_PLAINTEXTS[1]


async def test_stream_id_session_id_comp_offset_properties(dedup_file: DedupFile) -> None:
    assert dedup_file.stream_id == STREAM_ID
    assert dedup_file.session_id == SESSION_ID
    assert dedup_file.comp_offset == HEAD_OFF


async def test_pool_property(dedup_file: DedupFile) -> None:
    assert isinstance(dedup_file.pool, Pool)


class TestRead:
    async def test_returns_a_fresh_buffer_the_caller_owns(self, dedup_file: DedupFile) -> None:
        """The filled buffer is handed over uncopied, so each read must be a
        new one: mutating it must not reach the cache or a later read."""
        first = await dedup_file.read(0, 8192)
        assert isinstance(first, bytearray)
        first[:] = bytes(len(first))

        assert await dedup_file.read(0, 8192) == CHUNK_PLAINTEXTS[0] + CHUNK_PLAINTEXTS[1]

    async def test_reads_a_single_data_chunk(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(0, 4096) == CHUNK_PLAINTEXTS[0]
        assert await dedup_file.read(4096, 4096) == CHUNK_PLAINTEXTS[1]

    async def test_reads_across_multiple_data_chunks(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(2048, 4096)  # spans the boundary between chunk 0 and chunk 1
        assert result == CHUNK_PLAINTEXTS[0][2048:] + CHUNK_PLAINTEXTS[1][:2048]

    @pytest.mark.parametrize(
        ("offset", "length"),
        [
            pytest.param(12288, 8192, id="zero_region"),
            pytest.param(20480, 4096, id="hole_region"),
            pytest.param(40960, 4096, id="trailing_hole_past_last_record"),
        ],
    )
    async def test_reads_as_zero_bytes(self, dedup_file: DedupFile, offset: int, length: int) -> None:
        assert await dedup_file.read(offset, length) == b"\x00" * length

    async def test_reads_repeated_template_correctly(self, dedup_file: DedupFile) -> None:
        # map_num=2, repeat=1 at offset 24576: chunks [3, 4, 3, 4]
        result = await dedup_file.read(24576, 4 * 4096)
        expected = CHUNK_PLAINTEXTS[3] + CHUNK_PLAINTEXTS[4] + CHUNK_PLAINTEXTS[3] + CHUNK_PLAINTEXTS[4]
        assert result == expected

    async def test_reads_a_sub_range_within_a_repeated_chunk(self, dedup_file: DedupFile) -> None:
        # bytes [100, 200) of the 3rd repeated chunk (index 2 -> chunk 3 again)
        offset = 24576 + 2 * 4096 + 100
        result = await dedup_file.read(offset, 100)
        assert result == CHUNK_PLAINTEXTS[3][100:200]

    async def test_reads_spanning_data_zero_and_hole(self, dedup_file: DedupFile) -> None:
        # last 100 bytes of chunk 2 (DATA) + all of ZERO + all of HOLE + first
        # 100 bytes of the next DATA region
        start = 3 * 4096 - 100
        length = 100 + 8192 + 4096 + 100
        result = await dedup_file.read(start, length)
        expected = CHUNK_PLAINTEXTS[2][-100:] + b"\x00" * (8192 + 4096) + CHUNK_PLAINTEXTS[3][:100]
        assert result == expected

    async def test_default_length_reads_to_end_of_size(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(40960)
        assert result == b"\x00" * 4096

    async def test_zero_length_read_returns_empty(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(0, 0) == b""

    async def test_over_length_read_clamps_to_size_instead_of_zero_padding(self, dedup_file: DedupFile) -> None:
        # Only the 4096 bytes up to size=45056 come back, not 8192.
        result = await dedup_file.read(40960, 8192)
        assert result == b"\x00" * 4096

    async def test_read_starting_at_or_past_size_returns_empty(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(45056, 10) == b""
        assert await dedup_file.read(50000, 10) == b""

    async def test_negative_offset_raises(self, dedup_file: DedupFile) -> None:
        with pytest.raises(ValueError, match="offset must be non-negative"):
            await dedup_file.read(-1)

    async def test_negative_length_raises(self, dedup_file: DedupFile) -> None:
        with pytest.raises(ValueError, match="length must be non-negative"):
            await dedup_file.read(0, -1)

    async def test_read_without_length_raises_when_size_unknown(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="size is unknown"):
            await unsized.read(0)

    async def test_a_declared_size_past_the_single_read_ceiling_raises(self, tmp_path: Path) -> None:
        # Raised before any extent is walked; only the declared size matters.
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        oversized = DedupFile(comp_reader, pool, HEAD_OFF, size=MAX_SINGLE_READ_SIZE + 1)
        with pytest.raises(ResourceLimitExceededError, match="single-read safety ceiling"):
            await oversized.read(0)

    async def test_a_declared_size_at_the_single_read_ceiling_is_allowed(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        # Declared size exactly at the ceiling (the check is ``>``, not ``>=``),
        # read within the small real data.
        at_ceiling = DedupFile(comp_reader, pool, HEAD_OFF, size=MAX_SINGLE_READ_SIZE)
        assert await at_ceiling.read(0, 4096) == CHUNK_PLAINTEXTS[0]


class TestReadAcrossBuckets:
    """``_fill_data_extent()``'s batch path via ``_resolve_bucket_group()``: several
    distinct chunks in one ``read()`` are fetched with one
    ``BucketReader.read_chunks()`` per bucket, still using ``Pool``'s chunk cache."""

    async def test_read_spanning_two_buckets_via_a_single_extents_carry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With ``BUCKET_MAX_CHUNK_NUM`` patched to 2, one DATA extent with
        ``map_num=2`` carries from (bucket 0, chunk 1) into (bucket 1, chunk 0),
        so one ``_fill_data_extent()`` call needs chunks from two buckets."""
        monkeypatch.setattr(addressing, "BUCKET_MAX_CHUNK_NUM", 2)
        monkeypatch.setattr(chunk_walk, "BUCKET_MAX_CHUNK_NUM", 2)
        entries = mapping_record(0, 0, 1, map_num=2)
        record_bytes = record_head_bytes(map_num=1) + entries
        comp_path = tmp_path / "Composition" / str(STREAM_ID) / f"{SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(composition_header_bytes() + record_bytes)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        bucket_1_chunk0 = bytes([200]) * 4096
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bucket_1_chunk0])

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=2 * 4096)

        calls: list[int] = []
        real_read_chunk = Pool.read_chunk

        async def counting_read_chunk(self: Pool, addr: object, **kwargs: object) -> bytes:
            calls.append(1)
            return await real_read_chunk(self, addr, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(Pool, "read_chunk", counting_read_chunk)

        result = await file.read(0, 2 * 4096)
        assert result == CHUNK_PLAINTEXTS[1] + bucket_1_chunk0
        # The batch path was used, not Pool.read_chunk().
        assert calls == []
        # Both fetched chunks were backfilled into Pool's cache.
        assert pool._chunks[(StreamId(0), BucketId(0), ChunkIdx(1))] == CHUNK_PLAINTEXTS[1]
        assert pool._chunks[(StreamId(0), BucketId(1), ChunkIdx(0))] == bucket_1_chunk0

        # A later Pool.read_chunk() for one of them is a cache hit.
        store_reads: list[int] = []
        real_store_read = LocalFsStore.read

        async def counting_store_read(self: LocalFsStore, *args: object, **kwargs: object) -> bytes:
            store_reads.append(1)
            return await real_store_read(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(LocalFsStore, "read", counting_store_read)
        again = await pool.read_chunk(chunk_address(0, 0, 1))
        assert again == CHUNK_PLAINTEXTS[1]
        assert store_reads == []

    async def test_a_release_caches_racing_the_fetch_skips_the_backfill(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``release_caches()`` during the batch fetch (``Pool.release_epoch`` guard)
        must not backfill entries into the emptied ``Pool``."""
        monkeypatch.setattr(addressing, "BUCKET_MAX_CHUNK_NUM", 2)
        monkeypatch.setattr(chunk_walk, "BUCKET_MAX_CHUNK_NUM", 2)
        entries = mapping_record(0, 0, 1, map_num=2)
        record_bytes = record_head_bytes(map_num=1) + entries
        comp_path = tmp_path / "Composition" / str(STREAM_ID) / f"{SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(composition_header_bytes() + record_bytes)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        bucket_1_chunk0 = bytes([200]) * 4096
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bucket_1_chunk0])

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=2 * 4096)

        real_read_chunks = BucketReader.read_chunks

        async def releasing_read_chunks(
            self: BucketReader,
            stream_id: StreamId,
            bucket_id: BucketId,
            ranges: Sequence[tuple[int, int]],
            *,
            semaphore: asyncio.Semaphore | None = None,
        ) -> dict[int, bytes | memoryview]:
            # Simulates a concurrent release_caches() landing mid-fetch.
            result = await real_read_chunks(self, stream_id, bucket_id, ranges, semaphore=semaphore)
            pool.release_caches()
            return result

        monkeypatch.setattr(BucketReader, "read_chunks", releasing_read_chunks)

        result = await file.read(0, 2 * 4096)

        assert result == CHUNK_PLAINTEXTS[1] + bucket_1_chunk0  # correct despite the release
        assert (StreamId(0), BucketId(0), ChunkIdx(1)) not in pool._chunks
        assert (StreamId(0), BucketId(1), ChunkIdx(0)) not in pool._chunks

    async def test_read_spanning_two_separate_bucket_backed_extents(self, tmp_path: Path) -> None:
        # Two single-chunk DATA extents (bucket 0 at [0, 4096), bucket 1 at
        # [4096, 8192)), each resolved to its own bucket by one read().
        entries = mapping_record(0, 0, 0, map_num=1) + mapping_record(4096, 1, 0, map_num=1)
        record_bytes = record_head_bytes(map_num=2) + entries
        comp_path = tmp_path / "Composition" / str(STREAM_ID) / f"{SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(composition_header_bytes() + record_bytes)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", [CHUNK_PLAINTEXTS[0]])
        bucket_1_chunk0 = bytes([200]) * 4096
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bucket_1_chunk0])

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=8192)

        result = await file.read(0, 8192)
        assert result == CHUNK_PLAINTEXTS[0] + bucket_1_chunk0


class TestStream:
    async def test_stream_reassembles_to_the_same_bytes_as_read(self, dedup_file: DedupFile) -> None:
        whole = await dedup_file.read(0, SIZE)
        reassembled = b"".join([chunk async for _offset, chunk in dedup_file.stream(block=1000)])
        assert reassembled == whole

    async def test_stream_offsets_are_contiguous_and_correct(self, dedup_file: DedupFile) -> None:
        offsets = [offset async for offset, _chunk in dedup_file.stream(block=8192)]
        assert offsets == list(range(0, SIZE, 8192))

    async def test_stream_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        file, store = build_blocking_dedup_file(tmp_path)

        async def consume() -> list[int]:
            store.armed = True
            return [offset async for offset, _chunk in file.stream(block=1000)]

        task = asyncio.create_task(consume())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_stream_requires_known_size(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await anext(unsized.stream())


class TestExportTo:
    async def test_sparse_export_matches_read_content(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        result = await _export_to(dedup_file, dst, sparse=True)
        assert dst.stat().st_size == SIZE
        assert dst.read_bytes() == await dedup_file.read(0, SIZE)
        assert result.logical_size == SIZE
        assert result.bytes_written == 3 * 4096 + 4 * 4096
        assert result.zeros == 2 * 4096
        assert result.holes == 4096 + 4096

    async def test_non_sparse_export_matches_read_content(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        result = await _export_to(dedup_file, dst, sparse=False)
        assert dst.stat().st_size == SIZE
        assert dst.read_bytes() == await dedup_file.read(0, SIZE)
        assert result.bytes_written == 3 * 4096 + 4 * 4096

    async def test_export_reports_progress(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        calls: list[int] = []

        async def _progress(written: int) -> None:
            calls.append(written)

        await _export_to(dedup_file, tmp_path / "out.bin", progress=_progress)
        # One report per contiguous DATA write ([0,12288) and [24576,40960)); holes and zeros never count.
        assert sorted(calls) == [3 * 4096, 4 * 4096]

    async def test_export_is_cancellable_and_still_closes_its_partial_output(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A cancelled export closes the destination fd: ``run_sink_export`` calls
        ``LocalFileSink.abort()`` on ``asyncio.CancelledError`` too."""
        file, store = build_blocking_dedup_file(tmp_path)
        dst = tmp_path / "out.bin"

        opened_fds: list[int] = []
        closed_fds: list[int] = []
        real_open, real_close = os.open, os.close

        def _spy_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
            fd = real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]
            if str(path) == str(dst):
                opened_fds.append(fd)
            return fd

        def _spy_close(fd: int) -> None:
            if fd in opened_fds:
                closed_fds.append(fd)
            real_close(fd)

        monkeypatch.setattr(os, "open", _spy_open)
        monkeypatch.setattr(os, "close", _spy_close)

        async def do_export() -> object:
            store.armed = True
            return await _export_to(file, dst)

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(opened_fds) == 1  # the export reached opening its output
        assert closed_fds == opened_fds  # ...and closed it despite the cancel
        assert not dst.exists()  # nothing written, so no file is left

    async def test_export_clips_a_trailing_data_extent_that_runs_past_size(self, tmp_path: Path) -> None:
        """``_extents()`` doesn't clip to size, so the export must clip a trailing
        DATA extent past the declared size: here 100 bytes into ``[24576, 40960)``."""
        truncated_size = 40960 - 100
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=truncated_size)

        dst = tmp_path / "out.bin"
        result = await _export_to(file, dst)

        # [0,12288)=DATA, [12288,20480)=ZERO, [20480,24576)=HOLE,
        # [24576,40960)=DATA clipped to 40860 -> 16284 of its normal 16384.
        assert dst.stat().st_size == truncated_size
        assert result.zeros == 2 * 4096
        assert result.holes == 4096
        assert result.bytes_written == 12288 + 16284
        assert result.bytes_written == truncated_size - result.zeros - result.holes
        assert dst.read_bytes() == await file.read(0, truncated_size)

    async def test_export_requires_known_size(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), stream_id=STREAM_ID, session_id=SESSION_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await _export_to(unsized, tmp_path / "out.bin")


async def _export_range_to(
    file_like: DedupFile | ByteRangeView, dst: Path, start: int, end: int, *, sparse: bool = True
) -> ExportResult:
    """``export_range`` of ``[start, end)`` into an unstaged ``LocalFileSink`` at ``dst``."""
    sink = LocalFileSink(dst, staged=False)
    return await run_sink_export(
        sink, end - start, sparse=sparse, body=lambda: file_like.export_range(sink, start, end, sparse=sparse)
    )


class TestExportRange:
    async def test_a_sub_range_lands_at_offsets_relative_to_its_start(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        dst = tmp_path / "out.bin"
        result = await _export_range_to(dedup_file, dst, 4096, 24576, sparse=False)
        assert dst.read_bytes() == await dedup_file.read(4096, 24576 - 4096)
        assert result.logical_size == 24576 - 4096

    async def test_a_view_sub_range_is_taken_in_the_views_own_coordinates(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        view = dedup_file.view(8192, 24576)
        dst = tmp_path / "out.bin"
        result = await _export_range_to(view, dst, 4096, 12288, sparse=False)
        assert dst.read_bytes() == await view.read(4096, 12288 - 4096)
        assert result.logical_size == 12288 - 4096

    async def test_the_whole_range_is_the_whole_export(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        result = await _export_range_to(dedup_file, dst, 0, SIZE)
        assert dst.read_bytes() == await dedup_file.read(0, SIZE)
        assert result.logical_size == SIZE

    @pytest.mark.parametrize(("start", "end"), [(-1, 4096), (8192, 4096), (0, SIZE + 1)])
    async def test_a_range_outside_the_file_is_rejected(
        self, dedup_file: DedupFile, tmp_path: Path, start: int, end: int
    ) -> None:
        with pytest.raises(ValueError, match="is not inside"):
            await dedup_file.export_range(LocalFileSink(tmp_path / "out.bin", staged=False), start, end)

    @pytest.mark.parametrize(("start", "end"), [(-1, 4096), (8192, 4096), (0, 16385)])
    async def test_a_range_outside_the_view_is_rejected(
        self, dedup_file: DedupFile, tmp_path: Path, start: int, end: int
    ) -> None:
        view = dedup_file.view(8192, 16384)
        with pytest.raises(ValueError, match="is not inside"):
            await view.export_range(LocalFileSink(tmp_path / "out.bin", staged=False), start, end)

    async def test_a_range_not_starting_on_a_chunk_boundary_is_rejected(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        sink = LocalFileSink(tmp_path / "out.bin", staged=False)
        with pytest.raises(ValueError, match="multiple of 4096"):
            await dedup_file.export_range(sink, 100, 8192)
        with pytest.raises(ValueError, match="multiple of 4096"):
            await dedup_file.view(100, 20000).export_range(sink, 0, 4096)  # the view itself starts mid-chunk
        await _export_range_to(dedup_file.view(4096, 20000), tmp_path / "ok.bin", 0, 4096)  # aligned in the base file

    async def test_planned_bytes_counts_the_real_data_in_the_range(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.planned_bytes(0, SIZE) == 3 * 4096 + 4 * 4096
        assert await dedup_file.planned_bytes(0, 0) == 0
        view = dedup_file.view(8192, 24576)
        assert await view.planned_bytes(0, 24576) == await dedup_file.planned_bytes(8192, 8192 + 24576)
        assert await view.planned_bytes(4096, 8192) == await dedup_file.planned_bytes(12288, 16384)


class TestExportRangeHelpers:
    def test_a_range_inside_the_size_is_accepted(self) -> None:
        size = 3
        accepted: set[tuple[int, int]] = set()
        for start in range(-1, size + 2):
            for end in range(-1, size + 2):
                try:
                    validate_export_range(start, end, size)
                except ValueError:
                    continue
                accepted.add((start, end))
        assert accepted == {(s, e) for s in range(size + 1) for e in range(s, size + 1)}

    @pytest.mark.parametrize(("start", "end", "size"), [(-1, 5, 10), (6, 5, 10), (0, 11, 10)])
    def test_a_range_outside_the_size_is_rejected(self, start: int, end: int, size: int) -> None:
        with pytest.raises(ValueError, match="is not inside"):
            validate_export_range(start, end, size)

    async def test_read_blocks_yields_range_relative_offsets_in_block_sized_reads(self) -> None:
        reads: list[tuple[int, int | None]] = []

        @faithful_to(ContentSource)
        class _Source:
            size = 100

            async def read(self, offset: int = 0, length: int | None = None) -> bytes:
                reads.append((offset, length))
                return b"x" * (length or 0)

        blocks = [(offset, len(data)) async for offset, data in read_blocks(_Source(), 10, 35, block=10)]
        assert blocks == [(0, 10), (10, 10), (20, 5)]
        assert reads == [(10, 10), (20, 10), (30, 5)]

    async def test_read_blocks_advances_by_the_requested_length_when_a_read_comes_back_short(self) -> None:
        @faithful_to(ContentSource)
        class _Source:
            size = 100

            async def read(self, offset: int = 0, length: int | None = None) -> bytes:
                return b"x"

        assert [offset async for offset, _ in read_blocks(_Source(), 0, 25, block=10)] == [0, 10, 20]


class TestValidateReadArgs:
    def test_accepts_non_negative_arguments(self) -> None:
        accepted: set[tuple[int, int | None]] = set()
        for offset in (-2, -1, 0, 5):
            for length in (-2, -1, 0, 10, None):
                try:
                    validate_read_args(offset, length)
                except ValueError:
                    continue
                accepted.add((offset, length))
        assert accepted == {(offset, length) for offset in (0, 5) for length in (0, 10, None)}

    @pytest.mark.parametrize(("offset", "length"), [(-1, None), (0, -1), (-2, -2)])
    def test_rejects_a_negative_argument(self, offset: int, length: int | None) -> None:
        with pytest.raises(ValueError, match=r"read\(offset=.*\): offset/length must be non-negative"):
            validate_read_args(offset, length)


class TestByteRangeView:
    async def test_base_and_offset_properties(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(24576, 4 * 4096)
        assert view.base is dedup_file
        assert view.offset == 24576

    async def test_read_translates_coordinates(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(24576, 4 * 4096)  # the repeated-template DATA region
        assert view.size == 4 * 4096
        assert await view.read(0, 4096) == CHUNK_PLAINTEXTS[3]
        assert await view.read(4096, 4096) == CHUNK_PLAINTEXTS[4]

    async def test_read_default_length_reads_to_view_end(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 4096)
        assert await view.read(0) == CHUNK_PLAINTEXTS[0]

    async def test_over_length_read_clamps_instead_of_raising(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 100)
        assert await view.read(50, 100) == await view.read(50, 50)

    async def test_read_starting_at_or_past_view_end_returns_empty(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 100)
        assert await view.read(100, 10) == b""
        assert await view.read(150, 10) == b""

    async def test_negative_offset_or_length_still_raises(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 100)
        with pytest.raises(ValueError, match="non-negative"):
            await view.read(-1)
        with pytest.raises(ValueError, match="non-negative"):
            await view.read(0, -1)

    async def test_stream_reassembles_correctly(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 12288)  # exactly the first DATA region
        reassembled = b"".join([chunk async for _offset, chunk in view.stream(block=1000)])
        assert reassembled == b"".join(CHUNK_PLAINTEXTS[0:3])

    async def test_stream_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        file, store = build_blocking_dedup_file(tmp_path)
        view = file.view(0, 12288)

        async def consume() -> list[int]:
            store.armed = True
            return [offset async for offset, _chunk in view.stream(block=1000)]

        task = asyncio.create_task(consume())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_export_to_writes_the_view_window_only(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        view = dedup_file.view(24576, 4 * 4096)
        dst = tmp_path / "view.bin"
        result = await _export_to(view, dst, sparse=False)
        assert dst.stat().st_size == 4 * 4096
        assert dst.read_bytes() == await view.read(0, 4 * 4096)
        assert result.logical_size == 4 * 4096
        assert result.bytes_written == 4 * 4096

    async def test_export_to_clips_a_window_ending_mid_chunk(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        """A view ending mid-chunk is clipped to its length, as in ``TestExportTo``."""
        view = dedup_file.view(24576, 4 * 4096 - 100)
        dst = tmp_path / "view.bin"
        result = await _export_to(view, dst)
        assert dst.stat().st_size == 4 * 4096 - 100
        assert dst.read_bytes() == await view.read(0, 4 * 4096 - 100)
        assert result.bytes_written == 4 * 4096 - 100


class TestBinarySearchPerformance:
    async def test_read_near_the_end_of_a_large_map_does_not_linear_scan(self, tmp_path: Path) -> None:
        # With 2000 one-chunk MAPPING records, a read near the end must resolve by
        # binary search, bounded via a store read-count ceiling.
        n = 2000
        parts = []
        for i in range(n):
            # Every record points at (bucket 0, chunk 0); only file_offset varies.
            addr = chunk_address(0, 0, 0).to_int()
            parts.append(
                chunk_map_record_bytes(
                    kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=i, addr_int=addr, tail_u32=(1 << 16)
                )
            )
        record_bytes = record_head_bytes(map_num=n) + b"".join(parts)
        comp_path = tmp_path / "Composition" / str(STREAM_ID) / f"{SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(composition_header_bytes() + record_bytes)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", [bytes([0]) * 4096] * 1)

        store = LocalFsStore(tmp_path)
        counting_store = CountingStore(store)
        dir_cache = DirCache(counting_store)
        comp_reader = CompositionReader(counting_store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(counting_store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=n * 4096)

        target_offset = (n - 1) * 4096
        reads_before = counting_store.read_count
        await file.read(target_offset, 4096)
        reads_after = counting_store.read_count
        assert reads_after - reads_before < 25
