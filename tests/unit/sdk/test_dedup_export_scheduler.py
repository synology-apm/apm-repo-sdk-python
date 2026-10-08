"""Unit tests for ``synology_apm_repo.sdk.dedup.export_scheduler`` over
synthetic composition + Pool data written to real files.

``_naive_export_to`` is this file's correctness oracle: an independent
one-chunk-at-a-time walk that never groups ``DATA`` chunks by bucket.

Chunk-map CRC validation is ``verify``'s job, not export's, and is not
covered here.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import sys
from collections.abc import Awaitable, Callable
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import pytest

from support.fakes import faithful_to
from support.format_builders import (
    mapping_record,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
)
from support.store_fakes import BlockingStore, WrappingStore
from synology_apm_repo.sdk.api import export as api_export_mod
from synology_apm_repo.sdk.dedup import export_scheduler as export_scheduler_mod
from synology_apm_repo.sdk.dedup import export_sink as export_sink_mod
from synology_apm_repo.sdk.dedup import local_file_sink as local_file_sink_mod
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView, DedupFile
from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning, _ExportAccounting, export_to_writer
from synology_apm_repo.sdk.dedup.export_sink import AbortOutcome, OffsetWriter, RandomAccessExportSink, run_sink_export
from synology_apm_repo.sdk.dedup.export_workers import ExportExecutor
from synology_apm_repo.sdk.dedup.extent import ExportResult, ExtentKind
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileDescriptor, LocalFileSink
from synology_apm_repo.sdk.dedup.pool import BucketReaderCache, Pool
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.errors import ApmRepoError, DataCorruptError, WorkerProcessError
from synology_apm_repo.sdk.export import run_export
from synology_apm_repo.sdk.format.const import FIXED_CHUNK_LENGTH
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.dedup_export_fakes import (
    CHUNK_PLAINTEXTS,
    HEAD_OFF,
    SESSION_ID,
    SIZE,
    STREAM_ID,
    SegmentCollector,
    build_blocking_dedup_file,
    dedup_file_at,
    export_to,
    standard_entries,
)

_O_BINARY = getattr(os, "O_BINARY", 0)


@pytest.fixture(autouse=True)
def _workers_for_any_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    """The synthetic files here touch one or two bucket groups, far below ``_MIN_WORKER_GROUPS``; the tests of
    the worker path need the workers anyway (``TestWorkersNeedManyBucketGroups`` sets its own threshold)."""
    monkeypatch.setattr(export_scheduler_mod, "_MIN_WORKER_GROUPS", 1)


def _oracle_pwrite(fd: int, data: bytes, offset: int) -> None:
    """The oracle's own positional write, kept separate from the production
    helper so the oracle stays independent of the code under test."""
    if sys.platform != "win32":
        os.pwrite(fd, data, offset)
    else:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


_BUCKET_1_PLAINTEXTS = [bytes([100 + i]) * 4096 for i in range(2)]  # bucket 1, chunks 0..1


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    return dedup_file_at(tmp_path)


def _recording_progress(calls: list[int]) -> Callable[[int], Awaitable[None]]:
    """An ``async`` progress callback that appends each reported byte count to ``calls``."""

    async def _progress(written: int) -> None:
        calls.append(written)

    return _progress


async def _naive_export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool = True,
    progress: Callable[[int], Awaitable[None]] | None = None,
) -> ExportResult:
    """Walks extents in logical order, fetching one chunk at a time via
    ``pool.read_chunk()`` with no bucket grouping or merged reads."""
    if isinstance(file_like, ByteRangeView):
        base = file_like._base
        pool = base._pool
        window_start = file_like._offset
        size = file_like.size
    else:
        base = file_like
        pool = file_like._pool
        window_start = 0
        if file_like.size is None:
            raise ValueError("exporting requires a known size")
        size = file_like.size

    window_end = window_start + size
    holes = zeros = bytes_written = 0
    # A raw fd avoids a blocking Path method across the awaits (ASYNC230/ASYNC240).
    fd = os.open(dst, os.O_WRONLY | _O_BINARY | os.O_CREAT | os.O_TRUNC)
    try:
        os.ftruncate(fd, size)
        async for extent in base._extents(window_start, window_end):
            seg_start = max(extent.offset, window_start)
            seg_end = min(extent.end, window_end)
            if seg_end <= seg_start:
                continue
            length = seg_end - seg_start
            local_off = seg_start - window_start
            if extent.kind is ExtentKind.HOLE:
                holes += length
                if not sparse:
                    _oracle_pwrite(fd, bytes(length), local_off)
                continue
            if extent.kind is ExtentKind.ZERO:
                zeros += length
                if not sparse:
                    _oracle_pwrite(fd, bytes(length), local_off)
                continue
            assert extent.map_num > 0
            first_k = (seg_start - extent.offset) // FIXED_CHUNK_LENGTH
            last_k = (seg_end - 1 - extent.offset) // FIXED_CHUNK_LENGTH
            buf = bytearray(length)
            for k in range(first_k, last_k + 1):
                addr_k = extent.addr.advance(k % extent.map_num)
                chunk = await pool.read_chunk(addr_k)
                chunk_start = extent.offset + k * FIXED_CHUNK_LENGTH
                lo = max(seg_start, chunk_start) - chunk_start
                hi = min(seg_end, chunk_start + FIXED_CHUNK_LENGTH) - chunk_start
                dest = max(seg_start, chunk_start) - seg_start
                buf[dest : dest + (hi - lo)] = chunk[lo:hi]
            _oracle_pwrite(fd, bytes(buf), local_off)
            bytes_written += length
            if progress is not None:
                await progress(length)
    finally:
        os.close(fd)
    return ExportResult(bytes_written=bytes_written, logical_size=size, holes=holes, zeros=zeros)


class TestCorrectness:
    async def test_matches_the_naive_oracle_byte_for_byte(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(dedup_file, naive_dst, sparse=True)
        r_production = await export_to(dedup_file, production_dst, sparse=True)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_non_sparse(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(dedup_file, naive_dst, sparse=False)
        await export_to(dedup_file, production_dst, sparse=False)
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    async def test_matches_the_naive_oracle_for_a_byte_range_view(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        view = dedup_file.view(24576, 4 * 4096)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(view, naive_dst, sparse=True)
        r_production = await export_to(view, production_dst, sparse=True)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_for_a_byte_range_view_starting_mid_record(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        """A chunk-aligned view may start mid-record: the first ``DATA`` record
        spans 3 chunks and this view starts at chunk 1, so the leading chunk
        must be skipped."""
        view = dedup_file.view(4096, 8192)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(view, naive_dst, sparse=False)
        r_production = await export_to(view, production_dst, sparse=False)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_for_a_size_that_ends_mid_chunk(self, tmp_path: Path) -> None:
        truncated_size = 40960 - 100
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=truncated_size)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(file, production_dst)
        assert naive_dst.stat().st_size == truncated_size
        assert production_dst.stat().st_size == truncated_size
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_for_a_size_that_ends_mid_zero_extent(self, tmp_path: Path) -> None:
        """A size ending inside a ``ZERO`` extent (``[12288, 20480)``) clips
        that extent; ``truncated_size`` ends 2 bytes into it."""
        truncated_size = 12288 + 2
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=truncated_size)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst, sparse=False)
        r_production = await export_to(file, production_dst, sparse=False)
        assert naive_dst.stat().st_size == truncated_size
        assert production_dst.stat().st_size == truncated_size
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production
        assert r_production.zeros == 2

    async def test_multiple_buckets_are_all_visited_correctly(self, tmp_path: Path) -> None:
        # bucket 0: chunks 0,1 at [0,8192); bucket 1: chunks 0,1 at [8192,16384)
        entries = mapping_record(0, 0, 0, map_num=2) + mapping_record(8192, 1, 0, map_num=2)
        write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
        # Bucket 1's file is "Pool/0/1.buk": small bucket ids stay under the stream's "0" directory.
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=16384)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert production_dst.read_bytes() == CHUNK_PLAINTEXTS[0] + CHUNK_PLAINTEXTS[1] + b"".join(_BUCKET_1_PLAINTEXTS)


class TestBehavior:
    async def test_reports_progress(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        calls: list[int] = []
        await export_to(
            dedup_file,
            tmp_path / "out.bin",
            progress=_recording_progress(calls),
        )
        assert calls == [3 * 4096 + 4 * 4096]  # one bucket group, reported once

    async def test_is_cancellable_via_its_surrounding_task_and_closes_its_output_fd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancelling the export's task propagates ``CancelledError`` and
        closes the destination fd the sink opened."""
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

        async def _arm_on_first_progress(written: int) -> None:
            store.armed = True

        async def do_export() -> object:
            return await export_to(file, dst, progress=_arm_on_first_progress, tuning=ExportTuning(window_entries=1))

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(opened_fds) == 1  # the export really did get as far as opening its destination
        assert closed_fds == opened_fds  # ...and the cancelled export still closed it
        assert dst.stat().st_size == SIZE  # the pre-sized partial output is kept

    async def test_requires_known_size_for_a_plain_dedup_file(self, tmp_path: Path) -> None:
        write_composition_entries(
            tmp_path / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID
        )
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await export_to(unsized, tmp_path / "out.bin")

    async def test_rejects_a_byte_range_view_with_a_non_chunk_aligned_offset(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        """A view with a non-chunk-aligned offset raises ``ValueError`` rather
        than corrupting the output."""
        view = dedup_file.view(100, 4096)
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            await export_to(view, tmp_path / "out.bin")


class TestWindowing:
    """``window_entries`` through ``export_to``, including cross-window
    progress accumulation."""

    @pytest.mark.parametrize("window_entries", [1, 2, 3, 7, 1_000_000])
    async def test_matches_the_naive_oracle_regardless_of_window_size(
        self, dedup_file: DedupFile, tmp_path: Path, window_entries: int
    ) -> None:
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(dedup_file, naive_dst, sparse=True)
        r_production = await export_to(
            dedup_file, production_dst, sparse=True, tuning=ExportTuning(window_entries=window_entries)
        )
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_multiple_buckets_still_matches_the_naive_oracle_with_a_tiny_window(self, tmp_path: Path) -> None:
        # window_entries=1 forces a window boundary between the two buckets' chunks.
        entries = mapping_record(0, 0, 0, map_num=2) + mapping_record(8192, 1, 0, map_num=2)
        write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=16384)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst, tuning=ExportTuning(window_entries=1))
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    async def test_holes_and_zeros_are_summed_across_windows_not_just_the_last_ones(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        default_result = await export_to(dedup_file, tmp_path / "default.bin", sparse=False)
        windowed_result = await export_to(
            dedup_file, tmp_path / "windowed.bin", sparse=False, tuning=ExportTuning(window_entries=1)
        )
        assert windowed_result.holes == default_result.holes
        assert windowed_result.zeros == default_result.zeros

    async def test_progress_reports_each_windows_data_bytes_once(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        calls: list[int] = []
        await export_to(
            dedup_file, tmp_path / "out.bin", progress=_recording_progress(calls), tuning=ExportTuning(window_entries=1)
        )
        assert len(calls) > 1  # one report per window
        assert all(written > 0 for written in calls)
        assert sum(calls) == 3 * 4096 + 4 * 4096


@faithful_to(RandomAccessExportSink)
class _NoWriteSink:
    """An ``ExportWriter`` whose writes go nowhere — for exercising
    ``_ExportAccounting`` in isolation."""

    caps = export_sink_mod.SinkCaps()
    preallocated = False

    async def open(self, logical_size: int, *, sparse: bool) -> None: ...

    async def write_at(self, offset: int, data: bytes | memoryview) -> None: ...

    async def write_zero(self, offset: int, length: int) -> None: ...

    def worker_target(self) -> None:
        return None

    def note_worker_write(self) -> None: ...

    async def commit(self) -> None: ...

    async def abort(self) -> export_sink_mod.AbortOutcome:
        return export_sink_mod.AbortOutcome(kept=False, ever_written=False)


class TestPipelinedWriter:
    """How a ``LocalFileSink`` writer-thread failure reaches the caller, and
    ``_ExportAccounting``'s bookkeeping."""

    async def test_writer_thread_failure_propagates_to_the_caller(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A writer-thread failure surfaces as an exception from ``export_to_writer()``.

        Uses an unarmed ``BlockingStore`` file: ``dedup_file`` would take the
        multiprocess path, which this process's patched ``pwrite`` cannot reach."""
        file, _store = build_blocking_dedup_file(tmp_path)

        def _failing_pwrite(fd: int, data: bytes, offset: int) -> int:
            raise OSError("synthetic disk-full for this test")

        monkeypatch.setattr(local_file_sink_mod, "pwrite", _failing_pwrite)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await export_to(file, tmp_path / "out.bin")

    async def test_writer_thread_failure_during_cancellation_does_not_mask_the_cancellation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A writer-thread failure during cancellation must not replace the
        ``CancelledError``."""
        file, store = build_blocking_dedup_file(tmp_path)
        dst = tmp_path / "out.bin"

        def _failing_pwrite(fd: int, data: bytes, offset: int) -> int:
            raise OSError("synthetic writer failure racing the cancellation")

        monkeypatch.setattr(local_file_sink_mod, "pwrite", _failing_pwrite)

        async def _arm_on_first_progress(written: int) -> None:
            store.armed = True

        async def do_export() -> object:
            return await export_to(file, dst, progress=_arm_on_first_progress, tuning=ExportTuning(window_entries=1))

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_accounting_counts_external_bytes_and_reports_progress(self) -> None:
        """``record_external_bytes`` updates ``bytes_written`` and progress
        without a sink write."""
        calls: list[int] = []
        accounting = _ExportAccounting(_NoWriteSink(), write_zeros=False, progress=_recording_progress(calls))
        await accounting.record_external_bytes(40)
        await accounting.record_external_bytes(10)
        assert accounting.bytes_written == 50
        assert calls == [40, 10]

    async def test_accounting_writes_gaps_only_when_asked_to(self) -> None:
        recorded: list[tuple[int, int]] = []

        class _Sink(_NoWriteSink):
            async def write_zero(self, offset: int, length: int) -> None:
                recorded.append((offset, length))

        skipping = _ExportAccounting(_Sink(), write_zeros=False, progress=None)
        await skipping.write_gap(8, 4)
        writing = _ExportAccounting(_Sink(), write_zeros=True, progress=None)
        await writing.write_gap(16, 4)
        assert recorded == [(16, 4)]


class _FailingStore(WrappingStore):
    """An ``ObjectStore`` wrapper whose ``read()`` raises for paths ending in
    ``fail_path_suffix``."""

    def __init__(self, backing: LocalFsStore, *, fail_path_suffix: str) -> None:
        super().__init__(backing)
        self._fail_path_suffix = fail_path_suffix

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if path.endswith(self._fail_path_suffix):
            raise DataCorruptError(f"synthetic failure for {path!r}", ref=path)
        return await self._backing.read(path, offset, length)


def _two_bucket_file(tmp_path: Path, *, in_process: bool = False) -> DedupFile:
    """Two independent buckets sharing one composition, the minimal fixture
    with something to run concurrently across; ``in_process`` wraps the store
    (an unarmed ``BlockingStore``) so ``export_to`` skips the multiprocess path."""
    entries = mapping_record(0, 0, 0, map_num=2) + mapping_record(8192, 1, 0, map_num=2)
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
    write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
    store: ObjectStore = BlockingStore(LocalFsStore(tmp_path)) if in_process else LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=16384)


class _ProbeStore(WrappingStore):
    """An ``ObjectStore`` wrapper that records the peak number of overlapping
    ``read()`` calls; non-describable, so ``export_to`` stays in-process.
    A bucket read parks until ``overlap`` reads are in flight at once."""

    def __init__(self, backing: LocalFsStore, overlap: int) -> None:
        super().__init__(backing)
        self._inflight = 0
        self.peak = 0
        self._overlap = overlap
        self._overlapped = asyncio.Event()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self._inflight += 1
        self.peak = max(self.peak, self._inflight)
        if self._inflight >= self._overlap:
            self._overlapped.set()
        try:
            if path.startswith("Pool/"):
                await self._overlapped.wait()
            return await self._backing.read(path, offset, length)
        finally:
            self._inflight -= 1


async def _peak_reads(tmp_path: Path, tuning: ExportTuning, *, overlap: int) -> int:
    """Peak overlapping store reads while exporting eight single-chunk
    buckets, with bucket reads held until ``overlap`` of them overlap."""
    tmp_path.mkdir()
    num_buckets = 8
    entries = b"".join(mapping_record(b * 4096, b, 0, map_num=1) for b in range(num_buckets))
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    for b in range(num_buckets):
        write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", [bytes([b]) * 4096])
    store = _ProbeStore(LocalFsStore(tmp_path), overlap)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    file = DedupFile(comp_reader, pool, HEAD_OFF, size=num_buckets * 4096)
    await asyncio.wait_for(export_to(file, tmp_path / "out.bin", tuning=tuning), 10)
    return store.peak


class TestParallel:
    """Bucket-group concurrency through ``export_to``: the first two tests use a
    describable store (the multiprocess path), the rest ``max_concurrent_reads``
    in-process."""

    async def test_matches_the_naive_oracle_across_multiple_bucket_groups(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst)
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    async def test_combined_with_a_tiny_window_still_matches_the_naive_oracle(self, tmp_path: Path) -> None:
        # Windowing and bucket-major grouping interact at a window boundary.
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst, tuning=ExportTuning(window_entries=1))
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    @pytest.mark.parametrize("max_concurrent_reads", [2, 8])
    async def test_max_concurrent_reads_matches_the_naive_oracle(
        self, tmp_path: Path, max_concurrent_reads: int
    ) -> None:
        file = _two_bucket_file(tmp_path, in_process=True)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(
            file, production_dst, tuning=ExportTuning(max_concurrent_reads=max_concurrent_reads)
        )
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_max_concurrent_reads_progress_adds_up_to_the_bytes_written(self, tmp_path: Path) -> None:
        """Concurrent bucket groups' reports add up to exactly the bytes written."""
        file = _two_bucket_file(tmp_path, in_process=True)
        calls: list[int] = []
        result = await export_to(
            file, tmp_path / "out.bin", progress=_recording_progress(calls), tuning=ExportTuning(max_concurrent_reads=8)
        )
        # Concurrent groups report in completion order; the multiset is fixed.
        assert sorted(calls) == [8192, 8192]
        assert sum(calls) == result.bytes_written

    async def test_max_concurrent_reads_bounds_and_allows_overlapping_reads(self, tmp_path: Path) -> None:
        """Peak overlapping reads is 1 serially and capped at the setting otherwise."""
        serial = await _peak_reads(
            tmp_path / "serial", ExportTuning(max_concurrent_reads=1, max_concurrent_opens=1), overlap=1
        )
        capped = await _peak_reads(
            tmp_path / "capped", ExportTuning(max_concurrent_reads=3, max_concurrent_opens=1), overlap=3
        )
        assert serial == 1
        assert capped == 3

    async def test_max_concurrent_reads_surfaces_a_bucket_failure_as_an_exception_group(self, tmp_path: Path) -> None:
        """Unlike ``max_concurrent_reads=1``, a concurrent bucket-group failure
        surfaces as an ``ExceptionGroup``."""
        entries = mapping_record(0, 0, 0, map_num=2) + mapping_record(8192, 1, 0, map_num=2)
        write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = _FailingStore(LocalFsStore(tmp_path), fail_path_suffix="1.buk")
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=16384)

        with pytest.raises(ExceptionGroup, match="unhandled errors in a TaskGroup") as excinfo:
            await export_to(file, tmp_path / "out.bin", tuning=ExportTuning(max_concurrent_reads=8))
        assert any(isinstance(exc, DataCorruptError) for exc in excinfo.value.exceptions)


class TestWorkersNeedManyBucketGroups:
    """Each bucket group is one worker task, so a window touching few groups doesn't repay a pool's start-up:
    it runs in-process until a window touches ``_MIN_WORKER_GROUPS`` groups."""

    @staticmethod
    def _spy(monkeypatch: pytest.MonkeyPatch, threshold: int) -> tuple[list[ExportExecutor], list[int]]:
        monkeypatch.setattr(export_scheduler_mod, "_MIN_WORKER_GROUPS", threshold)
        built: list[ExportExecutor] = []
        dispatched: list[int] = []

        class _Counting(ExportExecutor):
            def __init__(self, pool_descriptor: PoolDescriptor, sink_descriptor: Any) -> None:
                super().__init__(pool_descriptor, sink_descriptor)
                built.append(self)

        real_dispatch = export_scheduler_mod._dispatch_window_multiprocess

        async def _dispatch(plan: Any, **kwargs: Any) -> None:
            dispatched.append(len(plan.groups))
            await real_dispatch(plan, **kwargs)

        monkeypatch.setattr(export_scheduler_mod, "ExportExecutor", _Counting)
        monkeypatch.setattr(export_scheduler_mod, "_dispatch_window_multiprocess", _dispatch)
        return built, dispatched

    async def test_one_bucket_group_stays_in_process_and_is_still_correct(
        self, dedup_file: DedupFile, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        built, dispatched = self._spy(monkeypatch, 2)
        naive_dst, dst = tmp_path / "naive.bin", tmp_path / "out.bin"
        await _naive_export_to(dedup_file, naive_dst)

        await export_to(dedup_file, dst)

        assert (built, dispatched) == ([], [])
        assert dst.read_bytes() == naive_dst.read_bytes()

    async def test_enough_groups_build_the_pool_and_dispatch_to_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        file = _two_bucket_file(tmp_path)
        built, dispatched = self._spy(monkeypatch, 2)
        naive_dst, dst = tmp_path / "naive.bin", tmp_path / "out.bin"
        await _naive_export_to(file, naive_dst)

        await export_to(file, dst)

        assert (len(built), dispatched) == (1, [2])
        assert dst.read_bytes() == naive_dst.read_bytes()

    async def test_one_group_too_few_stays_in_process(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        file = _two_bucket_file(tmp_path)
        built, dispatched = self._spy(monkeypatch, 3)

        await export_to(file, tmp_path / "out.bin")

        assert (built, dispatched) == ([], [])

    async def test_a_pool_built_by_one_window_serves_the_later_windows_too(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        file = _two_bucket_file(tmp_path)
        built, dispatched = self._spy(monkeypatch, 2)
        naive_dst, dst = tmp_path / "naive.bin", tmp_path / "out.bin"
        await _naive_export_to(file, naive_dst)

        # Three chunk entries per window: the first window touches both buckets, the second only one.
        await export_to(file, dst, tuning=ExportTuning(window_entries=3))

        assert (len(built), dispatched) == (1, [2, 1])
        assert dst.read_bytes() == naive_dst.read_bytes()

    async def test_windows_that_each_touch_few_groups_never_build_a_pool(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        file = _two_bucket_file(tmp_path)
        built, dispatched = self._spy(monkeypatch, 2)

        await export_to(file, tmp_path / "out.bin", tuning=ExportTuning(window_entries=1))

        assert (built, dispatched) == ([], [])

    async def test_a_caller_supplied_executor_is_used_whatever_the_group_count(
        self, dedup_file: DedupFile, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._spy(monkeypatch, 99)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        pool_descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert pool_descriptor is not None
        executor = ExportExecutor(pool_descriptor, LocalFileDescriptor(str(dst)))
        try:
            with pytest.raises(BrokenProcessPool, match="terminated abruptly"):
                executor.process_pool.submit(os._exit, 1).result()
            with pytest.raises(
                WorkerProcessError, match="export worker process died unexpectedly"
            ):  # the dead pool is what fails: the executor was used, not skipped
                await export_to(dedup_file, dst, tuning=ExportTuning(executor=executor))
        finally:
            await executor.close()


class TestMultiprocessFailure:
    """A worker process's failure reaches the caller as itself, not wrapped in the
    ``ExceptionGroup`` its ``TaskGroup`` raises."""

    async def test_a_corrupt_bucket_surfaces_as_its_own_error(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)  # a describable store, so the multiprocess path
        (tmp_path / "Pool" / "0" / "1.buk").write_bytes(b"not a bucket" * 8)

        with pytest.raises(ApmRepoError, match="bad magic") as excinfo:
            await export_to(file, tmp_path / "out.bin")

        assert not isinstance(excinfo.value, BaseExceptionGroup)

    async def test_the_sink_is_aborted_and_keeps_what_workers_may_have_written(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        (tmp_path / "Pool" / "0" / "1.buk").write_bytes(b"not a bucket" * 8)
        dst = tmp_path / "out.bin"

        with pytest.raises(ApmRepoError, match="bad magic"):
            await export_to(file, dst)

        assert dst.exists()  # unstaged, and the sink was told workers may write, so abort keeps the file


class TestWorkerProcessFailure:
    """What a dead or cancelled worker pool looks like from ``export_to_writer``."""

    async def test_a_worker_process_that_dies_raises_worker_process_error(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")  # the worker's initializer opens its destination
        pool_descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert pool_descriptor is not None
        executor = ExportExecutor(pool_descriptor, LocalFileDescriptor(str(dst)))
        try:
            with pytest.raises(BrokenProcessPool, match="terminated abruptly"):
                executor.process_pool.submit(os._exit, 1).result()  # kills the one worker the pool spawned

            with pytest.raises(WorkerProcessError, match="worker process died") as excinfo:
                await export_to(dedup_file, dst, tuning=ExportTuning(executor=executor))

            assert isinstance(excinfo.value.__cause__, BrokenProcessPool)
        finally:
            await executor.close()

    async def test_cancelling_while_workers_run_aborts_the_sink_and_reaps_the_workers(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)  # a describable store, so the multiprocess path
        dst = tmp_path / "out.bin"

        async def cancel_the_export(written: int) -> None:
            task.cancel()

        task = asyncio.create_task(
            export_to(file, dst, tuning=ExportTuning(window_entries=1), progress=cancel_the_export)
        )
        with pytest.raises(asyncio.CancelledError):
            await task

        assert multiprocessing.active_children() == []  # the owned executor was shut down and joined


class TestPrefetchOpens:
    """``max_concurrent_opens`` through ``export_to``, in-process."""

    @pytest.mark.parametrize("max_concurrent_opens", [2, 8])
    async def test_matches_the_naive_oracle(self, tmp_path: Path, max_concurrent_opens: int) -> None:
        file = _two_bucket_file(tmp_path, in_process=True)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(
            file, production_dst, tuning=ExportTuning(max_concurrent_opens=max_concurrent_opens)
        )
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_combined_with_max_concurrent_reads_still_matches_the_naive_oracle(self, tmp_path: Path) -> None:
        """The two knobs are independent; combined they still match the oracle."""
        file = _two_bucket_file(tmp_path, in_process=True)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(
            file, production_dst, tuning=ExportTuning(max_concurrent_reads=8, max_concurrent_opens=8)
        )
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_max_concurrent_opens_bounds_and_allows_overlapping_opens(self, tmp_path: Path) -> None:
        """With serial reads, peak overlapping reads is 1 without prefetch and
        capped at the setting with it."""
        off = await _peak_reads(
            tmp_path / "off", ExportTuning(max_concurrent_reads=1, max_concurrent_opens=1), overlap=1
        )
        capped = await _peak_reads(
            tmp_path / "capped", ExportTuning(max_concurrent_reads=1, max_concurrent_opens=3), overlap=3
        )
        assert off == 1
        assert 3 <= capped <= 3 + 1  # the prefetch opens, plus the serial main loop's own read

    async def test_a_bucket_failure_is_not_wrapped_in_an_exception_group_at_the_serial_default(
        self, tmp_path: Path
    ) -> None:
        """``max_concurrent_opens`` alone (serial reads) leaves a failure's
        exception type unchanged."""
        entries = mapping_record(0, 0, 0, map_num=2) + mapping_record(8192, 1, 0, map_num=2)
        write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS[:2])
        write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = _FailingStore(LocalFsStore(tmp_path), fail_path_suffix="1.buk")
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, HEAD_OFF, size=16384)

        with pytest.raises(DataCorruptError, match="synthetic failure for"):
            await export_to(file, tmp_path / "out.bin", tuning=ExportTuning(max_concurrent_opens=8))


def _build_many_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> tuple[DedupFile, Pool]:
    """``num_buckets`` single-chunk buckets sharing one inspectable ``Pool``
    (more than 16 exceeds its default bucket cache).

    Wrapped in an unarmed ``BlockingStore`` so ``export_to`` takes the
    single-process path, the only one where ``BucketReaderCache`` identity
    and reuse are observable."""
    entries = b"".join(mapping_record(b * 4096, b, 0, map_num=1) for b in range(num_buckets))
    write_composition_entries(tmp_path / "Composition", entries, session_id=SESSION_ID, stream_id=STREAM_ID)
    for b in range(num_buckets):
        write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", [bytes([b % 256]) * 4096])
    store = BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=num_buckets * 4096), pool


class TestBucketReaderCache:
    """Export uses its own ``BucketReaderCache``, never ``Pool``'s shared
    ``_buckets``, and reuses it across calls that share one instance."""

    async def test_export_never_writes_into_pool_shared_bucket_cache(self, tmp_path: Path) -> None:
        # 20 buckets exceed the 16-slot cache, so any write into it would evict.
        file, pool = _build_many_bucket_dedup_file(tmp_path, 20)

        # Warm 5 buckets through Pool's own cache.
        for b in range(5):
            await pool.bucket(StreamId(0), BucketId(b))
        before = dict(pool._buckets)
        assert set(before) == {(0, b) for b in range(5)}

        await export_to(file, tmp_path / "out.bin")

        after = dict(pool._buckets)
        assert after == before  # completely untouched by the export that followed

    async def test_standalone_export_leaves_the_shared_cache_empty(self, tmp_path: Path) -> None:
        file, pool = _build_many_bucket_dedup_file(tmp_path, 20)
        await export_to(file, tmp_path / "out.bin")
        assert dict(pool._buckets) == {}

    async def test_sharing_one_export_cache_across_two_calls_reuses_the_same_bucket(self, tmp_path: Path) -> None:
        file, _pool = _build_many_bucket_dedup_file(tmp_path, 3)
        cache = BucketReaderCache()

        await export_to(file, tmp_path / "out1.bin", tuning=ExportTuning(export_cache=cache))
        assert len(cache) == 3
        opened_once = dict(cache)

        # Identity, not just equal keys: a close-and-reopen would also fail.
        await export_to(file, tmp_path / "out2.bin", tuning=ExportTuning(export_cache=cache))
        assert dict(cache).keys() == opened_once.keys()
        for key, reader in opened_once.items():
            assert cache[key] is reader


class TestOffsetWriter:
    """``export_to_writer`` into a caller-opened sink: ``OffsetWriter`` shifts
    writes (and, on the multiprocess path, ``ExportGroupWorkerArgs.dst_offset``)
    without changing where the export reads from, and only the multiprocess
    path notes a worker write. ``dedup_file`` is describable, so a sink with a
    ``worker_target`` takes the multiprocess path."""

    async def test_offset_shifts_output_within_a_larger_destination(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        dst = tmp_path / "combined.bin"
        shift = 100_000
        sink = LocalFileSink(dst, staged=False)
        await sink.open(shift + SIZE, sparse=False)
        result = await export_to_writer(dedup_file, OffsetWriter(sink, shift), sparse=False)
        await sink.commit()

        naive_dst = tmp_path / "naive.bin"
        await _naive_export_to(dedup_file, naive_dst, sparse=False)
        combined = dst.read_bytes()
        assert combined[:shift] == bytes(shift)  # untouched prefix stays exactly as created
        assert combined[shift : shift + SIZE] == naive_dst.read_bytes()
        assert result.bytes_written == 3 * 4096 + 4 * 4096

    async def test_the_multiprocess_path_notes_a_worker_write(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        events: list[str] = []

        class _NotingSink(LocalFileSink):
            def note_worker_write(self) -> None:
                events.append("note")
                super().note_worker_write()

        sink = _NotingSink(tmp_path / "out.bin", staged=False)
        await sink.open(SIZE, sparse=True)
        result = await export_to_writer(dedup_file, sink, sparse=True)
        await sink.commit()
        assert result.bytes_written > 0  # data really went to worker processes
        assert events == ["note"], "the multiprocess path never called note_worker_write"
        assert await sink.abort() == AbortOutcome(kept=True, ever_written=True)

    async def test_the_in_process_path_never_notes_a_worker_write(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        noted: list[bool] = []

        class _ParentOnlySink(LocalFileSink):
            def worker_target(self) -> None:  # type: ignore[override]
                return None

            def note_worker_write(self) -> None:
                noted.append(True)

        sink = _ParentOnlySink(tmp_path / "out.bin", staged=False)
        await sink.open(SIZE, sparse=True)
        await export_to_writer(dedup_file, sink, sparse=True)
        await sink.commit()
        assert not noted

    async def test_a_second_export_does_not_reset_the_first_ones_writes(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        """Two exports into one already-open sink at different offsets — the
        second must not disturb the first's bytes."""
        dst = tmp_path / "combined.bin"
        total = SIZE * 2
        sink = LocalFileSink(dst, staged=False)
        await sink.open(total, sparse=False)

        await export_to_writer(dedup_file, OffsetWriter(sink, 0), sparse=False)
        await sink.commit()  # drains the writer thread
        first_half = dst.read_bytes()[:SIZE]

        sink = LocalFileSink(dst, staged=False)
        await sink.open(total, sparse=False)  # reopening recreates the file; rewrite both halves in order
        await export_to_writer(dedup_file, OffsetWriter(sink, 0), sparse=False)
        await export_to_writer(dedup_file, OffsetWriter(sink, SIZE), sparse=False)
        await sink.commit()
        combined = dst.read_bytes()
        assert combined[:SIZE] == first_half
        assert combined[SIZE:] == first_half  # same fixture written again, this time shifted
        assert dst.stat().st_size == total

    async def test_export_to_still_creates_the_destination_at_its_logical_size(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * (SIZE * 2))  # pre-existing, oversized garbage
        await export_to(dedup_file, dst, sparse=False)
        assert dst.stat().st_size == SIZE


@faithful_to(RandomAccessExportSink)
class _MemorySink:
    """A single-writer, sector-aligned, non-file ``ExportWriter`` backed by a
    ``bytearray`` — stands in for a disk API that can't be written from
    worker processes."""

    def __init__(self, *, supports_sparse: bool, preallocated: bool = False) -> None:
        self.caps = export_sink_mod.SinkCaps(supports_sparse=supports_sparse)
        self.preallocated = preallocated
        self.buffer = bytearray()
        self.data_writes: list[tuple[int, int]] = []
        self.zero_writes: list[tuple[int, int]] = []

    async def open(self, logical_size: int, *, sparse: bool) -> None:
        self.buffer = bytearray(b"\xff" * logical_size)  # anything unwritten stays visibly non-zero

    async def write_at(self, offset: int, data: bytes | memoryview) -> None:
        self.data_writes.append((offset, len(data)))
        self.buffer[offset : offset + len(data)] = data

    async def write_zero(self, offset: int, length: int) -> None:
        self.zero_writes.append((offset, length))
        self.buffer[offset : offset + length] = bytes(length)

    def worker_target(self) -> None:
        return None

    def note_worker_write(self) -> None: ...

    async def commit(self) -> None: ...

    async def abort(self) -> export_sink_mod.AbortOutcome:
        return export_sink_mod.AbortOutcome(kept=False, ever_written=False)


class _CountingExecutor(ExportExecutor):
    built: ClassVar[list[ExportExecutor]] = []

    def __init__(self, pool_descriptor: PoolDescriptor, sink_descriptor: Any) -> None:
        super().__init__(pool_descriptor, sink_descriptor)
        _CountingExecutor.built.append(self)


class TestExportFragmentsToWriter:
    """One export of several ``(file, start, end)`` segments into one sink, sharing one executor
    and one reader cache where it can."""

    @pytest.fixture(autouse=True)
    def _count_executors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _CountingExecutor.built = []
        monkeypatch.setattr(export_scheduler_mod, "ExportExecutor", _CountingExecutor)

    @staticmethod
    async def _run(
        segments: list[tuple[DedupFile, int, int]],
        sink: LocalFileSink,
        *,
        progress: Callable[[int], Awaitable[None]] | None = None,
        tuning: ExportTuning | None = None,
    ) -> ExportResult:
        size = max(end for _, _, end in segments)
        return await run_sink_export(
            sink,
            size,
            sparse=True,
            body=lambda: export_scheduler_mod.export_fragments_to_writer(
                segments, sink, sparse=True, progress=progress, tuning=tuning
            ),
        )

    async def test_segments_sharing_a_pool_share_one_executor_and_land_at_their_offsets(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        dst = tmp_path / "out.bin"

        result = await self._run([(file, 0, 8192), (file, 8192, 16384)], LocalFileSink(dst, staged=False))

        assert len(_CountingExecutor.built) == 1
        assert dst.read_bytes() == await file.read(0, 16384)
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (16384, 16384, 0, 0)

    async def test_segments_of_different_pools_each_get_their_own_executor(self, tmp_path: Path) -> None:
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        file_a, file_b = _two_bucket_file(tmp_path / "a"), _two_bucket_file(tmp_path / "b")
        dst = tmp_path / "out.bin"

        await self._run([(file_a, 0, 8192), (file_b, 8192, 16384)], LocalFileSink(dst, staged=False))

        assert len(_CountingExecutor.built) == 2  # one per segment, built and closed by its own export
        assert dst.read_bytes() == await file_a.read(0, 8192) + await file_b.read(8192, 8192)

    async def test_a_sink_without_worker_writes_builds_no_executor(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        sink = _MemorySink(supports_sparse=True)
        await sink.open(16384, sparse=True)

        await export_scheduler_mod.export_fragments_to_writer([(file, 0, 8192), (file, 8192, 16384)], sink, sparse=True)

        assert _CountingExecutor.built == []
        assert bytes(sink.buffer) == await file.read(0, 16384)

    async def test_a_caller_supplied_executor_is_used_for_every_segment_and_left_open(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"")
        pool_descriptor = PoolDescriptor.from_pool(file.pool)
        assert pool_descriptor is not None
        supplied = ExportExecutor(pool_descriptor, LocalFileDescriptor(str(dst)))
        try:
            await self._run(
                [(file, 0, 8192), (file, 8192, 16384)],
                LocalFileSink(dst, staged=False),
                tuning=ExportTuning(executor=supplied),
            )

            assert _CountingExecutor.built == []  # none built on top of the supplied one
            assert dst.read_bytes() == await file.read(0, 16384)
            supplied.process_pool.submit(int).result()  # still usable: the export did not close it
        finally:
            await supplied.close()

    async def test_every_segment_shares_one_reader_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        file = _two_bucket_file(tmp_path)
        caches: list[BucketReaderCache | None] = []
        real = export_scheduler_mod.export_to_writer

        async def _spy(*args: Any, **kwargs: Any) -> ExportResult:
            caches.append(kwargs["tuning"].export_cache)
            return await real(*args, **kwargs)

        monkeypatch.setattr(export_scheduler_mod, "export_to_writer", _spy)

        await self._run([(file, 0, 8192), (file, 8192, 16384)], LocalFileSink(tmp_path / "out.bin", staged=False))

        assert len(caches) == 2 and caches[0] is not None and caches[0] is caches[1]

    async def test_progress_covers_every_segment(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        calls: list[int] = []
        await self._run(
            [(file, 0, 8192), (file, 8192, 16384)],
            LocalFileSink(tmp_path / "out.bin", staged=False),
            progress=_recording_progress(calls),
        )

        assert sum(calls) == 16384


class TestFragmentSpan:
    """``span`` makes the offsets range-relative and turns the gaps between fragments into holes."""

    @staticmethod
    async def _export(
        file: DedupFile,
        fragments: list[tuple[DedupFile, int, int]],
        span: tuple[int, int],
        *,
        sparse: bool,
        supports_sparse: bool = True,
    ) -> tuple[_MemorySink, ExportResult]:
        sink = _MemorySink(supports_sparse=supports_sparse)
        await sink.open(span[1] - span[0], sparse=sparse)
        result = await export_scheduler_mod.export_fragments_to_writer(fragments, sink, span=span, sparse=sparse)
        return sink, result

    async def test_offsets_are_relative_to_the_span_start_and_gaps_are_holes(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        sink, result = await self._export(file, [(file, 8192, 16384)], (8192, 20480), sparse=True)

        assert bytes(sink.buffer[:8192]) == await file.read(8192, 8192)
        assert bytes(sink.buffer[8192:]) == b"\xff" * 4096  # the gap is left unwritten for a sparse export
        assert sink.zero_writes == []
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (8192, 12288, 4096, 0)

    async def test_a_dense_export_zero_fills_the_leading_between_and_trailing_gaps(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        fragments = [(file, 4096, 8192), (file, 12288, 16384)]
        sink, result = await self._export(file, fragments, (0, 20480), sparse=False)

        assert sorted(sink.zero_writes) == [(0, 4096), (8192, 4096), (16384, 4096)]
        expected = bytes(4096) + await file.read(4096, 4096) + bytes(4096) + await file.read(12288, 4096) + bytes(4096)
        assert bytes(sink.buffer) == expected
        assert (result.logical_size, result.holes) == (20480, 12288)

    async def test_no_fragments_is_one_whole_hole(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        sink, result = await self._export(file, [], (4096, 12288), sparse=False)

        assert sink.zero_writes == [(0, 8192)]
        assert (result.bytes_written, result.logical_size, result.holes) == (0, 8192, 8192)

    async def test_a_preallocated_writer_needs_no_zero_fill(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        sink = _MemorySink(supports_sparse=True, preallocated=True)
        await sink.open(12288, sparse=False)

        await export_scheduler_mod.export_fragments_to_writer([(file, 0, 4096)], sink, span=(0, 12288), sparse=False)

        assert sink.zero_writes == []

    async def test_a_fragment_outside_the_span_is_rejected(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        sink = _MemorySink(supports_sparse=True)

        with pytest.raises(ValueError, match="inside span"):
            await export_scheduler_mod.export_fragments_to_writer(
                [(file, 0, 8192)], sink, span=(4096, 16384), sparse=True
            )


class TestGapsWithin:
    def test_a_fragment_nested_inside_an_earlier_wider_one_does_not_reopen_a_gap(self) -> None:
        fragments: list[tuple[DedupFile, int, int]] = [(cast(Any, None), 0, 100), (cast(Any, None), 10, 30)]
        fragments.append((cast(Any, None), 150, 200))
        assert export_scheduler_mod._gaps_within((0, 200), fragments) == [(100, 150)]

    def test_gaps_are_found_whatever_order_the_fragments_come_in(self) -> None:
        fragments: list[tuple[DedupFile, int, int]] = [(cast(Any, None), 50, 60), (cast(Any, None), 10, 20)]
        assert export_scheduler_mod._gaps_within((0, 100), fragments) == [(0, 10), (20, 50), (60, 100)]

    def test_no_fragments_is_one_whole_gap(self) -> None:
        assert export_scheduler_mod._gaps_within((5, 100), []) == [(5, 100)]

    def test_one_fragment_covering_the_whole_span_has_no_gaps(self) -> None:
        assert export_scheduler_mod._gaps_within((0, 100), [(cast(Any, None), 0, 100)]) == []


class TestSegmentedExport:
    """A segmented sink gets exactly the bytes, and the same totals, a ``LocalFileSink`` does."""

    @pytest.mark.parametrize("storage", ["memory", "spool"])
    @pytest.mark.parametrize("sparse", [True, False])
    @pytest.mark.parametrize("segment_size", [4096, 8192, 12288, 4 * 4096 + 4096])
    async def test_a_file_with_data_zero_and_hole_extents_matches_the_local_file_export(
        self,
        dedup_file: DedupFile,
        tmp_path: Path,
        sparse: bool,
        segment_size: int,
        storage: Literal["memory", "spool"],
    ) -> None:
        local = tmp_path / "local.bin"
        expected = await run_export(dedup_file, LocalFileSink(local, staged=False), sparse=sparse)

        sink = SegmentCollector(segment_size, storage=storage, spool_dir=tmp_path)
        result = await run_export(dedup_file, sink, sparse=sparse)

        assert bytes(sink.output) == local.read_bytes()
        assert (result.bytes_written, result.logical_size, result.holes, result.zeros) == (
            expected.bytes_written,
            expected.logical_size,
            expected.holes,
            expected.zeros,
        )

    @pytest.mark.parametrize("storage", ["memory", "spool"])
    @pytest.mark.parametrize("segment_size", [4096, 8192])
    async def test_a_view_of_a_file_matches_a_read_of_the_same_range(
        self, dedup_file: DedupFile, tmp_path: Path, segment_size: int, storage: Literal["memory", "spool"]
    ) -> None:
        view = dedup_file.view(8192, 20480)

        sink = SegmentCollector(segment_size, storage=storage, spool_dir=tmp_path)
        result = await run_export(view, sink, sparse=False)

        assert bytes(sink.output) == await view.read(0, 20480)
        assert result.logical_size == 20480

    @pytest.mark.parametrize("storage", ["memory", "spool"])
    async def test_two_buckets_read_across_segment_boundaries_come_out_whole(
        self, tmp_path: Path, storage: Literal["memory", "spool"]
    ) -> None:
        file = _two_bucket_file(tmp_path)

        sink = SegmentCollector(4096, storage=storage, spool_dir=tmp_path)
        await run_export(file, sink, sparse=False)

        assert bytes(sink.output) == await file.read(0, 16384)

    @pytest.fixture
    def count_executors(self, monkeypatch: pytest.MonkeyPatch) -> list[ExportExecutor]:
        _CountingExecutor.built = []
        monkeypatch.setattr(api_export_mod, "ExportExecutor", _CountingExecutor)
        return _CountingExecutor.built

    async def test_spool_segments_share_one_worker_pool_for_the_whole_export(
        self, dedup_file: DedupFile, tmp_path: Path, count_executors: list[ExportExecutor]
    ) -> None:
        sink = SegmentCollector(8192, storage="spool", spool_dir=tmp_path)

        await run_export(dedup_file, sink, sparse=False)

        assert len(count_executors) == 1  # six segments, one pool
        assert bytes(sink.output) == await dedup_file.read(0, SIZE)

    async def test_memory_segments_share_one_worker_pool_too(
        self, dedup_file: DedupFile, count_executors: list[ExportExecutor]
    ) -> None:
        sink = SegmentCollector(8192)

        await run_export(dedup_file, sink, sparse=False)

        assert len(count_executors) == 1
        assert bytes(sink.output) == await dedup_file.read(0, SIZE)

    async def test_a_single_segment_leaves_the_worker_pool_to_the_lazy_rule(
        self, dedup_file: DedupFile, tmp_path: Path, count_executors: list[ExportExecutor]
    ) -> None:
        sink = SegmentCollector(SIZE, storage="spool", spool_dir=tmp_path)

        await run_export(dedup_file, sink, sparse=False)

        assert bytes(sink.output) == await dedup_file.read(0, SIZE)
        assert len(count_executors) <= 1  # built by the segment's own export if at all, never up front

    async def test_progress_counts_the_real_data_across_segments(self, dedup_file: DedupFile) -> None:
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        await run_export(dedup_file, SegmentCollector(8192), sparse=True, progress=_progress)

        assert calls[-1] == (3 * 4096 + 4 * 4096, 3 * 4096 + 4 * 4096)
        assert [done for done, _ in calls] == sorted(done for done, _ in calls)
        assert {total for _, total in calls} == {3 * 4096 + 4 * 4096}


class TestNonFileSink:
    """A sink without a ``worker_target`` must take the in-process path even
    though ``dedup_file``'s real ``LocalFsStore`` would otherwise be
    exported through worker processes."""

    async def test_never_builds_an_executor_and_reproduces_the_naive_export(
        self, dedup_file: DedupFile, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _no_executor(*args: object, **kwargs: object) -> object:
            raise AssertionError("a sink with no worker_target must not get an executor")

        monkeypatch.setattr(export_scheduler_mod, "ExportExecutor", _no_executor)
        sink = _MemorySink(supports_sparse=False)
        await sink.open(SIZE, sparse=True)

        result = await export_to_writer(dedup_file, sink, sparse=True)

        naive_dst = tmp_path / "naive.bin"
        await _naive_export_to(dedup_file, naive_dst, sparse=False)
        assert bytes(sink.buffer) == naive_dst.read_bytes()
        assert result.bytes_written == 3 * 4096 + 4 * 4096

    async def test_writes_stay_sector_aligned(self, dedup_file: DedupFile) -> None:
        sink = _MemorySink(supports_sparse=False)
        await sink.open(SIZE, sparse=True)
        await export_to_writer(dedup_file, sink, sparse=True)
        assert sink.data_writes
        sector = 512  # what a sector-addressed disk API needs; the scheduler writes whole 4096-byte chunks
        assert all(offset % sector == 0 for offset, _ in sink.data_writes + sink.zero_writes)
        assert all(length % sector == 0 for _, length in sink.data_writes + sink.zero_writes)

    async def test_writes_are_chunk_aligned_except_one_ending_at_the_range_end(self, dedup_file: DedupFile) -> None:
        """``ExportWriter``'s alignment guarantee: every offset and length is a
        multiple of 4096, except a write that ends at the range's end (here a
        view cut 100 bytes short of a chunk boundary)."""
        size = SIZE - 100
        view = dedup_file.view(0, size)
        sink = _MemorySink(supports_sparse=False)
        await sink.open(size, sparse=False)

        await export_to_writer(view, sink, sparse=False)

        writes = sink.data_writes + sink.zero_writes
        assert sorted(writes) == [(0, 12288), (12288, 8192), (20480, 4096), (24576, 16384), (40960, size - 40960)]
        assert all(offset % 4096 == 0 for offset, _ in writes)
        assert all(length % 4096 == 0 or offset + length == size for offset, length in writes)
        assert any(offset + length == size and length % 4096 != 0 for offset, length in writes)

    async def test_a_sink_without_sparse_support_gets_zero_fills_even_for_a_sparse_export(
        self, dedup_file: DedupFile
    ) -> None:
        sink = _MemorySink(supports_sparse=False)
        await sink.open(SIZE, sparse=True)
        await export_to_writer(dedup_file, sink, sparse=True)
        assert sink.zero_writes

    async def test_a_sparse_capable_sink_gets_no_zero_fills_for_a_sparse_export(self, dedup_file: DedupFile) -> None:
        sink = _MemorySink(supports_sparse=True)
        await sink.open(SIZE, sparse=True)
        await export_to_writer(dedup_file, sink, sparse=True)
        assert sink.zero_writes == []

    async def test_a_preallocated_sink_gets_no_zero_fills_even_for_a_dense_export(self, dedup_file: DedupFile) -> None:
        """The destination already reads as zero and is already allocated, so
        the dense export writes exactly the data a sparse export does."""
        dense = _MemorySink(supports_sparse=True, preallocated=True)
        await dense.open(SIZE, sparse=False)
        await export_to_writer(dedup_file, dense, sparse=False)

        sparse = _MemorySink(supports_sparse=True)
        await sparse.open(SIZE, sparse=True)
        await export_to_writer(dedup_file, sparse, sparse=True)

        assert dense.zero_writes == []
        assert sorted(dense.data_writes) == sorted(sparse.data_writes)
        assert dense.buffer == sparse.buffer

    async def test_a_non_sparse_export_zero_fills_even_on_a_sparse_capable_sink(self, dedup_file: DedupFile) -> None:
        sink = _MemorySink(supports_sparse=True)
        await sink.open(SIZE, sparse=False)
        await export_to_writer(dedup_file, sink, sparse=False)
        assert sink.zero_writes

    async def test_a_caller_supplied_executor_is_rejected_for_a_sink_without_a_worker_target(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        descriptor = PoolDescriptor.from_pool(dedup_file.pool)
        assert descriptor is not None
        executor = ExportExecutor(descriptor, LocalFileDescriptor(str(tmp_path / "out.bin")))
        try:
            sink = _MemorySink(supports_sparse=True)
            await sink.open(SIZE, sparse=True)
            with pytest.raises(ValueError, match="was not built for"):
                await export_to_writer(dedup_file, sink, tuning=ExportTuning(executor=executor))
        finally:
            await executor.close()


class TestMultiprocessExecutorTeardown:
    """``ProcessPoolExecutor.shutdown()`` is blocking, so the multiprocess
    path must route it through ``asyncio.to_thread`` rather than freeze the
    event loop. ``dedup_file`` is describable, so this is the multiprocess path."""

    async def test_executor_shutdown_is_routed_through_to_thread(
        self, dedup_file: DedupFile, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from concurrent.futures import ProcessPoolExecutor

        real_to_thread = asyncio.to_thread
        recorded: list[object] = []

        async def _recording_to_thread(func: object, *args: object, **kwargs: object) -> object:
            recorded.append(func)
            return await real_to_thread(func, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(asyncio, "to_thread", _recording_to_thread)

        await export_to(dedup_file, tmp_path / "out.bin")

        shutdown_calls = [
            f for f in recorded if getattr(f, "__name__", None) == "shutdown" and getattr(f, "__self__", None)
        ]
        assert len(shutdown_calls) == 1, (
            f"executor.shutdown() was not routed through asyncio.to_thread once; saw: {recorded}"
        )
        assert all(isinstance(f.__self__, ProcessPoolExecutor) for f in shutdown_calls)  # type: ignore[attr-defined]
