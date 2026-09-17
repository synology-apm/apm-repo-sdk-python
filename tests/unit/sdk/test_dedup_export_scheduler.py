"""Unit tests for ``synology_apm_repo.sdk.dedup.export_scheduler`` —
synthetic composition + Pool data written to real files, no sample
repositories required (see ``tests/integration/sdk/test_dedup_export_scheduler_real.py``
for the byte-for-byte cross-check against a real 32 GiB VM image).

``_naive_export_to`` below is this file's own correctness oracle: a
deliberately independent, unmerged, one-chunk-at-a-time walk that never
groups ``DATA`` chunks by bucket — production ``export_to()`` has no
naive path of its own to compare against (see ``export_scheduler.py``'s
own module docstring), so this file keeps one purely for the
cross-check.

No ``verify_map_crc`` coverage here — chunk-map CRC validation is
``verify``'s job specifically, not export's; see
``tests/unit/sdk/test_units_verify_reachable.py`` and
``format/composition.py``'s primitive-level
``tests/unit/sdk/test_format_composition.py``.
"""

from __future__ import annotations

import asyncio
import os
import struct
import sys
import threading
import zlib
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.dedup import export_scheduler as export_scheduler_mod
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView, DedupFile, ExportResult, ExtentKind
from synology_apm_repo.sdk.dedup.export_scheduler import _WRITER_QUEUE_SIZE, _ExportSink, export_to
from synology_apm_repo.sdk.dedup.pool import BucketReaderCache, Pool
from synology_apm_repo.sdk.errors import DataCorruptError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import FIXED_CHUNK_LENGTH, SUB_FILE_SIZE
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_O_BINARY = getattr(os, "O_BINARY", 0)


def _oracle_pwrite(fd: int, data: bytes, offset: int) -> None:
    """The oracle's own positional write — ``os.pwrite()`` where available
    (POSIX), ``lseek``+``write`` on Windows, which has no positional write.
    Deliberately this file's own helper rather than the one under test, so
    the oracle stays independent of ``export_scheduler``'s implementation."""
    if sys.platform != "win32":
        os.pwrite(fd, data, offset)
    else:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)


_STREAM_ID = StreamId(7)
_SESSION_ID = SessionId(3)
_HEAD_OFF = 64
_SIZE = 45056  # matches test_dedup_dedup_file.py's own standard fixture

_CHUNK_PLAINTEXTS = [bytes([i]) * 4096 for i in range(5)]  # bucket 0, chunks 0..4
_BUCKET_1_PLAINTEXTS = [bytes([100 + i]) * 4096 for i in range(2)]  # bucket 1, chunks 0..1


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _mapping_record(file_offset: int, bucket_id: int, chunk_idx: int, map_num: int, repeat: int = 0) -> bytes:
    addr_int = ChunkAddress(StreamId(0), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()
    return _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value,
        file_chunk_idx=file_offset >> 12,
        addr_int=addr_int,
        tail_u32=(map_num << 16) | repeat,
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


def _write_standard_composition(comp_root: Path, entries: bytes) -> None:
    map_num = len(entries) // 20
    map_crc = zlib.crc32(entries) & 0xFFFFFFFF
    record_bytes = _record_head_bytes(map_num=map_num, map_crc=map_crc) + entries
    path = comp_root / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
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
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    from synology_apm_repo.sdk.format.redundancy import redundancy_size

    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _standard_entries() -> bytes:
    # Same layout as test_dedup_dedup_file.py's standard fixture:
    # [0,12288)=DATA(3), [12288,20480)=ZERO(2), [20480,24576)=HOLE,
    # [24576,40960)=DATA(4, templated via repeat), [40960,45056)=trailing HOLE.
    return (
        _mapping_record(0, 0, 0, map_num=3)
        + _zero_record(12288, zero_num=2)
        + _mapping_record(24576, 0, 3, map_num=2, repeat=1)
    )


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    _write_standard_composition(tmp_path / "Composition", _standard_entries())
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE)


def _recording_progress(calls: list[tuple[int, int]]) -> Callable[[int, int], Awaitable[None]]:
    """``progress`` must be ``Callable[[int, int], Awaitable[None]]`` — a
    plain ``lambda done, total: calls.append(...)`` would hand the
    exporter a list instead of something awaitable."""

    async def _progress(done: int, total: int) -> None:
        calls.append((done, total))

    return _progress


class _BlockingStore:
    """An ``ObjectStore`` wrapper that can be *armed* to park forever
    inside its next ``read()`` — used to park this module's cancellation
    test at a real ``await``; there is no ``cancel=`` parameter to pass
    instead."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self.armed = False
        self.blocked = asyncio.Event()  # set once a read has actually parked
        self._never_released = asyncio.Event()  # deliberately never set

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if self.armed:
            self.blocked.set()
            await self._never_released.wait()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


def _build_blocking_dedup_file(tmp_path: Path) -> tuple[DedupFile, _BlockingStore]:
    """The standard fixture, but over a ``_BlockingStore``."""
    _write_standard_composition(tmp_path / "Composition", _standard_entries())
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = _BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE), store


async def _naive_export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool = True,
    progress: Callable[[int, int], Awaitable[None]] | None = None,
) -> ExportResult:
    """This module's own correctness oracle — walks extents in logical
    order and fetches exactly one chunk at a time via
    ``pool.read_chunk()``, never grouping by bucket or merging reads. Kept
    deliberately independent of ``export_scheduler.py``'s production
    ``export_to()`` (duplicated logic, not shared helpers) so the two
    catching the same bug would require it to survive two structurally
    different code paths — matches this file's own established
    "duplicate rather than import fixture helpers" convention.
    """
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
            raise ValueError("export_to() requires a known size")
        size = file_like.size

    window_end = window_start + size
    holes = zeros = bytes_written = 0
    # Raw os.open/pwrite/close (not Path.open()'s buffered file object) --
    # not a performance choice here (this is a test-only oracle, never on
    # a hot path), just the plainest way to hold an fd across the awaits
    # below without a blocking Path method flagging ASYNC230/ASYNC240.
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
            assert extent.addr is not None and extent.map_num > 0
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
                await progress(bytes_written, size)
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
        """A view whose own offset (though still chunk-aligned, e.g. real
        FS ``content_dedup_id`` values) can fall in the *middle* of a
        chunk-map record, not at its own start — ``plan_chunks_windowed()``'s
        ``DATA`` branch must skip the leading chunk(s) before the view's
        own start. The standard fixture's first ``DATA`` record spans
        ``[0, 12288)`` (3 chunks); this view starts at chunk 1 (offset
        4096), skipping chunk 0 entirely."""
        view = dedup_file.view(4096, 8192)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(view, naive_dst, sparse=False)
        r_production = await export_to(view, production_dst, sparse=False)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_for_a_size_that_ends_mid_chunk(self, tmp_path: Path) -> None:
        truncated_size = 40960 - 100
        _write_standard_composition(tmp_path / "Composition", _standard_entries())
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=truncated_size)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(file, production_dst)
        assert naive_dst.stat().st_size == truncated_size
        assert production_dst.stat().st_size == truncated_size
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_matches_the_naive_oracle_for_a_size_that_ends_mid_zero_extent(self, tmp_path: Path) -> None:
        """A declared size ending inside a ``ZERO`` extent (not a
        ``DATA`` one) must clip that extent's length the same way the
        naive oracle already clips every extent kind including ``ZERO``
        — this test class's own "matches the naive oracle" invariant
        applies to the ``ZERO`` case too. The standard fixture's ``ZERO``
        extent spans ``[12288, 20480)``; ``truncated_size`` ends 2 bytes
        into it."""
        truncated_size = 12288 + 2
        _write_standard_composition(tmp_path / "Composition", _standard_entries())
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=truncated_size)

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
        entries = _mapping_record(0, 0, 0, map_num=2) + _mapping_record(8192, 1, 0, map_num=2)
        _write_standard_composition(tmp_path / "Composition", entries)
        # pool_layer_path(stream_id=0, bucket_id) keeps small bucket ids
        # under the stream's own "0" directory, with the bucket id itself
        # becoming the leaf filename prefix — bucket 1's file is
        # "Pool/0/1.buk", not "Pool/1/0.buk".
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=16384)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert production_dst.read_bytes() == _CHUNK_PLAINTEXTS[0] + _CHUNK_PLAINTEXTS[1] + b"".join(
            _BUCKET_1_PLAINTEXTS
        )


class TestBehavior:
    async def test_reports_progress(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        calls: list[tuple[int, int]] = []
        await export_to(
            dedup_file,
            tmp_path / "out.bin",
            progress=_recording_progress(calls),
        )
        assert calls
        assert calls[-1][0] == 3 * 4096 + 4 * 4096

    async def test_is_cancellable_via_its_surrounding_task_and_closes_its_output_fd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Letting the export get genuinely under way, then cancelling
        the surrounding Task, must propagate
        ``asyncio.CancelledError``.

        Cancellation is arranged from the *inside*: the progress callback
        arms the store on its first call (so the destination fd is
        already open and one run already written by then), and the next
        window's composition read parks forever, giving a deterministic
        point to cancel at. ``window_entries=1`` is what guarantees there
        *is* a next window.

        The cleanup half is asserted directly: the fd
        ``export_scheduler.export_to()`` opened for ``dst`` really is
        closed on the way out, not leaked, via ``try/finally``."""
        file, store = _build_blocking_dedup_file(tmp_path)
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

        async def _arm_on_first_progress(done: int, total: int) -> None:
            store.armed = True

        async def do_export() -> object:
            return await export_to(file, dst, window_entries=1, progress=_arm_on_first_progress)

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(opened_fds) == 1  # the export really did get as far as opening its destination
        assert closed_fds == opened_fds  # ...and the cancelled export still closed it
        assert dst.stat().st_size == _SIZE  # the truncated-up-front partial output is left behind

    async def test_requires_known_size_for_a_plain_dedup_file(self, tmp_path: Path) -> None:
        _write_standard_composition(tmp_path / "Composition", _standard_entries())
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, _HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await export_to(unsized, tmp_path / "out.bin")

    async def test_rejects_a_byte_range_view_with_a_non_chunk_aligned_offset(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        """End-to-end: a ByteRangeView whose own offset isn't
        chunk-aligned (unlike every real FS content_dedup_id sampled so
        far, but not something this module structurally guarantees
        against) must fail with a clear ValueError through the real
        public entry point, not silent output corruption."""
        view = dedup_file.view(100, 4096)
        with pytest.raises(ValueError, match="chunk-aligned window_start"):
            await export_to(view, tmp_path / "out.bin")


class TestWindowing:
    """The sliding-window memory bound, exercised through the
    actual public entry point (not just ``chunk_walk.py``'s own unit
    tests) — this is where a real caller would ever pass ``window_entries``
    and where the cross-window progress-accumulation logic
    (``count_planned_bytes()`` + the running ``bytes_written`` offset)
    actually lives."""

    @pytest.mark.parametrize("window_entries", [1, 2, 3, 7, 1_000_000])
    async def test_matches_the_naive_oracle_regardless_of_window_size(
        self, dedup_file: DedupFile, tmp_path: Path, window_entries: int
    ) -> None:
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(dedup_file, naive_dst, sparse=True)
        r_production = await export_to(
            dedup_file,
            production_dst,
            sparse=True,
            window_entries=window_entries,
        )
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_multiple_buckets_still_matches_the_naive_oracle_with_a_tiny_window(self, tmp_path: Path) -> None:
        # Same fixture as TestCorrectness.test_multiple_buckets_are_all_visited_correctly,
        # but with window_entries=1 -- forces a window boundary *between*
        # the two buckets' own chunks, not just within one bucket's own
        # placements, which the single-bucket dedup_file fixture alone
        # can't exercise.
        entries = _mapping_record(0, 0, 0, map_num=2) + _mapping_record(8192, 1, 0, map_num=2)
        _write_standard_composition(tmp_path / "Composition", entries)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=16384)

        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst, window_entries=1)
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    async def test_holes_and_zeros_are_summed_across_windows_not_just_the_last_ones(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        default_result = await export_to(dedup_file, tmp_path / "default.bin", sparse=False)
        windowed_result = await export_to(
            dedup_file,
            tmp_path / "windowed.bin",
            sparse=False,
            window_entries=1,
        )
        assert windowed_result.holes == default_result.holes
        assert windowed_result.zeros == default_result.zeros

    async def test_progress_is_monotonic_and_the_total_stays_constant_across_windows(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        calls: list[tuple[int, int]] = []
        await export_to(
            dedup_file,
            tmp_path / "out.bin",
            window_entries=1,
            progress=_recording_progress(calls),
        )
        assert len(calls) > 1  # window_entries=1 forces multiple exec_chunks() calls, each reporting

        totals = {total for _done, total in calls}
        assert len(totals) == 1  # the grand total must not reset/change at a window boundary

        dones = [done for done, _total in calls]
        assert dones == sorted(dones)  # monotonically non-decreasing across every window
        assert dones[-1] == 3 * 4096 + 4 * 4096  # same final value TestBehavior.test_reports_progress checks


class TestPipelinedWriter:
    """``_ExportSink`` hands writes to a dedicated writer thread through a
    bounded queue rather than doing ``await asyncio.to_thread(os.pwrite,
    ...)`` inline (see ``export_scheduler.py``'s own module docstring for
    the measured overlap win this buys) — the one behavior that's genuinely
    new, not just a rearrangement of the same inline calls, is how a real
    write failure on that background thread reaches the caller."""

    async def test_writer_thread_failure_propagates_to_the_caller(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failure on the writer thread (disk full, permission error,
        ...) must still surface as a real exception from ``export_to()``,
        not be silently swallowed just because it happened off the
        awaiting coroutine's own thread.

        Built over ``_build_blocking_dedup_file`` (unarmed, a plain
        pass-through here) rather than the shared ``dedup_file`` fixture —
        that fixture's own real ``LocalFsStore`` is describable, which
        would route this export through the multiprocess path instead;
        this test's whole point is the single-process writer thread, and
        a `monkeypatch` on this *process's* `os.pwrite` has no effect on a
        separate worker process's own unpatched one anyway."""
        file, _store = _build_blocking_dedup_file(tmp_path)

        def _failing_pwrite(fd: int, data: bytes, offset: int) -> int:
            raise OSError("synthetic disk-full for this test")

        monkeypatch.setattr(export_scheduler_mod, "_pwrite", _failing_pwrite)
        with pytest.raises(OSError, match="synthetic disk-full"):
            await export_to(file, tmp_path / "out.bin")

    async def test_writer_thread_failure_during_cancellation_does_not_mask_the_cancellation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the writer thread *also* fails while a cancellation is in
        flight, the real ``asyncio.CancelledError`` must still win — a
        secondary error from the best-effort drain is exactly what
        ``export_to()``'s own ``except BaseException: ... contextlib.suppress``
        is for; this test is what proves that suppression path is reachable
        and correct, not just written defensively."""
        file, store = _build_blocking_dedup_file(tmp_path)
        dst = tmp_path / "out.bin"

        def _failing_pwrite(fd: int, data: bytes, offset: int) -> int:
            raise OSError("synthetic writer failure racing the cancellation")

        monkeypatch.setattr(export_scheduler_mod, "_pwrite", _failing_pwrite)

        async def _arm_on_first_progress(done: int, total: int) -> None:
            store.armed = True

        async def do_export() -> object:
            return await export_to(file, dst, window_entries=1, progress=_arm_on_first_progress)

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_write_data_and_write_gap_reraise_an_already_stored_error(self, tmp_path: Path) -> None:
        """Once the writer thread has failed once (``_error`` set — see
        the class docstring: touched only by the writer thread, read
        only here), every *subsequent* ``write_data``/``write_gap`` call
        must re-raise it immediately rather than queuing more doomed
        work — exercised directly against a bare ``_ExportSink`` with
        ``_error`` set by hand, not through a real failing write."""
        dst = tmp_path / "out.bin"
        fd = os.open(dst, os.O_WRONLY | _O_BINARY | os.O_CREAT, 0o644)
        try:
            sink = _ExportSink(fd, sparse=False, planned_total=0, progress=None)
            sink._error = OSError("synthetic pre-existing failure")
            with pytest.raises(OSError, match="synthetic pre-existing failure"):
                await sink.write_data(0, b"x")
            with pytest.raises(OSError, match="synthetic pre-existing failure"):
                await sink.write_gap(0, 10)
        finally:
            os.close(fd)

    async def test_record_external_bytes_reraises_an_already_stored_error(self, tmp_path: Path) -> None:
        """Same guard as ``write_data``'s own, for the multiprocess
        dispatch path's bookkeeping-only counterpart: a gap-write failure
        already observed on this sink's own writer thread must surface
        here too, rather than a worker's already-committed byte count
        being silently counted as if the export were still healthy."""
        dst = tmp_path / "out.bin"
        fd = os.open(dst, os.O_WRONLY | _O_BINARY | os.O_CREAT, 0o644)
        try:
            sink = _ExportSink(fd, sparse=False, planned_total=0, progress=None)
            sink._error = OSError("synthetic pre-existing failure")
            with pytest.raises(OSError, match="synthetic pre-existing failure"):
                await sink.record_external_bytes(4096)
        finally:
            os.close(fd)

    async def test_record_external_bytes_updates_written_count_and_reports_progress(self) -> None:
        """The success path — no writer thread/queue involved at all,
        just the same bytes_written/progress bookkeeping ``write_data``
        itself does."""
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        sink = _ExportSink(-1, sparse=True, planned_total=100, progress=_progress)
        await sink.record_external_bytes(40)
        await sink.record_external_bytes(10)
        assert sink.bytes_written == 50
        assert calls == [(40, 100), (50, 100)]

    async def test_put_falls_back_to_a_thread_hop_once_the_queue_is_genuinely_full(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_put()``'s common case (``put_nowait()`` succeeds) is already
        exercised by every other test in this file — this is the rare
        fallback, needing the queue to actually be at its
        ``_WRITER_QUEUE_SIZE`` (8) capacity, which needs the writer
        thread genuinely stalled (``export_scheduler``'s own ``_pwrite``
        patched to block
        on a controlled event) rather than racing a real disk write."""
        started = threading.Event()
        release = threading.Event()

        def blocking_pwrite(fd: int, data: bytes, offset: int) -> int:
            started.set()
            release.wait()
            return len(data)

        monkeypatch.setattr(export_scheduler_mod, "_pwrite", blocking_pwrite)

        dst = tmp_path / "out.bin"
        fd = os.open(dst, os.O_WRONLY | _O_BINARY | os.O_CREAT, 0o644)
        try:
            sink = _ExportSink(fd, sparse=False, planned_total=0, progress=None)
            # This first write is picked up by the writer thread and
            # parks it inside the patched, blocking pwrite -- the queue
            # itself is empty again the instant that happens.
            await sink.write_data(0, b"first")
            await asyncio.to_thread(started.wait)

            # Fill the queue to its own max capacity while the writer
            # thread can't drain it.
            for _ in range(_WRITER_QUEUE_SIZE):
                await sink.write_data(0, b"y")

            # One more write finds put_nowait() raising queue.Full and
            # must fall back to the asyncio.to_thread() hop instead of
            # propagating that exception -- this call blocks until
            # release.set() below frees up space.
            overflow_task = asyncio.create_task(sink.write_data(0, b"z"))
            # Proving a negative (the overflow write hasn't wrongly
            # resolved) needs a real elapsed-time budget, not a minimal
            # one -- the same margin as this project's other
            # negative-proof tests (see test_dedup_pool.py's own
            # test_no_semaphore_is_still_strictly_serial).
            await asyncio.sleep(0.2)
            assert not overflow_task.done(), "the overflow write must be blocked on the full queue, not have raised"
            release.set()
            await overflow_task
        finally:
            release.set()
            os.close(fd)


class _FailingStore:
    """An ``ObjectStore`` wrapper whose ``read()`` raises for one
    chosen path — lets a test construct a bucket-group failure
    deliberately (see
    ``TestParallel.test_max_concurrent_reads_surfaces_a_bucket_failure_as_an_exception_group``'s
    own docstring for what that's used to prove)."""

    def __init__(self, backing: LocalFsStore, *, fail_path_suffix: str) -> None:
        self._backing = backing
        self._fail_path_suffix = fail_path_suffix

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if path.endswith(self._fail_path_suffix):
            raise DataCorruptError(f"synthetic failure for {path!r}", ref=path)
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


def _two_bucket_file(tmp_path: Path) -> DedupFile:
    """Two independent buckets sharing one composition — the minimal
    fixture with something to run concurrently *across* (the single-
    bucket ``dedup_file`` fixture has nothing). Shared by
    ``TestParallel`` (``max_concurrent_reads``) and
    ``TestPrefetchOpens`` (``max_concurrent_opens``)."""
    entries = _mapping_record(0, 0, 0, map_num=2) + _mapping_record(8192, 1, 0, map_num=2)
    _write_standard_composition(tmp_path / "Composition", entries)
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
    _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=16384)


class TestParallel:
    """``max_concurrent_reads`` (see ``chunk_walk.py``'s own
    docstring for the mechanism), exercised through the actual public
    entry point (not just ``chunk_walk.py``'s own ``exec_chunks()`` unit
    tests) — the two-bucket fixture here matters for the same reason
    ``TestWindowing``'s own multi-bucket test does: the single-bucket
    ``dedup_file`` fixture has nothing to run concurrently *across*."""

    async def test_matches_the_naive_oracle_across_multiple_bucket_groups(self, tmp_path: Path) -> None:
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst)
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    async def test_combined_with_a_tiny_window_still_matches_the_naive_oracle(self, tmp_path: Path) -> None:
        # window_entries is the only knob here -- a window boundary that
        # also has to hand work across multiple bucket groups is still
        # worth its own case, since windowing and bucket-major grouping
        # interact.
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        await _naive_export_to(file, naive_dst)
        await export_to(file, production_dst, window_entries=1)
        assert naive_dst.read_bytes() == production_dst.read_bytes()

    @pytest.mark.parametrize("max_concurrent_reads", [2, 8])
    async def test_max_concurrent_reads_matches_the_naive_oracle(
        self, tmp_path: Path, max_concurrent_reads: int
    ) -> None:
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(file, production_dst, max_concurrent_reads=max_concurrent_reads)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_max_concurrent_reads_progress_stays_monotonic_and_reaches_the_full_total(
        self, tmp_path: Path
    ) -> None:
        """Concurrent bucket groups' ``on_bytes`` calls can interleave in
        wall-clock time, but each one's own increment never spans an
        ``await`` (see ``chunk_walk.exec_chunks``'s own docstring) — every
        report should still be the true cumulative total at the moment it
        fires, so the sequence stays non-decreasing end to end."""
        file = _two_bucket_file(tmp_path)
        calls: list[tuple[int, int]] = []
        result = await export_to(
            file,
            tmp_path / "out.bin",
            progress=_recording_progress(calls),
            max_concurrent_reads=8,
        )
        assert calls
        dones = [done for done, _total in calls]
        assert dones == sorted(dones)
        assert dones[-1] == result.bytes_written
        totals = {total for _done, total in calls}
        assert totals == {result.bytes_written}

    async def test_max_concurrent_reads_surfaces_a_bucket_failure_as_an_exception_group(self, tmp_path: Path) -> None:
        """The documented difference from ``max_concurrent_reads=1``:
        a failure inside one concurrently-running bucket group surfaces
        via ``asyncio.TaskGroup`` as an ``ExceptionGroup``, not the
        original exception type directly."""
        entries = _mapping_record(0, 0, 0, map_num=2) + _mapping_record(8192, 1, 0, map_num=2)
        _write_standard_composition(tmp_path / "Composition", entries)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = _FailingStore(LocalFsStore(tmp_path), fail_path_suffix="1.buk")
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=16384)

        with pytest.raises(ExceptionGroup) as excinfo:
            await export_to(file, tmp_path / "out.bin", max_concurrent_reads=8)
        assert any(isinstance(exc, DataCorruptError) for exc in excinfo.value.exceptions)


class TestPrefetchOpens:
    """``max_concurrent_opens`` (see
    ``chunk_walk.py``'s own ``_prefetch_bucket_opens`` docstring for the
    mechanism) — exercised through the actual public entry point, same
    ``_two_bucket_file`` fixture as ``TestParallel`` since a
    single-bucket range has only one bucket to ever prefetch."""

    @pytest.mark.parametrize("max_concurrent_opens", [2, 8])
    async def test_matches_the_naive_oracle(self, tmp_path: Path, max_concurrent_opens: int) -> None:
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(file, production_dst, max_concurrent_opens=max_concurrent_opens)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_combined_with_max_concurrent_reads_still_matches_the_naive_oracle(self, tmp_path: Path) -> None:
        """The two knobs are independent (one gates full bucket-group/
        in-bucket-run reads, the other only ever warms the open cache
        ahead of them) — combining them must still produce identical
        output."""
        file = _two_bucket_file(tmp_path)
        naive_dst = tmp_path / "naive.bin"
        production_dst = tmp_path / "production.bin"
        r_naive = await _naive_export_to(file, naive_dst)
        r_production = await export_to(file, production_dst, max_concurrent_reads=8, max_concurrent_opens=8)
        assert naive_dst.read_bytes() == production_dst.read_bytes()
        assert r_naive == r_production

    async def test_a_bucket_failure_is_not_wrapped_in_an_exception_group_at_the_serial_default(
        self, tmp_path: Path
    ) -> None:
        """The compatibility guarantee: unlike ``max_concurrent_reads``
        (its own ``test_max_concurrent_reads_surfaces_a_bucket_failure_as_an_exception_group``
        right above), ``max_concurrent_opens`` alone (``max_concurrent_reads``
        left at its serial default of 1) must never change the shape of a
        real failure — the prefetch task is a plain ``asyncio.create_task``
        outside the serial path's own control flow, never an
        ``asyncio.TaskGroup`` sibling of it."""
        entries = _mapping_record(0, 0, 0, map_num=2) + _mapping_record(8192, 1, 0, map_num=2)
        _write_standard_composition(tmp_path / "Composition", entries)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", _BUCKET_1_PLAINTEXTS)
        store = _FailingStore(LocalFsStore(tmp_path), fail_path_suffix="1.buk")
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=16384)

        with pytest.raises(DataCorruptError):
            await export_to(file, tmp_path / "out.bin", max_concurrent_opens=8)


def _build_many_bucket_dedup_file(tmp_path: Path, num_buckets: int) -> tuple[DedupFile, Pool]:
    """``num_buckets`` independent single-chunk buckets, sharing one
    ``Pool`` instance the caller can inspect directly — enough buckets to
    exceed ``Pool``'s default 16-slot ``bucket_cache_size`` when
    ``num_buckets > 16``, for ``TestBucketReaderCache``'s isolation tests.

    Wrapped in ``_BlockingStore`` (unarmed, a plain pass-through here) so
    ``export_to()`` always takes its single-process fallback path: every
    ``TestBucketReaderCache`` assertion is about ``Pool``/``BucketReaderCache``
    identity and reuse, a single-process-only concept once the always-on
    multiprocess path applies (each worker builds its own private cache) —
    a real, describable ``LocalFsStore`` here would silently stop
    exercising what these tests actually check."""
    entries = b"".join(_mapping_record(b * 4096, b, 0, map_num=1) for b in range(num_buckets))
    _write_standard_composition(tmp_path / "Composition", entries)
    for b in range(num_buckets):
        _write_bucket(tmp_path / "Pool" / "0" / f"{b}.buk", [bytes([b % 256]) * 4096])
    store = _BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=num_buckets * 4096), pool


class TestBucketReaderCache:
    """Export's own private, batch-scoped cache (``BucketReaderCache``)
    — never writes into ``Pool``'s shared ``_buckets``, and reuses what
    it already opened across repeated ``export_to()`` calls sharing one
    instance. See ``BucketReaderCache``'s own docstring for the design."""

    async def test_export_never_writes_into_pool_shared_bucket_cache(self, tmp_path: Path) -> None:
        # 20 buckets — more than Pool's default 16-slot bucket_cache_size,
        # so if export wrote into the shared cache at all, eviction would
        # be forced and the snapshot below would change.
        file, pool = _build_many_bucket_dedup_file(tmp_path, 20)

        # "Interactive" warm-up: directly open 5 buckets through Pool's
        # own cached path, exactly like a real browsing session would.
        for b in range(5):
            await pool.bucket(StreamId(0), BucketId(b))
        before = dict(pool._buckets)
        assert set(before) == {(0, b) for b in range(5)}

        await export_to(file, tmp_path / "out.bin")

        after = dict(pool._buckets)
        assert after == before  # completely untouched by the export that followed

    async def test_standalone_export_leaves_the_shared_cache_empty(self, tmp_path: Path) -> None:
        # No prior interactive activity at all — the same property from
        # the other direction: touching 20 buckets during export must
        # never populate Pool's shared cache in the first place.
        file, pool = _build_many_bucket_dedup_file(tmp_path, 20)
        await export_to(file, tmp_path / "out.bin")
        assert dict(pool._buckets) == {}

    async def test_sharing_one_export_cache_across_two_calls_reuses_the_same_bucket(self, tmp_path: Path) -> None:
        file, _pool = _build_many_bucket_dedup_file(tmp_path, 3)
        cache = BucketReaderCache()

        await export_to(file, tmp_path / "out1.bin", export_cache=cache)
        assert len(cache.buckets) == 3
        opened_once = dict(cache.buckets)

        # A second export sharing the same cache must reuse those same
        # BucketReader instances rather than reopening anything — identity,
        # not just equal keys, so a silent "closed and reopened" regression
        # would be caught too.
        await export_to(file, tmp_path / "out2.bin", export_cache=cache)
        assert dict(cache.buckets).keys() == opened_once.keys()
        for key, reader in opened_once.items():
            assert cache.buckets[key] is reader


class TestDstOffsetAndCreate:
    """``dst_offset``/``create`` — added for exactly one caller,
    ``sdk/units/content/pcps_disk.py``'s ``VirtualDiskContentSource``,
    assembling several disk-absolute-addressed fragments into one combined
    file. See ``_ExportSink``'s own docstring for why the shift happens at
    the sink's actual ``pwrite``/zero-fill calls rather than via
    ``plan_chunks_windowed``'s ``window_start`` — not an alignment
    workaround (a fragment's real ``extent()`` start is always
    chunk-aligned), but keeping two unrelated concerns — where to read
    from vs. where to write to — from being conflated into one
    parameter."""

    async def test_dst_offset_shifts_output_within_a_preexisting_file(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        dst = tmp_path / "combined.bin"
        shift = 100_000
        dst.write_bytes(bytes(shift + _SIZE))

        result = await export_to(dedup_file, dst, sparse=False, dst_offset=shift, create=False)

        naive_dst = tmp_path / "naive.bin"
        await _naive_export_to(dedup_file, naive_dst, sparse=False)
        combined = dst.read_bytes()
        assert combined[:shift] == bytes(shift)  # untouched prefix stays exactly as pre-created
        assert combined[shift : shift + _SIZE] == naive_dst.read_bytes()
        assert result.bytes_written == 3 * 4096 + 4 * 4096

    async def test_create_false_does_not_truncate_a_second_fragment_s_earlier_writes(
        self, dedup_file: DedupFile, tmp_path: Path
    ) -> None:
        """Two ``create=False`` exports into the same file at different
        ``dst_offset``s — the second call must not reset the first
        fragment's own bytes, which a stray ``_create_truncated`` would."""
        dst = tmp_path / "combined.bin"
        total = _SIZE * 2
        dst.write_bytes(bytes(total))

        await export_to(dedup_file, dst, sparse=False, dst_offset=0, create=False)
        first_half = dst.read_bytes()[:_SIZE]

        await export_to(dedup_file, dst, sparse=False, dst_offset=_SIZE, create=False)
        combined = dst.read_bytes()
        assert combined[:_SIZE] == first_half  # first fragment's write survived the second call
        assert combined[_SIZE:] == first_half  # same fixture written again, this time shifted
        assert dst.stat().st_size == total  # never re-truncated back down to a single fragment's size

    async def test_create_true_is_the_default_and_still_truncates(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        dst.write_bytes(b"\xff" * (_SIZE * 2))  # pre-existing, oversized garbage
        await export_to(dedup_file, dst, sparse=False)
        assert dst.stat().st_size == _SIZE  # default create=True still truncates to the logical size


class TestMultiprocessExecutorTeardown:
    """The multiprocess path's own ``ProcessPoolExecutor.shutdown()`` must
    never be called directly — it's a plain, synchronous, potentially slow
    blocking call (as slow as whatever a still-running worker takes to
    finish its current bucket group), which would otherwise freeze this
    whole process's event loop — every other ``Task``, not just this one
    export — for that whole stretch. Measured directly (a scratch script,
    not this test): ~1.4s of total event-loop freeze for one deliberately
    slow worker with no ``to_thread()`` hop. ``dedup_file``'s real
    ``LocalFsStore`` is describable, so this exercises the real
    multiprocess path, not the fallback."""

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
        assert shutdown_calls, f"executor.shutdown() was never routed through asyncio.to_thread; saw: {recorded}"
        assert all(isinstance(f.__self__, ProcessPoolExecutor) for f in shutdown_calls)  # type: ignore[attr-defined]


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
            export_scheduler_mod._pwrite(fd, b"hello", 3)
        finally:
            os.close(fd)
        assert dst.read_bytes() == bytes(3) + b"hello" + bytes(2)
