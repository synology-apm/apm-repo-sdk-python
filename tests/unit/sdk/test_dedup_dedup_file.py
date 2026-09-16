"""Unit tests for ``synology_apm_repo.sdk.dedup.dedup_file`` —
synthetic composition + Pool data written to real files, no sample
repositories required.

Synthetic file layout used by most tests below (stream 7, session 3,
comp_offset=64, size=45056):

======  ==================  ============================================
offset  kind                detail
======  ==================  ============================================
0       DATA (3 chunks)      bucket 0, chunks 0/1/2, map_num=3 repeat=0
12288   ZERO (2 chunks)      zero_num=2
20480   HOLE (1 chunk)       no record — gap before the next one
24576   DATA (4 chunks)      bucket 0, chunks 3/4 templated, repeat=1
                             (k=0,1,2,3 -> chunks 3,4,3,4 via advance())
40960   HOLE (1 chunk)       trailing hole up to size=45056
======  ==================  ============================================
"""

from __future__ import annotations

import asyncio
import os
import struct
import zlib
from pathlib import Path

import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile, ExtentKind
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.format import addressing
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_STREAM_ID = StreamId(7)
_SESSION_ID = SessionId(3)
_HEAD_OFF = 64
_SIZE = 45056  # last record ends at 40960; a trailing 4096-byte HOLE follows

_CHUNK_PLAINTEXTS = [bytes([i]) * 4096 for i in range(5)]  # bucket 0, chunks 0..4


# -- composition sub-file builders (same approach as
#    test_dedup_composition_reader.py) ----------------------------------


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


def _record_head_bytes(*, map_num: int, mode: int = 0x0001, attr_leng: int = 0) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[20:24] = attr_leng.to_bytes(4, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


def _write_standard_composition(comp_root: Path) -> None:
    entries = (
        _mapping_record(0, 0, 0, map_num=3)
        + _zero_record(12288, zero_num=2)
        + _mapping_record(24576, 0, 3, map_num=2, repeat=1)
    )
    record_bytes = _record_head_bytes(map_num=3) + entries
    path = comp_root / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_composition_header_bytes() + record_bytes)


# -- Pool/bucket builders (same approach as test_dedup_pool.py) --------


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


def _write_bucket(path: Path, plaintexts: list[bytes], *, vault_key: bytes | None = None) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads: list[bytes] = []
    entries: list[tuple[int, int]] = []
    for chunk_idx, plain in enumerate(plaintexts):
        compressed = compressor.compress(plain)
        if vault_key is not None:
            addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(chunk_idx))
            encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
            payload = encryptor.update(compressed) + encryptor.finalize()
        else:
            payload = compressed
        payloads.append(payload)
        entries.append((CompressType.ZSTD.value, len(payload)))

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
    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


@pytest.fixture
def dedup_file(tmp_path: Path) -> DedupFile:
    _write_standard_composition(tmp_path / "Composition")
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = LocalFsStore(tmp_path)
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE)


class _BlockingStore:
    """An ``ObjectStore`` wrapper that can be *armed* to park forever
    inside its next ``read()``.

    There is no ``cancel=`` parameter anywhere in the SDK; a cancellation
    test parks the call at a real ``await`` point inside the SDK with
    this and then cancels the surrounding ``asyncio.Task``, exactly
    how a real caller (the CLI/TUI) cancels an in-flight read or export.
    """

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
    """The standard fixture, but over a ``_BlockingStore`` so a test
    can park the SDK mid-read and cancel it."""
    _write_standard_composition(tmp_path / "Composition")
    _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
    store = _BlockingStore(LocalFsStore(tmp_path))
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, _HEAD_OFF, size=_SIZE), store


class TestExtents:
    """Deliberately calls the private ``_extents()`` directly
    — this class tests that method's own chunk-map-parsing correctness
    (kind classification, HOLE synthesis, template fields), which needs
    the raw ``Extent`` objects themselves, not what any public method
    built on top of it returns."""

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
        assert first.map_num == 3
        assert first.repeat == 0
        assert first.addr == ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(0))

    async def test_zero_and_hole_extents_have_no_addr(self, dedup_file: DedupFile) -> None:
        extents = [e async for e in dedup_file._extents()]
        zero_extent = extents[1]
        hole_extent = extents[2]
        assert zero_extent.addr is None
        assert hole_extent.addr is None

    async def test_no_trailing_hole_when_size_matches_last_record_end(self, tmp_path: Path) -> None:
        _write_standard_composition(tmp_path / "Composition")
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        exact_size_file = DedupFile(comp_reader, pool, _HEAD_OFF, size=40960)
        extents = [e async for e in exact_size_file._extents()]
        assert extents[-1].kind is not ExtentKind.HOLE or extents[-1].offset != 40960

    async def test_zero_length_entry_is_skipped_not_yielded_as_an_empty_extent(self, tmp_path: Path) -> None:
        # a Type::Zero record with zero_num=0 carries no coverage at all —
        # it must not produce a spurious zero-length Extent nor break the
        # cursor tracking for the record that follows it.
        entries = (
            _mapping_record(0, 0, 0, map_num=1)
            + _zero_record(4096, zero_num=0)
            + _mapping_record(4096, 0, 1, map_num=1)
        )
        record_bytes = _record_head_bytes(map_num=3) + entries
        comp_path = tmp_path / "Composition" / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(_composition_header_bytes() + record_bytes)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=8192)

        extents = [e async for e in file._extents()]
        assert [(e.kind, e.offset, e.length) for e in extents] == [
            (ExtentKind.DATA, 0, 4096),
            (ExtentKind.DATA, 4096, 4096),
        ]
        assert await file.read(0, 8192) == _CHUNK_PLAINTEXTS[0] + _CHUNK_PLAINTEXTS[1]


async def test_stream_id_session_id_comp_offset_properties(dedup_file: DedupFile) -> None:
    assert dedup_file.stream_id == _STREAM_ID
    assert dedup_file.session_id == _SESSION_ID
    assert dedup_file.comp_offset == _HEAD_OFF


async def test_pool_property(dedup_file: DedupFile) -> None:
    """``export_scheduler.py``'s bucket-major planning needs a ``DedupFile``'s
    own ``Pool`` to schedule reads against a shared ``BucketReaderCache``."""
    assert isinstance(dedup_file.pool, Pool)


class TestRead:
    async def test_reads_a_single_data_chunk(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(0, 4096) == _CHUNK_PLAINTEXTS[0]
        assert await dedup_file.read(4096, 4096) == _CHUNK_PLAINTEXTS[1]

    async def test_reads_across_multiple_data_chunks(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(2048, 4096)  # spans the boundary between chunk 0 and chunk 1
        assert result == _CHUNK_PLAINTEXTS[0][2048:] + _CHUNK_PLAINTEXTS[1][:2048]

    async def test_reads_zero_region_as_zero_bytes(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(12288, 8192)
        assert result == b"\x00" * 8192

    async def test_reads_hole_region_as_zero_bytes(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(20480, 4096)
        assert result == b"\x00" * 4096

    async def test_reads_trailing_hole_past_last_record(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(40960, 4096) == b"\x00" * 4096

    async def test_reads_repeated_template_correctly(self, dedup_file: DedupFile) -> None:
        # map_num=2, repeat=1 at offset 24576: chunks [3, 4, 3, 4]
        result = await dedup_file.read(24576, 4 * 4096)
        expected = _CHUNK_PLAINTEXTS[3] + _CHUNK_PLAINTEXTS[4] + _CHUNK_PLAINTEXTS[3] + _CHUNK_PLAINTEXTS[4]
        assert result == expected

    async def test_reads_a_sub_range_within_a_repeated_chunk(self, dedup_file: DedupFile) -> None:
        # bytes [100, 200) of the 3rd repeated chunk (index 2 -> chunk 3 again)
        offset = 24576 + 2 * 4096 + 100
        result = await dedup_file.read(offset, 100)
        assert result == _CHUNK_PLAINTEXTS[3][100:200]

    async def test_reads_spanning_data_zero_and_hole(self, dedup_file: DedupFile) -> None:
        # last 100 bytes of chunk 2 (DATA) + all of ZERO + all of HOLE + first
        # 100 bytes of the next DATA region
        start = 3 * 4096 - 100
        length = 100 + 8192 + 4096 + 100
        result = await dedup_file.read(start, length)
        expected = _CHUNK_PLAINTEXTS[2][-100:] + b"\x00" * (8192 + 4096) + _CHUNK_PLAINTEXTS[3][:100]
        assert result == expected

    async def test_default_length_reads_to_end_of_size(self, dedup_file: DedupFile) -> None:
        result = await dedup_file.read(40960)
        assert result == b"\x00" * 4096

    async def test_zero_length_read_returns_empty(self, dedup_file: DedupFile) -> None:
        assert await dedup_file.read(0, 0) == b""

    async def test_over_length_read_clamps_to_size_instead_of_zero_padding(self, dedup_file: DedupFile) -> None:
        # A request extending past `size` returns only the bytes that
        # exist (here, the trailing HOLE from 40960 to the 45056-byte
        # end) rather than silently zero-padding further past the file's
        # own declared end.
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
        _write_standard_composition(tmp_path / "Composition")
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, _HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="size is unknown"):
            await unsized.read(0)


class TestReadAcrossBuckets:
    """``_fill_data_extent()``'s cross-bucket batch path — see that
    function's own docstring: more than one distinct chunk needed within
    a single ``read()`` call is grouped by ``(stream_id, bucket_id)`` and
    fetched via one ``BucketReader.read_chunks()`` call per bucket instead
    of one ``Pool.read_chunk()`` per chunk."""

    async def test_read_spanning_two_buckets_via_a_single_extents_carry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One DATA extent whose own ``map_num=2`` run carries from
        (bucket 0, chunk 1) into (bucket 1, chunk 0) —
        ``ChunkAddress.advance()``'s carry semantics, exercised here for real through
        one ``_fill_data_extent()`` call needing two distinct chunks in
        two different buckets. ``BUCKET_MAX_CHUNK_NUM`` is monkeypatched
        down to 2 so this needs only two real chunks on disk, not a real
        8192-chunk bucket."""
        monkeypatch.setattr(addressing, "BUCKET_MAX_CHUNK_NUM", 2)
        entries = _mapping_record(0, 0, 1, map_num=2)
        record_bytes = _record_head_bytes(map_num=1) + entries
        comp_path = tmp_path / "Composition" / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(_composition_header_bytes() + record_bytes)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS[:2])
        bucket_1_chunk0 = bytes([200]) * 4096
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bucket_1_chunk0])

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=2 * 4096)

        calls: list[int] = []
        real_read_chunk = Pool.read_chunk

        async def counting_read_chunk(self: Pool, addr: object, **kwargs: object) -> bytes:
            calls.append(1)
            return await real_read_chunk(self, addr, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(Pool, "read_chunk", counting_read_chunk)

        result = await file.read(0, 2 * 4096)
        assert result == _CHUNK_PLAINTEXTS[1] + bucket_1_chunk0
        # 2 distinct chunks needed -> the batch path (BucketReader.read_chunks()
        # per bucket), never Pool.read_chunk() -- and it bypasses Pool's own
        # chunk cache the same way cache=False already does.
        assert calls == []
        assert pool._chunks == {}  # asserting the documented cache-bypass directly

    async def test_read_spanning_two_separate_bucket_backed_extents(self, tmp_path: Path) -> None:
        # bucket 0: chunk 0 at [0, 4096); bucket 1: chunk 0 at [4096, 8192)
        # -- two separate DATA extents rather than one carrying run, so
        # each individually takes _fill_data_extent()'s single-chunk fast
        # path; what this test asserts is that the right *bucket* gets
        # resolved for each, end to end through one read() call spanning
        # both extents.
        entries = _mapping_record(0, 0, 0, map_num=1) + _mapping_record(4096, 1, 0, map_num=1)
        record_bytes = _record_head_bytes(map_num=2) + entries
        comp_path = tmp_path / "Composition" / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(_composition_header_bytes() + record_bytes)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", [_CHUNK_PLAINTEXTS[0]])
        bucket_1_chunk0 = bytes([200]) * 4096
        _write_bucket(tmp_path / "Pool" / "0" / "1.buk", [bucket_1_chunk0])

        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=8192)

        result = await file.read(0, 8192)
        assert result == _CHUNK_PLAINTEXTS[0] + bucket_1_chunk0


class TestStream:
    async def test_stream_reassembles_to_the_same_bytes_as_read(self, dedup_file: DedupFile) -> None:
        whole = await dedup_file.read(0, _SIZE)
        reassembled = b"".join([chunk async for _offset, chunk in dedup_file.stream(block=1000)])
        assert reassembled == whole

    async def test_stream_offsets_are_contiguous_and_correct(self, dedup_file: DedupFile) -> None:
        offsets = [offset async for offset, _chunk in dedup_file.stream(block=8192)]
        assert offsets == list(range(0, _SIZE, 8192))

    async def test_stream_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Cancelled via the surrounding Task, not a ``cancel=``
        parameter (see ``_BlockingStore``) — must propagate
        ``asyncio.CancelledError`` out of the async generator's
        consumer."""
        file, store = _build_blocking_dedup_file(tmp_path)

        async def consume() -> list[int]:
            store.armed = True
            return [offset async for offset, _chunk in file.stream(block=1000)]

        task = asyncio.create_task(consume())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_stream_requires_known_size(self, tmp_path: Path) -> None:
        _write_standard_composition(tmp_path / "Composition")
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, _HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await anext(unsized.stream())


class TestExportTo:
    async def test_sparse_export_matches_read_content(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        result = await dedup_file.export_to(dst, sparse=True)
        assert dst.stat().st_size == _SIZE
        assert dst.read_bytes() == await dedup_file.read(0, _SIZE)
        assert result.logical_size == _SIZE
        assert result.bytes_written == 3 * 4096 + 4 * 4096
        assert result.zeros == 2 * 4096
        assert result.holes == 4096 + 4096

    async def test_non_sparse_export_matches_read_content(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        dst = tmp_path / "out.bin"
        result = await dedup_file.export_to(dst, sparse=False)
        assert dst.stat().st_size == _SIZE
        assert dst.read_bytes() == await dedup_file.read(0, _SIZE)
        assert result.bytes_written == 3 * 4096 + 4 * 4096

    async def test_export_reports_progress(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        calls: list[tuple[int, int]] = []

        async def _progress(done: int, total: int) -> None:
            calls.append((done, total))

        await dedup_file.export_to(tmp_path / "out.bin", progress=_progress)
        assert calls  # at least one progress callback for the DATA extents
        # Denominator is the real DATA-byte total, not the logical size —
        # holes/zeros never count toward either side of the progress
        # fraction (same convention physical order already used).
        assert calls[-1] == (3 * 4096 + 4 * 4096, 3 * 4096 + 4 * 4096)

    async def test_export_is_cancellable_and_still_closes_its_partial_output(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancelled via the surrounding Task, not a ``cancel=``
        parameter (see ``_BlockingStore``). The partial destination
        file must not leak an open fd: ``export_scheduler.export_to()``
        closes it in ``finally``, which runs on
        ``asyncio.CancelledError`` too."""
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

        async def do_export() -> object:
            store.armed = True
            return await file.export_to(dst)

        task = asyncio.create_task(do_export())
        await store.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(opened_fds) == 1  # the export really did get as far as opening its output
        assert closed_fds == opened_fds  # ...and the cancelled export still closed it
        assert dst.exists()

    async def test_export_clips_a_trailing_data_extent_that_runs_past_size(self, tmp_path: Path) -> None:
        """``extents()`` doesn't truncate its own boundary entries to
        size (see its own docstring), so the logical-order export walk
        must clip a trailing DATA extent that runs past the file's declared size —
        reproduced here by cutting the standard fixture's size 100 bytes
        into its last DATA extent (``[24576, 40960)``, not the trailing
        HOLE) instead of using the fixture's own exactly-chunk-aligned
        ``_SIZE``.
        """
        truncated_size = 40960 - 100
        _write_standard_composition(tmp_path / "Composition")
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=truncated_size)

        dst = tmp_path / "out.bin"
        result = await file.export_to(dst)

        # [0,12288)=DATA, [12288,20480)=ZERO, [20480,24576)=HOLE,
        # [24576,40960)=DATA clipped to 40860 -> 16284 of its normal 16384.
        assert dst.stat().st_size == truncated_size
        assert result.zeros == 2 * 4096
        assert result.holes == 4096
        assert result.bytes_written == 12288 + 16284
        assert result.bytes_written == truncated_size - result.zeros - result.holes
        assert dst.read_bytes() == await file.read(0, truncated_size)

    async def test_export_requires_known_size(self, tmp_path: Path) -> None:
        _write_standard_composition(tmp_path / "Composition")
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", _CHUNK_PLAINTEXTS)
        store = LocalFsStore(tmp_path)
        dir_cache = DirCache(store)
        comp_reader = CompositionReader(store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(store, "Pool", dir_cache)
        unsized = DedupFile(comp_reader, pool, _HEAD_OFF, size=None)
        with pytest.raises(ValueError, match="known size"):
            await unsized.export_to(tmp_path / "out.bin")


class TestSupportsConcurrentExport:
    async def test_dedup_file_supports_concurrent_export(self, dedup_file: DedupFile) -> None:
        assert dedup_file.supports_concurrent_export is True

    async def test_byte_range_view_supports_concurrent_export(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(24576, 4 * 4096)
        assert view.supports_concurrent_export is True


class TestByteRangeView:
    async def test_base_and_offset_properties(self, dedup_file: DedupFile) -> None:
        """``export_scheduler.py``'s bucket-major planning needs a view's
        own base file and window start to schedule against the base
        file's ``Pool``."""
        view = dedup_file.view(24576, 4 * 4096)
        assert view.base is dedup_file
        assert view.offset == 24576

    async def test_read_translates_coordinates(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(24576, 4 * 4096)  # the repeated-template DATA region
        assert view.size == 4 * 4096
        assert await view.read(0, 4096) == _CHUNK_PLAINTEXTS[3]
        assert await view.read(4096, 4096) == _CHUNK_PLAINTEXTS[4]

    async def test_read_default_length_reads_to_view_end(self, dedup_file: DedupFile) -> None:
        view = dedup_file.view(0, 4096)
        assert await view.read(0) == _CHUNK_PLAINTEXTS[0]

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
        assert reassembled == b"".join(_CHUNK_PLAINTEXTS[0:3])

    async def test_stream_is_cancellable_via_its_surrounding_task(self, tmp_path: Path) -> None:
        """Same cancellation contract as ``TestStream``'s own test — a
        view's stream must propagate ``asyncio.CancelledError``
        just like the whole file's does."""
        file, store = _build_blocking_dedup_file(tmp_path)
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
        result = await view.export_to(dst, sparse=False)
        assert dst.stat().st_size == 4 * 4096
        assert dst.read_bytes() == await view.read(0, 4 * 4096)
        assert result.logical_size == 4 * 4096
        assert result.bytes_written == 4 * 4096

    async def test_export_to_clips_a_window_ending_mid_chunk(self, dedup_file: DedupFile, tmp_path: Path) -> None:
        """Same trailing-extent clipping as ``TestExportTo``'s (the
        underlying logical-order walk behaves identically for both
        callers), for a view — the FS/SaaS common case: a file
        window into a shared image, ending mid-chunk (the normal case
        for a real file size)."""
        view = dedup_file.view(24576, 4 * 4096 - 100)
        dst = tmp_path / "view.bin"
        result = await view.export_to(dst)
        assert dst.stat().st_size == 4 * 4096 - 100
        assert dst.read_bytes() == await view.read(0, 4 * 4096 - 100)
        assert result.bytes_written == 4 * 4096 - 100


class TestBinarySearchPerformance:
    async def test_read_near_the_end_of_a_large_map_does_not_linear_scan(self, tmp_path: Path) -> None:
        # 2000 sequential 1-chunk MAPPING records; reading near the very
        # end must resolve via CompositionRecord's binary search, not a
        # linear walk from record 0 — verified indirectly via a read-count
        # ceiling, mirroring test_dedup_composition_reader.py's approach.
        n = 2000
        parts = []
        for i in range(n):
            # every record points at the same single chunk (bucket 0, chunk
            # 0) — only file_chunk_idx (hence file_offset) varies, which is
            # all this test needs to exercise the binary search.
            addr = ChunkAddress(StreamId(0), BucketId(0), ChunkIdx(0)).to_int()
            parts.append(
                _chunk_map_record_bytes(
                    kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=i, addr_int=addr, tail_u32=(1 << 16)
                )
            )
        record_bytes = _record_head_bytes(map_num=n) + b"".join(parts)
        comp_path = tmp_path / "Composition" / str(_STREAM_ID) / f"{_SESSION_ID}.com" / "c0"
        comp_path.parent.mkdir(parents=True, exist_ok=True)
        comp_path.write_bytes(_composition_header_bytes() + record_bytes)
        _write_bucket(tmp_path / "Pool" / "0" / "0.buk", [bytes([0]) * 4096] * 1)

        store = LocalFsStore(tmp_path)
        counting_store = _CountingStore(store)
        dir_cache = DirCache(counting_store)
        comp_reader = CompositionReader(counting_store, dir_cache, "Composition", _STREAM_ID, _SESSION_ID)
        pool = Pool(counting_store, "Pool", dir_cache)
        file = DedupFile(comp_reader, pool, _HEAD_OFF, size=n * 4096)

        target_offset = (n - 1) * 4096
        reads_before = counting_store.read_count
        await file.read(target_offset, 4096)
        reads_after = counting_store.read_count
        assert reads_after - reads_before < 25


class _CountingStore:
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
