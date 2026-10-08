"""Unit tests for ``synology_apm_repo.sdk.dedup.pool._bucket_reader`` — synthetic
buckets written to real files, opened through ``Pool`` or ``BucketReader.open``."""

from __future__ import annotations

import array
import asyncio
import contextlib
import dataclasses
import os
import random
import struct
import zlib
from pathlib import Path
from typing import Any, ClassVar

import pytest
import zstandard

from support.fakes import unchecked_fake
from support.format_builders import encode_size_store, sizestore_region_pad
from support.repo_builders import write_bucket
from support.store_fakes import WrappingStore
from synology_apm_repo.sdk.dedup.pool import BucketReader, Pool
from synology_apm_repo.sdk.dedup.pool._bucket_reader import _GAP_TOLERANCE, _IndexRun
from synology_apm_repo.sdk.errors import (
    ChunkCompactedError,
    DataCorruptError,
    FormatError,
    KeyRequiredError,
    NotFoundError,
)
from synology_apm_repo.sdk.format import compression
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS, BucketIndex, SizeStoreEntry
from synology_apm_repo.sdk.format.compression import _ZSTD_BATCH_THREADS_MIN_ENTRIES, CompressType
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.recording import TraceEvent, TracingStore
from unit.sdk.pool_fakes import (
    CIPHERTEXT_CRC,
    PLAINTEXTS,
    encrypted_pool_at,
    plaintext_pool_at,
    pool_at,
    pool_chunk_addr,
    write_bucket_with_real_size_store_redundancy,
)


@pytest.fixture
def plaintext_pool(tmp_path: Path) -> Pool:
    return plaintext_pool_at(tmp_path)


@pytest.fixture
def encrypted_pool(tmp_path: Path) -> tuple[Pool, bytes]:
    return encrypted_pool_at(tmp_path)


class TestLegacyUncompressedBucket:
    async def test_open_synthesizes_none_type_size_store_entries(self, tmp_path: Path) -> None:
        """The uncompressed layout (``MODE_COMPRESS`` unset) has no SizeStore
        region, so every chunk is synthesized as ``CompressType.NONE`` at a
        fixed stride."""
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", 0)  # mode: no MODE_COMPRESS
        header[12:16] = struct.pack(">I", 2)  # chunk_num
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        path.write_bytes(bytes(header))
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        reader = await pool.bucket(StreamId(5), BucketId(0))
        assert reader.header.is_compressed is False
        assert list(reader.index) == [SizeStoreEntry(CompressType.NONE, 0)] * 2


class TestDecodeRawChunk:
    """``decode_raw_chunk``: ``read_chunk``'s decrypt/decompress half, for a
    caller already holding a chunk's raw bytes from ``read_raw_chunks``."""

    async def test_matches_read_chunk_for_plaintext(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks(list(range(len(PLAINTEXTS))))

        for i, expected in enumerate(PLAINTEXTS):
            decoded = await reader.decode_raw_chunk(i, pool_chunk_addr(i), raw_by_chunk[i])
            assert decoded == expected == await plaintext_pool.read_chunk(pool_chunk_addr(i))

    async def test_matches_read_chunk_for_encrypted(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        reader = await pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks(list(range(len(PLAINTEXTS))))

        for i, expected in enumerate(PLAINTEXTS):
            decoded = await reader.decode_raw_chunk(i, pool_chunk_addr(i), raw_by_chunk[i])
            assert decoded == expected == await pool.read_chunk(pool_chunk_addr(i))

    async def test_a_reader_opened_with_ciphertext_crc_raises_on_mismatch(self, tmp_path: Path) -> None:
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        reader = await pool_at(tmp_path, verify=CIPHERTEXT_CRC).bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks([0])

        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.decode_raw_chunk(0, pool_chunk_addr(0), raw_by_chunk[0])

    async def test_a_reader_opened_without_it_skips_a_check_already_done_separately(self, tmp_path: Path) -> None:
        """A reader opened without ``verify_ciphertext_crc`` decodes a chunk
        whose CRC mismatch a separate ``verify_raw_chunk_ciphertext_crc``
        call already reported, without re-running that check."""
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        reader = await pool_at(tmp_path).bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks([0])

        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.verify_raw_chunk_ciphertext_crc(0, raw_by_chunk[0])
        decoded = await reader.decode_raw_chunk(0, pool_chunk_addr(0), raw_by_chunk[0])
        assert decoded == PLAINTEXTS[0]


class TestCompacted:
    async def test_compacted_chunk_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)

        entries = [(CompressType.COMPACTED.value, 0)]
        tight = encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 1)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = sizestore_region_pad(tight)

        from synology_apm_repo.sdk.format.redundancy import redundancy_size

        trailer = os.urandom(redundancy_size((1 * 15 + 7) >> 3, 256))  # 0 non-empty chunks -> no ChunkCrcStore bytes
        path.write_bytes(bytes(header) + sizestore_region + trailer)

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        with pytest.raises(ChunkCompactedError, match="is COMPACTED — reclaimed, cannot be recovered"):
            await pool.read_chunk(pool_chunk_addr(0))

    async def test_a_corrupt_chunk_is_reported_before_a_missing_key(self, tmp_path: Path) -> None:
        """With ciphertext-CRC checks on, a corrupt chunk is
        ``DataCorruptError`` even when no key was given."""
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=os.urandom(32),
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        reader = await BucketReader.open(LocalFsStore(tmp_path), "Pool/5/0.buk", verify_ciphertext_crc=True)

        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.read_chunks(StreamId(5), BucketId(0), [(0, 2)])

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``read_chunk`` raises on a COMPACTED slot before any data read: the
        slot's offset/length aren't meaningful to read."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0)]
        tight = encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 1)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = sizestore_region_pad(tight)
        path.write_bytes(bytes(header) + sizestore_region)

        store = LocalFsStore(tmp_path)

        read_calls = []
        original_read = store.read

        async def counting_read(read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if read_path == "Pool/5/0.buk" and offset >= 16384:  # past header/SizeStore, BucketReader.open()'s own read
                read_calls.append((offset, length))
            return await original_read(read_path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        with pytest.raises(ChunkCompactedError, match="is COMPACTED — reclaimed, cannot be recovered"):
            await reader.read_chunk(ChunkIdx(0), pool_chunk_addr(0))
        assert read_calls == []


def _reference_runs(located: list[tuple[int, int, int]]) -> list[list[tuple[int, int, int]]]:
    """The per-chunk run-merging rule, written plainly as an oracle for
    ``_plan_index_runs``: ``(chunk_idx, offset, length)`` items whose offset
    lies within ``_GAP_TOLERANCE`` after the previous item's end join its run."""
    runs: list[list[tuple[int, int, int]]] = []
    run_end = 0
    for item in located:
        if runs and 0 <= item[1] - run_end <= _GAP_TOLERANCE:
            runs[-1].append(item)
        else:
            runs.append([item])
        run_end = item[1] + item[2]
    return runs


class TestPlanIndexRuns:
    """``BucketReader._plan_index_runs`` groups chunks exactly as the
    plain per-chunk rule (``_reference_runs``) does."""

    @staticmethod
    def _reader_with(offsets: list[int], lengths: list[int]) -> BucketReader:
        reader = BucketReader.__new__(BucketReader)
        index = BucketIndex.uncompressed(len(offsets))
        reader.index = dataclasses.replace(
            index, offsets=array.array("I", offsets), effective_lens=array.array("H", lengths)
        )
        return reader

    def test_matches_the_per_chunk_planner(self) -> None:
        rng = random.Random(11)
        for _ in range(300):
            count = rng.randrange(1, 60)
            offsets, lengths, cursor = [], [], 0
            for _ in range(count):
                cursor += rng.choice([0, 0, 0, 1, _GAP_TOLERANCE, _GAP_TOLERANCE + 1])
                length = rng.randrange(1, 4097)
                offsets.append(cursor)
                lengths.append(length)
                cursor += length
            reader = self._reader_with(offsets, lengths)
            picked = sorted(rng.sample(range(count), rng.randrange(1, count + 1)))
            ranges: list[tuple[int, int]] = []
            for idx in picked:
                if ranges and ranges[-1][0] + ranges[-1][1] == idx:
                    ranges[-1] = (ranges[-1][0], ranges[-1][1] + 1)
                else:
                    ranges.append((idx, 1))

            planned = reader._plan_index_runs(ranges)
            expected = _reference_runs([(i, offsets[i], lengths[i]) for i in picked])

            assert [[i for a, b in run.chunks for i in range(a, b)] for run in planned] == [
                [item[0] for item in run] for run in expected
            ]
            assert [(run.start, run.end) for run in planned] == [
                (run[0][1], run[-1][1] + run[-1][2]) for run in expected
            ]
            assert all(isinstance(run, _IndexRun) for run in planned)


class TestNonCompactedChunkRanges:
    """``BucketReader.non_compacted_chunk_ranges()``, read straight off the
    index's compress-type array."""

    async def test_skips_compacted_slots(self, tmp_path: Path) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(4)]
        path = tmp_path / "Pool" / "5" / "0.buk"
        write_bucket(
            path, plaintexts, stream_id=5, bucket_id=0, compress_types=[CompressType.NONE] * 4, chunk_crc_store=False
        )

        # write_bucket() doesn't support COMPACTED: patch slot 1's SizeStore
        # entry afterwards (only a mixed SizeStore matters here).
        raw = bytearray(path.read_bytes())
        entries = [(CompressType.NONE.value, 0)] * 4
        entries[1] = (CompressType.COMPACTED.value, 0)
        tight = encode_size_store(entries)
        raw[64 : 64 + len(tight)] = tight
        raw[16:20] = struct.pack(">I", zlib.crc32(tight) & 0xFFFFFFFF)
        raw[60:64] = (zlib.crc32(bytes(raw[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        path.write_bytes(bytes(raw))

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        assert reader.non_compacted_chunk_ranges() == [(0, 1), (2, 2)]

    async def test_empty_when_every_chunk_is_compacted(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        write_bucket(
            path, [b"\x00" * 4096], stream_id=5, bucket_id=0, compress_types=[CompressType.NONE], chunk_crc_store=False
        )
        raw = bytearray(path.read_bytes())
        tight = encode_size_store([(CompressType.COMPACTED.value, 0)])
        raw[64 : 64 + len(tight)] = tight
        raw[16:20] = struct.pack(">I", zlib.crc32(tight) & 0xFFFFFFFF)
        raw[60:64] = (zlib.crc32(bytes(raw[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        path.write_bytes(bytes(raw))

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        assert reader.non_compacted_chunk_ranges() == []


def _fits_in_run(prev_end: int, offset: int) -> bool:
    """Whether ``_plan_index_runs`` merges a chunk starting at ``offset``
    into a run ending at ``prev_end``."""
    # The run up to prev_end is built from 4 KiB chunks, as a real bucket's is.
    lengths = [4096] * (prev_end // 4096) + ([prev_end % 4096] if prev_end % 4096 else [])
    offsets = [i * 4096 for i in range(len(lengths))]
    reader = TestPlanIndexRuns._reader_with([*offsets, offset], [*lengths, 1])
    return len(reader._plan_index_runs([(0, len(lengths) + 1)])) == 1


class TestFitsInRun:
    """``BucketReader._plan_index_runs``'s gap-tolerance boundary; a merged run has no size cap."""

    @pytest.mark.parametrize(
        ("prev_end", "next_start", "expected"),
        [
            pytest.param(150, 150, True, id="zero_gap_fits"),
            pytest.param(150, 150 + _GAP_TOLERANCE, True, id="gap_within_tolerance_fits"),  # exactly at the tolerance
            pytest.param(150, 150 + _GAP_TOLERANCE + 1, False, id="gap_beyond_tolerance_does_not_fit"),  # one byte past
            # An offset behind the run's end never merges backwards.
            pytest.param(250, 100, False, id="negative_gap_does_not_fit"),
        ],
    )
    def test_fits_in_run(self, prev_end: int, next_start: int, expected: bool) -> None:
        assert _fits_in_run(prev_end, next_start) is expected

    def test_a_32_mib_zero_gap_run_still_fits(self) -> None:
        assert _fits_in_run(32 << 20, 32 << 20) is True


class TestPlanRunsEdgeCases:
    """``BucketReader._plan_index_runs`` on hand-picked layouts (no ``.buk``
    file, no I/O)."""

    def test_empty_input_returns_no_runs(self) -> None:
        assert TestPlanIndexRuns._reader_with([0], [100])._plan_index_runs([]) == []

    def test_adjacent_chunks_merge_into_one_run(self) -> None:
        reader = TestPlanIndexRuns._reader_with([0, 100], [100, 100])
        assert reader._plan_index_runs([(0, 2)]) == [_IndexRun(0, 200, [(0, 2)])]

    def test_a_gap_beyond_tolerance_starts_a_new_run(self) -> None:
        reader = TestPlanIndexRuns._reader_with([0, 100 + _GAP_TOLERANCE + 1], [100, 100])
        assert reader._plan_index_runs([(0, 2)]) == [
            _IndexRun(0, 100, [(0, 1)]),
            _IndexRun(100 + _GAP_TOLERANCE + 1, 200 + _GAP_TOLERANCE + 1, [(1, 2)]),
        ]


class TestReadChunksBatch:
    """``BucketReader.read_chunks``/``Pool`` end-to-end via real
    ``.buk`` files (the merged multi-chunk pread)."""

    async def test_empty_requests_returns_empty_dict(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert await reader.read_chunks(StreamId(5), BucketId(0), []) == {}

    async def test_matches_read_chunk_for_every_chunk_plaintext(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        requests = [(0, len(PLAINTEXTS))]
        result = await reader.read_chunks(StreamId(5), BucketId(0), requests)
        assert result == dict(enumerate(PLAINTEXTS))

    async def test_matches_read_chunk_for_a_subset_skipping_the_middle_one(self, plaintext_pool: Pool) -> None:
        # Chunks 0 and 2 share one merged run across unrequested chunk 1's
        # bytes, a gap well within _GAP_TOLERANCE.
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        result = await reader.read_chunks(StreamId(5), BucketId(0), [(0, 1), (2, 1)])
        assert result == {0: PLAINTEXTS[0], 2: PLAINTEXTS[2]}

    async def test_matches_read_chunk_for_every_chunk_encrypted(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        reader = await pool.bucket(StreamId(5), BucketId(0))
        requests = [(0, len(PLAINTEXTS))]
        result = await reader.read_chunks(StreamId(5), BucketId(0), requests)
        assert result == dict(enumerate(PLAINTEXTS))

    async def test_missing_vault_key_raises_key_required(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=vault_key,
            chunk_crc_store=False,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))  # no vault_key supplied
        reader = await pool.bucket(StreamId(5), BucketId(0))

        with pytest.raises(KeyRequiredError, match="is encrypted but no vault key was provided"):
            await reader.read_chunks(StreamId(5), BucketId(0), [(0, 1)])

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # read_chunks() checks every request for COMPACTED before planning
        # or issuing any read.
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0), (CompressType.ZSTD.value, 100)]
        # Chunk 1 has no payload on disk; it must never be read.
        tight = encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 2)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = sizestore_region_pad(tight)
        path.write_bytes(bytes(header) + sizestore_region)

        store = LocalFsStore(tmp_path)

        read_calls = []
        original_read = store.read

        async def counting_read(read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if (
                read_path == "Pool/5/0.buk" and offset >= 16384
            ):  # past the header/SizeStore region BucketReader.open() itself reads
                read_calls.append((offset, length))
            return await original_read(read_path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        with pytest.raises(ChunkCompactedError, match="is COMPACTED — reclaimed, cannot be recovered"):
            await reader.read_chunks(StreamId(5), BucketId(0), [(0, 2)])
        assert read_calls == []


class TestReadChunksThreadHops:
    """A ``SyncReadable`` store's data is read on the decode thread; any other
    store (a ``TracingStore`` wrapper included) is read through ``read``."""

    async def test_a_sync_readable_store_is_never_read_asynchronously_for_data(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        reader = await Pool(store, "Pool", DirCache(store)).bucket(StreamId(5), BucketId(0))

        async def no_async_reads(*args: object, **kwargs: object) -> bytes:
            raise AssertionError("chunk data must come through read_sync")

        monkeypatch.setattr(store, "read", no_async_reads)

        result = await reader.read_chunks(StreamId(5), BucketId(0), [(0, len(PLAINTEXTS))])
        assert result == dict(enumerate(PLAINTEXTS))

    async def test_a_traced_store_still_sees_every_data_read(self, tmp_path: Path) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        events: list[TraceEvent] = []
        store = TracingStore(LocalFsStore(tmp_path), events.append)
        reader = await Pool(store, "Pool", DirCache(store)).bucket(StreamId(5), BucketId(0))
        events.clear()

        result = await reader.read_chunks(StreamId(5), BucketId(0), [(0, len(PLAINTEXTS))])

        assert result == dict(enumerate(PLAINTEXTS))
        assert [event.method for event in events] == ["read"]


class TestReadChunksMerging:
    """The I/O reduction itself: a spy on the real ``LocalFsStore`` counts
    data reads against the ``.buk`` file."""

    async def test_requesting_every_chunk_issues_one_merged_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(
            StreamId(5), BucketId(0)
        )  # BucketReader.open() already did its own header/SizeStore read

        data_reads = []
        original_read_sync = store.read_sync

        def counting_read_sync(path: str, offset: int = 0, length: int | None = None) -> bytes:
            # read_chunks reads a LocalFsStore through read_sync (one thread hop per run).
            if offset >= 16384:  # past the header/SizeStore region
                data_reads.append((offset, length))
            return original_read_sync(path, offset, length)

        monkeypatch.setattr(store, "read_sync", counting_read_sync)

        requests = [(0, len(PLAINTEXTS))]
        result = await reader.read_chunks(StreamId(5), BucketId(0), requests)

        assert result == dict(enumerate(PLAINTEXTS))
        assert len(data_reads) == 1  # 3 chunks, adjacent in the file -> one merged read, not three

    async def test_read_chunk_one_at_a_time_would_have_issued_three_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        data_reads = []
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            if offset >= 16384:
                data_reads.append((offset, length))
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        for i in range(len(PLAINTEXTS)):
            await pool.read_chunk(pool_chunk_addr(i))

        assert len(data_reads) == len(PLAINTEXTS)


class TestReadRawChunksBatch:
    """``BucketReader.read_raw_chunks`` (stored bytes, no decrypt/decompress),
    checked against the single-chunk ``read_raw_chunk`` as the oracle: stored
    bytes have no simpler independent expected value."""

    async def test_empty_indices_returns_empty_dict(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert await reader.read_raw_chunks([]) == {}

    async def test_matches_read_raw_chunk_for_every_chunk(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        expected = {i: await reader.read_raw_chunk(ChunkIdx(i)) for i in range(len(PLAINTEXTS))}
        result = await reader.read_raw_chunks(list(range(len(PLAINTEXTS))))
        assert {i: bytes(v) for i, v in result.items()} == expected

    async def test_matches_read_raw_chunk_for_a_subset_skipping_the_middle_one(self, plaintext_pool: Pool) -> None:
        # Chunk 1's bytes sit between 0 and 2 in the file, unrequested.
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        expected = {0: await reader.read_raw_chunk(ChunkIdx(0)), 2: await reader.read_raw_chunk(ChunkIdx(2))}
        result = await reader.read_raw_chunks([0, 2])
        assert {i: bytes(v) for i, v in result.items()} == expected

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0), (CompressType.ZSTD.value, 100)]
        tight = encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 2)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = sizestore_region_pad(tight)
        path.write_bytes(bytes(header) + sizestore_region)

        store = LocalFsStore(tmp_path)

        read_calls = []
        original_read = store.read

        async def counting_read(read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if read_path == "Pool/5/0.buk" and offset >= 16384:
                read_calls.append((offset, length))
            return await original_read(read_path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        with pytest.raises(ChunkCompactedError, match="is COMPACTED — reclaimed, cannot be recovered"):
            await reader.read_raw_chunks([0, 1])
        assert read_calls == []


class TestReadRawChunksMerging:
    """``read_raw_chunks``'s I/O reduction, counted as in ``TestReadChunksMerging``."""

    async def test_requesting_every_chunk_issues_one_merged_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        data_reads = []
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            if offset >= 16384:
                data_reads.append((offset, length))
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        result = await reader.read_raw_chunks(list(range(len(PLAINTEXTS))))

        assert len(result) == len(PLAINTEXTS)
        assert len(data_reads) == 1  # 3 chunks, adjacent in the file -> one merged read, not three

    async def test_read_raw_chunk_one_at_a_time_would_have_issued_three_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        data_reads = []
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            if offset >= 16384:
                data_reads.append((offset, length))
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        for i in range(len(PLAINTEXTS)):
            await reader.read_raw_chunk(ChunkIdx(i))

        assert len(data_reads) == len(PLAINTEXTS)


class TestReadChunksMixedCompressType:
    """``read_chunks`` decodes a merged run through ``decompress_many``, which
    groups by ``CompressType``: a run mixing NONE/LZ4/ZSTD chunks must keep
    each ``chunk_idx`` paired with its plaintext (oracle: ``Pool.read_chunk``)."""

    _MIXED_TYPES: ClassVar[list[CompressType]] = [
        CompressType.NONE,
        CompressType.ZSTD,
        CompressType.LZ4,
        CompressType.ZSTD,
        CompressType.NONE,
    ]
    _MIXED_PLAINTEXTS: ClassVar[list[bytes]] = [
        ((b"mixed-chunk-%d-content" % i) * 300)[:4096] for i in range(len(_MIXED_TYPES))
    ]

    async def test_matches_read_chunk_oracle_plaintext(self, tmp_path: Path) -> None:
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            self._MIXED_PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            compress_types=self._MIXED_TYPES,
            chunk_crc_store=False,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(0, len(self._MIXED_PLAINTEXTS))]
        batched = await reader.read_chunks(StreamId(5), BucketId(0), requests)

        for i, expected in enumerate(self._MIXED_PLAINTEXTS):
            assert batched[i] == expected
            assert await pool.read_chunk(pool_chunk_addr(i)) == expected

    async def test_matches_read_chunk_oracle_encrypted(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            self._MIXED_PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=vault_key,
            compress_types=self._MIXED_TYPES,
            chunk_crc_store=False,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), vault_key=vault_key)
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(0, len(self._MIXED_PLAINTEXTS))]
        batched = await reader.read_chunks(StreamId(5), BucketId(0), requests)

        for i, expected in enumerate(self._MIXED_PLAINTEXTS):
            assert batched[i] == expected
            assert await pool.read_chunk(pool_chunk_addr(i)) == expected

    async def test_large_all_zstd_batch_crosses_the_multithreaded_threshold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A run past ``_ZSTD_BATCH_THREADS_MIN_ENTRIES`` takes
        ``decompress_many``'s multi-threaded path and still pairs every
        ``chunk_idx`` with its plaintext."""
        threads_requested: list[int] = []

        @unchecked_fake("zstandard.ZstdDecompressor's multi_decompress_to_buffer")
        class _ThreadsRecordingDecompressor:
            def multi_decompress_to_buffer(self, frames: Any, **kwargs: Any) -> object:
                threads_requested.append(kwargs["threads"])
                return zstandard.ZstdDecompressor().multi_decompress_to_buffer(frames, **kwargs)

        monkeypatch.setattr(compression, "_zstd_decompressor", _ThreadsRecordingDecompressor)
        n = _ZSTD_BATCH_THREADS_MIN_ENTRIES + 50
        plaintexts = [((b"big-batch-chunk-%d-" % i) * 250)[:4096] for i in range(n)]
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", plaintexts, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(0, n)]
        batched = await reader.read_chunks(StreamId(5), BucketId(0), requests)

        assert batched == dict(enumerate(plaintexts))
        assert threads_requested and min(threads_requested) > 1


class _ParkFirstDataReadStore(WrappingStore):
    """Parks the *first* data-region read (offset past the 16384-byte
    header/SizeStore region) until ``_unblock()``; every other read proceeds
    immediately."""

    def __init__(self, backing: LocalFsStore) -> None:
        super().__init__(backing)
        self.data_read_offsets: list[int] = []
        self.parked = asyncio.Event()
        #: Set when a second data read starts.
        self.second_read_started = asyncio.Event()
        #: Whether the parked read had returned when the second one started.
        self.second_started_after_first_returned: bool | None = None
        self._release = asyncio.Event()
        self._parked_once = False
        self._first_returned = False

    def _unblock(self) -> None:
        self._release.set()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if offset >= 16384:
            self.data_read_offsets.append(offset)
            if not self._parked_once:
                self._parked_once = True
                self.parked.set()
                await self._release.wait()
                data = await self._backing.read(path, offset, length)
                self._first_returned = True
                return data
            if len(self.data_read_offsets) == 2:
                self.second_started_after_first_returned = self._first_returned
                self.second_read_started.set()
        return await self._backing.read(path, offset, length)


class TestReadChunksConcurrentReads:
    """``read_chunks``'s ``semaphore``: concurrency across one bucket's merged
    runs. With ``_GAP_TOLERANCE`` patched to 0, skipping chunk 1 splits the
    request into two runs. The fresh semaphores here always have spare
    permits; ``test_dedup_chunk_walk.py`` covers the pre-acquired permit
    hand-off from ``exec_chunks``."""

    async def test_output_is_unchanged_across_two_runs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._bucket_reader._GAP_TOLERANCE", 0)

        requests = [(0, 1), (2, 1)]  # skip chunk 1 -> forced 2-run split at tolerance=0
        result = await reader.read_chunks(StreamId(5), BucketId(0), requests, semaphore=asyncio.Semaphore(4))

        assert result == {0: PLAINTEXTS[0], 2: PLAINTEXTS[2]}

    async def test_the_two_runs_own_reads_actually_overlap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """While the first run's read is parked, the second run's read has
        already been issued."""
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        backing = LocalFsStore(tmp_path)
        pool = Pool(backing, "Pool", DirCache(backing))
        reader = await pool.bucket(
            StreamId(5), BucketId(0)
        )  # header already opened via ``backing``, before the store swap below
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._bucket_reader._GAP_TOLERANCE", 0)

        parking_store = _ParkFirstDataReadStore(backing)
        reader._store = parking_store

        requests = [(0, 1), (2, 1)]
        task = asyncio.create_task(
            reader.read_chunks(StreamId(5), BucketId(0), requests, semaphore=asyncio.Semaphore(4))
        )
        await parking_store.parked.wait()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(parking_store.second_read_started.wait(), timeout=5.0)
        assert len(parking_store.data_read_offsets) == 2, (
            f"expected both runs' reads to have started, got {parking_store.data_read_offsets!r}"
        )
        assert parking_store.second_started_after_first_returned is False
        parking_store._unblock()
        result = await task

        assert result == {0: PLAINTEXTS[0], 2: PLAINTEXTS[2]}

    async def test_no_semaphore_is_still_strictly_serial(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """With ``semaphore=None`` (the default), the second run's read waits
        for the first."""
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        backing = LocalFsStore(tmp_path)
        pool = Pool(backing, "Pool", DirCache(backing))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._bucket_reader._GAP_TOLERANCE", 0)

        parking_store = _ParkFirstDataReadStore(backing)
        reader._store = parking_store

        requests = [(0, 1), (2, 1)]
        task = asyncio.create_task(reader.read_chunks(StreamId(5), BucketId(0), requests, semaphore=None))
        await parking_store.parked.wait()
        parking_store._unblock()
        result = await task

        assert result == {0: PLAINTEXTS[0], 2: PLAINTEXTS[2]}
        assert len(parking_store.data_read_offsets) == 2
        assert parking_store.second_started_after_first_returned is True


class TestSizeStoreParityRepair:
    """``BucketReader.open()``'s Redundancy-blob self-repair on a
    ``chunk_size_crc`` mismatch, which every open checks."""

    async def test_a_single_corrupted_byte_is_transparently_repaired(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        store = LocalFsStore(tmp_path)

        reader = await BucketReader.open(store, "Pool/5/0.buk")  # must not raise

        assert list(reader.index) == [
            SizeStoreEntry(CompressType.ZSTD, 100),
            SizeStoreEntry(CompressType.ZSTD, 200),
        ]
        assert reader.sizestore_repaired is True  # verify_checks.check_bucket_structure's own trigger

    async def test_a_normal_open_is_not_marked_repaired(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        write_bucket(path, [b"x" * 100], stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)

        reader = await BucketReader.open(store, "Pool/5/0.buk")

        assert reader.sizestore_repaired is False

    async def test_an_unrecoverable_corruption_still_raises(self, tmp_path: Path) -> None:
        """Corruption in two non-adjacent windows fails the repair's final CRC
        re-check, so the original ``DataCorruptError`` propagates."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100 + i) for i in range(400)]  # tight_len = 750: windows 0,1,2
        write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=None)
        # Windows 0 and 2: attempt_repair() reconstructs only an adjacent pair.
        raw = bytearray(path.read_bytes())
        sizestore_off = 64
        raw[sizestore_off] ^= 0xFF  # window 0: byte 0
        raw[sizestore_off + 600] ^= 0xFF  # window 2: byte 600 (600 // 256 == 2)
        path.write_bytes(bytes(raw))
        store = LocalFsStore(tmp_path)

        with pytest.raises(DataCorruptError, match="SizeStore CRC mismatch"):
            await BucketReader.open(store, "Pool/5/0.buk")


class _SizedStore(LocalFsStore):
    """A ``LocalFsStore`` whose ``size()`` reports ``size`` (or raises it), as
    when the file changed between the header read and the trailer probe."""

    def __init__(self, root: Path, size: int | BaseException) -> None:
        super().__init__(root)
        self._reported = size

    async def size(self, path: str) -> int:
        if isinstance(self._reported, BaseException):
            raise self._reported
        return self._reported


class TestSizeStoreRepairNeedsATrustworthyFile:
    """When the SizeStore CRC fails but the trailer can't be used, the original ``DataCorruptError``
    surfaces rather than a repair of unknown quality."""

    @staticmethod
    def _corrupt_bucket(tmp_path: Path) -> Path:
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100 + i) for i in range(400)]
        write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        return path

    async def test_a_file_truncated_inside_the_size_store_is_a_format_error(self, tmp_path: Path) -> None:
        path = self._corrupt_bucket(tmp_path)
        path.write_bytes(path.read_bytes()[: 64 + 100])

        with pytest.raises(FormatError, match="SizeStore data too short: 100 bytes < 750"):
            await BucketReader.open(LocalFsStore(tmp_path), "Pool/5/0.buk")

    async def test_a_file_whose_trailer_was_cut_off_cannot_be_repaired(self, tmp_path: Path) -> None:
        path = self._corrupt_bucket(tmp_path)
        path.write_bytes(path.read_bytes()[:16384])  # header + SizeStore region, no chunks, no trailer

        with pytest.raises(DataCorruptError, match="SizeStore CRC mismatch"):
            await BucketReader.open(LocalFsStore(tmp_path), "Pool/5/0.buk")

    @pytest.mark.parametrize("error", [NotFoundError("gone"), FormatError("unreadable")])
    async def test_a_size_lookup_that_fails_leaves_the_original_error(self, tmp_path: Path, error: Exception) -> None:
        self._corrupt_bucket(tmp_path)

        with pytest.raises(DataCorruptError, match="SizeStore CRC mismatch"):
            await BucketReader.open(_SizedStore(tmp_path, error), "Pool/5/0.buk")

    async def test_a_reported_size_smaller_than_the_trailer_leaves_the_original_error(self, tmp_path: Path) -> None:
        self._corrupt_bucket(tmp_path)

        with pytest.raises(DataCorruptError, match="SizeStore CRC mismatch"):
            await BucketReader.open(_SizedStore(tmp_path, 10), "Pool/5/0.buk")
