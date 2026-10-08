"""Unit tests for the ``synology_apm_repo.sdk.dedup.pool`` facade (``Pool``) — synthetic
buckets written to real files (so ``LocalFsStore``/``DirCache``
are exercised for real, not stubbed), no sample repositories required."""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import os
from pathlib import Path

import pytest

from support.repo_builders import write_bucket, write_inf
from support.store_fakes import WrappingStore
from synology_apm_repo.sdk.cachemanager import DEFAULT_LIMITS
from synology_apm_repo.sdk.dedup.pool import FULL_VERIFY, NO_VERIFY, BucketReader, BucketReaderCache, Pool, VerifyPolicy
from synology_apm_repo.sdk.errors import DataCorruptError, KeyRequiredError, NotFoundError
from synology_apm_repo.sdk.format.bucket import chunk_crc_store_region
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.base import Entry
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.pool_fakes import (
    CIPHERTEXT_CRC,
    PLAINTEXTS,
    chunk_address,
    encrypted_pool_at,
    plaintext_pool_at,
    pool_at,
    pool_chunk_addr,
    write_fgp,
)


class _CallLog(WrappingStore):
    """Logs every call's ``(method, path)`` in ``calls``."""

    def __init__(self, root: Path) -> None:
        super().__init__(LocalFsStore(root))
        self.calls: list[tuple[str, str]] = []

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.calls.append(("read", path))
        return await super().read(path, offset, length)

    async def size(self, path: str) -> int:
        self.calls.append(("size", path))
        return await super().size(path)

    async def exists(self, path: str) -> bool:
        self.calls.append(("exists", path))
        return await super().exists(path)

    async def listdir(self, path: str) -> list[Entry]:
        self.calls.append(("listdir", path))
        return await super().listdir(path)


@pytest.fixture
def plaintext_pool(tmp_path: Path) -> Pool:
    return plaintext_pool_at(tmp_path)


@pytest.fixture
def encrypted_pool(tmp_path: Path) -> tuple[Pool, bytes]:
    return encrypted_pool_at(tmp_path)


_FINGERPRINT = VerifyPolicy(fingerprint=True)


class TestBucketPathResolution:
    @pytest.mark.parametrize(
        "filename",
        [
            pytest.param("0.buk", id="bare_bucket_file"),
            # Real buckets almost always carry a .<seqId> suffix.
            pytest.param("0.buk.7", id="sequence_suffixed_bucket_file"),
        ],
    )
    async def test_resolves(self, tmp_path: Path, filename: str) -> None:
        write_bucket(tmp_path / "Pool" / "5" / filename, PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        assert await pool.bucket_path(StreamId(5), BucketId(0)) == f"Pool/5/{filename}"


class TestReadChunkPlaintext:
    async def test_reads_all_chunks_correctly(self, plaintext_pool: Pool) -> None:
        for i, expected in enumerate(PLAINTEXTS):
            assert await plaintext_pool.read_chunk(pool_chunk_addr(i)) == expected

    async def test_bucket_reader_reports_correct_header(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert reader.header.is_compressed is True
        assert reader.header.is_vault_encrypted is False
        assert reader.header.chunk_num == len(PLAINTEXTS)

    async def test_bucket_reader_is_cached(self, plaintext_pool: Pool) -> None:
        first = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        second = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert first is second

    async def test_a_chunk_cache_hit_returns_the_same_bytes_object(self, plaintext_pool: Pool) -> None:
        a = await plaintext_pool.read_chunk(pool_chunk_addr(0))
        b = await plaintext_pool.read_chunk(pool_chunk_addr(0))
        assert a == PLAINTEXTS[0]
        assert a is b


def _cached(pool: Pool, *chunk_indices: int) -> tuple[dict[int, bytes], list[int]]:
    return pool.cached_chunks(StreamId(5), BucketId(0), chunk_indices)


def _backfill(pool: Pool, chunk_idx: int, plain: bytes | memoryview) -> None:
    pool.backfill_chunks(StreamId(5), BucketId(0), {chunk_idx: plain})


class TestCachedChunkAndBackfill:
    """``cached_chunks``/``backfill_chunks``: the peek/insert pair
    ``dedup_file.py``'s multi-chunk batch path uses instead of ``read_chunk()``."""

    async def test_nothing_is_cached_before_anything_populates_it(self, plaintext_pool: Pool) -> None:
        assert _cached(plaintext_pool, 0, 1) == ({}, [0, 1])

    async def test_read_chunk_populates_what_cached_chunks_then_sees(self, plaintext_pool: Pool) -> None:
        await plaintext_pool.read_chunk(pool_chunk_addr(0))
        assert _cached(plaintext_pool, 0, 1) == ({0: PLAINTEXTS[0]}, [1])

    async def test_a_hit_counts_in_the_chunk_cache_stats(self, plaintext_pool: Pool) -> None:
        _backfill(plaintext_pool, 0, PLAINTEXTS[0])
        before = plaintext_pool._chunks.stats()

        _cached(plaintext_pool, 0, 1)

        after = plaintext_pool._chunks.stats()
        assert (after.hits, after.misses) == (before.hits + 1, before.misses + 1)

    async def test_a_backfilled_memoryview_is_stored_as_its_own_bytes(self, plaintext_pool: Pool) -> None:
        buffer = bytearray(PLAINTEXTS[0])
        _backfill(plaintext_pool, 0, memoryview(buffer))
        buffer[:] = bytes(len(buffer))
        hits, _ = _cached(plaintext_pool, 0)
        assert type(hits[0]) is bytes
        assert hits[0] == PLAINTEXTS[0]

    async def test_backfill_makes_a_later_read_chunk_a_cache_hit(
        self, plaintext_pool: Pool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _backfill(plaintext_pool, 0, PLAINTEXTS[0])
        assert _cached(plaintext_pool, 0) == ({0: PLAINTEXTS[0]}, [])

        async def exploding_bucket(
            stream_id: StreamId, bucket_id: BucketId, *, cache: BucketReaderCache | None = None
        ) -> BucketReader:
            raise AssertionError("read_chunk() must not open a bucket for an already-backfilled chunk")

        monkeypatch.setattr(plaintext_pool, "bucket", exploding_bucket)
        assert await plaintext_pool.read_chunk(pool_chunk_addr(0)) == PLAINTEXTS[0]


class TestVerifyFingerprint:
    """A ``Pool`` built with ``VerifyPolicy(fingerprint=True)``. The check is
    not encryption-specific, so these use unencrypted buckets (``plaintext_pool``
    writes the bucket; ``pool_at`` reopens it with a policy)."""

    async def test_matching_fingerprint_succeeds(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        digest = hashlib.sha256(PLAINTEXTS[0]).digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", digest)
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        result = await pool.read_chunk(pool_chunk_addr(0))
        assert result == PLAINTEXTS[0]

    async def test_mismatched_fingerprint_raises_data_corrupt(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        wrong_digest = hashlib.sha256(b"not the real plaintext").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", wrong_digest)
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        with pytest.raises(DataCorruptError, match="chunk fingerprint mismatch at"):
            await pool.read_chunk(pool_chunk_addr(0))

    async def test_off_by_default_never_touches_inf_or_fgp_at_all(self, plaintext_pool: Pool) -> None:
        # No .inf/.fgp exists under this fixture's Pool root, so any attempted check would fail.
        result = await plaintext_pool.read_chunk(pool_chunk_addr(0))
        assert result == PLAINTEXTS[0]

    def test_verify_property_reports_the_construction_policy(self, tmp_path: Path) -> None:
        assert pool_at(tmp_path).verify == NO_VERIFY
        assert pool_at(tmp_path, verify=FULL_VERIFY).verify == FULL_VERIFY

    async def test_a_cache_hit_is_still_checked_not_skipped(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        wrong_digest = hashlib.sha256(b"wrong").digest()
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", wrong_digest)
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        # Populate the plaintext cache without any read (and so any check).
        _backfill(pool, 0, PLAINTEXTS[0])
        assert (5, 0, 0) in pool._chunks

        with pytest.raises(DataCorruptError, match="chunk fingerprint mismatch at"):
            await pool.read_chunk(pool_chunk_addr(0))


class TestVerifyFingerprints:
    """``Pool.verify_fingerprints``: the batch check ``BucketReader.read_chunks``'s
    result needs, since ``read_chunks`` bypasses ``read_chunk``'s own check."""

    async def test_matching_fingerprints_across_multiple_chunks_succeeds(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        write_fgp(
            tmp_path / "Pool" / "5" / "0_0.fgp",
            b"".join(hashlib.sha256(p).digest() for p in PLAINTEXTS),
        )
        chunks = dict(enumerate(PLAINTEXTS))
        store = _CallLog(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), verify=_FINGERPRINT)

        await pool.verify_fingerprints(StreamId(5), BucketId(0), chunks)
        assert {path for method, path in store.calls if method == "read"} == {"Pool/5/0.inf", "Pool/5/0_0.fgp"}

    async def test_one_wrong_fingerprint_among_several_raises_data_corrupt(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        digests = [hashlib.sha256(p).digest() for p in PLAINTEXTS]
        digests[1] = hashlib.sha256(b"wrong").digest()  # chunk 1's stored fingerprint is corrupt
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        chunks = dict(enumerate(PLAINTEXTS))
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        with pytest.raises(DataCorruptError, match="chunk fingerprint mismatch at"):
            await pool.verify_fingerprints(StreamId(5), BucketId(0), chunks)

    async def test_off_by_default_never_touches_inf_or_fgp_at_all(self, plaintext_pool: Pool, tmp_path: Path) -> None:
        chunks = dict(enumerate(PLAINTEXTS))
        store = _CallLog(tmp_path)
        await Pool(store, "Pool", DirCache(store)).verify_fingerprints(StreamId(5), BucketId(0), chunks)
        assert store.calls == []

    async def test_ciphertext_crc_alone_does_not_turn_on_fingerprints(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        chunks = dict(enumerate(PLAINTEXTS))
        store = _CallLog(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), verify=CIPHERTEXT_CRC)
        await pool.verify_fingerprints(StreamId(5), BucketId(0), chunks)
        assert store.calls == []

    async def test_read_chunks_batch_result_is_verified_once_fed_through_this_method(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        """The sequence ``chunk_walk.decode_bucket_chunks`` uses: ``BucketReader.read_chunks``
        (which never checks fingerprints), then ``verify_fingerprints`` on its result."""
        digests = [hashlib.sha256(p).digest() for p in PLAINTEXTS]
        digests[2] = hashlib.sha256(b"wrong").digest()  # chunk 2's stored fingerprint is corrupt
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        reader = await pool.bucket(StreamId(5), BucketId(0))
        requests = [(0, len(PLAINTEXTS))]
        decoded = await reader.read_chunks(
            StreamId(5), BucketId(0), requests
        )  # succeeds -- read_chunks() itself never checks fingerprints
        assert decoded == dict(enumerate(PLAINTEXTS))

        with pytest.raises(DataCorruptError, match="chunk fingerprint mismatch at"):
            await pool.verify_fingerprints(StreamId(5), BucketId(0), decoded)

    async def test_inf_header_and_allocation_table_are_read_once_for_every_chunk_in_one_bucket(
        self, tmp_path: Path, plaintext_pool: Pool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``verify_fingerprints`` reads a bucket's ``.inf`` header and
        allocation table once, however many of its chunks are checked."""
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        write_fgp(
            tmp_path / "Pool" / "5" / "0_0.fgp",
            b"".join(hashlib.sha256(p).digest() for p in PLAINTEXTS),
        )
        chunks = dict(enumerate(PLAINTEXTS))
        pool = pool_at(tmp_path, verify=_FINGERPRINT)

        inf_reads = 0
        original_read = pool._store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal inf_reads
            if path.endswith(".inf"):
                inf_reads += 1
            return await original_read(path, offset, length)

        monkeypatch.setattr(pool._store, "read", counting_read)

        await pool.verify_fingerprints(StreamId(5), BucketId(0), chunks)

        # Header + the group's allocation table, not 2 per chunk.
        assert inf_reads == 2


class TestVerifyCiphertextCrc:
    """A ``Pool`` built with ``VerifyPolicy(ciphertext_crc=True)`` (or a
    ``BucketReader`` opened with ``verify_ciphertext_crc=True``): the
    ``ChunkCrcStore`` check (FORMAT-SPEC.md §4.4), against a chunk's raw stored
    bytes before any decrypt/decompress. It needs no vault key, so these use
    unencrypted buckets."""

    async def test_matching_ciphertext_crc_succeeds(self, tmp_path: Path) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        pool = pool_at(tmp_path, verify=CIPHERTEXT_CRC)

        result = await pool.read_chunk(pool_chunk_addr(0))
        assert result == PLAINTEXTS[0]

    async def test_mismatched_ciphertext_crc_raises_data_corrupt(self, tmp_path: Path) -> None:
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        pool = pool_at(tmp_path, verify=CIPHERTEXT_CRC)

        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await pool.read_chunk(pool_chunk_addr(0))

    async def test_off_by_default_never_reads_the_trailer_at_all(self, plaintext_pool: Pool) -> None:
        # plaintext_pool writes its bucket with chunk_crc_store=False, so its
        # trailer is random filler and any attempted check would raise.
        result = await plaintext_pool.read_chunk(pool_chunk_addr(0))
        assert result == PLAINTEXTS[0]

    async def test_bucket_reader_opened_with_it_checks_read_chunk(self, tmp_path: Path) -> None:
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=1,
        )
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/5/0.buk", verify_ciphertext_crc=True)

        assert await reader.read_chunk(ChunkIdx(0), pool_chunk_addr(0)) == PLAINTEXTS[0]
        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.read_chunk(ChunkIdx(1), pool_chunk_addr(1))

    async def test_independent_of_fingerprint_each_togglable_alone_or_together(self, tmp_path: Path) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", hashlib.sha256(PLAINTEXTS[0]).digest())

        for policy in (CIPHERTEXT_CRC, _FINGERPRINT, FULL_VERIFY):
            assert await pool_at(tmp_path, verify=policy).read_chunk(pool_chunk_addr(0)) == PLAINTEXTS[0]

    async def test_read_chunks_batch_form_checks_every_requested_chunk(self, tmp_path: Path) -> None:
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=2,
        )
        reader = await pool_at(tmp_path, verify=CIPHERTEXT_CRC).bucket(StreamId(5), BucketId(0))

        requests = [(0, len(PLAINTEXTS))]
        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.read_chunks(StreamId(5), BucketId(0), requests)

    async def test_costs_exactly_one_trailer_read_regardless_of_chunk_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_bucket(tmp_path / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), verify=CIPHERTEXT_CRC)
        reader = await pool.bucket(StreamId(5), BucketId(0))
        trailer_offset, trailer_length = chunk_crc_store_region(reader.header, reader.index)

        trailer_reads = 0
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal trailer_reads
            if offset == trailer_offset and length == trailer_length:
                trailer_reads += 1
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        requests = [(0, len(PLAINTEXTS))]
        await reader.read_chunks(StreamId(5), BucketId(0), requests)

        assert trailer_reads == 1

    async def test_raw_batch_form_checks_every_requested_chunk(self, tmp_path: Path) -> None:
        """The ``read_raw_chunks`` + ``verify_raw_chunk_ciphertext_crc`` pairing
        ``verify_checks.check_chunk_ciphertext_crcs`` uses (no decrypt/decompress)."""
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=2,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        raw_by_chunk = await reader.read_raw_chunks(list(range(len(PLAINTEXTS))))
        await reader.verify_raw_chunk_ciphertext_crc(0, raw_by_chunk[0])
        await reader.verify_raw_chunk_ciphertext_crc(1, raw_by_chunk[1])
        with pytest.raises(DataCorruptError, match="chunk ciphertext CRC mismatch"):
            await reader.verify_raw_chunk_ciphertext_crc(2, raw_by_chunk[2])


class TestReadChunkEncrypted:
    async def test_reads_all_chunks_correctly(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        for i, expected in enumerate(PLAINTEXTS):
            assert await pool.read_chunk(pool_chunk_addr(i)) == expected

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

        with pytest.raises(KeyRequiredError, match="is encrypted but no vault key was provided"):
            await pool.read_chunk(pool_chunk_addr(0))

    async def test_a_wrong_vault_key_surfaces_as_a_zstd_decode_failure(self, tmp_path: Path) -> None:
        # AES-CTR has no integrity check, so a wrong key yields garbage that
        # lacks a zstd frame magic. Fixed keys: with random ones the garbage
        # could, rarely, start with that magic.
        write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=bytes(range(32)),
            chunk_crc_store=False,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), vault_key=bytes(range(1, 33)))

        with pytest.raises(DataCorruptError, match="zstd decompress failed"):
            await pool.read_chunk(pool_chunk_addr(0))


class TestCachedAesAlgorithm:
    """``Pool._cached_aes_algorithm``: built lazily on first use, reused by every
    ``BucketReader`` that ``Pool`` opens, and never shared with another ``Pool``,
    even one holding the same ``vault_key``."""

    async def test_not_built_until_first_needed(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        assert pool._aes_algorithm is None

    async def test_the_same_algorithm_object_is_reused_across_bucket_opens(
        self, encrypted_pool: tuple[Pool, bytes]
    ) -> None:
        # open_bucket_uncached bypasses Pool's BucketReader cache: two separate readers.
        pool, _vault_key = encrypted_pool
        first_reader = await pool.open_bucket_uncached(StreamId(5), BucketId(0))
        built = pool._aes_algorithm
        assert built is not None
        assert first_reader._algorithm is built

        second_reader = await pool.open_bucket_uncached(StreamId(5), BucketId(0))
        assert second_reader._algorithm is built
        assert pool._aes_algorithm is built

    async def test_two_pools_never_share_an_algorithm_object_even_with_the_same_key(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        write_bucket(
            tmp_path / "A" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=vault_key,
            chunk_crc_store=False,
        )
        write_bucket(
            tmp_path / "B" / "5" / "0.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=vault_key,
            chunk_crc_store=False,
        )
        store = LocalFsStore(tmp_path)
        pool_a = Pool(store, "A", DirCache(store), vault_key=vault_key)
        pool_b = Pool(store, "B", DirCache(store), vault_key=vault_key)

        reader_a = await pool_a.open_bucket_uncached(StreamId(5), BucketId(0))
        reader_b = await pool_b.open_bucket_uncached(StreamId(5), BucketId(0))

        assert reader_a._algorithm is not None
        assert reader_b._algorithm is not None
        assert reader_a._algorithm is not reader_b._algorithm
        assert await pool_a.read_chunk(pool_chunk_addr(0)) == PLAINTEXTS[0]
        assert await pool_b.read_chunk(pool_chunk_addr(0)) == PLAINTEXTS[0]

    async def test_an_unencrypted_pool_never_builds_an_algorithm(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.open_bucket_uncached(StreamId(5), BucketId(0))
        assert reader._algorithm is None
        assert plaintext_pool._aes_algorithm is None


class TestLruEviction:
    async def test_bucket_cache_evicts_least_recently_used(self, tmp_path: Path) -> None:
        for bucket_id in range(3):
            write_bucket(
                tmp_path / "Pool" / "5" / f"{bucket_id}.buk",
                PLAINTEXTS,
                stream_id=5,
                bucket_id=bucket_id,
                chunk_crc_store=False,
            )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), limits=dataclasses.replace(DEFAULT_LIMITS, bucket_readers=2))

        r0 = await pool.bucket(StreamId(5), BucketId(0))
        await pool.bucket(StreamId(5), BucketId(1))
        await pool.bucket(StreamId(5), BucketId(2))  # evicts bucket 0 (LRU), cache size stays at 2

        assert len(pool._buckets) == 2
        r0_again = await pool.bucket(StreamId(5), BucketId(0))
        assert r0_again is not r0  # had to be reopened

    async def test_bucket_and_chunk_cache_maxsize_round_trip(self, plaintext_pool: Pool) -> None:
        pool = plaintext_pool
        assert pool._buckets.maxsize == 16  # the constructor's own default
        assert pool._chunks.maxsize == 4096
        pool._buckets.maxsize = 2
        pool._chunks.maxsize = 1
        assert pool._buckets.maxsize == 2
        assert pool._chunks.maxsize == 1

    async def test_chunk_cache_respects_size_limit(self, plaintext_pool: Pool) -> None:
        small_pool = plaintext_pool
        small_pool._chunks.maxsize = 1
        await small_pool.read_chunk(pool_chunk_addr(0))
        await small_pool.read_chunk(pool_chunk_addr(1))
        assert len(small_pool._chunks) <= 1


async def test_concurrent_bucket_opens_for_the_same_key_only_open_the_file_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two concurrent misses on the *same* bucket must not both pay for
    opening it — the second joins the first's in-flight open instead of
    issuing its own (``_buckets`` is an ``AsyncKeyedCache``)."""
    for bucket_id in range(3):
        write_bucket(
            tmp_path / "Pool" / "5" / f"{bucket_id}.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=bucket_id,
            chunk_crc_store=False,
        )
    store = LocalFsStore(tmp_path)
    pool = Pool(store, "Pool", DirCache(store))

    opens = 0
    real_open_uncached = pool.open_bucket_uncached

    async def counting_open_uncached(stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        nonlocal opens
        opens += 1
        return await real_open_uncached(stream_id, bucket_id)

    monkeypatch.setattr(pool, "open_bucket_uncached", counting_open_uncached)

    readers = await asyncio.gather(*(pool.bucket(StreamId(5), BucketId(0)) for _ in range(10)))
    assert opens == 1
    assert all(r is readers[0] for r in readers)


async def test_bucket_reader_open_is_safe_under_concurrent_access(tmp_path: Path) -> None:
    for bucket_id in range(5):
        write_bucket(
            tmp_path / "Pool" / "5" / f"{bucket_id}.buk",
            PLAINTEXTS,
            stream_id=5,
            bucket_id=bucket_id,
            chunk_crc_store=False,
        )
    store = LocalFsStore(tmp_path)
    pool = Pool(store, "Pool", DirCache(store), limits=dataclasses.replace(DEFAULT_LIMITS, bucket_readers=3, chunks=10))

    async def work(i: int) -> bytes:
        return await pool.read_chunk(chunk_address(5, i % 5, i % 3))

    results = await asyncio.gather(*(work(i) for i in range(200)))

    assert len(results) == 200
    assert all(r in PLAINTEXTS for r in results)


class TestReleaseCaches:
    """``release_caches`` hands back the decoded chunks, open bucket readers and
    allocation tables a ``Pool`` otherwise holds for its whole lifetime."""

    async def test_release_caches_drops_what_real_reads_populated(self, plaintext_pool: Pool) -> None:
        for chunk_idx in range(len(PLAINTEXTS)):
            await plaintext_pool.read_chunk(chunk_address(5, 0, chunk_idx))
        assert len(plaintext_pool._chunks) > 0
        assert len(plaintext_pool._buckets) > 0

        plaintext_pool.release_caches()

        assert len(plaintext_pool._chunks) == 0
        assert len(plaintext_pool._buckets) == 0
        assert plaintext_pool._fingerprints.stats().size == 0

    async def test_release_caches_drops_backfilled_chunks_too(self, plaintext_pool: Pool) -> None:
        """Entries ``backfill_chunks`` inserted, not only those a fetch populated."""
        _backfill(plaintext_pool, 0, PLAINTEXTS[0])
        assert len(plaintext_pool._chunks) > 0

        plaintext_pool.release_caches()

        assert len(plaintext_pool._chunks) == 0

    async def test_the_pool_still_works_after_a_release(self, plaintext_pool: Pool) -> None:
        """Releasing is not closing: the next read re-opens what it needs."""
        addr = chunk_address(5, 0, 0)
        assert await plaintext_pool.read_chunk(addr) == PLAINTEXTS[0]
        plaintext_pool.release_caches()
        assert await plaintext_pool.read_chunk(addr) == PLAINTEXTS[0]


class TestCacheStatsAndLimits:
    def test_cache_stats_names_the_three_caches_with_the_configured_bounds(self) -> None:
        store = LocalFsStore(Path("."))
        pool = Pool(
            store, "Pool", DirCache(store), limits=dataclasses.replace(DEFAULT_LIMITS, bucket_readers=3, chunks=5)
        )

        stats = pool.cache_stats()

        assert set(stats) == {"pool.buckets", "pool.chunks", "pool.allocation_tables"}
        assert stats["pool.buckets"].maxsize == 3
        assert stats["pool.chunks"].maxsize == 5
        assert stats["pool.allocation_tables"].maxsize == DEFAULT_LIMITS.allocation_tables

    def test_a_pool_built_without_limits_is_bounded_by_default_limits(self) -> None:
        store = LocalFsStore(Path("."))
        stats = Pool(store, "Pool", DirCache(store)).cache_stats()

        assert stats["pool.buckets"].maxsize == DEFAULT_LIMITS.bucket_readers
        assert stats["pool.chunks"].maxsize == DEFAULT_LIMITS.chunks

    def test_a_scan_bucket_cache_is_bounded_by_default_and_verify_has_its_own_size(self) -> None:
        assert BucketReaderCache().maxsize == DEFAULT_LIMITS.bucket_readers
        assert BucketReaderCache.for_verify().maxsize == DEFAULT_LIMITS.verify_bucket_readers


class TestBucketSize:
    async def test_the_size_of_the_resolved_generation_comes_from_the_directory_listing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "Pool" / "0").mkdir(parents=True)
        (tmp_path / "Pool" / "0" / "0.buk").write_bytes(b"x" * 11)
        (tmp_path / "Pool" / "0" / "0.buk.2").write_bytes(b"x" * 22)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        async def no_size(self: LocalFsStore, path: str) -> int:
            raise AssertionError("bucket_size must not stat the file")

        monkeypatch.setattr(LocalFsStore, "size", no_size)

        assert await pool.bucket_size(StreamId(0), BucketId(0)) == 22
        assert await pool.bucket_path(StreamId(0), BucketId(0)) == "Pool/0/0.buk.2"

    async def test_a_missing_bucket_raises_not_found(self, tmp_path: Path) -> None:
        (tmp_path / "Pool" / "0").mkdir(parents=True)
        store = LocalFsStore(tmp_path)

        with pytest.raises(NotFoundError, match="no file matches logical name"):
            await Pool(store, "Pool", DirCache(store)).bucket_size(StreamId(0), BucketId(0))
