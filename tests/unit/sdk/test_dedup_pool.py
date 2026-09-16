"""Unit tests for ``synology_apm_repo.sdk.dedup.pool`` — synthetic
buckets written to real files (so ``LocalFsStore``/``DirCache``
are exercised for real, not stubbed), no sample repositories required."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import struct
import zlib
from pathlib import Path

import lz4.block
import pytest
import zstandard
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.dedup.pool import _GAP_TOLERANCE, BucketReader, Pool
from synology_apm_repo.sdk.errors import ChunkCompactedError, DataCorruptError, KeyRequiredError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import (
    MODE_CHUNK_CRC,
    MODE_COMPRESS,
    MODE_VAULT_ENCRYPT,
    SizeStoreEntry,
    chunk_crc_store_region,
)
from synology_apm_repo.sdk.format.compression import _ZSTD_BATCH_THREADS_MIN_ENTRIES, CompressType
from synology_apm_repo.sdk.format.crypto import chunk_iv
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_ALLOC_TABLE_OFFSET = 12288


def _inf_header() -> bytes:
    header = bytearray(64)
    header[0:4] = b"GMet"
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _write_inf(path: Path, entries: dict[int, tuple[int, int]]) -> None:
    """Same helper as ``test_dedup_fingerprint.py``'s own — ``entries``:
    bucket-index-within-group -> (byte_off, rec_num)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = bytearray(_ALLOC_TABLE_OFFSET + 1024 * 8)
    buf[0:64] = _inf_header()
    for idx, (byte_off, rec_num) in entries.items():
        raw_pos = ((byte_off // 4096) << 15) | rec_num
        off = _ALLOC_TABLE_OFFSET + idx * 8
        buf[off : off + 4] = raw_pos.to_bytes(4, "big")
    path.write_bytes(bytes(buf))


def _write_fgp(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


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


def _write_bucket(
    path: Path,
    plaintexts: list[bytes],
    *,
    stream_id: int,
    bucket_id: int,
    vault_key: bytes | None = None,
    compress_types: list[CompressType] | None = None,
    chunk_crc_store: bool = False,
    corrupt_chunk_crc_idx: int | None = None,
) -> None:
    """Write a complete, real, self-consistent ``.buk`` file — every chunk
    compressed with ZSTD by default, optionally AES-256-CTR encrypted
    exactly like a real vault-encrypted bucket.

    ``compress_types`` (default ``None``: every chunk is ``ZSTD``, the
    original behavior this helper has always had) lets a caller mix
    ``CompressType.NONE``/``LZ4``/``ZSTD`` per chunk within one bucket —
    for tests of ``decompress_many``'s batching, which groups by compress
    type across a whole merged run and needs a real bucket exercising
    more than one type at once to prove it doesn't mix them up.
    ``COMPACTED`` isn't supported here (it carries no
    real payload — see ``TestCompacted``'s own hand-built bucket instead).

    ``chunk_crc_store`` (default ``False``, preserving every existing
    caller's own random-filler trailer) builds a real, self-consistent
    ChunkCrcStore trailer instead — one true ciphertext CRC32 per chunk
    plus the header's own ``crcOfChunkCrc`` self-check — for a test that
    wants ``verify_ciphertext_crc=True`` to actually pass against a
    genuinely intact bucket, not just against random bytes it never
    validates in the first place.

    ``corrupt_chunk_crc_idx`` (requires ``chunk_crc_store=True``) flips one
    chunk's recorded entry *before* the trailer's own ``crcOfChunkCrc`` is
    computed — a wrong-but-internally-self-consistent trailer, so the
    mismatch is only caught by an actual per-chunk ciphertext check, not
    by the trailer's own self-consistency check.
    """
    if compress_types is None:
        compress_types = [CompressType.ZSTD] * len(plaintexts)
    assert len(compress_types) == len(plaintexts)

    compressor = zstandard.ZstdCompressor()
    ciphertexts: list[bytes] = []
    entries: list[tuple[int, int]] = []
    for chunk_idx, (plain, ctype) in enumerate(zip(plaintexts, compress_types, strict=True)):
        if ctype is CompressType.ZSTD:
            compressed = compressor.compress(plain)
        elif ctype is CompressType.LZ4:
            compressed = lz4.block.compress(plain, store_size=False)
        elif ctype is CompressType.NONE:
            compressed = plain
        else:
            raise ValueError(f"_write_bucket() doesn't support {ctype}")
        if vault_key is not None:
            addr = ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx))
            encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(chunk_iv(addr))).encryptor()
            payload = encryptor.update(compressed) + encryptor.finalize()
        else:
            payload = compressed
        ciphertexts.append(payload)
        stored_len = 0 if ctype is CompressType.NONE else len(payload)
        entries.append((ctype.value, stored_len))

    chunk_num = len(plaintexts)
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    mode = MODE_COMPRESS | MODE_CHUNK_CRC | (MODE_VAULT_ENCRYPT if vault_key is not None else 0)

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[6:8] = (0).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", mode)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    if chunk_crc_store:
        chunk_crcs = [zlib.crc32(ct) & 0xFFFFFFFF for ct in ciphertexts]
        if corrupt_chunk_crc_idx is not None:
            chunk_crcs[corrupt_chunk_crc_idx] ^= 0xFFFFFFFF
        chunk_crc_bytes = b"".join(crc.to_bytes(4, "big") for crc in chunk_crcs)
        header[29:33] = struct.pack(">I", zlib.crc32(chunk_crc_bytes) & 0xFFFFFFFF)
    else:
        assert corrupt_chunk_crc_idx is None, "corrupt_chunk_crc_idx requires chunk_crc_store=True"
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    chunk_data = b"".join(ciphertexts)

    from synology_apm_repo.sdk.format.redundancy import redundancy_size

    trailer_len = 4 * chunk_num + redundancy_size((chunk_num * 15 + 7) >> 3, 256)
    trailer = os.urandom(trailer_len)
    if chunk_crc_store:
        trailer = chunk_crc_bytes + trailer[4 * chunk_num :]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + chunk_data + trailer)


_PLAINTEXTS = [((b"chunk-%d-content" % i) * 300)[:4096] for i in range(3)]
assert all(len(p) == 4096 for p in _PLAINTEXTS)  # every real chunk is exactly FIXED_CHUNK_LENGTH


@pytest.fixture
def plaintext_pool(tmp_path: Path) -> Pool:
    _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
    store = LocalFsStore(tmp_path)
    return Pool(store, "Pool", DirCache(store))


@pytest.fixture
def encrypted_pool(tmp_path: Path) -> tuple[Pool, bytes]:
    vault_key = os.urandom(32)
    _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, vault_key=vault_key)
    store = LocalFsStore(tmp_path)
    pool = Pool(store, "Pool", DirCache(store), vault_key=vault_key)
    return pool, vault_key


def _addr(chunk_idx: int) -> ChunkAddress:
    return ChunkAddress(StreamId(5), BucketId(0), ChunkIdx(chunk_idx))


class TestBucketPathResolution:
    async def test_resolves_bare_bucket_file(self, tmp_path: Path) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        assert await pool.bucket_path(StreamId(5), BucketId(0)) == "Pool/5/0.buk"

    async def test_resolves_sequence_suffixed_bucket_file(self, tmp_path: Path) -> None:
        # real buckets almost always carry a .<seqId> suffix
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk.7", _PLAINTEXTS, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        assert await pool.bucket_path(StreamId(5), BucketId(0)) == "Pool/5/0.buk.7"


class TestLegacyUncompressedBucket:
    async def test_open_synthesizes_none_type_size_store_entries(self, tmp_path: Path) -> None:
        """No current writer produces this layout (see ``open()``'s own
        comment) — a real header with ``MODE_COMPRESS`` unset, no
        SizeStore region present at all."""
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
        assert list(reader.entries) == [SizeStoreEntry(CompressType.NONE, 0)] * 2


class TestReadChunkPlaintext:
    async def test_reads_all_chunks_correctly(self, plaintext_pool: Pool) -> None:
        for i, expected in enumerate(_PLAINTEXTS):
            assert await plaintext_pool.read_chunk(_addr(i)) == expected

    async def test_bucket_reader_reports_correct_header(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert reader.header.is_compressed is True
        assert reader.header.is_vault_encrypted is False
        assert reader.header.chunk_num == len(_PLAINTEXTS)

    async def test_bucket_reader_is_cached(self, plaintext_pool: Pool) -> None:
        first = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        second = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert first is second

    async def test_chunk_cache_returns_identical_bytes_object_semantics(self, plaintext_pool: Pool) -> None:
        a = await plaintext_pool.read_chunk(_addr(0))
        b = await plaintext_pool.read_chunk(_addr(0))
        assert a == b

    async def test_cache_false_bypasses_chunk_cache(self, plaintext_pool: Pool) -> None:
        # exercised path only, not observable from outside except that it
        # still returns correct data and never populates the chunk cache.
        result = await plaintext_pool.read_chunk(_addr(1), cache=False)
        assert result == _PLAINTEXTS[1]
        assert (5, 0, 1) not in plaintext_pool._chunks


class TestVerifyFingerprint:
    """``read_chunk(..., verify_fingerprint=...)`` — the general,
    per-chunk-read data-integrity option. General-purpose, not
    encryption-specific: every real chunk has a ``.fgp`` fingerprint, so
    these tests use the unencrypted ``plaintext_pool`` fixture
    throughout."""

    async def test_matching_fingerprint_succeeds(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        digest = hashlib.sha256(_PLAINTEXTS[0]).digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", digest)

        result = await plaintext_pool.read_chunk(_addr(0), verify_fingerprint=True)
        assert result == _PLAINTEXTS[0]

    async def test_mismatched_fingerprint_raises_data_corrupt(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        wrong_digest = hashlib.sha256(b"not the real plaintext").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", wrong_digest)

        with pytest.raises(DataCorruptError):
            await plaintext_pool.read_chunk(_addr(0), verify_fingerprint=True)

    async def test_off_by_default_never_touches_inf_or_fgp_at_all(self, plaintext_pool: Pool) -> None:
        # No .inf/.fgp written anywhere under this fixture's Pool root —
        # a real read would fail immediately if this call attempted the
        # check at all, so success here proves it's genuinely opt-in,
        # not merely opt-in for raising on mismatch.
        result = await plaintext_pool.read_chunk(_addr(0))
        assert result == _PLAINTEXTS[0]

    async def test_explicit_false_overrides_a_pool_wide_default_of_true(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        plaintext_pool._verify_fingerprint = True  # simulate Pool(..., verify_fingerprint=True)
        result = await plaintext_pool.read_chunk(_addr(0), verify_fingerprint=False)
        assert result == _PLAINTEXTS[0]  # no .inf/.fgp exists — would raise/crash if actually checked

    async def test_pool_wide_default_true_checks_every_call_with_no_override(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        wrong_digest = hashlib.sha256(b"wrong").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", wrong_digest)
        plaintext_pool._verify_fingerprint = True

        with pytest.raises(DataCorruptError):
            await plaintext_pool.read_chunk(_addr(0))  # no per-call override — defers to the Pool-wide setting

    async def test_a_cache_hit_is_still_checked_not_skipped(self, tmp_path: Path, plaintext_pool: Pool) -> None:
        # .inf/.fgp must exist before the *first* read below — DirCache
        # caches each directory's listing on first access, so writing
        # these afterward would make the fingerprint lookup itself
        # (unrelated to the cache-hit property under test) spuriously
        # raise NotFoundError instead of DataCorruptError.
        wrong_digest = hashlib.sha256(b"wrong").digest()
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", wrong_digest)

        # Populate the plaintext cache first, unverified.
        await plaintext_pool.read_chunk(_addr(0))
        assert (5, 0, 0) in plaintext_pool._chunks

        with pytest.raises(DataCorruptError):
            # Served from the cache (no bucket re-read) but still checked
            # against the fingerprint — the whole point being verifying
            # the bytes about to be handed back, not just fresh reads.
            await plaintext_pool.read_chunk(_addr(0), verify_fingerprint=True)


class TestVerifyFingerprints:
    """``Pool.verify_fingerprints`` — the batch form ``BucketReader.
    read_chunks``'s multi-chunk result needs, since that method bypasses
    ``read_chunk`` (and so its own fingerprint check) entirely: a
    read/export spanning more than one distinct chunk in one
    ``read_chunks`` call must still honor ``verify_fingerprint=True``,
    exactly like a single-chunk read through ``read_chunk``."""

    async def test_matching_fingerprints_across_multiple_chunks_succeeds(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        _write_fgp(
            tmp_path / "Pool" / "5" / "0_0.fgp",
            b"".join(hashlib.sha256(p).digest() for p in _PLAINTEXTS),
        )
        chunks = {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

        await plaintext_pool.verify_fingerprints(StreamId(5), BucketId(0), chunks, verify_fingerprint=True)

    async def test_one_wrong_fingerprint_among_several_raises_data_corrupt(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        digests = [hashlib.sha256(p).digest() for p in _PLAINTEXTS]
        digests[1] = hashlib.sha256(b"wrong").digest()  # chunk 1's stored fingerprint is corrupt
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))
        chunks = {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

        with pytest.raises(DataCorruptError):
            await plaintext_pool.verify_fingerprints(StreamId(5), BucketId(0), chunks, verify_fingerprint=True)

    async def test_off_by_default_never_touches_inf_or_fgp_at_all(self, plaintext_pool: Pool) -> None:
        # No .inf/.fgp written anywhere under this fixture's Pool root —
        # a real check would fail immediately if attempted at all.
        chunks = {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}
        await plaintext_pool.verify_fingerprints(StreamId(5), BucketId(0), chunks)

    async def test_read_chunks_batch_result_is_verified_once_fed_through_this_method(
        self, tmp_path: Path, plaintext_pool: Pool
    ) -> None:
        """The exact sequence ``dedup_file.py``'s ``_fill_data_extent``
        and ``chunk_walk.py``'s ``_exec_one_bucket_group`` both use:
        ``BucketReader.read_chunks`` first (which alone never checks
        anything), then this method on its result — the multi-chunk batch
        path honors ``verify_fingerprint=True`` too."""
        digests = [hashlib.sha256(p).digest() for p in _PLAINTEXTS]
        digests[2] = hashlib.sha256(b"wrong").digest()  # chunk 2's stored fingerprint is corrupt
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", b"".join(digests))

        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        decoded = await reader.read_chunks(requests)  # succeeds -- read_chunks() itself never checks fingerprints
        assert decoded == {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

        with pytest.raises(DataCorruptError):
            await plaintext_pool.verify_fingerprints(StreamId(5), BucketId(0), decoded, verify_fingerprint=True)

    async def test_inf_header_and_allocation_entry_are_fetched_once_for_every_chunk_in_one_bucket(
        self, tmp_path: Path, plaintext_pool: Pool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every chunk in one ``(stream_id, bucket_id)`` shares the same
        ``.inf`` header/allocation-table entry — a naive per-chunk
        ``fingerprint()`` loop would re-fetch both for every chunk;
        ``fingerprints()``'s batched resolution must fetch each exactly
        once regardless of how many chunks are checked."""
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 3)})
        _write_fgp(
            tmp_path / "Pool" / "5" / "0_0.fgp",
            b"".join(hashlib.sha256(p).digest() for p in _PLAINTEXTS),
        )
        chunks = {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

        inf_reads = 0
        original_read = plaintext_pool._store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal inf_reads
            if path.endswith(".inf"):
                inf_reads += 1
            return await original_read(path, offset, length)

        monkeypatch.setattr(plaintext_pool._store, "read", counting_read)

        await plaintext_pool.verify_fingerprints(StreamId(5), BucketId(0), chunks, verify_fingerprint=True)

        # Exactly 2 reads against the .inf file total (header + one
        # allocation-table entry) -- not 2 per chunk (6, for 3 chunks).
        assert inf_reads == 2


class TestVerifyCiphertextCrc:
    """``read_chunk``/``read_chunks``' ``verify_ciphertext_crc`` option —
    the ``ChunkCrcStore`` check (FORMAT-SPEC.md: ChunkCrcStore), checked
    against a chunk's raw stored bytes before any decrypt/decompress is
    attempted. Independent of ``verify_fingerprint``: these use
    unencrypted buckets throughout since the check itself needs no vault
    key, exactly like ``TestVerifyFingerprint``'s own choice above."""

    async def test_matching_ciphertext_crc_succeeds(self, tmp_path: Path) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        result = await pool.read_chunk(_addr(0), verify_ciphertext_crc=True)
        assert result == _PLAINTEXTS[0]

    async def test_mismatched_ciphertext_crc_raises_data_corrupt(self, tmp_path: Path) -> None:
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            _PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        with pytest.raises(DataCorruptError):
            await pool.read_chunk(_addr(0), verify_ciphertext_crc=True)

    async def test_off_by_default_never_reads_the_trailer_at_all(self, plaintext_pool: Pool) -> None:
        # plaintext_pool's trailer is random filler (chunk_crc_store=False,
        # the default) -- a real check would raise if attempted at all.
        result = await plaintext_pool.read_chunk(_addr(0))
        assert result == _PLAINTEXTS[0]

    async def test_independent_of_verify_fingerprint_each_togglable_alone_or_together(self, tmp_path: Path) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        _write_inf(tmp_path / "Pool" / "5" / "0.inf", {0: (0, 1)})
        _write_fgp(tmp_path / "Pool" / "5" / "0_0.fgp", hashlib.sha256(_PLAINTEXTS[0]).digest())
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        assert await pool.read_chunk(_addr(0), verify_ciphertext_crc=True, cache=False) == _PLAINTEXTS[0]
        assert await pool.read_chunk(_addr(0), verify_fingerprint=True, cache=False) == _PLAINTEXTS[0]
        assert (
            await pool.read_chunk(_addr(0), verify_ciphertext_crc=True, verify_fingerprint=True, cache=False)
            == _PLAINTEXTS[0]
        )

    async def test_read_chunks_batch_form_checks_every_requested_chunk(self, tmp_path: Path) -> None:
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            _PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=2,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        with pytest.raises(DataCorruptError):
            await reader.read_chunks(requests, verify_ciphertext_crc=True)

    async def test_costs_exactly_one_trailer_read_regardless_of_chunk_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=True)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        trailer_offset, trailer_length = chunk_crc_store_region(reader.header, reader.entries)

        trailer_reads = 0
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            nonlocal trailer_reads
            if offset == trailer_offset and length == trailer_length:
                trailer_reads += 1
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        await reader.read_chunks(requests, verify_ciphertext_crc=True)

        assert trailer_reads == 1

    async def test_raw_batch_form_checks_every_requested_chunk(self, tmp_path: Path) -> None:
        """The ``read_raw_chunks`` + ``verify_raw_chunk_ciphertext_crc``
        pairing ``check_chunk_ciphertext_crcs`` (``verify_checks.py``)
        uses — mirrors ``test_read_chunks_batch_form_checks_every_requested_chunk``
        above, but through the raw (no decrypt/decompress) path."""
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            _PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=2,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        raw_by_chunk = await reader.read_raw_chunks(list(range(len(_PLAINTEXTS))))
        await reader.verify_raw_chunk_ciphertext_crc(0, raw_by_chunk[0])
        await reader.verify_raw_chunk_ciphertext_crc(1, raw_by_chunk[1])
        with pytest.raises(DataCorruptError):
            await reader.verify_raw_chunk_ciphertext_crc(2, raw_by_chunk[2])


class TestDecodeRawChunk:
    """``decode_raw_chunk`` — ``read_chunk``'s own decrypt/decompress half,
    split out for a caller that already has a chunk's raw bytes in hand
    from a batched ``read_raw_chunks`` call, the same split
    ``verify_raw_chunk_ciphertext_crc`` already has from
    ``verify_chunk_ciphertext_crc``."""

    async def test_matches_read_chunk_for_plaintext(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks(list(range(len(_PLAINTEXTS))))

        for i, expected in enumerate(_PLAINTEXTS):
            decoded = await reader.decode_raw_chunk(i, _addr(i), raw_by_chunk[i])
            assert decoded == expected == await plaintext_pool.read_chunk(_addr(i), cache=False)

    async def test_matches_read_chunk_for_encrypted(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        reader = await pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks(list(range(len(_PLAINTEXTS))))

        for i, expected in enumerate(_PLAINTEXTS):
            decoded = await reader.decode_raw_chunk(i, _addr(i), raw_by_chunk[i])
            assert decoded == expected == await pool.read_chunk(_addr(i), cache=False)

    async def test_verify_ciphertext_crc_true_still_raises_on_mismatch(self, tmp_path: Path) -> None:
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            _PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks([0])

        with pytest.raises(DataCorruptError):
            await reader.decode_raw_chunk(0, _addr(0), raw_by_chunk[0], verify_ciphertext_crc=True)

    async def test_verify_ciphertext_crc_false_skips_a_check_already_done_separately(self, tmp_path: Path) -> None:
        """``decode_raw_chunk``'s own documented parameter contract: a chunk
        whose ciphertext CRC was already verified (and found mismatched)
        by a separate ``verify_raw_chunk_ciphertext_crc`` call can still be
        decoded — ``verify_ciphertext_crc=False`` here must not re-run (and
        raise past) that already-reported check."""
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            _PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            chunk_crc_store=True,
            corrupt_chunk_crc_idx=0,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        raw_by_chunk = await reader.read_raw_chunks([0])

        with pytest.raises(DataCorruptError):
            await reader.verify_raw_chunk_ciphertext_crc(0, raw_by_chunk[0])
        decoded = await reader.decode_raw_chunk(0, _addr(0), raw_by_chunk[0], verify_ciphertext_crc=False)
        assert decoded == _PLAINTEXTS[0]


class TestReadChunkEncrypted:
    async def test_reads_all_chunks_correctly(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        for i, expected in enumerate(_PLAINTEXTS):
            assert await pool.read_chunk(_addr(i)) == expected

    async def test_missing_vault_key_raises_key_required(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, vault_key=vault_key)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))  # no vault_key supplied

        with pytest.raises(KeyRequiredError):
            await pool.read_chunk(_addr(0))

    async def test_wrong_vault_key_does_not_raise_but_gives_garbage(self, tmp_path: Path) -> None:
        # AES-CTR has no integrity check — this documents the behavior
        # rather than asserting a specific exception; fingerprint-based
        # verification (a higher layer) is what actually catches this.
        vault_key = os.urandom(32)
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, vault_key=vault_key)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), vault_key=os.urandom(32))

        with pytest.raises(Exception):  # noqa: B017 - decompressing garbage plausibly raises *something*
            await pool.read_chunk(_addr(0))


class TestCompacted:
    async def test_compacted_chunk_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)

        entries = [(CompressType.COMPACTED.value, 0)]
        tight = _encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 1)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = tight + b"\x00" * (16320 - len(tight))

        from synology_apm_repo.sdk.format.redundancy import redundancy_size

        trailer = os.urandom(redundancy_size((1 * 15 + 7) >> 3, 256))  # 0 non-empty chunks -> no ChunkCrcStore bytes
        path.write_bytes(bytes(header) + sizestore_region + trailer)

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        with pytest.raises(ChunkCompactedError):
            await pool.read_chunk(_addr(0))

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``BucketReader.read_chunk``'s own singular-form counterpart to
        ``TestReadChunksBatch.test_compacted_chunk_raises_before_any_read``
        below — the COMPACTED check must run before ``decode_raw_chunk``'s
        real ``store.read()``, not after it, or a caller pays for (and, on
        a backend where a COMPACTED slot's offset/length aren't meaningful,
        risks a confusing storage-layer error instead of) a read it can
        never use."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0)]
        tight = _encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 1)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = tight + b"\x00" * (16320 - len(tight))
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

        with pytest.raises(ChunkCompactedError):
            await reader.read_chunk(ChunkIdx(0), _addr(0))
        assert read_calls == []


class TestNonCompactedChunkIndices:
    """``BucketReader.non_compacted_chunk_indices()`` — built off the raw
    compress-type array rather than the lazier ``entries`` sequence, for a
    dense, whole-bucket sweep."""

    async def test_skips_compacted_slots(self, tmp_path: Path) -> None:
        plaintexts = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(4)]
        path = tmp_path / "Pool" / "5" / "0.buk"
        _write_bucket(path, plaintexts, stream_id=5, bucket_id=0, compress_types=[CompressType.NONE] * 4)

        # Hand-patch entry 1's SizeStore slot to COMPACTED after the fact --
        # _write_bucket() itself doesn't support COMPACTED (see its own
        # docstring); this test only needs a mixed SizeStore, not a
        # genuinely reclaimed chunk's real on-disk byte layout.
        raw = bytearray(path.read_bytes())
        entries = [(CompressType.NONE.value, 0)] * 4
        entries[1] = (CompressType.COMPACTED.value, 0)
        tight = _encode_size_store(entries)
        raw[64 : 64 + len(tight)] = tight
        raw[16:20] = struct.pack(">I", zlib.crc32(tight) & 0xFFFFFFFF)
        raw[60:64] = (zlib.crc32(bytes(raw[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        path.write_bytes(bytes(raw))

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        assert reader.non_compacted_chunk_indices() == [0, 2, 3]

    async def test_empty_when_every_chunk_is_compacted(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        _write_bucket(path, [b"\x00" * 4096], stream_id=5, bucket_id=0, compress_types=[CompressType.NONE])
        raw = bytearray(path.read_bytes())
        tight = _encode_size_store([(CompressType.COMPACTED.value, 0)])
        raw[64 : 64 + len(tight)] = tight
        raw[16:20] = struct.pack(">I", zlib.crc32(tight) & 0xFFFFFFFF)
        raw[60:64] = (zlib.crc32(bytes(raw[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        path.write_bytes(bytes(raw))

        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        assert reader.non_compacted_chunk_indices() == []


class TestLruEviction:
    async def test_bucket_cache_evicts_least_recently_used(self, tmp_path: Path) -> None:
        for bucket_id in range(3):
            _write_bucket(tmp_path / "Pool" / "5" / f"{bucket_id}.buk", _PLAINTEXTS, stream_id=5, bucket_id=bucket_id)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), bucket_cache_size=2)

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
        await small_pool.read_chunk(_addr(0))
        await small_pool.read_chunk(_addr(1))
        assert len(small_pool._chunks) <= 1


async def test_concurrent_bucket_opens_for_the_same_key_only_open_the_file_once(tmp_path: Path) -> None:
    """``_buckets`` is an ``AsyncKeyedCache`` now, so this is a real,
    measured consequence of that migration, not just a cache-hit test:
    two concurrent misses on the *same* bucket must not both pay for
    opening it — the second joins the first's in-flight open instead of
    issuing its own."""
    for bucket_id in range(3):
        _write_bucket(tmp_path / "Pool" / "5" / f"{bucket_id}.buk", _PLAINTEXTS, stream_id=5, bucket_id=bucket_id)
    store = LocalFsStore(tmp_path)
    pool = Pool(store, "Pool", DirCache(store))

    opens = 0
    real_open_uncached = pool.open_bucket_uncached

    async def counting_open_uncached(stream_id: StreamId, bucket_id: BucketId) -> BucketReader:
        nonlocal opens
        opens += 1
        return await real_open_uncached(stream_id, bucket_id)

    pool.open_bucket_uncached = counting_open_uncached  # type: ignore[method-assign]

    readers = await asyncio.gather(*(pool.bucket(StreamId(5), BucketId(0)) for _ in range(10)))
    assert opens == 1
    assert all(r is readers[0] for r in readers)


async def test_bucket_reader_open_is_safe_under_concurrent_access(tmp_path: Path) -> None:
    # Concurrent openers of the same LRU-cached bucket must never corrupt
    # the cache or hand back wrong bytes -- exercised via asyncio.gather,
    # how a real caller of this async Pool actually issues concurrent
    # reads.
    for bucket_id in range(5):
        _write_bucket(tmp_path / "Pool" / "5" / f"{bucket_id}.buk", _PLAINTEXTS, stream_id=5, bucket_id=bucket_id)
    store = LocalFsStore(tmp_path)
    pool = Pool(store, "Pool", DirCache(store), bucket_cache_size=3, chunk_cache_size=10)

    async def work(i: int) -> bytes:
        return await pool.read_chunk(ChunkAddress(StreamId(5), BucketId(i % 5), ChunkIdx(i % 3)))

    results = await asyncio.gather(*(work(i) for i in range(200)))

    assert len(results) == 200
    assert all(r in _PLAINTEXTS for r in results)


class TestFitsInRun:
    """Pure-logic tests for ``BucketReader._fits_in_run`` (merge
    gap-tolerance arithmetic — no size cap, only a gap tolerance, see
    ``pool.py``'s own ``_GAP_TOLERANCE`` docstring for why). Takes a bare
    ``(prev_end, offset)`` pair rather than a whole run tuple — shared
    verbatim by ``read_chunks`` and ``read_raw_chunks``, each computing its
    own ``prev_end`` off its own run shape (see this method's own
    docstring) — so these tests don't need any particular run-tuple shape
    at all. Exact boundary behavior would need multi-hundred-KiB/multi-MiB
    real bucket fixtures to exercise through real files, which the
    batch-read correctness tests below don't need (they only need "does
    merging happen at all", not "exactly where is the boundary")."""

    def test_zero_gap_fits(self) -> None:
        assert BucketReader._fits_in_run(150, 150) is True

    def test_gap_within_tolerance_fits(self) -> None:
        assert BucketReader._fits_in_run(150, 150 + _GAP_TOLERANCE) is True  # exactly at the tolerance

    def test_gap_beyond_tolerance_does_not_fit(self) -> None:
        assert BucketReader._fits_in_run(150, 150 + _GAP_TOLERANCE + 1) is False  # one byte past tolerance

    def test_negative_gap_does_not_fit(self) -> None:
        # A caller bug (locators not actually chunk_idx-ascending) must
        # not silently merge backwards — merging assumes offsets only
        # ever grow, matching chunk_locators()'s own monotonic-by-
        # chunk_idx construction.
        assert BucketReader._fits_in_run(250, 100) is False

    def test_a_32_mib_zero_gap_run_still_fits(self) -> None:
        """No size cap, only a gap tolerance (``pool.py``'s
        ``_GAP_TOLERANCE`` docstring has the real-data reasoning) — a
        contiguous run past even one whole bucket's realistic size (here:
        32 MiB) still merges as long as the gap itself is zero."""
        assert BucketReader._fits_in_run(32 << 20, 32 << 20) is True


class TestPlanRuns:
    """Pure-logic tests for ``BucketReader._plan_runs`` — the run-merging
    step ``read_chunks`` delegates to, exercised directly (no ``.buk``
    file, no I/O) rather than only implicitly through
    ``TestReadChunksBatch``'s real-file tests below."""

    @staticmethod
    def _item(chunk_idx: int, offset: int, length: int) -> tuple[int, ChunkAddress | None, int, int, CompressType]:
        return (chunk_idx, None, offset, length, CompressType.NONE)

    def test_empty_input_returns_no_runs(self) -> None:
        assert BucketReader._plan_runs([]) == []

    def test_adjacent_locators_merge_into_one_run(self) -> None:
        located = [self._item(0, 0, 100), self._item(1, 100, 100)]
        runs = BucketReader._plan_runs(located)
        assert runs == [located]

    def test_a_gap_beyond_tolerance_starts_a_new_run(self) -> None:
        located = [self._item(0, 0, 100), self._item(1, 100 + _GAP_TOLERANCE + 1, 100)]
        runs = BucketReader._plan_runs(located)
        assert runs == [[located[0]], [located[1]]]

    def test_three_locators_split_into_two_runs(self) -> None:
        located = [
            self._item(0, 0, 100),
            self._item(1, 100, 100),  # merges with the first
            self._item(2, 200 + _GAP_TOLERANCE + 1, 50),  # starts a new run
        ]
        runs = BucketReader._plan_runs(located)
        assert runs == [located[:2], [located[2]]]


class TestReadChunksBatch:
    """``BucketReader.read_chunks``/``Pool`` end-to-end via real
    ``.buk`` files (the merged multi-chunk pread)."""

    async def test_empty_requests_returns_empty_dict(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert await reader.read_chunks([]) == {}

    async def test_matches_read_chunk_for_every_chunk_plaintext(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        result = await reader.read_chunks(requests)
        assert result == {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

    async def test_matches_read_chunk_for_a_subset_skipping_the_middle_one(self, plaintext_pool: Pool) -> None:
        # Requesting chunks 0 and 2 (skipping 1) still must decode
        # correctly even though they land in the same merged run as each
        # other (chunk 1's real file bytes sit between them, unrequested
        # — the reason merging is keyed off file byte-adjacency, not
        # request-list adjacency).
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        result = await reader.read_chunks([(0, _addr(0)), (2, _addr(2))])
        assert result == {0: _PLAINTEXTS[0], 2: _PLAINTEXTS[2]}

    async def test_matches_read_chunk_for_every_chunk_encrypted(self, encrypted_pool: tuple[Pool, bytes]) -> None:
        pool, _vault_key = encrypted_pool
        reader = await pool.bucket(StreamId(5), BucketId(0))
        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        result = await reader.read_chunks(requests)
        assert result == {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}

    async def test_missing_vault_key_raises_key_required(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0, vault_key=vault_key)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))  # no vault_key supplied
        reader = await pool.bucket(StreamId(5), BucketId(0))

        with pytest.raises(KeyRequiredError):
            await reader.read_chunks([(0, _addr(0))])

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Mirrors TestCompacted.test_compacted_chunk_raises but through
        # the batch path, and additionally proves the raise happens
        # *before* any ObjectStore.read() call — the compacted check
        # runs as its own pass over every request first (see
        # read_chunks()'s own docstring), so a caller never pays for a
        # read it can't use.
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0), (CompressType.ZSTD.value, 100)]
        # Deliberately not a fully-valid bucket for the second entry
        # (no real compressed payload after it) — irrelevant, since the
        # COMPACTED check for chunk 0 must raise before chunk 1's bytes
        # are ever read.
        tight = _encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 2)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = tight + b"\x00" * (16320 - len(tight))
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

        with pytest.raises(ChunkCompactedError):
            await reader.read_chunks([(0, _addr(0)), (1, _addr(1))])
        assert read_calls == []


class TestReadChunksMerging:
    """Proves the actual I/O reduction, not just correctness — a spy
    wrapping the real ``LocalFsStore`` counts ``read()`` calls
    against the ``.buk`` file itself."""

    async def test_requesting_every_chunk_issues_one_merged_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(
            StreamId(5), BucketId(0)
        )  # BucketReader.open() already did its own header/SizeStore read

        data_reads = []
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            if offset >= 16384:  # past the header/SizeStore region
                data_reads.append((offset, length))
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        requests = [(i, _addr(i)) for i in range(len(_PLAINTEXTS))]
        result = await reader.read_chunks(requests)

        assert result == {i: plaintext for i, plaintext in enumerate(_PLAINTEXTS)}
        assert len(data_reads) == 1  # 3 chunks, adjacent in the file -> one merged read, not three

    async def test_read_chunk_one_at_a_time_would_have_issued_three_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The same fixture as above, but through the independent,
        per-chunk ``Pool.read_chunk`` path — establishes, in the
        same test file, the read-count baseline the merged-read path
        above improves on, rather than asking a reader to trust an
        unproven claim about it."""
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))

        data_reads = []
        original_read = store.read

        async def counting_read(path: str, offset: int = 0, length: int | None = None) -> bytes:
            if offset >= 16384:
                data_reads.append((offset, length))
            return await original_read(path, offset, length)

        monkeypatch.setattr(store, "read", counting_read)

        for i in range(len(_PLAINTEXTS)):
            await pool.read_chunk(_addr(i), cache=False)

        assert len(data_reads) == len(_PLAINTEXTS)


class TestReadRawChunksBatch:
    """``BucketReader.read_raw_chunks`` — the ciphertext-CRC check's own
    batch primitive (no decrypt/decompress, unlike ``read_chunks``).
    Correctness is checked against ``read_raw_chunk``, the existing
    single-chunk form, as the oracle — a chunk's *stored* bytes (possibly
    compressed and/or encrypted) have no simpler independent expected
    value the way plaintext does."""

    async def test_empty_indices_returns_empty_dict(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        assert await reader.read_raw_chunks([]) == {}

    async def test_matches_read_raw_chunk_for_every_chunk(self, plaintext_pool: Pool) -> None:
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        expected = {i: await reader.read_raw_chunk(ChunkIdx(i)) for i in range(len(_PLAINTEXTS))}
        result = await reader.read_raw_chunks(list(range(len(_PLAINTEXTS))))
        assert {i: bytes(v) for i, v in result.items()} == expected

    async def test_matches_read_raw_chunk_for_a_subset_skipping_the_middle_one(self, plaintext_pool: Pool) -> None:
        # Same reasoning as read_chunks' own identically-named test: chunk
        # 1's bytes sit between 0 and 2 in the file, unrequested.
        reader = await plaintext_pool.bucket(StreamId(5), BucketId(0))
        expected = {0: await reader.read_raw_chunk(ChunkIdx(0)), 2: await reader.read_raw_chunk(ChunkIdx(2))}
        result = await reader.read_raw_chunks([0, 2])
        assert {i: bytes(v) for i, v in result.items()} == expected

    async def test_compacted_chunk_raises_before_any_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Mirrors TestReadChunksBatch's own identically-named test.
        path = tmp_path / "Pool" / "5" / "0.buk"
        path.parent.mkdir(parents=True)
        entries = [(CompressType.COMPACTED.value, 0), (CompressType.ZSTD.value, 100)]
        tight = _encode_size_store(entries)
        chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
        header = bytearray(64)
        header[0:4] = b"bFiL"
        header[4:6] = (3).to_bytes(2, "big")
        header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
        header[12:16] = struct.pack(">I", 2)
        header[16:20] = struct.pack(">I", chunk_size_crc)
        header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
        sizestore_region = tight + b"\x00" * (16320 - len(tight))
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

        with pytest.raises(ChunkCompactedError):
            await reader.read_raw_chunks([0, 1])
        assert read_calls == []


class TestReadRawChunksMerging:
    """Proves the actual I/O reduction for ``read_raw_chunks``, the same
    way ``TestReadChunksMerging`` does for ``read_chunks`` — real for the
    ciphertext-CRC check regardless of ``VerifyLevel``, since it needs no
    decrypt/decompress and so no vault key either way."""

    async def test_requesting_every_chunk_issues_one_merged_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
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

        result = await reader.read_raw_chunks(list(range(len(_PLAINTEXTS))))

        assert len(result) == len(_PLAINTEXTS)
        assert len(data_reads) == 1  # 3 chunks, adjacent in the file -> one merged read, not three

    async def test_read_raw_chunk_one_at_a_time_would_have_issued_three_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The same fixture as above, but through the independent,
        per-chunk ``read_raw_chunk`` path — the read-count baseline the
        merged-read path above improves on."""
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
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

        for i in range(len(_PLAINTEXTS)):
            await reader.read_raw_chunk(ChunkIdx(i))

        assert len(data_reads) == len(_PLAINTEXTS)


class TestReadChunksMixedCompressType:
    """``BucketReader.read_chunks`` decodes a whole merged run through
    ``decompress_many``, which groups the run by ``CompressType`` before
    batch-decompressing — these tests prove that grouping doesn't scramble
    the ``chunk_idx`` <-> plaintext correspondence when a real bucket mixes
    NONE/LZ4/ZSTD chunks within one contiguous run, checked against
    ``Pool.read_chunk``'s independent per-chunk path as the oracle."""

    _MIXED_TYPES = [CompressType.NONE, CompressType.ZSTD, CompressType.LZ4, CompressType.ZSTD, CompressType.NONE]
    _MIXED_PLAINTEXTS = [((b"mixed-chunk-%d-content" % i) * 300)[:4096] for i in range(len(_MIXED_TYPES))]

    async def test_matches_read_chunk_oracle_plaintext(self, tmp_path: Path) -> None:
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            self._MIXED_PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            compress_types=self._MIXED_TYPES,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(i, _addr(i)) for i in range(len(self._MIXED_PLAINTEXTS))]
        batched = await reader.read_chunks(requests)

        for i, expected in enumerate(self._MIXED_PLAINTEXTS):
            assert batched[i] == expected
            assert await pool.read_chunk(_addr(i), cache=False) == expected

    async def test_matches_read_chunk_oracle_encrypted(self, tmp_path: Path) -> None:
        vault_key = os.urandom(32)
        _write_bucket(
            tmp_path / "Pool" / "5" / "0.buk",
            self._MIXED_PLAINTEXTS,
            stream_id=5,
            bucket_id=0,
            vault_key=vault_key,
            compress_types=self._MIXED_TYPES,
        )
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store), vault_key=vault_key)
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(i, _addr(i)) for i in range(len(self._MIXED_PLAINTEXTS))]
        batched = await reader.read_chunks(requests)

        for i, expected in enumerate(self._MIXED_PLAINTEXTS):
            assert batched[i] == expected
            assert await pool.read_chunk(_addr(i), cache=False) == expected

    async def test_large_all_zstd_batch_crosses_the_multithreaded_threshold(self, tmp_path: Path) -> None:
        """Above ``compression.py``'s own ``_ZSTD_BATCH_THREADS_MIN_ENTRIES``,
        ``decompress_many`` hands ``multi_decompress_to_buffer`` more than
        one thread — a real bucket this large exercises that path, not
        just the small fixtures above."""
        n = _ZSTD_BATCH_THREADS_MIN_ENTRIES + 50
        plaintexts = [((b"big-batch-chunk-%d-" % i) * 250)[:4096] for i in range(n)]
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", plaintexts, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))

        requests = [(i, _addr(i)) for i in range(n)]
        batched = await reader.read_chunks(requests)

        assert batched == {i: plaintext for i, plaintext in enumerate(plaintexts)}


class _ParkFirstDataReadStore:
    """Wraps a real ``LocalFsStore``, parking exactly the *first*
    data-region read (offset past the 16384-byte header/SizeStore region)
    on an ``asyncio.Event`` — every other read (the bucket's own
    header-open, and any later data read) proceeds immediately. Used to
    prove two merged runs' own reads genuinely overlap under
    ``max_concurrent_reads`` rather than one waiting for the other."""

    def __init__(self, backing: LocalFsStore) -> None:
        self._backing = backing
        self.data_read_offsets: list[int] = []
        self.parked = asyncio.Event()
        #: Set the moment a *second* data read starts — real time, not
        #: event-loop ticks, is what a caller needs to wait on to observe
        #: this reliably under real scheduling contention.
        self.second_read_started = asyncio.Event()
        self._release = asyncio.Event()
        self._parked_once = False

    def release(self) -> None:
        self._release.set()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if offset >= 16384:
            self.data_read_offsets.append(offset)
            if not self._parked_once:
                self._parked_once = True
                self.parked.set()
                await self._release.wait()
            elif len(self.data_read_offsets) >= 2:
                self.second_read_started.set()
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[str]:
        return await self._backing.listdir(path)


class TestReadChunksConcurrentReads:
    """``semaphore`` — concurrency *within* one bucket's own merged runs
    (see ``BucketReader.read_chunks``'s own docstring for the
    mechanism and the "caller already acquired the first run's permit"
    hand-off contract shared with ``chunk_walk.py``'s cross-bucket
    dispatch loop). ``_GAP_TOLERANCE`` is monkeypatched to 0 for these
    tests — the smallest, cheapest way to force a real two-run split with
    only the module's existing 3-chunk fixture (skip chunk 1's request
    entirely; its own compressed length becomes a real, positive gap
    between chunk 0's end and chunk 2's start, which even a real gap of a
    few dozen bytes fails a zero-tolerance check).

    None of these tests pre-acquire a permit before calling
    ``read_chunks()`` — irrelevant to what's being tested here (the fresh
    ``asyncio.Semaphore(N)`` instances below always have spare capacity
    for the second run to acquire), unlike
    ``test_dedup_chunk_walk.py``'s integration-level test, which exercises
    the real pre-acquired hand-off from the cross-bucket dispatch loop."""

    async def test_output_is_unchanged_across_two_runs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)
        pool = Pool(store, "Pool", DirCache(store))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._GAP_TOLERANCE", 0)

        requests = [(0, _addr(0)), (2, _addr(2))]  # skip chunk 1 -> forced 2-run split at tolerance=0
        result = await reader.read_chunks(requests, semaphore=asyncio.Semaphore(4))

        assert result == {0: _PLAINTEXTS[0], 2: _PLAINTEXTS[2]}

    async def test_the_two_runs_own_reads_actually_overlap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The actual value proposition, not just correctness: while the
        first run's own read is parked, the *second* run's own read
        (spied on directly) should already have been issued — a
        strictly-serial ``semaphore=None`` loop could never reach it while
        the first run's own ``await`` is still parked."""
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        backing = LocalFsStore(tmp_path)
        pool = Pool(backing, "Pool", DirCache(backing))
        reader = await pool.bucket(
            StreamId(5), BucketId(0)
        )  # header already opened via ``backing``, before the store swap below
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._GAP_TOLERANCE", 0)

        parking_store = _ParkFirstDataReadStore(backing)
        reader._store = parking_store

        requests = [(0, _addr(0)), (2, _addr(2))]
        task = asyncio.create_task(reader.read_chunks(requests, semaphore=asyncio.Semaphore(4)))
        await parking_store.parked.wait()
        # A fixed count of zero-duration ``asyncio.sleep(0)`` ticks only
        # guarantees event-loop turns, not real wall-clock time for the
        # second run's own (already-scheduled) read to actually issue
        # under real scheduling contention -- wait on the real condition
        # instead.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(parking_store.second_read_started.wait(), timeout=5.0)
        assert len(parking_store.data_read_offsets) == 2, (
            f"expected both runs' reads to have started, got {parking_store.data_read_offsets!r}"
        )
        parking_store.release()
        result = await task

        assert result == {0: _PLAINTEXTS[0], 2: _PLAINTEXTS[2]}

    async def test_no_semaphore_is_still_strictly_serial(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """The default: the second run's read must NOT start until the
        first one's is released — the inverse of the test above, proving
        ``semaphore=None`` (the default) hasn't quietly gained the
        concurrency it's supposed to gate."""
        _write_bucket(tmp_path / "Pool" / "5" / "0.buk", _PLAINTEXTS, stream_id=5, bucket_id=0)
        backing = LocalFsStore(tmp_path)
        pool = Pool(backing, "Pool", DirCache(backing))
        reader = await pool.bucket(StreamId(5), BucketId(0))
        monkeypatch.setattr("synology_apm_repo.sdk.dedup.pool._GAP_TOLERANCE", 0)

        parking_store = _ParkFirstDataReadStore(backing)
        reader._store = parking_store

        requests = [(0, _addr(0)), (2, _addr(2))]
        task = asyncio.create_task(reader.read_chunks(requests, semaphore=None))
        await parking_store.parked.wait()
        # Proving a negative (the second run's read does NOT start) needs
        # a real elapsed-time budget, not a fixed tick count -- a real
        # timeout is the right tool here, not an anti-pattern: if the
        # event fires within it, the assertion below catches the
        # regression; if it doesn't, that real 200ms is exactly the
        # confidence this test is after.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(parking_store.second_read_started.wait(), timeout=0.2)
        assert len(parking_store.data_read_offsets) == 1  # the second run has NOT started yet
        parking_store.release()
        result = await task

        assert result == {0: _PLAINTEXTS[0], 2: _PLAINTEXTS[2]}


def _write_bucket_with_real_size_store_redundancy(
    path: Path, entries: list[tuple[int, int]], *, corrupt_byte_idx: int | None = None
) -> None:
    """Like ``_write_bucket``, but builds a *real*, valid Redundancy blob
    over the tight SizeStore bytes (coverage=256) instead of the random
    filler every other bucket in this file uses — ``_write_bucket``'s own
    trailer is never validated by anything today, so it can't be used to
    test the SizeStore self-repair path, which needs a genuine parity
    blob to reconstruct from. ``corrupt_byte_idx``, when given, flips one
    byte of the tight SizeStore region *after* computing ``chunk_size_crc``
    and the Redundancy blob against the correct bytes — a real, single
    -window-recoverable corruption, the same shape
    ``test_format_redundancy.py``'s own ``TestAttemptRepair`` already
    proves the pure algorithm handles; this proves the wiring into
    ``BucketReader.open()`` itself."""
    from synology_apm_repo.sdk.format.redundancy import REDUNDANCY_MAGIC

    chunk_num = len(entries)
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    coverage = 256

    num_windows = (len(tight) + coverage - 1) // coverage
    step_crc: list[int] = []
    running = 0
    parity = bytearray(min(len(tight), 2 * coverage))
    for i in range(num_windows):
        start, end = i * coverage, min((i + 1) * coverage, len(tight))
        window = tight[start:end]
        running = zlib.crc32(window, running) & 0xFFFFFFFF
        step_crc.append(running)
        half = i % 2
        for j, b in enumerate(window):
            parity[half * coverage + j] ^= b
    redundancy_blob = (
        REDUNDANCY_MAGIC
        + struct.pack(">H", 0)
        + struct.pack(">IQ", coverage, len(tight))
        + b"".join(struct.pack(">I", c) for c in step_crc)
        + bytes(parity)
    )

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    on_disk_tight = bytearray(tight)
    if corrupt_byte_idx is not None:
        on_disk_tight[corrupt_byte_idx] ^= 0xFF
    sizestore_region = bytes(on_disk_tight) + b"\x00" * (16320 - len(tight))
    chunk_data = b"".join(os.urandom(size) for _type, size in entries)
    chunk_crc_store = os.urandom(4 * chunk_num)  # not validated by open() itself

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + chunk_data + chunk_crc_store + redundancy_blob)


class TestSizeStoreParityRepair:
    """``BucketReader.open()``'s own Redundancy-blob self-repair for a
    ``chunk_size_crc`` mismatch — the one place in this SDK where parity
    self-repair is genuinely transparent for *every* caller, since
    SizeStore's CRC is already checked unconditionally on every open."""

    async def test_a_single_corrupted_byte_is_transparently_repaired(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        _write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        store = LocalFsStore(tmp_path)

        reader = await BucketReader.open(store, "Pool/5/0.buk")  # must not raise

        assert list(reader.entries) == [
            SizeStoreEntry(CompressType.ZSTD, 100),
            SizeStoreEntry(CompressType.ZSTD, 200),
        ]
        assert reader.sizestore_repaired is True  # verify_checks.check_bucket_structure's own trigger

    async def test_a_normal_open_is_not_marked_repaired(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        _write_bucket(path, [b"x" * 100], stream_id=5, bucket_id=0)
        store = LocalFsStore(tmp_path)

        reader = await BucketReader.open(store, "Pool/5/0.buk")

        assert reader.sizestore_repaired is False

    async def test_an_unrecoverable_corruption_still_raises(self, tmp_path: Path) -> None:
        """Two non-adjacent corrupted bytes (more than one parity-
        repairable window) — the repair attempt fails its own final CRC
        re-check, so the original ``DataCorruptError`` still propagates,
        exactly as it did before this self-repair path existed."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100 + i) for i in range(400)]  # tight_len = 750: windows 0,1,2
        _write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=None)
        # Corrupt one byte in window 0 and one in window 2 -- NOT the
        # adjacent [bad_idx, bad_idx+1] pair attempt_repair() reconstructs
        # (that would be windows 0+1, still recoverable), so this is
        # genuinely beyond the single-region-pair repair bound.
        raw = bytearray(path.read_bytes())
        sizestore_off = 64
        raw[sizestore_off] ^= 0xFF  # window 0: byte 0
        raw[sizestore_off + 600] ^= 0xFF  # window 2: byte 600 (600 // 256 == 2)
        path.write_bytes(bytes(raw))
        store = LocalFsStore(tmp_path)

        with pytest.raises(DataCorruptError):
            await BucketReader.open(store, "Pool/5/0.buk")
