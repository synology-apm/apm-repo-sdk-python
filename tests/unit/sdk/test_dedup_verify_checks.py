"""Unit tests for ``synology_apm_repo.sdk.dedup.verify_checks``'s standalone
check primitives — synthetic bytes written to real files, no sample
repositories required.

Every primitive's *success* path is already exercised indirectly through
``units/verify_reachable.py``'s top-down walk (the
``test_units_verify_reachable_*.py`` files); its own call-site guards mean some
branches never get exercised that way at all (``check_map_and_attr_crc``
is skipped entirely when ``record_head.map_num == 0``, and a broken
bucket's ``ensure_chunk_crc_store()`` failure is always caught by
``check_bucket_structure`` first, never independently by
``check_chunk_ciphertext_crc``). This file calls every primitive directly
instead of engineering around those guards, covering each one's own
exception-to-``Finding`` mapping branch by branch.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path

import pytest
import zstandard

from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.pool import BucketReader
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import (
    Stage,
    Symptom,
    VerifyLevel,
    check_bucket_structure,
    check_chunk_ciphertext_crc,
    check_chunk_ciphertext_crcs,
    check_composition_header,
    check_map_and_attr_crc,
    check_raw_chunk_ciphertext_crc,
    check_record_head,
    check_repo_info,
    verify_chunk_map_crc_threaded,
)
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.format import composition as composition_module
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.composition import CompositionStatus, RecordHead
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.format.repo_info import MAGIC as REPO_INFO_MAGIC
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore

_SIZE_STORE_REGION_LEN = 16320  # COMPRESS_RESERVED_LENG(16384) - HEADER_LEN(64)


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


def _chunk_crc_store_trailer(*stored_chunks: bytes, corrupt_idx: int | None = None) -> tuple[bytes, int]:
    """``corrupt_idx``, when given, flips that one chunk's own recorded
    entry *before* ``crcOfChunkCrc`` is computed over the (now-tampered)
    trailer -- a wrong-but-internally-self-consistent trailer, so the
    mismatch is only caught by an actual per-chunk ciphertext check,
    never by the bucket's own structural self-consistency check."""
    crcs = [zlib.crc32(chunk) & 0xFFFFFFFF for chunk in stored_chunks]
    if corrupt_idx is not None:
        crcs[corrupt_idx] ^= 0xFFFFFFFF
    chunk_crc_store = b"".join(crc.to_bytes(4, "big") for crc in crcs)
    return chunk_crc_store, zlib.crc32(chunk_crc_store) & 0xFFFFFFFF


def _write_plain_bucket(path: Path, plaintext: bytes, *, corrupt_chunk_crc: bool = False) -> None:
    compressed = zstandard.ZstdCompressor().compress(plaintext)
    tight = _encode_size_store([(CompressType.ZSTD.value, len(compressed))])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    chunk_crc_store, crc_of_chunk_crc = _chunk_crc_store_trailer(
        compressed, corrupt_idx=0 if corrupt_chunk_crc else None
    )
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", 1)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    trailer = chunk_crc_store + os.urandom(redundancy_size((15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + compressed + trailer)


def _write_bucket_with_real_size_store_redundancy(
    path: Path, entries: list[tuple[int, int]], *, corrupt_byte_idx: int | None = None
) -> None:
    """Like ``_write_plain_bucket``, but builds a *real*, valid Redundancy
    blob over the tight SizeStore bytes (coverage=256) and a real,
    self-consistent ChunkCrcStore trailer, instead of either's usual
    random filler -- ``test_dedup_pool.py``'s own copy of this helper only
    needs the former to prove ``BucketReader.open()``'s own repair wiring;
    this one also needs a *valid* ChunkCrcStore trailer, since
    ``check_bucket_structure``'s own ``ensure_chunk_crc_store()`` call
    would otherwise add a spurious CORRUPTION finding alongside the
    SizeStore-repair finding this file's own test wants to isolate.
    ``corrupt_byte_idx``, when given, flips one byte of the tight
    SizeStore region *after* computing ``chunk_size_crc`` and the
    Redundancy blob against the correct bytes -- a real,
    single-window-recoverable corruption."""
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

    chunk_data = [os.urandom(size) for _type, size in entries]
    chunk_crc_store, crc_of_chunk_crc = _chunk_crc_store_trailer(*chunk_data)

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    on_disk_tight = bytearray(tight)
    if corrupt_byte_idx is not None:
        on_disk_tight[corrupt_byte_idx] ^= 0xFF
    sizestore_region = bytes(on_disk_tight) + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(chunk_data) + chunk_crc_store + redundancy_blob)


def _write_plain_bucket_multi(path: Path, plaintexts: list[bytes]) -> None:
    """``_write_plain_bucket``, generalized to more than one chunk — for
    ``TestCheckChunkFingerprintsBatched``, which needs several distinct
    chunks in the same bucket to prove a batched lookup's per-chunk
    granularity."""
    compressed_chunks = [zstandard.ZstdCompressor().compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(c)) for c in compressed_chunks]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    chunk_crc_store, crc_of_chunk_crc = _chunk_crc_store_trailer(*compressed_chunks)
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[29:33] = struct.pack(">I", crc_of_chunk_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (_SIZE_STORE_REGION_LEN - len(tight))
    trailer = chunk_crc_store + os.urandom(redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(compressed_chunks) + trailer)


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = REPO_INFO_MAGIC
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _write_composition_body_at(root: Path, *, stream_id: int, session_id: int, comp_offset: int, body: bytes) -> None:
    """Writes ``body`` (a chunk-map array, optionally followed by an
    attribute blob) at the real ``comp_offset``-relative position inside
    ``c0`` -- ``check_map_and_attr_crc`` never reads the ``RecordHead``
    region itself (its caller already parsed it), so the bytes before
    ``chunk_map_array_offset(comp_offset)`` don't need to be a real
    header/``RecordHead`` here, just present."""
    array_off = comp_offset + 32  # RECORD_HEAD_LENGTH
    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * array_off + body)


class TestCheckCompositionHeaderMissing:
    async def test_no_subfile_at_all_is_data_missing(self, tmp_path: Path) -> None:
        """No ``c0`` sub-file exists under ``comp_root`` at all -- distinct
        from a ``c0`` file that exists but fails to parse (covered by
        ``test_units_verify_reachable_composition.py``)."""
        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        finding = await check_composition_header(reader, path="some/path")
        assert finding is not None
        assert finding.stage is Stage.COMPOSITION
        assert finding.symptom is Symptom.DATA_MISSING


class TestCheckMapAndAttrCrcEmpty:
    async def test_zero_map_num_short_circuits_with_no_read(self, tmp_path: Path) -> None:
        """``units/verify_reachable.py`` guards this call on
        ``record_head.map_num > 0`` before ever calling it -- called
        directly here to prove the function's own contract holds
        independent of that guard: nothing to CRC, nothing read, no
        ``Finding`` raised."""
        store = LocalFsStore(tmp_path)  # empty -- any real read would raise NotFoundError
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE, map_num=0, map_crc=0, mode=0, attr_leng=0, attr_crc=0
        )
        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")
        assert findings == []
        assert repaired is None


class TestCheckChunkCiphertextCrcErrors:
    """``check_bucket_structure`` always resolves ``ensure_chunk_crc_store()``
    first and ``units/verify_reachable.py`` skips the per-chunk ciphertext
    check on that failure -- so ``check_chunk_ciphertext_crc``'s own
    ``NotFoundError``/``FormatError`` branches are exercised directly here,
    against a ``BucketReader`` opened before its underlying bytes are
    tampered with, to simulate the kind of race a real backend can produce
    between open and read."""

    async def test_chunk_data_disappearing_after_open_is_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        real_read = LocalFsStore.read

        async def flaky_read(self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if read_path.endswith("0.buk") and offset == 16384:  # the chunk-data read, not header/SizeStore/trailer
                raise NotFoundError("simulated race: bucket file disappeared", ref=read_path)
            return await real_read(self, read_path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", flaky_read)
        finding = await check_chunk_ciphertext_crc(reader, 0)
        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.DATA_MISSING

    async def test_truncated_chunk_crc_store_trailer_is_corruption(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The trailer read comes back shorter than ``ChunkCrcStore``
        needs -- ``parse_chunk_crc_store`` itself raises ``FormatError``
        for this, distinct from ``DataCorruptError`` (a self-consistency CRC
        mismatch, already covered by
        ``test_units_verify_reachable_composition.py``)."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        real_read = LocalFsStore.read

        async def truncating_read(
            self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None
        ) -> bytes:
            result = await real_read(self, read_path, offset, length)
            if read_path.endswith("0.buk") and offset > 16384:  # the ChunkCrcStore trailer read
                return result[:1]  # far short of the 4 bytes one entry needs
            return result

        monkeypatch.setattr(LocalFsStore, "read", truncating_read)
        finding = await check_chunk_ciphertext_crc(reader, 0)
        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.CORRUPTION


class TestCheckChunkCiphertextCrcsErrors:
    """``check_chunk_ciphertext_crcs``'s own two exception branches — both
    come from its ``reader.read_raw_chunks`` call, not from the per-chunk
    verify loop after it (that loop's ``DataCorruptError``-per-mismatch path is
    already exercised end to end by
    ``test_units_verify_reachable_extents.py``'s
    ``test_full_level_still_checks_ciphertext_crc_exhaustively_without_a_key``)."""

    async def test_chunk_data_disappearing_is_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        real_read = LocalFsStore.read

        async def flaky_read(self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if read_path.endswith("0.buk") and offset == 16384:  # the chunk-data read, not header/SizeStore/trailer
                raise NotFoundError("simulated race: bucket file disappeared", ref=read_path)
            return await real_read(self, read_path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", flaky_read)
        findings = await check_chunk_ciphertext_crcs(reader, [0])
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.DATA_MISSING

    async def test_a_compacted_chunk_in_the_request_is_corruption(self, tmp_path: Path) -> None:
        """``read_raw_chunks`` raises ``ChunkCompactedError`` (a ``FormatError``)
        for a ``COMPACTED`` slot — no real caller passes one (every real
        caller sources its indices from ``non_compacted_chunk_indices()``),
        but this check must still turn that into a ``Finding`` rather than
        propagate the raw exception."""
        path = tmp_path / "Pool" / "0" / "0.buk"
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
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bytes(header) + sizestore_region)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_chunk_ciphertext_crcs(reader, [0])
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION

    async def test_a_real_mismatch_within_a_successful_batched_fetch_is_reported(self, tmp_path: Path) -> None:
        """Distinct from the two exception branches above: ``read_raw_chunks``
        succeeds outright here, and the mismatch is found by the per-chunk
        loop that follows it."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        plaintexts = [bytes([i]) * 4096 for i in range(2)]
        _write_plain_bucket_multi(path, plaintexts)
        # Flip chunk 0's own stored bytes after writing -- corrupts its
        # ciphertext against the still-correct ChunkCrcStore entry,
        # without touching the trailer's own self-consistency at all.
        raw = bytearray(path.read_bytes())
        raw[16384] ^= 0xFF
        path.write_bytes(bytes(raw))
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_chunk_ciphertext_crcs(reader, [0, 1])

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.MISMATCH


_VAULT_LAYOUT = RepoLayout(kind=RepoKind.VAULT, repo_root="")

_DUMMY_REPO_INFO = RepoInfo(
    uuid="0" * 16,
    major=3,
    minor=0,
    repo_type=None,
    repo_flag=None,
    is_global_dedup_supported=None,
    is_worm_supported=None,
    compress_algorithm=None,
    encrypt_algorithm=None,
    raw={},
)
"""Never read by ``check_repo_info`` itself (it only touches
``repo.dir_cache``/``repo.layout``/``repo.store``) — a placeholder so
``DedupRepo`` can be constructed directly, without going through
``DedupRepo.open()``'s own repo_info-reading requirement, for a case
where no repo_info file exists at all."""


class TestCheckRepoInfo:
    """``check_repo_info``'s three failure branches — the ``resolve_seq_path``
    lookup, the file read, and the JSON parse, each a distinct way for
    ``repo_info`` to be unusable."""

    async def test_no_repo_info_file_at_all_is_file_missing(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)  # empty -- resolve_seq_path itself raises NotFoundError
        repo = DedupRepo(store, _VAULT_LAYOUT, _DUMMY_REPO_INFO, DirCache(store))

        findings = await check_repo_info(repo)

        assert len(findings) == 1
        assert findings[0].stage is Stage.REPO_INFO
        assert findings[0].symptom is Symptom.FILE_MISSING

    async def test_repo_info_disappearing_after_listing_is_file_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The directory listing still shows ``repo_info`` (so
        ``resolve_seq_path`` itself succeeds), but the real read of it
        races and fails -- a different ``FILE_MISSING`` path than the
        one above."""
        _write_repo_info(tmp_path / "repo_info")
        store = LocalFsStore(tmp_path)
        real_read = LocalFsStore.read

        async def flaky_read(self: LocalFsStore, path: str, offset: int = 0, length: int | None = None) -> bytes:
            if path == "repo_info":
                raise NotFoundError("simulated race: repo_info disappeared", ref=path)
            return await real_read(self, path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", flaky_read)
        repo = DedupRepo(store, _VAULT_LAYOUT, _DUMMY_REPO_INFO, DirCache(store))

        findings = await check_repo_info(repo)

        assert len(findings) == 1
        assert findings[0].stage is Stage.REPO_INFO
        assert findings[0].symptom is Symptom.FILE_MISSING

    async def test_corrupt_repo_info_is_corruption(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        raw = bytearray((tmp_path / "repo_info").read_bytes())
        raw[0] ^= 0xFF  # bad magic
        (tmp_path / "repo_info").write_bytes(bytes(raw))
        store = LocalFsStore(tmp_path)
        repo = DedupRepo(store, _VAULT_LAYOUT, _DUMMY_REPO_INFO, DirCache(store))

        findings = await check_repo_info(repo)

        assert len(findings) == 1
        assert findings[0].stage is Stage.REPO_INFO
        assert findings[0].symptom is Symptom.CORRUPTION


class TestCheckRecordHeadDataMissing:
    async def test_no_subfile_at_all_is_data_missing(self, tmp_path: Path) -> None:
        """Distinct from ``TestCheckCompositionHeaderMissing``: this is
        ``check_record_head``'s own ``NotFoundError`` branch, not
        ``check_composition_header``'s."""
        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))

        finding, record_head = await check_record_head(reader, 64, path="some/path")

        assert finding is not None
        assert finding.stage is Stage.FILE_MAP
        assert finding.symptom is Symptom.DATA_MISSING
        assert record_head is None


class TestVerifyChunkMapCrcThreaded:
    async def test_an_array_at_the_thread_hop_threshold_still_verifies_correctly(self) -> None:
        """``should_thread_chunk_map_crc``'s own threshold (tested directly
        in ``test_format_composition.py``) is exercised here through the
        ``asyncio.to_thread`` branch it gates, not just the plain
        direct-call branch every other test in this file already takes
        (their arrays are all far smaller)."""
        array = os.urandom(composition_module._CRC_THREAD_HOP_MIN_BYTES)
        expected_crc = zlib.crc32(array) & 0xFFFFFFFF

        await verify_chunk_map_crc_threaded(array, expected_crc)  # no raise

        with pytest.raises(DataCorruptError):
            await verify_chunk_map_crc_threaded(array, expected_crc ^ 0xFFFFFFFF)


class TestCheckMapAndAttrCrcErrors:
    async def _reader(self, tmp_path: Path) -> CompositionReader:
        store = LocalFsStore(tmp_path)
        return CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))

    async def test_read_failure_is_data_missing(self, tmp_path: Path) -> None:
        reader = await self._reader(tmp_path)  # no c0 file at all -- read_at raises NotFoundError
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE, map_num=1, map_crc=0, mode=0, attr_leng=0, attr_crc=0
        )

        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")

        assert len(findings) == 1
        assert findings[0].stage is Stage.COMPOSITION
        assert findings[0].symptom is Symptom.DATA_MISSING
        assert repaired is None

    async def test_map_crc_mismatch_is_repaired_via_parity_when_recoverable(self, tmp_path: Path) -> None:
        """A single corrupted window is transparently reconstructed from
        the record's own Redundancy trailer rather than reported as
        unresolved corruption -- distinct from ``test_map_crc_mismatch_is_reported``
        below, whose ``map_crc`` is simply wrong with no trailer to
        recover from at all."""
        from synology_apm_repo.sdk.format.const import REDUNDANCY_COVERAGE_COMPOSITION
        from synology_apm_repo.sdk.format.redundancy import REDUNDANCY_MAGIC

        coverage = REDUNDANCY_COVERAGE_COMPOSITION
        map_array = os.urandom(20 * 500)  # 10000 bytes -- spans 2 windows at coverage=8192
        map_crc = zlib.crc32(map_array) & 0xFFFFFFFF

        # Build a real, valid Redundancy blob for map_array (write-time
        # equivalent of format.redundancy.attempt_repair's own read-time
        # reconstruction).
        num_windows = (len(map_array) + coverage - 1) // coverage
        step_crc: list[int] = []
        running = 0
        parity = bytearray(min(len(map_array), 2 * coverage))
        for i in range(num_windows):
            start, end = i * coverage, min((i + 1) * coverage, len(map_array))
            window = map_array[start:end]
            running = zlib.crc32(window, running) & 0xFFFFFFFF
            step_crc.append(running)
            half = i % 2
            for j, b in enumerate(window):
                parity[half * coverage + j] ^= b
        redundancy_blob = (
            REDUNDANCY_MAGIC
            + struct.pack(">H", 0)
            + struct.pack(">IQ", coverage, len(map_array))
            + b"".join(struct.pack(">I", c) for c in step_crc)
            + bytes(parity)
        )

        corrupted_map_array = bytearray(map_array)
        corrupted_map_array[100] ^= 0xFF  # inside window 0 -- recoverable
        _write_composition_body_at(
            tmp_path / "Composition",
            stream_id=7,
            session_id=3,
            comp_offset=64,
            body=bytes(corrupted_map_array) + redundancy_blob,
        )
        reader = await self._reader(tmp_path)
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE, map_num=500, map_crc=map_crc, mode=0, attr_leng=0, attr_crc=0
        )

        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")

        assert len(findings) == 1
        assert findings[0].stage is Stage.COMPOSITION
        assert findings[0].symptom is Symptom.REPAIRED_VIA_PARITY
        assert repaired == map_array

    async def test_map_crc_mismatch_is_reported(self, tmp_path: Path) -> None:
        map_array = os.urandom(20)  # one CHUNK_MAP_RECORD_LENGTH-sized entry, content irrelevant here
        _write_composition_body_at(tmp_path / "Composition", stream_id=7, session_id=3, comp_offset=64, body=map_array)
        reader = await self._reader(tmp_path)
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE,
            map_num=1,
            map_crc=(zlib.crc32(map_array) & 0xFFFFFFFF) ^ 0xFFFFFFFF,  # deliberately wrong
            mode=0,
            attr_leng=0,
            attr_crc=0,
        )

        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")

        assert len(findings) == 1
        assert findings[0].stage is Stage.COMPOSITION
        assert findings[0].symptom is Symptom.MISMATCH
        assert repaired is None

    async def test_attr_crc_mismatch_is_reported(self, tmp_path: Path) -> None:
        map_array = os.urandom(20)
        attr_bytes = b'{"k": "v"}'
        _write_composition_body_at(
            tmp_path / "Composition", stream_id=7, session_id=3, comp_offset=64, body=map_array + attr_bytes
        )
        reader = await self._reader(tmp_path)
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE,
            map_num=1,
            map_crc=zlib.crc32(map_array) & 0xFFFFFFFF,  # correct -- isolates the attr_crc branch
            mode=0,
            attr_leng=len(attr_bytes),
            attr_crc=(zlib.crc32(attr_bytes) & 0xFFFFFFFF) ^ 0xFFFFFFFF,  # deliberately wrong
        )

        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")

        assert len(findings) == 1
        assert findings[0].stage is Stage.COMPOSITION
        assert findings[0].symptom is Symptom.MISMATCH
        assert repaired is None


class TestCheckBucketStructureErrors:
    async def test_size_mismatch_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        path.write_bytes(path.read_bytes() + b"\x00\x00\x00\x00")  # grows the real on-disk size after open

        findings = await check_bucket_structure(store, reader)

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.MISMATCH
        assert "expected_bucket_size" in findings[0].detail

    async def test_trailer_disappearing_after_open_is_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        real_read = LocalFsStore.read

        async def flaky_read(self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            if read_path.endswith("0.buk") and offset > 16384:  # the ChunkCrcStore trailer read
                raise NotFoundError("simulated race: bucket file disappeared", ref=read_path)
            return await real_read(self, read_path, offset, length)

        monkeypatch.setattr(LocalFsStore, "read", flaky_read)

        findings = await check_bucket_structure(store, reader)

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.DATA_MISSING
        assert "ChunkCrcStore trailer" in findings[0].detail

    async def test_truncated_trailer_is_corruption(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        real_read = LocalFsStore.read

        async def truncating_read(
            self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None
        ) -> bytes:
            result = await real_read(self, read_path, offset, length)
            if read_path.endswith("0.buk") and offset > 16384:
                return result[:1]  # far short of the 4 bytes one entry needs
            return result

        monkeypatch.setattr(LocalFsStore, "read", truncating_read)

        findings = await check_bucket_structure(store, reader)

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION


class TestCheckBucketStructureSizeStoreRepair:
    """``check_bucket_structure``'s own ``Symptom.REPAIRED_VIA_PARITY``
    finding for a SizeStore CRC mismatch ``BucketReader.open()`` already
    repaired -- the SizeStore counterpart to
    ``TestCheckMapAndAttrCrcErrors``'s own map-CRC repair test, one layer
    up: ``open()`` itself has no ``Finding``-returning contract, so this
    is the first place a successful repair actually becomes visible."""

    async def test_repaired_sizestore_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        _write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/5/0.buk")
        assert reader.sizestore_repaired is True  # confirms the fixture actually exercised repair

        findings = await check_bucket_structure(store, reader)

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.REPAIRED_VIA_PARITY

    async def test_a_normal_bucket_reports_nothing(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_bucket_structure(store, reader)

        assert findings == []

    async def test_repaired_sizestore_reuses_the_size_already_fetched_during_repair(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_attempt_size_store_repair`` already fetched this file's
        actual size to locate its trailer -- ``check_bucket_structure``
        must reuse it (``reader.known_file_size``) rather than pay for a
        second ``store.size()`` round-trip on the same path."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        _write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/5/0.buk")
        assert reader.sizestore_repaired is True
        assert reader.known_file_size is not None  # confirms the fixture exercised the reuse path

        size_calls = 0
        real_size = LocalFsStore.size

        async def counting_size(self: LocalFsStore, path: str) -> int:
            nonlocal size_calls
            size_calls += 1
            return await real_size(self, path)

        monkeypatch.setattr(LocalFsStore, "size", counting_size)

        findings = await check_bucket_structure(store, reader)

        assert findings[0].symptom is Symptom.REPAIRED_VIA_PARITY
        assert size_calls == 0  # reused reader.known_file_size instead of re-fetching


class TestCheckChunkCiphertextCrcMismatchAndSuccess:
    """``check_chunk_ciphertext_crc``/``check_raw_chunk_ciphertext_crc``'s
    own remaining branches: a genuine per-chunk mismatch (as opposed to
    ``TestCheckChunkCiphertextCrcErrors``'s missing-data/corrupt-trailer
    cases), and the plain success path."""

    async def test_singular_mismatch_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096, corrupt_chunk_crc=True)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        finding = await check_chunk_ciphertext_crc(reader, 0)

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.MISMATCH

    async def test_singular_success_reports_nothing(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        assert await check_chunk_ciphertext_crc(reader, 0) is None

    async def test_raw_variant_data_missing(self, tmp_path: Path) -> None:
        """``check_raw_chunk_ciphertext_crc``'s own ``NotFoundError``
        branch -- unlike the singular form above, this comes from
        ``ensure_chunk_crc_store()`` (the trailer), not the chunk-data
        read, since the raw bytes are already in the caller's hand."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        raw = await reader.read_raw_chunks([0])

        async def always_missing(
            self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None
        ) -> bytes:
            raise NotFoundError("simulated race: bucket file disappeared", ref=read_path)

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(LocalFsStore, "read", always_missing)
            finding = await check_raw_chunk_ciphertext_crc(reader, 0, raw[0])

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.DATA_MISSING

    async def test_raw_variant_corruption(self, tmp_path: Path) -> None:
        """``check_raw_chunk_ciphertext_crc``'s own ``FormatError``
        branch, via the same truncated-trailer race as the singular
        form's own equivalent test."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        _write_plain_bucket(path, bytes([1]) * 4096)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        raw = await reader.read_raw_chunks([0])

        async def truncated(self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            return b"\x00"  # far short of the 4 bytes one ChunkCrcStore entry needs

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(LocalFsStore, "read", truncated)
            finding = await check_raw_chunk_ciphertext_crc(reader, 0, raw[0])

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.CORRUPTION


class TestEnumValues:
    """``Symptom``/``VerifyLevel``'s ``.value`` strings are part of the
    CLI's ``--json`` output contract, not just internal labels -- pinned
    directly so a rename ships as an intentional, reviewed diff to this
    test rather than silently."""

    def test_symptom_values(self) -> None:
        assert {s.value for s in Symptom} == {
            "Corruption",
            "FileMissing",
            "Mismatch",
            "DataMissing",
            "KeyMissing",
            "RepairedViaParity",
        }

    def test_verify_level_values(self) -> None:
        assert VerifyLevel.QUICK.value == "quick"
        assert VerifyLevel.FULL.value == "full"


__all__: list[str] = []
