"""Unit tests for ``synology_apm_repo.sdk.dedup.verify_checks``'s check
primitives, called directly so each exception-to-``Finding`` branch is
covered, including ones their callers' guards (``dedup/verify_walk.py``'s
``map_num > 0``, ``check_bucket_structure`` resolving the ChunkCrcStore
trailer first) keep the end-to-end ``test_units_verify_reachable_*.py``
walk from reaching. Most race cases open a reader, then fail a later read.
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

import pytest

from support.format_builders import (
    encode_size_store,
    redundancy_blob_bytes,
    repo_transaction_bytes,
    sizestore_region_pad,
)
from support.repo_builders import write_bucket, write_repo_info
from synology_apm_repo.sdk.dedup import verify_checks
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.pool import BucketReader
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.dedup.verify_checks import (
    check_bucket_structure,
    check_chunk_ciphertext_crc,
    check_chunk_ciphertext_crcs,
    check_composition_header,
    check_map_and_attr_crc,
    check_record_head,
    check_repo_info,
    verify_chunk_map_crc_threaded,
)
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.findings import Stage, Symptom
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.composition import CompositionStatus, RecordHead
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import REDUNDANCY_COVERAGE_COMPOSITION
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.pool_fakes import write_bucket_with_real_size_store_redundancy


def _write_composition_body_at(root: Path, *, stream_id: int, session_id: int, comp_offset: int, body: bytes) -> None:
    """Writes ``body`` (a chunk-map array, optionally followed by an
    attribute blob) at ``chunk_map_array_offset(comp_offset)`` inside ``c0``.
    ``check_map_and_attr_crc`` takes an already-parsed ``RecordHead``, so the
    bytes before it are zero filler."""
    array_off = comp_offset + 32  # RECORD_HEAD_LENGTH
    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * array_off + body)


class TestCheckCompositionHeaderMissing:
    async def test_no_subfile_at_all_is_data_missing(self, tmp_path: Path) -> None:
        """No ``c0`` sub-file at all (a ``c0`` that fails to parse is
        covered by ``test_units_verify_reachable_composition.py``)."""
        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        finding = await check_composition_header(reader, path="some/path")
        assert finding is not None
        assert finding.stage is Stage.COMPOSITION
        assert finding.symptom is Symptom.DATA_MISSING


class TestCheckMapAndAttrCrcEmpty:
    async def test_zero_map_num_short_circuits_with_no_read(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)  # empty -- any real read would raise NotFoundError
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))
        record_head = RecordHead(
            status=CompositionStatus.COMPLETE, map_num=0, map_crc=0, mode=0, attr_leng=0, attr_crc=0
        )
        findings, repaired = await check_map_and_attr_crc(reader, 64, record_head, path="some/path")
        assert findings == []
        assert repaired is None


class TestCheckChunkCiphertextCrcErrors:
    """``check_chunk_ciphertext_crc``'s ``NotFoundError``/``FormatError``
    branches, from reads that fail after the reader opened."""

    async def test_chunk_data_disappearing_after_open_is_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
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
        """A short trailer read makes ``parse_chunk_crc_store`` raise a plain
        ``FormatError``, not the ``DataCorruptError`` of a ``crcOfChunkCrc``
        mismatch."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
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
    """``check_chunk_ciphertext_crcs``: a failing batched
    ``read_raw_chunks`` falls back to one ``check_chunk_ciphertext_crc`` per
    chunk, whose own failure becomes that chunk's ``Finding``."""

    async def test_chunk_data_disappearing_is_data_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
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
        """A ``COMPACTED`` slot (real callers take indices from
        ``non_compacted_chunk_ranges()``, so never pass one) is a
        ``Finding``, not a raised ``ChunkCompactedError``."""
        path = tmp_path / "Pool" / "0" / "0.buk"
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
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bytes(header) + sizestore_region)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_chunk_ciphertext_crcs(reader, [0])
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION

    async def test_a_real_mismatch_within_a_successful_batched_fetch_is_reported(self, tmp_path: Path) -> None:
        """``read_raw_chunks`` succeeds; the mismatch is found by the
        per-chunk check that follows it."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        plaintexts = [bytes([i]) * 4096 for i in range(2)]
        write_bucket(path, plaintexts)
        # Corrupt chunk 0's stored bytes; its ChunkCrcStore entry stays correct.
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
"""Never read by ``check_repo_info``; lets ``DedupRepo`` be constructed
directly, without ``DedupRepo.open()`` reading a repo_info file."""


class TestCheckRepoInfo:
    """``check_repo_info``'s generation choice and its three failure
    branches: the lookup, the file read, and the parse."""

    async def test_object_storage_checks_the_committed_generation_not_the_newest_file(self, tmp_path: Path) -> None:
        """The latest committed transaction is 7, so ``repo_info.5`` is the
        repository's generation; the newer, uncommitted ``repo_info.9`` is
        not checked (FORMAT-SPEC.md: Multi-generation selection)."""
        write_repo_info(tmp_path / "repo_info.5", uuid=bytes(16))
        (tmp_path / "repo_info.9").write_bytes(b"not a repo_info")
        (tmp_path / "repo_transactions").mkdir()
        (tmp_path / "repo_transactions" / "repo_transaction.6").write_bytes(
            repo_transaction_bytes({"transaction_id": 7})
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="")
        repo = DedupRepo(store, layout, _DUMMY_REPO_INFO, DirCache(store))

        assert await check_repo_info(repo) == []

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
        """``resolve_seq_path`` finds ``repo_info`` but the read fails."""
        write_repo_info(tmp_path / "repo_info", uuid=bytes(16))
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
        write_repo_info(tmp_path / "repo_info", uuid=bytes(16))
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
        store = LocalFsStore(tmp_path)
        reader = CompositionReader(store, DirCache(store), "Composition", StreamId(7), SessionId(3))

        finding, record_head = await check_record_head(reader, 64, path="some/path")

        assert finding is not None
        assert finding.stage is Stage.FILE_MAP
        assert finding.symptom is Symptom.DATA_MISSING
        assert record_head is None


class TestVerifyChunkMapCrcThreaded:
    async def test_an_array_at_the_thread_hop_threshold_still_verifies_correctly(self) -> None:
        """An array at ``should_thread_chunk_map_crc``'s threshold verifies
        correctly through the ``asyncio.to_thread`` branch it gates."""
        array = os.urandom(verify_checks._CRC_THREAD_HOP_MIN_BYTES)
        expected_crc = zlib.crc32(array) & 0xFFFFFFFF

        await verify_chunk_map_crc_threaded(array, expected_crc)  # no raise

        with pytest.raises(DataCorruptError, match="chunk-map array CRC mismatch"):
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
        """A single corrupted window is reconstructed from the record's
        Redundancy trailer and reported as ``REPAIRED_VIA_PARITY``."""
        map_array = os.urandom(20 * 500)  # 10000 bytes -- spans 2 windows at coverage=8192
        map_crc = zlib.crc32(map_array) & 0xFFFFFFFF
        redundancy_blob = redundancy_blob_bytes(map_array, coverage=REDUNDANCY_COVERAGE_COMPOSITION)

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
        write_bucket(path, [bytes([1]) * 4096])
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
        write_bucket(path, [bytes([1]) * 4096])
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
        write_bucket(path, [bytes([1]) * 4096])
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
    """A SizeStore CRC mismatch ``BucketReader.open()`` repaired becomes
    visible as ``check_bucket_structure``'s ``REPAIRED_VIA_PARITY``
    finding."""

    async def test_repaired_sizestore_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/5/0.buk")
        assert reader.sizestore_repaired is True  # confirms the fixture actually exercised repair

        findings = await check_bucket_structure(store, reader)

        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.REPAIRED_VIA_PARITY

    async def test_a_normal_bucket_reports_nothing(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_bucket_structure(store, reader)

        assert findings == []

    async def test_repaired_sizestore_reuses_the_size_already_fetched_during_repair(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The SizeStore repair already fetched the file size
        (``reader.known_file_size``); no second ``store.size()``."""
        path = tmp_path / "Pool" / "5" / "0.buk"
        entries = [(CompressType.ZSTD.value, 100), (CompressType.ZSTD.value, 200)]
        write_bucket_with_real_size_store_redundancy(path, entries, corrupt_byte_idx=0)
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
    """``check_chunk_ciphertext_crc``'s mismatch and success paths, and its
    error branches when given ``raw``."""

    async def test_singular_mismatch_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096], corrupt_chunk_crc_idx=0)
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        finding = await check_chunk_ciphertext_crc(reader, 0)

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.MISMATCH

    async def test_singular_success_reports_nothing(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        assert await check_chunk_ciphertext_crc(reader, 0) is None

    async def test_raw_variant_data_missing(self, tmp_path: Path) -> None:
        """Given ``raw``, the only read left is the ChunkCrcStore trailer,
        so that is where ``NotFoundError`` comes from."""
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        raw = await reader.read_raw_chunks([0])

        async def always_missing(
            self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None
        ) -> bytes:
            raise NotFoundError("simulated race: bucket file disappeared", ref=read_path)

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(LocalFsStore, "read", always_missing)
            finding = await check_chunk_ciphertext_crc(reader, 0, raw[0])

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.DATA_MISSING

    async def test_raw_variant_corruption(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        raw = await reader.read_raw_chunks([0])

        async def truncated(self: LocalFsStore, read_path: str, offset: int = 0, length: int | None = None) -> bytes:
            return b"\x00"  # far short of the 4 bytes one ChunkCrcStore entry needs

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(LocalFsStore, "read", truncated)
            finding = await check_chunk_ciphertext_crc(reader, 0, raw[0])

        assert finding is not None
        assert finding.stage is Stage.BUCKET
        assert finding.symptom is Symptom.CORRUPTION


class TestCheckBucketStructureKnownFileSize:
    """``known_file_size``: a size the caller already has (from the directory
    listing) replaces the ``store.size()`` request, and is judged the same."""

    async def test_a_known_size_means_no_size_request(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        async def no_size(self: LocalFsStore, size_path: str) -> int:
            raise AssertionError("store.size() must not be asked when the size is known")

        monkeypatch.setattr(LocalFsStore, "size", no_size)

        findings = await check_bucket_structure(store, reader, known_file_size=path.stat().st_size)

        assert findings == []

    async def test_a_known_size_that_disagrees_with_the_expected_size_is_a_mismatch(self, tmp_path: Path) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")

        findings = await check_bucket_structure(store, reader, known_file_size=path.stat().st_size + 4)

        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.MISMATCH)]

    async def test_without_a_known_size_the_store_is_asked(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "Pool" / "0" / "0.buk"
        write_bucket(path, [bytes([1]) * 4096])
        store = LocalFsStore(tmp_path)
        reader = await BucketReader.open(store, "Pool/0/0.buk")
        asked: list[str] = []
        real_size = LocalFsStore.size

        async def spying_size(self: LocalFsStore, size_path: str) -> int:
            asked.append(size_path)
            return await real_size(self, size_path)

        monkeypatch.setattr(LocalFsStore, "size", spying_size)

        assert await check_bucket_structure(store, reader) == []
        assert asked == ["Pool/0/0.buk"]


class TestShouldThreadChunkMapCrc:
    def test_just_under_the_threshold_is_not_worth_threading(self) -> None:
        assert (
            verify_checks.should_thread_chunk_map_crc(b"\x00" * (verify_checks._CRC_THREAD_HOP_MIN_BYTES - 1)) is False
        )

    def test_exactly_at_the_threshold_is_worth_threading(self) -> None:
        assert verify_checks.should_thread_chunk_map_crc(b"\x00" * verify_checks._CRC_THREAD_HOP_MIN_BYTES) is True

    def test_well_past_the_threshold_is_worth_threading(self) -> None:
        assert (
            verify_checks.should_thread_chunk_map_crc(b"\x00" * (verify_checks._CRC_THREAD_HOP_MIN_BYTES * 4)) is True
        )

    def test_a_small_real_map_array_is_not_worth_threading(self) -> None:
        # A handful of ChunkMapRecord entries (20 bytes each) is the
        # common case -- well under the threshold.
        assert verify_checks.should_thread_chunk_map_crc(b"\x00" * 200) is False
