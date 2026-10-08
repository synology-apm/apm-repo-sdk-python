"""Unit tests for ``synology_apm_repo.sdk.units.verify_reachable``'s bucket
stage: a reachable version's missing, corrupt, legacy or vault-encrypted
bucket, each surfaced as its finding by a whole ``verify_reachable`` walk
(``test_dedup_verify_bucket_check.py`` checks one bucket directly)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from support.format_builders import mapping_record
from support.repo_builders import write_bucket, write_composition_entries, write_file_map, write_legacy_bucket
from synology_apm_repo.sdk.findings import Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.units.verify_reachable import verify_reachable
from unit.sdk.verify_reachable_fakes import open_vault_repo, write_fs_workload

_STREAM_ID = 8
_SESSION_ID = 3
_COMP_OFFSET = 64  # right after the composition sub-file's own 64-byte cMpS header


class TestBucketStageFindings:
    """A missing/corrupt bucket, and a legacy uncompressed bucket the
    per-chunk checks don't apply to, each take a distinct path through
    ``_check_one_bucket``."""

    def _write_reachable_fs_version(self, tmp_path: Path, dedup_img_size: int) -> None:
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=dedup_img_size,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )

    async def test_missing_bucket_is_data_missing(self, tmp_path: Path) -> None:
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.DATA_MISSING for f in findings)

    async def test_corrupt_bucket_header_is_corruption(self, tmp_path: Path) -> None:
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        bucket_path = tmp_path / "@data" / "Pool" / "0" / "0.buk"
        write_bucket(bucket_path, [bytes([1]) * 4096])
        raw = bytearray(bucket_path.read_bytes())
        raw[0] ^= 0xFF  # corrupt the "bFiL" magic
        bucket_path.write_bytes(bytes(raw))

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.BUCKET and f.symptom is Symptom.CORRUPTION for f in findings)

    async def test_legacy_uncompressed_bucket_has_nothing_to_check(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        self._write_reachable_fs_version(tmp_path, dedup_img_size=len(plaintexts) * 4096)
        write_legacy_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert findings == []

    async def test_unexpected_error_opening_a_bucket_is_a_finding_not_a_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unexpected exception from the bucket *open* becomes a
        ``Finding`` rather than escaping ``check_all_buckets``'s concurrent
        gather and cancelling the other bucket checks.
        (``test_units_verify_reachable_worker.py``'s
        ``TestConcurrentBucketCheckIsolation`` covers a failure after a
        successful open.)"""
        self._write_reachable_fs_version(tmp_path, dedup_img_size=4096)
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", [bytes([1]) * 4096])

        import synology_apm_repo.sdk.dedup.pool as pool_module

        async def failing_open(self: pool_module.Pool, key: tuple[StreamId, BucketId]) -> pool_module.BucketReader:
            raise RuntimeError("simulated unexpected open failure")

        monkeypatch.setattr(pool_module.Pool, "_open_bucket_by_key", failing_open)

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert len(findings) == 1
        assert findings[0].stage is Stage.BUCKET
        assert findings[0].symptom is Symptom.CORRUPTION
        assert "unexpected error" in findings[0].detail


class TestKeyMissing:
    """A vault-encrypted bucket in a repository opened without a vault key
    surfaces a ``KEY_MISSING`` finding, derived from the header, so it fires
    at both levels."""

    async def test_encrypted_bucket_without_key_is_key_missing(self, tmp_path: Path) -> None:
        plaintexts = [bytes([1]) * 4096]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=1),
        )
        write_bucket(tmp_path / "@data" / "Pool" / "0" / "0.buk", plaintexts, vault_key=os.urandom(32))

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.QUICK)
        finally:
            await repo.close()
        assert any(f.stage is Stage.ENCRYPT_KEY and f.symptom is Symptom.KEY_MISSING for f in findings)

    async def test_full_level_still_checks_ciphertext_crc_exhaustively_without_a_key(self, tmp_path: Path) -> None:
        """Only FULL's decrypt+fingerprint half needs a vault key; its
        ciphertext CRC32 still covers every chunk, so corrupting the last of
        several is caught."""
        chunk_num = 6
        plaintexts = [bytes([1]) * 4096 for _ in range(chunk_num)]
        dedup_img_path = write_fs_workload(
            tmp_path,
            workload_id=10,
            version_uid="vuid-1",
            target_id="fsA",
            meta_dirname="FSA_meta",
            dedup_version_id=1,
            dedup_img_size=len(plaintexts) * 4096,
        )
        write_file_map(tmp_path / "db" / "file_map", [(dedup_img_path, _STREAM_ID, _SESSION_ID, _COMP_OFFSET, 1, 2)])
        write_composition_entries(
            tmp_path / "@data" / "Composition",
            stream_id=_STREAM_ID,
            session_id=_SESSION_ID,
            entries=mapping_record(0, 0, 0, map_num=chunk_num),
        )
        write_bucket(
            tmp_path / "@data" / "Pool" / "0" / "0.buk",
            plaintexts,
            vault_key=os.urandom(32),
            corrupt_chunk_crc_idx=chunk_num - 1,
        )

        repo = await open_vault_repo(tmp_path)
        try:
            findings = await verify_reachable(repo, VerifyLevel.FULL)
        finally:
            await repo.close()
        assert any(f.stage is Stage.ENCRYPT_KEY and f.symptom is Symptom.KEY_MISSING for f in findings)
        assert any(
            f.stage is Stage.BUCKET and f.symptom is Symptom.MISMATCH and "ciphertext" in f.detail for f in findings
        )
