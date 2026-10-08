"""Unit tests for ``synology_apm_repo.sdk.dedup.verify_bucket_check``'s per-bucket
core (``check_one_bucket`` and its helpers), called directly on one byte-built
bucket instead of through a ``verify_reachable`` walk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from support.repo_builders import write_bucket, write_inf_and_fgp, write_legacy_bucket
from synology_apm_repo.sdk.dedup import verify_bucket_check as module
from synology_apm_repo.sdk.dedup.pool import FULL_VERIFY, NO_VERIFY, BucketReaderCache, Pool, VerifyPolicy
from synology_apm_repo.sdk.dedup.pool_descriptor import PoolDescriptor
from synology_apm_repo.sdk.dedup.verify_bucket_check import (
    VerifyExecutor,
    _listed_size,
    check_one_bucket,
)
from synology_apm_repo.sdk.errors import DataCorruptError, PermissionDeniedError, StorageBackendError
from synology_apm_repo.sdk.findings import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.format.bucket import MODE_VAULT_ENCRYPT
from synology_apm_repo.sdk.identifiers import BucketId, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

_KEY = (StreamId(0), BucketId(0))
_PLAINTEXTS = [((b"chunk-%d-" % i) * 600)[:4096] for i in range(3)]


def _pool(
    root: Path, verify: VerifyPolicy = FULL_VERIFY, *, vault_key: bytes | None = None
) -> tuple[Pool, LocalFsStore]:
    store = LocalFsStore(root)
    pool = Pool(store, "Pool", DirCache(store), vault_key=vault_key, verify=verify)
    return pool, store


async def _check(
    root: Path, level: VerifyLevel = VerifyLevel.FULL, *, vault_key: bytes | None = None
) -> tuple[list[Finding], bool]:
    pool, _store = _pool(root, vault_key=vault_key)
    return await check_one_bucket(pool, BucketReaderCache.for_verify(), _KEY, level)


def _one_bucket(root: Path, **kwargs: Any) -> Path:
    path = root / "Pool" / "0" / "0.buk"
    write_bucket(path, _PLAINTEXTS, **kwargs)
    write_inf_and_fgp(root / "Pool", _PLAINTEXTS)
    return path


class TestAnIntactBucket:
    async def test_checks_clean_at_both_levels(self, tmp_path: Path) -> None:
        _one_bucket(tmp_path)

        assert await _check(tmp_path, VerifyLevel.FULL) == ([], False)
        assert await _check(tmp_path, VerifyLevel.QUICK) == ([], False)

    async def test_a_legacy_uncompressed_layout_has_nothing_to_check(self, tmp_path: Path) -> None:
        write_legacy_bucket(tmp_path / "Pool" / "0" / "0.buk", [bytes(4096)])

        assert await _check(tmp_path) == ([], False)


class TestLevels:
    async def test_quick_does_not_decode_chunks_so_a_wrong_chunk_crc_only_shows_at_full(self, tmp_path: Path) -> None:
        _one_bucket(tmp_path, corrupt_chunk_crc_idx=1)

        quick, _ = await _check(tmp_path, VerifyLevel.QUICK)
        full, _ = await _check(tmp_path, VerifyLevel.FULL)

        assert quick == []
        assert [(f.stage, f.symptom) for f in full] == [(Stage.BUCKET, Symptom.MISMATCH)]
        assert "chunk decode/fingerprint check failed" in full[0].detail

    async def test_full_reports_a_wrong_fingerprint_as_a_mismatch(self, tmp_path: Path) -> None:
        write_bucket(tmp_path / "Pool" / "0" / "0.buk", _PLAINTEXTS)
        write_inf_and_fgp(tmp_path / "Pool", _PLAINTEXTS, wrong_digest_idx=2)

        findings, key_missing = await _check(tmp_path)

        assert not key_missing
        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.MISMATCH)]


class TestFullChunkCheckPolicy:
    @pytest.mark.parametrize("verify", [NO_VERIFY, VerifyPolicy(fingerprint=True), VerifyPolicy(ciphertext_crc=True)])
    async def test_refuses_a_pool_not_built_with_full_verify(self, tmp_path: Path, verify: VerifyPolicy) -> None:
        _one_bucket(tmp_path)
        pool, _store = _pool(tmp_path, verify)
        reader = await pool.bucket(*_KEY)

        with pytest.raises(AssertionError, match="FULL_VERIFY"):
            await module._check_chunks_full(pool, _KEY, reader, [(0, 1)])


class TestOpenFailures:
    async def test_a_missing_bucket_is_data_missing(self, tmp_path: Path) -> None:
        (tmp_path / "Pool" / "0").mkdir(parents=True)

        findings, key_missing = await _check(tmp_path)

        assert not key_missing
        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.DATA_MISSING)]
        assert findings[0].path == "bucket 0/0"

    async def test_a_corrupt_header_is_corruption(self, tmp_path: Path) -> None:
        path = _one_bucket(tmp_path)
        path.write_bytes(b"\xff" * 64 + path.read_bytes()[64:])

        findings, _ = await _check(tmp_path)

        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.CORRUPTION)]
        assert "bad magic" in findings[0].detail

    async def test_an_open_error_neither_missing_nor_format_is_reported_as_unexpected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _one_bucket(tmp_path)

        async def deny(*args: object, **kwargs: object) -> bytes:
            raise PermissionDeniedError("no access")

        monkeypatch.setattr(LocalFsStore, "read", deny)

        findings, _ = await _check(tmp_path)

        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.CORRUPTION)]
        assert findings[0].detail == "unexpected error: no access"

    async def test_a_storage_backend_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _one_bucket(tmp_path)

        async def unreachable(*args: object, **kwargs: object) -> bytes:
            raise StorageBackendError("connection reset")

        monkeypatch.setattr(LocalFsStore, "read", unreachable)

        with pytest.raises(StorageBackendError, match="connection reset"):
            await _check(tmp_path)


class TestAnEncryptedBucketWithoutAKey:
    async def test_reports_the_missing_key_and_still_checks_the_ciphertext_crcs(self, tmp_path: Path) -> None:
        _one_bucket(tmp_path, corrupt_chunk_crc_idx=0, mode_extra=MODE_VAULT_ENCRYPT)

        findings, key_missing = await _check(tmp_path)

        assert key_missing is True
        # No decrypt without a key, but the stored ciphertext's own CRC is still compared.
        assert [(f.stage, f.symptom, f.detail) for f in findings[1:]] == [
            (Stage.BUCKET, Symptom.MISMATCH, "chunk 0 ciphertext CRC mismatch")
        ]
        assert (findings[0].stage, findings[0].symptom) == (Stage.ENCRYPT_KEY, Symptom.KEY_MISSING)

    async def test_an_intact_bucket_reports_only_the_missing_key(self, tmp_path: Path) -> None:
        _one_bucket(tmp_path, mode_extra=MODE_VAULT_ENCRYPT)

        findings, key_missing = await _check(tmp_path)

        assert key_missing is True
        assert [(f.stage, f.symptom) for f in findings] == [(Stage.ENCRYPT_KEY, Symptom.KEY_MISSING)]

    async def test_quick_still_reports_the_missing_key_without_reading_chunks(self, tmp_path: Path) -> None:
        _one_bucket(tmp_path, corrupt_chunk_crc_idx=0, mode_extra=MODE_VAULT_ENCRYPT)

        findings, key_missing = await _check(tmp_path, VerifyLevel.QUICK)

        assert key_missing is True
        assert [(f.stage, f.symptom) for f in findings] == [(Stage.ENCRYPT_KEY, Symptom.KEY_MISSING)]


class TestNothingEscapes:
    """A failure while checking one bucket must come back as a finding: an exception would cancel every
    sibling bucket in the same concurrent dispatch."""

    async def test_an_sdk_error_in_the_structure_check_is_corruption_of_that_bucket(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _one_bucket(tmp_path)

        async def broken(*args: object, **kwargs: object) -> list[Finding]:
            raise DataCorruptError("trailer unreadable")

        monkeypatch.setattr(module, "check_bucket_structure", broken)

        findings, key_missing = await _check(tmp_path)

        assert not key_missing
        assert [(f.stage, f.symptom, f.path) for f in findings] == [(Stage.BUCKET, Symptom.CORRUPTION, "Pool/0/0.buk")]
        assert "trailer unreadable" in findings[0].detail

    async def test_any_other_error_is_reported_as_unexpected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _one_bucket(tmp_path)

        async def broken(*args: object, **kwargs: object) -> list[Finding]:
            raise RuntimeError("bug")

        monkeypatch.setattr(module, "check_bucket_structure", broken)

        findings, _ = await _check(tmp_path)

        assert [(f.stage, f.symptom) for f in findings] == [(Stage.BUCKET, Symptom.CORRUPTION)]
        assert findings[0].detail == "unexpected error: bug"

    async def test_except_a_storage_backend_error_which_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _one_bucket(tmp_path)

        async def unreachable(*args: object, **kwargs: object) -> list[Finding]:
            raise StorageBackendError("connection reset")

        monkeypatch.setattr(module, "check_bucket_structure", unreachable)

        with pytest.raises(StorageBackendError, match="connection reset"):
            await _check(tmp_path)


class _SizingPool:
    def __init__(self, result: int | BaseException | None) -> None:
        self._result = result

    async def bucket_size(self, stream_id: StreamId, bucket_id: BucketId) -> int | None:
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


class TestListedSize:
    async def test_returns_the_listed_size(self) -> None:
        assert await _listed_size(_SizingPool(12345), _KEY) == 12345  # type: ignore[arg-type]

    async def test_a_listing_without_sizes_is_none(self) -> None:
        assert await _listed_size(_SizingPool(None), _KEY) is None  # type: ignore[arg-type]

    @pytest.mark.parametrize("error", [RuntimeError("boom"), PermissionDeniedError("no")])
    async def test_a_failed_lookup_is_none_so_the_structure_check_asks_the_store_itself(self, error: Exception) -> None:
        assert await _listed_size(_SizingPool(error), _KEY) is None  # type: ignore[arg-type]

    async def test_a_storage_backend_error_propagates(self) -> None:
        with pytest.raises(StorageBackendError, match="timeout"):
            await _listed_size(_SizingPool(StorageBackendError("timeout")), _KEY)  # type: ignore[arg-type]


async def test_a_verify_executor_accepts_only_the_repository_it_was_built_for(tmp_path: Path) -> None:
    store = LocalFsStore(tmp_path)
    descriptor = PoolDescriptor.from_pool(Pool(store, "Pool", DirCache(store)))
    other = PoolDescriptor.from_pool(Pool(store, "OtherPool", DirCache(store)))
    assert descriptor is not None and other is not None

    async with VerifyExecutor(descriptor) as executor:
        assert type(executor.process_pool).__name__ == "ProcessPoolExecutor"
        assert executor.accepts_pool(descriptor)
        assert not executor.accepts_pool(other)
        assert not executor.accepts_pool(None)
