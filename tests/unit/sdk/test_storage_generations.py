"""Unit tests for ``synology_apm_repo.sdk.storage.generations`` —
synthetic ``LocalFsStore`` fixtures (real files on a real ``tmp_path``,
since this module's whole job is directory-listing + generation-number
arithmetic, not byte decoding). See
``tests/integration/sdk/test_storage_generations.py`` for the
cross-check against real ``s3-sample-2-encrypted``/``sample-1`` data."""

from __future__ import annotations

import json
import zlib
from pathlib import Path

import pytest

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.format.repo_transaction import MAGIC as _TXN_MAGIC
from synology_apm_repo.sdk.storage.generations import latest_transaction_id, resolve_generation
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _repo_transaction_bytes(transaction_id: int) -> bytes:
    payload = json.dumps({"transaction_id": transaction_id}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = _TXN_MAGIC
    header[4:6] = (1).to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header) + payload


def _write_transactions(root: Path, filename_to_txn_id: dict[int, int]) -> None:
    d = root / "repo_transactions"
    d.mkdir(parents=True, exist_ok=True)
    for filename_n, txn_id in filename_to_txn_id.items():
        (d / f"repo_transaction.{filename_n}").write_bytes(_repo_transaction_bytes(txn_id))


class TestLatestTransactionId:
    async def test_reads_the_embedded_id_not_the_filename(self, tmp_path: Path) -> None:
        # Mirrors real data exactly: filename 98 embeds transaction_id 100 —
        # a naive "use the filename number" implementation would return 98.
        _write_transactions(tmp_path, {73: 75, 96: 98, 98: 100})
        store = LocalFsStore(tmp_path)
        assert await latest_transaction_id(store, "repo_transactions") == 100

    async def test_picks_the_largest_filename_not_the_largest_embedded_id(self, tmp_path: Path) -> None:
        # Deliberately non-monotonic embedded ids relative to filenames,
        # to prove selection keys off the *filename* max (the on-disk
        # convention: latest file = latest committed transaction) rather
        # than scanning every file for the largest transaction_id.
        _write_transactions(tmp_path, {5: 999, 10: 50})
        store = LocalFsStore(tmp_path)
        assert await latest_transaction_id(store, "repo_transactions") == 50

    async def test_no_transaction_files_raises_not_found(self, tmp_path: Path) -> None:
        (tmp_path / "repo_transactions").mkdir()
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError):
            await latest_transaction_id(store, "repo_transactions")

    async def test_missing_directory_raises_not_found(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError):
            await latest_transaction_id(store, "repo_transactions")


class TestResolveGenerationPrimaryTables:
    async def test_no_suffixed_variant_falls_back_to_bare_name(self, tmp_path: Path) -> None:
        (tmp_path / "db").mkdir()
        (tmp_path / "db" / "file_map").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        path = await resolve_generation(
            store, "db", "file_map", transactions_dir="repo_transactions", suppl_dir="suppl_transaction_ids"
        )
        assert path == "db/file_map"

    async def test_picks_the_largest_generation_strictly_less_than_latest_txn(self, tmp_path: Path) -> None:
        # A deliberately-planted file_map.100 (numerically the largest) sits
        # at/beyond latest_txn and must be excluded in favor of .98.
        _write_transactions(tmp_path, {96: 98, 98: 100})
        db = tmp_path / "db"
        db.mkdir()
        for n in (73, 75, 78, 81, 84, 86, 89, 91, 93, 96, 98):
            (db / f"file_map.{n}").write_bytes(b"")
        # A generation >= latest_txn (100) also present — must be excluded.
        (db / "file_map.100").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        path = await resolve_generation(
            store, "db", "file_map", transactions_dir="repo_transactions", suppl_dir="suppl_transaction_ids"
        )
        assert path == "db/file_map.98"

    async def test_no_generation_committed_yet_raises_not_found(self, tmp_path: Path) -> None:
        _write_transactions(tmp_path, {5: 10})
        db = tmp_path / "db"
        db.mkdir()
        (db / "file_map.10").write_bytes(b"")  # not strictly less than latest_txn=10
        (db / "file_map.15").write_bytes(b"")  # ahead of latest_txn entirely
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError):
            await resolve_generation(
                store, "db", "file_map", transactions_dir="repo_transactions", suppl_dir="suppl_transaction_ids"
            )


class TestResolveGenerationSupplementalTables:
    async def test_picks_the_largest_generation_with_a_suppl_marker(self, tmp_path: Path) -> None:
        db = tmp_path / "db"
        db.mkdir()
        for n in range(11):
            (db / f"connection_config.{n}").write_bytes(b"")
        suppl = tmp_path / "suppl_transaction_ids"
        suppl.mkdir()
        for n in range(11):
            (suppl / str(n)).write_bytes(b"")
        store = LocalFsStore(tmp_path)
        path = await resolve_generation(
            store,
            "db",
            "connection_config",
            transactions_dir="repo_transactions",
            suppl_dir="suppl_transaction_ids",
        )
        assert path == "db/connection_config.10"

    async def test_unrelated_numbering_from_repo_transactions_is_ignored(self, tmp_path: Path) -> None:
        # Real data: file_map's generation numbers (73..98) and a
        # supplemental table's (0..10) come from two completely
        # independent sequences — a supplemental lookup must never
        # consult repo_transactions/ at all.
        db = tmp_path / "db"
        db.mkdir()
        (db / "connection_config.0").write_bytes(b"")
        (db / "connection_config.3").write_bytes(b"")
        suppl = tmp_path / "suppl_transaction_ids"
        suppl.mkdir()
        (suppl / "0").write_bytes(b"")
        (suppl / "3").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        # No repo_transactions/ directory exists at all — must not raise.
        path = await resolve_generation(
            store,
            "db",
            "connection_config",
            transactions_dir="repo_transactions",
            suppl_dir="suppl_transaction_ids",
        )
        assert path == "db/connection_config.3"

    async def test_generation_with_no_matching_marker_raises_not_found(self, tmp_path: Path) -> None:
        db = tmp_path / "db"
        db.mkdir()
        (db / "connection_config.5").write_bytes(b"")
        suppl = tmp_path / "suppl_transaction_ids"
        suppl.mkdir()
        (suppl / "0").write_bytes(b"")  # 5 has no marker
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError):
            await resolve_generation(
                store,
                "db",
                "connection_config",
                transactions_dir="repo_transactions",
                suppl_dir="suppl_transaction_ids",
            )


class TestPathJoining:
    async def test_bare_repo_root_does_not_produce_a_leading_slash(self, tmp_path: Path) -> None:
        # Regression: layout.repo_root == "" (a bare OBJECT_STORE repoId
        # root) must join to "db/file_map", never "/db/file_map" — the
        # latter fails LocalFsStore's own escapes-store-root guard.
        (tmp_path / "db").mkdir()
        (tmp_path / "db" / "file_map").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        path = await resolve_generation(
            store, "db", "file_map", transactions_dir="repo_transactions", suppl_dir="suppl_transaction_ids"
        )
        assert not path.startswith("/")
        assert await store.exists(path)


__all__: list[str] = []
