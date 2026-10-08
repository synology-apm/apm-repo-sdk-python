"""Unit tests for ``synology_apm_repo.sdk.storage.generations`` over real
files under ``tmp_path`` (the module is directory listing plus
generation-number arithmetic). ``tests/integration/sdk/test_storage_generations.py``
is the real-data counterpart."""

from __future__ import annotations

from pathlib import Path

import pytest

from support.format_builders import repo_transaction_bytes
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.base import Entry, list_names
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.generations import latest_transaction_id, resolve_generation
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _write_transactions(root: Path, filename_to_txn_id: dict[int, int]) -> None:
    d = root / "repo_transactions"
    d.mkdir(parents=True, exist_ok=True)
    for filename_n, txn_id in filename_to_txn_id.items():
        (d / f"repo_transaction.{filename_n}").write_bytes(repo_transaction_bytes({"transaction_id": txn_id}))


class TestLatestTransactionId:
    async def test_reads_the_embedded_id_not_the_filename(self, tmp_path: Path) -> None:
        # A file's embedded transaction_id runs ahead of its filename's number.
        _write_transactions(tmp_path, {73: 75, 96: 98, 98: 100})
        store = LocalFsStore(tmp_path)
        assert await latest_transaction_id(store, "repo_transactions") == 100

    async def test_picks_the_largest_filename_not_the_largest_embedded_id(self, tmp_path: Path) -> None:
        # The latest file (by filename) holds the latest committed transaction,
        # whatever ids the other files embed.
        _write_transactions(tmp_path, {5: 999, 10: 50})
        store = LocalFsStore(tmp_path)
        assert await latest_transaction_id(store, "repo_transactions") == 50

    async def test_no_transaction_files_raises_not_found(self, tmp_path: Path) -> None:
        (tmp_path / "repo_transactions").mkdir()
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError, match=r"no repo_transaction\.<N> files found"):
            await latest_transaction_id(store, "repo_transactions")

    async def test_missing_directory_raises_not_found(self, tmp_path: Path) -> None:
        store = LocalFsStore(tmp_path)
        with pytest.raises(NotFoundError, match="no such directory"):
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
        with pytest.raises(NotFoundError, match="no 'file_map' generation is committed"):
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
        # A supplemental table's generations are a sequence independent of
        # repo_transactions/, which a supplemental lookup never consults.
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
        with pytest.raises(NotFoundError, match="no 'connection_config' generation has a matching supplemental marker"):
            await resolve_generation(
                store,
                "db",
                "connection_config",
                transactions_dir="repo_transactions",
                suppl_dir="suppl_transaction_ids",
            )


class TestPathJoining:
    async def test_bare_repo_root_does_not_produce_a_leading_slash(self, tmp_path: Path) -> None:
        # Under a bare repo root (repo_root == "", an OBJECT_STORE repoId root) the
        # directory is plain "db": the result must be "db/file_map", never
        # "/db/file_map", which LocalFsStore's escape guard rejects.
        (tmp_path / "db").mkdir()
        (tmp_path / "db" / "file_map").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        path = await resolve_generation(
            store, "db", "file_map", transactions_dir="repo_transactions", suppl_dir="suppl_transaction_ids"
        )
        assert not path.startswith("/")
        assert await store.exists(path)


class _ListingSpy:
    """A ``listdir`` stand-in recording every directory it is asked for."""

    def __init__(self, store: LocalFsStore) -> None:
        self._store = store
        self.calls: list[str] = []

    async def __call__(self, path: str) -> list[str]:
        self.calls.append(path)
        return await list_names(self._store, path)


class TestCustomListdir:
    async def test_the_given_listdir_serves_every_directory_the_resolution_lists(self, tmp_path: Path) -> None:
        _write_transactions(tmp_path, {1: 10})
        db = tmp_path / "db"
        db.mkdir()
        (db / "file_map.3").write_bytes(b"")
        (db / "workload_config.2").write_bytes(b"")
        (tmp_path / "suppl_transaction_ids").mkdir()
        (tmp_path / "suppl_transaction_ids" / "2").write_bytes(b"")
        spy = _ListingSpy(LocalFsStore(tmp_path))
        store = LocalFsStore(tmp_path)

        primary = await resolve_generation(
            store,
            "db",
            "file_map",
            transactions_dir="repo_transactions",
            suppl_dir="suppl_transaction_ids",
            listdir=spy,
        )
        supplemental = await resolve_generation(
            store,
            "db",
            "workload_config",
            transactions_dir="repo_transactions",
            suppl_dir="suppl_transaction_ids",
            listdir=spy,
        )

        assert (primary, supplemental) == ("db/file_map.3", "db/workload_config.2")
        assert spy.calls == ["db", "repo_transactions", "db", "suppl_transaction_ids"]

    async def test_latest_transaction_id_lists_through_the_given_listdir(self, tmp_path: Path) -> None:
        _write_transactions(tmp_path, {4: 7})
        spy = _ListingSpy(LocalFsStore(tmp_path))

        assert await latest_transaction_id(LocalFsStore(tmp_path), "repo_transactions", listdir=spy) == 7
        assert spy.calls == ["repo_transactions"]

    async def test_a_dircache_listdir_makes_repeated_resolutions_share_one_listing_each(self, tmp_path: Path) -> None:
        _write_transactions(tmp_path, {1: 10})
        db = tmp_path / "db"
        db.mkdir()
        for name in ("file_map.3", "copy_target_version.2", "workload_config.2"):
            (db / name).write_bytes(b"")
        (tmp_path / "suppl_transaction_ids").mkdir()
        (tmp_path / "suppl_transaction_ids" / "2").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        listed: list[str] = []
        real_listdir = LocalFsStore.listdir

        async def spying(self: LocalFsStore, path: str) -> list[Entry]:
            listed.append(path)
            return await real_listdir(self, path)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(LocalFsStore, "listdir", spying)
            cache = DirCache(store)
            for name in ("file_map", "copy_target_version", "workload_config"):
                await resolve_generation(
                    store,
                    "db",
                    name,
                    transactions_dir="repo_transactions",
                    suppl_dir="suppl_transaction_ids",
                    listdir=cache.listdir,
                )

        assert sorted(listed) == ["db", "repo_transactions", "suppl_transaction_ids"]
