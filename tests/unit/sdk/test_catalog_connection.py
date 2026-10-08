"""Unit tests for ``synology_apm_repo.sdk.catalog.connection`` —
synthetic repository roots written to real files
(``tests/integration/sdk/test_catalog_catalog.py`` is the real-data
counterpart)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.repo_builders import (
    write_connection_config,
    write_copy_target_version,
    write_repo_info,
    write_vault_link_key,
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import ConnectionConfigId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.catalog_fakes import write_catalog_repo


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    write_catalog_repo(tmp_path)
    return tmp_path


@pytest.fixture
async def repo(repo_root: Path) -> AsyncIterator[DedupRepo]:
    store = LocalFsStore(repo_root)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as opened:
        yield opened


class TestConnections:
    async def test_returns_both_connections_with_display_names(self, repo: DedupRepo) -> None:
        conns = await connections(repo)
        by_id = {c.connection_config_id: c for c in conns}
        assert by_id[ConnectionConfigId(1)].display_name == "Test-Workload-02"
        assert by_id[ConnectionConfigId(2)].display_name == "Test-Workload-01"

    async def test_dp_name_with_underscore_is_preserved(self, repo_root: Path) -> None:
        # Only the first two "_" separate the link key's fields; the name keeps the rest.
        (repo_root / "db" / "vault_link_key").unlink()
        write_vault_link_key(
            repo_root / "db" / "vault_link_key",
            ["conn-a_9053e422-uuid_Test-Workload-02", "conn-b_2d90eeaf-uuid_Test_Workload_01"],
        )
        store = LocalFsStore(repo_root)
        async with await DedupRepo.open(store, RepoLayout(kind=RepoKind.VAULT, repo_root="")) as repo:
            by_id = {c.connection_config_id: c for c in await connections(repo)}
        assert by_id[ConnectionConfigId(2)].display_name == "Test_Workload_01"

    async def test_workload_and_version_counts(self, repo: DedupRepo) -> None:
        conns = await connections(repo)
        by_id = {c.connection_config_id: c for c in conns}
        assert by_id[ConnectionConfigId(1)].workload_count == 2  # VM + FS
        assert by_id[ConnectionConfigId(1)].version_count == 2
        assert by_id[ConnectionConfigId(2)].workload_count == 3  # mail + site + group
        assert by_id[ConnectionConfigId(2)].version_count == 3  # deleted versions are counted, not filtered out

    async def test_sorted_by_display_name(self, repo: DedupRepo) -> None:
        conns = await connections(repo)
        # "Test-Workload-01" precedes "Test-Workload-02" alphabetically, opposite of
        # connection_config_id order (2 registered after 1).
        assert [c.display_name for c in conns] == ["Test-Workload-01", "Test-Workload-02"]

    async def test_namespaces_aggregated_and_deduplicated(self, repo: DedupRepo) -> None:
        conns = await connections(repo)
        by_id = {c.connection_config_id: c for c in conns}
        assert by_id[ConnectionConfigId(1)].namespaces == ("ns-a",)
        assert by_id[ConnectionConfigId(2)].namespaces == ("ns-b",)

    async def test_unlinked_connection_id_degrades_to_raw_value(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "mystery-conn", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])  # no matching link key at all
        write_workload_config(tmp_path / "db" / "workload_config", [])
        write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "mystery-conn"

    async def test_missing_vault_link_key_file_degrades_to_raw_value(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        # no db/vault_link_key file at all (not even an empty one)
        write_workload_config(tmp_path / "db" / "workload_config", [])
        write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "conn-a"

    async def test_object_store_layout_resolves_via_key_root_link_listing(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_workload_config(tmp_path / "db" / "workload_config", [])
        write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        link_dir = tmp_path / "@ActiveProtectKey" / "link"
        link_dir.mkdir(parents=True)
        (link_dir / "conn-a_9053e422-uuid_Test-Workload-02").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "Test-Workload-02"

    async def test_no_connection_configs_at_all_returns_empty_list(self, tmp_path: Path) -> None:
        """With no ``connection_config`` rows, ``copy_target_version`` is
        never queried — this fixture doesn't write one."""
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await connections(repo) == []

    @pytest.mark.parametrize(
        "key_root",
        [
            pytest.param("@ActiveProtectKey", id="missing_link_dir"),
            # A real bucket with no @ActiveProtectKey tree at all.
            pytest.param(None, id="without_a_key_root"),
        ],
    )
    async def test_object_store_layout_without_a_link_dir_degrades_to_raw_value(
        self, tmp_path: Path, key_root: str | None
    ) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_workload_config(tmp_path / "db" / "workload_config", [])
        write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root=key_root)
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "conn-a"
