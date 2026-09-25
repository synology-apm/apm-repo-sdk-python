"""Unit tests for ``synology_apm_repo.sdk.catalog.connection`` —
synthetic repository roots written to real files, no sample repositories
required (see ``tests/integration/sdk/test_catalog_catalog.py`` for the
byte-for-byte cross-check against apv-sample-1's exact display names and
counts)."""

from __future__ import annotations

import json
import sqlite3
import zlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import ConnectionConfigId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = b"RpiF"
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"a" * 16
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _write_connection_config(path: Path, rows: list[tuple[int, str, int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, "
        "connection_id TEXT, version_type INTEGER)"
    )
    conn.executemany("INSERT INTO connection_config VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_vault_link_key(path: Path, keys: list[str]) -> None:
    conn = _db(path)
    conn.execute("CREATE TABLE vault_link_key(key TEXT)")
    conn.executemany("INSERT INTO vault_link_key VALUES (?)", [(k,) for k in keys])
    conn.commit()
    conn.close()


def _write_workload_config(path: Path, rows: list[tuple[int, str, str, dict[str, object]]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY, workload_uid TEXT, "
        "workload_type TEXT, workload_spec TEXT)"
    )
    conn.executemany(
        "INSERT INTO workload_config VALUES (?, ?, ?, ?)",
        [(wid, uid, wtype, json.dumps(spec)) for wid, uid, wtype, spec in rows],
    )
    conn.commit()
    conn.close()


def _write_copy_target_version(path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE copy_target_version(version_id INTEGER PRIMARY KEY, workload_id INTEGER, "
        "connection_config_id INTEGER, version_uid TEXT, target_type TEXT, target_id TEXT, "
        "saas_stream_uuid TEXT, saas_snapshot_uuid TEXT, saas_version_id INTEGER, deleted INTEGER, "
        "version_spec TEXT)"
    )
    conn.executemany("INSERT INTO copy_target_version VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _version_spec_json(start_time: object = None, end_time: object = None, status: object = None) -> str:
    """A minimal real-shaped ``version_spec`` blob — just the
    ``status.start_time``/``status.end_time``/``status.status`` fields
    this module actually reads (real production values are
    protobuf-JSON int64-as-string, e.g. ``"1786024626"``); omitted keys
    model a field genuinely absent from a real row, not just
    zero/empty."""
    status_obj: dict[str, object] = {}
    if start_time is not None:
        status_obj["start_time"] = str(start_time)
    if end_time is not None:
        status_obj["end_time"] = str(end_time)
    if status is not None:
        status_obj["status"] = status
    return json.dumps({"status": status_obj})


def _write_copy_target_version_meta(path: Path, rows: list[tuple[str, str, list[str], int]]) -> None:
    conn = _db(path)
    conn.execute(
        "CREATE TABLE copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
        "meta_filenames TEXT, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)",
        [(uid, path_, json.dumps(names), status) for uid, path_, names, status in rows],
    )
    conn.commit()
    conn.close()


_VM_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {
        "workload_type": "VM",
        "workload_name": "my-vm",
        "config_vm": {"os_name": "Windows 10", "hypervisor_name": "Cluster-02"},
    },
}
_FS_SPEC: dict[str, Any] = {
    "namespace": "ns-a",
    "spec": {"workload_type": "FS", "workload_name": "10.0.0.1", "config_fs": {"os_name": "smb"}},
}
_MAIL_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "MAIL"},
    "status": {"entity_meta": {"spec": {"user_info": {"name": "Alice", "email": "alice@x.com"}}}},
}
_SITE_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "SITE"},
    "status": {"entity_meta": {"spec": {"site_info": {"site_name": "My Site"}}}},
}
_GROUP_SPEC: dict[str, Any] = {
    "namespace": "ns-b",
    "spec": {"workload_type": "TEAM_DRIVE"},
    "status": {"entity_meta": {"spec": {"group_info": {"display_name": "My Group"}}}},
}


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    _write_repo_info(tmp_path / "repo_info")
    _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1), (2, "conn-b", 1)])
    _write_vault_link_key(
        tmp_path / "db" / "vault_link_key",
        ["conn-a_9053e422-uuid_Test-Workload-02", "conn-b_2d90eeaf-uuid_Test-Workload-01"],
    )
    _write_workload_config(
        tmp_path / "db" / "workload_config",
        [
            (10, "vm-uid", "VM", _VM_SPEC),
            (11, "fs-uid", "FS", _FS_SPEC),
            (12, "mail-uid", "GW", _MAIL_SPEC),
            (13, "site-uid", "M365", _SITE_SPEC),
            (14, "group-uid", "GW", _GROUP_SPEC),
            (15, "unknown-uid", "WEIRD", {"namespace": "ns-c", "spec": {}}),
        ],
    )
    _write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                100,
                10,
                1,
                "vuid-100",
                "VM",
                "target-1",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786024626, status="COMPLETED"),
            ),
            (
                101,
                11,
                1,
                "vuid-101",
                "FS",
                "target-2",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786024439, status="COMPLETED"),
            ),
            (
                102,
                12,
                2,
                "vuid-102",
                "GW",
                "target-3",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1785999588, status="COMPLETED"),
            ),
            (
                103,
                13,
                2,
                "vuid-103",
                "M365",
                "target-4",
                "",
                "",
                0,
                1,  # deleted
                _version_spec_json(start_time=1786026760, status="COMPLETED"),
            ),
            (
                104,
                14,
                2,
                "vuid-104",
                "GW",
                "target-5",
                "",
                "",
                0,
                0,
                _version_spec_json(start_time=1786027665, status="COMPLETED"),
            ),
        ],
    )
    _write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [("vuid-100", "/pv/copy_meta_file/VM_vuid-100", ["target.db"], 1)],
    )
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

    async def test_dp_name_with_underscore_is_preserved(self, repo: DedupRepo) -> None:
        conns = await connections(repo)
        by_id = {c.connection_config_id: c for c in conns}
        assert by_id[ConnectionConfigId(2)].display_name == "Test-Workload-01"

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
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "mystery-conn", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])  # no matching link key at all
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "mystery-conn"

    async def test_missing_vault_link_key_file_degrades_to_raw_value(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        # no db/vault_link_key file at all (not even an empty one)
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "conn-a"

    async def test_object_store_layout_resolves_via_key_root_link_listing(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        link_dir = tmp_path / "@ActiveProtectKey" / "link"
        link_dir.mkdir(parents=True)
        (link_dir / "conn-a_9053e422-uuid_Test-Workload-02").write_bytes(b"")
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "Test-Workload-02"

    async def test_no_connection_configs_at_all_returns_empty_list(self, tmp_path: Path) -> None:
        """The zero-connections case: ``connection_config`` itself has no
        rows, so ``_workload_ids_by_connection``/``_version_counts_by_connection``
        never even query ``copy_target_version`` — their own early
        ``return {}`` short-circuits before that."""
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            assert await connections(repo) == []

    async def test_object_store_layout_missing_link_dir_degrades_to_raw_value(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root="@ActiveProtectKey")
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "conn-a"

    async def test_object_store_layout_without_a_key_root_degrades_to_raw_value(self, tmp_path: Path) -> None:
        """Same degraded outcome as the missing-link-dir case above, but
        for an ``OBJECT_STORE`` layout with no ``key_root`` at all (a real
        bucket with no ``@ActiveProtectKey`` tree whatsoever) rather than
        one whose ``key_root`` just has no ``link`` subdirectory yet."""
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_workload_config(tmp_path / "db" / "workload_config", [])
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", [])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root="", key_root=None)
        async with await DedupRepo.open(store, layout) as repo:
            conns = await connections(repo)
            assert conns[0].display_name == "conn-a"
