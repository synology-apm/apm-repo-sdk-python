"""Unit tests for ``synology_apm_repo.sdk.catalog.version`` —
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
from synology_apm_repo.sdk.catalog.version import (
    ParsedVersionStatus,
    _version_display_name,
    versions,
)
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    VersionUid,
)
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


async def _open_repo(tmp_path: Path) -> DedupRepo:
    _write_repo_info(tmp_path / "repo_info")
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


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


def test_version_display_name_degrades_to_version_uid_for_an_out_of_range_epoch() -> None:
    """``_as_epoch_seconds`` narrows ``start_time``'s own protobuf-JSON
    string to ``int`` with no range check of its own -- an out-of-
    ``datetime``-range value (corrupt ``version_spec`` data) must degrade
    to the raw ``version_uid``, the same "degrade on any failure"
    contract ``_version_display_name`` uses for undecryptable/
    unparseable/absent timestamps too."""
    status = ParsedVersionStatus(start_time="99999999999999")
    assert _version_display_name(status, "vuid-100") == "vuid-100"


class TestVersions:
    async def test_returns_display_name_from_local_time(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(1)]) if w.workload_id == 10)
        vs = await versions(repo, wl)
        assert len(vs) == 1
        # exact local-time value depends on the test machine's timezone;
        # what matters is that it's derived (not the raw version_uid) and
        # keeps the same date+hour precision format.
        assert vs[0].display_name != vs[0].version_uid
        assert len(vs[0].display_name) == len("2026-08-06 21:59:59")

    async def test_deleted_versions_excluded_by_default(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(2)]) if w.workload_id == 13)
        assert await versions(repo, wl) == []

    async def test_deleted_versions_included_on_request(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(2)]) if w.workload_id == 13)
        vs = await versions(repo, wl, include_deleted=True)
        assert len(vs) == 1
        assert vs[0].deleted is True

    async def test_version_meta_attached_when_present(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(1)]) if w.workload_id == 10)
        vs = await versions(repo, wl)
        assert vs[0].meta is not None
        assert vs[0].meta.meta_filenames == ("target.db",)

    async def test_version_meta_none_when_absent(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(1)]) if w.workload_id == 11)
        vs = await versions(repo, wl)
        assert vs[0].meta is None

    async def test_null_meta_filenames_degrades_instead_of_crashing_the_whole_batch(self, tmp_path: Path) -> None:
        """A ``copy_target_version_meta`` row can have ``meta_filenames
        IS NULL`` for a still-mid-upload version (``status == 0``,
        "Writing") — ``_version_metas_for``'s batched query must not let
        that one row's ``json.loads(None)`` crash every *other* version
        sharing the same batch."""
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", _VM_SPEC)])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (
                    100,
                    10,
                    1,
                    "vuid-writing",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786000000, status="COMPLETED"),
                ),
                (
                    101,
                    10,
                    1,
                    "vuid-complete",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786099999, status="COMPLETED"),
                ),
            ],
        )
        conn = _db(tmp_path / "db" / "copy_target_version_meta")
        conn.execute(
            "CREATE TABLE copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
            "meta_filenames TEXT, status INTEGER)"
        )
        conn.execute(
            "INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)",
            ("vuid-writing", "/pv/copy_meta_file/VM_vuid-writing", None, 0),  # status 0 == "Writing", meta not landed
        )
        conn.execute(
            "INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)",
            ("vuid-complete", "/pv/copy_meta_file/VM_vuid-complete", json.dumps(["target.db"]), 1),
        )
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = {v.version_uid: v for v in await versions(repo, wl)}
        writing_meta = vs[VersionUid("vuid-writing")].meta
        complete_meta = vs[VersionUid("vuid-complete")].meta
        assert writing_meta is not None
        assert writing_meta.meta_filenames == ()
        assert complete_meta is not None
        assert complete_meta.meta_filenames == ("target.db",)

    async def test_versions_are_sorted_newest_first_by_real_backup_time(self, tmp_path: Path) -> None:
        """``copy_target_version`` rows come back from SQLite in whatever
        order they happen to be stored in (insertion/rowid order here,
        deliberately *not* chronological) — ``versions()`` must still
        return them newest-first by ``start_time``, not in that
        incidental row order."""
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", _VM_SPEC)])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (
                    100,
                    10,
                    1,
                    "vuid-middle",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786024626, status="COMPLETED"),
                ),
                (
                    101,
                    10,
                    1,
                    "vuid-oldest",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786000000, status="COMPLETED"),
                ),
                (
                    102,
                    10,
                    1,
                    "vuid-newest",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786099999, status="COMPLETED"),
                ),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            assert [v.version_uid for v in vs] == ["vuid-newest", "vuid-middle", "vuid-oldest"]

    async def test_versions_with_no_resolvable_timestamp_sort_last_by_version_id(self, tmp_path: Path) -> None:
        """A version whose ``version_spec`` has no usable ``start_time``/
        ``end_time`` (the same rare "corrupt data" case
        ``_version_display_name``
        degrades to showing the raw ``version_uid`` for) can't be placed
        chronologically at all — it sorts after every version with a
        real timestamp, never scattered in among them, with
        ``version_id`` (insertion order) as the tiebreak among any such
        rows."""
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", _VM_SPEC)])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (
                    100,
                    10,
                    1,
                    "vuid-has-time",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(1786024626, status="COMPLETED"),
                ),
                (101, 10, 1, "vuid-no-time-a", "VM", "t", "", "", 0, 0, _version_spec_json(status="COMPLETED")),
                (102, 10, 1, "vuid-no-time-b", "VM", "t", "", "", 0, 0, _version_spec_json(status="COMPLETED")),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            assert [v.version_uid for v in vs] == ["vuid-has-time", "vuid-no-time-b", "vuid-no-time-a"]

    async def test_version_meta_none_when_meta_db_file_missing_entirely(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", _VM_SPEC)])
        _write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (
                    100,
                    10,
                    1,
                    "vuid-100",
                    "VM",
                    "t",
                    "",
                    "",
                    0,
                    0,
                    _version_spec_json(start_time=1786024626, status="COMPLETED"),
                )
            ],
        )
        # no db/copy_target_version_meta file at all
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            assert vs[0].meta is None


class TestBrowsableVersionStatusFilter:
    """``versions()`` only returns rows whose ``version_spec.status.status``
    is ``COMPLETED``/``PARTIAL``/``CANCELED`` — everything else (nothing
    ever landed for the version) is filtered out at load time rather than
    exposed as another ``Version`` a caller has to know to skip."""

    async def _versions_for_statuses(self, tmp_path: Path, statuses: list[str | None]) -> list[str]:
        _write_repo_info(tmp_path / "repo_info")
        _write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        _write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        _write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", _VM_SPEC)])
        rows: list[tuple[object, ...]] = []
        for i, status in enumerate(statuses):
            spec = _version_spec_json(start_time=1786024626 + i, status=status) if status is not None else "{}"
            rows.append((100 + i, 10, 1, f"vuid-{100 + i}", "VM", "t", "", "", 0, 0, spec))
        _write_copy_target_version(tmp_path / "db" / "copy_target_version", rows)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            return [str(v.version_uid) for v in vs]

    async def test_completed_partial_and_canceled_are_kept(self, tmp_path: Path) -> None:
        # Newest first (versions() sorts by real backup time) — this
        # fixture's own start_time increases with i, so vuid-102 (i=2) is
        # newer than vuid-101 (i=1), newer than vuid-100 (i=0).
        kept = await self._versions_for_statuses(tmp_path, ["COMPLETED", "PARTIAL", "CANCELED"])
        assert kept == ["vuid-102", "vuid-101", "vuid-100"]

    @pytest.mark.parametrize(
        "status", ["BACKING_UP", "FAILED", "PAUSED", "DELETING", "DELETE_FAILED", "CLONING", "NONE"]
    )
    async def test_non_terminal_or_failed_statuses_are_excluded(self, tmp_path: Path, status: str) -> None:
        assert await self._versions_for_statuses(tmp_path, [status]) == []

    async def test_missing_status_field_is_excluded_not_kept(self, tmp_path: Path) -> None:
        # A version whose status can't be determined at all (no
        # status.status key present) is excluded right alongside a
        # known-bad status string, not kept.
        assert await self._versions_for_statuses(tmp_path, [None]) == []

    async def test_mixed_statuses_only_the_browsable_ones_survive(self, tmp_path: Path) -> None:
        # Newest first — vuid-103 (i=3) is newer than vuid-102 (i=2),
        # newer than vuid-101 (i=1); vuid-100 (i=0, FAILED) is excluded.
        kept = await self._versions_for_statuses(tmp_path, ["FAILED", "COMPLETED", "CANCELED", "PARTIAL"])
        assert kept == ["vuid-103", "vuid-102", "vuid-101"]


class TestParseVersionSpec:
    """Direct unit tests for ``parse_version_spec`` — the shared
    decrypt-then-parse step for one ``version_spec`` column value, used by
    both ``_parse_version_status`` (below) and
    ``units.saas.object_name_index``. Decrypts unconditionally whenever
    ``vault_key`` is given; never probes the raw bytes first."""

    def test_plaintext_no_key_parses_as_is(self) -> None:
        from synology_apm_repo.sdk.catalog.version import parse_version_spec

        spec = _version_spec_json(start_time=1786024626, status="COMPLETED")
        parsed = parse_version_spec(spec, "vuid-100", None)
        assert isinstance(parsed, dict)
        assert parsed["status"]["status"] == "COMPLETED"

    def test_real_ciphertext_with_key_decrypts_and_parses(self) -> None:
        import base64

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        from synology_apm_repo.sdk.catalog.version import parse_version_spec
        from synology_apm_repo.sdk.format.crypto import version_spec_iv

        vault_key = b"\x42" * 32
        version_uid = "vuid-100"
        plaintext = _version_spec_json(start_time=1786024626, status="COMPLETED")
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()).decode(
            "ascii"
        )

        parsed = parse_version_spec(ciphertext_b64, version_uid, vault_key)
        assert isinstance(parsed, dict)
        assert parsed["status"]["status"] == "COMPLETED"

    def test_undecryptable_ciphertext_with_key_returns_none(self) -> None:
        import base64

        from synology_apm_repo.sdk.catalog.version import parse_version_spec

        # A plain (unencrypted) version_spec base64'd and run through the
        # decrypt path anyway is not valid ciphertext for that key/IV —
        # must degrade to None, never raise or return mojibake.
        spec = _version_spec_json(start_time=1786024626, status="COMPLETED")
        not_really_ciphertext_b64 = base64.b64encode(spec.encode("utf-8")).decode("ascii")
        assert parse_version_spec(not_really_ciphertext_b64, "vuid-100", b"\x99" * 32) is None

    def test_malformed_json_returns_none(self) -> None:
        from synology_apm_repo.sdk.catalog.version import parse_version_spec

        assert parse_version_spec("not-json-at-all", "vuid-100", None) is None


class TestParseVersionStatus:
    """Direct unit tests for ``_parse_version_status``
    — the shared decrypt+parse step both ``_version_display_name``
    and the version-list status filter build on. No DB plumbing needed,
    just a raw ``version_spec`` string."""

    def test_parses_the_status_object(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _parse_version_status

        spec = _version_spec_json(start_time=1786024626, status="COMPLETED")
        status = _parse_version_status(spec, "vuid-100", None)
        assert status == ParsedVersionStatus(status="COMPLETED", start_time="1786024626")

    def test_missing_status_key_entirely_returns_none(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        assert _parse_version_status(json.dumps({"spec": {}}), "vuid-100", None) is None

    def test_malformed_json_returns_none(self) -> None:
        # Deliberately *not* a crtime fallback — version_spec is expected
        # to always be present and parseable, so this path is a rare
        # degrade for corrupt data, not a normal branch to design a
        # second timestamp source around.
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        assert _parse_version_status("not-json-at-all", "vuid-100", None) is None

    async def test_encrypted_version_spec_is_decrypted_before_parsing(self) -> None:
        # Real mechanism: AES-256-CTR, same vault_key as chunk-pool/aHlT,
        # IV derived from version_uid (version_spec_iv) — CTR encrypt/decrypt is the same
        # operation, so encrypting this fixture's plaintext with the
        # module's own real IV derivation is a faithful stand-in for a
        # real encrypted sample, not a shortcut around the real scheme.
        import base64

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _parse_version_status
        from synology_apm_repo.sdk.format.crypto import version_spec_iv

        vault_key = b"\x42" * 32
        version_uid = "vuid-100"
        plaintext = _version_spec_json(start_time=1786024626)
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()).decode(
            "ascii"
        )

        status = _parse_version_status(ciphertext_b64, version_uid, vault_key)
        assert status == ParsedVersionStatus(start_time="1786024626")

    def test_decrypting_data_that_is_not_real_ciphertext_degrades_safely(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        # A plain (unencrypted) version_spec base64'd and run through the
        # decrypt path anyway (e.g. caller mis-detected "this connection
        # has a vault key") is not valid ciphertext for that key/IV — the
        # AES-CTR XOR against real JSON text almost certainly yields
        # invalid UTF-8. Must degrade, never raise or show mojibake.
        any_key = b"\x99" * 32
        spec = _version_spec_json(start_time=1786024626)
        import base64

        not_really_ciphertext_b64 = base64.b64encode(spec.encode("utf-8")).decode("ascii")
        assert _parse_version_status(not_really_ciphertext_b64, "vuid-100", any_key) is None


class TestVersionDisplayName:
    """Direct unit tests for ``_version_display_name``
    — pure formatting from an already-parsed ``ParsedVersionStatus`` (see
    ``TestParseVersionStatus`` for the decrypt/parse half)."""

    def test_start_time_wins_over_end_time_when_both_present(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="1786024626", end_time="9999999999")
        name = _version_display_name(status, "vuid-100")
        assert len(name) == len("2026-08-06 21:59:59")
        assert name != "vuid-100"

    def test_falls_back_to_end_time_when_start_time_is_zero(self) -> None:
        # Real proto convention: "0" is the field's own not-set sentinel
        # for start_time/end_time alike, not a real 1970 epoch value to
        # format literally.
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="0", end_time="1786024626")
        name = _version_display_name(status, "vuid-100")
        assert len(name) == len("2026-08-06 21:59:59")
        assert name != "vuid-100"

    def test_both_zero_degrades_to_raw_version_uid(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="0", end_time="0")
        assert _version_display_name(status, "vuid-100") == "vuid-100"

    def test_none_status_degrades_to_raw_version_uid(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _version_display_name

        assert _version_display_name(None, "vuid-100") == "vuid-100"


class TestAsEpochSeconds:
    """Direct unit tests for ``_as_epoch_seconds`` — real
    ``version_spec`` data always has ``start_time``/``end_time`` as
    **strings** (protobuf-JSON's own int64-as-string convention), so
    ``TestVersionDisplayName`` above only ever exercises the
    ``str`` branch with well-formed digits; these pin down the
    ``bool``/``int``/malformed-``str`` branches its own defensive
    ``isinstance`` chain also handles."""

    def test_bool_is_never_treated_as_an_epoch(self) -> None:
        # bool is a subclass of int - the isinstance(value, bool) check
        # must be reached (and return None) before isinstance(value, int)
        # would otherwise accept it as 0 or 1.
        from synology_apm_repo.sdk.catalog.version import _as_epoch_seconds

        assert _as_epoch_seconds(True) is None
        assert _as_epoch_seconds(False) is None

    def test_zero_int_is_the_not_set_sentinel(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _as_epoch_seconds

        assert _as_epoch_seconds(0) is None

    def test_nonzero_int_passes_through(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _as_epoch_seconds

        assert _as_epoch_seconds(1786024626) == 1786024626

    def test_malformed_string_is_not_an_epoch(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _as_epoch_seconds

        assert _as_epoch_seconds("not-a-number") is None
