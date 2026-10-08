"""Unit tests for ``synology_apm_repo.sdk.catalog.version`` —
synthetic repository roots written to real files
(``tests/integration/sdk/test_catalog_catalog.py`` is the real-data
counterpart)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.model_factories import make_version
from support.repo_builders import (
    open_db,
    version_spec_json,
    write_connection_config,
    write_copy_target_version,
    write_repo_info,
    write_vault_link_key,
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import (
    ParsedVersionStatus,
    Version,
    VersionMeta,
    _version_display_name,
    resolve_copy_meta_dir,
    version_additional_meta,
    version_by_uid,
    versions,
)
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    VersionUid,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from unit.sdk.catalog_fakes import VM_SPEC, write_catalog_repo


async def _open_repo(tmp_path: Path) -> DedupRepo:
    write_repo_info(tmp_path / "repo_info")
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


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


def test_version_display_name_degrades_to_version_uid_for_an_out_of_range_epoch() -> None:
    """An epoch outside ``datetime``'s range (corrupt ``version_spec`` data)
    degrades to the raw ``version_uid``."""
    status = ParsedVersionStatus(start_time="99999999999999")
    assert _version_display_name(status, "vuid-100") == "vuid-100"


def _version_with_meta(meta: VersionMeta | None) -> Version:
    """A minimal ``Version``; ``resolve_copy_meta_dir`` reads only
    ``meta`` (and ``version_uid`` for its error message)."""
    return make_version(version_uid="uid-1", target_id="target-1", display_name="2026-01-01 00:00:00", meta=meta)


async def _metas_for_rows(
    tmp_path: Path, meta_rows: list[tuple[object, object, object, object]]
) -> dict[str, VersionMeta | None]:
    """Each version's ``meta`` when ``copy_target_version_meta`` holds
    ``meta_rows`` raw, for two VM versions ``vuid-writing``/``vuid-complete``."""
    write_repo_info(tmp_path / "repo_info")
    write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
    write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
    write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (100, 10, 1, "vuid-writing", "VM", "t", "", "", 0, 0, version_spec_json(1786000000, status="COMPLETED")),
            (101, 10, 1, "vuid-complete", "VM", "t", "", "", 0, 0, version_spec_json(1786099999, status="COMPLETED")),
        ],
    )
    conn = open_db(tmp_path / "db" / "copy_target_version_meta")
    conn.execute(
        "CREATE TABLE copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
        "meta_filenames TEXT, status INTEGER)"
    )
    conn.executemany("INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)", meta_rows)
    conn.commit()
    conn.close()
    store = LocalFsStore(tmp_path)
    async with await DedupRepo.open(store, RepoLayout(kind=RepoKind.VAULT, repo_root="")) as repo:
        wl = (await workloads(repo, (await connections(repo))[0]))[0]
        return {str(v.version_uid): v.meta for v in await versions(repo, wl)}


class TestResolveCopyMetaDir:
    def test_resolves_the_last_path_segment_onto_repo_root(self) -> None:
        version = _version_with_meta(VersionMeta(target_meta_path="2026-01-01/abc123", meta_filenames=(), status=1))
        assert resolve_copy_meta_dir(version, "repo") == "repo/copy_meta_file/abc123"

    def test_no_meta_row_raises_not_found(self) -> None:
        version = _version_with_meta(None)
        with pytest.raises(NotFoundError, match="no copy_target_version_meta row"):
            resolve_copy_meta_dir(version, "repo")

    @pytest.mark.parametrize(
        ("target_meta_path", "match"),
        [
            pytest.param("", "no copy_target_version_meta row", id="an_empty_target_meta_path_raises_not_found"),
            # Non-empty, but its last "/"-delimited segment is empty --
            # not caught by the "no meta row" check.
            pytest.param("/", "malformed", id="a_dirname_that_resolves_to_empty_is_rejected"),
            pytest.param("..", "malformed", id="a_dotdot_dirname_is_rejected"),
            pytest.param("2026-01-01/..", "malformed", id="a_dotdot_dirname_embedded_after_a_slash_is_rejected"),
            # No forward slash, so the whole string becomes dirname,
            # backslash intact.
            pytest.param("..\\..\\secret", "malformed", id="a_backslash_in_dirname_is_rejected"),
        ],
    )
    def test_an_unusable_target_meta_path_raises_not_found(self, target_meta_path: str, match: str) -> None:
        version = _version_with_meta(VersionMeta(target_meta_path=target_meta_path, meta_filenames=(), status=1))
        with pytest.raises(NotFoundError, match=match):
            resolve_copy_meta_dir(version, "repo")


class TestVersions:
    async def test_returns_display_name_from_local_time(self, repo: DedupRepo) -> None:
        conns = {c.connection_config_id: c for c in await connections(repo)}
        wl = next(w for w in await workloads(repo, conns[ConnectionConfigId(1)]) if w.workload_id == 10)
        vs = await versions(repo, wl)
        assert len(vs) == 1
        # start_time 1786024626 in tests/conftest.py's pinned Asia/Taipei (UTC+8).
        assert vs[0].display_name == "2026-08-06 21:57:06"

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
        """A ``copy_target_version_meta`` row has ``meta_filenames IS NULL``
        for a still-mid-upload version (``status == 0``, "Writing");
        ``_version_metas_for``'s batched query must not let that one row
        fail every other version in the batch."""
        metas = await _metas_for_rows(
            tmp_path,
            [
                ("vuid-writing", "/pv/copy_meta_file/VM_vuid-writing", None, 0),
                ("vuid-complete", "/pv/copy_meta_file/VM_vuid-complete", json.dumps(["target.db"]), 1),
            ],
        )
        writing_meta, complete_meta = metas["vuid-writing"], metas["vuid-complete"]
        assert writing_meta is not None
        assert writing_meta.meta_filenames == ()
        assert complete_meta is not None
        assert complete_meta.meta_filenames == ("target.db",)

    @pytest.mark.parametrize(
        ("target_meta_path", "status"),
        [
            pytest.param(b"/pv/copy_meta_file/VM_x", 1, id="blob_target_meta_path"),
            pytest.param("/pv/copy_meta_file/VM_x", "writing", id="text_status"),
        ],
    )
    async def test_a_meta_row_with_a_wrong_typed_column_counts_as_absent(
        self, tmp_path: Path, target_meta_path: object, status: object
    ) -> None:
        metas = await _metas_for_rows(
            tmp_path,
            [
                ("vuid-writing", target_meta_path, json.dumps(["target.db"]), status),
                ("vuid-complete", "/pv/copy_meta_file/VM_vuid-complete", json.dumps(["target.db", 7]), 1),
            ],
        )
        assert metas["vuid-writing"] is None
        complete_meta = metas["vuid-complete"]
        assert complete_meta is not None
        assert complete_meta.meta_filenames == ("target.db",)  # a non-string filename is dropped

    @pytest.mark.parametrize("raw_names", ['"target.db"', "{not json"])
    async def test_a_corrupt_meta_filenames_raises_data_corrupt_error(self, tmp_path: Path, raw_names: str) -> None:
        """A JSON string must not be read as a sequence of characters:
        anything but a JSON array is corrupt."""
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(100, 10, 1, "vuid-a", "VM", "t", "", "", 0, 0, version_spec_json(1786000000, status="COMPLETED"))],
        )
        conn = open_db(tmp_path / "db" / "copy_target_version_meta")
        conn.execute(
            "CREATE TABLE copy_target_version_meta(version_uid TEXT PRIMARY KEY, target_meta_path TEXT, "
            "meta_filenames TEXT, status INTEGER)"
        )
        conn.execute("INSERT INTO copy_target_version_meta VALUES (?, ?, ?, ?)", ("vuid-a", "/pv/m", raw_names, 1))
        conn.commit()
        conn.close()
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            with pytest.raises(DataCorruptError, match="meta_filenames"):
                await versions(repo, wl)

    async def test_version_by_uid_finds_one_version_with_the_same_filter_as_versions(self, tmp_path: Path) -> None:
        """A deleted version is still found (canonical refs name deleted
        versions too); a non-browsable status is not; an unknown uid is
        ``None``."""
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [
                (100, 10, 1, "vuid-ok", "VM", "t", "", "", 0, 0, version_spec_json(1786000000, status="COMPLETED")),
                (101, 10, 1, "vuid-gone", "VM", "t", "", "", 0, 1, version_spec_json(1786000001, status="COMPLETED")),
                (102, 10, 1, "vuid-failed", "VM", "t", "", "", 0, 0, version_spec_json(1786000002, status="FAILED")),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            found = await version_by_uid(repo, VersionUid("vuid-ok"))
            deleted = await version_by_uid(repo, VersionUid("vuid-gone"))
            assert found is not None and found.version_id == 100
            assert deleted is not None and deleted.deleted
            assert await version_by_uid(repo, VersionUid("vuid-failed")) is None
            assert await version_by_uid(repo, VersionUid("no-such-uid")) is None
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            assert found == next(v for v in await versions(repo, wl) if v.version_uid == "vuid-ok")

    async def test_versions_are_sorted_newest_first_by_real_backup_time(self, tmp_path: Path) -> None:
        """Rows are stored deliberately *not* in chronological order;
        ``versions()`` still returns them newest-first by ``start_time``."""
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        write_copy_target_version(
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
                    version_spec_json(1786024626, status="COMPLETED"),
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
                    version_spec_json(1786000000, status="COMPLETED"),
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
                    version_spec_json(1786099999, status="COMPLETED"),
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
        """A version with no usable ``start_time``/``end_time`` (corrupt
        ``version_spec``) sorts after every timestamped version, highest
        ``version_id`` first among such rows."""
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        write_copy_target_version(
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
                    version_spec_json(1786024626, status="COMPLETED"),
                ),
                (101, 10, 1, "vuid-no-time-a", "VM", "t", "", "", 0, 0, version_spec_json(status="COMPLETED")),
                (102, 10, 1, "vuid-no-time-b", "VM", "t", "", "", 0, 0, version_spec_json(status="COMPLETED")),
            ],
        )
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            assert [v.version_uid for v in vs] == ["vuid-has-time", "vuid-no-time-b", "vuid-no-time-a"]

    async def test_version_meta_none_when_meta_db_file_missing_entirely(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        write_copy_target_version(
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
                    version_spec_json(start_time=1786024626, status="COMPLETED"),
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
    is ``COMPLETED``/``PARTIAL``/``CANCELED``; any other or missing status
    is filtered out at load time."""

    async def _versions_for_statuses(self, tmp_path: Path, statuses: list[str | None]) -> list[str]:
        write_repo_info(tmp_path / "repo_info")
        write_connection_config(tmp_path / "db" / "connection_config", [(1, "conn-a", 1)])
        write_vault_link_key(tmp_path / "db" / "vault_link_key", [])
        write_workload_config(tmp_path / "db" / "workload_config", [(10, "vm-uid", "VM", VM_SPEC)])
        rows: list[tuple[object, ...]] = []
        for i, status in enumerate(statuses):
            spec = version_spec_json(start_time=1786024626 + i, status=status) if status is not None else "{}"
            rows.append((100 + i, 10, 1, f"vuid-{100 + i}", "VM", "t", "", "", 0, 0, spec))
        write_copy_target_version(tmp_path / "db" / "copy_target_version", rows)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo:
            wl = (await workloads(repo, (await connections(repo))[0]))[0]
            vs = await versions(repo, wl)
            return [str(v.version_uid) for v in vs]

    async def test_completed_partial_and_canceled_are_kept(self, tmp_path: Path) -> None:
        # Newest first: this fixture's start_time increases with i.
        kept = await self._versions_for_statuses(tmp_path, ["COMPLETED", "PARTIAL", "CANCELED"])
        assert kept == ["vuid-102", "vuid-101", "vuid-100"]

    @pytest.mark.parametrize(
        "status", ["BACKING_UP", "FAILED", "PAUSED", "DELETING", "DELETE_FAILED", "CLONING", "NONE"]
    )
    async def test_non_terminal_or_failed_statuses_are_excluded(self, tmp_path: Path, status: str) -> None:
        assert await self._versions_for_statuses(tmp_path, [status]) == []

    async def test_missing_status_field_is_excluded_not_kept(self, tmp_path: Path) -> None:
        assert await self._versions_for_statuses(tmp_path, [None]) == []

    async def test_mixed_statuses_only_the_browsable_ones_survive(self, tmp_path: Path) -> None:
        kept = await self._versions_for_statuses(tmp_path, ["FAILED", "COMPLETED", "CANCELED", "PARTIAL"])
        assert kept == ["vuid-103", "vuid-102", "vuid-101"]


class TestParseVersionSpec:
    """``parse_version_spec``: the decrypt-then-parse step behind both
    ``_parse_version_status`` and ``version_additional_meta``. Decrypts
    whenever ``vault_key`` is given; never probes the raw bytes first."""

    def test_plaintext_no_key_parses_as_is(self) -> None:
        from synology_apm_repo.sdk.catalog.version import parse_version_spec

        spec = version_spec_json(start_time=1786024626, status="COMPLETED")
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
        plaintext = version_spec_json(start_time=1786024626, status="COMPLETED")
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

        # Plaintext run through the decrypt path is not valid ciphertext:
        # None, never a raise or mojibake.
        spec = version_spec_json(start_time=1786024626, status="COMPLETED")
        not_really_ciphertext_b64 = base64.b64encode(spec.encode("utf-8")).decode("ascii")
        assert parse_version_spec(not_really_ciphertext_b64, "vuid-100", b"\x99" * 32) is None

    def test_malformed_json_returns_none(self) -> None:
        from synology_apm_repo.sdk.catalog.version import parse_version_spec

        assert parse_version_spec("not-json-at-all", "vuid-100", None) is None


class TestVersionAdditionalMeta:
    """``version_additional_meta``: ``None`` whenever the field can't be
    read, any level of it having the wrong JSON type included."""

    async def _additional_meta(self, tmp_path: Path, version_spec: str) -> object:
        write_copy_target_version(
            tmp_path / "db" / "copy_target_version",
            [(100, 10, 1, "vuid-100", "GW", "t", "", "", 0, 0, version_spec)],
        )
        async with await _open_repo(tmp_path) as repo:
            return await version_additional_meta(repo, make_version(version_uid="vuid-100"))

    async def test_parses_the_additional_meta_object(self, tmp_path: Path) -> None:
        spec = version_spec_json(additional_meta={"object_db_id": "s_0_10"})
        assert await self._additional_meta(tmp_path, spec) == {"object_db_id": "s_0_10"}

    @pytest.mark.parametrize(
        "version_spec",
        [
            pytest.param([1, 2], id="spec_an_array"),
            pytest.param({"status": "COMPLETED"}, id="status_a_string"),
            pytest.param({"status": [1]}, id="status_an_array"),
            pytest.param({"status": {"additional_meta": {"object_db_id": "s_0_10"}}}, id="additional_meta_an_object"),
            pytest.param({"status": {"additional_meta": 7}}, id="additional_meta_a_number"),
            pytest.param({"status": {"additional_meta": "[1]"}}, id="additional_meta_encodes_an_array"),
        ],
    )
    async def test_a_level_of_the_wrong_json_type_returns_none(self, tmp_path: Path, version_spec: object) -> None:
        assert await self._additional_meta(tmp_path, json.dumps(version_spec)) is None


class TestParseVersionStatus:
    """``_parse_version_status``: the ``status`` object of a raw
    ``version_spec`` string, which ``versions()`` feeds to its status
    filter and ``_version_display_name``."""

    def test_parses_the_status_object(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _parse_version_status

        spec = version_spec_json(start_time=1786024626, status="COMPLETED")
        status = _parse_version_status(spec, "vuid-100", None)
        assert status == ParsedVersionStatus(status="COMPLETED", start_time="1786024626")

    def test_missing_status_key_entirely_returns_none(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        assert _parse_version_status(json.dumps({"spec": {}}), "vuid-100", None) is None

    def test_malformed_json_returns_none(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        assert _parse_version_status("not-json-at-all", "vuid-100", None) is None

    async def test_encrypted_version_spec_is_decrypted_before_parsing(self) -> None:
        # AES-256-CTR under vault_key, IV derived from version_uid
        # (version_spec_iv).
        import base64

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _parse_version_status
        from synology_apm_repo.sdk.format.crypto import version_spec_iv

        vault_key = b"\x42" * 32
        version_uid = "vuid-100"
        plaintext = version_spec_json(start_time=1786024626)
        encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(version_spec_iv(version_uid))).encryptor()
        ciphertext_b64 = base64.b64encode(encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()).decode(
            "ascii"
        )

        status = _parse_version_status(ciphertext_b64, version_uid, vault_key)
        assert status == ParsedVersionStatus(start_time="1786024626")

    def test_decrypting_data_that_is_not_real_ciphertext_degrades_safely(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _parse_version_status

        # Plaintext run through the decrypt path yields invalid UTF-8:
        # None, never a raise or mojibake.
        any_key = b"\x99" * 32
        spec = version_spec_json(start_time=1786024626)
        import base64

        not_really_ciphertext_b64 = base64.b64encode(spec.encode("utf-8")).decode("ascii")
        assert _parse_version_status(not_really_ciphertext_b64, "vuid-100", any_key) is None


class TestVersionDisplayName:
    """``_version_display_name``: pure formatting from an already-parsed
    ``ParsedVersionStatus``."""

    def test_start_time_wins_over_end_time_when_both_present(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="1786024626", end_time="1786028226")
        # In tests/conftest.py's pinned Asia/Taipei (UTC+8).
        assert _version_display_name(status, "vuid-100") == "2026-08-06 21:57:06"

    def test_falls_back_to_end_time_when_start_time_is_zero(self) -> None:
        # "0" is protobuf's not-set value, not a 1970 epoch.
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="0", end_time="1786024626")
        assert _version_display_name(status, "vuid-100") == "2026-08-06 21:57:06"

    def test_both_zero_degrades_to_raw_version_uid(self) -> None:
        from synology_apm_repo.sdk.catalog.version import ParsedVersionStatus, _version_display_name

        status = ParsedVersionStatus(start_time="0", end_time="0")
        assert _version_display_name(status, "vuid-100") == "vuid-100"

    def test_none_status_degrades_to_raw_version_uid(self) -> None:
        from synology_apm_repo.sdk.catalog.version import _version_display_name

        assert _version_display_name(None, "vuid-100") == "vuid-100"


class TestAsEpochSeconds:
    """``_as_epoch_seconds``'s ``bool``/``int``/malformed-``str`` branches;
    real ``version_spec`` data carries ``start_time``/``end_time`` as
    digit strings (protobuf-JSON int64)."""

    def test_bool_is_never_treated_as_an_epoch(self) -> None:
        # bool subclasses int; True/False must not read as epoch 1/0.
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
