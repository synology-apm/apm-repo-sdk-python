"""Unit tests for ``synology_apm_repo.sdk.units.saas.workload_helpers``:
group-name and membership lookups over prefetched maps, and the owning
account's profile read from a synthetic ``db/workload_config``."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

import synology_apm_repo.sdk.units.saas.workload_helpers as workload_helpers_module
from support.model_factories import make_version
from support.repo_builders import write_repo_info, write_workload_config
from support.store_fakes import WrappingStore
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import StorageBackendError
from synology_apm_repo.sdk.identifiers import (
    WorkloadId,
)
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.saas.workload_helpers import (
    group_display_name_resolver,
    membership_detail,
    owning_account_user_info,
)

_LAYOUT = RepoLayout(kind=RepoKind.VAULT, repo_root="")
_ALICE = {"name": "Alice", "email": "alice@example.com"}


def _mail_spec(user_info: object) -> dict[str, Any]:
    return {
        "namespace": "ns-a",
        "spec": {"workload_type": "MAIL"},
        "status": {"entity_meta": {"spec": {"user_info": user_info}}},
    }


def _version(workload_id: int) -> Version:
    return make_version(
        workload_id=workload_id,
        target_type="GW",
        target_id="target-1",
        saas_stream_uuid="stream-1",
        saas_snapshot_uuid="snap-1",
        saas_version_id=1,
    )


async def _open(store: ObjectStore) -> DedupRepo:
    return await DedupRepo.open(store, _LAYOUT)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[DedupRepo]:
    write_repo_info(tmp_path / "repo_info")
    write_workload_config(
        tmp_path / "db" / "workload_config",
        [
            (1, "mail-uid", "GW", _mail_spec(_ALICE)),
            (2, "no-profile-uid", "GW", _mail_spec(None)),
            (3, "list-profile-uid", "GW", _mail_spec(["alice@example.com"])),
        ],
    )
    async with await _open(LocalFsStore(tmp_path)) as r:
        yield r


class TestGroupDisplayNameResolver:
    def test_a_known_group_resolves_to_its_name(self) -> None:
        resolve = group_display_name_resolver({"label-1": "Projects"})
        assert resolve("label-1") == "Projects"

    def test_an_unknown_group_falls_back_to_its_raw_key(self) -> None:
        resolve = group_display_name_resolver({"label-1": "Projects"})
        assert resolve("label-2") == "label-2"

    def test_no_name_map_returns_every_raw_key(self) -> None:
        resolve = group_display_name_resolver(None)
        assert resolve("label-1") == "label-1"


class TestMembershipDetail:
    def test_a_member_row_gets_its_names_under_the_detail_name(self) -> None:
        groups = {"7": ["Inbox", "Projects"]}
        assert membership_detail(groups, 7, "labels") == {"labels": ["Inbox", "Projects"]}

    def test_the_row_id_is_looked_up_by_its_string_form(self) -> None:
        groups = {"7": ["Friends"]}
        assert membership_detail(groups, "7", "groups") == {"groups": ["Friends"]}

    def test_a_row_with_no_membership_gets_no_detail(self) -> None:
        assert membership_detail({"7": ["Inbox"]}, 8, "labels") == {}

    def test_a_row_with_an_empty_membership_list_gets_no_detail(self) -> None:
        assert membership_detail({"7": []}, 7, "labels") == {}


class TestOwningAccountUserInfo:
    async def test_returns_the_owning_workloads_user_info(self, repo: DedupRepo) -> None:
        assert await owning_account_user_info(repo, _version(1)) == _ALICE

    @pytest.mark.parametrize(
        "workload_id",
        [
            pytest.param(2, id="null_user_info"),
            pytest.param(3, id="user_info_not_an_object"),
            pytest.param(99, id="no_such_workload_row"),
        ],
    )
    async def test_no_profile_returns_none(self, repo: DedupRepo, workload_id: int) -> None:
        assert await owning_account_user_info(repo, _version(workload_id)) is None

    async def test_a_missing_workload_config_db_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        async with await _open(LocalFsStore(tmp_path)) as repo:
            assert await owning_account_user_info(repo, _version(1)) is None

    async def test_a_workload_spec_that_is_not_a_json_object_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "mail-uid", "GW", [])])  # type: ignore[list-item]
        async with await _open(LocalFsStore(tmp_path)) as repo:
            assert await owning_account_user_info(repo, _version(1)) is None

    async def test_a_workload_config_file_that_is_not_sqlite_returns_none(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        (tmp_path / "db").mkdir()
        (tmp_path / "db" / "workload_config").write_bytes(b"not a sqlite database" * 64)
        async with await _open(LocalFsStore(tmp_path)) as repo:
            assert await owning_account_user_info(repo, _version(1)) is None

    async def test_a_value_error_while_reading_the_row_returns_none(
        self, repo: DedupRepo, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def raising_workload_by_id(repo: DedupRepo, workload_id: WorkloadId) -> None:
            raise ValueError("unparseable workload row")

        monkeypatch.setattr(workload_helpers_module, "workload_by_id", raising_workload_by_id)
        assert await owning_account_user_info(repo, _version(1)) is None

    async def test_a_store_failure_still_raises(self, tmp_path: Path) -> None:
        write_repo_info(tmp_path / "repo_info")
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "mail-uid", "GW", _mail_spec(_ALICE))])

        class _FailingDbStore(WrappingStore):
            """Fails every read of ``db/workload_config`` as a backend would."""

            async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
                if path.startswith("db/workload_config"):
                    raise StorageBackendError("connection reset", ref=path)
                return await super().read(path, offset, length)

        async with await _open(_FailingDbStore(LocalFsStore(tmp_path))) as repo:
            with pytest.raises(StorageBackendError, match="connection reset"):
                await owning_account_user_info(repo, _version(1))
