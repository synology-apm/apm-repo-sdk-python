"""Regression tests for ``storage.layout`` over a real object-store
bucket layout (``objstore-m365-encrypted``): detecting both repositories,
reading their small real files (whole, windowed, empty, past EOF) and
opening each detected layout as a ``DedupRepo`` with the sample's vault
key. The bytes come from a local copy of the bucket, so ``S3Store``'s own
request mapping is covered synthetically in
``tests/unit/sdk/test_storage_s3.py``. Both fixtures are recorded against
``objstore-m365-encrypted``:

- ``storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz``
  — ``iter_repository_layouts``'s walk plus byte-for-byte reads of small
  real files. No test's calls are a superset of the others', so recording
  needs every test using it run together.
- ``storage_layout_object_store_objstore_m365_encrypted_dedup_query.json.gz``
  — opening both repositories and querying ``db("file_map")`` on each; its
  one test.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from support.recording.sample_constants import OBJSTORE_M365_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, catalog_repo_layouts, iter_repository_layouts


async def test_replayed_layout_detection_finds_both_real_objstore_m365_encrypted_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz")
    layouts = [layout async for repo in iter_repository_layouts(store) for layout in catalog_repo_layouts(repo)]
    ids = {(layout.kind, layout.repo_id) for layout in layouts}
    assert ids == {
        (RepoKind.OBJECT_STORE, "5fkUi8kPsAlP"),
        (RepoKind.OBJECT_STORE, "gqDuTMuityBf"),
    }


async def test_replayed_repo_info_reads_parse_to_the_real_uuids(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz")

    data = await store.read("@ActiveProtectData/gqDuTMuityBf/repo_info")
    assert len(data) == 219
    assert parse_repo_info(data).uuid == "dZfyXQOqgdjxWRVT"

    data2 = await store.read("@ActiveProtectData/5fkUi8kPsAlP/repo_info.321")
    assert len(data2) == 219
    assert parse_repo_info(data2).uuid == "2cO3L0Dv1TmjzLf9"


@pytest.mark.parametrize(
    "rel_path",
    [
        "@ActiveProtectData/gqDuTMuityBf/db/file_map",
        "@ActiveProtectData/gqDuTMuityBf/db/copy_target_version",
    ],
)
async def test_replayed_real_empty_db_generation_files_read_as_empty(
    rel_path: str, record_target: Callable[[str], Awaitable[ObjectStore]]
) -> None:
    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz")
    assert await store.size(rel_path) == 0
    assert await store.read(rel_path) == b""


async def test_replayed_windowed_read_matches_the_real_file(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz")
    window = bytes.fromhex("e25c15fb00000000")
    assert await store.read("@ActiveProtectData/gqDuTMuityBf/repo_info", offset=8, length=8) == window
    assert await store.read("@ActiveProtectData/5fkUi8kPsAlP/repo_info.321", offset=8, length=8) == window


async def test_replayed_past_eof_read_on_a_real_small_file_returns_empty(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_layout_and_small_reads.json.gz")
    rel_path = "@ActiveProtectData/5fkUi8kPsAlP/repo_info.321"
    size = await store.size(rel_path)
    assert await store.read(rel_path, offset=size + 1000, length=10) == b""


async def test_replayed_dedup_repository_opens_and_queries_file_map_for_both_real_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    keys = KeyMaterial.from_key_string(OBJSTORE_M365_ENCRYPTED_KEY_STRING)

    store = await record_target("storage_layout_object_store_objstore_m365_encrypted_dedup_query.json.gz")
    layouts = [layout async for repo in iter_repository_layouts(store) for layout in catalog_repo_layouts(repo)]
    assert len(layouts) == 2

    expected_counts = {"5fkUi8kPsAlP": 72, "gqDuTMuityBf": 1}
    for layout in layouts:
        assert layout.repo_id is not None  # both real layouts here are object-store repositories
        async with await DedupRepo.open(store, layout, keys) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT COUNT(*) FROM file_map")
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == expected_counts[layout.repo_id]
