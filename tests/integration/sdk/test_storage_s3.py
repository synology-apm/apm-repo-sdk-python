"""Regression test for real ``sample-1`` object-store bytes — replayed
from committed fixtures, with **no external dependency**: this always
runs, on CI or anywhere else, because it goes through ``ReplayStore``
instead of a real backend.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``
wrapping a real store rooted at ``sample-1``):

- ``storage_s3_sample1_layout_and_small_reads.json.gz`` —
  ``iter_layouts``'s walk plus every small (<1 KB) path this sample has,
  read byte-for-byte. One real path this sample has
  (``@ActiveProtectData/5fkUi8kPsAlP/@data/Pool/19/0.inf.315``, a real
  4.2 MB Pool blob chunk) is deliberately excluded — recording its full
  content would bloat this fixture for no test benefit. The 5 tests below sharing this fixture each touch
  a different (path, offset, length) triple — none subsets another's —
  so recording needs all 5 run together against one real backend.
- ``storage_s3_sample1_dedup_query.json.gz`` — opening both real
  repositories (``5fkUi8kPsAlP``, ``gqDuTMuityBf``) with the sample's real key
  and querying ``db("file_map")`` on each. The real vault key is
  embedded below as a literal constant (this sample's own generated
  key, not customer data) rather than read from a real sample tree at
  test time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import RepoKind, iter_layouts

#: sample-1's real key.
_SAMPLE1_KEY_STRING = "wLeLZp9tnAYw@s9m9JIplgBRHN4IPJ+75W8ttZ5okHyFjswEYwGc1K+o="


async def test_replayed_layout_detection_finds_both_real_sample_1_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_s3_sample1_layout_and_small_reads.json.gz")
    layouts = [layout async for layout in iter_layouts(store)]
    ids = {(layout.kind, layout.repo_id) for layout in layouts}
    assert ids == {
        (RepoKind.OBJECT_STORE, "5fkUi8kPsAlP"),
        (RepoKind.OBJECT_STORE, "gqDuTMuityBf"),
    }


async def test_replayed_repo_info_reads_parse_to_the_real_uuids(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_s3_sample1_layout_and_small_reads.json.gz")

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
    store = await record_target("storage_s3_sample1_layout_and_small_reads.json.gz")
    assert await store.size(rel_path) == 0
    assert await store.read(rel_path) == b""


async def test_replayed_windowed_read_matches_the_real_file(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_s3_sample1_layout_and_small_reads.json.gz")
    window = bytes.fromhex("e25c15fb00000000")
    assert await store.read("@ActiveProtectData/gqDuTMuityBf/repo_info", offset=8, length=8) == window
    assert await store.read("@ActiveProtectData/5fkUi8kPsAlP/repo_info.321", offset=8, length=8) == window


async def test_replayed_past_eof_read_on_a_real_small_file_returns_empty(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_s3_sample1_layout_and_small_reads.json.gz")
    rel_path = "@ActiveProtectData/5fkUi8kPsAlP/repo_info.321"
    size = await store.size(rel_path)
    assert await store.read(rel_path, offset=size + 1000, length=10) == b""


async def test_replayed_dedup_repository_opens_and_queries_file_map_for_both_real_repos(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    keys = KeyMaterial.from_key_string(_SAMPLE1_KEY_STRING)

    store = await record_target("storage_s3_sample1_dedup_query.json.gz")
    layouts = [layout async for layout in iter_layouts(store)]
    assert len(layouts) == 2

    # Real file_map row counts for each repository — 5fkUi8kPsAlP has 72 real
    # entries, gqDuTMuityBf only 1.
    expected_counts = {"5fkUi8kPsAlP": 72, "gqDuTMuityBf": 1}
    for layout in layouts:
        assert layout.repo_id is not None  # both real layouts here are object-store repositories
        async with await DedupRepo.open(store, layout, keys) as repo:
            conn = await repo.db("file_map")
            cursor = await conn.execute("SELECT COUNT(*) FROM file_map")
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == expected_counts[layout.repo_id]


__all__: list[str] = []
