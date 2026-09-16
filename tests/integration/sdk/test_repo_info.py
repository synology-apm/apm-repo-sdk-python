"""Regression test for ``parse_repo_info`` — replayed from a committed
fixture recorded against real ``repo_info`` files, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixture (``tests/fixtures/repo_info_real_samples.json.gz``) was
produced by ``RecordingStore`` wrapping a real store rooted at the whole
``samples/`` tree (``local:`` with an empty path), recording the
``repo_info`` read from both ``apv-sample-1/@ActiveProtectVault`` and
``apv-sample-3/@ActiveProtectVault`` -- see ``tests/CLAUDE.md``'s
"Recording a fixture" section. Neither test below subsets the other
(different real samples), so recording needs both run together against
one real backend.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.storage.base import ObjectStore


async def test_replayed_apv_sample_1_repo_info(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("repo_info_real_samples.json.gz")
    info = parse_repo_info(await store.read("apv-sample-1/@ActiveProtectVault/repo_info"))

    assert info.uuid == "m7e61v80stMAZgru"
    assert info.major == 2
    assert info.minor == 2
    assert info.repo_type == 2
    assert info.repo_flag == 0
    assert info.is_global_dedup_supported is True
    assert info.is_worm_supported is True
    assert info.compress_algorithm == 1
    assert info.encrypt_algorithm == 0  # NOT reliable for judging encryption


async def test_replayed_apv_sample_3_repo_info_has_a_different_uuid(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("repo_info_real_samples.json.gz")
    info = parse_repo_info(await store.read("apv-sample-3/@ActiveProtectVault/repo_info"))
    assert info.uuid == "C8EQPjxuuAWsB3Wh"
    assert info.repo_type == 2


__all__: list[str] = []
