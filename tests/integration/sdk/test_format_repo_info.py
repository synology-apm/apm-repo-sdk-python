"""Regression tests for ``parse_repo_info`` against two real ``repo_info``
files.

Fixture: ``repo_info_real_samples.json.gz``, recorded against the directory
holding every sample repository. Each test reads a different sample, so
recording needs both run together.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.storage.base import ObjectStore


async def test_replayed_vault_plain_repo_info(record_target: Callable[[str], Awaitable[ObjectStore]]) -> None:
    store = await record_target("repo_info_real_samples.json.gz")
    info = parse_repo_info(await store.read("vault-plain/@ActiveProtectVault/repo_info"))

    assert info.uuid == "m7e61v80stMAZgru"
    assert info.major == 2
    assert info.minor == 2
    assert info.repo_type == 2
    assert info.repo_flag == 0
    assert info.is_global_dedup_supported is True
    assert info.is_worm_supported is True
    assert info.compress_algorithm == 1
    assert info.encrypt_algorithm == 0  # NOT reliable for judging encryption


async def test_replayed_vault_m365_repo_info_has_a_different_uuid(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("repo_info_real_samples.json.gz")
    info = parse_repo_info(await store.read("vault-m365/@ActiveProtectVault/repo_info"))
    assert info.uuid == "C8EQPjxuuAWsB3Wh"
    assert info.repo_type == 2
