"""Regression tests for ``synology_apm_repo.sdk.units.saas.mail`` against
real GWS/M365 Mail workloads, replayed from committed fixtures.

Each test proves only that a real Mail workload dispatches to
``open_mail_provider`` and resolves its folder structure; none reads a
message, whose content would be recorded into the fixture.
``build_eml()``'s ``X-ABL-ID`` reassembly, per-folder listing, pagination
and M365 folder-name resolution are covered by
``tests/unit/sdk/test_units_saas_mail.py``.

Fixtures, recorded against ``vault-plain/@ActiveProtectVault``, each by
its one test:

- ``units_saas_mail_gws_vault_plain.json.gz`` — a GWS Mail workload
  (workload_id 5): index resolution and the top-level "Mail" bucket's
  existence.
- ``units_saas_mail_m365_vault_plain.json.gz`` — an M365 Exchange Mail workload
  (workload_id 19): index resolution and the shape of its nested
  ``mail_folder_table`` hierarchy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.saas.mail import open_mail_provider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


async def _open_provider(repo: DedupRepo, saas_streams: SaasStreamCache, workload_id: int) -> SaasWorkloadProvider[Any]:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == workload_id)
    version = (await versions(repo, workload))[-1]  # latest
    return await open_mail_provider(repo, version, saas_streams)


async def test_replayed_gws_mail_workload_resolves_to_its_mail_bucket(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_mail_gws_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, workload_id=5)
        try:
            top = await provider.children(provider.root())
            assert [n.name for n in top] == ["Mail"]
        finally:
            await provider.close()


async def test_replayed_m365_mail_workload_resolves_to_its_real_folder_hierarchy(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_mail_m365_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, workload_id=19)
        try:
            top = await provider.children(provider.root())
            assert len(top) > 1
            assert all(n.is_leaf is False for n in top)
            # A nested subfolder shows the hierarchy is recursive, not flat.
            has_nested_subfolder = False
            for folder in top:
                if any(child.is_leaf is False for child in await provider.children(folder)):
                    has_nested_subfolder = True
                    break
            assert has_nested_subfolder
        finally:
            await provider.close()
