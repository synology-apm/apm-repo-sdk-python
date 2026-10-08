"""Regression tests for ``synology_apm_repo.sdk.units.saas.raw_object``
against a real shared SaaS stream in ``vault-plain``, replayed from a
committed fixture.

Fixture: ``units_saas_raw_object_vault_plain.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``: ``RawObjectProvider.create()``'s
root, children and content for both workloads sharing stream
``DRMdjvEJPzoxQiUC`` — ``workload_id=15`` (SITE) and ``workload_id=25``
(TEAMS). No test's calls are a superset of the others', so recording
needs the whole file run together.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAM_UUID = "DRMdjvEJPzoxQiUC"


async def _open_provider(
    repo: DedupRepo, saas_streams: SaasStreamCache, *, sub_type: str, version_id: int
) -> RawObjectProvider:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    version = await anext(
        v
        for w in all_workloads
        for v in await versions(repo, w)
        if w.sub_type == sub_type and v.saas_stream_uuid == _STREAM_UUID and v.version_id == version_id
    )
    provider = await RawObjectProvider.create(repo, version, saas_streams)
    assert isinstance(provider, RawObjectProvider)
    return provider


async def test_replayed_root_is_not_a_leaf_and_named_after_the_stream(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: RawObjectProvider.create() reads the object-name
    # index, an internal routing table, through dedup_file.read().
    store = await record_target("units_saas_raw_object_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, sub_type="TEAMS", version_id=111)
        try:
            root = provider.root()
            assert root.name == _STREAM_UUID
            assert root.is_leaf is False
        finally:
            await provider.close()


async def test_replayed_teams_root_shows_the_indexs_own_index_entry_and_reads_back_correctly(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: db_infos_in_snapshot is internal repository
    # structure, not backed-up content.
    store = await record_target("units_saas_raw_object_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, sub_type="TEAMS", version_id=111)
        try:
            nodes = await provider.children(provider.root())
            assert {n.name for n in nodes} == {"db_infos_in_snapshot"}
            [index_node] = nodes
            assert index_node.is_leaf is True
            assert index_node.kind is UnitKind.RAW_OBJECT

            content = (await provider.unit(index_node)).content
            data = await content.read(0, content.size or 0)
            assert data.startswith(b'{"db_objects":[{"name":"teams_channel_db","object_id":"v2_object_1"}')
        finally:
            await provider.close()


async def test_replayed_site_root_lists_its_db_infos_in_snapshot_entries(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only len(data) == node.size is asserted -- a
    # structural oracle, never the content's own meaning.
    store = await record_target("units_saas_raw_object_vault_plain.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, sub_type="SITE", version_id=110)
        try:
            nodes = await provider.children(provider.root())
            # The version's two db_infos_in_snapshot entries.
            assert {n.name for n in nodes} == {"site_list_db", "site_item_db"}

            for node in nodes:
                assert node.is_leaf is True
                assert node.kind is UnitKind.RAW_OBJECT
                content = (await provider.unit(node)).content
                data = await content.read(0, content.size or 0)
                assert len(data) == node.size
        finally:
            await provider.close()
