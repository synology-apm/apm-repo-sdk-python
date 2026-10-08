"""Regression test for ``RawObjectProvider.create``'s ``object_db_id=``
override, replayed from a committed fixture: pinning a real TEAMS version's
ObjectDB directly, as ``"{stream_uuid}_{offset}_{length}"`` from
the object-name index's own ``(offset, length)``, lists every object
automatic discovery lists.

Fixture: ``object_db_id_vault_plain_teams_chat.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``; the one test is its recording recipe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.units.saas.object_name_index import resolve_object_name_index
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider, _ObjectRange
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

# Stable catalog ids of the real TEAMS version.
_VERSION_UID = "882d6f32-6cab-44e1-9b5c-cbb9d1bddcd3"
_CONNECTION_CONFIG_ID = 3


async def test_replayed_manual_object_db_id_matches_the_object_name_indexs_own_location(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: RawObjectProvider.create() reads the object-name
    # index, an internal routing table, through dedup_file.read().
    store = await record_target("object_db_id_vault_plain_teams_chat.json.gz", allow_content=True)
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    async with await DedupRepo.open(store, layout) as repo:
        version = await anext(
            v
            for w in await workloads(
                repo, next(c for c in await connections(repo) if c.connection_config_id == _CONNECTION_CONFIG_ID)
            )
            for v in await versions(repo, w)
            if v.version_uid == _VERSION_UID
        )

        object_name_index = await resolve_object_name_index(repo, version)
        assert object_name_index is not None
        assert (object_name_index.offset, object_name_index.length) == (66_564_096, 12_288)

        async with SaasStreamCache(repo) as saas_streams:
            auto = await RawObjectProvider.create(repo, version, saas_streams)
            try:
                assert isinstance(auto, RawObjectProvider)
                auto_nodes = await auto.children(auto.root())
                assert auto_nodes
            finally:
                await auto.close()

        object_db_id = f"{version.saas_stream_uuid}_{object_name_index.offset}_{object_name_index.length}"
        assert object_db_id == "uvWRSFkGxCcZAMwt_66564096_12288"
        manual = await RawObjectProvider.create(repo, version, saas_streams, object_db_id=object_db_id)
        try:
            assert isinstance(manual, RawObjectProvider)
            manual_nodes = await manual.children(manual.root())
        finally:
            await manual.close()
            await saas_streams.close()

        auto_locations = {(h.offset, h.length) for n in auto_nodes if isinstance(h := n.handle, _ObjectRange)}
        manual_locations = {(h.offset, h.length) for n in manual_nodes if isinstance(h := n.handle, _ObjectRange)}
        assert auto_locations == {(66_560_000, 181)}
        assert manual_locations == {(3_518_464, 1467), (66_555_904, 2261), (66_560_000, 181)}
        assert auto_locations <= manual_locations
