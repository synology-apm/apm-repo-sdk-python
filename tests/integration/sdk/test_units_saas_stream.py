"""Regression tests for ``synology_apm_repo.sdk.units.saas.stream``.

- ``units_saas_stream_vault_plain.json.gz`` — recorded against
  ``vault-plain/@ActiveProtectVault``, shared by the tests using
  ``_open_repo``; recipe:
  ``test_replayed_open_saas_obj_resolves_a_real_object_for_every_non_deleted_version``
  (a superset of the others' calls).
- ``units_saas_stream_root_nonempty_vault_plain.json.gz`` — recorded against
  ``vault-plain`` itself (a non-empty ``repo_root``); its one test.
- ``units_saas_stream_agent_repo_{vault_plain,vault_encrypted,
  vault_m365,vault_vm_m365_encrypted}.json.gz`` — one per sample with a
  ``saas/agent_repo``, each recorded against
  ``<sample>/@ActiveProtectVault/saas/agent_repo`` by its one test. The
  four roots share store-relative paths, so one merged fixture couldn't
  hold all four.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, StreamUuid
from synology_apm_repo.sdk.storage.base import ObjectStore, list_names
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout, iter_repository_layouts
from synology_apm_repo.sdk.units.saas.stream import SaasStream

_KNOWN_STREAM_UUID = StreamUuid("DRMdjvEJPzoxQiUC")
_KNOWN_CCID = ConnectionConfigId(1)
_KNOWN_VERSION_ID = 61
_KNOWN_LOCATION = (132, 4, 64)
_KNOWN_SIZE = 26017792

_ALL_STREAMS: list[tuple[ConnectionConfigId, StreamUuid]] = [
    (ConnectionConfigId(1), StreamUuid("DRMdjvEJPzoxQiUC")),
    (ConnectionConfigId(1), StreamUuid("LNJAQtstRVxciJWy")),
    (ConnectionConfigId(1), StreamUuid("vTQbePdWJrIrRncl")),
    (ConnectionConfigId(1), StreamUuid("XfGkaDjWyGhXVoRC")),
    (ConnectionConfigId(3), StreamUuid("uvWRSFkGxCcZAMwt")),
    (ConnectionConfigId(3), StreamUuid("KxMWSUvtSZiaDTDy")),
    (ConnectionConfigId(3), StreamUuid("tfUJpbJdYextKnPE")),
]


async def _open_repo(record_target: Callable[[str], Awaitable[ObjectStore]]) -> DedupRepo:
    store = await record_target("units_saas_stream_vault_plain.json.gz")
    (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
    return await DedupRepo.open(store, layout)


async def test_replayed_version_chain_resolves_to_the_known_file_map_hit(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        version = await anext(
            v
            for w in all_workloads
            for v in await versions(repo, w)
            if v.saas_stream_uuid == _KNOWN_STREAM_UUID and v.version_id == _KNOWN_VERSION_ID
        )
        assert version.target_type == "M365"

        async with SaasStream(repo, _KNOWN_CCID, _KNOWN_STREAM_UUID) as stream:
            assert await stream.stream_version_for(version) == 1

            f = await stream.open_saas_obj(version)
            assert (f.stream_id, f.session_id, f.comp_offset) == _KNOWN_LOCATION
            assert f.size == _KNOWN_SIZE


async def test_replayed_stream_root_is_reachable_when_repo_root_is_non_empty(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_stream_root_nonempty_vault_plain.json.gz")
    layout = await anext(
        layout
        async for repo in iter_repository_layouts(store)
        for layout in catalog_repo_layouts(repo)
        if layout.repo_root
    )
    assert layout.repo_root == "@ActiveProtectVault"

    async with (
        await DedupRepo.open(store, layout) as repo,
        SaasStream(repo, _KNOWN_CCID, _KNOWN_STREAM_UUID) as stream,
    ):
        # The snapshot db resolves through the repo_root-prefixed path.
        path = await anext(stream._candidate_paths("saas_snapshot"))
        connection = (await stream._sources.resolve(path)).connection
        cursor = await connection.execute("SELECT COUNT(*) FROM snapshot_info")
        row = await cursor.fetchone()
        assert row is not None and row[0] == 6


async def test_replayed_all_seven_streams_resolve_their_real_known_versions(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``stream_version_for`` (which raises on a miss) resolves every listed
    M365/GWS version of each stream; ``LNJAQtstRVxciJWy`` has no catalog
    version, so it contributes none."""
    async with await _open_repo(record_target) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        versions_by_stream: dict[tuple[int, str], list[Version]] = {}
        for w in all_workloads:
            for v in await versions(repo, w):
                if v.target_type in ("M365", "GW"):
                    versions_by_stream.setdefault((v.connection_config_id, v.saas_stream_uuid), []).append(v)

        checked = 0
        for ccid, stream_uuid in _ALL_STREAMS:
            async with SaasStream(repo, ccid, stream_uuid) as stream:
                for v in versions_by_stream.get((ccid, stream_uuid), []):
                    await stream.stream_version_for(v)
                    checked += 1
        # Every listed version's stream is one of the seven, and every stream but LNJAQtstRVxciJWy has one.
        no_version = (ConnectionConfigId(1), StreamUuid("LNJAQtstRVxciJWy"))
        assert set(versions_by_stream) == set(_ALL_STREAMS) - {no_version}
        assert checked == sum(len(vs) for vs in versions_by_stream.values()) == 97


async def test_replayed_open_saas_obj_resolves_a_real_object_for_every_non_deleted_version(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """Every non-deleted M365/GWS version resolves to a non-empty
    ``saas_obj``; no object's body is read."""
    async with await _open_repo(record_target) as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        checked = 0
        for w in all_workloads:
            for v in await versions(repo, w):
                if v.target_type not in ("M365", "GW") or v.deleted:
                    continue
                async with SaasStream(repo, v.connection_config_id, v.saas_stream_uuid) as stream:
                    f = await stream.open_saas_obj(v)
                    assert f.size is not None and f.size > 0
                    checked += 1
        assert checked == 97  # M365+GWS non-deleted versions in this fixture


async def _assert_agent_repo_is_empty(store: ObjectStore) -> None:
    info = parse_repo_info(await store.read("repo_info"))
    assert info.repo_type == 6  # SaasRetention

    assert await list_names(store, "@data") == ["trashbin"]
    assert not await store.exists("db")
    assert not await store.exists("@data/Composition")
    assert not await store.exists("@data/Pool")


async def test_replayed_vault_plain_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_vault_plain.json.gz"))


async def test_replayed_vault_encrypted_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_vault_encrypted.json.gz"))


async def test_replayed_vault_m365_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_vault_m365.json.gz"))


async def test_replayed_vault_vm_m365_encrypted_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(
        await record_target("units_saas_stream_agent_repo_vault_vm_m365_encrypted.json.gz")
    )
