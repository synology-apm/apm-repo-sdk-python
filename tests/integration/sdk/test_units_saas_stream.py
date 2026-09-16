"""Regression test for ``synology_apm_repo.sdk.units.saas.stream`` —
replayed from committed fixtures recorded against real bytes, with **no
external dependency**: this always runs, on CI or anywhere else, because
it goes through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, all recorded once against apv-sample-1
unless noted, via each replay test's own ``record_target()`` call — see
``tests/conftest.py`` and ``tests/CLAUDE.md``'s "Recording a fixture"
section for the ``pytest --record-against=...`` workflow that
(re-)records these; every test below is its own recording recipe):

- ``units_saas_stream_apv1.json.gz`` — rooted at
  ``apv-sample-1/@ActiveProtectVault``: the known version chain's real
  ``file_map`` hit, real known-version ``stream_version`` resolution for
  all 7 real streams, and ``open_saas_obj`` resolving (non-zero size) for
  every real, non-deleted M365/GW version in the sample -- deliberately
  narrow: it never reads any of these objects' real body content.
- ``units_saas_stream_root_nonempty_apv1.json.gz`` — the same
  known stream, but rooted at ``apv-sample-1`` itself (a non-empty
  ``repo_root``, matching ``Session.discover()``'s real usage) rather
  than directly at ``@ActiveProtectVault``.
- ``units_saas_stream_agent_repo_{apv_sample_1,apv_sample_2_encrypted,
  apv_sample_3,sample_2}.json.gz`` (~477 B each) — one per real sample
  with a ``saas/agent_repo`` (4 of them, confirmed directly against the
  sample tree — see the four ``test_replayed_*_agent_repo_is_always_empty_
  of_restorable_content`` tests below), each just ``repo_info``
  + a directory listing + three ``exists()`` checks, no key material
  needed. Kept as four separate fixtures/tests rather than one merged
  fixture/test — ``exists``/``listdirs``/``reads`` are keyed by
  store-relative path only, and all four real roots share the same
  relative paths (``repo_info``, ``@data``, ...), so merging would
  silently overwrite one real sample's answers with another's (see
  ``tests/CLAUDE.md``'s fixture-merging guidance); each fixture also
  needs its own separate real backend, so one test per real root lets
  each be recorded independently via ``--record-against`` + ``-k``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, StreamUuid
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout, iter_layouts
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
    store = await record_target("units_saas_stream_apv1.json.gz")
    layout = await detect_layout(store)
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
    store = await record_target("units_saas_stream_root_nonempty_apv1.json.gz")
    layout = await anext(layout async for layout in iter_layouts(store) if layout.repo_root)
    assert layout.repo_root == "@ActiveProtectVault"

    async with (
        await DedupRepo.open(store, layout) as repo,
        SaasStream(repo, _KNOWN_CCID, _KNOWN_STREAM_UUID) as stream,
    ):
        # _snapshot_connection() resolving at all (through the non-empty
        # repo_root-prefixed stream path) is what "reachable" means here;
        # the real, deterministic snapshot count this fixture recorded
        # confirms it's the real db, not an empty/decoy one.
        connection = await stream._snapshot_connection()
        cursor = await connection.execute("SELECT COUNT(*) FROM snapshot_info")
        row = await cursor.fetchone()
        assert row is not None and row[0] == 6


async def test_replayed_all_seven_streams_resolve_their_real_known_versions(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
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
        assert checked > 0


async def test_replayed_open_saas_obj_resolves_a_real_object_for_every_non_deleted_version(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """Every real, non-deleted M365/GW version in this fixture resolves to
    a real, non-empty ``saas_obj`` via ``open_saas_obj`` -- deliberately
    narrow: it never reads any of these objects' real body content (see
    this module's own docstring)."""
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
        assert checked == 97  # the real, deterministic count of M365+GW non-deleted versions in this fixture


async def _assert_agent_repo_is_empty(store: ObjectStore) -> None:
    info = parse_repo_info(await store.read("repo_info"))
    assert info.repo_type == 6  # SaasRetention

    assert await store.listdir("@data") == ["trashbin"]
    assert not await store.exists("db")
    assert not await store.exists("@data/Composition")
    assert not await store.exists("@data/Pool")


#: One test per real sample with a ``saas/agent_repo`` -- see this
#: module's own docstring for why these stay four separate tests/fixtures
#: rather than one merged fixture read in a loop: each needs its own real
#: backend root (``<sample_root>/@ActiveProtectVault/saas/agent_repo``),
#: so each is independently recordable via ``--record-against`` + ``-k``.
async def test_replayed_apv_sample_1_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_apv_sample_1.json.gz"))


async def test_replayed_apv_sample_2_encrypted_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(
        await record_target("units_saas_stream_agent_repo_apv_sample_2_encrypted.json.gz")
    )


async def test_replayed_apv_sample_3_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_apv_sample_3.json.gz"))


async def test_replayed_sample_2_agent_repo_is_always_empty_of_restorable_content(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    await _assert_agent_repo_is_empty(await record_target("units_saas_stream_agent_repo_sample_2.json.gz"))


__all__: list[str] = []
