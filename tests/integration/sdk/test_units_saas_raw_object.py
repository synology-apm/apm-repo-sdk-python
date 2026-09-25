"""Regression test for ``synology_apm_repo.sdk.units.saas.raw_object``
against a real shared SaaS stream in ``apv-sample-1`` — replayed from a
committed fixture, with **no external dependency**: these always run,
on CI or anywhere else, because they go through ``ReplayStore`` instead
of a real ``LocalFsStore``.

The fixture (``tests/fixtures/units_saas_raw_object_apv1.json.gz``)
was produced against a real store rooted at
``apv-sample-1/@ActiveProtectVault`` — see ``tests/CLAUDE.md``'s
"Recording a fixture" section for the ``pytest --record-against=...``/
``make record-fixture`` workflow that (re-)records this — recording
``raw_fallback_provider_for()``'s real root/children/content for both
real co-tenant workloads sharing stream ``DRMdjvEJPzoxQiUC``:
``workload_id=15`` (SITE) and ``workload_id=25`` (TEAMS). None of the
3 tests below subsets another (root-name check, TEAMS children+
content, SITE children+content), so re-recording needs all 3 run
together against one real backend.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.dispatch import raw_fallback_provider_for
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
    provider = await raw_fallback_provider_for(repo, version, saas_streams)
    assert isinstance(provider, RawObjectProvider)
    return provider


async def test_replayed_root_is_not_a_leaf_and_named_after_the_stream(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: RawObjectProvider.create() resolves its own
    # object-db index via a real dedup_file.read() before this test's
    # root/is_leaf check even begins.
    store = await record_target("units_saas_raw_object_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
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
    # allow_content=True: reads the index's own db_infos_in_snapshot JSON
    # index -- internal repository structure, not backed-up user content.
    store = await record_target("units_saas_raw_object_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, sub_type="TEAMS", version_id=111)
        try:
            nodes = await provider.children(provider.root())
            assert {n.name for n in nodes} == {"db_infos_in_snapshot"}
            [index_node] = nodes
            assert index_node.is_leaf is True
            assert index_node.kind is UnitKind.RAW_OBJECT

            content = (await provider.unit(index_node)).open()
            data = await content.read(0, content.size or 0)
            assert data.startswith(b'{"db_objects":[{"name":"teams_channel_db","object_id":"v2_object_1"}')
        finally:
            await provider.close()


async def test_replayed_site_root_shows_the_catalogs_own_named_table_entries(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: only len(data) == node.size is asserted -- a
    # structural oracle, never the content's own meaning.
    store = await record_target("units_saas_raw_object_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, sub_type="SITE", version_id=110)
        try:
            nodes = await provider.children(provider.root())
            # The real, deterministic node set this fixture recorded for
            # this version -- both real db_infos_in_snapshot entries.
            assert {n.name for n in nodes} == {"site_list_db", "site_item_db"}

            for node in nodes:
                assert node.is_leaf is True
                assert node.kind is UnitKind.RAW_OBJECT
                content = (await provider.unit(node)).open()
                data = await content.read(0, content.size or 0)
                assert len(data) == node.size  # real bytes actually came back, not a truncated/empty read
        finally:
            await provider.close()


__all__: list[str] = []
