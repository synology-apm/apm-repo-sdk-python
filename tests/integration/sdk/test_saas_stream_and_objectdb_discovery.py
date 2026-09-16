"""Regression tests for SaaS stream/ObjectDb discovery across every real
stream in ``apv-sample-1`` — replayed from a committed fixture recorded
against real bytes, with **no external dependency**: this always runs, on
CI or anywhere else, because it goes through ``ReplayStore`` instead of a
real ``LocalFsStore``.

The fixture (``tests/fixtures/saas_stream_objectdb_discovery_apv1.json.gz``)
was produced once by ``RecordingStore`` wrapping a real store
rooted at ``apv-sample-1/@ActiveProtectVault``, recording, for each of
the 6 real streams a real catalog ``Version`` actually reaches, that
version's object-name index resolution, every named ``object_id``'s ObjectDb
lookup, and every non-empty object's service-name sniff.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import Version, versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import ConnectionConfigId, StreamUuid
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.saas.object_name_index import resolve_object_name_index
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.services import inspect_object
from synology_apm_repo.sdk.units.saas.stream import SaasStream

#: Each of the 6 real streams' object-name-index-listed ``object_id``s,
#: pinned to their exact ObjectDb ``(offset, length)`` — deterministic
#: against this committed fixture.
_EXPECTED_OBJECT_LOCATIONS: dict[tuple[int, str], dict[str, tuple[int, int]]] = {
    (1, "DRMdjvEJPzoxQiUC"): {"db_infos_in_snapshot": (27463680, 611)},
    (1, "vTQbePdWJrIrRncl"): {"drive_db": (6426624, 1694)},
    (1, "XfGkaDjWyGhXVoRC"): {"mail_db": (3624960, 700), "mail_label_db": (3629056, 412)},
    (3, "KxMWSUvtSZiaDTDy"): {"mail_db": (2969600, 4445), "mail_label_db": (2977792, 1622)},
    (3, "tfUJpbJdYextKnPE"): {"drive_db": (0, 697)},
    (3, "uvWRSFkGxCcZAMwt"): {"db_infos_in_snapshot": (66310144, 688)},
}

#: Each of the same 6 streams' real, sniffed service names. Both real
#: M365 streams here resolve only a ``db_infos_in_snapshot`` indirection
#: object through their object-name index (never a directly-sniffable
#: service DB), so their real set is empty — a fact this test pins
#: rather than lets pass vacuously.
_EXPECTED_SERVICE_NAMES: dict[tuple[int, str], frozenset[str]] = {
    (1, "DRMdjvEJPzoxQiUC"): frozenset(),
    (1, "vTQbePdWJrIrRncl"): frozenset({"drive"}),
    (1, "XfGkaDjWyGhXVoRC"): frozenset({"mail"}),
    (3, "KxMWSUvtSZiaDTDy"): frozenset({"mail"}),
    (3, "tfUJpbJdYextKnPE"): frozenset({"drive"}),
    (3, "uvWRSFkGxCcZAMwt"): frozenset(),
}


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]]) -> DedupRepo:
    store = await record_target("saas_stream_objectdb_discovery_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    return await DedupRepo.open(store, layout)


async def _latest_versions_by_stream(repo: DedupRepo) -> dict[tuple[ConnectionConfigId, StreamUuid], Version]:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    latest: dict[tuple[ConnectionConfigId, StreamUuid], Version] = {}
    for w in all_workloads:
        for v in await versions(repo, w):
            if v.target_type not in ("M365", "GW") or v.deleted:
                continue
            key = (v.connection_config_id, v.saas_stream_uuid)
            if key not in latest or v.version_id > latest[key].version_id:
                latest[key] = v
    return latest


async def test_objectdb_identified_via_catalog_index_and_object_ids_readable_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo:
        latest = await _latest_versions_by_stream(repo)
        assert len(latest) == 6

        for (ccid, stream_uuid), version in latest.items():
            async with SaasStream(repo, ccid, stream_uuid) as stream:
                f = await stream.open_saas_obj(version)
                object_name_index = await resolve_object_name_index(repo, version)
                assert object_name_index is not None, (ccid, stream_uuid)
                assert object_name_index.object_ids, (ccid, stream_uuid)

                async with await ObjectDb.load(f, object_name_index.offset, object_name_index.length) as db:
                    locations = {
                        name: await db.get(object_id) for name, object_id in object_name_index.object_ids.items()
                    }
                assert locations == _EXPECTED_OBJECT_LOCATIONS[(ccid, stream_uuid)], (ccid, stream_uuid)


async def test_sniff_results_match_the_real_expected_service_names_replayed(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target) as repo:
        latest = await _latest_versions_by_stream(repo)

        for (ccid, stream_uuid), version in latest.items():
            async with SaasStream(repo, ccid, stream_uuid) as stream:
                f = await stream.open_saas_obj(version)
                object_name_index = await resolve_object_name_index(repo, version)
                assert object_name_index is not None, (ccid, stream_uuid)

                service_names: set[str] = set()
                async with await ObjectDb.load(f, object_name_index.offset, object_name_index.length) as db:
                    for object_id in object_name_index.object_ids.values():
                        offset, length = await db.get(object_id)
                        if length == 0:
                            continue
                        result = await inspect_object(f, offset, length)
                        if result.service_name is not None:
                            service_names.add(result.service_name)

                assert frozenset(service_names) == _EXPECTED_SERVICE_NAMES[(ccid, stream_uuid)], (
                    ccid,
                    stream_uuid,
                    service_names,
                )


__all__: list[str] = []
