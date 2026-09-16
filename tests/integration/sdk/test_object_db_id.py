"""Regression test for ``--object-db-id``'s manual override — replayed
from a committed fixture recorded against a real Teams/USER_CHAT version,
with **no external dependency**: this always runs, on CI or anywhere else,
because it goes through ``ReplayStore`` instead of a real ``LocalFsStore``.

Confirms ``raw_fallback_provider_for``'s manual escape hatch (pinning a
real Teams/USER_CHAT version's ObjectDB directly via ``object_db_id=``,
bypassing dispatch) resolves to the same content the object-name index itself
names for that version, when given ``object_db_id`` in the exact
``"{stream_uuid}_{offset}_{length}"`` shape the object-name index's own
``(offset, length)`` produces.

The fixture (``tests/fixtures/object_db_id_apv1_teams_chat.json.gz``)
was produced once by ``RecordingStore`` wrapping a real store rooted at
``apv-sample-1/@ActiveProtectVault``, recording every call made finding
the real Teams/USER_CHAT version on stream ``uvWRSFkGxCcZAMwt``, resolving
its object-name index, then listing that version's ObjectDB both automatically
(no override) and manually (pinned to the object-name index's own location) —
no key material involved, ``apv-sample-1`` is an unencrypted vault.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.dispatch import raw_fallback_provider_for
from synology_apm_repo.sdk.units.saas.object_name_index import resolve_object_name_index
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider

# Real values the fixture's recorded stream/version resolve to (see this
# module's own docstring): stream uvWRSFkGxCcZAMwt, connection_config_id 3,
# this specific real chat version (workload_id 16).
_VERSION_UID = "882d6f32-6cab-44e1-9b5c-cbb9d1bddcd3"
_CONNECTION_CONFIG_ID = 3


async def test_replayed_manual_object_db_id_matches_the_catalog_indexs_own_location(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    # allow_content=True: RawObjectProvider.create() resolves its own
    # object-db index via a real dedup_file.read() -- exactly this
    # test's own subject matter, an internal routing table, not any
    # object's own content.
    store = await record_target("object_db_id_apv1_teams_chat.json.gz", allow_content=True)
    layout = await detect_layout(store)
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

        auto = await raw_fallback_provider_for(repo, version)
        try:
            assert isinstance(auto, RawObjectProvider)
            auto_nodes = await auto.children(auto.root())
            assert auto_nodes
        finally:
            await auto.close()

        object_db_id = f"{version.saas_stream_uuid}_{object_name_index.offset}_{object_name_index.length}"
        assert object_db_id == "uvWRSFkGxCcZAMwt_66564096_12288"
        manual = await raw_fallback_provider_for(repo, version, object_db_id=object_db_id)
        try:
            assert isinstance(manual, RawObjectProvider)
            manual_nodes = await manual.children(manual.root())
        finally:
            await manual.close()

        auto_locations = {(n.attrs["object_offset"], n.attrs["object_length"]) for n in auto_nodes}
        manual_locations = {(n.attrs["object_offset"], n.attrs["object_length"]) for n in manual_nodes}
        assert auto_locations == {(66_560_000, 181)}
        assert manual_locations == {(3_518_464, 1467), (66_555_904, 2261), (66_560_000, 181)}
        assert auto_locations <= manual_locations


__all__: list[str] = []
