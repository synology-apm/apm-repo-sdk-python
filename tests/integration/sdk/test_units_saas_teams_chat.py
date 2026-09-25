"""Regression test for ``synology_apm_repo.sdk.units.saas.teams_chat``
against real M365 Teams data, replayed from committed fixtures recorded
against real bytes, with **no external dependency**: this always runs,
on CI or anywhere else, because it goes through ``ReplayStore`` instead of
a real ``LocalFsStore``.

Deliberately narrow: this module only proves that real Teams/Chat
workloads dispatch to ``TeamsChatProvider`` and resolve without
degradation, and that a real *channel* sequence (not a 1:1/group chat)
lists its channels by name — channel names are the application's own
fixed containers, not backed-up content. It never exports a channel's
HTML page or reads a chat's own member-derived display name: a message's
text and a chat's own name (built from its real member list) *are*
content, and reading either would make ``RecordingStore`` capture that
real content into the committed fixture regardless of what the test
then asserts, since narrowing the assertion can't undo the capture.
HTML rendering, escaping,
sticker embedding, and
``_chat_display_name_from_members``'s self-exclusion behavior (including
the case where self doesn't match any member) are all covered
synthetically by ``tests/unit/sdk/test_units_saas_teams_chat.py``
instead.

The fixtures (``tests/fixtures/``, both recorded against
apv-sample-1/@ActiveProtectVault — see ``tests/CLAUDE.md``'s "Recording
a fixture" section for the ``pytest --record-against=...``/``make
record-fixture`` workflow that (re-)records these):

- ``units_saas_teams_chat_apv1.json.gz`` — the main Teams sequence
  (ccid=1, stream ``DRMdjvEJPzoxQiUC``): just its 6-channel listing.
- ``units_saas_teams_chat_second_stream_and_chats_apv1.json.gz`` — the
  second real Teams stream's one-channel listing, plus every real
  TEAMS/USER_CHAT workload's non-deleted M365/GW version resolving with
  zero degradation (dispatch/object-name-index resolution only, no chat
  listing) -- the 2 tests sharing this fixture don't subset each other
  (one lists a specific stream's channels, the other never calls
  ``children()`` at all), so recording needs both run together.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.dispatch import saas_provider_for
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.teams_chat import TeamsChatProvider

_TEAMS_STREAM = (1, "DRMdjvEJPzoxQiUC")
_EXPECTED_CHANNELS = {"General", "playground111", "haha", "hoho", "private 2", "testing channel"}

_SECOND_TEAMS_STREAM = (3, "uvWRSFkGxCcZAMwt")
_SECOND_TEAMS_CHANNELS = {"test"}


async def _open_repo(record_target: Callable[..., Awaitable[ObjectStore]], fixture_name: str) -> DedupRepo:
    store = await record_target(fixture_name, allow_content=True)
    layout = await detect_layout(store)
    return await DedupRepo.open(store, layout)


async def test_replayed_the_real_teams_channel_sequence_lists_all_six_channels_by_name(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "units_saas_teams_chat_apv1.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        candidates = [
            (w, v)
            for w in all_workloads
            for v in await versions(repo, w)
            if v.saas_stream_uuid == _TEAMS_STREAM[1] and v.connection_config_id == _TEAMS_STREAM[0] and not v.deleted
        ]
        workload, version = max(candidates, key=lambda pair: pair[1].version_id)
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, TeamsChatProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            assert provider.root().name == "Channels"
            categories = await provider.children(provider.root())
            assert {c.name for c in categories} == {"Standard Channels", "Private Channels"}
            channels_by_category = {c.name: await provider.children(c) for c in categories}
            all_channels = [n for nodes in channels_by_category.values() for n in nodes]
            assert {n.name for n in all_channels} == _EXPECTED_CHANNELS
            # The one real membershipType="private" channel in this
            # sample is the only one under Private Channels.
            assert {n.name for n in channels_by_category["Private Channels"]} == {"private 2"}
            for node in all_channels:
                assert node.attrs.get("degraded") is None
        finally:
            await provider.close()


async def test_replayed_the_second_real_teams_stream_lists_its_one_real_channel(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with (
        await _open_repo(record_target, "units_saas_teams_chat_second_stream_and_chats_apv1.json.gz") as repo,
        SaasStreamCache(repo) as saas_streams,
    ):
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c) if w.sub_type == "TEAMS"]
        candidates = [
            (w, v)
            for w in all_workloads
            for v in await versions(repo, w)
            if (v.connection_config_id, v.saas_stream_uuid) == _SECOND_TEAMS_STREAM and not v.deleted
        ]
        workload, version = max(candidates, key=lambda pair: pair[1].version_id)
        untyped_provider = await saas_provider_for(repo, workload, version, saas_streams)
        assert isinstance(untyped_provider, TeamsChatProvider), type(untyped_provider)
        provider = untyped_provider
        try:
            # This stream's one real channel has no private/shared
            # membershipType -- only "Standard Channels" appears.
            [standard] = await provider.children(provider.root())
            assert standard.name == "Standard Channels"
            names = {n.name for n in await provider.children(standard)}
            assert names == _SECOND_TEAMS_CHANNELS
        finally:
            await provider.close()


async def test_replayed_every_real_teams_and_chat_workload_now_resolves_with_zero_degradation(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    async with await _open_repo(record_target, "units_saas_teams_chat_second_stream_and_chats_apv1.json.gz") as repo:
        all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
        checked = 0
        async with SaasStreamCache(repo) as saas_streams:
            for w in all_workloads:
                if w.sub_type not in ("TEAMS", "USER_CHAT"):
                    continue
                for v in await versions(repo, w):
                    if v.target_type not in ("M365", "GW") or v.deleted:
                        continue
                    provider = await saas_provider_for(repo, w, v, saas_streams)
                    assert isinstance(provider, TeamsChatProvider), (
                        w.sub_type,
                        w.display_name,
                        v.version_id,
                        type(provider),
                    )
                    await provider.close()
                    checked += 1
        assert checked == 9


__all__: list[str] = []
