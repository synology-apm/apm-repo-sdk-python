"""Unit tests for ``synology_apm_repo.sdk.units.dispatch``'s SaaS
routing (``saas_provider_for``, ``SUPPORTED_SAAS_SUB_TYPES``,
``is_supported``), over a whole synthetic repository root built with
``saas_fakes``, since ``saas_provider_for`` constructs real providers."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from pathlib import Path
from typing import Any, cast

import pytest

import synology_apm_repo.sdk.units.dispatch as dispatch_module
from support.model_factories import make_version, make_workload
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import SaasSubType, Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import NotRestorableError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_SAAS_SUB_TYPES, is_supported, saas_provider_for
from synology_apm_repo.sdk.units.saas.composite_provider import CompositeSaasProvider, _Tagged
from synology_apm_repo.sdk.units.saas.context import SharedSaasContext
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from synology_apm_repo.sdk.units.saas.teams_chat import TeamsChatProvider
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    calendar_event_db,
    calendar_list_db,
    channel_list_db,
    chat_list_db,
    gws_contact_db,
    index_json,
    item_service_db,
    mail_db,
    site_item_db,
    site_list_db,
    write_empty_saas_repo,
    write_saas_object_repo,
)

_STREAM_UUID = "dispatch-stream-uuid"
_IDS = SaasStreamIds(stream_id=17, stream_uuid=_STREAM_UUID)
_CALENDAR_META = json.dumps(
    {
        "attachment_list": [],
        "client_metadata": {"summary": "Meeting", "start": {"date": "2026-01-01"}, "end": {"date": "2026-01-02"}},
    }
).encode()


def _write_repo(
    tmp_path: Path, *, session_id: int, payloads: list[tuple[str, bytes]], db_objects: list[tuple[str, str]]
) -> None:
    write_saas_object_repo(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=payloads,
        db_objects=db_objects,
        target_type="GW",
    )


def _build_empty_saas_repo(tmp_path: Path) -> None:
    """No ``copy_target_version`` db: the version has no object-name index
    (an expected shape, not corruption), so every candidate finds nothing."""
    write_empty_saas_repo(tmp_path, _IDS, session_id=20, target_type="GW")


def _build_mail_and_calendar_repo(tmp_path: Path) -> None:
    """A schema-only (no rows) ``mail_table`` and a populated calendar
    service-DB pair in the same version, both named in the
    ``copy_target_version`` index — M365 ``USER_EXCHANGE``/
    ``GROUP_EXCHANGE``'s shape, where several candidates match at once."""
    _write_repo(
        tmp_path,
        session_id=22,
        payloads=[
            ("mail_svc", mail_db([])),
            ("cal_svc", calendar_list_db([("cal-1", "Primary")])),
            ("event_svc", calendar_event_db([("event-1", "cal-1", "Meeting", "meta_1")])),
            ("meta_1", _CALENDAR_META),
        ],
        db_objects=[("mail_db", "mail_svc"), ("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
    )


def _build_calendar_only_repo(tmp_path: Path) -> None:
    """Only a calendar_table/calendar_event_table service DB pair, named in
    the ``copy_target_version`` index: every other ``USER_EXCHANGE``
    candidate finds no index entry, so only Calendar matches."""
    _write_repo(
        tmp_path,
        session_id=21,
        payloads=[
            ("cal_svc", calendar_list_db([("cal-1", "Primary")])),
            ("event_svc", calendar_event_db([("event-1", "cal-1", "Meeting", "meta_1")])),
            ("meta_1", _CALENDAR_META),
        ],
        db_objects=[("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
    )


def _build_contact_repo(tmp_path: Path) -> None:
    meta_1 = json.dumps(
        {"version": "2.0", "client_metadata": {"names": [{"givenName": "Grace", "familyName": "Hopper"}]}}
    ).encode()
    _write_repo(
        tmp_path,
        session_id=23,
        payloads=[("contact_svc", gws_contact_db([("contact-1", "Grace", "Hopper", "meta_1")])), ("meta_1", meta_1)],
        db_objects=[("contact_db", "contact_svc")],
    )


def _build_drive_repo(tmp_path: Path) -> None:
    item_db_bytes = item_service_db(
        root_folder_id="root-id", items=[("item-a", "file-a.txt", "root-id", 1, 4, "content_a", "hash-a")]
    )
    _write_repo(
        tmp_path,
        session_id=24,
        payloads=[("drive_svc", item_db_bytes), ("content_a", b"data")],
        db_objects=[("drive_db", "drive_svc")],
    )


def _build_site_repo(tmp_path: Path) -> None:
    meta_item_1 = json.dumps({"version": "1.0", "values": {"Title": "Task A"}, "content_list": []}).encode()
    _write_repo(
        tmp_path,
        session_id=25,
        payloads=[
            ("list_svc", site_list_db([("list-1", "Tasks", "meta_list_1", 0, "", 0)])),
            ("item_svc", site_item_db([("1", "list-1", "", "", "Task A", "0", "meta_item_1", "", "")])),
            ("meta_list_1", b'{"version": "1.0", "metadata": {}, "fields": {}, "views": {}}'),
            ("meta_item_1", meta_item_1),
        ],
        db_objects=[("site_list_db", "list_svc"), ("site_item_db", "item_svc")],
    )


def _build_teams_channel_repo(tmp_path: Path) -> None:
    _write_repo(
        tmp_path,
        session_id=26,
        payloads=[
            ("list_db", channel_list_db([("chan-a", "General")])),
            ("index", index_json([("teams_channel_db", "list_db")])),
        ],
        db_objects=[("db_infos_in_snapshot", "index")],
    )


def _build_teams_chat_repo(tmp_path: Path) -> None:
    _write_repo(
        tmp_path,
        session_id=27,
        payloads=[
            ("list_db", chat_list_db([("chat-a", "Project Sync")])),
            ("index", index_json([("chat_db", "list_db")])),
        ],
        db_objects=[("db_infos_in_snapshot", "index")],
    )


def _build_mail_only_repo(tmp_path: Path) -> None:
    _write_repo(
        tmp_path,
        session_id=28,
        payloads=[("mail_svc", mail_db([("mail-1", "Hello", "", "meta_1")])), ("meta_1", b'{"content_list": []}')],
        db_objects=[("mail_db", "mail_svc")],
    )


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-dispatch",
        target_type="GW",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


def _workload(sub_type: str | None) -> Workload:
    return make_workload(workload_uid="wuid", workload_type="GW", sub_type=sub_type, display_name="test workload")


async def _open_repo(tmp_path: Path) -> DedupRepo:
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)


class TestSupportedSaasSubTypes:
    def test_contains_the_expected_sub_types(self) -> None:
        assert {
            "MAIL",
            "CONTACT",
            "CALENDAR",
            "DRIVE",
            "USER_DRIVE",
            "SITE",
            "USER_EXCHANGE",
            "TEAMS",
            "USER_CHAT",
            "TEAM_DRIVE",
            "GROUP_EXCHANGE",
        } == SUPPORTED_SAAS_SUB_TYPES

    def test_genuinely_unrecognized_sub_types_are_excluded(self) -> None:
        assert "SOME_FUTURE_CONNECTOR_TYPE" not in SUPPORTED_SAAS_SUB_TYPES

    def test_every_known_sub_type_has_a_provider(self) -> None:
        assert set(SaasSubType) == SUPPORTED_SAAS_SUB_TYPES

    @pytest.mark.parametrize(
        ("workload_type", "expected"), [("GW", True), ("M365", True), ("VM", False), ("FS", False), ("NEW", False)]
    )
    def test_workload_is_saas_follows_its_workload_type(self, workload_type: str, expected: bool) -> None:
        assert dataclasses.replace(_workload(None), workload_type=workload_type).is_saas is expected


class TestIsSupported:
    """``is_supported()``: the no-I/O check behind
    ``Repository.workload_is_supported()``."""

    @pytest.mark.parametrize("workload_type", ["VM", "PC", "PS", "FS"])
    def test_device_and_fs_workload_types_are_supported_regardless_of_sub_type(self, workload_type: str) -> None:
        wl = make_workload(workload_uid="wuid", workload_type=workload_type, display_name="test workload")
        assert is_supported(wl) is True

    @pytest.mark.parametrize(
        ("sub_type", "expected"),
        [
            pytest.param("MAIL", True, id="recognized_saas_sub_type_is_supported"),
            pytest.param("SOME_FUTURE_CONNECTOR_TYPE", False, id="unrecognized_saas_sub_type_is_not_supported"),
            pytest.param(None, False, id="saas_workload_with_no_sub_type_at_all_is_not_supported"),
        ],
    )
    def test_saas_sub_type_support(self, sub_type: str | None, expected: bool) -> None:
        assert is_supported(_workload(sub_type)) is expected


class TestSaasProviderForDegradation:
    @pytest.mark.parametrize(
        "sub_type",
        [
            "CALENDAR",
            "TEAMS",
            "SOME_FUTURE_CONNECTOR_TYPE",  # unrecognized — degrades without attempting anything
            "TEAM_DRIVE",
            "GROUP_EXCHANGE",
            None,
            "CONTACT",
            "DRIVE",
            "USER_DRIVE",
            "SITE",
            "USER_CHAT",
            "MAIL",
        ],
    )
    async def test_sub_type_with_no_matching_service_db_degrades_to_raw(
        self, tmp_path: Path, sub_type: str | None
    ) -> None:
        _build_empty_saas_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload(sub_type), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, RawObjectProvider)


class TestSaasProviderForUserExchangeCandidates:
    """``root().name`` tells which config dispatch landed on — every
    candidate is the same ``SaasWorkloadProvider`` class, so ``isinstance``
    can't."""

    @pytest.mark.parametrize(
        "sub_type",
        [
            pytest.param("USER_EXCHANGE", id="user_exchange_tries_mail_and_contact_before_landing_on_calendar"),
            pytest.param("CALENDAR", id="direct_calendar_sub_type_also_resolves_to_calendar_provider"),
            # Same Mail, Contact, Calendar order as USER_EXCHANGE (which
            # adds archive_mail last): same M365 service-DB schema.
            pytest.param("GROUP_EXCHANGE", id="group_exchange_tries_mail_and_contact_before_landing_on_calendar"),
        ],
    )
    async def test_a_calendar_only_repo_resolves_to_the_calendar_provider(self, tmp_path: Path, sub_type: str) -> None:
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload(sub_type), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Calendars"


class TestSaasProviderForUserExchangeMultipleMatches:
    """Several candidates recognizing one version (a real M365
    ``USER_EXCHANGE`` account has Mail, Contact and Calendar at once) are
    all kept, wrapped in a ``CompositeSaasProvider``."""

    async def test_mail_and_calendar_both_present_wraps_in_composite(self, tmp_path: Path) -> None:
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            groups = await provider.children(provider.root())
            assert {g.name for g in groups} == {"Mail", "Calendars"}

    async def test_sibling_candidates_load_the_index_object_db_once_and_close_it_with_the_last(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every candidate reads the same index ObjectDB; it is loaded once
        between them, stays open while any sibling still holds it, and is
        closed when the composite closes."""
        _build_mail_and_calendar_repo(tmp_path)
        loaded: list[ObjectDb] = []
        closed: list[ObjectDb] = []
        original_load = ObjectDb.load.__func__  # type: ignore[attr-defined]
        original_close = ObjectDb.close

        async def counting_load(cls: type[ObjectDb], *args: object) -> ObjectDb:
            object_db = cast(ObjectDb, await original_load(cls, *args))
            loaded.append(object_db)
            return object_db

        async def counting_close(self: ObjectDb) -> None:
            closed.append(self)
            await original_close(self)

        monkeypatch.setattr(ObjectDb, "load", classmethod(counting_load))
        monkeypatch.setattr(ObjectDb, "close", counting_close)
        async with await _open_repo(tmp_path) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams)
            assert isinstance(provider, CompositeSaasProvider)
            assert len(loaded) == 1
            assert closed == []
            await provider.close()
            assert closed == loaded

    async def test_the_raw_fallback_reuses_the_index_object_db_the_candidates_loaded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every candidate fails here, so the raw fallback lists the index; it
        takes its hold on the same ObjectDB the candidates loaded instead of
        loading it again, and closing the fallback closes it. A Drive version
        dispatched as MAIL: the Mail candidate finds no mail_db alias."""
        _build_drive_repo(tmp_path)
        loaded: list[ObjectDb] = []
        closed: list[ObjectDb] = []
        original_load = ObjectDb.load.__func__  # type: ignore[attr-defined]
        original_close = ObjectDb.close

        async def counting_load(cls: type[ObjectDb], *args: object) -> ObjectDb:
            object_db = cast(ObjectDb, await original_load(cls, *args))
            loaded.append(object_db)
            return object_db

        async def counting_close(self: ObjectDb) -> None:
            closed.append(self)
            await original_close(self)

        monkeypatch.setattr(ObjectDb, "load", classmethod(counting_load))
        monkeypatch.setattr(ObjectDb, "close", counting_close)
        async with await _open_repo(tmp_path) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await saas_provider_for(repo, _workload("MAIL"), _version(), saas_streams)
            try:
                assert isinstance(provider, RawObjectProvider)
                loads_by_dispatch = len(loaded)
            finally:
                await provider.close()
            assert loads_by_dispatch == 1
            assert closed == loaded

    async def test_exactly_one_match_is_returned_unwrapped(self, tmp_path: Path) -> None:
        _build_calendar_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert not isinstance(provider, CompositeSaasProvider)

    async def test_unit_on_the_composite_root_raises(self, tmp_path: Path) -> None:
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            with pytest.raises(NotRestorableError, match="not a restorable unit"):
                await provider.unit(provider.root())

    async def test_unit_on_a_real_leaf_delegates_to_its_own_sub_provider(self, tmp_path: Path) -> None:
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            [calendar_group] = [g for g in await provider.children(provider.root()) if g.name == "Calendars"]
            # With no calendar_type column, the calendar lands under the
            # sole "My Calendars" category.
            [my_calendars] = await provider.children(calendar_group)
            [calendar] = await provider.children(my_calendars)
            [event] = await provider.children(calendar)
            unit = await provider.unit(event)
            data = await unit.content.read()
            assert b"Meeting" in data

    @pytest.mark.parametrize(
        "failure", [RuntimeError("synthetic unexpected failure"), asyncio.CancelledError()], ids=["error", "cancel"]
    )
    async def test_unexpected_failure_after_a_prior_candidate_succeeded_closes_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: BaseException
    ) -> None:
        """Mail (the first USER_EXCHANGE candidate) builds before Calendar
        raises something other than ``UnsupportedDataFormatError``; the
        already-built Mail provider must still be closed."""
        _build_mail_and_calendar_repo(tmp_path)

        async def raising_candidate(
            repo: DedupRepo, version: Version, saas_streams: SaasStreamCache, *, shared: SharedSaasContext | None = None
        ) -> SaasWorkloadProvider[Any]:
            raise failure

        candidates = dispatch_module._SAAS_SUB_TYPE_CANDIDATES["USER_EXCHANGE"]
        mail_factory = next(factory for tag, factory in candidates if tag == "mail")
        mail_close_calls: list[SaasWorkloadProvider[Any]] = []

        # Spies on the Mail instance only: Contact, the second candidate,
        # legitimately closes itself on its UnsupportedDataFormatError miss.
        async def spying_mail(
            repo: DedupRepo, version: Version, saas_streams: SaasStreamCache, *, shared: SharedSaasContext | None = None
        ) -> SaasWorkloadProvider[Any]:
            instance = cast(SaasWorkloadProvider[Any], await mail_factory(repo, version, saas_streams, shared=shared))
            original_close = instance.close

            async def spy_close() -> None:
                mail_close_calls.append(instance)
                await original_close()

            monkeypatch.setattr(instance, "close", spy_close)
            return instance

        patched = tuple(
            (tag, spying_mail if tag == "mail" else raising_candidate if tag == "calendar" else factory)
            for tag, factory in candidates
        )
        monkeypatch.setitem(dispatch_module._SAAS_SUB_TYPE_CANDIDATES, "USER_EXCHANGE", patched)

        async with await _open_repo(tmp_path) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(type(failure)) as raised:
                await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams)
            assert raised.value is failure
            assert len(mail_close_calls) == 1

    async def test_children_of_a_node_with_an_unrecognized_tag_is_empty(self, tmp_path: Path) -> None:
        # A pasted canonical ref can name a tag this composite wasn't built
        # with; that lists nothing rather than raising.
        _build_mail_and_calendar_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_EXCHANGE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, CompositeSaasProvider)
            [mail_group] = [g for g in await provider.children(provider.root()) if g.name == "Mail"]
            phantom = dataclasses.replace(mail_group, handle=_Tagged("no-such-tag", ("x",)))
            assert await provider.children(phantom) == []


class TestSaasProviderForSingleCandidateSubTypes:
    """Each single-candidate ``sub_type`` lands on the *right* provider,
    told apart by ``root().name``."""

    async def test_contact_sub_type_resolves_to_contact_provider(self, tmp_path: Path) -> None:
        _build_contact_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("CONTACT"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Contacts"

    @pytest.mark.parametrize("sub_type", ["DRIVE", "USER_DRIVE", "TEAM_DRIVE"])
    async def test_drive_family_sub_type_resolves_to_drive_provider(self, tmp_path: Path, sub_type: str) -> None:
        _build_drive_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload(sub_type), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "/"

    async def test_site_sub_type_resolves_to_site_provider(self, tmp_path: Path) -> None:
        _build_site_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("SITE"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Lists"

    async def test_mail_sub_type_alone_resolves_to_mail_provider(self, tmp_path: Path) -> None:
        _build_mail_only_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("MAIL"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, SaasWorkloadProvider)
            assert provider.root().name == "Mail"

    async def test_teams_sub_type_resolves_to_teams_chat_provider(self, tmp_path: Path) -> None:
        _build_teams_channel_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("TEAMS"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, TeamsChatProvider)
            assert provider.root().name == "Channels"

    async def test_user_chat_sub_type_resolves_to_teams_chat_provider(self, tmp_path: Path) -> None:
        _build_teams_chat_repo(tmp_path)
        async with (
            await _open_repo(tmp_path) as repo,
            SaasStreamCache(repo) as saas_streams,
            await saas_provider_for(repo, _workload("USER_CHAT"), _version(), saas_streams) as provider,
        ):
            assert isinstance(provider, TeamsChatProvider)
            assert provider.root().name == "Chats"
