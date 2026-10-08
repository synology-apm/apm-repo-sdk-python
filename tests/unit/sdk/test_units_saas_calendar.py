"""Unit tests for ``synology_apm_repo.sdk.units.saas.calendar`` (and
``units.content.saas_calendar``'s ``build_ics``) over a synthetic repository
root whose ``saas_obj`` content embeds two ZSTD-compressed service DBs
(``calendar_table`` + ``calendar_event_table``)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import icalendar
import pytest

from support.model_factories import make_version
from support.repo_builders import (
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, NotRestorableError, UnsupportedDataFormatError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node, UnitKind
from synology_apm_repo.sdk.units.content.saas_calendar import build_ics
from synology_apm_repo.sdk.units.saas.calendar import _event_display_name, _recurrence_label, open_calendar_provider
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache
from unit.sdk.saas_fakes import (
    SaasStreamIds,
    calendar_event_db,
    calendar_list_db,
    write_empty_saas_repo,
    write_saas_object_repo,
)

_STREAM_UUID = "calendar-stream-uuid"
_IDS = SaasStreamIds(stream_id=13, stream_uuid=_STREAM_UUID)


_META_EVENT_1 = json.dumps(
    {
        "attachment_list": [],
        "client_metadata": {
            "id": "event-1",
            "iCalUID": "event-1@example.com",
            "summary": "Standup",
            "location": "Room 1",
            "start": {"dateTime": "2026-01-01T09:00:00+00:00"},
            "end": {"dateTime": "2026-01-01T09:30:00+00:00"},
            "organizer": {"email": "boss@example.com"},
            "recurrence": ["RRULE:FREQ=WEEKLY"],
        },
        "version": "1.0",
    }
).encode()

_META_EVENT_EWS = json.dumps(
    {"attachment_list": [], "client_metadata": {"RawXML": "<xml/>", "RecurringMasterId": "abc"}, "version": "1.0"}
).encode()


def _build_calendar_repo(
    tmp_path: Path,
    *,
    session_id: int = 8,
    event_meta: bytes = _META_EVENT_1,
    event_times: dict[str, tuple[int, int]] | None = None,
    calendars: list[tuple[str, str]] | None = None,
    calendar_name_overrides: dict[str, str] | None = None,
) -> None:
    calendar_list_bytes = calendar_list_db(
        calendars or [("cal-1", "Primary Calendar")], overrides=calendar_name_overrides
    )
    event_db_bytes = calendar_event_db([("event-1", "cal-1", "Standup", "meta_1")], times=event_times)
    write_saas_object_repo(
        tmp_path,
        _IDS,
        session_id=session_id,
        version_uid=_version().version_uid,
        payloads=[("cal_svc", calendar_list_bytes), ("event_svc", event_db_bytes), ("meta_1", event_meta)],
        db_objects=[("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
        target_type="GW",
    )


def _version() -> Version:
    return make_version(
        version_id=61,
        version_uid="vuid-calendar",
        target_type="GW",
        target_id=_STREAM_UUID,
        saas_stream_uuid=_STREAM_UUID,
        saas_snapshot_uuid="snap-uuid",
        saas_version_id=3,
    )


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider[Any]]:
    _build_calendar_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await open_calendar_provider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


async def _calendars_of(provider: SaasWorkloadProvider[Any]) -> list[Node]:
    """Every calendar under the My/Other Calendars categories. This file's
    fixtures have no ``calendar_type`` column, so all of them land under
    My Calendars."""
    calendars: list[Node] = []
    for category in await provider.children(provider.root()):
        calendars.extend(await provider.children(category))
    return calendars


class TestTree:
    async def test_root_lists_a_single_my_calendars_category(self, provider: SaasWorkloadProvider[Any]) -> None:
        categories = await provider.children(provider.root())
        assert [c.name for c in categories] == ["My Calendars"]
        assert categories[0].is_leaf is False

    async def test_root_lists_the_calendar(self, provider: SaasWorkloadProvider[Any]) -> None:
        calendars = await _calendars_of(provider)
        assert len(calendars) == 1
        assert calendars[0].name == "Primary Calendar"
        assert calendars[0].is_leaf is False

    async def test_root_and_category_nodes_override_leaf_kind_to_category_group(
        self, provider: SaasWorkloadProvider[Any]
    ) -> None:
        """The root and the categories hold only containers, so a
        ``leaves_only`` column spec keyed on ``CALENDAR_EVENT`` would hide
        every child; a calendar holds events and keeps ``CALENDAR_EVENT``."""
        assert provider.root().leaf_kind is UnitKind.CATEGORY_GROUP
        [category] = await provider.children(provider.root())
        assert category.leaf_kind is UnitKind.CATEGORY_GROUP
        [calendar] = await provider.children(category)
        assert calendar.leaf_kind is UnitKind.CALENDAR_EVENT

    async def test_calendar_lists_its_events(self, provider: SaasWorkloadProvider[Any]) -> None:
        [calendar] = await _calendars_of(provider)
        events = await provider.children(calendar)
        assert len(events) == 1
        assert events[0].name == "Standup"
        assert events[0].is_leaf is True
        assert events[0].kind is UnitKind.CALENDAR_EVENT

    async def test_children_of_an_event_node_is_empty(self, provider: SaasWorkloadProvider[Any]) -> None:
        [calendar] = await _calendars_of(provider)
        [event] = await provider.children(calendar)
        assert await provider.children(event) == []

    async def test_repeated_calls_return_the_same_events(self, provider: SaasWorkloadProvider[Any]) -> None:
        # The event level queries on every call rather than caching an index.
        [calendar] = await _calendars_of(provider)
        first = await provider.children(calendar)
        second = await provider.children(calendar)
        assert [n.ref for n in first] == [n.ref for n in second]

    async def test_calendar_lists_its_events_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[tuple[str, Sequence[object]]] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append((where, params))
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        [calendar] = await _calendars_of(provider)
        calls.clear()
        await provider.children(calendar)
        assert calls == [("calendar_id = ?", ("cal-1",))]

    async def test_event_start_and_end_are_exposed_as_columns(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, event_times={"event-1": (0, 3600)})
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_calendar_provider(repo, _version(), saas_streams) as provider,
        ):
            [calendar] = await _calendars_of(provider)
            [event] = await provider.children(calendar)
            assert event.columns.event_start == datetime.fromtimestamp(0, UTC)
            assert event.columns.event_end == datetime.fromtimestamp(3600, UTC)


class TestPrimaryCalendarDisplayName:
    """An unrenamed Google primary calendar's ``calendar_id`` and
    ``calendar_name`` are both the account email; ``_group_name_override``
    shows the account's name instead."""

    _EMAIL = "user.test025@gwsdemo.example.com"

    async def test_primary_calendar_shows_the_owning_accounts_real_name(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, calendars=[(self._EMAIL, self._EMAIL)])
        spec = {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "wl-1", "M365", spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_calendar_provider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "User Test025"

    async def test_a_calendar_that_is_not_the_owning_account_keeps_its_own_name(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, calendars=[("cal-1", "Team Holidays")])
        spec = {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "wl-1", "M365", spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_calendar_provider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "Team Holidays"

    async def test_a_users_own_calendar_name_override_wins_over_the_account_name(self, tmp_path: Path) -> None:
        """``calendar_name_override`` (the user's own relabeling) wins over
        the account-name substitution."""
        _build_calendar_repo(
            tmp_path,
            calendars=[(self._EMAIL, self._EMAIL)],
            calendar_name_overrides={self._EMAIL: "Work"},
        )
        spec = {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        write_workload_config(tmp_path / "db" / "workload_config", [(1, "wl-1", "M365", spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_calendar_provider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "Work"

    async def test_no_workload_config_at_all_falls_back_to_the_plain_calendar_name(self, tmp_path: Path) -> None:
        """An email-shaped ``calendar_id`` with no owning identity to
        compare it against keeps its ``calendar_name``."""
        _build_calendar_repo(tmp_path, calendars=[(self._EMAIL, self._EMAIL)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await open_calendar_provider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == self._EMAIL


class TestUnit:
    async def test_builds_a_valid_reparseable_ics(self, provider: SaasWorkloadProvider[Any]) -> None:
        [calendar] = await _calendars_of(provider)
        [event] = await provider.children(calendar)
        content = (await provider.unit(event)).content
        data = await content.read()
        reparsed = icalendar.Calendar.from_ical(bytes(data))
        [vevent] = list(reparsed.walk("VEVENT"))
        assert str(vevent.get("summary")) == "Standup"
        assert str(vevent.get("location")) == "Room 1"
        assert str(vevent.get("organizer")) == "mailto:boss@example.com"

    async def test_unit_on_a_calendar_node_raises(self, provider: SaasWorkloadProvider[Any]) -> None:
        [calendar] = await _calendars_of(provider)
        with pytest.raises(NotRestorableError, match="not a restorable unit"):
            await provider.unit(calendar)

    async def test_ews_envelope_client_metadata_raises_on_first_access_not_construction(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, event_meta=_META_EVENT_EWS)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await open_calendar_provider(repo, _version(), saas_streams)
            try:
                [calendar] = await _calendars_of(p)
                [event] = await p.children(calendar)
                unit = await p.unit(event)  # must not raise here
                with pytest.raises(
                    UnsupportedDataFormatError,
                    match=r"M365 EWS-envelope client_metadata is not yet supported for \.ics export",
                ):
                    await unit.content.read()
            finally:
                await p.close()


class TestBuildIcs:
    def test_unrecognized_start_shape_raises_unsupported_data_format(self) -> None:
        # A start/end with neither "date" nor "dateTime".
        meta = json.dumps({"client_metadata": {"summary": "weird", "start": {"nonsense": "x"}}}).encode()
        with pytest.raises(UnsupportedDataFormatError, match="unrecognized calendar event start/end shape"):
            build_ics(meta, "event-weird")

    def test_minimal_event_without_optional_fields(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "bare event"}}).encode()
        ics = build_ics(meta, "event-x")
        assert b"SUMMARY:bare event" in ics
        assert b"UID:event-x" in ics  # falls back to the caller-supplied event_id

    def test_all_day_event_uses_date_not_datetime(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "All day", "start": {"date": "2026-03-01"}, "end": {"date": "2026-03-02"}}}
        ).encode()
        ics = build_ics(meta, "event-y")
        assert b"DTSTART;VALUE=DATE:20260301" in ics

    def test_m365_naive_datetime_with_utc_timezone_is_anchored_not_floating(self) -> None:
        """M365's ``dateTime`` carries no offset (the zone is in
        ``timeZone``); left naive it would serialize as an iCalendar
        floating time, read in the reader's local zone."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "UTC"},
                    "end": {"dateTime": "2026-01-01T09:30:00.0000000", "timeZone": "UTC"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-utc")
        assert b"DTSTART:20260101T090000Z" in ics
        assert b"DTEND:20260101T093000Z" in ics

    def test_m365_naive_datetime_with_named_timezone_resolves_via_zoneinfo(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "Asia/Taipei"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-named-zone")
        assert b"DTSTART;TZID=Asia/Taipei:20260101T090000" in ics

    @pytest.mark.parametrize(
        "time_zone",
        [
            # A legacy Windows zone name (Graph's default without an IANA
            # "Prefer: outlook.timezone"): no real sample data confirms its
            # mapping, so it is not guessed.
            pytest.param("Pacific Standard Time", id="unresolvable_timezone"),
            # ZoneInfo raises ValueError, not ZoneInfoNotFoundError, for a
            # malformed key such as an absolute-path-shaped string.
            pytest.param("/etc/passwd", id="malformed_timezone"),
        ],
    )
    def test_m365_naive_datetime_with_an_unusable_timezone_stays_floating_not_crash(self, time_zone: str) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": time_zone},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-unusable-zone")
        assert b"DTSTART:20260101T090000" in ics
        assert b"DTSTART;TZID" not in ics

    def test_meta_bytes_not_valid_json_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError, match=r"calendar event .* META did not parse as JSON"):
            build_ics(b"not json at all", "event-corrupt")

    def test_m365_naive_datetime_with_no_timezone_key_defaults_to_utc(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "M365 meeting", "start": {"dateTime": "2026-01-01T09:00:00.0000000"}}}
        ).encode()
        ics = build_ics(meta, "event-m365-no-zone")
        assert b"DTSTART:20260101T090000Z" in ics

    def test_gws_offset_bearing_datetime_is_used_as_is_ignoring_any_timezone_key(self) -> None:
        # GWS's "dateTime" carries its own offset; a "timeZone" key must not
        # be applied on top of it.
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "GWS meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00-08:00", "timeZone": "America/Los_Angeles"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-gws")
        assert b'DTSTART;TZID="UTC-08:00":20260101T090000' in ics

    def test_ews_envelope_raises_unsupported_data_format(self) -> None:
        with pytest.raises(
            UnsupportedDataFormatError, match=r"M365 EWS-envelope client_metadata is not yet supported for \.ics export"
        ):
            build_ics(_META_EVENT_EWS, "event-ews")

    def test_recurrence_rule_is_written_as_a_real_rrule(self) -> None:
        ics = build_ics(_META_EVENT_1, "event-1")
        assert b"RRULE:FREQ=WEEKLY" in ics

    def test_m365_absolute_yearly_recurrence_produces_a_real_rrule(self) -> None:
        # M365's recurrence is a dict, not GWS's list of RRULE lines;
        # iterating it would yield its keys as bogus RRULE lines.
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Labor Day",
                    "recurrence": {
                        "pattern": {"type": "absoluteYearly", "dayOfMonth": 1, "month": 5},
                        "range": {"type": "noEnd"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-yearly")
        assert b"RRULE:FREQ=YEARLY;BYMONTHDAY=1;BYMONTH=5" in ics

    def test_m365_weekly_recurrence_with_interval_and_days(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Sprint sync",
                    "recurrence": {
                        "pattern": {
                            "type": "weekly",
                            "interval": 2,
                            "daysOfWeek": ["monday", "wednesday"],
                        },
                        "range": {"type": "numbered", "numberOfOccurrences": 10},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-weekly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=WEEKLY" in rrule_line
        assert "INTERVAL=2" in rrule_line
        assert "BYDAY=MO,WE" in rrule_line
        assert "COUNT=10" in rrule_line

    def test_m365_relative_monthly_recurrence_with_end_date(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Last Friday review",
                    "recurrence": {
                        "pattern": {"type": "relativeMonthly", "index": "last", "daysOfWeek": ["friday"]},
                        "range": {"type": "endDate", "endDate": "2026-12-31"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-relative-monthly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=MONTHLY" in rrule_line
        assert "BYDAY=-1FR" in rrule_line
        assert "UNTIL=20261231" in rrule_line

    def test_m365_recurrence_with_a_timed_dtstart_widens_until_to_a_datetime(self) -> None:
        """RFC 5545 requires UNTIL's value type to match DTSTART's, though
        Graph's ``range.endDate`` is date-only."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Weekly standup",
                    "start": {"dateTime": "2026-01-05T09:00:00", "timeZone": "UTC"},
                    "end": {"dateTime": "2026-01-05T09:30:00", "timeZone": "UTC"},
                    "recurrence": {
                        "pattern": {"type": "weekly", "daysOfWeek": ["monday"]},
                        "range": {"type": "endDate", "endDate": "2026-12-31"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-timed-until")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "UNTIL=20261231T235959Z" in rrule_line

    def test_m365_recurrence_with_non_dict_pattern_omits_rrule(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "x", "recurrence": {"pattern": "not-a-dict"}}}).encode()
        ics = build_ics(meta, "event-bad-pattern")
        assert b"RRULE" not in ics

    def test_m365_relative_yearly_recurrence_includes_bymonth(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Thanksgiving-style holiday",
                    "recurrence": {
                        "pattern": {
                            "type": "relativeYearly",
                            "index": "fourth",
                            "daysOfWeek": ["thursday"],
                            "month": 11,
                        },
                        "range": {"type": "noEnd"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-relative-yearly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=YEARLY" in rrule_line
        assert "BYDAY=4TH" in rrule_line
        assert "BYMONTH=11" in rrule_line

    def test_m365_recurrence_with_unrecognized_pattern_type_omits_rrule(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "x", "recurrence": {"pattern": {"type": "bogus"}, "range": {}}}}
        ).encode()
        ics = build_ics(meta, "event-unrecognized")
        assert b"RRULE" not in ics

    def test_m365_recurrence_with_a_datetime_shaped_end_date_still_resolves_until(self) -> None:
        """Graph documents ``endDate`` as "YYYY-MM-DD", but some payloads
        carry a full dateTime string."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "x",
                    "recurrence": {
                        "pattern": {"type": "daily"},
                        "range": {"type": "endDate", "endDate": "2026-12-31T00:00:00Z"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-datetime-until")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "UNTIL=20261231" in rrule_line

    def test_m365_recurrence_with_malformed_end_date_omits_until(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "x",
                    "recurrence": {
                        "pattern": {"type": "daily"},
                        "range": {"type": "endDate", "endDate": "not-a-date"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-bad-until")
        assert b"RRULE:FREQ=DAILY" in ics
        assert b"UNTIL" not in ics

    @pytest.mark.parametrize(
        ("client_metadata", "expected"),
        [
            # The documented ``iCalUId or iCalUID or id or event_id`` chain.
            pytest.param(
                {"iCalUId": "m365-uid@example.com", "id": "graph-internal-id"},
                b"UID:m365-uid@example.com",
                id="prefers_m365_ical_uid_casing_over_the_internal_graph_id",
            ),
            pytest.param(
                {"iCalUID": "gws-uid@example.com"},
                b"UID:gws-uid@example.com",
                id="falls_back_to_gws_ical_uid_casing_when_m365_casing_is_absent",
            ),
            pytest.param(
                {"id": "graph-internal-id"},
                b"UID:graph-internal-id",
                id="falls_back_to_the_internal_graph_id_when_no_ical_uid_is_present",
            ),
        ],
    )
    def test_uid_resolution(self, client_metadata: dict[str, str], expected: bytes) -> None:
        meta = json.dumps({"client_metadata": client_metadata}).encode()
        ics = build_ics(meta, "event-fallback")
        assert expected in ics

    def test_recurring_instance_override_adds_recurrence_id(self) -> None:
        # originalStart is the instance's pre-modification time; RECURRENCE-ID
        # makes the VEVENT an override of that instance, not a new event.
        meta = json.dumps(
            {"client_metadata": {"summary": "Moved instance", "originalStart": "2026-03-01T09:00:00"}}
        ).encode()
        ics = build_ics(meta, "event-recurring")
        assert b"RECURRENCE-ID:20260301T090000" in ics

    @pytest.mark.parametrize(
        "top_level",
        [{}, {"client_metadata": None}, {"client_metadata": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_client_metadata_missing_null_or_empty_defaults_to_empty(self, top_level: dict[str, object]) -> None:
        meta = json.dumps(top_level).encode()
        ics = build_ics(meta, "event-id")
        assert b"UID:event-id" in ics
        assert b"SUMMARY" not in ics

    def test_title_falls_back_to_the_m365_subject_key(self) -> None:
        meta = json.dumps({"client_metadata": {"subject": "M365 Meeting"}}).encode()
        ics = build_ics(meta, "event-subject")
        assert b"SUMMARY:M365 Meeting" in ics

    def test_organizer_resolves_the_nested_m365_email_address_shape(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "x", "organizer": {"emailAddress": {"address": "boss@example.com"}}}}
        ).encode()
        ics = build_ics(meta, "event-organizer")
        assert b"mailto:boss@example.com" in ics

    def test_location_resolves_the_m365_display_name_dict_shape(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "x", "location": {"displayName": "Room 42"}}}).encode()
        ics = build_ics(meta, "event-location")
        assert b"LOCATION:Room 42" in ics

    def test_location_dict_without_a_display_name_omits_location_entirely(self) -> None:
        # A Graph ``location`` object can lack ``displayName``.
        meta = json.dumps({"client_metadata": {"summary": "x", "location": {"address": {}}}}).encode()
        ics = build_ics(meta, "event-location-2")
        assert b"LOCATION" not in ics


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_calendar_tables_exist(self, tmp_path: Path) -> None:
        write_empty_saas_repo(tmp_path, _IDS, session_id=8, target_type="GW")

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError, match="no object-name index for table"):
                await open_calendar_provider(repo, _version(), saas_streams)


class TestSharedObjectDbCaching:
    """``SaasWorkloadProvider`` (``units/saas/provider.py``) behavior with
    two required tables in the same embedded ``saas_obj`` ObjectDB, as
    Calendar's are: the ObjectDB is loaded once and closed once."""

    async def test_two_required_tables_sharing_one_object_load_it_only_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_calendar_repo(tmp_path)
        original_load = ObjectDb.load
        load_calls: list[tuple[int, int]] = []

        async def counting_load(dedup_file: object, offset: int, length: int) -> ObjectDb:
            load_calls.append((offset, length))
            return await original_load(dedup_file, offset, length)  # type: ignore[arg-type]

        monkeypatch.setattr(ObjectDb, "load", counting_load)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_calendar_provider(repo, _version(), saas_streams)
            try:
                assert len(load_calls) == 1
            finally:
                await provider.close()

    async def test_close_only_closes_the_shared_object_db_once_despite_two_table_entries(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_calendar_repo(tmp_path)
        original_load = ObjectDb.load
        close_calls: list[ObjectDb] = []

        async def spying_load(dedup_file: object, offset: int, length: int) -> ObjectDb:
            object_db = await original_load(dedup_file, offset, length)  # type: ignore[arg-type]
            original_close = object_db.close

            async def spy_close() -> None:
                close_calls.append(object_db)
                await original_close()

            monkeypatch.setattr(object_db, "close", spy_close)
            return object_db

        monkeypatch.setattr(ObjectDb, "load", spying_load)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_calendar_provider(repo, _version(), saas_streams)
            await provider.close()
            # Not two: both tables resolve through the provider's one hold
            # on the index ObjectDB.
            assert len(close_calls) == 1

    async def test_close_attempts_every_connection_when_one_fails_and_is_then_a_no_op(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing source close must not leave the shared ObjectDb open: an
        unclosed aiosqlite connection keeps the interpreter alive. A second
        close() has nothing left to close."""
        _build_calendar_repo(tmp_path)
        closed_dbs: list[ObjectDb] = []
        original_db_close = ObjectDb.close

        async def spy_db_close(self: ObjectDb) -> None:
            closed_dbs.append(self)
            await original_db_close(self)

        monkeypatch.setattr(ObjectDb, "close", spy_db_close)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await open_calendar_provider(repo, _version(), saas_streams)
            names = list(provider._sources)
            assert len(names) >= 2
            attempted: list[str] = []
            for name, source in provider._sources.items():
                real_close = source.close

                async def spy_close(name: str = name, real_close: Callable[[], Awaitable[None]] = real_close) -> None:
                    attempted.append(name)
                    await real_close()
                    if name == names[0]:  # the first one closed fails; the rest must still be attempted
                        raise OSError("source close failed")

                monkeypatch.setattr(source, "close", spy_close)
            with pytest.raises(
                ExceptionGroup, match=r"SaasWorkloadProvider\.close\(\) failed to close every connection"
            ) as exc_info:
                await provider.close()
            assert [str(e) for e in exc_info.value.exceptions] == ["source close failed"]
            assert attempted == names
            assert len(closed_dbs) == 1
            await provider.close()
            assert len(closed_dbs) == 1


class TestEventDisplayName:
    """Covers the empty or null summary, which no fixture event has."""

    @pytest.mark.parametrize(
        ("summary", "expected"),
        [
            pytest.param("", "(no title)", id="empty_string_summary_gets_the_no_title_label"),
            pytest.param(None, "(no title)", id="null_summary_gets_the_no_title_label"),
            pytest.param("Standup", "Standup", id="real_summary_is_used_as_is"),
        ],
    )
    def test_event_display_name(self, summary: str | None, expected: str) -> None:
        assert _event_display_name({"summary": summary}) == expected


class TestRecurrenceLabel:
    def test_no_recurrence_rule_is_blank(self) -> None:
        assert _recurrence_label({"recurrence_rule": None}) == ""
        assert _recurrence_label({"recurrence_rule": ""}) == ""

    def test_malformed_json_is_blank_not_a_raise(self) -> None:
        assert _recurrence_label({"recurrence_rule": "{not json"}) == ""

    def test_no_pattern_key_is_blank(self) -> None:
        assert _recurrence_label({"recurrence_rule": json.dumps({"type": "seriesMaster"})}) == ""

    def test_known_pattern_types_get_their_own_label(self) -> None:
        for pattern_type, label in (
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("absoluteMonthly", "Monthly"),
            ("relativeMonthly", "Monthly"),
            ("absoluteYearly", "Yearly"),
            ("relativeYearly", "Yearly"),
        ):
            row: dict[str, object | None] = {"recurrence_rule": json.dumps({"pattern": {"type": pattern_type}})}
            assert _recurrence_label(row) == label

    def test_unrecognized_pattern_type_still_says_recurring(self) -> None:
        row: dict[str, object | None] = {"recurrence_rule": json.dumps({"pattern": {"type": "somethingNew"}})}
        assert _recurrence_label(row) == "Recurring"
