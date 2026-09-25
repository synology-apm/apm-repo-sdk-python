"""Content Layer — Calendar's ``.ics`` assembly from a
``calendar_event_table`` META object's ``client_metadata``. The
tree-navigation and object-fetching logic that calls this lives in
``units/saas/calendar.py``.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import icalendar

from ...errors import UnsupportedDataFormatError
from .saas_artifact import parse_meta_json


def _parse_when(when: dict[str, object]) -> date | datetime:
    if "date" in when:
        return date.fromisoformat(str(when["date"]))
    if "dateTime" in when:
        parsed = datetime.fromisoformat(str(when["dateTime"]))
        if parsed.tzinfo is not None:
            return parsed  # GWS's own shape already carries a real UTC offset
        # M365's dateTimeTimeZone shape: "dateTime" has no offset at all —
        # the actual zone lives only in this sibling "timeZone" field
        # (Microsoft Graph). Left naive here, this would serialize as an
        # icalendar "floating" time, silently reinterpreted in whatever
        # zone the receiving calendar app happens to run in — not what
        # Graph's dateTime+timeZone pair actually means. "UTC" needs no
        # zone-database lookup; anything else is tried via zoneinfo and
        # left naive, unchanged, only if that genuinely can't resolve it
        # (an untranslated Windows zone name, or no local tzdata at all) —
        # never guessed at.
        time_zone = when.get("timeZone")
        if time_zone == "UTC" or not time_zone:
            return parsed.replace(tzinfo=UTC)
        try:
            return parsed.replace(tzinfo=ZoneInfo(str(time_zone)))
        except (ZoneInfoNotFoundError, ValueError):
            # ZoneInfo(str(x)) raises ValueError (not
            # ZoneInfoNotFoundError) for a malformed key (e.g. an
            # absolute/traversal-shaped string) — a corrupted timeZone
            # field degrades to naive time same as a genuinely unknown
            # zone name, rather than crashing this event's .ics export.
            return parsed
    raise UnsupportedDataFormatError(f"unrecognized calendar event start/end shape: {when!r}")


#: Microsoft Graph's own recurrence-pattern-type vocabulary -> the RRULE
#: FREQ it maps to. "absoluteMonthly"/"relativeMonthly" both mean
#: FREQ=MONTHLY (the absolute/relative distinction is BYMONTHDAY vs.
#: BYDAY, handled separately in ``_m365_recurrence_rrule``); same for the
#: two yearly variants. Not private (unlike this module's other
#: constants) -- ``units/saas/calendar.py``'s own ``_recurrence_label``
#: also derives its display label from this same vocabulary, reusing it
#: rather than maintaining a second, independent pattern-type table.
M365_RECURRENCE_FREQ = {
    "daily": "DAILY",
    "weekly": "WEEKLY",
    "absoluteMonthly": "MONTHLY",
    "relativeMonthly": "MONTHLY",
    "absoluteYearly": "YEARLY",
    "relativeYearly": "YEARLY",
}

#: Graph's own lowercase day-name spelling -> RRULE's 2-letter BYDAY code.
_M365_RECURRENCE_WEEKDAY = {
    "sunday": "SU",
    "monday": "MO",
    "tuesday": "TU",
    "wednesday": "WE",
    "thursday": "TH",
    "friday": "FR",
    "saturday": "SA",
}

#: Graph's own "index" value (relativeMonthly/relativeYearly only) -> the
#: BYDAY ordinal prefix RFC 5545 uses for the same concept ("the first
#: Monday" -> BYDAY=1MO, "the last Friday" -> BYDAY=-1FR).
_M365_RECURRENCE_INDEX = {"first": 1, "second": 2, "third": 3, "fourth": 4, "last": -1}


def _m365_recurrence_rrule(recurrence: dict[str, object], dtstart: date | datetime | None) -> icalendar.vRecur | None:
    """Microsoft Graph's own recurrence shape (``{"pattern": {...},
    "range": {...}}``) translated into a real RRULE -- distinct from
    GWS's own shape (a list of literal ``RRULE:...``/``EXDATE:...``
    strings, handled separately in ``build_ics`` itself, since Graph's
    own structured shape is never a list). ``None`` for a pattern type
    this doesn't recognize, rather than guessing at one. ``dtstart`` is
    this same event's own parsed ``DTSTART`` (``None`` only when the
    event has no ``start`` at all) -- needed because RFC 5545 requires
    ``UNTIL``'s value type to match ``DTSTART``'s, so a timed event's
    date-only ``endDate`` must be widened to a full ``DATE-TIME``."""
    pattern = recurrence.get("pattern")
    if not isinstance(pattern, dict):
        return None
    pattern_type = pattern.get("type")
    freq = M365_RECURRENCE_FREQ.get(str(pattern_type))
    if freq is None:
        return None

    rule = icalendar.vRecur()
    rule["FREQ"] = freq
    interval = pattern.get("interval")
    if isinstance(interval, int) and interval > 1:
        rule["INTERVAL"] = interval

    if pattern_type == "weekly":
        days = [_M365_RECURRENCE_WEEKDAY[d] for d in pattern.get("daysOfWeek") or () if d in _M365_RECURRENCE_WEEKDAY]
        if days:
            rule["BYDAY"] = days
    elif pattern_type in ("absoluteMonthly", "absoluteYearly"):
        day_of_month = pattern.get("dayOfMonth")
        if isinstance(day_of_month, int) and day_of_month:
            rule["BYMONTHDAY"] = day_of_month
        if pattern_type == "absoluteYearly":
            month = pattern.get("month")
            if isinstance(month, int) and month:
                rule["BYMONTH"] = month
    elif pattern_type in ("relativeMonthly", "relativeYearly"):
        index = _M365_RECURRENCE_INDEX.get(str(pattern.get("index")))
        days = [d for d in pattern.get("daysOfWeek") or () if d in _M365_RECURRENCE_WEEKDAY]
        if index is not None and days:
            rule["BYDAY"] = [f"{index}{_M365_RECURRENCE_WEEKDAY[d]}" for d in days]
        if pattern_type == "relativeYearly":
            month = pattern.get("month")
            if isinstance(month, int) and month:
                rule["BYMONTH"] = month

    range_ = recurrence.get("range")
    if isinstance(range_, dict):
        range_type = range_.get("type")
        if range_type == "endDate":
            end_date = range_.get("endDate")
            if isinstance(end_date, str) and end_date:
                # Graph's own spec shape is a bare "YYYY-MM-DD", but some
                # real payloads carry a full dateTime string instead --
                # tried second, only if the bare-date parse fails, rather
                # than raising and leaving the series open-ended for a
                # value that really does have a resolvable end.
                parsed_end_date: date | None = None
                with contextlib.suppress(ValueError):
                    parsed_end_date = date.fromisoformat(end_date)
                if parsed_end_date is None:
                    with contextlib.suppress(ValueError):
                        parsed_end_date = datetime.fromisoformat(end_date).date()
                if parsed_end_date is not None:
                    # RFC 5545 requires UNTIL's own value type to match
                    # DTSTART's -- Graph's own endDate is date-only
                    # regardless of whether the series itself is timed,
                    # so a timed event's UNTIL is widened to the end of
                    # that same calendar day (UTC), not left a bare
                    # date next to a DATE-TIME DTSTART (which real
                    # calendar clients can reject or mishandle on
                    # import).
                    if isinstance(dtstart, datetime):
                        rule["UNTIL"] = datetime.combine(parsed_end_date, time(23, 59, 59), tzinfo=UTC)
                    else:
                        rule["UNTIL"] = parsed_end_date
        elif range_type == "numbered":
            count = range_.get("numberOfOccurrences")
            if isinstance(count, int) and count > 0:
                rule["COUNT"] = count
        # "noEnd" (or anything else): no UNTIL/COUNT -- an open-ended
        # series, RRULE's own default when neither is present.

    return rule


def build_ics(meta_bytes: bytes, event_id: str) -> bytes:
    """Assemble one ``.ics`` (a single ``VEVENT``) from a
    ``calendar_event_table.meta_object_id`` META object's raw bytes.

    Raises:
        UnsupportedDataFormatError: For the M365 EWS-envelope
            ``client_metadata`` shape — a spec-derived layout this
            refuses rather than guesses at.
    """
    meta = parse_meta_json(meta_bytes, f"calendar event {event_id!r} META", ref=event_id)
    client_metadata = meta.get("client_metadata") or {}
    if "RawXML" in client_metadata:
        raise UnsupportedDataFormatError(
            "M365 EWS-envelope client_metadata is not yet supported for .ics export "
            "(the normal Graph-API-shaped client_metadata is)",
            ref=event_id,
        )

    cal = icalendar.Calendar()
    cal.add("prodid", "-//synology-apm-repo-sdk//")
    cal.add("version", "2.0")

    event = icalendar.Event()
    # "iCalUID" (GWS's own Google Calendar API casing) vs "iCalUId"
    # (M365's own Graph API casing, lowercase "d") — a naive single-key
    # lookup silently misses M365 entirely and falls through to the
    # internal Graph "id" instead, which is opaque and (unlike iCalUId)
    # not stable across a recurring series' own instances. **Known real
    # Exchange quirk, not normalized here**: a materialized exception
    # occurrence's own iCalUId is not byte-identical to its series
    # master's — this is how Exchange itself encodes the exception
    # marker into the UID, not a bug to paper over by substituting the
    # master's UID in its place.
    uid = client_metadata.get("iCalUId") or client_metadata.get("iCalUID") or client_metadata.get("id") or event_id
    event.add("uid", uid)
    # "summary" (GWS) vs "subject" (M365): the *file name* comes from
    # SaasWorkloadProvider's own tree reading the table's ``summary``
    # column directly (a separate value), but this function's own
    # VEVENT content needs both keys checked or a M365 event's real
    # title would be silently absent from it.
    title = client_metadata.get("summary") or client_metadata.get("subject")
    if title:
        event.add("summary", title)
    # "location" is a plain string on GWS, but a Graph API ``location``
    # object on M365 (``{"displayName": ..., "address": {...}, ...}``;
    # ``displayName`` itself is optional and absent on some real events,
    # e.g. a plain "in tester's office" case) — passing
    # the raw dict straight to ``.add()`` would serialize as ``str(dict)``,
    # so the two shapes are handled separately below.
    location = client_metadata.get("location")
    location_text = location if isinstance(location, str) else None
    if location_text is None and isinstance(location, dict):
        display_name = location.get("displayName")
        location_text = display_name if isinstance(display_name, str) else None
    if location_text:
        event.add("location", location_text)
    start = client_metadata.get("start")
    dtstart: date | datetime | None = None
    if start is not None:
        dtstart = _parse_when(start)
        event.add("dtstart", dtstart)
    end = client_metadata.get("end")
    if end is not None:
        event.add("dtend", _parse_when(end))
    # A real, materialized detached occurrence (Microsoft Graph's own
    # ``type: "exception"`` — a modified instance of a recurring series;
    # e.g. seriesMasterId set, originalStart="2024-07-31T13:00:00Z").
    # ``originalStart`` is a bare
    # ISO 8601 string (not the ``{"dateTime": ..., "timeZone": ...}`` shape
    # ``start``/``end`` use) — the *pre-modification* scheduled time, which
    # RECURRENCE-ID must carry so a calendar app treats this VEVENT as an
    # override of that specific instance, not a separate event sharing
    # the same UID by coincidence.
    original_start = client_metadata.get("originalStart")
    if isinstance(original_start, str) and original_start:
        event.add("recurrence-id", datetime.fromisoformat(original_start))
    # GWS's shape is a list of literal RRULE:/... strings; M365's is a
    # structured dict -- iterating a dict only ever yields its own keys,
    # never an RRULE-prefixed string, so the two shapes need this
    # explicit branch.
    recurrence = client_metadata.get("recurrence")
    if isinstance(recurrence, dict):
        rrule = _m365_recurrence_rrule(recurrence, dtstart)
        if rrule is not None:
            event.add("rrule", rrule)
    else:
        for rule in recurrence or ():
            if isinstance(rule, str) and rule.startswith("RRULE:"):
                event.add("rrule", icalendar.vRecur.from_ical(rule[len("RRULE:") :]))
    # GWS's own organizer shape is flat (`{"email": ..., "displayName":
    # ..., "self": ...}`); M365's is nested one level deeper
    # (``{"emailAddress": {"address": ..., "name": ...}}``) — both shapes
    # are checked below.
    organizer = client_metadata.get("organizer") or {}
    organizer_email = organizer.get("email") or (organizer.get("emailAddress") or {}).get("address")
    if organizer_email:
        event.add("organizer", f"mailto:{organizer_email}")

    cal.add_component(event)
    ics_bytes: bytes = cal.to_ical()
    return ics_bytes
