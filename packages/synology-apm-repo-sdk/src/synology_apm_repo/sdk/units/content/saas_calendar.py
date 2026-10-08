"""Content Layer — Calendar's ``.ics`` assembly from a
``calendar_event_table`` META object's ``client_metadata`` (GWS and M365
shapes); ``units/saas/calendar.py`` calls it.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import icalendar

from ..._util.jsonparse import parse_json_object
from ...errors import UnsupportedDataFormatError


def _parse_when(when: dict[str, object]) -> date | datetime:
    if "date" in when:
        return date.fromisoformat(str(when["date"]))
    if "dateTime" in when:
        parsed = datetime.fromisoformat(str(when["dateTime"]))
        if parsed.tzinfo is not None:
            return parsed  # GWS's shape carries its own UTC offset
        # M365's dateTimeTimeZone shape: the zone is only in "timeZone".
        # Left naive, it would serialize as an iCalendar "floating" time in
        # the reader's zone. Stays naive only when zoneinfo can't resolve
        # the name (e.g. a Windows zone name, no local tzdata).
        time_zone = when.get("timeZone")
        if time_zone == "UTC" or not time_zone:
            return parsed.replace(tzinfo=UTC)
        try:
            return parsed.replace(tzinfo=ZoneInfo(str(time_zone)))
        except (ZoneInfoNotFoundError, ValueError):
            # ValueError: a malformed key (e.g. a path-shaped string).
            return parsed
    raise UnsupportedDataFormatError(f"unrecognized calendar event start/end shape: {when!r}")


#: Microsoft Graph's recurrence pattern type -> RRULE FREQ. The
#: absolute/relative variants differ in BYMONTHDAY vs. BYDAY, handled in
#: ``_m365_recurrence_rrule``. Also read by ``units/saas/calendar.py``
#: for its recurrence display label.
M365_RECURRENCE_FREQ = {
    "daily": "DAILY",
    "weekly": "WEEKLY",
    "absoluteMonthly": "MONTHLY",
    "relativeMonthly": "MONTHLY",
    "absoluteYearly": "YEARLY",
    "relativeYearly": "YEARLY",
}

#: Graph's lowercase day name -> RRULE's 2-letter BYDAY code.
_M365_RECURRENCE_WEEKDAY = {
    "sunday": "SU",
    "monday": "MO",
    "tuesday": "TU",
    "wednesday": "WE",
    "thursday": "TH",
    "friday": "FR",
    "saturday": "SA",
}

#: Graph's "index" (relativeMonthly/relativeYearly only) -> RFC 5545's
#: BYDAY ordinal prefix ("last Friday" -> BYDAY=-1FR).
_M365_RECURRENCE_INDEX = {"first": 1, "second": 2, "third": 3, "fourth": 4, "last": -1}


def _m365_recurrence_rrule(recurrence: dict[str, object], dtstart: date | datetime | None) -> icalendar.vRecur | None:
    """Microsoft Graph's structured recurrence (``{"pattern": {...},
    "range": {...}}``) as an RRULE; ``None`` for an unrecognized pattern
    type. ``dtstart`` is the event's parsed ``DTSTART`` (``None`` when it
    has no ``start``), whose value type ``UNTIL`` must match."""
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
                # Graph specifies a bare "YYYY-MM-DD", but some payloads
                # carry a full dateTime; both are accepted.
                parsed_end_date: date | None = None
                with contextlib.suppress(ValueError):
                    parsed_end_date = date.fromisoformat(end_date)
                if parsed_end_date is None:
                    with contextlib.suppress(ValueError):
                        parsed_end_date = datetime.fromisoformat(end_date).date()
                if parsed_end_date is not None:
                    # RFC 5545: UNTIL's value type must match DTSTART's,
                    # so a timed series' UNTIL is the end of that day (UTC).
                    if isinstance(dtstart, datetime):
                        rule["UNTIL"] = datetime.combine(parsed_end_date, time(23, 59, 59), tzinfo=UTC)
                    else:
                        rule["UNTIL"] = parsed_end_date
        elif range_type == "numbered":
            count = range_.get("numberOfOccurrences")
            if isinstance(count, int) and count > 0:
                rule["COUNT"] = count
        # "noEnd" (or anything else): open-ended, no UNTIL/COUNT.

    return rule


def build_ics(meta_bytes: bytes, event_id: str) -> bytes:
    """Assemble one ``.ics`` (a single ``VEVENT``) from a
    ``calendar_event_table.meta_object_id`` META object's raw bytes.

    Raises:
        DataCorruptError: The META object is not a JSON object.
        UnsupportedDataFormatError: The M365 EWS-envelope
            ``client_metadata`` shape, or an unrecognized ``start``/``end``
            shape.
        ValueError: A ``start``/``end``/``originalStart`` date-time or an
            ``RRULE`` string is malformed.
    """
    meta = parse_json_object(meta_bytes, f"calendar event {event_id!r} META", ref=event_id)
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
    # "iCalUId" is M365's casing, "iCalUID" GWS's; Graph's "id" is only a
    # fallback, since it isn't stable across a series' instances. An
    # Exchange exception occurrence's iCalUId differs from its master's by
    # design and is kept as-is.
    uid = client_metadata.get("iCalUId") or client_metadata.get("iCalUID") or client_metadata.get("id") or event_id
    event.add("uid", uid)
    # "summary" (GWS) vs "subject" (M365).
    title = client_metadata.get("summary") or client_metadata.get("subject")
    if title:
        event.add("summary", title)
    # A plain string on GWS; a Graph ``location`` object on M365, whose
    # ``displayName`` is optional.
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
    # A Graph detached occurrence (``type: "exception"``): ``originalStart``
    # (a bare ISO 8601 string) is the instance's pre-modification time,
    # which RECURRENCE-ID carries so the VEVENT overrides that instance.
    original_start = client_metadata.get("originalStart")
    if isinstance(original_start, str) and original_start:
        event.add("recurrence-id", datetime.fromisoformat(original_start))
    # M365: a structured dict; GWS: a list of literal "RRULE:..." strings.
    recurrence = client_metadata.get("recurrence")
    if isinstance(recurrence, dict):
        rrule = _m365_recurrence_rrule(recurrence, dtstart)
        if rrule is not None:
            event.add("rrule", rrule)
    else:
        for rule in recurrence or ():
            if isinstance(rule, str) and rule.startswith("RRULE:"):
                event.add("rrule", icalendar.vRecur.from_ical(rule[len("RRULE:") :]))
    # GWS: flat {"email": ...}; M365: {"emailAddress": {"address": ...}}.
    organizer = client_metadata.get("organizer") or {}
    organizer_email = organizer.get("email") or (organizer.get("emailAddress") or {}).get("address")
    if organizer_email:
        event.add("organizer", f"mailto:{organizer_email}")

    cal.add_component(event)
    ics_bytes: bytes = cal.to_ical()
    return ics_bytes
