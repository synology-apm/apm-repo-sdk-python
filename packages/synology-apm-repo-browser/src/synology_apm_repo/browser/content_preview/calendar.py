"""Calendar event (``.ics``) preview: parses via ``icalendar``."""

from __future__ import annotations

from datetime import date, datetime

import icalendar

from ._common import _NONE_LABEL

_NO_TITLE_LABEL = "(no title)"


def _format_when(when: date | datetime) -> str:
    return when.isoformat(sep=" ") if isinstance(when, datetime) else when.isoformat()


def render_calendar_event_preview(data: bytes) -> str:
    """Parse one exported ``.ics`` (a single ``VEVENT`` —
    ``units/content/saas_calendar.py::build_ics``'s output shape) into
    Organizer/Title/Location/Start Time/End Time/Recurrence lines, one
    per line, always in that order. A field the real event genuinely
    doesn't have shows ``_NONE_LABEL`` (``_NO_TITLE_LABEL`` for Title
    specifically) — never dropped, never a raw internal id. Same
    let-it-propagate posture as ``render_mail_preview``."""
    cal = icalendar.Calendar.from_ical(data)
    events = cal.walk("VEVENT")
    if not events:
        return _NONE_LABEL
    event = events[0]

    organizer = event.get("organizer")
    organizer_text = str(organizer).removeprefix("mailto:") if organizer else _NONE_LABEL
    title = event.get("summary")
    title_text = str(title) if title else _NO_TITLE_LABEL
    location = event.get("location")
    location_text = str(location) if location else _NONE_LABEL
    dtstart = event.get("dtstart")
    start_text = _format_when(dtstart.dt) if dtstart is not None else _NONE_LABEL
    dtend = event.get("dtend")
    end_text = _format_when(dtend.dt) if dtend is not None else _NONE_LABEL
    rrule = event.get("rrule")
    recurrence_text = rrule.to_ical().decode("ascii") if rrule is not None else _NONE_LABEL

    return "\n".join(
        [
            f"Organizer: {organizer_text}",
            f"Title: {title_text}",
            f"Location: {location_text}",
            f"Start Time: {start_text}",
            f"End Time: {end_text}",
            f"Recurrence: {recurrence_text}",
        ]
    )
