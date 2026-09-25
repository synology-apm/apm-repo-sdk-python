"""Contact preview — M365's Outlook-compatible CSV or GWS's raw
People-API JSON, sniffed from the bytes.
"""

from __future__ import annotations

import csv
import io
import json

from ._common import _NONE_LABEL


def _join_nonempty(*parts: str, sep: str = ", ") -> str:
    return sep.join(p for p in parts if p)


def _render_m365_contact_csv(data: bytes) -> str:
    """Full Name/Email always shown (placeholder when absent, same as
    ``render_calendar_event_preview``'s Title); every other real CSV
    column (``FORMAT-SPEC.md`` §7.5) is dropped when blank rather than
    shown as ``(none)`` — a contact this sparse is the common case, and
    a wall of placeholders would bury the fields that *are* real."""
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig")))
    header = next(reader, [])
    row = next(reader, [])
    fields = dict(zip(header, row, strict=False))
    name_parts = (fields.get("First Name", ""), fields.get("Middle Name", ""), fields.get("Last Name", ""))
    full_name = _join_nonempty(*name_parts, sep=" ") or _NONE_LABEL
    email_address = fields.get("E-mail Address") or _NONE_LABEL
    lines = [f"Full Name: {full_name}", f"Email: {email_address}"]
    for label in ("Job Title", "Company", "Business Phone", "Home Phone", "Mobile Phone"):
        value = fields.get(label) or ""
        if value:
            lines.append(f"{label}: {value}")
    address = _join_nonempty(
        fields.get("Business Street") or "",
        _join_nonempty(fields.get("Business City") or "", fields.get("Business State") or "", sep=" "),
        fields.get("Business Postal Code") or "",
        fields.get("Business Country/Region") or "",
    )
    if address:
        lines.append(f"Address: {address}")
    notes = fields.get("Notes") or ""
    if notes:
        lines.append(f"Notes: {notes}")
    return "\n".join(lines)


def _gws_first_entry(items: object) -> dict[str, object] | None:
    """The first entry of a People API repeated field (``names``/
    ``emailAddresses``/``organizations``/``biographies``/``birthdays``/
    ``phoneNumbers``/``addresses``, each a list of objects) -- ``None``
    if the list is empty, isn't a list, or its first entry isn't an
    object itself."""
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    return None


def _gws_first(items: object, key: str) -> str:
    """The first entry's own ``key`` from a People API repeated field,
    first-entry-wins the same way ``build_contact_csv``'s M365 side
    already treats a repeated Graph field -- ``""`` if
    ``_gws_first_entry`` finds no entry, or that key is missing/blank."""
    entry = _gws_first_entry(items)
    if entry is not None:
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _gws_phone_lines(client_metadata: dict[str, object]) -> list[str]:
    phones = client_metadata.get("phoneNumbers") or []
    lines: list[str] = []
    if not isinstance(phones, list):
        return lines
    for phone in phones:
        if not isinstance(phone, dict):
            continue
        value = phone.get("value")
        if not (isinstance(value, str) and value):
            continue
        # Real sample data has no "type"/"formattedType" on every entry
        # (People API declares both optional) -- shown only when present,
        # never guessed at.
        phone_type = phone.get("formattedType") or phone.get("type")
        label = f"Phone ({phone_type})" if isinstance(phone_type, str) and phone_type else "Phone"
        lines.append(f"{label}: {value}")
    return lines


def _gws_address_line(client_metadata: dict[str, object]) -> str:
    address = _gws_first_entry(client_metadata.get("addresses"))
    if address is None:
        return ""
    formatted = address.get("formattedValue")
    if isinstance(formatted, str) and formatted:
        return formatted
    parts = (address.get(k) for k in ("streetAddress", "city", "region", "postalCode", "country"))
    return _join_nonempty(*(p for p in parts if isinstance(p, str)))


def _gws_birthday_line(client_metadata: dict[str, object]) -> str:
    birthday = _gws_first_entry(client_metadata.get("birthdays"))
    if birthday is None:
        return ""
    # "text" is the People API's own pre-formatted display string (real
    # sample: "10/11/1996") -- used as-is rather than re-composed from
    # the sibling ``date.{day,month,year}`` object, which would risk a
    # different (and possibly locale-wrong) rendering of the same value.
    text = birthday.get("text")
    if isinstance(text, str) and text:
        return text
    date = birthday.get("date")
    if isinstance(date, dict):
        year, month, day = date.get("year"), date.get("month"), date.get("day")
        # The People API's own "unspecified" sentinel is 0, not an
        # absent field, for year/month/day alike -- checked
        # independently for each so a partially-known birthday still
        # shows whichever fields ARE known (e.g. a year-omitted
        # "06-15", or a day-omitted "1990-06") instead of either
        # dropping a known field or rendering a nonsensical "0"/"00"
        # placeholder for an unknown one.
        has_year = isinstance(year, int) and bool(year)
        has_month = isinstance(month, int) and bool(month)
        has_day = isinstance(day, int) and bool(day)
        if has_month and has_day:
            month_day = f"{month:02d}-{day:02d}"
        elif has_month:
            month_day = f"{month:02d}"
        elif has_day:
            month_day = f"{day:02d}"
        else:
            month_day = ""
        if has_year:
            return f"{year}-{month_day}" if month_day else str(year)
        return month_day
    return ""


def _render_gws_contact_json(data: bytes) -> str:
    """Full Name/Email always shown (placeholder when absent); every
    other real People API field (organizations/phoneNumbers/addresses/
    biographies/birthdays) is dropped when absent rather than shown as
    ``(none)``, same reasoning as ``_render_m365_contact_csv``."""
    client_metadata = (json.loads(data).get("client_metadata")) or {}
    full_name = _gws_first(client_metadata.get("names"), "displayName") or _NONE_LABEL
    email_address = _gws_first(client_metadata.get("emailAddresses"), "value") or _NONE_LABEL
    lines = [f"Full Name: {full_name}", f"Email: {email_address}"]
    job_title = _gws_first(client_metadata.get("organizations"), "title")
    if job_title:
        lines.append(f"Job Title: {job_title}")
    company = _gws_first(client_metadata.get("organizations"), "name")
    if company:
        lines.append(f"Company: {company}")
    lines.extend(_gws_phone_lines(client_metadata))
    address = _gws_address_line(client_metadata)
    if address:
        lines.append(f"Address: {address}")
    birthday = _gws_birthday_line(client_metadata)
    if birthday:
        lines.append(f"Birthday: {birthday}")
    notes = _gws_first(client_metadata.get("biographies"), "value")
    if notes:
        lines.append(f"Notes: {notes}")
    return "\n".join(lines)


def render_contact_preview(data: bytes) -> str:
    """Full Name and Email, plus whichever of the platform's own other
    real fields this contact actually has (job title/company, phone
    numbers, address, birthday, notes) — from CSV (M365,
    Outlook-compatible, UTF-8 BOM — ``units/content/saas_contact.py::
    build_contact_csv``'s output shape) or raw JSON (GWS, Google
    People API ``client_metadata``). Which shape ``data`` actually is gets
    sniffed from the bytes (a leading UTF-8 BOM vs. a JSON object), the
    same "no caller-supplied flag" posture as ``render_html_preview``'s
    doctype sniff. Unlike that one, a genuinely malformed contact still
    propagates — same let-it-propagate posture as ``render_mail_preview``."""
    if data.startswith(b"\xef\xbb\xbf"):
        return _render_m365_contact_csv(data)
    return _render_gws_contact_json(data)
