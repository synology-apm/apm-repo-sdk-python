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
    """Full Name/Email always (``_NONE_LABEL`` when absent); every other
    column (``FORMAT-SPEC.md``: M365 Contact) only when set, since most contacts are
    sparse."""
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
    """The first object of a People API repeated field, or ``None``."""
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    return None


def _gws_first(items: object, key: str) -> str:
    """``key`` of a People API repeated field's first entry, or ``""``."""
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
        # Both type fields are optional in the People API.
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
    # "text" is the People API's own display string; prefer it to
    # re-composing ``date``.
    text = birthday.get("text")
    if isinstance(text, str) and text:
        return text
    date = birthday.get("date")
    if isinstance(date, dict):
        year, month, day = date.get("year"), date.get("month"), date.get("day")
        # 0 means "unspecified" for each part; show whichever parts are
        # known (e.g. "06-15" with no year).
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
    """Full Name/Email always; every other field only when set, as in
    ``_render_m365_contact_csv``."""
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
    """Full Name and Email plus whichever other fields the contact has (job
    title, company, phones, address, birthday, notes), from M365's
    Outlook-compatible CSV (UTF-8 BOM) or GWS's People API JSON, told
    apart by the BOM. Parse failures propagate."""
    if data.startswith(b"\xef\xbb\xbf"):
        return _render_m365_contact_csv(data)
    return _render_gws_contact_json(data)
