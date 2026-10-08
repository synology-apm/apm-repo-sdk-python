"""Content Layer — Contact's M365 CSV assembly. Pure ``bytes -> bytes``
JSON-to-CSV translation; the tree-navigation and object-fetching logic
that calls this lives in ``units/saas/contact.py``.
"""

from __future__ import annotations

import csv
import io

from ..._util.jsonparse import parse_json_object

# M365 Outlook-compatible CSV column order (FORMAT-SPEC.md: M365 Contact).
_CSV_HEADER = [
    "First Name",
    "Middle Name",
    "Last Name",
    "E-mail Address",
    "Business Phone",
    "Home Phone",
    "Mobile Phone",
    "Job Title",
    "Company",
    "Business Street",
    "Business City",
    "Business State",
    "Business Postal Code",
    "Business Country/Region",
    "Notes",
]


def build_contact_csv(meta_bytes: bytes) -> bytes:
    """M365 only: an Outlook-compatible CSV (UTF-8 BOM, header plus one
    row) from a contact META object's ``client_metadata`` (Graph API
    contact fields), using a subset of the fields documented in
    FORMAT-SPEC.md: M365 Contact.

    Raises:
        DataCorruptError: ``meta_bytes`` is not a JSON object.
    """
    meta = parse_json_object(meta_bytes, "contact META")
    client_metadata = meta.get("client_metadata") or {}
    emails = client_metadata.get("emailAddresses") or []
    business_phones = client_metadata.get("businessPhones") or []
    home_phones = client_metadata.get("homePhones") or []
    address = client_metadata.get("businessAddress") or {}

    row = [
        client_metadata.get("givenName") or "",
        client_metadata.get("middleName") or "",
        client_metadata.get("surname") or "",
        (emails[0].get("address") if emails and isinstance(emails[0], dict) else "") or "",
        business_phones[0] if business_phones else "",
        home_phones[0] if home_phones else "",
        client_metadata.get("mobilePhone") or "",
        client_metadata.get("jobTitle") or "",
        client_metadata.get("companyName") or "",
        address.get("street") or "",
        address.get("city") or "",
        address.get("state") or "",
        address.get("postalCode") or "",
        address.get("countryOrRegion") or "",
        client_metadata.get("personalNotes") or "",
    ]

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_ALL)
    writer.writerow(_CSV_HEADER)
    writer.writerow(row)
    return b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8")
