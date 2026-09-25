"""Content Layer — Contact's M365 CSV assembly. Pure ``bytes -> bytes``
JSON-to-CSV translation; the tree-navigation and object-fetching logic
that calls this lives in ``units/saas/contact.py``.
"""

from __future__ import annotations

import csv
import io

from .saas_artifact import parse_meta_json

# M365 Outlook-compatible CSV column order (FORMAT-SPEC.md: m365-contact).
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
    """M365 only: one Outlook-compatible CSV row (UTF-8 BOM) from a
    ``client_metadata`` (Graph API contact fields, camelCase) JSON
    object (FORMAT-SPEC.md: m365-contact — column set is this module's own
    reasonable subset of the fields that table documents)."""
    meta = parse_meta_json(meta_bytes, "contact META")
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
