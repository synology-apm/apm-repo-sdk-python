"""SharePoint List item field filtering."""

from __future__ import annotations


def visible_site_fields(values: dict[str, object]) -> dict[str, object]:
    """Drops SharePoint's own OData/id plumbing from one Site List
    item's field dict, keeping order: any key starting with ``odata``
    (case-insensitive), any ending in ``Id`` (case-sensitive, so ``ID``
    survives), and ``GUID``."""
    return {
        key: value
        for key, value in values.items()
        if not (key.lower().startswith("odata") or key.endswith("Id") or key == "GUID")
    }
