"""SharePoint Site List field filtering — the one export in this package
that isn't bytes-in/text-out.
"""

from __future__ import annotations


def visible_site_fields(values: dict[str, object]) -> dict[str, object]:
    """Drops SharePoint's own OData/id plumbing from one Site List
    item's field dict: any key starting with ``odata``/``OData``
    (a single case-insensitive prefix check catches both the
    lowercase-dotted REST convention, e.g. ``odata.type``, and the
    double-underscore "unspeakable property name" convention, e.g.
    ``OData__UIVersionString``), anything ending in ``Id``
    (case-sensitive, so a plain ``ID`` field survives), and ``GUID``.
    This is SharePoint's own protocol noise, not this project's internal
    repository identifiers, but a preview exists to be scannable for the same
    reason a repository's own ids stay hidden. Order is preserved (dict
    insertion order mirrors the SharePoint field's own order).

    Public (not ``_``-prefixed): called directly by ``unit_screen.py``'s
    ``_load_list_overview`` for each item's *filtered field dict*, not
    pre-rendered text — a List's own items have no per-node preview
    dispatch, since they're never individually browsable as tree nodes."""
    return {
        key: value
        for key, value in values.items()
        if not (key.lower().startswith("odata") or key.endswith("Id") or key == "GUID")
    }
