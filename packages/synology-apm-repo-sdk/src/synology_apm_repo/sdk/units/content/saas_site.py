"""Content Layer — Site's no-attachment item content: a plain list row's
own field values, serialized as JSON. The tree-navigation and
object-fetching logic that calls this lives in ``units/saas/site.py``.
"""

from __future__ import annotations

import json


def build_values_json(values: dict[str, object]) -> bytes:
    """A list/document-library row with no attachment has no
    ``content_list`` entry — its own field values become its content
    instead."""
    return json.dumps(values).encode("utf-8")
