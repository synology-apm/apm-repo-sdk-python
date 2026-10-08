"""``db/workload_config`` access shared by ``catalog/workload.py`` and
``catalog/connection.py``: its required columns and ``workload_spec``
parsing. A separate module because ``workload.py`` imports
``connection.py``, so the reverse import would be a cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._util.jsonparse import parse_json_object
from ..storage.table import Column, as_str

WORKLOAD_COLUMNS = [Column("workload_id"), Column("workload_uid"), Column("workload_type"), Column("workload_spec")]


def workload_spec_from_row(row: Mapping[str, object]) -> dict[str, Any]:
    """A ``workload_config`` row's ``workload_spec`` JSON object.

    Raises:
        DataCorruptError: The column is not a JSON object.
    """
    return parse_json_object(as_str(row["workload_spec"]), "workload_spec", ref=str(row.get("workload_uid")))
