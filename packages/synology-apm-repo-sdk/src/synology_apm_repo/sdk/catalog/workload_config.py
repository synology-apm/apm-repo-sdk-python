"""Shared ``db/workload_config`` required-column list — used by both
``catalog/workload.py`` and ``catalog/connection.py`` so a schema change to
one can't silently drift out of sync with the other. A leaf module rather
than either file importing the other's constant, which would create a
two-way dependency (the intended chain is `connection.py` <- `workload.py`
<- `version.py`).
"""

from __future__ import annotations

from ..storage.table import Column

_WORKLOAD_COLUMNS = [Column("workload_id"), Column("workload_uid"), Column("workload_type"), Column("workload_spec")]
