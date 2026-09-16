"""Shared ``db/workload_config`` required-column list — both
``catalog/workload.py`` (``workloads()``/``workload_by_id()``, reading
every column) and ``catalog/connection.py`` (``_namespace_by_workload()``,
reading only ``workload_id``/``workload_spec`` off the same rows) declare
this same required set, so a schema-drift-driven change to one can't
silently drift out of sync with the other. One shared module here rather
than either file importing the other's constant directly, which would
create a two-way dependency between them (ARCHITECTURE.md's Catalog Layer
section: the intended shape is the one-way `connection.py` <- `workload.py`
<- `version.py` chain, and this module is a leaf both `connection.py` and
`workload.py` depend on instead).
"""

from __future__ import annotations

from ..storage.table import Column

_WORKLOAD_COLUMNS = [Column("workload_id"), Column("workload_uid"), Column("workload_type"), Column("workload_spec")]
