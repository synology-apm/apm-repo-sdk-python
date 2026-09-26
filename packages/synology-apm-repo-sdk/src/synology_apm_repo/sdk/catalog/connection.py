"""``Connection``: one backup source (a ``db/connection_config`` row) and
its own cheap enumeration query, ``connections()`` — a plain SQLite read,
never touching Pool or Composition, sorted by ``display_name`` since
``connection_config`` has no chronological field.

``workload_config`` carries no ``connection_config_id`` column of its own
— the only table with both ``workload_id`` and ``connection_config_id`` is
``copy_target_version``, so that join is the source of truth
``_workload_ids_by_connection``/``_namespace_by_workload`` below use
(``catalog/workload.py``'s ``_workload_ids_for_connection`` relies on the
same join, single-connection form).
"""

from __future__ import annotations

import dataclasses
import json

from ..dedup.repository import DedupRepo
from ..errors import NotFoundError
from ..identifiers import ConnectionConfigId, ConnectionId, WorkloadId
from ..storage.layout import RepoKind
from ..storage.table import Column, Table, as_int, as_str, sql_placeholders
from .workload_config import _WORKLOAD_COLUMNS


@dataclasses.dataclass(frozen=True)
class Connection:
    """One backup source: a ``db/connection_config`` row, i.e.
    a distinct originating APM this repository has received Copy data from."""

    connection_config_id: ConnectionConfigId
    connection_id: ConnectionId
    display_name: str
    namespaces: tuple[str, ...]
    workload_count: int
    version_count: int


_CONNECTION_CONFIG_COLUMNS = [Column("connection_config_id"), Column("connection_id")]


async def connections(repo: DedupRepo) -> list[Connection]:
    """Sorted by ``display_name`` (case-insensitive). Per-connection
    enrichment (workload ids, namespaces, version counts) is batched
    across every row in one shot each, rather than resolved per row."""
    table = await Table.create(await repo.db("connection_config"), "connection_config", _CONNECTION_CONFIG_COLUMNS)
    rows = [row async for row in table.select()]
    connection_config_ids = [ConnectionConfigId(as_int(row["connection_config_id"])) for row in rows]

    workload_ids_by_connection = await _workload_ids_by_connection(repo, connection_config_ids)
    version_counts_by_connection = await _version_counts_by_connection(repo, connection_config_ids)
    all_workload_ids = [wid for ids in workload_ids_by_connection.values() for wid in ids]
    namespace_by_workload = await _namespace_by_workload(repo, all_workload_ids)
    # Repository-wide, not per-connection — fetched once here rather than
    # once per row below.
    display_name_candidates = await _connection_display_name_candidates(repo)

    result = []
    for row in rows:
        connection_config_id = ConnectionConfigId(as_int(row["connection_config_id"]))
        connection_id = as_str(row["connection_id"])
        workload_ids = workload_ids_by_connection.get(connection_config_id, [])
        namespaces = list(
            dict.fromkeys(namespace_by_workload[wid] for wid in workload_ids if namespace_by_workload.get(wid))
        )
        result.append(
            Connection(
                connection_config_id=connection_config_id,
                connection_id=ConnectionId(connection_id),
                display_name=_connection_display_name(display_name_candidates, connection_id),
                namespaces=tuple(namespaces),
                workload_count=len(workload_ids),
                version_count=version_counts_by_connection.get(connection_config_id, 0),
            )
        )
    result.sort(key=lambda connection: connection.display_name.casefold())
    return result


async def _workload_ids_by_connection(
    repo: DedupRepo, connection_config_ids: list[ConnectionConfigId]
) -> dict[ConnectionConfigId, list[WorkloadId]]:
    """Batched form of ``catalog/workload.py``'s ``_workload_ids_for_connection``
    — one ``WHERE connection_config_id IN (...)`` query for every id, used
    only by ``connections`` (which needs every connection's workload ids
    at once)."""
    if not connection_config_ids:
        return {}
    placeholders = sql_placeholders(len(connection_config_ids))
    conn = await repo.db("copy_target_version")
    cursor = await conn.execute(
        "SELECT DISTINCT connection_config_id, workload_id FROM copy_target_version "
        f"WHERE connection_config_id IN ({placeholders})",
        connection_config_ids,
    )
    rows = await cursor.fetchall()
    result: dict[ConnectionConfigId, list[WorkloadId]] = {}
    for connection_config_id, workload_id in rows:
        result.setdefault(ConnectionConfigId(as_int(connection_config_id)), []).append(WorkloadId(workload_id))
    return result


async def _version_counts_by_connection(
    repo: DedupRepo, connection_config_ids: list[ConnectionConfigId]
) -> dict[ConnectionConfigId, int]:
    """Batched ``COUNT(*) ... GROUP BY connection_config_id``. A
    ``connection_config_id`` with zero versions is absent from the
    returned dict — callers should treat a miss as ``0``."""
    if not connection_config_ids:
        return {}
    placeholders = sql_placeholders(len(connection_config_ids))
    conn = await repo.db("copy_target_version")
    cursor = await conn.execute(
        "SELECT connection_config_id, COUNT(*) FROM copy_target_version "
        f"WHERE connection_config_id IN ({placeholders}) GROUP BY connection_config_id",
        connection_config_ids,
    )
    rows = await cursor.fetchall()
    return {ConnectionConfigId(as_int(connection_config_id)): count for connection_config_id, count in rows}


async def _namespace_by_workload(repo: DedupRepo, workload_ids: list[WorkloadId]) -> dict[WorkloadId, str]:
    """``workload_id -> namespace`` for every id in ``workload_ids`` that
    actually has one recorded — one batched query, used by ``connections``
    to avoid re-querying ``workload_config`` per connection."""
    if not workload_ids:
        return {}
    placeholders = sql_placeholders(len(workload_ids))
    table = await Table.create(await repo.db("workload_config"), "workload_config", _WORKLOAD_COLUMNS)
    result: dict[WorkloadId, str] = {}
    async for row in table.select(f"workload_id IN ({placeholders})", workload_ids):
        namespace = json.loads(as_str(row["workload_spec"])).get("namespace")
        if namespace:
            result[WorkloadId(as_int(row["workload_id"]))] = namespace
    return result


async def _connection_display_name_candidates(repo: DedupRepo) -> list[str]:
    """The full link-name candidate list ``_connection_display_name``
    prefix-matches against — independent of any specific
    ``connection_id``, so ``connections`` fetches this once per call
    rather than once per connection."""
    if repo.layout.kind is RepoKind.VAULT:
        try:
            conn = await repo.db("vault_link_key")
            cursor = await conn.execute("SELECT key FROM vault_link_key")
            return [row[0] for row in await cursor.fetchall()]
        except NotFoundError:
            return []
    if repo.layout.key_root is not None:
        try:
            return await repo.store.listdir(f"{repo.layout.key_root}/link")
        except NotFoundError:
            return []
    return []


def _connection_display_name(candidates: list[str], connection_id: str) -> str:
    """Picks ``connection_id``'s own display name out of ``candidates``
    (``_connection_display_name_candidates``'s repository-wide link-name list) —
    pure, no I/O of its own, since the same ``candidates`` list serves
    every connection in one ``connections`` call."""
    prefix = f"{connection_id}_"
    match = next((c for c in candidates if c.startswith(prefix)), None)
    if match is None:
        return connection_id  # degrade to the raw id rather than fail
    parts = match.split("_", 2)
    return parts[2] if len(parts) >= 3 else connection_id
