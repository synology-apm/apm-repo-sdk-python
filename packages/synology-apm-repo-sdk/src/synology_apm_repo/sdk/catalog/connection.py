"""``Connection``: one backup source (a ``db/connection_config`` row), and
``connections()``, a plain SQLite read that never touches Pool or
Composition.

``workload_config`` has no ``connection_config_id`` column: a workload
belongs to a connection through ``copy_target_version``, the only table
holding both ids.
"""

from __future__ import annotations

import dataclasses

from ..dedup.repository import DedupRepo
from ..errors import NotFoundError
from ..identifiers import ConnectionConfigId, ConnectionId, WorkloadId
from ..storage.base import list_names
from ..storage.layout import RepoKind
from ..storage.table import Column, Table, as_int, as_str, sql_placeholders
from .workload_config import WORKLOAD_COLUMNS, workload_spec_from_row


@dataclasses.dataclass(frozen=True, slots=True)
class Connection:
    """One backup source: a ``db/connection_config`` row, i.e. a distinct
    originating APM this repository has received Copy data from.

    Attributes:
        connection_config_id: The row's id, local to this catalog's database.
        connection_id: The source's own id, as named in link records.
        display_name: The source's name from its link record (FORMAT-SPEC.md:
            ``vault_link_key`` / connection naming), or ``connection_id``
            when none matches.
        namespaces: The distinct non-empty workload namespaces under this
            source, in first-seen order.
        workload_count: Distinct workloads in this source's
            ``copy_target_version`` rows.
        version_count: This source's ``copy_target_version`` rows, deleted
            versions included.
    """

    connection_config_id: ConnectionConfigId
    connection_id: ConnectionId
    display_name: str
    namespaces: tuple[str, ...]
    workload_count: int
    version_count: int


_CONNECTION_CONFIG_COLUMNS = [Column("connection_config_id"), Column("connection_id")]


async def connections(repo: DedupRepo) -> list[Connection]:
    """Every backup source in ``repo``, sorted by ``display_name``
    (case-insensitive); ``connection_config`` has no time field to sort by.
    Each enrichment (workload ids, namespaces, version counts) is one
    batched query for all rows.

    Raises:
        DataCorruptError: A workload's ``workload_spec`` is not a JSON object.
    """
    table = await Table.create(await repo.db("connection_config"), "connection_config", _CONNECTION_CONFIG_COLUMNS)
    rows = [row async for row in table.select()]
    connection_config_ids = [ConnectionConfigId(as_int(row["connection_config_id"])) for row in rows]

    workload_ids_of = await workload_ids_by_connection(repo, connection_config_ids)
    version_counts_by_connection = await _version_counts_by_connection(repo, connection_config_ids)
    all_workload_ids = [wid for ids in workload_ids_of.values() for wid in ids]
    namespace_by_workload = await _namespace_by_workload(repo, all_workload_ids)
    display_name_candidates = await _connection_display_name_candidates(repo)

    result = []
    for row in rows:
        connection_config_id = ConnectionConfigId(as_int(row["connection_config_id"]))
        connection_id = as_str(row["connection_id"])
        workload_ids = workload_ids_of.get(connection_config_id, [])
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


async def workload_ids_by_connection(
    repo: DedupRepo, connection_config_ids: list[ConnectionConfigId]
) -> dict[ConnectionConfigId, list[WorkloadId]]:
    """Each connection's workload ids, through ``copy_target_version``, in
    one query; a connection with none is absent."""
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
    """Version count per connection in one query; a connection with none
    is absent, not ``0``."""
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
    """``workload_id -> namespace`` for the ids in ``workload_ids`` that
    have one, in one query."""
    if not workload_ids:
        return {}
    placeholders = sql_placeholders(len(workload_ids))
    table = await Table.create(await repo.db("workload_config"), "workload_config", WORKLOAD_COLUMNS)
    result: dict[WorkloadId, str] = {}
    async for row in table.select(f"workload_id IN ({placeholders})", workload_ids):
        namespace = workload_spec_from_row(row).get("namespace")
        if namespace:
            result[WorkloadId(as_int(row["workload_id"]))] = namespace
    return result


async def _connection_display_name_candidates(repo: DedupRepo) -> list[str]:
    """Every link name in the repository (``vault_link_key`` rows, or the
    object-storage ``link/`` directory), for ``_connection_display_name``
    to match against."""
    if repo.layout.kind is RepoKind.VAULT:
        try:
            conn = await repo.db("vault_link_key")
            cursor = await conn.execute("SELECT key FROM vault_link_key")
            return [row[0] for row in await cursor.fetchall()]
        except NotFoundError:
            return []
    if repo.layout.key_root is not None:
        try:
            return await list_names(repo.store, f"{repo.layout.key_root}/link")
        except NotFoundError:
            return []
    return []


def _connection_display_name(candidates: list[str], connection_id: str) -> str:
    """The name part of ``connection_id``'s ``<connection_id>_<x>_<name>``
    entry in ``candidates``, else ``connection_id`` itself."""
    prefix = f"{connection_id}_"
    match = next((c for c in candidates if c.startswith(prefix)), None)
    if match is None:
        return connection_id
    parts = match.split("_", 2)
    return parts[2] if len(parts) >= 3 else connection_id
