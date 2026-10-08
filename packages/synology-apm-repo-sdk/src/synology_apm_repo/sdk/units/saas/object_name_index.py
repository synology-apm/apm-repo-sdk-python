"""The per-version object-name index the connector records in
``copy_target_version``'s ``additional_meta``: which object holds each
named service DB, and the ObjectDB location
(``<stream_uuid>_<offset>_<length>``) that maps them. Plus the shared
lookup helpers SaaS providers' secondary-table reads build on.

Every function here returns ``None`` rather than raising when the index
isn't recorded or a lookup can't be completed; a store failure still
raises.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from typing import NamedTuple

import aiosqlite

from ..._util.jsonparse import json_object
from ...catalog.version import Version, version_additional_meta
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, NotFoundError, ResourceLimitExceededError
from ...storage.sqlite_source import SqliteSource, close_on_error
from ...storage.table import Column, Table
from .objectdb import ObjectDb, name_object_id_pairs, parse_object_db_id
from .services import open_service_db

#: The errors meaning "this candidate isn't a safely-openable service
#: DB/ObjectDB", caught wherever a SaaS provider reads and opens one
#: speculatively: corruption, or a size over a safety ceiling or over the
#: free disk space above the reserve.
DEGRADABLE_OPEN_ERRORS: tuple[type[Exception], ...] = (
    sqlite3.DatabaseError,
    DataCorruptError,
    ResourceLimitExceededError,
)


@dataclasses.dataclass(frozen=True, slots=True)
class ObjectNameIndex:
    """One version's object-name index: indexed db name -> object_id
    (``object_ids``), resolved through the ObjectDB at ``offset``/``length``
    in stream ``stream_uuid``."""

    stream_uuid: str
    offset: int
    length: int
    object_ids: Mapping[str, str]


async def resolve_object_name_index(repo: DedupRepo, version: Version) -> ObjectNameIndex | None:
    """``version``'s object-name index, or ``None`` when none is recorded
    (no ``copy_target_version`` row, an unreadable ``version_spec``, or a
    missing/malformed ``additional_meta``, e.g. an older connector; a field
    of the wrong JSON type counts as missing).
    Doesn't validate the ObjectDB it points at."""
    additional_meta = await version_additional_meta(repo, version)
    if additional_meta is None:
        return None

    object_db_id = additional_meta.get("object_db_id")
    db_objects = json_object(additional_meta.get("db_object_ids")).get("db_objects")
    if not isinstance(object_db_id, str) or not object_db_id or not isinstance(db_objects, list):
        return None
    try:
        stream_uuid, offset, length = parse_object_db_id(object_db_id)
    except NotFoundError:
        return None

    object_ids = dict(name_object_id_pairs(db_objects))
    if not object_ids:
        return None
    return ObjectNameIndex(stream_uuid=stream_uuid, offset=offset, length=length, object_ids=object_ids)


class ResolvedServiceDb(NamedTuple):
    """A service DB ``resolve_service_db`` opened: the index ``object_id`` it
    came from, and its open ``source`` (the caller closes it)."""

    object_id: str
    source: SqliteSource


async def resolve_service_db(
    dedup_file: DedupFile,
    object_db: ObjectDb,
    object_name_index: ObjectNameIndex,
    object_names: tuple[str, ...],
    table_name: str,
) -> ResolvedServiceDb | None:
    """Open the service DB defining ``table_name``: tries each
    ``object_names`` alias in order against ``object_db`` (the index's
    loaded ObjectDB), returning the first that exists, opens, and defines
    ``table_name``; one whose DB can't be opened or whose schema can't be
    read (``DEGRADABLE_OPEN_ERRORS``) is skipped. ``None`` when no alias
    resolves."""
    for catalog_name in object_names:
        object_id = object_name_index.object_ids.get(catalog_name)
        if object_id is None:
            continue
        try:
            offset, length = await object_db.get(object_id)
        except NotFoundError:
            continue
        try:
            source = await open_service_db(await dedup_file.read(offset, length))
        except DEGRADABLE_OPEN_ERRORS:
            continue
        try:
            async with close_on_error(source):
                cursor = await source.connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
                )
                defines_table = await cursor.fetchone() is not None
        except sqlite3.DatabaseError:
            continue  # corrupt past its header; close_on_error closed it
        if not defines_table:
            await source.close()
            continue
        return ResolvedServiceDb(object_id, source)
    return None


async def read_indexed_table[T](
    dedup_file: DedupFile,
    object_name_index: ObjectNameIndex | None,
    object_names: tuple[str, ...],
    table_name: str,
    reader: Callable[[aiosqlite.Connection], Awaitable[T]],
    *,
    object_db: ObjectDb | None = None,
) -> T | None:
    """Best-effort one-shot read of a secondary table (folder, label and
    group name tables): opens it via ``resolve_service_db``, hands its
    connection to ``reader``, and closes it.

    ``None`` when there's no index, no alias resolves, or the read hits
    corruption or schema drift — callers treat the result as optional
    enrichment and show less without it.

    ``object_db`` is the index's ObjectDB if the caller already holds it;
    otherwise one is loaded and closed here."""
    if object_name_index is None:
        return None
    owned = object_db is None
    if object_db is None:
        try:
            object_db = await ObjectDb.load(dedup_file, object_name_index.offset, object_name_index.length)
        except DEGRADABLE_OPEN_ERRORS:
            return None
    try:
        resolved = await resolve_service_db(dedup_file, object_db, object_name_index, object_names, table_name)
        if resolved is None:
            return None
        source = resolved.source
        try:
            try:
                return await reader(source.connection)
            except (sqlite3.DatabaseError, DataCorruptError):
                # Schema drift (e.g. a required column missing).
                return None
        finally:
            await source.close()
    finally:
        if owned:
            await object_db.close()


async def read_id_to_name_map(
    dedup_file: DedupFile,
    object_name_index: ObjectNameIndex | None,
    object_names: tuple[str, ...],
    table_name: str,
    *,
    id_column: str,
    name_column: str,
    object_db: ObjectDb | None = None,
) -> dict[str, str] | None:
    """``read_indexed_table`` for a folder/label/group definitions table:
    ``{str(id_column): str(name_column)}``, or ``None``."""

    async def _reader(connection: aiosqlite.Connection) -> dict[str, str]:
        table = await Table.create(connection, table_name, [Column(id_column), Column(name_column)])
        return {str(row[id_column]): str(row[name_column]) async for row in table.select()}

    return await read_indexed_table(
        dedup_file, object_name_index, object_names, table_name, _reader, object_db=object_db
    )


async def read_grouped_names(
    dedup_file: DedupFile,
    object_name_index: ObjectNameIndex | None,
    *,
    definition_names: tuple[str, ...],
    definition_table: str,
    id_column: str,
    name_column: str,
    membership_names: tuple[str, ...],
    membership_table: str,
    item_column: str,
    group_column: str,
    object_db: ObjectDb | None = None,
    membership_connection: aiosqlite.Connection | None = None,
) -> dict[str, list[str]] | None:
    """Best-effort ``item_id -> [group/label names]`` map joining a
    definitions table (``id_column -> name_column``) with a membership
    table (``item_column``, ``group_column``) — GWS's mail labels and
    contact groups. ``None`` if either half is unavailable; a group id
    with no definition shows as the raw id.

    ``membership_connection``, the ``membership_names`` DB when the caller
    already holds it open, is queried in place of opening it again, if it
    defines ``membership_table``."""
    names = await read_id_to_name_map(
        dedup_file,
        object_name_index,
        definition_names,
        definition_table,
        id_column=id_column,
        name_column=name_column,
        object_db=object_db,
    )
    if not names:
        return None

    async def _membership_reader(connection: aiosqlite.Connection) -> list[tuple[str, str]]:
        table = await Table.create(connection, membership_table, [Column(item_column), Column(group_column)])
        return [(str(row[item_column]), str(row[group_column])) async for row in table.select()]

    membership: list[tuple[str, str]] | None
    if membership_connection is not None and await Table.exists_in(membership_connection, membership_table):
        try:
            membership = await _membership_reader(membership_connection)
        except (sqlite3.DatabaseError, DataCorruptError):
            membership = None  # schema drift, as read_indexed_table treats it
    else:
        membership = await read_indexed_table(
            dedup_file, object_name_index, membership_names, membership_table, _membership_reader, object_db=object_db
        )
    if membership is None:
        return None
    grouped: dict[str, list[str]] = {}
    for item_id, group_id in membership:
        grouped.setdefault(item_id, []).append(names.get(group_id, group_id))
    return grouped
