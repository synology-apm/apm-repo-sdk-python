"""Resolves a per-version, connector-recorded index from
``copy_target_version.version_spec``, plus the shared lookup helpers
(``resolve_service_db``, ``read_indexed_table``, ``read_id_to_name_map``,
``read_grouped_names``) every SaaS provider's own secondary-table
lookups build on.

The connector records, at backup-completion time, exactly which object
holds each named service DB and exactly which physical location
(``<stream_uuid>_<offset>_<length>``, the shape ``parse_object_db_id``
parses) holds all of them — a lookup into fixed, connector-written
bookkeeping, never a schema-scanning guess (see ``provider.py``'s own
module docstring for why no scan fallback exists anywhere behind it).
``version_spec`` may be vault-key encrypted — see
``catalog.version.parse_version_spec``, the shared decrypt-then-parse
entry point for this column. Every function here resolves to ``None``
rather than raising when the index simply isn't recorded; a resolved
index whose own *content* later fails to validate is a distinct case,
surfaced by ``provider.py``'s own ``_open_table_via_index``, not here.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from collections.abc import Awaitable, Callable
from typing import TypeVar

import aiosqlite

from ...catalog.version import Version, parse_version_spec
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, NotFoundError
from ...storage.sqlite_source import SqliteSource
from ...storage.table import Column, Table
from .objectdb import ObjectDb, name_object_id_pairs, parse_object_db_id
from .services import open_service_db

_VERSION_SPEC_COLUMNS = [Column("version_uid"), Column("version_spec")]


@dataclasses.dataclass(frozen=True)
class ObjectNameIndex:
    """One version's connector-recorded map of indexed db name ->
    object_id (``object_ids``), plus exactly which ``ObjectDbCandidate``
    (``offset``/``length``) holds all of them — parsed straight from
    ``object_db_id``, the authoritative physical location; nothing
    downstream of this ever needs to scan for it."""

    stream_uuid: str
    offset: int
    length: int
    object_ids: dict[str, str]


async def resolve_object_name_index(repo: DedupRepo, version: Version) -> ObjectNameIndex | None:
    """``None`` on any failure to *locate* the index — no
    ``copy_target_version`` table, no row for this version,
    unparseable/undecryptable ``version_spec``, or a missing/malformed
    ``additional_meta``: all shapes of "no index recorded here" (an
    old repository, or a connector version predating this
    bookkeeping), never corruption. Never raises for those; a resolved
    index whose *content* later fails to validate is a distinct case,
    handled by this module's callers, not here."""
    try:
        db = await repo.db("copy_target_version")
    except NotFoundError:
        # No index at all in this repository (every unit-test fixture
        # repository built from scratch, and presumably some genuinely old
        # real repositories) — nothing to resolve.
        return None
    table = await Table.create(db, "copy_target_version", _VERSION_SPEC_COLUMNS)
    row = await table.select_one("version_uid = ?", (version.version_uid,))
    if row is None:
        return None
    raw = str(row["version_spec"])

    spec = parse_version_spec(raw, version.version_uid, repo.vault_key)
    if not isinstance(spec, dict):
        return None

    additional_meta_raw = (spec.get("status") or {}).get("additional_meta")
    if not additional_meta_raw:
        return None
    try:
        additional_meta = json.loads(additional_meta_raw)
    except json.JSONDecodeError:
        return None

    object_db_id = additional_meta.get("object_db_id")
    db_objects = (additional_meta.get("db_object_ids") or {}).get("db_objects")
    if not object_db_id or not isinstance(db_objects, list):
        return None
    try:
        stream_uuid, offset, length = parse_object_db_id(object_db_id)
    except NotFoundError:
        return None

    object_ids = dict(name_object_id_pairs(db_objects))
    if not object_ids:
        return None
    return ObjectNameIndex(stream_uuid=stream_uuid, offset=offset, length=length, object_ids=object_ids)


_T = TypeVar("_T")


async def resolve_service_db(
    dedup_file: DedupFile,
    object_db: ObjectDb,
    object_name_index: ObjectNameIndex,
    object_names: tuple[str, ...],
    table_name: str,
) -> SqliteSource | None:
    """The shared "no-scan" resolution loop behind both
    ``SaasWorkloadProvider._open_table_via_index`` (which keeps the
    returned ``SqliteSource`` open long-term) and ``read_indexed_table``
    (which reads it once and closes it immediately) — only how each
    caller disposes of a successful result differs, not how one is
    found. Tries every
    ``object_names`` alias against ``object_db`` (already loaded at
    ``object_name_index``'s own location) in order, opening and returning
    the first whose resolved object both exists and actually defines
    ``table_name``. Returns ``None``, closing nothing, when no alias
    resolves."""
    for catalog_name in object_names:
        object_id = object_name_index.object_ids.get(catalog_name)
        if object_id is None:
            continue
        try:
            offset, length = await object_db.get(object_id)
        except NotFoundError:
            continue  # this alias isn't present at this location — try the next one
        try:
            source = await open_service_db(await dedup_file.read(offset, length))
        except (sqlite3.DatabaseError, DataCorruptError):
            # Real corruption at an index-authoritative location. Tries
            # the next alias, if any, rather than failing this table_name
            # immediately: aliases are mutually-exclusive product
            # variants, not scan guesses, so it remains possible in
            # principle for one alias's slot to be bad while a different
            # alias for the same table_name is fine.
            continue
        cursor = await source.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
        )
        if await cursor.fetchone() is None:
            await source.close()
            continue  # right db, wrong table for this alias — try the next one
        return source
    return None


async def read_indexed_table(
    dedup_file: DedupFile,
    object_name_index: ObjectNameIndex | None,
    object_names: tuple[str, ...],
    table_name: str,
    reader: Callable[[aiosqlite.Connection], Awaitable[_T]],
) -> _T | None:
    """Best-effort secondary-table lookup behind the object-name index
    (M365's ``mail_folder_table``/``contact_folder_table``, GWS's own
    label/group name tables — see ``mail.py``/``contact.py``), as
    opposed to ``SaasWorkloadProvider.create``'s own primary-table
    resolution, which this doesn't replace. Hands ``reader`` the live
    connection ``resolve_service_db`` resolves.

    ``None`` on ``object_name_index`` being ``None``, every name missing,
    or any read/decompress/validate failure — this is enrichment (a
    nicer display name, an extra attrs field), never required content,
    so a caller degrades to showing less rather than failing.

    By design, no schema-only scan: the same table name can legitimately
    mean two different real tables depending on which db holds it (see
    ``mail.py``'s own ``_gws_mail_labels``), so only the index's own
    naming can tell them apart."""
    if object_name_index is None:
        return None
    try:
        # object_name_index.offset/.length name one physical location that
        # holds every named object for this version, regardless of which
        # alias resolve_service_db() tries turns out to be the real one.
        object_db = await ObjectDb.load(dedup_file, object_name_index.offset, object_name_index.length)
    except (sqlite3.DatabaseError, DataCorruptError):
        return None  # candidate location isn't a valid ObjectDb at all
    try:
        source = await resolve_service_db(dedup_file, object_db, object_name_index, object_names, table_name)
        if source is None:
            return None
        try:
            try:
                return await reader(source.connection)
            except (sqlite3.DatabaseError, DataCorruptError):
                # A schema-drifted table (reader's Table.create finding
                # a required column missing) is exactly the same "no
                # index recorded here" shape this function's own
                # docstring promises to degrade on, not a reason to
                # crash the caller (or, worse, a sibling SaaS provider
                # sharing the same version — see units/dispatch.py).
                return None
        finally:
            await source.close()
    finally:
        await object_db.close()  # only the raw bytes are needed past this point


async def read_id_to_name_map(
    dedup_file: DedupFile,
    object_name_index: ObjectNameIndex | None,
    object_names: tuple[str, ...],
    table_name: str,
    *,
    id_column: str,
    name_column: str,
) -> dict[str, str] | None:
    """``read_indexed_table`` specialized for the "id -> display
    name" shape every folder/label/group *definitions* table in this
    project shares (``mail_folder_table``, ``contact_folder_table``,
    ``mail_label_table``'s definitions half, ``group_table``, ...) —
    ``table_name``'s own ``id_column``/``name_column`` values, keyed by
    ``str(id_column value)``."""

    async def _reader(connection: aiosqlite.Connection) -> dict[str, str]:
        table = await Table.create(connection, table_name, [Column(id_column), Column(name_column)])
        return {str(row[id_column]): str(row[name_column]) async for row in table.select()}

    return await read_indexed_table(dedup_file, object_name_index, object_names, table_name, _reader)


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
) -> dict[str, list[str]] | None:
    """Best-effort ``item_id -> [real group/label names]`` map, built
    from a *definitions* table (``id_column -> name_column``) plus a
    *membership* table (``item_column``, ``group_column``) — the shape
    GWS's own many-to-many label/group mechanisms share (``mail.py``'s
    labels, ``contact.py``'s groups). ``None`` if either half is
    unavailable — a caller degrades to showing no labels/groups at all,
    never a wrong or incomplete join."""
    names = await read_id_to_name_map(
        dedup_file, object_name_index, definition_names, definition_table, id_column=id_column, name_column=name_column
    )
    if not names:
        return None

    async def _membership_reader(connection: aiosqlite.Connection) -> list[tuple[str, str]]:
        table = await Table.create(connection, membership_table, [Column(item_column), Column(group_column)])
        return [(str(row[item_column]), str(row[group_column])) async for row in table.select()]

    membership = await read_indexed_table(
        dedup_file, object_name_index, membership_names, membership_table, _membership_reader
    )
    if membership is None:
        return None
    grouped: dict[str, list[str]] = {}
    for item_id, group_id in membership:
        grouped.setdefault(item_id, []).append(names.get(group_id, group_id))
    return grouped
