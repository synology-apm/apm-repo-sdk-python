"""Embedded ``ObjectDB``: ``object_id -> (offset, length)`` inside a
``saas_obj``. It has no ``file_map`` path of its own: it is a small
SQLite file embedded in the stream's dedup content.

It is located only by an ``object_db_id`` string
(``"<streamUuid>_<offset>_<length>"``), normally read from the version's
object-name index (``object_name_index.py``); a caller may also pass one
by hand (the CLI's ``--object-db-id``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Self, override

from ..._util.closing import AsyncClosing
from ...dedup.dedup_file import DedupFile
from ...errors import DataCorruptError, NotFoundError
from ...storage.sqlite_source import SqliteSource, close_on_error
from ...storage.table import Column, Table, as_int, as_str, sql_placeholders

_OBJECT_TABLE_COLUMNS = [Column("object_id"), Column("offset"), Column("length")]

#: Fallback ceiling on an embedded ObjectDB slice's decompressed size,
#: used only when a zstd frame declares none. A real ObjectDB is never
#: zstd-framed, so this only bounds a corrupt or hostile payload; a
#: generous, unmeasured bound for a small SQLite file.
_MAX_OBJECT_DB_DECOMPRESS_SIZE_FALLBACK = 64 << 20  # 64 MiB


def parse_object_db_id(object_db_id: str) -> tuple[str, int, int]:
    """Parse an ``object_db_id`` string (``"<streamUuid>_<offset>_<length>"``)
    into ``(stream_uuid, offset, length)``.

    Raises:
        NotFoundError: ``object_db_id`` isn't shaped like
            ``<text>_<int>_<int>``, each ``<int>`` ASCII digits only.
    """
    # From the right, in case stream_uuid contains an underscore.
    parts = object_db_id.rsplit("_", 2)
    if len(parts) != 3 or not all(part.isascii() and part.isdigit() for part in parts[1:]):
        raise NotFoundError(
            f"malformed --object-db-id {object_db_id!r} — expected '<streamUuid>_<offset>_<length>'",
            ref=object_db_id,
        )
    stream_uuid, offset_s, length_s = parts
    return stream_uuid, int(offset_s), int(length_s)


def name_object_id_pairs(items: object) -> list[tuple[str, str]]:
    """The ``(name, object_id)`` pairs in a parsed ``db_objects`` JSON
    array (``[{"name": str, "object_id": str}, ...]``), silently dropping
    malformed entries; ``[]`` when ``items`` isn't a list."""
    if not isinstance(items, list):
        return []
    return [
        (item["name"], item["object_id"])
        for item in items
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("object_id"), str)
    ]


class ObjectDb(AsyncClosing):
    """``object_id -> (offset, length)`` lookup backed by one embedded
    ObjectDB slice, materialized via ``SqliteSource``. Build one with
    ``from_bytes`` or ``load``."""

    #: Populated by ``from_bytes``.
    _source: SqliteSource
    _table: Table

    @classmethod
    async def from_bytes(cls, data: bytes | bytearray) -> Self:
        """Open ``data`` (one ObjectDB slice) as SQLite.

        Raises:
            DataCorruptError: ``data`` doesn't decode or has no valid
                ``object_table``.
            sqlite3.DatabaseError: ``data`` is not SQLite.
            ResourceLimitExceededError: Its temporary copy doesn't fit in
                free disk space with the reserve left free.
        """
        self = cls()
        self._source, _envelopes = await SqliteSource.from_enveloped_bytes(
            data, max_output_size=_MAX_OBJECT_DB_DECOMPRESS_SIZE_FALLBACK, what="embedded ObjectDB slice"
        )
        # A stale index entry can point at real SQLite without an
        # object_table; the source must not leak then.
        async with close_on_error(self._source):
            self._table = await Table.create(
                self._source.connection, "object_table", _OBJECT_TABLE_COLUMNS, index_hints=[["object_id"]]
            )
        return self

    @classmethod
    async def load(cls, dedup_file: DedupFile, offset: int, length: int) -> Self:
        return await cls.from_bytes(await dedup_file.read(offset, length))

    @override
    async def close(self) -> None:
        await self._source.close()

    async def get(self, object_id: str) -> tuple[int, int]:
        row = await self._table.select_one("object_id = ?", (object_id,))
        if row is None:
            raise NotFoundError(f"no object_table row for object_id={object_id!r}")
        return as_int(row["offset"]), as_int(row["length"])

    async def get_many(self, object_ids: Sequence[str]) -> dict[str, tuple[int, int]]:
        """``object_id -> (offset, length)`` for every id in ``object_ids``
        that has a row, in one query; ids without a row are left out."""
        if not object_ids:
            return {}
        return {
            as_str(row["object_id"]): (as_int(row["offset"]), as_int(row["length"]))
            async for row in self._table.select(f"object_id IN ({sql_placeholders(len(object_ids))})", object_ids)
        }

    async def object_map(self) -> dict[str, tuple[int, int]]:
        """The full ``object_id -> (offset, length)`` mapping, ordered by
        ``object_id`` so pagination over it is stable."""
        return {
            as_str(row["object_id"]): (as_int(row["offset"]), as_int(row["length"]))
            async for row in self._table.select(order_by="object_id")
        }


async def read_object(
    object_db: ObjectDb, dedup_file: DedupFile, object_id: str, *, expected_size: int | None = None
) -> bytes:
    """Read one object's bytes eagerly, for a caller that parses them
    (META JSON, mail fragments). Content handed back unmodified should
    stay a lazy ``dedup_file.view()`` instead.

    Raises:
        NotFoundError: ``object_id`` has no ``object_table`` row.
        DataCorruptError: ``expected_size`` is given and the read length
            differs.
    """
    offset, length = await object_db.get(object_id)
    data = await dedup_file.read(offset, length)
    if expected_size is not None and len(data) != expected_size:  # pragma: no cover - defensive
        raise DataCorruptError(
            f"object {object_id!r} read {len(data)} bytes, expected size={expected_size}", ref=object_id
        )
    # Small metadata objects, for parsers typed for bytes.
    return bytes(data)
