"""Embedded ``ObjectDB``: ``object_id -> (offset, length)`` inside a
``saas_obj``. Unlike every other SQLite this project opens, an
``ObjectDB`` isn't addressed by a ``file_map`` path of its own — it's a
tiny SQLite file *embedded* directly in the stream's dedup content.

**Located exclusively via an ``object_db_id`` string**
(``"<streamUuid>_<offset>_<length>"``) — a direct ``DedupFile.view(offset,
length)``, no scanning of any kind. That string's original source is the
connector package's own ``SnapshotDB``
(``/<volume>/@ActiveBackup-GSuite/db/snapshot.sqlite`` and the M365
equivalent), which lives outside ``@ActiveProtectVault``/
``@ActiveProtectData`` and is never exported with a vault — but the
connector *also* copies it into the vault's own object-name index, at
backup-completion time, as part of ``copy_target_version``'s
``additional_meta`` (see ``object_name_index.py``). That copy is how every
application-layer provider — including ``RawObjectProvider`` — gets
this string automatically, offline, with no scanning at all; a caller
supplying one by hand (the CLI's ``--object-db-id`` escape hatch) is
the manual exception, not the normal path.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

from ...dedup.dedup_file import DedupFile
from ...errors import DataCorruptError, NotFoundError
from ...storage.sqlite_source import SqliteSource, peel
from ...storage.table import Column, Table, as_int, as_str

_OBJECT_TABLE_COLUMNS = [Column("object_id"), Column("offset"), Column("length")]


def parse_object_db_id(object_db_id: str) -> tuple[str, int, int]:
    """Parse an ``object_db_id`` string (``"<streamUuid>_<offset>_<length>"``)
    into ``(stream_uuid, offset, length)`` — the manual escape hatch for
    a caller that already knows the exact location and wants to hand it
    in directly rather than going through the object-name index.

    Raises:
        NotFoundError: ``object_db_id`` isn't shaped like
            ``<text>_<int>_<int>`` — the same category used elsewhere
            for an unresolvable location.
    """
    # Split from the right: offset/length are always the last two
    # ``_``-separated fields, so this stays correct even if stream_uuid
    # itself ever contained an underscore.
    parts = object_db_id.rsplit("_", 2)
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        raise NotFoundError(
            f"malformed --object-db-id {object_db_id!r} — expected '<streamUuid>_<offset>_<length>'",
            ref=object_db_id,
        )
    stream_uuid, offset_s, length_s = parts
    return stream_uuid, int(offset_s), int(length_s)


def name_object_id_pairs(items: object) -> list[tuple[str, str]]:
    """Filters ``items`` down to the ``(name, object_id)`` pairs it
    actually holds, silently dropping anything that doesn't match —
    ``items`` is expected to be a JSON array of ``{"name": str,
    "object_id": str}`` objects, the shape an object-name index's own
    ``db_objects`` array uses (``object_name_index.py`` and
    ``services.py``'s own INDEX-object sniffing both need this
    identical filter). Returns ``[]`` outright when ``items`` isn't
    even a list."""
    if not isinstance(items, list):
        return []
    return [
        (item["name"], item["object_id"])
        for item in items
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("object_id"), str)
    ]


class ObjectDb:
    """``object_id -> (offset, length)`` lookup backed by one embedded
    ObjectDB slice, materialized via ``SqliteSource``. The envelope
    chain here is always ``raw`` — ``peel`` is still run for
    uniformity, but is a no-op passthrough on real data."""

    #: Populated by ``from_bytes``, the only supported constructor —
    #: declared here so the type checker sees them without ``__init__``
    #: inventing placeholder values it would immediately overwrite.
    _source: SqliteSource
    _table: Table

    @classmethod
    async def from_bytes(cls, data: bytes) -> Self:
        """Materializing the slice as SQLite and introspecting
        ``object_table`` are both I/O, so construction is an async
        classmethod factory rather than ``__init__``."""
        self = cls()
        # Deliberately NOT hopped to asyncio.to_thread, unlike this
        # project's other peel() call sites: never a multi-MB payload
        # (a tiny embedded SQLite file, per the module docstring above),
        # matching dedup/pool/_bucket_reader.py's own read_chunk()
        # precedent ("one 4096-byte decode is far cheaper than a thread
        # round-trip"). Revisit only if a real embedded ObjectDB is ever
        # observed to be large.
        payload, _ = peel(data)
        self._source = await SqliteSource.from_bytes(payload)
        try:
            self._table = await Table.create(
                self._source.connection, "object_table", _OBJECT_TABLE_COLUMNS, index_hints=[["object_id"]]
            )
        except BaseException:
            # Load-bearing: a bad (offset, length) — a stale/corrupt
            # object-name index entry, not real SQLite or real SQLite with
            # no object_table — must not leak this open SqliteSource;
            # an unclosed aiosqlite connection hangs interpreter
            # shutdown (ARCHITECTURE.md's "Async-native, by
            # design"). Callers (e.g. provider.py's
            # _open_table_via_index) catch sqlite3.DatabaseError/
            # DataCorruptError around this constructor for exactly that reason.
            await self._source.close()
            raise
        return self

    @classmethod
    async def load(cls, dedup_file: DedupFile, offset: int, length: int) -> Self:
        return await cls.from_bytes(await dedup_file.read(offset, length))

    async def close(self) -> None:
        await self._source.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def get(self, object_id: str) -> tuple[int, int]:
        row = await self._table.select_one("object_id = ?", (object_id,))
        if row is None:
            raise NotFoundError(f"no object_table row for object_id={object_id!r}")
        return as_int(row["offset"]), as_int(row["length"])

    async def object_map(self) -> dict[str, tuple[int, int]]:
        """The full ``object_id -> (offset, length)`` mapping."""
        return {
            as_str(row["object_id"]): (as_int(row["offset"]), as_int(row["length"]))
            async for row in self._table.select()
        }


async def read_object(
    object_db: ObjectDb, dedup_file: DedupFile, object_id: str, *, expected_size: int | None = None
) -> bytes:
    """Read one object's bytes eagerly via ``object_db``'s
    ``object_id -> (offset, length)`` lookup — the shared shape behind
    every "read one already-located object, then parse/transform it"
    call site (``calendar.py``, ``contact.py``, ``site.py``'s META
    lookup, ``mail.py``'s fragment/META reads). Deliberately eager
    (``dedup_file.read()``, not ``.view()``): every one of those
    callers needs the bytes materialized in-process to parse JSON or
    decompress, unlike ``drive.py``/``site.py``'s content branch/
    ``raw_object.py``, which hand back the final, unmodified content as
    a lazy ``dedup_file.view()`` instead — those call sites must NOT be
    routed through this function, since that would force an eager read
    of what's meant to stay a lazy, possibly-large stream.

    ``expected_size``, when given, raises ``DataCorruptError`` if the read
    doesn't match it — the one extra check some callers (e.g. a
    mail fragment's own META-declared size) need that a plain
    metadata read doesn't; omit it (the default) to skip the check
    entirely."""
    offset, length = await object_db.get(object_id)
    data = await dedup_file.read(offset, length)
    if expected_size is not None and len(data) != expected_size:  # pragma: no cover - defensive
        raise DataCorruptError(
            f"object {object_id!r} read {len(data)} bytes, expected size={expected_size}", ref=object_id
        )
    return data
