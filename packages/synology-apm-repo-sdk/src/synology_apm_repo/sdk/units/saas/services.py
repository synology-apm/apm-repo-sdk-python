"""Content inspection for embedded ``saas_obj`` objects already located
through the object-name index: ``open_service_db`` opens a service DB
snapshot, ``sniff``/``inspect_object`` classify an object's bytes. Never
used to discover where an object is.

Bytes classify into ``ServiceKind`` by a fixed rule: ZSTD-framed SQLite,
then a JSON object, then an RFC822 header, else binary.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
from typing import Any

import zstandard

from ..._util.jsonparse import try_parse_json_object
from ...dedup.dedup_file import DedupFile
from ...errors import DataCorruptError, KeyRequiredError
from ...format.compression import is_zstd_frame, zstd_content_size
from ...storage.sqlite_source import Envelope, SqliteSource, close_on_error, peel
from .objectdb import name_object_id_pairs

_SQLITE_MAGIC = b"SQLite format 3\x00"
_RFC822_MARKERS = (b"Received:", b"From:", b"To:", b"MIME-Version:", b"Return-Path:", b"Date:", b"Subject:")
_RFC822_SEARCH_WINDOW = 512

# Sniffing only: a candidate decompressing past this is unclassifiable
# rather than materialized. Not a ceiling on service DB size
# (open_service_db has none).
_MAX_SNIFF_DECOMPRESS = 128 << 20

# inspect_object reads an object up to this size whole; a larger one is
# read whole only when its head is zstd-framed (a possible service DB),
# otherwise just its first _HEAD_SIZE bytes are sniffed.
_FULL_READ_CAP = 8 << 20
_HEAD_SIZE = 4096

_SERVICE_TABLE_HINTS: dict[str, str] = {
    "mail_table": "mail",
    "item_table": "drive",
    "contact_table": "contact",
    "calendar_event_table": "calendar",
    "calendar_table": "calendar",
    "item_version_table": "site",
    "list_version_table": "site",
    "msg_info_table": "teams_chat",
    "channel_info_table": "teams_channel",
}


class ServiceKind(enum.Enum):
    """What one already-located object's raw bytes turn out to be
    (``sniff``'s classification).

    - ``SERVICE_DB``: ZSTD -> SQLite; the owning app is guessed from
      its ``sqlite_master`` table names (``_SERVICE_TABLE_HINTS``).
    - ``INDEX``: JSON matching the ``db_objects``/
      ``db_infos_in_snapshot`` shape (``_index_entries``) — extracted
      as ``IndexEntry`` tuples.
    - ``META_JSON``: any other JSON object (content-list fragments,
      Contact/Calendar client metadata, search-index documents).
    - ``MAIL_SKELETON``: an RFC822 header in the first few hundred
      bytes.
    - ``BINARY``: anything else — the common case for real Drive
      content."""

    INDEX = "index"
    SERVICE_DB = "service_db"
    META_JSON = "meta_json"
    MAIL_SKELETON = "mail_skeleton"
    BINARY = "binary"


@dataclasses.dataclass(frozen=True, slots=True)
class IndexEntry:
    """One entry in a connector's own index object: a display name paired
    with the backing object id."""

    name: str
    object_id: str


@dataclasses.dataclass(frozen=True, slots=True)
class SniffResult:
    """What sniffing a service-DB/index object's bytes found: its
    ``ServiceKind``, plus whichever of ``tables``/``service_name``/
    ``index_entries`` that kind actually carries."""

    kind: ServiceKind
    tables: frozenset[str] = frozenset()
    service_name: str | None = None
    index_entries: tuple[IndexEntry, ...] = ()


def _read_head(path: str, n: int) -> bytes:
    with open(path, "rb") as f:
        return f.read(n)


async def open_service_db(data: bytes | bytearray) -> SqliteSource:
    """Decompress one service-level DB snapshot's bytes into a temp file
    and open it as SQLite; the caller closes the returned source. No fixed
    size cap, since the object was already identified through the index; a
    frame declaring no size is bounded by the free disk space above the
    reserve.

    Raises:
        DataCorruptError: ``data`` isn't ZSTD-framed SQLite.
        ResourceLimitExceededError: The decompressed size doesn't fit
            in free disk space with the reserve left free.
    """
    source, envelopes = await SqliteSource.from_enveloped_bytes(data, max_output_size=None, what="service DB blob")
    async with close_on_error(source):
        if Envelope.ZSTD not in envelopes:
            raise DataCorruptError("service DB blob is not ZSTD-framed")
        assert source.path is not None  # from_enveloped_bytes always uses a temp file
        head = await asyncio.to_thread(_read_head, source.path, len(_SQLITE_MAGIC))
        if head != _SQLITE_MAGIC:
            raise DataCorruptError("decompressed service DB blob is not a SQLite file")
    return source


async def _table_names(sqlite_bytes: bytes | bytearray) -> frozenset[str]:
    async with await SqliteSource.from_bytes(sqlite_bytes) as source:
        cursor = await source.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        rows = await cursor.fetchall()
    return frozenset(str(row[0]) for row in rows)


def _service_name_for(tables: frozenset[str]) -> str | None:
    # Sorted: a DB holding tables for two hints must name the same service on every run.
    for table in sorted(tables):
        hint = _SERVICE_TABLE_HINTS.get(table)
        if hint is not None:
            return hint
    return None


def _index_entries(parsed: dict[str, Any]) -> tuple[IndexEntry, ...] | None:
    """The INDEX shape's entries: a ``db_objects`` array of
    ``name``/``object_id`` pairs, or a single ``db_infos_in_snapshot``
    indirection entry. ``None`` when ``parsed`` matches neither."""
    entries = [
        IndexEntry(name=name, object_id=object_id) for name, object_id in name_object_id_pairs(parsed.get("db_objects"))
    ]
    if entries:
        return tuple(entries)
    object_id = parsed.get("object_id")
    if parsed.get("name") == "db_infos_in_snapshot" and isinstance(object_id, str):
        return (IndexEntry(name="db_infos_in_snapshot", object_id=object_id),)
    return None


async def sniff(data: bytes | bytearray) -> SniffResult:
    """Classify one object's raw bytes into a ``ServiceKind``. Touches no
    repository; decompression runs off the event loop.

    A zstd frame that fails to decompress, or declares a size over
    ``_MAX_SNIFF_DECOMPRESS`` (checked before decompressing, since
    ``data`` is untrusted), is unclassifiable rather than an error, as is
    ``{``-prefixed data that doesn't parse as JSON (invalid, or nested
    too deep for the decoder).

    Raises:
        sqlite3.DatabaseError: The payload has the SQLite magic but its
            table list can't be read.
        ResourceLimitExceededError: The decompressed SQLite payload doesn't
            fit in free disk space with the reserve left free.
    """
    payload = data
    try:
        declared = zstd_content_size(data)
    except zstandard.ZstdError:
        declared = None
    if declared is not None and declared > _MAX_SNIFF_DECOMPRESS:
        envelopes: list[Envelope] = []
    else:
        try:
            payload, envelopes = await asyncio.to_thread(peel, data, max_zstd_output_size=_MAX_SNIFF_DECOMPRESS)
        except (zstandard.ZstdError, KeyRequiredError, DataCorruptError):
            envelopes = []
    if Envelope.ZSTD in envelopes:
        if payload[: len(_SQLITE_MAGIC)] == _SQLITE_MAGIC:
            tables = await _table_names(payload)
            return SniffResult(kind=ServiceKind.SERVICE_DB, tables=tables, service_name=_service_name_for(tables))
        return SniffResult(kind=ServiceKind.BINARY)

    parsed = try_parse_json_object(data) if data.lstrip()[:1] == b"{" else None
    if parsed is not None:
        entries = _index_entries(parsed)
        if entries is not None:
            return SniffResult(kind=ServiceKind.INDEX, index_entries=entries)
        return SniffResult(kind=ServiceKind.META_JSON)

    window = data[:_RFC822_SEARCH_WINDOW]
    if any(window.startswith(marker) or marker in window for marker in _RFC822_MARKERS):
        return SniffResult(kind=ServiceKind.MAIL_SKELETON)

    return SniffResult(kind=ServiceKind.BINARY)


async def inspect_object(dedup_file: DedupFile, offset: int, length: int) -> SniffResult:
    """``sniff`` an already-located object, reading only its head when
    it's over ``_FULL_READ_CAP`` and not zstd-framed.

    Raises:
        sqlite3.DatabaseError: As ``sniff``.
        ResourceLimitExceededError: As ``sniff``, or a zstd-framed object
            is over ``DedupFile.read()``'s single-read ceiling.
    """
    if length <= _FULL_READ_CAP:
        return await sniff(await dedup_file.read(offset, length))
    head = await dedup_file.read(offset, _HEAD_SIZE)
    if not is_zstd_frame(head):
        return await sniff(head)
    return await sniff(await dedup_file.read(offset, length))
