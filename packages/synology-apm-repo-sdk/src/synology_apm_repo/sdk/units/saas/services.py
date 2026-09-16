"""Content inspection for embedded ``saas_obj`` objects.

The SaaS application layer locates a service-level DB snapshot via the
connector's own object-name index (see ``object_name_index.py``) — a top-down
lookup into fixed, connector-written bookkeeping, never a guess. This
module's role is different: given an already-located object's raw
bytes, determine (``sniff``) or read (``inspect_object``) *what it is*
— the corruption check every object-name-index caller still wants (confirm
it decompresses, confirm it's really SQLite, confirm it defines the
expected table), never discovery of an unknown object's location; see
``provider.py``'s own module docstring for the canonical explanation of
why every provider here locates things by index lookup, never a scan.

Raw bytes classify into one of ``ServiceKind``'s members by a fixed
prefix rule — see that class's own docstring. (Real ``SnapshotDB``
reassembly is unavailable offline; see ``objectdb.py``'s own module
docstring.)
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import json

import zstandard

from ...dedup.dedup_file import DedupFile
from ...errors import DataCorruptError, KeyRequiredError
from ...storage.sqlite_source import Envelope, SqliteSource, is_zstd_frame, peel
from .objectdb import name_object_id_pairs

_SQLITE_MAGIC = b"SQLite format 3\x00"
_RFC822_MARKERS = (b"Received:", b"From:", b"To:", b"MIME-Version:", b"Return-Path:", b"Date:", b"Subject:")
_RFC822_SEARCH_WINDOW = 512

# A decompressed payload past this cap is treated as a false-positive
# zstd-magic match rather than materialized in full — a sniffing-only
# backstop against genuinely pathological input, not a real ceiling on
# service DB size: a healthy service DB with many large attachments can
# legitimately decompress into the tens of MiB. Content reads that
# already know they want a specific, already-identified object
# (``decompress_service_db``) don't use this cap at all; this one only
# ever gates *sniffing* an object nobody has confirmed the identity of
# yet.
_MAX_SNIFF_DECOMPRESS = 128 << 20

# Objects at or under this size get their zstd-magic checked against a
# small head read first; only a genuine match triggers a second, full
# read (``inspect_object``) — never for large objects generically.
# Magic-gated, not a blind size cutoff,
# so a large real service DB (see ``_MAX_SNIFF_DECOMPRESS``'s own
# comment) still gets classified correctly rather than misreported as
# ``ServiceKind.BINARY``.
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


@dataclasses.dataclass(frozen=True)
class IndexEntry:
    """One entry in a connector's own index object: a display name paired
    with the backing object id."""

    name: str
    object_id: str


@dataclasses.dataclass(frozen=True)
class SniffResult:
    """What sniffing a service-DB/index object's bytes found: its
    ``ServiceKind``, plus whichever of ``tables``/``service_name``/
    ``index_entries`` that kind actually carries."""

    kind: ServiceKind
    tables: frozenset[str] = frozenset()
    service_name: str | None = None
    index_entries: tuple[IndexEntry, ...] = ()


async def decompress_service_db(data: bytes) -> bytes:
    """Decompress one service-level DB snapshot's raw bytes to plain
    SQLite bytes — the half of ``open_service_db`` that callers holding
    bytes directly need (``TeamsChatProvider``'s several short-lived
    connections).

    Deliberately unbounded, unlike ``sniff``'s capped speculative
    read: content here is already identified via the object-name index, so
    a resolved-but-wrong location is caught by the schema check every
    caller already does afterward, not by refusing to decompress
    upfront. Also deliberately unbounded in *size* -- large real service
    DBs are expected (see ``_MAX_SNIFF_DECOMPRESS``'s own comment) --
    which is exactly why this is ``async`` and hops to a real OS thread
    for the decrypt+decompress itself: peel() stays synchronous by
    design (storage/sqlite_source.py's own TestPeel docstring), but
    leaving a large decompress on the event loop would stall every
    other Task for its duration, the same reasoning dedup/pool.py's own
    stated policy already covers.

    Raises:
        DataCorruptError: ``data`` isn't ZSTD-framed SQLite.
    """
    try:
        payload, envelopes = await asyncio.to_thread(peel, data)
    except (zstandard.ZstdError, KeyRequiredError) as exc:
        raise DataCorruptError(f"service DB blob failed to decompress: {exc}") from exc
    if Envelope.ZSTD not in envelopes:
        raise DataCorruptError("service DB blob is not ZSTD-framed")
    if payload[: len(_SQLITE_MAGIC)] != _SQLITE_MAGIC:
        raise DataCorruptError("decompressed service DB blob is not a SQLite file")
    return payload


async def open_service_db(data: bytes) -> SqliteSource:
    """Decompress and open one service-level DB snapshot's raw bytes as a
    live, queryable connection — the counterpart to ``sniff`` for
    callers (the application-layer providers) that need to actually run
    queries against the DB, not just inspect it."""
    return await SqliteSource.from_bytes(await decompress_service_db(data))


async def _table_names(sqlite_bytes: bytes) -> frozenset[str]:
    async with await SqliteSource.from_bytes(sqlite_bytes) as source:
        cursor = await source.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        rows = await cursor.fetchall()
    return frozenset(str(row[0]) for row in rows)


def _service_name_for(tables: frozenset[str]) -> str | None:
    for table in tables:
        hint = _SERVICE_TABLE_HINTS.get(table)
        if hint is not None:
            return hint
    return None


def _index_entries(parsed: object) -> tuple[IndexEntry, ...] | None:
    """Extract the INDEX shape's entries — the real GWS connector encodes
    this as either a ``db_objects`` array of ``name``/``object_id``
    pairs, or a ``db_infos_in_snapshot`` indirection name for the "too
    long to inline" case. ``None`` when ``parsed`` matches neither."""
    if not isinstance(parsed, dict):  # pragma: no cover - sniff() only calls this after a "{"-prefixed parse
        return None
    entries = [
        IndexEntry(name=name, object_id=object_id) for name, object_id in name_object_id_pairs(parsed.get("db_objects"))
    ]
    if entries:
        return tuple(entries)
    object_id = parsed.get("object_id")
    if parsed.get("name") == "db_infos_in_snapshot" and isinstance(object_id, str):
        return (IndexEntry(name="db_infos_in_snapshot", object_id=object_id),)
    return None


async def sniff(data: bytes) -> SniffResult:
    """Classify one object's raw bytes into one of ``ServiceKind``'s
    shapes. Pure function of ``data`` — touches no repository, no
    store, no ``DedupFile`` — so it's cheap to unit test with synthetic
    bytes and is reused as-is by ``inspect_object``; it is ``async``
    because the ``SERVICE_DB`` branch opens the decompressed payload as
    real SQLite to read its table names (that's I/O), and because the
    speculative decompress attempt below hops to a real OS thread
    rather than blocking the event loop — up to ``_MAX_SNIFF_DECOMPRESS``
    (128 MiB) of synchronous work otherwise, on data this function
    hasn't even confirmed is real SQLite yet. Never raises: a ``peel``
    failure here means "not actually zstd-framed", not "sniff itself
    failed"."""
    try:
        payload, envelopes = await asyncio.to_thread(peel, data, max_zstd_output_size=_MAX_SNIFF_DECOMPRESS)
    except (zstandard.ZstdError, KeyRequiredError):
        envelopes = []
    # peel() already returns data unchanged, no envelopes stripped, when
    # the first 4 bytes aren't zstd magic, so this covers "wasn't
    # zstd-framed at all" without a second, independent magic check.
    if Envelope.ZSTD in envelopes:
        if payload[: len(_SQLITE_MAGIC)] == _SQLITE_MAGIC:
            tables = await _table_names(payload)
            return SniffResult(kind=ServiceKind.SERVICE_DB, tables=tables, service_name=_service_name_for(tables))
        return SniffResult(kind=ServiceKind.BINARY)

    if data.lstrip()[:1] == b"{":
        try:
            parsed = json.loads(data)
        except (ValueError, UnicodeDecodeError):
            pass
        else:
            entries = _index_entries(parsed)
            if entries is not None:
                return SniffResult(kind=ServiceKind.INDEX, index_entries=entries)
            return SniffResult(kind=ServiceKind.META_JSON)

    window = data[:_RFC822_SEARCH_WINDOW]
    if any(window.startswith(marker) or marker in window for marker in _RFC822_MARKERS):
        # Real reassembly finds a skeleton via mail_table.meta_object_id's
        # own content_list, never by sniffing headers — this branch exists
        # for generic/unknown-object inspection only, not the real restore
        # path.
        return SniffResult(kind=ServiceKind.MAIL_SKELETON)

    return SniffResult(kind=ServiceKind.BINARY)


async def inspect_object(dedup_file: DedupFile, offset: int, length: int) -> SniffResult:
    """Read just enough of an already-located object's bytes to
    classify (``sniff``) and validate it — never a step in
    *finding* the object: ``offset``/``length`` always come from the
    connector's own object-name index or from an already-resolved INDEX
    object's own entries. Applies ``_FULL_READ_CAP``
    (magic-byte-gated, not a blind size cutoff — see that constant's
    own comment)."""
    if length <= _FULL_READ_CAP:
        return await sniff(await dedup_file.read(offset, length))
    head = await dedup_file.read(offset, _HEAD_SIZE)
    if not is_zstd_frame(head):
        return await sniff(head)
    return await sniff(await dedup_file.read(offset, length))
