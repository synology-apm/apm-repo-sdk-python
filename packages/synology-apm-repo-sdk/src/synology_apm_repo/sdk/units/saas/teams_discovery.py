"""Teams/Chat service-DB discovery for ``TeamsChatProvider``: finding the
channel/chat container among a version's indexed objects, and naming each
channel (with its Standard/Private/Shared category) and each chat (from its
topic or its other members).

Unlike other SaaS workloads, the object-name index entry points at an INDEX
object listing one message DB per channel/chat plus the container, since
their number is only known at backup time (FORMAT-SPEC.md: Teams/Chat container discrimination).
"""

from __future__ import annotations

import enum
import sqlite3

from ..._util.jsonparse import try_parse_json_array
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import NotFoundError
from ...storage.sqlite_source import SqliteSource
from ...storage.table import Column, Table, as_int
from .object_name_index import DEGRADABLE_OPEN_ERRORS, ObjectNameIndex
from .objectdb import ObjectDb
from .services import (
    ServiceKind,
    inspect_object,
)
from .workload_helpers import owning_account_user_info

# A DB holding either table is the channel (Teams) or chat (Chat) list
# container rather than a per-entity message DB.
_CONTAINER_TABLE_NAMES = frozenset({"channel_info_table", "chat_info_table"})

# INDEX entry names of the container; every other entry is named by its
# channel/chat id.
_CONTAINER_DB_NAMES = frozenset({"teams_channel_db", "chat_db"})

_CHANNEL_COLUMNS = [
    Column("channel_id"),
    Column("name", required=False),
    Column("channel_type", required=False),
    Column("create_time", required=False),
]

#: ``channel_type`` values (Microsoft Graph's ``membershipType``), each a
#: tree category; an unrecognized or missing value is Standard.
CHANNEL_CATEGORY_STANDARD = "standard"
_CHANNEL_CATEGORY_PRIVATE = "private"
_CHANNEL_CATEGORY_SHARED = "shared"
_CHANNEL_CATEGORIES = (CHANNEL_CATEGORY_STANDARD, _CHANNEL_CATEGORY_PRIVATE, _CHANNEL_CATEGORY_SHARED)
CHANNEL_CATEGORY_LABELS = {
    CHANNEL_CATEGORY_STANDARD: "Standard Channels",
    _CHANNEL_CATEGORY_PRIVATE: "Private Channels",
    _CHANNEL_CATEGORY_SHARED: "Shared Channels",
}

# Beside chat_info_table: one row per chat_id with a JSON array of member
# objects (``userEmail``, ``display_name``, ...), used to name a chat
# without a topic.
_CHAT_MEMBERS_TABLE = "chat_members_table"
_CHAT_MEMBERS_COLUMNS = [Column("chat_id"), Column("members")]


class _ChatType(enum.IntEnum):
    """The ``chatType`` values (Microsoft Graph's enum) this module
    branches on."""

    ONE_ON_ONE = 0
    MEETING = 2


# A MEETING chat without a subject, as Teams labels it; never named from
# its attendees.
_NO_TITLE_MEETING_LABEL = "(no title)"
# A ONE_ON_ONE chat whose member list has no one but the account itself
# (typically the other side is a bot), as Teams labels it.
_BOT_CHAT_LABEL = "Bot"


async def _is_container(dedup_file: DedupFile, db: ObjectDb, object_id: str) -> frozenset[str] | None:
    """The table set of ``object_id``'s object if it is a channel/chat
    list container, else ``None``."""
    offset, length = await db.get(object_id)
    if length == 0:
        return None
    result = await inspect_object(dedup_file, offset, length)
    if result.kind is ServiceKind.SERVICE_DB and result.tables & _CONTAINER_TABLE_NAMES:
        return result.tables
    return None


async def resolve_message_index(
    dedup_file: DedupFile, object_name_index: ObjectNameIndex, object_db: ObjectDb
) -> tuple[frozenset[str], str, dict[str, str]] | None:
    """Locate Teams/Chat's channel/chat INDEX object through ``object_db``
    (the index's ObjectDB, borrowed) and validate its container entry.

    Returns:
        ``(container_tables, container_object_id, entity_object_ids)``, or
        ``None`` when the INDEX object or its container is missing or
        doesn't validate.
    """
    # The "db_infos_in_snapshot" entry's object is the INDEX object.
    object_id = object_name_index.object_ids.get("db_infos_in_snapshot")
    if object_id is None:
        return None
    try:
        offset, length = await object_db.get(object_id)
        result = await inspect_object(dedup_file, offset, length)
    except NotFoundError:
        return None
    except DEGRADABLE_OPEN_ERRORS:
        return None
    if result.kind is not ServiceKind.INDEX:
        return None
    container_object_id = next(
        (entry.object_id for entry in result.index_entries if entry.name in _CONTAINER_DB_NAMES), None
    )
    if container_object_id is None:
        return None
    try:
        container_tables = await _is_container(dedup_file, object_db, container_object_id)
    except NotFoundError:
        return None
    except DEGRADABLE_OPEN_ERRORS:
        return None
    if container_tables is None:
        return None
    entity_object_ids = {
        entry.name: entry.object_id for entry in result.index_entries if entry.object_id != container_object_id
    }
    return container_tables, container_object_id, entity_object_ids


async def channel_info(source: SqliteSource) -> tuple[dict[str, str], dict[str, str], dict[str, int]]:
    """``channel_id -> name``, ``channel_id -> category`` and
    ``channel_id -> create_time`` (epoch seconds) from ``source``'s
    ``channel_info_table``. Doesn't close ``source``.

    Raises:
        DataCorruptError: ``channel_info_table`` is missing or lacks
            ``channel_id``.
    """
    table = await Table.create(source.connection, "channel_info_table", _CHANNEL_COLUMNS)
    labels: dict[str, str] = {}
    categories: dict[str, str] = {}
    create_times: dict[str, int] = {}
    async for row in table.select():
        channel_id = str(row["channel_id"])
        if row.get("name"):
            labels[channel_id] = str(row["name"])
        channel_type = row.get("channel_type")
        categories[channel_id] = str(channel_type) if channel_type in _CHANNEL_CATEGORIES else CHANNEL_CATEGORY_STANDARD
        raw_create_time = row.get("create_time")
        if raw_create_time is not None:
            create_times[channel_id] = as_int(raw_create_time)
    return labels, categories, create_times


async def owning_account_email(repo: DedupRepo, version: Version) -> str | None:
    """The backed-up account's email (``owning_account_user_info``), or
    ``None``."""
    user_info = await owning_account_user_info(repo, version)
    return str(user_info["email"]) if user_info and user_info.get("email") else None


def _chat_display_name_from_members(members_json: object, self_email: str | None) -> str | None:
    """Every other member's ``display_name``, comma-joined, as Teams
    names a chat without a topic. ``None`` if ``members_json`` isn't a
    JSON array or no other member has a ``display_name``."""
    members = try_parse_json_array(members_json)
    if members is None:
        return None
    others = [
        str(member["display_name"])
        for member in members
        if isinstance(member, dict) and member.get("display_name") and member.get("userEmail") != self_email
    ]
    return ", ".join(others) if others else None


async def chat_labels(source: SqliteSource, self_email: str | None) -> tuple[dict[str, str], dict[str, int]]:
    """Best-effort ``chat_id -> label`` (the topic, else
    ``_NO_TITLE_MEETING_LABEL`` for a meeting, else the other members'
    names, else ``_BOT_CHAT_LABEL`` for a 1:1 chat) and ``chat_id ->
    create_time`` maps. ``chat_info_table``'s columns are looked up by
    name, not assumed. Doesn't close ``source``."""
    if not await Table.exists_in(source.connection, "chat_info_table"):
        return {}, {}
    cols = (await Table.create(source.connection, "chat_info_table", [])).columns_present
    id_col = next((c for c in cols if c.endswith("chat_id") or c == "id"), None)
    label_col = next((c for c in cols if c in ("topic", "name", "title")), None)
    type_col = "chat_type" if "chat_type" in cols else None
    create_time_col = "create_time" if "create_time" in cols else None
    if id_col is None:
        return {}, {}
    labels: dict[str, str] = {}
    chat_type_by_id: dict[str, int] = {}
    create_times: dict[str, int] = {}
    try:
        select_columns = [Column(id_col)]
        if label_col is not None:
            select_columns.append(Column(label_col))
        if type_col is not None:
            select_columns.append(Column(type_col))
        if create_time_col is not None:
            select_columns.append(Column(create_time_col))
        info_table = await Table.create(source.connection, "chat_info_table", select_columns)
        async for row in info_table.select():
            chat_id = str(row[id_col])
            chat_type = row.get(type_col) if type_col is not None else None
            if isinstance(chat_type, int):
                chat_type_by_id[chat_id] = chat_type
            if label_col is not None and row.get(label_col):
                labels[chat_id] = str(row[label_col])
            elif chat_type == _ChatType.MEETING:
                labels[chat_id] = _NO_TITLE_MEETING_LABEL
            raw_create_time = row.get(create_time_col) if create_time_col is not None else None
            if raw_create_time is not None:
                create_times[chat_id] = as_int(raw_create_time)
    except sqlite3.Error:
        return {}, {}

    if await Table.exists_in(source.connection, _CHAT_MEMBERS_TABLE):
        members_table = await Table.create(source.connection, _CHAT_MEMBERS_TABLE, _CHAT_MEMBERS_COLUMNS)
        async for row in members_table.select():
            chat_id = str(row["chat_id"])
            if chat_id in labels:
                continue
            name = _chat_display_name_from_members(row.get("members"), self_email)
            if name is None and chat_type_by_id.get(chat_id) == _ChatType.ONE_ON_ONE:
                labels[chat_id] = _BOT_CHAT_LABEL
                continue
            if name is not None:
                labels[chat_id] = name
    return labels, create_times
