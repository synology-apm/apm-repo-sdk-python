"""``TeamsChatProvider``: M365 Teams channels + 1:1/group chats.

``render_channel_html`` renders each channel/chat as one self-contained
HTML page — the unit's only exported form. The raw message DB is
still reachable via ``RawObjectProvider`` (the CLI's ``--verbose`` / TUI's
diagnostic mode), listed like any other unrecognized SaaS object; this
makes the provider's own tree unusually shallow, one flat level instead
of two.

Unlike every other SaaS provider here, Teams and Chat locate their
service-level DBs via one shared discovery mechanism — see
``_resolve_message_index``'s own docstring for why. Chat's own rendering
path is implemented generically from the docs' description of that
shared mechanism (FORMAT-SPEC.md: teams-chat-containers) rather than
from an observed instance.
"""

from __future__ import annotations

import enum
import json
import sqlite3
from types import TracebackType
from typing import Self

import aiosqlite

from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, NotFoundError, UnsupportedDataFormatError
from ...storage.sqlite_source import SqliteSource
from ...storage.table import Column, Table
from ..base import Node, RestorableUnit, UnitKind, not_restorable, paginate
from ..content.saas_artifact import LazyArtifact
from ..content.saas_teams_chat import render_channel_html
from ..node_ref import NodeRef, canonical_ref_for
from .object_name_index import ObjectNameIndex, resolve_object_name_index
from .objectdb import ObjectDb
from .provider import SharedSaasContext
from .services import (
    ServiceKind,
    decompress_service_db,
    inspect_object,
)
from .stream import SaasStream

# The two "this index entry is the channel/chat *list*" table names —
# whichever of these appears in an index entry's own sniffed tables
# marks it as the container, not a per-channel/per-chat message DB:
# Teams via "channel_info_table", Chat via "chat_info_table" (columns:
# row_id, chat_id, chat_type, create_time, last_update_time, topic,
# web_url, metadata).
_CONTAINER_TABLE_NAMES = frozenset({"channel_info_table", "chat_info_table"})

# The object-name index's own name for the entry that names the
# channel/chat list container, as opposed to a per-channel/per-chat
# entry (whose own "name" is the raw channel/chat id string instead) —
# see _resolve_message_index's docstring.
_CONTAINER_DB_NAMES = frozenset({"teams_channel_db", "chat_db"})

_CHANNEL_COLUMNS = [Column("channel_id"), Column("name", required=False)]

# A sibling of chat_info_table inside the same decompressed chat-list
# DB — the connector's real member list, not just the (almost always
# empty) topic column: one row per chat_id, with a JSON array of member
# objects (``userId``, ``userEmail``, ``role``, ``tenantId``,
# ``visibleHistoryStartDateTime``, ``membershipId``, ``display_name``)
# populated from a live Microsoft Graph membership call. This is
# exactly where a real Microsoft Teams client itself gets an unnamed
# chat's own display name from — see ``_chat_labels``'s own docstring.
_CHAT_MEMBERS_TABLE = "chat_members_table"
_CHAT_MEMBERS_COLUMNS = [Column("chat_id"), Column("members")]


class _ChatType(enum.IntEnum):
    """Matches Microsoft Graph's own ``chatType`` enum 1:1. A MEETING
    chat's display name comes from the meeting's own subject rather than
    being synthesized from its member list the way an ordinary group/1:1
    chat's unnamed display falls back to — even when that member list is
    non-empty and the subject is unset."""

    ONE_ON_ONE = 0
    MEETING = 2


# A real Microsoft Teams client's own literal label for a MEETING chat
# with no subject set anywhere — never derived from the attendee list,
# see _ChatType.MEETING's own comment above.
_NO_TITLE_MEETING_LABEL = "(no title)"
# A ONE_ON_ONE chat whose chat_members_table row lists only the
# backed-up account itself, with no second entry at all. The other side
# of a ONE_ON_ONE conversation can be missing from this table entirely
# instead of having its own row like an ordinary counterpart — most
# often because it's a bot/app identity, which Microsoft Graph's
# membership endpoint doesn't always surface the same way it does a
# human account. Real Teams clients show literally "Bot" for exactly
# this shape of conversation.
_BOT_CHAT_LABEL = "Bot"

# msg_info_table's columns — none marked required: a row missing all of
# them still renders (as an unattributed, timestamp-less, empty
# message) rather than raising — the resilience principle applied at
# row granularity instead of provider granularity.
_MESSAGE_COLUMNS = [
    Column("author", required=False),
    Column("create_time", required=False),
    Column("content_preview", required=False),
    Column("metadata", required=False),
    Column("is_sys_message", required=False),
    Column("is_deleted", required=False),
    Column("reply_to_id", required=False),
    Column("msg_id", required=False),
]

# A separate table, sibling to msg_info_table in the same message DB,
# not embedded in msg_info_table.metadata at all: a Teams sticker is
# referenced inline as an ordinary <img> tag inside an "html"
# contentType message body (msg_info_table.metadata.body.content), but
# its actual bytes are cached here, keyed by msg_id, for exactly this
# offline-export use case. metadata.attachments[] is always empty for a
# message that only has stickers — this table, not that field, is
# where their content lives.
_STICKER_TABLE = "sticker_info_table"
_STICKER_COLUMNS = [Column("msg_id"), Column("url"), Column("base64_content")]


async def _is_container(dedup_file: DedupFile, db: ObjectDb, object_id: str) -> frozenset[str] | None:
    """Inspects one already-located index entry's own object; returns
    its table set if it looks like a channel/chat list
    (``_CONTAINER_TABLE_NAMES`` intersects), else ``None`` — the entry
    is an ordinary per-channel/per-chat message DB instead. This is
    validation of a specific, index-named entry, never a step in
    finding one."""
    offset, length = await db.get(object_id)
    if length == 0:
        return None
    result = await inspect_object(dedup_file, offset, length)
    if result.kind is ServiceKind.SERVICE_DB and result.tables & _CONTAINER_TABLE_NAMES:
        return result.tables
    return None


async def _resolve_message_index(
    dedup_file: DedupFile, object_name_index: ObjectNameIndex | None
) -> tuple[ObjectDb, frozenset[str], str, dict[str, str]] | None:
    """Locates Teams/Chat's channel/chat index. Unlike every other SaaS
    provider, whose index entry names one service DB directly, this
    entry points at an INDEX object instead, because the number of
    channels/chats is only known at backup time. One index entry is the
    container (``_CONTAINER_DB_NAMES``); every other entry is a
    channel/chat's own per-entity message DB. The resolved container is
    re-validated via ``_is_container``, never assumed correct just
    because it decoded as an INDEX.

    Returns:
        ``(object_db, container_tables, container_object_id,
        entity_object_ids)`` for ``TeamsChatProvider.create``, or
        ``None`` on any failure (caller raises
        ``UnsupportedDataFormatError``).
    """
    if object_name_index is None:
        return None
    # The connector's own additional_meta.db_object_ids.db_objects always
    # has exactly one "db_infos_in_snapshot" entry; its object_id is the
    # INDEX object itself, not a named service DB — resolved via the
    # connector's own object-name index, never a scan (see provider.py's
    # module docstring).
    object_id = object_name_index.object_ids.get("db_infos_in_snapshot")
    if object_id is None:
        return None
    try:
        object_db = await ObjectDb.load(dedup_file, object_name_index.offset, object_name_index.length)
    except (sqlite3.DatabaseError, DataCorruptError):
        return None
    try:
        offset, length = await object_db.get(object_id)
    except NotFoundError:
        await object_db.close()
        return None
    # inspect_object()/sniff() is documented non-raising for bad data
    # (returns SniffResult(kind=BINARY) etc. instead), but this path
    # still never trusts that boundary blindly.
    try:
        result = await inspect_object(dedup_file, offset, length)
    except (sqlite3.DatabaseError, DataCorruptError):
        await object_db.close()
        return None
    if result.kind is not ServiceKind.INDEX:
        await object_db.close()
        return None
    container_object_id = next(
        (entry.object_id for entry in result.index_entries if entry.name in _CONTAINER_DB_NAMES), None
    )
    if container_object_id is None:
        await object_db.close()
        return None
    container_tables = await _is_container(dedup_file, object_db, container_object_id)
    if container_tables is None:
        await object_db.close()
        return None
    entity_object_ids = {
        entry.name: entry.object_id for entry in result.index_entries if entry.object_id != container_object_id
    }
    return object_db, container_tables, container_object_id, entity_object_ids


async def _channel_labels(container_bytes: bytes) -> dict[str, str]:
    """``channel_id -> name`` from a decompressed ``channel_info_table``."""
    async with await SqliteSource.from_bytes(container_bytes) as src:
        table = await Table.create(src.connection, "channel_info_table", _CHANNEL_COLUMNS)
        return {str(row["channel_id"]): str(row["name"]) async for row in table.select() if row.get("name")}


async def _owning_account_email(repo: DedupRepo, version: Version) -> str | None:
    """The backed-up account's own email, read directly off the owning
    ``USER_CHAT`` workload's ``workload_spec.status.entity_meta.spec
    .user_info.email`` rather than via ``_saas_display_name`` (whose
    bare-email fallback is untyped). A narrow ``workload_config`` read
    by ``workload_id``, skipping ``workloads``'s unneeded joins.
    ``None``, never raises, if not found."""
    try:
        table = await Table.create(
            await repo.db("workload_config"), "workload_config", [Column("workload_id"), Column("workload_spec")]
        )
        row = await table.select_one("workload_id = ?", (version.workload_id,))
        if row is None:
            return None
        spec = json.loads(str(row["workload_spec"]))
        entity_spec = ((spec.get("status") or {}).get("entity_meta") or {}).get("spec") or {}
        user_info = entity_spec.get("user_info")
        return str(user_info["email"]) if user_info and user_info.get("email") else None
    except (NotFoundError, ValueError, DataCorruptError, sqlite3.DatabaseError):
        # Synthetic single-workload test repositories often have no
        # workload_config table at all; a schema-drifted
        # workload_config (missing workload_spec) is the same
        # "nothing to read" shape, not a reason to crash chat/channel
        # name resolution over an enrichment-only lookup.
        return None


def _chat_display_name_from_members(members_json: object, self_email: str | None) -> str | None:
    """The real mechanism a Microsoft Teams client itself uses to name
    an unnamed chat: every *other* member's real ``display_name``,
    comma-joined, with ``self_email``'s own entry excluded — see
    ``_chat_labels`` for the full display-name fallback rule. ``None``
    if ``members_json`` doesn't parse, isn't a list, or every member is
    self (or has no ``display_name`` at all)."""
    try:
        members = json.loads(members_json) if isinstance(members_json, str) else None
    except ValueError:
        return None
    if not isinstance(members, list):
        return None
    others = [
        str(member["display_name"])
        for member in members
        if isinstance(member, dict) and member.get("display_name") and member.get("userEmail") != self_email
    ]
    return ", ".join(others) if others else None


async def _chat_labels(container_bytes: bytes, self_email: str | None) -> dict[str, str]:
    """Best-effort ``chat_id -> label`` map: a chat's own ``topic``
    when set, else the same member-list-derived name a real Teams
    client synthesizes for an unnamed chat (see
    ``_chat_display_name_from_members``), sourced from
    ``chat_info_table``/``_CHAT_MEMBERS_TABLE``. Two chat shapes get a
    literal label instead — see ``_NO_TITLE_MEETING_LABEL`` and
    ``_BOT_CHAT_LABEL``."""
    async with await SqliteSource.from_bytes(container_bytes) as src:
        if not await Table.exists_in(src.connection, "chat_info_table"):
            return {}
        cols = (await Table.create(src.connection, "chat_info_table", [])).columns_present
        # Column names are presence-checked, not assumed fixed, so a
        # future connector version's schema drift loses a label instead
        # of crashing.
        id_col = next((c for c in cols if c.endswith("chat_id") or c == "id"), None)
        label_col = next((c for c in cols if c in ("topic", "name", "title")), None)
        type_col = "chat_type" if "chat_type" in cols else None
        if id_col is None:
            return {}
        labels: dict[str, str] = {}
        chat_type_by_id: dict[str, int] = {}
        try:
            select_columns = [Column(id_col)]
            if label_col is not None:
                select_columns.append(Column(label_col))
            if type_col is not None:
                select_columns.append(Column(type_col))
            info_table = await Table.create(src.connection, "chat_info_table", select_columns)
            async for row in info_table.select():
                chat_id = str(row[id_col])
                chat_type = row.get(type_col) if type_col is not None else None
                if isinstance(chat_type, int):
                    chat_type_by_id[chat_id] = chat_type
                if label_col is not None and row.get(label_col):
                    labels[chat_id] = str(row[label_col])
                elif chat_type == _ChatType.MEETING:
                    labels[chat_id] = _NO_TITLE_MEETING_LABEL
        except sqlite3.Error:
            return {}

        if await Table.exists_in(src.connection, _CHAT_MEMBERS_TABLE):
            members_table = await Table.create(src.connection, _CHAT_MEMBERS_TABLE, _CHAT_MEMBERS_COLUMNS)
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
        return labels


async def _read_stickers(connection: aiosqlite.Connection) -> dict[str, dict[str, str]]:
    """Best-effort ``msg_id -> {sticker_url: base64_content}`` map from
    this channel's own ``sticker_info_table`` — ``{}`` if the table
    doesn't exist (the common case: most channels have no stickers),
    never raises. See ``_STICKER_TABLE`` for the shape this reads."""
    if not await Table.exists_in(connection, _STICKER_TABLE):
        return {}
    table = await Table.create(connection, _STICKER_TABLE, _STICKER_COLUMNS)
    result: dict[str, dict[str, str]] = {}
    async for row in table.select():
        result.setdefault(str(row["msg_id"]), {})[str(row["url"])] = str(row["base64_content"])
    return result


class TeamsChatProvider:
    """``UnitProvider`` for one Teams or Chat (M365 only) workload
    version. Raises ``UnsupportedDataFormatError`` from ``create`` if no
    channel/chat index is found, so callers can degrade to
    ``RawObjectProvider`` like every other application-layer provider.
    Build one with ``create``, never the constructor directly —
    locating the index and reading the container DB are I/O and can't
    run synchronously."""

    #: Populated by ``create``, the only supported constructor.
    _dedup_file: DedupFile
    _db: ObjectDb
    _entity_object_ids: dict[str, str]
    _is_channel: bool
    _chat_schema_found: bool
    _labels: dict[str, str]

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        """Pure field initialization; ``create`` does the real work."""
        self._repo = repo
        self._version = version
        self._stream = SaasStream(repo, version.connection_config_id, version.saas_stream_uuid)

    @classmethod
    async def create(cls, repo: DedupRepo, version: Version, *, shared: SharedSaasContext | None = None) -> Self:
        """``shared`` is accepted only for calling-convention uniformity
        with ``units/dispatch.py``'s ``_ProviderFactory`` — ``TEAMS``/
        ``USER_CHAT`` never offer more than this one candidate, so it
        is always ``None`` in practice and unused here: Teams/Chat's
        channel/chat index discovery is its own mechanism (see this
        module's own docstring), not the ``saas_obj``/object-name-index
        resolution ``SharedSaasContext`` carries."""
        self = cls(repo, version)
        try:
            self._dedup_file = await self._stream.open_saas_obj(version)

            # The sole discovery mechanism — see _resolve_message_index's own
            # docstring.
            object_name_index = await resolve_object_name_index(repo, version)
            found = await _resolve_message_index(self._dedup_file, object_name_index)
            if found is None:
                raise UnsupportedDataFormatError(
                    f"no Teams/Chat channel-or-chat index found for version {version.version_uid!r}",
                    ref=version.version_uid,
                )
            self._db, container_tables, container_object_id, self._entity_object_ids = found
            self._is_channel = "channel_info_table" in container_tables
            self._chat_schema_found = "chat_info_table" in container_tables

            offset, length = await self._db.get(container_object_id)
            container_bytes = await decompress_service_db(await self._dedup_file.read(offset, length))
            if self._is_channel:
                self._labels = await _channel_labels(container_bytes)
            else:
                self_email = await _owning_account_email(repo, version)
                self._labels = await _chat_labels(container_bytes, self_email)
        except Exception:
            # No index found (the routine degrade-to-RawObjectProvider case,
            # not just a corrupt-data one) or any later failure — either way
            # self._stream (and, once assigned, self._db) must not leak.
            await self.close()
            raise
        return self

    async def close(self) -> None:
        """Release every sqlite connection this provider owns: ``_db``
        and ``_stream`` — ``SaasStream`` holds two more ``SqliteSource``s
        of its own (``saas_snapshot``/
        ``saas_version``). Tolerates ``_db`` never having been assigned
        (``create()`` failing before it resolved an index), so it's safe
        to call from ``create()``'s own failure path."""
        db = getattr(self, "_db", None)
        if db is not None:
            await db.close()
        await self._stream.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def _ref(self, *extra: str) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        """Pure construction — no I/O, so this stays synchronous (see
        ``UnitProvider``)."""
        return Node(ref=self._ref(), name="Channels" if self._is_channel else "Chats", is_leaf=False)

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        # This one body genuinely does no I/O — ``create`` already read
        # the whole channel/chat index — but ``children()`` is async
        # across every provider (see ``units/base.py``'s ``UnitProvider``).
        nodes = [
            Node(
                ref=self._ref(entity_id),
                name=self._labels.get(entity_id, entity_id),
                is_leaf=True,
                kind=UnitKind.RAW_OBJECT,
                attrs={"entity_id": entity_id, "object_id": object_id, "degraded": self._degraded_reason(entity_id)},
            )
            for entity_id, object_id in self._entity_object_ids.items()
        ]
        return paginate(nodes, offset, limit)

    def _degraded_reason(self, entity_id: str) -> str | None:
        # The resilience principle, applied at leaf granularity rather
        # than provider granularity: Chat's list is real (we did find and
        # parse the index) — most real chats resolve to a real,
        # member-list-derived name (_chat_labels), but two real cases
        # still don't (_chat_labels's own docstring): a real Meet chat
        # with no subject set, or a 1:1 chat whose own member list has no
        # real counterpart to name it from. The two "no label" causes are
        # distinguished for a diagnostic-mode caller: schema genuinely
        # absent (id shown because nothing here was even parseable) vs
        # schema present but this specific chat's own topic/member list
        # didn't resolve to a name (id shown because the chat itself
        # has no nicer name, not because anything failed to read).
        if self._is_channel or entity_id in self._labels:
            return None
        if self._chat_schema_found:
            return "this chat has no topic set in the backup and no other real member to name it — showing raw chat id"
        return "chat_info_table not found for this version — showing raw chat ids"

    async def unit(self, node: Node) -> RestorableUnit:
        # Like ``children()``, this body does no I/O of its own — every
        # read is inside the LazyArtifact's build callback below.
        object_id = node.attrs.get("object_id")
        if object_id is None:
            not_restorable("node", node.name)
        channel_name = node.name

        async def _build() -> bytes:
            offset, length = await self._db.get(str(object_id))
            db_bytes = await decompress_service_db(await self._dedup_file.read(offset, length))
            async with await SqliteSource.from_bytes(db_bytes) as source:
                table = await Table.create(source.connection, "msg_info_table", _MESSAGE_COLUMNS)
                rows = [row async for row in table.select()]
                stickers_by_msg_id = await _read_stickers(source.connection)
            return render_channel_html(rows, channel_name=channel_name, stickers_by_msg_id=stickers_by_msg_id).encode(
                "utf-8"
            )

        return RestorableUnit(
            ref=node.ref, name=f"{node.name}.html", is_leaf=True, kind=node.kind, content=LazyArtifact(_build)
        )
