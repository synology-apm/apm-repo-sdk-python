"""``TeamsChatProvider``: M365 Teams channels and 1:1/group chats, each
exported as one self-contained HTML transcript (``render_channel_html``).

Channels are listed under Standard/Private/Shared categories
(``CategorizedGroupTree`` over ``channel_info_table.channel_type``); chats
are one flat list. The service DBs are located by ``teams_discovery.py``,
which doesn't fit ``SaasWorkloadProvider``'s per-table model. Chat support
follows FORMAT-SPEC.md: Teams/Chat container discrimination, and has not
been checked against an observed chat backup.

Every leaf is ``UnitKind.TEAMS_CHAT_MESSAGE``, and every container
carries the same value as ``Node.leaf_kind``.
"""

from __future__ import annotations

import dataclasses
from typing import Self, override

import aiosqlite

from ..._util.closing import AsyncClosing, close_preserving
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import UnsupportedDataFormatError
from ...storage.table import Column, Table
from ...units.provider_kit import mtime_from_raw, not_restorable, paginate
from ..base import Node, RestorableUnit, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_teams_chat import render_channel_html
from ..node_ref import NodeRef, canonical_ref_for
from .context import SharedIndexObjectDb, SharedSaasContext, resolve_shared_saas_context
from .object_name_index import DEGRADABLE_OPEN_ERRORS
from .objectdb import ObjectDb
from .provider import SaasHandle, TreeBackedProvider
from .services import (
    open_service_db,
)
from .stream import SaasStreamCache
from .teams_discovery import (
    CHANNEL_CATEGORY_LABELS,
    CHANNEL_CATEGORY_STANDARD,
    channel_info,
    chat_labels,
    owning_account_email,
    resolve_message_index,
)
from .tree_strategy import CategorizedGroupTree, Key, TreeEntry, TreeStrategy

# None required: a row missing them still renders, as an empty message.
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

# A sticker is an <img> in a message's html body; its bytes are cached in
# this sibling of msg_info_table, keyed by msg_id and URL.
_STICKER_TABLE = "sticker_info_table"
_STICKER_COLUMNS = [Column("msg_id"), Column("url"), Column("base64_content")]


async def _read_stickers(connection: aiosqlite.Connection) -> dict[str, dict[str, str]]:
    """``msg_id -> {sticker_url: base64_content}`` from this message DB's
    ``sticker_info_table``; ``{}`` when the table doesn't exist."""
    if not await Table.exists_in(connection, _STICKER_TABLE):
        return {}
    table = await Table.create(connection, _STICKER_TABLE, _STICKER_COLUMNS)
    result: dict[str, dict[str, str]] = {}
    async for row in table.select():
        result.setdefault(str(row["msg_id"]), {})[str(row["url"])] = str(row["base64_content"])
    return result


class _TeamsEntityFlatTree:
    """A flat ``TreeStrategy`` over an already-resolved channel/chat list:
    every entity is a leaf at ``(entity_id,)``, sorted by name. No I/O."""

    def __init__(self, entity_object_ids: dict[str, str], labels: dict[str, str]) -> None:
        self._entity_object_ids = entity_object_ids
        self._labels = labels

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        if key != ():
            return []
        entries = [
            TreeEntry(
                (entity_id,),
                self._labels.get(entity_id, entity_id),
                True,
                {"entity_id": entity_id, "object_id": object_id},
            )
            for entity_id, object_id in self._entity_object_ids.items()
        ]
        entries.sort(key=lambda entry: entry.name)
        return paginate(entries, offset, limit)


@dataclasses.dataclass(frozen=True, slots=True)
class _Message:
    """``Node.handle`` of a channel/chat leaf: its message DB's object id."""

    object_id: str


class TeamsChatProvider(AsyncClosing, TreeBackedProvider):
    """``UnitProvider`` for one Teams or Chat (M365 only) workload
    version. Build one with ``create``."""

    #: Populated by ``create``.
    _dedup_file: DedupFile
    _db: ObjectDb
    _entity_object_ids: dict[str, str]
    _is_channel: bool
    _chat_schema_found: bool
    _labels: dict[str, str]
    #: ``entity_id -> create_time`` (epoch seconds); entries may be missing.
    _create_times: dict[str, int]
    #: Categorized for channels, flat for chats.
    _tree: TreeStrategy

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        self._repo = repo
        self._version = version
        self._index_hold: SharedIndexObjectDb | None = None

    @classmethod
    async def create(
        cls,
        repo: DedupRepo,
        version: Version,
        saas_streams: SaasStreamCache,
        *,
        shared: SharedSaasContext | None = None,
    ) -> Self:
        """Locate the version's channel/chat index and read its container
        DB.

        Args:
            repo: The repository ``version`` belongs to.
            version: The Teams or Chat version to open.
            saas_streams: Borrowed, not owned; the ``saas_obj`` stays
                cached there.
            shared: The version's resolved stream object and index, when
                ``units/dispatch.py`` already resolved them; resolved here
                otherwise.

        Raises:
            UnsupportedDataFormatError: No channel/chat index is found.
            DataCorruptError: The container DB doesn't decode, or its
                ``channel_info_table`` lacks ``channel_id``.
        """
        self = cls(repo, version)
        try:
            if shared is None:
                shared = await resolve_shared_saas_context(repo, version, saas_streams)
            self._dedup_file = shared.dedup_file
            found = None
            if shared.object_name_index is not None and shared.index_object_db is not None:
                try:
                    self._db = await shared.index_object_db.acquire()
                except DEGRADABLE_OPEN_ERRORS:
                    pass
                else:
                    # Held only once acquired: close() releases whatever is held.
                    self._index_hold = shared.index_object_db
                    found = await resolve_message_index(self._dedup_file, shared.object_name_index, self._db)
            if found is None:
                raise UnsupportedDataFormatError(
                    f"no Teams/Chat channel-or-chat index found for version {version.version_uid!r}",
                    ref=version.version_uid,
                )
            container_tables, container_object_id, self._entity_object_ids = found
            self._is_channel = "channel_info_table" in container_tables
            self._chat_schema_found = "chat_info_table" in container_tables

            offset, length = await self._db.get(container_object_id)
            async with await open_service_db(await self._dedup_file.read(offset, length)) as source:
                if self._is_channel:
                    self._labels, channel_categories, self._create_times = await channel_info(source)
                    inner = _TeamsEntityFlatTree(self._entity_object_ids, self._labels)
                    # CategorizedGroupTree needs a category for every entity,
                    # including one without a channel_info_table row.
                    categories_for_tree = {
                        entity_id: channel_categories.get(entity_id, CHANNEL_CATEGORY_STANDARD)
                        for entity_id in self._entity_object_ids
                    }
                    self._tree = CategorizedGroupTree(
                        inner, categories=categories_for_tree, labels=CHANNEL_CATEGORY_LABELS
                    )
                else:
                    self_email = await owning_account_email(repo, version)
                    self._labels, self._create_times = await chat_labels(source, self_email)
                    self._tree = _TeamsEntityFlatTree(self._entity_object_ids, self._labels)
        except BaseException as exc:
            await close_preserving(exc, [self.close])
            raise
        return self

    @override
    async def close(self) -> None:
        """Release this provider's hold on the index ObjectDB; the borrowed
        stream stays open. Safe after a partial ``create()`` and more than
        once."""
        hold, self._index_hold = self._index_hold, None
        if hold is not None:
            await hold.release()

    def _ref(self, *extra: str) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        return Node(
            ref=self._ref(),
            name="Channels" if self._is_channel else "Chats",
            is_leaf=False,
            leaf_kind=UnitKind.TEAMS_CHAT_MESSAGE,
            handle=SaasHandle(()),
        )

    @override
    def _node_for(self, entry: TreeEntry) -> Node:
        key, name, row = entry.key, entry.name, entry.row
        if not entry.is_leaf:
            return Node(
                ref=self._ref(*key),
                name=name,
                is_leaf=False,
                leaf_kind=UnitKind.TEAMS_CHAT_MESSAGE,
                handle=SaasHandle(key),
            )
        details: dict[str, object] = {}
        degraded = None
        mtime = None
        message: _Message | None = None
        if row is not None:
            entity_id = str(row["entity_id"])
            details["entity_id"] = entity_id
            degraded = self._degraded_reason(entity_id)
            mtime = mtime_from_raw(self._create_times.get(entity_id))
            message = _Message(str(row["object_id"]))
        return Node(
            ref=self._ref(*key),
            name=name,
            is_leaf=True,
            kind=UnitKind.TEAMS_CHAT_MESSAGE,
            mtime=mtime,
            degraded=degraded,
            details=details,
            export_name=f"{name}.html",
            handle=message,
        )

    def _degraded_reason(self, entity_id: str) -> str | None:
        # Why an unlabelled chat shows its raw id: chat_info_table missing,
        # or this chat having no topic and no other member to name it.
        if self._is_channel or entity_id in self._labels:
            return None
        if self._chat_schema_found:
            return "this chat has no topic set in the backup and no other real member to name it — showing raw chat id"
        return "this backup has no chat names for this version — showing raw chat ids"

    async def unit(self, node: Node) -> RestorableUnit:
        if not isinstance(node.handle, _Message):
            not_restorable("node", node.name)
        object_id = node.handle.object_id
        channel_name = node.name

        async def _build() -> bytes:
            offset, length = await self._db.get(str(object_id))
            async with await open_service_db(await self._dedup_file.read(offset, length)) as source:
                table = await Table.create(source.connection, "msg_info_table", _MESSAGE_COLUMNS)
                rows = [row async for row in table.select()]
                stickers_by_msg_id = await _read_stickers(source.connection)
            return render_channel_html(rows, channel_name=channel_name, stickers_by_msg_id=stickers_by_msg_id).encode(
                "utf-8"
            )

        # The raw-id name is the only gap; the chat's content is whole.
        return RestorableUnit.of(node, LazyArtifact(_build), name=f"{channel_name}.html", degraded=None)
