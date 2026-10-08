"""``RawObjectProvider``: the SaaS fallback ``UnitProvider``, listing the
named objects of a version's object-name index raw, with no service DB.

``dispatch.py`` falls back to it when no application-layer provider
recognizes a version (or its ``sub_type`` is unknown); ``Catalog.provider``'s
``raw`` view requests it directly. It exposes internal index names and
object ids, so it is a diagnostic view.
"""

from __future__ import annotations

import dataclasses
from typing import Self, override

from ..._util.closing import AsyncClosing, close_all, close_preserving
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import NotFoundError
from ...units.provider_kit import diagnostic_node, not_restorable, paginate
from ..base import Node, RestorableUnit, UnitKind
from ..node_ref import NodeRef, canonical_ref_for
from .context import SharedIndexObjectDb, SharedSaasContext, resolve_shared_saas_context
from .object_name_index import DEGRADABLE_OPEN_ERRORS, ObjectNameIndex
from .objectdb import ObjectDb, parse_object_db_id
from .stream import SaasStreamCache


@dataclasses.dataclass(frozen=True, slots=True)
class _ObjectRange:
    """``Node.handle`` of a raw object: its byte range in the stream."""

    offset: int
    length: int


@dataclasses.dataclass(frozen=True, slots=True)
class _UnreadableIndex:
    """``Node.handle`` of the placeholder listed when the version's
    object-name index ObjectDB could not be opened."""


class RawObjectProvider(AsyncClosing):
    """``UnitProvider`` over one SaaS ``Version``'s raw object-name-index
    entries: a flat list under the version root, one leaf per index entry
    (or, given ``object_db_id``, one per object in that ObjectDB), each a
    byte range of the version's ``saas_obj``. Build one with ``create``.
    """

    #: Populated by ``create``.
    _dedup_file: DedupFile
    _object_name_index: ObjectNameIndex | None
    _indexed_db: ObjectDb | None
    _manual_db: ObjectDb | None

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        self._repo = repo
        self._version = version
        self._index_hold: SharedIndexObjectDb | None = None
        self._indexed_db = None
        self._manual_db = None
        self._index_failure: str | None = None

    @classmethod
    async def create(
        cls,
        repo: DedupRepo,
        version: Version,
        saas_streams: SaasStreamCache,
        *,
        object_db_id: str | None = None,
        shared: SharedSaasContext | None = None,
    ) -> Self:
        """Open the version's ``saas_obj`` and its object-name index.

        Args:
            repo: The repository ``version`` belongs to.
            version: The SaaS version to list.
            saas_streams: Borrowed, not owned; the ``saas_obj`` stays
                cached there.
            object_db_id: List this ObjectDB's objects by id instead of
                the index's named entries.
            shared: The version's resolved stream object and index, when
                ``units/dispatch.py`` already resolved them; resolved here
                otherwise.

        Raises:
            NotFoundError: ``object_db_id`` is malformed or names another
                stream, or ``shared`` is omitted and the version's
                ``saas_obj`` can't be located.
        """
        self = cls(repo, version)
        try:
            if shared is None:
                shared = await resolve_shared_saas_context(repo, version, saas_streams)
            self._dedup_file = shared.dedup_file

            if object_db_id is not None:
                stream_uuid, offset, length = parse_object_db_id(object_db_id)
                if stream_uuid != version.saas_stream_uuid:
                    raise NotFoundError(
                        f"--object-db-id {object_db_id!r} names stream {stream_uuid!r}, "
                        f"but this version's stream is {version.saas_stream_uuid!r}",
                        ref=object_db_id,
                    )
                self._manual_db = await ObjectDb.load(self._dedup_file, offset, length)

            # None for a version with no index: nothing to list unless
            # object_db_id was given.
            self._object_name_index = shared.object_name_index
            if shared.index_object_db is not None:
                try:
                    self._indexed_db = await shared.index_object_db.acquire()
                except DEGRADABLE_OPEN_ERRORS as exc:
                    # The fallback of last resort degrades instead of failing:
                    # the root lists one placeholder saying why it is empty.
                    self._index_failure = str(exc)
                else:
                    # Held only once acquired: close() releases whatever is held.
                    self._index_hold = shared.index_object_db
        except BaseException as exc:
            await close_preserving(exc, [self.close])
            raise
        return self

    def _ref(self, *extra: str) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    # -- UnitProvider -------------------------------------------------

    @override
    async def close(self) -> None:
        """Close the ``object_db_id`` ObjectDB and release this provider's
        hold on the index's; the borrowed stream stays open. Safe after a
        partial ``create()`` and more than once."""
        manual, self._manual_db = self._manual_db, None
        hold, self._index_hold = self._index_hold, None
        self._indexed_db = None
        closers = [*([manual.close] if manual is not None else []), *([hold.release] if hold is not None else [])]
        await close_all(closers, "RawObjectProvider.close() failed to close every connection")

    def root(self) -> Node:
        return Node(ref=self._ref(), name=self._version.saas_stream_uuid, is_leaf=False)

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if node.is_leaf:
            # Flat tree: only the root has children.
            return []
        if self._manual_db is not None:
            # object_db_id given: named by object_id, there being no index names.
            return await self._object_id_nodes(self._manual_db, offset, limit)
        if self._indexed_db is not None and self._object_name_index is not None:
            return await self._named_nodes(self._indexed_db, self._object_name_index, offset, limit)
        if self._index_failure is not None and offset == 0 and limit != 0:
            return [self._unreadable_index_node(self._index_failure)]
        return []

    def _unreadable_index_node(self, reason: str) -> Node:
        node = diagnostic_node(
            self._ref("diagnostic"),
            "(object index unreadable)",
            "this version's index of backed-up objects could not be read, so none of its "
            "objects can be listed — the repository copy may be incomplete or damaged.",
            handle=_UnreadableIndex(),
        )
        # The technical cause is verbose-only display data, never the listing name.
        return dataclasses.replace(node, details={"cause": reason})

    def _raw_object_node(self, name: str, offset: int, length: int) -> Node:
        return Node(
            ref=self._ref(name),
            name=name,
            is_leaf=True,
            kind=UnitKind.RAW_OBJECT,
            size=length,
            handle=_ObjectRange(offset, length),
        )

    async def _named_nodes(
        self, db: ObjectDb, object_name_index: ObjectNameIndex, offset: int, limit: int | None
    ) -> list[Node]:
        located = await db.get_many(list(object_name_index.object_ids.values()))
        # A named entry the index recorded but this ObjectDB doesn't have (a
        # stale/malformed index) is left out: absent, not corruption.
        nodes = [
            self._raw_object_node(name, *located[object_id])
            for name, object_id in object_name_index.object_ids.items()
            if object_id in located
        ]
        return paginate(nodes, offset, limit)

    async def _object_id_nodes(self, db: ObjectDb, offset: int, limit: int | None) -> list[Node]:
        nodes = [
            self._raw_object_node(object_id, obj_offset, obj_length)
            for object_id, (obj_offset, obj_length) in (await db.object_map()).items()
        ]
        return paginate(nodes, offset, limit)

    async def unit(self, node: Node) -> RestorableUnit:
        """Open ``node`` as a restorable unit.

        Raises:
            NotFoundError: ``node`` is the unreadable-index placeholder.
            NotRestorableError: ``node`` is not a restorable unit.
        """
        match node.handle:
            case _ObjectRange(offset=offset, length=length):
                return RestorableUnit.of(node, self._dedup_file.view(offset, length))
            case _UnreadableIndex():
                raise NotFoundError(node.diagnostic or "", ref=str(node.ref))
            case _:
                not_restorable("node", node.name)
