"""``RawObjectProvider``: the SaaS fallback ``UnitProvider``, used
when a stream's service-DB can't be identified.

Every SaaS application-layer provider (``mail``/``drive``/``contact``/
``calendar``/``site``) needs its own service-level DB to make sense of a
stream's content; this one needs none — it resolves the exact same
object-name index every application-layer provider does
(``resolve_object_name_index``) and shows whatever named objects it lists,
raw. ``dispatch.py`` falls back here when no application-layer provider
recognizes a version's content (or its ``sub_type`` is unknown); the
TUI's diagnostic (``d``) mode / the CLI's ``--verbose`` can also request it
directly via ``Catalog.provider``'s ``force_raw``, even when an
application-layer provider did recognize the version. Because it
exposes internal index-entry-name/object_id structure directly, it
is diagnostic-mode only — the default view of an unrecognized version
is a presentation-layer concern handled elsewhere.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import NotFoundError
from ..base import Node, RestorableUnit, UnitKind, not_restorable, paginate
from ..node_ref import NodeRef, canonical_ref_for
from .object_name_index import ObjectNameIndex, resolve_object_name_index
from .objectdb import ObjectDb, parse_object_db_id
from .stream import SaasStreamCache


class RawObjectProvider:
    """``UnitProvider`` over one SaaS ``Version``'s raw object-name-index
    entries. ``root`` is the version itself; ``children`` is one leaf
    per object-name-index entry (or, with ``object_db_id`` — see
    ``parse_object_db_id`` — one leaf per ObjectDB object) — flat,
    restorable via a ``ByteRangeView`` into the version's ``saas_obj``.
    """

    #: Populated by ``create``, the only supported constructor.
    _dedup_file: DedupFile
    _object_name_index: ObjectNameIndex | None
    _indexed_db: ObjectDb | None
    _manual_db: ObjectDb | None

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        """Pure field initialization; ``create`` does the real work
        (opening ``saas_obj``, resolving the object-name index — all I/O) —
        never construct this class directly."""
        self._repo = repo
        self._version = version

    @classmethod
    async def create(
        cls, repo: DedupRepo, version: Version, saas_streams: SaasStreamCache, *, object_db_id: str | None = None
    ) -> Self:
        """``saas_streams`` is borrowed, not owned — the version's
        ``saas_obj`` is opened via the caller's shared ``SaasStreamCache``
        rather than a private ``SaasStream`` this provider would otherwise
        need to close itself."""
        self = cls(repo, version)
        try:
            self._dedup_file = await saas_streams.open_saas_obj(version)

            self._manual_db = None
            if object_db_id is not None:
                stream_uuid, offset, length = parse_object_db_id(object_db_id)
                if stream_uuid != version.saas_stream_uuid:
                    # A mismatch here is a real "pointed at the wrong version"
                    # bug — raise rather than materializing a plausible-looking
                    # but wrong offset/length silently.
                    raise NotFoundError(
                        f"--object-db-id {object_db_id!r} names stream {stream_uuid!r}, "
                        f"but this version's stream is {version.saas_stream_uuid!r}",
                        ref=object_db_id,
                    )
                self._manual_db = await ObjectDb.load(self._dedup_file, offset, length)

            # The sole location-resolution mechanism — the same
            # resolve_object_name_index call every application-layer
            # provider (mail/drive/contact/calendar/site) makes.
            # None whenever this version simply has no index at all (an old connector predating this bookkeeping) — this
            # provider then has nothing to show unless ``object_db_id`` was
            # given, which is exactly as honest as every other provider's
            # "no object-name index" case.
            self._object_name_index = await resolve_object_name_index(repo, version)
            self._indexed_db = None
            if self._object_name_index is not None:
                index = self._object_name_index
                self._indexed_db = await ObjectDb.load(self._dedup_file, index.offset, index.length)
        except Exception:
            # A bad --object-db-id, a corrupt manual/indexed ObjectDb, or
            # any other failure here must not leak whichever of
            # _manual_db/_indexed_db already got opened -- this is the
            # guaranteed-to-succeed fallback provider, reachable directly
            # from CLI/TUI diagnostic tooling.
            await self.close()
            raise
        return self

    def _ref(self, *extra: str) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    # -- UnitProvider -------------------------------------------------

    async def close(self) -> None:
        """Release the sqlite connection(s) this provider owns — an
        unclosed ``aiosqlite`` connection owns a non-daemon background
        thread that keeps the interpreter alive forever, so this
        matters beyond tidiness. The version's own stream is borrowed
        from the caller's ``SaasStreamCache``, not owned here, so there's
        nothing of its own to release.
        Tolerates ``_indexed_db``/``_manual_db`` never having been
        assigned (``create()`` failing before either was set), so it's
        safe to call from ``create()``'s own failure path."""
        indexed_db = getattr(self, "_indexed_db", None)
        if indexed_db is not None:
            await indexed_db.close()
            self._indexed_db = None
        manual_db = getattr(self, "_manual_db", None)
        if manual_db is not None:
            await manual_db.close()
            self._manual_db = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def root(self) -> Node:
        """Pure construction — no I/O, so this stays synchronous (see
        ``UnitProvider``)."""
        return Node(ref=self._ref(), name=self._version.saas_stream_uuid, is_leaf=False)

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if self._manual_db is not None:
            # object_db_id override: objects named by internal object_id
            # rather than an index-recorded name — there is no index entry to name
            # them by once a caller has pinned a location down manually.
            return await self._object_id_nodes(self._manual_db, offset, limit)
        if self._indexed_db is not None and self._object_name_index is not None:
            return await self._named_nodes(self._indexed_db, self._object_name_index, offset, limit)
        # No manual override, no object-name index for this version — nothing
        # to show: there is no scan fallback, only a resolved index or a
        # manually supplied object_db_id can produce a listing.
        return []

    def _raw_object_node(self, name: str, offset: int, length: int) -> Node:
        return Node(
            ref=self._ref(name),
            name=name,
            is_leaf=True,
            kind=UnitKind.RAW_OBJECT,
            size=length,
            attrs={"object_offset": offset, "object_length": length},
        )

    async def _named_nodes(
        self, db: ObjectDb, object_name_index: ObjectNameIndex, offset: int, limit: int | None
    ) -> list[Node]:
        nodes = []
        for name, object_id in object_name_index.object_ids.items():
            try:
                obj_offset, obj_length = await db.get(object_id)
            except NotFoundError:
                # A named entry the index recorded but this ObjectDB
                # doesn't actually have (a stale/malformed index) — not
                # this provider's job to flag as corruption, just absent.
                continue
            nodes.append(self._raw_object_node(name, obj_offset, obj_length))
        return paginate(nodes, offset, limit)

    async def _object_id_nodes(self, db: ObjectDb, offset: int, limit: int | None) -> list[Node]:
        nodes = [
            self._raw_object_node(object_id, obj_offset, obj_length)
            for object_id, (obj_offset, obj_length) in (await db.object_map()).items()
        ]
        return paginate(nodes, offset, limit)

    async def unit(self, node: Node) -> RestorableUnit:
        # This body does no I/O — ``children()`` already resolved the
        # object's ``(offset, length)`` into the node's attrs and
        # ``view()`` is a pure construction — but ``unit()`` is async
        # across every provider (see ``units/base.py``'s ``UnitProvider``).
        object_offset = node.attrs.get("object_offset")
        object_length = node.attrs.get("object_length")
        if object_offset is None or object_length is None:
            not_restorable("node", node.name)
        view = self._dedup_file.view(int(object_offset), int(object_length))
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=UnitKind.RAW_OBJECT, size=node.size, content=view
        )
