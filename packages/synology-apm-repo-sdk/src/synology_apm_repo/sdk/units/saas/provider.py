"""``SaasWorkloadProvider`` + ``SaasWorkloadConfig``: the shared
skeleton Mail/Drive/Contact/Calendar/Site all reduce to. The provider
opens the version's ``saas_obj``, opens each service DB the config names
through the connector's object-name index (``object_name_index.py``),
builds the config's ``TreeStrategy``, and answers ``root()``/
``children()``/``unit()`` by delegating to it; a workload supplies only a
``SaasWorkloadConfig`` and its ``content()`` helpers.

Every table is resolved by a direct object-name-index lookup: a table the
index doesn't name is not found. Only the index's naming tells
schema-identical tables apart (Archive Mail's ``mail_table`` and regular
Mail's).

``TeamsChatProvider`` is not built on this base: its discovery (one
shared channel/chat INDEX-object lookup) doesn't fit the per-table
``object_names`` model.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any, Protocol, Self, override

import aiosqlite

from ..._util.closing import AsyncClosing, close_all, close_preserving
from ..._util.once import AsyncOnce
from ...catalog.version import Version
from ...catalog.workload import TargetType
from ...dedup.dedup_file import ByteRangeView, DedupFile
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, NotFoundError, UnsupportedDataFormatError
from ...storage.sqlite_source import SqliteSource
from ...units.provider_kit import not_restorable
from ..base import ContentSource, ItemColumns, Node, NodeRole, RestorableUnit, UnitKind
from ..node_ref import NodeRef, canonical_ref_for
from .context import SharedIndexObjectDb, SharedSaasContext, resolve_shared_saas_context
from .object_name_index import (
    DEGRADABLE_OPEN_ERRORS,
    ObjectNameIndex,
    read_grouped_names,
    read_id_to_name_map,
    resolve_service_db,
)
from .objectdb import ObjectDb, read_object
from .stream import SaasStreamCache
from .tree_strategy import Key, RecursiveTree, Row, TreeEntry, TreeStrategy


@dataclasses.dataclass(frozen=True, slots=True)
class NodeExtras:
    """What a ``SaasWorkloadConfig`` hook adds to a node the provider builds.

    Attributes:
        mtime: The node's ``Node.mtime``.
        columns: The node's ``Node.columns``.
        details: The node's ``Node.details``.
        role: The node's ``Node.role``.
        leaf_kind: A group's own ``Node.leaf_kind``, instead of the
            config's ``leaf_kind``.
    """

    mtime: datetime | None = None
    columns: ItemColumns = dataclasses.field(default_factory=ItemColumns)
    details: Mapping[str, object] = dataclasses.field(default_factory=dict)
    role: NodeRole = NodeRole.ORDINARY
    leaf_kind: UnitKind | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class SaasWorkloadConfig[StateT]:
    """What one workload needs beyond the shared skeleton: which
    ``tables`` to open (resolved via ``object_names``), a ``tree_factory``
    that builds the ``TreeStrategy`` once every table is open, and a
    ``content()`` callback giving one leaf row's ``ContentSource``; the
    provider builds the leaf's ``RestorableUnit`` around it.

    ``tree_factory``/``content`` may do I/O and are ``async``;
    ``leaf_extras``/``group_extras``/``leaf_size`` only reshape data
    already fetched and stay synchronous.
    """

    root_name: str
    leaf_kind: UnitKind
    tables: tuple[str, ...]
    tree_factory: Callable[[SaasWorkloadProvider[StateT]], Awaitable[tuple[TreeStrategy, StateT]]]
    """Builds the tree, plus whatever the hooks below need prefetched once,
    with I/O (GWS mail labels, GWS contact groups, Site list create times),
    which becomes ``provider.state``."""
    content: Callable[[SaasWorkloadProvider[StateT], Row, Key], Awaitable[ContentSource]]
    """A leaf's content; raises ``NotRestorableError`` (``not_restorable``)
    for a row with nothing to restore."""
    object_names: dict[str, tuple[str, ...]] = dataclasses.field(default_factory=dict)
    """Maps a ``tables`` entry (a schema table name, e.g. ``"mail_table"``)
    to the index's own name(s) for the service DB defining it — plural
    because the name depends on ``sub_type`` (``"mail_table"`` is
    ``"mail_db"`` for ``USER_EXCHANGE`` but ``"group_mail_db"`` for
    ``GROUP_EXCHANGE``). Aliases are tried in order; the first the index
    has and that validates wins. A table with no entry here, or a version
    with no usable index, raises ``UnsupportedDataFormatError``."""
    leaf_extras: Callable[[SaasWorkloadProvider[StateT], Row], NodeExtras] = dataclasses.field(
        default=lambda provider, row: NodeExtras()
    )
    """A leaf's ``mtime``, ``columns`` and ``details`` from its own row (Drive's ``hash``
    column; GWS Mail's label names and GWS Contact's group names, read from
    ``provider.state``)."""
    group_extras: Callable[[SaasWorkloadProvider[StateT], Key], NodeExtras] = dataclasses.field(
        default=lambda provider, key: NodeExtras()
    )
    """A group node's extras (the root included), keyed by the group's tree
    key since no row exists at group level (Site's List roles and create
    times, Calendar's category ``leaf_kind``)."""
    leaf_size: Callable[[Row], int | None] = dataclasses.field(default=lambda row: None)
    """A leaf listing ``Node``'s ``size`` from its row, ``None`` when not
    cheaply known at listing time (only Drive and Site's document-library
    items have one)."""
    leaf_export_name: Callable[[SaasWorkloadProvider[StateT], Row], str] | None = None
    """A leaf's synthesized file name (``Node.export_name``) from its row,
    for a workload whose items have none of their own (Mail, Calendar,
    Contact); its ``RestorableUnit`` is named the same. Without one, the
    unit takes the listed node's name."""


@dataclasses.dataclass(frozen=True, slots=True)
class SaasHandle:
    """``Node.handle`` of every container node a ``TreeBackedProvider``
    builds, and of every ``SaasWorkloadProvider`` leaf: its tree
    ``key`` (``()`` for the root) and the ``row`` the tree listed it from
    (``TreeEntry.row``), which ``unit()`` builds the leaf from. Compared and
    hashed by ``key`` alone."""

    key: Key
    row: Row | None = dataclasses.field(default=None, compare=False)


def tree_key(node: Node) -> Key | None:
    """The tree key a ``TreeBackedProvider`` stored in ``node.handle``
    (``()`` for the root), or ``None`` when ``node.handle`` is not a
    ``SaasHandle``."""
    return node.handle.key if isinstance(node.handle, SaasHandle) else None


def tree_row(node: Node) -> Row | None:
    """The row ``node`` was listed from, or ``None`` (a synthetic group, the
    root, or a node a ``TreeBackedProvider`` didn't build)."""
    return node.handle.row if isinstance(node.handle, SaasHandle) else None


class TreeBackedProvider:
    """``children()`` for a provider whose tree is a ``TreeStrategy``:
    subclasses set ``_tree`` and build each listed entry's ``Node`` in
    ``_node_for``. Every container node carries a ``SaasHandle`` (key
    ``()`` for the root)."""

    _tree: TreeStrategy

    def _node_for(self, entry: TreeEntry) -> Node:
        raise NotImplementedError

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        key = tree_key(node)
        if key is None or node.is_leaf:
            return []
        return [self._node_for(entry) for entry in await self._tree.children_of(key, offset=offset, limit=limit)]


class SaasWorkloadProvider[StateT](AsyncClosing, TreeBackedProvider):
    """``UnitProvider`` shared by every config-driven SaaS workload,
    driven by the ``SaasWorkloadConfig`` a workload (Mail, Drive, ...)
    supplies. Build one with ``create``, not the constructor."""

    #: Populated by ``create``, the only supported constructor.
    _dedup_file: DedupFile
    _object_name_index: ObjectNameIndex | None
    _object_dbs: dict[str, ObjectDb]
    _sources: dict[str, SqliteSource]
    _tree: TreeStrategy
    state: StateT
    """What the config's ``tree_factory`` prefetched for its hooks."""

    def __init__(self, repo: DedupRepo, version: Version, config: SaasWorkloadConfig[StateT]) -> None:
        self._repo = repo
        self._version = version
        self._config = config
        # Initialized here so close() is safe even if create() fails early.
        self._object_dbs = {}
        self._sources = {}
        #: The index object id each ``_sources`` entry was opened from.
        self._source_object_ids: dict[str, str] = {}
        self._index_db: SharedIndexObjectDb | None = None
        # Single-flight, so two concurrent first uses take one hold, not two.
        self._held_index_db: AsyncOnce[ObjectDb] = AsyncOnce(self._acquire_index_db)

    @classmethod
    async def create(
        cls,
        repo: DedupRepo,
        version: Version,
        config: SaasWorkloadConfig[StateT],
        saas_streams: SaasStreamCache,
        *,
        shared: SharedSaasContext | None = None,
    ) -> Self:
        """Open the version's ``saas_obj`` and every configured table, then
        build the tree.

        Args:
            repo: The repository ``version`` belongs to.
            version: The SaaS version to open.
            config: The workload's configuration.
            saas_streams: Borrowed, not owned; the ``saas_obj`` is opened
                through it and stays cached there.
            shared: The version's context, resolved once by
                ``units/dispatch.py``'s ``saas_provider_for`` for every
                candidate; resolved here when omitted.

        Raises:
            UnsupportedDataFormatError: A configured table can't be
                resolved, or building the tree hits a schema mismatch.
            NotFoundError: ``shared`` is omitted and the version's
                ``saas_obj`` can't be located.
        """
        self = cls(repo, version, config)
        try:
            if shared is None:
                shared = await resolve_shared_saas_context(repo, version, saas_streams)
            self._dedup_file = shared.dedup_file
            # None when the version has no index (an older connector):
            # every table below is then unsupported.
            object_name_index = shared.object_name_index if config.object_names else None
            self._index_db = shared.index_object_db if object_name_index is not None else None
            self._object_name_index = object_name_index

            for table_name in config.tables:
                self._object_dbs[table_name], self._sources[table_name] = await self._open_table_via_index(table_name)

            try:
                self._tree, self.state = await config.tree_factory(self)
            except (sqlite3.DatabaseError, DataCorruptError) as exc:
                # Schema drift in a tree_factory's reads degrades like any
                # other non-match, so saas_provider_for's candidate loop
                # moves on instead of crashing.
                raise UnsupportedDataFormatError(
                    f"tree construction failed for version {version.version_uid!r}: {exc}", ref=version.version_uid
                ) from exc
        except BaseException as exc:
            # Nothing opened above may leak, including on a tree_factory failure.
            await close_preserving(exc, [self.close])
            raise
        return self

    async def _open_table_via_index(self, table_name: str) -> tuple[ObjectDb, SqliteSource]:
        """Open ``table_name`` through ``object_names[table_name]``'s
        aliases via ``resolve_service_db``, returning the open source.

        Raises:
            UnsupportedDataFormatError: No alias resolves and validates.
        """
        version = self._version
        object_name_index = self._object_name_index
        object_names = self._config.object_names.get(table_name, ())
        if object_name_index is None or not object_names:
            raise UnsupportedDataFormatError(
                f"no object-name index for table {table_name!r}, version {version.version_uid!r}",
                ref=version.version_uid,
            )
        try:
            object_db = await self._index_object_db()
        except DEGRADABLE_OPEN_ERRORS as exc:
            raise UnsupportedDataFormatError(
                f"object-name index for version {version.version_uid!r} did not validate: {exc}",
                ref=version.version_uid,
            ) from exc
        resolved = await resolve_service_db(self._dedup_file, object_db, object_name_index, object_names, table_name)
        if resolved is None:
            # ``object_db`` is the provider-wide index hold; ``close()`` releases it.
            raise UnsupportedDataFormatError(
                f"no object-name index entry for table {table_name!r} validated, version {version.version_uid!r}",
                ref=version.version_uid,
            )
        self._source_object_ids[table_name] = resolved.object_id
        return object_db, resolved.source

    async def _index_object_db(self) -> ObjectDb:
        """This provider's hold on the index ObjectDB, taken on first use."""
        return await self._held_index_db.get()

    async def _acquire_index_db(self) -> ObjectDb:
        assert self._index_db is not None
        return await self._index_db.acquire()

    async def _release_index_db(self) -> None:
        index_db = self._index_db
        if index_db is not None:
            await self._held_index_db.close(lambda _object_db: index_db.release())

    async def _optional_index_object_db(self) -> ObjectDb | None:
        """``_index_object_db``, or ``None`` when this version has no index or
        it doesn't validate — for the best-effort secondary lookups."""
        if self._index_db is None:
            return None
        try:
            return await self._index_object_db()
        except DEGRADABLE_OPEN_ERRORS:
            return None

    async def open_optional_table_via_index(self, table_name: str) -> aiosqlite.Connection | None:
        """Open an optional table that stays queryable (e.g. Mail's
        ``mail_folder_table``), resolved like a ``config.tables`` entry
        through ``object_names[table_name]``. Once open, ``table(table_name)``
        returns it and ``close()`` releases it.

        Returns:
            The table's connection, or ``None`` when it can't be found or
            validated.
        """
        if self._object_name_index is None:
            # Unreachable today: the only caller (mail.py) also declares
            # object_names for a required table, so create() always resolved
            # an index before this runs.
            return None  # pragma: no cover
        try:
            object_db, source = await self._open_table_via_index(table_name)
        except UnsupportedDataFormatError:
            return None
        self._object_dbs[table_name] = object_db
        self._sources[table_name] = source
        return source.connection

    @override
    async def close(self) -> None:
        """Close every SQLite source this provider owns and release its
        hold on the index ObjectDB; the borrowed stream stays open."""
        sources, self._sources = list(self._sources.values()), {}
        self._object_dbs = {}
        closers: list[Callable[[], Awaitable[object]]] = [s.close for s in sources]
        closers.append(self._release_index_db)
        await close_all(closers, "SaasWorkloadProvider.close() failed to close every connection")

    # -- accessors for TreeStrategy/content() implementations ------------

    def table(self, name: str) -> aiosqlite.Connection:
        return self._sources[name].connection

    async def read_object(self, table: str, object_id: str, *, expected_size: int | None = None) -> bytes:
        """``objectdb.read_object`` through ``table``'s ObjectDB."""
        return await read_object(self._object_dbs[table], self._dedup_file, object_id, expected_size=expected_size)

    async def object_view(self, table: str, object_id: str, key: Key) -> ByteRangeView:
        """A lazy view of ``object_id``'s bytes, located through ``table``'s
        ObjectDB. A stale index entry makes only the item at ``key``
        unrestorable (``NotRestorableError``)."""
        try:
            offset, length = await self._object_dbs[table].get(object_id)
        except NotFoundError:
            not_restorable("item", key)
        return self._dedup_file.view(offset, length)

    async def read_id_to_name_map(
        self, object_names: tuple[str, ...], table: str, *, id_column: str, name_column: str
    ) -> dict[str, str] | None:
        """``object_name_index.read_id_to_name_map`` over this version's
        index, reusing the provider's index ObjectDB."""
        return await read_id_to_name_map(
            self._dedup_file,
            self._object_name_index,
            object_names,
            table,
            id_column=id_column,
            name_column=name_column,
            object_db=await self._optional_index_object_db(),
        )

    async def read_grouped_names(
        self,
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
        """``object_name_index.read_grouped_names`` over this version's
        index, reusing the provider's index ObjectDB, and the membership DB
        itself when a configured table already opened it (GWS's ``mail_db``
        and ``contact_db``), rather than materializing it a second time."""
        held = self._source_opened_from(membership_names)
        return await read_grouped_names(
            self._dedup_file,
            self._object_name_index,
            object_db=await self._optional_index_object_db(),
            membership_connection=held.connection if held is not None else None,
            definition_names=definition_names,
            definition_table=definition_table,
            id_column=id_column,
            name_column=name_column,
            membership_names=membership_names,
            membership_table=membership_table,
            item_column=item_column,
            group_column=group_column,
        )

    def _source_opened_from(self, object_names: tuple[str, ...]) -> SqliteSource | None:
        """An already-open source holding the object ``object_names`` resolves
        to first (its first alias the index has), if any."""
        index = self._object_name_index
        if index is None:
            return None
        object_id = next((index.object_ids[n] for n in object_names if n in index.object_ids), None)
        for table_name, opened_id in self._source_object_ids.items():
            if opened_id == object_id and table_name in self._sources:
                return self._sources[table_name]
        return None

    @property
    def version(self) -> Version:
        return self._version

    @property
    def is_m365(self) -> bool:
        """Whether this version is M365 rather than GWS (Google Workspace),
        the only two SaaS target types."""
        return self.version.target_type == TargetType.M365

    @property
    def repo(self) -> DedupRepo:
        return self._repo

    def ref_for(self, key: Key) -> NodeRef:
        """The canonical ref for tree ``key`` — the same one this provider
        gives the listed ``Node``."""
        return canonical_ref_for(self._repo, self._version, key)

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        return self._group_node((), self._config.root_name, None)

    def _group_node(self, key: Key, name: str, row: Row | None) -> Node:
        # leaf_kind at every depth, so a caller knows a folder's leaf kind
        # without listing it (even when it's empty).
        extras = self._config.group_extras(self, key)
        return Node(
            ref=self.ref_for(key),
            name=name,
            is_leaf=False,
            mtime=extras.mtime,
            leaf_kind=extras.leaf_kind or self._config.leaf_kind,
            role=extras.role,
            columns=extras.columns,
            details=extras.details,
            handle=SaasHandle(key, row),
        )

    def _leaf_node(self, key: Key, name: str, row: Row | None) -> Node:
        config = self._config
        extras = config.leaf_extras(self, row) if row is not None else NodeExtras()
        return Node(
            ref=self.ref_for(key),
            name=name,
            is_leaf=True,
            kind=config.leaf_kind,
            size=config.leaf_size(row) if row is not None else None,
            mtime=extras.mtime,
            columns=extras.columns,
            details=extras.details,
            export_name=(
                config.leaf_export_name(self, row) if row is not None and config.leaf_export_name is not None else None
            ),
            handle=SaasHandle(key, row),
        )

    @override
    def _node_for(self, entry: TreeEntry) -> Node:
        if not entry.is_leaf:
            return self._group_node(entry.key, entry.name, entry.row)
        return self._leaf_node(entry.key, entry.name, entry.row)

    async def unit(self, node: Node) -> RestorableUnit:
        key, row = tree_key(node), tree_row(node)
        if key is None or row is None or not node.is_leaf:
            not_restorable("node", node.name)
        leaf = self._leaf_node(key, node.name, row)
        return RestorableUnit.of(leaf, await self._config.content(self, row, key), name=leaf.export_name or leaf.name)


class RecursiveTreeSaasProvider[StateT](SaasWorkloadProvider[StateT]):
    """A ``SaasWorkloadProvider`` that implements
    ``SupportsDirectRefLookup``, for a config whose ``tree_factory``
    returns a ``RecursiveTree`` (Drive's depth-independent ``item_id``
    refs). Kept a subclass so the ``isinstance`` check stays false for
    every other SaaS provider."""

    @property
    def _recursive_tree(self) -> RecursiveTree:
        assert isinstance(self._tree, RecursiveTree), "make_saas_provider pairs this class with a RecursiveTree config"
        return self._tree

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None:
        if len(extra_segments) != 1:
            return None
        entry = await self._recursive_tree.resolve_id(extra_segments[0])
        return self._node_for(entry) if entry is not None else None

    async def parent_of(self, node: Node) -> Node | None:
        row = tree_row(node)
        if not tree_key(node) or row is None:
            return None  # node is already the version root, or not one of ours
        tree = self._recursive_tree
        parent_id = tree.parent_id_of(row)
        if parent_id is None:
            return self.root()
        entry = await tree.resolve_id(parent_id)
        return self._node_for(entry) if entry is not None else None


class SaasProviderFactory[ProviderT](Protocol):
    """The calling convention of every SaaS provider factory ``units/
    dispatch.py`` tries (``make_saas_provider``'s factories,
    ``TeamsChatProvider.create``): ``await factory(repo, version,
    saas_streams, shared=...)``. A ``Protocol`` because a ``Callable`` alias
    can't express a keyword-only parameter."""

    def __call__(
        self,
        repo: DedupRepo,
        version: Version,
        saas_streams: SaasStreamCache,
        *,
        shared: SharedSaasContext | None = None,
    ) -> Awaitable[ProviderT]: ...


def make_saas_provider[StateT](
    config: SaasWorkloadConfig[StateT],
    *,
    name: str,
    provider_cls: type[SaasWorkloadProvider[Any]] = SaasWorkloadProvider,
) -> SaasProviderFactory[SaasWorkloadProvider[StateT]]:
    """Build an async ``SaasProviderFactory`` over ``config``, forwarding to
    ``provider_cls.create``.

    ``name`` becomes the factory's ``__name__``/``__qualname__``, so a
    traceback names the specific provider. ``provider_cls`` is
    ``RecursiveTreeSaasProvider`` for a ``RecursiveTree`` config.
    """

    async def _provider(
        repo: DedupRepo, version: Version, saas_streams: SaasStreamCache, *, shared: SharedSaasContext | None = None
    ) -> SaasWorkloadProvider[StateT]:
        provider: SaasWorkloadProvider[StateT] = await provider_cls.create(
            repo, version, config, saas_streams, shared=shared
        )
        return provider

    _provider.__name__ = name
    _provider.__qualname__ = name
    return _provider
