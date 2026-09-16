"""``SaasWorkloadProvider`` + ``SaasWorkloadConfig``: the shared
skeleton Mail/Drive/Contact/Calendar/Site all reduce to. Every SaaS
provider does the same four things — open the version's ``saas_obj``,
locate and open its service-level DB snapshot(s) by table name via the
connector's own object-name index (see ``object_name_index.py``), build a
``TreeStrategy`` over whichever shape its tree has, and answer
``root()``/``children()``/``unit()`` purely by delegating to it — so
``SaasWorkloadProvider`` is the only place those three methods are
implemented; each concrete workload supplies only a
``SaasWorkloadConfig`` (see its own docstring for the fields) plus
whatever helpers its ``assemble()`` needs.

**No schema-classification discovery scan, anywhere, for any table**:
every table is resolved by a direct object-name-index lookup (see
``SaasWorkloadProvider._open_table_via_index``); a table simply
isn't found when the index doesn't name it. A schema-only scan could
never safely replace this anyway — Archive Mail's ``mail_table`` is one
such case (``mail.py``'s own module docstring), where only the
index's own naming can tell two schema-identical tables apart.

``TeamsChatProvider`` is **not** built on this base — see
``units/saas/teams_chat.py``'s own module docstring for why.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Self, cast

import aiosqlite

from ...catalog.version import Version
from ...catalog.workload import TargetType
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from ...errors import DataCorruptError, UnsupportedDataFormatError
from ...storage.sqlite_source import SqliteSource
from ..base import (
    ClosableUnitProvider,
    Node,
    RestorableUnit,
    UnitKind,
    not_restorable,
    paginate,
)
from ..node_ref import NodeRef, canonical_ref_for
from .object_name_index import ObjectNameIndex, resolve_object_name_index, resolve_service_db
from .objectdb import ObjectDb
from .stream import SaasStream
from .tree_strategy import RecursiveTree, TreeStrategy

_Row = dict[str, object | None]
_Key = tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class SharedSaasContext:
    """What every sibling candidate for a multi-candidate
    ``Workload.sub_type`` (M365's ``USER_EXCHANGE``/``GROUP_EXCHANGE`` —
    see ``units/dispatch.py``'s ``_SAAS_SUB_TYPE_CANDIDATES``) would
    otherwise each independently resolve from the exact same ``(repo,
    version)``: the version's ``saas_obj`` and its object-name index.
    Resolved once via ``resolve_shared_saas_context`` and handed to
    every candidate factory (see ``SaasWorkloadProvider.create``'s
    ``shared`` parameter), instead of each of the 3-4 siblings paying
    for its own ``open_saas_obj``/``resolve_object_name_index`` call for a
    guaranteed byte-identical result.

    Neither field needs closing: ``DedupFile`` has no ``close()`` at
    all (a stateless, explicit-offset read view), and ``ObjectNameIndex``
    is a plain, connection-free dataclass. Only the resolution
    machinery itself (a throwaway ``SaasStream``) owns anything that
    needs releasing, and ``resolve_shared_saas_context`` already does
    that before returning."""

    dedup_file: DedupFile
    object_name_index: ObjectNameIndex | None


async def resolve_shared_saas_context(repo: DedupRepo, version: Version) -> SharedSaasContext:
    """Resolve the one ``(dedup_file, object_name_index)`` pair every
    candidate for a multi-candidate ``sub_type`` would otherwise
    resolve independently for itself — see ``SharedSaasContext``'s own
    docstring for why sharing it needs no ownership/refcounting scheme.
    The ``SaasStream`` used here is closed immediately after
    ``open_saas_obj`` returns:
    the two ``SqliteSource`` connections it owns
    (``stream_version_for``'s snapshot/version lookups) are unrelated
    to the returned ``DedupFile``, which is built from the repository's
    own long-lived ``Pool``/``CompositionReader`` instead (see
    ``DedupRepo.open_composition``) — so this stream has nothing
    left to do once the ``DedupFile`` is in hand."""
    async with SaasStream(repo, version.connection_config_id, version.saas_stream_uuid) as stream:
        dedup_file = await stream.open_saas_obj(version)
    object_name_index = await resolve_object_name_index(repo, version)
    return SharedSaasContext(dedup_file=dedup_file, object_name_index=object_name_index)


@dataclasses.dataclass(frozen=True)
class SaasWorkloadConfig:
    """What one workload needs beyond the shared skeleton: which
    ``tables`` to open (resolved via ``object_names`` — see that
    field's own docstring), a ``tree_factory`` that builds the single
    ``TreeStrategy`` ``children()``/``unit()`` delegate to once every
    table is open, and an ``assemble()`` callback turning one leaf row
    into a ``RestorableUnit``.

    ``tree_factory``/``assemble`` are ``async def`` because some
    workloads' implementations genuinely need I/O (Drive's
    ``tree_factory`` looks up a config row; Drive's/Site's ``assemble``
    read a content or META object); ``extra_attrs``/``group_attrs``/
    ``leaf_size`` (below) never do and stay plain functions.
    """

    root_name: str
    leaf_kind: UnitKind
    tables: tuple[str, ...]
    tree_factory: Callable[[SaasWorkloadProvider], Awaitable[TreeStrategy]]
    assemble: Callable[[SaasWorkloadProvider, _Row, _Key], Awaitable[RestorableUnit]]
    object_names: dict[str, tuple[str, ...]] = dataclasses.field(default_factory=dict)
    """Maps a ``tables`` entry (a schema table name, e.g. ``"mail_table"``)
    to the *index's own* name(s) for the service DB defining it —
    plural because the same schema table can live under a different
    index name depending on the workload's ``sub_type``
    (``"mail_table"`` is ``"mail_db"`` for ``USER_EXCHANGE`` but
    ``"group_mail_db"`` for ``GROUP_EXCHANGE``, both real). Every alias
    is tried in order; the first one the index has *and* validates
    wins. A table with no entry here — or a stream with no usable
    object-name index at all — is simply not found: ``UnsupportedDataFormatError``,
    never a scan (see this module's own docstring for why)."""
    extra_attrs: Callable[[SaasWorkloadProvider, _Row], dict[str, object]] = dataclasses.field(
        default=lambda provider, row: {}
    )
    """Reshapes a leaf's own row into extra display-metadata fields
    (Drive's ``hash`` column; GWS Mail's label names; GWS Contact's
    group names — the latter two read from ``provider.extras``, a
    scratch dict ``tree_factory`` populates once, up front, with
    whatever prefetched data a purely-row-based function can't compute
    on its own). Stays synchronous: the prefetch is what needs I/O,
    done once in ``tree_factory`` (already ``async``) — reshaping an
    already-fetched row never does."""
    group_attrs: Callable[[SaasWorkloadProvider, _Key], dict[str, object]] = dataclasses.field(
        default=lambda provider, key: {}
    )
    """Same idea as ``extra_attrs``, for a non-leaf (group) node's own
    ``attrs``: no row exists at group level, so this reads whatever
    ``tree_factory`` stashed in ``provider.extras`` keyed by the
    group's own key. Only Site needs this today (flagging which
    List-shaped groups get a spreadsheet-style overview); every other
    workload leaves it at the default no-op."""
    leaf_size: Callable[[_Row], int | None] = dataclasses.field(default=lambda row: None)
    """Populates a leaf listing ``Node``'s own ``size`` (not the
    ``RestorableUnit`` ``assemble()`` builds separately) — only Drive's
    ``item_table`` and Site's ``item_version_table`` (document-library
    items' cached ``value1`` column; a general List row's real size is
    only known once its content is assembled) have one cheaply
    available at listing time. Every other workload leaves this at the
    default (``None``)."""


class SaasWorkloadProvider:
    """``UnitProvider`` shared by every config-driven SaaS
    application-layer workload. See this module's own docstring for
    what's shared vs supplied.

    Build one with ``create``, never ``SaasWorkloadProvider(...)``
    directly: everything ``create`` does — opening the ``saas_obj``,
    resolving extents, opening each configured service DB, building the
    tree — is I/O and can't happen in a synchronous constructor."""

    #: Populated by ``create``, the only supported constructor.
    _dedup_file: DedupFile
    _object_name_index: ObjectNameIndex | None
    _object_dbs: dict[str, ObjectDb]
    _object_db_cache: dict[tuple[int, int], ObjectDb]
    _sources: dict[str, SqliteSource]
    _tree: TreeStrategy

    def __init__(self, repo: DedupRepo, version: Version, config: SaasWorkloadConfig) -> None:
        """Pure field initialization; ``create`` does the real work."""
        self._repo = repo
        self._version = version
        self._config = config
        self._stream = SaasStream(repo, version.connection_config_id, version.saas_stream_uuid)
        #: Scratch space a config's ``tree_factory`` populates once
        #: (up front, with I/O) for its ``extra_attrs`` to read later
        #: (per row, no I/O) — see ``SaasWorkloadConfig.extra_attrs``'s
        #: own docstring for why this exists at all.
        self.extras: dict[str, object] = {}
        #: Populated by ``create``; initialized here (rather than there)
        #: so ``close()`` is always safe to call even if ``create`` fails
        #: before opening any table.
        self._object_dbs = {}
        self._object_db_cache = {}
        self._sources = {}

    @classmethod
    async def create(
        cls,
        repo: DedupRepo,
        version: Version,
        config: SaasWorkloadConfig,
        *,
        shared: SharedSaasContext | None = None,
    ) -> Self:
        """``shared``, when given, is a ``SharedSaasContext`` a caller
        (``units/dispatch.py::saas_provider_for``, for a multi-candidate
        ``sub_type``) already resolved once for this exact ``(repo,
        version)`` — skips this instance's own
        ``self._stream.open_saas_obj``/``resolve_object_name_index`` calls
        in favor of reusing it directly. ``self._stream`` is then never
        used to open anything, so ``close``'s existing
        ``await self._stream.close()`` call stays a safe no-op with no
        special-casing needed — ``SaasStream.close`` only ever releases
        connections it actually opened, and neither
        ``_snapshot_source`` nor ``_version_source`` is opened on this
        path."""
        self = cls(repo, version, config)
        try:
            if shared is not None:
                self._dedup_file = shared.dedup_file
                object_name_index = shared.object_name_index if config.object_names else None
            else:
                self._dedup_file = await self._stream.open_saas_obj(version)

                # The sole location-resolution mechanism — see this module's own
                # docstring for why there's no schema-classification scan as a
                # fallback. None whenever config.object_names is empty (no workload here
                # is configured without it), or this version simply has no index
                # at all (see
                # object_name_index.resolve_object_name_index's own docstring for every
                # such case) — either way every table below is then unsupported.
                # Stashed on self (see the object_name_index property below) so a
                # config's own tree_factory/assemble callbacks can reuse this
                # exact resolution — same repository, same version, guaranteed
                # byte-identical to whatever a second call would produce —
                # instead of each independently re-resolving it.
                object_name_index = await resolve_object_name_index(repo, version) if config.object_names else None
            self._object_name_index = object_name_index

            for table_name in config.tables:
                self._object_dbs[table_name], self._sources[table_name] = await self._open_table_via_index(
                    table_name, object_name_index, version
                )

            try:
                self._tree = await config.tree_factory(self)
            except (sqlite3.DatabaseError, DataCorruptError) as exc:
                # A tree_factory reading its own secondary table (Drive's
                # config_table, Site's list_version_table, ...) hits the
                # same "schema doesn't match what this connector version
                # expects" shape _open_table_via_index already converts
                # above — convert it identically here so one candidate's
                # schema drift degrades like any other non-match instead
                # of crashing saas_provider_for's whole dispatch loop (see
                # units/dispatch.py's own per-candidate UnsupportedDataFormatError
                # handling).
                raise UnsupportedDataFormatError(
                    f"tree construction failed for version {version.version_uid!r}: {exc}", ref=version.version_uid
                ) from exc
        except Exception:
            # Nothing opened above (self._stream, self._sources,
            # self._object_db_cache) must leak on any failure here,
            # including a tree_factory failure — same shape as
            # RawObjectProvider.create()/TeamsChatProvider.create().
            await self.close()
            raise
        return self

    async def _open_table_via_index(
        self, table_name: str, object_name_index: ObjectNameIndex | None, version: Version
    ) -> tuple[ObjectDb, SqliteSource]:
        """Resolve ``table_name`` straight from the index's recorded
        ``(offset, length)`` and named ``object_id`` — no scan, no
        per-object schema classification. Validates the resolved
        object before trusting it (decompresses, is really SQLite,
        defines ``table_name``). Tries every alias in
        ``object_names[table_name]`` in order (mutually-exclusive
        product-variant names, not scan guesses — see that field's own
        docstring) against the same loaded object, via
        ``resolve_service_db`` — the same alias-resolution loop
        ``read_indexed_table`` uses, except the resolved
        ``SqliteSource`` is returned to the caller rather than read
        once and closed.

        ``object_name_index`` is the same, unchanged object across every
        ``table_name`` in one ``create`` call — a multi-table config
        (Calendar, Site) would otherwise re-materialize the identical
        embedded ObjectDB once per table for no reason. ``self.
        _object_db_cache``, keyed by ``(offset, length)``, makes the
        second and later table in such a config reuse the first's
        already-loaded instance instead; ``close`` owns closing every
        cached instance exactly once, whether zero, one, or every table
        ultimately claimed it.

        Raises:
            UnsupportedDataFormatError: No alias resolves — never returns
                ``None``.
        """
        object_names = self._config.object_names.get(table_name, ())
        if object_name_index is None or not object_names:
            raise UnsupportedDataFormatError(
                f"no object-name index for table {table_name!r}, version {version.version_uid!r}",
                ref=version.version_uid,
            )
        cache_key = (object_name_index.offset, object_name_index.length)
        object_db = self._object_db_cache.get(cache_key)
        if object_db is None:
            try:
                object_db = await ObjectDb.load(self._dedup_file, object_name_index.offset, object_name_index.length)
            except (sqlite3.DatabaseError, DataCorruptError) as exc:
                raise UnsupportedDataFormatError(
                    f"object-name index for version {version.version_uid!r} did not validate: {exc}",
                    ref=version.version_uid,
                ) from exc
            self._object_db_cache[cache_key] = object_db
        source = await resolve_service_db(self._dedup_file, object_db, object_name_index, object_names, table_name)
        if source is None:
            # Not closing ``object_db`` here: it's owned by
            # ``self._object_db_cache`` (keyed by (offset, length), not by
            # this one table_name) — a sibling table sharing the same
            # cache_key may still need it, and ``close()`` reclaims it
            # exactly once regardless.
            raise UnsupportedDataFormatError(
                f"no object-name index entry for table {table_name!r} validated, version {version.version_uid!r}",
                ref=version.version_uid,
            )
        return object_db, source

    async def open_optional_table_via_index(self, table_name: str) -> aiosqlite.Connection | None:
        """Best-effort sibling of ``_open_table_via_index``: same
        object-name-index resolution (``self._config.object_names[table_name]``
        — the same aliases a *required* ``config.tables`` entry would use),
        but returns ``None`` instead of raising when it can't be found or
        validated — for a ``tree_factory`` that wants a persistently
        queryable *optional* secondary table (repeated ``WHERE``-filtered
        queries across many later ``children_of()`` calls — e.g. mail.py's
        own real ``mail_folder_table`` hierarchy), not
        ``object_name_index.read_indexed_table``'s one-shot full-table read.

        A table this resolves is registered into the same ``self._sources``/
        ``self._object_dbs`` a required ``config.tables`` entry already
        uses — ``self.table(table_name)`` works from then on, and ``close()``
        already covers releasing it, with no separate bookkeeping needed.
        """
        if self._object_name_index is None:
            # Unreachable via any current real caller: every config that
            # calls this method (mail.py's own optional folder-hierarchy
            # table) also declares object_names for its *required* table,
            # which `create()` already resolves an index for up front —
            # this guard only matters for a hypothetical future config
            # with an entirely empty `object_names`.
            return None  # pragma: no cover
        try:
            object_db, source = await self._open_table_via_index(table_name, self._object_name_index, self._version)
        except UnsupportedDataFormatError:
            return None
        self._object_dbs[table_name] = object_db
        self._sources[table_name] = source
        return source.connection

    async def close(self) -> None:
        """Release every sqlite connection this provider owns: ``_sources``,
        ``_object_db_cache``, and ``_stream`` — ``SaasStream`` holds
        two more ``SqliteSource``s of its own. Any unclosed aiosqlite
        connection hangs interpreter shutdown (see
        ``ARCHITECTURE.md``'s "Async-native, by design"). Closes
        ``_object_db_cache`` rather than ``_object_dbs``: a multi-table
        config can have several ``_object_dbs`` keys pointing at the
        same cached instance (see ``_open_table_via_index``), and
        ``_object_db_cache``'s own ``(offset, length)`` keying already
        de-duplicates that for us.
        """
        for source in self._sources.values():
            await source.close()
        for object_db in self._object_db_cache.values():
            await object_db.close()
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

    # -- accessors for TreeStrategy/assemble() implementations -----------

    def table(self, name: str) -> aiosqlite.Connection:
        return self._sources[name].connection

    def object_db(self, name: str) -> ObjectDb:
        return self._object_dbs[name]

    @property
    def dedup_file(self) -> DedupFile:
        return self._dedup_file

    @property
    def object_name_index(self) -> ObjectNameIndex | None:
        """The same ``ObjectNameIndex`` resolution ``create`` already did
        once — ``None`` under the exact same conditions documented
        there. A config's own ``tree_factory``/``assemble`` callbacks
        that need to look up a secondary table via
        ``read_indexed_table`` (``mail.py``'s folder-name/label
        lookups, ``contact.py``'s equivalents) should read this rather
        than calling ``resolve_object_name_index`` again — same repository, same
        version, guaranteed identical result, so a second call only
        pays for a repeat SQL query and JSON parse (an extra vault-key
        decrypt too, on an encrypted repository) for nothing."""
        return self._object_name_index

    @property
    def version(self) -> Version:
        return self._version

    @property
    def is_m365(self) -> bool:
        """Whether this provider's version is an M365 (Microsoft 365)
        workload rather than GWS (Google Workspace) — the only two
        ``target_type`` values a SaaS workload provider's ``version`` can
        ever carry (device workloads never reach this class). Shared by
        ``mail.py``/``contact.py``'s config-specific ``assemble``
        callbacks, which each need this same check."""
        return self.version.target_type == TargetType.M365

    @property
    def repo(self) -> DedupRepo:
        return self._repo

    def ref_for(self, key: _Key) -> NodeRef:
        """Public — ``assemble()`` callbacks (the per-workload config,
        living outside this class) need to build a ``RestorableUnit``
        with the *same* ref this class already built for the ``Node``
        it was handed, not reach into this class's own layout/version
        fields to rebuild it independently."""
        return canonical_ref_for(self._repo, self._version, key)

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        """Pure construction — no I/O, so this stays synchronous (see
        ``UnitProvider``)."""
        return Node(ref=self.ref_for(()), name=self._config.root_name, is_leaf=False, attrs={"key": ()})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        # A node with no "key" attr at all is not one this provider ever
        # built (some other provider's node handed back here by mistake,
        # or a test's deliberately bare Node) — distinct from the *real*
        # root, whose "key" is always present and set to () (root() sets
        # it explicitly, above).
        if "key" not in node.attrs:
            return []
        key = tuple(node.attrs["key"])
        # offset/limit are threaded straight into children_of() itself —
        # a real SQL-level window, not a post-hoc Python slice of an
        # eagerly-fetched full list; see tree_strategy.py's own module
        # docstring for why.
        entries = await self._tree.children_of(key, offset=offset, limit=limit)
        return [self._node_for(child_key, name, is_leaf) for child_key, name, is_leaf in entries]

    def _node_for(self, key: _Key, name: str, is_leaf: bool) -> Node:
        # Stays synchronous: ``row_for()`` is a lookup into the index
        # ``children_of()`` just populated, and every config hook used here
        # (``extra_attrs``/``leaf_size``/``group_attrs``) only reshapes that row
        # (or, for ``group_attrs``, whatever ``tree_factory`` already stashed
        # in ``provider.extras`` keyed by this same key) — none of them do
        # I/O of their own.
        if not is_leaf:
            group_attrs: dict[str, object] = {"key": key, **self._config.group_attrs(self, key)}
            return Node(ref=self.ref_for(key), name=name, is_leaf=False, attrs=group_attrs)
        row = self._tree.row_for(key)
        attrs: dict[str, object] = {"key": key}
        size = None
        if row is not None:
            attrs.update(self._config.extra_attrs(self, row))
            size = self._config.leaf_size(row)
        return Node(ref=self.ref_for(key), name=name, is_leaf=True, kind=self._config.leaf_kind, size=size, attrs=attrs)

    async def unit(self, node: Node) -> RestorableUnit:
        key = tuple(node.attrs.get("key", ()))
        row = self._tree.row_for(key)
        if row is None:
            not_restorable("node", node.name)
        return await self._config.assemble(self, row, key)


class RecursiveTreeSaasProvider(SaasWorkloadProvider):
    """Built only by a config whose ``tree_factory`` returns a
    ``RecursiveTree`` (Drive's flat, depth-independent ``item_id``
    addressing) — the one SaaS shape whose ``extra_segments`` carries
    no prefix relationship for ``units/resolve.py``'s generic descent
    to use, so it implements ``SupportsDirectRefLookup`` instead. Every
    other SaaS provider stays a plain ``SaasWorkloadProvider``, which
    doesn't define these two methods at all — they can't live there
    unconditionally: ``isinstance(provider, SupportsDirectRefLookup)``
    would then wrongly say yes for Mail/Contact/Calendar/Site too,
    none of which this applies to."""

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None:
        if len(extra_segments) != 1:
            return None
        tree = cast(RecursiveTree, self._tree)
        entry = await tree.resolve_id(extra_segments[0])
        if entry is None:
            return None
        key, name, is_leaf = entry
        return self._node_for(key, name, is_leaf)

    async def parent_of(self, node: Node) -> Node | None:
        key = tuple(node.attrs.get("key", ()))
        if not key:
            return None  # node is already the version root
        tree = cast(RecursiveTree, self._tree)
        parent_id = tree.parent_id_of(key)
        if parent_id is None:
            return self.root()
        entry = await tree.resolve_id(parent_id)
        if entry is None:
            return None
        parent_key, name, is_leaf = entry
        return self._node_for(parent_key, name, is_leaf)


def group_display_name_resolver(names: dict[str, str] | None) -> Callable[[str], str]:
    """Builds a ``group_display_name`` callable for
    ``SyntheticGroupedTree``: looks a group key up in ``names`` when
    one resolved, falls back to the raw key otherwise — the same
    "costs a label, never correctness" degradation ``mail.py``/
    ``contact.py``'s own folder/group name resolvers document. Shared
    since both build this identical closure around their own,
    differently-sourced ``names`` map."""

    def _group_display_name(group_id: str) -> str:
        return names[group_id] if names is not None and group_id in names else group_id

    return _group_display_name


def extras_attr(provider: SaasWorkloadProvider, extras_key: str, row_id: object, attr_name: str) -> dict[str, object]:
    """One ``SaasWorkloadConfig.extra_attrs`` callback's whole body: read
    ``provider.extras[extras_key]`` (a ``tree_factory``-populated ``{row
    id: [value, ...]}`` map — GWS mail labels, GWS contact groups, ...),
    look ``row_id`` up in it, and return ``{attr_name: value}`` — or
    ``{}`` when the extras entry is missing/not a dict, or the lookup
    finds nothing. Shared since ``mail.py``/``contact.py`` each build
    this identical shape around their own ``extras_key``/``attr_name``.
    """
    values = provider.extras.get(extras_key)
    if not isinstance(values, dict):
        return {}
    found = values.get(str(row_id))
    return {attr_name: found} if found else {}


def make_saas_provider(
    config: SaasWorkloadConfig, *, name: str, provider_cls: type[SaasWorkloadProvider] = SaasWorkloadProvider
) -> Callable[..., Awaitable[SaasWorkloadProvider]]:
    """Build a constructor-style async factory over ``config`` — called
    exactly like a constructor (``await XProvider(repo, version)``),
    matching ``units/dispatch.py``'s ``_ProviderFactory`` calling
    convention. Opens the ``saas_obj`` and resolves the service DB via
    the connector's own object-name index, never a scan — see
    ``units/saas/raw_object.py``'s own module docstring. ``shared``, when
    given (M365 ``USER_EXCHANGE``/``GROUP_EXCHANGE``'s multi-candidate
    dispatch — see ``units/dispatch.py::saas_provider_for``), is passed
    straight through to ``SaasWorkloadProvider.create``.

    ``name`` sets the returned callable's ``__name__``/``__qualname__``,
    so a traceback still names the specific provider (``MailProvider``,
    ...) rather than this function's own generic inner closure — every
    one of ``mail.py``'s/``contact.py``'s/``calendar.py``'s/``drive.py``'s/
    ``site.py``'s provider factories is one call to this function, not a
    hand-written 3-line ``async def`` wrapper of its own.

    ``provider_cls`` defaults to plain ``SaasWorkloadProvider``;
    ``drive.py`` passes ``RecursiveTreeSaasProvider`` instead, the one
    config whose tree needs that subclass's extra capability.
    """

    async def _provider(
        repo: DedupRepo, version: Version, *, shared: SharedSaasContext | None = None
    ) -> SaasWorkloadProvider:
        return await provider_cls.create(repo, version, config, shared=shared)

    _provider.__name__ = name
    _provider.__qualname__ = name
    return _provider


class CompositeSaasProvider:
    """``UnitProvider`` for the one shape no single-app provider covers:
    M365's ``USER_EXCHANGE``/
    ``GROUP_EXCHANGE`` sub_types bundle Mail, Contacts and Calendars as
    three independently-populated services within the same version, all
    reachable at once — not alternatives to pick between. Every other
    SaaS sub_type maps 1:1 to one provider; this class is what a caller
    gets instead when more than one sub-provider recognizes the same
    version: a synthetic root (``"Exchange"``) whose children are each
    sub-provider's own root as siblings, with every deeper
    ``children()``/``unit()`` call routed back to whichever
    sub-provider owns that node.

    **Ref/key prefixing, not object identity**: every ``Node`` handed
    out gets ``attrs["key"]`` rewritten to ``(tag, *original_key)``
    (and its ``ref`` rebuilt to match), with the ``tag`` segment
    stripped back off before a node reaches the sub-provider that built
    it — a ``NodeRef`` round-tripped through ``str()``/``parse()``
    carries this prefix as an ordinary extra segment, so it resolves
    correctly on a fresh lookup like any other multi-segment ref."""

    def __init__(
        self,
        repo: DedupRepo,
        version: Version,
        sub_providers: dict[str, ClosableUnitProvider],
    ) -> None:
        self._repo = repo
        self._version = version
        self._sub_providers = sub_providers

    async def close(self) -> None:
        for provider in self._sub_providers.values():
            await provider.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def _ref_for(self, extra: tuple[str, ...]) -> NodeRef:
        return canonical_ref_for(self._repo, self._version, extra)

    def _tag_node(self, node: Node, tag: str, rest: tuple[str, ...]) -> Node:
        key = (tag, *rest)
        return dataclasses.replace(node, ref=self._ref_for(key), attrs={**node.attrs, "key": key})

    def root(self) -> Node:
        return Node(ref=self._ref_for(()), name="Exchange", is_leaf=False, attrs={"key": ()})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        key = tuple(node.attrs.get("key", ()))
        if key == ():
            tops = [self._tag_node(provider.root(), tag, ()) for tag, provider in self._sub_providers.items()]
            return paginate(tops, offset, limit)
        tag, *rest = key
        provider = self._sub_providers.get(tag)
        if provider is None:
            return []
        sub_node = dataclasses.replace(node, attrs={**node.attrs, "key": tuple(rest)})
        children = await provider.children(sub_node, offset, limit)
        return [self._tag_node(child, tag, tuple(child.attrs.get("key", ()))) for child in children]

    async def unit(self, node: Node) -> RestorableUnit:
        key = tuple(node.attrs.get("key", ()))
        if not key:
            not_restorable("node", node.name)
        tag, *rest = key
        provider = self._sub_providers[tag]
        sub_node = dataclasses.replace(node, attrs={**node.attrs, "key": tuple(rest)})
        return await provider.unit(sub_node)
