"""``SaasStream``: the browsing layer shared by every SaaS workload.

A "stream" is ``saas/<connection_config_id>/<streamUuid>/`` — its own
``db/{saas_snapshot,saas_version}`` (plain SQLite, never ``aHlT``-
enveloped even on an encrypted repository, unlike ``copy_meta_file``'s
per-file envelope) track *snapshots* (one per distinct backup job) and
*versions* (one per run of that job) independently of
``db/copy_target_version``.

A catalog ``Version`` carries no key directly usable against
``file_map``; resolving it to the actual ``saas_obj`` means two SQLite
lookups (``SaasStream.stream_version_for``) followed by a ``file_map``
path with an ambiguous middle segment (``SaasStream.open_saas_obj``).

``stream_version`` is a monotonically increasing *per-stream* generation
counter, not one-per-catalog-``Version`` — several catalog ``Version``
rows can share one (multiple application-layer backups landing in one
write session before a checkpoint). ``version_info.stream_version`` only
records which generation a given version was written into; it is not a
live pointer to where that content currently resides. A later generation's
composition record is always a superset of an earlier one's (unwritten
regions are inherited via the ``INHERIT`` bit, not re-written) — see
FORMAT-SPEC.md §7.3 for the rotation/GC behavior this implies for a reader
(server-side generation GC; see workload-format/saas-obj.md §3/§11.4.6).
So a catalog ``Version`` whose own recorded ``stream_version`` no longer
has a ``file_map`` row is expected, routine rotation, not necessarily
data loss — ``open_saas_obj`` forward-resolves to the nearest later
generation that's still live (bounded by ``stream_info.
latest_complete_version``, to exclude not-yet-committed generations)
rather than requiring the literal requested one to still exist.

Constructing a ``SaasStream`` directly, outside ``SaasStreamCache``,
discards this per-instance resolution cache — almost every caller should
go through ``SaasStreamCache``, not build one directly.
"""

from __future__ import annotations

import bisect
import contextlib
import sqlite3
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Self

import aiosqlite

from ...asynccache import AsyncKeyedCache
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import FILE_MAP_STATUS_COMPLETE, DedupRepo
from ...errors import DataCorruptError, NotFoundError
from ...identifiers import ConnectionConfigId, StreamUuid, VersionUid
from ...storage.base import join_path
from ...storage.dircache import DirCache
from ...storage.seqid import resolve_seq_file
from ...storage.sqlite_source import SqliteSource
from ...storage.table import Column, Table, as_int

#: ``saas_obj``'s own fixed filename (FORMAT-SPEC.md §7.3) -- the suffix
#: every generation's ``file_map`` path ends with, regardless of stream or
#: middle segment.
_SAAS_OBJ_SUFFIX = "/saas_obj"


def _nearest_live_generation(
    generations: list[tuple[int, str]], requested: int, cap: int | None
) -> tuple[int, str] | None:
    """The ``(stream_version, middle)`` in ``generations`` (sorted
    ascending by ``stream_version``, possibly with duplicate
    ``stream_version`` values across middle forms) with the smallest
    ``stream_version >= requested`` that's also ``<= cap`` when ``cap``
    is given -- i.e. the nearest still-live generation forward-reachable
    from ``requested`` without exceeding ``stream_info.
    latest_complete_version``. ``None`` if nothing qualifies (a genuine
    gap). Pure, no I/O -- callers resolve ``generations``/``cap`` first.
    """
    keys = [g[0] for g in generations]
    idx = bisect.bisect_left(keys, requested)
    if idx >= len(generations):
        return None
    candidate = generations[idx]
    if cap is not None and candidate[0] > cap:
        return None
    return candidate


class SaasStream:
    def __init__(self, repo: DedupRepo, connection_config_id: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
        self._repo = repo
        self.connection_config_id = connection_config_id
        self.stream_uuid = stream_uuid
        self._dir_cache = DirCache(repo.store)
        # repo_root must be prefixed here — same reasoning as
        # DeviceProvider._resolve_meta_dir().
        self._stream_root = join_path(repo.layout.repo_root, "saas", str(connection_config_id), stream_uuid)
        self._snapshot_source: SqliteSource | None = None
        self._version_source: SqliteSource | None = None
        # Deliberately its own connection, never sharing _version_source
        # even though stream_info and version_info live in the same
        # logical "saas_version" file: a stream_info schema-drift fallback
        # (see _fetch_latest_complete_version) must not permanently swap
        # the connection stream_version_for()'s version_info lookups
        # depend on for every later catalog Version this instance resolves.
        self._stream_info_source: SqliteSource | None = None
        # Single-value AsyncKeyedCache instances (a constant ``None`` key),
        # same idiom as DedupRepo's own probe_cache/_file_meta_table_cache
        # -- this stream's own candidate-middle list, generation list, and
        # latest_complete_version never change within one SaasStream's
        # lifetime, so each is fetched at most once no matter how many
        # distinct catalog Versions this instance resolves.
        self._candidate_middles_cache: AsyncKeyedCache[None, list[str]] = AsyncKeyedCache(self._fetch_candidate_middles)
        self._generations_cache: AsyncKeyedCache[None, list[tuple[int, str]]] = AsyncKeyedCache(self._fetch_generations)
        self._latest_complete_version_cache: AsyncKeyedCache[None, int | None] = AsyncKeyedCache(
            self._fetch_latest_complete_version
        )
        # Keyed by the *requested* stream_version -- distinct catalog
        # Versions sharing one requested stream_version (the common case:
        # several application-layer backups landing in one write session,
        # see this module's own docstring) reuse one resolution instead of
        # repeating the walk. A negative (``None``) result is cached too
        # (AsyncKeyedCache's own presence check, not an ``is not None``
        # check, covers this) -- a genuinely-missing generation isn't
        # re-walked on every repeated reference to it either.
        self._table_cache: AsyncKeyedCache[tuple[str, tuple[str, ...]], Table] = AsyncKeyedCache()
        self._forward_resolution_cache: AsyncKeyedCache[int, tuple[int, str] | None] = AsyncKeyedCache(
            self._do_resolve_forward
        )
        # Set by open_saas_obj() as a side effect: (version_uid, requested,
        # resolved) for the most recently successfully opened version --
        # last_open_resolution()'s own cheap, synchronous readback of "what
        # did that call actually do", not a general cache keyed by version.
        self._last_open_resolution: tuple[VersionUid, int, int] | None = None

    async def close(self) -> None:
        """Release every sqlite connection this instance opened.

        The table cache goes with them: each ``Table`` it holds is bound to one
        of these connections, so keeping them would turn this instance's
        documented lazy-reopen (every ``_*_connection()`` getter rebuilds from
        ``None``) into a failure against a closed connection.
        """
        self._table_cache.invalidate()
        if self._snapshot_source is not None:
            await self._snapshot_source.close()
            self._snapshot_source = None
        if self._version_source is not None:
            await self._version_source.close()
            self._version_source = None
        if self._stream_info_source is not None:
            await self._stream_info_source.close()
            self._stream_info_source = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -- db connections -------------------------------------------------

    async def _resolve_db_path(self, logical_name: str) -> str:
        """Resolve ``logical_name`` (``"saas_snapshot"``/``"saas_version"``)
        to the physical file to open. The bare, un-suffixed name is
        preferred when present — the connector writes live updates to it
        directly, only rotating to a numbered generation later."""
        db_dir = f"{self._stream_root}/db"
        bare = f"{db_dir}/{logical_name}"
        if await self._repo.store.exists(bare):
            return bare
        index = await self._dir_cache.grouped(db_dir)
        physical_name = resolve_seq_file(index, logical_name, ref=bare)
        return f"{db_dir}/{physical_name}"

    async def _resolve_generation_only(self, logical_name: str) -> str:
        """The largest-numbered-generation half of ``_resolve_db_path``,
        skipping the bare-file preference entirely — used only as a
        fallback (see ``_open_table_with_fallback``) when the normally-
        preferred bare file turns out to not actually be live."""
        db_dir = f"{self._stream_root}/db"
        index = await self._dir_cache.grouped(db_dir)
        physical_name = resolve_seq_file(index, logical_name, ref=f"{db_dir}/{logical_name}")
        return f"{db_dir}/{physical_name}"

    async def _snapshot_connection(self) -> aiosqlite.Connection:
        if self._snapshot_source is None:
            path = await self._resolve_db_path("saas_snapshot")
            # saas_snapshot is always raw, never aHlT-enveloped — see
            # module docstring — so no vault_key is needed here;
            # SqliteSource.from_raw_store() still handles a real -wal
            # sidecar correctly if one ever exists (that's
            # open_sqlite()'s own job, unaffected by skipping the
            # envelope check).
            self._snapshot_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._snapshot_source.connection

    async def _snapshot_connection_via_generation_fallback(self) -> aiosqlite.Connection:
        """Re-resolve ``saas_snapshot`` skipping the bare-file preference,
        closing and replacing whatever ``_snapshot_connection`` had
        already cached."""
        if self._snapshot_source is not None:
            await self._snapshot_source.close()
        path = await self._resolve_generation_only("saas_snapshot")
        self._snapshot_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._snapshot_source.connection

    async def _version_connection(self) -> aiosqlite.Connection:
        if self._version_source is None:
            path = await self._resolve_db_path("saas_version")
            self._version_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._version_source.connection

    async def _version_connection_via_generation_fallback(self) -> aiosqlite.Connection:
        """``_snapshot_connection_via_generation_fallback``'s counterpart
        for ``saas_version``."""
        if self._version_source is not None:
            await self._version_source.close()
        path = await self._resolve_generation_only("saas_version")
        self._version_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._version_source.connection

    async def _stream_info_connection(self) -> aiosqlite.Connection:
        if self._stream_info_source is None:
            path = await self._resolve_db_path("saas_version")
            self._stream_info_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._stream_info_source.connection

    async def _stream_info_connection_via_generation_fallback(self) -> aiosqlite.Connection:
        """``_version_connection_via_generation_fallback``'s counterpart,
        scoped to ``_stream_info_source`` alone -- see ``__init__``'s own
        comment for why ``stream_info`` never falls back through
        ``_version_source``."""
        if self._stream_info_source is not None:
            await self._stream_info_source.close()
        path = await self._resolve_generation_only("saas_version")
        self._stream_info_source = await SqliteSource.from_raw_store(self._repo.store, path)
        return self._stream_info_source.connection

    async def _open_table_with_fallback(
        self,
        table_name: str,
        columns: list[Column],
        *,
        primary: Callable[[], Awaitable[aiosqlite.Connection]],
        fallback: Callable[[], Awaitable[aiosqlite.Connection]],
    ) -> Table:
        """Cached by table name, then ``_do_open_table_with_fallback``.

        The connections underneath are themselves cached for this stream's
        lifetime, so the ``Table`` bound to one stays valid — and building one
        costs a ``PRAGMA table_info`` round trip that ``stream_version_for``
        would otherwise pay twice for every single version it resolves. Same
        reasoning, and the same ``AsyncKeyedCache`` in-flight de-duplication,
        as ``dedup.repository.DedupRepo``'s own ``file_meta`` table cache.
        """

        async def _load(_key: tuple[str, tuple[str, ...]]) -> Table:
            return await self._do_open_table_with_fallback(table_name, columns, primary=primary, fallback=fallback)

        # Keyed on the columns too, not the table name alone: a second caller
        # asking for the same table with a different column list must not be
        # handed the first one's Table, whose own ``columns_present`` was
        # narrowed to that first list.
        return await self._table_cache.resolve((table_name, tuple(c.name for c in columns)), _load)

    async def _do_open_table_with_fallback(
        self,
        table_name: str,
        columns: list[Column],
        *,
        primary: Callable[[], Awaitable[aiosqlite.Connection]],
        fallback: Callable[[], Awaitable[aiosqlite.Connection]],
    ) -> Table:
        """``Table.create(await primary(), table_name, columns)``, retried
        once against ``fallback()`` if that raises
        ``sqlite3.DatabaseError``/``DataCorruptError`` -- covers an empty
        (0-byte) placeholder bare ``saas_snapshot``/``saas_version`` file
        coexisting with a real, populated numbered generation, so
        ``primary`` (which prefers the bare file) opens successfully but
        has no tables at all. Only reached when ``primary``'s own table turns out
        unreadable -- an already-healthy bare file (every currently
        known sample) never attempts ``fallback`` at all, so this adds no
        new ``ObjectStore`` calls to that path.

        A ``fallback`` that *also* fails degrades to ``NotFoundError`` -- the
        same "schema drift degrades like any other non-match" convention
        already applied everywhere else in ``units/saas/`` (``provider.py``,
        ``object_name_index.py``, ``teams_chat.py``) -- rather than
        propagating and aborting ``Catalog.versions()``'s whole
        filtering loop for every other version in this stream too.
        """
        try:
            return await Table.create(await primary(), table_name, columns)
        except (sqlite3.DatabaseError, DataCorruptError):
            pass
        try:
            return await Table.create(await fallback(), table_name, columns)
        except (sqlite3.DatabaseError, DataCorruptError) as exc:
            raise NotFoundError(f"{table_name} unreadable: {exc}", ref=self._stream_root) from exc

    # -- generation resolution --------------------------------------------

    async def _candidate_middles(self) -> list[str]:
        return await self._candidate_middles_cache.resolve(None)

    async def _fetch_candidate_middles(self, _key: None) -> list[str]:
        """Every middle-segment string this stream's ``saas_obj``
        ``file_map`` path might use -- the "Copy" form (this stream's own
        registered ``connection_id``, non-numeric) tried first, then the
        "Tiering" form (this stream's own numeric ``connection_config_id``)
        -- since which applies isn't knowable ahead of a real ``file_map``
        hit (see ``open_saas_obj``). In practice a given
        ``connection_config_id`` has exactly one ``version_type`` (Copy xor
        Tiering), so only one of the two ever actually resolves; both are
        still probed since that's not something this stream can assume."""
        conn_table = await Table.create(
            await self._repo.db("connection_config"), "connection_config", [Column("connection_id")]
        )
        conn_row = await conn_table.select_one("connection_config_id = ?", (self.connection_config_id,))
        candidates = []
        if conn_row is not None:
            candidates.append(str(conn_row["connection_id"]))
        candidates.append(str(self.connection_config_id))
        return candidates

    async def _generations(self) -> list[tuple[int, str]]:
        return await self._generations_cache.resolve(None)

    async def _fetch_generations(self, _key: None) -> list[tuple[int, str]]:
        """Every currently-live, Complete generation (FORMAT-SPEC.md:
        file_map-status) this stream has a resolvable ``saas_obj``
        ``file_map`` row for, as ``(stream_version, middle)`` pairs sorted
        ascending by ``stream_version`` -- one ``file_map`` prefix scan per
        candidate middle (``_candidate_middles``), for this ``SaasStream``'s
        whole lifetime. ``open_saas_obj``'s forward resolution bisects into
        this instead of a fresh scan per call."""
        generations: list[tuple[int, str]] = []
        for middle in await self._candidate_middles():
            prefix = f"{self.stream_uuid}/{middle}/"
            for path in await self._repo.file_map_paths_with_prefix(prefix, status=FILE_MAP_STATUS_COMPLETE):
                if not path.endswith(_SAAS_OBJ_SUFFIX):
                    continue
                version_str = path[len(prefix) : -len(_SAAS_OBJ_SUFFIX)]
                try:
                    stream_version = int(version_str)
                except ValueError:
                    continue  # a malformed/unexpected path shape -- not a generation this stream recognizes
                generations.append((stream_version, middle))
        generations.sort(key=lambda g: g[0])
        return generations

    async def _latest_complete_version(self) -> int | None:
        return await self._latest_complete_version_cache.resolve(None)

    async def _fetch_latest_complete_version(self, _key: None) -> int | None:
        """This stream's ``stream_info.latest_complete_version`` -- the
        upper bound forward resolution never searches past, since anything
        beyond it is a not-yet-committed generation, treated as crash
        garbage per the on-disk format (see workload-format/saas-obj.md
        §11.2.3/§11.4.6). ``None`` when ``stream_info`` itself can't be
        read (schema drift, same degrade-like-any-other-optional-read
        posture as ``_open_table_with_fallback``'s own fallback) --
        forward resolution then searches unbounded rather than refusing to
        resolve at all."""
        try:
            table = await self._open_table_with_fallback(
                "stream_info",
                [Column("latest_complete_version")],
                primary=self._stream_info_connection,
                fallback=self._stream_info_connection_via_generation_fallback,
            )
        except NotFoundError:
            return None
        row = await table.select_one("id = ?", (1,))
        if row is None:
            return None
        value = row["latest_complete_version"]
        return None if value is None else as_int(value)

    async def _resolve_forward(self, requested: int) -> tuple[int, str] | None:
        return await self._forward_resolution_cache.resolve(requested)

    async def _do_resolve_forward(self, requested: int) -> tuple[int, str] | None:
        generations = await self._generations()
        cap = await self._latest_complete_version()
        return _nearest_live_generation(generations, requested, cap)

    # -- version chain -> saas_obj --------------------------------------

    async def stream_version_for(self, version: Version) -> int:
        """Resolve a catalog ``Version``'s ``(saas_snapshot_uuid,
        saas_version_id)`` down to this stream's own ``stream_version``,
        via ``snapshot_info.snapshot_uuid -> snapshot_id`` then
        ``version_info.(snapshot_id, version_id) -> stream_version``."""
        snap_table = await self._open_table_with_fallback(
            "snapshot_info",
            [Column("snapshot_id")],
            primary=self._snapshot_connection,
            fallback=self._snapshot_connection_via_generation_fallback,
        )
        snap_row = await snap_table.select_one("snapshot_uuid = ?", (version.saas_snapshot_uuid,))
        if snap_row is None:
            raise NotFoundError(
                f"no snapshot_info row for snapshot_uuid={version.saas_snapshot_uuid!r}",
                ref=self._stream_root,
            )
        snapshot_id = as_int(snap_row["snapshot_id"])

        ver_table = await self._open_table_with_fallback(
            "version_info",
            [Column("stream_version")],
            primary=self._version_connection,
            fallback=self._version_connection_via_generation_fallback,
        )
        ver_row = await ver_table.select_one(
            "snapshot_id = ? AND version_id = ?", (snapshot_id, version.saas_version_id)
        )
        if ver_row is None:
            raise NotFoundError(
                f"no version_info row for snapshot_id={snapshot_id!r} version_id={version.saas_version_id!r}",
                ref=self._stream_root,
            )
        return as_int(ver_row["stream_version"])

    async def open_saas_obj(self, version: Version) -> DedupFile:
        """Locate and open this catalog ``Version``'s ``saas_obj``.

        Resolves ``version``'s own recorded ``stream_version``
        (``stream_version_for``), then forward-resolves it to the nearest
        still-live generation at or after that value (see this module's
        own docstring for why substituting a later generation is
        correct) — an older generation routinely superseded and
        server-side garbage-collected is not itself an error.

        Raises:
            NotFoundError: no live generation exists anywhere from
                ``version``'s own ``stream_version`` through this
                stream's ``stream_info.latest_complete_version`` — a
                genuine gap, not routine rotation. ``ref`` names the
                originally-requested (not last-tried) path.
        """
        requested = await self.stream_version_for(version)
        resolved = await self._resolve_forward(requested)
        if resolved is None:
            cap = await self._latest_complete_version()
            candidates = await self._candidate_middles()
            primary_middle = candidates[0] if candidates else str(self.connection_config_id)
            raise NotFoundError(
                f"no live saas_obj for stream_version>={requested} through "
                f"stream_info.latest_complete_version={cap!r} -- searched forward within this "
                "stream and found nothing live",
                ref=f"{self.stream_uuid}/{primary_middle}/{requested}{_SAAS_OBJ_SUFFIX}",
            )
        resolved_version, middle = resolved
        self._last_open_resolution = (version.version_uid, requested, resolved_version)
        location = await self._repo.locate_file(f"{self.stream_uuid}/{middle}/{resolved_version}{_SAAS_OBJ_SUFFIX}")
        return self._repo.open_composition(
            location.stream_id, location.session_id, location.comp_offset, size=location.file_size
        )

    def last_open_resolution(self, version: Version) -> tuple[int, int] | None:
        """The ``(requested, resolved)`` ``stream_version`` pair the most
        recent successful ``open_saas_obj(version)`` call actually used —
        ``None`` if that wasn't the last call this instance made for this
        exact ``version`` (a different version, or none yet). Purely
        synchronous: reads a plain instance attribute ``open_saas_obj``
        already set as its own side effect, no I/O and no re-derivation —
        for a caller (``verify_reachable._saas_extents``) that wants to
        report which generation actually got read, immediately after its
        own ``open_saas_obj`` call, without touching that method's return
        type (which every other SaaS content-reading caller also uses and
        has no reason to care about this)."""
        if self._last_open_resolution is None:
            return None
        version_uid, requested, resolved = self._last_open_resolution
        if version_uid != version.version_uid:
            return None
        return requested, resolved


def _stream_key(version: Version) -> tuple[int, str]:
    """``SaasStreamCache``'s own cache key for ``version``'s stream --
    shared by ``open_saas_obj``/``last_open_resolution`` so both always
    derive it the same way."""
    return (int(version.connection_config_id), str(version.saas_stream_uuid))


#: Default cap on SaasStreamCache's own concurrently-warm SaasStream
#: instances. Each holds up to 3 live aiosqlite connections (snapshot/
#: version/stream_info -- see SaasStream's own docstring), so this bounds
#: this cache's own contribution to at most 3x this many concurrently
#: open connections/background threads/fds, regardless of how many
#: distinct streams one run eventually touches.
_DEFAULT_STREAM_CACHE_SIZE = 8


class SaasStreamCache:
    """One ``SaasStream`` per distinct ``(connection_config_id,
    saas_stream_uuid)`` pair, reused across every version sharing that
    pair — a ``SaasStream``'s own forward-resolution caches (see its
    docstring) only pay off when the same instance resolves every
    catalog ``Version`` for its stream, not a fresh one per version, so a
    batch caller (``units.verify_reachable``'s ``_ReachabilityWalker``,
    scoped to one run) constructs exactly one of these and reuses it for
    every ``(workload, version)`` pair it discovers.

    Bounded to at most ``maxsize`` concurrently warm streams (default
    ``_DEFAULT_STREAM_CACHE_SIZE``) — the least-recently-touched one is
    evicted, and immediately closed (releasing its own live aiosqlite
    connections), once a run touches more distinct streams than that.
    Hand-rolled rather than built on ``AsyncKeyedCache``: evicting a live
    resource needs an explicit close, which ``AsyncKeyedCache`` has no
    hook for. No lock guards this cache's own bookkeeping (see
    ``_stream_for``'s construction site for why that's safe) — this
    relies on today's one real caller (``_ReachabilityWalker``'s own
    version-by-version walk) never calling ``open_saas_obj`` concurrently
    for two different keys at once; a future caller that parallelizes that
    walk would need to revisit this.

    Every opened stream is closed on exit regardless of how the batch
    ended (``async with``)."""

    def __init__(self, repo: DedupRepo, *, maxsize: int = _DEFAULT_STREAM_CACHE_SIZE) -> None:
        if maxsize < 1:
            raise ValueError(f"maxsize must be at least 1, got {maxsize}")
        self._repo = repo
        self._maxsize = maxsize
        self._streams: OrderedDict[tuple[int, str], SaasStream] = OrderedDict()

    async def _stream_for(self, key: tuple[int, str]) -> SaasStream:
        """The bounded-LRU counterpart of what ``AsyncKeyedCache.resolve``
        would do here — see this class's own docstring for why it isn't
        built on that primitive. Evicts (and closes) the least-recently
        touched stream once inserting a new one pushes this cache past
        ``maxsize``."""
        stream = self._streams.get(key)
        if stream is not None:
            self._streams.move_to_end(key)
            return stream
        connection_config_id, stream_uuid = key
        # Synchronous, no `await` inside -- no window between the cache
        # miss above and the insert below for a concurrent caller to land
        # in. Only closing an evicted stream below does real I/O, and
        # that always happens after this bookkeeping is already committed.
        stream = SaasStream(self._repo, ConnectionConfigId(connection_config_id), StreamUuid(stream_uuid))
        self._streams[key] = stream
        evicted: list[SaasStream] = []
        while len(self._streams) > self._maxsize:
            evicted.append(self._streams.popitem(last=False)[1])
        for old_stream in evicted:
            # Best-effort: closing a stream we're already done with must
            # never abort an otherwise-successful run.
            with contextlib.suppress(Exception):
                await old_stream.close()
        return stream

    async def open_saas_obj(self, version: Version) -> DedupFile:
        """``version``'s ``saas_obj``, via the shared ``SaasStream`` for
        its ``(connection_config_id, saas_stream_uuid)`` — see
        ``SaasStream.open_saas_obj`` for resolution/error semantics."""
        stream = await self._stream_for(_stream_key(version))
        return await stream.open_saas_obj(version)

    def last_open_resolution(self, version: Version) -> tuple[int, int] | None:
        """See ``SaasStream.last_open_resolution`` — meaningful only right
        after this cache's own ``open_saas_obj(version)`` succeeded for
        the same ``version``. Synchronous, and doesn't itself resolve a
        new stream: ``None`` (not a lookup at all) if none has been built
        yet for this version's own pair, or if it was since evicted —
        which can only mean ``open_saas_obj`` was never actually called
        for it, or was but the stream has since been recycled under
        pressure from other streams."""
        stream = self._streams.get(_stream_key(version))
        if stream is None:
            return None
        return stream.last_open_resolution(version)

    async def close(self) -> None:
        # Same "attempt every item, then report" posture as
        # Repository.close() — one stream's failure must not abandon
        # closing the rest.
        streams = list(self._streams.values())
        self._streams.clear()
        errors: list[Exception] = []
        for stream in streams:
            try:
                await stream.close()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise ExceptionGroup("SaasStreamCache.close() failed to close every tracked stream", errors)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
