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

``stream_version`` is a monotonically increasing per-stream generation
counter; several catalog ``Version`` rows can share one. It records which
generation a version was written into, not where its content lives now:
a later generation's composition record is a superset of an earlier
one's, and older generations are garbage-collected server-side
(FORMAT-SPEC.md: Generic SaaS object addressing). So a missing
generation is routine rotation, and ``resolve_saas_obj`` forward-resolves
to the nearest later live generation, bounded by
``stream_info.latest_complete_version``.

Callers go through ``SaasStreamCache``, which shares one ``SaasStream``
(and its resolution caches) per stream.
"""

from __future__ import annotations

import bisect
import contextlib
import dataclasses
import sqlite3
from collections import OrderedDict
from collections.abc import AsyncIterator
from typing import override

from ..._util.closing import AsyncClosing, close_all
from ...asynccache import AsyncKeyedCache, CacheStats
from ...cachemanager import DEFAULT_LIMITS
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import FILE_MAP_STATUS_COMPLETE, DedupRepo
from ...errors import DataCorruptError, NotFoundError
from ...identifiers import ConnectionConfigId, StreamUuid
from ...storage.base import join_path
from ...storage.seqid import resolve_seq_file
from ...storage.sqlite_source import SqliteSource
from ...storage.table import Column, Table, as_int

#: The suffix every generation's ``saas_obj`` ``file_map`` path ends with
#: (FORMAT-SPEC.md: Generic SaaS object addressing).
_SAAS_OBJ_SUFFIX = "/saas_obj"


def _nearest_live_generation(
    generations: list[tuple[int, str]], requested: int, cap: int | None
) -> tuple[int, str] | None:
    """The entry of ``generations`` (``(stream_version, middle)``, sorted
    by ``stream_version``) with the smallest ``stream_version >=
    requested``, provided it is ``<= cap`` when ``cap`` is given; ``None``
    if nothing qualifies (a genuine gap).
    """
    keys = [g[0] for g in generations]
    idx = bisect.bisect_left(keys, requested)
    if idx >= len(generations):
        return None
    candidate = generations[idx]
    if cap is not None and candidate[0] > cap:
        return None
    return candidate


@dataclasses.dataclass(frozen=True, slots=True)
class ResolvedSaasObj:
    """A version's opened ``saas_obj`` and which generation was read.

    Attributes:
        dedup_file: The opened ``saas_obj``.
        requested_stream_version: The version's own recorded ``stream_version``.
        stream_version: The generation actually read: ``requested_stream_version``
            or a later one, when routine rotation removed it.
    """

    dedup_file: DedupFile
    requested_stream_version: int
    stream_version: int


class SaasStream(AsyncClosing):
    def __init__(self, repo: DedupRepo, connection_config_id: ConnectionConfigId, stream_uuid: StreamUuid) -> None:
        self._repo = repo
        self.connection_config_id = connection_config_id
        self.stream_uuid = stream_uuid
        # The repository's shared (bounded) listing cache: this stream's db/ directory is one more entry.
        self._dir_cache = repo.dir_cache
        # Store paths are relative to the store root, so repo_root is prefixed.
        self._stream_root = join_path(repo.layout.repo_root, "saas", str(connection_config_id), stream_uuid)
        # One SqliteSource per physical file, keyed by path: tables in the
        # same file (version_info and stream_info in saas_version) share one
        # connection. Bounded by construction: a bare file and one
        # generation for each of the two logical names.
        self._sources: AsyncKeyedCache[str, SqliteSource] = AsyncKeyedCache(self._fetch_source)
        # Whether each bare db file exists, asked once per file: version_info and
        # stream_info both look at saas_version. Keys are the two bare paths.
        self._bare_exists: AsyncKeyedCache[str, bool] = AsyncKeyedCache(self._fetch_bare_exists)
        # Single-value caches (key None): fixed for this instance's lifetime.
        self._candidate_middles_cache: AsyncKeyedCache[None, list[str]] = AsyncKeyedCache(self._fetch_candidate_middles)
        self._generations_cache: AsyncKeyedCache[None, list[tuple[int, str]]] = AsyncKeyedCache(self._fetch_generations)
        self._latest_complete_version_cache: AsyncKeyedCache[None, int | None] = AsyncKeyedCache(
            self._fetch_latest_complete_version
        )
        self._table_cache: AsyncKeyedCache[tuple[str, str, tuple[str, ...]], Table] = AsyncKeyedCache()
        # Keyed by the requested stream_version, which several Versions
        # share; a None (gap) result is cached too.
        self._forward_resolution_cache: AsyncKeyedCache[int, tuple[int, str] | None] = AsyncKeyedCache(
            self._do_resolve_forward, maxsize=DEFAULT_LIMITS.forward_resolutions
        )

    @override
    async def close(self) -> None:
        """Close every SQLite connection this instance opened, including
        one still being opened when this is called."""
        self._table_cache.invalidate()
        sources, _errors = await self._sources.settle_all()
        try:
            await close_all([source.close for source in sources.values()], "closing a SaaS stream's sources failed")
        finally:
            self._sources.invalidate()
            self._bare_exists.invalidate()

    # -- db connections -------------------------------------------------

    async def _candidate_paths(self, logical_name: str) -> AsyncIterator[str]:
        """The physical files ``logical_name`` (``"saas_snapshot"``/
        ``"saas_version"``) might live in, best first, produced lazily so a
        healthy bare file costs no directory listing.

        The bare, un-suffixed name comes first when present — the connector
        writes live updates to it directly, only rotating to a numbered
        generation later — but it can exist as an empty placeholder next to
        the real, numbered generation, so the largest-numbered generation
        follows as the fallback.

        Raises:
            NotFoundError: Neither a bare file nor any numbered generation
                exists (raised once the candidates run out).
        """
        db_dir = f"{self._stream_root}/db"
        bare = f"{db_dir}/{logical_name}"
        if await self._bare_exists.resolve(bare):
            yield bare
        index = await self._dir_cache.grouped(db_dir)
        generation = f"{db_dir}/{resolve_seq_file(index, logical_name, ref=bare)}"
        if generation != bare:
            yield generation

    async def _fetch_bare_exists(self, bare: str) -> bool:
        return await self._repo.store.exists(bare)

    async def _fetch_source(self, path: str) -> SqliteSource:
        """``_sources``' fetch. Both db files are never ``aHlT``-enveloped,
        so no ``vault_key`` is needed."""
        return await SqliteSource.from_raw_store(self._repo.store, path)

    async def _open_table(self, logical_name: str, table_name: str, columns: list[Column]) -> Table:
        """``table_name`` from ``logical_name``'s first candidate file that
        holds it, cached for this stream's lifetime per ``(logical_name,
        table_name, columns)`` — keyed on the columns since a ``Table`` is
        narrowed to the columns it was built with.

        Raises:
            NotFoundError: No candidate file exists, or none holds a readable
                ``table_name`` (an empty placeholder, schema drift).
        """

        async def _load(_key: tuple[str, str, tuple[str, ...]]) -> Table:
            last_error: Exception | None = None
            try:
                async for path in self._candidate_paths(logical_name):
                    try:
                        source = await self._sources.resolve(path)
                        return await Table.create(source.connection, table_name, columns)
                    except (sqlite3.DatabaseError, DataCorruptError) as exc:
                        # A bare placeholder next to the real numbered generation:
                        # empty (opens, no tables) or unreadable (fails to open).
                        last_error = exc
            except NotFoundError:
                if last_error is None:
                    raise  # no candidate file exists at all
            raise NotFoundError(f"{table_name} unreadable: {last_error}", ref=self._stream_root) from last_error

        return await self._table_cache.resolve((logical_name, table_name, tuple(c.name for c in columns)), _load)

    # -- generation resolution --------------------------------------------

    async def _candidate_middles(self) -> list[str]:
        return await self._candidate_middles_cache.resolve(None)

    async def _fetch_candidate_middles(self, _key: None) -> list[str]:
        """The middle segments this stream's ``saas_obj`` ``file_map`` path
        might use: the Copy form (the registered ``connection_id``) first,
        then the Tiering form (the numeric ``connection_config_id``). Only
        one resolves for a given stream, but which isn't known up front."""
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
        """Every live, Complete generation (FORMAT-SPEC.md: ``db/file_map``) with a
        ``saas_obj`` ``file_map`` row, as ``(stream_version, middle)`` pairs
        sorted by ``stream_version`` — one prefix scan per candidate
        middle."""
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
                    continue  # not a generation path
                generations.append((stream_version, middle))
        generations.sort(key=lambda g: g[0])
        return generations

    async def _latest_complete_version(self) -> int | None:
        return await self._latest_complete_version_cache.resolve(None)

    async def _fetch_latest_complete_version(self, _key: None) -> int | None:
        """``stream_info.latest_complete_version``: forward resolution never
        goes past it, since a later generation isn't committed
        (FORMAT-SPEC.md: Generation resolution). ``None`` when ``stream_info`` can't be read;
        resolution is then unbounded."""
        try:
            table = await self._open_table("saas_version", "stream_info", [Column("latest_complete_version")])
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
        saas_version_id)`` to its recorded ``stream_version``, via
        ``snapshot_info`` then ``version_info``.

        Raises:
            NotFoundError: Either lookup has no row, or a db file is missing
                or unreadable.
        """
        snap_table = await self._open_table("saas_snapshot", "snapshot_info", [Column("snapshot_id")])
        snap_row = await snap_table.select_one("snapshot_uuid = ?", (version.saas_snapshot_uuid,))
        if snap_row is None:
            raise NotFoundError(
                f"no snapshot_info row for snapshot_uuid={version.saas_snapshot_uuid!r}",
                ref=self._stream_root,
            )
        snapshot_id = as_int(snap_row["snapshot_id"])

        ver_table = await self._open_table("saas_version", "version_info", [Column("stream_version")])
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
        """``resolve_saas_obj(version)``'s file."""
        return (await self.resolve_saas_obj(version)).dedup_file

    async def resolve_saas_obj(self, version: Version) -> ResolvedSaasObj:
        """Locate and open this catalog ``Version``'s ``saas_obj``.

        Reads the nearest live generation at or after ``version``'s
        recorded ``stream_version`` (see the module docstring).

        Raises:
            NotFoundError: ``stream_version_for`` fails, or no live
                generation exists from that ``stream_version`` through
                ``stream_info.latest_complete_version`` (a genuine gap;
                ``ref`` names the originally-requested path).
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
        dedup_file = await self._repo.open_file(f"{self.stream_uuid}/{middle}/{resolved_version}{_SAAS_OBJ_SUFFIX}")
        return ResolvedSaasObj(dedup_file, requested_stream_version=requested, stream_version=resolved_version)


def _stream_key(version: Version) -> tuple[int, str]:
    """``SaasStreamCache``'s own cache key for ``version``'s stream."""
    return (int(version.connection_config_id), str(version.saas_stream_uuid))


#: Default cap on ``SaasStreamCache``'s warm streams. Each holds 2 SQLite
#: connections (4 when a bare placeholder sits beside a numbered
#: generation), so this bounds the cache to 4x this many.
_DEFAULT_STREAM_CACHE_SIZE = DEFAULT_LIMITS.saas_streams


class SaasStreamCache(AsyncClosing):
    """One ``SaasStream`` per ``(connection_config_id, saas_stream_uuid)``,
    shared by every version of that stream so its resolution caches pay
    off. SaaS providers borrow streams from it and never close them.

    Holds at most ``maxsize`` streams, evicting the least recently used
    one not in use (see ``_stream_for``). Closing it closes every stream.

    Raises:
        ValueError: ``maxsize`` is less than 1.
    """

    def __init__(self, repo: DedupRepo, *, maxsize: int = _DEFAULT_STREAM_CACHE_SIZE) -> None:
        if maxsize < 1:
            raise ValueError(f"maxsize must be at least 1, got {maxsize}")
        self._repo = repo
        self._maxsize = maxsize
        self._streams: OrderedDict[tuple[int, str], SaasStream] = OrderedDict()
        # Per-key count of in-flight resolve_saas_obj calls; _stream_for
        # never evicts a key in use.
        self._in_use: dict[tuple[int, str], int] = {}
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def cache_stats(self) -> dict[str, CacheStats]:
        """Counters of the stream LRU, keyed ``saas_streams``."""
        return {
            "saas_streams": CacheStats(
                size=len(self._streams),
                maxsize=self._maxsize,
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
            )
        }

    async def _stream_for(self, key: tuple[int, str]) -> SaasStream:
        """Bounded-LRU stream lookup. Evicts and closes the least recently
        used stream not in use once past ``maxsize``, exceeding
        ``maxsize`` rather than closing one in use.

        The caller marks ``key`` in ``_in_use`` first, so a concurrent
        eviction can't pick it."""
        stream = self._streams.get(key)
        if stream is not None:
            self._hits += 1
            self._streams.move_to_end(key)
            return stream
        self._misses += 1
        connection_config_id, stream_uuid = key
        # No await until the bookkeeping below is committed.
        stream = SaasStream(self._repo, ConnectionConfigId(connection_config_id), StreamUuid(stream_uuid))
        self._streams[key] = stream
        evicted: list[SaasStream] = []
        while len(self._streams) > self._maxsize:
            victim_key = next((k for k in self._streams if self._in_use.get(k, 0) == 0), None)
            if victim_key is None:
                break  # every remaining entry is in use -- exceed maxsize rather than close one mid-read
            evicted.append(self._streams.pop(victim_key))
            self._evictions += 1
        for old_stream in evicted:
            # Best-effort: a failed close of an evicted stream mustn't fail this lookup.
            with contextlib.suppress(Exception):
                await old_stream.close()
        return stream

    async def open_saas_obj(self, version: Version) -> DedupFile:
        """``resolve_saas_obj(version)``'s file."""
        return (await self.resolve_saas_obj(version)).dedup_file

    async def resolve_saas_obj(self, version: Version) -> ResolvedSaasObj:
        """``version``'s ``saas_obj`` via its stream's shared ``SaasStream``;
        see ``SaasStream.resolve_saas_obj`` for semantics and errors."""
        # Marked in use before _stream_for, which can await while closing an
        # evicted stream; the key may already be gone after a concurrent close().
        key = _stream_key(version)
        self._in_use[key] = self._in_use.get(key, 0) + 1
        try:
            stream = await self._stream_for(key)
            return await stream.resolve_saas_obj(version)
        finally:
            remaining = self._in_use.get(key, 0) - 1
            if remaining <= 0:
                self._in_use.pop(key, None)
            else:
                self._in_use[key] = remaining

    @override
    async def close(self) -> None:
        streams = list(self._streams.values())
        self._streams.clear()
        self._in_use.clear()
        await close_all([s.close for s in streams], "SaasStreamCache.close() failed to close every tracked stream")
