"""``DedupRepo``: opens one repository root and ties
``RepoInfo``/key material/``Pool``/``CompositionReader`` together behind
``open_file()``/``open_composition()``.

Repository-root path constants (FORMAT-SPEC.md: Locating the repository
root, and directory layout) are **uniform across both layout kinds** —
``<repo_root>/@data/Pool``, ``<repo_root>/@data/Composition``,
``<repo_root>/db/``; only ``layout.repo_root`` itself differs (the vault
root directly for ``RepoKind.VAULT``, or
``@ActiveProtectData/<repoId>`` for ``RepoKind.OBJECT_STORE``).
"""

from __future__ import annotations

import dataclasses
from typing import Self, override

import aiosqlite

from .._util.closing import RESOURCE_CLOSE_TIMEOUT, AsyncClosing, close_each, leaf_exceptions
from ..asynccache import AsyncKeyedCache
from ..cachemanager import DEFAULT_LIMITS, CacheLimits, CacheManager
from ..errors import DataCorruptError, KeyRequiredError, NotFoundError
from ..format.repo_info import RepoInfo, parse_repo_info
from ..identifiers import CompOffset, SessionId, StreamId
from ..storage.base import ObjectStore, join_path
from ..storage.dircache import DirCache
from ..storage.generations import (
    PHYSICAL_NAME_ALIASES,
    REPO_TRANSACTIONS_DIR,
    SUPPL_TRANSACTION_IDS_DIR,
)
from ..storage.generations import resolve_generation as _resolve_generation
from ..storage.layout import REPO_INFO_NAME, RepoKind, RepoLayout
from ..storage.seqid import resolve_seq_path
from ..storage.sqlite_source import SqliteSource
from ..storage.table import Column, Table
from .composition_reader import CompositionReader, CompositionRecord
from .dedup_file import DedupFile
from .keys import KeyMaterial
from .pool import NO_VERIFY, Pool, VerifyPolicy

POOL_ROOT = "@data/Pool"
COMPOSITION_ROOT = "@data/Composition"
DB_ROOT = "db"

#: Every ``db/<name>`` ``DedupRepo.db()`` serves (``copy_target_file`` is
#: aliased to ``copy_target_version`` before lookup). This closed set is what
#: bounds ``_db_sources``: add a name here when a new caller needs one.
DB_SOURCE_NAMES = frozenset(
    {
        "connection_config",
        "copy_target_version",
        "copy_target_version_meta",
        "file_map",
        "file_meta",
        "vault_link_key",
        "workload_config",
    }
)


async def _resolve_versioned(
    store: ObjectStore, dir_cache: DirCache, layout: RepoLayout, dir_path: str, logical_name: str
) -> str:
    """The generation-aware counterpart to ``resolve_seq_path``, for the two
    logical names FORMAT-SPEC.md: Multi-generation selection governs
    (``repo_info``, ``db/<name>``). ``VAULT`` layouts have no
    ``.<N>``-suffixed files, so only ``RepoKind.OBJECT_STORE`` takes the
    generation path. ``dir_cache`` is the fixed-directory ``DirCache``, so
    every listing either path needs is shared across resolutions.
    """
    if layout.kind is not RepoKind.OBJECT_STORE:
        return await resolve_seq_path(dir_cache, dir_path, logical_name)
    return await _resolve_generation(
        store,
        dir_path,
        logical_name,
        transactions_dir=join_path(layout.repo_root, REPO_TRANSACTIONS_DIR),
        suppl_dir=join_path(layout.repo_root, SUPPL_TRANSACTION_IDS_DIR),
        listdir=dir_cache.listdir,
    )


#: The ``db/file_map.status`` of a complete file, the only one
#: ``locate_file`` accepts; see its Raises section for the others
#: (FORMAT-SPEC.md: db/file_map). Public because ``units.saas.stream``
#: pre-filters candidates with it.
FILE_MAP_STATUS_COMPLETE = 2
_FILE_MAP_STATUS_KNOWN_BAD = frozenset({4, 5})


@dataclasses.dataclass(frozen=True, slots=True)
class FileLocation:
    """One ``db/file_map`` row, resolved to what ``DedupRepo.open_composition``
    needs plus (when available) ``file_meta.file_size``. ``status`` is not
    carried here — ``locate_file`` already rejects every row except
    Complete (FORMAT-SPEC.md: db/file_map) before constructing one."""

    stream_id: StreamId
    session_id: SessionId
    comp_offset: CompOffset
    block: int
    file_size: int | None


class DedupRepo(AsyncClosing):
    """One opened repository: its ``Pool``, a cache of read-only ``db/<name>``
    sqlite connections, and a repo-wide ``CompositionRecord`` cache shared by
    every ``CompositionReader`` it builds. Hands back a ``DedupFile`` via
    ``open_file``/``open_composition``. Async context manager; ``close()``
    releases the connections and caches.

    Attributes:
        layout: The ``RepoLayout`` this repository was opened from.
        info: The parsed ``repo_info``.
    """

    def __init__(
        self,
        store: ObjectStore,
        layout: RepoLayout,
        info: RepoInfo,
        dir_cache: DirCache,
        *,
        fixed_dir_cache: DirCache | None = None,
        vault_key: bytes | None = None,
        limits: CacheLimits = DEFAULT_LIMITS,
        verify: VerifyPolicy = NO_VERIFY,
    ) -> None:
        self._store = store
        self.layout = layout
        self.info = info
        self._dir_cache = dir_cache
        self._fixed_dir_cache = (
            fixed_dir_cache if fixed_dir_cache is not None else DirCache(store, maxsize=limits.dir_fixed)
        )
        self._limits = limits
        #: Every cache this repository owns, by name; see ``CacheManager``.
        self.caches = CacheManager()
        self.caches.register(
            "dir_scan", dir_cache.invalidate, lambda: {"dir_scan": dir_cache.stats()}, bounded_by="maxsize (LRU)"
        )
        self.caches.register(
            "dir_fixed",
            self._fixed_dir_cache.invalidate,
            lambda: {"dir_fixed": self._fixed_dir_cache.stats()},
            bounded_by="maxsize (LRU)",
        )
        self._vault_key = vault_key
        self._comp_root = join_path(layout.repo_root, COMPOSITION_ROOT)
        self._db_root = join_path(layout.repo_root, DB_ROOT)
        self._pool_root = join_path(layout.repo_root, POOL_ROOT)
        self._pool = Pool(
            store,
            self._pool_root,
            dir_cache,
            vault_key=vault_key,
            limits=limits,
            verify=verify,
        )
        self.caches.register("pool", self._pool.release_caches, self._pool.cache_stats, bounded_by="maxsize (LRU)")
        self._db_sources: AsyncKeyedCache[str, SqliteSource] = self.caches.keyed(
            "db_sources",
            self._build_db_source,
            maxsize=None,
            bounded_by="closed key set DB_SOURCE_NAMES",
            on_invalidate=self._close_db_sources,
        )
        # Single-value cache (constant None key) for _get_file_meta_table(); a
        # None result ("checked, found nothing") is cached too. Read-only, so
        # it stays valid until invalidated.
        self._file_meta_table_cache: AsyncKeyedCache[None, Table | None] = self.caches.keyed(
            "file_meta_table", self._build_file_meta_table, maxsize=1
        )
        self._composition_records: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = (
            self.caches.keyed("composition_records", maxsize=limits.composition_records)
        )

    @property
    def store(self) -> ObjectStore:
        """The ``ObjectStore`` this repository was opened against, for callers
        above Catalog that need a raw read outside ``Pool``/
        ``CompositionReader``/``db()`` (e.g. catalog's link-key display-name
        lookup)."""
        return self._store

    @property
    def vault_key(self) -> bytes | None:
        """The resolved VaultKey (``None`` for an unencrypted repository or
        one opened without key material), for callers that decrypt outside
        ``Pool`` (e.g. ``catalog.version.open_target_db`` peeling an
        ``aHlT``-enveloped ``target.db``)."""
        return self._vault_key

    @property
    def dir_cache(self) -> DirCache:
        """This repository's ``DirCache`` for bulk-scannable directories (Pool
        leaves, Composition, a SaaS stream's ``db/``)."""
        return self._dir_cache

    async def repo_info_path(self) -> str:
        """The store path of the ``repo_info`` generation this repository
        uses — on object storage the committed one, as ``open`` reads it.

        Raises:
            NotFoundError: No usable ``repo_info`` exists.
        """
        return await _resolve_versioned(
            self._store, self._fixed_dir_cache, self.layout, self.layout.repo_root, REPO_INFO_NAME
        )

    def new_pool(self, *, verify: VerifyPolicy) -> Pool:
        """A separate ``Pool`` over this repository's chunks, with its own
        caches (sized by this repository's ``CacheLimits``) and ``verify``
        policy; it shares this repository's directory cache."""
        return Pool(
            self._store, self._pool_root, self._dir_cache, vault_key=self._vault_key, limits=self._limits, verify=verify
        )

    @classmethod
    async def open(
        cls,
        store: ObjectStore,
        layout: RepoLayout,
        keys: KeyMaterial | None = None,
        *,
        limits: CacheLimits = DEFAULT_LIMITS,
        verify: VerifyPolicy = NO_VERIFY,
    ) -> Self:
        """Open ``layout``. Reads only ``repo_info`` and, given ``keys`` for an
        encrypted repository, the wrapped VaultKey record; never scans Pool
        or Composition.

        Args:
            store: The repository's ``ObjectStore``.
            layout: Where the repository root lives.
            keys: Key material; ``None`` opens without a VaultKey.
            limits: Bounds of every cache this repository owns: the
                ``Pool``'s, the directory caches and the composition-record
                cache.
            verify: The per-chunk checks every ``Pool`` read runs.

        Raises:
            NotFoundError: ``repo_info`` is missing.
            DataCorruptError: ``repo_info`` or the key record is corrupt.
            FormatError: ``repo_info`` is truncated.
            KeyMismatchError: ``keys`` fails the GCM tag check (raised here
                because AES-CTR would otherwise decrypt to garbage silently).
            KeyRequiredError: ``keys`` names a user key with no wrapped
                VaultKey on record.
        """
        dir_cache = DirCache(store, maxsize=limits.dir_scan)
        fixed_dir_cache = DirCache(store, maxsize=limits.dir_fixed)
        info_path = await _resolve_versioned(store, fixed_dir_cache, layout, layout.repo_root, REPO_INFO_NAME)
        info = parse_repo_info(await store.read(info_path))

        vault_key: bytes | None = None
        if keys is not None and not keys.is_no_encryption:
            vault_key = await keys.resolve_vault_key(store, layout)
            if vault_key is None:
                raise KeyRequiredError(
                    f"no wrapped VaultKey on record for user_key_id={keys.user_key_id!r}", ref=info_path
                )

        return cls(
            store,
            layout,
            info,
            dir_cache,
            fixed_dir_cache=fixed_dir_cache,
            vault_key=vault_key,
            limits=limits,
            verify=verify,
        )

    async def db(self, name: str) -> aiosqlite.Connection:
        """Open (and cache) a connection to ``db/<name>``.

        Read-only against the store: the fast path opens the real file
        immutable; the slow path opens a private materialized copy
        read-write (so an index hint can take effect) whose writes never
        reach the store.

        ``name`` is first resolved through ``PHYSICAL_NAME_ALIASES``
        (``"copy_target_file"`` lives inside ``"copy_target_version"``'s
        file), so both names share one connection.

        On a ``RepoKind.OBJECT_STORE`` layout the generation is chosen by
        FORMAT-SPEC.md: Multi-generation selection (``resolve_generation``),
        not the largest ``.<N>`` suffix, since a generation can exist on disk
        before its transaction commits. ``db/<name>`` is always raw (never
        ``aHlT``/zstd-enveloped).

        Args:
            name: Logical ``db/<name>``.

        Returns:
            The cached connection; ``close()`` owns its lifetime.

        Raises:
            NotFoundError: No such db file.
            DataCorruptError: The file is not a readable sqlite database.
            ResourceLimitExceededError: The slow path's private copy doesn't
                fit in the temp directory with its free-space reserve left.
            ValueError: ``name`` is not in ``DB_SOURCE_NAMES``.
        """
        name = PHYSICAL_NAME_ALIASES.get(name, name)
        if name not in DB_SOURCE_NAMES:
            raise ValueError(f"{name!r} is not a db name DedupRepo serves; add it to DB_SOURCE_NAMES")
        source = await self._db_sources.resolve(name)
        return source.connection

    async def _build_db_source(self, name: str) -> SqliteSource:
        """``self._db_sources``'s fetch — ``name`` is already alias-resolved
        by ``db`` before this runs."""
        path = await _resolve_versioned(self._store, self._fixed_dir_cache, self.layout, self._db_root, name)
        return await SqliteSource.from_raw_store(self._store, path)

    async def locate_file(self, path: str) -> FileLocation:
        """Look up ``path`` in ``db/file_map`` and supplement it with
        ``file_meta.file_size`` when a matching row exists. ``file_meta.path``
        is only unique per ``connection_config_id``; this takes the first
        match.

        Raises:
            NotFoundError: no row for ``path`` at all, or its ``status`` is
                Initialized/Written/Compacted (0/1/3) — not yet, or no
                longer, resolvable content (FORMAT-SPEC.md: db/file_map).
            DataCorruptError: the row's ``status`` is Corrupted/Tainted
                (4/5) — the format itself flags this data as known-bad.
        """
        conn = await self.db("file_map")
        cursor = await conn.execute(
            "SELECT stream_id, session_id, comp_offset, block, status FROM file_map WHERE path = ?",
            (path,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise NotFoundError(f"no file_map entry for path {path!r}", ref=path)
        stream_id, session_id, comp_offset, block, status = row
        if status != FILE_MAP_STATUS_COMPLETE:
            if status in _FILE_MAP_STATUS_KNOWN_BAD:
                raise DataCorruptError(
                    f"file_map row for path {path!r} has status={status} (Corrupted/Tainted)",
                    ref=path,
                    spec="FORMAT-SPEC.md: db/file_map",
                )
            raise NotFoundError(
                f"file_map row for path {path!r} has status={status}, not Complete",
                ref=path,
                spec="FORMAT-SPEC.md: db/file_map",
            )

        file_size = await self._file_size_from_meta(path)

        return FileLocation(
            stream_id=StreamId(stream_id),
            session_id=SessionId(session_id),
            comp_offset=CompOffset(comp_offset),
            block=block,
            file_size=file_size,
        )

    async def file_map_paths_with_prefix(self, prefix: str, *, status: int | None = None) -> list[str]:
        """Every ``db/file_map`` path starting with ``prefix``, sorted.
        ``%``/``_``/``\\`` in ``prefix`` match literally.

        ``status``, when given, restricts to rows at exactly that
        ``file_map.status`` value (FORMAT-SPEC.md: db/file_map); without it,
        unlike ``locate_file``, no status filtering applies.
        """
        conn = await self.db("file_map")
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = "SELECT path FROM file_map WHERE path LIKE ? ESCAPE '\\'"
        params: list[object] = [f"{escaped}%"]
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        cursor = await conn.execute(f"{query} ORDER BY path", params)
        return [row[0] for row in await cursor.fetchall()]

    async def _get_file_meta_table(self) -> Table | None:
        """The cached ``file_meta`` ``Table``, or ``None`` if there is no
        ``file_meta`` db file or it lacks the table. Concurrent callers share
        one build.
        """
        return await self._file_meta_table_cache.resolve(None)

    async def _build_file_meta_table(self, _key: None) -> Table | None:
        try:
            conn = await self.db("file_meta")
        except NotFoundError:
            return None
        if not await Table.exists_in(conn, "file_meta"):
            return None
        return await Table.create(conn, "file_meta", [Column("path"), Column("file_size", required=False)])

    async def _file_size_from_meta(self, path: str) -> int | None:
        """``file_meta.file_size`` for ``path``, or ``None`` without a usable
        ``file_meta`` table, a matching row, or a recorded ``file_size``."""
        table = await self._get_file_meta_table()
        if table is None:
            return None
        row = await table.select_one("path = ?", (path,))
        if row is None:
            return None
        value = row["file_size"]
        if value is None:
            return None
        assert isinstance(value, int)
        return value

    async def open_file(self, path: str, *, fallback_size: int | None = None) -> DedupFile:
        """Resolve ``path`` via ``db/file_map`` and return a ready-to-read
        ``DedupFile``, sized by ``file_meta.file_size``, or by
        ``fallback_size`` when ``file_meta`` records none for it.

        Raises:
            NotFoundError: see ``locate_file``.
            DataCorruptError: see ``locate_file``.
        """
        location = await self.locate_file(path)
        size = location.file_size if location.file_size is not None else fallback_size
        return self.open_composition(location.stream_id, location.session_id, location.comp_offset, size=size)

    def open_composition(
        self, stream_id: StreamId, session_id: SessionId, comp_offset: CompOffset, size: int | None = None
    ) -> DedupFile:
        """A ``DedupFile`` for a ``(stream_id, session_id, comp_offset)``
        triple, with ``size`` as its known size (``None``: unknown). Its
        ``CompositionReader`` shares this repository's composition-record
        cache."""
        comp_reader = self.composition_reader(stream_id, session_id, shared_cache=True)
        return DedupFile(comp_reader, self._pool, comp_offset, size=size)

    def composition_reader(
        self, stream_id: StreamId, session_id: SessionId, *, shared_cache: bool = False
    ) -> CompositionReader:
        """A ``CompositionReader`` for one session's Composition, caching its
        records in this repository's shared cache only with ``shared_cache``."""
        return CompositionReader(
            self._store,
            self._dir_cache,
            self._comp_root,
            stream_id,
            session_id,
            composition_cache=self._composition_records if shared_cache else None,
        )

    async def _close_db_sources(self, cache: AsyncKeyedCache[str, SqliteSource]) -> None:
        """``db_sources``' invalidate hook: settle in-flight fetches, then give
        every source a close attempt (each bounded by a timeout) before the
        cache is cleared.

        Raises:
            ExceptionGroup: One or more sources failed to settle or close.
        """
        # A Table is bound to a connection about to be closed, so it goes first
        # even when only db_sources was named (not just under invalidate_all()).
        self._file_meta_table_cache.invalidate()
        sources, errors = await cache.settle_all()
        errors.extend(await close_each((s.close for s in sources.values()), per_close_timeout=RESOURCE_CLOSE_TIMEOUT))
        cache.invalidate()
        if errors:
            raise ExceptionGroup("db source close failed", errors)

    @override
    async def close(self) -> None:
        """Close every cached sqlite connection (and any temp file or directory
        a slow-path materialization created) and drop every other cache this
        repository holds, via ``caches.invalidate_all()``.

        In-flight fetches are settled first, and every resource gets a close
        attempt, bounded by a timeout, even if an earlier one failed.

        Raises:
            ExceptionGroup: One or more resources failed to close.
        """
        try:
            await self.caches.invalidate_all()
        except ExceptionGroup as group:
            raise ExceptionGroup(
                "DedupRepo.close() failed to close every tracked resource", leaf_exceptions(group)
            ) from None
