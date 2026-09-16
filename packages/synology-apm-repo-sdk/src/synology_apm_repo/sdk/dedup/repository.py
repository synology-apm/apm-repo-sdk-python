"""``DedupRepo``: opens one repository root and ties
``RepoInfo``/key material/``Pool``/``CompositionReader`` together behind
``open_file()``/``open_composition()``.

Repository-root path constants (FORMAT-SPEC.md: repo-root-layout's fixed
directory names) are **uniform across both layout kinds** —
``<repo_root>/@data/Pool``, ``<repo_root>/@data/Composition``,
``<repo_root>/db/``; only ``layout.repo_root`` itself differs (the vault
root directly for ``RepoKind.VAULT``, or
``@ActiveProtectData/<repoId>`` for ``RepoKind.OBJECT_STORE``).
These are the same relative paths ``pool`` and ``keys`` already use.
"""

from __future__ import annotations

import asyncio
import dataclasses
from types import TracebackType
from typing import Self

import aiosqlite

from ..asynccache import AsyncKeyedCache
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
from ..storage.layout import RepoKind, RepoLayout
from ..storage.seqid import resolve_seq_path
from ..storage.sqlite_source import SqliteSource
from ..storage.table import Column, Table
from .composition_reader import CompositionReader
from .dedup_file import DedupFile
from .keys import KeyMaterial
from .keys import probe_encrypted as _probe_encrypted
from .pool import Pool

POOL_ROOT = "@data/Pool"
COMPOSITION_ROOT = "@data/Composition"
DB_ROOT = "db"
REPO_INFO_NAME = "repo_info"

# Bounds one SqliteSource.close() call -- a hung close must not block the
# rest of _db_sources from ever getting a close attempt of their own. Same
# rationale and value as api.repository.Repository's own
# _RESOURCE_CLOSE_TIMEOUT one layer up; not imported from there since this
# module sits below api/ (see ARCHITECTURE.md's "dependencies only point
# downward").
_RESOURCE_CLOSE_TIMEOUT = 10.0


async def _resolve_versioned(
    store: ObjectStore, dir_cache: DirCache, layout: RepoLayout, dir_path: str, logical_name: str
) -> str:
    """The generation-aware counterpart to ``resolve_seq_path``,
    for the two logical names FORMAT-SPEC.md: generation-selection's transaction-log
    algorithm actually governs (``repo_info``, ``db/<name>``) —
    everything else in this module (Pool/Composition per-generation
    files) keeps using the plain ``resolve_seq_path``
    rule instead. Only actually branches for
    ``RepoKind.OBJECT_STORE`` —
    ``VAULT`` layouts never have ``.<N>``-suffixed files at all, and
    ``resolve_generation`` already degrades to the bare name when
    there's nothing to select between.
    """
    if layout.kind is not RepoKind.OBJECT_STORE:
        return await resolve_seq_path(dir_cache, dir_path, logical_name)
    return await _resolve_generation(
        store,
        dir_path,
        logical_name,
        transactions_dir=join_path(layout.repo_root, REPO_TRANSACTIONS_DIR),
        suppl_dir=join_path(layout.repo_root, SUPPL_TRANSACTION_IDS_DIR),
    )


#: ``db/file_map.status`` values (FORMAT-SPEC.md: file_map-status) — see
#: ``locate_file``'s own ``Raises:`` section for how each is handled.
#: ``FILE_MAP_STATUS_COMPLETE`` is public: ``units.saas.stream`` also needs
#: it, to pre-filter forward-resolution's own generation candidates down
#: to ones ``locate_file`` will actually accept.
FILE_MAP_STATUS_COMPLETE = 2
_FILE_MAP_STATUS_KNOWN_BAD = frozenset({4, 5})


@dataclasses.dataclass(frozen=True)
class FileLocation:
    """One ``db/file_map`` row, resolved to what ``DedupRepo.open_composition``
    needs plus (when available) ``file_meta.file_size``. ``status`` is not
    carried here — ``locate_file`` already rejects every row except
    Complete (FORMAT-SPEC.md: file_map-status) before constructing one."""

    stream_id: StreamId
    session_id: SessionId
    comp_offset: CompOffset
    block: int
    file_size: int | None


class DedupRepo:
    """One opened repository: cheap metadata (``RepoInfo``, key material)
    plus the machinery — ``Pool``, ``CompositionReader`` construction, a
    small cache of read-only ``db/<name>`` sqlite connections — that
    ``open_file``/``open_composition`` need to hand back a ``DedupFile``.
    """

    def __init__(
        self,
        store: ObjectStore,
        layout: RepoLayout,
        info: RepoInfo,
        dir_cache: DirCache,
        *,
        vault_key: bytes | None = None,
        bucket_cache_size: int = 16,
        chunk_cache_size: int = 4096,
        verify_fingerprint: bool = False,
    ) -> None:
        self._store = store
        self.layout = layout
        self.info = info
        self._dir_cache = dir_cache
        self._vault_key = vault_key
        self._comp_root = join_path(layout.repo_root, COMPOSITION_ROOT)
        self._db_root = join_path(layout.repo_root, DB_ROOT)
        self._pool_root = join_path(layout.repo_root, POOL_ROOT)
        self._pool = Pool(
            store,
            self._pool_root,
            dir_cache,
            vault_key=vault_key,
            bucket_cache_size=bucket_cache_size,
            chunk_cache_size=chunk_cache_size,
            verify_fingerprint=verify_fingerprint,
        )
        # AsyncKeyedCache, not a hand-rolled dict+lock: two concurrent
        # db() calls for different names (e.g. "file_map" and "repo_info")
        # only ever contend on the same key here, never on each other, so
        # unrelated names build in parallel instead of one blocking behind
        # the other's full SqliteSource.from_raw_store() (which can be a
        # real S3/Azure GET). See probe_encrypted()'s cache below for the
        # other reason this class already leans on this same mechanism.
        # Deliberately left unbounded, unlike units.saas.stream.SaasStreamCache's
        # own bounded SaasStream cache -- this one's key space is a small, fixed
        # set of logical db/<name> names per repository, not one per generation
        # or per distinct remote object, so it can't reproduce that fd-exhaustion
        # failure mode. Revisit with the same bounded-LRU-plus-close-on-evict
        # pattern if that ever stops being true.
        self._db_sources: AsyncKeyedCache[str, SqliteSource] = AsyncKeyedCache(self._build_db_source)
        # probe_encrypted()'s cache, and _get_file_meta_table()'s —
        # single-value AsyncKeyedCache instances (a constant ``None`` key)
        # rather than a hand-rolled bool/lock pair each: both results are
        # legitimately ``None`` ("checked, found nothing" — see each
        # method's own docstring for what that covers), which
        # AsyncKeyedCache's own presence check (``key in self._store``, not
        # ``... is not None``) caches correctly rather than re-probing
        # forever. A repository opened read-only never rewrites its own
        # encryption-key record or file_meta shape mid-session, so caching
        # either "unknown"/"unavailable" outcome for this
        # DedupRepo's whole lifetime is safe, not just an
        # optimization that could go stale.
        self._probe_cache: AsyncKeyedCache[None, bool | None] = AsyncKeyedCache(
            lambda _: _probe_encrypted(self._store, self.layout)
        )
        self._file_meta_table_cache: AsyncKeyedCache[None, Table | None] = AsyncKeyedCache(self._build_file_meta_table)

    @property
    def store(self) -> ObjectStore:
        """The underlying ``ObjectStore`` this repository was opened
        against — exposed for Catalog-Layer-and-above callers that
        occasionally need a raw path read outside the
        ``Pool``/``CompositionReader``/``db()``
        machinery (e.g. catalog's object-store link-key display-name
        lookup, which lists ``@ActiveProtectKey/link/`` directly)."""
        return self._store

    @property
    def vault_key(self) -> bytes | None:
        """The resolved VaultKey, if any (``None`` for an unencrypted
        repository or one opened without key material) — exposed for
        Unit-Layer-and-above callers that need to decrypt something
        outside the
        ``Pool``/``CompositionReader`` machinery (e.g. ``DeviceProvider``
        peeling an ``aHlT``-enveloped ``target.db``)."""
        return self._vault_key

    @property
    def dir_cache(self) -> DirCache:
        """This repository's shared ``DirCache`` — exposed for the same
        reason as ``store``/``vault_key`` — a caller outside
        ``Pool``/``CompositionReader`` (``verify_checks.check_repo_info``,
        ``units/verify_reachable.py``'s top-down walk building its own
        ``CompositionReader``) needs to resolve a ``.<seqId>``-suffixed
        path itself, without duplicating a second, uncached ``listdir``."""
        return self._dir_cache

    @property
    def comp_root(self) -> str:
        """``<repo_root>/@data/Composition`` — exposed so
        ``units/verify_reachable.py``'s top-down walk can build its own
        ``CompositionReader`` per visited composition record without
        ``open_composition()``'s ``DedupFile`` wrapping (verify wants the
        raw record/header, not a readable byte range)."""
        return self._comp_root

    @property
    def pool_root(self) -> str:
        """``<repo_root>/@data/Pool`` — exposed for the same reason as
        ``comp_root``: ``units/verify_reachable.py``'s top-down walk
        constructs its own private ``Pool`` (forcing
        ``verify_fingerprint``/``verify_ciphertext_crc`` on for the run,
        rather than mutating this repository's own shared one), which
        needs this repository's ``pool_root`` to do so."""
        return self._pool_root

    @classmethod
    async def open(
        cls,
        store: ObjectStore,
        layout: RepoLayout,
        keys: KeyMaterial | None = None,
        *,
        bucket_cache_size: int = 16,
        chunk_cache_size: int = 4096,
        verify_fingerprint: bool = False,
    ) -> Self:
        """Open ``layout``. Cheap by construction — reads only
        ``repo_info`` (and, if ``keys`` is given and the repository is
        encrypted, whatever ``KeyMaterial.resolve_vault_key``
        touches: one sqlite row or one small key file); never scans Pool
        or Composition.

        A ``keys`` whose GCM tag doesn't check out raises
        ``KeyMismatchError`` immediately —
        better to fail here than to hand back a repository that will
        silently decrypt every chunk into garbage later (AES-CTR has no
        integrity check of its own). ``verify_fingerprint=True`` makes
        per-chunk ``.fgp`` verification this session's default for every
        read through ``pool`` instead of the off-by-default cost —
        see ``Pool.read_chunk``'s
        own docstring — or pass it per-call there instead.
        """
        dir_cache = DirCache(store)
        info_path = await _resolve_versioned(store, dir_cache, layout, layout.repo_root, REPO_INFO_NAME)
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
            vault_key=vault_key,
            bucket_cache_size=bucket_cache_size,
            chunk_cache_size=chunk_cache_size,
            verify_fingerprint=verify_fingerprint,
        )

    async def db(self, name: str) -> aiosqlite.Connection:
        """Open (and cache) a read-only connection to ``db/<name>``.

        ``name`` is resolved through
        ``PHYSICAL_NAME_ALIASES``
        first (e.g. ``"copy_target_file"`` has no on-disk object of its
        own; its table lives inside whichever generation
        ``"copy_target_version"`` resolves to), so requesting either
        aliased name transparently shares one connection.

        On a ``RepoKind.OBJECT_STORE``
        layout, the real generation is selected by
        FORMAT-SPEC.md: generation-selection's transaction-log algorithm (``resolve_generation``),
        not the naive "largest ``.<N>`` suffix" rule every other
        per-generation file uses — a ``db/<name>.<N>`` generation can
        exist on disk before the transaction referencing it commits.

        ``db/<name>`` is always raw (never ``aHlT``/zstd-enveloped), so
        ``SqliteSource.from_raw_store`` skips straight to
        materializing it — no envelope detection needed here, unlike
        every other envelope→SQLite path in this project.
        """
        name = PHYSICAL_NAME_ALIASES.get(name, name)
        source = await self._db_sources.resolve(name)
        return source.connection

    async def _build_db_source(self, name: str) -> SqliteSource:
        """``self._db_sources``'s fetch — ``name`` is already alias-resolved
        by ``db`` before this runs."""
        path = await _resolve_versioned(self._store, self._dir_cache, self.layout, self._db_root, name)
        return await SqliteSource.from_raw_store(self._store, path)

    async def locate_file(self, path: str) -> FileLocation:
        """Look up ``path`` in ``db/file_map`` and supplement it with
        ``file_meta.file_size`` when a matching row exists there too.
        ``file_meta.path`` is only unique per ``connection_config_id`` —
        this takes the first match, fine for the common single-workload
        case but a placeholder pending the Catalog Layer's proper
        disambiguation.

        Raises:
            NotFoundError: no row for ``path`` at all, or its ``status`` is
                Initialized/Written/Compacted (0/1/3) — not yet, or no
                longer, resolvable content (FORMAT-SPEC.md: file_map-status).
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
                    spec="FORMAT-SPEC.md: file_map-status",
                )
            raise NotFoundError(
                f"file_map row for path {path!r} has status={status}, not Complete",
                ref=path,
                spec="FORMAT-SPEC.md: file_map-status",
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
        """Every ``db/file_map`` path starting with ``prefix``, via a
        ``LIKE``-prefix scan — ``file_map.path`` is the table's own
        primary key, so SQLite resolves this as an indexed range scan,
        not a table scan. ``%``/``_``/``\\`` in ``prefix`` are escaped so
        they match literally rather than as SQL wildcards.

        ``status``, when given, restricts to rows at exactly that
        ``file_map.status`` value (FORMAT-SPEC.md: file_map-status) — unlike
        ``locate_file``, this method applies no status filtering on its own.
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
        """The (cached) ``file_meta`` ``Table``, or ``None`` if this repository
        shape has no ``file_meta`` db file at all, or has one missing
        the table itself — two genuinely different reasons kept
        distinguishable in the check below rather than conflated into
        one bare ``except``, but both cache to the same ``None``
        result: the ``file_meta`` db file may not exist on this repository
        shape (``NotFoundError`` from ``db``), or it may exist but be missing
        the table/column (``Table.exists_in`` and ``Table``'s own
        optional-column handling, respectively).

        Cached (including the ``None`` outcome) rather than rebuilt on
        every call: unlike ``db``, whose own connection cache already
        covers the underlying sqlite connection, building a ``Table``
        costs two more ``PRAGMA table_info`` queries — real repeated
        cost under ``locate_file``'s concurrent per-fragment PC/PS
        disk callers (``units/device.py``'s ``asyncio.gather``, which is
        also exactly why this goes through
        ``AsyncKeyedCache`` rather
        than a bare instance attribute — its in-flight de-duplication
        means concurrent fragments miss together and only one of them
        actually builds the ``Table``).
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
        """``file_meta.file_size`` for ``path``, or ``None`` if
        unavailable (no usable ``file_meta`` table at all, no matching
        row, or a matching row with no ``file_size`` recorded) — see
        ``_get_file_meta_table`` for what "unavailable" covers."""
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

    async def open_file(self, path: str) -> DedupFile:
        """Resolve ``path`` via ``db/file_map`` (+ ``file_meta.file_size``
        when available) and return a ready-to-read ``DedupFile``.

        Raises:
            NotFoundError: see ``locate_file``.
            DataCorruptError: see ``locate_file``.
        """
        location = await self.locate_file(path)
        return self.open_composition(
            location.stream_id, location.session_id, location.comp_offset, size=location.file_size
        )

    def open_composition(
        self, stream_id: StreamId, session_id: SessionId, comp_offset: CompOffset, size: int | None = None
    ) -> DedupFile:
        """Construct a ``DedupFile`` directly from a
        ``(stream_id, session_id, comp_offset)`` triple — the path any
        workload-specific Unit Layer provider ultimately takes once it has
        resolved its own metadata down to this triple."""
        comp_reader = CompositionReader(self._store, self._dir_cache, self._comp_root, stream_id, session_id)
        return DedupFile(comp_reader, self._pool, comp_offset, size=size)

    async def probe_encrypted(self) -> bool | None:
        """Cheaply determine whether this repository is actually vault-encrypted
        — no key needed at all, and never raises ``KeyRequiredError``. Reads
        the repository's own encryption-key record directly
        (``probe_encrypted``) — never opens a bucket file, never touches
        the Pool. Not the compile-time ``RepoInfo.encrypt_algorithm`` field
        (see ``BucketFileHeader.is_vault_encrypted``'s docstring for why
        that's unreliable); this reads the same live, per-repository record
        ``KeyMaterial.resolve_vault_key`` already reads to find one
        specific candidate key's wrapped VaultKey, just its latest entry
        rather than one looked up by id.

        ``None`` means "couldn't tell" (the encryption-key record itself
        is entirely absent, which should not happen for a properly
        initialized repository) — a genuinely different answer from
        "confirmed not encrypted" (``False``), the same distinction
        ``KeyStatus``'s own ``NO_KEY_PROVIDED`` draws for the same reason.

        Result is cached for the lifetime of this (read-only,
        never-changes-under-us) ``DedupRepo`` — repeated calls cost
        nothing after the first.
        """
        return await self._probe_cache.resolve(None)

    async def close(self) -> None:
        """Release every cached sqlite connection (and any temp file/
        directory a slow-path materialization created).

        Settles every in-flight ``db()`` fetch first, not just what's
        already landed in ``_db_sources`` -- a fetch cancelled mid-flight
        (a TUI worker torn down while a catalog load was still running,
        say) can already have opened a real, connected ``SqliteSource``
        that nothing else references; skipping straight to ``.values()``
        would abandon exactly that connection's aiosqlite background
        thread forever. See ``AsyncKeyedCache.settle_all()``'s own
        docstring, and ``api.repository.Repository.close()``'s identical
        reasoning for its own dedup-catalog cache one layer up.

        Every source gets a close attempt regardless of whether an
        earlier one raised or hung -- same "attempt all, then report"
        posture as ``Repository.close()``, for the same reason.
        """
        sources, errors = await self._db_sources.settle_all()
        for source in sources.values():
            try:
                await asyncio.wait_for(source.close(), timeout=_RESOURCE_CLOSE_TIMEOUT)
            except Exception as exc:
                errors.append(exc)
        self._db_sources.invalidate()
        if errors:
            raise ExceptionGroup("DedupRepo.close() failed to close every tracked resource", errors)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
