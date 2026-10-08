"""``FsProvider``: FS workloads. The whole version is one shared virtual
image, ``<snapshotUuid>/<versionId>/dedup.img``, and every file is a
``[content_dedup_id, content_dedup_id + file_size)`` byte range within it
(FORMAT-SPEC.md: Workload-specific addressing, FS). ``entry_table.dirname`` is a full absolute path,
so ``children()`` is one indexed equality query per level.

``dedup.img`` is located through the version's ``target.db``: its
``version_table.version_id`` combined with ``Version.target_id`` (the
``snapshotUuid``) is the path looked up in ``db/file_map``.
"""

from __future__ import annotations

import dataclasses
from typing import Any, override

import aiosqlite

from .._util.closing import AsyncClosing
from .._util.once import AsyncOnce
from ..catalog.version import Version, open_target_db, resolve_copy_meta_dir, target_version_id
from ..dedup.dedup_file import DedupFile
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource
from ..units.provider_kit import dir_first_order_by, mtime_from_epoch, not_restorable
from .base import Node, RestorableUnit, UnitKind
from .node_ref import NodeRef, canonical_ref_for

#: Fallback ceiling on ``version.db.zst``'s decompressed size, used only
#: when its zstd frame declares none (a declared size is enforced instead —
#: see ``format.compression.iter_decompressed_zstd``).
_MAX_VERSION_DB_DECOMPRESS_SIZE_FALLBACK = 2 << 30  # 2 GiB

_FILE_TYPE_FILE = 1
_FILE_TYPE_DIR = 2


def _join_path(dirname: str, basename: str) -> str:
    return f"/{basename}" if dirname == "/" else f"{dirname}/{basename}"


@dataclasses.dataclass(frozen=True, slots=True)
class _Dir:
    """``Node.handle`` of a directory: its ``entry_table.dirname``."""

    dirname: str


@dataclasses.dataclass(frozen=True, slots=True)
class _File:
    """``Node.handle`` of a file: its offset in the version's dedup image."""

    content_dedup_id: int


class FsProvider(AsyncClosing):
    """``ClosableUnitProvider`` for one FS workload version."""

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        self._repo = repo
        self._version = version
        self._entry_db: AsyncOnce[SqliteSource] = AsyncOnce(self._open_entry_db)
        self._dedup_img: AsyncOnce[DedupFile] = AsyncOnce(self._open_dedup_img)

    # -- location resolution ------------------------------------------

    def _resolve_meta_dir(self) -> str:
        """This version's ``copy_meta_file/<dir>``; pure, so not cached.

        Raises:
            NotFoundError: No usable ``copy_target_version_meta`` row.
        """
        return resolve_copy_meta_dir(self._version, self._repo.layout.repo_root)

    async def _entry_table_connection(self) -> aiosqlite.Connection:
        return (await self._entry_db.get()).connection

    async def _open_entry_db(self) -> SqliteSource:
        meta_dir = self._resolve_meta_dir()
        meta = self._version.meta
        assert meta is not None  # _resolve_meta_dir() succeeded
        version_db_rel = next((f for f in meta.meta_filenames if f.endswith("version.db.zst")), None)
        if version_db_rel is None:
            raise NotFoundError(f"version {self._version.version_uid} has no version.db.zst in meta_filenames")
        raw = await self._repo.store.read(f"{meta_dir}/{version_db_rel}")
        entry_db, _envelopes = await SqliteSource.from_enveloped_bytes(
            raw,
            vault_key=self._repo.vault_key,
            max_output_size=_MAX_VERSION_DB_DECOMPRESS_SIZE_FALLBACK,
            what="version.db.zst",
        )
        return entry_db

    async def dedup_img(self) -> DedupFile:
        """The version's shared ``dedup.img`` composition, opened once and
        cached — the content every file node's ``(offset, size)`` indexes.

        Raises:
            NotFoundError: The version's meta directory, ``target.db``'s
                ``version_table`` row, or ``dedup.img`` itself is missing.
            ResourceLimitExceededError: ``target.db``'s temporary copy
                doesn't fit with the free-space reserve left.
        """
        return await self._dedup_img.get()

    async def _open_dedup_img(self) -> DedupFile:
        meta_dir = self._resolve_meta_dir()
        async with await open_target_db(self._repo, self._version, meta_dir) as target_db:
            version_id = await target_version_id(target_db, meta_dir)
        path = f"{self._version.target_id}/{version_id}/dedup.img"
        return await self._repo.open_file(path)

    # -- UnitProvider -------------------------------------------------

    def _ref_for_dir(self, dirname: str) -> NodeRef:
        segments = [seg for seg in dirname.split("/") if seg]
        return canonical_ref_for(self._repo, self._version, segments)

    @override
    async def close(self) -> None:
        """Close the entry-table connection, if opened."""
        await self._entry_db.close(SqliteSource.close)

    def root(self) -> Node:
        """The ``/`` directory node. No I/O."""
        return Node(ref=self._ref_for_dir("/"), name="/", is_leaf=False, handle=_Dir("/"))

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if not isinstance(node.handle, _Dir):
            return []
        dirname = node.handle.dirname
        conn = await self._entry_table_connection()
        await apply_index_hint(conn, "entry_table", ["dirname"])
        order_by = dir_first_order_by(f"file_type = {_FILE_TYPE_DIR}", "basename, rowid")
        cursor = await conn.execute(
            "SELECT basename, file_size, file_mtime, file_type, content_dedup_id, xattr "
            f"FROM entry_table WHERE dirname = ? ORDER BY {order_by} LIMIT ? OFFSET ?",
            (dirname, limit if limit is not None else -1, offset),
        )
        rows = await cursor.fetchall()
        nodes = []
        for basename, file_size, file_mtime, file_type, content_dedup_id, xattr in rows:
            is_dir = file_type == _FILE_TYPE_DIR
            child_path = _join_path(dirname, basename)
            details: dict[str, Any] = {"xattr": xattr, "path": child_path}
            mtime = mtime_from_epoch(file_mtime)
            if is_dir:
                nodes.append(
                    Node(
                        ref=self._ref_for_dir(child_path),
                        name=basename,
                        is_leaf=False,
                        mtime=mtime,
                        details=details,
                        handle=_Dir(child_path),
                    )
                )
            else:
                nodes.append(
                    Node(
                        ref=self._ref_for_dir(child_path),
                        name=basename,
                        is_leaf=True,
                        kind=UnitKind.FILE,
                        size=file_size,
                        mtime=mtime,
                        details=details,
                        handle=None if content_dedup_id is None else _File(int(content_dedup_id)),
                    )
                )
        return nodes

    async def unit(self, node: Node) -> RestorableUnit:
        if not isinstance(node.handle, _File):
            not_restorable("node", node.name)
        dedup_img = await self.dedup_img()
        view = dedup_img.view(node.handle.content_dedup_id, node.size or 0)
        return RestorableUnit.of(node, view)
