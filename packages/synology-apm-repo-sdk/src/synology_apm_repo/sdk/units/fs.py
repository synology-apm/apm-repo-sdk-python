"""``FsProvider``: FS workloads. Unlike VM/PC/PS, an FS
version has no per-file ``object_table`` rows — the whole version is
one shared virtual image, ``<snapshotUuid>/<versionId>/dedup.img``, and
every file is a ``[content_dedup_id, content_dedup_id + file_size)``
byte range within it (FORMAT-SPEC.md: fs-addressing). This is also the
cheapest tree in the project: ``entry_table.dirname`` is a full
absolute path string, so ``children()`` is one indexed equality query
per level — no recursion, no parent-id chain (FORMAT-SPEC.md: fs-addressing).

Locating ``dedup.img`` itself goes through the same ``target.db``
mechanism as ``units.device`` (FORMAT-SPEC.md: fs-addressing): FS lands a ``target.db``
too, purely to register ``version.db.zst`` as a ``dedup_object=false``
object and give up its ``version_table.version_id``, which combines
with ``Version.target_id`` (the ``snapshotUuid``) to look ``dedup.img`` up
in ``db/file_map``.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Self

import aiosqlite

from ..catalog.version import Version, open_target_db, resolve_copy_meta_dir
from ..dedup.dedup_file import DedupFile
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource, peel
from .base import Node, RestorableUnit, UnitKind, dir_first_order_by, mtime_attrs, not_restorable
from .node_ref import NodeRef, canonical_ref_for

_FILE_TYPE_FILE = 1
_FILE_TYPE_DIR = 2


def _join_path(dirname: str, basename: str) -> str:
    return f"/{basename}" if dirname == "/" else f"{dirname}/{basename}"


class FsProvider:
    """``UnitProvider`` for one FS workload version."""

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        self._repo = repo
        self._version = version
        self._meta_dir: str | None = None
        self._entry_db: SqliteSource | None = None
        self._dedup_img_file: DedupFile | None = None

    # -- location resolution ------------------------------------------

    def _resolve_meta_dir(self) -> str:
        if self._meta_dir is None:
            self._meta_dir = resolve_copy_meta_dir(self._version, self._repo.layout.repo_root)
        return self._meta_dir

    async def _entry_table_connection(self) -> aiosqlite.Connection:
        if self._entry_db is not None:
            return self._entry_db.connection
        meta_dir = self._resolve_meta_dir()  # raises NotFoundError if this version has no meta at all
        meta = self._version.meta
        assert meta is not None  # guaranteed by the successful _resolve_meta_dir() call above
        version_db_rel = next((f for f in meta.meta_filenames if f.endswith("version.db.zst")), None)
        if version_db_rel is None:
            raise NotFoundError(f"version {self._version.version_uid} has no version.db.zst in meta_filenames")
        raw = await self._repo.store.read(f"{meta_dir}/{version_db_rel}")
        # asyncio.to_thread: peel() itself stays synchronous by design --
        # it is pure bytes work, no I/O -- but this
        # is the highest-risk peel() call site in the codebase --
        # version.db.zst is a full filesystem backup's entire
        # file/directory listing, potentially the largest single payload
        # this project ever decrypts+decompresses. Left on the event loop,
        # a multi-MB decrypt+decompress would stall every other Task for
        # its duration -- the same responsiveness reasoning behind
        # dedup/pool's own to_thread hops.
        payload, _envelopes = await asyncio.to_thread(peel, raw, vault_key=self._repo.vault_key)
        self._entry_db = await SqliteSource.from_bytes(payload)
        return self._entry_db.connection

    async def _dedup_img(self) -> DedupFile:
        if self._dedup_img_file is not None:
            return self._dedup_img_file
        meta_dir = self._resolve_meta_dir()
        async with await open_target_db(self._repo, self._version, meta_dir) as target_db:
            cursor = await target_db.connection.execute("SELECT version_id FROM version_table")
            row = await cursor.fetchone()
        if row is None:
            raise NotFoundError("target.db has no version_table row", ref=meta_dir)
        version_id = row[0]
        path = f"{self._version.target_id}/{version_id}/dedup.img"
        location = await self._repo.locate_file(path)
        self._dedup_img_file = self._repo.open_composition(
            location.stream_id, location.session_id, location.comp_offset, size=location.file_size
        )
        return self._dedup_img_file

    # -- UnitProvider -------------------------------------------------

    def _ref_for_dir(self, dirname: str) -> NodeRef:
        segments = [seg for seg in dirname.split("/") if seg]
        return canonical_ref_for(self._repo, self._version, segments)

    async def close(self) -> None:
        """Release the sqlite connection(s) this provider opened. Not
        merely for tidiness: a leaked ``aiosqlite`` connection's own
        background worker thread keeps the interpreter alive."""
        if self._entry_db is not None:
            await self._entry_db.close()
            self._entry_db = None

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
        return Node(ref=self._ref_for_dir("/"), name="/", is_leaf=False, attrs={"dirname": "/"})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        dirname = node.attrs.get("dirname")
        if dirname is None:
            return []
        conn = await self._entry_table_connection()
        # apply_index_hint() is safe to call unconditionally here (a cheap
        # leading-prefix check skips it when the columns are already
        # indexed, and a read-only connection just makes CREATE INDEX
        # raise, caught as a no-op).
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
            attrs: dict[str, Any] = {"xattr": xattr, "path": child_path}
            attrs.update(mtime_attrs(file_mtime))
            if is_dir:
                attrs["dirname"] = child_path
                nodes.append(Node(ref=self._ref_for_dir(child_path), name=basename, is_leaf=False, attrs=attrs))
            else:
                attrs["content_dedup_id"] = content_dedup_id
                nodes.append(
                    Node(
                        ref=self._ref_for_dir(child_path),
                        name=basename,
                        is_leaf=True,
                        kind=UnitKind.FILE,
                        size=file_size,
                        attrs=attrs,
                    )
                )
        return nodes

    async def unit(self, node: Node) -> RestorableUnit:
        content_dedup_id = node.attrs.get("content_dedup_id")
        if content_dedup_id is None:
            not_restorable("node", node.name)
        dedup_img = await self._dedup_img()
        view = dedup_img.view(int(content_dedup_id), node.size or 0)
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, attrs=node.attrs, content=view
        )
