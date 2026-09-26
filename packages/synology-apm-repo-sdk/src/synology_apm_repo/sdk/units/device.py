"""``DeviceProvider``: VM/PC/PS workloads via ``copy_meta_file``.

The dedup engine's smallest unit for these workload types is a whole
disk/volume image (FORMAT-SPEC.md: copy_meta_file-layout) — this
provider's disk-image leaves are exactly that. Every dedup disk-image
object also gets one additive sibling node, "``<name>`` (filesystem)",
built by ``units.content.disk_fs`` (Dissect-based) via
``units.device_disk_fs``, parsing the guest OS filesystem
(NTFS/FAT/exFAT/ext2-4/XFS/Btrfs/APFS) inside that disk image. When
Dissect can't recognize a filesystem, that sibling is absent or shows one
diagnostic leaf — the whole-image node is unaffected either way.

Two paths, dispatched directly on ``Version.target_type`` (``"VM"`` vs
``{"PC", "PS"}``), never by probing which files exist:

- **VM/FS**: ``target.db`` → ``version_table`` → ``device_table`` (one
  row per disk device) → ``object_table`` (one row per disk image, joined
  on ``config_device_id``). Handled directly in this module.
- **PC/PS**: no ``target.db``, only ``snapshot_info.json``. The disk list
  comes from ``db/copy_target_file`` (``version_id`` → ``fid``) joined
  with ``db/file_meta`` (``fid`` → ``path``), looked up in
  ``db/file_map``. Handled by ``device_pcps.PcpsDiskTree``, since one
  physical PC/PS disk can land as several fragment objects that need
  reassembling.

**Trap:** a PC/PS landing directory exists for every normally-landed
version too, just without ``target.db`` — ``target_type`` is the only
reliable dispatch signal.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from types import TracebackType
from typing import Self

from ..catalog.version import Version, open_target_db, resolve_copy_meta_dir
from ..catalog.workload import TargetType
from ..dedup.dedup_file import DEFAULT_STREAM_BLOCK, ExportResult, stream_via_read
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError, UnsupportedDataFormatError
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource
from .base import (
    ContentSource,
    Node,
    RestorableUnit,
    UnitKind,
    disk_fs_containers_before_leaves,
    not_restorable,
    paginate,
)
from .content.disk_fs import disk_fs_available
from .device_disk_fs import DiskFsSibling
from .device_kind import _NodeKind
from .device_pcps import PcpsDiskTree
from .node_ref import NodeRef, canonical_ref_for

_DATA_FORMAT_DEDUP = 1


class DeviceProvider:
    """``UnitProvider`` for one VM/PC/PS workload version.

    **Build one with** ``create``, never ``DeviceProvider(...)`` directly
    — the documented construction path, even though it does no I/O.
    """

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        """Pure field initialization."""
        self._repo = repo
        self._version = version
        self._meta_dir: str | None = None
        self._target_db: SqliteSource | None = None
        self._is_pcps = version.target_type != TargetType.VM
        # Delegates PC/PS listing and the disk-fs sibling axis to these
        # two collaborators.
        self._pcps = PcpsDiskTree(self)
        self._disk_fs = DiskFsSibling(self)

    @classmethod
    async def create(cls, repo: DedupRepo, version: Version) -> Self:
        return cls(repo, version)

    # -- shared accessors for PcpsDiskTree/DiskFsSibling ---------------

    @property
    def repo(self) -> DedupRepo:
        """Exposed for ``PcpsDiskTree``/``DiskFsSibling``, the two
        collaborators this class delegates to."""
        return self._repo

    @property
    def version(self) -> Version:
        """Same reason as ``repo``."""
        return self._version

    @property
    def disk_fs(self) -> DiskFsSibling:
        """The disk-fs sibling collaborator — exposed so ``PcpsDiskTree``
        can build a PC/PS disk's own "(filesystem)" sibling node the
        same way this class's own ``_object_nodes`` does for a VM disk."""
        return self._disk_fs

    def extra_ref(self, *extra: str) -> NodeRef:
        """Append to this provider's version ref, rather than nesting a
        new ``repo_path`` — nesting would bake a literal ``#`` into the
        ref string that ``NodeRef.parse()`` then silently swallows on
        round-trip. Public — also used by ``PcpsDiskTree``."""
        return self._version_ref().child(*extra)

    # -- copy_meta_file location (VM/FS only) --------------------------

    def _resolve_meta_dir(self) -> str:
        """The ``copy_meta_file/<dir>`` this VM/FS version's ``target.db``
        lives under. VM-only.

        Raises:
            NotFoundError: No ``copy_target_version_meta`` row for this
                version.
        """
        if self._meta_dir is not None:
            return self._meta_dir
        path = resolve_copy_meta_dir(self._version, self._repo.layout.repo_root)
        self._meta_dir = path
        return path

    async def _target_db_source(self) -> SqliteSource:
        if self._target_db is not None:
            return self._target_db
        meta_dir = self._resolve_meta_dir()
        self._target_db = await open_target_db(self._repo, self._version, meta_dir)
        return self._target_db

    async def close(self) -> None:
        """Release the sqlite connection(s) this provider opened — an
        unclosed ``aiosqlite`` connection's background thread (no
        ``daemon=True``) keeps the interpreter alive forever.
        ``Repository.close()`` already does this for every provider it
        hands out.
        """
        if self._target_db is not None:
            await self._target_db.close()
            self._target_db = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        """Pure construction, no I/O. Carries no ``degraded``/caveat
        attrs — whether this version's objects resolve is only knowable
        via a lazy, per-call query (``PcpsDiskTree.object_nodes``), not
        here."""
        if self._is_pcps:
            return Node(ref=self._version_ref(), name="Disks", is_leaf=False, attrs={"_kind": _NodeKind.PCPS_ROOT})
        return Node(ref=self._version_ref(), name="Devices", is_leaf=False, attrs={"_kind": _NodeKind.ROOT})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        kind = node.attrs.get("_kind")
        match kind:
            case _NodeKind.ROOT:
                return await self._device_nodes(offset=offset, limit=limit)
            case _NodeKind.DEVICE:
                return await self._object_nodes(node.attrs["config_device_id"], offset=offset, limit=limit)
            case _NodeKind.PCPS_ROOT:
                return await self._pcps.object_nodes(offset=offset, limit=limit)
            case _NodeKind.DISK_FS_ROOT | _NodeKind.DISK_FS_ENTRY:
                return await self._disk_fs.children(node, offset=offset, limit=limit)
            case _:
                return []

    async def unit(self, node: Node) -> RestorableUnit:
        kind = node.attrs.get("_kind")
        match kind:
            case _NodeKind.OBJECT:
                return await self._open_object(node)
            case _NodeKind.PCPS_DISK:
                return await self._pcps.open_disk(node)
            case _NodeKind.DISK_FS_ENTRY:
                return await self._disk_fs.open_entry(node)
            case _NodeKind.PCPS_DIAGNOSTIC:
                # Not a real restorable object -- attrs["diagnostic"] holds why.
                raise NotFoundError(node.attrs["diagnostic"], ref=str(node.attrs["missing_fids"]))
            case _NodeKind.DISK_FS_DIAGNOSTIC:
                # Not a real restorable object -- attrs["diagnostic"] holds why.
                raise NotFoundError(node.attrs["diagnostic"], ref=self._version.version_uid)
            case _:
                not_restorable("node", node.name)

    # -- VM path ----------------------------------------------------------

    def _version_ref(self) -> NodeRef:
        return canonical_ref_for(self._repo, self._version)

    async def _device_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        # A version with no registered/removed target.db routinely raises
        # NotFoundError here -- a normal degrade the caller already
        # handles, not a bug. Left to propagate.
        conn = (await self._target_db_source()).connection
        # ORDER BY host_name, with device_id (the real rowid PK) as the
        # pagination tiebreaker.
        cursor = await conn.execute(
            "SELECT config_device_id, device_uuid, host_name, os_name FROM device_table "
            "ORDER BY host_name, device_id LIMIT ? OFFSET ?",
            (limit if limit is not None else -1, offset),
        )
        rows = await cursor.fetchall()
        nodes = []
        for config_device_id, device_uuid, host_name, os_name in rows:
            ref = self.extra_ref(f"device:{config_device_id}")
            nodes.append(
                Node(
                    ref=ref,
                    name=host_name,
                    is_leaf=False,
                    attrs={
                        "_kind": _NodeKind.DEVICE,
                        "config_device_id": config_device_id,
                        "device_uuid": device_uuid,
                        "os_name": os_name,
                    },
                )
            )
        return nodes

    async def _object_nodes(self, config_device_id: int, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        conn = (await self._target_db_source()).connection
        version_cursor = await conn.execute("SELECT version_id FROM version_table")
        version_row = await version_cursor.fetchone()
        if version_row is None:
            raise NotFoundError("target.db has no version_table row", ref=self._meta_dir)
        version_id = version_row[0]
        # object_table has no index covering (version_id,
        # config_device_id) -- applied unconditionally; safe as a no-op
        # when already covered or the connection is read-only.
        await apply_index_hint(conn, "object_table", ["version_id", "config_device_id"])
        # Fetched unpaginated, sliced in Python: a dedup object
        # contributes two nodes (disk image + filesystem sibling), so SQL
        # LIMIT/OFFSET wouldn't match this method's actual node count.
        # coverage.py attributes this multi-line call's hit to its closing
        # line, not this opening one, so this line is flagged uncovered
        # even though it always executes.
        cursor = await conn.execute(  # pragma: no cover
            "SELECT object_id, data_format, file_path, src_file_path, temp_postfix, dedup_object, file_size "
            "FROM object_table WHERE version_id = ? AND config_device_id = ? "
            "AND (temp_postfix IS NULL OR temp_postfix = '') "
            "ORDER BY file_path, object_id",
            (version_id, config_device_id),
        )
        rows = await cursor.fetchall()
        nodes = []
        for object_id, data_format, file_path, src_file_path, _temp_postfix, dedup_object, file_size in rows:
            leaf_name = file_path.rsplit("/", 1)[-1] if file_path else f"object-{object_id}"
            ref = self.extra_ref(f"device:{config_device_id}", f"object:{object_id}")
            unsupported = bool(dedup_object) and data_format != _DATA_FORMAT_DEDUP
            object_node = Node(
                ref=ref,
                name=leaf_name,
                is_leaf=True,
                kind=UnitKind.DISK_IMAGE if dedup_object else UnitKind.FILE,
                size=file_size,
                attrs={
                    "_kind": _NodeKind.OBJECT,
                    "object_id": object_id,
                    "data_format": data_format,
                    "file_path": file_path,
                    "src_file_path": src_file_path,
                    "dedup_object": bool(dedup_object),
                    "unsupported": unsupported,
                },
            )
            nodes.append(object_node)
            if dedup_object and not unsupported and disk_fs_available():
                nodes.append(
                    self._disk_fs.root_node(disk_key=("object", object_id), source_node=object_node, name=leaf_name)
                )
        # disk_fs_containers_before_leaves(): each "(filesystem)" sibling
        # keeps its own file_path/object_id-ordered position relative to
        # its peers.
        return paginate(disk_fs_containers_before_leaves(nodes), offset, limit)

    async def _open_object(self, node: Node) -> RestorableUnit:
        attrs = node.attrs
        content: ContentSource
        if attrs["dedup_object"]:
            if attrs["data_format"] != _DATA_FORMAT_DEDUP:
                raise UnsupportedDataFormatError(
                    f"object {attrs['object_id']} has unsupported data_format {attrs['data_format']} "
                    "(CBT chains are not yet supported — v1 refuses rather than return wrong content)",
                    ref=attrs["src_file_path"],
                )
            location = await self._repo.locate_file(attrs["src_file_path"])
            content = self._repo.open_composition(
                location.stream_id, location.session_id, location.comp_offset, size=node.size
            )
        else:
            meta_dir = self._resolve_meta_dir()
            content = await _LocalFileContentSource.create(self._repo, f"{meta_dir}/{attrs['file_path']}", node.size)
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, attrs=attrs, content=content
        )


class _LocalFileContentSource:
    """A ``ContentSource`` reading a file directly from the store, not
    through the dedup layer — for the non-dedup descriptor files
    (``.vmx``/``.vmdk`` headers/``.delta``) under ``copy_meta_file/<dir>/``.

    Build one with ``create``: a caller-unknown size is resolved via
    ``store.size()`` there, since ``size`` must stay a sync property."""

    def __init__(self, repo: DedupRepo, path: str, size: int | None) -> None:
        self._repo = repo
        self._path = path
        self._size = size

    @classmethod
    async def create(cls, repo: DedupRepo, path: str, size: int | None) -> Self:
        return cls(repo, path, size if size is not None else await repo.store.size(path))

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — a plain store-level file read has no bucket
        concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return await self._repo.store.read(self._path, offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        # Plain files, not dedup content -- always a full, non-sparse
        # copy regardless of `sparse`. Returns a real ExportResult so a
        # caller treating every ContentSource uniformly still has one to
        # report.
        assert self.size is not None
        data = await self.read(0, self.size)
        # No native async form for local file writes in CPython -- routed
        # through to_thread.
        await asyncio.to_thread(Path(dst).write_bytes, data)
        return ExportResult(bytes_written=self.size, logical_size=self.size, holes=0, zeros=0)
