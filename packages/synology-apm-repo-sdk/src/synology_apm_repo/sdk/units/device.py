"""``DeviceProvider``: VM/PC/PS workloads via ``copy_meta_file``.

The smallest backed-up unit for these workload types is a whole
disk/volume image (FORMAT-SPEC.md: Landing directory layout); this
provider's disk-image leaves are exactly that. When a Dissect package is
installed, every supported dedup disk image also gets one sibling node,
"``<name>`` (filesystem)", built via
``units.device_disk_fs`` by parsing the guest filesystem inside the
image with ``units.content.disk_fs``. When no filesystem is recognized,
that sibling is absent or shows one diagnostic leaf; the whole-image
node is unaffected.

Two paths, dispatched directly on ``Version.target_type`` (``"VM"`` vs
``{"PC", "PS"}``), never by probing which files exist:

- **VM**: ``target.db`` → ``version_table`` → ``device_table`` (one
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

from typing import Self, override

from .._util.closing import AsyncClosing, close_preserving
from .._util.once import AsyncOnce
from ..catalog.version import Version, open_target_db, resolve_copy_meta_dir, target_version_id
from ..catalog.workload import TargetType
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError, UnsupportedDataFormatError
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource
from ..units.provider_kit import disk_fs_containers_before_leaves, not_restorable, paginate
from .base import ContentSource, Node, RestorableUnit, UnitKind
from .content.disk_fs import disk_fs_available
from .content.local_file import LocalFileContentSource
from .device_disk_fs import DiskFsSibling
from .device_handles import (
    Device,
    DeviceRoot,
    DiskFsDiagnostic,
    DiskFsEntry,
    DiskFsRoot,
    PcpsDiagnostic,
    PcpsDisk,
    PcpsRoot,
    VmObject,
)
from .device_pcps import PcpsDiskTree
from .node_ref import NodeRef, canonical_ref_for

_DATA_FORMAT_DEDUP = 1


class DeviceProvider(AsyncClosing):
    """``ClosableUnitProvider`` for one VM/PC/PS workload version. Build
    one with ``create``."""

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        self._repo = repo
        self._version = version
        self._target_db: AsyncOnce[SqliteSource] = AsyncOnce(self._open_target_db)
        self._is_pcps = version.target_type != TargetType.VM
        self._pcps = PcpsDiskTree(self)
        self._disk_fs = DiskFsSibling(self)

    @classmethod
    async def create(cls, repo: DedupRepo, version: Version) -> Self:
        """Build a provider for ``version``. No I/O happens here."""
        return cls(repo, version)

    # -- shared accessors for PcpsDiskTree/DiskFsSibling ---------------

    @property
    def repo(self) -> DedupRepo:
        """For the ``PcpsDiskTree``/``DiskFsSibling`` collaborators."""
        return self._repo

    @property
    def version(self) -> Version:
        """For the ``PcpsDiskTree``/``DiskFsSibling`` collaborators."""
        return self._version

    @property
    def disk_fs(self) -> DiskFsSibling:
        """The disk-fs sibling collaborator, through which ``PcpsDiskTree``
        builds a PC/PS disk's "(filesystem)" sibling node."""
        return self._disk_fs

    def extra_ref(self, *extra: str) -> NodeRef:
        """This provider's version ref with ``extra`` segments appended
        (never a nested ``repo_path``, whose literal ``#`` would not
        survive ``NodeRef.parse()``)."""
        return self._version_ref().child(*extra)

    # -- copy_meta_file location (VM only) -----------------------------

    def _resolve_meta_dir(self) -> str:
        """The ``copy_meta_file/<dir>`` this VM version's ``target.db``
        lives under.

        Raises:
            NotFoundError: No ``copy_target_version_meta`` row for this
                version.
        """
        return resolve_copy_meta_dir(self._version, self._repo.layout.repo_root)

    async def _open_target_db(self) -> SqliteSource:
        source = await open_target_db(self._repo, self._version, self._resolve_meta_dir())
        try:
            # object_table has no index covering (version_id, config_device_id);
            # a no-op when already covered or the connection is read-only.
            await apply_index_hint(source.connection, "object_table", ["version_id", "config_device_id"])
        except BaseException as exc:
            await close_preserving(exc, [source.close])
            raise
        return source

    async def _target_db_source(self) -> SqliteSource:
        return await self._target_db.get()

    @override
    async def close(self) -> None:
        """Close the ``target.db`` connection, if opened."""
        await self._target_db.close(SqliteSource.close)

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        """The top node: "Disks" for PC/PS, "Devices" for VM. No I/O."""
        if self._is_pcps:
            return Node(ref=self._version_ref(), name="Disks", is_leaf=False, handle=PcpsRoot())
        return Node(ref=self._version_ref(), name="Devices", is_leaf=False, handle=DeviceRoot())

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        match node.handle:
            case DeviceRoot():
                return await self._device_nodes(offset=offset, limit=limit)
            case Device(config_device_id=config_device_id):
                return await self._object_nodes(config_device_id, offset=offset, limit=limit)
            case PcpsRoot():
                return await self._pcps.object_nodes(offset=offset, limit=limit)
            case DiskFsRoot() | DiskFsEntry():
                return await self._disk_fs.children(node, offset=offset, limit=limit)
            case _:
                return []

    async def unit(self, node: Node) -> RestorableUnit:
        """Open ``node`` as a restorable unit.

        Raises:
            NotFoundError: ``node`` is a diagnostic placeholder, or its data
                is missing from this repository.
            DataCorruptError: Its ``file_map`` entry is marked
                Corrupted/Tainted.
            UnsupportedDataFormatError: A VM disk object is not plain dedup
                content (a CBT chain).
            NotRestorableError: ``node`` is not a restorable unit.
        """
        match node.handle:
            case VmObject() as handle:
                return await self._open_object(node, handle)
            case PcpsDisk() as handle:
                return await self._pcps.open_disk(node, handle)
            case DiskFsEntry() as handle:
                return await self._disk_fs.open_entry(node, handle)
            case PcpsDiagnostic(missing_fids=missing_fids):
                # Not a real restorable object -- node.diagnostic holds why.
                raise NotFoundError(node.diagnostic or "", ref=str(missing_fids))
            case DiskFsDiagnostic():
                # Not a real restorable object -- node.diagnostic holds why.
                raise NotFoundError(node.diagnostic or "", ref=self._version.version_uid)
            case _:
                not_restorable("node", node.name)

    # -- VM path ----------------------------------------------------------

    def _version_ref(self) -> NodeRef:
        return canonical_ref_for(self._repo, self._version)

    async def _device_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        # A missing target.db raises NotFoundError, which callers handle.
        conn = (await self._target_db_source()).connection
        # device_id (the rowid PK) is the pagination tiebreaker.
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
                    details={"device_uuid": device_uuid, "os_name": os_name},
                    handle=Device(config_device_id),
                )
            )
        return nodes

    async def _object_nodes(self, config_device_id: int, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        source = await self._target_db_source()
        conn = source.connection
        version_id = await target_version_id(source, self._resolve_meta_dir())
        # Sliced in Python: a dedup object contributes two nodes (disk image
        # + filesystem sibling), so SQL LIMIT/OFFSET wouldn't match.
        cursor = await conn.execute(
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
                details={"object_id": object_id, "file_path": file_path},
                handle=VmObject(
                    object_id=object_id,
                    data_format=data_format,
                    file_path=file_path,
                    src_file_path=src_file_path,
                    dedup_object=bool(dedup_object),
                    unsupported=unsupported,
                ),
            )
            nodes.append(object_node)
            if dedup_object and not unsupported and disk_fs_available():
                nodes.append(
                    self._disk_fs.root_node(disk_key=("object", object_id), source_node=object_node, name=leaf_name)
                )
        # Each "(filesystem)" sibling keeps its file_path/object_id order
        # among its peers.
        return paginate(disk_fs_containers_before_leaves(nodes), offset, limit)

    async def _open_object(self, node: Node, handle: VmObject) -> RestorableUnit:
        content: ContentSource
        if handle.dedup_object:
            if handle.data_format != _DATA_FORMAT_DEDUP:
                raise UnsupportedDataFormatError(
                    f"object {handle.object_id} has unsupported data_format {handle.data_format} "
                    "(CBT chains are not yet supported — v1 refuses rather than return wrong content)",
                    ref=handle.src_file_path,
                )
            location = await self._repo.locate_file(handle.src_file_path)
            content = self._repo.open_composition(
                location.stream_id, location.session_id, location.comp_offset, size=node.size
            )
        else:
            meta_dir = self._resolve_meta_dir()
            content = await LocalFileContentSource.create(self._repo.store, f"{meta_dir}/{handle.file_path}", node.size)
        return RestorableUnit.of(node, content)
