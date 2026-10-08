"""``DiskFsSibling``: the ``units/content/disk_fs/``-backed "filesystem
inside a disk image" axis next to a VM or PC/PS disk-image leaf.

The disk image is opened through the owning ``DeviceProvider``'s own
``unit``, so this class never needs to know whether it came from the VM
path or a PC/PS ``VirtualDiskContentSource``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..asynccache import AsyncKeyedCache
from ..errors import NotFoundError
from ..units.provider_kit import diagnostic_node, paginate
from .base import Node, RestorableUnit, UnitKind
from .content.disk_fs import DiskFilesystem, DiskFilesystemUnavailableError
from .device_handles import DiskFsDiagnostic, DiskFsEntry, DiskFsRoot, DiskKey

if TYPE_CHECKING:
    from .device import DeviceProvider


class DiskFsSibling:
    """One ``DeviceProvider``'s disk-fs sibling axis: builds the
    "``<name>`` (filesystem)" node next to a disk-image leaf, and lazily
    parses it via ``DiskFilesystem`` the first time someone navigates
    into it — never eagerly for every disk in a listing.
    """

    def __init__(self, provider: DeviceProvider) -> None:
        self._provider = provider
        # One DiskFilesystem per browsed disk, parsed once even by
        # concurrent first callers -- None means "already tried, found
        # nothing", so a repeat children() call doesn't re-parse it just to
        # get the same diagnostic node again.
        self._filesystems: AsyncKeyedCache[DiskKey, DiskFilesystem | None] = AsyncKeyedCache()
        # Populated only when a disk's DiskFilesystem resolved to None
        # because opening its real content raised NotFoundError (data simply
        # absent from this repository copy), rather than Dissect recognizing
        # nothing.
        self._diagnostic_reasons: dict[DiskKey, str] = {}

    def root_node(self, *, disk_key: DiskKey, source_node: Node, name: str) -> Node:
        """The "``<name>`` (filesystem)" sibling next to the disk-image leaf
        ``source_node``. No I/O."""
        return Node(
            ref=source_node.ref.child("fs"),
            name=f"{name} (filesystem)",
            is_leaf=False,
            kind=UnitKind.DISK_FILESYSTEM,
            handle=DiskFsRoot(disk_key, source_node),
        )

    async def resolve(self, handle: DiskFsRoot | DiskFsEntry) -> DiskFilesystem | None:
        """The disk's parsed filesystem, cached per ``disk_key``; ``None``
        when none could be recognized or the disk's data is absent."""
        return await self._filesystems.resolve(handle.disk_key, lambda _disk_key: self._open(handle))

    async def _open(self, handle: DiskFsRoot | DiskFsEntry) -> DiskFilesystem | None:
        try:
            content = (await self._provider.unit(handle.source_node)).content
            return await DiskFilesystem.open(content)
        except DiskFilesystemUnavailableError:
            # No dissect package installed: same diagnostic as a parse failure.
            return None
        except NotFoundError as exc:
            # The disk's data isn't in this repository copy (e.g. @data/
            # excluded): same diagnostic as a parse failure.
            self._diagnostic_reasons[handle.disk_key] = str(exc)
            return None

    def diagnostic_node(self, node: Node, disk_key: DiskKey) -> Node:
        # The raw reason (an internal path, when from a NotFoundError) is
        # kept out of user-facing text (Presentation principle) and only
        # kept in the handle.
        raw_reason = self._diagnostic_reasons.get(disk_key)
        return diagnostic_node(
            node.ref.child("diagnostic"),
            "(no filesystem recognized on this disk)",
            "no filesystem could be recognized on any partition of this disk — it may be "
            "encrypted (e.g. BitLocker), use an unsupported filesystem (e.g. HFS+/ISO9660), "
            "not be a partitioned/filesystem-bearing image, or this repository copy may simply "
            "not have this disk's real content synced. The whole disk image itself is still "
            "available unchanged via its own node, one level up.",
            handle=DiskFsDiagnostic(raw_reason),
        )

    async def children(self, node: Node, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        """Partitions under a sibling root, or a directory's entries under
        an entry node; one diagnostic leaf when no filesystem was found."""
        handle = node.handle
        assert isinstance(handle, DiskFsRoot | DiskFsEntry)  # DeviceProvider dispatches only these here
        disk_key, source_node = handle.disk_key, handle.source_node
        disk_fs = await self.resolve(handle)
        if disk_fs is None:
            return [] if offset else [self.diagnostic_node(node, disk_key)]

        # A pasted canonical ref can land on a partition/directory node in a
        # fresh provider whose cache is empty; source_node lets resolve()
        # rebuild it, so every child carries it forward.
        if isinstance(handle, DiskFsRoot):
            partitions = disk_fs.partitions()
            window = paginate(partitions, offset, limit)
            return [
                Node(
                    ref=node.ref.child(f"p{addr}"),
                    name=label,
                    is_leaf=False,
                    kind=UnitKind.DISK_FILESYSTEM,
                    details={"path": "/"},
                    handle=DiskFsEntry(disk_key, source_node, addr, "/"),
                )
                for addr, label in window
            ]

        partition_addr, path = handle.partition_addr, handle.path
        entries = await disk_fs.list_dir(partition_addr, path)
        dir_window = paginate(entries, offset, limit)
        parent = "" if path == "/" else path
        children_nodes = []
        for entry in dir_window:
            entry_path = f"{parent}/{entry.name}"
            children_nodes.append(
                Node(
                    ref=node.ref.child(entry.name),
                    name=entry.name,
                    is_leaf=not entry.is_dir,
                    kind=UnitKind.DISK_FILESYSTEM if entry.is_dir else UnitKind.DISK_FILE,
                    size=entry.size,
                    mtime=entry.mtime,
                    file_state=entry.file_state,
                    details={"path": entry_path},
                    handle=DiskFsEntry(disk_key, source_node, partition_addr, entry_path),
                )
            )
        return children_nodes

    async def open_entry(self, node: Node, handle: DiskFsEntry) -> RestorableUnit:
        """Open a file entry as a restorable unit.

        Raises:
            NotFoundError: No filesystem was recognized on the disk.
        """
        # resolve() also covers unit() reached without children() first.
        disk_fs = await self.resolve(handle)
        if disk_fs is None:
            raise NotFoundError("no filesystem recognized on this disk", ref=str(node.ref))
        content = await disk_fs.open_file(handle.partition_addr, handle.path)
        return RestorableUnit.of(node, content, kind=UnitKind.DISK_FILE, size=content.size)
