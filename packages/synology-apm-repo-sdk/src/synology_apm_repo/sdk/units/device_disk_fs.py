"""``DiskFsSibling``: the ``units/content/disk_fs/``-backed "filesystem
inside a disk image" axis, additive to a disk-image leaf built by either
of ``units/device.py``'s two disk axes — when Dissect can't recognize a
filesystem, this sibling is absent or shows one diagnostic leaf, never
affecting the disk-image node itself.

Resolution goes through the owning ``DeviceProvider``'s own ``unit``
(never duplicated here), so this class never needs to know whether the
disk image came from the VM path or a PC/PS ``VirtualDiskContentSource``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict, cast

from ..errors import NotFoundError
from .base import FileState, Node, RestorableUnit, UnitKind, diagnostic_node, paginate
from .content.disk_fs import DiskFilesystem, DiskFilesystemUnavailableError
from .device_kind import _NodeKind

if TYPE_CHECKING:
    from .device import DeviceProvider


#: A disk-fs sibling's own stable cache key — either ``("object",
#: object_id)`` (VM/FS) or ``("pcps", disk_uuid, disk_index)`` (PC/PS).
DiskKey = tuple[str, int] | tuple[str, str, str]


class _DiskFsSharedAttrs(TypedDict):
    """The keys every disk-fs ``Node.attrs`` dict carries regardless of
    ``_kind`` — narrows the base ``Node``'s ``attrs: dict[str, Any]`` so
    a typo'd key or missing key is a ``mypy`` error, not a runtime
    ``KeyError``. Every key here must be carried forward to every child
    node ``children()`` builds."""

    _kind: _NodeKind
    disk_key: DiskKey
    source_node: Node


class _DiskFsEntryAttrs(_DiskFsSharedAttrs):
    """``_NodeKind.DISK_FS_ENTRY`` only — a partition/directory/file node
    one level (or more) below the "(filesystem)" sibling root, so it
    also carries which partition and path within it this node
    addresses."""

    partition_addr: int
    path: str
    #: This file's cloud-sync/encryption state, surfaced as
    #: ``Node.attrs["file_state"]`` — always ``FileState.NORMAL`` for a
    #: partition/directory node or a file this SDK has no reason to flag.
    file_state: FileState


class _DiskFsDiagnosticAttrs(TypedDict):
    """``_NodeKind.DISK_FS_DIAGNOSTIC`` only — unrelated to
    ``_DiskFsSharedAttrs``/``_DiskFsEntryAttrs`` (this node never
    resolves a real disk-fs entry, so it carries neither ``disk_key``
    nor ``source_node``)."""

    _kind: _NodeKind
    _raw_reason: str | None
    diagnostic: str


#: Public ``Node.attrs`` key: set on a "``<name>`` (filesystem)" sibling
#: root to the disk-image ``Node``'s own ``ref`` it sits beside, so a
#: caller can associate the two without parsing ``name`` text.
DISK_FS_SIBLING_REF_ATTR = "disk_fs_sibling_ref"


class DiskFsSibling:
    """One ``DeviceProvider``'s disk-fs sibling axis: builds the
    "``<name>`` (filesystem)" node next to a disk-image leaf, and lazily
    parses it via ``DiskFilesystem`` the first time someone navigates
    into it — never eagerly for every disk in a listing.
    """

    def __init__(self, provider: DeviceProvider) -> None:
        self._provider = provider
        # One DiskFilesystem per browsed disk -- None means "already
        # tried, found nothing", so a repeat children() call doesn't
        # re-parse it just to get the same diagnostic node again.
        self._filesystems: dict[DiskKey, DiskFilesystem | None] = {}
        # Populated only when a disk's DiskFilesystem resolved to None
        # because opening its real content raised NotFoundError (data simply
        # absent from this repository copy), rather than Dissect recognizing
        # nothing.
        self._diagnostic_reasons: dict[DiskKey, str] = {}

    def root_node(self, *, disk_key: DiskKey, source_node: Node, name: str) -> Node:
        """The "``<name>`` (filesystem)" sibling next to a disk-image leaf
        — pure construction, no I/O. ``source_node`` is stashed here so
        ``resolve`` can reuse the owning ``DeviceProvider``'s ``unit()``
        dispatch instead of duplicating VM/PC-PS resolution logic."""
        ref = source_node.ref.child("fs")
        attrs: _DiskFsSharedAttrs = {"_kind": _NodeKind.DISK_FS_ROOT, "disk_key": disk_key, "source_node": source_node}
        return Node(
            ref=ref,
            name=f"{name} (filesystem)",
            is_leaf=False,
            kind=UnitKind.DISK_FILESYSTEM,
            attrs={**attrs, DISK_FS_SIBLING_REF_ATTR: source_node.ref},
        )

    async def resolve(self, disk_key: DiskKey, node: Node) -> DiskFilesystem | None:
        if disk_key in self._filesystems:
            return self._filesystems[disk_key]
        source_node = cast("_DiskFsSharedAttrs", node.attrs)["source_node"]
        assert isinstance(source_node, Node)
        disk_fs: DiskFilesystem | None
        try:
            content = (await self._provider.unit(source_node)).open()
            disk_fs = await DiskFilesystem.open(content)
        except DiskFilesystemUnavailableError:
            # Reaching here means dissect.* was importable enough for
            # find_spec but the real import failed (broken/partial
            # install) -- same diagnostic shape as a parse failure.
            disk_fs = None
        except NotFoundError as exc:
            # The disk's real bytes aren't present in this repository
            # copy (a metadata-only PC/PS sample with @data/ excluded) --
            # the first real read Dissect issues hits a composition
            # chunk never synced. Same diagnostic shape as a genuine
            # parse failure.
            self._diagnostic_reasons[disk_key] = str(exc)
            disk_fs = None
        self._filesystems[disk_key] = disk_fs
        return disk_fs

    def diagnostic_node(self, node: Node) -> Node:
        # The raw reason (an internal path, when from a NotFoundError) is
        # kept out of user-facing text (Presentation principle) and only
        # exposed via attrs["_raw_reason"].
        raw_reason = self._diagnostic_reasons.get(cast("_DiskFsSharedAttrs", node.attrs)["disk_key"])
        diagnostic_attrs: _DiskFsDiagnosticAttrs = {
            "_kind": _NodeKind.DISK_FS_DIAGNOSTIC,
            "_raw_reason": raw_reason,
            "diagnostic": (
                "no filesystem could be recognized on any partition of this disk — it may be "
                "encrypted (e.g. BitLocker), use an unsupported filesystem (e.g. HFS+/ISO9660), "
                "not be a partitioned/filesystem-bearing image, or this repository copy may simply "
                "not have this disk's real content synced. The whole disk image itself is still "
                "available unchanged via its own node, one level up."
            ),
        }
        return diagnostic_node(
            node.ref.child("diagnostic"), "(no filesystem recognized on this disk)", dict(diagnostic_attrs)
        )

    async def children(self, node: Node, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        shared_attrs = cast("_DiskFsSharedAttrs", node.attrs)
        disk_key = shared_attrs["disk_key"]
        disk_fs = await self.resolve(disk_key, node)
        if disk_fs is None:
            return [] if offset else [self.diagnostic_node(node)]

        # A pasted canonical ref can land directly on a partition/directory
        # node in a fresh provider instance with an empty _filesystems
        # cache -- resolve() above only succeeded because `node` still
        # carries source_node. Every child built below must carry it
        # forward too.
        source_node = shared_attrs["source_node"]

        if shared_attrs["_kind"] == _NodeKind.DISK_FS_ROOT:
            partitions = disk_fs.partitions()
            window = paginate(partitions, offset, limit)
            return [
                Node(
                    ref=node.ref.child(f"p{addr}"),
                    name=label,
                    is_leaf=False,
                    kind=UnitKind.DISK_FILESYSTEM,
                    attrs=dict(
                        _DiskFsEntryAttrs(
                            _kind=_NodeKind.DISK_FS_ENTRY,
                            disk_key=disk_key,
                            source_node=source_node,
                            partition_addr=addr,
                            path="/",
                            file_state=FileState.NORMAL,
                        )
                    ),
                )
                for addr, label in window
            ]

        entry_attrs = cast("_DiskFsEntryAttrs", node.attrs)
        partition_addr = entry_attrs["partition_addr"]
        path = entry_attrs["path"]
        entries = await disk_fs.list_dir(partition_addr, path)
        dir_window = paginate(entries, offset, limit)
        parent = "" if path == "/" else path
        children_nodes = []
        for entry in dir_window:
            attrs: dict[str, Any] = dict(
                _DiskFsEntryAttrs(
                    _kind=_NodeKind.DISK_FS_ENTRY,
                    disk_key=disk_key,
                    source_node=source_node,
                    partition_addr=partition_addr,
                    path=f"{parent}/{entry.name}",
                    file_state=entry.file_state,
                )
            )
            # Not through mtime_attrs(): entry.mtime already arrives as a
            # real datetime -- set directly, omitted when None.
            if entry.mtime is not None:
                attrs["mtime"] = entry.mtime
            children_nodes.append(
                Node(
                    ref=node.ref.child(entry.name),
                    name=entry.name,
                    is_leaf=not entry.is_dir,
                    kind=UnitKind.DISK_FILESYSTEM if entry.is_dir else UnitKind.DISK_FILE,
                    size=entry.size,
                    attrs=attrs,
                )
            )
        return children_nodes

    async def open_entry(self, node: Node) -> RestorableUnit:
        entry_attrs = cast("_DiskFsEntryAttrs", node.attrs)
        disk_key = entry_attrs["disk_key"]
        disk_fs = self._filesystems.get(disk_key)
        if disk_fs is None:
            # unit() reached directly (e.g. a saved ref pasted back in)
            # without children() resolving this disk first -- resolve
            # now rather than assume it's cached.
            disk_fs = await self.resolve(disk_key, node)
        if disk_fs is None:
            raise NotFoundError("no filesystem recognized on this disk", ref=str(node.ref))
        content = await disk_fs.open_file(entry_attrs["partition_addr"], entry_attrs["path"])
        return RestorableUnit(
            ref=node.ref,
            name=node.name,
            is_leaf=True,
            kind=UnitKind.DISK_FILE,
            size=content.size,
            attrs=node.attrs,
            content=content,
        )
