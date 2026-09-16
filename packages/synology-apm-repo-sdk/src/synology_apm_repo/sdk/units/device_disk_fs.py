"""``DiskFsSibling``: the ``units/content/disk_fs.py``-backed "filesystem
inside a disk image" axis, additive to a disk-image leaf built by either
of ``units/device.py``'s two disk axes (VM's own ``_object_nodes()``, or
``units/device_pcps.py``'s ``PcpsDiskTree``) — see ``units/device.py``'s own
module docstring for the whole-picture rationale (what "additive" means,
and what happens when Dissect can't recognize a disk's filesystem).

Resolution goes through the owning ``DeviceProvider``'s own ``unit`` (never
duplicated here) so this class never needs to know whether the disk
image it's parsing came from the VM path or a PC/PS
``VirtualDiskContentSource`` — both already resolve to the same
``ContentSource`` shape through that one call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, cast

from ..errors import NotFoundError
from .base import Node, RestorableUnit, UnitKind, diagnostic_node, paginate
from .content.disk_fs import DiskFilesystem, DiskFilesystemUnavailableError
from .device_kind import _NodeKind
from .node_ref import NodeRef

if TYPE_CHECKING:
    from .device import DeviceProvider


#: A disk-fs sibling's own stable cache key — either ``("object",
#: object_id)`` (VM/FS, ``object_id`` a real ``object_table.object_id``
#: int) or ``("pcps", disk_uuid, disk_index)`` (PC/PS, both real strings
#: — see ``units/device_pcps.py``'s own ``_pcps_disk_key``). This alias
#: lets mypy catch a wrong-shape tuple introduced anywhere along that
#: flow, even though the ultimate ``object_id`` value itself still
#: originates from an untyped sqlite row unpack (a separate, much
#: broader gap this alias doesn't attempt to close).
DiskKey = tuple[str, int] | tuple[str, str, str]


class _DiskFsSharedAttrs(TypedDict):
    """The keys every disk-fs ``Node.attrs`` dict carries regardless of
    ``_kind`` (``_NodeKind.DISK_FS_ROOT`` or ``_NodeKind.DISK_FS_ENTRY``)
    — narrows the base ``Node``'s own ``attrs: dict[str, Any]`` at this
    family's read/write sites so a typo'd key string (``"disk_uid"`` vs
    ``"disk_uuid"``) or a missing key is a ``mypy`` error, not a runtime
    ``KeyError`` (see ``children()``'s own comment on ``source_node`` for
    why every key must be carried forward). Never used to change
    ``Node.attrs``'s own declared type: different ``_kind`` values
    genuinely carry different, incompatible attrs (see
    ``units/device.py``'s own diagnostic node builders), and downstream
    code already dispatches on ``_kind`` before reading anything else —
    the fix belongs at construction/read sites, not the field's own
    type."""

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


class _DiskFsDiagnosticAttrs(TypedDict):
    """``_NodeKind.DISK_FS_DIAGNOSTIC`` only — same shape the other
    diagnostic node builders in this project use, unrelated to
    ``_DiskFsSharedAttrs``/``_DiskFsEntryAttrs`` (this node never
    resolves a real disk-fs entry, so it carries neither ``disk_key``
    nor ``source_node``)."""

    _kind: _NodeKind
    _raw_reason: str | None
    diagnostic: str


#: Public ``Node.attrs`` key (unlike ``_DiskFsSharedAttrs``'s own keys, this
#: one is part of the cross-layer contract): set on a "``<name>``
#: (filesystem)" sibling root to the disk-image ``Node``'s own ``ref`` it
#: sits beside. Lets a caller associate the two — e.g. to present the
#: sibling nested under the disk image rather than next to it — without
#: parsing ``name`` text or assuming list adjacency.
DISK_FS_SIBLING_REF_ATTR = "disk_fs_sibling_ref"


def disk_fs_sibling_ref(node: Node) -> NodeRef | None:
    """The disk-image sibling's own ``NodeRef`` if ``node`` is a
    "(filesystem)" root built by ``DiskFsSibling.root_node``, else
    ``None``."""
    ref = node.attrs.get(DISK_FS_SIBLING_REF_ATTR)
    return ref if isinstance(ref, NodeRef) else None


class DiskFsSibling:
    """One ``DeviceProvider``'s disk-fs sibling axis: builds the
    "``<name>`` (filesystem)" node next to a disk-image leaf, and lazily
    parses it via ``DiskFilesystem`` the first time someone navigates
    into it — never eagerly for every disk in a listing.
    """

    def __init__(self, provider: DeviceProvider) -> None:
        self._provider = provider
        # One DiskFilesystem per disk that's actually been browsed into,
        # keyed by that disk's own object_id/pcps disk key — None means
        # "already tried, Dissect found nothing", so a repeat children()
        # call on the same disk doesn't re-parse it just to get the same
        # diagnostic node again.
        self._filesystems: dict[DiskKey, DiskFilesystem | None] = {}
        # Populated only when a disk's DiskFilesystem resolved to None
        # because opening its real content raised NotFoundError (data simply
        # absent from this repository copy), rather than Dissect recognizing
        # nothing — see resolve()'s own comment.
        self._diagnostic_reasons: dict[DiskKey, str] = {}

    def root_node(self, *, disk_key: DiskKey, source_node: Node, name: str) -> Node:
        """The "``<name>`` (filesystem)" sibling next to a disk-image leaf
        — pure construction, no I/O (whether Dissect can actually parse
        anything on this disk is only resolved lazily, the first time
        someone navigates into this node — see ``resolve``). ``source_node``
        is the disk-image ``Node`` the caller already built for the *same*
        disk — stashed here (not re-derived) so ``resolve`` can reuse the
        owning ``DeviceProvider``'s own existing ``unit()`` dispatch to get
        that disk's already-correct ``ContentSource``, instead of
        duplicating VM/PC-PS resolution logic a third time."""
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
            # disk_fs_available() already gated whether this node was
            # ever offered at all — reaching here means this
            # environment's dissect.* install was importable enough for
            # find_spec to see it but the real import still failed (a
            # broken/partial install). Same "resolvable in principle,
            # nothing found" diagnostic shape as a parse failure; the
            # diagnostic text below distinguishes the two.
            disk_fs = None
        except NotFoundError as exc:
            # The disk's own real bytes aren't present in this copy of
            # the repository at all (real trap, not hypothetical — a
            # metadata-only PC/PS sample with @data/ excluded, see
            # tests/CLAUDE.md: opening the disk's ContentSource succeeds
            # structurally, but the first real read Dissect issues while
            # sniffing the partition table/filesystem hits a composition
            # chunk that was never synced). Same "nothing to show, whole
            # image still available one level up" diagnostic shape as a
            # genuine parse failure — this SDK reports what it observed,
            # it doesn't distinguish "encrypted"/"unsupported fs"/"data
            # absent" any more finely than diagnostic_node()'s own text
            # already does for the parse-failure case.
            self._diagnostic_reasons[disk_key] = str(exc)
            disk_fs = None
        self._filesystems[disk_key] = disk_fs
        return disk_fs

    def diagnostic_node(self, node: Node) -> Node:
        # The raw reason (an internal store-relative path, when this
        # came from a NotFoundError) is kept out of the user-facing
        # name/diagnostic text (Presentation principle,
        # ARCHITECTURE.md) and only exposed via
        # attrs["_raw_reason"] — same restraint the other diagnostic
        # node builders in units/device.py/units/device_pcps.py use.
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
        # node in a *fresh* provider instance, whose own _filesystems
        # cache is empty (unlike a live tree walk that starts at the root
        # sibling and populates the cache on the way down) — that fresh
        # instance's resolve() call above only succeeded because ``node``
        # itself still carries "source_node". Every child built below
        # must carry it forward too, or the next children()/unit() call
        # on it raises a bare KeyError one level deeper.
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
        return [
            Node(
                ref=node.ref.child(child_name),
                name=child_name,
                is_leaf=not is_dir,
                kind=UnitKind.DISK_FILESYSTEM if is_dir else UnitKind.DISK_FILE,
                size=size,
                attrs=dict(
                    _DiskFsEntryAttrs(
                        _kind=_NodeKind.DISK_FS_ENTRY,
                        disk_key=disk_key,
                        source_node=source_node,
                        partition_addr=partition_addr,
                        path=f"{parent}/{child_name}",
                    )
                ),
            )
            for child_name, is_dir, size in dir_window
        ]

    async def open_entry(self, node: Node) -> RestorableUnit:
        entry_attrs = cast("_DiskFsEntryAttrs", node.attrs)
        disk_key = entry_attrs["disk_key"]
        disk_fs = self._filesystems.get(disk_key)
        if disk_fs is None:
            # unit() reached directly (e.g. a saved ref pasted back in)
            # without children() ever having resolved this disk's
            # filesystem first in this provider instance — resolve it
            # now rather than assuming it must already be cached.
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
