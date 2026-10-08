"""``PcpsDiskTree``: ``DeviceProvider``'s PC/PS disk listing and
assembly. One physical disk can land as several fragment objects — see
``_pcps_disk_key()`` for the grouping rule and ``PcpsDiskTree.open_disk``
for reassembling a group into one ``VirtualDiskContentSource``.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from .._util.once import AsyncOnce
from ..catalog.version import version_file_ids
from ..errors import NotFoundError
from ..presentation.format import pluralize
from ..storage.sqlite import apply_index_hint
from ..storage.table import sql_placeholders
from ..units.provider_kit import diagnostic_node, disk_fs_containers_before_leaves, paginate
from .base import Node, RestorableUnit, UnitKind
from .content.disk_fs import disk_fs_available
from .content.pcps_disk import DiskFragment, VirtualDiskContentSource
from .device_handles import PcpsDiagnostic, PcpsDisk, PcpsFragment

if TYPE_CHECKING:
    from .device import DeviceProvider


def _format_fids(fids: list[int]) -> str:
    """The fid-list rendering every diagnostic/error message here uses."""
    return ", ".join(f"fid={fid}" for fid in fids)


def _disk_label(disk_uuid: str, disk_index: str) -> str:
    """One disk's ref/message label."""
    return f"{disk_uuid}:{disk_index}"


def _missing_fids_diagnostic(fids: list[int], *, scope: str, table: str) -> str:
    """Shared wording for "some of ``copy_target_file``'s registered fids
    are absent from the currently committed generation" — ``scope`` names
    what's missing them (``"this version"``/``"this disk"``), ``table``
    names which db table (``"file_meta"``/``"file_map"``)."""
    return (
        f"copy_target_file registers {len(fids)} more {pluralize(len(fids), 'object')} for {scope} "
        f"({_format_fids(fids)}), but they are absent from {table}'s currently committed "
        "generation. This SDK cannot tell from the repository alone whether that's expired "
        "retention, an interrupted Copy, or something else — it only reports what it observed."
    )


# FORMAT-SPEC.md: PC/PS disk fragments. D(diskUuid)O(offset)[V(volumeUuid)]S(diskIndex)[_{seq}].img
#
# O and V may be absent; D and S are required, since without them an
# object can't be tied to one physical disk.
_PCPS_NAME_RE = re.compile(r"D\(([^()]*)\)(?:O\([^()]*\))?(?:V\([^()]*\))?S\(([^()]*)\)")


def _pcps_disk_sort_key(key: tuple[str, str]) -> tuple[int, int, str]:
    """Display order for one ``_pcps_disk_key()`` group: real disks with a
    numeric ``diskIndex`` first, ordered by that index rather than
    ``disk_uuid`` (UUID order doesn't match a user's "Disk 0"/"Disk 1"
    expectations). A non-numeric index or a singleton fallback
    (``disk_uuid == "_single"``) sorts after every real disk."""
    disk_uuid, disk_index = key
    if disk_uuid == "_single":
        return (1, int(disk_index), disk_uuid)
    try:
        return (0, int(disk_index), disk_uuid)
    except ValueError:
        return (1, -1, disk_uuid)


def _pcps_disk_key(fid: int, path: str) -> tuple[str, str]:
    """Group key for one PC/PS fragment: ``(disk_uuid, disk_index)``
    parsed from the ``D(...)[O(...)][V(...)]S(...)`` fragment naming
    (FORMAT-SPEC.md: PC/PS disk fragments) Windows ``VOL_BASE`` clients use; a
    non-matching filename (e.g. a macOS PC's single whole-disk object)
    is a singleton group keyed by its own ``fid``."""
    leaf = path.rsplit("/", 1)[-1]
    m = _PCPS_NAME_RE.search(leaf)
    if m is None:
        return ("_single", str(fid))
    disk_uuid, disk_index = m.groups()
    return (disk_uuid, disk_index)


class PcpsDiskTree:
    """One ``DeviceProvider``'s PC/PS disk listing/assembly — resolves
    ``copy_target_version`` → ``copy_target_file`` → ``file_meta``
    (FORMAT-SPEC.md: dedup data mapping chain) into disk nodes, and assembles a disk's
    fragments into one ``ContentSource`` only when opened.
    """

    def __init__(self, provider: DeviceProvider) -> None:
        self._provider = provider
        # (all disk nodes, never-resolved fids); the version is immutable.
        # Single-flight, so concurrent first listings run one query.
        self._nodes: AsyncOnce[tuple[list[Node], list[int]]] = AsyncOnce(self._build_nodes)
        # (disk_uuid, disk_index) -> assembled unit; safe to share since
        # VirtualDiskContentSource reads are stateless.
        self._disk_units: dict[tuple[str, str], RestorableUnit] = {}

    async def object_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        """List this PC/PS version's disks. Any registered ``fid`` that
        never resolved in ``file_meta`` is summarized in one diagnostic
        node, appended on the first page only and only when it fits within
        ``limit``."""
        all_nodes, never_resolved = await self._nodes.get()
        nodes = paginate(all_nodes, offset, limit)
        if never_resolved and offset == 0 and (limit is None or len(nodes) < limit):
            nodes.append(self._diagnostic_node(never_resolved))
        return nodes

    async def _build_nodes(self) -> tuple[list[Node], list[int]]:
        """Group every resolved fragment by disk into nodes, without
        opening any composition (``open_disk`` does that). Returns ``(all
        disk nodes, fids registered in copy_target_file but never resolved
        in file_meta)``."""
        repo = self._provider.repo
        fids = await version_file_ids(repo, self._provider.version)
        if not fids:
            return [], []
        placeholders = sql_placeholders(len(fids))
        file_meta_conn = await repo.db("file_meta")
        # Unlike copy_target_version/copy_target_file, file_meta has no
        # existing index on `path` -- this may build a real one on a
        # materialized connection.
        await apply_index_hint(file_meta_conn, "file_meta", ["path"])
        # Unpaginated: the never-resolved diagnostic needs the complete set.
        # file_size gives the listing a real size without opening fragments.
        file_meta_cursor = await file_meta_conn.execute(
            f"SELECT fid, path, file_size FROM file_meta WHERE fid IN ({placeholders})", fids
        )
        resolved: dict[int, tuple[str, int | None]] = {
            fid: (path, file_size) for fid, path, file_size in await file_meta_cursor.fetchall()
        }

        disks: dict[tuple[str, str], list[tuple[int, str, int | None]]] = defaultdict(list)
        for fid, (path, file_size) in resolved.items():
            disks[_pcps_disk_key(fid, path)].append((fid, path, file_size))

        # Sliced as a flat node list: a key can contribute two nodes.
        ordered_keys = sorted(disks.keys(), key=_pcps_disk_sort_key)
        all_nodes: list[Node] = []
        for key in ordered_keys:
            all_nodes.extend(self._disk_nodes(key, disks[key]))
        # Each "(filesystem)" sibling keeps its disk-index order among its peers.
        all_nodes = disk_fs_containers_before_leaves(all_nodes)

        # Registrations can age out of file_meta/file_map's generation
        # window (FORMAT-SPEC.md: Multi-generation selection); the diagnostic doesn't guess why.
        never_resolved = [fid for fid in fids if fid not in resolved]
        return all_nodes, never_resolved

    def _disk_nodes(self, key: tuple[str, str], fragments: list[tuple[int, str, int | None]]) -> list[Node]:
        """Build one disk's listing node, plus its "(filesystem)" sibling
        when disk-fs is available. The size is ``file_meta.file_size``
        (identical across a disk's fragments), ``None`` when unknown until
        ``open_disk``."""
        disk_uuid, disk_index = key
        disk_size = next((file_size for _fid, _path, file_size in fragments if file_size is not None), None)
        is_single = disk_uuid == "_single"
        name = fragments[0][1].rsplit("/", 1)[-1] if is_single else f"Disk {disk_index}"
        ref_key = disk_index if is_single else _disk_label(disk_uuid, disk_index)
        disk_node = Node(
            ref=self._provider.extra_ref(f"pcps-disk:{ref_key}"),
            name=name,
            is_leaf=True,
            kind=UnitKind.DISK_IMAGE,
            size=disk_size,
            details={"disk_uuid": disk_uuid, "disk_index": disk_index},
            handle=PcpsDisk(
                disk_uuid=disk_uuid,
                disk_index=disk_index,
                fragments=tuple(PcpsFragment(fid, path, file_size) for fid, path, file_size in fragments),
            ),
        )
        if not disk_fs_available():
            return [disk_node]
        fs_node = self._provider.disk_fs.root_node(
            disk_key=("pcps", disk_uuid, disk_index), source_node=disk_node, name=name
        )
        return [disk_node, fs_node]

    def _diagnostic_node(self, missing_fids: list[int]) -> Node:
        return diagnostic_node(
            self._provider.extra_ref(f"pcps-diagnostic:{','.join(str(fid) for fid in missing_fids)}"),
            f"({len(missing_fids)} registered {pluralize(len(missing_fids), 'object')} not found in current data)",
            _missing_fids_diagnostic(missing_fids, scope="this version", table="file_meta"),
            handle=PcpsDiagnostic(tuple(missing_fids)),
        )

    async def open_disk(self, node: Node, handle: PcpsDisk) -> RestorableUnit:
        """Assemble one PC/PS disk's fragments into a
        ``VirtualDiskContentSource``, resolving them concurrently.

        Fragments that fail to resolve make the unit ``degraded`` (the
        technical detail goes in ``details["missing_fragments"]``) instead of
        failing the whole disk; reads in their range come back as a hole. A successful assembly is cached per
        disk; a failed one is retried on the next call.

        Raises:
            NotFoundError: Every fragment failed to resolve.
        """
        repo = self._provider.repo
        disk_key = (handle.disk_uuid, handle.disk_index)
        cached = self._disk_units.get(disk_key)
        if cached is not None:
            return cached

        async def _open_one(frag: PcpsFragment) -> tuple[int, DiskFragment | None]:
            fid, path, file_size = frag.fid, frag.src_file_path, frag.file_size
            try:
                content = await repo.open_file(path, fallback_size=file_size)
            except NotFoundError:
                # Per fragment: gather() would otherwise fail the whole disk.
                return fid, None
            record = await content.cached_record()
            start, end = await record.extent()
            return fid, DiskFragment(fid=fid, start=start, end=end, dedup_file=content, src_file_path=path)

        opened = await asyncio.gather(*(_open_one(frag) for frag in handle.fragments))
        disk_fragments = [frag for _fid, frag in opened if frag is not None]
        failed_fids = [fid for fid, frag in opened if frag is None]

        disk_label = _disk_label(*disk_key)
        if not disk_fragments:
            raise NotFoundError(
                f"disk {disk_label} has {len(failed_fids)} {pluralize(len(failed_fids), 'fragment')} "
                f"registered in file_meta ({_format_fids(failed_fids)}), but none of them "
                "resolve in file_map's currently committed generation",
                ref=disk_label,
            )

        disk_size = node.size if node.size is not None else max(f.end for f in disk_fragments)
        vdisk = VirtualDiskContentSource(size=disk_size, fragments=disk_fragments)
        degraded = None
        details = dict(node.details)
        if failed_fids:
            total = len(failed_fids) + len(disk_fragments)
            degraded = (
                f"{len(failed_fids)} of {total} parts of this disk are missing from the backup; they read as zeros"
            )
            details["missing_fragments"] = _missing_fids_diagnostic(failed_fids, scope="this disk", table="file_map")
        unit = RestorableUnit.of(node, vdisk, size=disk_size, degraded=degraded, details=details)
        self._disk_units[disk_key] = unit
        return unit
