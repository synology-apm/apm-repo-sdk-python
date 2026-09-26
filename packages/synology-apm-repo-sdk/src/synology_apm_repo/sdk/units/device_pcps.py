"""``PcpsDiskTree``: the PC/PS disk-fragment listing/assembly axis, split
out of ``units/device.py``'s ``DeviceProvider``, which dispatches to it
based on ``Version.target_type`` alone, never by probing which files
exist.

**PC/PS disk grouping:** one physical disk can land as several
independently-registered fragment objects, not one — see
``_pcps_disk_key()`` for the grouping rule and ``PcpsDiskTree.open_disk``
for reassembling a group's fragments into one ``VirtualDiskContentSource``.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import TYPE_CHECKING, TypedDict

from ..errors import NotFoundError
from ..storage.sqlite import apply_index_hint
from ..storage.table import sql_placeholders
from .base import Node, RestorableUnit, UnitKind, diagnostic_node, disk_fs_containers_before_leaves, paginate
from .content.disk_fs import disk_fs_available
from .content.pcps_disk import DiskFragment, VirtualDiskContentSource
from .device_kind import _NodeKind

if TYPE_CHECKING:
    from .device import DeviceProvider


def _format_fids(fids: list[int]) -> str:
    """``", ".join(f"fid={fid}" ...)`` — the fid-list rendering shared by
    every diagnostic/error message below that names which fids didn't
    resolve."""
    return ", ".join(f"fid={fid}" for fid in fids)


def _disk_label(disk_uuid: str, disk_index: str) -> str:
    """``"{disk_uuid}:{disk_index}"`` — this disk's own display/ref label,
    shared by every site that needs to name one disk in a message."""
    return f"{disk_uuid}:{disk_index}"


def _missing_fids_diagnostic(fids: list[int], *, scope: str, table: str) -> str:
    """Shared wording for "some of ``copy_target_file``'s registered fids
    are absent from the currently committed generation" — ``scope`` names
    what's missing them (``"this version"``/``"this disk"``), ``table``
    names which db table (``"file_meta"``/``"file_map"``)."""
    return (
        f"copy_target_file registers {len(fids)} more object(s) for {scope} "
        f"({_format_fids(fids)}), but they are absent from {table}'s currently committed "
        "generation. This SDK cannot tell from the repository alone whether that's expired "
        "retention, an interrupted Copy, or something else — it only reports what it observed."
    )


# FORMAT-SPEC.md: pcps-fragments: D(diskUuid)O(offset)[V(volumeUuid)]S(diskIndex)[_{seq}].img
#
# Every component is independently optional in the encoder, so O may
# legitimately be absent without falling back to the singleton case
# below. D/S stay mandatory for this regex: without them an object isn't
# identifiable as belonging to one physical disk.
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
    parsed from the ``D(...)[O(...)][V(...)]S(...)`` naming convention
    (FORMAT-SPEC.md: pcps-fragments) when the filename matches — Windows
    ``VOL_BASE`` clients segment a disk into several fragment objects
    named this way; a real macOS PC sample lands one whole-disk object
    with no such naming. A non-matching filename falls back to a
    singleton group unique to its own ``fid``."""
    leaf = path.rsplit("/", 1)[-1]
    m = _PCPS_NAME_RE.search(leaf)
    if m is None:
        return ("_single", str(fid))
    disk_uuid, disk_index = m.groups()
    return (disk_uuid, disk_index)


class _PcpsFragment(TypedDict):
    """One ``node.attrs["fragments"]`` entry, shared between
    ``_disk_nodes`` (producer) and ``open_disk`` (reader). ``file_size``
    is genuinely ``int | None`` (``file_meta.file_size`` can be NULL)."""

    fid: int
    src_file_path: str
    file_size: int | None


class PcpsDiskTree:
    """One ``DeviceProvider``'s PC/PS disk listing/assembly — resolves
    ``copy_target_version`` → ``copy_target_file`` → ``file_meta``
    (FORMAT-SPEC.md: version-meta-mapping) into disk nodes, and assembles a
    disk's fragments into one real ``ContentSource`` only when actually
    opened.
    """

    def __init__(self, provider: DeviceProvider) -> None:
        self._provider = provider
        # (all disk nodes, never-resolved fids) from _build_nodes(),
        # cached once -- a provider is scoped to one immutable version,
        # so this never changes.
        self._nodes: tuple[list[Node], list[int]] | None = None
        # (disk_uuid, disk_index) -> the RestorableUnit already assembled
        # for it -- a second open() reuses it instead of re-resolving
        # every fragment. Safe to share: VirtualDiskContentSource.read()
        # is a stateless, explicit-offset read.
        self._disk_units: dict[tuple[str, str], RestorableUnit] = {}

    async def object_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        """List this PC/PS version's disks. Any registered ``fid`` that
        never resolved in ``file_meta`` is summarized in one diagnostic
        node, appended on the first page only."""
        if self._nodes is None:
            self._nodes = await self._build_nodes()
        all_nodes, never_resolved = self._nodes
        nodes = paginate(all_nodes, offset, limit)
        # Reported once, on the first page, only if it wouldn't push the
        # page past `limit` -- unlike sibling diagnostic-node sites
        # elsewhere, this adds to the page rather than substituting for
        # real content.
        if never_resolved and offset == 0 and (limit is None or len(nodes) < limit):
            nodes.append(self._diagnostic_node(never_resolved))
        return nodes

    async def _build_nodes(self) -> tuple[list[Node], list[int]]:
        """Groups every resolved fragment by disk (``_pcps_disk_key()``)
        into one ``Node`` each — no ``locate_file()``/composition I/O yet;
        that happens lazily in ``open_disk``. Returns ``(all disk nodes,
        fids registered in copy_target_file but never resolved in
        file_meta)``."""
        repo = self._provider.repo
        version = self._provider.version
        try:
            ctv_conn = await repo.db("copy_target_version")
        except NotFoundError:
            return [], []
        # apply_index_hint() is safe to call unconditionally -- a
        # read-only connection just makes CREATE INDEX raise, caught as a
        # no-op.
        await apply_index_hint(ctv_conn, "copy_target_version", ["version_uid"])
        version_cursor = await ctv_conn.execute(
            "SELECT version_id FROM copy_target_version WHERE version_uid = ?", (version.version_uid,)
        )
        version_row = await version_cursor.fetchone()
        if version_row is None:
            return [], []
        await apply_index_hint(ctv_conn, "copy_target_file", ["version_id"])
        # copy_target_file lives in the same physical sqlite file as
        # copy_target_version -- queried through ctv_conn directly rather
        # than a second repo.db() call to the same underlying connection.
        fid_cursor = await ctv_conn.execute("SELECT fid FROM copy_target_file WHERE version_id = ?", (version_row[0],))
        fids = [r[0] for r in await fid_cursor.fetchall()]
        if not fids:
            return [], []
        placeholders = sql_placeholders(len(fids))
        file_meta_conn = await repo.db("file_meta")
        # Unlike copy_target_version/copy_target_file, file_meta has no
        # existing index on `path` -- this may build a real one on a
        # materialized connection.
        await apply_index_hint(file_meta_conn, "file_meta", ["path"])
        # Fetched unpaginated, sliced in Python: the never-resolved
        # diagnostic below needs the complete set, not a
        # LIMIT/OFFSET-narrowed one.
        #
        # file_size selected here so the listing carries a real size
        # without opening each fragment.
        file_meta_cursor = await file_meta_conn.execute(
            f"SELECT fid, path, file_size FROM file_meta WHERE fid IN ({placeholders})", fids
        )
        resolved: dict[int, tuple[str, int | None]] = {
            fid: (path, file_size) for fid, path, file_size in await file_meta_cursor.fetchall()
        }

        disks: dict[tuple[str, str], list[tuple[int, str, int | None]]] = defaultdict(list)
        for fid, (path, file_size) in resolved.items():
            disks[_pcps_disk_key(fid, path)].append((fid, path, file_size))

        # Built for every key up front, sliced as a flat node list: a
        # disk with a filesystem sibling contributes two nodes per key,
        # so windowing by key wouldn't produce exactly `limit` nodes per
        # page.
        ordered_keys = sorted(disks.keys(), key=_pcps_disk_sort_key)
        all_nodes: list[Node] = []
        for key in ordered_keys:
            all_nodes.extend(self._disk_nodes(key, disks[key]))
        # disk_fs_containers_before_leaves(): each "(filesystem)" sibling
        # keeps its own disk-index-ordered position relative to its peers.
        all_nodes = disk_fs_containers_before_leaves(all_nodes)

        # Registrations can age out of file_meta/file_map's
        # generation-selection window (FORMAT-SPEC.md:
        # generation-selection) -- deliberately doesn't guess why; see
        # _diagnostic_node()'s own text.
        never_resolved = [fid for fid in fids if fid not in resolved]
        return all_nodes, never_resolved

    def _disk_nodes(self, key: tuple[str, str], fragments: list[tuple[int, str, int | None]]) -> list[Node]:
        """Build one disk's listing node, plus its "(filesystem)" sibling
        when ``disk-fs`` is available — pure construction, no I/O.
        ``disk_size`` comes from ``file_meta.file_size`` (identical
        across sibling fragments) with no per-fragment fallback here —
        that's ``open_disk``'s job, once a fragment's composition is
        actually open."""
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
            attrs={
                "_kind": _NodeKind.PCPS_DISK,
                "disk_uuid": disk_uuid,
                "disk_index": disk_index,
                "fragments": tuple(
                    _PcpsFragment(fid=fid, src_file_path=path, file_size=file_size)
                    for fid, path, file_size in fragments
                ),
            },
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
            f"({len(missing_fids)} registered object(s) not found in current data)",
            {
                "_kind": _NodeKind.PCPS_DIAGNOSTIC,
                "missing_fids": tuple(missing_fids),
                "diagnostic": _missing_fids_diagnostic(missing_fids, scope="this version", table="file_meta"),
            },
        )

    async def open_disk(self, node: Node) -> RestorableUnit:
        """Assemble one PC/PS disk's fragments (deferred from
        ``_disk_nodes()``): resolve each via ``locate_file()``, open its
        composition, and read its real extent
        (``CompositionRecord.extent``), combined into one
        ``VirtualDiskContentSource``. Fragments resolve and open
        concurrently.

        A fragment that fails to resolve is recorded in
        ``attrs["diagnostic"]`` instead of failing the whole disk — reads
        landing in its range come back as an ordinary hole.

        A successful assembly is cached on ``self`` by ``(disk_uuid,
        disk_index)`` — safe to share (``VirtualDiskContentSource.read()``
        is stateless). A failed assembly (every fragment unresolvable) is
        not cached and is retried from scratch.

        Raises:
            NotFoundError: Every fragment failed to resolve.
        """
        repo = self._provider.repo
        disk_key = (str(node.attrs["disk_uuid"]), str(node.attrs["disk_index"]))
        cached = self._disk_units.get(disk_key)
        if cached is not None:
            return cached

        async def _open_one(frag: _PcpsFragment) -> tuple[int, DiskFragment | None]:
            fid = frag["fid"]
            path = frag["src_file_path"]
            file_size = frag["file_size"]
            try:
                location = await repo.locate_file(path)
            except NotFoundError:
                # Caught per-fragment -- gather() would otherwise cancel
                # every other fragment's task on the first exception,
                # turning one missing fragment into a hard failure for
                # the whole disk.
                return fid, None
            size = location.file_size if location.file_size is not None else file_size
            content = repo.open_composition(location.stream_id, location.session_id, location.comp_offset, size=size)
            record = await content.cached_record()
            start, end = await record.extent()
            return fid, DiskFragment(fid=fid, start=start, end=end, dedup_file=content, src_file_path=path)

        # Every fragment resolves independently, so this runs them
        # concurrently -- a real speedup for a multi-fragment disk.
        opened = await asyncio.gather(*(_open_one(frag) for frag in node.attrs["fragments"]))
        disk_fragments = [frag for _fid, frag in opened if frag is not None]
        failed_fids = [fid for fid, frag in opened if frag is None]

        disk_label = _disk_label(*disk_key)
        if not disk_fragments:
            raise NotFoundError(
                f"disk {disk_label} has {len(failed_fids)} fragment(s) "
                f"registered in file_meta ({_format_fids(failed_fids)}), but none of them "
                "resolve in file_map's currently committed generation",
                ref=disk_label,
            )

        disk_size = node.size if node.size is not None else max(f.end for f in disk_fragments)
        vdisk = VirtualDiskContentSource(size=disk_size, fragments=disk_fragments)
        attrs = dict(node.attrs)
        if failed_fids:
            attrs["diagnostic"] = _missing_fids_diagnostic(failed_fids, scope="this disk", table="file_map")
        unit = RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=disk_size, attrs=attrs, content=vdisk
        )
        self._disk_units[disk_key] = unit
        return unit
