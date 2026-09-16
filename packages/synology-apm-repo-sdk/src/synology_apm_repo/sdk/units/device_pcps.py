"""``PcpsDiskTree``: the PC/PS disk-fragment listing/assembly axis, split
out of ``units/device.py``'s ``DeviceProvider`` — see that module's own
docstring for the VM-vs-PC/PS dispatch rule and why PC/PS has no
``target.db`` at all.

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
from .base import Node, RestorableUnit, UnitKind, diagnostic_node, paginate
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
    names which db table (``"file_meta"``/``"file_map"``). See
    ``_disk_nodes``'s/``open_disk``'s own docstrings for why this SDK
    reports the observation rather than guessing a cause."""
    return (
        f"copy_target_file registers {len(fids)} more object(s) for {scope} "
        f"({_format_fids(fids)}), but they are absent from {table}'s currently committed "
        "generation. This SDK cannot tell from the repository alone whether that's expired "
        "retention, an interrupted Copy, or something else — it only reports what it observed."
    )


# FORMAT-SPEC.md: pcps-fragments: D(diskUuid)O(offset)[V(volumeUuid)]S(diskIndex)[_{seq}].img
#
# Every component is independently optional: the encoder only ever
# emits a component when its value is non-empty, so a real disk-based
# object can legitimately omit ``O(...)``. ``O`` is therefore optional here
# too — an object legitimately missing it must not fall back to the
# singleton case below. ``D``/``S`` stay mandatory for *this* regex
# regardless: this is the disk-grouping key specifically, and an object
# missing either isn't identifiable as belonging to one particular
# physical disk anyway.
_PCPS_NAME_RE = re.compile(r"D\(([^()]*)\)(?:O\([^()]*\))?(?:V\([^()]*\))?S\(([^()]*)\)")


def _pcps_disk_key(fid: int, path: str) -> tuple[str, str]:
    """Group key for one PC/PS fragment: ``(disk_uuid, disk_index)``
    parsed from the ``D(...)[O(...)][V(...)]S(...)`` naming convention
    (FORMAT-SPEC.md: pcps-fragments) when the filename matches it — Windows
    ``VOL_BASE`` clients segment a disk into several fragment objects
    named this way, while a real macOS PC sample instead lands one
    whole-disk object with no such naming at all. A filename that
    doesn't match falls back to a key unique to this one ``fid``, so it
    becomes its own singleton group of exactly one fragment rather than
    being silently dropped or lumped in with something unrelated."""
    leaf = path.rsplit("/", 1)[-1]
    m = _PCPS_NAME_RE.search(leaf)
    if m is None:
        return ("_single", str(fid))
    disk_uuid, disk_index = m.groups()
    return (disk_uuid, disk_index)


class _PcpsFragment(TypedDict):
    """One ``node.attrs["fragments"]`` entry — ``_disk_nodes``'s own dict
    literal is this shape's only producer, ``open_disk``'s ``_open_one``
    its only reader. Narrows the base ``Node``'s own ``attrs: dict[str,
    Any]`` at both sites instead of each independent ``isinstance()``
    check on read. ``file_size`` is genuinely ``int | None`` (``file_meta.
    file_size`` itself can be NULL), the same case ``_open_one`` already
    falls back for via ``location.file_size``."""

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
        # (all real disk nodes, never-resolved fids) from _build_nodes(),
        # resolved once and reused by every object_nodes() call
        # regardless of offset/limit — a provider is scoped to one
        # immutable version, so this never needs to change across
        # repeated children() calls, matching DiskFsSibling's own
        # cache-once posture.
        self._nodes: tuple[list[Node], list[int]] | None = None
        # (disk_uuid, disk_index) -> the RestorableUnit open_disk()
        # already assembled for it — a second open() of the same disk
        # (preview, then export; or a repeated size query) reuses it
        # rather than re-running every fragment's own locate_file()/
        # composition-open/extent() from scratch. Safe to hand the same
        # frozen RestorableUnit to more than one caller: its own
        # VirtualDiskContentSource.read() is a stateless, explicit-offset
        # read with no cursor to race over.
        self._disk_units: dict[tuple[str, str], RestorableUnit] = {}

    async def object_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        """List this PC/PS version's disks — see ``_build_nodes()`` for
        how they're resolved. Any registered ``fid`` that never resolved
        in ``file_meta`` at all is summarized in one diagnostic node,
        appended on the first page only."""
        if self._nodes is None:
            self._nodes = await self._build_nodes()
        all_nodes, never_resolved = self._nodes
        nodes = paginate(all_nodes, offset, limit)
        # Reported once, on the first page only — it's a summary of the
        # whole version's gap, not a real paginated item. Only when
        # ``nodes`` hasn't already filled the page: appending unconditionally
        # could hand back limit+1 nodes, unlike every sibling diagnostic-
        # node site in units/device.py (_device_nodes, DiskFsSibling.children),
        # which substitute a diagnostic for real content rather than add
        # to it.
        if never_resolved and offset == 0 and (limit is None or len(nodes) < limit):
            nodes.append(self._diagnostic_node(never_resolved))
        return nodes

    async def _build_nodes(self) -> tuple[list[Node], list[int]]:
        """Resolves ``copy_target_version`` → ``copy_target_file`` →
        ``file_meta`` (FORMAT-SPEC.md: version-meta-mapping) and groups every
        resolved fragment by disk (``_pcps_disk_key()``) into one ``Node``
        each — no ``locate_file()``/composition I/O yet, that happens
        lazily in ``open_disk``. Returns ``(all disk nodes, fids registered
        in copy_target_file but never resolved in file_meta)``;
        ``object_nodes`` (this method's only caller) owns pagination and
        the diagnostic node, caching this pair on ``self`` since it never
        changes for the life of the owning provider."""
        repo = self._provider.repo
        version = self._provider.version
        try:
            ctv_conn = await repo.db("copy_target_version")
        except NotFoundError:
            return [], []
        # This ``repo.db(name)`` connection may be the real, immutable
        # on-disk repository file (see storage/sqlite.py's own module
        # docstring for the fast-path/slow-path split) — apply_index_hint()
        # is safe to call unconditionally here (see its own docstring),
        # applied uniformly rather than reasoned about per call site.
        await apply_index_hint(ctv_conn, "copy_target_version", ["version_uid"])
        version_cursor = await ctv_conn.execute(
            "SELECT version_id FROM copy_target_version WHERE version_uid = ?", (version.version_uid,)
        )
        version_row = await version_cursor.fetchone()
        if version_row is None:
            return [], []
        await apply_index_hint(ctv_conn, "copy_target_file", ["version_id"])
        # copy_target_file lives in the same physical sqlite file as
        # copy_target_version, so it's queried through ctv_conn directly
        # rather than a second repo.db("copy_target_file") call —
        # DedupRepo.db() aliases the two names to one shared
        # connection anyway, so going direct just skips a redundant cache
        # lookup for what's already the same connection.
        fid_cursor = await ctv_conn.execute("SELECT fid FROM copy_target_file WHERE version_id = ?", (version_row[0],))
        fids = [r[0] for r in await fid_cursor.fetchall()]
        if not fids:
            return [], []
        placeholders = sql_placeholders(len(fids))
        file_meta_conn = await repo.db("file_meta")
        # Unlike copy_target_version/copy_target_file, file_meta has no
        # existing index on ``path`` — this one genuinely may build a real
        # index the first time this path runs against a materialized
        # (non-immutable) connection.
        await apply_index_hint(file_meta_conn, "file_meta", ["path"])
        # Fetched unpaginated and sliced in Python (unlike the VM path's
        # object_nodes()) specifically so the "which fids from
        # copy_target_file never resolved in file_meta" diagnostic below
        # can compare against the *complete* set — pushing a LIMIT/OFFSET
        # into this query would only ever see the resolved subset, never
        # the gap itself. Real versions register a handful of fids (one
        # per disk/volume), never thousands, so this stays cheap.
        #
        # ``file_size`` is selected here, not just at open time, so the
        # listing itself carries a real size, matching the VM path's
        # object_nodes() — file_meta has
        # the column, and DedupRepo.locate_file() already trusts it
        # via FileLocation.file_size for the exact same objects.
        file_meta_cursor = await file_meta_conn.execute(
            f"SELECT fid, path, file_size FROM file_meta WHERE fid IN ({placeholders})", fids
        )
        resolved: dict[int, tuple[str, int | None]] = {
            fid: (path, file_size) for fid, path, file_size in await file_meta_cursor.fetchall()
        }

        disks: dict[tuple[str, str], list[tuple[int, str, int | None]]] = defaultdict(list)
        for fid, (path, file_size) in resolved.items():
            disks[_pcps_disk_key(fid, path)].append((fid, path, file_size))

        # Built for every key up front, then sliced as a flat node list
        # (not by key) — same reasoning the VM path's object_nodes() has
        # for object_table: a disk with a "(filesystem)" sibling contributes
        # two nodes for one key, so windowing by key first wouldn't
        # produce exactly ``limit`` nodes per page.
        ordered_keys = sorted(disks.keys())
        all_nodes: list[Node] = []
        for key in ordered_keys:
            all_nodes.extend(self._disk_nodes(key, disks[key]))

        # An old version's own registrations can age out of
        # file_meta/file_map's generation-selection window
        # (FORMAT-SPEC.md: generation-selection) once its Copy
        # destination stops receiving new versions. Deliberately does not
        # guess *why* (expired retention vs. corruption vs. anything else
        # look identical from here) — see
        # _diagnostic_node()'s own attrs["diagnostic"] text.
        never_resolved = [fid for fid in fids if fid not in resolved]
        return all_nodes, never_resolved

    def _disk_nodes(self, key: tuple[str, str], fragments: list[tuple[int, str, int | None]]) -> list[Node]:
        """Build one disk's listing node, plus its "(filesystem)" sibling
        when ``disk-fs`` is available (see ``units/device.py``'s own
        docstring) — pure construction, no I/O (see ``object_nodes``'s own
        docstring for why). ``disk_size`` comes straight from
        ``file_meta.file_size`` (identical across every sibling fragment,
        FORMAT-SPEC.md: pcps-fragments) with no fallback here — a real per-fragment
        ``extent()``-based fallback only makes sense once a fragment's
        composition is actually open, which is ``open_disk``'s job, not
        this one's."""
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
        ``VirtualDiskContentSource``. Fragments resolve independently and
        open concurrently.

        A fragment that fails to resolve is recorded in
        ``attrs["diagnostic"]`` instead of failing the whole disk — reads
        landing in its range come back as an ordinary hole.

        A successful assembly is cached on ``self`` by ``(disk_uuid,
        disk_index)`` — see ``self._disk_units``'s own comment — so a
        second ``unit()`` call on the same disk skips straight to the
        cached ``RestorableUnit``. A failed assembly (every fragment
        unresolvable) is not cached and is retried from scratch on the
        next call.

        Raises:
            NotFoundError: Every fragment failed to resolve, so there is
                nothing to build a disk from.
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
                # Caught per-fragment, not left to propagate: asyncio.gather()
                # below would otherwise cancel every other fragment's task on
                # this one's first exception, turning one missing fragment
                # into a hard failure for the whole disk. Any *other*
                # exception still propagates and fails this call outright.
                return fid, None
            size = location.file_size if location.file_size is not None else file_size
            content = repo.open_composition(location.stream_id, location.session_id, location.comp_offset, size=size)
            record = await content._get_record()  # noqa: SLF001 - DedupFile/CompositionRecord are one cohesive unit
            start, end = await record.extent()
            return fid, DiskFragment(fid=fid, start=start, end=end, dedup_file=content, src_file_path=path)

        # Every fragment's own resolution is independent of every other's
        # (nothing here reads what another fragment's locate_file()/extent()
        # call produced), so this runs them concurrently rather than one at a
        # time — a real speedup opening a multi-fragment PC/PS disk, since a
        # VM's single-composition disk_image never pays this setup cost at
        # all.
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
