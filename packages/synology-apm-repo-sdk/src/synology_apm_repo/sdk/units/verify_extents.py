"""Per-workload-type extent resolution for ``verify_reachable``: turns a
``Version`` into the ``CompositionExtent``(s) its content actually lives
in, reusing each workload type's own already-implemented resolution
(``DeviceProvider``/``FsProvider``/``SaasStream``) rather than re-deriving
any path/SQL logic. Feeds into ``units.verify_reachable``'s top-down walk,
which runs FULL or QUICK checks (see ``VerifyLevel``) against the buckets
these extents resolve to.
"""

from __future__ import annotations

import dataclasses

from ..catalog.version import Version
from ..catalog.workload import TargetType, Workload
from ..dedup.dedup_file import DedupFile
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import Finding, Stage, Symptom
from ..errors import ApmRepoError, NotFoundError
from .base import Node
from .content.pcps_disk import VirtualDiskContentSource
from .device import DeviceProvider
from .device_kind import _NodeKind
from .fs import FsProvider
from .saas.stream import SaasStreamCache

_STALE_ROTATED_SUFFIX = "possibly a stale/rotated reference"
"""Appended (behind an em dash) to a ``NotFoundError``'s message at every
resolution-failure site producing a ``Symptom.DATA_MISSING`` ``Finding``
across ``units.verify_reachable``'s three modules — one shared literal
instead of copies that could drift. Editing the wording here changes all
three at once."""


@dataclasses.dataclass(frozen=True)
class CompositionExtent:
    """One composition record's byte range a ``Version``'s content lives
    in — ``[start, end)`` within ``dedup_file``. A VM/FS/SaaS version has
    exactly one of these; a PC/PS version has one per disk fragment.
    """

    dedup_file: DedupFile
    start: int
    end: int
    unit_label: str
    """For a ``Finding.path`` — e.g. ``"VM disk 'disk1.vmdk'"``,
    ``"PC/PS disk fragment fid=42"``."""


async def _all_children(provider: DeviceProvider, node: Node) -> list[Node]:
    """Page through ``provider.children(node)`` fully. Only called on a
    VM/PC-PS device or disk-listing node — a handful of entries, never
    the FS/SaaS per-file/per-item scale this module deliberately never
    walks.

    Duplicated from ``units/resolve.py``'s own private ``_all_children``,
    kept independent since a handful of entries per call makes page size
    moot here."""
    nodes: list[Node] = []
    offset = 0
    page = 1024
    while True:
        batch = await provider.children(node, offset=offset, limit=page)
        nodes.extend(batch)
        if len(batch) < page:
            return nodes
        offset += page


async def composition_extents_for_version(
    repo: DedupRepo, workload: Workload, version: Version, saas_streams: SaasStreamCache
) -> tuple[list[CompositionExtent], list[Finding]]:
    """Every composition record this version's content lives in — one
    for a VM/FS/SaaS version's single dedup image/stream, one per disk
    fragment for a PC/PS version. Reuses each workload type's own
    resolution (``DeviceProvider``/``FsProvider``/``SaasStream`` via
    ``saas_streams``) rather than re-deriving path/SQL logic.
    ``saas_streams`` is the caller's run-scoped ``SaasStreamCache``, so
    GW/M365 forward-resolution caching is shared across calls.

    Deliberately does not recurse into a device version's guest
    filesystem or a SaaS version's individual items: both are byte-range
    views into the same composition record(s) already returned here.

    Returns ``(extents, findings)``: a per-disk/fragment resolution
    failure is folded into ``findings`` rather than raised, so one bad
    disk doesn't discard every other's already-resolved content. FS/SaaS
    always return an empty ``findings``.

    Raises:
        ApmRepoError: This version's metadata claims it should resolve
            to real content but doesn't resolve at all — the caller
            turns this into a ``Stage.VERSION`` ``Finding``.
    """
    match version.target_type:
        case TargetType.VM:
            return await _vm_extents(repo, version)
        case TargetType.PC | TargetType.PS:
            return await _pcps_extents(repo, version)
        case TargetType.FS:
            return await _fs_extents(repo, version), []
        case _:  # GW, M365
            return await _saas_extents(saas_streams, version), []


def _unresolvable_finding(label: str, exc: ApmRepoError) -> Finding:
    """A ``Stage.VERSION`` ``Finding`` for a catalog/workload/
    version-enumeration failure or a single disk/fragment's own
    resolution failure. ``NotFoundError`` gets the "possibly a
    stale/rotated reference" wording; any other ``ApmRepoError`` is
    ``Symptom.CORRUPTION`` instead."""
    if isinstance(exc, NotFoundError):
        detail = f"{exc} — {_STALE_ROTATED_SUFFIX}"
        return Finding(Stage.VERSION, Symptom.DATA_MISSING, label, detail)
    return Finding(Stage.VERSION, Symptom.CORRUPTION, label, str(exc))


async def _vm_extents(repo: DedupRepo, version: Version) -> tuple[list[CompositionExtent], list[Finding]]:
    extents: list[CompositionExtent] = []
    findings: list[Finding] = []
    async with await DeviceProvider.create(repo, version) as provider:
        for device_node in await _all_children(provider, provider.root()):
            for object_node in await _all_children(provider, device_node):
                attrs = object_node.attrs
                if attrs.get("_kind") != _NodeKind.OBJECT:
                    continue  # a disk-fs "(filesystem)" sibling node, not new dedup content
                if not attrs.get("dedup_object") or attrs.get("unsupported"):
                    continue  # a plain sidecar file, or a data_format this SDK refuses
                    # to read (CBT chains) -- nothing to check either way.
                label = f"VM disk {object_node.name!r}"
                try:
                    unit = await provider.unit(object_node)
                except ApmRepoError as exc:
                    findings.append(_unresolvable_finding(label, exc))
                    continue
                content = unit.content
                if not isinstance(content, DedupFile) or content.size is None:
                    continue
                extents.append(CompositionExtent(content, 0, content.size, label))
    return extents, findings


async def _pcps_extents(repo: DedupRepo, version: Version) -> tuple[list[CompositionExtent], list[Finding]]:
    extents: list[CompositionExtent] = []
    findings: list[Finding] = []
    async with await DeviceProvider.create(repo, version) as provider:
        for disk_node in await _all_children(provider, provider.root()):
            if disk_node.attrs.get("_kind") != _NodeKind.PCPS_DISK:
                continue  # a disk-fs sibling node
            try:
                unit = await provider.unit(disk_node)  # may raise NotFoundError -- every fragment unresolvable
            except ApmRepoError as exc:
                findings.append(_unresolvable_finding(f"PC/PS disk {disk_node.name!r}", exc))
                continue
            content = unit.content
            if not isinstance(content, VirtualDiskContentSource):
                continue
            extents.extend(
                CompositionExtent(frag.dedup_file, frag.start, frag.end, f"PC/PS disk fragment fid={frag.fid}")
                for frag in content.fragments
            )
    return extents, findings


async def _fs_extents(repo: DedupRepo, version: Version) -> list[CompositionExtent]:
    provider = FsProvider(repo, version)
    try:
        # FsProvider's node tree is the guest file tree itself (potentially
        # huge) -- reaches for the shared dedup.img directly instead of
        # walking it.
        dedup_img = await provider._dedup_img()  # noqa: SLF001 - no public accessor; reasoning above
    finally:
        await provider.close()
    if dedup_img.size is None:
        return []
    return [CompositionExtent(dedup_img, 0, dedup_img.size, "FS dedup.img")]


def _annotate_with_saas_resolution(label: str, saas_streams: SaasStreamCache, version: Version) -> str:
    """Appends "(stream_version R, requested Q)" to ``label`` when
    ``version`` is GW/M365 and its ``saas_obj`` was substituted during
    forward resolution (routine server-side generation rotation/GC) —
    unchanged for a non-substituted resolution or a non-SaaS version.
    Meaningful only right after this same ``saas_streams``'s own
    ``open_saas_obj(version)`` already succeeded for this exact
    ``version``."""
    if version.target_type not in (TargetType.GW, TargetType.M365):
        return label
    resolution = saas_streams.last_open_resolution(version)
    if resolution is None:
        return label
    requested, resolved = resolution
    if resolved == requested:
        return label
    return f"{label} (stream_version {resolved}, requested {requested})"


async def _saas_extents(saas_streams: SaasStreamCache, version: Version) -> list[CompositionExtent]:
    """``saas_streams`` owns the underlying ``SaasStream`` and its
    lifetime (closed with the cache, not here) — must go through the
    caller's shared cache rather than construct its own."""
    dedup_file = await saas_streams.open_saas_obj(version)
    if dedup_file.size is None:
        return []
    label = _annotate_with_saas_resolution("SaaS saas_obj", saas_streams, version)
    return [CompositionExtent(dedup_file, 0, dedup_file.size, label)]
