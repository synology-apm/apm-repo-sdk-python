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
"""Appended (behind an em dash) to a ``NotFoundError``'s own message at
every resolution-failure site that produces a ``Symptom.DATA_MISSING``
``Finding`` across ``units.verify_reachable``'s three modules — one shared
literal instead of separately-typed copies that could drift apart. The
sites: ``_unresolvable_finding`` below (VM/PC-PS disk/fragment failures),
``units.verify_reachable``'s own ``_ReachabilityWalker.discover_version``
(FS composition-resolution failures — GW/M365 uses its own
``_SAAS_GENUINE_GAP_SUFFIX`` instead), and
``units.verify_bucket_check._open_bucket_or_finding`` (a Pool-bucket-level
case unrelated to any workload type's own resolution). Editing the wording
here changes all three at once — check each site's own reasoning still
applies before doing so."""


@dataclasses.dataclass(frozen=True)
class CompositionExtent:
    """One composition record's byte range a ``Version``'s own content
    lives in — ``[start, end)`` within ``dedup_file``, which already
    exposes its own ``(stream_id, session_id, comp_offset)`` triple
    directly (``DedupFile.stream_id``/``.session_id``/``.comp_offset``),
    so this doesn't duplicate those fields. A VM/FS/SaaS version has
    exactly one of these; a PC/PS version has one per disk fragment.
    """

    dedup_file: DedupFile
    start: int
    end: int
    unit_label: str
    """For a ``Finding.path`` — e.g. ``"VM disk 'disk1.vmdk'"``,
    ``"PC/PS disk fragment fid=42"``."""


async def _all_children(provider: DeviceProvider, node: Node) -> list[Node]:
    """Page through ``provider.children(node)`` fully. Only ever called on
    a VM/PC-PS device or disk-listing node — a handful of entries per
    version, never the FS/SaaS per-file/per-item scale this module
    deliberately never walks (see ``composition_extents_for_version``
    for why).

    The same "page to exhaustion" shape as ``units/resolve.py``'s own
    private ``_all_children``/``_iter_pages``, duplicated rather than
    imported across that module boundary (a leading-underscore name is
    module-private by convention) — kept independent since a handful of
    real entries per call makes the exact page size moot either way,
    unlike ``resolve.py``'s own tree-walk callers, whose page size trades
    off against a wide, per-node-fetch-bound level."""
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
    """Every composition record this version's own content lives in — one
    for a VM/FS/SaaS version's single dedup image/stream, one per disk
    fragment for a PC/PS version. Reuses each workload type's own,
    already-implemented resolution (``DeviceProvider``/``FsProvider``/
    ``SaasStream`` via ``saas_streams``, the exact machinery real
    browsing uses) rather than re-deriving any path/SQL logic — this
    function is a thin adapter over each, not a second implementation of
    "how does a version's content resolve." ``saas_streams`` is the
    caller's own run-scoped ``SaasStreamCache`` (``_ReachabilityWalker``
    owns one) — GW/M365's own forward-resolution caching (see
    ``units.saas.stream``) only pays off when every version sharing a
    stream resolves through the same ``SaasStream`` instance, not a
    fresh one per call.

    Deliberately does not recurse into a device version's guest
    filesystem (``DiskFsSibling``) or a SaaS version's individual
    Mail/Drive/Calendar items: both are byte-range/offset views into the
    *same* composition record(s) this function already returns, so
    walking them would only re-touch chunks already covered — a device
    provider's own node tree is shallow (root → children already are the
    leaf disks).

    Returns ``(extents, findings)``: a VM/PC-PS version has more than one
    disk/fragment, and one of them failing to resolve must not discard
    every other disk's already-resolved content the same way every other
    stage in this module already treats a partial failure (see
    ``units.verify_reachable._ReachabilityWalker._discover_extent``'s own
    chunk-map-walk ``except`` clause) — so a per-disk resolution failure
    is folded into ``findings`` instead of raised, and every other disk
    keeps its own place in ``extents``. FS/SaaS always return an empty
    ``findings`` (a single dedup image/stream, nothing to be partial
    about).

    Raises:
        ApmRepoError: This version's metadata claims it should resolve
            to real content but doesn't resolve *at all* — the caller
            (``verify_reachable``) turns this into a ``Stage.VERSION``
            ``Finding`` rather than silently skipping the version.
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
    """A ``Stage.VERSION`` ``Finding`` for anything from a catalog/
    workload/version-enumeration failure down to a single disk/fragment's
    own ``provider.unit()`` failure -- ``Stage.VERSION`` covers failure
    at either the version level or the workload/connection-enumeration
    level above it, and a per-disk/fragment resolution failure is folded
    into a ``Finding`` here rather than raised so one bad disk doesn't
    discard every other already-resolved disk's content. ``NotFoundError`` gets the "possibly a
    stale/rotated reference" wording every other resolution-failure site
    in this module already uses; any other ``ApmRepoError`` (a genuinely
    corrupt row/table) is ``Symptom.CORRUPTION`` instead."""
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
                    continue  # a plain sidecar file (no Pool addressing at all), or a
                    # data_format this SDK deliberately refuses to read (CBT chains) --
                    # nothing this walk can check either way.
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
        # FsProvider's own node tree is the guest file tree itself (real
        # per-file entries, potentially huge) -- walking it would only
        # re-touch the one shared dedup.img every file is already a byte
        # range into, so this reaches for the same image directly instead.
        dedup_img = await provider._dedup_img()  # noqa: SLF001 - no public accessor; reasoning above
    finally:
        await provider.close()
    if dedup_img.size is None:
        return []
    return [CompositionExtent(dedup_img, 0, dedup_img.size, "FS dedup.img")]


def _annotate_with_saas_resolution(label: str, saas_streams: SaasStreamCache, version: Version) -> str:
    """Appends "(stream_version R, requested Q)" to ``label`` when
    ``version`` is GW/M365 and its ``saas_obj`` was substituted during
    forward resolution — ``open_saas_obj`` resolving a ``Version`` whose
    recorded ``stream_version`` no longer has a ``file_map`` row (routine
    server-side generation rotation/GC) to the nearest later generation
    that's still live, instead of the literal requested one — unchanged
    for a plain/non-substituted resolution, or a non-SaaS version.
    Synchronous and free
    (``SaasStreamCache.last_open_resolution`` reads only already-cached
    state, no new I/O) — meaningful only right after this same
    ``saas_streams``'s own ``open_saas_obj(version)`` already succeeded
    for this exact ``version``, otherwise degrades to ``label`` unchanged.

    Used only by ``_saas_extents``, which feeds the result into
    ``CompositionExtent.unit_label`` — reaching a real ``Finding.path``
    only for a ``Stage.COMPOSITION``/``Symptom.CORRUPTION`` finding (a
    chunk-map-walk or parity-repair-reseed failure). A ``Stage.BUCKET``/
    ``Symptom.MISMATCH`` finding (the common per-chunk case) is tagged
    from ``_bucket_claim``'s own ``ref`` only (``_tag_with_claim``) — its
    ``label`` half feeds ``Progress.detail`` during the checking phase,
    never a ``Finding`` field, so this function's own result never
    reaches one that way."""
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
    lifetime (closed with the cache, not here) — see
    ``composition_extents_for_version`` for why this must go through the
    caller's shared cache rather than construct its own."""
    dedup_file = await saas_streams.open_saas_obj(version)
    if dedup_file.size is None:
        return []
    label = _annotate_with_saas_resolution("SaaS saas_obj", saas_streams, version)
    return [CompositionExtent(dedup_file, 0, dedup_file.size, label)]
