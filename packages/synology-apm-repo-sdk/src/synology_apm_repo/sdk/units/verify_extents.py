"""Per-workload-type extent resolution for ``units.verify_reachable``:
turns a ``Version`` into the composition extents its content lives in.
"""

from __future__ import annotations

from ..catalog.version import Version
from ..catalog.workload import TargetType
from ..dedup.dedup_file import DedupFile
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import STALE_ROTATED_SUFFIX
from ..dedup.verify_walk import CompositionExtent
from ..errors import ApmRepoError, NotFoundError, StorageBackendError, UnsupportedDataFormatError
from ..findings import Finding, Stage, Symptom
from .base import UnitKind
from .content.pcps_disk import VirtualDiskContentSource
from .device import DeviceProvider
from .fs import FsProvider
from .resolve import all_children
from .saas.stream import ResolvedSaasObj, SaasStreamCache


async def composition_extents_for_version(
    repo: DedupRepo, version: Version, saas_streams: SaasStreamCache
) -> tuple[list[CompositionExtent], list[Finding]]:
    """Every composition record this version's content lives in — one per
    VM disk image (skipping one in a data_format this SDK can't read), one
    per PC/PS disk fragment, one for an FS version's
    ``dedup.img`` or a SaaS version's ``saas_obj``. Resolved through
    each workload type's own provider/stream; guest filesystems and SaaS
    items are not walked, being byte-range views into these same records.
    ``saas_streams`` is the caller's run-scoped cache.

    Returns ``(extents, findings)``: a per-disk/fragment resolution
    failure is folded into ``findings`` rather than raised. FS/SaaS
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
        case _:  # GWS, M365
            return await _saas_extents(saas_streams, version), []


def unresolvable_finding(
    label: str, exc: ApmRepoError, *, missing_suffix: str = STALE_ROTATED_SUFFIX, ref: str | None = None
) -> Finding:
    """A ``Stage.VERSION`` ``Finding`` for a catalog/workload/version that
    couldn't be enumerated or resolved, or a single disk/fragment's own
    resolution failure. ``NotFoundError`` is ``Symptom.DATA_MISSING``,
    explained by ``missing_suffix`` (by default, "possibly a stale/rotated
    reference"); any other ``ApmRepoError`` is ``Symptom.CORRUPTION``."""
    if isinstance(exc, NotFoundError):
        return Finding(Stage.VERSION, Symptom.DATA_MISSING, label, f"{exc} — {missing_suffix}", ref=ref)
    return Finding(Stage.VERSION, Symptom.CORRUPTION, label, str(exc), ref=ref)


async def _vm_extents(repo: DedupRepo, version: Version) -> tuple[list[CompositionExtent], list[Finding]]:
    extents: list[CompositionExtent] = []
    findings: list[Finding] = []
    async with await DeviceProvider.create(repo, version) as provider:
        for device_node in await all_children(provider, provider.root()):
            for object_node in await all_children(provider, device_node):
                if object_node.kind is not UnitKind.DISK_IMAGE:
                    continue  # a plain sidecar file, or a disk-fs "(filesystem)" sibling: no new dedup content
                label = f"VM disk {object_node.name!r}"
                try:
                    unit = await provider.unit(object_node)
                except UnsupportedDataFormatError:
                    continue  # a data_format this SDK can't read yet (CBT chains): nothing to check
                except StorageBackendError:
                    raise
                except ApmRepoError as exc:
                    findings.append(unresolvable_finding(label, exc))
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
        for disk_node in await all_children(provider, provider.root()):
            if disk_node.kind is not UnitKind.DISK_IMAGE:
                continue  # a disk-fs sibling, or the never-resolved-fragments placeholder
            try:
                unit = await provider.unit(disk_node)  # may raise NotFoundError -- every fragment unresolvable
            except StorageBackendError:
                raise
            except ApmRepoError as exc:
                findings.append(unresolvable_finding(f"PC/PS disk {disk_node.name!r}", exc))
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
        # The shared dedup.img directly, not a walk of the guest file tree.
        dedup_img = await provider.dedup_img()
    finally:
        await provider.close()
    if dedup_img.size is None:
        return []
    return [CompositionExtent(dedup_img, 0, dedup_img.size, "FS dedup.img")]


def _annotate_with_saas_resolution(label: str, resolved: ResolvedSaasObj) -> str:
    """Appends "(stream_version R, requested Q)" to ``label`` when the
    ``saas_obj`` read was a later generation than the version recorded
    (routine server-side generation rotation/GC)."""
    if resolved.stream_version == resolved.requested_stream_version:
        return label
    return f"{label} (stream_version {resolved.stream_version}, requested {resolved.requested_stream_version})"


async def _saas_extents(saas_streams: SaasStreamCache, version: Version) -> list[CompositionExtent]:
    """``saas_streams`` owns the ``SaasStream`` this reads; it is closed
    with the cache, not here."""
    resolved = await saas_streams.resolve_saas_obj(version)
    dedup_file = resolved.dedup_file
    if dedup_file.size is None:
        return []
    label = _annotate_with_saas_resolution("SaaS saas_obj", resolved)
    return [CompositionExtent(dedup_file, 0, dedup_file.size, label)]
