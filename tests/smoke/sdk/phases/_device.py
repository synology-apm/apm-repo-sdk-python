"""``device`` domain: VM/PC/PS workloads, through one code path (the Unit
Layer hides PC/PS's multi-fragment disks from callers).
"""

from __future__ import annotations

from synology_apm_repo.sdk import (
    ChunkCompactedError,
    DataCorruptError,
    Node,
    NotFoundError,
    RestorableUnit,
    TargetType,
    UnitKind,
    UnitProvider,
    Version,
    Workload,
)
from synology_apm_repo.sdk.identifiers import CatalogId

from ..._shared_refs import close_if_closable, pick_workload_with_retry, prefer_adversarial_name
from .._context import RepoInfo, SmokeContext
from ._shared import bounded_read_and_export

_DEVICE_TYPES = (TargetType.VM, TargetType.PC, TargetType.PS)
_LEAF_KINDS = frozenset({UnitKind.DISK_IMAGE, UnitKind.DISK_FILE, UnitKind.DISK_FILESYSTEM})

#: A device leaf whose chunk bytes the sample doesn't hold (a metadata-only
#: PC/PS fragment, a chunk compacted away) is a gap in that sample's data,
#: not a bug: degraded, not failed.
_DEGRADE_ON = (NotFoundError, DataCorruptError, ChunkCompactedError)


async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None:
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])

    for target_type in _DEVICE_TYPES:
        entries = [(r, c, w, v) for r, c, w, v in workloads if r is ri and w.workload_type == target_type]
        step_prefix = f"device.{ri.sample_name}.{target_type}"
        if not entries:
            ctx.skip("device", f"{step_prefix}.workload_present", f"no {target_type} workload in {ri.sample_name}")
            continue
        if not ri.readable:
            ctx.skip(
                "device",
                f"{step_prefix}.browse",
                f"{ri.sample_name} is encrypted, key_status={ri.key_status.value}",
            )
            continue

        picked, truncated = await pick_workload_with_retry(entries, _LEAF_KINDS, prefer=prefer_adversarial_name)
        if picked is None:
            if truncated:
                ctx.check(
                    "device",
                    f"{step_prefix}.browse",
                    False,
                    note="search bound exhausted on every candidate, no matching leaf found",
                )
            else:
                ctx.skip("device", f"{step_prefix}.browse", "no working version found across any candidate")
            continue
        _ri, _workload, _version, picked_provider, picked_leaf = picked

        # pick_workload_with_retry leaves the winner's provider open for us.
        try:

            async def _get_provider(provider: UnitProvider = picked_provider) -> UnitProvider:
                return provider

            provider = await ctx.call("device", f"{step_prefix}.provider", _get_provider, degrade_on=_DEGRADE_ON)
            if provider is None:
                continue

            async def _get_unit(provider: UnitProvider = provider, leaf: Node = picked_leaf) -> RestorableUnit:
                return await provider.unit(leaf)

            unit = await ctx.call("device", f"{step_prefix}.unit", _get_unit, degrade_on=_DEGRADE_ON)
            if unit is None:
                continue
            content = unit.content
            await bounded_read_and_export(ctx, "device", step_prefix, content, degrade_on=_DEGRADE_ON)
        finally:
            await close_if_closable(picked_provider)
