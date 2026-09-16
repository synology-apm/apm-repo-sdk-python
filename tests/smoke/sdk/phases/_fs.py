"""``fs`` domain: FS workloads. Same shape as ``device``, but the leaf
``kind`` is ``FILE``, and "meaningful" means preferring a non-zero-size
leaf over an empty one within the capped breadth-first walk (no
content-type sniffing beyond that -- this is a smoke test, not a content
classifier).
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

_LEAF_KINDS = frozenset({UnitKind.FILE})
_DEGRADE_ON = (NotFoundError, DataCorruptError, ChunkCompactedError)


def _prefer_non_empty(node: Node) -> bool:
    return (node.size or 0) > 0


def _prefer_non_empty_and_adversarial(node: Node) -> bool:
    return _prefer_non_empty(node) and prefer_adversarial_name(node)


async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None:
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])
    entries = [(r, c, w, v) for r, c, w, v in workloads if r is ri and w.workload_type == TargetType.FS]
    step_prefix = f"fs.{ri.sample_name}"
    if not entries:
        # Purely per-repo judgment: this sample alone lacking an FS
        # workload is reported here, not deferred to a once-per-run
        # aggregate.
        ctx.skip("fs", f"{step_prefix}.workload_present", f"no FS workload in {ri.sample_name}")
        return
    if not ri.readable:
        ctx.skip("fs", f"{step_prefix}.browse", f"{ri.sample_name} is encrypted, key_status={ri.key_status.value}")
        return

    picked, truncated = await pick_workload_with_retry(entries, _LEAF_KINDS, prefer=_prefer_non_empty_and_adversarial)
    if picked is None:
        if truncated:
            ctx.check(
                "fs",
                f"{step_prefix}.browse",
                False,
                note="search bound exhausted on every candidate, no matching leaf found",
            )
        else:
            ctx.skip("fs", f"{step_prefix}.browse", "no working version found across any candidate")
        return
    _ri, _workload, _version, picked_provider, picked_leaf = picked

    # The winning candidate is this call's own to close once done --
    # pick_workload_with_retry only closes rejected ones (see its own
    # docstring).
    try:

        async def _get_provider(provider: UnitProvider = picked_provider) -> UnitProvider:
            return provider

        provider = await ctx.call("fs", f"{step_prefix}.provider", _get_provider, degrade_on=_DEGRADE_ON)
        if provider is None:
            return

        async def _get_unit(provider: UnitProvider = provider, leaf: Node = picked_leaf) -> RestorableUnit:
            return await provider.unit(leaf)

        unit = await ctx.call("fs", f"{step_prefix}.unit", _get_unit, degrade_on=_DEGRADE_ON)
        if unit is None:
            return
        content = unit.open()
        await bounded_read_and_export(ctx, "fs", step_prefix, content, degrade_on=_DEGRADE_ON)
    finally:
        await close_if_closable(picked_provider)
