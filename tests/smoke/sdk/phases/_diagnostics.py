"""``diagnostics`` domain: ``verify()``, a ``NodeRef`` round trip, and the
``file_map_tree()`` fallback -- one repository at a time, independent of which
workload type a repository happens to carry.
"""

from __future__ import annotations

from synology_apm_repo.sdk import (
    ChunkCompactedError,
    DataCorruptError,
    Finding,
    NotFoundError,
    UnitProvider,
    UnsupportedDataFormatError,
    Version,
    Workload,
)
from synology_apm_repo.sdk.identifiers import CatalogId

from ..._shared_refs import close_if_closable, resolve_catalog
from .._context import RepoInfo, SmokeContext

#: Known, sample-specific data gaps rather than real bugs. A populated
#: ``version.meta`` (see ``_browsable_versions``) only means the catalog
#: *claims* the version is browsable; its ``copy_meta_file`` directory can be
#: gone.
_DEGRADE_ON = (NotFoundError, DataCorruptError, ChunkCompactedError, UnsupportedDataFormatError)


def _browsable_versions(
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]], ri: RepoInfo
) -> list[tuple[CatalogId, Version]]:
    return [
        (catalog_id, version)
        for owner, catalog_id, _workload, versions in workloads
        if owner.sample_name == ri.sample_name
        for version in versions
        if version.meta is not None
    ]


async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None:
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])
    step_prefix = f"diagnostics.{ri.sample_name}"

    async def _verify(ri: RepoInfo = ri) -> list[Finding]:
        return await ri.repo.verify()

    await ctx.call("diagnostics", f"{step_prefix}.verify", _verify)

    async def _file_map_tree(ri: RepoInfo = ri) -> list[dict[str, object]]:
        provider = await ri.repo.file_map_tree()
        root = provider.root()
        children = await provider.children(root, limit=5)
        return [{"name": c.name, "is_leaf": c.is_leaf} for c in children]

    await ctx.call("diagnostics", f"{step_prefix}.file_map_tree", _file_map_tree)

    if not ri.readable:
        ctx.skip(
            "diagnostics",
            f"{step_prefix}.node_ref_round_trip",
            f"{ri.sample_name} is encrypted, key_status={ri.key_status.value}",
        )
        return

    versions = _browsable_versions(workloads, ri)
    if not versions:
        ctx.skip("diagnostics", f"{step_prefix}.node_ref_round_trip", "no version with browsable metadata")
        return

    async def _round_trip(
        ri: RepoInfo = ri, versions: list[tuple[CatalogId, Version]] = versions
    ) -> tuple[bool, str, str]:
        """Tries each browsable version in turn until one opens (see
        ``_DEGRADE_ON``)."""
        last_exc: BaseException | None = None
        for catalog_id, version in versions:
            provider: UnitProvider | None = None
            try:
                catalog = await resolve_catalog(ri.repo, catalog_id)
                provider = await catalog.provider(version)
                root = provider.root()
                children = await provider.children(root, limit=1)
            except _DEGRADE_ON as exc:
                await close_if_closable(provider)
                last_exc = exc
                continue
            if not children:
                await close_if_closable(provider)
                return True, "", ""  # empty tree -- nothing to round-trip, not a failure
            child = children[0]
            frame = await ri.repo.resolve(child.ref)
            await close_if_closable(provider)
            assert frame.node is not None  # resolve() always reaches a node
            return frame.node.ref == child.ref, str(child.ref), str(frame.node.ref)
        assert last_exc is not None  # versions is non-empty, so some iteration always sets this
        raise last_exc

    result = await ctx.call("diagnostics", f"{step_prefix}.node_ref_round_trip", _round_trip, degrade_on=_DEGRADE_ON)
    if result is not None:
        matched, original_ref, resolved_ref = result
        ctx.check(
            "diagnostics",
            f"{step_prefix}.node_ref_round_trip.matches",
            matched,
            note=f"{original_ref!r} -> {resolved_ref!r}",
        )
