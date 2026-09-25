"""``diagnostics`` domain: ``verify()``, a ``NodeRef`` round trip, and the
``file_map_tree()`` fallback -- one repository at a time, independent of which
workload type a repository happens to carry.
"""

from __future__ import annotations

from synology_apm_repo.sdk import (
    ChunkCompactedError,
    DataCorruptError,
    Finding,
    Node,
    NotFoundError,
    UnitProvider,
    UnsupportedDataFormatError,
    Version,
    Workload,
)
from synology_apm_repo.sdk.identifiers import CatalogId

from ..._shared_refs import close_if_closable, resolve_catalog
from .._context import RepoInfo, SmokeContext

#: Same set _device.py/_fs.py/_saas.py already treat as a known,
#: sample-specific data gap rather than a real bug -- version.meta being
#: populated (see _browsable_versions) only means the catalog *claims*
#: this version is browsable, not that its own object-store data still
#: exists: a real S3 sample can have a stale catalog entry whose backing
#: copy_meta_file directory was removed without the catalog being updated
#: to match.
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
        """Tries every version with browsable catalog metadata in
        turn -- the same "a single version's known data gap mustn't
        doom the whole check when a sibling version's real data is
        still there" reasoning _shared_refs.py's own
        pick_workload_with_retry documents, needed here because
        _browsable_versions's own filter can't tell a version with
        real backing data from one whose catalog entry outlived it.

        Each candidate carries the ``CatalogId`` of the specific
        ``Catalog`` its own ``version`` came from -- resolved fresh
        via ``resolve_catalog`` every time:
        re-deriving "some catalog" from ``ri.repo.catalogs()[0]`` is
        wrong the moment a repository holds more than one, and caching the
        ``Catalog`` object itself (as ``ctx.data["workloads"]``
        deliberately doesn't) would go stale the moment this domain's
        own key round trip, above, runs against this same repository."""
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
            # Unlike _shared_refs.py's own pick_workload_with_retry (whose
            # winning candidate is handed back to its caller, still needed),
            # nothing past this point needs `provider` to stay open --
            # closed on every remaining path below, not just the rejected
            # one above.
            if not children:
                await close_if_closable(provider)
                return True, "", ""  # empty tree -- nothing to round-trip, not a failure
            child = children[0]
            resolved: Node = await ri.repo.resolve(child.ref)
            await close_if_closable(provider)
            return resolved.ref == child.ref, str(child.ref), str(resolved.ref)
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
