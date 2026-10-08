"""``verify_reachable()``: the top-down, reachability-scoped integrity
check behind ``Repository.verify()``/``Catalog.verify()``. See
``ARCHITECTURE.md``'s Dedup Layer section for what FULL and QUICK check.

Walks Catalog → Workload → Version → each version's composition
record(s) (``units.verify_extents``) and checks only data reachable that
way; ``dedup.verify_walk.ReachabilitySweep`` checks those records and
every ``(stream_id, bucket_id)`` they touch, once, whole.

Callers check the key first (``Repository.verify``/``Catalog.verify``
do): an encrypted repository run without a key reports a misleadingly
clean result.

Not checked: ``link.key`` ↔ ``repo_state.link_key`` (APV) consistency;
any DB beyond the ones this walk queries; dangling buckets (in ``Pool/``
but unreferenced by any live version), which need a full ``Pool/``
listing rather than a catalog-driven walk.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable

from .._util.closing import close_preserving
from ..catalog.connection import connections
from ..catalog.version import Version, versions
from ..catalog.workload import Workload, workloads
from ..dedup.repository import DedupRepo
from ..dedup.verify_bucket_check import VerifyExecutor
from ..dedup.verify_checks import STALE_ROTATED_SUFFIX, check_repo_info
from ..dedup.verify_walk import ReachabilitySweep
from ..errors import (
    ApmRepoError,
    StorageBackendError,
)
from ..findings import Finding, VerifyLevel
from ..presentation.progress import Progress, ProgressCallback
from .node_ref import canonical_ref_for
from .saas.stream import SaasStreamCache
from .verify_extents import composition_extents_for_version, unresolvable_finding

_BUCKET_BATCH_SIZE = 256
"""Buckets claimed before ``finalize_pending_buckets`` moves them into
the check phase — draining in batches bounds peak memory during
discovery and lets FULL level's per-bucket size lookups start before
discovery finishes."""

_SAAS_GENUINE_GAP_SUFFIX = "a genuine SaaS resolution gap, not routine backend-side generation rotation"
"""GWS/M365's counterpart to ``STALE_ROTATED_SUFFIX``: SaaS resolution
already forward-resolves past routine generation rotation, so a gap left
over is presumed genuine."""


class _ReachabilityWalker(ReachabilitySweep):
    """``ReachabilitySweep`` plus the Unit-layer half: turning a version into
    its composition extents (``units.verify_extents``), with one run-scoped
    ``SaasStreamCache`` shared across versions."""

    def __init__(
        self,
        repo: DedupRepo,
        level: VerifyLevel,
        *,
        progress: ProgressCallback | None = None,
        executor: VerifyExecutor | None = None,
    ) -> None:
        super().__init__(repo, level, progress=progress, executor=executor)
        self._saas_streams = SaasStreamCache(repo)

    async def close(self) -> None:
        """Close every ``SaasStream`` this walker opened."""
        await self._saas_streams.close()

    async def discover_version(self, workload: Workload, version: Version) -> list[Finding]:
        """Walk this version's composition extents, checking each record
        immediately but only *claiming* (not checking) the buckets
        touched. Returns only composition-stage findings; a claimed
        bucket's own findings surface later, from ``check_all_buckets``.
        """
        label = f"{workload.display_name}/{version.display_name}"
        # No store access, so every Finding below can carry it.
        ref = str(canonical_ref_for(self._repo, version))
        try:
            extents, resolution_findings = await composition_extents_for_version(
                self._repo, version, self._saas_streams
            )
        except StorageBackendError:
            raise
        except ApmRepoError as exc:
            suffix = _SAAS_GENUINE_GAP_SUFFIX if version.is_saas else STALE_ROTATED_SUFFIX
            return [unresolvable_finding(label, exc, missing_suffix=suffix, ref=ref)]
        findings: list[Finding] = list(resolution_findings)
        for extent in extents:
            findings.extend(await self.claim_extent(extent, ref, label))
        return [dataclasses.replace(f, ref=ref) for f in findings]


async def _listed[T](listing: Awaitable[list[T]], label: str, findings: list[Finding]) -> list[T]:
    """``await listing``; an ``ApmRepoError`` other than a
    ``StorageBackendError`` becomes an ``unresolvable_finding`` against ``label`` in ``findings``, and an empty
    list, so the walk skips that item and goes on with its siblings."""
    try:
        return await listing
    except StorageBackendError:
        raise
    except ApmRepoError as exc:
        findings.append(unresolvable_finding(label, exc))
        return []


async def verify_reachable(
    repo: DedupRepo,
    level: VerifyLevel = VerifyLevel.QUICK,
    *,
    progress: ProgressCallback | None = None,
    executor: VerifyExecutor | None = None,
) -> list[Finding]:
    """Run the top-down, reachability-scoped check and return every
    ``Finding`` — see ``VerifyLevel`` for what ``level`` controls.

    ``progress`` reports two phases: ``phase="discovering"`` (one tick per
    ``(workload, version)`` pair) then ``phase="verifying"`` (one tick
    per bucket checked, unit ``"bytes"`` at FULL or ``"buckets"`` at
    QUICK). ``Progress.detail`` names the ``"workload/version"`` the item
    belongs to.

    A failure listing connections, workloads or versions becomes a
    ``Finding``; the failing item is skipped and its siblings still
    checked.

    ``executor`` is FULL's process pool, caller-owned when given (to
    share one across calls) and built for ``repo``; ``None`` builds one
    only if needed and shuts it down before returning. QUICK never uses it.

    Raises:
        ValueError: ``executor`` was built for another repository.
        StorageBackendError: A store call failed; a transport failure says
            nothing about the data, so it aborts the check.
    """
    findings: list[Finding] = []
    findings += await check_repo_info(repo)

    pairs: list[tuple[Workload, Version]] = []
    for connection in await _listed(connections(repo), repo.layout.repo_root, findings):
        for workload in await _listed(workloads(repo, connection), connection.display_name, findings):
            vers = await _listed(versions(repo, workload, include_deleted=False), workload.display_name, findings)
            pairs.extend((workload, version) for version in vers)

    walker = _ReachabilityWalker(repo, level, progress=progress, executor=executor)
    try:
        for index, (workload, version) in enumerate(pairs):
            if progress is not None:
                await progress(
                    Progress(
                        phase="discovering",
                        determinate=True,
                        done=index,
                        total=len(pairs),
                        unit="items",
                        detail=f"{workload.display_name}/{version.display_name}",
                    )
                )
            findings += await walker.discover_version(workload, version)
            if walker.pending_bucket_count >= _BUCKET_BATCH_SIZE:
                await walker.finalize_pending_buckets()
        await walker.finalize_pending_buckets()
        findings += await walker.check_all_buckets()
    except BaseException as exc:
        await close_preserving(exc, [walker.close])
        raise
    else:
        await walker.close()
    return findings
