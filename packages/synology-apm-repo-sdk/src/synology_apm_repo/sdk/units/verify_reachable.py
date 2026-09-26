"""``verify_reachable()``: the top-down, reachability-scoped integrity
check — what ``Repository.verify()``/``Catalog.verify()`` call. See
``ARCHITECTURE.md``'s Dedup Layer section for the FULL/QUICK table this
implements.

Walks Catalog → Workload → Version → each version's own composition
record(s) (``units.verify_extents.composition_extents_for_version``) and
only checks data reachable that way. Per-workload-type extent resolution
lives in ``units.verify_extents``; the shared bucket-check core and
multiprocess worker glue live in ``units.verify_bucket_check``.

Every touched ``(stream_id, bucket_id)`` is checked once, whole — every
live (non-``COMPACTED``) chunk in it, not just the chunk-map-referenced
indices that touched it.

- **FULL**: every live chunk's ciphertext CRC32 and — once a vault key is
  available — decrypt+decompress+SHA-256 fingerprint, via one merged
  ``chunk_walk.exec_chunks`` pass per bucket.
- **QUICK**: no chunk content read; only ``check_bucket_structure()``
  (file size vs. ``expected_bucket_size()``, ChunkCrcStore trailer
  self-consistency, SizeStore-repair reporting) plus the header-derived
  ``Symptom.KEY_MISSING`` check.

Callers must gate on ``_require_key_verified()`` first (as
``workloads()``/``versions()`` do) — without it, an encrypted repository
run without a key reports a misleadingly clean result.

GW/M365's own resolution (``units.saas.stream.SaasStream.open_saas_obj``)
already forward-resolves past routine backend-side generation rotation
before this walk sees a failure, so a GW/M365 ``Finding`` here means a
genuine gap.

Every ``Finding`` is tagged (``Finding.ref``) with whichever version's
check triggered it first, when a bucket/chunk is shared by more than one
version through dedup.

Not checked: ``link.key`` ↔ ``repo_state.link_key`` (APV) consistency;
any DB beyond the ones this walk actually queries; dangling buckets
(present in ``Pool/`` but unreferenced by any live version) — the last
needs a full recursive ``Pool/`` listing, a different operation from this
catalog-driven walk.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor

from ..asynccache import AsyncKeyedCache
from ..catalog.connection import connections
from ..catalog.version import Version, versions
from ..catalog.workload import TargetType, Workload, workloads
from ..concurrency import bounded_gather, default_worker_count, dispatch_to_pool, new_process_pool
from ..dedup.chunk_walk import plan_chunks_windowed
from ..dedup.composition_reader import CompositionReader, CompositionRecord
from ..dedup.pool import BucketReaderCache, Pool
from ..dedup.pool_descriptor import PoolDescriptor
from ..dedup.repository import DedupRepo
from ..dedup.verify_checks import (
    Finding,
    Stage,
    Symptom,
    VerifyLevel,
    check_composition_header,
    check_map_and_attr_crc,
    check_record_head,
    check_repo_info,
)
from ..errors import ApmRepoError, DataCorruptError, FormatError, NotFoundError, PermissionDeniedError
from ..format.addressing import group_start_bucket_id
from ..identifiers import BucketId, SessionId, StreamId
from ..presentation.progress import Progress
from .node_ref import canonical_ref_for
from .saas.stream import SaasStreamCache
from .verify_bucket_check import (
    _BUCKET_CACHE_MAXSIZE,
    _MAX_CONCURRENT_BUCKET_CHECKS,
    _check_one_bucket_core,
    _verify_bucket_worker,
    _verify_worker_init,
)
from .verify_extents import (
    _STALE_ROTATED_SUFFIX,
    CompositionExtent,
    _unresolvable_finding,
    composition_extents_for_version,
)

_ProgressCallback = Callable[[Progress], Awaitable[None]]

_BUCKET_BATCH_SIZE = 256
"""Buckets claimed before ``finalize_pending_buckets`` moves them into
the check phase — draining in batches bounds peak memory during
discovery and lets FULL level's per-bucket size lookups start before
discovery finishes."""

_SAAS_GENUINE_GAP_SUFFIX = "a genuine SaaS resolution gap, not routine backend-side generation rotation"
"""``discover_version``'s GW/M365 counterpart to
``units.verify_extents._STALE_ROTATED_SUFFIX`` — used once
``SaasStream.open_saas_obj``'s own forward-resolution has already ruled
out every routine rotation case, so the gap reported is presumed
genuine rather than possibly-stale."""

_COMPOSITION_RECORD_CACHE_MAXSIZE = 32
"""``_ReachabilityWalker._composition_records``'s bound, in whole
records kept warm at once. Each record's own chunk-map page cache is
separately bounded (128 pages, ``composition_reader.
_DEFAULT_PAGE_CACHE_MAXSIZE``)."""


@dataclasses.dataclass(frozen=True)
class _RecordCheckState:
    """Once-per-run outcome of checking one composition record's
    ``RecordHead``/``map_crc`` — memoized in ``_record_checks`` so a
    second version/extent sharing the same ``(stream_id, session_id,
    comp_offset)`` never re-runs either check."""

    broken: bool = False
    """``RecordHead`` itself failed to parse — skipped on every later
    visit to this record."""
    repaired_map_array: bytes | None = None
    """Non-``None`` when this record's ``map_crc`` mismatch was repaired
    via parity — reseeded into any later visiting extent's shared
    ``CompositionRecord`` so it never re-derives entries from the
    still-corrupted on-disk bytes."""


class _ReachabilityWalker:
    """Per-run state for the three-phase walk: a private ``Pool``
    (fingerprint/ciphertext-CRC verification forced on) and
    ``BucketReaderCache``, kept separate from the repository's own shared
    instances so this run-scoped sweep doesn't thrash them for other
    concurrent callers.

    Discovery (``discover_version``/``_discover_extent``) and checking
    (``_check_one_bucket``, via ``check_all_buckets``) are two separate
    passes, never interleaved.
    """

    def __init__(
        self,
        repo: DedupRepo,
        level: VerifyLevel,
        *,
        progress: _ProgressCallback | None = None,
        executor: ProcessPoolExecutor | None = None,
    ) -> None:
        self._repo = repo
        self._level = level
        self._progress = progress
        self._pool = Pool(
            repo.store,
            repo.pool_root,
            repo.dir_cache,
            vault_key=repo.vault_key,
            verify_fingerprint=True,
            verify_ciphertext_crc=True,
        )
        self._bucket_cache = BucketReaderCache(maxsize=_BUCKET_CACHE_MAXSIZE)
        # Owned for this run's whole lifetime, not per-version, so GW/M365
        # forward-resolution caching is shared across versions. Closed by
        # close().
        self._saas_streams = SaasStreamCache(repo)
        # A given `executor` is caller-owned and never closed here; `None`
        # means this walker builds and owns one lazily, at FULL level's
        # first actual need.
        self._executor = executor
        self._owns_executor = executor is None
        self._pool_descriptor = PoolDescriptor.from_repo(repo, verify_fingerprint=True, verify_ciphertext_crc=True)
        self._checked_sessions: set[tuple[StreamId, SessionId]] = set()
        self._record_checks: dict[tuple[StreamId, SessionId, int], _RecordCheckState] = {}
        """Every composition record's settled outcome this run, read on
        every visit — unbounded, since evicting an entry could let a
        revisit re-emit a duplicate ``Finding`` or lose a still-needed
        ``repaired_map_array``. Cheap to leave unbounded regardless
        (values are a ``bool`` plus a usually-``None`` ``bytes | None``)."""
        self._composition_records: AsyncKeyedCache[tuple[StreamId, SessionId, int], CompositionRecord] = (
            AsyncKeyedCache(maxsize=_COMPOSITION_RECORD_CACHE_MAXSIZE)
        )
        """Every resolved ``CompositionRecord``, keyed like
        ``_record_checks`` — lets a later version/extent reuse an
        already-fetched, already-parsed chunk-map instead of refetching
        it. Bounded (unlike ``_record_checks``): a miss just
        cold-refetches."""
        self._walked_extent_windows: set[tuple[tuple[StreamId, SessionId, int], int, int]] = set()
        """Every ``(record_key, start, end)`` window already walked via
        ``plan_chunks_windowed`` this run — a later version/extent
        sharing the same window skips the walk, since its bucket keys are
        a pure function of the record's bytes and window and were already
        claimed into ``_bucket_claim`` by the walk that added this entry.
        Unbounded: entries are cheap (three ints)."""
        self._pending_buckets: list[tuple[StreamId, BucketId]] = []
        """Claimed since the last ``finalize_pending_buckets()`` call."""
        self._bucket_claim: dict[tuple[StreamId, BucketId], tuple[str, str]] = {}
        """Each claimed ``(stream_id, bucket_id)`` → whichever version
        claimed it first, as ``(ref, label)`` — ``ref`` tags that
        bucket's findings (``_tag_with_claim``); ``label`` is its human
        ``"workload/version"`` form, for ``Progress.detail`` during the
        check phase."""
        self._buckets_to_check: list[tuple[StreamId, BucketId]] = []
        """Every claimed bucket, in claim order, once
        ``finalize_pending_buckets()`` has moved it over."""
        self._bucket_sizes: dict[tuple[StreamId, BucketId], int] = {}
        self._total_bytes_to_verify = 0
        self._bytes_verified = 0
        self._key_missing_reported = False

    async def close(self) -> None:
        """Closes this walker's own ``SaasStreamCache`` — every
        ``SaasStream`` it opened across the whole run."""
        await self._saas_streams.close()

    @property
    def pending_bucket_count(self) -> int:
        return len(self._pending_buckets)

    async def discover_version(self, workload: Workload, version: Version) -> list[Finding]:
        """Walk this version's composition extents, checking each record
        immediately but only *claiming* (not checking) the buckets
        touched. Returns only composition-stage findings; a claimed
        bucket's own findings surface later, from ``check_all_buckets``.
        """
        label = f"{workload.display_name}/{version.display_name}"
        # Synchronous, no store access -- safe even if this version's
        # content fails to resolve, so every Finding below can still
        # carry it.
        ref = str(canonical_ref_for(self._repo, version))
        try:
            extents, resolution_findings = await composition_extents_for_version(
                self._repo, workload, version, self._saas_streams
            )
        except ApmRepoError as exc:
            if isinstance(exc, NotFoundError):
                # GW/M365 already forward-resolves past routine generation
                # rotation before raising, so a NotFoundError here is a
                # more confident signal than other workload types get.
                is_saas = version.target_type in (TargetType.GW, TargetType.M365)
                suffix = _SAAS_GENUINE_GAP_SUFFIX if is_saas else _STALE_ROTATED_SUFFIX
                detail = f"{exc} — {suffix}"
                return [Finding(Stage.VERSION, Symptom.DATA_MISSING, label, detail, ref=ref)]
            return [Finding(Stage.VERSION, Symptom.CORRUPTION, label, str(exc), ref=ref)]
        findings: list[Finding] = list(resolution_findings)
        for extent in extents:
            findings.extend(await self._discover_extent(extent, ref, label))
        return [dataclasses.replace(f, ref=ref) for f in findings]

    async def _ensure_record_checked(
        self, reader: CompositionReader, record_key: tuple[StreamId, SessionId, int], comp_offset: int, path: str
    ) -> list[Finding]:
        """Check this composition record's ``RecordHead``/``map_crc``
        exactly once per run, caching the outcome in ``_record_checks`` —
        a repeat call for the same key returns ``[]`` immediately."""
        if record_key in self._record_checks:
            return []
        findings: list[Finding] = []
        head_finding, record_head = await check_record_head(reader, comp_offset, path=path)
        if head_finding is not None:
            findings.append(head_finding)
            self._record_checks[record_key] = _RecordCheckState(broken=True)
            return findings
        repaired_array: bytes | None = None
        if record_head is not None and record_head.map_num > 0:
            crc_findings, repaired_array = await check_map_and_attr_crc(reader, comp_offset, record_head, path=path)
            findings.extend(crc_findings)
        self._record_checks[record_key] = _RecordCheckState(repaired_map_array=repaired_array)
        return findings

    async def _discover_extent(self, extent: CompositionExtent, ref: str, label: str) -> list[Finding]:
        findings: list[Finding] = []
        stream_id = StreamId(extent.dedup_file.stream_id)
        session_id = SessionId(extent.dedup_file.session_id)
        comp_offset = extent.dedup_file.comp_offset
        # Cheap, stateless, no I/O -- built fresh rather than reused from
        # DedupFile's own reader.
        reader = CompositionReader(self._repo.store, self._repo.dir_cache, self._repo.comp_root, stream_id, session_id)

        session_key = (stream_id, session_id)
        if session_key not in self._checked_sessions:
            self._checked_sessions.add(session_key)
            header_finding = await check_composition_header(reader, path=extent.unit_label)
            if header_finding is not None:
                findings.append(header_finding)

        record_key = (stream_id, session_id, comp_offset)
        findings.extend(await self._ensure_record_checked(reader, record_key, comp_offset, extent.unit_label))
        check_state = self._record_checks[record_key]
        if check_state.broken:
            # Already reported via _ensure_record_checked; plan_chunks_windowed
            # would only re-raise the same corruption.
            return findings

        # Catch-and-continue: one bucket's failure must not abort the rest
        # of this version's extents.
        try:
            # A cache miss cold-fetches via cached_record(), which already
            # sets extent.dedup_file's cache; seed_record() below is then a
            # no-op there, and the needed override on a hit.
            cached_record = await self._composition_records.resolve(
                record_key,
                lambda _key: extent.dedup_file.cached_record(),
            )
            extent.dedup_file.seed_record(cached_record)

            if check_state.repaired_map_array is not None:
                # Reseed with the parity-repaired bytes so the shared
                # CompositionRecord doesn't carry entries derived from the
                # still-corrupted original. Idempotent and I/O-free, so
                # safe to redo on every visit.
                record = cached_record
                try:
                    await record.seed_pages_from_array(check_state.repaired_map_array)
                except ValueError as exc:
                    # Defensive length-mismatch from a concurrent writer
                    # changing the record mid-read. Caught here, not
                    # folded into the except below, which must not also
                    # absorb an unrelated ValueError from deeper in the
                    # call chain (a caller-invariant guard that should
                    # crash loudly if ever violated).
                    findings.append(
                        Finding(
                            Stage.COMPOSITION,
                            Symptom.CORRUPTION,
                            extent.unit_label,
                            f"parity-repair reseed failed: {exc}",
                        )
                    )
                    return findings
            plan_key = (record_key, extent.start, extent.end)
            if plan_key not in self._walked_extent_windows:
                # Skip if already walked this run (deterministic key set
                # for a given record+window). Claimed incrementally per
                # yielded window, so a later window's failure still leaves
                # earlier ones' buckets claimed; only marked walked once
                # the whole loop completes without error.
                async for plan in plan_chunks_windowed(
                    extent.dedup_file, extent.start, extent.end, extent.start, write_zero_fill=None
                ):
                    for key in plan.groups:
                        if key not in self._bucket_claim:
                            self._bucket_claim[key] = (ref, label)
                            self._pending_buckets.append(key)
                self._walked_extent_windows.add(plan_key)
        except (NotFoundError, DataCorruptError, FormatError) as exc:
            # Reported here rather than aborting the rest of this
            # version's extents or the whole run.
            findings.append(
                Finding(Stage.COMPOSITION, Symptom.CORRUPTION, extent.unit_label, f"chunk-map walk failed: {exc}")
            )
        return findings

    async def finalize_pending_buckets(self) -> None:
        """Move every pending claimed bucket into ``_buckets_to_check``,
        never interleaved with checking — keeps the claim-before-check
        invariant race-free (no ``await`` between checking and claiming)
        and lets ``check_all_buckets()`` report progress against a stable
        total. Called once ``_BUCKET_BATCH_SIZE`` buckets have
        accumulated, or discovery finishes.

        At FULL, also resolves each bucket's on-disk size (a stat only,
        bounded by ``_MAX_CONCURRENT_BUCKET_CHECKS``) for
        ``_total_bytes_to_verify``; QUICK skips this — its own per-bucket
        cost doesn't scale with size, so bucket count is the honest
        progress metric there instead.
        """
        if not self._pending_buckets:
            return
        batch, self._pending_buckets = self._pending_buckets, []
        if self._level is not VerifyLevel.FULL:
            self._buckets_to_check.extend(batch)
            return

        async def _size_and_record(key: tuple[StreamId, BucketId]) -> None:
            size = await self._size_one_bucket(key)
            self._bucket_sizes[key] = size
            self._total_bytes_to_verify += size
            self._buckets_to_check.append(key)

        await bounded_gather(batch, _size_and_record, max_concurrent=_MAX_CONCURRENT_BUCKET_CHECKS)

    async def _size_one_bucket(self, key: tuple[StreamId, BucketId]) -> int:
        """Must never let an exception escape — a concurrent sibling's
        cancellation would abort the whole run over what should cost this
        bucket 0 bytes."""
        stream_id, bucket_id = key
        try:
            path = await self._pool.bucket_path(stream_id, bucket_id)
            return await self._repo.store.size(path)
        except (NotFoundError, PermissionDeniedError):
            # check_all_buckets() surfaces this properly when it opens the
            # key for real; sizing just contributes nothing to the total.
            return 0
        except Exception:
            return 0

    async def check_all_buckets(self) -> list[Finding]:
        """Check every bucket ``finalize_pending_buckets()`` has queued,
        once discovery (and FULL-level sizing) is complete — so progress
        here reports against a stable, already-fixed total.

        At FULL, when this repository's store can be reconstructed in a
        fresh process, dispatches through a real ``ProcessPoolExecutor``
        for genuine multi-core parallelism (in-process ``asyncio`` never
        gets past CPython's GIL for this CPU-bound work — see
        ``ARCHITECTURE.md``'s "Async-native, by design" section). QUICK,
        and any non-describable store, use the in-process
        ``asyncio.TaskGroup`` path instead, bounded by
        ``_MAX_CONCURRENT_BUCKET_CHECKS``.

        Reports progress once per completed bucket: bytes
        (``_total_bytes_to_verify``) at FULL, a plain bucket count at
        QUICK — see ``finalize_pending_buckets`` for why bytes would
        misrepresent QUICK's progress.
        """
        findings: list[Finding] = []
        full = self._level is VerifyLevel.FULL
        total_buckets = len(self._buckets_to_check)
        buckets_checked = 0

        async def _tick(key: tuple[StreamId, BucketId]) -> None:
            nonlocal buckets_checked
            # No await between increment and read -- safe under
            # cooperative concurrency, so concurrent siblings each report
            # a distinct running total.
            buckets_checked += 1
            if self._progress is not None:
                if full:
                    self._bytes_verified += self._bucket_sizes.get(key, 0)
                    done, total, unit = self._bytes_verified, self._total_bytes_to_verify, "bytes"
                else:
                    done, total, unit = buckets_checked, total_buckets, "buckets"
                await self._progress(
                    Progress(
                        phase="verifying",
                        determinate=True,
                        done=done,
                        total=total,
                        unit=unit,
                        detail=self._bucket_claim.get(key, ("", ""))[1],
                    )
                )

        if full and self._pool_descriptor is not None and self._buckets_to_check:
            return await self._check_all_buckets_multiprocess(_tick)

        async def _check(key: tuple[StreamId, BucketId]) -> None:
            findings.extend(await self._check_one_bucket(key))

        # on_done=_tick runs after the semaphore's released, so a slow
        # progress callback can't narrow effective concurrency below
        # _MAX_CONCURRENT_BUCKET_CHECKS.
        await bounded_gather(
            self._buckets_to_check, _check, max_concurrent=_MAX_CONCURRENT_BUCKET_CHECKS, on_done=_tick
        )
        return findings

    async def _check_all_buckets_multiprocess(
        self, tick: Callable[[tuple[StreamId, BucketId]], Awaitable[None]]
    ) -> list[Finding]:
        """``check_all_buckets``'s FULL-level, store-describable path —
        split out for its own executor lifetime management (dispatch
        itself is ``concurrency.dispatch_to_pool``'s). Buckets submit
        sorted by ``group_start_bucket_id`` so a worker's
        ``AllocationTableCache`` is more often already warm for the next
        one."""
        assert self._pool_descriptor is not None
        findings: list[Finding] = []
        ordered = sorted(self._buckets_to_check, key=lambda key: group_start_bucket_id(key[1]))

        executor = self._executor
        if executor is None:
            executor = new_process_pool(initializer=_verify_worker_init, initargs=(self._pool_descriptor,))
            self._executor = executor

        async def _on_result(key: tuple[StreamId, BucketId], result: tuple[list[Finding], bool]) -> None:
            worker_findings, key_missing = result
            findings.extend(self._tag_with_claim(key, self._dedup_key_missing(worker_findings, key_missing)))
            await tick(key)

        try:
            await dispatch_to_pool(
                executor,
                _verify_bucket_worker,
                ordered,
                max_concurrent=default_worker_count(),
                on_result=_on_result,
            )
        finally:
            if self._owns_executor:
                # to_thread() so a blocking shutdown(wait=True) doesn't
                # freeze this process's event loop until the slowest
                # worker finishes.
                await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)
        return findings

    def _tag_with_claim(self, key: tuple[StreamId, BucketId], findings: list[Finding]) -> list[Finding]:
        claim = self._bucket_claim.get(key)
        ref = claim[0] if claim is not None else None
        return [dataclasses.replace(f, ref=ref) for f in findings]

    async def _check_one_bucket(self, key: tuple[StreamId, BucketId]) -> list[Finding]:
        """In-process wrapper around the shared ``_check_one_bucket_core``
        — also used by ``_verify_bucket_worker`` (the multiprocess path),
        so both share one implementation."""
        findings, key_missing = await _check_one_bucket_core(
            self._pool, self._bucket_cache, self._repo.store, self._repo.vault_key, key, self._level
        )
        return self._tag_with_claim(key, self._dedup_key_missing(findings, key_missing))

    def _dedup_key_missing(self, findings: list[Finding], key_missing: bool) -> list[Finding]:
        """Reports ``Symptom.KEY_MISSING`` only once per run — shared here
        so both the in-process and multiprocess check paths use the same
        dedup logic."""
        if not key_missing:
            return findings
        if self._key_missing_reported:
            return [f for f in findings if f.symptom is not Symptom.KEY_MISSING]
        self._key_missing_reported = True
        return findings


async def verify_reachable(
    repo: DedupRepo,
    level: VerifyLevel = VerifyLevel.QUICK,
    *,
    progress: _ProgressCallback | None = None,
    executor: ProcessPoolExecutor | None = None,
) -> list[Finding]:
    """Run the top-down, reachability-scoped check and return every
    ``Finding`` — see ``VerifyLevel`` for what ``level`` controls.

    ``progress`` reports two phases: ``phase="discovering"`` (one tick per
    ``(workload, version)`` pair) then ``phase="verifying"`` (one tick
    per bucket checked, unit ``"bytes"`` at FULL or ``"buckets"`` at
    QUICK — see ``finalize_pending_buckets`` for why). ``Progress.detail``
    names the ``(workload, version)`` pair the bucket was claimed under.

    A resolution failure at any level (``connections``/``workloads``/
    ``versions``) is folded into a ``Finding`` via
    ``_unresolvable_finding`` rather than propagating; the failing
    connection/workload is skipped, its siblings still checked.

    ``executor`` (default ``None``: built here iff FULL actually needs
    one, and closed before returning) is FULL's multi-core pool for its
    per-bucket decode sweep. Pass a shared one across multiple calls for
    one logical operation instead of letting each spin up its own. QUICK
    never uses it.
    """
    findings: list[Finding] = []
    findings += await check_repo_info(repo)

    pairs: list[tuple[Workload, Version]] = []
    try:
        conns = await connections(repo)
    except ApmRepoError as exc:
        findings.append(_unresolvable_finding(repo.layout.repo_root, exc))
        conns = []
    for connection in conns:
        try:
            wls = await workloads(repo, connection)
        except ApmRepoError as exc:
            findings.append(_unresolvable_finding(connection.display_name, exc))
            continue
        for workload in wls:
            try:
                vers = await versions(repo, workload, include_deleted=False)
            except ApmRepoError as exc:
                findings.append(_unresolvable_finding(workload.display_name, exc))
                continue
            pairs.extend([(workload, version) for version in vers])

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
    except BaseException:
        # The walk's own failure takes priority -- a close() failure must
        # never replace it.
        with contextlib.suppress(BaseException):
            await walker.close()
        raise
    else:
        await walker.close()
    return findings
