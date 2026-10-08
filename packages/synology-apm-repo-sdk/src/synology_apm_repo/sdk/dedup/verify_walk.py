"""``ReachabilitySweep``: the dedup half of ``units.verify_reachable``'s
reachability-scoped integrity check. Given the composition extents a
version's content lives in, it checks each composition header and record
once per run, claims every bucket the extents' chunk maps touch, then
checks each claimed bucket once, whole, through
``dedup.verify_bucket_check`` (in a process pool at FULL when the store
can be reopened in a worker).

A bucket shared by several versions tags its findings (``Finding.ref``)
with the version that claimed it first.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable

from ..concurrency import (
    bounded_gather,
    default_worker_count,
    dispatch_to_pool,
    first_failure,
    raise_worker_failure,
)
from ..errors import FormatError, NotFoundError, StorageBackendError
from ..findings import Finding, Stage, Symptom, VerifyLevel
from ..format.addressing import group_start_bucket_id
from ..identifiers import BucketId, SessionId, StreamId
from ..presentation.progress import Progress, ProgressCallback, ProgressUnit
from .chunk_walk import iter_bucket_keys
from .composition_reader import CompositionReader
from .dedup_file import DedupFile
from .pool import FULL_VERIFY, BucketReaderCache
from .pool_descriptor import PoolDescriptor
from .repository import DedupRepo
from .verify_bucket_check import (
    MAX_CONCURRENT_BUCKET_CHECKS,
    VerifyExecutor,
    check_one_bucket,
    verify_bucket_worker,
)
from .verify_checks import check_composition_header, check_map_and_attr_crc, check_record_head


async def _gather_or_raise_first(
    keys: list[tuple[StreamId, BucketId]],
    worker: Callable[[tuple[StreamId, BucketId]], Awaitable[None]],
    *,
    on_done: Callable[[tuple[StreamId, BucketId]], Awaitable[None]] | None = None,
) -> None:
    """``bounded_gather`` over ``keys`` at ``MAX_CONCURRENT_BUCKET_CHECKS``,
    raising the first failure itself (a ``StorageBackendError``, the only
    one a bucket check lets escape) rather than its exception group."""
    try:
        await bounded_gather(keys, worker, max_concurrent=MAX_CONCURRENT_BUCKET_CHECKS, on_done=on_done)
    except BaseExceptionGroup as group:
        raise first_failure(group, operation="verify") from None


@dataclasses.dataclass(frozen=True, slots=True)
class CompositionExtent:
    """One composition record's byte range a ``Version``'s content lives
    in — ``[start, end)`` within ``dedup_file``. An FS or SaaS version has
    exactly one of these, a VM version one per disk, and a PC/PS version
    one per disk fragment.
    """

    dedup_file: DedupFile
    start: int
    end: int
    unit_label: str
    """For a ``Finding.path`` — e.g. ``"VM disk 'disk1.vmdk'"``,
    ``"PC/PS disk fragment fid=42"``."""


@dataclasses.dataclass(frozen=True, slots=True)
class _RecordCheckState:
    """Once-per-run outcome of checking one composition record's
    ``RecordHead``/``map_crc``, memoized in ``_record_checks``."""

    broken: bool = False
    """``RecordHead`` itself failed to parse — skipped on every later
    visit to this record."""
    repaired_map_array: bytes | None = None
    """Non-``None`` when this record's ``map_crc`` mismatch was repaired
    via parity; reseeded into the shared ``CompositionRecord`` on every
    visit."""


class ReachabilitySweep:
    """Per-run state for one repository's sweep. Owns a private ``Pool``
    (fingerprint and ciphertext-CRC verification forced on) and
    ``BucketReaderCache``, so the sweep doesn't thrash the repository's
    shared ones.

    ``claim_extent`` claims buckets; ``check_all_buckets`` checks them as a
    separate pass once every extent is claimed (``finalize_pending_buckets``
    first).
    """

    def __init__(
        self,
        repo: DedupRepo,
        level: VerifyLevel,
        *,
        progress: ProgressCallback | None = None,
        executor: VerifyExecutor | None = None,
    ) -> None:
        self._repo = repo
        self._level = level
        self._progress = progress
        self._pool = repo.new_pool(verify=FULL_VERIFY)
        self._bucket_cache = BucketReaderCache.for_verify()
        self._pool_descriptor = PoolDescriptor.from_pool(self._pool)
        if executor is not None and not executor.accepts_pool(self._pool_descriptor):
            raise ValueError(
                "executor was not built for this repository: its worker processes are bound to one repository "
                "when they spawn, and are never re-initialized per task"
            )
        # A given executor is caller-owned; None means build and own one
        # lazily, at FULL level's first need.
        self._executor = executor
        self._owns_executor = executor is None
        self._checked_sessions: set[tuple[StreamId, SessionId]] = set()
        self._record_checks: dict[tuple[StreamId, SessionId, int], _RecordCheckState] = {}
        """Every composition record's settled outcome this run. Unbounded:
        evicting one could re-emit a duplicate ``Finding`` or lose a
        still-needed ``repaired_map_array``."""
        self._walked_extent_windows: set[tuple[tuple[StreamId, SessionId, int], int, int]] = set()
        """Every ``(record_key, start, end)`` window already walked this
        run; its buckets are already claimed, so a revisit skips it."""
        self._pending_buckets: list[tuple[StreamId, BucketId]] = []
        """Claimed since the last ``finalize_pending_buckets()`` call."""
        self._bucket_claim: dict[tuple[StreamId, BucketId], tuple[str, str]] = {}
        """Each claimed bucket → the first claiming version's ``(ref,
        label)``: ``ref`` tags the bucket's findings, ``label``
        (``"workload/version"``) is its ``Progress.detail``."""
        self._buckets_to_check: list[tuple[StreamId, BucketId]] = []
        """Every claimed bucket, in claim order, once
        ``finalize_pending_buckets()`` has moved it over."""
        self._bucket_sizes: dict[tuple[StreamId, BucketId], int] = {}
        self._total_bytes_to_verify = 0
        self._bytes_verified = 0
        self._key_missing_reported = False

    @property
    def pending_bucket_count(self) -> int:
        return len(self._pending_buckets)

    async def _ensure_record_checked(
        self, reader: CompositionReader, record_key: tuple[StreamId, SessionId, int], comp_offset: int, path: str
    ) -> list[Finding]:
        """Check this composition record's ``RecordHead``/``map_crc`` once
        per run; a repeat call for the same key returns ``[]``."""
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

    async def claim_extent(self, extent: CompositionExtent, ref: str, label: str) -> list[Finding]:
        """Check ``extent``'s composition header and record once per run,
        then claim (not check) every bucket its chunk map touches for the
        version ``ref``/``label`` names, unless an earlier version already
        did. Returns only composition-stage findings."""
        findings: list[Finding] = []
        stream_id = extent.dedup_file.stream_id
        session_id = extent.dedup_file.session_id
        comp_offset = extent.dedup_file.comp_offset
        # Only read_at() is used below, so no composition cache is needed.
        reader = self._repo.composition_reader(stream_id, session_id)

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
            # Already reported via _ensure_record_checked; iter_bucket_keys
            # would only re-raise the same corruption.
            return findings

        try:
            cached_record = await extent.dedup_file.cached_record()

            if check_state.repaired_map_array is not None:
                # Reseed so the shared CompositionRecord doesn't carry
                # entries from the corrupted original. Idempotent, no I/O.
                record = cached_record
                try:
                    await record.seed_pages_from_array(check_state.repaired_map_array)
                except ValueError as exc:
                    # A length mismatch. Caught only here: a ValueError
                    # deeper in the walk is a broken invariant and must
                    # propagate.
                    findings.append(
                        Finding(
                            Stage.COMPOSITION,
                            Symptom.CORRUPTION,
                            extent.unit_label,
                            f"parity-repair reseed failed: {exc}",
                        )
                    )
                    return findings
            walk_key = (record_key, extent.start, extent.end)
            if walk_key not in self._walked_extent_windows:
                # Claimed key by key, so a later failure keeps earlier
                # claims; marked walked only once complete.
                async for key in iter_bucket_keys(extent.dedup_file, extent.start, extent.end):
                    if key not in self._bucket_claim:
                        self._bucket_claim[key] = (ref, label)
                        self._pending_buckets.append(key)
                self._walked_extent_windows.add(walk_key)
        except (NotFoundError, FormatError) as exc:
            findings.append(
                Finding(Stage.COMPOSITION, Symptom.CORRUPTION, extent.unit_label, f"chunk-map walk failed: {exc}")
            )
        return findings

    async def finalize_pending_buckets(self) -> None:
        """Move every pending claimed bucket into ``_buckets_to_check``.
        Called after every batch of claims (``units.verify_reachable``) and
        once discovery finishes.

        At FULL, also sizes each bucket for ``_total_bytes_to_verify``
        (FULL progress is in bytes). QUICK's per-bucket cost doesn't scale
        with size, so its progress counts buckets and it skips sizing.
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

        await _gather_or_raise_first(batch, _size_and_record)

    async def _size_one_bucket(self, key: tuple[StreamId, BucketId]) -> int:
        """A bucket's on-disk size, ``0`` on any failure but a
        ``StorageBackendError`` (the check phase reports it); raising would
        cancel every concurrent sibling."""
        stream_id, bucket_id = key
        try:
            listed = await self._pool.bucket_size(stream_id, bucket_id)
            if listed is not None:
                return listed
            path = await self._pool.bucket_path(stream_id, bucket_id)
            return await self._repo.store.size(path)
        except StorageBackendError:
            raise
        except Exception:  # noqa: BLE001
            return 0

    async def check_all_buckets(self) -> list[Finding]:
        """Check every bucket ``finalize_pending_buckets()`` has queued,
        once discovery is complete.

        At FULL with a store a worker process can reopen (a
        ``PoolDescriptor``), buckets are checked in a process pool for
        multi-core decoding; otherwise in-process, bounded by
        ``MAX_CONCURRENT_BUCKET_CHECKS``. Progress ticks once per bucket.
        """
        findings: list[Finding] = []
        full = self._level is VerifyLevel.FULL
        total_buckets = len(self._buckets_to_check)
        buckets_checked = 0

        async def _tick(key: tuple[StreamId, BucketId]) -> None:
            nonlocal buckets_checked
            buckets_checked += 1
            if self._progress is not None:
                unit: ProgressUnit
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

        # on_done runs after the semaphore is released, so a slow progress
        # callback doesn't narrow concurrency.
        await _gather_or_raise_first(self._buckets_to_check, _check, on_done=_tick)
        return findings

    async def _check_all_buckets_multiprocess(
        self, tick: Callable[[tuple[StreamId, BucketId]], Awaitable[None]]
    ) -> list[Finding]:
        """``check_all_buckets``'s process-pool path. Buckets submit sorted
        by ``group_start_bucket_id`` so a worker's ``FingerprintIndex``
        is more often already warm."""
        assert self._pool_descriptor is not None
        findings: list[Finding] = []
        ordered = sorted(self._buckets_to_check, key=lambda key: group_start_bucket_id(key[1]))

        executor = self._executor
        if executor is None:
            executor = VerifyExecutor(self._pool_descriptor)
            self._executor = executor

        async def _on_result(key: tuple[StreamId, BucketId], result: tuple[list[Finding], bool]) -> None:
            worker_findings, key_missing = result
            findings.extend(self._tag_with_claim(key, self._dedup_key_missing(worker_findings, key_missing)))
            await tick(key)

        try:
            await dispatch_to_pool(
                executor.process_pool,
                verify_bucket_worker,
                ordered,
                max_concurrent=default_worker_count(),
                on_result=_on_result,
            )
        except BaseExceptionGroup as group:
            raise_worker_failure(group, operation="verify")
        finally:
            if self._owns_executor:
                await executor.close()
        return findings

    def _tag_with_claim(self, key: tuple[StreamId, BucketId], findings: list[Finding]) -> list[Finding]:
        claim = self._bucket_claim.get(key)
        ref = claim[0] if claim is not None else None
        return [dataclasses.replace(f, ref=ref) for f in findings]

    async def _check_one_bucket(self, key: tuple[StreamId, BucketId]) -> list[Finding]:
        """In-process counterpart of ``verify_bucket_worker``; both run
        ``check_one_bucket``."""
        findings, key_missing = await check_one_bucket(self._pool, self._bucket_cache, key, self._level)
        return self._tag_with_claim(key, self._dedup_key_missing(findings, key_missing))

    def _dedup_key_missing(self, findings: list[Finding], key_missing: bool) -> list[Finding]:
        """Drop ``Symptom.KEY_MISSING`` findings after the first bucket
        that reported one this run."""
        if not key_missing:
            return findings
        if self._key_missing_reported:
            return [f for f in findings if f.symptom is not Symptom.KEY_MISSING]
        self._key_missing_reported = True
        return findings
