"""The shared, ``self``-free bucket-check core for ``verify_reachable``,
plus the multiprocess worker/executor glue built on top of it — run
against every claimed bucket at either FULL or QUICK level (see
``VerifyLevel``).

``_check_one_bucket_core`` is the one implementation both the in-process
path (``units.verify_reachable._ReachabilityWalker._check_one_bucket``)
and the multiprocess path (``_verify_bucket_worker``, below) call — it
takes every dependency as an explicit parameter since a worker process has
no ``self``. The worker functions below are module-level and
process-global (required for ``spawn`` picklability): each runs inside a
freshly-spawned worker process, and ``_verify_worker_init`` runs once per
worker's whole lifetime, so its ``Pool``/``BucketReaderCache`` persist
across every bucket that worker checks.
"""

from __future__ import annotations

import atexit
from concurrent.futures import ProcessPoolExecutor

from ..concurrency import close_worker_loop, new_process_pool, run_in_worker_loop
from ..dedup.chunk_walk import ChunkPlan, ChunkRun, exec_chunks
from ..dedup.pool import BucketReader, BucketReaderCache, Pool
from ..dedup.pool_descriptor import PoolDescriptor, aclose_worker_store, build_worker_pool
from ..dedup.verify_checks import (
    Finding,
    Stage,
    Symptom,
    VerifyLevel,
    check_bucket_structure,
    check_chunk_ciphertext_crcs,
)
from ..errors import DataCorruptError, FormatError, NotFoundError
from ..identifiers import BucketId, StreamId
from ..storage.base import ObjectStore
from .verify_extents import _STALE_ROTATED_SUFFIX

_MAX_CONCURRENT_BUCKET_CHECKS = 8
"""Concurrent ``_check_one_bucket`` tasks per ``check_all_buckets`` call
(also bounds ``finalize_pending_buckets``'s concurrent
``ObjectStore.size()`` stats at FULL). Per-chunk decode is CPU-bound, so
more tasks add scheduling overhead, not more decode parallelism."""

_BUCKET_CACHE_MAXSIZE = 2 * _MAX_CONCURRENT_BUCKET_CHECKS
"""``BucketReaderCache(maxsize=...)`` for this module's two construction
sites (in-process and multiprocess-worker) — never the unbounded default.
At most ``_MAX_CONCURRENT_BUCKET_CHECKS`` buckets are ever mid-flight at
once; doubled here as headroom."""


async def _noop_on_run(offset: int, data: bytes | memoryview) -> None:
    """``exec_chunks`` needs an ``on_run`` sink; this walk only cares
    about the checks performed as a side effect of decoding, never the
    decoded bytes themselves."""


async def _open_bucket_or_finding(
    pool: Pool, bucket_cache: BucketReaderCache, key: tuple[StreamId, BucketId]
) -> tuple[BucketReader, None] | tuple[None, list[Finding]]:
    """Opens the bucket at ``key``, converting any failure into a
    ``Finding`` instead of raising — an escaping exception would abort
    every other in-flight bucket in the same concurrent dispatch. Returns
    ``(reader, None)`` on success or ``(None, findings)`` on failure.
    Findings returned here are untagged (no ``Finding.ref``) — tagging is
    a parent-only concern (``_ReachabilityWalker._tag_with_claim``)."""
    stream_id, bucket_id = key
    try:
        reader = await bucket_cache.buckets.resolve(key, pool.open_bucket_uncached_by_key)
    except NotFoundError as exc:
        return None, [
            Finding(
                Stage.BUCKET,
                Symptom.DATA_MISSING,
                f"bucket {stream_id}/{bucket_id}",
                f"{exc} — {_STALE_ROTATED_SUFFIX}",
            )
        ]
    except (DataCorruptError, FormatError) as exc:
        return None, [Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", str(exc))]
    except Exception as exc:
        # A real ObjectStore can raise past these three (e.g.
        # PermissionDeniedError) -- must still become a Finding, not escape.
        return None, [
            Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", f"unexpected error: {exc}")
        ]
    return reader, None


async def _check_chunks_full(
    pool: Pool,
    bucket_cache: BucketReaderCache,
    key: tuple[StreamId, BucketId],
    reader: BucketReader,
    indices: list[int],
) -> list[Finding]:
    """Every one of ``indices``, ciphertext CRC *and*
    decrypt+decompress+fingerprint together, in one merged ``exec_chunks``
    pass. Runs against a throwaway ``Pool`` with both checks forced on."""
    # size=0 makes _flush_run() a no-op, so on_run is never invoked --
    # only the decode-time checks (ciphertext CRC, fingerprint) matter.
    plan = ChunkPlan(groups={key: [ChunkRun(idx, 1, 0) for idx in indices]}, holes=0, zeros=0)
    try:
        await exec_chunks(plan, pool=pool, on_run=_noop_on_run, size=0, export_cache=bucket_cache)
    except (NotFoundError, DataCorruptError, FormatError) as exc:
        return [Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk decode/fingerprint check failed: {exc}")]
    return []


async def _check_chunks_ciphertext_only(reader: BucketReader, indices: list[int]) -> list[Finding]:
    """FULL level's path for a vault-encrypted bucket opened without a
    key: every one of ``indices`` still gets its ciphertext CRC32 checked
    (no decrypt needed), but decode+fingerprint is skipped for all of
    them."""
    return await check_chunk_ciphertext_crcs(reader, indices)


async def _check_one_bucket_core(
    pool: Pool,
    bucket_cache: BucketReaderCache,
    store: ObjectStore,
    vault_key: bytes | None,
    key: tuple[StreamId, BucketId],
    level: VerifyLevel,
) -> tuple[list[Finding], bool]:
    """Open (via ``_open_bucket_or_finding``), structurally check, and
    content-check one bucket — shared by both the in-process and
    multiprocess paths.

    Must never let an exception escape (a final broad ``except
    Exception`` converts anything unexpected into a ``Finding``) —
    otherwise a bug in one bucket's check would cancel every other
    in-flight sibling in the same concurrent dispatch.
    ``asyncio.CancelledError`` is a ``BaseException``, so real task
    cancellation still propagates.

    Returns ``(findings, key_missing)``: findings are untagged, and, when
    ``key_missing``, always include their own ``Symptom.KEY_MISSING``
    finding — deduplicating that across a run is the caller's job
    (``_ReachabilityWalker._dedup_key_missing``).
    """
    reader, open_findings = await _open_bucket_or_finding(pool, bucket_cache, key)
    if reader is None:
        assert open_findings is not None
        return open_findings, False

    findings: list[Finding] = []
    key_missing = False
    try:
        findings.extend(await check_bucket_structure(store, reader))

        if not reader.header.is_compressed:
            return findings, False

        key_missing = reader.header.is_vault_encrypted and vault_key is None
        if key_missing:
            findings.append(
                Finding(
                    Stage.ENCRYPT_KEY,
                    Symptom.KEY_MISSING,
                    reader.path,
                    "bucket is vault-encrypted but this repository was opened without a key",
                )
            )

        # QUICK never reaches this -- `indices` stays empty.
        indices = reader.non_compacted_chunk_indices() if level is VerifyLevel.FULL else []
        if indices:
            if key_missing:
                # Ciphertext CRC needs no key; only decode+fingerprint
                # does, so that's the only half skipped here.
                findings.extend(await _check_chunks_ciphertext_only(reader, indices))
            else:
                findings.extend(await _check_chunks_full(pool, bucket_cache, key, reader, indices))
    except (NotFoundError, DataCorruptError, FormatError) as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc)))
    except Exception as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, f"unexpected error: {exc}"))
    return findings, key_missing


_worker_pool: Pool | None = None
_worker_bucket_cache: BucketReaderCache | None = None
_worker_store: ObjectStore | None = None
_worker_vault_key: bytes | None = None


def _verify_worker_init(descriptor: PoolDescriptor) -> None:
    """``ProcessPoolExecutor(initializer=...)`` target — runs once per
    worker's whole lifetime, so the ``Pool``/``BucketReaderCache`` built
    here stay warm across every bucket it later checks."""
    global _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key
    _worker_store, _worker_pool = build_worker_pool(descriptor)
    _worker_bucket_cache = BucketReaderCache(maxsize=_BUCKET_CACHE_MAXSIZE)
    _worker_vault_key = descriptor.vault_key
    # Registered last, so a shutdown hook never runs against
    # half-initialized state.
    atexit.register(_verify_worker_shutdown)


def _verify_worker_shutdown() -> None:
    """Runs once, at this worker's normal exit — releases
    ``_worker_store`` before closing this worker's persistent event
    loop."""
    run_in_worker_loop(aclose_worker_store(_worker_store))
    close_worker_loop()


def build_verify_executor(descriptor: PoolDescriptor) -> ProcessPoolExecutor:
    """Factory for an executor this module's worker functions can serve —
    lets a caller (``Repository.verify()``'s multi-catalog fan-out) share
    one executor across several ``verify_reachable()`` calls instead of
    each building its own."""
    return new_process_pool(initializer=_verify_worker_init, initargs=(descriptor,))


def _verify_bucket_worker(key: tuple[StreamId, BucketId]) -> tuple[list[Finding], bool]:
    """The multiprocess path's per-bucket work item — a thin wrapper
    around ``_check_one_bucket_core``. Always called at FULL level."""
    assert _worker_pool is not None
    assert _worker_bucket_cache is not None
    assert _worker_store is not None
    # asyncio.run() would close its loop and break _worker_pool's cached
    # client on the next call -- run against this worker's persistent loop.
    return run_in_worker_loop(
        _check_one_bucket_core(
            _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key, key, VerifyLevel.FULL
        )
    )
