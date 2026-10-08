"""The per-bucket check ``dedup.verify_walk.ReachabilitySweep`` runs on every
claimed bucket at FULL or QUICK level (see ``VerifyLevel``), plus its
multiprocess worker/executor glue.

``check_one_bucket`` is the one implementation both the in-process path
(``ReachabilitySweep._check_one_bucket``) and the multiprocess path
(``verify_bucket_worker``) call, so it takes every dependency as a
parameter. ``verify_worker_init`` runs once per worker process, so its
``Pool``/``BucketReaderCache`` stay warm across every bucket that worker
checks.
"""

from __future__ import annotations

import atexit
from collections.abc import Sequence
from typing import TYPE_CHECKING

from ..concurrency import common_descriptor, run_in_worker_loop
from ..errors import FormatError, NotFoundError, StorageBackendError
from ..findings import Finding, Stage, Symptom, VerifyLevel
from ..identifiers import BucketId, StreamId
from .chunk_walk import decode_bucket_chunks
from .pool import FULL_VERIFY, BucketReader, BucketReaderCache, Pool
from .pool_descriptor import BoundProcessPool, PoolDescriptor, WorkerContext
from .verify_checks import STALE_ROTATED_SUFFIX, check_bucket_structure, check_chunk_ciphertext_crcs

if TYPE_CHECKING:
    from .repository import DedupRepo

MAX_CONCURRENT_BUCKET_CHECKS = 8
"""Concurrent ``ReachabilitySweep._check_one_bucket`` tasks per in-process
``check_all_buckets`` call (also bounds ``finalize_pending_buckets``'s
concurrent bucket sizing at FULL). Per-chunk decode is CPU-bound, so
more tasks add scheduling overhead, not more decode parallelism."""


async def _listed_size(pool: Pool, key: tuple[StreamId, BucketId]) -> int | None:
    """The bucket's on-disk size from its directory listing (no ``size``
    request), or ``None`` when that is unavailable for any reason but a
    ``StorageBackendError`` — ``check_bucket_structure`` then asks the store
    itself and reports what that finds."""
    try:
        return await pool.bucket_size(*key)
    except StorageBackendError:
        raise
    except Exception:  # noqa: BLE001
        return None


async def _open_bucket_or_finding(
    pool: Pool, bucket_cache: BucketReaderCache, key: tuple[StreamId, BucketId]
) -> tuple[BucketReader, None] | tuple[None, list[Finding]]:
    """``(reader, None)``, or ``(None, findings)`` when the bucket at ``key``
    fails to open; only a ``StorageBackendError`` is raised. Findings are untagged (no
    ``Finding.ref``)."""
    stream_id, bucket_id = key
    try:
        reader = await pool.bucket(*key, cache=bucket_cache)
    except NotFoundError as exc:
        return None, [
            Finding(
                Stage.BUCKET,
                Symptom.DATA_MISSING,
                f"bucket {stream_id}/{bucket_id}",
                f"{exc} — {STALE_ROTATED_SUFFIX}",
            )
        ]
    except FormatError as exc:
        return None, [Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", str(exc))]
    except StorageBackendError:
        raise
    except Exception as exc:  # noqa: BLE001
        # A real ObjectStore can raise past these three (e.g.
        # PermissionDeniedError) -- must still become a Finding, not escape.
        return None, [
            Finding(Stage.BUCKET, Symptom.CORRUPTION, f"bucket {stream_id}/{bucket_id}", f"unexpected error: {exc}")
        ]
    return reader, None


async def _check_chunks_full(
    pool: Pool, key: tuple[StreamId, BucketId], reader: BucketReader, ranges: list[tuple[int, int]]
) -> list[Finding]:
    """Every chunk of ``ranges`` (``(chunk_idx_start, length)``), ciphertext CRC *and*
    decrypt+decompress+fingerprint together, in merged reads; the decoded
    bytes themselves are dropped."""
    assert pool.verify == FULL_VERIFY, "a FULL check needs a pool reading with FULL_VERIFY"
    try:
        await decode_bucket_chunks(reader, *key, ranges, pool=pool)
    except (NotFoundError, FormatError) as exc:
        return [Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk decode/fingerprint check failed: {exc}")]
    return []


async def check_one_bucket(
    pool: Pool,
    bucket_cache: BucketReaderCache,
    key: tuple[StreamId, BucketId],
    level: VerifyLevel,
) -> tuple[list[Finding], bool]:
    """Open, structurally check, and (at FULL) content-check one bucket.

    Every ``Exception`` but ``StorageBackendError`` becomes a ``Finding``,
    so one bucket's failure never cancels its siblings in a concurrent
    dispatch; a transport failure says nothing about the data, so it
    propagates and aborts the verify, as cancellation does.

    Returns ``(findings, key_missing)``: findings are untagged, and, when
    ``key_missing``, include a ``Symptom.KEY_MISSING`` finding the caller
    deduplicates across a run.
    """
    reader, open_findings = await _open_bucket_or_finding(pool, bucket_cache, key)
    if reader is None:
        assert open_findings is not None
        return open_findings, False

    findings: list[Finding] = []
    key_missing = False
    try:
        findings.extend(await check_bucket_structure(pool.store, reader, known_file_size=await _listed_size(pool, key)))

        if not reader.header.is_compressed:
            return findings, False

        key_missing = reader.header.is_vault_encrypted and pool.vault_key is None
        if key_missing:
            findings.append(
                Finding(
                    Stage.ENCRYPT_KEY,
                    Symptom.KEY_MISSING,
                    reader.path,
                    "bucket is vault-encrypted but this repository was opened without a key",
                )
            )

        ranges = reader.non_compacted_chunk_ranges() if level is VerifyLevel.FULL else []
        if ranges:
            if key_missing:
                # Ciphertext CRC needs no key; only decode+fingerprint
                # does, so that's the only half skipped here.
                indices = [idx for start, length in ranges for idx in range(start, start + length)]
                findings.extend(await check_chunk_ciphertext_crcs(reader, indices))
            else:
                findings.extend(await _check_chunks_full(pool, key, reader, ranges))
    except (NotFoundError, FormatError) as exc:
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, str(exc)))
    except StorageBackendError:
        raise
    except Exception as exc:  # noqa: BLE001
        findings.append(Finding(Stage.BUCKET, Symptom.CORRUPTION, reader.path, f"unexpected error: {exc}"))
    return findings, key_missing


_worker = WorkerContext()
_worker_bucket_cache: BucketReaderCache | None = None


def verify_worker_init(descriptor: PoolDescriptor) -> None:
    """``ProcessPoolExecutor`` initializer: builds this worker's ``Pool``
    and ``BucketReaderCache`` once per process."""
    global _worker_bucket_cache
    _worker.init(descriptor)
    _worker_bucket_cache = BucketReaderCache.for_verify()
    # Registered last so the hook never sees half-initialized state.
    atexit.register(_verify_worker_shutdown)


def _verify_worker_shutdown() -> None:
    """At worker exit: releases ``_worker``'s store and event loop."""
    _worker.shutdown()


class VerifyExecutor(BoundProcessPool):
    """Worker processes serving ``verify_bucket_worker`` for the repository
    ``pool_descriptor`` describes (a FULL-verify pool's)."""

    def __init__(self, pool_descriptor: PoolDescriptor) -> None:
        super().__init__(pool_descriptor, initializer=verify_worker_init, initargs=(pool_descriptor,))


def shared_verify_executor(repos: Sequence[DedupRepo]) -> VerifyExecutor | None:
    """One executor several catalogs' FULL verifies can share, when every
    one describes the same pool (the common vault case); ``None`` when
    they differ (object-storage siblings can be separate stores) or one
    can't be rebuilt in a worker, so each verify builds its own."""
    descriptor = common_descriptor([PoolDescriptor.from_pool(repo.new_pool(verify=FULL_VERIFY)) for repo in repos])
    return VerifyExecutor(descriptor) if descriptor is not None else None


def verify_bucket_worker(key: tuple[StreamId, BucketId]) -> tuple[list[Finding], bool]:
    """The multiprocess path's per-bucket work item — a thin wrapper
    around ``check_one_bucket``. Always called at FULL level."""
    assert _worker.pool is not None
    assert _worker_bucket_cache is not None
    return run_in_worker_loop(check_one_bucket(_worker.pool, _worker_bucket_cache, key, VerifyLevel.FULL))
