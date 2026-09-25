"""The shared, ``self``-free bucket-check core for ``verify_reachable``,
plus the multiprocess worker/executor glue built on top of it. Feeds into
``units.verify_reachable``'s top-down walk, which runs this against every
claimed bucket at either FULL or QUICK level (see ``VerifyLevel``).

Everything below is module-level and takes every dependency as an
explicit parameter, on purpose: ``_check_one_bucket_core`` is the one
implementation both ``units.verify_reachable._ReachabilityWalker.
_check_one_bucket`` (in-process) and ``_verify_bucket_worker`` (below,
multiprocess) call — a worker process has no ``self`` (no
``_ReachabilityWalker`` instance at all, just its own freshly-rebuilt
``Pool``/``BucketReaderCache``/``ObjectStore``), so the only way to
guarantee both paths run the identical open→check→branch sequence is to
give that sequence no ``self`` to begin with. The worker functions
further down are module-level (never a method or closure — required for
``spawn`` picklability) and process-global rather than passed as
arguments: each runs inside its own freshly-spawned worker process, one
``_verify_worker_init`` call per worker's whole lifetime, so the ``Pool``
it builds (and that ``Pool``'s own ``AllocationTableCache``, when
fingerprinting) persists across every bucket this worker ever checks —
not rebuilt per task.
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
(also reused to bound ``finalize_pending_buckets``'s own concurrent
``ObjectStore.size()`` stats at FULL level). Per-chunk decode work is
CPU-bound, so more concurrent tasks add scheduling overhead without more
decode parallelism, and the storage backend itself can also start
queuing under too many outstanding requests."""

_BUCKET_CACHE_MAXSIZE = 2 * _MAX_CONCURRENT_BUCKET_CHECKS
"""``BucketReaderCache(maxsize=...)`` for this module's own two
construction sites (in-process, in
``units.verify_reachable._ReachabilityWalker.__init__``, and
multiprocess-worker, in ``_verify_worker_init`` below) — never the
unbounded default. Discovery fully finishes (and ``_bucket_claim`` fully
dedupes every distinct bucket) before checking ever starts, so no bucket
is ever re-visited *across* the check phase; a checked bucket's own
``BucketReader`` is only ever looked up a second time by
``_check_one_bucket_core``'s own FULL-level content-check re-entry into
the *same* bucket moments after its
first open. At most ``_MAX_CONCURRENT_BUCKET_CHECKS`` buckets are ever
mid-flight at once, so that many slots already cover every real reuse;
doubled here as headroom against eviction ordering rather than tuned
against a measured miss rate."""


async def _noop_on_run(offset: int, data: bytes | memoryview) -> None:
    """``exec_chunks`` needs an ``on_run`` sink; this walk only cares
    about the checks ``BucketReader.read_chunks``/``Pool.verify_fingerprints``
    already perform as a side effect of decoding, never the decoded bytes
    themselves."""


async def _open_bucket_or_finding(
    pool: Pool, bucket_cache: BucketReaderCache, key: tuple[StreamId, BucketId]
) -> tuple[BucketReader, None] | tuple[None, list[Finding]]:
    """Opens the bucket at ``key``, converting
    ``NotFoundError``/``DataCorruptError``/``FormatError``/anything else
    into a ``Finding`` instead of raising — an exception escaping this
    call would abort every other in-flight bucket in the same concurrent
    dispatch, not just this one. Scoped here to just the
    open call's own failure modes. Returns ``(reader, None)`` on success or
    ``(None, findings)`` on failure — never both non-``None``. Findings
    returned here are untagged (no ``Finding.ref``) — tagging is a
    parent-only concern (``_ReachabilityWalker._tag_with_claim``)."""
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
        # The same broad safety net applies here too: a real ObjectStore
        # can raise something past NotFoundError/DataCorruptError/FormatError
        # on open (PermissionDeniedError, for one) that must still become a
        # Finding rather than escape into the caller's own concurrent
        # dispatch (an asyncio.TaskGroup, or a worker process boundary).
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
    pass — one read per chunk, not two. Runs against a throwaway ``Pool``
    constructed with both ``verify_fingerprint``/``verify_ciphertext_crc``
    forced on, reusing the same bucket-major batched reader and read
    merging every real export uses."""
    # dest_offset_start is meaningless here (verify never writes
    # anything) -- size=0 makes every _flush_run() call a no-op
    # (write_len <= 0 unconditionally), so on_run is never actually
    # invoked; only the decode-time checks (ciphertext CRC inside
    # read_chunks, fingerprint via Pool.verify_fingerprints) matter.
    plan = ChunkPlan(groups={key: [ChunkRun(idx, 1, 0) for idx in indices]}, holes=0, zeros=0)
    try:
        await exec_chunks(plan, pool=pool, on_run=_noop_on_run, size=0, export_cache=bucket_cache)
    except (NotFoundError, DataCorruptError, FormatError) as exc:
        return [Finding(Stage.BUCKET, Symptom.MISMATCH, reader.path, f"chunk decode/fingerprint check failed: {exc}")]
    return []


async def _check_chunks_ciphertext_only(reader: BucketReader, indices: list[int]) -> list[Finding]:
    """FULL level's own path for a vault-encrypted bucket opened without a
    key: every one of ``indices`` still gets its ciphertext CRC32 checked
    (no decrypt attempt, so no key needed), but decode+fingerprint is
    skipped entirely for all of them rather than falling back to QUICK's
    small sample — the ``KEY_MISSING`` finding already reported once per
    run explains why fingerprinting can't run at all here.
    ``check_chunk_ciphertext_crcs`` batches the underlying reads — one
    merged ``store.read()`` per contiguous run instead of one per chunk —
    real here too, since a bucket opened at FULL without
    a key can still mean *every* live chunk in it gets this check."""
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
    content-check one bucket — shared verbatim by
    ``units.verify_reachable._ReachabilityWalker._check_one_bucket``
    (in-process) and ``_verify_bucket_worker`` (multiprocess, below).

    Called from inside a concurrent dispatch (an ``asyncio.TaskGroup``
    task, or a separate worker process), so this must never let an
    exception escape: past the specific
    ``NotFoundError``/``DataCorruptError``/``FormatError`` branches below,
    a final broad ``except Exception`` converts anything unexpected into a
    ``Finding`` too. Skipping that net would let a bug in one bucket's
    check propagate into the caller's own concurrent dispatch, which
    cancels/aborts every other in-flight sibling — a strictly worse blast
    radius than one bucket's own failure. ``asyncio.CancelledError`` is a
    ``BaseException``, not an ``Exception``, so real task cancellation
    still propagates through this net untouched.

    Returns ``(findings, key_missing)``: findings are **untagged** (no
    ``Finding.ref``) and, when ``key_missing``, *always* include their own
    ``Symptom.KEY_MISSING`` finding — the "report this only once per run"
    bookkeeping, like tagging, is a parent-only concern this core function
    has no state for; its caller (``_ReachabilityWalker._dedup_key_missing``)
    applies both.
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

        # QUICK never reaches this: `indices` stays empty, so no chunk is
        # ever read -- only FULL enumerates the bucket's own live chunks.
        indices = reader.non_compacted_chunk_indices() if level is VerifyLevel.FULL else []
        if indices:
            if key_missing:
                # Ciphertext CRC needs no key and stays exhaustive even
                # here -- only decode+fingerprint is what actually needs one,
                # so that's the only half skipped. Routing this through
                # _check_chunks_full instead would attempt to decrypt with
                # no key and raise KeyRequiredError.
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
    worker process's whole lifetime, not once per task, so the
    ``Pool``/``BucketReaderCache`` it builds here stay warm across every
    bucket this worker later checks instead of being rebuilt per task."""
    global _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key
    _worker_store, _worker_pool = build_worker_pool(descriptor)
    _worker_bucket_cache = BucketReaderCache(maxsize=_BUCKET_CACHE_MAXSIZE)
    _worker_vault_key = descriptor.vault_key
    # Registered last, only once every worker-global above is actually set,
    # so a shutdown hook never runs against half-initialized state.
    atexit.register(_verify_worker_shutdown)


def _verify_worker_shutdown() -> None:
    """Runs once, at this worker process's normal exit (registered by
    ``_verify_worker_init``) — releases ``_worker_store`` (see
    ``aclose_worker_store()``) before closing this
    worker's persistent event loop."""
    run_in_worker_loop(aclose_worker_store(_worker_store))
    close_worker_loop()


def build_verify_executor(descriptor: PoolDescriptor) -> ProcessPoolExecutor:
    """The one public factory for an executor this module's own worker
    functions (above) know how to serve — for a caller (``Repository.verify()``'s
    own multi-catalog fan-out) that wants to share **one** executor across
    several ``verify_reachable()`` calls instead of letting each build (and
    tear down) its own. Kept here, not re-derived by that caller, so
    ``_verify_worker_init`` itself stays module-private: every executor
    this module's workers can serve is built through this one function."""
    return new_process_pool(initializer=_verify_worker_init, initargs=(descriptor,))


def _verify_bucket_worker(key: tuple[StreamId, BucketId]) -> tuple[list[Finding], bool]:
    """The multiprocess path's per-bucket work item — a thin wrapper
    around ``_check_one_bucket_core``. Always called at FULL level:
    ``check_all_buckets`` never dispatches here for QUICK."""
    assert _worker_pool is not None
    assert _worker_bucket_cache is not None
    assert _worker_store is not None
    # asyncio.run() closes its loop when this one call returns, which would
    # break _worker_pool's cached client (bound to whichever loop first ran
    # it) on the very next call in this same worker process -- run against
    # this worker's own persistent loop instead.
    return run_in_worker_loop(
        _check_one_bucket_core(
            _worker_pool, _worker_bucket_cache, _worker_store, _worker_vault_key, key, VerifyLevel.FULL
        )
    )
