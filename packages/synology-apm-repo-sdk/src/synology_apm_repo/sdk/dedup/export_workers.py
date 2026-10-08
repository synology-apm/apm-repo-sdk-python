"""The worker-process side of a multiprocess export: ``ExportExecutor`` (one
repository, one destination) and the per-bucket-group task its workers run,
``export_bucket_group_worker``. ``export_scheduler`` decides which windows go
here.

Each worker process builds its ``Pool`` and opens the destination writer once,
in its initializer; the module-level state below is that one process's.
"""

from __future__ import annotations

import atexit
import dataclasses
import pickle

from ..concurrency import run_in_worker_loop
from ..identifiers import BucketId, StreamId
from .chunk_walk import ChunkRun, exec_one_bucket_group
from .export_sink import SinkDescriptor, WorkerTarget, WorkerWriter
from .pool import BucketReaderCache
from .pool_descriptor import BoundProcessPool, PoolDescriptor, WorkerContext


@dataclasses.dataclass(frozen=True, slots=True)
class ExportGroupWorkerArgs:
    """One ``(stream_id, bucket_id)`` group's (picklable) work for
    ``export_bucket_group_worker``."""

    stream_id: StreamId
    bucket_id: BucketId
    runs: list[ChunkRun]
    size: int
    dst_offset: int


_worker = WorkerContext()
_worker_writer: WorkerWriter | None = None


def _export_worker_init(pool_descriptor: PoolDescriptor, sink_descriptor: SinkDescriptor) -> None:
    """``ProcessPoolExecutor`` initializer. The parent has already created
    the destination at full size."""
    global _worker_writer
    _worker.init(pool_descriptor)
    _worker_writer = sink_descriptor.open_writer()
    # Registered last so the hook never sees half-initialized state.
    atexit.register(_export_worker_shutdown)


def _export_worker_shutdown() -> None:
    """At worker exit: releases ``_worker``'s store and event loop, then closes
    the destination writer."""
    _worker.shutdown()
    if _worker_writer is not None:
        _worker_writer.close()


def _require_picklable(label: str, descriptor: object) -> None:
    """Raises ``ValueError`` naming ``descriptor`` when a spawned worker could not receive it."""
    try:
        pickle.dumps(descriptor)
    except Exception as exc:
        raise ValueError(
            f"the {label} {type(descriptor).__qualname__!r} cannot be pickled for the export workers "
            f"(a class defined inside a function or holding a live handle is not picklable): {exc}"
        ) from exc


class ExportExecutor(BoundProcessPool):
    """Worker processes for ``export_to_writer``, bound for their lifetime to
    one repository (``pool_descriptor``) and one destination
    (``sink_descriptor``). Share one across the calls of a single logical
    export to that destination.

    Raises:
        ValueError: A descriptor cannot be pickled for the workers.
    """

    def __init__(self, pool_descriptor: PoolDescriptor, sink_descriptor: SinkDescriptor) -> None:
        _require_picklable("pool descriptor", pool_descriptor)
        _require_picklable("sink descriptor", sink_descriptor)
        self.sink_descriptor = sink_descriptor
        super().__init__(pool_descriptor, initializer=_export_worker_init, initargs=(pool_descriptor, sink_descriptor))

    def accepts(self, pool_descriptor: PoolDescriptor | None, target: WorkerTarget | None) -> bool:
        """Whether an export of ``pool_descriptor``'s repository into
        ``target`` may be dispatched to these workers."""
        return self.accepts_pool(pool_descriptor) and target is not None and target.descriptor == self.sink_descriptor


async def _export_bucket_group_worker_async(args: ExportGroupWorkerArgs) -> int:
    assert _worker.pool is not None
    assert _worker_writer is not None
    writer = _worker_writer
    bytes_written = 0

    async def _on_run(offset: int, payload: bytes | memoryview) -> None:
        nonlocal bytes_written
        writer.write_at(offset + args.dst_offset, payload)
        bytes_written += len(payload)

    # A fresh cache per task: a bucket re-submitted in a later window is decoded again.
    await exec_one_bucket_group(
        args.stream_id,
        args.bucket_id,
        args.runs,
        pool=_worker.pool,
        on_run=_on_run,
        size=args.size,
        export_cache=BucketReaderCache(),
    )
    return bytes_written


def export_bucket_group_worker(args: ExportGroupWorkerArgs) -> int:
    """The multiprocess path's per-bucket-group work item; returns the bytes
    this group wrote."""
    return run_in_worker_loop(_export_bucket_group_worker_async(args))
