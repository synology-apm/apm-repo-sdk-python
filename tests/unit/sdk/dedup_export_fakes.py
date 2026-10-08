"""The standard synthetic ``DedupFile`` (plain or over a ``BlockingStore``),
the ``export_to`` driver and a segment-collecting ``BufferedExportSink``
the dedup export and content-source tests share."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal

from support.format_builders import (
    mapping_record,
    zero_record,
)
from support.repo_builders import (
    write_bucket,
    write_composition_entries,
)
from support.store_fakes import BlockingStore
from synology_apm_repo.sdk.dedup.buffered_export_sink import BufferedExportSink, FlushableSegment
from synology_apm_repo.sdk.dedup.composition_reader import CompositionReader
from synology_apm_repo.sdk.dedup.dedup_file import ByteRangeView, DedupFile
from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning, export_to_writer
from synology_apm_repo.sdk.dedup.export_sink import run_sink_export
from synology_apm_repo.sdk.dedup.extent import ExportResult
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink
from synology_apm_repo.sdk.dedup.pool import Pool
from synology_apm_repo.sdk.identifiers import SessionId, StreamId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore


async def export_to(
    file_like: DedupFile | ByteRangeView,
    dst: Path,
    *,
    sparse: bool = True,
    progress: Callable[[int], Awaitable[None]] | None = None,
    tuning: ExportTuning | None = None,
) -> ExportResult:
    """``export_to_writer`` into an unstaged ``LocalFileSink`` at ``dst``, driven by ``run_sink_export``."""
    _, _, size = file_like.export_window()
    sink = LocalFileSink(dst, staged=False)
    return await run_sink_export(
        sink,
        size,
        sparse=sparse,
        body=lambda: export_to_writer(file_like, sink, sparse=sparse, progress=progress, tuning=tuning),
    )


STREAM_ID = StreamId(7)
SESSION_ID = SessionId(3)
HEAD_OFF = 64
SIZE = 45056

CHUNK_PLAINTEXTS = [bytes([i]) * 4096 for i in range(5)]  # bucket 0, chunks 0..4


def standard_entries() -> bytes:
    """The standard file's chunk map, all DATA in bucket 0:
    ``[0,12288)`` DATA, chunks 0/1/2; ``[12288,20480)`` ZERO;
    ``[20480,24576)`` HOLE (no record); ``[24576,40960)`` DATA, chunks 3/4
    with ``repeat=1`` (3,4,3,4); ``[40960,45056)`` trailing HOLE up to ``SIZE``."""
    return (
        mapping_record(0, 0, 0, map_num=3)
        + zero_record(12288, zero_num=2)
        + mapping_record(24576, 0, 3, map_num=2, repeat=1)
    )


def dedup_file_at(root: Path, *, store: ObjectStore | None = None) -> DedupFile:
    """The standard ``standard_entries()`` file over ``CHUNK_PLAINTEXTS``,
    written under ``root`` and read through ``store`` (default: a
    ``LocalFsStore`` over ``root``)."""
    write_composition_entries(root / "Composition", standard_entries(), session_id=SESSION_ID, stream_id=STREAM_ID)
    write_bucket(root / "Pool" / "0" / "0.buk", CHUNK_PLAINTEXTS)
    store = LocalFsStore(root) if store is None else store
    dir_cache = DirCache(store)
    comp_reader = CompositionReader(store, dir_cache, "Composition", STREAM_ID, SESSION_ID)
    pool = Pool(store, "Pool", dir_cache)
    return DedupFile(comp_reader, pool, HEAD_OFF, size=SIZE)


def build_blocking_dedup_file(root: Path) -> tuple[DedupFile, BlockingStore]:
    """The standard file over a ``BlockingStore``."""
    store = BlockingStore(LocalFsStore(root))
    return dedup_file_at(root, store=store), store


class SegmentCollector(BufferedExportSink):
    """Concatenates every flushed segment."""

    def __init__(
        self, segment_size: int, *, storage: Literal["memory", "spool"] = "memory", spool_dir: Path | None = None
    ) -> None:
        super().__init__(segment_size, storage=storage, spool_dir=spool_dir if storage == "spool" else None)
        self.output = bytearray()

    async def create_destination(self, logical_size: int, *, sparse: bool) -> None: ...

    async def flush_segment(self, segment: FlushableSegment) -> None:
        self.output += await segment.read_all()

    async def finalize_destination(self) -> None: ...

    async def discard_destination(self) -> bool:
        return False
