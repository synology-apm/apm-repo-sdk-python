"""Exporting a ``RestorableUnit``'s content: the sink contract a consumer
implements for its own destination (``ExportSink`` and the writers it
hands out), the built-in ``LocalFileSink``/``BufferedExportSink``,
``run_export``, the folder-export helpers that plan, check and run a
whole subtree into a local directory, and ``preload_resource_tracker``,
the start-up hook a frontend that captures stderr calls before export or
verify starts a worker pool.
"""

from __future__ import annotations

from .api.export import ExportProgressCallback, run_export
from .api.export_paths import safe_export_join, safe_file_name
from .api.export_tree import (
    IncompleteItem,
    SkippedItem,
    SkipReason,
    TreeExport,
    TreeItem,
    TreePlan,
    TreePreflight,
    TreeProblem,
    TreeProblemKind,
    plan_tree_export,
    preflight_tree,
    run_tree_export,
)
from .concurrency import preload_resource_tracker
from .dedup.buffered_export_sink import BufferedExportSink, FlushableSegment
from .dedup.export_sink import (
    AbortOutcome,
    ExportSink,
    ExportWriter,
    RandomAccessExportSink,
    SegmentWriter,
    SinkCaps,
    SinkDescriptor,
    WorkerTarget,
    WorkerWriter,
    WrittenBytesCallback,
)
from .dedup.extent import ExportResult
from .dedup.local_file_sink import LocalFileSink

__all__ = [
    "AbortOutcome",
    "BufferedExportSink",
    "ExportProgressCallback",
    "ExportResult",
    "ExportSink",
    "ExportWriter",
    "FlushableSegment",
    "IncompleteItem",
    "LocalFileSink",
    "RandomAccessExportSink",
    "SegmentWriter",
    "SinkCaps",
    "SinkDescriptor",
    "SkipReason",
    "SkippedItem",
    "TreeExport",
    "TreeItem",
    "TreePlan",
    "TreePreflight",
    "TreeProblem",
    "TreeProblemKind",
    "WorkerTarget",
    "WorkerWriter",
    "WrittenBytesCallback",
    "plan_tree_export",
    "preflight_tree",
    "preload_resource_tracker",
    "run_export",
    "run_tree_export",
    "safe_export_join",
    "safe_file_name",
]
