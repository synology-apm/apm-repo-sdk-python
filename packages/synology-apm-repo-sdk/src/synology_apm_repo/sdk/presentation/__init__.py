"""Rendering helpers every frontend shares, so the CLI, the Browser and any
other consumer word and format things identically: Rich markup escaping,
byte/duration/timestamp formatting, file-state icons, progress lines,
export destination checks and reports, verify findings grouped for
display, and logging setup.

A public module of the SDK: import these names from here, not from the
submodules.
"""

from __future__ import annotations

from .export_report import (
    NO_SPACE_ERRNOS,
    ExportTracker,
    is_out_of_space,
    leftover_note,
    out_of_space_message,
    size_summary,
)
from .export_target import (
    destination_state,
    folder_destination_problem,
    part_path_for,
    single_destination_message,
    single_destination_problem,
)
from .format import format_bytes, format_duration, format_rate, format_timestamp, pluralize
from .icons import DIAGNOSTIC_ICON, FILE_STATE_ICON, diagnostic_suffix, file_state_suffix
from .logging_setup import LOG_FILE_ENV, configure_logging
from .markup import safe
from .progress import (
    FormattedProgress,
    Progress,
    ProgressCallback,
    ProgressMeter,
    ProgressPhase,
    ProgressUnit,
    reading_progress_callback,
)
from .verify_report import AugmentedFinding, VerifySummary, group_count_label, summarize_findings

__all__ = [
    "DIAGNOSTIC_ICON",
    "FILE_STATE_ICON",
    "LOG_FILE_ENV",
    "NO_SPACE_ERRNOS",
    "AugmentedFinding",
    "ExportTracker",
    "FormattedProgress",
    "Progress",
    "ProgressCallback",
    "ProgressMeter",
    "ProgressPhase",
    "ProgressUnit",
    "VerifySummary",
    "configure_logging",
    "destination_state",
    "diagnostic_suffix",
    "file_state_suffix",
    "folder_destination_problem",
    "format_bytes",
    "format_duration",
    "format_rate",
    "format_timestamp",
    "group_count_label",
    "is_out_of_space",
    "leftover_note",
    "out_of_space_message",
    "part_path_for",
    "pluralize",
    "reading_progress_callback",
    "safe",
    "single_destination_message",
    "single_destination_problem",
    "size_summary",
    "summarize_findings",
]
