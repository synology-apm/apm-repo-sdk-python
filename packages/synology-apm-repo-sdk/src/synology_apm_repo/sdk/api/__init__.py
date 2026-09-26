"""``Session``/``Repository``/``Catalog``: the Repository Layer, the SDK's
outward-facing door for CLI/TUI code — see ``ARCHITECTURE.md``'s
"Repository Layer" section for the split and what each covers. Every
public name from all three is re-exported here, plus the value types
their methods hand back (``Connection``, ``Version``/``Workload``,
``ExportResult``, ``KeyVerification``, ``RepositoryLayout``) and the
``Finding``-rendering helpers (``group_findings``/``sort_key``/
``AugmentedFinding``/``group_count_label``, from ``dedup.verify_report``).
Everything below this layer is an implementation detail this package
hides; CLI/TUI code otherwise imports only ``units.base``,
``units.node_ref``, ``presentation``, ``storage``, ``errors``,
``identifiers``, and ``profiles``.
"""

from __future__ import annotations

from ..catalog.connection import Connection
from ..catalog.version import Version
from ..catalog.workload import Workload
from ..dedup.dedup_file import ExportResult
from ..dedup.keys import KeyVerification
from ..dedup.verify_report import AugmentedFinding, group_count_label, group_findings, sort_key
from ..storage.layout import RepositoryLayout
from .catalog import Catalog, Frame
from .repository import Finding, KeyStatus, Repository, Stage, Symptom, VerifyLevel
from .session import Session, TraceEvent

__all__ = [
    "AugmentedFinding",
    "Catalog",
    "Connection",
    "ExportResult",
    "Finding",
    "Frame",
    "KeyStatus",
    "KeyVerification",
    "Repository",
    "RepositoryLayout",
    "Session",
    "Stage",
    "Symptom",
    "TraceEvent",
    "Version",
    "VerifyLevel",
    "Workload",
    "group_count_label",
    "group_findings",
    "sort_key",
]
