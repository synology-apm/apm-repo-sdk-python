"""The Repository Layer, the SDK's facade (see ``ARCHITECTURE.md``'s
"Repository Layer"): ``Session``/``Repository``/``Catalog`` and every
public name they need, gathered from below — the value types their methods
return.
The export surface is gathered separately, in ``synology_apm_repo.sdk.export``.

Internal: the top-level ``synology_apm_repo.sdk`` package re-exports all of
it, and consumers import from there.
"""

from __future__ import annotations

from ..catalog.connection import Connection
from ..catalog.version import Version
from ..catalog.workload import Workload
from ..dedup.keys import KeyVerification
from ..findings import Finding, Stage, Symptom, VerifyLevel
from ..storage.layout import RepositoryLayout
from ..units.resolve import find_path_with_children
from .catalog import Catalog, CatalogFrame, Frame, NodeFrame, RawView, RootFrame, VersionLocation, WorkloadFrame
from .repository import KeyStatus, Repository, SetKeyResult
from .session import Session, TraceEvent

__all__ = [
    "Catalog",
    "CatalogFrame",
    "Connection",
    "Finding",
    "Frame",
    "KeyStatus",
    "KeyVerification",
    "NodeFrame",
    "RawView",
    "Repository",
    "RepositoryLayout",
    "RootFrame",
    "Session",
    "SetKeyResult",
    "Stage",
    "Symptom",
    "TraceEvent",
    "VerifyLevel",
    "Version",
    "VersionLocation",
    "Workload",
    "WorkloadFrame",
    "find_path_with_children",
]
