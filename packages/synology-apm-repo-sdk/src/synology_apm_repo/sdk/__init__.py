"""synology-apm-repo-sdk — offline, read-only SDK for the Synology
APV/Object-Storage dedup repository on-disk format.

This package is the SDK's public surface, together with four public
modules: ``synology_apm_repo.sdk.export`` (the export-sink contract,
``run_export`` and folder export), ``synology_apm_repo.sdk.presentation``
(rendering helpers every frontend shares, ``Progress`` and the
verify-report grouping among them), ``synology_apm_repo.sdk.profiles``
(saved connection profiles, their form fields and their errors) and ``synology_apm_repo.sdk.diagnostics`` (format-level
inspection). Every other module is internal.

``Session``/``Repository``/``Catalog`` (the Repository Layer) are the entry
point; see ``ARCHITECTURE.md``'s "Repository Layer — ``api/``, the facade".
The other exports are the values that facade hands back and the helpers to
read them (catalog/workload/version models and their ids, ``Node``/
``RestorableUnit`` and the node helpers, the ``Frame`` union, verify
findings), the error hierarchy, and the ``ObjectStore`` contract (with
``TracingStore``) for a consumer writing its own backend.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _get_version

from .api import (
    Catalog,
    CatalogFrame,
    Connection,
    Finding,
    Frame,
    KeyStatus,
    KeyVerification,
    NodeFrame,
    RawView,
    Repository,
    RepositoryLayout,
    RootFrame,
    Session,
    SetKeyResult,
    Stage,
    Symptom,
    TraceEvent,
    VerifyLevel,
    Version,
    VersionLocation,
    Workload,
    WorkloadFrame,
    find_path_with_children,
)
from .catalog.version import (
    VersionMeta,
)
from .catalog.workload import (
    SaasSubType,
    TargetType,
)
from .errors import (
    ApmRepoError,
    ChunkCompactedError,
    ContentUnavailableError,
    DataCorruptError,
    FormatError,
    KeyMaterialError,
    KeyMismatchError,
    KeyRequiredError,
    NotFoundError,
    NotRestorableError,
    PermissionDeniedError,
    ResourceLimitExceededError,
    StorageBackendError,
    UnsupportedDataFormatError,
    UnsupportedVersionError,
    WorkerProcessError,
)
from .identifiers import (
    CatalogId,
    ConnectionConfigId,
    ConnectionId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
    WorkloadUid,
)
from .storage import (
    Entry,
    LocalFsStore,
    ObjectStore,
    TracingStore,
)
from .units.base import (
    ClosableUnitProvider,
    ContentSource,
    FileState,
    ItemColumns,
    Node,
    NodeRole,
    RestorableUnit,
    UnitKind,
    UnitProvider,
    node_kind_label,
)
from .units.node_ref import (
    NodeRef,
    RefKind,
    disambiguate,
    disambiguate_catalogs,
    disambiguate_versions,
    disambiguate_workloads,
)
from .units.saas.site import (
    SiteListItems,
    read_site_list_items,
)

try:
    __version__ = _get_version("synology-apm-repo-sdk")
except PackageNotFoundError:  # pragma: no cover - editable/dev checkout w/o metadata
    __version__ = "0.0.0.dev0"

__all__ = [
    "ApmRepoError",
    "Catalog",
    "CatalogFrame",
    "CatalogId",
    "ChunkCompactedError",
    "ClosableUnitProvider",
    "Connection",
    "ConnectionConfigId",
    "ConnectionId",
    "ContentSource",
    "ContentUnavailableError",
    "DataCorruptError",
    "Entry",
    "FileState",
    "Finding",
    "FormatError",
    "Frame",
    "ItemColumns",
    "KeyMaterialError",
    "KeyMismatchError",
    "KeyRequiredError",
    "KeyStatus",
    "KeyVerification",
    "LocalFsStore",
    "Node",
    "NodeFrame",
    "NodeRef",
    "NodeRole",
    "NotFoundError",
    "NotRestorableError",
    "ObjectStore",
    "PermissionDeniedError",
    "RawView",
    "RefKind",
    "Repository",
    "RepositoryLayout",
    "ResourceLimitExceededError",
    "RestorableUnit",
    "RootFrame",
    "SaasSubType",
    "SaasVersionId",
    "Session",
    "SetKeyResult",
    "SiteListItems",
    "SnapshotUuid",
    "Stage",
    "StorageBackendError",
    "StreamUuid",
    "Symptom",
    "TargetId",
    "TargetType",
    "TraceEvent",
    "TracingStore",
    "UnitKind",
    "UnitProvider",
    "UnsupportedDataFormatError",
    "UnsupportedVersionError",
    "VerifyLevel",
    "Version",
    "VersionId",
    "VersionLocation",
    "VersionMeta",
    "VersionUid",
    "WorkerProcessError",
    "Workload",
    "WorkloadFrame",
    "WorkloadId",
    "WorkloadUid",
    "__version__",
    "disambiguate",
    "disambiguate_catalogs",
    "disambiguate_versions",
    "disambiguate_workloads",
    "find_path_with_children",
    "node_kind_label",
    "read_site_list_items",
]
