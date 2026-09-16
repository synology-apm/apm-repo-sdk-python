"""synology-apm-repo-sdk — offline, read-only SDK for the Synology
APV/Object-Storage dedup repository on-disk format.

``Session``/``Repository`` (the Repository Layer) are the intended entry
point for a normal consumer — see ``ARCHITECTURE.md``'s "Repository Layer
— ``api/``, the facade" for the full contract. Everything else exported
here is either a type a consumer receives back from that facade
(``Catalog``/``Connection``/``Workload``/``Version``, ``Node``/``RestorableUnit``,
``DedupFile``/``ByteRangeView``, ``Progress``, ``Finding``/``VerifyLevel``
from ``Repository.verify``, ``Frame`` from ``Repository.walk_human_ref``,
``TraceEvent`` from ``Session.discover``'s
``trace=`` callback) or a lower-layer building block (``ObjectStore`` and
its implementations) for a consumer that genuinely needs to construct its
own backend rather than let ``Session.discover()`` do it.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _get_version

from .api import Catalog, Finding, Frame, KeyStatus, Repository, Session, TraceEvent, VerifyLevel
from .catalog.connection import Connection
from .catalog.version import Version, VersionMeta
from .catalog.workload import TargetType, Workload
from .dedup.dedup_file import (
    ByteRangeView,
    DedupFile,
    ExportResult,
    Extent,
    ExtentKind,
)
from .dedup.keys import KeyMaterial, KeyVerification
from .errors import (
    ApmRepoError,
    ChunkCompactedError,
    DataCorruptError,
    FormatError,
    KeyMaterialError,
    KeyMismatchError,
    KeyRequiredError,
    NotFoundError,
    ProfileConfigCorruptError,
    ProfileNotFoundError,
    ProfileSecretBackendUnavailableError,
    UnsupportedDataFormatError,
    UnsupportedVersionError,
)
from .presentation.progress import Progress, ProgressMeter
from .profiles import (
    AzureProfileConfig,
    BackendKind,
    Profile,
    ProfileSummary,
    S3ProfileConfig,
    SmbProfileConfig,
    build_store,
    client_kwargs_with_secrets,
    delete_profile,
    get_profile,
    list_profiles,
    list_remote_items,
    load_profile,
    save_profile,
    store_from_config,
)
from .storage import (
    DirCache,
    LocalFsStore,
    ObjectStore,
    RepoKind,
    RepoLayout,
    RepositoryLayout,
    catalog_repo_layouts,
    detect_layout,
    detect_repository_layout,
    iter_layouts,
    iter_repository_layouts,
)
from .units.base import (
    ContentSource,
    Node,
    RestorableUnit,
    UnitKind,
    UnitProvider,
)
from .units.node_ref import NodeRef, RefKind, disambiguate

try:
    __version__ = _get_version("synology-apm-repo-sdk")
except PackageNotFoundError:  # pragma: no cover - editable/dev checkout w/o metadata
    __version__ = "0.0.0.dev0"

__all__ = [
    "ApmRepoError",
    "AzureProfileConfig",
    "BackendKind",
    "ByteRangeView",
    "Catalog",
    "Connection",
    "ChunkCompactedError",
    "ContentSource",
    "DataCorruptError",
    "DedupFile",
    "DirCache",
    "ExportResult",
    "Extent",
    "ExtentKind",
    "Finding",
    "FormatError",
    "Frame",
    "KeyMaterial",
    "KeyMaterialError",
    "KeyMismatchError",
    "KeyRequiredError",
    "KeyStatus",
    "KeyVerification",
    "LocalFsStore",
    "Node",
    "NodeRef",
    "NotFoundError",
    "ObjectStore",
    "Profile",
    "ProfileConfigCorruptError",
    "ProfileNotFoundError",
    "ProfileSecretBackendUnavailableError",
    "ProfileSummary",
    "Progress",
    "ProgressMeter",
    "RefKind",
    "RepoKind",
    "RepoLayout",
    "Repository",
    "RepositoryLayout",
    "RestorableUnit",
    "S3ProfileConfig",
    "Session",
    "SmbProfileConfig",
    "TargetType",
    "TraceEvent",
    "UnitKind",
    "UnitProvider",
    "UnsupportedDataFormatError",
    "UnsupportedVersionError",
    "VerifyLevel",
    "Version",
    "VersionMeta",
    "Workload",
    "__version__",
    "build_store",
    "catalog_repo_layouts",
    "client_kwargs_with_secrets",
    "delete_profile",
    "detect_layout",
    "detect_repository_layout",
    "disambiguate",
    "get_profile",
    "iter_layouts",
    "iter_repository_layouts",
    "list_profiles",
    "list_remote_items",
    "load_profile",
    "save_profile",
    "store_from_config",
]
