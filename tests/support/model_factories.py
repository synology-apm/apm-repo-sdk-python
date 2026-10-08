"""One factory per SDK catalog model, for tests in any distribution that need
a ``Connection``/``Workload``/``Version``/``Catalog`` value without a
repository behind it. Every model field is a keyword argument with a
synthetic default; identifiers are taken as plain ``int``/``str`` and wrapped in their
``NewType`` here. A SaaS version is the same factory with
``target_type``/``saas_stream_uuid``/``saas_snapshot_uuid``/
``saas_version_id`` given."""

from __future__ import annotations

import types
from collections.abc import Mapping
from typing import cast

from synology_apm_repo.sdk.api.catalog import Catalog
from synology_apm_repo.sdk.api.key_manager import KeyManager
from synology_apm_repo.sdk.api.provider_registry import ProviderRegistry
from synology_apm_repo.sdk.catalog.connection import Connection
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.catalog.workload import Workload
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.identifiers import (
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
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


def make_connection(
    *,
    connection_config_id: int = 1,
    connection_id: str = "cc",
    display_name: str = "Source",
    namespaces: tuple[str, ...] = (),
    workload_count: int = 1,
    version_count: int = 1,
) -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(connection_config_id),
        connection_id=ConnectionId(connection_id),
        display_name=display_name,
        namespaces=namespaces,
        workload_count=workload_count,
        version_count=version_count,
    )


def make_workload(
    *,
    workload_id: int = 1,
    workload_uid: str | None = None,
    workload_type: str = "VM",
    sub_type: str | None = None,
    display_name: str = "Workload",
    subtitle: str | None = None,
    spec: Mapping[str, object] | None = None,
) -> Workload:
    """``workload_uid`` defaults to ``wl-<workload_id>``."""
    return Workload(
        workload_id=WorkloadId(workload_id),
        workload_uid=WorkloadUid(f"wl-{workload_id}" if workload_uid is None else workload_uid),
        workload_type=workload_type,
        sub_type=sub_type,
        display_name=display_name,
        subtitle=subtitle,
        spec={} if spec is None else spec,
    )


def make_version(
    *,
    version_id: int = 1,
    version_uid: str | None = None,
    workload_id: int = 1,
    connection_config_id: int = 1,
    target_type: str = "VM",
    target_id: str = "target",
    saas_stream_uuid: str = "",
    saas_snapshot_uuid: str = "",
    saas_version_id: int = 0,
    deleted: bool = False,
    display_name: str = "2026-01-01 00:00",
    meta: VersionMeta | None = None,
) -> Version:
    """``version_uid`` defaults to ``vuid-<version_id>``."""
    return Version(
        version_id=VersionId(version_id),
        version_uid=VersionUid(f"vuid-{version_id}" if version_uid is None else version_uid),
        workload_id=WorkloadId(workload_id),
        connection_config_id=ConnectionConfigId(connection_config_id),
        target_type=target_type,
        target_id=TargetId(target_id),
        saas_stream_uuid=StreamUuid(saas_stream_uuid),
        saas_snapshot_uuid=SnapshotUuid(saas_snapshot_uuid),
        saas_version_id=SaasVersionId(saas_version_id),
        deleted=deleted,
        display_name=display_name,
        meta=meta,
    )


def make_catalog(
    connection: Connection | None = None,
    *,
    dedup_repo: DedupRepo | None = None,
    providers: ProviderRegistry | None = None,
    keys: KeyManager | None = None,
) -> Catalog:
    """A real ``Catalog`` over ``connection`` (default ``make_connection()``).
    Without ``dedup_repo`` it sits on a placeholder vault repository whose
    I/O must never be called."""
    if dedup_repo is None:
        dedup_repo = cast(DedupRepo, types.SimpleNamespace(layout=RepoLayout(kind=RepoKind.VAULT, repo_root="")))
    return Catalog(
        dedup_repo,
        make_connection() if connection is None else connection,
        saas_streams=SaasStreamCache(dedup_repo),
        providers=ProviderRegistry() if providers is None else providers,
        keys=KeyManager(None, None) if keys is None else keys,
    )
