"""Unit tests for ``synology_apm_repo.sdk.units.dispatch``."""

from __future__ import annotations

from typing import cast

import pytest

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import UnsupportedDataFormatError
from synology_apm_repo.sdk.identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.dispatch import SUPPORTED_TARGET_TYPES, provider_for
from synology_apm_repo.sdk.units.fs import FsProvider


class _FakeRepo:
    """Both provider constructors only touch ``repo.store`` (to build
    their own ``DirCache``, which itself does nothing eager with it) —
    a placeholder is enough to test dispatch without a real repository."""

    store = object()


def _version(target_type: str) -> Version:
    return Version(
        version_id=VersionId(1),
        version_uid=VersionUid("vuid"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(1),
        target_type=target_type,
        target_id=TargetId("target"),
        saas_stream_uuid=StreamUuid(""),
        saas_snapshot_uuid=SnapshotUuid(""),
        saas_version_id=SaasVersionId(0),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


_FAKE_REPO = cast("DedupRepo", _FakeRepo())


@pytest.mark.parametrize("target_type", ["VM", "PC", "PS"])
async def test_device_target_types_dispatch_to_device_provider(target_type: str) -> None:
    provider = await provider_for(_FAKE_REPO, _version(target_type))
    assert isinstance(provider, DeviceProvider)


async def test_fs_dispatches_to_fs_provider() -> None:
    provider = await provider_for(_FAKE_REPO, _version("FS"))
    assert isinstance(provider, FsProvider)


@pytest.mark.parametrize("target_type", ["M365", "GW"])
async def test_saas_target_types_raise_unsupported_data_format(target_type: str) -> None:
    with pytest.raises(UnsupportedDataFormatError):
        await provider_for(_FAKE_REPO, _version(target_type))


async def test_supported_target_types_matches_what_provider_for_actually_handles() -> None:
    assert {"VM", "PC", "PS", "FS"} == SUPPORTED_TARGET_TYPES
    for target_type in SUPPORTED_TARGET_TYPES:
        await provider_for(_FAKE_REPO, _version(target_type))  # must not raise
