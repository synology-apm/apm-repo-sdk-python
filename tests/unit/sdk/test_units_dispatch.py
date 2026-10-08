"""Unit tests for ``synology_apm_repo.sdk.units.dispatch``."""

from __future__ import annotations

from typing import cast

import pytest

from support.model_factories import make_version
from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.catalog.workload import DEVICE_TARGET_TYPES
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import UnsupportedDataFormatError
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.dispatch import provider_for
from synology_apm_repo.sdk.units.fs import FsProvider


class _FakeRepo:
    """A placeholder repository: neither provider's construction touches it."""

    store = object()


def _version(target_type: str) -> Version:
    return make_version(version_uid="vuid", target_type=target_type)


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
    with pytest.raises(UnsupportedDataFormatError, match="no provider yet for target_type"):
        await provider_for(_FAKE_REPO, _version(target_type))


@pytest.mark.parametrize(
    ("target_type", "expected"), [("GW", True), ("M365", True), ("VM", False), ("PC", False), ("NEW", False)]
)
def test_version_is_saas_follows_its_target_type(target_type: str, expected: bool) -> None:
    assert _version(target_type).is_saas is expected


async def test_device_target_types_match_what_provider_for_actually_handles() -> None:
    assert {"VM", "PC", "PS", "FS"} == DEVICE_TARGET_TYPES
    for target_type in DEVICE_TARGET_TYPES:
        await provider_for(_FAKE_REPO, _version(target_type))  # must not raise
