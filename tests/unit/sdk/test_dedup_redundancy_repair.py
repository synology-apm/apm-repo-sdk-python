"""Unit tests for ``synology_apm_repo.sdk.dedup.redundancy_repair``: fetching
a region's Redundancy trailer and repairing it off the event loop."""

from __future__ import annotations

import threading
import zlib

import pytest

import synology_apm_repo.sdk.dedup.redundancy_repair as redundancy_repair_module
from support.format_builders import redundancy_blob_bytes
from synology_apm_repo.sdk.dedup.redundancy_repair import repair_via_trailer
from synology_apm_repo.sdk.errors import (
    DataCorruptError,
    FormatError,
    NotFoundError,
    PermissionDeniedError,
    StorageBackendError,
)
from synology_apm_repo.sdk.format.redundancy import attempt_repair

_COVERAGE = 256
_DATA = bytes((i * 7 + 3) % 251 for i in range(600))  # 3 windows: [0, 256) [256, 512) [512, 600)
_EXPECTED_CRC = zlib.crc32(_DATA) & 0xFFFFFFFF


def _corrupted() -> bytes:
    damaged = bytearray(_DATA)
    damaged[300] ^= 0xFF
    return bytes(damaged)


class _Trailer:
    """A ``fetch_trailer`` callable returning ``raw`` and counting its calls."""

    def __init__(self, raw: bytes) -> None:
        self._raw = raw
        self.calls = 0

    async def __call__(self) -> bytes:
        self.calls += 1
        return self._raw


async def test_a_single_damaged_window_is_repaired_from_the_fetched_trailer() -> None:
    fetch = _Trailer(redundancy_blob_bytes(_DATA, coverage=_COVERAGE))
    repaired = await repair_via_trailer(
        _corrupted(), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch
    )
    assert repaired == _DATA
    assert fetch.calls == 1


async def test_an_unrepairable_region_returns_none() -> None:
    damaged = bytearray(_DATA)
    damaged[10] ^= 0xFF  # window 0
    damaged[590] ^= 0xFF  # window 2: not adjacent, beyond one repairable pair
    fetch = _Trailer(redundancy_blob_bytes(_DATA, coverage=_COVERAGE))
    assert (
        await repair_via_trailer(bytes(damaged), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch)
        is None
    )


async def test_a_malformed_trailer_returns_none() -> None:
    fetch = _Trailer(b"XX" + redundancy_blob_bytes(_DATA, coverage=_COVERAGE)[2:])
    assert (
        await repair_via_trailer(_corrupted(), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch)
        is None
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(NotFoundError("no trailer", ref="@data/Pool/1/0/0.buk"), id="not_found"),
        pytest.param(FormatError("truncated trailer"), id="format_error"),
        pytest.param(DataCorruptError("trailer CRC mismatch"), id="a_format_error_subclass"),
    ],
)
async def test_a_trailer_that_cannot_be_fetched_returns_none(error: Exception) -> None:
    async def fetch() -> bytes:
        raise error

    assert (
        await repair_via_trailer(_corrupted(), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch)
        is None
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(StorageBackendError("connection reset"), id="storage_backend_error"),
        pytest.param(PermissionDeniedError("read denied"), id="permission_denied"),
    ],
)
async def test_any_other_fetch_failure_propagates(error: Exception) -> None:
    async def fetch() -> bytes:
        raise error

    with pytest.raises(type(error)):
        await repair_via_trailer(_corrupted(), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch)


async def test_the_repair_runs_on_a_worker_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    threads: list[threading.Thread] = []

    def recording_attempt_repair(
        data: bytes, redundancy_raw: bytes, *, coverage: int, expected_crc: int
    ) -> bytes | None:
        threads.append(threading.current_thread())
        return attempt_repair(data, redundancy_raw, coverage=coverage, expected_crc=expected_crc)

    monkeypatch.setattr(redundancy_repair_module, "attempt_repair", recording_attempt_repair)
    fetch = _Trailer(redundancy_blob_bytes(_DATA, coverage=_COVERAGE))

    assert (
        await repair_via_trailer(_corrupted(), coverage=_COVERAGE, expected_crc=_EXPECTED_CRC, fetch_trailer=fetch)
        == _DATA
    )
    [worker] = threads
    assert worker is not threading.current_thread()
