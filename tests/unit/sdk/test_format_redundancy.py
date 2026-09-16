"""Unit tests for ``synology_apm_repo.sdk.format.redundancy``."""

from __future__ import annotations

import os
import struct
import zlib

from synology_apm_repo.sdk.format.redundancy import (
    REDUNDANCY_MAGIC,
    attempt_repair,
    parse_redundancy_blob,
    redundancy_size,
)


def _build_redundancy_blob(data: bytes, *, coverage: int) -> bytes:
    """Write-time-equivalent construction of a Redundancy blob for
    ``data`` — the inverse of ``attempt_repair``'s read-time reconstruction,
    used only by these tests to produce a real, valid blob to then corrupt
    and repair against."""
    data_size = len(data)
    num_windows = (data_size + coverage - 1) // coverage if data_size else 0
    step_crc: list[int] = []
    running = 0
    parity = bytearray(min(data_size, 2 * coverage))
    for i in range(num_windows):
        start = i * coverage
        end = min(start + coverage, data_size)
        window = data[start:end]
        running = zlib.crc32(window, running) & 0xFFFFFFFF
        step_crc.append(running)
        half = i % 2
        for j, b in enumerate(window):
            parity[half * coverage + j] ^= b
    header = REDUNDANCY_MAGIC + struct.pack(">H", 0) + struct.pack(">IQ", coverage, data_size)
    step_crc_bytes = b"".join(struct.pack(">I", c) for c in step_crc)
    return header + step_crc_bytes + bytes(parity)


def test_worked_example_from_spec() -> None:
    # on-disk-format.md §4.9's own worked example: a full bucket's SizeStore
    # (chunkNum=8192 -> getChunkSizeLeng=15360) with bucket coverage 256:
    # 16 + 4*ceil(15360/256) + min(15360, 512) = 16 + 240 + 512 = 768.
    assert redundancy_size(15360, 256) == 768


def test_zero_data_size() -> None:
    assert redundancy_size(0, 256) == 16  # header only, no StepCrc entries, no parity


def test_exact_multiple_of_coverage() -> None:
    # data_size exactly divisible by coverage: no partial final StepCrc entry
    assert redundancy_size(512, 256) == 16 + 4 * 2 + min(512, 512)


def test_non_exact_multiple_rounds_step_crc_up() -> None:
    # 300 bytes / 256 coverage -> ceil = 2 StepCrc entries, not 1
    assert redundancy_size(300, 256) == 16 + 4 * 2 + min(300, 512)


def test_parity_capped_at_two_coverage() -> None:
    # data_size far exceeds 2*coverage -> parity length caps at 2*coverage
    huge = 1_000_000
    coverage = 8192
    expected_step_crc = 4 * ((huge + coverage - 1) // coverage)
    assert redundancy_size(huge, coverage) == 16 + expected_step_crc + 2 * coverage


def test_composition_coverage_constant() -> None:
    # composition record trailers use coverage=8192 (on-disk-format.md §15.2)
    assert redundancy_size(20 * 20, 8192) == 16 + 4 * 1 + min(400, 16384)


class TestParseRedundancyBlob:
    def test_round_trips_a_well_formed_blob(self) -> None:
        data = os.urandom(600)
        coverage = 256
        blob_raw = _build_redundancy_blob(data, coverage=coverage)

        blob = parse_redundancy_blob(blob_raw, data_size=len(data), coverage=coverage)

        assert blob.coverage == coverage
        assert blob.data_size == len(data)
        assert len(blob.step_crc) == 3  # ceil(600/256)
        assert len(blob.parity) == min(600, 512)

    def test_rejects_mismatched_coverage_or_data_size(self) -> None:
        data = os.urandom(600)
        blob_raw = _build_redundancy_blob(data, coverage=256)

        import pytest

        from synology_apm_repo.sdk.errors import FormatError

        with pytest.raises(FormatError):
            parse_redundancy_blob(blob_raw, data_size=len(data), coverage=128)
        with pytest.raises(FormatError):
            parse_redundancy_blob(blob_raw, data_size=len(data) + 1, coverage=256)

    def test_rejects_bad_magic(self) -> None:
        import pytest

        from synology_apm_repo.sdk.errors import FormatError

        data = os.urandom(600)
        blob_raw = bytearray(_build_redundancy_blob(data, coverage=256))
        blob_raw[0:2] = b"XX"
        with pytest.raises(FormatError):
            parse_redundancy_blob(bytes(blob_raw), data_size=len(data), coverage=256)


class TestAttemptRepair:
    def test_repairs_a_single_corrupted_window(self) -> None:
        coverage = 256
        data = os.urandom(600)  # 3 windows: [0,256) [256,512) [512,600)
        redundancy_raw = _build_redundancy_blob(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[300] ^= 0xFF  # inside the middle window (index 1)

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired == data

    def test_repairs_a_corrupted_first_window(self) -> None:
        """The edge case where ``bad_idx == 0`` -- ``bad_idx - 1`` doesn't
        exist, only the ``[0, 1]`` pair is reconstructed."""
        coverage = 256
        data = os.urandom(600)
        redundancy_raw = _build_redundancy_blob(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[10] ^= 0xFF  # inside window 0

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired == data

    def test_repairs_a_corrupted_last_window_with_no_following_window(self) -> None:
        """``bad_idx`` is the final window and ``bad_idx + 1`` doesn't
        exist -- only a single window is reconstructed, not a pair."""
        coverage = 256
        data = os.urandom(600)  # last window is [512, 600), only 88 bytes
        redundancy_raw = _build_redundancy_blob(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[590] ^= 0xFF  # inside the final, truncated window (index 2)

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired == data

    def test_gives_up_on_two_non_adjacent_corrupted_windows(self) -> None:
        """More than one corrupted region, not confined to one
        parity-repairable pair -- the final whole-buffer CRC re-check must
        fail, and this returns None rather than a wrong patch."""
        coverage = 256
        data = os.urandom(1200)  # 5 windows
        redundancy_raw = _build_redundancy_blob(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[10] ^= 0xFF  # window 0
        corrupted[1100] ^= 0xFF  # window 4 -- not adjacent to window 0

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_gives_up_when_the_redundancy_blob_itself_is_corrupted(self) -> None:
        coverage = 256
        data = os.urandom(600)
        redundancy_raw = bytearray(_build_redundancy_blob(data, coverage=coverage))
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[10] ^= 0xFF
        redundancy_raw[0:2] = b"XX"  # trash the blob's own magic

        repaired = attempt_repair(bytes(corrupted), bytes(redundancy_raw), coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_returns_none_when_data_is_not_actually_corrupted(self) -> None:
        """No divergent checkpoint at all -- the rolling scan finds
        nothing to localize (callers are only expected to reach this after
        their own CRC check already failed)."""
        coverage = 256
        data = os.urandom(600)
        redundancy_raw = _build_redundancy_blob(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        repaired = attempt_repair(data, redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_empty_data_never_repairable(self) -> None:
        assert attempt_repair(b"", b"", coverage=256, expected_crc=0) is None
