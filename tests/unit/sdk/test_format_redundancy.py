"""Unit tests for ``synology_apm_repo.sdk.format.redundancy``."""

from __future__ import annotations

import os
import struct
import zlib

import pytest

from support.format_builders import redundancy_blob_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.redundancy import (
    attempt_repair,
    parse_redundancy_blob,
    redundancy_size,
)


@pytest.mark.parametrize(
    ("data_size", "expected"),
    [
        # FORMAT-SPEC.md: a full bucket's SizeStore (chunkNum=8192 -> 15360
        # bytes) through the redundancy-size formula with coverage 256:
        # 16 + 4*ceil(15360/256) + min(15360, 512) = 16 + 240 + 512 = 768.
        pytest.param(15360, 768, id="worked_example_from_spec"),
        pytest.param(0, 16, id="zero_data_size"),  # header only, no StepCrc entries, no parity
        # Exactly divisible by coverage: no partial final StepCrc entry.
        pytest.param(512, 16 + 4 * 2 + min(512, 512), id="exact_multiple_of_coverage"),
        # 300 / 256 -> ceil = 2 StepCrc entries, not 1.
        pytest.param(300, 16 + 4 * 2 + min(300, 512), id="non_exact_multiple_rounds_step_crc_up"),
    ],
)
def test_redundancy_size(data_size: int, expected: int) -> None:
    assert redundancy_size(data_size, 256) == expected


def test_parity_capped_at_two_coverage() -> None:
    huge = 1_000_000
    coverage = 8192
    expected_step_crc = 4 * ((huge + coverage - 1) // coverage)
    assert redundancy_size(huge, coverage) == 16 + expected_step_crc + 2 * coverage


def test_composition_coverage_constant() -> None:
    # composition record trailers use coverage=8192 (FORMAT-SPEC.md: RecordHead)
    assert redundancy_size(20 * 20, 8192) == 16 + 4 * 1 + min(400, 16384)


class TestParseRedundancyBlob:
    def test_round_trips_a_well_formed_blob(self) -> None:
        data = os.urandom(600)
        coverage = 256
        blob_raw = redundancy_blob_bytes(data, coverage=coverage)

        blob = parse_redundancy_blob(blob_raw, data_size=len(data), coverage=coverage)

        assert blob.coverage == coverage
        assert blob.data_size == len(data)
        assert len(blob.step_crc) == 3  # ceil(600/256)
        assert len(blob.parity) == min(600, 512)

    def test_rejects_mismatched_coverage_or_data_size(self) -> None:
        data = os.urandom(600)
        blob_raw = redundancy_blob_bytes(data, coverage=256)

        import pytest

        from synology_apm_repo.sdk.errors import FormatError

        with pytest.raises(FormatError, match=r"Redundancy blob header .* doesn't match expected"):
            parse_redundancy_blob(blob_raw, data_size=len(data), coverage=128)
        with pytest.raises(FormatError, match=r"Redundancy blob header .* doesn't match expected"):
            parse_redundancy_blob(blob_raw, data_size=len(data) + 1, coverage=256)

    @pytest.mark.parametrize("version", [1, 2, 0xFFFF])
    def test_rejects_an_unsupported_version(self, version: int) -> None:
        data = os.urandom(600)
        blob_raw = bytearray(redundancy_blob_bytes(data, coverage=256))
        blob_raw[2:4] = struct.pack(">H", version)

        with pytest.raises(FormatError, match=f"unsupported Redundancy blob version {version}"):
            parse_redundancy_blob(bytes(blob_raw), data_size=len(data), coverage=256)

    def test_rejects_bad_magic(self) -> None:
        import pytest

        from synology_apm_repo.sdk.errors import FormatError

        data = os.urandom(600)
        blob_raw = bytearray(redundancy_blob_bytes(data, coverage=256))
        blob_raw[0:2] = b"XX"
        with pytest.raises(FormatError, match="bad Redundancy magic"):
            parse_redundancy_blob(bytes(blob_raw), data_size=len(data), coverage=256)


class TestAttemptRepair:
    @pytest.mark.parametrize(
        "corrupt_at",
        [
            pytest.param(300, id="a_single_corrupted_window"),  # middle window (index 1)
            pytest.param(10, id="a_corrupted_first_window"),
            # Final, truncated window [512, 600): no bad_idx + 1, so it is
            # reconstructed alone, not as a pair.
            pytest.param(590, id="a_corrupted_last_window_with_no_following_window"),
        ],
    )
    def test_repairs_one_corrupted_window(self, corrupt_at: int) -> None:
        coverage = 256
        data = os.urandom(600)  # 3 windows: [0,256) [256,512) [512,600)
        redundancy_raw = redundancy_blob_bytes(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[corrupt_at] ^= 0xFF

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired == data

    def test_gives_up_on_two_non_adjacent_corrupted_windows(self) -> None:
        """Damage beyond one parity-repairable pair fails the final
        whole-buffer CRC re-check: ``None``, not a wrong patch."""
        coverage = 256
        data = os.urandom(1200)  # 5 windows
        redundancy_raw = redundancy_blob_bytes(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[10] ^= 0xFF  # window 0
        corrupted[1100] ^= 0xFF  # window 4 -- not adjacent to window 0

        repaired = attempt_repair(bytes(corrupted), redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_gives_up_when_the_redundancy_blob_itself_is_corrupted(self) -> None:
        coverage = 256
        data = os.urandom(600)
        redundancy_raw = bytearray(redundancy_blob_bytes(data, coverage=coverage))
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        corrupted = bytearray(data)
        corrupted[10] ^= 0xFF
        redundancy_raw[0:2] = b"XX"  # trash the blob's own magic

        repaired = attempt_repair(bytes(corrupted), bytes(redundancy_raw), coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_returns_none_when_data_is_not_actually_corrupted(self) -> None:
        """No divergent StepCrc checkpoint, so nothing to localize."""
        coverage = 256
        data = os.urandom(600)
        redundancy_raw = redundancy_blob_bytes(data, coverage=coverage)
        expected_crc = zlib.crc32(data) & 0xFFFFFFFF

        repaired = attempt_repair(data, redundancy_raw, coverage=coverage, expected_crc=expected_crc)

        assert repaired is None

    def test_a_valid_blob_without_any_window_checksums_cannot_repair(self) -> None:
        """A blob for zero-length data parses but holds no StepCrc entry
        to localize damage with."""
        blob_raw = redundancy_blob_bytes(b"", coverage=256)
        assert parse_redundancy_blob(blob_raw, data_size=0, coverage=256).step_crc == ()

        assert attempt_repair(b"", blob_raw, coverage=256, expected_crc=0) is None

    def test_empty_data_never_repairable(self) -> None:
        assert attempt_repair(b"", b"", coverage=256, expected_crc=0) is None
