"""Property tests for ``synology_apm_repo.sdk.format.redundancy``: every
``support.format_builders`` encoding round-trips and repairs one damaged
byte, and any bytes either parse or raise a ``FormatError``."""

from __future__ import annotations

import contextlib
import zlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import redundancy_blob_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.redundancy import attempt_repair, parse_redundancy_blob, redundancy_size
from unit.sdk.format_strategies import mutated, u32

_coverage = st.integers(1, 24)


@given(
    data=st.binary(min_size=1, max_size=96), coverage=_coverage, damage=st.tuples(st.integers(0), st.integers(1, 0xFF))
)
def test_blob_round_trips_and_repairs_one_damaged_byte(data: bytes, coverage: int, damage: tuple[int, int]) -> None:
    """A single damaged byte sits in one window, which the parity of its
    even/odd half rebuilds."""
    raw = redundancy_blob_bytes(data, coverage=coverage)
    assert len(raw) == redundancy_size(len(data), coverage)
    blob = parse_redundancy_blob(raw, data_size=len(data), coverage=coverage)
    assert (blob.coverage, blob.data_size, len(blob.step_crc)) == (coverage, len(data), -(-len(data) // coverage))
    assert blob.step_crc[-1] == zlib.crc32(data)

    position, mask = damage
    damaged = bytearray(data)
    damaged[position % len(data)] ^= mask
    assert attempt_repair(bytes(damaged), raw, coverage=coverage, expected_crc=zlib.crc32(data)) == data


_valid = st.builds(redundancy_blob_bytes, st.binary(max_size=48), coverage=_coverage, version=st.integers(0, 1))


@given(
    raw=st.binary(max_size=64) | _valid | mutated(_valid),
    data=st.binary(max_size=48),
    coverage=_coverage,
    expected_crc=u32,
)
def test_parse_raises_format_error_and_repair_never_raises(
    raw: bytes, data: bytes, coverage: int, expected_crc: int
) -> None:
    with contextlib.suppress(FormatError):
        parse_redundancy_blob(raw, data_size=len(data), coverage=coverage)
    repaired = attempt_repair(data, raw, coverage=coverage, expected_crc=expected_crc)
    assert repaired is None or zlib.crc32(repaired) == expected_crc
