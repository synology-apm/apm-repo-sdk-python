"""Property tests for ``synology_apm_repo.sdk.format.composition``: every
``support.format_builders`` encoding round-trips, and any bytes either parse
or raise a ``FormatError``."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import COMPOSITION_SUB_FILE_SIZE, composition_header_bytes, record_head_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.composition import (
    CompositionStatus,
    parse_composition_header,
    parse_record_head,
    verify_attr_crc,
    verify_chunk_map_crc,
)
from unit.sdk.format_strategies import mutated, u16, u32, u64


@given(
    status=st.sampled_from(list(CompositionStatus)),
    map_num=u64,
    map_crc=u32,
    mode=u16.map(lambda mode: mode | 0x0001),  # the Redundancy bit every supported record carries
    attr_leng=u32,
    attr_crc=u32,
    minor=u16,
)
def test_record_head_and_header_round_trip(
    status: CompositionStatus, map_num: int, map_crc: int, mode: int, attr_leng: int, attr_crc: int, minor: int
) -> None:
    head = parse_record_head(
        record_head_bytes(
            status=status.value, map_num=map_num, map_crc=map_crc, mode=mode, attr_leng=attr_leng, attr_crc=attr_crc
        )
    )
    assert (head.status, head.map_num, head.map_crc) == (status, map_num, map_crc)
    assert (head.mode, head.attr_leng, head.attr_crc, head.has_redundancy) == (mode, attr_leng, attr_crc, True)
    header = parse_composition_header(composition_header_bytes(minor=minor))
    assert (header.major, header.minor) == (1, minor)


_valid = st.builds(record_head_bytes, map_num=u64, status=st.integers(0, 2), mode=u16) | st.builds(
    composition_header_bytes,
    major=st.integers(0, 2),
    sub_file_size=st.sampled_from([COMPOSITION_SUB_FILE_SIZE]) | u32,
)


@given(data=st.binary(max_size=80) | _valid | mutated(_valid), expected_crc=u32)
def test_parsers_return_or_raise_format_error(data: bytes, expected_crc: int) -> None:
    with contextlib.suppress(FormatError):
        parse_record_head(data)
    with contextlib.suppress(FormatError):
        parse_composition_header(data)
    with contextlib.suppress(FormatError):
        verify_chunk_map_crc(data, expected_crc)
    with contextlib.suppress(FormatError):
        verify_attr_crc(data, expected_crc)
