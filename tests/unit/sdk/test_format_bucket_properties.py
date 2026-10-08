"""Property tests for ``synology_apm_repo.sdk.format.bucket``: every
``support.format_builders`` encoding round-trips, and any bytes either parse
or raise a ``FormatError``."""

from __future__ import annotations

import array
import contextlib
import zlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import bucket_header_bytes, chunk_crc_store_bytes, encode_size_store
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.bucket import (
    BucketIndex,
    SizeStoreEntry,
    parse_bucket_header,
    parse_chunk_crc_store,
    parse_size_store,
)
from synology_apm_repo.sdk.format.compression import CompressType
from unit.sdk.format_strategies import mutated, u16, u32

_KNOWN_TYPES = [member.value for member in CompressType]
_UNKNOWN_TYPES = [value for value in range(8) if value not in _KNOWN_TYPES]

# The 12-bit field's extremes are drawn as often as the rest of its range.
_stored_len = st.sampled_from([0, 0xFFF]) | st.integers(0, 0xFFF)
_entry = st.tuples(st.sampled_from(_KNOWN_TYPES), _stored_len)


@given(entries=st.lists(_entry, max_size=40), with_crc=st.booleans())
def test_size_store_round_trips(entries: list[tuple[int, int]], with_crc: bool) -> None:
    """Counts across several 8-record/15-byte group boundaries, so every
    record's bit phase in a group, with and without the CRC check."""
    tight = encode_size_store(entries)
    crc = zlib.crc32(tight) if with_crc else None
    decoded = parse_size_store(tight, len(entries), verify_crc=crc)
    assert [(entry.compress_type.value, entry.stored_len) for entry in decoded] == entries


@given(
    st.lists(st.tuples(st.integers(0, 7), _stored_len), min_size=1, max_size=24).filter(
        lambda entries: any(type_value in _UNKNOWN_TYPES for type_value, _ in entries)
    )
)
def test_size_store_names_the_first_unknown_type(entries: list[tuple[int, int]]) -> None:
    position = next(i for i, (type_value, _) in enumerate(entries) if type_value in _UNKNOWN_TYPES)
    with pytest.raises(DataCorruptError, match=f"unknown CompressType {entries[position][0]} at chunk {position} "):
        parse_size_store(encode_size_store(entries), len(entries))


@given(st.lists(st.builds(SizeStoreEntry, st.sampled_from(list(CompressType)), _stored_len), max_size=40))
def test_index_lengths_and_crc_positions_follow_each_entry(entries: list[SizeStoreEntry]) -> None:
    """Runs of NONE/COMPACTED anywhere, including at either end, against
    ``SizeStoreEntry.effective_len`` one entry at a time."""
    index = BucketIndex.of(
        array.array("B", [entry.compress_type.value for entry in entries]),
        array.array("H", [entry.stored_len for entry in entries]),
        data_start=0,
    )
    assert list(index.effective_lens) == [entry.effective_len for entry in entries]
    assert list(index.crc_store_positions()) == [
        sum(1 for entry in entries[:i] if entry.effective_len > 0) for i in range(len(entries))
    ]


@given(
    major=st.integers(0, 3),
    minor=u16,
    mode=u32,
    chunk_num=u32,
    chunk_size_crc=u32,
    chunk_crcs=st.lists(u32, max_size=16),
)
def test_header_and_chunk_crc_store_round_trip(
    major: int, minor: int, mode: int, chunk_num: int, chunk_size_crc: int, chunk_crcs: list[int]
) -> None:
    """The header's ``crcOfChunkCrc`` checks the trailer it describes."""
    trailer = chunk_crc_store_bytes(chunk_crcs)
    header = parse_bucket_header(
        bucket_header_bytes(
            major=major,
            minor=minor,
            mode=mode,
            chunk_num=chunk_num,
            chunk_size_crc=chunk_size_crc,
            crc_of_chunk_crc=zlib.crc32(trailer),
        )
    )
    assert (header.major, header.minor, header.mode) == (major, minor, mode)
    assert (header.chunk_num, header.chunk_size_crc) == (chunk_num, chunk_size_crc)
    assert parse_chunk_crc_store(trailer, len(chunk_crcs), verify_crc=header.crc_of_chunk_crc) == tuple(chunk_crcs)


_valid_header = st.builds(bucket_header_bytes, mode=u32, chunk_num=st.integers(0, 32), major=st.integers(0, 3))
_valid_size_store = st.lists(_entry, max_size=24).map(encode_size_store)


@given(
    data=st.binary(max_size=96) | mutated(_valid_header) | mutated(_valid_size_store),
    count=st.integers(0, 32),
    verify_crc=st.none() | u32,
)
def test_parsers_return_or_raise_format_error(data: bytes, count: int, verify_crc: int | None) -> None:
    with contextlib.suppress(FormatError):
        parse_bucket_header(data)
    with contextlib.suppress(FormatError):
        parse_size_store(data, count, verify_crc=verify_crc)
    with contextlib.suppress(FormatError):
        parse_chunk_crc_store(data, count, verify_crc=verify_crc)
