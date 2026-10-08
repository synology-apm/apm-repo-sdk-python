"""Property tests for ``synology_apm_repo.sdk.format.chunkmap``: every
``support.format_builders`` encoding round-trips, and any bytes either parse
or raise a ``FormatError``."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import chunk_map_record_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.chunkmap import (
    ChunkMapKind,
    chunk_map_end_offset,
    iter_chunk_map_page,
    parse_chunk_map_record,
)
from unit.sdk.format_strategies import mutated, u32, u64

_Fields = tuple[ChunkMapKind, int, int, int, bool]

_fields = st.tuples(st.sampled_from(list(ChunkMapKind)), st.integers(0, (1 << 56) - 1), u64, u32, st.booleans())


def _encode(fields: _Fields) -> bytes:
    kind, file_chunk_idx, addr_int, tail_u32, inherit = fields
    return chunk_map_record_bytes(
        kind_value=kind.value, file_chunk_idx=file_chunk_idx, addr_int=addr_int, tail_u32=tail_u32, inherit=inherit
    )


@given(st.lists(_fields, min_size=1, max_size=4))
def test_records_round_trip(records: list[_Fields]) -> None:
    page = b"".join(_encode(fields) for fields in records)
    entries = list(iter_chunk_map_page(page, len(records)))
    for index, ((kind, file_chunk_idx, addr_int, tail_u32, inherit), entry) in enumerate(
        zip(records, entries, strict=True)
    ):
        assert entry == parse_chunk_map_record(page[index * 20 :])
        assert (entry.kind, entry.file_offset, entry.is_inherit) == (kind, file_chunk_idx << 12, inherit)
        if kind is ChunkMapKind.MAPPING:
            assert entry.addr is not None
            assert (entry.addr.to_int(), entry.map_num, entry.repeat) == (addr_int, tail_u32 >> 16, tail_u32 & 0xFFFF)
        else:
            assert (entry.addr, entry.map_num, entry.repeat) == (None, tail_u32, 0)
        assert chunk_map_end_offset(page, index) == entry.end_offset


_valid_page = st.lists(_fields, min_size=1, max_size=3).map(lambda records: b"".join(map(_encode, records)))


@given(data=st.binary(max_size=64) | mutated(_valid_page), count=st.integers(0, 4))
def test_parsers_return_or_raise_format_error(data: bytes, count: int) -> None:
    with contextlib.suppress(FormatError):
        parse_chunk_map_record(data)
    with contextlib.suppress(FormatError):
        list(iter_chunk_map_page(data, count))
    with contextlib.suppress(FormatError):
        chunk_map_end_offset(data, count)
