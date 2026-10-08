"""Unit tests for ``synology_apm_repo.sdk.format.chunkmap``.

Fixture bytes come from ``support.format_builders.chunk_map_record_bytes``,
built from the byte-offset table (FORMAT-SPEC.md: ChunkMapRecord)
independently of ``chunkmap.py``'s own bit-packing formulas, so the two
can't share a blind spot.
"""

from __future__ import annotations

import pytest

from support.format_builders import (
    chunk_addr_int,
    chunk_map_record_bytes,
)
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.chunkmap import (
    ChunkMapKind,
    chunk_map_end_offset,
    iter_chunk_map_page,
    parse_chunk_map_record,
)
from unit.sdk.pool_fakes import chunk_address


class TestMapping:
    def test_basic_decode(self) -> None:
        addr_int = chunk_addr_int(132, 330, 17)
        data = chunk_map_record_bytes(
            kind_value=0,
            inherit=False,
            file_chunk_idx=5,
            addr_int=addr_int,
            tail_u32=(3 << 16) | 7,  # map_num=3, repeat=7
        )
        entry = parse_chunk_map_record(data)

        assert entry.kind is ChunkMapKind.MAPPING
        assert entry.is_inherit is False
        assert entry.file_offset == 5 * 4096
        assert entry.addr == chunk_address(132, 330, 17)
        assert entry.map_num == 3
        assert entry.repeat == 7

    def test_length_formula(self) -> None:
        data = chunk_map_record_bytes(
            kind_value=0, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=(10 << 16) | 4
        )
        entry = parse_chunk_map_record(data)
        assert entry.length == 10 * (1 + 4) * 4096
        assert entry.end_offset == entry.file_offset + entry.length

    def test_zero_repeat_means_run_once(self) -> None:
        data = chunk_map_record_bytes(kind_value=0, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=(5 << 16) | 0)
        entry = parse_chunk_map_record(data)
        assert entry.length == 5 * 4096

    def test_inherit_bit_is_parsed_but_does_not_change_addressing(self) -> None:
        addr_int = chunk_addr_int(1, 1, 1)
        with_inherit = parse_chunk_map_record(
            chunk_map_record_bytes(
                kind_value=0, inherit=True, file_chunk_idx=0, addr_int=addr_int, tail_u32=(1 << 16) | 0
            )
        )
        without_inherit = parse_chunk_map_record(
            chunk_map_record_bytes(
                kind_value=0, inherit=False, file_chunk_idx=0, addr_int=addr_int, tail_u32=(1 << 16) | 0
            )
        )
        assert with_inherit.is_inherit is True
        assert without_inherit.is_inherit is False
        assert with_inherit.addr == without_inherit.addr
        assert with_inherit.length == without_inherit.length

    def test_large_file_chunk_idx(self) -> None:
        # 7-byte field: exercise a value well past 32 bits to catch any
        # accidental truncation to a narrower int type.
        big_idx = (1 << 40) + 12345
        data = chunk_map_record_bytes(kind_value=0, inherit=False, file_chunk_idx=big_idx, addr_int=0, tail_u32=1 << 16)
        entry = parse_chunk_map_record(data)
        assert entry.file_offset == big_idx * 4096

    def test_address_with_chunk_idx_past_bucket_capacity_is_not_validated_here(self) -> None:
        # chunk_idx field (low 16 bits of the address word) at 8192 is out
        # of any real bucket's capacity, but ``parse_chunk_map_record()``
        # trusts it, same as ``ChunkAddress.from_int()``.
        bad_addr_int = (1 << 56) | (0 << 16) | 8192
        data = chunk_map_record_bytes(
            kind_value=0, inherit=False, file_chunk_idx=0, addr_int=bad_addr_int, tail_u32=1 << 16
        )
        entry = parse_chunk_map_record(data)
        assert entry.addr is not None
        assert entry.addr.chunk_idx == 8192


class TestZero:
    def test_basic_decode(self) -> None:
        data = chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=8, addr_int=0, tail_u32=42)
        entry = parse_chunk_map_record(data)

        assert entry.kind is ChunkMapKind.ZERO
        assert entry.file_offset == 8 * 4096
        assert entry.addr is None
        assert entry.map_num == 42
        assert entry.repeat == 0
        assert entry.length == 42 * 4096

    def test_32_bit_count_not_split_into_map_num_and_repeat(self) -> None:
        huge = (1 << 20) + 3  # would overflow a 16-bit map_num field if mis-split
        data = chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=huge)
        entry = parse_chunk_map_record(data)
        assert entry.map_num == huge
        assert entry.length == huge * 4096

    def test_addr_bytes_present_but_ignored(self) -> None:
        data = chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=0, addr_int=0xDEADBEEF, tail_u32=1)
        entry = parse_chunk_map_record(data)
        assert entry.addr is None

    def test_inherit_bit_on_zero_record(self) -> None:
        data = chunk_map_record_bytes(kind_value=1, inherit=True, file_chunk_idx=0, addr_int=0, tail_u32=1)
        entry = parse_chunk_map_record(data)
        assert entry.is_inherit is True
        assert entry.kind is ChunkMapKind.ZERO


class TestValidation:
    def test_unknown_type_raises_data_corrupt(self) -> None:
        data = chunk_map_record_bytes(kind_value=7, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=0)
        with pytest.raises(DataCorruptError, match="unknown ChunkMapRecord type"):
            parse_chunk_map_record(data)

    def test_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError, match="ChunkMapRecord too short"):
            parse_chunk_map_record(b"\x00" * 19)

    def test_extra_trailing_bytes_are_ignored(self) -> None:
        # Callers parse records back to back out of a larger buffer, so
        # ``parse_chunk_map_record`` reads only its own 20 bytes.
        data = (
            chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=1) + b"\xff" * 20
        )
        entry = parse_chunk_map_record(data)
        assert entry.kind is ChunkMapKind.ZERO


class TestIterChunkMapPage:
    def test_decodes_several_records_in_order(self) -> None:
        page_bytes = b"".join(
            chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=i, addr_int=0, tail_u32=i + 1)
            for i in range(4)
        )
        entries = list(iter_chunk_map_page(page_bytes, 4))
        assert [e.file_offset for e in entries] == [i * 4096 for i in range(4)]
        assert [e.map_num for e in entries] == [1, 2, 3, 4]
        for i, entry in enumerate(entries):
            assert entry == parse_chunk_map_record(page_bytes[i * 20 : (i + 1) * 20])

    def test_count_less_than_available_only_consumes_the_first_count_records(self) -> None:
        page_bytes = b"".join(
            chunk_map_record_bytes(kind_value=1, inherit=False, file_chunk_idx=i, addr_int=0, tail_u32=1)
            for i in range(4)
        )
        entries = list(iter_chunk_map_page(page_bytes, 2))
        assert len(entries) == 2
        assert [e.file_offset for e in entries] == [0, 4096]

    def test_zero_count_yields_nothing(self) -> None:
        assert list(iter_chunk_map_page(b"", 0)) == []


class TestChunkMapEndOffset:
    """``chunk_map_end_offset`` must agree with the full parse it skips."""

    def _page(self, *records: bytes) -> bytes:
        return b"".join(records)

    def test_matches_the_parsed_end_offset_for_both_kinds(self) -> None:
        mapping = chunk_map_record_bytes(
            kind_value=ChunkMapKind.MAPPING.value,
            file_chunk_idx=3,
            addr_int=0x0102_0304_0000_0005,
            tail_u32=(4 << 16) | 2,
        )
        zero = chunk_map_record_bytes(kind_value=ChunkMapKind.ZERO.value, file_chunk_idx=15, addr_int=0, tail_u32=7)
        page = self._page(mapping, zero)
        for index, record in enumerate((mapping, zero)):
            assert chunk_map_end_offset(page, index) == parse_chunk_map_record(record).end_offset

    def test_an_unknown_type_is_corrupt(self) -> None:
        page = chunk_map_record_bytes(kind_value=0x0E, file_chunk_idx=0, addr_int=0, tail_u32=1)
        with pytest.raises(DataCorruptError, match="unknown ChunkMapRecord type"):
            chunk_map_end_offset(page, 0)

    def test_a_page_ending_early_is_a_format_error(self) -> None:
        page = chunk_map_record_bytes(kind_value=ChunkMapKind.ZERO.value, file_chunk_idx=0, addr_int=0, tail_u32=1)
        with pytest.raises(FormatError, match="chunk map page ends before record"):
            chunk_map_end_offset(page, 1)
