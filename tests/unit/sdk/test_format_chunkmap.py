"""Unit tests for ``synology_apm_repo.sdk.format.chunkmap``.

Fixture bytes are built directly from the byte-offset table
(on-disk-format.md §8), independently of ``chunkmap.py``'s own bit-packing
formulas, so a bug shared between the implementation and a naively-mirrored
test fixture can't hide from these.
"""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind, parse_chunk_map_record
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId


def _record_bytes(*, type_value: int, inherit: bool, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = (type_value & 0x0F) | (0x10 if inherit else 0)
    idx_bytes = file_chunk_idx.to_bytes(7, "big")
    return bytes([type_byte]) + idx_bytes + addr_int.to_bytes(8, "big") + tail_u32.to_bytes(4, "big")


def _addr_int(stream_id: int, bucket_id: int, chunk_idx: int) -> int:
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()


class TestMapping:
    def test_basic_decode(self) -> None:
        addr_int = _addr_int(132, 330, 17)
        data = _record_bytes(
            type_value=0,
            inherit=False,
            file_chunk_idx=5,
            addr_int=addr_int,
            tail_u32=(3 << 16) | 7,  # map_num=3, repeat=7
        )
        entry = parse_chunk_map_record(data)

        assert entry.kind is ChunkMapKind.MAPPING
        assert entry.is_inherit is False
        assert entry.file_offset == 5 * 4096
        assert entry.addr == ChunkAddress(StreamId(132), BucketId(330), ChunkIdx(17))
        assert entry.map_num == 3
        assert entry.repeat == 7

    def test_length_formula(self) -> None:
        data = _record_bytes(type_value=0, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=(10 << 16) | 4)
        entry = parse_chunk_map_record(data)
        assert entry.length == 10 * (1 + 4) * 4096
        assert entry.end_offset == entry.file_offset + entry.length

    def test_zero_repeat_means_run_once(self) -> None:
        data = _record_bytes(type_value=0, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=(5 << 16) | 0)
        entry = parse_chunk_map_record(data)
        assert entry.length == 5 * 4096

    def test_inherit_bit_is_parsed_but_does_not_change_addressing(self) -> None:
        addr_int = _addr_int(1, 1, 1)
        with_inherit = parse_chunk_map_record(
            _record_bytes(type_value=0, inherit=True, file_chunk_idx=0, addr_int=addr_int, tail_u32=(1 << 16) | 0)
        )
        without_inherit = parse_chunk_map_record(
            _record_bytes(type_value=0, inherit=False, file_chunk_idx=0, addr_int=addr_int, tail_u32=(1 << 16) | 0)
        )
        assert with_inherit.is_inherit is True
        assert without_inherit.is_inherit is False
        assert with_inherit.addr == without_inherit.addr
        assert with_inherit.length == without_inherit.length

    def test_large_file_chunk_idx(self) -> None:
        # 7-byte field: exercise a value well past 32 bits to catch any
        # accidental truncation to a narrower int type.
        big_idx = (1 << 40) + 12345
        data = _record_bytes(type_value=0, inherit=False, file_chunk_idx=big_idx, addr_int=0, tail_u32=1 << 16)
        entry = parse_chunk_map_record(data)
        assert entry.file_offset == big_idx * 4096

    def test_address_with_chunk_idx_past_bucket_capacity_is_not_validated_here(self) -> None:
        # chunk_idx field (low 16 bits of the address word) at 8192 is out
        # of any real bucket's capacity, but ``parse_chunk_map_record()``
        # trusts it, same as ``ChunkAddress.from_int()`` -- see
        # ``ChunkAddress``'s own docstring for why range-checking here is
        # redundant with what already happens downstream / in ``verify``.
        bad_addr_int = (1 << 56) | (0 << 16) | 8192
        data = _record_bytes(type_value=0, inherit=False, file_chunk_idx=0, addr_int=bad_addr_int, tail_u32=1 << 16)
        entry = parse_chunk_map_record(data)
        assert entry.addr is not None
        assert entry.addr.chunk_idx == 8192


class TestZero:
    def test_basic_decode(self) -> None:
        data = _record_bytes(type_value=1, inherit=False, file_chunk_idx=8, addr_int=0, tail_u32=42)
        entry = parse_chunk_map_record(data)

        assert entry.kind is ChunkMapKind.ZERO
        assert entry.file_offset == 8 * 4096
        assert entry.addr is None
        assert entry.map_num == 42
        assert entry.repeat == 0
        assert entry.length == 42 * 4096

    def test_32_bit_count_not_split_into_map_num_and_repeat(self) -> None:
        huge = (1 << 20) + 3  # would overflow a 16-bit map_num field if mis-split
        data = _record_bytes(type_value=1, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=huge)
        entry = parse_chunk_map_record(data)
        assert entry.map_num == huge
        assert entry.length == huge * 4096

    def test_addr_bytes_present_but_ignored(self) -> None:
        data = _record_bytes(type_value=1, inherit=False, file_chunk_idx=0, addr_int=0xDEADBEEF, tail_u32=1)
        entry = parse_chunk_map_record(data)
        assert entry.addr is None

    def test_inherit_bit_on_zero_record(self) -> None:
        data = _record_bytes(type_value=1, inherit=True, file_chunk_idx=0, addr_int=0, tail_u32=1)
        entry = parse_chunk_map_record(data)
        assert entry.is_inherit is True
        assert entry.kind is ChunkMapKind.ZERO


class TestValidation:
    def test_unknown_type_raises_data_corrupt(self) -> None:
        data = _record_bytes(type_value=7, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=0)
        with pytest.raises(DataCorruptError):
            parse_chunk_map_record(data)

    def test_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError):
            parse_chunk_map_record(b"\x00" * 19)

    def test_extra_trailing_bytes_are_ignored(self) -> None:
        # ``entries()`` reads records back-to-back out of a larger buffer —
        # ``parse_chunk_map_record`` must only ever look at its own 20 bytes.
        data = _record_bytes(type_value=1, inherit=False, file_chunk_idx=0, addr_int=0, tail_u32=1) + b"\xff" * 20
        entry = parse_chunk_map_record(data)
        assert entry.kind is ChunkMapKind.ZERO
