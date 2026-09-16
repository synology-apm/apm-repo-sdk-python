"""Unit tests for ``synology_apm_repo.sdk.format.composition``."""

from __future__ import annotations

import struct
import zlib

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format import composition
from synology_apm_repo.sdk.format.composition import (
    CompositionStatus,
    chunk_map_array_offset,
    parse_composition_header,
    parse_record_head,
    record_total_length,
    should_thread_chunk_map_crc,
    verify_chunk_map_crc,
)
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE


def _build_composition_header(*, major: int = 1, minor: int = 1, sub_file_size: int = SUB_FILE_SIZE) -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = struct.pack(">I", sub_file_size)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _build_record_head(
    *,
    status: int = 0,
    map_num: int = 37,
    map_crc: int = 0xC9D61ADB,
    mode: int = 0x0001,
    attr_leng: int = 60,
    attr_crc: int = 0x3487C5DE,
) -> bytes:
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[2:4] = status.to_bytes(2, "big")
    head[4:6] = (0).to_bytes(2, "big")  # hotID, always 0
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = mode.to_bytes(2, "big")
    head[20:24] = attr_leng.to_bytes(4, "big")
    head[24:28] = attr_crc.to_bytes(4, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(head)


class TestCompositionHeader:
    def test_valid_header(self) -> None:
        data = _build_composition_header()
        header = parse_composition_header(data)
        assert header.major == 1
        assert header.minor == 1

    def test_bad_magic_raises(self) -> None:
        data = bytearray(_build_composition_header())
        data[0:4] = b"XXXX"
        with pytest.raises(DataCorruptError):
            parse_composition_header(bytes(data))

    def test_unsupported_major_raises(self) -> None:
        data = _build_composition_header(major=0)  # CompMajor::Basic, obsolete
        with pytest.raises(UnsupportedVersionError):
            parse_composition_header(data)

    def test_wrong_sub_file_size_raises_data_corrupt(self) -> None:
        data = _build_composition_header(sub_file_size=1234)
        with pytest.raises(DataCorruptError):
            parse_composition_header(data)


class TestRecordHead:
    def test_matches_real_sample_values(self) -> None:
        # exact field values from apv-sample-1's Composition/132/0.com/c0.8.
        data = _build_record_head()
        record = parse_record_head(data)

        assert record.status is CompositionStatus.COMPLETE
        assert record.map_num == 37
        assert record.map_crc == 0xC9D61ADB
        assert record.mode == 0x0001
        assert record.attr_leng == 60
        assert record.attr_crc == 0x3487C5DE
        assert record.has_redundancy is True

    def test_interrupted_status_is_decoded_not_rejected(self) -> None:
        data = _build_record_head(status=1)
        record = parse_record_head(data)
        assert record.status is CompositionStatus.INTERRUPTED

    def test_bad_magic_raises(self) -> None:
        data = bytearray(_build_record_head())
        data[0:2] = b"XX"
        with pytest.raises(DataCorruptError):
            parse_record_head(bytes(data))

    def test_bad_head_crc_raises(self) -> None:
        data = bytearray(_build_record_head())
        data[10] ^= 0xFF  # corrupt a byte inside the mapNum field
        with pytest.raises(DataCorruptError):
            parse_record_head(bytes(data))

    def test_unknown_status_raises_data_corrupt(self) -> None:
        data = _build_record_head(status=99)
        with pytest.raises(DataCorruptError):
            parse_record_head(data)

    def test_missing_redundancy_bit_raises_unsupported_version(self) -> None:
        data = _build_record_head(mode=0x0000)
        with pytest.raises(UnsupportedVersionError):
            parse_record_head(data)

    def test_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError):
            parse_record_head(b"\x00" * 31)


class TestRecordTotalLength:
    def test_matches_real_sample_next_record_offset(self) -> None:
        # apv-sample-1: headOff=64, mapNum=37, attrLeng=60 -> next
        # record's "Mu" magic sits at exactly 64 + 1592 = 1656.
        assert record_total_length(map_num=37, attr_leng=60) == 1592

    def test_zero_map_num_and_attr(self) -> None:
        # 32 (head) + 0 (map array) + 0 (attr) + redundancy_size(0, 8192)=16
        assert record_total_length(map_num=0, attr_leng=0) == 32 + 16

    def test_scales_with_map_num(self) -> None:
        # 32 (head) + 200 (map array) + 0 (attr) + redundancy_size(200, 8192)=220
        assert record_total_length(map_num=10, attr_leng=0) == 32 + 200 + 220
        # 32 (head) + 20000 (map array) + 0 (attr) + redundancy_size(20000, 8192)=16412
        assert record_total_length(map_num=1000, attr_leng=0) == 32 + 20000 + 16412


class TestChunkMapArrayOffset:
    def test_immediately_after_record_head(self) -> None:
        assert chunk_map_array_offset(64) == 64 + 32
        assert chunk_map_array_offset(0) == 32


class TestVerifyChunkMapCrc:
    def test_matching_crc_does_not_raise(self) -> None:
        data = b"some chunk map bytes" * 3
        crc = zlib.crc32(data) & 0xFFFFFFFF
        verify_chunk_map_crc(data, crc)  # should not raise

    def test_mismatched_crc_raises_data_corrupt(self) -> None:
        data = b"some chunk map bytes"
        with pytest.raises(DataCorruptError):
            verify_chunk_map_crc(data, 0)


class TestShouldThreadChunkMapCrc:
    def test_just_under_the_threshold_is_not_worth_threading(self) -> None:
        assert should_thread_chunk_map_crc(b"\x00" * (composition._CRC_THREAD_HOP_MIN_BYTES - 1)) is False

    def test_exactly_at_the_threshold_is_worth_threading(self) -> None:
        assert should_thread_chunk_map_crc(b"\x00" * composition._CRC_THREAD_HOP_MIN_BYTES) is True

    def test_well_past_the_threshold_is_worth_threading(self) -> None:
        assert should_thread_chunk_map_crc(b"\x00" * (composition._CRC_THREAD_HOP_MIN_BYTES * 4)) is True

    def test_a_small_real_map_array_is_not_worth_threading(self) -> None:
        # A handful of ChunkMapRecord entries (20 bytes each) is the
        # common case -- well under the threshold.
        assert should_thread_chunk_map_crc(b"\x00" * 200) is False
