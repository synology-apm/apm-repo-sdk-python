"""Unit tests for ``synology_apm_repo.sdk.format.bucket``.

SizeStore bytes come from ``support.format_builders.encode_size_store``, an
encoder written from FORMAT-SPEC.md: SizeStore independently of
``bucket.py``'s decoder, so encode/decode bugs can't share a blind spot.
"""

from __future__ import annotations

import array
import zlib

import pytest

from support.format_builders import encode_size_store
from support.repo_builders import filler_bucket_bytes
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format.bucket import (
    MODE_CHUNK_CRC,
    MODE_COMPRESS,
    MODE_VAULT_ENCRYPT,
    BucketFileHeader,
    BucketIndex,
    SizeStoreEntry,
    chunk_crc_store_region,
    chunk_size_store_tight_length,
    expected_bucket_size,
    parse_bucket_header,
    parse_chunk_crc_store,
    parse_size_store,
)
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import COMPRESS_RESERVED_LENG, RESERVED_LENG
from synology_apm_repo.sdk.format.redundancy import redundancy_size


def _index_of(entries: list[SizeStoreEntry]) -> BucketIndex:
    return BucketIndex.of(
        array.array("B", [entry.compress_type.value for entry in entries]),
        array.array("H", [entry.stored_len for entry in entries]),
        data_start=COMPRESS_RESERVED_LENG,
    )


_MIXED_ENTRIES: list[tuple[CompressType, int]] = [
    (CompressType.NONE, 0),
    (CompressType.LZ4, 50),
    (CompressType.ZSTD, 80),
    (CompressType.COMPACTED, 0),
    (CompressType.LZ4, 4000),
]


class TestParseBucketHeader:
    def test_valid_header(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        assert header.major == 3
        assert header.minor == 0
        assert header.mode == MODE_COMPRESS | MODE_CHUNK_CRC
        assert header.chunk_num == 5
        assert header.is_compressed is True
        assert header.is_vault_encrypted is False

    def test_vault_encrypted_mode(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES, mode=MODE_COMPRESS | MODE_CHUNK_CRC | MODE_VAULT_ENCRYPT)
        header = parse_bucket_header(data)
        assert header.is_vault_encrypted is True

    def test_bad_magic_raises(self) -> None:
        data = bytearray(filler_bucket_bytes(_MIXED_ENTRIES))
        data[0:4] = b"XXXX"
        with pytest.raises(DataCorruptError, match="bad magic"):
            parse_bucket_header(bytes(data))

    def test_major_too_new_raises(self) -> None:
        data = bytearray(filler_bucket_bytes(_MIXED_ENTRIES))
        data[4:6] = (4).to_bytes(2, "big")
        header_crc = zlib.crc32(bytes(data[:60])) & 0xFFFFFFFF
        data[60:64] = header_crc.to_bytes(4, "big")

        with pytest.raises(UnsupportedVersionError, match="major version"):
            parse_bucket_header(bytes(data))


class TestParseSizeStore:
    def test_round_trip(self) -> None:
        # Covers: compression is per-chunk, not repository-wide;
        # ``CompressType.NONE``'s size==0 means a full 4096 bytes, not
        # "empty"; ``stored_len`` != ``effective_len`` for
        # ``CompressType.NONE``/``CompressType.COMPACTED``.
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = data[64:16384]
        entries = parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc)

        assert len(entries) == 5
        assert entries[0].compress_type is CompressType.NONE
        assert entries[0].effective_len == 4096
        assert entries[1].compress_type is CompressType.LZ4
        assert entries[1].stored_len == 50
        assert entries[1].effective_len == 50
        assert entries[2].compress_type is CompressType.ZSTD
        assert entries[2].effective_len == 80
        assert entries[3].compress_type is CompressType.COMPACTED
        assert entries[3].effective_len == 0
        assert entries[4].effective_len == 4000

    def test_crc_mismatch_raises_data_corrupt(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = data[64:16384]
        with pytest.raises(DataCorruptError, match="SizeStore CRC mismatch"):
            parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc ^ 1)

    def test_crc_not_checked_when_omitted(self) -> None:
        # The rewritten bytes no longer match the header CRC; without
        # ``verify_crc`` they still decode, as rewritten.
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = bytearray(data[64:16384])
        tight_len = chunk_size_store_tight_length(header.chunk_num)
        corrupted_first_entry = encode_size_store([(CompressType.ZSTD.value, 123)] + [(0, 0)] * 4)
        sizestore_region[:tight_len] = corrupted_first_entry[:tight_len]

        entries = parse_size_store(bytes(sizestore_region), header.chunk_num)  # no ``verify_crc``

        assert len(entries) == 5
        assert entries[0].compress_type is CompressType.ZSTD
        assert entries[0].stored_len == 123

    @pytest.mark.parametrize(
        ("count", "step"),
        [
            # One full 8-record group *and* a 5-record remainder group in
            # the same decode call.
            pytest.param(13, 37, id="not_a_multiple_of_the_group_size"),
            # Byte offsets and bit shifts cycle through every
            # (bitOff % 8) phase multiple times.
            pytest.param(200, 7, id="bit_packing"),
        ],
    )
    def test_many_chunks_round_trip(self, count: int, step: int) -> None:
        entries = [(CompressType.LZ4.value, (i * step) % 4096) for i in range(count)]
        tight = encode_size_store(entries)
        decoded = parse_size_store(tight, count)
        assert [(e.compress_type.value, e.stored_len) for e in decoded] == entries

    def test_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError, match="SizeStore data too short"):
            parse_size_store(b"\x00" * 2, 100)

    def test_unknown_compress_type_raises_data_corrupt(self) -> None:
        # CompressType skips the value 3.
        tight = encode_size_store([(3, 0)])
        with pytest.raises(DataCorruptError, match="unknown CompressType"):
            parse_size_store(tight, 1)

    @pytest.mark.parametrize("chunk_num", [8191, 8192])
    def test_a_full_bucket_round_trips(self, chunk_num: int) -> None:
        """``test_format_bucket_properties.py`` covers small counts; this is
        the largest, ending in a partial and a whole group."""
        type_values = [member.value for member in CompressType]
        entries = [(type_values[i % len(type_values)], (i * 2731) % 4096) for i in range(chunk_num)]
        entries[-1] = (type_values[-1], 0xFFF)  # the last record's 12-bit length all ones
        decoded = parse_size_store(encode_size_store(entries), chunk_num)
        assert [(e.compress_type.value, e.stored_len) for e in decoded] == entries

    def test_unknown_compress_type_names_the_first_offending_chunk(self) -> None:
        entries = [(CompressType.ZSTD.value, 10)] * 20
        entries[13] = (3, 0)
        entries[17] = (7, 0)
        with pytest.raises(DataCorruptError, match="unknown CompressType 3 at chunk 13"):
            parse_size_store(encode_size_store(entries), len(entries))


class TestBucketIndex:
    def test_compressed_layout_offsets_are_contiguous(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        index = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        locators = [index.locator(i) for i in range(len(index))]

        expected_lengths = [4096, 50, 80, 0, 4000]
        assert [loc.length for loc in locators] == expected_lengths
        assert locators[0].offset == COMPRESS_RESERVED_LENG
        assert locators[1].offset == COMPRESS_RESERVED_LENG + 4096
        assert locators[2].offset == COMPRESS_RESERVED_LENG + 4096 + 50
        assert locators[3].offset == COMPRESS_RESERVED_LENG + 4096 + 50 + 80
        assert locators[4].offset == locators[3].offset  # COMPACTED occupies zero space
        assert list(index.offsets) == [loc.offset for loc in locators]
        assert list(index.effective_lens) == expected_lengths
        assert list(index.compress_types) == [ctype.value for ctype, _ in _MIXED_ENTRIES]

    def test_legacy_uncompressed_layout(self) -> None:
        index = BucketIndex.uncompressed(3)

        assert list(index) == [SizeStoreEntry(CompressType.NONE, 0)] * 3
        assert list(index.offsets) == [RESERVED_LENG, RESERVED_LENG + 4096, RESERVED_LENG + 8192]
        assert list(index.effective_lens) == [4096, 4096, 4096]

    def test_slicing_matches_plain_indexing(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        index = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        assert index[1:3] == [index[1], index[2]]

    def test_counts_and_totals(self) -> None:
        index = _index_of([SizeStoreEntry(CompressType.ZSTD, 100), SizeStoreEntry(CompressType.COMPACTED, 0)])
        assert (index.total_effective_len, index.non_empty_count) == (100, 1)


class TestExpectedBucketSize:
    def test_matches_constructed_file_size(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        assert expected_bucket_size(header, entries) == len(data)

    def test_matches_for_full_size_bucket(self) -> None:
        # A full 8192-chunk bucket: the ``tight_len == 15360`` boundary
        # (``SIZE_STORE_REC_BIT_NUM * BUCKET_MAX_CHUNK_NUM >> 3``).
        entries_spec = [(CompressType.LZ4, 500)] * 8192
        data = filler_bucket_bytes(entries_spec)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        assert expected_bucket_size(header, entries) == len(data)

    def test_raises_for_uncompressed_layout(self) -> None:
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=0, chunk_size_crc=0, crc_of_chunk_crc=0)
        with pytest.raises(ValueError, match="only applies to the compressed layout"):
            expected_bucket_size(header, BucketIndex.uncompressed(0))


class TestChunkCrcStore:
    def test_region_offset_and_length_match_the_constructed_trailer(self) -> None:
        # _MIXED_ENTRIES has 5 chunks, one COMPACTED (empty) -> 4 non-empty
        # trailer entries.
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        offset, length = chunk_crc_store_region(header, entries)

        assert length == 4 * 4  # CHUNK_CRC_SIZE(4) * non_empty(4)
        # ``filler_bucket_bytes`` ends the file with the SizeStore's
        # coverage-256 Redundancy blob right after this region.
        tight_len = chunk_size_store_tight_length(header.chunk_num)
        trailer_len = length + redundancy_size(tight_len, 256)
        assert offset + trailer_len == len(data)

    def test_region_raises_for_uncompressed_layout(self) -> None:
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=0, chunk_size_crc=0, crc_of_chunk_crc=0)
        with pytest.raises(ValueError, match="only applies to the compressed layout"):
            chunk_crc_store_region(header, BucketIndex.uncompressed(0))

    def test_parse_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError, match="ChunkCrcStore trailer too short"):
            parse_chunk_crc_store(b"\x00\x00\x00", non_empty=1)

    def test_parse_round_trips_without_crc_check(self) -> None:
        trailer = (1234).to_bytes(4, "big") + (5678).to_bytes(4, "big")
        assert parse_chunk_crc_store(trailer, non_empty=2) == (1234, 5678)

    def test_parse_empty_when_non_empty_is_zero(self) -> None:
        assert parse_chunk_crc_store(b"", non_empty=0) == ()

    def test_positions_count_the_non_empty_chunks_below_each_chunk(self) -> None:
        data = filler_bucket_bytes(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        index = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        positions = index.crc_store_positions()
        assert len(positions) == len(index)
        for chunk_idx in range(len(index)):
            assert positions[chunk_idx] == sum(1 for entry in index[:chunk_idx] if entry.effective_len > 0)
        # chunk 4 sits after the non-empty chunks 0-2; COMPACTED chunk 3 has no trailer entry.
        assert positions[4] == 3

    def test_positions_for_a_full_size_bucket(self) -> None:
        """With no COMPACTED entry, every position is its own chunk_idx."""
        entries_spec = [(CompressType.LZ4, 500)] * 8192
        data = filler_bucket_bytes(entries_spec)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        positions = entries.crc_store_positions()

        assert list(positions) == list(range(8192))
