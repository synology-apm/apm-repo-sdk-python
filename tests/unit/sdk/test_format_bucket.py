"""Unit tests for ``synology_apm_repo.sdk.format.bucket``.

The synthetic SizeStore fixture is *encoded* independently of the module's
own decoder (writing bits with an OR-into-window loop, mirroring the
write-side formula in FORMAT-SPEC.md: SizeStore, rather than calling anything
in ``bucket.py``) so encode/decode bugs can't share a blind spot.
"""

from __future__ import annotations

import os
import struct
import zlib

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format.bucket import (
    MODE_CHUNK_CRC,
    MODE_COMPRESS,
    MODE_VAULT_ENCRYPT,
    BucketFileHeader,
    SizeStoreEntry,
    chunk_crc_store_index,
    chunk_crc_store_positions,
    chunk_crc_store_region,
    chunk_locators,
    chunk_size_store_tight_length,
    expected_bucket_size,
    parse_bucket_header,
    parse_chunk_crc_store,
    parse_size_store,
    raw_chunk_arrays,
)
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import COMPRESS_RESERVED_LENG, RESERVED_LENG
from synology_apm_repo.sdk.format.redundancy import redundancy_size


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    """Independent write-side encoder for test fixtures.

    Args:
        entries: ``(type_value, size)`` pairs, one per record.
    """
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)  # slack for the last record's OR window
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _build_bucket(entries: list[tuple[CompressType, int]], *, mode: int = MODE_COMPRESS | MODE_CHUNK_CRC) -> bytes:
    """Build a complete, self-consistent synthetic ``.buk`` file.

    Trailer bytes are filler — only their length matters for these tests.

    Args:
        entries: ``(CompressType, stored_len)`` pairs, one per chunk.
    """
    chunk_num = len(entries)
    tight = _encode_size_store([(e[0].value, e[1]) for e in entries])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")  # major = MAJOR_VAULT
    header[6:8] = (0).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", mode)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (16320 - len(tight))

    chunk_data = b""
    for ctype, stored_len in entries:
        eff = 4096 if ctype is CompressType.NONE else (0 if ctype is CompressType.COMPACTED else stored_len)
        chunk_data += os.urandom(eff)

    non_empty = sum(1 for ctype, _ in entries if ctype is not CompressType.COMPACTED)
    trailer_len = 4 * non_empty + redundancy_size(chunk_size_store_tight_length(chunk_num), 256)
    trailer = os.urandom(trailer_len)

    return bytes(header) + sizestore_region + chunk_data + trailer


_MIXED_ENTRIES: list[tuple[CompressType, int]] = [
    (CompressType.NONE, 0),
    (CompressType.LZ4, 50),
    (CompressType.ZSTD, 80),
    (CompressType.COMPACTED, 0),
    (CompressType.LZ4, 4000),
]


class TestParseBucketHeader:
    def test_valid_header(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        assert header.major == 3
        assert header.minor == 0
        assert header.mode == MODE_COMPRESS | MODE_CHUNK_CRC
        assert header.chunk_num == 5
        assert header.is_compressed is True
        assert header.is_vault_encrypted is False

    def test_vault_encrypted_mode(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES, mode=MODE_COMPRESS | MODE_CHUNK_CRC | MODE_VAULT_ENCRYPT)
        header = parse_bucket_header(data)
        assert header.is_vault_encrypted is True

    def test_bad_magic_raises(self) -> None:
        data = bytearray(_build_bucket(_MIXED_ENTRIES))
        data[0:4] = b"XXXX"
        with pytest.raises(DataCorruptError):
            parse_bucket_header(bytes(data))

    def test_major_too_new_raises(self) -> None:
        data = bytearray(_build_bucket(_MIXED_ENTRIES))
        data[4:6] = (4).to_bytes(2, "big")
        header_crc = zlib.crc32(bytes(data[:60])) & 0xFFFFFFFF
        data[60:64] = header_crc.to_bytes(4, "big")

        with pytest.raises(UnsupportedVersionError):
            parse_bucket_header(bytes(data))


class TestParseSizeStore:
    def test_round_trip(self) -> None:
        # traps #2 (compression is per-chunk, not repository-wide),
        # #5 (``CompressType.NONE``'s size==0 means a full 4096 bytes, not
        # "empty") and #29 (``stored_len`` != ``effective_len`` for
        # ``CompressType.NONE``/``CompressType.COMPACTED``).
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = data[64:16384]
        entries = parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc)

        assert len(entries) == 5
        assert entries[0].compress_type is CompressType.NONE
        assert entries[0].effective_len == 4096  # stored_len=0 is meaningless for NONE
        assert entries[1].compress_type is CompressType.LZ4
        assert entries[1].stored_len == 50
        assert entries[1].effective_len == 50
        assert entries[2].compress_type is CompressType.ZSTD
        assert entries[2].effective_len == 80
        assert entries[3].compress_type is CompressType.COMPACTED
        assert entries[3].effective_len == 0
        assert entries[4].effective_len == 4000

    def test_crc_mismatch_raises_data_corrupt(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = data[64:16384]
        with pytest.raises(DataCorruptError):
            parse_size_store(sizestore_region, header.chunk_num, verify_crc=header.chunk_size_crc ^ 1)

    def test_crc_not_checked_when_omitted(self) -> None:
        # Corrupt the tightly-packed SizeStore bytes so the *real* CRC
        # would no longer match, then decode without ``verify_crc`` and
        # prove decoding still runs off the (now-different) bytes rather
        # than silently reusing some cached/earlier-computed result.
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        sizestore_region = bytearray(data[64:16384])
        tight_len = chunk_size_store_tight_length(header.chunk_num)
        corrupted_first_entry = _encode_size_store([(CompressType.ZSTD.value, 123)] + [(0, 0)] * 4)
        sizestore_region[:tight_len] = corrupted_first_entry[:tight_len]

        entries = parse_size_store(bytes(sizestore_region), header.chunk_num)  # no ``verify_crc``

        assert len(entries) == 5
        assert entries[0].compress_type is CompressType.ZSTD
        assert entries[0].stored_len == 123

    def test_many_chunks_not_a_multiple_of_the_group_size(self) -> None:
        # 13 chunks means ``parse_size_store``'s decode loop runs one full
        # 8-record group *and* a 5-record remainder group in the same
        # call — both phases exercised and checked together.
        entries = [(CompressType.LZ4.value, (i * 37) % 4096) for i in range(13)]
        tight = _encode_size_store(entries)
        decoded = parse_size_store(tight, 13)
        assert [(e.compress_type.value, e.stored_len) for e in decoded] == entries

    def test_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError):
            parse_size_store(b"\x00" * 2, 100)

    def test_unknown_compress_type_raises_data_corrupt(self) -> None:
        # type value 3 is deliberately never used on the wire
        tight = _encode_size_store([(3, 0)])
        with pytest.raises(DataCorruptError):
            parse_size_store(tight, 1)

    def test_many_chunks_bit_packing(self) -> None:
        # exercise bit-window crossing across many records, not just a
        # handful — 200 chunks means byte offsets and bit shifts cycle
        # through every possible (bitOff % 8) phase multiple times.
        entries = [(CompressType.LZ4.value, (i * 7) % 4096) for i in range(200)]
        tight = _encode_size_store(entries)
        decoded = parse_size_store(tight, 200)
        assert [(e.compress_type.value, e.stored_len) for e in decoded] == entries


class TestChunkLocators:
    def test_compressed_layout_offsets_are_contiguous(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        locators = chunk_locators(header, entries)

        expected_lengths = [4096, 50, 80, 0, 4000]
        assert [loc.length for loc in locators] == expected_lengths
        assert locators[0].offset == COMPRESS_RESERVED_LENG
        assert locators[1].offset == COMPRESS_RESERVED_LENG + 4096
        assert locators[2].offset == COMPRESS_RESERVED_LENG + 4096 + 50
        assert locators[3].offset == COMPRESS_RESERVED_LENG + 4096 + 50 + 80
        assert locators[4].offset == locators[3].offset  # COMPACTED occupies zero space

    def test_legacy_uncompressed_layout(self) -> None:
        # this layout only consults ``len(entries)``, never their contents —
        # any 3 ``SizeStoreEntry`` values will do.
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=3, chunk_size_crc=0, crc_of_chunk_crc=0)
        entries = [SizeStoreEntry(CompressType.NONE, 0) for _ in range(3)]

        locators = chunk_locators(header, entries)

        assert [loc.offset for loc in locators] == [RESERVED_LENG, RESERVED_LENG + 4096, RESERVED_LENG + 8192]
        assert all(loc.length == 4096 for loc in locators)

    def test_compressed_layout_with_a_plain_list_not_the_array_fast_path(self) -> None:
        """Falls back correctly when ``entries``/``locators`` are a plain
        list, not ``parse_size_store()``'s array fast path.

        Every real ``BucketReader.open()`` call gets ``entries`` from
        ``parse_size_store()`` (a ``_SizeStoreArray``, the fast path
        above); this covers the fallback for a caller passing a plain
        list of already-decoded ``SizeStoreEntry`` values instead, still
        against a real compressed-layout header.
        """
        header = BucketFileHeader(
            major=3, minor=0, mode=MODE_COMPRESS, chunk_num=2, chunk_size_crc=0, crc_of_chunk_crc=0
        )
        entries = [SizeStoreEntry(CompressType.ZSTD, 100), SizeStoreEntry(CompressType.ZSTD, 200)]

        locators = chunk_locators(header, entries)

        assert [loc.length for loc in locators] == [100, 200]
        assert locators[0].offset == COMPRESS_RESERVED_LENG
        assert locators[1].offset == COMPRESS_RESERVED_LENG + 100

    def test_array_fast_path_slicing_matches_plain_indexing(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        locators = chunk_locators(header, entries)  # a ``_ChunkLocatorArray``, since entries is a ``_SizeStoreArray``

        assert entries[1:3] == [entries[1], entries[2]]
        assert locators[1:3] == [locators[1], locators[2]]


class TestRawChunkArrays:
    def test_compressed_layout_is_zero_copy_from_the_array_fast_path(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        locators = chunk_locators(header, entries)

        compress_types, offsets, lengths = raw_chunk_arrays(header, entries, locators)

        assert list(compress_types) == [e.value for e, _ in _MIXED_ENTRIES]
        assert list(offsets) == [loc.offset for loc in locators]
        assert list(lengths) == [loc.length for loc in locators]
        # Zero-copy: the exact same underlying arrays ``parse_size_store()``/
        # ``chunk_locators()`` already built, not fresh copies.
        assert compress_types is entries.raw_compress_types()
        assert offsets is locators.raw_offsets()  # type: ignore[attr-defined]
        assert lengths is locators.raw_lengths()  # type: ignore[attr-defined]

    def test_uncompressed_layout_is_a_formulaic_fill(self) -> None:
        # header.is_compressed is False, so ``raw_chunk_arrays()`` never
        # reads entries/locators' contents at all -- only ``len(entries)``
        # matters.
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=3, chunk_size_crc=0, crc_of_chunk_crc=0)
        entries = [SizeStoreEntry(CompressType.LZ4, 999) for _ in range(3)]
        locators = chunk_locators(header, entries)

        compress_types, offsets, lengths = raw_chunk_arrays(header, entries, locators)

        assert list(compress_types) == [CompressType.NONE.value] * 3
        assert list(offsets) == [RESERVED_LENG, RESERVED_LENG + 4096, RESERVED_LENG + 8192]
        assert list(lengths) == [4096, 4096, 4096]

    def test_compressed_layout_with_a_plain_list_raises_assertion_error(self) -> None:
        # A compressed-layout header always pairs with the array fast
        # path's own entries/locators (``parse_size_store()``/
        # ``chunk_locators()``) -- ``raw_chunk_arrays()`` asserts that
        # combination rather than silently reading mismatched arrays for
        # any other one.
        header = BucketFileHeader(
            major=3, minor=0, mode=MODE_COMPRESS, chunk_num=2, chunk_size_crc=0, crc_of_chunk_crc=0
        )
        entries = [SizeStoreEntry(CompressType.ZSTD, 100), SizeStoreEntry(CompressType.ZSTD, 200)]
        locators = chunk_locators(header, entries)  # a plain list, not the array fast path

        with pytest.raises(AssertionError):
            raw_chunk_arrays(header, entries, locators)


class TestExpectedBucketSize:
    def test_matches_constructed_file_size(self) -> None:
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        assert expected_bucket_size(header, entries) == len(data)

    def test_matches_for_full_size_bucket(self) -> None:
        # a "full" 8192-chunk bucket, all uniform LZ4 chunks — exercises the
        # ``tight_len == 15360`` boundary (``SIZE_STORE_REC_BIT_NUM *
        # BUCKET_MAX_CHUNK_NUM >> 3``) from the spec's own worked example.
        entries_spec = [(CompressType.LZ4, 500)] * 8192
        data = _build_bucket(entries_spec)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        assert expected_bucket_size(header, entries) == len(data)

    def test_raises_for_uncompressed_layout(self) -> None:
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=0, chunk_size_crc=0, crc_of_chunk_crc=0)
        with pytest.raises(ValueError):
            expected_bucket_size(header, [])

    def test_matches_for_a_plain_list_not_the_array_fast_path(self) -> None:
        """Cross-checks ``expected_bucket_size()`` against a plain list,
        not just ``parse_size_store()``'s array fast path.

        Computes the same real value either way — once via the array
        fast path (``parse_size_store()``'s own ``_SizeStoreArray``),
        once via a plain list of the identical entries — for a direct,
        same-input comparison between the two branches.
        """
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        array_entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        plain_entries = list(array_entries)

        assert expected_bucket_size(header, plain_entries) == expected_bucket_size(header, array_entries)


class TestChunkCrcStore:
    def test_region_offset_and_length_match_the_constructed_trailer(self) -> None:
        # _MIXED_ENTRIES has 5 chunks, one COMPACTED (empty) -> 4 non-empty
        # trailer entries.
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        offset, length = chunk_crc_store_region(header, entries)

        assert length == 4 * 4  # CHUNK_CRC_SIZE(4) * non_empty(4)
        tight_len = chunk_size_store_tight_length(header.chunk_num)
        trailer_len = length + redundancy_size(tight_len, 256)
        assert offset + trailer_len == len(data)  # the region ends exactly at EOF

    def test_region_raises_for_uncompressed_layout(self) -> None:
        header = BucketFileHeader(major=1, minor=0, mode=0, chunk_num=0, chunk_size_crc=0, crc_of_chunk_crc=0)
        with pytest.raises(ValueError):
            chunk_crc_store_region(header, [])

    def test_index_for_a_plain_list_not_the_array_fast_path(self) -> None:
        """Cross-checks ``chunk_crc_store_index()`` against a plain list,
        not just ``parse_size_store()``'s array fast path."""
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        array_entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        plain_entries = list(array_entries)

        # chunk 4 (the last, non-COMPACTED entry) sits after 3 earlier
        # non-empty chunks (0, 1, 2) plus the COMPACTED one at index 3,
        # which contributes no trailer entry of its own.
        assert chunk_crc_store_index(plain_entries, 4) == chunk_crc_store_index(array_entries, 4) == 3

    def test_parse_too_short_raises_format_error(self) -> None:
        with pytest.raises(FormatError):
            parse_chunk_crc_store(b"\x00\x00\x00", non_empty=1)

    def test_parse_round_trips_without_crc_check(self) -> None:
        trailer = (1234).to_bytes(4, "big") + (5678).to_bytes(4, "big")
        assert parse_chunk_crc_store(trailer, non_empty=2) == (1234, 5678)

    def test_parse_empty_when_non_empty_is_zero(self) -> None:
        assert parse_chunk_crc_store(b"", non_empty=0) == ()

    def test_positions_matches_index_for_every_chunk(self) -> None:
        """``chunk_crc_store_positions()`` (the O(n)-once batch form) must
        agree with ``chunk_crc_store_index()`` (the O(chunk_idx)-per-call
        single lookup) for every chunk, both array-fast-path and
        plain-list entries -- the correctness property a caller switching
        from the latter to the former in a hot loop depends on.
        """
        data = _build_bucket(_MIXED_ENTRIES)
        header = parse_bucket_header(data)
        array_entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)
        plain_entries = list(array_entries)

        for entries in (array_entries, plain_entries):
            positions = chunk_crc_store_positions(entries)
            assert len(positions) == len(entries)
            for chunk_idx in range(len(entries)):
                assert positions[chunk_idx] == chunk_crc_store_index(entries, chunk_idx)

    def test_positions_for_a_full_size_bucket(self) -> None:
        """The same property at a real bucket's max size (8192 chunks,
        no COMPACTED entries) — every position is just its own chunk_idx
        when nothing is empty."""
        entries_spec = [(CompressType.LZ4, 500)] * 8192
        data = _build_bucket(entries_spec)
        header = parse_bucket_header(data)
        entries = parse_size_store(data[64:16384], header.chunk_num, verify_crc=header.chunk_size_crc)

        positions = chunk_crc_store_positions(entries)

        assert list(positions) == list(range(8192))
