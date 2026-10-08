"""Bucket file (``.buk``) format (FORMAT-SPEC.md §4).

Pure ``bytes -> dataclass`` decode, zero I/O. The Dedup Layer's
``BucketReader`` fetches bytes at the offsets this module computes, then
hands ciphertext through ``format.crypto`` and ``compression``.

In practice ``mode`` is ``COMPRESS|CHUNK_CRC`` (``0x03``), plus
``VAULT_ENCRYPT`` (``0x83``) for an encrypted bucket, but readers test each
bit rather than assume it (FORMAT-SPEC.md: Header & mode bits).
"""

from __future__ import annotations

import array
import dataclasses
import itertools
import struct
import sys
from collections.abc import Sequence
from typing import NamedTuple, overload, override

from ..errors import DataCorruptError, FormatError
from .compression import CompressType
from .const import (
    CHUNK_CRC_SIZE,
    COMPRESS_RESERVED_LENG,
    FIXED_CHUNK_LENGTH,
    REDUNDANCY_COVERAGE_BUCKET,
    RESERVED_LENG,
    SIZE_STORE_REC_BIT_NUM,
)
from .headers import MAGIC, parse_index_header, verify_crc32
from .redundancy import redundancy_size

_SPEC = "FORMAT-SPEC.md §4"

MODE_COMPRESS = 0x01
MODE_CHUNK_CRC = 0x02
# 0x04-0x40 are reserved bits, unset in practice (FORMAT-SPEC.md: Header & mode bits).
MODE_BUCKET_PARITY = 0x04
MODE_ENCRYPT = 0x08  # the non-VaultKey DATA_KEY scheme
MODE_LOGIC_LOCALITY = 0x10
MODE_EXTENT_PARITY = 0x20
MODE_INPLACE_PARITY = 0x40
MODE_VAULT_ENCRYPT = 0x80

_MAJOR_VAULT = 3
"""Newest bucket major version, the one current writers use; reads accept
anything ``<= 3``."""

_OFF_MODE = 8  # mode/chunk_num/chunk_size_crc are contiguous uint32s at [8, 20)
_OFF_CRC_OF_CHUNK_CRC = 29  # not contiguous with the [8, 20) group above

COMPRESS_TYPE_BY_VALUE = {member.value: member for member in CompressType}
"""Plain-dict stand-in for ``CompressType(value)`` in the per-chunk hot
loops (up to 8192 calls per bucket), avoiding ``Enum.__call__`` overhead."""

_COMPRESS_TYPE_NONE_VALUE = CompressType.NONE.value
COMPRESS_TYPE_COMPACTED_VALUE = CompressType.COMPACTED.value
"""``CompressType.COMPACTED``'s raw value, for hot loops that compare raw
SizeStore values (``BucketIndex``, ``BucketReader``) without building
``CompressType`` members."""


@dataclasses.dataclass(frozen=True, slots=True)
class BucketFileHeader:
    """Parsed ``.buk`` header (FORMAT-SPEC.md: Header & mode bits)."""

    major: int
    minor: int
    mode: int
    chunk_num: int
    chunk_size_crc: int
    crc_of_chunk_crc: int
    """CRC32 over the whole ChunkCrcStore trailer (FORMAT-SPEC.md:
    ChunkCrcStore & Redundancy); not needed to read a chunk, checked when
    the trailer is loaded (``parse_chunk_crc_store``'s ``verify_crc``)."""

    @property
    def is_compressed(self) -> bool:
        return bool(self.mode & MODE_COMPRESS)

    @property
    def is_vault_encrypted(self) -> bool:
        """The only reliable signal that this bucket's chunks are encrypted;
        ``repo_info``'s ``encrypt_algorithm`` says nothing about an individual
        bucket (FORMAT-SPEC.md: Header & mode bits)."""
        return bool(self.mode & MODE_VAULT_ENCRYPT)


def parse_bucket_header(data: bytes) -> BucketFileHeader:
    """Parse a ``.buk`` file's 64-byte header from the start of ``data``.

    Raises:
        FormatError: ``data`` is shorter than 64 bytes.
        DataCorruptError: A magic or header-CRC mismatch.
        UnsupportedVersionError: ``major`` is above 3.
    """
    header = parse_index_header(data, expect_magic=MAGIC["bucket"], max_major=_MAJOR_VAULT, spec=_SPEC)
    mode, chunk_num, chunk_size_crc = struct.unpack(">III", data[_OFF_MODE : _OFF_MODE + 12])
    (crc_of_chunk_crc,) = struct.unpack(">I", data[_OFF_CRC_OF_CHUNK_CRC : _OFF_CRC_OF_CHUNK_CRC + 4])
    return BucketFileHeader(
        major=header.major,
        minor=header.minor,
        mode=mode,
        chunk_num=chunk_num,
        chunk_size_crc=chunk_size_crc,
        crc_of_chunk_crc=crc_of_chunk_crc,
    )


class SizeStoreEntry(NamedTuple):
    """One chunk's compression type and *stored* (compressed) length, as
    recorded in ``SizeStore`` (FORMAT-SPEC.md: SizeStore).

    A ``NamedTuple``, not a frozen dataclass: built up to 8192 times per
    bucket, where it is cheaper.
    """

    compress_type: CompressType
    stored_len: int
    """The raw 12-bit size field. Not the same as ``effective_len``."""

    @property
    def effective_len(self) -> int:
        """Actual bytes this chunk occupies in the data region. ``4096``
        for ``CompressType.NONE`` (``stored_len`` is meaningless there),
        ``0`` for ``CompressType.COMPACTED``, otherwise ``stored_len``
        verbatim."""
        if self.compress_type is CompressType.NONE:
            return FIXED_CHUNK_LENGTH
        if self.compress_type is CompressType.COMPACTED:
            return 0
        return self.stored_len


def chunk_size_store_tight_length(chunk_num: int) -> int:
    """``ceil(chunk_num * 15 / 8)``, the tightly-packed SizeStore length in
    bytes, before zero-padding to the fixed 16320-byte on-disk allocation."""
    return (chunk_num * SIZE_STORE_REC_BIT_NUM + 7) >> 3


_SIZE_STORE_GROUP_RECORDS = 8
"""8 consecutive 15-bit records pack into exactly 15 bytes (the assertion
below ties this to ``SIZE_STORE_REC_BIT_NUM == 15``)."""
_SIZE_STORE_GROUP_BYTES = 15
_SIZE_STORE_GROUP_BITS = _SIZE_STORE_GROUP_BYTES * 8
assert _SIZE_STORE_GROUP_RECORDS * SIZE_STORE_REC_BIT_NUM == _SIZE_STORE_GROUP_BITS, (
    "_SIZE_STORE_GROUP_RECORDS/_SIZE_STORE_GROUP_BYTES assume SIZE_STORE_REC_BIT_NUM == 15 exactly"
)


_KNOWN_COMPRESS_TYPE_BYTES = bytes(COMPRESS_TYPE_BY_VALUE)

# Per raw compress-type byte, for ``BucketIndex.of``: whether the stored
# length is also the effective one (all-ones), and the effective length's
# high byte when it isn't (a NONE chunk's 4096; a COMPACTED chunk's 0).
_KEEPS_STORED_LEN = bytes(
    0 if value in (_COMPRESS_TYPE_NONE_VALUE, COMPRESS_TYPE_COMPACTED_VALUE) else 0xFF for value in range(256)
)
_FIXED_LEN_HIGH_BYTE = bytes(
    FIXED_CHUNK_LENGTH >> 8 if value == _COMPRESS_TYPE_NONE_VALUE else 0 for value in range(256)
)
assert FIXED_CHUNK_LENGTH & 0xFF == 0

_FieldPlan = tuple[tuple[int, bytes], ...]


def _field_plan(start: int, width: int) -> _FieldPlan:
    """How to pull bits ``[start, start + width)`` (``width <= 8``, counted
    from a group's most significant bit) of every group into one byte each:
    ``(byte position in the group, translate table)`` parts, at most two,
    whose per-byte results OR together into the field's value."""
    byte_pos, bit_pos = divmod(start, 8)
    mask = (1 << width) - 1
    end = bit_pos + width
    if end <= 8:
        return ((byte_pos, bytes((value >> (8 - end)) & mask for value in range(256))),)
    return (
        (byte_pos, bytes((value << (end - 8)) & mask for value in range(256))),
        (byte_pos + 1, bytes(value >> (16 - end) for value in range(256))),
    )


# Each record: a 3-bit CompressType, then a 12-bit stored length, split
# into its high 4 and low 8 bits so every field fits one byte.
_COMPRESS_TYPE_PLANS = tuple(
    _field_plan(record * SIZE_STORE_REC_BIT_NUM, 3) for record in range(_SIZE_STORE_GROUP_RECORDS)
)
_STORED_LEN_HIGH_PLANS = tuple(
    _field_plan(record * SIZE_STORE_REC_BIT_NUM + 3, 4) for record in range(_SIZE_STORE_GROUP_RECORDS)
)
_STORED_LEN_LOW_PLANS = tuple(
    _field_plan(record * SIZE_STORE_REC_BIT_NUM + 7, 8) for record in range(_SIZE_STORE_GROUP_RECORDS)
)


def _native_u16(lows: bytes | bytearray, highs: bytes | bytearray) -> bytearray:
    """Each ``(lows[i], highs[i])`` pair as one ``"H"`` value's bytes, in the
    machine's own byte order (``array.frombytes`` input)."""
    native = bytearray(2 * len(lows))
    first, second = (lows, highs) if sys.byteorder == "little" else (highs, lows)
    native[0::2], native[1::2] = first, second
    return native


def _field_bytes(columns: Sequence[bytes], plan: _FieldPlan, group_count: int) -> bytes:
    """One field of every group, one byte each, per ``plan``."""
    parts = [columns[byte_pos].translate(table) for byte_pos, table in plan]
    if len(parts) == 1:
        return parts[0]
    # The parts hold disjoint bits of each field, so one big-integer OR is
    # a per-byte OR that rebuilds every field at once.
    return (int.from_bytes(parts[0]) | int.from_bytes(parts[1])).to_bytes(group_count)


def parse_size_store(data: bytes, chunk_num: int, *, verify_crc: int | None = None) -> BucketIndex:
    """Decode ``chunk_num`` 15-bit packed SizeStore records starting at the
    beginning of ``data`` (FORMAT-SPEC.md: SizeStore, the bucket's SizeStore
    region, bytes ``[64, 16384)``); only the first
    ``chunk_size_store_tight_length`` bytes are read.

    Every record's ``CompressType`` is validated eagerly; the result is the
    compressed layout's ``BucketIndex``.

    Args:
        data: Bytes starting at the SizeStore region.
        chunk_num: Number of records to decode, from the bucket header.
        verify_crc: The header's ``chunkSizeCrc`` field. When given, the
            tightly-packed bytes' CRC32 is checked against it.

    Raises:
        FormatError: ``data`` is shorter than the tightly-packed region
            ``chunk_num`` implies.
        DataCorruptError: ``verify_crc`` was given and does not match, or a
            decoded record's ``CompressType`` value is unknown.
    """
    tight_len = chunk_size_store_tight_length(chunk_num)
    if len(data) < tight_len:
        raise FormatError(f"SizeStore data too short: {len(data)} bytes < {tight_len}", spec=_SPEC)
    tight = data[:tight_len]

    if verify_crc is not None:
        verify_crc32(tight, verify_crc, label="SizeStore", spec=_SPEC)

    # Decoded a field at a time across every group (each field's bytes are a
    # strided slice, translate()d in C), never a record at a time: per-record
    # Python work is what dominates opening a bucket.
    group_count = -(-chunk_num // _SIZE_STORE_GROUP_RECORDS)
    padded = tight.ljust(group_count * _SIZE_STORE_GROUP_BYTES, b"\0")
    columns = [padded[pos::_SIZE_STORE_GROUP_BYTES] for pos in range(_SIZE_STORE_GROUP_BYTES)]
    record_count = group_count * _SIZE_STORE_GROUP_RECORDS
    types, highs, lows = bytearray(record_count), bytearray(record_count), bytearray(record_count)
    for record in range(_SIZE_STORE_GROUP_RECORDS):
        types[record::_SIZE_STORE_GROUP_RECORDS] = _field_bytes(columns, _COMPRESS_TYPE_PLANS[record], group_count)
        highs[record::_SIZE_STORE_GROUP_RECORDS] = _field_bytes(columns, _STORED_LEN_HIGH_PLANS[record], group_count)
        lows[record::_SIZE_STORE_GROUP_RECORDS] = _field_bytes(columns, _STORED_LEN_LOW_PLANS[record], group_count)
    del types[chunk_num:]
    if types.translate(None, _KNOWN_COMPRESS_TYPE_BYTES):
        chunk_idx = next(i for i, value in enumerate(types) if value not in COMPRESS_TYPE_BY_VALUE)
        raise DataCorruptError(f"unknown CompressType {types[chunk_idx]} at chunk {chunk_idx}", spec=_SPEC)
    compress_types = array.array("B", types)
    stored_lens = array.array("H")
    stored_lens.frombytes(_native_u16(lows, highs))
    del stored_lens[chunk_num:]
    return BucketIndex.of(compress_types, stored_lens, data_start=COMPRESS_RESERVED_LENG)


class ChunkLocator(NamedTuple):
    """Absolute file byte-range for one chunk's (still compressed and/or
    encrypted) data. A ``NamedTuple``, not a frozen dataclass: built per
    chunk read, where it is cheaper.
    """

    offset: int
    length: int


@dataclasses.dataclass(frozen=True, slots=True, eq=False)
class BucketIndex(Sequence[SizeStoreEntry]):
    """One bucket's per-chunk SizeStore entries and data locations, as
    parallel ``array.array`` columns (FORMAT-SPEC.md: SizeStore, Physical
    layout). A ``Sequence[SizeStoreEntry]``; ``locator(i)`` is chunk ``i``'s
    byte range. Hot loops read the columns directly; they are shared, so
    never mutate them.

    Attributes:
        compress_types: Raw ``CompressType`` values (``"B"``).
        stored_lens: Raw 12-bit size fields (``"H"``).
        effective_lens: Bytes each chunk occupies in the data region
            (``SizeStoreEntry.effective_len``; ``"H"``); also each locator's
            length.
        offsets: Each chunk's absolute file offset (``"I"``: a bucket tops
            out at ~33.6 MB).
    """

    compress_types: array.array[int]
    stored_lens: array.array[int]
    effective_lens: array.array[int]
    offsets: array.array[int]

    @classmethod
    def of(cls, compress_types: array.array[int], stored_lens: array.array[int], *, data_start: int) -> BucketIndex:
        """The index of chunks laid out back to back from ``data_start``."""
        # Stored length is the effective one except for NONE (a full chunk)
        # and COMPACTED (none): masked and filled in per 16-bit lane, as big
        # integers, so no chunk costs a Python-level step.
        raw_types = compress_types.tobytes()
        keep = raw_types.translate(_KEEPS_STORED_LEN)
        stored = stored_lens.tobytes()
        effective = (int.from_bytes(stored) & int.from_bytes(_native_u16(keep, keep))) | int.from_bytes(
            _native_u16(bytes(len(raw_types)), raw_types.translate(_FIXED_LEN_HIGH_BYTE))
        )
        effective_lens = array.array("H")
        effective_lens.frombytes(effective.to_bytes(len(stored)))
        offsets = array.array("I", itertools.accumulate(effective_lens, initial=data_start))
        offsets.pop()  # accumulate() also yields the end of the last chunk
        return cls(compress_types, stored_lens, effective_lens, offsets)

    @classmethod
    def uncompressed(cls, chunk_num: int) -> BucketIndex:
        """The uncompressed layout's index: every chunk ``CompressType.NONE``,
        ``FIXED_CHUNK_LENGTH`` apart from ``RESERVED_LENG`` (FORMAT-SPEC.md:
        Header & mode bits). Current writers never produce it."""
        return cls.of(
            array.array("B", bytes(chunk_num)), array.array("H", bytes(2 * chunk_num)), data_start=RESERVED_LENG
        )

    @override
    def __len__(self) -> int:
        return len(self.compress_types)

    @overload
    def __getitem__(self, index: int) -> SizeStoreEntry: ...
    @overload
    def __getitem__(self, index: slice) -> list[SizeStoreEntry]: ...

    @override
    def __getitem__(self, index: int | slice) -> SizeStoreEntry | list[SizeStoreEntry]:
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        return SizeStoreEntry(COMPRESS_TYPE_BY_VALUE[self.compress_types[index]], self.stored_lens[index])

    def locator(self, chunk_idx: int) -> ChunkLocator:
        """Chunk ``chunk_idx``'s absolute byte range."""
        return ChunkLocator(self.offsets[chunk_idx], self.effective_lens[chunk_idx])

    @property
    def total_effective_len(self) -> int:
        """Bytes of the whole chunk-data region."""
        return sum(self.effective_lens)

    @property
    def non_empty_count(self) -> int:
        """Chunks with ``effective_len > 0``, so ``COMPACTED`` ones count as
        empty. The spec does not distinguish this from counting ``chunk_num``
        verbatim, and no real compacted chunk has settled it."""
        return len(self.effective_lens) - self.effective_lens.count(0)

    def crc_store_positions(self) -> array.array[int]:
        """Every chunk's position within the ChunkCrcStore trailer: the count
        of non-empty chunks below it. A COMPACTED chunk's slot holds the next
        non-empty chunk's position and is never read."""
        if self.non_empty_count == len(self):
            return array.array("I", range(len(self)))
        positions = array.array("I", itertools.accumulate((length > 0 for length in self.effective_lens), initial=0))
        positions.pop()  # accumulate() also yields the count past the last chunk
        return positions


def expected_bucket_size(header: BucketFileHeader, index: BucketIndex) -> int:
    """Self-check total on-disk file size.

    ``COMPRESS_RESERVED_LENG + Σ effective_len + 4×non-empty-chunks +
    redundancy_size(tight_sizestore_len, 256)``.

    Raises:
        ValueError: ``header`` is not the compressed layout.
    """
    if not header.is_compressed:
        raise ValueError("expected_bucket_size only applies to the compressed layout")
    tight_len = chunk_size_store_tight_length(header.chunk_num)
    trailer = CHUNK_CRC_SIZE * index.non_empty_count + redundancy_size(tight_len, REDUNDANCY_COVERAGE_BUCKET)
    return COMPRESS_RESERVED_LENG + index.total_effective_len + trailer


def chunk_crc_store_region(header: BucketFileHeader, index: BucketIndex) -> tuple[int, int]:
    """Byte ``(offset, length)`` of the ChunkCrcStore trailer (FORMAT-SPEC.md:
    ChunkCrcStore & Redundancy) within the bucket file — immediately after the chunk-data
    region, one 4-byte entry per non-empty chunk.

    Raises:
        ValueError: ``header`` is not the compressed layout.
    """
    if not header.is_compressed:
        raise ValueError("chunk_crc_store_region only applies to the compressed layout")
    return COMPRESS_RESERVED_LENG + index.total_effective_len, CHUNK_CRC_SIZE * index.non_empty_count


def parse_chunk_crc_store(data: bytes, non_empty: int, *, verify_crc: int | None = None) -> tuple[int, ...]:
    """Decode the ChunkCrcStore trailer: one big-endian 4-byte ciphertext
    CRC32 per non-empty chunk (FORMAT-SPEC.md: ChunkCrcStore & Redundancy), computed at
    write time over each chunk's *stored* bytes (after compression and
    encryption, if any) — not a plaintext check.

    Args:
        data: Bytes starting at ``chunk_crc_store_region``'s offset; only
            the first ``CHUNK_CRC_SIZE * non_empty`` bytes are read.
        non_empty: Number of entries, from ``chunk_crc_store_region``.
        verify_crc: The header's ``crc_of_chunk_crc`` field. When given,
            the trailer's own CRC32 is checked against it (no chunk's data
            is checked here).

    Raises:
        FormatError: ``data`` is shorter than the trailer needs.
        DataCorruptError: ``verify_crc`` was given and does not match.
    """
    length = CHUNK_CRC_SIZE * non_empty
    if len(data) < length:
        raise FormatError(f"ChunkCrcStore trailer too short: {len(data)} bytes < {length}", spec=_SPEC)
    trailer = data[:length]
    if verify_crc is not None:
        verify_crc32(trailer, verify_crc, label="ChunkCrcStore", spec=_SPEC)
    return struct.unpack(f">{non_empty}I", trailer) if non_empty else ()
