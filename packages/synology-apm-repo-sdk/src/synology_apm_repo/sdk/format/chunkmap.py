"""``ChunkMapRecord`` — the 20-byte record describing one segment of a
file's content within a composition record (FORMAT-SPEC.md: ChunkMapRecord).

Every byte of every restorable unit is reached by walking an array of
these; the ambiguity between the two record kinds (``MAPPING``/``ZERO``) is
resolved once, here.
"""

from __future__ import annotations

import dataclasses
import enum
import struct
from collections.abc import Iterator

from ..errors import DataCorruptError, FormatError
from .addressing import ChunkAddress
from .const import CHUNK_MAP_RECORD_LENGTH, FIXED_CHUNK_BIT_NUM, FIXED_CHUNK_LENGTH

_SPEC = "FORMAT-SPEC.md: ChunkMapRecord"

# Byte 0 of the big-endian uint64 at [0,8): low nibble is Type, bit 4 is INHERIT.
_TYPE_FILTER = 0x0F
_TYPE_INHERIT_BIT = 0x10
_TYPE_DATA_SHIFT = 56
_FILE_IDX_FILTER = (1 << _TYPE_DATA_SHIFT) - 1  # low 56 bits of the offset-0 word

_MAPNUM_SHIFT = 16
_REPEAT_FILTER = (1 << _MAPNUM_SHIFT) - 1  # 0xFFFF

# Type/fileChunkIdx word, ChunkAddress word, mapNum/repeat (or zero count) word.
_RECORD = struct.Struct(">QQI")


class ChunkMapKind(enum.Enum):
    """``Type`` (FORMAT-SPEC.md: ChunkMapRecord). Values match the on-disk encoding
    exactly — do not renumber."""

    MAPPING = 0
    ZERO = 1


_KIND_BY_VALUE = {kind.value: kind for kind in ChunkMapKind}


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkMapEntry:
    """One decoded ``ChunkMapRecord``.

    ``map_num``'s meaning depends on ``kind``: for ``ChunkMapKind.MAPPING``
    it is the address *template* length in chunks (the template repeats
    ``1 + repeat`` times); for ``ChunkMapKind.ZERO`` it is the literal
    chunk count covered (``repeat`` is always 0 there). Both come from bytes
    [16,20), split differently.
    """

    kind: ChunkMapKind
    file_offset: int
    """``fileChunkIdx << 12`` — the byte offset within the described file
    where this record's coverage begins."""
    is_inherit: bool
    """The ``INHERIT`` bit, purely descriptive ("carried over unchanged
    from a reference version"); reading never depends on it
    (FORMAT-SPEC.md: Hole vs. Zero vs. INHERIT)."""
    addr: ChunkAddress | None
    """The chunk's base address in Pool. ``None`` for ``ChunkMapKind.ZERO``."""
    map_num: int
    repeat: int

    @property
    def length(self) -> int:
        """Byte length this record covers, starting at ``file_offset``."""
        if self.kind is ChunkMapKind.ZERO:
            return self.map_num * FIXED_CHUNK_LENGTH
        return self.map_num * (1 + self.repeat) * FIXED_CHUNK_LENGTH

    @property
    def end_offset(self) -> int:
        return self.file_offset + self.length


def parse_chunk_map_record(data: bytes) -> ChunkMapEntry:
    """Decode one 20-byte ``ChunkMapRecord`` from the start of ``data``
    (``data`` may be longer; only the first 20 bytes are consulted).

    A Mapping record's embedded ``ChunkAddress`` is not range-checked (see
    ``ChunkAddress``).

    Raises:
        FormatError: ``data`` is shorter than 20 bytes.
        DataCorruptError: The type nibble is neither 0 (Mapping) nor 1 (Zero).
    """
    if len(data) < CHUNK_MAP_RECORD_LENGTH:
        raise FormatError(f"ChunkMapRecord too short: {len(data)} bytes < {CHUNK_MAP_RECORD_LENGTH}", spec=_SPEC)
    return _parse_at(data, 0)


def _kind_of(word0: int) -> ChunkMapKind:
    kind_value = (word0 >> _TYPE_DATA_SHIFT) & _TYPE_FILTER
    kind = _KIND_BY_VALUE.get(kind_value)
    if kind is None:
        raise DataCorruptError(f"unknown ChunkMapRecord type {kind_value}", spec=_SPEC)
    return kind


def _parse_at(data: bytes, offset: int) -> ChunkMapEntry:
    word0, addr_raw, num = _RECORD.unpack_from(data, offset)
    kind = _kind_of(word0)
    file_offset = (word0 & _FILE_IDX_FILTER) << FIXED_CHUNK_BIT_NUM
    is_inherit = bool((word0 >> _TYPE_DATA_SHIFT) & _TYPE_INHERIT_BIT)
    if kind is ChunkMapKind.MAPPING:
        return ChunkMapEntry(
            kind=kind,
            file_offset=file_offset,
            is_inherit=is_inherit,
            addr=ChunkAddress.from_int(addr_raw),
            map_num=num >> _MAPNUM_SHIFT,
            repeat=num & _REPEAT_FILTER,
        )
    # ChunkMapKind.ZERO — bytes [8,16) are unwritten and not consulted; the
    # full 32 bits at [16,20) are one literal chunk count, not split.
    return ChunkMapEntry(kind=kind, file_offset=file_offset, is_inherit=is_inherit, addr=None, map_num=num, repeat=0)


def chunk_map_end_offset(page_bytes: bytes, index: int) -> int:
    """``ChunkMapEntry.end_offset`` of record ``index`` in ``page_bytes``,
    read without building the entry, for a binary search over a page.

    Raises:
        FormatError: ``page_bytes`` ends before record ``index`` does.
        DataCorruptError: The record has an unknown type.
    """
    word0: int
    num: int
    try:
        word0, _addr_raw, num = _RECORD.unpack_from(page_bytes, index * CHUNK_MAP_RECORD_LENGTH)
    except struct.error as exc:
        raise FormatError(f"chunk map page ends before record {index}", spec=_SPEC) from exc
    file_offset = (word0 & _FILE_IDX_FILTER) << FIXED_CHUNK_BIT_NUM
    if _kind_of(word0) is ChunkMapKind.ZERO:
        return file_offset + num * FIXED_CHUNK_LENGTH
    return file_offset + (num >> _MAPNUM_SHIFT) * (1 + (num & _REPEAT_FILTER)) * FIXED_CHUNK_LENGTH


def iter_chunk_map_page(page_bytes: bytes, count: int) -> Iterator[ChunkMapEntry]:
    """Lazily decode ``count`` consecutive ``ChunkMapRecord``\\ s starting
    at the front of ``page_bytes``; the batch form of
    ``parse_chunk_map_record``.

    Raises:
        FormatError: ``page_bytes`` holds fewer than ``count`` records.
        DataCorruptError: A record has an unknown type.
    """
    if len(page_bytes) < count * CHUNK_MAP_RECORD_LENGTH:
        raise FormatError(
            f"chunk map page holds {len(page_bytes) // CHUNK_MAP_RECORD_LENGTH} records, expected {count}", spec=_SPEC
        )
    for i in range(count):
        yield _parse_at(page_bytes, i * CHUNK_MAP_RECORD_LENGTH)
