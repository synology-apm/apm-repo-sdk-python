"""``ChunkMapRecord`` — the 20-byte record describing one segment of a
file's content within a composition record (FORMAT-SPEC.md: ChunkMapRecord).

This is the core of the entire read path: every byte of every restorable
unit is reached by walking an array of these. Decode precisely — traps
#4/#5/#7 all live in the ambiguity between the two record kinds this
module resolves once, here.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Iterator

from ..errors import DataCorruptError, FormatError
from .addressing import ChunkAddress
from .const import CHUNK_MAP_RECORD_LENGTH, FIXED_CHUNK_BIT_NUM, FIXED_CHUNK_LENGTH

_SPEC = "FORMAT-SPEC.md: ChunkMapRecord"

# Byte 0's top nibble/bit (of the big-endian uint64 at offset [0,8)).
_TYPE_FILTER = 0x0F
_TYPE_INHERIT_BIT = 0x10
_TYPE_DATA_SHIFT = 56
_FILE_IDX_FILTER = (1 << _TYPE_DATA_SHIFT) - 1  # low 56 bits of the offset-0 word

_MAPNUM_SHIFT = 16
_REPEAT_FILTER = (1 << _MAPNUM_SHIFT) - 1  # 0xFFFF


class ChunkMapKind(enum.Enum):
    """``Type`` (FORMAT-SPEC.md: ChunkMapRecord). Values match the on-disk encoding
    exactly — do not renumber."""

    MAPPING = 0
    ZERO = 1


@dataclasses.dataclass(frozen=True)
class ChunkMapEntry:
    """One decoded ``ChunkMapRecord``.

    ``map_num``'s meaning depends on ``kind``: for ``ChunkMapKind.MAPPING``
    it is the address *template* length in chunks (the template repeats
    ``1 + repeat`` times); for ``ChunkMapKind.ZERO`` it is the literal
    chunk count covered (``repeat`` is always 0 and meaningless there) — both
    are decoded from the identical byte offset [16,20), just split
    differently, which is exactly why a single field name is used for both
    rather than inventing a separate ``zero_num``.
    """

    kind: ChunkMapKind
    file_offset: int
    """``fileChunkIdx << 12`` — the byte offset within the described file
    where this record's coverage begins. Recorded as ``ChunkMapRecord`` in
    FORMAT-SPEC.md."""
    is_inherit: bool
    """The ``INHERIT`` bit — purely descriptive ("this data was carried
    over unchanged from a reference composition"). See FORMAT-SPEC.md's
    hole-zero-inherit note. The read path never branches on it — every
    record, inherited or not, is self-sufficient to read."""
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

    A Mapping record's embedded ``ChunkAddress`` isn't range-checked here
    either — see ``ChunkAddress``'s own docstring.

    Raises:
        FormatError: ``data`` is shorter than 20 bytes.
        DataCorruptError: The type nibble is neither 0 (Mapping) nor 1 (Zero).
    """
    if len(data) < CHUNK_MAP_RECORD_LENGTH:
        raise FormatError(f"ChunkMapRecord too short: {len(data)} bytes < {CHUNK_MAP_RECORD_LENGTH}", spec=_SPEC)

    word0 = int.from_bytes(data[0:8], "big")
    type_byte = word0 >> _TYPE_DATA_SHIFT
    kind_value = type_byte & _TYPE_FILTER
    is_inherit = bool(type_byte & _TYPE_INHERIT_BIT)
    file_chunk_idx = word0 & _FILE_IDX_FILTER
    file_offset = file_chunk_idx << FIXED_CHUNK_BIT_NUM

    try:
        kind = ChunkMapKind(kind_value)
    except ValueError as exc:
        raise DataCorruptError(f"unknown ChunkMapRecord type {kind_value}", spec=_SPEC) from exc

    if kind is ChunkMapKind.MAPPING:
        addr_raw = int.from_bytes(data[8:16], "big")
        addr = ChunkAddress.from_int(addr_raw)
        num_repeat = int.from_bytes(data[16:20], "big")
        map_num = num_repeat >> _MAPNUM_SHIFT
        repeat = num_repeat & _REPEAT_FILTER
        return ChunkMapEntry(
            kind=kind,
            file_offset=file_offset,
            is_inherit=is_inherit,
            addr=addr,
            map_num=map_num,
            repeat=repeat,
        )

    # ChunkMapKind.ZERO — bytes [8,16) are unwritten and not consulted; the
    # full 32 bits at [16,20) are one literal chunk count, not split.
    zero_num = int.from_bytes(data[16:20], "big")
    return ChunkMapEntry(
        kind=kind,
        file_offset=file_offset,
        is_inherit=is_inherit,
        addr=None,
        map_num=zero_num,
        repeat=0,
    )


def iter_chunk_map_page(page_bytes: bytes, count: int) -> Iterator[ChunkMapEntry]:
    """Lazily decode ``count`` consecutive ``ChunkMapRecord``\\ s starting
    at the front of ``page_bytes`` — the batch form of
    ``parse_chunk_map_record`` a page-at-a-time cache's caller uses
    instead of re-deriving the same slice arithmetic itself."""
    for i in range(count):
        yield parse_chunk_map_record(page_bytes[i * CHUNK_MAP_RECORD_LENGTH : (i + 1) * CHUNK_MAP_RECORD_LENGTH])
