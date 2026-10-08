"""Composition file format (FORMAT-SPEC.md: Composition file splitting; RecordHead).

Two independent binary structures live in a composition sub-file:

- ``CompositionHeader`` (64 bytes) — the generic ``IndexHeader`` shell,
  present only at the very start of a session's ``subID=0`` file.
- ``RecordHead`` (32 bytes) — one per backup version, at ``comp_offset``
  (``db/file_map.comp_offset``) within the session's global offset space.
  This is a **bespoke** structure: 2-byte magic (not 4), and its own CRC
  covering ``[0,28)`` stored at offset 28 (not the generic shell's
  ``[0,60)``/60) — do not reuse ``parse_index_header`` for it.

``record_total_length`` predicts the exact file offset of the *next*
record.
"""

from __future__ import annotations

import dataclasses
import enum
import struct

from ..errors import DataCorruptError, FormatError, UnsupportedVersionError
from .const import (
    CHUNK_MAP_RECORD_LENGTH,
    RECORD_HEAD_LENGTH,
    REDUNDANCY_COVERAGE_COMPOSITION,
    SUB_FILE_SIZE,
)
from .headers import MAGIC, parse_index_header, verify_crc32
from .redundancy import redundancy_size

_SPEC_HEADER = "FORMAT-SPEC.md: Composition file splitting"
_SPEC_RECORD = "FORMAT-SPEC.md: RecordHead"

_COMPOSITION_MAJOR = 1
"""The only composition major version ever written or supported; major=0
is an obsolete, no-longer-supported composition format."""

_OFF_SUB_FILE_SIZE = 8

_RECORD_MAGIC = b"Mu"
_MODE_REDUNDANCY = 0x0001


@dataclasses.dataclass(frozen=True, slots=True)
class CompositionHeader:
    """Parsed composition sub-file header."""

    major: int
    minor: int


def parse_composition_header(data: bytes) -> CompositionHeader:
    """Parse the 64-byte header at the start of a session's ``subID=0``
    file (FORMAT-SPEC.md: Composition file splitting).

    Raises:
        FormatError: ``data`` is shorter than 64 bytes.
        DataCorruptError: A magic or header-CRC mismatch, or ``subFileSize``
            is not the fixed 16 MiB every sub-file uses.
        UnsupportedVersionError: ``major`` is not exactly 1 (the only value
            ever written).
    """
    header = parse_index_header(data, expect_magic=MAGIC["composition"], spec=_SPEC_HEADER)
    if header.major != _COMPOSITION_MAJOR:
        raise UnsupportedVersionError(
            f"composition major {header.major} != supported {_COMPOSITION_MAJOR}",
            spec=_SPEC_HEADER,
        )
    sub_file_size = struct.unpack(">I", data[_OFF_SUB_FILE_SIZE : _OFF_SUB_FILE_SIZE + 4])[0]
    if sub_file_size != SUB_FILE_SIZE:
        raise DataCorruptError(f"subFileSize {sub_file_size} != expected {SUB_FILE_SIZE}", spec=_SPEC_HEADER)
    return CompositionHeader(major=header.major, minor=header.minor)


class CompositionStatus(enum.Enum):
    """A record's status (FORMAT-SPEC.md: RecordHead). The restore path never
    branches on it: a record reached via ``file_map`` is ``COMPLETE`` in
    practice, but ``INTERRUPTED`` is decoded rather than rejected."""

    COMPLETE = 0
    INTERRUPTED = 1


@dataclasses.dataclass(frozen=True, slots=True)
class RecordHead:
    """Parsed 32-byte ``RecordHead`` (FORMAT-SPEC.md: RecordHead)."""

    status: CompositionStatus
    map_num: int
    map_crc: int
    mode: int
    attr_leng: int
    attr_crc: int

    @property
    def has_redundancy(self) -> bool:
        return bool(self.mode & _MODE_REDUNDANCY)


def parse_record_head(data: bytes) -> RecordHead:
    """Decode the 32-byte ``RecordHead`` at the start of ``data``. The
    pre-Redundancy record layout is refused outright.

    Raises:
        FormatError: ``data`` is shorter than 32 bytes.
        DataCorruptError: A magic or head-CRC mismatch, or an unknown status.
        UnsupportedVersionError: The ``Redundancy`` mode bit is absent.
    """
    if len(data) < RECORD_HEAD_LENGTH:
        raise FormatError(f"RecordHead too short: {len(data)} bytes < {RECORD_HEAD_LENGTH}", spec=_SPEC_RECORD)
    if data[0:2] != _RECORD_MAGIC:
        raise DataCorruptError(f"bad magic {data[0:2]!r}, expected {_RECORD_MAGIC!r}", spec=_SPEC_RECORD)

    head_crc = struct.unpack(">I", data[28:32])[0]
    verify_crc32(data[:28], head_crc, label="RecordHead", spec=_SPEC_RECORD)

    # status_value/map_num/map_crc/mode/attr_leng/attr_crc, contiguous
    # over [2, 28) with a 2-byte reserved gap at [4, 6) between status
    # and map_num.
    status_value, map_num, map_crc, mode, attr_leng, attr_crc = struct.unpack(">H2xQIHII", data[2:28])
    try:
        status = CompositionStatus(status_value)
    except ValueError as exc:
        raise DataCorruptError(f"unknown RecordHead status {status_value}", spec=_SPEC_RECORD) from exc

    record = RecordHead(
        status=status,
        map_num=map_num,
        map_crc=map_crc,
        mode=mode,
        attr_leng=attr_leng,
        attr_crc=attr_crc,
    )
    if not record.has_redundancy:
        raise UnsupportedVersionError(
            "RecordHead lacks the Redundancy mode bit — pre-Redundancy composition format is not supported",
            spec=_SPEC_RECORD,
        )
    return record


def verify_chunk_map_crc(map_array_bytes: bytes, expected_crc: int) -> None:
    """Validate a full ``ChunkMapRecord`` array's bytes against a
    ``RecordHead.map_crc`` (FORMAT-SPEC.md: RecordHead).

    Not called by ``parse_record_head``, which would force every read to
    load the whole (possibly multi-hundred-MB) array; ``verify`` and
    ``diagnostics`` call it explicitly.

    Raises:
        DataCorruptError: The CRC32 does not match.
    """
    verify_crc32(map_array_bytes, expected_crc, label="chunk-map array", spec=_SPEC_RECORD)


def verify_attr_crc(attr_bytes: bytes, expected_crc: int) -> None:
    """Validate a record's JSON attribute blob against
    ``RecordHead.attr_crc`` (FORMAT-SPEC.md: RecordHead). Only ``verify``
    calls it.

    Raises:
        DataCorruptError: The CRC32 does not match.
    """
    verify_crc32(attr_bytes, expected_crc, label="attribute blob", spec=_SPEC_RECORD)


def record_total_length(map_num: int, attr_leng: int) -> int:
    """Total byte length of one composition record starting at its
    ``headOff``: ``RECORD_HEAD_LENGTH + map_num*20 + attr_leng +
    redundancy_size(map_num*20, 8192)`` (FORMAT-SPEC.md: RecordHead;
    ChunkCrcStore & Redundancy).

    Records are packed back-to-back with no padding, so ``headOff +
    record_total_length(...)`` is exactly the next record's ``headOff``.
    """
    map_array_len = map_num * CHUNK_MAP_RECORD_LENGTH
    trailer = redundancy_size(map_array_len, REDUNDANCY_COVERAGE_COMPOSITION)
    return RECORD_HEAD_LENGTH + map_array_len + attr_leng + trailer


def chunk_map_array_offset(head_off: int) -> int:
    """Session-global offset where a record's ``ChunkMapRecord`` array begins,
    right after its ``RecordHead`` (FORMAT-SPEC.md: RecordHead)."""
    return head_off + RECORD_HEAD_LENGTH
