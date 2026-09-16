"""Composition file format (FORMAT-SPEC.md: composition-splitting, RecordHead).

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

_SPEC_HEADER = "FORMAT-SPEC.md: composition-splitting"
_SPEC_RECORD = "FORMAT-SPEC.md: RecordHead"

_COMPOSITION_MAJOR = 1
"""``CompMajor::Advance`` — the only value ever written or supported;
``CompMajor::Basic`` (0) is explicitly obsolete on both read and write."""

_OFF_SUB_FILE_SIZE = 8

_RECORD_MAGIC = b"Mu"
_MODE_REDUNDANCY = 0x0001


@dataclasses.dataclass(frozen=True)
class CompositionHeader:
    """Parsed composition sub-file header (present only at ``subID=0``)."""

    major: int
    minor: int


def parse_composition_header(data: bytes) -> CompositionHeader:
    """Parse the 64-byte header at the start of a session's ``subID=0``
    file (FORMAT-SPEC.md: composition-splitting).

    Raises:
        UnsupportedVersionError: ``major`` is not exactly 1 (the only value
            ever written).
        DataCorruptError: ``subFileSize`` does not equal the fixed 16 MiB
            constant every sub-file uses.
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
    """(FORMAT-SPEC.md: RecordHead) — the restore path never branches on this;
    a record reached via ``file_map`` (i.e. successfully committed) is
    always ``COMPLETE`` in practice, but this module decodes whichever
    value is present rather than rejecting ``INTERRUPTED``."""

    COMPLETE = 0
    INTERRUPTED = 1


@dataclasses.dataclass(frozen=True)
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
    """Decode the 32-byte ``RecordHead`` at the start of ``data``.

    Every current reader (and this one) refuses the pre-Redundancy record
    layout outright rather than guess at its (unspecified here) trailer
    shape.

    Raises:
        DataCorruptError: A magic or head-CRC mismatch.
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

    Deliberately **not** called automatically by ``parse_record_head``
    — doing so would force every interactive read to first load the entire
    (potentially multi-hundred-MB) map array. Callers doing a full-unit
    export or ``verify`` call this explicitly once they already have those
    bytes in hand.
    """
    verify_crc32(map_array_bytes, expected_crc, label="chunk-map array", spec=_SPEC_RECORD)


_CRC_THREAD_HOP_MIN_BYTES = 1 << 18  # 256 KiB
"""``zlib.crc32`` runs on the order of 1+ GB/s; an ``asyncio.to_thread()``
dispatch/context-switch costs on the order of 100us — below this many
bytes, calling ``verify_chunk_map_crc`` directly is faster than the hop
itself, the same "too small to bother" reasoning ``dedup/pool.py``'s
``read_chunk``/``units/saas/objectdb.py`` already document for a
decrypt+decompress pass. Most ``file_map`` rows' chunk-map arrays are
well under this; the rare multi-hundred-MB array
(``verify_chunk_map_crc``'s own docstring) is what actually needs it."""


def should_thread_chunk_map_crc(map_array_bytes: bytes) -> bool:
    """Whether ``verify_chunk_map_crc(map_array_bytes, ...)`` is worth
    running via ``asyncio.to_thread()`` rather than calling directly —
    see ``_CRC_THREAD_HOP_MIN_BYTES``'s own docstring. This module stays
    synchronous throughout (the Codec Layer never does I/O or threading
    itself), so both async callers (``dedup/verify_checks.py``,
    ``diagnostics.py``) that CRC-verify a chunk-map array make this same
    decision themselves rather than each guessing independently."""
    return len(map_array_bytes) >= _CRC_THREAD_HOP_MIN_BYTES


def verify_attr_crc(attr_bytes: bytes, expected_crc: int) -> None:
    """Validate a record's JSON attribute blob bytes against its own
    ``RecordHead.attr_crc`` (FORMAT-SPEC.md: RecordHead) — like
    ``verify_chunk_map_crc``, not needed to read the record's content and
    not called automatically by ``parse_record_head``; a ``verify`` caller
    checks it explicitly once the blob bytes are already in hand.
    """
    verify_crc32(attr_bytes, expected_crc, label="attribute blob", spec=_SPEC_RECORD)


def record_total_length(map_num: int, attr_leng: int) -> int:
    """Total byte length of one composition record starting at its
    ``headOff``: ``RECORD_HEAD_LENGTH + map_num*20 + attr_leng +
    redundancy_size(map_num*20, 8192)`` (FORMAT-SPEC.md: RecordHead/ChunkCrcStore).

    Records are packed back-to-back with no padding, so ``headOff +
    record_total_length(...)`` is exactly the next record's ``headOff``.
    """
    map_array_len = map_num * CHUNK_MAP_RECORD_LENGTH
    trailer = redundancy_size(map_array_len, REDUNDANCY_COVERAGE_COMPOSITION)
    return RECORD_HEAD_LENGTH + map_array_len + attr_leng + trailer


def chunk_map_array_offset(head_off: int) -> int:
    """Byte offset (relative to the composition sub-file) where a record's
    ``ChunkMapRecord`` array begins — immediately after its ``RecordHead``
    (FORMAT-SPEC.md: RecordHead)."""
    return head_off + RECORD_HEAD_LENGTH
