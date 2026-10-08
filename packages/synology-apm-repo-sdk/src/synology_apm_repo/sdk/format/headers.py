"""The generic 64-byte ``IndexHeader`` shell shared by every binary format in
this project (FORMAT-SPEC.md: 64-byte header shell):

::

    offset  size  field
    0       4     magic (ASCII)
    4       2     major (BE u16)
    6       2     minor (BE u16)
    8       52    format-specific payload
    60      4     header CRC32 (BE u32) = crc32(0, buf, 60)

Validation order (identical for every format, FORMAT-SPEC.md: 64-byte header
shell): magic, then header CRC, then major-only version acceptance. Any minor
is accepted.

This module owns only the shell (magic / CRC / version); each format module
(``bucket.py``, ``composition.py``, ...) decodes its own payload fields and
calls ``parse_index_header`` for the shell checks.
"""

from __future__ import annotations

import dataclasses
import struct
import zlib
from typing import Any

from .._util.jsonparse import parse_json_object
from ..errors import DataCorruptError, FormatError, UnsupportedVersionError

HEADER_LEN = 64
OFF_MAGIC = 0
MAGIC_LEN = 4
OFF_MAJOR = 4
OFF_MINOR = 6
OFF_HEADER_CRC = 60
_OFF_JSON_CRC = 8
_OFF_DATA_SIZE = 12

# Magic constants for every binary format (FORMAT-SPEC.md: 64-byte header shell
# table); not every format has a parser module.
MAGIC = {
    "repo_info": b"RpiF",
    "link_key": b"lINk",
    "repo_transaction": b"rPTs",
    "bucket": b"bFiL",
    "bucket_meta": b"GMet",
    "bucket_info": b"biNF",  # nested sub-header inside .inf per-bucket record; restore path never reads it
    "bucket_refcount": b"bRfC",
    "composition": b"cMpS",
    "collection": b"cLcF",  # never produced/read by this project
    "hot_index": b"HoOt",  # write-side dedup acceleration index; restore path never reads it
    "sample_index": b"sMPl",  # write-side dedup acceleration index; restore path never reads it
    "ahlt": b"aHlT",  # copy_meta_file file-level AES-256-CTR envelope
}


@dataclasses.dataclass(frozen=True, slots=True)
class IndexHeader:
    """The validated generic shell of a 64-byte header. ``raw`` is the full
    64 bytes, for format modules to slice their payload fields from."""

    magic: bytes
    major: int
    minor: int
    raw: bytes


def parse_index_header(
    data: bytes | bytearray,
    *,
    expect_magic: bytes,
    max_major: int | None = None,
    spec: str | None = None,
) -> IndexHeader:
    """Validate and parse the generic 64-byte header shell at the start of
    ``data`` (which may be longer; only the first 64 bytes are read).

    Args:
        data: Bytes starting at the header.
        expect_magic: The format's magic (see ``MAGIC``).
        max_major: Highest accepted major version; ``None`` accepts any.
        spec: ``FORMAT-SPEC.md: ...`` citation attached to errors.

    Returns:
        The validated shell.

    Raises:
        FormatError: ``data`` is shorter than 64 bytes.
        DataCorruptError: A magic or header-CRC mismatch.
        UnsupportedVersionError: ``max_major`` is given and ``major`` exceeds
            it.
    """
    if len(data) < HEADER_LEN:
        raise FormatError(f"header too short: {len(data)} bytes < {HEADER_LEN}", spec=spec)
    data = bytes(data[:HEADER_LEN])

    magic = data[OFF_MAGIC : OFF_MAGIC + MAGIC_LEN]
    if magic != expect_magic:
        raise DataCorruptError(f"bad magic {magic!r}, expected {expect_magic!r}", spec=spec)

    header_crc = int.from_bytes(data[OFF_HEADER_CRC : OFF_HEADER_CRC + 4], "big")
    verify_crc32(data[:OFF_HEADER_CRC], header_crc, label="header", spec=spec)

    major, minor = struct.unpack(">HH", data[OFF_MAJOR : OFF_MINOR + 2])
    if max_major is not None and major > max_major:
        raise UnsupportedVersionError(f"major version {major} > max supported {max_major}", spec=spec)

    return IndexHeader(magic=magic, major=major, minor=minor, raw=data[:HEADER_LEN])


def parse_json_payload_header(
    data: bytes, *, expect_magic: bytes, spec: str | None = None, payload_kind: str = "payload"
) -> tuple[IndexHeader, dict[str, Any]]:
    """Shared shell for a format whose 64-byte header (validated via
    ``parse_index_header``) is followed by a CRC32'd JSON payload:
    ``json_crc`` (BE u32) at offset 8, ``data_size`` (BE u64) at offset 12,
    payload at ``[HEADER_LEN, HEADER_LEN + data_size)``. Used by
    ``repo_info`` and ``repo_transaction``.

    Args:
        data: Bytes starting at the header.
        expect_magic: The format's magic.
        spec: ``FORMAT-SPEC.md: ...`` citation attached to errors.
        payload_kind: Names the payload in error messages.

    Returns:
        ``(header, decoded JSON payload)``.

    Raises:
        FormatError: The payload is shorter than ``data_size`` declares.
        DataCorruptError: A payload CRC mismatch (in addition to
            ``parse_index_header``'s own magic/header-CRC checks), or a
            payload that is not a JSON object.
    """
    header = parse_index_header(data, expect_magic=expect_magic, spec=spec)
    json_crc, data_size = struct.unpack(">IQ", data[_OFF_JSON_CRC : _OFF_DATA_SIZE + 8])
    payload = data[HEADER_LEN : HEADER_LEN + data_size]
    if len(payload) != data_size:
        raise FormatError(
            f"{payload_kind} payload truncated: expected {data_size} bytes, got {len(payload)}", spec=spec
        )
    verify_crc32(payload, json_crc, label="payload", spec=spec)
    return header, parse_json_object(payload, f"{payload_kind} payload")


def compute_crc32(data: bytes | bytearray | memoryview) -> int:
    """``zlib.crc32(data) & 0xFFFFFFFF``, the unsigned 32-bit form every CRC32
    check compares against (shared by ``verify_crc32`` and
    ``format.redundancy.attempt_repair``)."""
    return zlib.crc32(data) & 0xFFFFFFFF


def verify_crc32(data: bytes | bytearray | memoryview, expected: int, *, label: str, spec: str | None = None) -> None:
    """Raise ``DataCorruptError`` unless ``compute_crc32(data)`` matches
    ``expected``; the verify-or-raise shape shared by every binary format.

    Args:
        data: The checked bytes.
        expected: The stored CRC32.
        spec: ``FORMAT-SPEC.md: ...`` citation attached to the error.
        label: Names the checked region in the error message (e.g.
            ``"header"``, ``"payload"``, ``"RecordHead"``).

    Raises:
        DataCorruptError: The computed CRC32 doesn't match ``expected``.
    """
    computed = compute_crc32(data)
    if computed != expected:
        raise DataCorruptError(
            f"{label} CRC mismatch: computed {computed:#010x} != stored {expected:#010x}",
            spec=spec,
        )
