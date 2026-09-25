"""Unit tests for ``synology_apm_repo.sdk.format.headers``."""

from __future__ import annotations

import json
import zlib

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError, UnsupportedVersionError
from synology_apm_repo.sdk.format.headers import HEADER_LEN, MAGIC, parse_index_header, parse_json_payload_header


def _build(magic: bytes = b"bFiL", major: int = 3, minor: int = 0, payload: bytes = b"") -> bytes:
    header = bytearray(64)
    header[0:4] = magic
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8 : 8 + len(payload)] = payload
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def test_valid_header_parses() -> None:
    data = _build(major=3, minor=0)
    parsed = parse_index_header(data, expect_magic=b"bFiL")
    assert parsed.magic == b"bFiL"
    assert parsed.major == 3
    assert parsed.minor == 0
    assert len(parsed.raw) == 64


def test_wrong_magic_raises_data_corrupt() -> None:
    data = _build(magic=b"XXXX")
    with pytest.raises(DataCorruptError):
        parse_index_header(data, expect_magic=b"bFiL")


def test_bad_crc_raises_data_corrupt() -> None:
    data = bytearray(_build())
    data[10] ^= 0xFF  # corrupt a payload byte covered by the header CRC
    with pytest.raises(DataCorruptError):
        parse_index_header(bytes(data), expect_magic=b"bFiL")


def test_too_short_raises_format_error() -> None:
    with pytest.raises(FormatError):
        parse_index_header(b"short", expect_magic=b"bFiL")


def test_major_beyond_max_raises_unsupported_version() -> None:
    data = _build(major=5)
    with pytest.raises(UnsupportedVersionError):
        parse_index_header(data, expect_magic=b"bFiL", max_major=3)


def test_minor_is_never_checked_on_read() -> None:
    data = _build(major=3, minor=99)
    parsed = parse_index_header(data, expect_magic=b"bFiL", max_major=3)
    assert parsed.minor == 99


def test_major_at_exactly_max_is_accepted() -> None:
    data = _build(major=3)
    parsed = parse_index_header(data, expect_magic=b"bFiL", max_major=3)
    assert parsed.major == 3


def test_magic_table_has_expected_entries() -> None:
    assert MAGIC["bucket"] == b"bFiL"
    assert MAGIC["composition"] == b"cMpS"
    assert MAGIC["repo_info"] == b"RpiF"
    assert MAGIC["ahlt"] == b"aHlT"
    assert len(MAGIC) == 12


def _build_json_payload(
    payload: bytes,
    *,
    magic: bytes = b"RpiF",
    major: int = 1,
    minor: int = 0,
    data_size: int | None = None,
) -> bytes:
    """Independent encoder for ``parse_json_payload_header``'s shell:
    64-byte header (magic/major/minor + ``json_crc``(4) at offset 8 +
    ``data_size``(8) at offset 12) followed by ``payload`` verbatim.
    ``data_size`` defaults to ``len(payload)``; pass a different value to
    build a deliberately-truncated fixture."""
    header = bytearray(64)
    header[0:4] = magic
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = (len(payload) if data_size is None else data_size).to_bytes(8, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header) + payload


def test_parse_json_payload_header_happy_path() -> None:
    payload = json.dumps({"key": "value", "n": 3}).encode("utf-8")
    data = _build_json_payload(payload, magic=b"RpiF", major=2, minor=1)

    header, parsed = parse_json_payload_header(data, expect_magic=b"RpiF")

    assert header.magic == b"RpiF"
    assert header.major == 2
    assert header.minor == 1
    assert len(header.raw) == HEADER_LEN
    assert parsed == {"key": "value", "n": 3}


def test_parse_json_payload_header_payload_crc_mismatch_raises_data_corrupt() -> None:
    payload = json.dumps({"a": 1}).encode("utf-8")
    data = bytearray(_build_json_payload(payload))
    data[HEADER_LEN] ^= 0xFF  # corrupt a payload byte, header CRC untouched
    with pytest.raises(DataCorruptError):
        parse_json_payload_header(bytes(data), expect_magic=b"RpiF")


def test_parse_json_payload_header_truncated_payload_raises_format_error_naming_the_payload_kind() -> None:
    payload = json.dumps({"a": 1}).encode("utf-8")
    # data_size claims more bytes than are actually present after the header.
    data = _build_json_payload(payload, data_size=len(payload) + 10)
    with pytest.raises(FormatError, match="my_custom_kind payload truncated"):
        parse_json_payload_header(data, expect_magic=b"RpiF", payload_kind="my_custom_kind")


def test_parse_json_payload_header_truncated_payload_default_kind_in_message() -> None:
    payload = json.dumps({"a": 1}).encode("utf-8")
    data = _build_json_payload(payload, data_size=len(payload) + 10)
    with pytest.raises(FormatError, match=r"^payload payload truncated"):
        parse_json_payload_header(data, expect_magic=b"RpiF")


def test_parse_json_payload_header_reuses_parse_index_header_shell_checks() -> None:
    payload = json.dumps({}).encode("utf-8")
    data = bytearray(_build_json_payload(payload, magic=b"RpiF"))
    data[0:4] = b"XXXX"
    with pytest.raises(DataCorruptError):
        parse_json_payload_header(bytes(data), expect_magic=b"RpiF")
