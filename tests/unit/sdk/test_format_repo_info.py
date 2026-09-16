"""Unit tests for ``synology_apm_repo.sdk.format.repo_info`` — synthetic
bytes only (see ``tests/integration/sdk/test_repo_info.py`` for the
byte-for-byte cross-check against a real sample)."""

from __future__ import annotations

import json
import zlib

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.repo_info import MAGIC, parse_repo_info


def _build(
    *,
    uuid: str = "abcdefghijklmnop",
    major: int = 2,
    minor: int = 2,
    payload_obj: dict[str, object] | None = None,
) -> bytes:
    assert len(uuid) == 16
    payload_obj = (
        payload_obj
        if payload_obj is not None
        else {
            "is_global_dedup_supported": True,
            "is_worm_supported": True,
            "repo_flag": 0,
            "repo_type": 2,
            "storage_algorithm": {"compress_algorithm": 1, "encrypt_algorithm": 0},
        }
    )
    payload = json.dumps(payload_obj).encode("utf-8")
    header = bytearray(64)
    header[0:4] = MAGIC
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = uuid.encode("ascii")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header) + payload


def test_parse_round_trip() -> None:
    data = _build()
    info = parse_repo_info(data)
    assert info.uuid == "abcdefghijklmnop"
    assert info.major == 2
    assert info.minor == 2
    assert info.repo_type == 2
    assert info.repo_flag == 0
    assert info.is_global_dedup_supported is True
    assert info.is_worm_supported is True
    assert info.compress_algorithm == 1
    assert info.encrypt_algorithm == 0


def test_bad_magic_raises_data_corrupt() -> None:
    data = bytearray(_build())
    data[0:4] = b"XXXX"
    with pytest.raises(DataCorruptError):
        parse_repo_info(bytes(data))


def test_bad_header_crc_raises_data_corrupt() -> None:
    data = bytearray(_build())
    data[59] ^= 0xFF  # corrupt a byte inside the header-CRC-covered region
    with pytest.raises(DataCorruptError):
        parse_repo_info(bytes(data))


def test_bad_payload_crc_raises_data_corrupt() -> None:
    data = bytearray(_build())
    # flip a byte in the JSON payload without updating either CRC
    data[70] ^= 0xFF
    with pytest.raises(DataCorruptError):
        parse_repo_info(bytes(data))


def test_truncated_payload_raises_format_error() -> None:
    data = _build()
    with pytest.raises(FormatError):
        parse_repo_info(data[:-5])


def test_too_short_raises_format_error() -> None:
    with pytest.raises(FormatError):
        parse_repo_info(b"short")


def test_missing_optional_fields_default_to_none() -> None:
    data = _build(payload_obj={"repo_type": 9})
    info = parse_repo_info(data)
    assert info.repo_type == 9
    assert info.compress_algorithm is None
    assert info.encrypt_algorithm is None
    assert info.repo_flag is None


def test_explicit_json_null_storage_algorithm_also_defaults_to_none() -> None:
    """``raw.get("storage_algorithm") or {}`` (repo_info.py's own use of
    the ``CLAUDE.md``-documented ``.get(key) or default`` convention)
    must also handle the key being *present* with a JSON ``null``, not
    just entirely absent (the case ``test_missing_optional_fields_default_to_none``
    covers) -- a plain ``raw.get("storage_algorithm", {})`` would let a
    JSON ``null`` through unchanged and crash on the next ``.get()``."""
    data = _build(payload_obj={"repo_type": 9, "storage_algorithm": None})
    info = parse_repo_info(data)
    assert info.repo_type == 9
    assert info.compress_algorithm is None
    assert info.encrypt_algorithm is None
