"""Unit tests for ``synology_apm_repo.sdk.format.repo_info``
(``tests/integration/sdk/test_format_repo_info.py`` parses real samples'
files)."""

from __future__ import annotations

import json
import zlib

import pytest

from support.format_builders import repo_info_bytes
from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.repo_info import MAGIC, parse_repo_info


def _build(
    *,
    uuid: str = "abcdefghijklmnop",
    major: int = 2,
    minor: int = 2,
    payload_obj: object = None,
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
    with pytest.raises(DataCorruptError, match="bad magic"):
        parse_repo_info(bytes(data))


@pytest.mark.parametrize(
    "offset",
    [
        pytest.param(59, id="bad_header_crc"),  # inside the header-CRC-covered region
        pytest.param(70, id="bad_payload_crc"),  # in the JSON payload, neither CRC updated
    ],
)
def test_a_bad_crc_raises_data_corrupt(offset: int) -> None:
    data = bytearray(_build())
    data[offset] ^= 0xFF
    with pytest.raises(DataCorruptError, match="CRC mismatch"):
        parse_repo_info(bytes(data))


def test_truncated_payload_raises_format_error() -> None:
    data = _build()
    with pytest.raises(FormatError, match="repo_info payload truncated"):
        parse_repo_info(data[:-5])


def test_a_non_ascii_uuid_raises_data_corrupt() -> None:
    data = repo_info_bytes({}, uuid=bytes(15) + b"\x80")
    with pytest.raises(DataCorruptError, match=r"repo_info uuid .* is not ASCII"):
        parse_repo_info(data)


def test_too_short_raises_format_error() -> None:
    with pytest.raises(FormatError, match="header too short"):
        parse_repo_info(b"short")


def test_missing_optional_fields_default_to_none() -> None:
    data = _build(payload_obj={"repo_type": 9})
    info = parse_repo_info(data)
    assert info.repo_type == 9
    assert info.compress_algorithm is None
    assert info.encrypt_algorithm is None
    assert info.repo_flag is None


def test_explicit_json_null_storage_algorithm_also_defaults_to_none() -> None:
    """The key present with a JSON ``null`` reads like an absent one."""
    data = _build(payload_obj={"repo_type": 9, "storage_algorithm": None})
    info = parse_repo_info(data)
    assert info.repo_type == 9
    assert info.compress_algorithm is None
    assert info.encrypt_algorithm is None


def test_a_payload_that_is_not_a_json_object_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="expected an object"):
        parse_repo_info(_build(payload_obj=[1, 2]))


def test_body_fields_of_the_wrong_json_type_read_as_none() -> None:
    info = parse_repo_info(
        _build(
            payload_obj={
                "repo_type": "2",
                "repo_flag": True,
                "is_worm_supported": 1,
                "storage_algorithm": "zstd",
            }
        )
    )
    assert info.repo_type is None
    assert info.repo_flag is None
    assert info.is_worm_supported is None
    assert info.compress_algorithm is None
