"""Unit tests for ``synology_apm_repo.sdk.format.repo_transaction``. A real
``repo_transaction.<N>``'s filename ``<N>`` can differ from its embedded
``transaction_id`` (``tests/integration/sdk/test_storage_generations.py``)."""

from __future__ import annotations

import json
import zlib

import pytest

from synology_apm_repo.sdk.errors import DataCorruptError, FormatError
from synology_apm_repo.sdk.format.repo_transaction import MAGIC, parse_repo_transaction


def _build(*, major: int = 1, minor: int = 0, payload_obj: object = None) -> bytes:
    payload_obj = (
        payload_obj
        if payload_obj is not None
        else {"transaction_id": 100, "session_id": 6, "compact_id": 0, "commit_bucket_id_array": [0] * 256}
    )
    payload = json.dumps(payload_obj).encode("utf-8")
    header = bytearray(64)
    header[0:4] = MAGIC
    header[4:6] = major.to_bytes(2, "big")
    header[6:8] = minor.to_bytes(2, "big")
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    # bytes [20,60) are always zero-filled for repo_transaction — no uuid field
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header) + payload


@pytest.mark.parametrize("transaction_id", [100, 98765])
def test_parse_round_trip(transaction_id: int) -> None:
    payload_obj = {
        "transaction_id": transaction_id,
        "session_id": 6,
        "compact_id": 0,
        "commit_bucket_id_array": [0] * 256,
    }
    txn = parse_repo_transaction(_build(payload_obj=payload_obj))
    assert txn.transaction_id == transaction_id
    assert txn.session_id == 6
    assert txn.compact_id == 0
    assert txn.raw["transaction_id"] == transaction_id


def test_bad_magic_raises_data_corrupt() -> None:
    data = bytearray(_build())
    data[0:4] = b"XXXX"
    with pytest.raises(DataCorruptError, match="bad magic"):
        parse_repo_transaction(bytes(data))


@pytest.mark.parametrize("offset", [pytest.param(59, id="bad_header_crc"), pytest.param(70, id="bad_payload_crc")])
def test_a_bad_crc_raises_data_corrupt(offset: int) -> None:
    data = bytearray(_build())
    data[offset] ^= 0xFF
    with pytest.raises(DataCorruptError, match="CRC mismatch"):
        parse_repo_transaction(bytes(data))


def test_truncated_payload_raises_format_error() -> None:
    data = _build()
    with pytest.raises(FormatError, match="repo_transaction payload truncated"):
        parse_repo_transaction(data[:-5])


def test_too_short_raises_format_error() -> None:
    with pytest.raises(FormatError, match="header too short"):
        parse_repo_transaction(b"short")


def test_missing_transaction_id_raises_data_corrupt() -> None:
    data = _build(payload_obj={"session_id": 1})
    with pytest.raises(DataCorruptError, match="repo_transaction payload has no integer transaction_id"):
        parse_repo_transaction(data)


def test_a_non_numeric_transaction_id_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="no integer transaction_id"):
        parse_repo_transaction(_build(payload_obj={"transaction_id": "abc"}))


def test_a_decimal_string_past_the_int_digit_limit_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="no integer transaction_id"):
        parse_repo_transaction(_build(payload_obj={"transaction_id": "1" * 5000}))


def test_decimal_string_ids_parse_as_integers() -> None:
    txn = parse_repo_transaction(_build(payload_obj={"transaction_id": "100", "session_id": "6", "compact_id": 0}))
    assert (txn.transaction_id, txn.session_id, txn.compact_id) == (100, 6, 0)


def test_a_payload_that_is_not_a_json_object_raises_data_corrupt() -> None:
    with pytest.raises(DataCorruptError, match="expected an object"):
        parse_repo_transaction(_build(payload_obj="100"))
