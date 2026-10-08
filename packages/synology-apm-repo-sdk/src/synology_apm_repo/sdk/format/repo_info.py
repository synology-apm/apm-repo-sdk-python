"""``repo_info`` — magic ``RpiF``, 64-byte header + JSON payload
(FORMAT-SPEC.md: repo_info).

Pure ``bytes -> dataclass`` decode; callers fetch the bytes via an
``ObjectStore`` and hand them to ``parse_repo_info``.

``DedupRepo.open()`` always parses this file, and a missing or corrupt one
blocks opening. No parsed value drives a chunk-decode decision:
``storage_algorithm.encrypt_algorithm`` is not a reliable encryption signal
(FORMAT-SPEC.md: Header & mode bits; see
``BucketFileHeader.is_vault_encrypted``), and every other field is for
display and diagnostics.

Generation selection (``repo_info.<N>`` on S3/Azure, FORMAT-SPEC.md:
Multi-generation selection) lives in ``storage/generations.py``.
"""

from __future__ import annotations

import dataclasses

from .._util.jsonparse import json_bool, json_int
from ..errors import DataCorruptError
from .headers import MAGIC as _MAGICS
from .headers import parse_json_payload_header

MAGIC = _MAGICS["repo_info"]

_OFF_REPO_UUID = 20
_REPO_UUID_LEN = 16

_SPEC = "FORMAT-SPEC.md: repo_info"


@dataclasses.dataclass(frozen=True, slots=True)
class RepoInfo:
    """Parsed ``repo_info``: header UUID plus the display-only JSON body
    fields below; ``raw`` keeps the whole decoded payload."""

    uuid: str
    major: int
    minor: int
    repo_type: int | None
    repo_flag: int | None
    is_global_dedup_supported: bool | None
    is_worm_supported: bool | None
    compress_algorithm: int | None
    encrypt_algorithm: int | None
    raw: dict[str, object]


def parse_repo_info(data: bytes) -> RepoInfo:
    """Parse a ``repo_info`` file's full bytes (header + JSON payload). A
    body field of the wrong JSON type reads as ``None``.

    Raises:
        DataCorruptError: A magic or CRC32 mismatch, a uuid that is not
            ASCII, or a payload that is not a JSON object.
        FormatError: ``data`` is shorter than the 64-byte header, or the
            payload is truncated relative to the length the header declares.
    """
    header, raw = parse_json_payload_header(data, expect_magic=MAGIC, spec=_SPEC, payload_kind="repo_info")
    raw_uuid = data[_OFF_REPO_UUID : _OFF_REPO_UUID + _REPO_UUID_LEN]
    if not raw_uuid.isascii():
        raise DataCorruptError(f"repo_info uuid {raw_uuid!r} is not ASCII", spec=_SPEC)
    uuid = raw_uuid.decode("ascii")
    storage_algorithm = raw.get("storage_algorithm")
    if not isinstance(storage_algorithm, dict):
        storage_algorithm = {}
    return RepoInfo(
        uuid=uuid,
        major=header.major,
        minor=header.minor,
        repo_type=json_int(raw.get("repo_type")),
        repo_flag=json_int(raw.get("repo_flag")),
        is_global_dedup_supported=json_bool(raw.get("is_global_dedup_supported")),
        is_worm_supported=json_bool(raw.get("is_worm_supported")),
        compress_algorithm=json_int(storage_algorithm.get("compress_algorithm")),
        encrypt_algorithm=json_int(storage_algorithm.get("encrypt_algorithm")),
        raw=raw,
    )
