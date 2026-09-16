"""``repo_info`` — magic ``RpiF``, 64-byte header + JSON payload
(FORMAT-SPEC.md: repo_info).

Pure ``bytes -> dataclass`` decode, zero I/O (the Codec Layer's charter) — callers
fetch the bytes via an ``ObjectStore`` and hand them to ``parse_repo_info``.

``DedupRepo.open()`` always reads and parses this file — a missing
or corrupt ``repo_info`` blocks opening the repository at all (FORMAT-SPEC.md:
repo_info) — but none of its parsed *values* ever drive a chunk-decode decision:
this file's own ``storage_algorithm.encrypt_algorithm`` is not a reliable
encryption signal (see FORMAT-SPEC.md: bucket-header for why, and
``BucketFileHeader.is_vault_encrypted``'s docstring for the actual one);
every other field here is likewise parsed for display/diagnostics only.

Generation selection for this file on S3/Azure object-storage repositories
(where ``repo_info`` may only exist as ``repo_info.<N>``, selected by the
*same* transaction-log algorithm as ``db/file_map`` — FORMAT-SPEC.md:
generation-selection) lives in ``storage/generations.py``; this module only
parses bytes once you already have them.
"""

from __future__ import annotations

import dataclasses

from .headers import MAGIC as _MAGICS
from .headers import parse_json_payload_header

MAGIC = _MAGICS["repo_info"]

_OFF_REPO_UUID = 20
_REPO_UUID_LEN = 16

_SPEC = "FORMAT-SPEC.md: repo_info"


@dataclasses.dataclass(frozen=True)
class RepoInfo:
    """Parsed ``repo_info``: header UUID plus the (display-only) JSON body
    fields listed below. ``raw`` keeps the whole decoded payload."""

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
    """Parse a ``repo_info`` file's full bytes (header + JSON payload).

    Raises:
        DataCorruptError: A magic or CRC32 mismatch.
        FormatError: The payload is truncated relative to the length the
            header declares.
    """
    header, raw = parse_json_payload_header(data, expect_magic=MAGIC, spec=_SPEC, payload_kind="repo_info")
    uuid = data[_OFF_REPO_UUID : _OFF_REPO_UUID + _REPO_UUID_LEN].decode("ascii")
    storage_algorithm = raw.get("storage_algorithm") or {}
    return RepoInfo(
        uuid=uuid,
        major=header.major,
        minor=header.minor,
        repo_type=raw.get("repo_type"),
        repo_flag=raw.get("repo_flag"),
        is_global_dedup_supported=raw.get("is_global_dedup_supported"),
        is_worm_supported=raw.get("is_worm_supported"),
        compress_algorithm=storage_algorithm.get("compress_algorithm"),
        encrypt_algorithm=storage_algorithm.get("encrypt_algorithm"),
        raw=raw,
    )
