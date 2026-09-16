"""``repo_transaction.<N>`` — magic ``rPTs``, the same generic
``RepoJsonFileHeader`` shell ``repo_info.py`` uses (crc@8, data_size BE
u64 @12), minus a meaningful repo-uuid field (bytes [20,36) are always
zero-filled here) — followed by a small JSON payload (FORMAT-SPEC.md:
generation-selection).
"""

from __future__ import annotations

import dataclasses

from ..errors import DataCorruptError
from .headers import MAGIC as _MAGICS
from .headers import parse_json_payload_header

MAGIC = _MAGICS["repo_transaction"]

_SPEC = "FORMAT-SPEC.md: generation-selection"


@dataclasses.dataclass(frozen=True)
class RepoTransaction:
    """Parsed ``repo_transaction.<N>`` payload. ``session_id``/
    ``compact_id`` are carried through for completeness/diagnostics
    (``dump``-style tooling); generation selection only ever needs
    ``transaction_id``."""

    transaction_id: int
    session_id: int | None
    compact_id: int | None
    raw: dict[str, object]


def parse_repo_transaction(data: bytes) -> RepoTransaction:
    """Parse a ``repo_transaction.<N>`` file's full bytes (header + JSON
    payload).

    Raises:
        DataCorruptError: A magic, header-CRC, or payload-CRC mismatch, or a
            payload missing ``transaction_id`` entirely.
        FormatError: The payload is truncated relative to the length the
            header declares.
    """
    # header's own fields are unused here (see this module's docstring)
    _, raw = parse_json_payload_header(data, expect_magic=MAGIC, spec=_SPEC, payload_kind="repo_transaction")
    if "transaction_id" not in raw:
        raise DataCorruptError("repo_transaction payload missing transaction_id", spec=_SPEC)

    return RepoTransaction(
        transaction_id=int(raw["transaction_id"]),
        session_id=raw.get("session_id"),
        compact_id=raw.get("compact_id"),
        raw=raw,
    )
