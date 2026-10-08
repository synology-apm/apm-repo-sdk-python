"""``repo_transaction.<N>`` — magic ``rPTs``, the ``parse_json_payload_header``
shell ``repo_info.py`` also uses, except bytes [20,36) (the repo uuid there)
are always zero, followed by a small JSON payload (FORMAT-SPEC.md:
Multi-generation selection).
"""

from __future__ import annotations

import dataclasses

from .._util.jsonparse import json_int
from ..errors import DataCorruptError
from .headers import MAGIC as _MAGICS
from .headers import parse_json_payload_header

MAGIC = _MAGICS["repo_transaction"]

_SPEC = "FORMAT-SPEC.md: Multi-generation selection"


@dataclasses.dataclass(frozen=True, slots=True)
class RepoTransaction:
    """Parsed ``repo_transaction.<N>`` payload. ``session_id``/
    ``compact_id`` are carried through for completeness/diagnostics
    (``dump``-style tooling); generation selection only ever needs
    ``transaction_id``."""

    transaction_id: int
    session_id: int | None
    compact_id: int | None
    raw: dict[str, object]


def _id_field(value: object) -> int | None:
    """An id field as an int: a JSON integer, or a decimal string (a writer
    may string-encode a 64-bit id); ``None`` for anything else."""
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        try:
            return int(value)
        except ValueError:  # longer than sys.get_int_max_str_digits()
            return None
    return json_int(value)


def parse_repo_transaction(data: bytes) -> RepoTransaction:
    """Parse a ``repo_transaction.<N>`` file's full bytes (header + JSON
    payload).

    Raises:
        DataCorruptError: A magic, header-CRC, or payload-CRC mismatch, a
            payload that is not a JSON object, or one whose
            ``transaction_id`` is neither an integer nor a decimal string
            short enough to convert.
        FormatError: ``data`` is shorter than the 64-byte header, or the
            payload is truncated relative to the length the header declares.
    """
    _, raw = parse_json_payload_header(data, expect_magic=MAGIC, spec=_SPEC, payload_kind="repo_transaction")
    transaction_id = _id_field(raw.get("transaction_id"))
    if transaction_id is None:
        raise DataCorruptError("repo_transaction payload has no integer transaction_id", spec=_SPEC)

    return RepoTransaction(
        transaction_id=transaction_id,
        session_id=_id_field(raw.get("session_id")),
        compact_id=_id_field(raw.get("compact_id")),
        raw=raw,
    )
