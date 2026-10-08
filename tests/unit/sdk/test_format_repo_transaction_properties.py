"""Property tests for ``synology_apm_repo.sdk.format.repo_transaction``:
every ``support.format_builders`` encoding round-trips, and any bytes either
parse or raise a ``FormatError``."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import repo_transaction_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.repo_transaction import parse_repo_transaction
from unit.sdk.format_strategies import json_scalars, mutated, u16, u32, u64

_id_value = json_scalars | u64 | u64.map(str)


def _as_id(value: object) -> int | None:
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


@given(
    transaction_id=u64 | u64.map(str),
    ids=st.dictionaries(st.sampled_from(["session_id", "compact_id"]), _id_value),
    major=u16,
    minor=u16,
)
def test_repo_transaction_round_trips(
    transaction_id: int | str, ids: dict[str, object], major: int, minor: int
) -> None:
    """An id reads as an integer from a JSON integer or a decimal string,
    else as ``None``."""
    payload = {"transaction_id": transaction_id, **ids}
    txn = parse_repo_transaction(repo_transaction_bytes(payload, major=major, minor=minor))
    assert (txn.transaction_id, txn.raw) == (int(transaction_id), payload)
    assert (txn.session_id, txn.compact_id) == (_as_id(ids.get("session_id")), _as_id(ids.get("compact_id")))


_payload = st.dictionaries(st.sampled_from(["transaction_id", "session_id"]) | st.text(max_size=4), _id_value)
_any_fields = st.builds(
    repo_transaction_bytes,
    _payload | st.binary(max_size=24),
    data_size=st.none() | u64,
    json_crc=st.none() | u32,
)


@given(st.binary(max_size=96) | _any_fields | mutated(st.builds(repo_transaction_bytes, _payload)))
def test_parse_returns_or_raises_format_error(data: bytes) -> None:
    with contextlib.suppress(FormatError):
        parse_repo_transaction(data)
