"""Property tests for ``synology_apm_repo.sdk.format.repo_info``: every
``support.format_builders`` encoding round-trips, and any bytes either parse
or raise a ``FormatError``."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import repo_info_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.repo_info import parse_repo_info
from unit.sdk.format_strategies import json_scalars, mutated, u16, u32, u64

_INT_FIELDS = ("repo_type", "repo_flag")
_BOOL_FIELDS = ("is_global_dedup_supported", "is_worm_supported")
_ALGORITHM_FIELDS = ("compress_algorithm", "encrypt_algorithm")

_ascii_uuid = st.text(st.characters(max_codepoint=0x7F), min_size=16, max_size=16)
_payload = st.dictionaries(
    st.sampled_from([*_INT_FIELDS, *_BOOL_FIELDS, "storage_algorithm"]) | st.text(max_size=4),
    json_scalars | st.dictionaries(st.sampled_from(_ALGORITHM_FIELDS), json_scalars),
    max_size=6,
)


def _as_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


@given(payload=_payload, uuid=_ascii_uuid, major=u16, minor=u16)
def test_repo_info_round_trips(payload: dict[str, object], uuid: str, major: int, minor: int) -> None:
    """Each body field reads as its value when of the right JSON type, else
    as ``None``."""
    info = parse_repo_info(repo_info_bytes(payload, uuid=uuid.encode("ascii"), version=major, minor=minor))
    assert (info.uuid, info.major, info.minor, info.raw) == (uuid, major, minor, payload)
    assert (info.repo_type, info.repo_flag) == tuple(_as_int(payload.get(field)) for field in _INT_FIELDS)
    assert (info.is_global_dedup_supported, info.is_worm_supported) == tuple(
        value if isinstance(value := payload.get(field), bool) else None for field in _BOOL_FIELDS
    )
    algorithm = payload.get("storage_algorithm")
    algorithm_fields = algorithm if isinstance(algorithm, dict) else {}
    assert (info.compress_algorithm, info.encrypt_algorithm) == tuple(
        _as_int(algorithm_fields.get(field)) for field in _ALGORITHM_FIELDS
    )


_any_fields = st.builds(
    repo_info_bytes,
    _payload | st.binary(max_size=24),
    uuid=st.binary(min_size=16, max_size=16),
    data_size=st.none() | u64,
    json_crc=st.none() | u32,
)


@given(st.binary(max_size=96) | _any_fields | mutated(st.builds(repo_info_bytes, _payload)))
def test_parse_returns_or_raises_format_error(data: bytes) -> None:
    with contextlib.suppress(FormatError):
        parse_repo_info(data)
