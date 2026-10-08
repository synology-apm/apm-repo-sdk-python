"""Property tests for ``synology_apm_repo.sdk.format.headers``: any bytes
either parse or raise a ``FormatError``. The formats built on the shell
round-trip in their own modules' property tests."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from support.format_builders import bucket_header_bytes, repo_info_bytes
from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.headers import parse_index_header, parse_json_payload_header
from unit.sdk.format_strategies import mutated, u16, u32, u64

_valid = st.builds(bucket_header_bytes, mode=u32, chunk_num=u32, major=u16) | st.builds(
    repo_info_bytes, st.binary(max_size=16), uuid=st.binary(min_size=16, max_size=16), data_size=st.none() | u64
)


@given(data=st.binary(max_size=96) | _valid | mutated(_valid), max_major=st.none() | u16)
def test_parsers_return_or_raise_format_error(data: bytes, max_major: int | None) -> None:
    with contextlib.suppress(FormatError):
        parse_index_header(data, expect_magic=b"bFiL", max_major=max_major)
    with contextlib.suppress(FormatError):
        parse_json_payload_header(data, expect_magic=b"RpiF")
