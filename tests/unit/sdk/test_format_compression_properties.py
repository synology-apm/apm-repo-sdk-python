"""Property tests for ``synology_apm_repo.sdk.format.compression``: a
compressed 4096-byte chunk round-trips through every decompression entry
point, and any bytes either decompress or raise a ``FormatError``."""

from __future__ import annotations

import contextlib

import lz4.block
import zstandard
from hypothesis import given
from hypothesis import strategies as st

from synology_apm_repo.sdk.errors import FormatError
from synology_apm_repo.sdk.format.compression import (
    CompressType,
    decompress,
    decompress_many,
    decompress_zstd_stream,
    zstd_content_size,
)
from unit.sdk.format_strategies import mutated

# A whole chunk from a short repeated pattern: compressible, and cheap to draw.
_chunk = st.binary(min_size=1, max_size=48).map(lambda pattern: (pattern * (4096 // len(pattern) + 1))[:4096])


def _zstd(plain: bytes) -> bytes:
    return zstandard.ZstdCompressor().compress(plain)


def _lz4(plain: bytes) -> bytes:
    compressed: bytes = lz4.block.compress(plain, store_size=False)
    return compressed


@given(_chunk)
def test_a_chunk_round_trips(plain: bytes) -> None:
    stored = {CompressType.NONE: plain, CompressType.LZ4: _lz4(plain), CompressType.ZSTD: _zstd(plain)}
    for ctype, data in stored.items():
        assert decompress(ctype, data) == plain
    assert [bytes(chunk) for chunk in decompress_many(list(stored.items()))] == [plain] * len(stored)
    assert zstd_content_size(stored[CompressType.ZSTD]) == len(plain)
    assert decompress_zstd_stream(stored[CompressType.ZSTD], max_output_size=None) == plain


_frame = _chunk.map(_zstd) | _chunk.map(_lz4)


@given(ctype=st.sampled_from(list(CompressType)), data=st.binary(max_size=64) | mutated(_frame))
def test_decompression_returns_or_raises_format_error(ctype: CompressType, data: bytes) -> None:
    """``decompress_zstd_stream`` and ``zstd_content_size`` document a
    malformed frame as ``zstandard.ZstdError``."""
    with contextlib.suppress(FormatError):
        decompress(ctype, data)
    with contextlib.suppress(FormatError):
        decompress_many([(ctype, data)])
    with contextlib.suppress(FormatError, zstandard.ZstdError):
        decompress_zstd_stream(data, max_output_size=8192)
    with contextlib.suppress(zstandard.ZstdError):
        zstd_content_size(data)
