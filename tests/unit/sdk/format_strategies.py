"""Hypothesis strategies the ``test_format_*_properties.py`` files share."""

from __future__ import annotations

from hypothesis import strategies as st

u16 = st.integers(0, 0xFFFF)
u32 = st.integers(0, 0xFFFF_FFFF)
u64 = st.integers(0, 0xFFFF_FFFF_FFFF_FFFF)

json_scalars: st.SearchStrategy[object] = st.none() | st.booleans() | st.integers() | st.text(max_size=8)
"""Every JSON scalar a payload field can hold (no floats: none of the
parsed fields is one)."""


def _flip(data: bytes, position: int, mask: int) -> bytes:
    return data[:position] + bytes([data[position] ^ mask]) + data[position + 1 :]


def mutated(valid: st.SearchStrategy[bytes]) -> st.SearchStrategy[bytes]:
    """A valid encoding from ``valid`` damaged once: cut short at any
    length, or one byte XORed with a non-zero mask."""

    def damage(data: bytes) -> st.SearchStrategy[bytes]:
        truncated = st.integers(0, max(len(data) - 1, 0)).map(lambda length: data[:length])
        if not data:
            return truncated
        flipped = st.builds(_flip, st.just(data), st.integers(0, len(data) - 1), st.integers(1, 0xFF))
        return truncated | flipped

    return valid.flatmap(damage)
