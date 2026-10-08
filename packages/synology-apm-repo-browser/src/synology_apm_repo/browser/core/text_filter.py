"""The one text-filter rule every ``/`` filter in the Browser applies."""

from __future__ import annotations


def matches_filter(needle: str, text: str) -> bool:
    """Whether ``text`` survives a filter of ``needle``: a case-insensitive
    substring match, where an empty ``needle`` keeps everything."""
    return not needle or needle.lower() in text.lower()
