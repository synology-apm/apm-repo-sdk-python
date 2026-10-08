"""Listing/probing helpers shared by ``S3Store``/``AzureStore`` (the
prefix-addressed, directory-less backends) for ``ObjectStore.listdir``/
``exists``. Each backend keeps its own pagination and exception translation.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable

from .base import Entry


def as_list_prefix(key: str) -> str:
    """``key`` as a trailing-slash-terminated list prefix; an empty ``key``
    (the store root) stays empty."""
    return f"{key}/" if key else ""


def sorted_relative_entries(raw_entries: Iterable[tuple[str, int | None]], list_prefix: str) -> list[Entry]:
    """``(full key, size)`` pairs sharing ``list_prefix`` (already
    delimiter-grouped) reduced to ``listdir()``'s form: ``list_prefix`` and
    any trailing ``"/"`` trimmed, the prefix marker itself dropped, sorted by
    name, each keeping the size it was listed with."""
    trimmed = (Entry(name.removeprefix(list_prefix).rstrip("/"), size) for name, size in raw_entries)
    return sorted((entry for entry in trimmed if entry.name), key=lambda entry: entry.name)


async def exists_via_prefix_probe[E: BaseException](
    *,
    head: Callable[[], Awaitable[object]],
    error_type: type[E],
    is_not_found: Callable[[E], bool],
    probe_prefix: Callable[[], Awaitable[bool]],
) -> bool:
    """``ObjectStore.exists()`` for a directory-less backend: ``head()`` for
    an object exactly at the path, falling back to ``probe_prefix()`` (a
    "directory") only when ``head()`` raises ``error_type`` and
    ``is_not_found`` recognizes it as absent. Other failures, such as
    permission errors, propagate.
    """
    try:
        await head()
        return True
    except error_type as exc:
        if not is_not_found(exc):
            raise
    return await probe_prefix()
