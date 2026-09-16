"""Shared listing/probing shapes ``S3Store``/``AzureStore`` (the two
prefix-addressed, directory-less backends) both need to implement
``ObjectStore.listdir``/``ObjectStore.exists`` — each backend still owns
its own pagination mechanics and exception translation; only the parts
that are identical regardless of backend live here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

_E = TypeVar("_E", bound=BaseException)


def as_list_prefix(key: str) -> str:
    """``key`` (possibly empty, for a listing/probe at the store's root)
    turned into the trailing-slash-terminated prefix both backends'
    ``listdir``/``exists`` list/probe calls key off — empty stays empty
    rather than becoming a bare ``"/"``."""
    return f"{key}/" if key else ""


def sorted_relative_names(raw_names: Iterable[str], list_prefix: str) -> list[str]:
    """``raw_names`` (absolute keys/blob names sharing ``list_prefix``,
    already delimiter-grouped by the caller's own backend-specific
    listing call) reduced to ``listdir()``'s own contract: each name
    with ``list_prefix`` and any trailing ``"/"`` trimmed off, the
    prefix marker itself (which trims to ``""``) dropped, sorted."""
    trimmed = (name.removeprefix(list_prefix).rstrip("/") for name in raw_names)
    return sorted(name for name in trimmed if name)


async def exists_via_prefix_probe(
    *,
    head: Callable[[], Awaitable[object]],
    error_type: type[_E],
    is_not_found: Callable[[_E], bool],
    probe_prefix: Callable[[], Awaitable[bool]],
) -> bool:
    """``ObjectStore.exists()``'s shared shape for a backend with no real
    directory entities: a direct ``head()`` lookup for an object exactly
    at the path, falling back to ``probe_prefix()`` (at least one object
    under it, i.e. a "directory") only when ``head()`` fails with
    ``error_type`` and ``is_not_found`` recognizes it as absent rather
    than some other failure (a permissions error, say, is never treated
    as "does not exist"). ``error_type`` is caught exactly as narrowly as
    each backend's own ``exists()`` always has — never a bare
    ``Exception`` — so nothing here changes which failures propagate.
    """
    try:
        await head()
        return True
    except error_type as exc:
        if not is_not_found(exc):
            raise
    return await probe_prefix()
