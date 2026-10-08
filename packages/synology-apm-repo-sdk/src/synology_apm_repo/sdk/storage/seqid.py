"""Sequence-id resolution (FORMAT-SPEC.md: Sequence-id suffix mechanism) — "take the largest
``.<N>`` suffix, a bare name is also valid" — for ``.buk``, ``c<subID>``,
``.inf``, ``.fgp``, ``.ref``, and similar per-generation files.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..errors import NotFoundError
from .base import join_path
from .dircache import DirCache, split_seq_suffix


def resolve_seq_file(
    dir_index: Mapping[str, list[str]],
    logical_name: str,
    *,
    ref: str | None = None,
) -> str:
    """Resolve ``logical_name`` to the physical file name carrying the
    largest numeric sequence-id suffix, given a ``{logical_name:
    [physical_names]}`` index (``DirCache.grouped``).

    A bare, unsuffixed name is a valid candidate and wins if no suffixed
    variant exists.

    Raises:
        NotFoundError: No entry matches ``logical_name``; ``ref`` (default
            ``logical_name``) names it in the error.
    """
    candidates = dir_index.get(logical_name)
    if not candidates:
        raise NotFoundError(f"no file matches logical name {logical_name!r}", ref=ref or logical_name)
    return max(candidates, key=_seq_key)


def _seq_key(name: str) -> int:
    """``resolve_seq_file``'s sort key: the sequence-id suffix, or -1 for a
    bare name, so a bare name loses to any suffixed variant."""
    _, seq = split_seq_suffix(name)
    return seq if seq is not None else -1


async def resolve_seq_size(dir_cache: DirCache, dir_path: str, logical_name: str) -> int | None:
    """The size of the file ``resolve_seq_path`` would pick, taken from the
    directory listing itself (no ``size`` request); ``None`` if the backend
    lists no sizes.

    Raises:
        NotFoundError: No file matches ``logical_name`` in ``dir_path``.
    """
    index = await dir_cache.grouped(dir_path)
    physical_name = resolve_seq_file(index, logical_name, ref=join_path(dir_path, logical_name))
    return await dir_cache.size_of(dir_path, physical_name)


async def resolve_seq_path(dir_cache: DirCache, dir_path: str, logical_name: str) -> str:
    """The store-relative path of the file ``resolve_seq_file`` picks for
    ``logical_name`` in ``dir_path``, via ``dir_cache``'s listing.

    Raises:
        NotFoundError: No file matches ``logical_name`` in ``dir_path``.
    """
    index = await dir_cache.grouped(dir_path)
    physical_name = resolve_seq_file(index, logical_name, ref=join_path(dir_path, logical_name))
    return join_path(dir_path, physical_name)
