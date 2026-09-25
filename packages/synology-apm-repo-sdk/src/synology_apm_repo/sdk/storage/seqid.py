"""Sequence-id resolution (FORMAT-SPEC.md: sequence-id-suffix) — "take the largest
``.<N>`` suffix, a bare name is also valid" — for ``.buk``, ``c<subID>``,
``.inf``, ``.fgp``, ``.ref``, and similar per-generation files.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..errors import NotFoundError
from .base import join_path
from .dircache import DirCache
from .dircache import split_seq_suffix as split_seq_suffix


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
    """
    candidates = dir_index.get(logical_name)
    if not candidates:
        raise NotFoundError(f"no file matches logical name {logical_name!r}", ref=ref or logical_name)
    return max(candidates, key=_seq_key)


def _seq_key(name: str) -> int:
    """``resolve_seq_file``'s ``max()`` sort key: the name's own sequence-id
    suffix, or -1 for a bare (unsuffixed) name — ``split_seq_suffix`` only
    ever returns a non-negative int for a real suffix, so a bare name loses
    to any suffixed variant and wins only when it's the sole candidate."""
    _, seq = split_seq_suffix(name)
    return seq if seq is not None else -1


async def resolve_seq_path(dir_cache: DirCache, dir_path: str, logical_name: str) -> str:
    """``resolve_seq_file``, plus the ``dir_cache.grouped()`` lookup
    and ``dir_path``/physical-name join around it — the shared shape
    behind every "resolve one ``.<N>``-suffixed per-generation file inside
    a known directory" call site (``dedup/repository.py``,
    ``dedup/verify_checks.py``, ``dedup/pool/__init__.py``,
    ``dedup/fingerprint.py``, ``dedup/composition_reader.py``). Each
    caller still builds its own ``dir_path``/``logical_name`` split
    (layer paths split differently — see e.g.
    ``dedup/pool/__init__.py``'s own ``bucket_path``); this only covers
    the three lines common past that point.

    Not for ``units/saas/stream.py``'s own ``_resolve_db_path``, which
    looks similar but has a genuine extra rule (preferring the
    un-suffixed live file) this doesn't implement."""
    index = await dir_cache.grouped(dir_path)
    physical_name = resolve_seq_file(index, logical_name, ref=join_path(dir_path, logical_name))
    return join_path(dir_path, physical_name)
