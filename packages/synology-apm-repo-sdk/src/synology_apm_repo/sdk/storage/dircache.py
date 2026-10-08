"""``DirCache`` — the directory-listing cache behind ``resolve_seq_file``'s
"logical name -> [physical names]" lookups for per-generation files
(``.buk``, ``.inf``, ``.fgp``, ``.ref``, ...).

Re-listing a directory on every lookup would make an export touching a few
hundred buckets do O(n^2) scans (hundreds of paginated list calls on object
storage), so each opened repository keeps ``DirCache`` instances for its
lifetime, bounded by count; ``invalidate()`` (reached through
``CacheManager``) forces a re-scan.

``generations.py``'s ``db/<name>`` generation selection lists through this
cache's flat ``listdir`` (``DedupRepo``'s fixed-directory instance), so the
several ``db/<name>`` resolutions of one repository share three listings.
"""

from __future__ import annotations

import dataclasses
import re

from ..asynccache import AsyncKeyedCache, CacheStats
from ..cachemanager import DEFAULT_LIMITS
from .base import ObjectStore

_SEQ_SUFFIX_RE = re.compile(r"^(?P<base>.+)\.(?P<seq>\d+)$")


def split_seq_suffix(name: str) -> tuple[str, int | None]:
    """Split a trailing ``.<digits>`` sequence-id suffix off ``name``
    (FORMAT-SPEC.md: Sequence-id suffix mechanism). Returns ``(name, None)``
    if there is none. Only the last dot-segment counts: ``"132.buk.1"`` ->
    ``("132.buk", 1)``, ``"132.buk"`` -> ``("132.buk", None)``.
    """
    m = _SEQ_SUFFIX_RE.match(name)
    if m is None:
        return name, None
    return m.group("base"), int(m.group("seq"))


@dataclasses.dataclass(frozen=True, slots=True)
class _Listing:
    """One directory as listed: the grouped index, plus the size of every
    file the backend reported one for (physical name -> bytes)."""

    grouped: dict[str, list[str]]
    sizes: dict[str, int]


class DirCache:
    """Bounded cache of directory listings, keyed by store-relative directory
    path. One listing per cached directory; the grouped index and any sizes
    the backend reported with it are kept, and ``listdir`` is derived from
    them, so a directory costs one entry however it is asked for.

    ``maxsize`` bounds the directories held (LRU). Tasks racing on the same
    cold directory share one listing.
    """

    def __init__(self, store: ObjectStore, *, maxsize: int = DEFAULT_LIMITS.dir_scan) -> None:
        self._store = store
        self._listings: AsyncKeyedCache[str, _Listing] = AsyncKeyedCache(self._build_listing, maxsize=maxsize)

    async def listdir(self, dir_path: str) -> list[str]:
        """The entry names ``store.listdir(dir_path)`` reports, sorted, from
        the cached listing."""
        index = await self.grouped(dir_path)
        return sorted(name for names in index.values() for name in names)

    async def grouped(self, dir_path: str) -> dict[str, list[str]]:
        """``{logical_name: [physical_name, ...]}`` for ``dir_path``.
        ``logical_name`` is the entry with any sequence-id suffix stripped
        (``split_seq_suffix``).
        """
        return (await self._listings.resolve(dir_path)).grouped

    async def size_of(self, dir_path: str, name: str) -> int | None:
        """The size of ``name`` (a physical entry name) in ``dir_path`` as the
        listing itself reported it, with no extra request; ``None`` when the
        listing had no size for it, ``name`` is a directory, or it is absent."""
        return (await self._listings.resolve(dir_path)).sizes.get(name)

    async def _build_listing(self, dir_path: str) -> _Listing:
        index: dict[str, list[str]] = {}
        sizes: dict[str, int] = {}
        for name, size in await self._store.listdir(dir_path):
            base, _ = split_seq_suffix(name)
            index.setdefault(base, []).append(name)
            if size is not None:
                sizes[name] = size
        return _Listing(grouped=index, sizes=sizes)

    def stats(self) -> CacheStats:
        """Counters of the underlying directory cache."""
        return self._listings.stats()

    def invalidate(self, dir_path: str | None = None) -> None:
        """Drop the cached entry for ``dir_path`` (or everything, if omitted)."""
        self._listings.invalidate(dir_path)
