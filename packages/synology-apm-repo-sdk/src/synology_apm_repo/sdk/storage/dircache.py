"""``DirCache`` — mandatory directory-listing cache.

``resolve_seq_file`` needs "logical name -> [physical names]" lookups for
every per-generation file (``.buk``, ``.inf``, ``.fgp``, ``.ref``, ...).
Re-``listdir()``-ing the same directory on every such lookup turns "export
one image that touches a few hundred buckets" into O(n^2) directory scans —
on an object-storage backend, hundreds of *paginated list-object API
calls*. This is a correctness-adjacent performance requirement, not an
optional optimization: a ``Session`` holds exactly one ``DirCache`` per
open repository for its whole lifetime; only an explicit ``invalidate()``
(e.g. the TUI's "refresh" action) forces a re-scan.

``generations.py``'s S3/Azure ``db/<name>`` generation selection is a
separate mechanism with its own transaction-log-based rule — it calls
``store.listdir()`` directly and does not go through this cache.
"""

from __future__ import annotations

import re

from ..asynccache import AsyncKeyedCache
from .base import ObjectStore

_SEQ_SUFFIX_RE = re.compile(r"^(?P<base>.+)\.(?P<seq>\d+)$")


def split_seq_suffix(name: str) -> tuple[str, int | None]:
    """Split a trailing ``.<digits>`` sequence-id suffix off ``name``
    (FORMAT-SPEC.md: sequence-id-suffix). Returns ``(name, None)`` unchanged if
    there is no such suffix.

    Note the suffix is only ever the *last* dot-segment: ``"132.buk.1"`` ->
    ``("132.buk", 1)`` — the base name ``"132.buk"`` itself legitimately
    contains a dot. ``"132.buk"`` (no suffix) -> ``("132.buk", None)``.
    """
    m = _SEQ_SUFFIX_RE.match(name)
    if m is None:
        return name, None
    return m.group("base"), int(m.group("seq"))


class DirCache:
    """Per-repository cache of directory listings, keyed by store-relative
    directory path.

    Both ``_raw`` and ``_grouped`` are ``AsyncKeyedCache`` instances —
    unbounded, session-wide shared. Two Tasks racing on the same cold
    directory only pay for one listing: the second awaits the first's
    still-in-flight ``store.listdir()`` future instead of issuing its own,
    and both end up with the same result (or exception).
    """

    def __init__(self, store: ObjectStore) -> None:
        self._store = store
        # A lambda, not ``store.listdir`` directly: some callers construct a
        # DirCache with a placeholder store whose ``.listdir`` is never
        # actually meant to be touched unless a real lookup happens —
        # binding the bound method eagerly here would resolve that
        # attribute at construction time instead of on first real use.
        self._raw: AsyncKeyedCache[str, list[str]] = AsyncKeyedCache(lambda dir_path: store.listdir(dir_path))
        self._grouped: AsyncKeyedCache[str, dict[str, list[str]]] = AsyncKeyedCache(self._build_grouped)

    async def listdir(self, dir_path: str) -> list[str]:
        """Cached passthrough to ``store.listdir(dir_path)``."""
        return await self._raw.resolve(dir_path)

    async def grouped(self, dir_path: str) -> dict[str, list[str]]:
        """Return ``{logical_name: [physical_name, ...]}`` for ``dir_path``,
        built from exactly one ``listdir`` call and cached thereafter.
        ``logical_name`` is each entry with any trailing sequence-id suffix
        stripped (``split_seq_suffix``) — a directory entry with no suffix
        maps to itself.
        """
        return await self._grouped.resolve(dir_path)

    async def _build_grouped(self, dir_path: str) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        for name in await self.listdir(dir_path):
            base, _ = split_seq_suffix(name)
            index.setdefault(base, []).append(name)
        return index

    async def invalidate(self, dir_path: str | None = None) -> None:
        """Drop cached entries for ``dir_path`` (or everything, if omitted).

        Stays ``async def`` (nothing here actually awaits) purely to
        preserve the existing ``await dir_cache.invalidate(...)`` call
        shape at every call site.
        """
        self._raw.invalidate(dir_path)
        self._grouped.invalidate(dir_path)
