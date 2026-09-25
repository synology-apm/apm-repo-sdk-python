"""``CategorizedGroupTree``: a synthetic wrapper layered on top of any
other shape in this package whose own root lists named groups — a
further synthetic split on top of the two schema shapes every
service-level DB in this project expands into.
"""

from __future__ import annotations

from ...base import paginate
from ._base import TreeStrategy, _Key, _Row


def categories_present_in_order(categories: dict[str, str], labels: dict[str, str]) -> list[str]:
    """The distinct category tokens actually used in ``categories`` (a
    member -> category mapping), ordered canonically by ``labels``' own
    key order rather than by whichever member was scanned first —
    dropping an entirely-empty category (e.g. no "Shared Channels" in
    this workload) is noise, not information, the same reasoning every
    caller of this applies. Shared by ``CategorizedGroupTree`` below and
    ``teams_chat.py``'s own Standard/Private/Shared channel split, which
    can't sit on top of ``CategorizedGroupTree`` itself (that class
    wraps a ``TreeStrategy``, a protocol ``TeamsChatProvider`` isn't
    built on — it hands out ``Node`` values directly) but needs the identical
    dedup/ordering rule, so a fix to it can't drift between the two."""
    present = set(categories.values())
    return [category for category in labels if category in present]


class CategorizedGroupTree:
    """Wraps any inner ``TreeStrategy`` whose own root (``children_of(())``)
    lists named groups, adding one extra synthetic level that splits those
    groups into caller-supplied categories — Site's Document Library/List
    split, Calendar's My Calendars/Other Calendars split. Every key this
    class hands out or accepts is ``(category, *inner_key)``; the wrapped
    tree only ever sees ``inner_key``, so this works identically regardless
    of whether the inner tree is flat (``NamedGroupFlatTree``) or itself
    recursive within each group (``NamedGroupRecursiveTree``) — nothing
    here is specific to either shape.

    ``categories`` maps each of the inner tree's own top-level group keys
    (e.g. a ``list_id``/``calendar_id``) to a category token; ``labels``
    maps each category token to its displayed name. Both are computed once
    by the caller's own ``tree_factory`` (a small, bounded scan of however
    many groups this workload has), not by this class."""

    def __init__(
        self,
        inner: TreeStrategy,
        *,
        categories: dict[str, str],
        labels: dict[str, str],
    ) -> None:
        self._inner = inner
        self._categories = categories
        self._labels = labels
        # Memoized by _all_inner_groups -- the inner tree's own root
        # listing never changes within one provider's lifetime (same
        # table, same open connection), so a category listing's own
        # repeated pagination pages (or a second category, sharing the
        # same unfiltered scan) don't each need their own real query.
        self._all_groups_cache: list[tuple[_Key, str, bool]] | None = None

    async def _all_inner_groups(self) -> list[tuple[_Key, str, bool]]:
        if self._all_groups_cache is None:
            self._all_groups_cache = await self._inner.children_of((), offset=0, limit=None)
        return self._all_groups_cache

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        if key == ():
            entries = [
                ((category,), self._labels[category], False)
                for category in categories_present_in_order(self._categories, self._labels)
            ]
            return paginate(entries, offset, limit)
        category, *rest = key
        if not rest:
            all_groups = await self._all_inner_groups()
            in_category = [
                ((category, *inner_key), name, is_leaf)
                for inner_key, name, is_leaf in all_groups
                if self._categories.get(inner_key[0]) == category
            ]
            return paginate(in_category, offset, limit)
        inner_children = await self._inner.children_of(tuple(rest), offset=offset, limit=limit)
        return [((category, *inner_key), name, is_leaf) for inner_key, name, is_leaf in inner_children]

    def row_for(self, key: _Key) -> _Row | None:
        if len(key) < 2:
            return None
        _category, *rest = key
        return self._inner.row_for(tuple(rest))
