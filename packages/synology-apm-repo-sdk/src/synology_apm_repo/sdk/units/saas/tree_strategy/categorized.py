"""``CategorizedGroupTree``: a synthetic category level over another
tree's top-level entries.
"""

from __future__ import annotations

from ....units.provider_kit import paginate
from ._base import Key, TreeEntry, TreeStrategy


def categories_present_in_order(categories: dict[str, str], labels: dict[str, str]) -> list[str]:
    """The category tokens used in ``categories`` (member -> category), in
    ``labels``' key order; empty categories are dropped."""
    present = set(categories.values())
    return [category for category in labels if category in present]


class CategorizedGroupTree:
    """Wraps an inner ``TreeStrategy``, adding a category level above its
    top-level entries (Site's Document Library/List,
    Calendar's My/Other Calendars, Teams' channel types). Keys are
    ``(category, *inner_key)``; the inner tree sees only ``inner_key``.

    ``categories`` maps each inner top-level id to a category token, and
    must cover every entry to be listed; ``labels`` maps each token to
    its displayed name, in display order."""

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
        # The inner root listing is fixed for a provider's lifetime.
        self._all_groups_cache: list[TreeEntry] | None = None

    async def _all_inner_groups(self) -> list[TreeEntry]:
        if self._all_groups_cache is None:
            self._all_groups_cache = await self._inner.children_of((), offset=0, limit=None)
        return self._all_groups_cache

    async def children_of(self, key: Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        if key == ():
            entries = [
                TreeEntry((category,), self._labels[category], False)
                for category in categories_present_in_order(self._categories, self._labels)
            ]
            return paginate(entries, offset, limit)
        category, *rest = key
        if not rest:
            all_groups = await self._all_inner_groups()
            in_category = [
                entry._replace(key=(category, *entry.key))
                for entry in all_groups
                if self._categories.get(entry.key[0]) == category
            ]
            return paginate(in_category, offset, limit)
        inner_children = await self._inner.children_of(tuple(rest), offset=offset, limit=limit)
        return [entry._replace(key=(category, *entry.key)) for entry in inner_children]
