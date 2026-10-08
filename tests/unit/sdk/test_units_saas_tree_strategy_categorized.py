"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``CategorizedGroupTree``/``categories_present_in_order``, over a fake inner
``TreeStrategy``: none of their logic depends on what the inner tree is."""

from __future__ import annotations

from support.fakes import faithful_to
from synology_apm_repo.sdk.units.saas.tree_strategy import (
    CategorizedGroupTree,
    TreeEntry,
    TreeStrategy,
    categories_present_in_order,
)

_Key = tuple[str, ...]
_Row = dict[str, object | None]


@faithful_to(TreeStrategy)
class _FakeInnerTree:
    """Just enough of ``TreeStrategy`` to delegate to: a flat, hand-built root listing."""

    def __init__(self, root_children: list[tuple[_Key, str, bool]], rows: dict[_Key, _Row] | None = None) -> None:
        rows = rows or {}
        self._root_children = [TreeEntry(key, name, is_leaf, rows.get(key)) for key, name, is_leaf in root_children]

    async def children_of(self, key: _Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        assert key == ()  # only ever asked for the root
        return self._root_children


class TestCategoriesPresentInOrder:
    def test_orders_by_labels_key_order_not_by_categories_insertion_order(self) -> None:
        # A table scan may meet an "other" calendar first; ``labels`` fixes the order.
        categories = {"cal-2": "other", "cal-1": "my"}
        labels = {"my": "My Calendars", "other": "Other Calendars"}
        assert categories_present_in_order(categories, labels) == ["my", "other"]

    def test_drops_a_category_with_no_members_present(self) -> None:
        categories = {"cal-1": "my"}
        labels = {"my": "My Calendars", "other": "Other Calendars"}
        assert categories_present_in_order(categories, labels) == ["my"]

    def test_empty_categories_yields_no_categories(self) -> None:
        assert categories_present_in_order({}, {"my": "My Calendars", "other": "Other Calendars"}) == []


class TestCategorizedGroupTreeRootOrder:
    async def test_root_lists_categories_in_canonical_order_regardless_of_scan_order(self) -> None:
        """``children_of`` applies ``categories_present_in_order``'s ordering."""
        inner = _FakeInnerTree(root_children=[], rows={})
        tree = CategorizedGroupTree(
            inner,
            categories={"cal-2": "other", "cal-1": "my"},
            labels={"my": "My Calendars", "other": "Other Calendars"},
        )
        entries = await tree.children_of(())
        assert [name for _key, name, _leaf, _row in entries] == ["My Calendars", "Other Calendars"]
        assert [key for key, _name, _leaf, _row in entries] == [("my",), ("other",)]
        assert all(is_leaf is False for _key, _name, is_leaf, _row in entries)

    async def test_root_omits_an_entirely_empty_category(self) -> None:
        inner = _FakeInnerTree(root_children=[], rows={})
        tree = CategorizedGroupTree(
            inner,
            categories={"cal-1": "my"},
            labels={"my": "My Calendars", "other": "Other Calendars"},
        )
        entries = await tree.children_of(())
        assert [name for _key, name, _leaf, _row in entries] == ["My Calendars"]


class _CountingInnerTree(_FakeInnerTree):
    """``_FakeInnerTree`` plus a root-scan call counter."""

    def __init__(self, root_children: list[tuple[_Key, str, bool]]) -> None:
        super().__init__(root_children, rows={})
        self.calls = 0

    async def children_of(self, key: _Key, *, offset: int = 0, limit: int | None = None) -> list[TreeEntry]:
        self.calls += 1
        return await super().children_of(key, offset=offset, limit=limit)


class TestCategorizedGroupTreeCachesTheInnerRootScan:
    async def test_a_second_page_of_the_same_category_reuses_the_first_scan(self) -> None:
        """Pagination within one category must not re-run the inner tree's full
        root scan (a real SQL query for ``NamedGroupFlatTree``/``NamedGroupRecursiveTree``)
        on every page."""
        groups: list[tuple[_Key, str, bool]] = [((f"cal-{i}",), f"cal-{i}", False) for i in range(3)]
        inner = _CountingInnerTree(groups)
        tree = CategorizedGroupTree(inner, categories={"cal-0": "my", "cal-1": "my", "cal-2": "my"}, labels={"my": "M"})
        await tree.children_of(("my",), offset=0, limit=2)
        await tree.children_of(("my",), offset=2, limit=2)
        assert inner.calls == 1

    async def test_a_second_category_on_the_same_tree_reuses_the_first_scan(self) -> None:
        groups: list[tuple[_Key, str, bool]] = [(("cal-0",), "cal-0", False), (("cal-1",), "cal-1", False)]
        inner = _CountingInnerTree(groups)
        tree = CategorizedGroupTree(
            inner, categories={"cal-0": "my", "cal-1": "other"}, labels={"my": "M", "other": "O"}
        )
        await tree.children_of(("my",))
        await tree.children_of(("other",))
        assert inner.calls == 1


class TestCategorizedGroupTreeRows:
    async def test_a_listed_entry_keeps_its_inner_row_under_the_category_key(self) -> None:
        inner = _FakeInnerTree(root_children=[(("cal-1",), "Primary", False)], rows={("cal-1",): {"name": "Primary"}})
        tree = CategorizedGroupTree(inner, categories={"cal-1": "my"}, labels={"my": "My Calendars"})
        [entry] = await tree.children_of(("my",))
        assert (entry.key, entry.row) == (("my", "cal-1"), {"name": "Primary"})
