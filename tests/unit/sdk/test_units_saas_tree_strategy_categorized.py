"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``CategorizedGroupTree``/``categories_present_in_order`` -- exercised
here via a minimal fake inner ``TreeStrategy`` rather than a full
Calendar/Site repository build, since none of this class's own logic
(category dedup/ordering, key composition, delegation to the inner
tree) depends on what the inner tree actually is."""

from __future__ import annotations

from synology_apm_repo.sdk.units.saas.tree_strategy import CategorizedGroupTree, categories_present_in_order

_Key = tuple[str, ...]
_Row = dict[str, object | None]


class _FakeInnerTree:
    """Just enough of ``TreeStrategy`` for ``CategorizedGroupTree`` to
    delegate to -- a flat, hand-built ``key -> (children, row)`` map."""

    def __init__(self, root_children: list[tuple[_Key, str, bool]], rows: dict[_Key, _Row]) -> None:
        self._root_children = root_children
        self._rows = rows

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        assert key == ()  # this fake is only ever asked for the root -- see the class docstring
        return self._root_children

    def row_for(self, key: _Key) -> _Row | None:
        return self._rows.get(key)


class TestCategoriesPresentInOrder:
    def test_orders_by_labels_key_order_not_by_categories_insertion_order(self) -> None:
        # "other" is inserted before "my" here -- simulating a
        # table-scan that happened to encounter an "other" calendar
        # first -- but ``labels`` (the caller's fixed, canonical order)
        # still puts "my" first.
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
        """Confirms ``children_of`` actually applies
        ``categories_present_in_order``'s own ordering (see
        ``TestCategoriesPresentInOrder`` for why that ordering matters),
        not just that the helper itself is correct."""
        inner = _FakeInnerTree(root_children=[], rows={})
        tree = CategorizedGroupTree(
            inner,
            categories={"cal-2": "other", "cal-1": "my"},
            labels={"my": "My Calendars", "other": "Other Calendars"},
        )
        entries = await tree.children_of(())
        assert [name for _key, name, _leaf in entries] == ["My Calendars", "Other Calendars"]
        assert [key for key, _name, _leaf in entries] == [("my",), ("other",)]
        assert all(is_leaf is False for _key, _name, is_leaf in entries)

    async def test_root_omits_an_entirely_empty_category(self) -> None:
        inner = _FakeInnerTree(root_children=[], rows={})
        tree = CategorizedGroupTree(
            inner,
            categories={"cal-1": "my"},
            labels={"my": "My Calendars", "other": "Other Calendars"},
        )
        entries = await tree.children_of(())
        assert [name for _key, name, _leaf in entries] == ["My Calendars"]


class _CountingInnerTree(_FakeInnerTree):
    """Same fake, plus a call counter -- for proving
    ``CategorizedGroupTree`` doesn't re-issue the inner tree's own root
    scan on every category-level page/category."""

    def __init__(self, root_children: list[tuple[_Key, str, bool]]) -> None:
        super().__init__(root_children, rows={})
        self.calls = 0

    async def children_of(
        self, key: _Key, *, offset: int = 0, limit: int | None = None
    ) -> list[tuple[_Key, str, bool]]:
        self.calls += 1
        return await super().children_of(key, offset=offset, limit=limit)


class TestCategorizedGroupTreeCachesTheInnerRootScan:
    async def test_a_second_page_of_the_same_category_reuses_the_first_scan(self) -> None:
        """Pagination within one category must not re-run the inner
        tree's own full, unfiltered root scan on every page -- a real
        SQL query for ``NamedGroupFlatTree``/``NamedGroupRecursiveTree``,
        not just an in-memory filter."""
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


class TestCategorizedGroupTreeRowFor:
    def test_row_for_a_bare_category_key_is_none(self) -> None:
        inner = _FakeInnerTree(root_children=[], rows={("cal-1",): {"name": "Primary"}})
        tree = CategorizedGroupTree(inner, categories={"cal-1": "my"}, labels={"my": "My Calendars"})
        assert tree.row_for(("my",)) is None  # length 1 -- too short to ever be a real row

    def test_row_for_delegates_to_the_inner_tree_with_the_category_stripped(self) -> None:
        inner = _FakeInnerTree(root_children=[], rows={("cal-1",): {"name": "Primary"}})
        tree = CategorizedGroupTree(inner, categories={"cal-1": "my"}, labels={"my": "My Calendars"})
        assert tree.row_for(("my", "cal-1")) == {"name": "Primary"}


__all__: list[str] = []
