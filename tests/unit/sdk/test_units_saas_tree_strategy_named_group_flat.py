"""Unit tests for ``synology_apm_repo.sdk.units.saas.tree_strategy``'s
``NamedGroupFlatTree`` (Calendar's shape): named groups from their own
table, each holding flat leaves, over a small real SQLite database."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable

import pytest

from synology_apm_repo.sdk.storage.table import Column
from synology_apm_repo.sdk.units.saas.tree_strategy import NamedGroupFlatTree, Row, TreeEntry
from unit.sdk.tree_strategy_fakes import FakeTableProvider, TableSpec, table_provider

_CALENDARS: TableSpec = (
    "calendar_id TEXT PRIMARY KEY, calendar_name TEXT",
    [("cal-work", "Work"), ("cal-home", "Home"), ("cal-empty", "Archive")],
)
_EVENTS: TableSpec = (
    "event_id TEXT PRIMARY KEY, summary TEXT, calendar_id TEXT, event_start_time INTEGER",
    [
        ("ev-3", "Standup", "cal-work", 300),
        ("ev-1", "Planning", "cal-work", 100),
        ("ev-2", "Review", "cal-work", 200),
        ("ev-4", "Dinner with Alice", "cal-home", 100),
    ],
)


@pytest.fixture
async def provider() -> AsyncIterator[FakeTableProvider]:
    async with table_provider({"calendars": _CALENDARS, "events": _EVENTS}) as p:
        yield p


def _tree(
    provider: FakeTableProvider,
    *,
    order_by: list[str] | None = None,
    group_name_override: Callable[[Row], str | None] | None = None,
) -> NamedGroupFlatTree:
    return NamedGroupFlatTree(
        provider,
        group_table="calendars",
        group_columns=[Column("calendar_id"), Column("calendar_name")],
        group_id_column="calendar_id",
        group_name_column="calendar_name",
        leaf_table="events",
        leaf_columns=[Column("event_id"), Column("summary"), Column("calendar_id"), Column("event_start_time")],
        leaf_id_column="event_id",
        leaf_group_column="calendar_id",
        display_name=lambda row: str(row["summary"]),
        order_by=order_by if order_by is not None else ["event_start_time"],
        group_name_override=group_name_override,
    )


def _names(entries: list[TreeEntry]) -> list[str]:
    return [entry.name for entry in entries]


class TestRoot:
    async def test_lists_every_group_by_name_including_an_empty_one(self, provider: FakeTableProvider) -> None:
        entries = await _tree(provider).children_of(())
        assert entries == [
            TreeEntry(("cal-empty",), "Archive", False),
            TreeEntry(("cal-home",), "Home", False),
            TreeEntry(("cal-work",), "Work", False),
        ]

    @pytest.mark.parametrize(
        ("offset", "limit", "expected"),
        [
            pytest.param(1, None, ["Home", "Work"], id="offset_only"),
            pytest.param(0, 2, ["Archive", "Home"], id="limit_only"),
            pytest.param(1, 1, ["Home"], id="offset_and_limit"),
            pytest.param(3, None, [], id="offset_past_the_end"),
        ],
    )
    async def test_pages_the_groups(
        self, provider: FakeTableProvider, offset: int, limit: int | None, expected: list[str]
    ) -> None:
        assert _names(await _tree(provider).children_of((), offset=offset, limit=limit)) == expected

    async def test_the_override_replaces_a_groups_name(self, provider: FakeTableProvider) -> None:
        tree = _tree(
            provider, group_name_override=lambda row: "alice@example.com" if row["calendar_id"] == "cal-home" else None
        )
        # Listed by the stored name, so the override doesn't reorder the groups.
        assert _names(await tree.children_of(())) == ["Archive", "alice@example.com", "Work"]

    async def test_only_the_group_table_is_opened_once_to_list_the_groups(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        await tree.children_of(())
        await tree.children_of((), offset=1)
        assert provider.requested == ["calendars"]


class TestGroup:
    async def test_lists_the_groups_leaves_sorted_by_order_by_with_their_rows(
        self, provider: FakeTableProvider
    ) -> None:
        entries = await _tree(provider).children_of(("cal-work",))
        assert [(entry.key, entry.name, entry.is_leaf) for entry in entries] == [
            (("cal-work", "ev-1"), "Planning", True),
            (("cal-work", "ev-2"), "Review", True),
            (("cal-work", "ev-3"), "Standup", True),
        ]
        assert [entry.row["event_start_time"] for entry in entries if entry.row is not None] == [100, 200, 300]

    async def test_a_group_with_no_leaves_lists_nothing(self, provider: FakeTableProvider) -> None:
        assert await _tree(provider).children_of(("cal-empty",)) == []

    async def test_an_unknown_group_lists_nothing(self, provider: FakeTableProvider) -> None:
        assert await _tree(provider).children_of(("cal-none",)) == []

    async def test_an_order_by_column_the_table_lacks_is_skipped(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider, order_by=["no_such_column", "summary"])
        assert _names(await tree.children_of(("cal-work",))) == ["Planning", "Review", "Standup"]

    @pytest.mark.parametrize(
        ("offset", "limit", "expected"),
        [
            pytest.param(1, None, ["Review", "Standup"], id="offset_only"),
            pytest.param(0, 2, ["Planning", "Review"], id="limit_only"),
            pytest.param(2, 5, ["Standup"], id="limit_past_the_end"),
        ],
    )
    async def test_pages_the_leaves(
        self, provider: FakeTableProvider, offset: int, limit: int | None, expected: list[str]
    ) -> None:
        assert _names(await _tree(provider).children_of(("cal-work",), offset=offset, limit=limit)) == expected

    async def test_the_leaf_table_is_created_once_across_groups(self, provider: FakeTableProvider) -> None:
        tree = _tree(provider)
        await tree.children_of(("cal-work",))
        await tree.children_of(("cal-home",))
        assert provider.requested == ["events"]


async def test_a_leaf_key_has_no_children(provider: FakeTableProvider) -> None:
    assert await _tree(provider).children_of(("cal-work", "ev-1")) == []
