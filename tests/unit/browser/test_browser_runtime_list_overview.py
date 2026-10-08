"""Unit tests for ``synology_apm_repo.browser.runtime.list_overview``, the
SharePoint List overview's data loading, driven by a fake provider."""

from __future__ import annotations

import asyncio
import json

import pytest

from support.fakes import faithful_to
from synology_apm_repo.browser.runtime.list_overview import ListOverview, load_list_overview
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef

_LIST = Node(ref=NodeRef("repo", ("list",)), name="MyList", is_leaf=False)


@faithful_to(ContentSource)
class _Content:
    """A content source whose ``read`` returns canned bytes (or raises)."""

    def __init__(self, data: bytes | BaseException) -> None:
        self._data = data
        self.read_limits: list[int | None] = []

    size = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        self.read_limits.append(length)
        if isinstance(self._data, BaseException):
            raise self._data
        return self._data

    def stream(self, block: int = 0) -> object:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        raise AssertionError("not used in this test")


def _item(name: str, data: bytes | BaseException, *, is_leaf: bool = True) -> tuple[Node, _Content]:
    return Node(ref=NodeRef("repo", ("list", name)), name=name, is_leaf=is_leaf), _Content(data)


@faithful_to(UnitProvider)
class _Provider:
    def __init__(
        self, items: list[tuple[Node, _Content]], *, list_error: ApmRepoError | None = None, overlap: int = 1
    ) -> None:
        """``unit()`` calls park until ``overlap`` of them are in flight at once."""
        self._items = items
        self._list_error = list_error
        self.children_calls: list[tuple[int, int | None]] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self._overlap = overlap
        self._overlapped = asyncio.Event()

    def root(self) -> Node:
        return _LIST

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.children_calls.append((offset, limit))
        if self._list_error is not None:
            raise self._list_error
        return [child for child, _ in self._items][:limit]

    async def unit(self, node: Node) -> RestorableUnit:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        if self.in_flight >= self._overlap:
            self._overlapped.set()
        try:
            await self._overlapped.wait()
            content = next(c for n, c in self._items if n is node)
            return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, content=content)  # type: ignore[arg-type]
        finally:
            self.in_flight -= 1


async def _load(provider: _Provider, *, item_cap: int = 50, max_concurrent: int = 8) -> ListOverview:
    return await load_list_overview(
        provider,
        _LIST,
        item_cap=item_cap,
        read_limit=1024,
        max_concurrent=max_concurrent,
    )


async def test_rows_follow_the_providers_order_and_drop_sharepoint_plumbing() -> None:
    provider = _Provider(
        [
            _item("a", json.dumps({"Title": "First", "odata.type": "x", "ItemId": 1}).encode()),
            _item("b", json.dumps({"Title": "Second"}).encode()),
        ]
    )

    overview = await _load(provider)

    assert overview.rows == [{"Title": "First"}, {"Title": "Second"}]
    assert overview.truncated is False
    assert provider.children_calls == [(0, 50)]


async def test_an_unreadable_malformed_or_non_object_item_is_skipped_not_fatal() -> None:
    provider = _Provider(
        [
            _item("ok", b'{"Title": "kept"}'),
            _item("broken", RuntimeError("unreadable")),
            _item("not-json", b"<html>"),
            _item("not-an-object", b"[1, 2, 3]"),
        ]
    )

    overview = await _load(provider)

    assert overview.rows == [{"Title": "kept"}]


async def test_folders_are_never_fetched() -> None:
    provider = _Provider([_item("folder", b'{"Title": "no"}', is_leaf=False), _item("leaf", b'{"Title": "yes"}')])

    overview = await _load(provider)

    assert overview.rows == [{"Title": "yes"}]


async def test_each_item_is_read_through_the_read_limit() -> None:
    item = _item("a", b'{"Title": "x"}')
    await _load(_Provider([item]))
    assert item[1].read_limits == [1024]


async def test_truncated_reports_a_list_that_filled_the_item_cap() -> None:
    provider = _Provider([_item(str(i), b'{"Title": "t"}') for i in range(3)])

    assert (await _load(provider, item_cap=3)).truncated is True
    assert (await _load(provider, item_cap=4)).truncated is False


async def test_loading_never_exceeds_the_concurrency_bound() -> None:
    provider = _Provider([_item(str(i), b'{"Title": "t"}') for i in range(12)], overlap=3)

    await asyncio.wait_for(_load(provider, max_concurrent=3), 10)

    assert provider.max_in_flight == 3


async def test_a_listing_failure_propagates_to_the_caller() -> None:
    provider = _Provider([], list_error=ApmRepoError("cannot list"))

    with pytest.raises(ApmRepoError, match="cannot list"):
        await _load(provider)
