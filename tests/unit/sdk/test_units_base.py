"""Unit tests for ``synology_apm_repo.sdk.units.base``."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator

import pytest

from synology_apm_repo.sdk.units.base import Node, RestorableUnit, UnitKind, not_restorable, paginate
from synology_apm_repo.sdk.units.node_ref import NodeRef


def test_restorable_unit_open_without_content_raises() -> None:
    unit = RestorableUnit(ref=NodeRef("repo", ("a",)), name="orphan", is_leaf=True, content=None)
    with pytest.raises(ValueError, match="orphan.*no content source"):
        unit.open()


def test_restorable_unit_open_returns_the_stored_content() -> None:
    class _FakeContent:
        size = 4
        supports_concurrent_export = False

        async def read(self, offset: int = 0, length: int | None = None) -> bytes:
            return b"data"

        async def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
            yield 0, b"data"

        async def export_to(self, dst: object, **kwargs: object) -> None:
            return None

    content = _FakeContent()
    unit = RestorableUnit(ref=NodeRef("repo", ("a",)), name="leaf", is_leaf=True, content=content)
    # open() itself stays synchronous — it only hands back the
    # already-constructed ContentSource, doing no I/O of its own.
    assert unit.open() is content


def test_node_default_attrs_is_an_independent_dict_per_instance() -> None:
    a = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True)
    b = Node(ref=NodeRef("repo", ("b",)), name="b", is_leaf=True)
    a.attrs["x"] = 1
    assert b.attrs == {}


def test_unit_kind_values_are_stable_strings() -> None:
    assert UnitKind.DISK_IMAGE.value == "disk_image"
    assert UnitKind.RAW_OBJECT.value == "raw_object"


def test_not_restorable_raises_a_value_error_naming_the_kind_and_ref() -> None:
    with pytest.raises(ValueError, match=r"^node 'stray' is not a restorable unit$"):
        not_restorable("node", "stray")


def test_not_restorable_ref_uses_repr_not_str() -> None:
    # ref is often a tuple key (SaaS providers' own row key) -- !r must
    # show its real shape, not str()'s looser rendering.
    with pytest.raises(ValueError, match=re.escape("item ('a', 'b') is not a restorable unit")):
        not_restorable("item", ("a", "b"))


# -- paginate() ----------------------------------------------------------
#
# paginate() lives in units/base.py so every provider family (device.py,
# file_map_tree.py, saas/raw_object.py, and SaaS's own tree strategies)
# can share one implementation of the same offset/limit slice instead of
# each hand-rolling it independently.


class TestPaginate:
    def test_no_offset_no_limit_returns_everything(self) -> None:
        assert paginate([1, 2, 3], 0, None) == [1, 2, 3]

    def test_offset_only(self) -> None:
        assert paginate([1, 2, 3, 4], 2, None) == [3, 4]

    def test_offset_and_limit(self) -> None:
        assert paginate([1, 2, 3, 4, 5], 1, 2) == [2, 3]

    def test_offset_past_end_returns_empty(self) -> None:
        assert paginate([1, 2], 10, None) == []

    def test_returns_a_list_not_the_original_sequence_type(self) -> None:
        result = paginate((1, 2, 3), 0, None)
        assert isinstance(result, list)
