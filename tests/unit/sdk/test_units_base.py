"""Unit tests for ``synology_apm_repo.sdk.units.base``."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from synology_apm_repo.sdk.units.base import (
    Node,
    RestorableUnit,
    UnitKind,
    mtime_from_epoch,
    node_kind_label,
    node_modified_time,
    not_restorable,
    paginate,
)
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


def test_node_modified_time_is_none_when_attrs_has_no_mtime() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True)
    assert node_modified_time(node) is None


def test_node_modified_time_is_none_for_a_malformed_value() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True, attrs={"mtime": 12345})
    assert node_modified_time(node) is None


def test_node_modified_time_narrows_a_real_datetime() -> None:
    dt = datetime.fromtimestamp(0, UTC)
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True, attrs={"mtime": dt})
    assert node_modified_time(node) == dt


def test_node_kind_label_uses_the_real_kind_when_set() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True, kind=UnitKind.MAIL)
    assert node_kind_label(node) == UnitKind.MAIL.value


def test_node_kind_label_falls_back_to_folder_for_a_kindless_container() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=False)
    assert node_kind_label(node) == "folder"


def test_node_kind_label_falls_back_to_item_for_a_kindless_leaf() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True)
    assert node_kind_label(node) == "item"


def test_mtime_from_epoch_is_none_for_none() -> None:
    assert mtime_from_epoch(None) is None


def test_mtime_from_epoch_converts_a_real_epoch() -> None:
    assert mtime_from_epoch(0) == datetime.fromtimestamp(0, UTC)


def test_mtime_from_epoch_degrades_to_none_for_an_out_of_range_value() -> None:
    """A provider's own raw catalog value is an unvalidated integer --
    out of ``datetime``'s own representable range must degrade this one
    node's Modified cell to blank (``None``) rather than raising out of
    ``children()`` and failing the whole containing folder's listing."""
    assert mtime_from_epoch(99999999999999) is None  # ValueError: year out of range
    assert mtime_from_epoch(-(2**62)) is None  # OSError: value too large


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
