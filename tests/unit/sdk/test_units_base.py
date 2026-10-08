"""Unit tests for ``synology_apm_repo.sdk.units.base``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest

from synology_apm_repo.sdk.units.base import ContentSource, FileState, Node, RestorableUnit, UnitKind, node_kind_label
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.provider_kit import diagnostic_node


def test_node_default_details_is_an_independent_dict_per_instance() -> None:
    a = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True)
    b = Node(ref=NodeRef("repo", ("b",)), name="b", is_leaf=True)
    assert a.details == b.details == {}
    assert a.details is not b.details


def test_unit_kind_values_are_stable_strings() -> None:
    assert UnitKind.DISK_IMAGE.value == "disk_image"
    assert UnitKind.RAW_OBJECT.value == "raw_object"


def test_a_node_is_a_diagnostic_placeholder_exactly_when_it_has_a_diagnostic_reason() -> None:
    ref = NodeRef("repo", ("a",))
    assert not Node(ref=ref, name="a", is_leaf=True).is_diagnostic
    assert diagnostic_node(ref, "(missing)", "gone").is_diagnostic
    assert diagnostic_node(ref, "(missing)", "gone").diagnostic == "gone"


def test_restorable_unit_of_copies_every_node_field_and_applies_changes() -> None:
    dt = datetime.fromtimestamp(0, UTC)
    node = Node(
        ref=NodeRef("repo", ("a",)),
        name="a",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=3,
        mtime=dt,
        file_state=FileState.ENCRYPTED,
        details={"path": "/a"},
        handle=("key",),
    )
    content = object()
    unit = RestorableUnit.of(node, cast(ContentSource, content), size=4)
    assert (unit.name, unit.kind, unit.mtime, unit.file_state, unit.details) == (
        "a",
        UnitKind.FILE,
        dt,
        FileState.ENCRYPTED,
        {"path": "/a"},
    )
    assert unit.size == 4
    assert unit.handle == ("key",)
    assert unit.content is content


def test_node_kind_label_uses_the_real_kind_when_set() -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=True, kind=UnitKind.MAIL)
    assert node_kind_label(node) == UnitKind.MAIL.value


@pytest.mark.parametrize(
    ("is_leaf", "expected"),
    [
        pytest.param(False, "folder", id="folder_for_a_kindless_container"),
        pytest.param(True, "item", id="item_for_a_kindless_leaf"),
    ],
)
def test_node_kind_label_falls_back(is_leaf: bool, expected: str) -> None:
    node = Node(ref=NodeRef("repo", ("a",)), name="a", is_leaf=is_leaf)
    assert node_kind_label(node) == expected
