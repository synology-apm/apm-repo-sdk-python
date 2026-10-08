"""Unit tests for ``core/unit/detail.py`` -- what the detail pane shows,
derived purely from ``UnitModel``."""

from __future__ import annotations

from datetime import UTC, datetime

from synology_apm_repo.browser.core.unit.detail import DetailView, detail_view, header_text
from synology_apm_repo.browser.core.unit.model import DetailError, DetailIdle, DetailLoading, DetailState, UnitModel
from synology_apm_repo.sdk.units.base import FileState, Node, NodeRole, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _leaf(name: str = "item", **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


def test_header_text_includes_kind_size_and_modified() -> None:
    node = _leaf("report.pdf", kind=UnitKind.DISK_FILESYSTEM, size=1024, mtime=datetime(2024, 1, 1, tzinfo=UTC))

    text = header_text(node, verbose=False)

    assert "report.pdf" in text
    assert "kind: disk_filesystem" in text
    assert "size: 1.0 KiB" in text
    assert "modified: 2024-01-01" in text
    assert "ref:" not in text


def test_header_text_shows_the_fallback_label_for_a_kindless_node() -> None:
    assert "kind: item" in header_text(_leaf("item"), verbose=False)


def test_header_text_flags_a_cloud_only_files_size() -> None:
    node = _leaf("placeholder.docx", size=2048, file_state=FileState.CLOUD_ONLY)

    assert "size: 2.0 KiB (0 Byte on disk)" in header_text(node, verbose=False)


def test_header_text_escapes_markup_in_a_backed_up_name() -> None:
    assert "[b]\\[x][/b]" in header_text(_leaf("[x]"), verbose=False)


def test_header_text_notes_why_a_node_is_incomplete_outside_verbose_too() -> None:
    node = _leaf("disk.img", degraded="1 of 2 parts [missing]")

    assert "note: 1 of 2 parts \\[missing]" in header_text(node, verbose=False)


def test_header_text_appends_ref_and_attrs_only_in_verbose_mode() -> None:
    node = _leaf("item", details={"custom": "value"})

    text = header_text(node, verbose=True)

    assert f"ref: {node.ref}" in text
    assert "custom: value" in text
    assert "custom" not in header_text(node, verbose=False)


def test_header_text_is_empty_for_a_content_only_node_outside_verbose() -> None:
    assert header_text(_leaf("mail-1", kind=UnitKind.MAIL), verbose=False) == ""


def test_header_text_shows_only_ref_and_attrs_for_a_content_only_node_in_verbose() -> None:
    node = _leaf("mail-1", kind=UnitKind.MAIL, details={"sender": "a@example.com"})

    text = header_text(node, verbose=True)

    assert text == f"ref: {node.ref}\nsender: a@example.com"


def test_detail_view_of_a_fresh_model_is_empty() -> None:
    assert detail_view(UnitModel()) == DetailView(node=None, body=DetailIdle(), verbose=False)


def test_detail_view_carries_the_selected_node_body_and_verbose_flag() -> None:
    node = _leaf("a")
    model = UnitModel(detail=DetailState(node=node, body=DetailLoading()), verbose=True)

    assert detail_view(model) == DetailView(node=node, body=DetailLoading(), verbose=True)


def test_a_root_load_error_takes_the_pane_over() -> None:
    model = UnitModel(root_error="boom", detail=DetailState(node=_leaf("a"), body=DetailIdle()))

    assert detail_view(model) == DetailView(node=None, body=DetailError("boom"), verbose=False)


def test_only_a_list_overview_group_is_wide() -> None:
    overview = Node(ref=NodeRef("repo", ("list",)), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
    folder = Node(ref=NodeRef("repo", ("dir",)), name="d", is_leaf=False)

    assert DetailView(node=overview, body=DetailIdle(), verbose=False).wide
    assert not DetailView(node=folder, body=DetailIdle(), verbose=False).wide
    assert not DetailView(node=_leaf("a"), body=DetailIdle(), verbose=False).wide
    assert not DetailView(node=None, body=DetailIdle(), verbose=False).wide
