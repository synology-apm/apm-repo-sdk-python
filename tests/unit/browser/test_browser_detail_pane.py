"""Unit tests for ``DetailPane`` — driven directly against a minimal
fake host exposing only the surface it actually reaches into
(``query_one``, ``app_state.verbose``), never a real ``UnitScreen``."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from synology_apm_repo.browser.screens.detail_pane import DetailPane
from synology_apm_repo.sdk.units.base import FileState, Node, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _leaf(name: str = "item", **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


class _FakeApp(App[None]):
    def __init__(self, *, verbose: bool = False) -> None:
        super().__init__()
        self.app_state = SimpleNamespace(verbose=verbose)

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="detail-scroll"):
            yield Static(id="detail")


async def test_header_text_includes_kind_size_and_modified() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf(
            "report.pdf", kind=UnitKind.DISK_FILESYSTEM, size=1024, attrs={"mtime": datetime(2024, 1, 1, tzinfo=UTC)}
        )

        text = pane.header_text(node)

        assert "report.pdf" in text
        assert "kind: disk_filesystem" in text
        assert "size: 1.0 KiB" in text
        assert "modified: 2024-01-01" in text
        assert "ref:" not in text  # not verbose


async def test_header_text_shows_the_fallback_label_for_a_kindless_node() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")

        text = pane.header_text(node)

        assert "kind: item" in text


async def test_header_text_flags_a_cloud_only_files_size() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("placeholder.docx", size=2048, attrs={"file_state": FileState.CLOUD_ONLY})

        text = pane.header_text(node)

        assert "size: 2.0 KiB (0 Byte on disk)" in text


async def test_header_text_appends_ref_and_attrs_only_in_verbose_mode() -> None:
    app = _FakeApp(verbose=True)
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item", attrs={"custom": "value"})

        text = pane.header_text(node)

        assert f"ref: {node.ref}" in text
        assert "custom: value" in text


async def test_header_text_is_empty_for_a_content_only_node_outside_verbose() -> None:
    app = _FakeApp(verbose=False)
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("mail-1", kind=UnitKind.MAIL)

        assert pane.header_text(node) == ""


async def test_header_text_shows_only_ref_and_attrs_for_a_content_only_node_in_verbose() -> None:
    app = _FakeApp(verbose=True)
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("mail-1", kind=UnitKind.MAIL, attrs={"sender": "a@example.com"})

        text = pane.header_text(node)

        assert text == f"ref: {node.ref}\nsender: a@example.com"
        assert "kind:" not in text  # the ordinary name/kind lines are dropped entirely


async def test_show_sets_the_current_node_and_writes_the_header() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")

        pane.show(node)

        assert pane.node is node
        assert "item" in pane.current_text()


async def test_clear_resets_the_node_and_the_text() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        pane.show(_leaf("item"))

        pane.clear()

        assert pane.node is None
        assert pane.current_text() == ""


async def test_set_wide_toggles_the_class_on_both_the_static_and_its_scroll_container() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.set_wide(True)
        assert app.query_one("#detail", Static).has_class("wide-preview")
        assert app.query_one("#detail-scroll", VerticalScroll).has_class("wide-preview")

        pane.set_wide(False)
        assert not app.query_one("#detail", Static).has_class("wide-preview")
        assert not app.query_one("#detail-scroll", VerticalScroll).has_class("wide-preview")


async def test_append_preview_combines_the_header_and_body() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")
        pane.show(node)

        pane.append_preview(node, "the preview body")

        text = pane.current_text()
        assert "item" in text
        assert "the preview body" in text
        assert "─" in text  # the preview separator


async def test_append_preview_is_discarded_for_a_node_the_user_has_since_navigated_away_from() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        stale_node = _leaf("stale")
        pane.show(stale_node)
        pane.show(_leaf("current"))  # navigates away before the stale preview lands
        before = pane.current_text()

        pane.append_preview(stale_node, "a late-arriving preview")

        assert pane.current_text() == before
        assert "a late-arriving preview" not in pane.current_text()


async def test_append_preview_error_renders_the_message() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")
        pane.show(node)

        pane.append_preview_error(node, "boom")

        assert "error:" in pane.current_text()
        assert "boom" in pane.current_text()


async def test_append_preview_note_renders_the_message_without_the_error_label() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")
        pane.show(node)

        pane.append_preview_note(node, "cloud-sync placeholder -- no local data at backup time")

        assert "note:" in pane.current_text()
        assert "error:" not in pane.current_text()
        assert "cloud-sync placeholder -- no local data at backup time" in pane.current_text()


async def test_append_list_overview_with_no_rows_shows_a_placeholder() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("folder")
        pane.show(node)

        pane.append_list_overview(node, [], truncated=False)

        assert "(no items)" in pane.current_text()


async def test_show_loading_then_clear_loading_removes_the_animated_cue() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        node = _leaf("item")
        pane.show(node)
        header_only = pane.current_text()

        pane.show_loading(node, "|")
        assert "loading" in pane.current_text()

        pane.clear_loading(node)
        assert pane.current_text() == header_only
