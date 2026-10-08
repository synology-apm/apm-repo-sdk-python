"""Unit tests for ``DetailPane`` against a minimal fake host: how a
``DetailView`` becomes text and layout."""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from synology_apm_repo.browser.core.unit.detail import DetailView
from synology_apm_repo.browser.core.unit.model import (
    DetailBody,
    DetailError,
    DetailIdle,
    DetailLoading,
    DetailNote,
    DetailOverview,
    DetailPreview,
)
from synology_apm_repo.browser.screens.detail_pane import DetailPane
from synology_apm_repo.sdk.units.base import Node, NodeRole, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _leaf(name: str = "item", **kwargs: object) -> Node:
    return Node(ref=NodeRef("repo", ("root", name)), name=name, is_leaf=True, **kwargs)  # type: ignore[arg-type]


def _view(node: Node | None, body: DetailBody, *, verbose: bool = False) -> DetailView:
    return DetailView(node=node, body=body, verbose=verbose)


def _shown(app: App[None]) -> str:
    """What ``#detail`` currently shows; empty once it's gone."""
    statics = app.query("#detail").results(Static)
    return "".join(str(static.render()) for static in statics)


class _FakeApp(App[None]):
    def compose(self) -> ComposeResult:
        with VerticalScroll(id="detail-scroll"):
            yield Static(id="detail")


async def test_render_writes_the_header() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailIdle()))

        assert "item" in _shown(app)


class _TornDownApp(App[None]):
    """A host whose ``#detail`` widgets are already gone, as during a
    screen's teardown."""


async def test_render_after_the_widgets_are_gone_is_a_no_op_that_keeps_the_view() -> None:
    """A worker's result can land after the screen's children are removed;
    raising there would fail the worker."""
    app = _TornDownApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        view = _view(_leaf("item"), DetailIdle())

        pane.render(view)

        assert _shown(app) == ""
        assert "item" in pane.text(pane._view)


async def test_render_of_no_selection_clears_the_pane() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        pane.render(_view(_leaf("item"), DetailIdle()))

        pane.render(_view(None, DetailIdle()))

        assert _shown(app) == ""


async def test_render_toggles_wide_on_both_the_static_and_its_scroll_container() -> None:
    overview = Node(ref=NodeRef("repo", ("list",)), name="L", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(overview, DetailLoading()))
        assert app.query_one("#detail", Static).has_class("wide-preview")
        assert app.query_one("#detail-scroll", VerticalScroll).has_class("wide-preview")

        pane.render(_view(_leaf("item"), DetailIdle()))
        assert not app.query_one("#detail", Static).has_class("wide-preview")
        assert not app.query_one("#detail-scroll", VerticalScroll).has_class("wide-preview")


async def test_a_preview_goes_below_the_header() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailPreview("the preview body")))

        text = _shown(app)
        assert "item" in text
        assert "the preview body" in text
        assert text.index("item") < text.index("the preview body")


async def test_a_preview_with_no_header_is_the_panes_whole_text() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("mail-1", kind=UnitKind.MAIL), DetailPreview("From: alice")))

        assert _shown(app) == "From: alice"


async def test_preview_text_is_escaped_for_markup() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailPreview("[red]not markup[/red]")))

        assert "[red]not markup[/red]" in _shown(app)


async def test_an_error_renders_the_message_with_the_error_label() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailError("boom")))

        assert "error:" in _shown(app)
        assert "boom" in _shown(app)


async def test_an_error_with_no_selection_is_the_panes_whole_text() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(None, DetailError("root failed")))

        assert _shown(app) == "error: root failed"


async def test_a_note_renders_the_message_without_the_error_label() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailNote("cloud-sync placeholder -- no local data at backup time")))

        assert "note:" in _shown(app)
        assert "error:" not in _shown(app)
        assert "cloud-sync placeholder -- no local data at backup time" in _shown(app)


async def test_an_overview_with_no_rows_shows_a_placeholder() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailOverview(rows=(), truncated=False)))

        assert "(no items)" in _shown(app)


async def test_an_overview_renders_its_rows_as_a_table() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]

        pane.render(_view(_leaf("item"), DetailOverview(rows=({"Title": "alpha"}, {"Title": "beta"}), truncated=False)))

        text = _shown(app)
        assert "Title" in text
        assert "alpha" in text
        assert "beta" in text


async def test_show_loading_then_restore_removes_the_animated_cue() -> None:
    app = _FakeApp()
    async with app.run_test():
        pane = DetailPane(app)  # type: ignore[arg-type]
        pane.render(_view(_leaf("item"), DetailLoading()))
        header_only = _shown(app)

        pane.show_loading("|")
        assert "loading" in _shown(app)

        pane.restore()
        assert _shown(app) == header_only
