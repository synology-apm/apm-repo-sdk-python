"""``NavigableScreen``: the base class of the full-screen browsing screens."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from textual import events
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import Input, Static, Tree

from synology_apm_repo.browser.widgets.worker_progress import work

from .app_state import AppStateMixin
from .breadcrumb import breadcrumb_with_tasks_hint
from .goto_ref import parse_canonical_ref, resolve_and_open_goto_target
from .key_delegation import forward_to_focused
from .tree_nav import move_cursor_to_parent

if TYPE_CHECKING:
    from synology_apm_repo.sdk import NodeRef


class NavigableScreen(AppStateMixin, Screen[None]):
    """Forwards ``j``/``k``/``l``/Enter to the focused ``DataTable``/
    ``Tree``, and provides Esc (``_go_back``), ``g`` goto, ``v``
    diagnostics and the breadcrumb with its job-count suffix.
    """

    #: The breadcrumb text as last given to ``_update_breadcrumb_text``,
    #: cached so ``_render_breadcrumb`` can re-add the job-count suffix alone.
    _breadcrumb_text: str = ""
    #: The styled "Loading" suffix ``_set_loading_indicator`` last set.
    _loading_markup: str | None = None

    def on_mount(self) -> None:
        # init=False: no breadcrumb text is set yet.
        self.watch(self.app, "jobs", self._render_breadcrumb, init=False)

    def on_resize(self, event: events.Resize) -> None:
        # The suffix padding is computed at render time, so a resize must
        # re-render it.
        self._render_breadcrumb()

    def on_screen_resume(self, event: events.ScreenResume) -> None:
        # _render_breadcrumb skips job ticks while covered; catch up here.
        self._render_breadcrumb()

    def action_cursor_down(self) -> None:
        forward_to_focused(self, "action_cursor_down")

    def action_cursor_up(self) -> None:
        forward_to_focused(self, "action_cursor_up")

    def action_select(self) -> None:
        forward_to_focused(self, "action_select_cursor")

    def action_cursor_to_parent(self) -> None:
        # Tree has no built-in "jump to parent" action to forward to.
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def action_go_back(self) -> None:
        """Esc: closes an open filter box, else an open goto box, else
        ``_go_back()``."""
        if self._close_open_filter():
            return
        if self._goto_input_active():
            self._close_goto()
            return
        self._go_back()

    def _go_back(self) -> None:
        """What Esc does with no box open: pop this screen."""
        self.app.pop_screen()

    def _close_open_filter(self) -> bool:
        """Closes this screen's open filter box; whether one was open. A
        screen without one keeps this default."""
        return False

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "goto-input":
            self._submit_goto(event.value)
        elif event.input.id == "filter-input":
            self._close_open_filter()

    def action_show_diagnostics(self) -> None:
        # Local import: DiagnosticsScreen imports NavigableScreen (cycle).
        from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

        self.app.push_screen(DiagnosticsScreen())

    # -- goto ref (``g``) -------------------------------------------------
    # Shared show/hide of the #goto-input Input; each screen owns its
    # own _submit_goto.

    def action_goto_ref(self) -> None:
        goto_input = self.query_one("#goto-input", Input)
        goto_input.value = ""
        goto_input.add_class("active")
        goto_input.focus()

    def _close_goto(self) -> None:
        self.query_one("#goto-input", Input).remove_class("active")

    def _goto_input_active(self) -> bool:
        with contextlib.suppress(NoMatches):
            return self.query_one("#goto-input", Input).has_class("active")
        return False

    def _submit_goto(self, text: str) -> None:
        self._close_goto()
        node_ref = parse_canonical_ref(text, notify=self.notify)
        if node_ref is not None:
            self._goto(node_ref)

    def _goto(self, node_ref: NodeRef) -> None:
        """Opens a parsed canonical ref; by default through the version it
        names (``_goto_elsewhere``)."""
        self._goto_elsewhere(node_ref)

    # Resolving does SDK I/O, so it runs as a worker; busy=False because
    # resolve_and_open_goto_target wraps its own fetch.
    @work(busy=False)
    async def _goto_elsewhere(self, node_ref: NodeRef) -> None:
        repo = self.app_state.current_repo
        assert repo is not None
        await resolve_and_open_goto_target(self, repo, node_ref)

    def _set_loading_indicator(self, markup: str | None) -> None:
        """Hook ``DebouncedProgress`` calls with an already-styled "Loading"
        suffix once its debounce fires, and with ``None`` when the timed call
        finishes; the breadcrumb shows it after its text meanwhile."""
        self._loading_markup = markup
        self._render_breadcrumb()

    def _update_breadcrumb_text(self, text: str) -> None:
        """Records ``text`` as this screen's breadcrumb, then renders it."""
        self._breadcrumb_text = text
        self._render_breadcrumb()

    def _render_breadcrumb(self) -> None:
        """Writes ``_breadcrumb_text`` plus the job-count suffix into
        ``#breadcrumb``, tolerating a missing widget (a late
        ``DebouncedProgress.stop()`` can land after the screen is popped).

        Only for the top screen, so a covered one doesn't repaint on every
        job tick: ``self.app.screen is self``, since ``Screen.is_current``
        also counts screens visible through a translucent one."""
        if self.app.screen is not self:
            return
        with contextlib.suppress(NoMatches):
            breadcrumb = self.query_one("#breadcrumb", Static)
            job_count = len(self.app_state.jobs)
            # Subtract the widget's padding or the hint's "(t)" is clipped;
            # the widget's own .size is (0, 0) before the first layout.
            usable_width = self.size.width - breadcrumb.styles.padding.width
            text = self._breadcrumb_text
            if self._loading_markup is not None:
                text = f"{text}  {self._loading_markup}"
            breadcrumb.update(breadcrumb_with_tasks_hint(text, job_count, usable_width))
