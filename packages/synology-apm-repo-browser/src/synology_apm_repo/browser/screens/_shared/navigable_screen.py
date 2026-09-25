"""``NavigableScreen``: shared base class for screens whose primary
widget is a ``DataTable``/``Tree``, composed here from sibling modules
under the private (leading-underscore) ``_shared`` package.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from textual import events
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import Input, Static, Tree

from .breadcrumb import _breadcrumb_with_tasks_hint
from .key_delegation import forward_to_focused
from .tree_nav import move_cursor_to_parent

if TYPE_CHECKING:
    from synology_apm_repo.browser.app import ApmRepoBrowserApp


class NavigableScreen(Screen[None]):
    """``j``/``k`` move, ``l``/Enter select — forwarded to whichever
    focused widget already implements ``action_cursor_down``/``_up``/
    ``action_select_cursor`` (``DataTable`` and ``Tree`` both do); a
    focused widget without those (e.g. an ``Input``) simply ignores the
    forward, letting its own bindings (if any) take the key instead.
    """

    #: The breadcrumb's own "pure" text, as last given to
    #: ``_update_breadcrumb_text`` -- cached so ``_render_breadcrumb`` can
    #: recombine it with a *fresh* job count on its own, without needing
    #: a matching breadcrumb-text update to happen at the same time (a
    #: job starting/finishing while the breadcrumb is otherwise idle
    #: still needs the "N Task(s) (t)" suffix to appear/disappear).
    _breadcrumb_text: str = ""

    def on_mount(self) -> None:
        # init=False: this fires long before either subclass's own
        # on_mount has set a real breadcrumb_text -- nothing worth
        # rendering yet, same reasoning BrowseScreen's own "verbose"
        # watch already uses.
        self.watch(self.app, "jobs", self._render_breadcrumb, init=False)

    def on_resize(self, event: events.Resize) -> None:
        # _breadcrumb_with_tasks_hint's own padding is computed against
        # this screen's *current* width at the time it last ran, not
        # recomputed live on paint the way a width-aware Rich renderable
        # would be -- it stays a plain str because Static.update()/
        # .render() only stringify a plain str/Content sensibly, not an
        # arbitrary Rich renderable -- without this, a terminal resize
        # alone (no other breadcrumb-affecting event) would leave the
        # "N Task(s) (t)" suffix's own right-alignment stale until
        # something else happened to re-render it.
        self._render_breadcrumb()

    def on_screen_resume(self, event: events.ScreenResume) -> None:
        # A job could have started/finished while this screen sat
        # suspended underneath another one -- _render_breadcrumb's own
        # is_current guard skipped every one of those ticks, so this
        # catches the "N Task(s) (t)" suffix back up to the real,
        # current count the moment this screen becomes visible again.
        self._render_breadcrumb()

    def action_cursor_down(self) -> None:
        forward_to_focused(self, "action_cursor_down")

    def action_cursor_up(self) -> None:
        forward_to_focused(self, "action_cursor_up")

    def action_select(self) -> None:
        forward_to_focused(self, "action_select_cursor")

    def action_cursor_to_parent(self) -> None:
        # Not a plain _forward(): Tree has no built-in "jump to parent"
        # action of its own to forward to (unlike cursor_down/_up/
        # select_cursor, which Tree/DataTable already implement).
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def action_go_back(self) -> None:
        """Plain pop, the default for a screen with nothing of its own to
        close first — ``BrowseScreen``/``UnitScreen`` override this to
        close an open filter/goto box before popping instead."""
        self.app.pop_screen()

    def action_show_diagnostics(self) -> None:
        # Local import: DiagnosticsScreen itself imports NavigableScreen
        # from this module, so a module-level import here would be
        # circular.
        from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen

        self.app.push_screen(DiagnosticsScreen())

    # -- goto ref (``g``) -------------------------------------------------
    # Shared by every screen with a #goto-input Input (BrowseScreen/
    # UnitScreen): both methods just show/hide that same Input. Each
    # screen's own _submit_goto stays on the screen itself since it
    # isn't identical between them.

    def action_goto_ref(self) -> None:
        goto_input = self.query_one("#goto-input", Input)
        goto_input.value = ""
        goto_input.add_class("active")
        goto_input.focus()

    def _close_goto(self) -> None:
        self.query_one("#goto-input", Input).remove_class("active")

    def _set_loading_indicator(self, markup: str | None) -> None:
        """Hook ``DebouncedProgress`` calls once its debounce delay fires,
        and once more with ``None`` when the call it's timing finishes —
        appending ``markup`` (an already-styled "Loading" suffix) to this
        screen's own breadcrumb, or restoring the plain breadcrumb when
        ``markup`` is ``None``.

        A safe no-op by default: only this base class knows when
        loading starts/stops, but only each subclass knows its own
        breadcrumb's current *base* text (a live, multi-part path vs. a
        fixed version name) — a screen that actually uses
        ``DebouncedProgress`` must override this."""

    def _update_breadcrumb_text(self, text: str) -> None:
        """Records ``text`` as this screen's own breadcrumb going
        forward, then renders it -- see ``_render_breadcrumb`` for the
        actual write (shared with the ``jobs``-triggered re-render
        ``on_mount`` registers, so a job starting/finishing mid-way
        through, say, a debounced "Loading" suffix still recombines with
        *this* call's own text correctly)."""
        self._breadcrumb_text = text
        self._render_breadcrumb()

    def _render_breadcrumb(self) -> None:
        """Writes the current ``_breadcrumb_text`` (plus a "N Task(s)
        (t)" suffix when at least one background job exists — see
        ``_breadcrumb_with_tasks_hint``) into this screen's own
        ``#breadcrumb`` ``Static``, tolerating the widget not existing at
        all: not every ``NavigableScreen`` has one (``DiagnosticsScreen``,
        say), yet ``on_resize`` below calls this unconditionally on
        every one of them regardless. Also tolerates the widget having
        *existed but since being gone*: a ``DebouncedProgress``'s final
        ``stop()`` can fire after this screen has been popped (its timed
        call keeps running as a background ``Task`` no one awaited).
        Either way, ``query_one()`` raising ``NoMatches`` must not crash
        what should be an entirely harmless call -- checked *before*
        touching ``app_state.jobs`` below, so a screen/test double with
        no breadcrumb also never needs a working ``jobs`` attribute on
        its own App.

        Skipped entirely while this screen isn't the exact top of the
        stack: every screen still under it stays mounted (never
        unmounted just for being covered), so without this check, one
        background job tick would repaint every stacked screen's own
        invisible breadcrumb, not just the visible one. Deliberately
        ``self.app.screen is self``, not ``Screen.is_current`` --
        that property also counts a screen still visible *through* a
        translucent one stacked on top, which doesn't apply to any
        ``NavigableScreen`` (always fully opaque; only the unrelated
        ``ModalScreen`` dialogs have any transparency in this app, and
        none of those are one of these). ``on_screen_resume`` below is
        what catches this back up once a screen *becomes* the top
        again."""
        if self.app.screen is not self:
            return
        with contextlib.suppress(NoMatches):
            breadcrumb = self.query_one("#breadcrumb", Static)
            job_count = len(self.app_state.jobs)
            # theme.tcss gives #breadcrumb its own "padding: 0 1" -- the
            # widget's *usable* width is 2 columns narrower than the
            # screen's own, or the hint's own trailing "(t)" gets
            # silently clipped off the right edge. Padding is a resolved
            # style, available immediately (unlike the widget's own
            # .size, which stays (0, 0) until the first real layout pass
            # completes).
            usable_width = self.size.width - breadcrumb.styles.padding.width
            breadcrumb.update(_breadcrumb_with_tasks_hint(self._breadcrumb_text, job_count, usable_width))

    @property
    def app_state(self) -> ApmRepoBrowserApp:
        return self.app  # type: ignore[return-value]
