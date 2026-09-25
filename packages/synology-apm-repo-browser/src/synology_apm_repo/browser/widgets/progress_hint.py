"""``DebouncedProgress`` (below): debounces the loading indicator for an
expensive call behind a 300 ms delay, so a call that finishes quickly never
touches the screen.

Where the spinner actually renders is pluggable via a ``_LoadingSink`` —
each anchored at whatever widget/region is actually about to show the
fetch's own result, per ``browser/README.md``'s "anchor a loading
indicator at the specific widget..." rule: ``_BreadcrumbSink`` (a whole
screen's own location is changing — a ``g`` jump, or a transient resolve
before pushing an entirely different screen), ``TreeNodeLoadingSink`` (one
``TreeNode`` — its own children, or, for a permanent non-domain tree
root, that tree's whole column), ``DataTableLoadingRowSink`` (a flat
``DataTable`` column with no per-node equivalent), and ``StaticTextSink``
(a Store-less screen's own single status line).

Must be constructed/stopped on the App's event loop (arms timers) — safe
because every worker in this package runs as a native async task on that
same loop, never a thread: a thread-hosted worker would have to marshal
back onto the loop before it could touch a timer at all.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

from textual.css.query import NoMatches
from textual.timer import Timer
from textual.widgets import Static
from textual.widgets.data_table import CellDoesNotExist, RowDoesNotExist

if TYPE_CHECKING:
    from textual.widget import Widget
    from textual.widgets import DataTable
    from textual.widgets.data_table import RowKey
    from textual.widgets.tree import TreeNode

    from synology_apm_repo.browser.screens._shared import NavigableScreen

_DEFAULT_DELAY = 0.3
#: Every tick repaints the sink's target, and a repaint is not free —
#: Textual reapplies the stylesheet per widget, the dominant cost of
#: driving this app at all. 500ms still reads as "something is running"
#: while costing a fifth of what 100ms did.
_FRAME_INTERVAL = 0.5
#: A Braille dot spinner — unlike ASCII fallbacks such as ``|/-\``, never
#: mistaken for real content.
_SPINNER_FRAMES = "⠁⠉⠙⠹⠼⠶⠧⠇⠃"
#: Deliberately loud — this is the one signal telling the user something
#: is still running, not a subtle status-bar tone. Only ``_BreadcrumbSink``
#: uses this: a ``Static``'s text parses Rich markup, a ``TreeNode``'s
#: label doesn't (see ``TreeNodeLoadingSink``).
_LOADING_STYLE = "bold bright_yellow"


class _LoadingSink(Protocol):
    """Where one ``DebouncedProgress``'s animated frames actually render.
    ``show`` is called once per tick with the current spinner frame;
    ``hide`` once, on ``stop()``, only if ``show`` ever ran."""

    def show(self, frame: str) -> None: ...

    def hide(self) -> None: ...


class _TimerHost(Protocol):
    """Structural minimum ``DebouncedProgress`` needs from its host: just
    enough to arm timers. Lets a ``runtime/*_effects.py`` caller (whose
    ``self._screen`` is typed as a bare ``Widget``, per this package's own
    ``runtime`` -> ``screens`` layering ban forbidding the real
    ``NavigableScreen`` type there) pass it directly, with no
    ``# type: ignore``. The default sink (``_BreadcrumbSink``) still
    statically requires a real ``NavigableScreen``, so only an explicit,
    non-default ``sink=`` can actually make use of the wider type here."""

    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...

    def set_interval(self, interval: float, callback: Callable[[], None]) -> Timer: ...


class _BreadcrumbSink:
    """Appends the styled spinner frame to the screen's own breadcrumb text
    via ``NavigableScreen._set_loading_indicator``."""

    def __init__(self, screen: NavigableScreen) -> None:
        self._screen = screen

    def show(self, frame: str) -> None:
        self._screen._set_loading_indicator(  # noqa: SLF001 - NavigableScreen's documented hook for exactly this call
            f"[{_LOADING_STYLE}]{frame} Loading[/{_LOADING_STYLE}]"
        )

    def hide(self) -> None:
        self._screen._set_loading_indicator(None)  # noqa: SLF001 - NavigableScreen's documented hook for exactly this call


class TreeNodeLoadingSink:
    """Targets one expanding ``TreeNode``'s own label instead of the
    breadcrumb — for a call site whose loading is naturally attached to a
    single tree node being expanded. Never snapshots the node's "real"
    label once at construction: each ``show``/``hide`` instead strips
    *this sink's own* previously-appended suffix (if still present) off
    whatever the label currently is, so an unrelated relabel of the same
    node while this sink is animating (e.g. ``BrowseScreen``'s
    ``_refresh_repo_labels``, run when the user toggles verbose mode
    mid-load) is preserved rather than clobbered by a stale snapshot —
    the next tick/``hide()`` picks up the new real label as its base
    instead of overwriting it. Real children added under the node
    meanwhile are unaffected either way, since children are separate
    child ``TreeNode``s, never encoded into the parent's own label text.
    A ``Tree``'s label doesn't parse Rich markup the way a ``Static``'s
    text does, so the frame is appended as plain text, with no
    ``_LOADING_STYLE`` wrapping."""

    def __init__(self, node: TreeNode[Any]) -> None:
        self._node = node
        self._own_suffix: str | None = None

    def _strip_own_suffix(self) -> str:
        label = str(self._node.label)
        if self._own_suffix is not None and label.endswith(self._own_suffix):
            return label[: -len(self._own_suffix)]
        return label

    def show(self, frame: str) -> None:
        base = self._strip_own_suffix()
        suffix = f"  {frame} Loading"
        self._node.set_label(f"{base}{suffix}")
        self._own_suffix = suffix

    def hide(self) -> None:
        base = self._strip_own_suffix()
        self._node.set_label(base)
        self._own_suffix = None


class _ConditionalResetSink:
    """Shared bookkeeping for a sink whose ``show``/``hide`` write into a
    widget that a slow-enough operation's own final result might *also*
    write into: ``work()`` wraps a decorated method's entire body, so a
    call that runs past the debounce delay can already have its real
    result on screen by the time ``hide()`` runs. Resetting unconditionally
    there would clobber that result right after showing it -- ``hide()``
    only resets when ``read()`` still returns exactly what ``show()`` last
    wrote, i.e. nothing else has touched the widget since. Used by both
    ``StaticTextSink`` (below) and ``screens/detail_pane.py``'s
    ``DetailPaneLoadingSink`` -- same mechanism, same reasoning, each
    supplying only its own widget-specific ``write_loading``/
    ``write_reset``/``read``."""

    def __init__(
        self, *, write_loading: Callable[[str], None], write_reset: Callable[[], None], read: Callable[[], str]
    ) -> None:
        self._write_loading = write_loading
        self._write_reset = write_reset
        self._read = read
        self._last_shown: str | None = None

    def show(self, frame: str) -> None:
        self._write_loading(frame)
        self._last_shown = self._read()

    def hide(self) -> None:
        if self._last_shown is not None and self._read() == self._last_shown:
            self._write_reset()
        self._last_shown = None


class StaticTextSink:
    """Targets a Store-less screen's own single status ``Static`` (e.g.
    ``ConnectDialog``'s/``DiagnosticsScreen``'s ``#connect-status``/
    ``#diag-status``).

    ``Static.render()`` strips Rich markup, so comparing against its
    plain-text output is what lets ``_ConditionalResetSink`` tell "nothing
    else touched this since I last wrote it" apart from "something already
    superseded me" without parsing the widget's live markup back out --
    the same problem ``TreeNodeLoadingSink`` solves for a ``TreeNode``'s
    plain-text label, solved here the same way for a ``Static``'s
    rendered plain text instead of its markup source."""

    def __init__(self, host: Widget, selector: str, *, base: Callable[[], str]) -> None:
        self._host = host
        self._selector = selector
        self._base = base
        self._core = _ConditionalResetSink(
            write_loading=self._write_loading, write_reset=self._write_reset, read=self._read
        )

    def _static(self) -> Static | None:
        # Tolerates the widget already being gone -- same rationale as
        # NavigableScreen._update_breadcrumb_text: a DebouncedProgress's
        # final stop() can fire after its host screen has already been
        # popped/dismissed.
        with contextlib.suppress(NoMatches):
            return self._host.query_one(self._selector, Static)
        return None

    def _write_loading(self, frame: str) -> None:
        static = self._static()
        if static is not None:
            static.update(f"{self._base()}  [{_LOADING_STYLE}]{frame} Loading[/{_LOADING_STYLE}]")

    def _write_reset(self) -> None:
        static = self._static()
        if static is not None:
            static.update(self._base())

    def _read(self) -> str:
        static = self._static()
        return str(static.render()) if static is not None else ""

    def show(self, frame: str) -> None:
        self._core.show(frame)

    def hide(self) -> None:
        self._core.hide()


class DataTableLoadingRowSink:
    """A flat ``DataTable`` column's own counterpart to
    ``TreeNodeLoadingSink`` -- used for both ``BrowseScreen``'s own
    column 3 (the version list) and ``UnitScreen``'s own file table, each
    with no per-node label to append a suffix onto, so this instead
    appends one trailing ``"{frame} Loading"`` row rather than clearing
    the table — real rows already on screen (a stale-but-real render
    kept visible during a refresh, per ``RemoteData.Loading.previous``)
    stay untouched underneath it instead of being wiped by the spinner.
    ``hide()`` removes exactly that row and no other.

    ``is_current`` (default: always current) is ``show()``'s own
    staleness guard -- a caller whose fetch is for one specific
    row/folder/workload, not the table as a whole, passes a predicate
    reading live selection state, the same shape ``DetailPaneLoadingSink``
    already uses via ``DetailPane._render_if_current``. Deliberately
    asymmetric with that sink, though: only ``show()`` checks it.
    Removing this sink's own tracked row by key is self-scoped and
    always safe regardless of staleness (already idempotent via
    ``hide()``'s own ``RowDoesNotExist`` tolerance below) — unlike
    ``DetailPaneLoadingSink``'s guard, which exists to avoid clobbering
    *real content* a newer selection already wrote, ``hide()`` here never
    touches anything but its own row, so gating it on staleness would
    only risk the opposite failure: a fetch that outlives the user's own
    selection (e.g. switching between two folders that are both already
    empty, so nothing else ever calls ``table.clear()`` to clean this
    row up first) would then never get its own leftover row removed at
    all once its last tick's ``hide()`` skipped it.

    ``hide()`` tolerates the row already being gone: it runs inside
    ``DebouncedProgress.stop()``, called from ``__exit__`` only once the
    ``with`` block's own body -- the whole fetch, dispatch included --
    has returned. Since a successful/failed fetch's own dispatch
    (``VersionsLoaded``/``VersionsLoadFailed``) triggers
    ``BrowseScreen._render_versions()`` synchronously (``Store.dispatch``
    drains inline), the table has typically *already* been cleared and
    repopulated by the time ``hide()`` runs -- taking this sink's own row
    with it. ``remove_row`` would otherwise raise ``RowDoesNotExist``
    every time a fetch actually finishes, which is the common case, not
    an edge one."""

    def __init__(self, table: DataTable[Any], *, is_current: Callable[[], bool] = lambda: True) -> None:
        self._table = table
        self._row_key: RowKey | None = None
        self._is_current = is_current

    def show(self, frame: str) -> None:
        if not self._is_current():
            # The selection this fetch was for has since moved on. An
            # unrelated table.clear() (a real folder/workload switch)
            # usually already took this row with it -- but not always
            # (the empty-to-empty folder-switch case above), so still
            # remove it here rather than assume.
            if self._row_key is not None:
                self._remove_own_row()
                self._row_key = None
            return
        text = f"{frame} Loading"
        if self._row_key is not None:
            # Update the existing row's own cell in place on a repeat
            # tick, rather than remove+re-add just to change one frame
            # character -- cheaper, and avoids a moment with no loading
            # row at all between the remove and the re-add.
            try:
                self._table.update_cell(self._row_key, self._table.ordered_columns[0].key, text)
            except CellDoesNotExist:
                self._row_key = None
            else:
                return
        # Padded to every column, not just the first: add_row() fills a
        # missing trailing cell with None, and default_cell_formatter(None)
        # renders the literal string "None" -- fine for BrowseScreen's own
        # single-column version table, but a multi-column table like
        # UnitScreen's own file table would otherwise show
        # "{frame} Loading | None | None | ...".
        blanks = ("",) * (len(self._table.ordered_columns) - 1)
        self._row_key = self._table.add_row(text, *blanks)

    def hide(self) -> None:
        if self._row_key is not None:
            self._remove_own_row()
            self._row_key = None

    def _remove_own_row(self) -> None:
        with contextlib.suppress(RowDoesNotExist):
            self._table.remove_row(cast("RowKey", self._row_key))


class DebouncedProgress:
    """Owns one loading-indicator's debounced start/stop for the lifetime
    of a single expensive call. Constructing this arms a ``delay``-second
    timer; if ``stop`` is called before it fires, ``sink`` is never
    touched at all — only a call still running past ``delay`` ever
    animates anything.

    A context manager over that same lifetime — ``with
    DebouncedProgress(self): ...`` calls ``stop()`` on the way out
    regardless of how the block exits, the shape every call site needs."""

    def __init__(
        self, screen: _TimerHost | NavigableScreen, sink: _LoadingSink | None = None, *, delay: float | None = None
    ) -> None:
        self._screen = screen
        # sink=None (the default breadcrumb) needs `screen` to actually be
        # a NavigableScreen at runtime, even though the widened parameter
        # type above accepts any _TimerHost -- unlike an explicit sink=,
        # which works with a bare Widget/DOMNode, this one path still has
        # no static check behind it (the runtime -> screens layering ban
        # forbids narrowing the parameter type to prove it). A caller
        # whose host isn't provably a NavigableScreen must pass an
        # explicit sink=; every current default-sink call site is
        # commented at its own use explaining why it's actually safe.
        self._sink: _LoadingSink = sink if sink is not None else _BreadcrumbSink(screen)  # type: ignore[arg-type]
        self._stopped = False
        self._frame = 0
        self._anim_timer: Timer | None = None
        # Resolved here, not as `delay: float = _DEFAULT_DELAY` -- a
        # default-argument expression is bound once at function-definition
        # time, so a module-level `_DEFAULT_DELAY` override (e.g. a test's
        # own autouse fixture) could never reach it that way.
        resolved_delay = _DEFAULT_DELAY if delay is None else delay
        self._timer: Timer = screen.set_timer(resolved_delay, self._start_animating)

    def _start_animating(self) -> None:
        if self._stopped:  # pragma: no cover - defensive: stop() already cancels the timer
            return
        self._tick()  # render the first frame immediately, not one _FRAME_INTERVAL late
        self._anim_timer = self._screen.set_interval(_FRAME_INTERVAL, self._tick)

    def _tick(self) -> None:
        frame = _SPINNER_FRAMES[self._frame % len(_SPINNER_FRAMES)]
        self._frame += 1
        self._sink.show(frame)

    def stop(self) -> None:
        self._stopped = True
        self._timer.stop()
        if self._anim_timer is not None:
            self._anim_timer.stop()
            self._anim_timer = None
            self._sink.hide()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.stop()
