"""``DebouncedProgress``: a loading indicator shown only once a call has
run for 300 ms, so a quick call never touches the screen.

A ``LoadingSink`` decides where it renders (``browser/README.md``'s
placement rule): the screen's breadcrumb by default, ``TreeNodeLoadingSink``
(a tree node's label), ``DataTableLoadingRowSink`` (a trailing table row) or
``StaticTextSink`` (a status line).

Construct and stop it on the App's event loop, since it arms timers.
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


_DELAY = 0.3
#: Every tick repaints the sink's target, which is costly in Textual (the
#: stylesheet is reapplied per widget); 500ms still reads as "running".
_FRAME_INTERVAL = 0.5
#: A Braille dot spinner, never mistaken for real content.
_SPINNER_FRAMES = "⠁⠉⠙⠹⠼⠶⠧⠇⠃"
#: Deliberately loud: the one signal that something is still running. For
#: markup-parsing targets only; a ``TreeNode`` label gets plain text.
_LOADING_STYLE = "bold bright_yellow"


class LoadingSink(Protocol):
    """Where one ``DebouncedProgress``'s frames render: ``show`` once per
    tick with the current frame, ``hide`` once on ``stop()`` if ``show``
    ever ran."""

    def show(self, frame: str) -> None: ...

    def hide(self) -> None: ...


class _TimerHost(Protocol):
    """What ``DebouncedProgress`` needs from a host given an explicit
    ``sink``: timers."""

    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...

    def set_interval(self, interval: float, callback: Callable[[], None]) -> Timer: ...


class _BreadcrumbHost(_TimerHost, Protocol):
    """A screen with a breadcrumb: ``NavigableScreen``, named structurally
    since widgets/ may not import screens/."""

    def _set_loading_indicator(self, markup: str | None) -> None: ...


class _BreadcrumbSink:
    """Appends the styled spinner frame to the screen's own breadcrumb text
    via ``NavigableScreen._set_loading_indicator``."""

    def __init__(self, screen: _BreadcrumbHost) -> None:
        self._screen = screen

    def show(self, frame: str) -> None:
        self._screen._set_loading_indicator(  # noqa: SLF001 - NavigableScreen's documented hook for exactly this call
            f"[{_LOADING_STYLE}]{frame} Loading[/{_LOADING_STYLE}]"
        )

    def hide(self) -> None:
        self._screen._set_loading_indicator(None)  # noqa: SLF001 - NavigableScreen's documented hook for exactly this call


class TreeNodeLoadingSink:
    """Appends the frame to one ``TreeNode``'s label as plain text. Each
    ``show``/``hide`` strips its own suffix from the current label rather
    than restoring a snapshot, so a relabel mid-load (e.g. a verbose
    toggle) survives."""

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
    """Bookkeeping for a sink whose widget the operation's result may also
    write into before ``hide()`` runs: ``hide()`` resets only when
    ``read()`` still returns what ``show()`` last wrote."""

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
    """Appends the frame to a screen's status ``Static`` (e.g.
    ``ConnectDialog``'s ``#connect-status``); ``hide()`` resets it to
    ``base()`` only if nothing else has written since."""

    def __init__(self, host: Widget, selector: str, *, base: Callable[[], str]) -> None:
        self._host = host
        self._selector = selector
        self._base = base
        self._core = _ConditionalResetSink(
            write_loading=self._write_loading, write_reset=self._write_reset, read=self._read
        )

    def _static(self) -> Static | None:
        # The widget may be gone: a final stop() can fire after the host
        # screen was popped/dismissed.
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
    """Appends one trailing ``"{frame} Loading"`` row to a ``DataTable``;
    ``hide()`` removes it, tolerating it already being gone. ``show()``
    adds nothing once ``is_current()`` is false (the fetch's folder or
    workload is no longer selected)."""

    def __init__(self, table: DataTable[Any], *, is_current: Callable[[], bool] = lambda: True) -> None:
        self._table = table
        self._row_key: RowKey | None = None
        self._is_current = is_current

    def show(self, frame: str) -> None:
        if not self._is_current():
            # The selection moved on. A table.clear() usually took this
            # row with it already, but not always, so remove it if tracked.
            if self._row_key is not None:
                self._remove_own_row()
                self._row_key = None
            return
        text = f"{frame} Loading"
        if self._row_key is not None:
            # Repeat tick: update the cell in place rather than remove+re-add.
            try:
                self._table.update_cell(self._row_key, self._table.ordered_columns[0].key, text)
            except CellDoesNotExist:
                self._row_key = None
            else:
                return
        # Padded to every column: a missing trailing cell would render as
        # the literal string "None" in a multi-column table.
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
    """One call's debounced loading indicator. Constructing it arms a 300 ms
    timer; if ``stop`` runs first, ``sink`` is never touched. As a context
    manager, it stops on exit.

    ``sink=None`` uses the breadcrumb, which needs ``screen`` to be a
    ``NavigableScreen``; any other host passes a ``sink``."""

    def __init__(self, screen: _TimerHost | _BreadcrumbHost, sink: LoadingSink | None = None) -> None:
        self._screen = screen
        self._sink: LoadingSink = sink if sink is not None else _BreadcrumbSink(screen)  # type: ignore[arg-type]
        self._stopped = False
        self._frame = 0
        self._anim_timer: Timer | None = None
        self._timer: Timer = screen.set_timer(_DELAY, self._start_animating)

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
