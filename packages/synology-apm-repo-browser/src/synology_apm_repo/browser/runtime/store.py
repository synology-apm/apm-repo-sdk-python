"""``Store``: the MVU dispatch loop -- ``dispatch(msg)`` runs the pure
``update(model, msg) -> (model, cmds)``, notifies every subscriber whose
slice changed, then hands each returned ``Cmd`` to ``perform``. Each
``Store``-backed screen owns one, plus the one app-level store.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from typing import Any


class _Entry[ModelT, SliceT]:
    """One subscription; ``last`` is the slice value it last saw."""

    def __init__(self, select: Callable[[ModelT], SliceT], render: Callable[[SliceT], None], last: SliceT) -> None:
        self.select = select
        self.render = render
        self.last = last


class Subscription:
    """Returned by ``Store.subscribe``; call ``.unsubscribe()`` when the
    view goes away (its ``on_unmount``) so a later dispatch can't call
    into a removed widget. Idempotent."""

    def __init__(self, unsubscribe: Callable[[], None]) -> None:
        self._unsubscribe = unsubscribe
        self._active = True

    def unsubscribe(self) -> None:
        if self._active:
            self._active = False
            self._unsubscribe()


class Store[ModelT, MsgT, CmdT]:
    """One screen's (or the app's) MVU loop. ``update`` is pure and
    synchronous (``core/*/update.py``); ``perform`` is where a ``Cmd``
    does anything (``runtime/*_effects.py``)."""

    def __init__(
        self,
        model: ModelT,
        update: Callable[[ModelT, MsgT], tuple[ModelT, tuple[CmdT, ...]]],
        perform: Callable[[CmdT], None],
    ) -> None:
        self._model = model
        self._update = update
        self._perform = perform
        # Each subscription has its own slice type, erased here.
        self._subs: list[_Entry[ModelT, Any]] = []
        self._queue: deque[MsgT] = deque()
        self._draining = False
        self._closed = False

    @property
    def model(self) -> ModelT:
        return self._model

    def dispatch(self, msg: MsgT) -> None:
        """Queues ``msg`` and drains the queue, unless a drain further up
        the call stack will. A no-op once ``close()`` has run."""
        if self._closed:
            return
        self._queue.append(msg)
        if not self._draining:
            self._drain()

    def _drain(self) -> None:
        """Runs every queued message through ``update``, notifies
        subscribers once for the batch, then performs the returned
        ``Cmd``s, so a UI change is on screen before the effect that will
        replace it starts. A dispatch from ``perform`` runs in a second
        pass."""
        self._draining = True
        try:
            commands: list[CmdT] = []
            while self._queue:
                msg = self._queue.popleft()
                self._model, cmds = self._update(self._model, msg)
                commands.extend(cmds)
            self._notify()
            for cmd in commands:
                self._perform(cmd)
        finally:
            self._draining = False
        if self._queue:
            self._drain()

    def _notify(self) -> None:
        # A snapshot: a render callback may unsubscribe mid-loop.
        model = self._model
        for entry in tuple(self._subs):
            value = entry.select(model)
            if value != entry.last:
                entry.last = value
                entry.render(value)

    def subscribe[SliceT](
        self, select: Callable[[ModelT], SliceT], render: Callable[[SliceT], None], *, init: bool = True
    ) -> Subscription:
        """Calls ``render(select(model))`` whenever the selected value
        changes (``!=``). ``init=True`` also renders once now, like
        Textual's ``watch(..., init=True)``; either way the current value
        is the baseline."""
        value = select(self._model)
        entry: _Entry[ModelT, Any] = _Entry(select, render, value)
        self._subs.append(entry)
        if init:
            render(value)
        return Subscription(lambda: self._remove(entry))

    def _remove(self, entry: _Entry[ModelT, Any]) -> None:
        if entry in self._subs:
            self._subs.remove(entry)

    def close(self) -> None:
        """Makes every further ``dispatch`` a no-op and drops every
        subscription. Call before tearing down workers, so a result in
        flight finds the store closed."""
        self._closed = True
        self._subs.clear()
