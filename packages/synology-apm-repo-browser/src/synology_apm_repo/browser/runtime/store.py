"""``Store``: the MVU dispatch loop shared by every screen's model —
``dispatch(msg)`` runs the pure ``update(model, msg) -> (model, cmds)``
to completion, notifies every subscribed view of whichever slices
actually changed, then hands each returned ``Cmd`` to ``perform`` to run.

Each screen owns one ``Store`` instance (plus one shared app-level store)
rather than the package sharing a single global model, so a subscriber
only re-runs its selector on a dispatch that could touch its slice.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from typing import Any, Generic, TypeVar

ModelT = TypeVar("ModelT")
MsgT = TypeVar("MsgT")
CmdT = TypeVar("CmdT")
SliceT = TypeVar("SliceT")


class _Entry(Generic[ModelT, SliceT]):
    """``last`` always starts as a real, already-computed value, not a
    "never rendered yet" sentinel — ``init=False`` skips only the first
    render, never the baseline capture."""

    def __init__(self, select: Callable[[ModelT], SliceT], render: Callable[[SliceT], None], last: SliceT) -> None:
        self.select = select
        self.render = render
        self.last = last


class Subscription:
    """Returned by ``Store.subscribe`` — call ``.unsubscribe()`` when the
    view that requested it goes away (a screen's own ``on_unmount``), so
    a dispatch that lands after can't call back into a removed widget.
    Safe to call more than once; every call after the first is a no-op."""

    def __init__(self, unsubscribe: Callable[[], None]) -> None:
        self._unsubscribe = unsubscribe
        self._active = True

    def unsubscribe(self) -> None:
        if self._active:
            self._active = False
            self._unsubscribe()


class Store(Generic[ModelT, MsgT, CmdT]):
    """One screen's (or the app's) own MVU loop. ``update`` must be a
    pure, synchronous function — see ``core/*/update.py`` in each
    domain. ``perform`` is the one place a ``Cmd`` actually does
    anything (starts a worker, calls ``notify``, pushes a screen, ...);
    see ``runtime/app_effects.py``/``runtime/unit_effects.py``."""

    def __init__(
        self,
        model: ModelT,
        update: Callable[[ModelT, MsgT], tuple[ModelT, tuple[CmdT, ...]]],
        perform: Callable[[CmdT], None],
    ) -> None:
        self._model = model
        self._update = update
        self._perform = perform
        # Each subscription's own slice type is real (see subscribe()'s
        # signature) but unrelated to every other subscription's -- Any
        # is this list's deliberate type-erasure boundary, not laziness.
        self._subs: list[_Entry[ModelT, Any]] = []
        self._queue: deque[MsgT] = deque()
        self._draining = False
        self._closed = False

    @property
    def model(self) -> ModelT:
        return self._model

    def dispatch(self, msg: MsgT) -> None:
        """Queues ``msg`` and drains the queue to completion, unless a
        drain is already running further up the call stack (a ``Cmd``'s
        ``perform`` handler dispatching again queues for a second,
        separate pass instead of recursing). A no-op once ``close()`` has
        run."""
        if self._closed:
            return
        self._queue.append(msg)
        if not self._draining:
            self._drain()

    def _drain(self) -> None:
        """Runs every queued message through ``update``, notifies
        subscribers once for the whole batch (not once per message), then
        performs every returned ``Cmd`` only after that render has
        happened -- a ``Cmd`` that starts a worker must not see stale
        state, and a UI change must already be on screen before the
        effect that will replace it starts. A ``Cmd`` whose ``perform``
        dispatches again queues rather than recursing, deferred by
        ``_draining`` to a second pass once this one finishes."""
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
        # Iterates a snapshot, not self._subs itself: a render callback
        # can synchronously unsubscribe, mutating self._subs mid-loop and
        # silently skipping a later subscriber otherwise.
        model = self._model
        for entry in tuple(self._subs):
            value = entry.select(model)
            if value != entry.last:
                entry.last = value
                entry.render(value)

    def subscribe(
        self, select: Callable[[ModelT], SliceT], render: Callable[[SliceT], None], *, init: bool = True
    ) -> Subscription:
        """Calls ``render(select(model))`` again only when ``select``'s
        return value actually changes (``!=``, not on every dispatch) —
        an untouched slice stays the same object, making this an O(1)
        identity check for the common case. ``init=True`` (the default)
        renders once immediately, matching Textual's ``watch(...,
        init=True)``; pass ``init=False`` when the caller renders its own
        first frame some other way. Either way, the current value is
        captured as the comparison baseline now."""
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
        subscription. Call before tearing down workers, not after: a
        result already in flight when workers start draining must still
        find the store closed."""
        self._closed = True
        self._subs.clear()
