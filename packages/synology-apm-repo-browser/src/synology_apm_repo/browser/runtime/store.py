"""``Store``: the MVU dispatch loop shared by every screen's own model —
``dispatch(msg)`` runs the pure ``update(model, msg) -> (model, cmds)``
to completion, notifies every subscribed view of whichever slices
actually changed, then hands each returned ``Cmd`` to ``perform`` (see
``runtime/app_effects.py``/``runtime/unit_effects.py``) to actually run.

Each screen owns one ``Store`` instance (plus one shared app-level
store) rather than the whole package sharing a single global model — a
global model would mean every subscribed view on every mounted screen
re-runs its selector on every dispatch, defeating the point of
subscribing to a slice at all.
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
    """``last`` always starts as a real, already-computed value (see
    ``Store.subscribe``, the only place this is constructed) rather than
    a "never rendered yet" sentinel — ``init=False`` skips the first
    *render*, never the baseline capture, so the next real notify has
    something genuine to compare against instead of looking like the
    first one ever."""

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
        """Queues ``msg`` and drains the queue to completion (unless a
        drain is already running further up the call stack -- a ``Cmd``'s
        own ``perform`` handler calling ``dispatch`` again queues that
        message for a second, separate drain pass instead of recursing
        into ``update`` mid-batch). A no-op once ``close()`` has run:
        a worker whose result lands after its screen unmounted has
        nothing left to notify."""
        if self._closed:
            return
        self._queue.append(msg)
        if not self._draining:
            self._drain()

    def _drain(self) -> None:
        """Runs every currently-queued message through ``update`` (model
        first, cheaply, no rendering yet), notifies subscribers exactly
        once for the whole batch — not once per message — then performs
        every returned ``Cmd``, in order, only after that render has
        already happened. That ordering matters: a ``Cmd`` that starts a
        worker must not be able to see stale state, and a UI change (a
        newly-``Loading`` slice, an Esc-cleared list) must already be on
        screen before the effect that will eventually replace it even
        starts — see browser/README.md's 200ms Esc-cancellation budget,
        which this makes independent of how fast a worker actually
        reaches its first ``await``.

        A ``Cmd`` whose own ``perform`` handler calls ``dispatch`` again
        (a synchronous command, e.g. "show a notification") queues that
        message rather than recursing into ``update`` immediately — the
        ``_draining`` guard below defers it to a second, separate drain
        pass once this one's ``perform`` loop has finished, so a command
        can never observe ``update`` mid-batch."""
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
        # can synchronously unsubscribe (its own screen popping itself,
        # say, in reaction to the very value just rendered), which
        # mutates self._subs mid-loop -- list.remove() during a live
        # `for` shifts a later entry into the removed one's index and
        # the loop's own cursor skips past it, silently dropping that
        # subscriber from this notify pass.
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
        own return value actually changes (``!=``, not on every
        dispatch) — an ``update()`` branch that returns an untouched
        slice unchanged (the same object, not just an equal one — see
        each domain's own ``update.py``) makes this comparison an O(1)
        identity check for the common "this dispatch didn't touch that
        part of the model" case, rather than a deep comparison.
        ``init=True`` (the default) renders once immediately, matching
        Textual's own ``watch(..., init=True)``; pass ``init=False``
        when the caller will render its own first frame some other way.
        Either way, the current value is captured as the comparison
        baseline right now -- skipping that on ``init=False`` would make
        the *next* notify (even one from a dispatch this slice was never
        touched by) look like the first one ever, and render
        unconditionally instead of comparing against a real baseline."""
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
        subscription, so a result that lands after this screen has
        already unmounted can neither run ``update`` nor render into a
        widget that no longer exists. Call before tearing down workers,
        not after: a worker's cancellation only takes effect once it
        reaches its next ``await``, so a result already in flight when
        workers start draining must still find the store closed."""
        self._closed = True
        self._subs.clear()
