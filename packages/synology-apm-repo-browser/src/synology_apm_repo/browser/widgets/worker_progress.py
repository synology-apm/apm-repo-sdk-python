"""Structural coverage for busy-indicator wraps: ``work``/
``run_worker_with_progress``/``run_worker_no_progress`` below make
showing a ``DebouncedProgress`` while a worker runs the default,
opt-out behavior instead of something a call site has to remember to
add. ``tests/unit/browser/
test_browser_screens_use_tracked_work.py`` and
``test_browser_effects_use_progress_helpers.py`` enforce that every
``screens/*.py`` worker and every ``runtime/*_effects.py`` dispatch goes
through one of these three, mirroring ``test_browser_core_no_textual_import.py``'s
own ast-walk technique for a different rule.

``work`` is for a ``@work``-decorated method on a real ``Widget``/
``Screen``/``App`` (``self`` provides the timer host ``DebouncedProgress``
needs). ``run_worker_with_progress``/``run_worker_no_progress`` are the
``runtime/*_effects.py`` counterpart: an ``Effects`` instance's own
``self`` is a plain object, never a ``Widget`` (this package's own
``runtime`` -> ``screens`` layering ban), so ``textual.work`` doesn't
apply there at all -- these wrap the same ``host.run_worker(...)`` call
those classes already made by hand.

*Where* a wrap's sink should point (which column, which tree node, the
screen-wide breadcrumb) is a placement judgment left to each call site --
neither this module nor its enforcement tests can check that a sink
points at the right widget, only that a sink-shaped wrap exists at all.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, overload

from textual import work as _textual_work

from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress, _LoadingSink

if TYPE_CHECKING:
    from textual.dom import DOMNode
    from textual.worker import Worker

_P = ParamSpec("_P")
_R = TypeVar("_R")

#: Called with the same ``(self, *args, **kwargs)`` the decorated method
#: itself receives, evaluated fresh on every call -- needed for a sink
#: like ``TreeNodeLoadingSink`` whose target isn't known until call time
#: (e.g. which tree node is being expanded). ``None`` (the default) uses
#: ``DebouncedProgress``'s own default (the screen's breadcrumb).
SinkFactory = Callable[..., "_LoadingSink | None"]


@overload
def work(func: Callable[_P, Coroutine[Any, Any, _R]]) -> Callable[_P, Worker[_R]]: ...


@overload
def work(
    *,
    sink: SinkFactory | None = None,
    busy: bool = True,
    delay: float | None = None,
    **work_kwargs: Any,
) -> Callable[[Callable[_P, Coroutine[Any, Any, _R]]], Callable[_P, Worker[_R]]]: ...


def work(
    func: Callable[_P, Coroutine[Any, Any, _R]] | None = None,
    *,
    sink: SinkFactory | None = None,
    busy: bool = True,
    delay: float | None = None,
    **work_kwargs: Any,
) -> Any:
    """Drop-in replacement for ``textual.work``: same bare (``@work``) and
    parameterized (``@work(group=..., ...)``) call shapes, every other
    kwarg forwarded to ``textual.work`` unchanged. Additionally wraps the
    decorated coroutine's own body in ``DebouncedProgress`` by default,
    using ``sink(self, *args, **kwargs)`` (or the breadcrumb default when
    ``sink`` is ``None``) as where it renders.

    ``busy=False`` is the one explicit, visible opt-out — for a worker
    that delegates its entire body to an already-wrapped helper (so
    wrapping again would double the same breadcrumb's own debounce/
    animation state for no benefit). Every use needs a comment at the
    call site explaining why; that's a convention this module's own
    enforcement test can't check (it only checks *that* a worker goes
    through ``work``, never *whether* ``busy=False`` was the right call).

    Rejects ``thread=True`` outright: ``DebouncedProgress`` arms Textual
    timers, which must only ever be touched from the app's own event
    loop — a thread worker could never drive one safely, so this turns a
    latent race into a loud, immediate error at decoration time instead."""
    if work_kwargs.get("thread"):
        raise ValueError("work(): thread=True can't be combined with a DebouncedProgress wrap (busy=True default)")

    def decorator(inner: Callable[_P, Coroutine[Any, Any, _R]]) -> Callable[_P, Worker[_R]]:
        if not busy:
            target = inner
        else:

            @functools.wraps(inner)
            async def target(*args: _P.args, **kwargs: _P.kwargs) -> _R:
                # args[0] is the bound `self` -- every use here decorates an
                # instance method; ParamSpec.args has no way to say that
                # statically, so this one boundary crossing needs Any.
                host = cast(Any, args[0])
                actual_sink = sink(*args, **kwargs) if sink is not None else None
                progress_kwargs = {} if delay is None else {"delay": delay}
                with DebouncedProgress(host, actual_sink, **progress_kwargs):
                    return await inner(*args, **kwargs)

        # textual.work ships without inline type stubs for this call shape.
        return _textual_work(**work_kwargs)(target)  # type: ignore[no-untyped-call,no-any-return]

    if func is not None:
        return decorator(func)
    return decorator


def run_worker_with_progress(
    host: DOMNode,
    factory: Callable[[], Coroutine[Any, Any, None]],
    *,
    sink: _LoadingSink | None = None,
    delay: float | None = None,
    group: str = "",
    name: str = "",
) -> Worker[None]:
    """``Effects.perform()``'s replacement for a bare
    ``host.run_worker(functools.partial(...))`` call, for a coroutine
    whose own ``self`` (a ``BrowseEffects``/``UnitEffects`` instance) is
    never a ``Widget`` — wraps ``factory()``'s own body in
    ``DebouncedProgress(host, sink)`` before launching it."""
    progress_kwargs = {} if delay is None else {"delay": delay}

    async def wrapped() -> None:
        with DebouncedProgress(host, sink, **progress_kwargs):
            await factory()

    return host.run_worker(wrapped, group=group, name=name)


def run_worker_no_progress(
    host: DOMNode, factory: Callable[[], Coroutine[Any, Any, None]], *, group: str = "", name: str = ""
) -> Worker[None]:
    """The explicit opt-out twin of ``run_worker_with_progress`` — a
    plain, undecorated ``host.run_worker(...)`` call by another name, so
    a dispatch that deliberately shows no busy feedback (background
    cleanup that outlives any one screen, never user-visible) is a
    visible, greppable choice rather than code that merely looks like
    every other case forgot the wrap."""
    return host.run_worker(factory, group=group, name=name)
