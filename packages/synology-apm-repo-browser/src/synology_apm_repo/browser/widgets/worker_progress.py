"""Worker launchers that show a ``DebouncedProgress`` busy indicator by
default: ``work`` for a ``@work`` method on a ``Widget``/``Screen``/``App``
(its ``self`` hosts the indicator's timers), ``run_worker_with_progress``/
``run_worker_no_progress`` for ``runtime/*_effects.py``, whose ``self`` is a
plain object. Ruff's ``TID251`` bans ``textual.work`` outside this module,
and an ast-walk test (``test_browser_runtime_effects_use_progress_helpers.py``)
makes every ``runtime/*_effects.py`` dispatch use one of the other two;
where the sink points is each call site's choice.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, cast, overload

from textual import work as _textual_work

from synology_apm_repo.browser.widgets.progress_hint import DebouncedProgress, LoadingSink

if TYPE_CHECKING:
    from textual.dom import DOMNode
    from textual.worker import Worker


#: Called with the decorated method's ``(self, *args, **kwargs)`` on every
#: call, for a sink whose target is known only then (e.g. the tree node
#: being expanded).
SinkFactory = Callable[..., "LoadingSink | None"]


@overload
def work[**P, R](func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Worker[R]]: ...


@overload
def work[**P, R](
    *,
    sink: SinkFactory | None = None,
    busy: bool = True,
    **work_kwargs: Any,
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Callable[P, Worker[R]]]: ...


def work[**P, R](
    func: Callable[P, Coroutine[Any, Any, R]] | None = None,
    *,
    sink: SinkFactory | None = None,
    busy: bool = True,
    **work_kwargs: Any,
) -> Any:
    """Drop-in replacement for ``textual.work``: same bare (``@work``) and
    parameterized (``@work(group=..., ...)``) call shapes, every other
    kwarg forwarded to ``textual.work`` unchanged. Additionally wraps the
    decorated coroutine's body in ``DebouncedProgress``, rendering to
    ``sink(self, *args, **kwargs)`` (the breadcrumb when ``sink`` is
    ``None``).

    ``busy=False`` is the explicit opt-out, for a worker that delegates its
    entire body to an already-wrapped helper; explain it in a comment at
    the call site.

    Raises:
        ValueError: ``thread=True`` was passed -- ``DebouncedProgress``
            arms Textual timers, which only the app's event loop may touch.
    """
    if work_kwargs.get("thread"):
        raise ValueError("work(): thread=True can't be combined with a DebouncedProgress wrap (busy=True default)")

    def decorator(inner: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Worker[R]]:
        if not busy:
            target = inner
        else:

            @functools.wraps(inner)
            async def target(*args: P.args, **kwargs: P.kwargs) -> R:
                # args[0] is the bound `self`; ParamSpec can't say so.
                host = cast(Any, args[0])
                actual_sink = sink(*args, **kwargs) if sink is not None else None
                with DebouncedProgress(host, actual_sink):
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
    sink: LoadingSink | None = None,
    group: str = "",
    name: str = "",
    exclusive: bool = False,
) -> Worker[None]:
    """``host.run_worker(factory)`` with ``factory()`` wrapped in
    ``DebouncedProgress(host, sink)`` (the breadcrumb when ``sink`` is
    ``None``)."""

    async def wrapped() -> None:
        with DebouncedProgress(host, sink):
            await factory()

    return host.run_worker(wrapped, group=group, name=name, exclusive=exclusive)


def run_worker_no_progress(
    host: DOMNode, factory: Callable[[], Coroutine[Any, Any, None]], *, group: str = "", name: str = ""
) -> Worker[None]:
    """``host.run_worker(factory)`` with no busy indicator, for a dispatch
    with its own progress UI or background cleanup."""
    return host.run_worker(factory, group=group, name=name)
