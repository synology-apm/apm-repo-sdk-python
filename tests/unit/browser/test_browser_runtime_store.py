"""Unit tests for ``browser.runtime.store.Store`` — the shared MVU
dispatch loop every screen's own model will run on. No Textual/App/Pilot
involved at all: ``Store`` is plain Python, and this is exactly the kind
of test the rest of this refactor is meant to make possible.

A small self-contained counter model/update/cmd triple stands in for a
real domain's own ``core/*/model.py`` — this file doesn't import one
from a sibling test module (``tests/`` isn't a package, see
``tests/CLAUDE.md``)."""

from __future__ import annotations

import dataclasses
from typing import assert_never

from synology_apm_repo.browser.runtime.store import Store, Subscription


@dataclasses.dataclass(frozen=True, slots=True)
class _Model:
    count: int = 0
    log: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class _Increment:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class _Append:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class _IncrementTwice:
    pass


_Msg = _Increment | _Append | _IncrementTwice


@dataclasses.dataclass(frozen=True, slots=True)
class _Log:
    text: str


_Cmd = _Log


def _update(model: _Model, msg: _Msg) -> tuple[_Model, tuple[_Cmd, ...]]:
    match msg:
        case _Increment():
            new_count = model.count + 1
            return dataclasses.replace(model, count=new_count), (_Log(text=f"count={new_count}"),)
        case _IncrementTwice():
            new_count = model.count + 2
            return dataclasses.replace(model, count=new_count), (
                _Log(text=f"first:{new_count - 1}"),
                _Log(text=f"second:{new_count}"),
            )
        case _Append(text=text):
            # Deliberately leaves `count` untouched and unreplaced -- a
            # fresh tuple would still compare == to the old one, but
            # dataclasses.replace() only touching `log` here is what lets
            # test_unrelated_dispatch_does_not_render_an_untouched_slice
            # below exercise the real "same object" identity path rather
            # than an incidentally-equal new one.
            return dataclasses.replace(model, log=(*model.log, text)), ()
        case _:
            assert_never(msg)


def test_dispatch_updates_the_model() -> None:
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    store.dispatch(_Increment())
    assert store.model == _Model(count=1)


def test_dispatch_performs_every_returned_command() -> None:
    performed: list[str] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: performed.append(cmd.text))
    store.dispatch(_Increment())
    assert performed == ["count=1"]


def test_commands_are_performed_in_the_order_update_returned_them() -> None:
    performed: list[str] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: performed.append(cmd.text))
    store.dispatch(_IncrementTwice())
    assert performed == ["first:1", "second:2"]


def test_subscribe_with_init_true_renders_immediately() -> None:
    rendered: list[int] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(count=5), _update, lambda cmd: None)
    store.subscribe(lambda m: m.count, rendered.append)  # init=True is the default
    assert rendered == [5]


def test_subscribe_with_init_false_does_not_render_immediately() -> None:
    rendered: list[int] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(count=5), _update, lambda cmd: None)
    store.subscribe(lambda m: m.count, rendered.append, init=False)
    assert rendered == []


def test_subscribe_renders_again_when_the_selected_slice_changes() -> None:
    rendered: list[int] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    store.subscribe(lambda m: m.count, rendered.append, init=False)
    store.dispatch(_Increment())
    store.dispatch(_Increment())
    assert rendered == [1, 2]


def test_unrelated_dispatch_does_not_render_an_untouched_slice() -> None:
    """``_Append`` never touches ``count`` -- the whole point of
    slice-diffed subscriptions is that this dispatch costs this
    subscriber nothing at all."""
    rendered: list[int] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    store.subscribe(lambda m: m.count, rendered.append, init=False)
    store.dispatch(_Append(text="hello"))
    assert rendered == []


def test_notify_runs_before_perform_for_the_same_dispatch() -> None:
    events: list[str] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: events.append(f"perform:{cmd.text}"))
    store.subscribe(lambda m: m.count, lambda count: events.append(f"render:{count}"), init=False)
    store.dispatch(_Increment())
    assert events == ["render:1", "perform:count=1"]


def test_a_command_dispatching_again_is_deferred_to_a_second_drain_pass() -> None:
    """A ``perform`` handler that calls ``store.dispatch(...)`` again
    (a synchronous command reacting to its own effect, e.g. "show a
    notification then immediately update again") must not have that
    second dispatch run ``update()`` while the first dispatch's own
    ``perform`` loop is still in progress."""
    events: list[str] = []

    def perform(cmd: _Cmd) -> None:
        events.append(f"perform:{cmd.text}")
        if cmd.text == "count=1":
            store.dispatch(_Increment())

    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, perform)
    store.subscribe(lambda m: m.count, lambda count: events.append(f"render:{count}"), init=False)

    store.dispatch(_Increment())

    assert events == ["render:1", "perform:count=1", "render:2", "perform:count=2"]


def test_unsubscribe_stops_further_renders() -> None:
    rendered: list[int] = []
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    subscription = store.subscribe(lambda m: m.count, rendered.append, init=False)
    store.dispatch(_Increment())
    subscription.unsubscribe()
    store.dispatch(_Increment())
    assert rendered == [1]


def test_a_subscribers_own_render_unsubscribing_itself_does_not_skip_the_next_subscriber() -> None:
    """A render callback can synchronously unsubscribe itself (e.g. a
    screen popping itself in reaction to the very value just rendered).
    ``list.remove()`` during a live ``for`` loop over that same list
    shifts a later entry into the removed one's own index, and the
    loop's cursor then advances past it -- silently skipping that
    subscriber for this notify pass even though its own slice changed
    too. Proves ``_notify`` iterates a snapshot, not the live list."""
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    rendered_b: list[int] = []
    subscription_a: Subscription | None = None

    def _render_a(count: int) -> None:
        assert subscription_a is not None
        subscription_a.unsubscribe()

    subscription_a = store.subscribe(lambda m: m.count, _render_a, init=False)
    store.subscribe(lambda m: m.count, rendered_b.append, init=False)

    store.dispatch(_Increment())

    assert rendered_b == [1]


def test_unsubscribe_is_safe_to_call_more_than_once() -> None:
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    subscription = store.subscribe(lambda m: m.count, lambda count: None, init=False)
    subscription.unsubscribe()
    subscription.unsubscribe()  # must not raise


def test_close_makes_further_dispatch_a_no_op() -> None:
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    store.close()
    store.dispatch(_Increment())
    assert store.model == _Model()  # unchanged -- update() never ran


def test_close_drops_every_subscription() -> None:
    """A worker whose result lands after its screen unmounted must not
    be able to call back into a removed widget -- close() clears
    subscriptions outright rather than relying on dispatch() alone to
    guard every path."""
    store: Store[_Model, _Msg, _Cmd] = Store(_Model(), _update, lambda cmd: None)
    store.subscribe(lambda m: m.count, lambda count: None, init=False)
    store.close()
    assert store._subs == []  # whitebox: no public way to observe an empty subscriber list
