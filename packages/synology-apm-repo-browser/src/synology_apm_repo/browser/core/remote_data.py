"""``RemoteData``: the state of one piece of data fetched from the SDK —
``NotAsked`` (never requested), ``Loading`` (in flight, optionally
carrying the last known-good value), ``Success``, or ``FailureInfo``. A
single ``core.*`` model field holding one of these covers "is it loading",
"is it an error", and "what's the error text" together, instead of three
separate attributes per fetched value.

``Loading.previous`` lets a reload render the slot's last-known value
instead of blanking the screen while a refetch is in flight — see
``value_or_stale``.
"""

from __future__ import annotations

import dataclasses
import enum
from typing import Generic, TypeAlias, TypeVar

T = TypeVar("T")


class FailureKind(enum.StrEnum):
    """Distinguishes the one failure branch ``update()`` must react to
    (pushing ``KeyDialog``) from every other failure, which just renders
    ``FailureInfo.message`` and does nothing else."""

    #: ``KeyRequiredError``/``KeyMismatchError`` — the repository/catalog
    #: needs a key before this fetch can succeed.
    KEY_REQUIRED = "key_required"
    NOT_FOUND = "not_found"
    OTHER = "other"


@dataclasses.dataclass(frozen=True, slots=True)
class FailureInfo:
    """``message`` is already ``str(exc)`` — this TUI never gates an
    ``ApmRepoError``'s detail behind verbose mode (``ARCHITECTURE.md``'s
    Presentation section), so nothing downstream ever needs the original
    exception object. Keeping the exception
    itself out of the model is what keeps a model comparable
    (``==``/``!=``) and a failure assertable in a plain unit test."""

    message: str
    kind: FailureKind = FailureKind.OTHER


@dataclasses.dataclass(frozen=True, slots=True)
class NotAsked:
    """Nothing has fetched this value yet."""


@dataclasses.dataclass(frozen=True, slots=True)
class NoValue:
    """The sentinel ``Loading.previous``/``value_or_stale`` use in place
    of bare ``None``, so a ``T`` that's itself ``Optional`` doesn't get
    confused with "nothing resolved yet." Checked with ``isinstance``,
    like every other ``RemoteData`` variant -- never ``is``/``==`` against
    one particular instance."""


@dataclasses.dataclass(frozen=True, slots=True)
class Loading(Generic[T]):
    """A fetch is in flight. ``previous`` is the last ``Success.value``
    this slot held, or a ``NoValue()`` on a first load."""

    previous: T | NoValue = NoValue()


@dataclasses.dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    value: T


#: Every ``update()`` branch matches exhaustively on these four cases
#: (``case _: assert_never(data)``), so a fifth case can never be added
#: without every existing ``match`` block failing to type-check.
RemoteData: TypeAlias = NotAsked | Loading[T] | Success[T] | FailureInfo


def loading_preserving(previous: RemoteData[T]) -> Loading[T]:
    """The one place ``update()`` decides whether a fresh ``Loading()``
    should carry the slot's own last-known value forward -- only when
    ``previous`` was a real ``Success``; ``NotAsked``/``Loading``/
    ``FailureInfo`` have nothing worth keeping. A policy decided once
    here, instead of re-argued at every call site that resets a slot
    before dispatching a fetch."""
    if isinstance(previous, Success):
        return Loading(previous=previous.value)
    return Loading()


def is_pending_or_done(data: RemoteData[T]) -> bool:
    """Whether ``data`` must not be (re-)fetched: a ``Loading`` fetch is
    already in flight, or a ``Success`` already has the answer. Re-dispatching
    while ``Loading`` duplicates the worker and its loading indicator on
    whatever widget the fetch is anchored to -- exactly as wasteful as
    re-dispatching an already-``Success`` slot, which is why both share one
    guard instead of each call site hand-writing its own ``isinstance``
    check (and risking the same "forgot `Loading`" gap more than once)."""
    return isinstance(data, Loading | Success)


def value_or_stale(data: RemoteData[T]) -> T | NoValue:
    """The value a selector should render for ``data``: a real
    ``Success``'s value, or a refreshing ``Loading``'s own carried-forward
    ``previous`` (see ``loading_preserving`` above) -- both rendered
    identically, since a caller only cares "is there something real to
    show," not which of the two put it there. ``NoValue()`` for
    ``NotAsked``, a first ``Loading`` with nothing stale yet, or
    ``FailureInfo`` (a caller renders that error separately, via its own
    ``message``). The one place this "unwrap or fall back to
    stale-while-revalidate" policy is decided, instead of re-argued at
    every selector that reads a ``RemoteData`` field and wants ordinary
    content rendering, not the ``NotAsked``/``Loading``/``FailureInfo``
    cases themselves."""
    if isinstance(data, Success):
        return data.value
    if isinstance(data, Loading) and not isinstance(data.previous, NoValue):
        return data.previous
    return NoValue()


def has_ever_resolved(data: RemoteData[T]) -> bool:
    """Whether ``value_or_stale(data)`` would return a real value rather
    than a ``NoValue()`` -- for a caller that, unlike ``value_or_stale``'s
    own callers, renders an explicit empty-state placeholder and so needs
    to tell a genuinely resolved-empty answer apart from one that simply
    hasn't resolved yet."""
    value = value_or_stale(data)
    return not isinstance(value, NoValue)
