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
    """``message`` is already ``str(exc)`` — nothing downstream needs the
    original exception object, and keeping it out of the model keeps a
    model comparable and a failure assertable in a plain unit test."""

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
    never ``is``/``==`` against one particular instance."""


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
    """Whether a fresh ``Loading()`` should carry the slot's last-known
    value forward -- only when ``previous`` was a real ``Success``."""
    if isinstance(previous, Success):
        return Loading(previous=previous.value)
    return Loading()


def is_pending_or_done(data: RemoteData[T]) -> bool:
    """Whether ``data`` must not be (re-)fetched: a ``Loading`` fetch is
    already in flight, or a ``Success`` already has the answer.
    Re-dispatching either duplicates a worker and its loading indicator
    for no benefit."""
    return isinstance(data, Loading | Success)


def value_or_stale(data: RemoteData[T]) -> T | NoValue:
    """The value a selector should render: a real ``Success``'s value, or
    a refreshing ``Loading``'s carried-forward ``previous`` -- both
    rendered identically. ``NoValue()`` for ``NotAsked``, a first
    ``Loading`` with nothing stale yet, or ``FailureInfo`` (rendered
    separately via its own ``message``)."""
    if isinstance(data, Success):
        return data.value
    if isinstance(data, Loading) and not isinstance(data.previous, NoValue):
        return data.previous
    return NoValue()


def has_ever_resolved(data: RemoteData[T]) -> bool:
    """Whether ``value_or_stale(data)`` would return a real value rather
    than a ``NoValue()`` -- for a caller rendering an explicit empty-state
    placeholder, which needs to tell resolved-empty apart from
    not-yet-resolved."""
    value = value_or_stale(data)
    return not isinstance(value, NoValue)
