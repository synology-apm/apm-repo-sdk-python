"""``RemoteData``: the state of one piece of data fetched from the SDK —
``NotAsked``, ``Loading`` (optionally carrying the last known value, so a
reload doesn't blank the screen), ``Success`` or ``FailureInfo``.
"""

from __future__ import annotations

import dataclasses
import enum


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
    """A failed fetch; ``message`` is ``str(exc)``, so the model stays
    comparable."""

    message: str
    kind: FailureKind = FailureKind.OTHER


@dataclasses.dataclass(frozen=True, slots=True)
class NotAsked:
    """Nothing has fetched this value yet."""


@dataclasses.dataclass(frozen=True, slots=True)
class NoValue:
    """ "No value yet", distinct from a ``T`` that is itself ``None``. Test
    for it with ``isinstance``."""


@dataclasses.dataclass(frozen=True, slots=True)
class Loading[T]:
    """A fetch is in flight. ``previous`` is the last ``Success.value``
    this slot held, or a ``NoValue()`` on a first load."""

    previous: T | NoValue = NoValue()


@dataclasses.dataclass(frozen=True, slots=True)
class Success[T]:
    value: T


type RemoteData[T] = NotAsked | Loading[T] | Success[T] | FailureInfo


def loading_preserving[T](previous: RemoteData[T]) -> Loading[T]:
    """A fresh ``Loading``, carrying ``previous``'s value forward when it
    was a ``Success``."""
    if isinstance(previous, Success):
        return Loading(previous=previous.value)
    return Loading()


def is_pending_or_done[T](data: RemoteData[T]) -> bool:
    """Whether ``data`` must not be (re-)fetched: it is already ``Loading``
    or a ``Success``."""
    return isinstance(data, Loading | Success)


def value_or_stale[T](data: RemoteData[T]) -> T | NoValue:
    """The value to render: a ``Success``'s value or a reloading
    ``Loading``'s ``previous``; otherwise ``NoValue()``."""
    if isinstance(data, Success):
        return data.value
    if isinstance(data, Loading) and not isinstance(data.previous, NoValue):
        return data.previous
    return NoValue()


def has_ever_resolved[T](data: RemoteData[T]) -> bool:
    """Whether ``value_or_stale(data)`` has a value, telling resolved-empty
    from not-yet-resolved."""
    value = value_or_stale(data)
    return not isinstance(value, NoValue)
