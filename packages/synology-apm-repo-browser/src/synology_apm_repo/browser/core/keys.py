"""Domain-identity keys a ``core.*`` model uses instead of ``id(TreeNode)``
— ``NewType``s so mypy catches a mixed-up handle, plus small composite
keys that uniquely address one catalog/workload/version.

Every type here is safe as a ``dict``/``set`` key: none holds a field
(like ``Workload.spec``/``Node.attrs``, both plain ``dict``s) that would
make it unhashable at runtime despite mypy accepting it as one.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from typing import NewType, Protocol, TypeVar

from synology_apm_repo.sdk.identifiers import CatalogId, VersionUid, WorkloadUid

RepoHandle = NewType("RepoHandle", int)
"""Opaque handle for a live, closable ``Repository``, minted by
``runtime.resources.ResourceTable.put_repo`` — never the ``Repository``
object itself, since a frozen model can't hold its live connection."""

ProviderHandle = NewType("ProviderHandle", int)
"""Opaque handle for a live, closable ``UnitProvider`` — same reasoning
as ``RepoHandle``, minted by ``ResourceTable.put_provider``."""

RequestId = NewType("RequestId", int)
"""Per-``Slot`` sequence number a model allocates on every dispatch. A
result whose ``request`` doesn't match ``inflight[slot]`` is stale."""

Epoch = NewType("Epoch", int)
"""Per-domain generation counter, coarser than ``RequestId``: bumped by
anything that invalidates every slot at once (a rescan, provider reset,
verbose-mode reload). A stale ``epoch`` is dropped regardless of ``RequestId``."""

JobId = NewType("JobId", int)
"""Identifies one backgroundable job in ``AppModel.jobs``, minted
one-higher each time by ``core/app/update.py``'s ``StartExport`` handler,
never reused."""


@dataclasses.dataclass(frozen=True, slots=True)
class Slot:
    """One entry in a model's ``inflight: Mapping[Slot, RequestId]``,
    identifying which fetch a result belongs to. ``kind`` is a
    domain-chosen tag; ``key`` narrows it to one instance, or stays
    ``None`` for a slot with only one fetch in flight at a time regardless
    of domain identity. ``kind``+``key`` together must be unique within
    one model's ``inflight`` mapping."""

    kind: str
    key: object = None


def is_stale(
    model_epoch: Epoch, inflight: Mapping[Slot, RequestId], slot: Slot, epoch: Epoch, request: RequestId
) -> bool:
    """Whether a fetch-result ``Msg``'s ``epoch``/``request`` no longer
    matches the model's current ones — a stale ``epoch`` means a
    rescan/reset/reload invalidated every slot; a stale ``request`` means
    a newer fetch into the same ``Slot`` has since started."""
    return model_epoch != epoch or inflight.get(slot) != request


class _HasNextRequest(Protocol):
    """Structural minimum ``next_request`` needs: a dataclass with its own
    ``next_request: RequestId`` field, declared read-only since a
    ``frozen=True`` dataclass field has no setter."""

    @property
    def next_request(self) -> RequestId: ...


_ModelT = TypeVar("_ModelT", bound=_HasNextRequest)


def next_request(model: _ModelT) -> tuple[RequestId, _ModelT]:
    """Allocates the next ``RequestId`` for ``model``'s ``next_request``
    counter, returning it alongside the model with that counter incremented."""
    request = model.next_request
    # mypy can't express "T is both this Protocol and some real dataclass"
    # strongly enough for dataclasses.replace to accept it directly -- a
    # type-checker gap, not a runtime one (every real caller is frozen).
    return request, dataclasses.replace(model, next_request=RequestId(request + 1))  # type: ignore[type-var]


class _HasFilterText(Protocol):
    """Structural minimum a "narrowed by ``/``" filter state needs: a
    dataclass with its own ``text: str`` field."""

    @property
    def text(self) -> str: ...


_FilterStateT = TypeVar("_FilterStateT", bound=_HasFilterText)
_M = TypeVar("_M")


def filter_text_changed(
    model: _M,
    get_state: Callable[[_M], _FilterStateT | None],
    apply_state: Callable[[_M, _FilterStateT], _M],
    text: str,
) -> _M:
    """The ``*FilterTextChanged`` case body every domain's filter state
    repeats identically: a no-op (by identity) if no filter is open,
    otherwise that state's ``text`` replaced."""
    state = get_state(model)
    if state is None:
        return model
    return apply_state(model, dataclasses.replace(state, text=text))  # type: ignore[type-var]


def filter_closed(model: _M, get_state: Callable[[_M], object | None], clear_state: Callable[[_M], _M]) -> _M:
    """The ``*FilterClosed`` case body's identical guard+clear shape: a
    no-op (by identity) if no filter is open, otherwise cleared."""
    if get_state(model) is None:
        return model
    return clear_state(model)


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogKey:
    """Identifies one catalog across every repository a scan discovered.
    ``repo`` is part of the key deliberately: ``CatalogId`` degrades to a
    per-repository local autoincrement for a vault, which two
    independently-opened repositories can legitimately share."""

    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadKey:
    """Identifies one workload within one catalog. ``workload_uid``, not a
    bare ``Workload`` — its ``spec`` field is a plain ``dict``, making it
    unhashable at runtime despite mypy accepting it as a key."""

    catalog: CatalogKey
    workload_uid: WorkloadUid


@dataclasses.dataclass(frozen=True, slots=True)
class VersionKey:
    """Identifies one version within one workload, by ``version_uid``
    rather than a bare ``Version``, keeping this key's shape consistent
    with ``WorkloadKey`` above."""

    workload: WorkloadKey
    version_uid: VersionUid
