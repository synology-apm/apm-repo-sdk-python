"""Domain-identity keys a ``core.*`` model uses instead of ``id(TreeNode)``:
the two resource handles, ``NewType`` counters so mypy catches a mixed-up
id, and composite keys addressing one catalog or workload.

Every type here is hashable at runtime: none holds a plain ``dict`` field
(as ``Workload.spec``/``Node.details`` do), which mypy would accept as a key
anyway.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from typing import NewType, Protocol

from synology_apm_repo.sdk import CatalogId, WorkloadUid


@dataclasses.dataclass(frozen=True, slots=True)
class RepoHandle:
    """Opaque handle for a live, closable ``Repository``, minted by
    ``runtime.resources.ResourceTable.put_repo``; a frozen model holds this
    instead of the object. Its own type, so a tree payload holding one can
    be told apart from any other value."""

    id: int


@dataclasses.dataclass(frozen=True, slots=True)
class ProviderHandle:
    """Opaque handle for a live, closable ``UnitProvider``, minted by
    ``ResourceTable.put_provider``."""

    id: int


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
    """A frozen dataclass with a ``next_request: RequestId`` field."""

    @property
    def next_request(self) -> RequestId: ...


def next_request[ModelT: _HasNextRequest](model: ModelT) -> tuple[RequestId, ModelT]:
    """Allocates the next ``RequestId`` for ``model``'s ``next_request``
    counter, returning it alongside the model with that counter incremented."""
    request = model.next_request
    # mypy can't see that ModelT is also a dataclass.
    return request, dataclasses.replace(model, next_request=RequestId(request + 1))  # type: ignore[type-var]


class _HasFilterText(Protocol):
    """A ``/`` filter state: a frozen dataclass with a ``text: str`` field."""

    @property
    def text(self) -> str: ...


def filter_text_changed[M, FilterStateT: _HasFilterText](
    model: M,
    get_state: Callable[[M], FilterStateT | None],
    apply_state: Callable[[M, FilterStateT], M],
    text: str,
) -> M:
    """Every domain's ``*FilterTextChanged`` handling: ``model`` itself if
    no filter is open, else that state's ``text`` replaced."""
    state = get_state(model)
    if state is None:
        return model
    return apply_state(model, dataclasses.replace(state, text=text))  # type: ignore[type-var]


def filter_closed[M](model: M, get_state: Callable[[M], object | None], clear_state: Callable[[M], M]) -> M:
    """Every domain's ``*FilterClosed`` handling: ``model`` itself if no
    filter is open, else cleared."""
    if get_state(model) is None:
        return model
    return clear_state(model)


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogKey:
    """Identifies one catalog across every repository a scan discovered:
    ``CatalogId`` is unique only within one repository."""

    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadKey:
    """Identifies one workload within one catalog."""

    catalog: CatalogKey
    workload_uid: WorkloadUid
