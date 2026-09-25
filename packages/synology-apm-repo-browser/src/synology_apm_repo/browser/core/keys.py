"""Domain-identity keys a ``core.*`` model uses instead of ``id(TreeNode)``
— ``NewType``s so mypy catches a mixed-up handle (matching the SDK's own
``identifiers.py`` convention), plus a few small composite keys that
uniquely address one catalog/workload/version regardless of which
repository or filter state is currently on screen.

Every type declared here is safe to use as a ``dict``/``set`` key or a
``RemoteData``/``Slot`` type parameter — none holds a field that would
make it unhashable at runtime the way ``Workload.spec``/``Node.attrs``
(both plain ``dict``s) make those SDK types unhashable despite having a
generated ``__hash__``: ``hash(a_workload)`` raises ``TypeError`` at
runtime even though ``mypy`` accepts ``dict[Workload, ...]`` without
complaint.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from typing import NewType, Protocol, TypeVar

from synology_apm_repo.sdk.identifiers import CatalogId, VersionUid, WorkloadUid

RepoHandle = NewType("RepoHandle", int)
"""Opaque handle for a live, closable ``Repository`` — minted by
``runtime.resources.ResourceTable.put_repo``. Never the ``Repository``
object itself: it owns an ``aiosqlite`` connection with a non-daemon
background thread, which a frozen model can't hold without leaking
threads across repeated version visits once unclosed."""

ProviderHandle = NewType("ProviderHandle", int)
"""Opaque handle for a live, closable ``UnitProvider`` — same reasoning
as ``RepoHandle``, minted by ``ResourceTable.put_provider``."""

RequestId = NewType("RequestId", int)
"""Per-``Slot`` sequence number a model allocates on every dispatch into
that slot. A result whose ``request`` doesn't match the model's current
``inflight[slot]`` is stale and must be dropped — see ``Slot`` below."""

Epoch = NewType("Epoch", int)
"""Per-domain generation counter, coarser than ``RequestId``: bumped by
a rescan, a provider reset, or a verbose-mode reload — anything that
invalidates *every* slot at once rather than just one. A result whose
``epoch`` doesn't match the model's current ``epoch`` is stale
regardless of its own ``RequestId``."""

JobId = NewType("JobId", int)
"""Identifies one backgroundable job (an export) in ``AppModel.jobs`` —
minted by ``core/app/update.py``'s own ``StartExport`` handler, one
higher than the previous mint every time, never reused."""


@dataclasses.dataclass(frozen=True, slots=True)
class Slot:
    """One entry in a model's ``inflight: Mapping[Slot, RequestId]`` —
    identifies *which* fetch a result belongs to. ``kind`` is a short,
    domain-chosen tag (e.g. ``"workloads"``, ``"versions"``, ``"provider"``);
    ``key`` narrows it to one instance (a ``CatalogKey``, a
    ``WorkloadKey``, a ``RepoHandle``, ...) or stays ``None`` for a slot
    that only ever has one fetch in flight at a time regardless of
    domain identity (e.g. "the provider currently loading" in
    ``UnitScreen``, which has exactly one provider open at once).
    ``kind``+``key`` together must be unique among everything one
    model's own ``inflight`` mapping can hold at once — enforcing that
    is each domain's own responsibility, not this dataclass's."""

    kind: str
    key: object = None


def is_stale(
    model_epoch: Epoch, inflight: Mapping[Slot, RequestId], slot: Slot, epoch: Epoch, request: RequestId
) -> bool:
    """Whether a fetch-result ``Msg``'s own ``epoch``/``request`` no
    longer matches the model's current ones -- the exact two-part check
    (a stale ``epoch`` means a rescan/reset/reload invalidated every slot
    at once; a stale ``request`` means a newer fetch into that same
    ``Slot`` has since started) every domain's own ``update()`` repeats
    verbatim at the top of each fetch-result case (``core/unit/update.py``,
    ``core/browse/update.py``). Shared here so a future case that needs
    this same check can't hand-copy it slightly wrong (e.g. checking only
    ``epoch``, or the wrong ``slot``) without it ever being tested as its
    own thing."""
    return model_epoch != epoch or inflight.get(slot) != request


class _HasNextRequest(Protocol):
    """Structural minimum ``next_request`` needs from a domain model --
    a dataclass with its own ``next_request: RequestId`` field. Declared
    as a read-only ``@property``, not a plain attribute: a Protocol's
    plain attribute member requires read *and* write compatibility, which
    a ``frozen=True`` dataclass field (no setter) can never satisfy."""

    @property
    def next_request(self) -> RequestId: ...


_ModelT = TypeVar("_ModelT", bound=_HasNextRequest)


def next_request(model: _ModelT) -> tuple[RequestId, _ModelT]:
    """Allocates the next ``RequestId`` for ``model``'s own
    ``next_request`` counter, returning it alongside the model with that
    counter incremented -- the one shared shape ``core/unit/update.py``
    and ``core/browse/update.py`` each dispatch through on every fetch
    they start, so a future domain's own ``update()`` reaches for this
    instead of hand-copying the increment."""
    request = model.next_request
    # mypy has no way to express "T is both this Protocol and some real
    # dataclass" strongly enough for dataclasses.replace's own generic
    # signature (bound to _typeshed.DataclassInstance) to accept it
    # directly -- every real caller (UnitModel, BrowseModel) is a frozen
    # dataclass, so this is a type-checker gap, not a runtime one.
    return request, dataclasses.replace(model, next_request=RequestId(request + 1))  # type: ignore[type-var]


class _HasFilterText(Protocol):
    """Structural minimum a "narrowed by ``/``" filter state needs -- a
    dataclass with its own ``text: str`` field, same read-only-``@property``
    reasoning as ``_HasNextRequest`` above."""

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
    """The ``*FilterTextChanged`` case body every domain's own filter
    state (``FilterState``, ``TreeFilterState``, ``VersionFilterState``)
    repeats identically: a no-op (same ``model``, by identity, not just
    equality -- callers rely on this to skip a redundant render) if no
    filter is currently open, otherwise that state's own ``text``
    replaced. ``get_state``/``apply_state`` are the one- or two-line
    field accessors each call site already has to write regardless;
    this is the guard+replace shape around them that must not drift
    between domains."""
    state = get_state(model)
    if state is None:
        return model
    # Same type-checker gap as next_request() above -- dataclasses.replace
    # needs _FilterStateT to also prove itself a real dataclass.
    return apply_state(model, dataclasses.replace(state, text=text))  # type: ignore[type-var]


def filter_closed(model: _M, get_state: Callable[[_M], object | None], clear_state: Callable[[_M], _M]) -> _M:
    """The ``*FilterClosed`` case body's own identical guard+clear shape
    -- a no-op (by identity) if no filter is currently open, otherwise
    ``clear_state``'s own replacement (always ``dataclasses.replace(model,
    <field>=None)`` at every real call site)."""
    if get_state(model) is None:
        return model
    return clear_state(model)


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogKey:
    """Identifies one catalog across every repository a scan discovered.
    ``repo`` is part of the key deliberately: ``CatalogId`` degrades to
    ``str(connection_config_id)`` for a vault repository (a per-repository
    local autoincrement), which two independently-opened repositories in
    the same scan can
    legitimately share. Matching on ``catalog_id`` alone would collide
    across such repositories; making the repository part of the key turns
    disambiguating between them into a type-level invariant."""

    repo: RepoHandle
    catalog_id: CatalogId


@dataclasses.dataclass(frozen=True, slots=True)
class WorkloadKey:
    """Identifies one workload within one catalog. ``workload_uid``, not
    a bare ``Workload`` object — a ``Workload``'s own ``spec`` field is a
    plain ``dict``, so ``Workload`` is unhashable at runtime despite
    having a generated ``__hash__``."""

    catalog: CatalogKey
    workload_uid: WorkloadUid


@dataclasses.dataclass(frozen=True, slots=True)
class VersionKey:
    """Identifies one version within one workload. ``version_uid``, not a
    bare ``Version`` — unlike ``Workload``, a plain ``Version`` actually
    is hashable (its own ``meta`` field is a frozen dataclass of tuples,
    not a dict), but using the id keeps this key the same shape as
    ``WorkloadKey`` above and independent of ``Version``'s own field set
    ever changing."""

    workload: WorkloadKey
    version_uid: VersionUid
