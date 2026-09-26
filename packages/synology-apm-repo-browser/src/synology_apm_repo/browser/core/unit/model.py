"""``UnitModel``: ``UnitScreen``'s tree navigation state -- the provider's
lifecycle/epoch, and every node's loaded (paginated) children, keyed by
``NodeRef``. Everything else ``UnitScreen`` touches (detail pane, goto
resolution, export, hex-preview) stays outside this model.

Two staleness tokens: ``epoch`` invalidates every in-flight fetch at once
(a rescan, a verbose-mode reload); ``inflight`` tracks one ``RequestId``
per ``Slot`` for narrower fetches. A result whose epoch or request
doesn't match is dropped unconditionally -- worker cancellation alone
isn't trusted for this, since a worker already past its last ``await``
still runs to completion and tries to publish."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId, Slot
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

#: The provider fetch's own slot -- exactly one provider ever loads at a
#: time for a given ``UnitScreen``, so ``key`` stays ``None``.
PROVIDER_SLOT = Slot(kind="provider")


def children_slot(ref: NodeRef) -> Slot:
    """One node's children-fetch slot -- a fresh dispatch for the same
    ``ref`` always supersedes whatever it was previously tracking."""
    return Slot(kind="children", key=ref)


def has_pending_children(model: UnitModel, ref: NodeRef) -> bool:
    """Whether ``ref``'s children-fetch is currently running."""
    return children_slot(ref) in model.pending


@dataclasses.dataclass(frozen=True)
class LoadedLevel:
    """Everything loaded so far for one expanded node. ``children`` is the
    raw list ``provider.children()`` returned -- disk-fs-sibling nesting
    and the SharePoint-List-overview ``allow_expand`` decision are purely
    presentational and live in ``select.py`` instead. ``exhausted`` is
    ``False`` while more pages remain."""

    children: tuple[Node, ...]
    exhausted: bool

    @property
    def next_offset(self) -> int:
        """The offset a load-more's next page starts at."""
        return len(self.children)


@dataclasses.dataclass(frozen=True, slots=True)
class FilterState:
    """The tree level narrowed by ``/`` -- ``ref`` is the parent whose
    children are being filtered."""

    ref: NodeRef
    text: str = ""


@dataclasses.dataclass(frozen=True)
class UnitModel:
    epoch: Epoch = Epoch(0)
    inflight: Mapping[Slot, RequestId] = dataclasses.field(default_factory=dict)
    #: A node's children-fetch slot, present while genuinely still running
    #: -- added on dispatch, removed once its current result lands (a
    #: stale one leaves it alone). Every dispatcher checks this first, so
    #: a still-pending slot is never re-triggered.
    pending: frozenset[Slot] = dataclasses.field(default_factory=frozenset)
    next_request: RequestId = RequestId(1)
    provider: ProviderHandle | None = None
    root: Node | None = None
    #: The root/provider fetch's failure message, when current -- rendered
    #: into the detail pane, not a toast, since it must persist rather
    #: than fade with nothing else yet on screen.
    root_error: str | None = None
    loaded: Mapping[NodeRef, LoadedLevel] = dataclasses.field(default_factory=dict)
    #: Every child ``Node`` ever loaded into ``loaded``, indexed by ``ref``
    #: -- kept in lockstep with ``loaded`` so ``select.py``'s
    #: ``find_node_in_model`` is an O(1) lookup instead of a linear scan.
    node_index: Mapping[NodeRef, Node] = dataclasses.field(default_factory=dict)
    #: A node whose initial children fetch failed -- rendered as a
    #: synthetic error leaf under it, so the failure stays visible in the
    #: tree rather than fading with a notification. Never populated for a
    #: load-more failure (that node already has real children on screen).
    errors: Mapping[NodeRef, str] = dataclasses.field(default_factory=dict)
    filter: FilterState | None = None
    #: Which folder's children the file table shows -- ``None`` only
    #: before the root has ever loaded.
    selected: NodeRef | None = None
