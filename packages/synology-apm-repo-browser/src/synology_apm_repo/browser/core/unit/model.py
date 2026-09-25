"""``UnitModel``: ``UnitScreen``'s own tree navigation state -- the
provider's own lifecycle/epoch, and every node's own loaded (paginated)
children, keyed by ``NodeRef``. Everything else ``UnitScreen`` touches
(the detail pane's own preview/list-overview content, cross-version
goto resolution, export, hex-preview) stays outside this model -- none
of it has the id()/race hazards this refactor targets, and nothing about
it is held onto across a call the way this model's fields are -- the same
reasoning keeps ``core/connect/validate.py``'s own validation functions
as plain one-shot translations rather than Model state.

Two independent staleness tokens, per the refactor's own race-fix
convention: ``epoch`` is bumped by anything that invalidates *every*
in-flight fetch at once (a rescan, a verbose-mode reload); ``inflight``
tracks one ``RequestId`` per ``Slot`` for everything narrower (one
node's own children fetch superseding an earlier one for that same
node). A result whose own epoch or request doesn't match the model's
current one is dropped, unconditionally, before it's ever applied --
never trust worker cancellation alone for this: a worker already past
its last ``await`` when cancelled still runs to completion and still
tries to publish."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId, Slot
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef

#: The provider fetch's own slot -- exactly one provider is ever loading
#: at a time for a given ``UnitScreen``, so ``key`` stays ``None`` (a
#: ``Slot`` only needs a domain key when more than one instance of that
#: kind can be in flight at once).
PROVIDER_SLOT = Slot(kind="provider")


def children_slot(ref: NodeRef) -> Slot:
    """One node's own children-fetch slot -- a fresh dispatch (an
    ordinary expand, or ``+``/load-more) for the same ``ref`` always
    supersedes whatever that ref's own slot was previously tracking."""
    return Slot(kind="children", key=ref)


def has_pending_children(model: UnitModel, ref: NodeRef) -> bool:
    """Whether ``ref``'s own children-fetch is currently running. Named
    the same way ``core/remote_data.py``'s ``is_pending_or_done`` is, for the same
    reason: every call site that needs this answer shares one predicate
    rather than re-writing ``children_slot(ref) in model.pending`` by
    hand each time."""
    return children_slot(ref) in model.pending


@dataclasses.dataclass(frozen=True)
class LoadedLevel:
    """Everything loaded so far for one expanded node. ``children`` and
    ``exhausted`` genuinely diverge, since a level loads one page at a
    time: a node can be loaded (this exists at all) without being
    loaded *in full* (``exhausted`` is ``False``). ``children`` is the
    *raw* list ``provider.children()`` returned -- disk-fs-sibling
    nesting and the SharePoint-List-overview ``allow_expand`` decision
    are both purely presentational and live in ``select.py`` instead,
    so a goto-ref chain step's own exhaustive sibling list
    (``ChainStepResolved``) renders identically to an ordinary page
    (``ChildrenLoaded``) without this model needing to know the
    difference."""

    children: tuple[Node, ...]
    exhausted: bool

    @property
    def next_offset(self) -> int:
        """The offset a load-more's own next page starts at -- always
        ``len(children)``, so nothing else needs to keep the two in
        sync by hand."""
        return len(self.children)


@dataclasses.dataclass(frozen=True, slots=True)
class FilterState:
    """The tree level currently narrowed by ``/`` -- ``ref`` is the
    parent whose children are being filtered, matching
    ``UnitScreen``'s own single-active-filter-at-a-time UI (only one
    ``#filter-input`` exists)."""

    ref: NodeRef
    text: str = ""


@dataclasses.dataclass(frozen=True)
class UnitModel:
    epoch: Epoch = Epoch(0)
    inflight: Mapping[Slot, RequestId] = dataclasses.field(default_factory=dict)
    #: One node's own children-fetch slot is in here while a fetch for it
    #: is genuinely still running -- added on dispatch
    #: (``ChildrenRequested``/``LoadMoreRequested``), removed once its own
    #: *current* result lands (a *stale* one leaves it alone: it's either
    #: already reset wholesale by whatever invalidated it, or still
    #: correctly tracking a genuinely newer request for the same slot,
    #: which will clear it itself when that one resolves). Answers "is
    #: this slot's fetch still running", which neither ``loaded`` (Success-only) nor
    #: ``inflight`` (kept for ``is_stale``'s own arbitration, not cleared
    #: on an ordinary result) can. Every dispatcher of a children fetch
    #: checks this first, so a still-pending slot is never re-triggered.
    pending: frozenset[Slot] = dataclasses.field(default_factory=frozenset)
    next_request: RequestId = RequestId(1)
    provider: ProviderHandle | None = None
    root: Node | None = None
    #: The root/provider fetch's own failure message, when current --
    #: rendered into the detail pane (see ``UnitScreen._on_root_error``),
    #: not a toast: unlike a load-more failure, there's nothing else on
    #: screen yet for a toast to leave the user looking at, so the
    #: message needs to persist rather than fade.
    root_error: str | None = None
    loaded: Mapping[NodeRef, LoadedLevel] = dataclasses.field(default_factory=dict)
    #: Every child ``Node`` ever loaded into ``loaded``, indexed by its own
    #: ``ref`` -- kept incrementally in lockstep with ``loaded`` (same
    #: reset-on-rescan, merge-on-load lifecycle) so ``select.py``'s
    #: ``find_node_in_model`` -- read on every ``Store`` dispatch via two
    #: ``column_headers_for``/``file_table_rows`` subscriptions, not just
    #: on a folder switch -- is an ``O(1)`` lookup instead of a linear scan
    #: over every loaded level's own children.
    node_index: Mapping[NodeRef, Node] = dataclasses.field(default_factory=dict)
    #: A node whose *initial* children fetch failed -- rendered as a
    #: single synthetic "error: ..." leaf under it (see ``select.py``'s
    #: own ``error_leaf_ref``) rather than a toast, so the failure stays
    #: visible in the tree itself, not just for as long as a
    #: notification lingers. Never populated for a load-*more* failure
    #: (``MoreChildrenLoadFailed`` only ever notifies) -- that node
    #: already has real children on screen, so there's nothing to
    #: replace with an error leaf.
    errors: Mapping[NodeRef, str] = dataclasses.field(default_factory=dict)
    filter: FilterState | None = None
    #: Which folder's children the file table currently shows -- ``None``
    #: only before the root has ever loaded (``RootLoaded`` always sets
    #: this to the new root's own ``ref``, even when that root is itself a
    #: leaf -- harmless, since a leaf ref is never used as a
    #: ``LoadChildren`` target either way).
    selected: NodeRef | None = None
