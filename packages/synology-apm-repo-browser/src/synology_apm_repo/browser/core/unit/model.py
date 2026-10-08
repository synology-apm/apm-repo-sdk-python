"""``UnitModel``: ``UnitScreen``'s state -- the provider, every expanded
node's loaded (paginated) children keyed by ``NodeRef``, the selection, the
filter, what the detail pane shows (one ``DETAIL_SLOT`` request at a
time), a ``g`` target being resolved and where it landed, and a leaf being
opened for an export or a hex preview.

``epoch`` is bumped by every root load (a refresh, a verbose-mode reload),
invalidating every in-flight fetch; ``inflight`` tracks one ``RequestId``
per ``Slot``."""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId, Slot
from synology_apm_repo.sdk import Node, NodeRef

#: The provider fetch's slot; one provider loads at a time.
PROVIDER_SLOT = Slot(kind="provider")


#: The detail pane's slot; one selection's preview/overview at a time.
DETAIL_SLOT = Slot(kind="detail")

#: Worker group of the detail pane's fetches: at most one runs at a time.
DETAIL_GROUP = "unit-detail"

#: The ``g`` target's resolution; a newer goto supersedes the one in flight.
GOTO_SLOT = Slot(kind="goto")

#: Worker group of goto resolutions: at most one runs at a time.
GOTO_GROUP = "unit-goto"

#: Opening a leaf as a ``RestorableUnit``; a newer request supersedes it.
UNIT_OPEN_SLOT = Slot(kind="unit-open")

#: Worker group of unit opens: at most one runs at a time.
UNIT_OPEN_GROUP = "unit-open"

#: Caps a preview read; ``content_preview`` notes any truncation.
PREVIEW_READ_LIMIT = 256 * 1024

#: Items a SharePoint List's spreadsheet-style overview fetches. Smaller
#: than ``update.CHILDREN_PAGE_SIZE`` because each item's content is read.
#: The rendered table says when the cap was hit.
LIST_OVERVIEW_ITEM_CAP = 50

#: Concurrent item content fetches for a List overview.
LIST_OVERVIEW_MAX_CONCURRENT = 8


def children_slot(ref: NodeRef) -> Slot:
    """One node's children-fetch slot; a fresh dispatch for ``ref``
    supersedes the previous one."""
    return Slot(kind="children", key=ref)


def has_pending_children(model: UnitModel, ref: NodeRef) -> bool:
    """Whether ``ref``'s children-fetch is currently running."""
    return children_slot(ref) in model.pending


class UnitPurpose(enum.Enum):
    """What an opened unit is for: the screen each one pushes."""

    EXPORT = "export"
    HEX_PREVIEW = "hex_preview"


@dataclasses.dataclass(frozen=True, slots=True)
class GotoState:
    """A ``g`` target in this version, waiting for the root to load or
    being resolved. ``root_expand_skipped`` when it (or a goto it
    superseded) was pending as the root loaded, so the view left the root
    collapsed for the landing to expand."""

    target: NodeRef
    root_expand_skipped: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class Landing:
    """Where the view moves the cursor once a goto ends: ``folder``'s tree
    node, revealed (and itself expanded when ``expand``: a leaf target's
    parent, or a root kept collapsed for a goto that ended unlanded), then
    a leaf target's file-table row. ``request`` keeps two landings on the
    same target distinct values."""

    request: RequestId
    folder: NodeRef
    leaf: NodeRef | None = None
    expand: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class DetailIdle:
    """Nothing further to show under the header."""


@dataclasses.dataclass(frozen=True, slots=True)
class DetailLoading:
    """A preview/overview fetch for this node is in flight."""


@dataclasses.dataclass(frozen=True, slots=True)
class DetailPreview:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class DetailNote:
    """The content is deliberately unreadable (a cloud-sync placeholder, an
    EFS-encrypted file) -- shown as a hint, not a failure."""

    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class DetailError:
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class DetailOverview:
    """A SharePoint List's items as display rows; ``truncated`` when the
    List may hold more than were fetched."""

    rows: tuple[Mapping[str, object], ...]
    truncated: bool


DetailBody = DetailIdle | DetailLoading | DetailPreview | DetailNote | DetailError | DetailOverview


@dataclasses.dataclass(frozen=True, slots=True)
class DetailState:
    """The node the detail pane is showing and what is under its header."""

    node: Node
    body: DetailBody


def detail_loading(model: UnitModel, request: RequestId) -> bool:
    """Whether ``request`` is still the detail pane's awaited fetch."""
    detail = model.detail
    return detail is not None and isinstance(detail.body, DetailLoading) and model.inflight.get(DETAIL_SLOT) == request


@dataclasses.dataclass(frozen=True, slots=True)
class LoadedLevel:
    """Everything loaded so far for one expanded node: ``children`` as
    ``provider.children()`` returned them (``select.py`` derives the
    display), ``exhausted`` once no page remains."""

    children: tuple[Node, ...]
    exhausted: bool

    @property
    def next_offset(self) -> int:
        """The offset a load-more's next page starts at."""
        return len(self.children)


@dataclasses.dataclass(frozen=True, slots=True)
class FilterState:
    """The open ``/`` filter; ``ref`` is the parent whose children it
    narrows."""

    ref: NodeRef
    text: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class UnitModel:
    epoch: Epoch = Epoch(0)
    inflight: Mapping[Slot, RequestId] = dataclasses.field(default_factory=dict)
    #: Children-fetch slots still running: added on dispatch, removed when
    #: the current result lands. A pending slot is never re-dispatched.
    pending: frozenset[Slot] = dataclasses.field(default_factory=frozenset)
    next_request: RequestId = RequestId(1)
    provider: ProviderHandle | None = None
    root: Node | None = None
    #: The root/provider fetch's failure, shown by the detail pane
    #: (``detail_view``) so it persists with nothing else on screen.
    root_error: str | None = None
    loaded: Mapping[NodeRef, LoadedLevel] = dataclasses.field(default_factory=dict)
    #: Every child ``Node`` in ``loaded``, by ``ref``, kept in lockstep for
    #: ``select.py``'s ``find_node_in_model``.
    node_index: Mapping[NodeRef, Node] = dataclasses.field(default_factory=dict)
    #: Nodes whose first children fetch failed, each rendered with an error
    #: leaf under it. A load-more failure is a toast instead.
    errors: Mapping[NodeRef, str] = dataclasses.field(default_factory=dict)
    filter: FilterState | None = None
    #: The folder whose children the file table shows; ``None`` until the
    #: root loads.
    selected: NodeRef | None = None
    #: What the detail pane shows; ``None`` with no selection, including
    #: after a reset.
    detail: DetailState | None = None
    #: The app's verbose flag, set by the screen through ``VerboseSet``.
    verbose: bool = False
    #: The ``g`` target not yet landed; ``None`` with none in flight.
    goto: GotoState | None = None
    #: The last goto's landing, for the view to apply once.
    landing: Landing | None = None
