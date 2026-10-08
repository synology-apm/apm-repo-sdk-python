"""``UnitMsg``: every event ``UnitScreen``'s store handles. Every fetch
result carries the ``epoch``/``request`` of the ``Cmd`` that started it, so
``update()`` can drop a stale one."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.browser.core.unit.model import DetailBody, UnitPurpose
from synology_apm_repo.sdk import Node, NodeRef, RestorableUnit


@dataclasses.dataclass(frozen=True, slots=True)
class RootRequested:
    """A fresh root/provider load. ``force_raw`` is the app's verbose flag
    at dispatch time."""

    invalidate: bool
    force_raw: bool


@dataclasses.dataclass(frozen=True, slots=True)
class RootLoaded:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    root: Node


@dataclasses.dataclass(frozen=True, slots=True)
class RootLoadFailed:
    epoch: Epoch
    request: RequestId
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class ChildrenRequested:
    """A request for ``node``'s first page of children; a no-op if they
    are loaded or loading."""

    node: Node


@dataclasses.dataclass(frozen=True, slots=True)
class ChildrenLoaded:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    children: tuple[Node, ...]
    exhausted: bool


@dataclasses.dataclass(frozen=True, slots=True)
class ChildrenLoadFailed:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class LoadMoreRequested:
    node: Node


@dataclasses.dataclass(frozen=True, slots=True)
class MoreChildrenLoaded:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    more: tuple[Node, ...]
    exhausted: bool


@dataclasses.dataclass(frozen=True, slots=True)
class MoreChildrenLoadFailed:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class GotoRequested:
    """A ``g`` target in this version; resolved once the root has loaded."""

    target: NodeRef


@dataclasses.dataclass(frozen=True, slots=True)
class GotoResolved:
    """``chain`` is ``(root, ..., target)``; ``children_by_step[i]`` is
    ``chain[i]``'s complete child list, for every step but the target."""

    epoch: Epoch
    request: RequestId
    chain: tuple[Node, ...]
    children_by_step: tuple[tuple[Node, ...], ...]


@dataclasses.dataclass(frozen=True, slots=True)
class GotoNotFound:
    epoch: Epoch
    request: RequestId


@dataclasses.dataclass(frozen=True, slots=True)
class GotoFailed:
    epoch: Epoch
    request: RequestId
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class UnitOpenRequested:
    """Open leaf ``node`` as a unit, for ``purpose``."""

    node: Node
    purpose: UnitPurpose


@dataclasses.dataclass(frozen=True, slots=True)
class UnitOpened:
    epoch: Epoch
    request: RequestId
    unit: RestorableUnit
    purpose: UnitPurpose


@dataclasses.dataclass(frozen=True, slots=True)
class UnitOpenFailed:
    epoch: Epoch
    request: RequestId
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class FilterOpened:
    ref: NodeRef


@dataclasses.dataclass(frozen=True, slots=True)
class FilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class FilterClosed:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class FolderSelected:
    """The user navigated to ``folder`` (in the folder tree or the file
    table): the file table shows its children. A flat category's are
    fetched here (it has no expand arrow); any other folder's come with its
    expansion (``ChildrenRequested``). A List-overview group, which has no
    file-table contents, leaves the selection as it was."""

    folder: Node


@dataclasses.dataclass(frozen=True, slots=True)
class DetailRequested:
    """The user selected ``node`` (a folder, a file row, a goto landing):
    the detail pane shows its header and, for a leaf or a SharePoint List
    group, fetches what goes under it."""

    node: Node


@dataclasses.dataclass(frozen=True, slots=True)
class DetailResolved:
    """The finished body of the detail fetch ``request`` started."""

    epoch: Epoch
    request: RequestId
    body: DetailBody


@dataclasses.dataclass(frozen=True, slots=True)
class VerboseSet:
    verbose: bool


UnitMsg = (
    RootRequested
    | RootLoaded
    | RootLoadFailed
    | ChildrenRequested
    | ChildrenLoaded
    | ChildrenLoadFailed
    | LoadMoreRequested
    | MoreChildrenLoaded
    | MoreChildrenLoadFailed
    | GotoRequested
    | GotoResolved
    | GotoNotFound
    | GotoFailed
    | UnitOpenRequested
    | UnitOpened
    | UnitOpenFailed
    | FilterOpened
    | FilterTextChanged
    | FilterClosed
    | FolderSelected
    | DetailRequested
    | DetailResolved
    | VerboseSet
)
