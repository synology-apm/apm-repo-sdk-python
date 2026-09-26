"""``UnitMsg``: every event ``UnitScreen``'s own store can react to.
Every fetch-result variant carries the ``epoch``/``request`` its own
dispatching ``Cmd`` was minted with (captured at dispatch time, in
``update.py`` -- never re-read from live model state inside an effect),
so ``update()`` can tell a stale result apart from the one it's still
waiting on before ever applying it."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.sdk.units.base import Node
from synology_apm_repo.sdk.units.node_ref import NodeRef


@dataclasses.dataclass(frozen=True)
class RootRequested:
    """A fresh root/provider load. ``force_raw`` is captured by the
    screen from ``app_state.verbose`` at dispatch time, since ``update()``
    is pure and can't read a Textual reactive itself."""

    invalidate: bool
    force_raw: bool


@dataclasses.dataclass(frozen=True)
class RootLoaded:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    root: Node


@dataclasses.dataclass(frozen=True)
class RootLoadFailed:
    epoch: Epoch
    request: RequestId
    message: str


@dataclasses.dataclass(frozen=True)
class ChildrenRequested:
    """An ordinary expand of a node not yet in ``model.loaded`` --
    ``node`` is captured from the ``TreeNode``'s own ``.data`` at dispatch
    time, not re-derived from the model."""

    node: Node


@dataclasses.dataclass(frozen=True)
class ChildrenLoaded:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    children: tuple[Node, ...]
    exhausted: bool


@dataclasses.dataclass(frozen=True)
class ChildrenLoadFailed:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    message: str


@dataclasses.dataclass(frozen=True)
class LoadMoreRequested:
    node: Node


@dataclasses.dataclass(frozen=True)
class MoreChildrenLoaded:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    more: tuple[Node, ...]
    exhausted: bool


@dataclasses.dataclass(frozen=True)
class MoreChildrenLoadFailed:
    epoch: Epoch
    request: RequestId
    ref: NodeRef
    message: str


@dataclasses.dataclass(frozen=True)
class ChainStepResolved:
    """``GotoChainWalker``'s dispatch for one chain step's exhaustive
    sibling list -- unconditionally replaces whatever ``ref`` was
    previously loaded, whether nothing yet or only a partial page."""

    ref: NodeRef
    children: tuple[Node, ...]


@dataclasses.dataclass(frozen=True)
class FilterOpened:
    ref: NodeRef


@dataclasses.dataclass(frozen=True)
class FilterTextChanged:
    text: str


@dataclasses.dataclass(frozen=True)
class FilterClosed:
    pass


@dataclasses.dataclass(frozen=True)
class FolderSelected:
    """Dispatched whenever the user navigates to a different folder --
    either the folder tree's own selection, or activating a subfolder row
    (both converge here). Pure: just updates ``model.selected``, no
    ``Cmd`` -- fetching unloaded children is ``ChildrenRequested``,
    dispatched separately."""

    ref: NodeRef


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
    | ChainStepResolved
    | FilterOpened
    | FilterTextChanged
    | FilterClosed
    | FolderSelected
)
