"""``UnitCmd``: every effect ``update()`` can ask ``runtime/unit_effects.py``
to perform."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.browser.core.unit.model import UnitPurpose
from synology_apm_repo.sdk import Node, NodeRef, RestorableUnit


@dataclasses.dataclass(frozen=True, slots=True)
class LoadRoot:
    epoch: Epoch
    request: RequestId
    invalidate: bool
    force_raw: bool


@dataclasses.dataclass(frozen=True, slots=True)
class CloseProvider:
    """Releases a superseded or discarded provider, on an App-hosted
    worker: a screen-hosted one would be cancelled mid-close when the
    screen unmounts."""

    provider: ProviderHandle


@dataclasses.dataclass(frozen=True, slots=True)
class LoadChildren:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    node: Node
    offset: int
    limit: int


@dataclasses.dataclass(frozen=True, slots=True)
class LoadPreview:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    node: Node
    read_limit: int


@dataclasses.dataclass(frozen=True, slots=True)
class LoadListOverview:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    node: Node
    item_cap: int
    read_limit: int
    max_concurrent: int


@dataclasses.dataclass(frozen=True, slots=True)
class ResolveGoto:
    """Finds ``target``'s chain from the root, with every step's complete
    child list but the target's own."""

    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    target: NodeRef


@dataclasses.dataclass(frozen=True, slots=True)
class OpenUnit:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    node: Node
    purpose: UnitPurpose


@dataclasses.dataclass(frozen=True, slots=True)
class ShowUnit:
    """Pushes the screen ``purpose`` names for ``unit``."""

    unit: RestorableUnit
    purpose: UnitPurpose


@dataclasses.dataclass(frozen=True, slots=True)
class CancelDetailFetch:
    """Stops the detail pane's in-flight fetch, for a selection that starts
    none of its own (one that does supersedes it by running exclusively)."""


UnitCmd = (
    LoadRoot
    | CloseProvider
    | LoadChildren
    | LoadPreview
    | LoadListOverview
    | ResolveGoto
    | OpenUnit
    | ShowUnit
    | CancelDetailFetch
    | Notify
)
