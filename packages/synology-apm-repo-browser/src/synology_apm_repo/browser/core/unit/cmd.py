"""``UnitCmd``: every effect ``update()`` can ask ``runtime/unit_effects.py``
to perform -- data, never a callable, so ``assert cmds == (LoadRoot(...),)``
is a one-line test with no Pilot involved."""

from __future__ import annotations

import dataclasses

from synology_apm_repo.browser.core.keys import Epoch, ProviderHandle, RequestId
from synology_apm_repo.browser.core.notify import Notify as Notify
from synology_apm_repo.sdk.units.base import Node


@dataclasses.dataclass(frozen=True)
class LoadRoot:
    epoch: Epoch
    request: RequestId
    invalidate: bool
    force_raw: bool


@dataclasses.dataclass(frozen=True)
class CloseProvider:
    """Releases a superseded/discarded provider -- hosted on the App,
    never the screen, since a screen-hosted close worker would be
    cancelled mid-close the instant the screen unmounts."""

    provider: ProviderHandle


@dataclasses.dataclass(frozen=True)
class LoadChildren:
    epoch: Epoch
    request: RequestId
    provider: ProviderHandle
    node: Node
    offset: int
    limit: int


UnitCmd = LoadRoot | CloseProvider | LoadChildren | Notify
