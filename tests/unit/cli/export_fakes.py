"""Fakes the ``export`` command's tests share."""

from __future__ import annotations

from typing import Any, cast

from support.fakes import faithful_to
from synology_apm_repo.sdk.api import NodeFrame
from synology_apm_repo.sdk.units.base import RestorableUnit, UnitProvider


@faithful_to(UnitProvider)
class ItemProvider:
    """Hands back the one item a fake ``resolve()`` frame names."""

    def __init__(self, item: RestorableUnit) -> None:
        self._item = item

    async def unit(self, node: object) -> RestorableUnit:
        return self._item


def item_frame(item: RestorableUnit) -> NodeFrame:
    """A resolved frame whose provider hands back ``item``."""
    return NodeFrame(cast(Any, ItemProvider(item)), item)
