"""``SharedSaasContext``: what every candidate provider for one SaaS
version shares — its ``saas_obj``, object-name index and the index's
reference-counted ObjectDB."""

from __future__ import annotations

import dataclasses

from ..._util.once import AsyncOnce
from ...catalog.version import Version
from ...dedup.dedup_file import DedupFile
from ...dedup.repository import DedupRepo
from .object_name_index import ObjectNameIndex, resolve_object_name_index
from .objectdb import ObjectDb
from .stream import SaasStreamCache


class SharedIndexObjectDb:
    """One version's object-name-index ObjectDB, reference-counted: loaded on
    the first ``acquire()``, closed once every holder has called
    ``release()``."""

    def __init__(self, dedup_file: DedupFile, object_name_index: ObjectNameIndex) -> None:
        self.object_name_index = object_name_index
        self._once: AsyncOnce[ObjectDb] = AsyncOnce(
            lambda: ObjectDb.load(dedup_file, object_name_index.offset, object_name_index.length)
        )
        self._holders = 0

    async def acquire(self) -> ObjectDb:
        object_db = await self._once.get()
        self._holders += 1
        return object_db

    async def release(self) -> None:
        self._holders -= 1
        if self._holders == 0:
            await self._once.close(lambda object_db: object_db.close())


@dataclasses.dataclass(frozen=True, slots=True)
class SharedSaasContext:
    """One SaaS version's stream object and object-name index, resolved once
    by ``resolve_shared_saas_context``: ``units/dispatch.py`` passes it to
    every candidate provider (M365's ``USER_EXCHANGE`` tries four) and to
    the ``RawObjectProvider`` fallback as ``shared``, and a provider built
    on its own resolves one the same way.

    ``index_object_db`` loads the index's ObjectDB once between them; each
    provider takes a hold and releases it when it closes. The other fields
    need no closing."""

    dedup_file: DedupFile
    object_name_index: ObjectNameIndex | None
    index_object_db: SharedIndexObjectDb | None


async def resolve_shared_saas_context(
    repo: DedupRepo, version: Version, saas_streams: SaasStreamCache
) -> SharedSaasContext:
    """Resolve the ``SharedSaasContext`` for ``version``. ``saas_streams``
    is borrowed: the ``SaasStream`` it opens stays cached there."""
    dedup_file = await saas_streams.open_saas_obj(version)
    object_name_index = await resolve_object_name_index(repo, version)
    index_object_db = SharedIndexObjectDb(dedup_file, object_name_index) if object_name_index is not None else None
    return SharedSaasContext(
        dedup_file=dedup_file, object_name_index=object_name_index, index_object_db=index_object_db
    )
