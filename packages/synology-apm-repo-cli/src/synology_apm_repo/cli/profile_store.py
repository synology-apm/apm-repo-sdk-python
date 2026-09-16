"""``resolve_profile_store()`` — the CLI's one integration point with saved
connection profiles: everything under ``commands/`` that accepts
``--profile`` goes through this rather than importing ``sdk.profiles``
directly.
"""

from __future__ import annotations

from synology_apm_repo.sdk.profiles import build_store
from synology_apm_repo.sdk.storage import ObjectStore


async def resolve_profile_store(name: str) -> ObjectStore:
    """Build a ready-to-use ``ObjectStore`` for the saved profile ``name``.
    Thin wrapper around ``build_store``.

    Raises:
        ProfileNotFoundError: If ``name`` isn't saved.
    """
    return await build_store(name)
