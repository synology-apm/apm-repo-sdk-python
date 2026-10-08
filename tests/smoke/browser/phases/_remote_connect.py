"""``remote_connect`` domain: for one ``[[profile]]``/``[[remote_storage]]``
sample, drives ``ConnectDialog``'s S3/Azure/SMB tab (saved-profile picker
or raw fields) against the live bucket/container/share, until the
repository node appears. ``__main__.py`` gives each sample its own
``App.run_test()`` session.
"""

from __future__ import annotations

from typing import Any

from synology_apm_repo.sdk.profiles import get_profile

from ..._samples import ProfileSample, RemoteStorageSample
from .._context import SmokeContext
from ._shared import connect_remote


async def run(ctx: SmokeContext, app: Any, pilot: Any, entry: ProfileSample | RemoteStorageSample) -> None:
    async def _connect() -> bool:
        if isinstance(entry, ProfileSample):
            profile = await get_profile(entry.profile)
            await connect_remote(app, pilot, profile.kind, profile_name=entry.profile)
        else:
            await connect_remote(app, pilot, entry.kind, config=entry.config, secrets=entry.secrets)
        return True

    await ctx.call("remote_connect", f"remote_connect.{entry.name}.connect", _connect)


__all__ = ["run"]
