"""``remote_connect`` domain: for one configured ``[[profile]]``/
``[[remote_storage]]`` sample, drives ``ConnectDialog``'s real S3/Azure/SMB
tab end to end against the live bucket/container/share -- the one place
any of this project's tests exercises those tabs (the saved-profile
picker and raw manual entry alike) against a real backend, rather than a
fake ``Session``/``BlobServiceClient``.

Runs once per configured remote sample, each in its own, separate
``App.run_test()`` session (see ``__main__.py``): connecting a second
source in the same session *replaces* the first's tree rather than
accumulating (``BrowseScreen._reset_for_new_scan`` clears ``_repos``/the
tree on every new scan), so unlike
``navigate.py``'s one local connect, this can't share a single session
across several remote samples the way pressing ``c`` in real use might
suggest. One fresh session per sample keeps each connect attempt
independent -- the same reasoning ``key_dialog.py``'s own separate
session already has for its one encrypted sample.
"""

from __future__ import annotations

from typing import Any

from synology_apm_repo.sdk import get_profile

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
