"""``remote_connect`` domain: one ``doctor --profile`` per ``[[profile]]``/
``[[remote_storage]]`` sample against the live bucket/container/share -- a
connect-and-scan check; the local refs cover per-command rendering. A
``[[remote_storage]]`` sample goes through its ``_remote_profiles.py``
throwaway profile.
"""

from __future__ import annotations

from ..._samples import ProfileSample, RemoteStorageSample
from .._context import SmokeContext

#: A remote ``doctor`` scans the whole bucket/container over the network.
_TIMEOUT_SECONDS = 300.0


def run(ctx: SmokeContext) -> None:
    entries: list[ProfileSample | RemoteStorageSample] = ctx.data.get("remote_entries", [])
    profiles: dict[str, str] = ctx.data.get("remote_profiles", {})
    if not entries:
        ctx.skip("remote_connect", "remote_connect.no_remote_samples", "no remote sample configured")
        return
    for entry in entries:
        profile = entry.profile if isinstance(entry, ProfileSample) else profiles.get(entry.name)
        if profile is None:
            ctx.skip("remote_connect", f"remote_connect.{entry.name}.doctor", "its throwaway profile wasn't saved")
            continue
        args = ["--profile", profile, *(["--key", entry.key] if entry.key else [])]
        ctx.run("remote_connect", f"remote_connect.{entry.name}.doctor", "doctor", *args, timeout=_TIMEOUT_SECONDS)
