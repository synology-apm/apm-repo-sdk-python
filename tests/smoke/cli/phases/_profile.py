"""``profile`` domain: ``profile add/list/show/remove`` round trip against
a sandboxed config dir, driven through ``--no-input``'s stdin-secrets flow
(``add`` reads secrets one-per-line off stdin in prompt order; ``remove``
needs ``--force`` instead of a confirm prompt) -- fully offline, no real
repository needed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from .._context import SmokeContext

_PROFILE_NAME = "smoke-test-profile"


def run(ctx: SmokeContext) -> None:
    with tempfile.TemporaryDirectory(prefix="apm-cli-smoke-home-") as home_dir:
        env = ctx.runner.sandboxed_env(Path(home_dir))

        # --no-verify: this profile's bucket doesn't exist -- --verify's
        # default connectivity check would only ever fail against it, and
        # this phase is checking the add/list/show/remove round trip
        # itself, not real S3 connectivity (already the concern of
        # whichever real sample's own ObjectStore this tool discovers
        # elsewhere).
        ctx.run(
            "profile",
            "profile.add",
            "profile",
            "add",
            _PROFILE_NAME,
            "--backend",
            "s3",
            "--bucket",
            "smoke-test-bucket",
            "--no-verify",
            "--force",
            env_overrides=env,
            input_text="fake-access-key\nfake-secret-key\n",
        )

        list_result = ctx.run("profile", "profile.list", "profile", "list", env_overrides=env)
        ctx.check("profile", "profile.list.shows_added", _PROFILE_NAME in list_result.stdout)

        show_result = ctx.run("profile", "profile.show", "profile", "show", _PROFILE_NAME, env_overrides=env)
        ctx.check("profile", "profile.show.shows_bucket", "smoke-test-bucket" in show_result.stdout)

        ctx.run("profile", "profile.remove", "profile", "remove", _PROFILE_NAME, "--force", env_overrides=env)

        list_after_result = ctx.run("profile", "profile.list.after_remove", "profile", "list", env_overrides=env)
        ctx.check("profile", "profile.list.after_remove.gone", _PROFILE_NAME not in list_after_result.stdout)


__all__ = ["run"]
