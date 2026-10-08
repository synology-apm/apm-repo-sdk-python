"""One-off saved profiles for ``[[remote_storage]]`` samples, so the real CLI
(which only reopens a repository via ``--profile``) can reach them.

Each profile is created with the real ``profile add`` in a temp config dir
whose secrets live in a file-backed keyring, then removed with ``profile
remove`` when the run ends; the temp dir goes with it.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from synology_apm_repo.sdk.profiles import AzureProfileConfig, S3ProfileConfig, SmbProfileConfig
from synology_apm_repo.sdk.profiles.model import secret_fields_for

from .._samples import RemoteStorageSample
from ._context import SmokeContext


def _add_args(sample: RemoteStorageSample) -> list[str]:
    config = sample.config
    args = ["--backend", sample.kind.value]
    if isinstance(config, S3ProfileConfig):
        args += ["--bucket", config.bucket]
        if config.endpoint:
            args += ["--endpoint", config.endpoint]
        if config.region:
            args += ["--region", config.region]
        if not config.verify_tls:
            args.append("--no-verify-tls")
    elif isinstance(config, AzureProfileConfig):
        args += ["--container", config.container]
        if config.account_url:
            args += ["--account-url", config.account_url]
        if not config.verify_tls:
            args.append("--no-verify-tls")
    else:
        assert isinstance(config, SmbProfileConfig)
        args += ["--server", config.server, "--share", config.share, "--port", str(config.port)]
        if config.username:
            args += ["--username", config.username]
    return args


def _stdin_secrets(sample: RemoteStorageSample) -> str:
    """One line per secret, in ``profile add``'s prompt order (blank == unset)."""
    return "".join(sample.secrets.get(field, "") + "\n" for field in secret_fields_for(sample.kind))


@contextmanager
def remote_profiles(ctx: SmokeContext, samples: list[RemoteStorageSample]) -> Iterator[dict[str, str]]:
    """Create one profile per sample; yield ``{sample name: profile name}`` for
    the ones that were saved. Every subprocess ``ctx`` runs meanwhile sees them."""
    if not samples:
        yield {}
        return
    created: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="apm-cli-smoke-remote-") as tmp:
        tmp_dir = Path(tmp)
        ctx.runner.base_env = ctx.runner.file_keyring_env(tmp_dir / "config", tmp_dir / "keyring.json")
        try:
            for sample in samples:
                profile = f"smoke-{sample.name}"
                # --no-verify: remote_connect's doctor is the connectivity check.
                result = ctx.run(
                    "profile",
                    f"profile.remote.add[{sample.name}]",
                    "profile",
                    "add",
                    profile,
                    *_add_args(sample),
                    "--no-verify",
                    "--force",
                    input_text=_stdin_secrets(sample),
                )
                if result.exit_code == 0:
                    created[sample.name] = profile
            yield created
        finally:
            for sample_name, profile in created.items():
                ctx.run("profile", f"profile.remote.remove[{sample_name}]", "profile", "remove", profile, "--force")
            ctx.runner.base_env = {}
