"""``synology-apm-repo-cli key <repo> --key <str>`` — verify a key string against a
repository. ``gcm_ok`` alone is the whole answer to "is this key
correct": a successful AES-256-GCM authentication tag is cryptographic
proof of the key for every chunk, not a probabilistic check. To also
verify every chunk *read* against its stored fingerprint, use ``verify
--level full``.
"""

from __future__ import annotations

from typing import Annotated, TypedDict

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.errors import err_console
from synology_apm_repo.cli.options import ProfileOption, RepoArgument
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import opened_repo_or_profile
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import KEY_HELP
from synology_apm_repo.sdk import KeyStatus, KeyVerification, Repository
from synology_apm_repo.sdk.presentation import safe


class _KeyVerificationReport(TypedDict):
    status: str
    gcm_ok: bool


def _build_report(repository: Repository, verification: KeyVerification) -> _KeyVerificationReport:
    return {
        "status": repository.key_status.value,
        "gcm_ok": verification.gcm_ok,
    }


def _render_human(report: _KeyVerificationReport, repository: Repository) -> None:
    # key_status is I/O-free, so reading it after the session closed is safe.
    ok = repository.key_status is KeyStatus.VERIFIED
    color = "green" if ok else "red"
    console.print(f"[{color}]{report['status']}[/{color}]")
    console.print(f"  gcm_ok={report['gcm_ok']}")


@typer_async
async def key(
    ctx: typer.Context,
    # A required option must precede the defaulted REPO argument in Python;
    # the command line itself takes either order.
    key_string: Annotated[str, typer.Option("--key", help=KEY_HELP)],
    repo: RepoArgument = None,
    profile: ProfileOption = None,
) -> None:
    """Verify KEY against REPO and print whether it's correct."""
    state: CliState = ctx.obj
    async with opened_repo_or_profile(repo, None, profile=profile, state=state) as repository:
        result = await repository.set_key(key_string)
        if result.warning is not None:
            # The key verified, but reopening a sibling catalog failed:
            # warn rather than fail, as the TUI's KeyDialog._verify does.
            err_console.print(f"[yellow]warning: {safe(result.warning)}[/yellow]")
        report = _build_report(repository, result.verification)

    render(console, state, json=report, human=lambda: _render_human(report, repository))
