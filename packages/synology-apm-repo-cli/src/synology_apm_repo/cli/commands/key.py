"""``synology-apm-repo-cli key <repo> --key <str>`` — verify a key string against a
repository. ``gcm_ok`` alone is the whole answer to "is this key
correct" (see ``sdk.dedup.keys``'s own module docstring for why) — there
is no separate, deeper check this command runs. A caller who additionally
wants every chunk *read* verified against its stored fingerprint should
reach for ``verify --level full`` or ``DedupRepo.open(...,
verify_fingerprint=True)`` directly — a data-integrity property of
reads, not of this key.
"""

from __future__ import annotations

from typing import TypedDict

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import opened_repo_or_profile
from synology_apm_repo.cli.errors import err_console
from synology_apm_repo.cli.options import ProfileOption, RepoArgument
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import KEY_HELP
from synology_apm_repo.sdk.api import KeyStatus, KeyVerification, Repository

console = Console()


class _KeyVerificationReport(TypedDict):
    status: str
    gcm_ok: bool


def _build_report(repository: Repository, verification: KeyVerification) -> _KeyVerificationReport:
    return {
        # key_status stays a plain sync property (it's I/O-free — it only
        # reads the verification result set_key() just produced), so it
        # needs no await here.
        "status": repository.key_status.value,
        "gcm_ok": verification.gcm_ok,
    }


def _render_human(report: _KeyVerificationReport, repository: Repository) -> None:
    # repository.key_status is read again here, after the session
    # opened_repo() already closed — safe, since it's the same
    # plain, no-I/O property _build_report already read above.
    ok = repository.key_status is KeyStatus.VERIFIED
    color = "green" if ok else "red"
    console.print(f"[{color}]{report['status']}[/{color}]")
    console.print(f"  gcm_ok={report['gcm_ok']}")


@typer_async
async def key(
    ctx: typer.Context,
    repo: RepoArgument = None,
    key_string: str = typer.Option(..., "--key", help=KEY_HELP),
    profile: ProfileOption = None,
) -> None:
    """Verify KEY against REPO and print whether it's correct."""
    state: CliState = ctx.obj
    async with opened_repo_or_profile(repo, None, profile=profile, state=state) as repository:
        try:
            verification = await repository.set_key(key_string)
        except ExceptionGroup as exc:
            # Repository.set_key() raises this only when the key itself
            # verified fine but reopening/closing one specific
            # already-opened sibling catalog independently failed --
            # repository.key_status is already VERIFIED/INVALID by the
            # time this is raised (set_key()'s own docstring), so this is
            # a genuinely accepted key with a partial cleanup failure
            # alongside it, not a rejected one. Warn rather than fail --
            # same posture as the TUI's own KeyDialog._verify.
            err_console.print(f"[yellow]warning: {exc}[/yellow]")
            maybe_verification = repository.key_verification
            assert maybe_verification is not None  # set_key() always sets this before raising
            verification = maybe_verification
        report = _build_report(repository, verification)

    if state.json:
        console.print_json(data=report)
    else:
        _render_human(report, repository)
