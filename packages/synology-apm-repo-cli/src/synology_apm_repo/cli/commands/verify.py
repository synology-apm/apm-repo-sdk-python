"""``synology-apm-repo-cli verify <repo> [--level quick|full]`` — integrity check."""

from __future__ import annotations

from typing import Annotated

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.errors import ExitCode
from synology_apm_repo.cli.options import KeyOption, ProfileOption, RepoArgument
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.progress_render import build_progress_meter
from synology_apm_repo.cli.repo_session import opened_repo_or_profile
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    VERIFY_CONCURRENT_DECODES_HELP,
    VERIFY_CONCURRENT_READS_HELP,
    VERIFY_LEVEL_HELP,
)
from synology_apm_repo.sdk import Finding, VerifyLevel, VerifyTuning
from synology_apm_repo.sdk.presentation import VerifySummary, group_count_label, safe, summarize_findings


def _build_report(summary: VerifySummary, level: VerifyLevel, *, verbose: bool) -> dict[str, object]:
    """``--json``'s object: the level, the unresolved-finding count exit
    status 3 is decided by, and every finding."""
    # ref is an internal NodeRef identifier, so it is --verbose-gated.
    findings = [
        {
            "stage": f.stage,
            "symptom": f.symptom.value,
            "path": f.path,
            "detail": f.detail,
            **({"ref": f.ref} if verbose and f.ref is not None else {}),
        }
        for f in summary.findings
    ]
    return {"level": level.value, "problem_count": summary.problem_count, "findings": findings}


def _finding_ref_suffix(finding: Finding, *, verbose: bool) -> str:
    return f" ({safe(finding.ref)})" if verbose and finding.ref is not None else ""


def _render_human(summary: VerifySummary, level: VerifyLevel, *, verbose: bool) -> None:
    headline = summary.headline(level.value)
    if headline is None:
        console.print(f"[green]clean[/green] — no findings at level={level.value}")
        return
    console.print(headline)
    # Every group, even a single-member one, renders header plus instance lines.
    for group in summary.groups:
        rep = group[0].finding
        # symptom/stage are pure structure, unescaped; path/detail/ref can
        # embed real content-derived text (a file_map path, a filename
        # near a composition offset), so those go through safe() first.
        console.print(f"  [{rep.symptom.value}] {rep.stage}: {safe(group[0].template)} ({group_count_label(group)})")
        for aug in group:
            line = f"    - {safe(aug.finding.path)}"
            if aug.variable_parts:
                line += f" — {safe(aug.variable_parts)}"
            console.print(line + _finding_ref_suffix(aug.finding, verbose=verbose))


def _tuning(*, concurrent_reads: int | None, concurrent_decodes: int | None) -> VerifyTuning | None:
    """The ``VerifyTuning`` the given flags ask for, the SDK's defaults for the rest; ``None`` when neither is given."""
    given = {
        name: value
        for name, value in (("max_concurrent_reads", concurrent_reads), ("max_concurrent_decodes", concurrent_decodes))
        if value is not None
    }
    return VerifyTuning(**given) if given else None


@typer_async
async def verify(
    ctx: typer.Context,
    repo: RepoArgument = None,
    key: KeyOption = None,
    level: Annotated[VerifyLevel, typer.Option("--level", help=VERIFY_LEVEL_HELP)] = VerifyLevel.QUICK,
    concurrent_reads: Annotated[
        int | None, typer.Option("--concurrent-reads", min=1, help=VERIFY_CONCURRENT_READS_HELP)
    ] = None,
    concurrent_decodes: Annotated[
        int | None, typer.Option("--concurrent-decodes", min=1, help=VERIFY_CONCURRENT_DECODES_HELP)
    ] = None,
    profile: ProfileOption = None,
) -> None:
    """Run an integrity check against REPO and report every Finding; exits 3 when any is still unresolved."""
    state: CliState = ctx.obj
    verify_meter = build_progress_meter(state)
    tuning = _tuning(concurrent_reads=concurrent_reads, concurrent_decodes=concurrent_decodes)
    async with opened_repo_or_profile(ctx, repo, key, profile=profile) as repository:
        # Two stages: one "items" tick per discovered workload/version, then
        # one per bucket checked ("bytes" at FULL, "buckets" at QUICK).
        summary = summarize_findings(await repository.verify(level, progress=verify_meter.update, tuning=tuning))

    report = _build_report(summary, level, verbose=state.verbose)
    render(console, state, json=report, human=lambda: _render_human(summary, level, verbose=state.verbose), page=True)
    if summary.problem_count:
        raise typer.Exit(code=ExitCode.FINDINGS)
