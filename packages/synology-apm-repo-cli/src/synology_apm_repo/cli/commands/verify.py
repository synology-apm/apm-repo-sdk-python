"""``synology-apm-repo-cli verify <repo> [--level quick|full]`` — integrity check."""

from __future__ import annotations

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.options import KeyOption, ProfileOption, RepoArgument
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.progress_render import build_progress_meter
from synology_apm_repo.cli.repo_session import opened_repo_or_profile
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import VERIFY_LEVEL_HELP
from synology_apm_repo.sdk.api import (
    Finding,
    Symptom,
    VerifyLevel,
    group_count_label,
    group_findings,
    sort_key,
)
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe

console = Console()

_Report = list[dict[str, object]]


def _build_report(findings: list[Finding], *, verbose: bool) -> _Report:
    # ref (a canonical cat:/wl:/ver: NodeRef -- an internal identifier,
    # never a real display name) follows this CLI's usual
    # internal-identifier convention: --verbose-gated, like doctor's own
    # catalog_id/workload_id.
    return [
        {
            "stage": f.stage,
            "symptom": f.symptom.value,
            "path": f.path,
            "detail": f.detail,
            **({"ref": f.ref} if verbose and f.ref is not None else {}),
        }
        for f in findings
    ]


def _finding_ref_suffix(finding: Finding, *, verbose: bool) -> str:
    return f" ({safe(finding.ref)})" if verbose and finding.ref is not None else ""


def _render_human(findings: list[Finding], level: VerifyLevel, *, verbose: bool) -> None:
    if not findings:
        console.print(f"[green]clean[/green] — no findings at level={level.value}")
        return
    groups = group_findings(findings)
    group_count = f"{len(groups)} {pluralize(len(groups), 'group')}"
    problem_count = sum(1 for f in findings if f.symptom is not Symptom.REPAIRED_VIA_PARITY)
    if problem_count:
        console.print(f"[red]{problem_count} finding(s)[/red] in {group_count} at level={level.value}")
    else:
        # Not a problem left unresolved -- REPAIRED_VIA_PARITY means a CRC
        # mismatch that this SDK's own Redundancy-blob self-repair already
        # reconstructed and confirmed byte-for-byte correct.
        console.print(
            f"[green]clean[/green] ({len(findings)} self-repaired via parity) in {group_count} at level={level.value}"
        )
    # Every group -- even one with a single member -- renders the same
    # header-plus-instance-line shape: a report mixing some repeated
    # findings with a one-off is visually consistent this way, and it's
    # one rendering path instead of two.
    for group in groups:
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


@typer_async
async def verify(
    ctx: typer.Context,
    repo: RepoArgument = None,
    key: KeyOption = None,
    level: VerifyLevel = typer.Option(VerifyLevel.QUICK.value, "--level", help=VERIFY_LEVEL_HELP),
    profile: ProfileOption = None,
) -> None:
    """Run an integrity check against REPO and report every Finding."""
    state: CliState = ctx.obj
    verify_meter = build_progress_meter(state)
    async with opened_repo_or_profile(repo, key, profile=profile, state=state) as repository:
        # verify_reachable()'s own progress runs in two stages: an
        # "items" tick while discovering/sizing every (workload, version)
        # pair's buckets, then a "bytes" tick per bucket actually checked
        # once that whole discovery pass is done and a real, stable byte
        # total is known -- discovery must walk every version's
        # composition extents and claim every touched bucket before the
        # total to check is a final, stable number, so checking can't
        # start until that pass is complete.
        findings = sorted(await repository.verify(level, progress=verify_meter.update), key=sort_key)

    # Sorted once, here, on the flat Finding list -- report/--json inherit
    # that order unchanged (list order survives _build_report's list
    # comprehension), and _render_human's own grouping relies on it too:
    # group_findings expects its input already sorted by sort_key to
    # bucket findings correctly.
    report = _build_report(findings, verbose=state.verbose)
    render(console, state, json=report, human=lambda: _render_human(findings, level, verbose=state.verbose), page=True)
