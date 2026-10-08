"""``index.md`` renderer, shared by ``sdk/``, ``cli/``, and ``browser/`` --
run metadata plus a per-step checklist with four outcomes (passed/skipped/degraded/failed).
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from ._context import DomainStats, StepResult, step_slug

_ICONS = {"passed": "✓", "skipped": "−", "degraded": "◐", "failed": "✗"}


def make_report_dir(tool: str) -> Path:
    """Create and return a new ``tests/smoke/reports/<tool>/<UTC timestamp>/`` report directory."""
    base_dir = Path(__file__).resolve().parent / "reports" / tool
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = base_dir / timestamp
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def resource_usage(*, children: bool = False) -> dict[str, str]:
    """CPU and peak-memory figures for this process, or for its waited-for
    children (``children=True``, for a tool that drives subprocesses).

    ``ru_maxrss`` is bytes on macOS and KiB elsewhere; with ``children`` it is
    the largest single child, not a sum. The ``resource`` module does not exist
    on Windows, so there the section only says so.
    """
    if sys.platform == "win32":
        return {"scope": "unavailable on Windows"}
    import resource

    usage = resource.getrusage(resource.RUSAGE_CHILDREN if children else resource.RUSAGE_SELF)
    scale = 1 if sys.platform == "darwin" else 1024
    return {
        "scope": "child processes" if children else "this process",
        "user CPU": f"{usage.ru_utime:.1f} s",
        "system CPU": f"{usage.ru_stime:.1f} s",
        "max RSS": f"{usage.ru_maxrss * scale / (1024 * 1024):.0f} MiB",
    }


def write_index(
    report_dir: Path,
    *,
    title: str,
    domains: Sequence[str],
    group: str,
    sample_count: int,
    started_at: datetime,
    finished_at: datetime,
    stats: dict[str, DomainStats],
    step_results: dict[str, list[StepResult]],
    trace_files: Sequence[str] = (),
    resources: Mapping[str, str] | None = None,
) -> None:
    """Write ``index.md``: run metadata, per-domain stats table, full
    checklist, file pointers. ``trace_files`` lists extra artifacts this run
    wrote (``store_trace.jsonl``) to link from ``## Files``; ``resources``
    (see ``resource_usage``) adds a ``## Resource usage`` section."""
    lines: list[str] = [
        f"# {title}",
        "",
        f"- group: {group}",
        f"- samples configured: {sample_count}",
        f"- started: {started_at.isoformat()}",
        f"- finished: {finished_at.isoformat()}",
        "",
        "## Per-domain results",
        "",
        "| Domain | Ran | Skipped | Degraded | Checks passed | Checks failed | Unexpected |",
        "|---|---|---|---|---|---|---|",
    ]
    for domain in domains:
        s = stats[domain]
        lines.append(
            f"| {domain} | {s.ran} | {s.skipped} | {s.degraded} | "
            f"{s.checks_passed} | {s.checks_failed} | {s.unexpected} |"
        )

    if resources:
        lines += ["", "## Resource usage", ""]
        lines += [f"- {key}: {value}" for key, value in resources.items()]

    lines += ["", "## Checklist", ""]
    for domain in domains:
        results = step_results[domain]
        if not results:
            continue
        lines.append(f"### {domain}")
        lines.append("")
        lines.append("| | Step | Result | Detail |")
        lines.append("|---|---|---|---|")
        for r in results:
            icon = _ICONS[r.status]
            result_cell = r.label + (f" -- {r.note}" if r.note else "")
            detail_cell = f"[→]({domain}.md#{step_slug(r.step)})" if r.has_detail else ""
            lines.append(f"| {icon} | `{r.step}` | {result_cell} | {detail_cell} |")
        lines.append("")

    lines += ["## Files", ""]
    domains_with_detail = [d for d in domains if any(r.has_detail for r in step_results[d])]
    for domain in domains_with_detail:
        lines.append(f"- [{domain}.md]({domain}.md)")
    for trace_file in trace_files:
        lines.append(f"- [{trace_file}]({trace_file})")
    lines += [
        "",
        "## Test data",
        "",
        "If many steps above show `skipped`, the configured sample set may be"
        " missing prerequisite data -- see"
        # report_dir is tests/smoke/reports/<tool>/<timestamp>/.
        " [smoke_samples.toml](../../../smoke_samples.toml)'s own per-sample"
        " comments for what each named sample is expected to cover, and any"
        " `degraded` outcome documented there as expected for that sample.",
        "",
    ]

    (report_dir / "index.md").write_text("\n".join(lines), encoding="utf-8")
