"""Entry point: ``uv run python -m tests.smoke.sdk [--group ...]``.

Drives ``synology_apm_repo.sdk``'s public async API against the samples
``smoke_samples.toml`` configures, one repository at a time: discover ->
enumerate its catalog/workloads/versions (whatever ``--group``) -> every
selected domain's ``run_for_repo`` -> close. Writes Markdown reports and
``store_trace.jsonl`` to ``tests/smoke/reports/sdk/<UTC timestamp>/``.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import IO

from synology_apm_repo.sdk import Catalog, KeyMismatchError, KeyRequiredError, Session, TraceEvent, Version, Workload
from synology_apm_repo.sdk.identifiers import CatalogId

from .._report import make_report_dir, resource_usage, write_index
from .._samples import SampleEntry, load_smoke_samples
from .._shared_refs import RepoInfo, discover_repos
from .._trace_step import current_step, trace_step
from ._context import DOMAINS, SmokeContext
from .phases import _catalog, _device, _diagnostics, _fs, _saas

_ORDER = ("catalog", "device", "fs", "saas", "diagnostics")
_PHASES = {"catalog": _catalog, "device": _device, "fs": _fs, "saas": _saas, "diagnostics": _diagnostics}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.smoke.sdk",
        description="Run the synology-apm-repo-sdk smoke test against the sample "
        "repositories configured in tests/smoke/smoke_samples.toml.",
    )
    parser.add_argument("--group", choices=("all", *_ORDER), default="all", help="Run one domain only (default: all)")
    return parser.parse_args(argv)


def _make_trace_writer(trace_file: IO[str]) -> Callable[[str, TraceEvent], None]:
    def _write(sample_name: str, event: TraceEvent) -> None:
        record = {"sample": sample_name, "step": current_step(), **dataclasses.asdict(event)}
        trace_file.write(json.dumps(record, default=str))
        trace_file.write("\n")
        trace_file.flush()

    return _write


async def _enumerate_catalog(ctx: SmokeContext, ri: RepoInfo) -> None:
    """One repository's catalog/workload/version walk: extends
    ``ctx.data["workloads"]`` with its entries and records its wall-clock
    cost in ``ctx.data["bootstrap_elapsed"]`` (read by
    ``phases/_catalog.py``'s enumeration-budget check)."""
    started = time.monotonic()

    async def _list_workloads(ri: RepoInfo = ri) -> list[tuple[Catalog, list[Workload]]]:
        return [(catalog, await catalog.workloads()) for catalog in await ri.repo.catalogs()]

    # workloads()/versions() require a verified key themselves, so a sample
    # configured without its correct key raises here: degraded, not a bug.
    by_catalog = await ctx.call(
        "catalog",
        f"bootstrap.workloads[{ri.sample_name}]",
        _list_workloads,
        degrade_on=(KeyRequiredError, KeyMismatchError),
    )
    own_workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = []
    if by_catalog:
        # One ctx.call per workload, so one failing versions() call doesn't
        # discard its siblings.
        for catalog, connection_workloads in by_catalog:
            for workload in connection_workloads:

                async def _versions(catalog: Catalog = catalog, workload: Workload = workload) -> list[Version]:
                    return await catalog.versions(workload)

                versions = await ctx.call(
                    "catalog",
                    f"bootstrap.versions[{ri.sample_name}.{workload.workload_id}]",
                    _versions,
                    degrade_on=(KeyRequiredError, KeyMismatchError),
                )
                if versions is not None:
                    # catalog_id, not the Catalog: see resolve_catalog.
                    own_workloads.append((ri, catalog.catalog_id, workload, versions))
    ctx.data.setdefault("workloads", []).extend(own_workloads)
    ctx.data.setdefault("bootstrap_elapsed", {})[ri.sample_name] = time.monotonic() - started


async def _process_entry(
    ctx: SmokeContext,
    session: Session,
    entry: SampleEntry,
    write_trace: Callable[[str, TraceEvent], None],
    domains: tuple[str, ...],
) -> int:
    """Discover, enumerate, run every domain in ``domains`` against, and
    close each repository this sample entry yields, one at a time so only
    one repository's resources are open at once. Returns how many
    repositories the entry yielded."""

    async def _discover_one() -> list[RepoInfo]:
        return await discover_repos(session, [entry], trace=write_trace)

    # One ctx.call per entry, so one misconfigured sample doesn't discard the rest.
    found = await ctx.call("catalog", f"bootstrap.discover[{entry.name}]", _discover_one)
    if not found:
        return 0

    for ri in found:
        print(f"[smoke] processing sample: {ri.sample_name}")
        # Phase-level fallback labels: a store call a phase makes between its
        # ctx.call steps is attributed to the phase, and an inner ctx.call
        # still overrides this with its own step.
        with trace_step("catalog", f"enumerate[{ri.sample_name}]"):
            await _enumerate_catalog(ctx, ri)
        for domain in domains:
            with trace_step(domain, f"run_for_repo[{ri.sample_name}]"):
                await _PHASES[domain].run_for_repo(ctx, ri)
        await session.close_repo(ri.repo)
        # Each row holds the RepoInfo; dropping them unpins the closed repository.
        workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])
        ctx.data["workloads"] = [row for row in workloads if row[0] is not ri]
    return len(found)


async def _run(args: argparse.Namespace) -> int:
    entries = load_smoke_samples()
    report_dir = make_report_dir("sdk")
    started_at = datetime.now(UTC)
    domains = _ORDER if args.group == "all" else (args.group,)

    with (report_dir / "store_trace.jsonl").open("w", encoding="utf-8") as trace_file:
        write_trace = _make_trace_writer(trace_file)
        ctx = SmokeContext(report_dir)
        try:
            if not entries:
                print("[smoke] no samples configured -- see tests/smoke/smoke_samples.toml.example")
                # Step names embed per-repo data, so there is no fixed list for
                # skip_remaining(): one sentinel skip per domain instead.
                reason = "no samples configured -- see smoke_samples.toml.example"
                for domain in _ORDER:
                    ctx.skip(domain, f"{domain}.no_samples_configured", reason)
            else:
                async with Session() as session:
                    total_repos = 0
                    for entry in entries:
                        total_repos += await _process_entry(ctx, session, entry, write_trace, domains)
                    if total_repos == 0:
                        # Every discovery failed (already recorded) or found
                        # nothing: explain the otherwise-empty report.
                        reason = "no repository discovered across any configured sample"
                        for domain in domains:
                            ctx.skip(domain, f"{domain}.no_repository_discovered", reason)
        finally:
            finished_at = datetime.now(UTC)
            write_index(
                report_dir,
                title="SDK smoke test report",
                domains=DOMAINS,
                group=args.group,
                sample_count=len(entries),
                started_at=started_at,
                finished_at=finished_at,
                stats=ctx.stats,
                step_results=ctx.step_results,
                resources=resource_usage(children=False),
                trace_files=("store_trace.jsonl",),
            )
            ctx.close()

    print(f"[smoke] report written to {report_dir}")
    unexpected = sum(s.unexpected for s in ctx.stats.values())
    checks_failed = sum(s.checks_failed for s in ctx.stats.values())
    if unexpected or checks_failed:
        print(f"[smoke] FAILED: {unexpected} unexpected result(s), {checks_failed} failed check(s)")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
