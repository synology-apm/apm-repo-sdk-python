"""Entry point: ``uv run python -m tests.smoke.sdk [--group ...]``.

Drives ``synology_apm_repo.sdk``'s public async API directly against
whatever real sample repositories ``smoke_samples.toml`` configures (see
``.._samples``/``../smoke_samples.toml.example``), writing Markdown
reports + ``store_trace.jsonl`` to ``tests/smoke/reports/sdk/<UTC
timestamp>/``.

Unlike the ``../../apm-sdk-python/tests/smoke/`` tool this is modeled on,
repository discovery and catalog enumeration always run before any
domain's own per-repo checks for that same repository, regardless of
``--group`` -- it's cheap, metadata-only I/O (never touches encrypted
``target.db`` or chunk bytes), so there's no reason to hide it behind
``--group`` the way that reference project's own, much heavier, live-
server queries are.

One repository at a time, start to finish: discover -> enumerate its own
catalog/workloads/versions -> run every selected domain's own
``run_for_repo`` against it -> close it -> move to the next. Every
domain's own skip decisions are purely per-repo ("this sample doesn't
have a VM workload"), not a once-per-run aggregate computed from every
sample's data up front.
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

from .._report import make_report_dir, write_index
from .._samples import SampleEntry, load_smoke_samples
from .._shared_refs import RepoInfo, discover_repos
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
        record = {"sample": sample_name, **dataclasses.asdict(event)}
        trace_file.write(json.dumps(record, default=str))
        trace_file.write("\n")
        trace_file.flush()

    return _write


async def _enumerate_catalog(ctx: SmokeContext, ri: RepoInfo) -> None:
    """This one repository's own catalog/workload/version walk --
    extends the shared ``ctx.data["workloads"]`` list with just its own
    entries, and records its own ``ctx.data["bootstrap_elapsed"]`` wall-
    clock cost (``catalog.py``'s own enumeration-budget check reads it).
    Always runs, for every domain's ``run_for_repo`` to read, regardless
    of ``--group`` (see module docstring)."""
    started = time.monotonic()

    async def _list_workloads(ri: RepoInfo = ri) -> list[tuple[Catalog, list[Workload]]]:
        return [(catalog, await catalog.workloads()) for catalog in await ri.repo.catalogs()]

    # degrade_on=(KeyRequiredError, KeyMismatchError): workloads()/versions() now
    # gate on key_status themselves (api/repository.py's
    # _require_key_verified) -- a [[sample]] configured without its
    # (correct) key raises here instead of RepoInfo.readable's own gate
    # (in device/fs/saas) ever coming into play; that's an expected,
    # sample-specific data gap (see that sample's own comment in your
    # smoke_samples.toml), not a real bug, so it must not be recorded
    # unexpected the way an actual failure would be.
    by_catalog = await ctx.call(
        "catalog",
        f"bootstrap.workloads[{ri.sample_name}]",
        _list_workloads,
        degrade_on=(KeyRequiredError, KeyMismatchError),
    )
    own_workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = []
    if by_catalog:
        # One ctx.call per (repository, workload)'s own versions() lookup --
        # not one lumped per repository -- so a single workload whose
        # versions() call raises is recorded as its own failure instead of
        # silently discarding every *other* workload in the same
        # repository too.
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
                    # catalog_id, not the live Catalog object -- a Catalog
                    # cached here would go stale the moment any later
                    # domain's set_key() call closes and replaces the
                    # underlying DedupRepo on this same repository (see
                    # _shared_refs.py's resolve_catalog docstring).
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
    """Discover, catalog-enumerate, run every domain in ``domains``, and
    close every repository this one sample entry yields -- one at a time,
    so no repository's own resources (``SaasStreamCache``, ``Pool``
    caches, its ``db/<name>`` connections, ...) ever need to stay open
    past its own turn, and no other repository's need to already exist
    yet either (see module docstring). Returns how many repositories this
    entry actually yielded, for ``_run()``'s own "nothing was ever
    discovered" fallback."""

    async def _discover_one() -> list[RepoInfo]:
        return await discover_repos(session, [entry], trace=write_trace)

    # One ctx.call per sample entry, not one bulk discover_repos() call
    # for all of them -- so one misconfigured sample's discovery
    # raising doesn't also discard every other sample's.
    found = await ctx.call("catalog", f"bootstrap.discover[{entry.name}]", _discover_one)
    if not found:
        return 0

    for ri in found:
        print(f"[smoke] processing sample: {ri.sample_name}")
        await _enumerate_catalog(ctx, ri)
        for domain in domains:
            await _PHASES[domain].run_for_repo(ctx, ri)
        await ri.repo.close()
        # Drop this repository's own entries once every domain has had them:
        # each one holds the RepoInfo, so leaving them in the shared list
        # would pin every repository (closed or not) for the whole run, which
        # is the opposite of closing them one at a time above.
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
                # Step names elsewhere are dynamic (embed the sample name and
                # workload type, discovered per repo), so there's no fixed
                # list to hand skip_remaining() here -- one sentinel skip per
                # domain instead, still enough for a clean all-skipped report.
                reason = "no samples configured -- see smoke_samples.toml.example"
                for domain in _ORDER:
                    ctx.skip(domain, f"{domain}.no_samples_configured", reason)
            else:
                async with Session() as session:
                    total_repos = 0
                    for entry in entries:
                        total_repos += await _process_entry(ctx, session, entry, write_trace, domains)
                    if total_repos == 0:
                        # Every configured entry's own discovery either
                        # failed (already recorded as its own
                        # bootstrap.discover[...] failure/degradation
                        # above) or yielded nothing -- same "explain the
                        # otherwise-empty report" reasoning as the
                        # no-samples-configured case above, just reached
                        # a step later.
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
