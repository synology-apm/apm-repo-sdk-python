"""Entry point: ``uv run python -m tests.smoke.cli [--group ...]``.

Drives the real, installed ``synology-apm-repo-cli`` console script as a
subprocess against whatever real sample repositories
``smoke_samples.toml`` configures (see ``.._samples``), writing Markdown
reports to ``tests/smoke/reports/cli/<UTC timestamp>/``. Ref selection
(``_bootstrap``) uses the SDK in-process, once, purely to pick a real,
representative ``NodeRef`` per sample/workload-type -- every actual check
after that goes through the real subprocess CLI (see ``.._shared_refs.
list_representative_refs``).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from synology_apm_repo.sdk import Session

from .._report import make_report_dir, write_index
from .._samples import load_smoke_samples
from .._shared_refs import RepresentativeRef, list_representative_refs
from ._context import DOMAINS, SmokeContext
from .phases import _commands, _errors, _export_lifecycle, _global_flags, _profile

_ORDER = ("commands", "global_flags", "export_lifecycle", "profile", "errors")
_PHASES = {
    "commands": _commands,
    "global_flags": _global_flags,
    "export_lifecycle": _export_lifecycle,
    "profile": _profile,
    "errors": _errors,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.smoke.cli",
        description="Run the synology-apm-repo-cli smoke test against the sample "
        "repositories configured in tests/smoke/smoke_samples.toml.",
    )
    parser.add_argument("--group", choices=("all", *_ORDER), default="all", help="Run one domain only (default: all)")
    return parser.parse_args(argv)


async def _bootstrap() -> tuple[int, list[RepresentativeRef], list[str]]:
    entries = load_smoke_samples()
    if not entries:
        return 0, [], []
    async with Session() as session:
        # exclude_unreopenable_by_cli=True: a RemoteStorageSample-derived
        # ref has no --profile to reopen it with (see that function's own
        # docstring) -- sdk/ and browser/ still cover it fully.
        refs, skip_reasons = await list_representative_refs(session, entries, exclude_unreopenable_by_cli=True)
    return len(entries), refs, skip_reasons


def _run(args: argparse.Namespace) -> int:
    sample_count, refs, bootstrap_skips = asyncio.run(_bootstrap())
    report_dir = make_report_dir("cli")
    started_at = datetime.now(UTC)

    ctx = SmokeContext(report_dir)
    try:
        if not sample_count:
            print("[smoke] no samples configured -- see tests/smoke/smoke_samples.toml.example")
            reason = "no samples configured -- see smoke_samples.toml.example"
            for domain in _ORDER:
                ctx.skip(domain, f"{domain}.no_samples_configured", reason)
        else:
            # Recorded here, not inside list_representative_refs() itself
            # (this SmokeContext doesn't exist yet at that point) -- see
            # that function's own docstring for why a bare print() alone
            # would leave a real cli-specific coverage gap invisible in
            # index.md.
            for i, reason in enumerate(bootstrap_skips):
                ctx.skip("commands", f"commands.bootstrap_skip[{i}]", reason)
            ctx.data["refs"] = refs
            phases = _ORDER if args.group == "all" else (args.group,)
            for phase in phases:
                print(f"[smoke] running phase: {phase}")
                _PHASES[phase].run(ctx)
    finally:
        finished_at = datetime.now(UTC)
        write_index(
            report_dir,
            title="CLI smoke test report",
            domains=DOMAINS,
            group=args.group,
            sample_count=sample_count,
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
    return _run(args)


if __name__ == "__main__":
    raise SystemExit(main())
