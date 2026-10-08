"""Entry point: ``uv run python -m tests.smoke.cli [--group ...]``.

Drives the installed ``synology-apm-repo-cli`` console script as a
subprocess against the samples ``smoke_samples.toml`` configures, writing
Markdown reports to ``tests/smoke/reports/cli/<UTC timestamp>/``. The SDK
runs in-process only to pick ``main_ref``/``encrypted_ref`` from the
``[[local]]`` samples (``list_representative_refs``/``pick_session_refs``);
remote samples are reached only by ``remote_connect``.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from .._report import make_report_dir, resource_usage, write_index
from .._samples import LocalSample, RemoteStorageSample, SampleEntry, load_smoke_samples
from .._shared_refs import list_representative_refs, pick_session_refs
from ._context import DOMAINS, SmokeContext
from ._remote_profiles import remote_profiles
from .phases import _commands, _errors, _export_lifecycle, _global_flags, _profile, _remote_connect

_ORDER = ("commands", "global_flags", "export_lifecycle", "profile", "errors", "remote_connect")
_PHASES = {
    "commands": _commands,
    "global_flags": _global_flags,
    "export_lifecycle": _export_lifecycle,
    "profile": _profile,
    "errors": _errors,
    "remote_connect": _remote_connect,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.smoke.cli",
        description="Run the synology-apm-repo-cli smoke test against the sample "
        "repositories configured in tests/smoke/smoke_samples.toml.",
    )
    parser.add_argument("--group", choices=("all", *_ORDER), default="all", help="Run one domain only (default: all)")
    parser.add_argument(
        "--sample",
        action="append",
        default=[],
        metavar="NAME",
        help="Run only the named sample (repeatable; default: every configured sample)",
    )
    return parser.parse_args(argv)


def _load_entries(names: list[str]) -> list[SampleEntry]:
    entries = load_smoke_samples()
    if names:
        entries = [entry for entry in entries if entry.name in names]
    return entries


def _run(args: argparse.Namespace) -> int:
    entries = _load_entries(args.sample)
    sample_count = len(entries)
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
            refs, bootstrap_skips = asyncio.run(list_representative_refs(entries))
            remote = [entry for entry in entries if not isinstance(entry, LocalSample)]
            # The real CLI reopens a remote target only via --profile, so each
            # [[remote_storage]] sample gets a throwaway profile for this run.
            with remote_profiles(ctx, [e for e in remote if isinstance(e, RemoteStorageSample)]) as profiles:
                # Recorded as steps; a bare print() would leave the gap out of index.md.
                for i, reason in enumerate(bootstrap_skips):
                    ctx.skip("commands", f"commands.bootstrap_skip[{i}]", reason)
                main_ref, encrypted_ref = pick_session_refs(refs)
                # main_ref first (global_flags/export_lifecycle run on refs[0]);
                # it can be the same ref as encrypted_ref.
                picked = [r for r in (main_ref, encrypted_ref) if r is not None]
                ctx.data["refs"] = picked if len(picked) < 2 or picked[0] is not picked[1] else picked[:1]
                ctx.data["remote_entries"] = remote
                ctx.data["remote_profiles"] = profiles
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
            resources=resource_usage(children=True),
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
