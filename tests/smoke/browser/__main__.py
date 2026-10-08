"""Entry point: ``uv run python -m tests.smoke.browser [--group ...]``.

Drives the real ``ApmRepoBrowserApp`` in-process through Textual's
``App.run_test()``, against real sample repositories. The SDK runs
in-process only to pick the refs (``list_representative_refs``/
``pick_session_refs``); every check drives the real screens.

Sessions: one shared by every phase in ``_MAIN_SESSION_PHASES``, one for
``key_dialog``, and one per remote sample for ``remote_connect``.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.sdk.export import preload_resource_tracker

from .._report import make_report_dir, resource_usage, write_index
from .._samples import LocalSample, load_smoke_samples
from .._shared_refs import list_representative_refs, pick_session_refs
from ._context import DOMAINS, SmokeContext
from .phases import (
    _diagnostics_and_verbose,
    _export_folder,
    _export_worklist,
    _help_screen,
    _hex_preview,
    _key_dialog,
    _navigate,
    _remote_connect,
)

_ORDER = (
    "navigate",
    "diagnostics_and_verbose",
    "export_worklist",
    "export_folder",
    "hex_preview",
    "key_dialog",
    "remote_connect",
    "help_screen",
)
_MAIN_SESSION_PHASES = {
    "navigate": _navigate,
    "diagnostics_and_verbose": _diagnostics_and_verbose,
    "export_worklist": _export_worklist,
    "export_folder": _export_folder,
    "hex_preview": _hex_preview,
    "help_screen": _help_screen,
}
#: Domains that connect to a different sample, in their own
#: ``App.run_test()`` session (each started explicitly in ``_run``), so the
#: main session's connected repository stays put.
_OWN_SESSION_PHASES = {"key_dialog", "remote_connect"}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.smoke.browser",
        description="Run the synology-apm-repo-browser smoke test against the sample "
        "repositories configured in tests/smoke/smoke_samples.toml.",
    )
    parser.add_argument("--group", choices=("all", *_ORDER), default="all", help="Run one domain only (default: all)")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    entries = load_smoke_samples()
    sample_count = len(entries)
    report_dir = make_report_dir("browser")
    started_at = datetime.now(UTC)

    ctx = SmokeContext(report_dir)
    try:
        if not sample_count:
            print("[smoke] no samples configured -- see tests/smoke/smoke_samples.toml.example")
            reason = "no samples configured -- see smoke_samples.toml.example"
            for domain in _ORDER:
                ctx.skip(domain, f"{domain}.no_samples_configured", reason)
        else:
            refs, bootstrap_skips = await list_representative_refs(entries)
            # Recorded as steps; a bare print() would leave the gap out of index.md.
            for i, reason in enumerate(bootstrap_skips):
                ctx.skip("navigate", f"navigate.bootstrap_skip[{i}]", reason)

            ctx.data["main_ref"], ctx.data["encrypted_ref"] = pick_session_refs(refs)
            ctx.data["remote_entries"] = [e for e in entries if not isinstance(e, LocalSample)]

            phases = _ORDER if args.group == "all" else (args.group,)
            if any(p not in _OWN_SESSION_PHASES for p in phases):
                app = ApmRepoBrowserApp()
                async with app.run_test() as pilot:
                    await pilot.pause()  # let the initial mount (BrowseScreen -> ConnectDialog) land first
                    for phase in phases:
                        if phase in _OWN_SESSION_PHASES:
                            continue
                        print(f"[smoke] running phase: {phase}")
                        await _MAIN_SESSION_PHASES[phase].run(ctx, app, pilot)
            if "key_dialog" in phases:
                print("[smoke] running phase: key_dialog")
                key_app = ApmRepoBrowserApp()
                async with key_app.run_test() as key_pilot:
                    await key_pilot.pause()
                    await _key_dialog.run(ctx, key_app, key_pilot)
            if "remote_connect" in phases:
                remote_entries = ctx.data["remote_entries"]
                if not remote_entries:
                    ctx.skip(
                        "remote_connect",
                        "remote_connect.no_remote_samples",
                        "no [[profile]]/[[remote_storage]] sample configured",
                    )
                for entry in remote_entries:
                    # One session per sample: a new scan's RescanStarted
                    # replaces the previous source's tree.
                    print(f"[smoke] running phase: remote_connect ({entry.name})")
                    remote_app = ApmRepoBrowserApp()
                    async with remote_app.run_test() as remote_pilot:
                        await remote_pilot.pause()
                        await _remote_connect.run(ctx, remote_app, remote_pilot, entry)
    finally:
        finished_at = datetime.now(UTC)
        write_index(
            report_dir,
            title="Browser smoke test report",
            domains=DOMAINS,
            group=args.group,
            sample_count=sample_count,
            started_at=started_at,
            finished_at=finished_at,
            stats=ctx.stats,
            step_results=ctx.step_results,
            resources=resource_usage(children=False),
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
    # Before any run_test(), as browser/app.py's main() does: Textual swaps
    # sys.stderr for a capture stream with no real fileno(), which
    # preload_resource_tracker() needs.
    preload_resource_tracker()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
