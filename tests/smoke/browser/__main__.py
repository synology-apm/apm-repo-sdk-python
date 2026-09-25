"""Entry point: ``uv run python -m tests.smoke.browser [--group ...]``.

Drives the real ``ApmRepoBrowserApp`` in-process, via Textual's own
``App.run_test()`` (the same harness every ``tests/unit/browser``/
``tests/integration/browser`` test uses) -- just pointed at real, on-disk
sample repositories instead of a fake ``ContentSource``/replayed fixture.
Ref selection (bootstrap) uses the SDK in-process, once, purely to pick a
real, representative ``NodeRef`` per sample/workload-type (see
``.._shared_refs.list_representative_refs``) -- every actual check after
that drives the real screens/keybindings.

Three separate ``App.run_test()`` sessions: one for every phase but
``key_dialog``/``remote_connect`` (all share one connected, real repository),
one just for ``key_dialog`` (a fresh connect to a different, encrypted
sample), and one just for ``remote_connect`` (a fresh connect per
configured ``[[profile]]``/``[[remote_storage]]`` sample) -- kept apart so
none of these fresh-connect flows disturbs the main session's
already-connected state.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.sdk import Session, TargetType
from synology_apm_repo.sdk.concurrency import preload_resource_tracker

from .._report import make_report_dir, write_index
from .._samples import LocalSample, SampleEntry, load_smoke_samples
from .._shared_refs import RepresentativeRef, list_representative_refs
from ._context import DOMAINS, SmokeContext
from .phases import (
    _diagnostics_and_verbose,
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
    "hex_preview",
    "key_dialog",
    "remote_connect",
    "help_screen",
)
_MAIN_SESSION_PHASES = {
    "navigate": _navigate,
    "diagnostics_and_verbose": _diagnostics_and_verbose,
    "export_worklist": _export_worklist,
    "hex_preview": _hex_preview,
    "help_screen": _help_screen,
}
#: Domains that drive their own, separate ``App.run_test()`` session
#: instead of sharing the main one -- each connects to a different sample
#: (key_dialog's own encrypted one, remote_connect's per-sample profile/
#: remote_storage target), and a fresh connect in the shared main session
#: would disturb its own already-connected state.
_OWN_SESSION_PHASES = {"key_dialog", "remote_connect"}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tests.smoke.browser",
        description="Run the synology-apm-repo-browser smoke test against the sample "
        "repositories configured in tests/smoke/smoke_samples.toml.",
    )
    parser.add_argument("--group", choices=("all", *_ORDER), default="all", help="Run one domain only (default: all)")
    return parser.parse_args(argv)


async def _bootstrap(entries: list[SampleEntry]) -> tuple[list[RepresentativeRef], list[str]]:
    if not entries:
        return [], []
    async with Session() as session:
        return await list_representative_refs(session, entries)


async def _run(args: argparse.Namespace) -> int:
    entries = load_smoke_samples()
    refs, bootstrap_skips = await _bootstrap(entries)
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
            # Recorded here, not inside list_representative_refs() itself
            # (this SmokeContext doesn't exist yet at that point) -- a bare
            # print() alone would leave a real browser-specific coverage
            # gap invisible in index.md, visible only in whatever terminal
            # happened to run the tool.
            for i, reason in enumerate(bootstrap_skips):
                ctx.skip("navigate", f"navigate.bootstrap_skip[{i}]", reason)

            # main_ref/encrypted_ref are picked from local refs only:
            # every domain but remote_connect drives ConnectDialog via
            # connect_local(), which takes a filesystem path -- a
            # profile/remote_storage ref has no such path. remote_connect
            # covers profile/remote_storage samples on its own, below.
            local_refs = [r for r in refs if r.local]

            # Prefer an unencrypted, non-SaaS ref for the main session --
            # unlocking a key is key_dialog.py's own, deliberate job
            # (unchanged); avoiding a real M365/GW leaf specifically is
            # because UnitScreen.refresh_for_verbose_mode() re-loads the
            # whole tree -- discarding cursor position -- only for that
            # version kind, which would silently break every later phase's
            # "cursor stays on the leaf navigate landed on" assumption the
            # moment diagnostics_and_verbose (or hex_preview's own
            # on-demand toggle) flips verbose mode. Falls back to a SaaS
            # ref only when the configured samples have nothing else.
            unencrypted = [r for r in local_refs if not r.key]
            ctx.data["main_ref"] = (
                next((r for r in unencrypted if r.version.target_type not in (TargetType.M365, TargetType.GW)), None)
                or (unencrypted[0] if unencrypted else None)
                or (local_refs[0] if local_refs else None)
            )
            ctx.data["encrypted_ref"] = next((r for r in local_refs if r.key), None)
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
                    # One fresh session per remote sample, not one shared
                    # loop -- BrowseScreen._reset_for_new_scan clears
                    # _repos/the tree on every new scan, so connecting a
                    # second source in the same session replaces the
                    # first's tree instead of adding to it.
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
    # Must happen before any ApmRepoBrowserApp.run_test() session: Textual
    # redirects sys.stderr to its own capture stream for the session's
    # duration, whose fileno() returns a sentinel rather than a real, open
    # descriptor that preload_resource_tracker() needs. The real CLI/TUI
    # entry point (browser/app.py's main()) does this before its own
    # .run() for the same reason.
    preload_resource_tracker()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
