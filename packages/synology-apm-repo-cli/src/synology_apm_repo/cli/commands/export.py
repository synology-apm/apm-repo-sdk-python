"""``synology-apm-repo-cli export <ref> -o FILE`` — export one item to a real file.

The output is written to ``<FILE>.part`` and only renamed to ``FILE`` on
success: a truncated-but-plausible-looking output file is a safety-level
problem for a restore tool, not just a UX nicety, so a failure or Ctrl-C
leaves the ``.part`` file rather than a file named ``FILE`` that looks
complete but isn't.

Progress: ``ContentSource.export_to()``'s own progress callback is
``async (done: int, total: int) -> None``, with a *planned*-bytes total
(the extents walk already knows how much real data there is before
writing a single byte) — adapted via ``reading_progress_callback`` into a
``Progress`` snapshot fed to a ``ProgressMeter``; neither the CLI nor the
TUI computes rate/ETA itself.

**Ctrl-C**: the export body runs as its own ``asyncio.Task``, cancelled by
SIGINT — the first press cancels that Task for a clean unwind, a second
press exits immediately (``os._exit()``) since a repeated Ctrl-C means the
user has made clear they don't want to wait for anything; see the
``except asyncio.CancelledError`` branch below for what "cancelled"
actually does to the partial output (``--keep-partial`` controls whether
it's kept or deleted).

**Pre-existing destination**: an already-present ``FILE`` is refused,
via ``destination_available()``, before anything is opened; ``--force``
overrides.
"""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar, cast

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import parse_ref_argument, resolve_restorable
from synology_apm_repo.cli.errors import err_console, fail, fail_from_apm_error
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.profile_store import resolve_profile_store
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
from synology_apm_repo.cli.repo_session import open_single_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    EXPORT_FORCE_HELP,
    EXPORT_KEEP_PARTIAL_HELP,
    EXPORT_OUTPUT_HELP,
    EXPORT_SPARSE_HELP,
    REF_HELP_SINGLE_ITEM,
)
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk.api import ExportResult, Session
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.export_target import (
    destination_available,
    finalize_export,
    part_path_for,
    resolve_cancelled_partial,
)
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.progress import reading_progress_callback

console = Console()

_T = TypeVar("_T")


def handle_cancelled(part_path: Path, *, keep_partial: bool) -> str:
    """What to do with ``part_path`` once a cancelled export unwinds, and
    the message to show for it — the CLI's own phrasing of
    ``resolve_cancelled_partial()``'s shared decision. Split out from
    ``export`` itself so it (unlike the actual SIGINT plumbing around it)
    is unit-testable without a real signal."""
    outcome = resolve_cancelled_partial(part_path, keep_partial=keep_partial)
    if not outcome.ever_written:
        # A cloud-sync placeholder's export_to() defers creating part_path
        # until its first block reads successfully (unlike a dedup-backed
        # export, which creates it immediately), so a cancellation that
        # lands before that never writes it at all.
        return "cancelled — no output file was ever written"
    if outcome.kept:
        return f"cancelled — partial file kept as {part_path.name}"
    return "cancelled — partial file removed (use --keep-partial to keep it)"


def _install_sigint_cancel(task: asyncio.Task[_T]) -> Callable[[], None]:
    """Routes SIGINT into ``task.cancel()``; returns the undo callable.
    First Ctrl-C cancels the export Task — a normal, clean unwind into
    the ``except`` branch in ``export``. Second Ctrl-C terminates
    immediately instead, since the user has made clear they don't want
    to wait for anything.

    ``signal.signal()``, not ``loop.add_signal_handler()``: the latter
    delivers the handler as an ordinary loop callback, so the
    second-press force quit would only fire once the loop next got to
    run one — never while it's inside a long synchronous stretch,
    exactly when "Ctrl-C twice means stop now" matters most. A
    ``signal.signal()`` handler runs between bytecodes instead, so
    ``os._exit(130)`` stays immediate.

    ``Task.cancel()`` is the one thing still deferred to the loop, since
    it can't run inside a signal handler: it goes through
    ``loop.call_soon_threadsafe()``, asyncio's own thread-safe entry
    point and the same self-pipe wakeup ``add_signal_handler()`` uses
    internally."""
    loop = asyncio.get_running_loop()
    sigint_count = 0

    def _on_sigint(signum: int, frame: object) -> None:
        nonlocal sigint_count
        sigint_count += 1
        if sigint_count == 1:
            loop.call_soon_threadsafe(task.cancel)
            err_console.print("\n[yellow]cancelling... (press Ctrl-C again to force quit)[/yellow]")
        else:  # pragma: no cover - os._exit() below would kill pytest itself if this branch actually ran
            err_console.print("\n[red]force quit[/red]")
            # Bypasses all cleanup (atexit, finally blocks) — that's the point.
            os._exit(130)

    previous_handler = signal.signal(signal.SIGINT, _on_sigint)

    def _restore() -> None:
        signal.signal(signal.SIGINT, previous_handler)

    return _restore


@typer_async
async def export(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help=REF_HELP_SINGLE_ITEM),
    output: Path = typer.Option(..., "-o", "--output", help=EXPORT_OUTPUT_HELP),
    key: KeyOption = None,
    sparse: bool = typer.Option(True, "--sparse/--no-sparse", help=EXPORT_SPARSE_HELP),
    object_db_id: ObjectDbIdOption = None,
    keep_partial: bool = typer.Option(False, "--keep-partial", help=EXPORT_KEEP_PARTIAL_HELP),
    force: bool = typer.Option(False, "--force", help=EXPORT_FORCE_HELP),
    profile: ProfileOption = None,
) -> None:
    """Export REF's content to OUTPUT."""
    state: CliState = ctx.obj
    parsed = parse_ref_argument(ref)
    # A separate concern from the .part-rename safety in the try/finally
    # below, which only guards a crash/cancel *during* the write: this
    # refuses an already-present OUTPUT before anything is even opened.
    if not destination_available(output, force=force):
        fail(f"{output} already exists — pass --force to overwrite it")
    session = Session()
    part_path = part_path_for(output)
    discover_meter = build_progress_meter(state)
    export_meter = build_progress_meter(state)
    trace = build_trace_callback(state)
    on_export_progress = reading_progress_callback(export_meter)

    async def _do_export() -> ExportResult:
        """Everything a Ctrl-C should be able to interrupt, in one
        Task — discovery included, so a first press during a slow
        repository scan cancels there too instead of only taking
        effect once the export itself starts."""
        store = await resolve_profile_store(profile) if profile is not None else None
        repo = await open_single_repo(
            session, parsed.fs_path, key, store=store, progress=discover_meter.update, trace=trace
        )
        resolved = await resolve_restorable(
            repo,
            parsed.node_ref,
            ref=ref,
            hint="export a specific item inside it instead",
            object_db_id=object_db_id,
        )
        content = resolved.open()
        output.parent.mkdir(parents=True, exist_ok=True)
        # ContentSource.export_to()'s Protocol return type is deliberately
        # ``object`` (a future ContentSource implementation isn't
        # required to return ExportResult specifically), but every real
        # implementation today does; this is a display-only cast, not a
        # behavioral assumption. No concurrency kwargs to forward here —
        # export_to()'s own real parallelism (a multiprocess dispatch,
        # unconditional whenever the repository's store supports it) isn't
        # a CLI-facing knob.
        return cast(
            ExportResult,
            await content.export_to(part_path, sparse=sparse, progress=on_export_progress),
        )

    task = asyncio.create_task(_do_export())
    restore_sigint = _install_sigint_cancel(task)
    try:
        result = await task
        finalize_export(part_path, output)
    except asyncio.CancelledError:
        # This is the first-Ctrl-C path (a second press exits immediately
        # via os._exit() instead of reaching here). Cancellation is prompt
        # on the single-process (fallback) path, since the read path awaits
        # per extent/chunk; on the real, unconditional multiprocess path
        # (each claimed bucket group's decode+write dispatched to its own
        # process) it widens slightly instead — an already-running worker
        # finishes its one in-flight bucket group's decode/write rather
        # than being torn down mid-write, so this lands roughly one bucket
        # group's own decode time after the signal, not instantly. This
        # command manages its own Session/progress lifecycle instead of
        # going through opened_repo (whose shared skeleton doesn't fit
        # export's own SIGINT/Task cancellation), so it clears its own
        # leftover progress line before each of its own exit prints,
        # rather than getting it for free the way doctor/ls/tree/cat/key/
        # verify do.
        finish_live_progress(state)
        console.print(f"[yellow]{handle_cancelled(part_path, keep_partial=keep_partial)}[/yellow]")
        return
    except ApmRepoError as exc:
        fail_from_apm_error(exc, state)
    finally:
        # Covers the success path (this line only clears something once,
        # even though the two branches above already called it once each
        # for their own timing needs) and, unlike those two, also a
        # non-ApmRepoError/CancelledError
        # exception this function doesn't otherwise catch at all (a bug,
        # not an expected failure).
        finish_live_progress(state)
        restore_sigint()
        await session.close()

    if not state.quiet:
        console.print(
            f"[green]exported[/green] {output} ({format_bytes(result.bytes_written)} written, "
            f"{format_bytes(result.logical_size)} logical, {format_bytes(result.holes)} holes, "
            f"{format_bytes(result.zeros)} zero-fill)"
        )
