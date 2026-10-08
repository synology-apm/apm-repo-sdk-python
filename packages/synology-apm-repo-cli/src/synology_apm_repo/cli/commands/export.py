"""``synology-apm-repo-cli export <ref> -o FILE`` — export one item to a real file,
or a folder's items to a directory.

The output is written to ``<FILE>.part`` and only renamed to ``FILE`` on
success, so a failure or Ctrl-C never leaves a file named ``FILE`` that
looks complete but isn't.

**A folder REF** exports every item below it to ``DIR/<path below the folder>``,
one file at a time (``plan_tree_export`` decides what is left out). A name
that is not a safe path component, or a path another item already takes, is
skipped and reported, and the command then exits 1. The first failure stops
the run; files already finished stay.

**Ctrl-C**: the export body runs as its own ``asyncio.Task``; the first
SIGINT cancels it for a clean unwind, the second exits immediately
(``os._exit()``). ``--keep-partial`` decides whether the ``.part`` file
survives a cancel or failure.

**Pre-existing destination**: an already-present ``FILE`` is refused
before anything is opened; ``--force`` overrides. For a folder REF every
file the export would replace is checked before the first one is written.
"""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import parse_ref_argument
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.errors import ExitCode, err_console, fail
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption, raw_view
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
from synology_apm_repo.cli.repo_session import cli_session, open_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    EXPORT_FORCE_HELP,
    EXPORT_KEEP_PARTIAL_HELP,
    EXPORT_OUTPUT_HELP,
    EXPORT_SPARSE_HELP,
    REF_HELP_EXPORT,
)
from synology_apm_repo.sdk import NodeFrame
from synology_apm_repo.sdk.export import (
    ExportProgressCallback,
    ExportResult,
    LocalFileSink,
    TreeExport,
    TreeItem,
    plan_tree_export,
    preflight_tree,
    run_export,
    run_tree_export,
)
from synology_apm_repo.sdk.presentation import (
    ExportTracker,
    destination_state,
    folder_destination_problem,
    is_out_of_space,
    pluralize,
    reading_progress_callback,
    safe,
    single_destination_message,
    single_destination_problem,
    size_summary,
)


def _install_sigint_cancel[T](task: asyncio.Task[T]) -> Callable[[], None]:
    """Routes SIGINT into ``task.cancel()``; returns the undo callable.
    The second Ctrl-C exits immediately via ``os._exit()``.

    ``signal.signal()``, not ``loop.add_signal_handler()``, so the force quit
    stays immediate even during a synchronous stretch."""
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
            os._exit(ExitCode.CANCELLED)

    previous_handler = signal.signal(signal.SIGINT, _on_sigint)

    def _restore() -> None:
        signal.signal(signal.SIGINT, previous_handler)

    return _restore


async def _export_tree(
    frame: NodeFrame,
    *,
    output: Path,
    sparse: bool,
    keep_partial: bool,
    force: bool,
    state: CliState,
    active: ExportTracker,
) -> TreeExport:
    """Exports every item below the folder ``frame`` resolved to into the directory ``output``."""
    if (folder_problem := folder_destination_problem(output)) is not None:
        fail(folder_problem)
    plan = await plan_tree_export(frame.provider, frame.node, output)
    # Nothing is written until every destination is known to be usable; --force only replaces files.
    preflight = await asyncio.to_thread(preflight_tree, plan, output, force=force)
    if (problem := preflight.problem) is not None:
        fail(problem.message(force_hint=True))
    active.total = len(plan.items)

    def on_item_start(_index: int, item: TreeItem, sink: LocalFileSink) -> ExportProgressCallback:
        active.item_started(sink, item.path, item.relative)
        return reading_progress_callback(build_progress_meter(state))

    def on_item_done(_item: TreeItem, _result: ExportResult) -> None:
        finish_live_progress(state)
        active.item_finished()

    return await run_tree_export(
        frame.provider,
        plan,
        output,
        preflight=preflight,
        sparse=sparse,
        keep_partial=keep_partial,
        on_item_start=on_item_start,
        on_item_done=on_item_done,
    )


def _report_tree(done: TreeExport, output: Path, state: CliState) -> None:
    """Prints the folder export's summary, which items are incomplete and what it skipped; exits 1
    when anything was skipped."""
    if not state.quiet:
        files = f"{len(done.exported)} {pluralize(len(done.exported), 'file')}"
        console.print(f"[green]exported[/green] {files} to {output} ({size_summary(done.totals)})")
    for incomplete in done.incomplete:
        err_console.print(f"[yellow]incomplete[/yellow] {safe(incomplete.relative)}: {safe(incomplete.reason)}")
    for item in done.skipped:
        err_console.print(f"[yellow]skipped[/yellow] {safe(item.relative)}: {item.reason.description}")
    if done.skipped:
        count = len(done.skipped)
        fail(f"{count} {pluralize(count, 'item')} {pluralize(count, 'was', 'were')} not exported")


@typer_async
async def export(
    ctx: typer.Context,
    ref: Annotated[str, typer.Argument(help=REF_HELP_EXPORT)],
    output: Annotated[Path, typer.Option("-o", "--output", help=EXPORT_OUTPUT_HELP)],
    key: KeyOption = None,
    sparse: Annotated[bool, typer.Option("--sparse/--no-sparse", help=EXPORT_SPARSE_HELP)] = True,
    object_db_id: ObjectDbIdOption = None,
    keep_partial: Annotated[bool, typer.Option("--keep-partial", help=EXPORT_KEEP_PARTIAL_HELP)] = False,
    force: Annotated[bool, typer.Option("--force", help=EXPORT_FORCE_HELP)] = False,
    profile: ProfileOption = None,
) -> None:
    """Export REF's content to OUTPUT: one item to a file, or a folder's items to a directory
    (each at its path below the folder; an item whose name is not a safe file name here is
    skipped and reported, and the command then exits 1)."""
    state: CliState = ctx.obj
    parsed = parse_ref_argument(ref)
    if destination_state(output) == "file" and not force:
        fail(
            f"{output} already exists — pass --force to overwrite it "
            "(only for a single item; a folder REF needs a directory)"
        )
    active = ExportTracker(output)
    on_export_progress = reading_progress_callback(build_progress_meter(state))
    degraded: str | None = None
    async with cli_session(state, error_prefix=lambda: f"{active.label}: " if active.label else "") as cli:

        async def _do_export() -> ExportResult | TreeExport:
            """Everything a Ctrl-C should interrupt, discovery included."""
            nonlocal degraded
            repo = await open_repo(cli, parsed.fs_path, key, profile=profile)
            frame = await repo.resolve(parsed.node_ref, raw=raw_view(object_db_id))
            if frame.node.is_leaf:
                if (problem := single_destination_problem(output, force=True)) is not None:
                    fail(single_destination_message(output, problem))
                unit = await frame.unit()
                degraded = unit.degraded
                active.sink = sink = LocalFileSink(output, staged=True, keep_partial=keep_partial)
                return await run_export(unit.content, sink, sparse=sparse, progress=on_export_progress)
            return await _export_tree(
                frame,
                output=output,
                sparse=sparse,
                keep_partial=keep_partial,
                force=force,
                state=state,
                active=active,
            )

        task = asyncio.create_task(_do_export())
        restore_sigint = _install_sigint_cancel(task)
        try:
            result = await task
        except asyncio.CancelledError:
            finish_live_progress(state)  # before printing, so the message doesn't land on the progress line
            # run_export already aborted the sink; abort() is idempotent and repeats what it left.
            leftover = await active.leftover(keep_partial_hint=True)
            console.print(f"[yellow]cancelled — {leftover}[/yellow]" if leftover else "[yellow]cancelled[/yellow]")
            if (files := active.files_note()) is not None:
                console.print(f"[yellow]{files}[/yellow]")
            raise typer.Exit(code=ExitCode.CANCELLED) from None
        except OSError as exc:
            if not is_out_of_space(exc):
                raise  # any other OSError stays an unexpected failure
            finish_live_progress(state)
            fail(await active.out_of_space(exc, keep_partial_hint=True), cause=exc)
        finally:
            restore_sigint()

    if isinstance(result, TreeExport):
        _report_tree(result, output, state)
        return
    if not state.quiet:
        console.print(f"[green]exported[/green] {output} ({size_summary(result)})")
    if degraded is not None:
        err_console.print(f"[yellow]incomplete:[/yellow] {safe(degraded)}")
