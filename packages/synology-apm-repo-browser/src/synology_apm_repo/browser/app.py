"""Entry point for the ``synology-apm-repo-browser`` command. Owns the one
long-lived ``Session`` every screen shares, the ``verbose`` flag every
screen reads to decide whether to show internal identifiers, the
``default_sparse`` setting every export starts from (see ``main``'s own
docstring), and the background job registry that lets an export keep
running while the user carries on browsing.
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
from collections.abc import Sequence
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Literal

from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Static
from textual.worker import Worker

from synology_apm_repo.browser.keymap import COMMON_BINDINGS, WORKLIST_BINDING
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.strings import EXPORT_NOTIFY_TITLE
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.concurrency import preload_resource_tracker
from synology_apm_repo.sdk.presentation.markup import safe

_CSS_PATH = Path(__file__).with_name("theme.tcss")


class JobStatus(enum.StrEnum):
    """A ``BackgroundJob``'s own lifecycle — ``RUNNING`` until something
    (the export screen's Esc/cancel button, or ``WorklistScreen``'s ``x``)
    requests cancellation, never reversed back to ``RUNNING`` afterward.
    A ``StrEnum`` so status bar/worklist text built from it (e.g.
    ``f"{label} ({job.status})"``) keeps reading as the plain
    "running"/"cancelling" text it always has."""

    RUNNING = "running"
    CANCELLING = "cancelling"


@dataclasses.dataclass(frozen=True)
class BackgroundJob:
    """One backgroundable long-running operation, shown in the status bar
    and the worklist — currently only ``ExportScreen`` creates these.

    ``worker`` is the actual Textual ``Worker`` doing the work: an async
    (non-threaded) worker, i.e. a plain ``asyncio.Task`` on this App's own
    event loop.

    Cancelling a job cancels that task — the async SDK accepts no
    ``cancel=`` parameter of its own, so this is the only cancellation
    mechanism there is — and ``CancelledError`` is delivered at the SDK
    call's next ``await``, with its own ``try/finally`` blocks (e.g.
    ``export_to``'s partial-file close) still running on the way out. Same
    mechanism as pressing Esc while watching the export, just reachable
    after backgrounding.

    Frozen: every field but ``id``/``label`` changes over a job's
    lifetime (``worker`` once attached, ``done``/``total`` on every
    progress tick, ``status`` on cancellation) — ``ApmRepoBrowserApp``'s
    own ``attach_worker``/``update_job``/``mark_job_cancelling`` are the
    only places that replace an entry in its ``jobs`` registry, so every
    reader (the status bar, ``WorklistScreen``) always sees one
    consistent instance per id rather than racing a mutation."""

    id: int
    label: str
    worker: Worker[None] | None = None
    done: int = 0
    total: int | None = None
    status: JobStatus = JobStatus.RUNNING

    def cancel(self) -> None:
        """Cancel the underlying async worker, if one is attached yet."""
        if self.worker is not None:
            self.worker.cancel()

    @property
    def percent(self) -> int | None:
        """Whole-percent completion, or ``None`` when ``total`` isn't
        known (falsy) yet — the same guard the status bar and
        ``WorklistScreen`` both need before dividing by it."""
        if not self.total:
            return None
        return int(100 * self.done / self.total)


class ApmRepoBrowserApp(App[None]):
    """Offline browser/exporter for Synology APV/Object-Storage dedup
    repositories.

    Holds exactly the state shared across every screen: the one ``Session``
    (closed on exit — it owns every connection it opens and is the single
    place responsible for releasing them), which ``Repository`` is currently
    selected, whether verbose mode (``d``) is on, and ``default_sparse``
    (every export starts from this session-wide setting, not a per-export
    choice — see ``main``). Screen-specific state stays on the screen itself,
    not here."""

    TITLE = "APM Repository Browser"
    CSS_PATH = _CSS_PATH
    BINDINGS = [*COMMON_BINDINGS, WORKLIST_BINDING]

    def __init__(self, *, default_sparse: bool = True) -> None:
        super().__init__()
        self.session = Session()
        self.repo: Repository | None = None
        self.verbose: bool = False
        self.default_sparse = default_sparse
        self.jobs: dict[int, BackgroundJob] = {}
        self._next_job_id = 1

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="root-placeholder")
        yield Static("", id="job-status-bar")  # empty (and so invisible) whenever self.jobs is empty
        yield Footer()

    def on_mount(self) -> None:
        # BrowseScreen is the only root screen — pushed exactly once,
        # never popped (see its own action_go_back's comment) — but it
        # starts with nothing to show: picking *what* to browse is
        # entirely ConnectDialog's job now (see that module's own
        # docstring), so it's auto-opened immediately on top, the exact
        # same push ``c``/action_connect_remote triggers later. Esc-ing out
        # of this first dialog without connecting anything just leaves
        # BrowseScreen empty, reopenable any time with ``c``.
        screen = BrowseScreen()
        self.push_screen(screen)
        screen.action_connect_remote()

    async def on_unmount(self) -> None:
        # ``async def`` deliberately: ``Session.close()`` is a coroutine —
        # see keymap.py's module docstring for why Textual allows an
        # ``async def`` handler here.

        # Textual's own shutdown already cancels every outstanding worker
        # before dispatching Unmount — same mechanism as BackgroundJob.worker's
        # own docstring. Cancelling each job explicitly here is kept anyway:
        # it holds the "stop the export within 200ms, before
        # ``session.close()`` can race with it" guarantee without
        # depending on Textual's internal shutdown ordering.
        for job in self.jobs.values():
            job.cancel()
        await self.session.close()

    def action_quit_app(self) -> None:
        self.exit()

    def action_toggle_verbose(self) -> None:
        self.verbose = not self.verbose
        self.set_class(self.verbose, "verbose")
        self.notify(f"verbose mode {'on' if self.verbose else 'off'}")
        # Duck-typed hook: a screen may hold already-rendered labels that
        # embed verbose-mode-dependent text (BrowseScreen's own repository
        # labels, uuid/layout appended only when verbose), which would
        # otherwise stay stale on screen until the next unrelated
        # re-render. Screens with nothing verbose-dependent to redraw
        # simply don't define this method.
        refresh = getattr(self.screen, "refresh_for_verbose_mode", None)
        if callable(refresh):
            refresh()

    def action_show_help(self) -> None:
        from synology_apm_repo.browser.screens.help_screen import HelpScreen

        # Captured from ``self.screen`` -- the screen that was active when
        # ``?`` was pressed -- before ``HelpScreen`` itself becomes the
        # active screen and its own (much smaller) bindings would otherwise
        # be what gets shown.
        self.push_screen(HelpScreen(self.screen.active_bindings))

    def action_toggle_worklist(self) -> None:
        from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen

        self.push_screen(WorklistScreen())

    # -- background job registry (backgroundable exports) ---------------

    def start_job(self, label: str) -> BackgroundJob:
        # ``worker`` is None here — attached by the caller after this call
        # returns (see ExportScreen._start) — both happen synchronously, so
        # a job is never observably cancellable-but-unattached.
        job = BackgroundJob(id=self._next_job_id, label=label)
        self._next_job_id += 1
        self.jobs[job.id] = job
        self._refresh_job_status_bar()
        return job

    def attach_worker(self, job_id: int, worker: Worker[None]) -> BackgroundJob:
        """Record ``worker`` as ``job_id``'s own cancellable task, once
        started (``ExportScreen._start``, right after ``app.run_worker``)
        — returns the updated instance so the caller can rebind its own
        reference too, since ``BackgroundJob`` is frozen."""
        job = dataclasses.replace(self.jobs[job_id], worker=worker)
        self.jobs[job_id] = job
        return job

    def mark_job_cancelling(self, job_id: int) -> None:
        """Record that ``job_id``'s own ``cancel()`` was already called
        and it's now waiting for its worker to actually stop —
        ``ExportScreen``'s own cancel action and ``WorklistScreen``'s
        ``x`` both call this right after ``BackgroundJob.cancel()``."""
        job = self.jobs.get(job_id)
        if job is not None:
            self.jobs[job_id] = dataclasses.replace(job, status=JobStatus.CANCELLING)

    def update_job(self, job_id: int, done: int, total: int | None) -> None:
        job = self.jobs.get(job_id)
        if job is None:  # already finished/removed — a late progress tick, not an error
            return
        self.jobs[job_id] = dataclasses.replace(job, done=done, total=total)
        self._refresh_job_status_bar()

    def finish_job(
        self, job_id: int, message: str, *, severity: Literal["information", "warning", "error"] = "information"
    ) -> None:
        self.jobs.pop(job_id, None)
        self._refresh_job_status_bar()
        self.notify(message, severity=severity, title=EXPORT_NOTIFY_TITLE)

    def _refresh_job_status_bar(self) -> None:
        bar = self.query_one("#job-status-bar", Static)
        bar.set_class(bool(self.jobs), "has-jobs")
        if not self.jobs:
            bar.update("")
            return
        # job.label embeds a real unit name (filename/subject) — escaped
        # before reaching this Static, per sdk/presentation/markup.py's docstring.
        parts = []
        for job in self.jobs.values():
            label = safe(job.label)
            if job.percent is not None:
                parts.append(f"{label} {job.percent}% ({job.status})")
            else:
                parts.append(f"{label} ({job.status})")
        suffix = " · press t for tasks" if len(self.jobs) > 1 else ""
        bar.update(" | ".join(parts) + suffix)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="synology-apm-repo-browser",
        description="Offline browser/exporter for Synology APV/Object-Storage dedup repositories.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"synology-apm-repo-browser {_pkg_version('synology-apm-repo-browser')}",
    )
    parser.add_argument(
        "--no-sparse-export",
        action="store_true",
        help="Export writes sparse files by default (skips zero/hole regions). "
        "ExportScreen has no per-export toggle — this is the one place to turn that off.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - opens a real terminal, see below
    """``argv`` is ``None`` in real use (argparse then reads ``sys.argv``
    itself); a real list is only ever passed by a test that wants a
    fixed argument list.

    ``--no-sparse-export`` sets ``ApmRepoBrowserApp.default_sparse`` for the
    whole session — see ``ExportScreen``'s own module docstring for why
    there's no per-export toggle instead.

    Excluded from the coverage gate: ``.run()`` opens a real terminal —
    ``App.run_test()`` (used everywhere else in this package's tests) is
    the tested path; ``_parse_args()`` right above is pure and already
    covered directly by ``test_browser_app.py``."""
    args = _parse_args(argv)
    # Must happen before .run(): once the app is running, Textual redirects
    # sys.stderr to its own capture stream for the whole session, and
    # preload_resource_tracker() needs the real one — see its own docstring.
    preload_resource_tracker()
    ApmRepoBrowserApp(default_sparse=not args.no_sparse_export).run()


if __name__ == "__main__":  # pragma: no cover - same reason as main() above
    main()
