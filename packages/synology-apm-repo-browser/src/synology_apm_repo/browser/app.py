"""Entry point for the ``synology-apm-repo-browser`` command. Owns the one
long-lived ``Session`` every screen shares, the ``verbose`` flag every
screen reads to decide whether to show internal identifiers, the
``default_sparse`` setting every export starts from (set once at launch,
never offered per-export), and the app-level MVU store that lets a
background export keep running while the user carries on browsing.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from importlib.metadata import version as _pkg_version
from pathlib import Path

from textual.app import App, ComposeResult
from textual.reactive import var
from textual.widgets import Header, Static
from textual.worker import Worker

from synology_apm_repo.browser.core.app.cmd import AppCmd
from synology_apm_repo.browser.core.app.model import AppModel, Job
from synology_apm_repo.browser.core.app.model import JobStatus as JobStatus
from synology_apm_repo.browser.core.app.msg import AppMsg
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.core.keys import JobId, RepoHandle
from synology_apm_repo.browser.keymap import COMMON_BINDINGS, WORKLIST_BINDING
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.browse_effects import BROWSE_REPO_CLOSE_GROUP
from synology_apm_repo.browser.runtime.resources import ResourceTable
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.runtime.unit_effects import UNIT_PROVIDER_CLOSE_GROUP
from synology_apm_repo.browser.screens.browse_screen import BrowseScreen
from synology_apm_repo.browser.worker_drain import drain
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.concurrency import preload_resource_tracker
from synology_apm_repo.sdk.presentation.logging_setup import configure_logging

_CSS_PATH = Path(__file__).with_name("theme.tcss")


class ApmRepoBrowserApp(App[None]):
    """Offline browser/exporter for Synology APV/Object-Storage dedup
    repositories.

    Holds the state shared across every screen: the one ``Session`` (closed
    on exit), which repository is selected (``repo_handle``), ``verbose``
    mode, ``default_sparse`` (session-wide, set once at launch), ``resources``
    (the ``ResourceTable`` every screen's ``ProviderHandle``/``RepoHandle``
    resolves through — a frozen ``core.*`` model can't hold a real closable
    connection directly), and ``store``, the app-level MVU loop for
    background jobs. Screen-specific state stays on the screen itself."""

    TITLE = "APM Repository Browser"
    CSS_PATH = _CSS_PATH
    BINDINGS = [*COMMON_BINDINGS, WORKLIST_BINDING]

    #: Toggled by ``d``. A screen registers ``self.watch(self.app, "verbose",
    #: ..., init=False)`` in its own ``on_mount`` to redraw regardless of
    #: screen-stack position. ``init=False`` here is separate: it stops this
    #: class's own ``watch_verbose`` firing an unwanted notification on launch.
    verbose: var[bool] = var(False, init=False)

    #: A mirror of ``self.store.model.jobs``, synced by the subscription in
    #: ``__init__`` below. A real ``Reactive`` (not read from the store
    #: directly) so screens can ``self.watch(self.app, "jobs", ...)`` to stay
    #: in sync regardless of screen-stack position.
    jobs: var[Mapping[JobId, Job]] = var(dict)

    def __init__(self, *, default_sparse: bool = True) -> None:
        super().__init__()
        self.session = Session()
        self.resources = ResourceTable(self.session)
        #: The selected repository's handle, not the live object —
        #: ``ResourceTable`` is the sole owner; readers dereference through
        #: ``self.resources.repo(...)`` at point of use.
        self.repo_handle: RepoHandle | None = None
        self.default_sparse = default_sparse
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, self._perform)
        self.effects = AppEffects(self, self.store)
        self.store.subscribe(lambda model: model.jobs, self._sync_jobs, init=False)

    @property
    def current_repo(self) -> Repository | None:
        """``repo_handle`` dereferenced through ``resources``."""
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def _sync_jobs(self, jobs: Mapping[JobId, Job]) -> None:
        self.jobs = jobs

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="root-placeholder")

    def on_mount(self) -> None:
        # BrowseScreen is the only root screen, pushed once here; it starts
        # empty and immediately opens ConnectDialog, the only place a
        # source is picked (also reachable later via `c`).
        screen = BrowseScreen()
        self.push_screen(screen)
        screen.action_connect_remote()

    async def on_unmount(self) -> None:
        # Closed first so a racing job worker can't reach a mid-teardown store.
        self.store.close()

        # Cancel each job's workers, then wait them out with a bound: a
        # worker parked in an already-started to_thread() read won't return
        # until that read finishes, and quitting must not hang on it.
        cancelled: list[Worker[None]] = []
        for job in self.store.model.jobs.values():
            cancelled.extend(self.workers.cancel_group(self, job.group))
        await drain(cancelled)

        # Provider/repo-close workers are drained, not cancelled: their
        # cleanup must finish before session.close() below closes the same
        # connections they're still releasing.
        resource_closes = [w for w in self.workers if w.group in (UNIT_PROVIDER_CLOSE_GROUP, BROWSE_REPO_CLOSE_GROUP)]
        await drain(resource_closes)
        await self.session.close()

    def action_quit_app(self) -> None:
        self.exit()

    def action_toggle_verbose(self) -> None:
        self.verbose = not self.verbose

    def watch_verbose(self, verbose: bool) -> None:
        self.set_class(verbose, "verbose")
        self.notify(f"verbose mode {'on' if verbose else 'off'}")

    def action_show_help(self) -> None:
        from synology_apm_repo.browser.screens.help_screen import HelpScreen

        # Captured before HelpScreen becomes active and its own bindings
        # would otherwise be what's shown.
        self.push_screen(HelpScreen(self.screen.active_bindings))

    def action_toggle_worklist(self) -> None:
        from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen

        self.push_screen(WorklistScreen())


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
    """``argv`` is ``None`` in real use; a real list is only passed by a
    test wanting a fixed argument list. Excluded from the coverage gate:
    ``.run()`` opens a real terminal — ``App.run_test()`` is the tested
    path elsewhere in this package."""
    # A TUI can't share its terminal with a dependency's own stderr output
    # (shared call site with cli/main.py::main()); configure_logging()
    # suppresses it, or redirects to SYNOLOGY_APM_REPO_LOG for debugging.
    configure_logging()
    args = _parse_args(argv)
    # Must happen before .run(): once running, Textual redirects sys.stderr
    # to a capture stream whose fileno() isn't a real descriptor, which
    # preload_resource_tracker() requires.
    preload_resource_tracker()
    ApmRepoBrowserApp(default_sparse=not args.no_sparse_export).run()


if __name__ == "__main__":  # pragma: no cover - same reason as main() above
    main()
