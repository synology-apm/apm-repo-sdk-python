"""Entry point for the ``synology-apm-repo-browser`` command and its
``App``."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import ClassVar, override

from textual.app import App, ComposeResult
from textual.binding import BindingType
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
from synology_apm_repo.sdk import Repository, Session
from synology_apm_repo.sdk.export import preload_resource_tracker
from synology_apm_repo.sdk.presentation import configure_logging

_CSS_PATH = Path(__file__).with_name("theme.tcss")


class ApmRepoBrowserApp(App[None]):
    """Offline browser/exporter for Synology APV/Object-Storage dedup
    repositories.

    Holds the state shared across every screen: the one ``Session`` (closed
    on exit), the selected repository (``repo_handle``), ``verbose`` mode,
    ``default_sparse`` (set once at launch, for every export), ``resources``
    (the ``ResourceTable`` every ``ProviderHandle``/``RepoHandle`` resolves
    through), and ``store``, the app-level MVU loop that lets background
    exports outlive the screen that started them."""

    TITLE = "APM Repository Browser"
    CSS_PATH = _CSS_PATH
    BINDINGS: ClassVar[list[BindingType]] = [*COMMON_BINDINGS, WORKLIST_BINDING]

    #: Toggled by ``d``. A screen watches it (``self.watch(self.app,
    #: "verbose", ...)``) to redraw wherever it sits in the screen stack.
    #: ``init=False`` keeps ``watch_verbose`` from notifying on launch.
    verbose: var[bool] = var(False, init=False)

    #: A mirror of ``self.store.model.jobs``, kept as a reactive so screens
    #: can watch it wherever they sit in the screen stack.
    jobs: var[Mapping[JobId, Job]] = var(dict)

    def __init__(self, *, default_sparse: bool = True) -> None:
        super().__init__()
        self.session = Session()
        self.resources = ResourceTable(self.session)
        #: The selected repository's handle; ``resources`` owns the object.
        self.repo_handle: RepoHandle | None = None
        self.default_sparse = default_sparse
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, self._perform)
        self.effects = AppEffects(self, self.store, self.resources.load_gate)
        self.store.subscribe(lambda model: model.jobs, self._sync_jobs, init=False)

    @property
    def current_repo(self) -> Repository | None:
        """``repo_handle`` dereferenced through ``resources``."""
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def _sync_jobs(self, jobs: Mapping[JobId, Job]) -> None:
        self.jobs = jobs

    @override
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="root-placeholder")

    def on_mount(self) -> None:
        # BrowseScreen is the only root screen; it starts empty and opens
        # ConnectDialog, where every source is picked.
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


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - opens a real terminal
    """Parse ``argv`` (``sys.argv`` when ``None``) and run the app."""
    # A TUI can't share its terminal with a dependency's stderr output;
    # configure_logging() silences it, or redirects it to
    # SYNOLOGY_APM_REPO_LOG.
    configure_logging()
    args = _parse_args(argv)
    # Must happen before .run(): once running, Textual redirects sys.stderr
    # to a capture stream whose fileno() isn't a real descriptor, which
    # preload_resource_tracker() requires.
    preload_resource_tracker()
    ApmRepoBrowserApp(default_sparse=not args.no_sparse_export).run()


if __name__ == "__main__":
    main()
