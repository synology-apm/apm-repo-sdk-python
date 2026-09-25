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

    Holds exactly the state shared across every screen: the one ``Session``
    (closed on exit — it owns every connection it opens and is the single
    place responsible for releasing them), which repository is currently
    selected (``repo_handle``), whether verbose mode (``d``) is on, ``default_sparse``
    (every export starts from this session-wide setting, set once at
    launch, not a per-export choice), ``resources`` (the one
    ``ResourceTable`` every screen's own ``Store``-held
    ``ProviderHandle``/``RepoHandle`` resolves through, because a
    ``Repository``/``UnitProvider`` owns a non-daemon-thread ``aiosqlite``
    connection that a frozen ``core.*`` model can't hold without leaking
    threads across repeated version visits), and ``store``, the app-level
    MVU loop every screen dispatches a job-related ``AppMsg`` into
    (``ExportScreen`` on Export/Cancel, ``WorklistScreen``'s own ``x``)
    and can subscribe to for job state. Screen-specific state stays on
    the screen itself, not here."""

    TITLE = "APM Repository Browser"
    CSS_PATH = _CSS_PATH
    BINDINGS = [*COMMON_BINDINGS, WORKLIST_BINDING]

    #: Toggled by ``d``. A screen that has anything verbose-mode-dependent
    #: to redraw registers ``self.watch(self.app, "verbose",
    #: self.refresh_for_verbose_mode, init=False)`` in its own ``on_mount``
    #: (see ``BrowseScreen``/``UnitScreen``) — that watch fires regardless
    #: of whether the registering screen is currently on top of the screen
    #: stack. The reactive's own ``init=False`` is
    #: a separate, unrelated knob (whether *this* class's own
    #: ``watch_verbose`` fires once automatically right after the App
    #: mounts) — left at ``var``'s own default of ``True`` here would
    #: fire an unwanted "verbose mode off" notification on every launch
    #: even though the user never pressed ``d``.
    verbose: var[bool] = var(False, init=False)

    #: A mirror of ``self.store.model.jobs``, kept in sync by the
    #: subscription registered in ``__init__`` below. Exists as a real
    #: reactive (rather than reading ``self.store.model.jobs`` directly)
    #: because ``self.watch(self.app, "jobs", ...)`` — how
    #: ``NavigableScreen``'s own breadcrumb tasks-hint (shared by
    #: ``BrowseScreen``/``UnitScreen``) stays in sync regardless of
    #: screen-stack position, and how ``WorklistScreen`` reacts live —
    #: needs an actual ``Reactive`` descriptor to hook into, not just a
    #: same-named plain attribute (``Store.subscribe`` has no equivalent
    #: "fires regardless of who's currently on top" behavior of its own).
    jobs: var[Mapping[JobId, Job]] = var(dict)

    def __init__(self, *, default_sparse: bool = True) -> None:
        super().__init__()
        self.session = Session()
        self.resources = ResourceTable(self.session)
        #: The currently-selected repository's own handle -- never the live
        #: object itself, since ``ResourceTable`` is the sole owner of the
        #: live ``Repository`` for its whole lifetime. Every
        #: reader dereferences through ``self.resources.repo(...)`` at the
        #: point of use rather than caching the live object, so a repo
        #: released between selection and use naturally reads back as
        #: ``None`` instead of needing a separate, hand-synchronized reset.
        self.repo_handle: RepoHandle | None = None
        self.default_sparse = default_sparse
        self.store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, self._perform)
        self.effects = AppEffects(self, self.store)
        self.store.subscribe(lambda model: model.jobs, self._sync_jobs, init=False)

    @property
    def current_repo(self) -> Repository | None:
        """``repo_handle`` dereferenced through ``resources`` -- the one
        place every reader does this, rather than repeating the same
        ``resources.repo(repo_handle) if repo_handle is not None else
        None`` ternary at each call site."""
        return self.resources.repo(self.repo_handle) if self.repo_handle is not None else None

    def _perform(self, cmd: AppCmd) -> None:
        self.effects.perform(cmd)

    def _sync_jobs(self, jobs: Mapping[JobId, Job]) -> None:
        # A plain assignment, not mutate_reactive(): update() always
        # returns a fresh dict on any real change (never mutates
        # model.jobs in place), so Textual's own reactive `!=` check
        # already fires exactly when the mirror genuinely needs to.
        self.jobs = jobs

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="root-placeholder")

    def on_mount(self) -> None:
        # BrowseScreen is the only root screen -- pushed exactly once, here,
        # and never popped by anything else -- but it starts with nothing to
        # show: picking *what* to browse (a local directory, an S3/Azure
        # Blob Storage-backed repository, or an SMB share) is entirely
        # ConnectDialog's job now -- it's the only place a source is ever
        # picked, and BrowseScreen has no path field of its own -- so it's
        # auto-opened immediately on top, the exact
        # same push ``c``/action_connect_remote triggers later. Esc-ing out
        # of this first dialog without connecting anything just leaves
        # BrowseScreen empty, reopenable any time with ``c``.
        screen = BrowseScreen()
        self.push_screen(screen)
        screen.action_connect_remote()

    async def on_unmount(self) -> None:
        # ``async def`` deliberately: ``Session.close()`` is a coroutine.
        # Textual dispatches handlers through ``textual._callback.invoke()``,
        # which awaits the result when it's awaitable, so an ``async def``
        # handler here is awaited to completion exactly like any other
        # handler.

        # Closed first so a job's own worker racing this shutdown (its
        # CancelledError handler dispatching ExportFinished) can't reach
        # back into a store that's mid-teardown -- matches every screen's
        # own on_unmount, which closes its store before touching workers.
        self.store.close()

        # Textual's own shutdown already cancels every outstanding worker
        # before dispatching Unmount — same mechanism cancel_group relies
        # on below. Cancelling each job's own group explicitly here is
        # kept anyway, so the ordering holds without depending on
        # Textual's internals.
        #
        # Then *waited for*: cancellation only lands at the job's next await,
        # and ``session.close()`` closes the providers those jobs are still
        # reading through — closing one underneath an in-flight read leaks its
        # connection (see ``UnitScreen.on_unmount``). ``drain()`` bounds the
        # wait: a job parked inside an already-started ``to_thread`` read does
        # not come back until that read returns, and quitting must not hang on
        # it. A job that outlives the bound is left to the cancellation it was
        # already handed; only the ordering guarantee is given up, not the
        # cancel. cancel_group's own return value is exactly the workers it
        # actually cancelled, so nothing here needs its own live-Worker
        # bookkeeping.
        # Read from the store directly, not the `jobs` mirror -- close()
        # just above already dropped every subscription, so the mirror
        # stops receiving further updates from this point on; the store's
        # own model is still readable and remains the source of truth.
        cancelled: list[Worker[None]] = []
        for job in self.store.model.jobs.values():
            cancelled.extend(self.workers.cancel_group(self, job.group))
        await drain(cancelled)

        # A provider-close/repo-close worker (a UnitScreen's/BrowseScreen's
        # own, hosted here on the App rather than the screen -- Textual's
        # own ``Widget._on_unmount`` cancels every worker on a node once that
        # node unmounts, regardless of its own group, so a close worker
        # hosted on the screen could be cancelled out from under itself
        # mid-close, exactly the leaked-connection failure mode closing a
        # provider exists to prevent) is never cancelled, only drained: unlike a job,
        # its own cleanup must actually finish, not just stop, before
        # ``session.close()`` below closes the same connections it's still
        # releasing.
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

        # Captured from ``self.screen`` -- the screen that was active when
        # ``?`` was pressed -- before ``HelpScreen`` itself becomes the
        # active screen and its own (much smaller) bindings would otherwise
        # be what gets shown.
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
    """``argv`` is ``None`` in real use (argparse then reads ``sys.argv``
    itself); a real list is only ever passed by a test that wants a
    fixed argument list.

    ``--no-sparse-export`` sets ``ApmRepoBrowserApp.default_sparse`` for the
    whole session; ``ExportScreen`` has no per-export toggle, because
    skipping sparse writes is a rare exception not worth asking about on
    every export.

    Excluded from the coverage gate: ``.run()`` opens a real terminal —
    ``App.run_test()`` (used everywhere else in this package's tests) is
    the tested path; ``_parse_args()`` right above is pure and already
    covered directly by ``test_browser_app.py``."""
    # A TUI cannot share its terminal: anything a dependency writes to
    # stderr lands on top of the rendered screen while the app runs, and
    # after it exits, in the user's shell. Shared with the CLI's own call
    # site (cli/main.py::main()), since both must behave identically here.
    # configure_logging() adds a NullHandler to the root logger so
    # logging's last-resort handler (which would otherwise dump every
    # WARNING and above from a dependency like smbprotocol straight to
    # stderr) never fires -- or redirects everything to a file named by
    # SYNOLOGY_APM_REPO_LOG instead, for debugging a backend.
    configure_logging()
    args = _parse_args(argv)
    # Must happen before .run(): once the app is running, Textual redirects
    # sys.stderr to its own capture stream, whose fileno() returns a
    # sentinel instead of raising. preload_resource_tracker()'s launch code
    # appends sys.stderr.fileno() to the file descriptors it hands the
    # tracker helper with no validation, and crashes the first
    # ProcessPoolExecutor built afterward if that value isn't a real, open
    # descriptor -- so it needs the real stderr, captured here first.
    preload_resource_tracker()
    ApmRepoBrowserApp(default_sparse=not args.no_sparse_export).run()


if __name__ == "__main__":  # pragma: no cover - same reason as main() above
    main()
