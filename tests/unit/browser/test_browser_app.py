"""Unit tests for ``browser.app``'s ``--no-sparse-export``
launch-time argument parsing and
``ApmRepoBrowserApp.default_sparse`` — the one place sparse output is
chosen, since ``ExportScreen`` reads this attribute rather than offering
its own per-export toggle.
No ``Pilot``/``run_test()`` needed for either:
``_parse_args()`` is a pure function, and constructing
``ApmRepoBrowserApp()`` itself does no I/O and needs no running event
loop (``Session()``'s own construction is synchronous, and ``App.__init__()``
just sets attributes).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from textual.screen import Screen

from synology_apm_repo.browser import app as app_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp, _parse_args, main
from synology_apm_repo.browser.core.app.msg import ExportProgressed, StartExport
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.screens.help_screen import HelpScreen
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


def test_parse_args_defaults_to_sparse_export_on() -> None:
    args = _parse_args([])
    assert args.no_sparse_export is False


def test_parse_args_no_sparse_export_turns_it_off() -> None:
    args = _parse_args(["--no-sparse-export"])
    assert args.no_sparse_export is True


def test_app_default_sparse_defaults_to_true() -> None:
    app = ApmRepoBrowserApp()
    assert app.default_sparse is True


def test_app_accepts_an_explicit_default_sparse() -> None:
    app = ApmRepoBrowserApp(default_sparse=False)
    assert app.default_sparse is False


def test_main_configures_logging_then_preloads_then_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    # main() itself is excluded from the coverage gate (it opens a real
    # terminal via .run()) -- this only proves the ordering that matters:
    # configure_logging() first (shared mechanism, proven once in
    # test_presentation_logging_setup.py, not here), then
    # preload_resource_tracker() must run before .run() hands sys.stderr
    # over to Textual's own capture, or the tracker's later fd
    # validation crashes the first ProcessPoolExecutor built afterward.
    calls: list[str] = []
    monkeypatch.setattr(app_module, "configure_logging", lambda: calls.append("configure_logging"))
    monkeypatch.setattr(app_module, "preload_resource_tracker", lambda: calls.append("preload"))
    monkeypatch.setattr(ApmRepoBrowserApp, "run", lambda self, *a, **kw: calls.append("run"))

    main([])

    assert calls == ["configure_logging", "preload", "run"]


async def test_action_show_help_pushes_the_help_screen() -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_show_help()
        await pilot.pause()
        assert isinstance(app.screen, HelpScreen)


async def test_action_toggle_worklist_pushes_the_worklist_screen() -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_toggle_worklist()
        await pilot.pause()
        assert isinstance(app.screen, WorklistScreen)


async def test_common_bindings_still_reachable_while_the_worklist_dialog_is_open() -> None:
    """``WorklistScreen`` keeps ``COMMON_BINDINGS`` in its own
    ``BINDINGS`` (matching ``ExportScreen``'s identical-category modal)
    specifically so ``d``/``?`` (and ``q``) still work while it's open --
    a ``ModalScreen`` without a matching binding of its own can't reach
    the App's, so dropping ``COMMON_BINDINGS`` here would silently break
    these while browsing the background-jobs list."""
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_toggle_worklist()
        await pilot.pause()
        assert isinstance(app.screen, WorklistScreen)

        await pilot.press("d")
        await pilot.pause()
        assert app.verbose is True

        await pilot.press("question_mark")
        await pilot.pause()
        assert isinstance(app.screen, HelpScreen)


async def test_action_quit_app_calls_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    # exit() itself is Textual's own real shutdown mechanism -- recorded
    # here via monkeypatch rather than actually invoked, to avoid racing
    # this same test's own run_test() teardown.
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        calls: list[bool] = []
        monkeypatch.setattr(app, "exit", lambda: calls.append(True))
        app.action_quit_app()
        assert calls == [True]


async def test_action_toggle_verbose_flips_the_flag_and_css_class(monkeypatch: pytest.MonkeyPatch) -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.verbose is False
        assert app.has_class("verbose") is False

        app.action_toggle_verbose()
        await pilot.pause()
        assert app.verbose is True
        assert app.has_class("verbose") is True

        app.action_toggle_verbose()
        await pilot.pause()
        assert app.verbose is False
        assert app.has_class("verbose") is False


async def test_action_toggle_verbose_notifies_every_registered_watcher() -> None:
    """``action_toggle_verbose`` no longer reaches into ``self.screen``
    directly at all -- any screen that cares registers its own
    ``self.watch(self.app, "verbose", ...)`` (see ``BrowseScreen``/
    ``UnitScreen``'s own ``on_mount``), and every registered watch fires
    on toggle regardless of whether that screen is currently on top of
    the screen stack -- the actual bug this mechanism replaced the old
    ``getattr(self.screen, "refresh_for_verbose_mode", None)`` hook to
    fix: the old hook only ever reached whichever screen happened to be
    current at the moment ``d`` was pressed, silently missing a covered
    one."""

    class _FakeScreen(Screen[None]):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[bool] = []

        def refresh_for_verbose_mode(self) -> None:
            self.calls.append(True)

        def on_mount(self) -> None:
            self.watch(self.app, "verbose", self.refresh_for_verbose_mode, init=False)

    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        covered = _FakeScreen()
        app.push_screen(covered)
        await pilot.pause()
        on_top = _FakeScreen()
        app.push_screen(on_top)
        await pilot.pause()

        app.action_toggle_verbose()
        await pilot.pause()

        assert covered.calls == [True]
        assert on_top.calls == [True]


async def test_export_progressed_for_an_unknown_job_id_is_a_no_op() -> None:
    """The app-level wiring end-to-end: dispatching through the real
    ``app.store`` for a job id nothing started must not raise or add an
    entry -- ``core/app/update.py``'s own dedicated tests already cover
    the pure logic; this proves the real ``Store``/mirror plumbing
    reaches the same no-op."""
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.store.dispatch(
            ExportProgressed(
                job_id=JobId(999), done=10, total=100, size_text="", rate_text="", eta_text="", elapsed_text=""
            )
        )
        await pilot.pause()
        assert JobId(999) not in app.jobs


class _BlockingContentSource:
    """Never completes on its own, only via cancellation -- the same fake
    shape as this package's other export-cancellation tests, duplicated
    here rather than imported."""

    size = 10
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        await asyncio.Event().wait()  # never set -- only cancellation ends this
        raise AssertionError("unreachable -- this export can only end by being cancelled")


async def test_on_unmount_cancels_and_drains_every_outstanding_job(tmp_path: Path) -> None:
    """Cancelling alone isn't enough: a cancelled worker only stops at
    its next ``await``, and ``session.close()`` runs right after, so
    teardown has to wait the worker out before closing what it's still
    reading through. Proven end-to-end with a real, group-cancelled
    export worker (see ``app.py``'s own ``on_unmount``) rather than a
    fake ``Worker`` stand-in, since cancellation now goes through
    ``workers.cancel_group`` by name, not a stored reference -- if
    ``on_unmount`` didn't actually cancel/drain it, exiting
    ``run_test()``'s context below would hang instead of returning."""
    unit = RestorableUnit(
        ref=NodeRef("repo", ("item",)),
        name="item.bin",
        is_leaf=True,
        content=_BlockingContentSource(),  # genuinely satisfies the ContentSource protocol
    )
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.store.dispatch(StartExport(unit=unit, dst_text=str(tmp_path / "out.bin"), sparse=True))
        await pilot.pause()  # let the worker actually start running
        assert len(app.jobs) == 1
    # Exiting run_test()'s context triggers real shutdown/on_unmount.


__all__: list[str] = []
