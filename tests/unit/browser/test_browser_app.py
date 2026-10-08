"""Unit tests for ``browser.app``: ``--no-sparse-export`` parsing,
``ApmRepoBrowserApp.default_sparse``, and the app-level actions and shutdown.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from textual.screen import Screen

from support.content_fakes import BlockingContentSource
from support.pilot import SDK_TIMEOUT, wait_for_screen, wait_until
from synology_apm_repo.browser import app as app_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp, _parse_args, main
from synology_apm_repo.browser.core.app.msg import StartExport
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
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
    # preload_resource_tracker() must run before .run() captures sys.stderr.
    calls: list[str] = []
    monkeypatch.setattr(app_module, "configure_logging", lambda: calls.append("configure_logging"))
    monkeypatch.setattr(app_module, "preload_resource_tracker", lambda: calls.append("preload"))
    monkeypatch.setattr(ApmRepoBrowserApp, "run", lambda self, *a, **kw: calls.append("run"))

    main([])

    assert calls == ["configure_logging", "preload", "run"]


async def test_action_show_help_pushes_the_help_screen() -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.action_show_help()
        await wait_for_screen(pilot, HelpScreen)


async def test_action_toggle_worklist_pushes_the_worklist_screen() -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.action_toggle_worklist()
        await wait_for_screen(pilot, WorklistScreen)


async def test_common_bindings_still_reachable_while_the_worklist_dialog_is_open() -> None:
    """``d``/``?`` still work on ``WorklistScreen``: a ``ModalScreen``
    can't reach the App's bindings, so it carries its own
    ``COMMON_BINDINGS``."""
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.action_toggle_worklist()
        await wait_for_screen(pilot, WorklistScreen)

        await pilot.press("d")
        await wait_until(pilot, lambda: app.verbose)

        await pilot.press("question_mark")
        await wait_for_screen(pilot, HelpScreen)


async def test_quit_still_reachable_while_the_worklist_dialog_is_open(monkeypatch: pytest.MonkeyPatch) -> None:
    app = ApmRepoBrowserApp()
    quits: list[bool] = []
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        monkeypatch.setattr(app, "action_quit_app", lambda: quits.append(True))
        app.action_toggle_worklist()
        await wait_for_screen(pilot, WorklistScreen)
        await pilot.press("q")
        await wait_until(pilot, lambda: quits == [True])


async def test_action_quit_app_calls_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        calls: list[bool] = []
        monkeypatch.setattr(app, "exit", lambda: calls.append(True))
        app.action_quit_app()
        assert calls == [True]


async def test_action_toggle_verbose_flips_the_flag_and_css_class(monkeypatch: pytest.MonkeyPatch) -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        assert app.verbose is False
        assert app.has_class("verbose") is False

        app.action_toggle_verbose()
        await wait_until(pilot, lambda: app.verbose and app.has_class("verbose"))

        app.action_toggle_verbose()
        await wait_until(pilot, lambda: not app.verbose and not app.has_class("verbose"))


async def test_action_toggle_verbose_notifies_every_registered_watcher() -> None:
    """Every screen's ``watch(app, "verbose", ...)`` fires on toggle,
    whether or not that screen is on top of the stack."""

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
        await wait_for_screen(pilot, ConnectDialog)
        covered = _FakeScreen()
        app.push_screen(covered)
        await wait_until(pilot, lambda: app.screen is covered and covered.is_mounted)
        on_top = _FakeScreen()
        app.push_screen(on_top)
        await wait_until(pilot, lambda: app.screen is on_top and on_top.is_mounted)

        app.action_toggle_verbose()
        await wait_until(pilot, lambda: covered.calls == [True] and on_top.calls == [True])


async def test_on_unmount_cancels_and_drains_every_outstanding_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An export still running at unmount (``run_test()``'s exit) is
    cancelled after the store closes, and its worker has finished before the
    session closes."""
    source = BlockingContentSource()
    unit = RestorableUnit(
        ref=NodeRef("repo", ("item",)),
        name="item.bin",
        is_leaf=True,
        content=source,
    )
    app = ApmRepoBrowserApp()
    # (step, export cancelled yet, job workers still running) at each close.
    steps: list[tuple[str, bool, int]] = []
    async with app.run_test() as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.store.dispatch(StartExport(target=unit, dst_text=str(tmp_path / "out.bin"), sparse=True))
        await wait_until(pilot, lambda: source.started, timeout=SDK_TIMEOUT, interval=0.02)
        assert len(app.jobs) == 1
        (job,) = app.jobs.values()

        def running_job_workers() -> int:
            return sum(1 for w in app.workers if w.group == job.group and not w.is_finished)

        real_store_close = app.store.close
        real_session_close = app.session.close

        def recording_store_close() -> None:
            steps.append(("store.close", source.cancelled, running_job_workers()))
            real_store_close()

        async def recording_session_close() -> None:
            steps.append(("session.close", source.cancelled, running_job_workers()))
            await real_session_close()

        monkeypatch.setattr(app.store, "close", recording_store_close)
        monkeypatch.setattr(app.session, "close", recording_session_close)
    assert steps == [("store.close", False, 1), ("session.close", True, 0)]
    assert not [w for w in app.workers if not w.is_finished]
