"""Unit tests for ``browser.app``'s ``--no-sparse-export``
launch-time argument parsing and
``ApmRepoBrowserApp.default_sparse`` — the one place sparse output is
chosen, since ``ExportScreen`` has no per-export toggle of its own (see
that module's own docstring). No ``Pilot``/``run_test()`` needed for either:
``_parse_args()`` is a pure function, and constructing
``ApmRepoBrowserApp()`` itself does no I/O and needs no running event
loop (``Session()``'s own construction is synchronous, and ``App.__init__()``
just sets attributes).
"""

from __future__ import annotations

import pytest
from textual.worker import WorkerCancelled

from synology_apm_repo.browser import app as app_module
from synology_apm_repo.browser.app import ApmRepoBrowserApp, _parse_args, main
from synology_apm_repo.browser.screens.help_screen import HelpScreen
from synology_apm_repo.browser.screens.worklist_screen import WorklistScreen


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
    # over to Textual's own capture, per its own docstring.
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


async def test_action_toggle_verbose_calls_the_screens_refresh_hook_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        calls: list[bool] = []
        app.screen.refresh_for_verbose_mode = lambda: calls.append(True)  # type: ignore[attr-defined]
        app.action_toggle_verbose()
        await pilot.pause()
        assert calls == [True]


async def test_update_job_for_an_unknown_job_id_is_a_no_op() -> None:
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.update_job(999, 10, 100)  # no job with this id -- must not raise
        assert 999 not in app.jobs


async def test_on_unmount_cancels_and_waits_for_every_outstanding_job() -> None:
    """Cancelling alone isn't enough: a cancelled worker only stops at its next
    await, and ``session.close()`` runs right after, so teardown has to wait the
    worker out before closing what it is still reading through."""

    class _FakeWorker:
        def __init__(self) -> None:
            self.cancelled = False
            self.waited = False

        def cancel(self) -> None:
            self.cancelled = True

        async def wait(self) -> None:
            self.waited = True
            # What a real cancelled Textual worker raises — teardown must treat
            # it as the expected outcome, not as a failure to propagate.
            raise WorkerCancelled()

    fake_worker = _FakeWorker()
    app = ApmRepoBrowserApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        job = app.start_job("an export")
        app.attach_worker(job.id, fake_worker)  # type: ignore[arg-type]
    # Exiting run_test()'s context triggers real shutdown/on_unmount.
    assert fake_worker.cancelled is True
    assert fake_worker.waited is True


__all__: list[str] = []
