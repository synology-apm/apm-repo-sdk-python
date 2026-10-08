"""``Pilot``-driven coverage of ``ExportScreen`` against fake ``ContentSource``s.

Scenarios that leave a blocked export running cancel it before returning:
otherwise ``run_test()``'s teardown cancels it, racing widget teardown and
occasionally raising ``NoMatches``.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, Input, ProgressBar, Static

from support.content_fakes import BlockingContentSource
from support.fakes import faithful_to
from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, UI_TIMEOUT, settle, wait_for_screen, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.model import Job, JobStatus
from synology_apm_repo.browser.core.app.msg import CancelJobRequested
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.runtime import app_effects
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.help_screen import HelpScreen
from synology_apm_repo.browser.strings import (
    EXPORT_NO_DESTINATION_WARNING,
    EXPORT_NOTHING_RUNNING_WARNING,
    EXPORT_RUNNING_STATUS_TEXT,
)
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.presentation import ProgressMeter
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact
from synology_apm_repo.sdk.units.node_ref import NodeRef


async def _unread_content() -> bytes:
    raise AssertionError("this test never reads a unit's content")


_OpenExportScreen = Callable[..., AbstractAsyncContextManager[tuple[ApmRepoBrowserApp, Pilot[None], ExportScreen]]]


@pytest.fixture
def open_export_screen() -> _OpenExportScreen:
    """``async with open_export_screen(unit) as (app, pilot, export_screen)``
    pushes ``ExportScreen`` for ``unit`` onto a fresh (or ``app=``) app and
    waits until it is composed."""

    @asynccontextmanager
    async def _open(
        unit: RestorableUnit, *, app: ApmRepoBrowserApp | None = None
    ) -> AsyncIterator[tuple[ApmRepoBrowserApp, Pilot[None], ExportScreen]]:
        app = app if app is not None else ApmRepoBrowserApp()
        async with app.run_test(size=RUN_TEST_SIZE) as pilot:
            await wait_for_screen(pilot, ConnectDialog)
            app.push_screen(ExportScreen(unit))
            # #export-dst is present once compose() has mounted.
            await wait_until(
                pilot,
                lambda: (
                    isinstance(app.screen, ExportScreen)
                    and app.screen.is_mounted
                    and bool(app.screen.query("#export-dst"))
                ),
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="ExportScreen never became the active, fully composed screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            yield app, pilot, app.screen

    return _open


@pytest.mark.parametrize(
    ("platform", "name", "expected"),
    [
        # Windows-invalid characters are sanitized out.
        pytest.param(
            "win32", 'Report: "Q1/Q2" <draft>.pdf', "./Report_ _Q1_Q2_ _draft_.pdf", id="sanitized_on_windows"
        ),
        pytest.param("linux", "weird:name.txt", "./weird:name.txt", id="not_sanitized_off_windows"),
    ],
)
def test_export_dialog_suggested_filename(
    monkeypatch: pytest.MonkeyPatch, open_export_screen: _OpenExportScreen, platform: str, name: str, expected: str
) -> None:
    monkeypatch.setattr(sys, "platform", platform)

    async def scenario() -> str:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name=name,
            is_leaf=True,
            size=0,
            content=LazyArtifact(_unread_content),
        )
        async with open_export_screen(unit) as (_app, _pilot, export_screen):
            return export_screen.query_one("#export-dst", Input).value

    assert asyncio.run(scenario()) == expected


def test_export_dialog_ux_details(tmp_path: Path, open_export_screen: _OpenExportScreen) -> None:
    """Button alignment, progress-bar animation, the Enter-key shortcut and
    button relabeling, checked against actual widget state."""

    async def scenario() -> tuple[bool, bool, str, str, bool, str, str, str]:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="item.bin",
            is_leaf=True,
            size=content.size,
            content=content,
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            box = export_screen.query_one("Vertical")
            button_before = export_screen.query_one("#export-start", Button)
            input_widget = export_screen.query_one("#export-dst", Input)
            button_right_aligned = (button_before.region.x + button_before.region.width) > (
                box.region.x + box.region.width - 8
            )
            progress = export_screen.query_one("#export-progress", ProgressBar)
            progress_active_before_start = progress.has_class("active")
            label_before = str(button_before.label)

            dst = tmp_path / "export_target.bin"
            input_widget.value = str(dst)
            input_widget.focus()
            await pilot.press("enter")  # Enter in the path field starts the export

            await wait_until(
                pilot,
                lambda: str(export_screen.query_one("#export-start", Button).label) == "Cancel",
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="the start button never flipped to Cancel",
            )
            label_during = str(export_screen.query_one("#export-start", Button).label)
            variant_during = export_screen.query_one("#export-start", Button).variant
            progress_active_during = export_screen.query_one("#export-progress", ProgressBar).has_class("active")

            # Pressed while running, the button cancels.
            export_screen.query_one("#export-start", Button).press()

            def _settled_on_cancelled() -> bool:
                text = str(export_screen.query_one("#export-status").render()).lower()
                return "cancel" in text and "cancelling" not in text

            await wait_until(
                pilot,
                _settled_on_cancelled,
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="export never reported cancelled",
            )
            status = str(export_screen.query_one("#export-status").render())
            label_after = str(export_screen.query_one("#export-start", Button).label)

            return (
                button_right_aligned,
                progress_active_before_start,
                label_before,
                label_during,
                progress_active_during,
                variant_during,
                label_after,
                status,
            )

    (
        button_right_aligned,
        progress_active_before_start,
        label_before,
        label_during,
        progress_active_during,
        variant_during,
        label_after,
        status,
    ) = asyncio.run(scenario())

    assert button_right_aligned, "the Export/Cancel button must be right-aligned, not flush left"
    # Textual's indeterminate spin starts when a ProgressBar mounts with no total set.
    assert not progress_active_before_start, "the progress bar must not animate before an export has started"
    assert label_before == "Export"
    assert label_during == "Cancel", "the button must relabel to Cancel while an export is running"
    assert variant_during == "warning"
    assert progress_active_during, "the progress bar must become visible once an export actually starts"
    assert "cancel" in status.lower(), status
    assert label_after == "Export", "the button must relabel back to Export once the export finishes/cancels"


def test_export_screen_uses_the_apps_default_sparse_setting(open_export_screen: _OpenExportScreen) -> None:
    """``ExportScreen`` has no sparse ``Checkbox``; it passes
    ``ApmRepoBrowserApp.default_sparse`` through, checked for both values."""

    async def scenario(default_sparse: bool) -> bool | None:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        app = ApmRepoBrowserApp(default_sparse=default_sparse)
        async with open_export_screen(unit, app=app) as (app, pilot, export_screen):
            assert len(export_screen.query("#export-sparse")) == 0, "no per-export sparse Checkbox"

            dst_input = export_screen.query_one("#export-dst", Input)
            dst_input.focus()
            await pilot.press("enter")

            await wait_until(
                pilot,
                lambda: content.received_sparse is not None,
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="export_range was never called",
            )
            received_sparse = content.received_sparse

            export_screen.query_one("#export-start", Button).press()
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            return received_sparse

    assert asyncio.run(scenario(True)) is True
    assert asyncio.run(scenario(False)) is False


@faithful_to(ContentSource)
class _InstantContentSource:
    """A ``ContentSource`` whose export finishes immediately."""

    size = 4096

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        await sink.write_at(0, b"x")
        return ExportResult(bytes_written=1, logical_size=1, holes=0, zeros=0)


@faithful_to(ContentSource)
class _ProgressingContentSource:
    """Reports two progress ticks 0.25 s apart on ``clock`` (the test hands
    it to the meter as its ``now``): enough elapsed time for a non-zero
    rate, and 2% done clears ``ProgressMeter``'s 1% ETA warm-up fraction, so
    an ETA appears without waiting out the 2 s warm-up. Then holds until
    ``seen`` is set: renders are throttled, so finishing right after the
    second tick could skip the render that shows the ETA."""

    size = 1000

    def __init__(self) -> None:
        self.seen = asyncio.Event()
        self.clock = 0.0

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        assert progress is not None
        await progress(0)  # type: ignore[operator]
        self.clock += 0.25
        await progress(20)  # type: ignore[operator]
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.seen.wait(), timeout=30.0)
        await sink.write_at(0, b"x" * 20)
        return ExportResult(bytes_written=20, logical_size=1000, holes=0, zeros=0)


@faithful_to(ContentSource)
class _ErroringContentSource:
    """A ``ContentSource`` whose export raises ``ApmRepoError``."""

    size = 1000

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        raise ApmRepoError("simulated export failure")


@faithful_to(ContentSource)
class _UnexpectedlyFailingContentSource:
    """A ``ContentSource`` whose export raises a non-``ApmRepoError``, as a
    leaked third-party (e.g. ``dissect.*``) failure would."""

    size = 1000

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        raise RuntimeError("simulated unexpected dependency failure")


def test_export_screen_renders_rate_and_eta_once_progress_ticks_arrive(
    tmp_path: Path, open_export_screen: _OpenExportScreen, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _ProgressingContentSource()
    monkeypatch.setattr(app_effects, "ProgressMeter", functools.partial(ProgressMeter, now=lambda: content.clock))

    async def scenario() -> str:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            await wait_until(
                pilot,
                lambda: "ETA" in str(export_screen.query_one("#export-rate").render()),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="rate/ETA text never appeared",
            )
            rate_text = str(export_screen.query_one("#export-rate").render())
            content.seen.set()

            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            return rate_text

    rate_text = asyncio.run(scenario())
    assert "elapsed" in rate_text
    assert "ETA" in rate_text, rate_text
    assert "/s" in rate_text, rate_text  # a rate segment was rendered too


def test_export_screen_export_failure_shows_the_error_and_resets_the_button(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> tuple[str, str]:
        content = _ErroringContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            await wait_until(
                pilot,
                lambda: "error" in str(export_screen.query_one("#export-status").render()).lower(),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="export error status was never rendered",
            )
            status = str(export_screen.query_one("#export-status").render())

            return status, str(export_screen.query_one("#export-start", Button).label)

    status, label = asyncio.run(scenario())
    assert "error" in status.lower(), status
    assert "simulated export failure" in status, status
    assert label == "Export"  # reset regardless of outcome


def test_export_screen_unexpected_non_apm_repo_error_shows_a_clean_error_too(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    """A non-``ApmRepoError`` failure shows the same error status and
    button reset, rather than crashing the worker."""

    async def scenario() -> tuple[str, str]:
        content = _UnexpectedlyFailingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            await wait_until(
                pilot,
                lambda: "error" in str(export_screen.query_one("#export-status").render()).lower(),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="export error status was never rendered",
            )
            status = str(export_screen.query_one("#export-status").render())

            return status, str(export_screen.query_one("#export-start", Button).label)

    status, label = asyncio.run(scenario())
    assert "error" in status.lower(), status
    assert "simulated unexpected dependency failure" in status, status
    assert label == "Export"  # reset regardless of outcome


def test_export_screen_calling_start_twice_only_creates_one_job(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    """``_start()``'s own guard prevents a second job, exercised directly
    (the button relabeling to "Cancel" already prevents it via the UI)."""

    async def scenario() -> int:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")

            export_screen._start()
            export_screen._start()
            await settle(pilot)
            job_count = len(app.jobs)

            export_screen.query_one("#export-start", Button).press()  # cancel
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )
            return job_count

    assert asyncio.run(scenario()) == 1


def test_export_screen_empty_destination_warns_and_does_not_start(
    open_export_screen: _OpenExportScreen, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty destination surfaces the warning through ``app.notify()``
    and starts no job."""

    async def scenario() -> tuple[list[str], bool]:
        content = _InstantContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            warnings: list[str] = []
            monkeypatch.setattr(app, "notify", lambda message, **kwargs: warnings.append(message))
            export_screen.query_one("#export-dst", Input).value = ""
            export_screen.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: warnings)
            return warnings, export_screen._job_id is None

    warnings, no_job = asyncio.run(scenario())
    assert warnings == [EXPORT_NO_DESTINATION_WARNING]
    assert no_job


def test_export_screen_background_action(
    tmp_path: Path, open_export_screen: _OpenExportScreen, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> tuple[list[str], list[str], bool]:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            warnings: list[str] = []
            monkeypatch.setattr(export_screen, "notify", lambda message, **kwargs: warnings.append(message))
            export_screen.action_background()  # nothing running yet
            idle_warnings = list(warnings)

            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")
            export_screen.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: content.started, timeout=SDK_TIMEOUT)

            warnings.clear()
            export_screen.action_background()
            await wait_until(pilot, lambda: not isinstance(app.screen, ExportScreen))
            screens_after = [type(s).__name__ for s in app.screen_stack]

            job_id = next(iter(app.jobs))
            app.store.dispatch(CancelJobRequested(job_id=job_id))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            return idle_warnings, warnings, "ExportScreen" not in screens_after

    idle_warnings, running_notifications, popped = asyncio.run(scenario())
    assert idle_warnings == [EXPORT_NOTHING_RUNNING_WARNING]
    assert running_notifications == ["item.bin: continuing export in the background"]
    assert popped


def test_export_screen_escape_pops_when_idle_and_cancels_when_running(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> tuple[bool, bool, bool]:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            # Idle: Esc pops the screen.
            await pilot.press("escape")
            await wait_until(pilot, lambda: not isinstance(app.screen, ExportScreen))
            popped_while_idle = "ExportScreen" not in [type(s).__name__ for s in app.screen_stack]

            # Running: Esc cancels instead of popping.
            app.push_screen(ExportScreen(unit))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen = app.screen
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out2.bin")
            export_screen.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: content.started, timeout=SDK_TIMEOUT)
            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: "cancel" in str(export_screen.query_one("#export-status").render()).lower(),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="export never reported cancelled",
            )
            status = str(export_screen.query_one("#export-status").render())
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            # The cancelled export has settled, so a pop it caused would have run.
            await settle(pilot)
            still_stacked = app.screen is export_screen and export_screen in app.screen_stack

            return popped_while_idle, "cancel" in status.lower(), still_stacked

    popped_while_idle, cancelled_while_running, still_stacked = asyncio.run(scenario())
    assert popped_while_idle
    assert cancelled_while_running
    assert still_stacked, "Esc on a running export must cancel it, not pop the screen"


def test_export_screen_cancelling_a_queued_export_shows_cancelled_not_cancelling(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    """Cancelling a ``QUEUED`` job (blocked behind unit_a's export) leaves
    its terminal "cancelled" status, not a stale "cancelling..."."""

    async def scenario() -> str:
        content_a = BlockingContentSource()
        unit_a = RestorableUnit(
            ref=NodeRef("repo", ("a",)), name="a.bin", is_leaf=True, size=content_a.size, content=content_a
        )
        content_b = _InstantContentSource()
        unit_b = RestorableUnit(
            ref=NodeRef("repo", ("b",)), name="b.bin", is_leaf=True, size=content_b.size, content=content_b
        )
        async with open_export_screen(unit_a) as (app, pilot, export_screen_a):
            export_screen_a.query_one("#export-dst", Input).value = str(tmp_path / "a_out.bin")
            export_screen_a.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: content_a.started, timeout=SDK_TIMEOUT)

            app.push_screen(ExportScreen(unit_b))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen_b = app.screen
            export_screen_b.query_one("#export-dst", Input).value = str(tmp_path / "b_out.bin")
            export_screen_b.query_one("#export-start", Button).press()
            await wait_until(
                pilot, lambda: "queued" in str(export_screen_b.query_one("#export-status").render()).lower()
            )
            queued_status = str(export_screen_b.query_one("#export-status").render())

            export_screen_b.query_one("#export-start", Button).press()  # cancel the queued job
            await wait_until(
                pilot,
                lambda: "cancelled" in str(export_screen_b.query_one("#export-status").render()).lower(),
                timeout=SDK_TIMEOUT,
                interval=0.02,
                message="cancelled status was never rendered",
            )
            status = str(export_screen_b.query_one("#export-status").render())

            # Cleanup: cancel unit_a's export too.
            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            assert "queued" in queued_status.lower(), queued_status
            return status

    status = asyncio.run(scenario())
    assert "cancelled" in status.lower(), status
    assert "cancelling" not in status.lower(), status


def test_export_screen_clears_stale_rate_text_when_the_next_job_is_queued(
    open_export_screen: _OpenExportScreen,
) -> None:
    """Rendering a ``QUEUED`` job still runs ``_render_progress``, whose
    empty rate/ETA/elapsed text clears a previous export's leftover line."""

    async def scenario() -> str:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="item.bin",
            is_leaf=True,
            size=100,
            content=LazyArtifact(_unread_content),
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            # What a finished, rate-reporting export leaves behind.
            export_screen.query_one("#export-rate", Static).update("187 MiB/s · ETA 00:08 · elapsed 00:42")

            queued_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.QUEUED)
            export_screen._render_job(queued_job)
            await wait_until(pilot, lambda: str(export_screen.query_one("#export-rate").render()) == "")

            return str(export_screen.query_one("#export-rate").render())

    assert asyncio.run(scenario()) == ""


def test_export_screen_status_text_flips_from_queued_to_exporting_on_promotion(
    open_export_screen: _OpenExportScreen,
) -> None:
    """``_render_job`` itself rewrites ``#export-status`` when a job goes
    ``QUEUED`` -> ``RUNNING``; ``_start()`` isn't called again for that."""

    async def scenario() -> tuple[str, str]:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="item.bin",
            is_leaf=True,
            size=100,
            content=LazyArtifact(_unread_content),
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            queued_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.QUEUED)
            export_screen._render_job(queued_job)
            queued_text = str(export_screen.query_one("#export-status").render())

            running_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING)
            export_screen._render_job(running_job)
            await wait_until(
                pilot, lambda: "exporting" in str(export_screen.query_one("#export-status").render()).lower()
            )
            running_text = str(export_screen.query_one("#export-status").render())

            return queued_text, running_text

    queued_text, running_text = asyncio.run(scenario())
    assert "queued" in queued_text.lower(), queued_text
    assert "exporting" in running_text.lower(), running_text
    assert "queued" not in running_text.lower(), running_text


def test_export_screen_progress_bar_treats_a_genuinely_zero_total_as_complete(
    open_export_screen: _OpenExportScreen,
) -> None:
    """A known total of 0 (an empty unit) renders as complete, not as an
    indeterminate spinner because ``0`` is falsy."""

    async def scenario() -> tuple[float | None, float | None]:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="empty.bin",
            is_leaf=True,
            size=0,
            content=LazyArtifact(_unread_content),
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            job = Job(id=JobId(1), label="export empty.bin", group="job-1", done=0, total=0)
            export_screen._render_job(job)
            bar = export_screen.query_one("#export-progress", ProgressBar)
            await wait_until(pilot, lambda: bar.total == 0)
            return bar.total, bar.percentage

    total, percentage = asyncio.run(scenario())
    assert total == 0
    assert percentage == 1.0


def test_export_screen_common_bindings_are_reachable_while_running(
    tmp_path: Path, open_export_screen: _OpenExportScreen
) -> None:
    """``d``/``?`` work on ``ExportScreen`` during an export: a
    ``ModalScreen`` needs its own ``action_toggle_verbose``/
    ``action_show_help`` delegating to the App, not just the bindings."""

    async def scenario() -> tuple[bool, str]:
        content = BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")
            export_screen.query_one("#export-start", Button).press()
            # _start() moves focus off the Input once running.
            await wait_until(pilot, lambda: content.started and not isinstance(app.focused, Input), timeout=SDK_TIMEOUT)

            await pilot.press("d")
            await wait_until(pilot, lambda: app.verbose)
            verbose = app.verbose

            await pilot.press("question_mark")
            await wait_for_screen(pilot, HelpScreen)
            screen_name = type(app.screen).__name__

            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            return verbose, screen_name

    verbose, screen_name = asyncio.run(scenario())
    assert verbose is True
    assert screen_name == HelpScreen.__name__


def test_export_screen_background_action_wording_for_a_queued_job(
    tmp_path: Path, open_export_screen: _OpenExportScreen, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``action_background`` on a ``QUEUED`` job (behind unit_a's export)
    reports it as queued, not as "continuing export in the background"."""

    async def scenario() -> str:
        content_a = BlockingContentSource()
        unit_a = RestorableUnit(
            ref=NodeRef("repo", ("a",)), name="a.bin", is_leaf=True, size=content_a.size, content=content_a
        )
        content_b = _InstantContentSource()
        unit_b = RestorableUnit(
            ref=NodeRef("repo", ("b",)), name="b.bin", is_leaf=True, size=content_b.size, content=content_b
        )
        async with open_export_screen(unit_a) as (app, pilot, export_screen_a):
            export_screen_a.query_one("#export-dst", Input).value = str(tmp_path / "a_out.bin")
            export_screen_a.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: content_a.started, timeout=SDK_TIMEOUT)

            app.push_screen(ExportScreen(unit_b))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and app.screen.is_mounted,
                timeout=UI_TIMEOUT,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen_b = app.screen
            export_screen_b.query_one("#export-dst", Input).value = str(tmp_path / "b_out.bin")
            export_screen_b.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: export_screen_b._job_id is not None)

            warnings: list[str] = []
            monkeypatch.setattr(export_screen_b, "notify", lambda message, **kwargs: warnings.append(message))
            export_screen_b.action_background()
            await wait_until(pilot, lambda: warnings)

            # Cleanup: cancel unit_a's still-running export.
            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=SDK_TIMEOUT, interval=0.02, message="job never finished"
            )

            (message,) = warnings
            return message

    message = asyncio.run(scenario())
    assert "continuing export in the background" not in message, message
    assert "queued" in message.lower(), message


def test_export_screen_does_not_rewrite_status_text_on_repeated_running_renders(
    open_export_screen: _OpenExportScreen,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``#export-status`` is rewritten only on a QUEUED<->RUNNING
    transition, not on every progress tick (``Static.update()`` always
    repaints)."""

    async def scenario() -> list[object]:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="item.bin",
            is_leaf=True,
            size=100,
            content=LazyArtifact(_unread_content),
        )
        async with open_export_screen(unit) as (_app, pilot, export_screen):
            status = export_screen.query_one("#export-status", Static)
            calls: list[object] = []
            original_update = status.update

            def _counting_update(content: object = "", **kwargs: object) -> None:
                calls.append(content)
                original_update(content, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(status, "update", _counting_update)

            running = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING, done=1)
            export_screen._render_job(running)
            running_tick = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING, done=2)
            export_screen._render_job(running_tick)
            await settle(pilot)
            return calls

    calls = asyncio.run(scenario())
    assert calls == [EXPORT_RUNNING_STATUS_TEXT]  # written once, not once per render
