"""``textual`` ``Pilot``-driven coverage of ``ExportScreen`` that needs no
real sample data — every scenario here drives the screen
against a fake ``ContentSource``, not a leaf from a real repo, so
it lives in ``tests/unit/`` rather than alongside
``tests/integration/browser/test_browser_pilot.py``'s own
real-data Pilot walkthroughs.

Written as synchronous ``def test_...()`` functions wrapping
``asyncio.run(App.run_test(...))``, matching that same file's own
convention: each scenario owns exactly one loop for the whole
``App.run_test()`` lifetime.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, Input, ProgressBar, Static

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.model import Job, JobStatus
from synology_apm_repo.browser.core.app.msg import CancelJobRequested
from synology_apm_repo.browser.core.keys import JobId
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.screens.help_screen import HelpScreen
from synology_apm_repo.browser.strings import (
    EXPORT_NO_DESTINATION_WARNING,
    EXPORT_NOTHING_RUNNING_WARNING,
    EXPORT_RUNNING_STATUS_TEXT,
)
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef

_OpenExportScreen = Callable[..., AbstractAsyncContextManager[tuple[ApmRepoBrowserApp, Pilot[None], ExportScreen]]]


@pytest.fixture
def open_export_screen(wait_until: Any, ui_timeout: float) -> _OpenExportScreen:
    """``async with open_export_screen(unit) as (app, pilot, export_screen):``
    mounts a fresh app (or a caller-supplied ``app=`` for a non-default
    constructor arg like ``default_sparse``), pushes ``ExportScreen`` for
    ``unit``, and waits for it to actually become the active screen rather
    than a fixed pause — the boilerplate every scenario() closure below
    starts with."""

    @asynccontextmanager
    async def _open(
        unit: RestorableUnit, *, app: ApmRepoBrowserApp | None = None
    ) -> AsyncIterator[tuple[ApmRepoBrowserApp, Pilot[None], ExportScreen]]:
        app = app if app is not None else ApmRepoBrowserApp()
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            app.push_screen(ExportScreen(unit))
            # isinstance(app.screen, ExportScreen) alone only proves the
            # screen has been pushed, not that its compose() has actually
            # mounted anything yet -- every caller below immediately
            # queries a widget by id, which can race an unmounted screen
            # (NoMatches) on a slow/busy runner. #export-dst is always
            # present once compose() runs, so wait for that too.
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen) and bool(app.screen.query("#export-dst")),
                timeout=ui_timeout,
                interval=0.02,
                message="ExportScreen never became the active, fully composed screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            yield app, pilot, app.screen

    return _open


def test_export_dialog_suggests_a_windows_sanitized_filename_on_windows(
    monkeypatch: pytest.MonkeyPatch, open_export_screen: _OpenExportScreen
) -> None:
    """The suggested destination is sanitized for Windows-invalid
    characters only when the dialog is actually built while running on
    Windows: it's just a freely-editable default, so a POSIX run has no
    reason to touch an already-valid name."""
    monkeypatch.setattr(sys, "platform", "win32")

    async def scenario() -> str:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name='Report: "Q1/Q2" <draft>.pdf',
            is_leaf=True,
            size=0,
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            return export_screen.query_one("#export-dst", Input).value

    assert asyncio.run(scenario()) == "./Report_ _Q1_Q2_ _draft_.pdf"


def test_export_dialog_does_not_sanitize_the_suggested_filename_off_windows(
    monkeypatch: pytest.MonkeyPatch, open_export_screen: _OpenExportScreen
) -> None:
    """The same Windows-invalid name is left untouched when not actually
    running on Windows -- a valid POSIX filename must not be needlessly
    mangled just because this machine happens to be macOS/Linux."""
    monkeypatch.setattr(sys, "platform", "linux")

    async def scenario() -> str:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="weird:name.txt", is_leaf=True, size=0)
        async with open_export_screen(unit) as (app, pilot, export_screen):
            return export_screen.query_one("#export-dst", Input).value

    assert asyncio.run(scenario()) == "./weird:name.txt"


class _BlockingContentSource:
    """A fake ``ContentSource`` (a Protocol) whose
    ``export_to`` never completes on its own, instead of doing any real
    I/O — deterministic control over exactly when an export "finishes"
    (only when cancelled), so a test can inspect mid-export UI state
    without racing a real export that might complete before the test gets
    to look. Duck-types the Protocol structurally; ``size`` as a plain
    attribute (not a ``@property``) needs the same
    ``# type: ignore[arg-type]`` precedent
    ``tests/unit/cli/test_cli_export_cancel.py``'s own ``_CancellingContentSource``
    already established for the identical reason.

    ``read``/``export_to`` are ``async def``, ``stream`` is an async
    generator, and no method takes a ``cancel=`` parameter: it parks on
    an ``asyncio.Event`` that is never set — the export Task's
    cancellation (``BackgroundJob.cancel()``) delivers
    ``asyncio.CancelledError`` right here, which is exactly the SDK
    behavior this fake is standing in for."""

    size = 4096
    supports_concurrent_export = False

    def __init__(self) -> None:
        self.received_sparse: bool | None = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        self.received_sparse = sparse
        await asyncio.Event().wait()  # never set — only cancellation ends this
        raise AssertionError("unreachable — this export can only end by being cancelled")


def test_export_dialog_ux_details(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """Four direct user-reported UX asks against the export dialog
    (button alignment, progress-bar animation, the Enter-key shortcut,
    button relabeling — see the assertions below for each), all verified
    against actual widget state, not just "it didn't crash".

    Uses a controllable ``_BlockingContentSource`` rather than
    drilling to a real leaf in a sample repo: the mid-export assertions
    need the export to reliably still be running when inspected, which a
    real (possibly tiny, possibly near-instant) leaf can't guarantee.
    """

    async def scenario() -> tuple[bool, bool, str, str, bool, str, str, str]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name="item.bin",
            is_leaf=True,
            size=content.size,
            # No ``# type: ignore[arg-type]`` needed: _BlockingContentSource
            # genuinely satisfies the async ContentSource protocol.
            content=content,
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
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
            # UX ask #3: Enter in the destination path field starts the
            # export, the same as clicking the button.
            await pilot.press("enter")  # Enter, not a button click

            # _BlockingContentSource.export_to() cannot return until
            # cancelled, so this state is guaranteed stable to inspect —
            # no race with a real export finishing underneath.
            await wait_until(
                pilot,
                lambda: str(export_screen.query_one("#export-start", Button).label) == "Cancel",
                timeout=sdk_timeout,
                interval=0.02,
                message="the start button never flipped to Cancel",
            )
            label_during = str(export_screen.query_one("#export-start", Button).label)
            variant_during = export_screen.query_one("#export-start", Button).variant
            progress_active_during = export_screen.query_one("#export-progress", ProgressBar).has_class("active")

            # Press the button again while running: it must cancel, not
            # silently no-op or re-start.
            export_screen.query_one("#export-start", Button).press()

            def _settled_on_cancelled() -> bool:
                text = str(export_screen.query_one("#export-status").render()).lower()
                return "cancel" in text and "cancelling" not in text

            await wait_until(
                pilot,
                _settled_on_cancelled,
                timeout=sdk_timeout,
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

    # UX ask #1: the Export/Cancel button sits right-aligned, not flush left.
    assert button_right_aligned, "the Export/Cancel button must be right-aligned, not flush left"
    # UX ask #2: the progress bar is not animating (Textual's own
    # indeterminate spin, which starts the instant a ProgressBar mounts
    # with no total set) before an export has actually been started.
    assert not progress_active_before_start, "the progress bar must not animate before an export has started"
    assert label_before == "Export"
    # UX ask #4: the button relabels to "Cancel" (and a different
    # variant) the moment an export starts, and back to "Export" once
    # it's done — pressing it *while running* cancels rather than
    # re-starting (see the button-press above).
    assert label_during == "Cancel", "the button must relabel to Cancel while an export is running"
    assert variant_during == "warning"
    assert progress_active_during, "the progress bar must become visible once an export actually starts"
    assert "cancel" in status.lower(), status
    assert label_after == "Export", "the button must relabel back to Export once the export finishes/cancels"


def test_export_screen_uses_the_apps_default_sparse_setting(
    wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """``ExportScreen`` has no per-export sparse ``Checkbox`` -- it always
    defers to whatever ``ApmRepoBrowserApp.default_sparse`` was set to at
    launch. Checked
    both ways (``True``, the default, and ``False``) so a future
    regression that silently hardcodes one value would still be caught."""

    async def scenario(default_sparse: bool) -> bool | None:
        content = _BlockingContentSource()
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
                timeout=sdk_timeout,
                interval=0.02,
                message="export_to was never called",
            )
            received_sparse = content.received_sparse

            # Cancel and wait for it to actually finish before this
            # scenario returns — otherwise ``run_test()``'s own teardown
            # cancels the still-running job itself (on_unmount()'s own
            # cleanup loop), which races the App's own widget teardown
            # and can raise NoMatches from a widget query during
            # export-finish handling; every other test using
            # _BlockingContentSource resolves its job the same way
            # before returning, for the same reason.
            export_screen.query_one("#export-start", Button).press()
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            return received_sparse

    assert asyncio.run(scenario(True)) is True
    assert asyncio.run(scenario(False)) is False


class _RecordingArtifactSource:
    """Declares ``supports_concurrent_export = False`` — a fast-completing,
    non-blocking ``ContentSource`` standing in for an assembled-artifact
    source (mail/calendar/...), used wherever a test needs a second unit
    that finishes immediately rather than blocking like
    ``_BlockingContentSource``."""

    size = 4096
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        dst.write_bytes(b"x")
        return ExportResult(bytes_written=1, logical_size=1, holes=0, zeros=0)


class _ProgressingContentSource:
    """Actually calls its ``progress`` callback (unlike every other fake
    ContentSource in this file, whose ``export_to()`` ignores it) — one
    real ~0.25s gap between two ticks is enough to give
    ``ProgressMeter.rate`` a non-zero value (it needs a real elapsed-time
    delta between two samples, see ``sdk/presentation/progress.py``'s own
    ``ProgressMeter.update()``) and, since the second tick already covers
    2% of ``total`` (above the 1% warm-up fraction), an ETA too, without
    waiting out the full 2-second warm-up window."""

    size = 1000
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None, **kwargs: object) -> object:
        assert progress is not None
        await progress(0, 1000)  # type: ignore[operator]
        await asyncio.sleep(0.25)
        await progress(20, 1000)  # type: ignore[operator]
        dst.write_bytes(b"x" * 20)
        return ExportResult(bytes_written=20, logical_size=1000, holes=0, zeros=0)


class _ErroringContentSource:
    size = 1000
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None, **kwargs: object) -> object:
        raise ApmRepoError("simulated export failure")


class _UnexpectedlyFailingContentSource:
    """Stands in for a content source wrapping a third-party dependency
    (e.g. a dissect.* filesystem parser) that leaks something that isn't
    an ``ApmRepoError`` at all — same broad-catch scenario
    ``unit_screen.py``'s own ``_load_children`` guards against, proven
    here for ``_run_export``'s own widened catch."""

    size = 1000
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None, **kwargs: object) -> object:
        raise RuntimeError("simulated unexpected dependency failure")


def test_export_screen_renders_rate_and_eta_once_progress_ticks_arrive(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> str:
        content = _ProgressingContentSource()
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
                timeout=sdk_timeout,
                interval=0.02,
                message="rate/ETA text never appeared",
            )
            rate_text = str(export_screen.query_one("#export-rate").render())

            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            return rate_text

    rate_text = asyncio.run(scenario())
    assert "elapsed" in rate_text  # _render_progress always appends this
    assert "ETA" in rate_text, rate_text
    assert "/s" in rate_text, rate_text  # a rate segment was rendered too


def test_export_screen_export_failure_shows_the_error_and_resets_the_button(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> tuple[str, str]:
        content = _ErroringContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            await wait_until(
                pilot,
                lambda: bool(str(export_screen.query_one("#export-status").render())),
                timeout=sdk_timeout,
                interval=0.02,
                message="export status was never rendered",
            )
            status = str(export_screen.query_one("#export-status").render())

            return status, str(export_screen.query_one("#export-start", Button).label)

    status, label = asyncio.run(scenario())
    assert "error" in status.lower(), status
    assert "simulated export failure" in status, status
    assert label == "Export"  # _finish() flips the button back regardless of outcome


def test_export_screen_unexpected_non_apm_repo_error_shows_a_clean_error_too(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """The broad ``except Exception`` in ``_run_export`` must show the
    same clean error message and button reset for a raw,
    non-``ApmRepoError`` failure, not let it crash the worker as an
    unhandled ``WorkerFailed``."""

    async def scenario() -> tuple[str, str]:
        content = _UnexpectedlyFailingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            await wait_until(
                pilot,
                lambda: bool(str(export_screen.query_one("#export-status").render())),
                timeout=sdk_timeout,
                interval=0.02,
                message="export status was never rendered",
            )
            status = str(export_screen.query_one("#export-status").render())

            return status, str(export_screen.query_one("#export-start", Button).label)

    status, label = asyncio.run(scenario())
    assert "error" in status.lower(), status
    assert "simulated unexpected dependency failure" in status, status
    assert label == "Export"  # _finish() flips the button back regardless of outcome


def test_export_screen_calling_start_twice_only_creates_one_job(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """``on_button_pressed`` already prevents this via the button
    relabeling to "Cancel", but ``_start()``'s own internal guard is the
    thing actually responsible — exercised directly here."""

    async def scenario() -> int:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")

            export_screen._start()
            export_screen._start()
            await pilot.pause()
            job_count = len(app.jobs)

            export_screen.query_one("#export-start", Button).press()  # cancel, so teardown doesn't race it
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )
            return job_count

    assert asyncio.run(scenario()) == 1


def test_export_screen_empty_destination_warns_and_does_not_start(open_export_screen: _OpenExportScreen) -> None:
    """The warning now fires via ``app.notify()``, not ``screen.notify()``
    — ``StartExport``'s own validation moved into ``core/app/update.py``
    (pure, testable there directly), returning a ``Notify`` command
    ``AppEffects.perform`` carries out on the App itself. ``Widget.notify``
    (which ``Screen`` inherits) is documented as delegating straight to
    ``self.app.notify()`` regardless, so this is an internal plumbing
    change only — not a user-visible one."""

    async def scenario() -> tuple[list[str], bool]:
        content = _RecordingArtifactSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            warnings: list[str] = []
            app.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
            export_screen.query_one("#export-dst", Input).value = ""
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()
            return warnings, export_screen._job_id is None

    warnings, no_job = asyncio.run(scenario())
    assert warnings == [EXPORT_NO_DESTINATION_WARNING]
    assert no_job


def test_export_screen_background_action(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> tuple[list[str], list[str], bool]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            warnings: list[str] = []
            export_screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
            export_screen.action_background()  # nothing running yet
            idle_warnings = list(warnings)

            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()

            warnings.clear()
            export_screen.action_background()
            await pilot.pause()
            screens_after = [type(s).__name__ for s in app.screen_stack]

            # Cancel the still-running backgrounded job before the
            # scenario returns -- otherwise run_test()'s own teardown
            # cancels it itself, racing the App's own widget teardown and
            # occasionally raising NoMatches during export-finish
            # handling. Dispatched through the store, not job.cancel() --
            # Job is a frozen value with nothing to call -- matching how
            # WorklistScreen's own action_cancel_selected and
            # ExportScreen._cancel_job both request it now.
            job_id = next(iter(app.jobs))
            app.store.dispatch(CancelJobRequested(job_id=job_id))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            return idle_warnings, warnings, "ExportScreen" not in screens_after

    idle_warnings, running_notifications, popped = asyncio.run(scenario())
    assert idle_warnings == [EXPORT_NOTHING_RUNNING_WARNING]
    assert running_notifications == ["item.bin: continuing export in the background"]
    assert popped


def test_export_screen_escape_pops_when_idle_and_cancels_when_running(
    tmp_path: Path, wait_until: Any, ui_timeout: float, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    async def scenario() -> tuple[bool, bool]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            # Idle: Esc pops the screen (action_cancel_or_back's else branch).
            await pilot.press("escape")
            await pilot.pause()
            popped_while_idle = "ExportScreen" not in [type(s).__name__ for s in app.screen_stack]

            # Running: Esc cancels instead of popping.
            app.push_screen(ExportScreen(unit))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen),
                timeout=ui_timeout,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen = app.screen
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out2.bin")
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()
            await pilot.press("escape")
            await wait_until(
                pilot,
                lambda: "cancel" in str(export_screen.query_one("#export-status").render()).lower(),
                timeout=sdk_timeout,
                interval=0.02,
                message="export never reported cancelled",
            )
            status = str(export_screen.query_one("#export-status").render())
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            return popped_while_idle, "cancel" in status.lower()

    popped_while_idle, cancelled_while_running = asyncio.run(scenario())
    assert popped_while_idle
    assert cancelled_while_running


def test_export_screen_cancelling_a_queued_export_shows_cancelled_not_cancelling(
    tmp_path: Path, wait_until: Any, ui_timeout: float, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """``_cancel_job`` dispatches ``CancelJobRequested`` then
    unconditionally used to write "cancelling..." afterward -- but for a
    still-``QUEUED`` job (this one, blocked behind unit_a's own running
    export), that dispatch removes the job and renders its real terminal
    status *synchronously*, before ``_cancel_job``'s own next line would
    otherwise clobber it back to a stuck, wrong "cancelling..." with
    nothing left to ever correct it."""

    async def scenario() -> str:
        content_a = _BlockingContentSource()
        unit_a = RestorableUnit(
            ref=NodeRef("repo", ("a",)), name="a.bin", is_leaf=True, size=content_a.size, content=content_a
        )
        content_b = _RecordingArtifactSource()
        unit_b = RestorableUnit(
            ref=NodeRef("repo", ("b",)), name="b.bin", is_leaf=True, size=content_b.size, content=content_b
        )
        async with open_export_screen(unit_a) as (app, pilot, export_screen_a):
            export_screen_a.query_one("#export-dst", Input).value = str(tmp_path / "a_out.bin")
            export_screen_a.query_one("#export-start", Button).press()
            await pilot.pause()

            app.push_screen(ExportScreen(unit_b))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen),
                timeout=ui_timeout,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen_b = app.screen
            export_screen_b.query_one("#export-dst", Input).value = str(tmp_path / "b_out.bin")
            export_screen_b.query_one("#export-start", Button).press()
            await pilot.pause()
            queued_status = str(export_screen_b.query_one("#export-status").render())

            export_screen_b.query_one("#export-start", Button).press()  # cancel the queued job
            await wait_until(
                pilot,
                lambda: bool(str(export_screen_b.query_one("#export-status").render())),
                timeout=sdk_timeout,
                interval=0.02,
                message="cancelled status was never rendered",
            )
            status = str(export_screen_b.query_one("#export-status").render())

            # Cleanup: cancel unit_a's still-running export too, so
            # run_test()'s own teardown doesn't race it.
            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            assert "queued" in queued_status.lower(), queued_status
            return status

    status = asyncio.run(scenario())
    assert "cancelled" in status.lower(), status
    assert "cancelling" not in status.lower(), status


def test_export_screen_clears_stale_rate_text_when_the_next_job_is_queued(
    open_export_screen: _OpenExportScreen,
) -> None:
    """``_render_progress`` (the only place that ever writes
    ``#export-rate``) must still run for a ``QUEUED`` job -- its own
    ``rate_text``/``eta_text``/``elapsed_text`` are all ``""``, which is
    exactly what clears a previous, already-finished export's leftover
    rate/ETA/elapsed line from the same screen instance. Skipping that
    call for QUEUED (rendering only the "queued" status text instead)
    would leave the stale line showing right next to it."""

    async def scenario() -> str:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=100)
        async with open_export_screen(unit) as (app, pilot, export_screen):
            # Simulates what a finished, rate-reporting export would have
            # left behind -- _render_job's FinishedJob branch never
            # touches #export-rate itself, matching the real behavior.
            export_screen.query_one("#export-rate", Static).update("187 MiB/s · ETA 00:08 · elapsed 00:42")

            queued_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.QUEUED)
            export_screen._render_job(queued_job)
            await pilot.pause()

            return str(export_screen.query_one("#export-rate").render())

    assert asyncio.run(scenario()) == ""


def test_export_screen_status_text_flips_from_queued_to_exporting_on_promotion(
    open_export_screen: _OpenExportScreen,
) -> None:
    """A ``QUEUED`` job promoted to ``RUNNING`` reaches this screen only
    via the store's own subscription re-firing ``_render_job`` (this
    screen never calls ``_start()`` again for that transition) --
    ``_render_job`` itself must be what rewrites ``#export-status`` on
    every render, not a one-time write ``_start()`` makes when the job
    is first minted, or the stale "queued..." text would never clear
    once the export is actually running underneath it."""

    async def scenario() -> tuple[str, str]:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=100)
        async with open_export_screen(unit) as (app, pilot, export_screen):
            queued_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.QUEUED)
            export_screen._render_job(queued_job)
            queued_text = str(export_screen.query_one("#export-status").render())

            running_job = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING)
            export_screen._render_job(running_job)
            await pilot.pause()
            running_text = str(export_screen.query_one("#export-status").render())

            return queued_text, running_text

    queued_text, running_text = asyncio.run(scenario())
    assert "queued" in queued_text.lower(), queued_text
    assert "exporting" in running_text.lower(), running_text
    assert "queued" not in running_text.lower(), running_text


def test_export_screen_progress_bar_treats_a_genuinely_zero_total_as_complete(
    open_export_screen: _OpenExportScreen,
) -> None:
    """``job.total`` (not ``job.total or None``) is passed straight
    through to ``ProgressBar.update`` -- a genuinely known total of 0
    (an empty unit) must render as complete, the same special case
    ``Job.percent`` already carves out, not fall back to an
    indeterminate spinner because ``0`` is falsy."""

    async def scenario() -> tuple[float | None, float | None]:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="empty.bin", is_leaf=True, size=0)
        async with open_export_screen(unit) as (app, pilot, export_screen):
            job = Job(id=JobId(1), label="export empty.bin", group="job-1", done=0, total=0)
            export_screen._render_job(job)
            await pilot.pause()
            bar = export_screen.query_one("#export-progress", ProgressBar)
            return bar.total, bar.percentage

    total, percentage = asyncio.run(scenario())
    assert total == 0
    assert percentage == 1.0


def test_export_screen_common_bindings_are_reachable_while_running(
    tmp_path: Path, wait_until: Any, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """``ExportScreen`` keeps ``COMMON_BINDINGS`` (``q``/``d``/``?``) in
    its own ``BINDINGS``, but that alone doesn't make them work on a
    ``ModalScreen``: Textual's own action dispatch runs the method on
    whichever node the key's own ``Binding`` was found on, never
    bubbling further once the modal chain is truncated here -- so
    ``action_toggle_verbose``/``action_show_help`` must exist directly
    on this class too, delegating to the App's real implementation."""

    async def scenario() -> tuple[bool, str]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with open_export_screen(unit) as (app, pilot, export_screen):
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()  # _start() moves focus off the Input once running

            await pilot.press("d")
            await pilot.pause()
            verbose = app.verbose

            await pilot.press("question_mark")
            await pilot.pause()
            screen_name = type(app.screen).__name__

            # Cleanup: cancel the still-running job so run_test()'s own
            # teardown doesn't race it -- same reasoning every other
            # _BlockingContentSource scenario in this file gives.
            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            return verbose, screen_name

    verbose, screen_name = asyncio.run(scenario())
    assert verbose is True
    assert screen_name == HelpScreen.__name__


def test_export_screen_background_action_wording_for_a_queued_job(
    tmp_path: Path, wait_until: Any, ui_timeout: float, sdk_timeout: float, open_export_screen: _OpenExportScreen
) -> None:
    """``action_background`` on a still-``QUEUED`` job (behind unit_a's
    own running export) must not claim it's "continuing export in the
    background" -- it hasn't started yet. Popping the screen is still
    correct either way (the job already lives in the store regardless of
    whether this screen stays open); only the notify wording must match
    reality."""

    async def scenario() -> str:
        content_a = _BlockingContentSource()
        unit_a = RestorableUnit(
            ref=NodeRef("repo", ("a",)), name="a.bin", is_leaf=True, size=content_a.size, content=content_a
        )
        content_b = _RecordingArtifactSource()
        unit_b = RestorableUnit(
            ref=NodeRef("repo", ("b",)), name="b.bin", is_leaf=True, size=content_b.size, content=content_b
        )
        async with open_export_screen(unit_a) as (app, pilot, export_screen_a):
            export_screen_a.query_one("#export-dst", Input).value = str(tmp_path / "a_out.bin")
            export_screen_a.query_one("#export-start", Button).press()
            await pilot.pause()

            app.push_screen(ExportScreen(unit_b))
            await wait_until(
                pilot,
                lambda: isinstance(app.screen, ExportScreen),
                timeout=ui_timeout,
                interval=0.02,
                message="ExportScreen never became the active screen",
            )
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen_b = app.screen
            export_screen_b.query_one("#export-dst", Input).value = str(tmp_path / "b_out.bin")
            export_screen_b.query_one("#export-start", Button).press()
            await pilot.pause()

            warnings: list[str] = []
            export_screen_b.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
            export_screen_b.action_background()
            await pilot.pause()

            # Cleanup: cancel unit_a's still-running export.
            app.store.dispatch(CancelJobRequested(job_id=next(iter(app.jobs))))
            await wait_until(
                pilot, lambda: not app.jobs, timeout=sdk_timeout, interval=0.02, message="job never finished"
            )

            (message,) = warnings
            return message

    message = asyncio.run(scenario())
    assert "continuing export in the background" not in message, message
    assert "queued" in message.lower(), message


def test_export_screen_does_not_rewrite_status_text_on_repeated_running_renders(
    open_export_screen: _OpenExportScreen,
) -> None:
    """``#export-status`` must be rewritten only on an actual QUEUED<->
    RUNNING transition, not on every render -- ``_render_job`` is the
    store subscription callback and fires on every ``ExportProgressed``
    tick (up to 10/s) for the whole duration of a running export, and
    ``Static.update()`` always repaints regardless of whether the text
    changed, which this app's own ``progress_hint.py`` already documents
    as the dominant cost of driving it."""

    async def scenario() -> list[object]:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=100)
        async with open_export_screen(unit) as (app, pilot, export_screen):
            status = export_screen.query_one("#export-status", Static)
            calls: list[object] = []
            original_update = status.update

            def _counting_update(content: object = "", **kwargs: object) -> None:
                calls.append(content)
                original_update(content, **kwargs)  # type: ignore[arg-type]

            status.update = _counting_update  # type: ignore[method-assign]

            running = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING, done=1)
            export_screen._render_job(running)
            running_tick = Job(id=JobId(1), label="export item.bin", group="job-1", status=JobStatus.RUNNING, done=2)
            export_screen._render_job(running_tick)
            await pilot.pause()
            return calls

    calls = asyncio.run(scenario())
    assert calls == [EXPORT_RUNNING_STATUS_TEXT]  # written once, not once per render
