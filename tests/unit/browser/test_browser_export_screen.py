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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from textual.pilot import Pilot
from textual.widgets import Button, Input, ProgressBar

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.browser.strings import EXPORT_NO_DESTINATION_WARNING, EXPORT_NOTHING_RUNNING_WARNING
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef


@asynccontextmanager
async def _open_export_screen(
    unit: RestorableUnit,
) -> AsyncIterator[tuple[ApmRepoBrowserApp, Pilot[None], ExportScreen]]:
    """Mounts a fresh app, pushes ``ExportScreen`` for ``unit``, and
    yields ``(app, pilot, export_screen)`` once it's on screen — the
    boilerplate every scenario() closure below starts with."""
    app = ApmRepoBrowserApp()
    async with app.run_test(size=(140, 45)) as pilot:
        await pilot.pause()
        app.push_screen(ExportScreen(unit))
        await pilot.pause(0.2)
        assert isinstance(app.screen, ExportScreen), app.screen
        yield app, pilot, app.screen


def test_export_dialog_suggests_a_windows_sanitized_filename_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    """The suggested destination is sanitized for Windows-invalid
    characters only when the dialog is actually built while running on
    Windows -- see _windows_safe_filename's own docstring for why this
    is gated rather than applied unconditionally."""
    monkeypatch.setattr(sys, "platform", "win32")

    async def scenario() -> str:
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)),
            name='Report: "Q1/Q2" <draft>.pdf',
            is_leaf=True,
            size=0,
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            return export_screen.query_one("#export-dst", Input).value

    assert asyncio.run(scenario()) == "./Report_ _Q1_Q2_ _draft_.pdf"


def test_export_dialog_does_not_sanitize_the_suggested_filename_off_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same Windows-invalid name is left untouched when not actually
    running on Windows -- a valid POSIX filename must not be needlessly
    mangled just because this machine happens to be macOS/Linux."""
    monkeypatch.setattr(sys, "platform", "linux")

    async def scenario() -> str:
        unit = RestorableUnit(ref=NodeRef("repo", ("item",)), name="weird:name.txt", is_leaf=True, size=0)
        async with _open_export_screen(unit) as (app, pilot, export_screen):
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


def test_export_dialog_ux_details(tmp_path: Path, wait_until: Any) -> None:
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
        async with _open_export_screen(unit) as (app, pilot, export_screen):
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
                timeout=0.8,
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
                pilot, _settled_on_cancelled, timeout=1.0, interval=0.02, message="export never reported cancelled"
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


def test_export_screen_uses_the_apps_default_sparse_setting(wait_until: Any) -> None:
    """``ExportScreen`` has no per-export sparse ``Checkbox`` (see
    ``app.py``'s own ``main()`` docstring) — every export uses whatever
    ``ApmRepoBrowserApp.default_sparse`` was set to at launch. Checked
    both ways (``True``, the default, and ``False``) so a future
    regression that silently hardcodes one value would still be caught."""

    async def scenario(default_sparse: bool) -> bool | None:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        app = ApmRepoBrowserApp(default_sparse=default_sparse)
        async with app.run_test(size=(140, 45)) as pilot:
            await pilot.pause()
            app.push_screen(ExportScreen(unit))
            await pilot.pause(0.2)
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen = app.screen

            assert len(export_screen.query("#export-sparse")) == 0, "no per-export sparse Checkbox"

            dst_input = export_screen.query_one("#export-dst", Input)
            dst_input.focus()
            await pilot.press("enter")

            await wait_until(
                pilot,
                lambda: content.received_sparse is not None,
                timeout=0.8,
                interval=0.02,
                message="export_to was never called",
            )
            received_sparse = content.received_sparse

            # Cancel and wait for it to actually finish before this
            # scenario returns — otherwise ``run_test()``'s own teardown
            # cancels the still-running job itself (on_unmount()'s own
            # cleanup loop), which races the App's own widget teardown
            # and can raise NoMatches from inside finish_job(); every
            # other test using _BlockingContentSource resolves its job
            # the same way before returning, for the same reason.
            export_screen.query_one("#export-start", Button).press()
            await wait_until(pilot, lambda: not app.jobs, timeout=1.0, interval=0.02, message="job never finished")

            return received_sparse

    assert asyncio.run(scenario(True)) is True
    assert asyncio.run(scenario(False)) is False


class _RecordingArtifactSource:
    """Declares ``supports_concurrent_export = False`` — matches an
    assembled-artifact ``ContentSource`` (mail/calendar/...), the case
    ``test_export_screen_hides_max_concurrent_reads_for_a_source_with_no_bucket_concept``
    exists to check."""

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


def test_export_screen_renders_rate_and_eta_once_progress_ticks_arrive(tmp_path: Path) -> None:
    async def scenario() -> str:
        content = _ProgressingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            rate_text = ""
            for _ in range(80):
                await pilot.pause(0.02)
                rate_text = str(export_screen.query_one("#export-rate").render())
                if "ETA" in rate_text:
                    break

            for _ in range(50):
                await pilot.pause(0.02)
                if not app.jobs:
                    break

            return rate_text

    rate_text = asyncio.run(scenario())
    assert "elapsed" in rate_text  # _render_progress always appends this
    assert "ETA" in rate_text, rate_text
    assert "/s" in rate_text, rate_text  # a rate segment was rendered too


def test_export_screen_export_failure_shows_the_error_and_resets_the_button(tmp_path: Path) -> None:
    async def scenario() -> tuple[str, str]:
        content = _ErroringContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            dst = tmp_path / "export_target.bin"
            export_screen.query_one("#export-dst", Input).value = str(dst)
            export_screen.query_one("#export-start", Button).press()

            status = ""
            for _ in range(50):
                await pilot.pause(0.02)
                status = str(export_screen.query_one("#export-status").render())
                if status:
                    break

            return status, str(export_screen.query_one("#export-start", Button).label)

    status, label = asyncio.run(scenario())
    assert "error" in status.lower(), status
    assert "simulated export failure" in status, status
    assert label == "Export"  # _finish() flips the button back regardless of outcome


def test_export_screen_calling_start_twice_only_creates_one_job(tmp_path: Path) -> None:
    """``on_button_pressed`` already prevents this via the button
    relabeling to "Cancel", but ``_start()``'s own internal guard is the
    thing actually responsible — exercised directly here."""

    async def scenario() -> int:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out.bin")

            export_screen._start()
            export_screen._start()
            await pilot.pause()
            job_count = len(app.jobs)

            export_screen.query_one("#export-start", Button).press()  # cancel, so teardown doesn't race it
            for _ in range(50):
                await pilot.pause(0.02)
                if not app.jobs:
                    break
            return job_count

    assert asyncio.run(scenario()) == 1


def test_export_screen_empty_destination_warns_and_does_not_start() -> None:
    async def scenario() -> tuple[list[str], bool]:
        content = _RecordingArtifactSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            warnings: list[str] = []
            export_screen.notify = lambda message, **kwargs: warnings.append(message)  # type: ignore[method-assign]
            export_screen.query_one("#export-dst", Input).value = ""
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()
            return warnings, export_screen._job is None

    warnings, no_job = asyncio.run(scenario())
    assert warnings == [EXPORT_NO_DESTINATION_WARNING]
    assert no_job


def test_export_screen_background_action(tmp_path: Path) -> None:
    async def scenario() -> tuple[list[str], list[str], bool]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
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

            # Cancel the still-running backgrounded job before the scenario
            # returns — see test_export_screen_uses_the_apps_default_sparse_setting's
            # own comment for why.
            job = next(iter(app.jobs.values()))
            job.cancel()
            for _ in range(50):
                await pilot.pause(0.02)
                if not app.jobs:
                    break

            return idle_warnings, warnings, "ExportScreen" not in screens_after

    idle_warnings, running_notifications, popped = asyncio.run(scenario())
    assert idle_warnings == [EXPORT_NOTHING_RUNNING_WARNING]
    assert running_notifications == ["item.bin: continuing export in the background"]
    assert popped


def test_export_screen_escape_pops_when_idle_and_cancels_when_running(tmp_path: Path) -> None:
    async def scenario() -> tuple[bool, bool]:
        content = _BlockingContentSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            # Idle: Esc pops the screen (action_cancel_or_back's else branch).
            await pilot.press("escape")
            await pilot.pause()
            popped_while_idle = "ExportScreen" not in [type(s).__name__ for s in app.screen_stack]

            # Running: Esc cancels instead of popping.
            app.push_screen(ExportScreen(unit))
            await pilot.pause(0.2)
            assert isinstance(app.screen, ExportScreen), app.screen
            export_screen = app.screen
            export_screen.query_one("#export-dst", Input).value = str(tmp_path / "out2.bin")
            export_screen.query_one("#export-start", Button).press()
            await pilot.pause()
            await pilot.press("escape")
            status = ""
            for _ in range(50):
                await pilot.pause(0.02)
                status = str(export_screen.query_one("#export-status").render())
                if "cancel" in status.lower():
                    break
            for _ in range(50):
                await pilot.pause(0.02)
                if not app.jobs:
                    break

            return popped_while_idle, "cancel" in status.lower()

    popped_while_idle, cancelled_while_running = asyncio.run(scenario())
    assert popped_while_idle
    assert cancelled_while_running


def test_export_screen_touch_ui_skips_once_detached_or_on_stale_widgets(tmp_path: Path) -> None:
    async def scenario() -> tuple[bool, bool]:
        content = _RecordingArtifactSource()
        unit = RestorableUnit(
            ref=NodeRef("repo", ("item",)), name="item.bin", is_leaf=True, size=content.size, content=content
        )
        async with _open_export_screen(unit) as (app, pilot, export_screen):
            calls = 0

            def bump() -> None:
                nonlocal calls
                calls += 1

            # A stale widget id raises NoMatches — caught, flips _detached.
            def _query_stale_widget() -> None:
                export_screen.query_one("#does-not-exist", Input)

            export_screen._touch_ui(_query_stale_widget)
            became_detached = export_screen._detached

            # Once _detached, the fast path skips fn() entirely.
            export_screen._touch_ui(bump)

            return became_detached, calls == 0

    became_detached, never_called = asyncio.run(scenario())
    assert became_detached
    assert never_called
