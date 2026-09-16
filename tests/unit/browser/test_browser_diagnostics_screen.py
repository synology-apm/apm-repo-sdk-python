"""``textual`` ``Pilot``-driven coverage for ``DiagnosticsScreen`` (the
``v`` panel) — driven against a fake, sample-independent repository whose
``verify()`` returns canned ``Finding``\\ s, so this lives in
``tests/unit/`` rather than needing a real sample.

``_FakeApp`` mirrors ``test_browser_unit_screen_pagination.py``'s own
minimal-host convention: ``DiagnosticsScreen`` only ever reads
``app_state.repo`` (``NavigableScreen.app_state`` is just ``self.app``,
duck-typed, no runtime type check), so a bare ``App`` subclass exposing
that one attribute satisfies it without going through
``ApmRepoBrowserApp``'s own auto-pushed ``BrowseScreen``/``ConnectDialog``."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Static

from synology_apm_repo.browser.screens.diagnostics_screen import DiagnosticsScreen
from synology_apm_repo.sdk.api import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.errors import ApmRepoError

_QUICK_FINDING = Finding(Stage.FILE_MAP, Symptom.FILE_MISSING, "db/file_map", "file_map not found")
_FULL_FINDING = Finding(Stage.BUCKET, Symptom.CORRUPTION, "bucket 1/2", "checksum mismatch")


class _FakeRepo:
    def __init__(self, verify: Callable[[VerifyLevel], list[Finding] | ApmRepoError]) -> None:
        self._verify = verify

    async def verify(self, level: VerifyLevel) -> list[Finding]:
        result = self._verify(level)
        if isinstance(result, Exception):
            raise result
        return result


class _FakeApp(App[None]):
    def __init__(self, repo: _FakeRepo) -> None:
        super().__init__()
        self.repo = repo

    def compose(self) -> ComposeResult:
        return iter(())

    def on_mount(self) -> None:
        self.push_screen(DiagnosticsScreen())


async def test_quick_check_with_findings_populates_the_table(wait_until: Any) -> None:
    app = _FakeApp(_FakeRepo(lambda level: [_QUICK_FINDING]))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count > 0, timeout=0.6, interval=0.02)
        assert table.row_count == 1
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "finding(s)" in str(status.render()), timeout=0.6, interval=0.02)
        assert "1 finding(s)" in str(status.render())
        assert "level=quick" in str(status.render())


async def test_quick_check_clean_reports_clean_status(wait_until: Any) -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=0.6, interval=0.02)
        assert "press f for a full check" in str(status.render())


async def test_repaired_via_parity_only_reports_clean_not_red(wait_until: Any) -> None:
    """``Symptom.REPAIRED_VIA_PARITY`` is a successful self-heal, not a
    problem left unresolved (see that symptom's own docstring) --
    ``_show_findings`` must not fold it into the red "finding(s)" count."""
    repaired = Finding(
        Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
    )
    app = _FakeApp(_FakeRepo(lambda level: [repaired]))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "clean" in str(status.render()), timeout=0.6, interval=0.02)
        assert "1 self-repaired via parity" in str(status.render())
        assert "finding(s)" not in str(status.render())


async def test_pressing_f_runs_a_full_check_and_replaces_the_findings(wait_until: Any) -> None:
    calls: list[VerifyLevel] = []

    def verify(level: VerifyLevel) -> list[Finding]:
        calls.append(level)
        return [_QUICK_FINDING] if level is VerifyLevel.QUICK else [_FULL_FINDING, _FULL_FINDING]

    app = _FakeApp(_FakeRepo(verify))
    async with app.run_test() as pilot:
        table = app.screen.query_one("#diag-table", DataTable)
        await wait_until(pilot, lambda: table.row_count == 1, timeout=0.6, interval=0.02)

        await pilot.press("f")
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "level=full" in str(status.render()), timeout=0.6, interval=0.02)

        assert calls == [VerifyLevel.QUICK, VerifyLevel.FULL]
        assert table.row_count == 2
        assert "2 finding(s)" in str(status.render())


async def test_verify_error_shows_in_the_status_line(wait_until: Any) -> None:
    app = _FakeApp(_FakeRepo(lambda level: ApmRepoError("repo is locked")))
    async with app.run_test() as pilot:
        status = app.screen.query_one("#diag-status", Static)
        await wait_until(pilot, lambda: "error:" in str(status.render()), timeout=0.6, interval=0.02)
        assert "repo is locked" in str(status.render())


async def test_action_go_back_pops_the_screen() -> None:
    app = _FakeApp(_FakeRepo(lambda level: []))
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, DiagnosticsScreen)
        screen.action_go_back()
        await pilot.pause()
        assert app.screen is not screen


__all__: list[str] = []
