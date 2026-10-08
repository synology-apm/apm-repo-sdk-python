"""Unit tests for ``browser.runtime.app_effects.AppEffects``' folder export: ``StartExport`` with a
``FolderExport`` target through the real ``Store`` and ``core.app.update`` (plan -> preflight -> one file at a
time) against a fake catalog/provider; also a ``FolderExport``'s queueing in ``core.app.update`` and
``ExportScreen``'s folder mode."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

from textual.app import App
from textual.widgets import Input, Static

from support.fakes import faithful_to
from support.pilot import RUN_TEST_SIZE, SDK_TIMEOUT, UI_TIMEOUT, wait_for_screen, wait_until
from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.app.cmd import AppCmd, RunExport
from synology_apm_repo.browser.core.app.model import AppModel, FolderExport, JobStatus
from synology_apm_repo.browser.core.app.msg import AppMsg, CancelJobRequested, StartExport
from synology_apm_repo.browser.core.app.update import update
from synology_apm_repo.browser.runtime.app_effects import AppEffects
from synology_apm_repo.browser.runtime.load_gate import LoadGate
from synology_apm_repo.browser.runtime.store import Store
from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog
from synology_apm_repo.browser.screens.export_screen import ExportScreen
from synology_apm_repo.sdk.api import Catalog, Repository, Version
from synology_apm_repo.sdk.errors import NotRestorableError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef


def _make_store(app: App[None], gate: LoadGate | None = None) -> Store[AppModel, AppMsg, AppCmd]:
    effects: AppEffects

    def _perform(cmd: AppCmd) -> None:
        effects.perform(cmd)

    store: Store[AppModel, AppMsg, AppCmd] = Store(AppModel(), update, _perform)
    effects = AppEffects(app, store, gate if gate is not None else LoadGate())
    return store


def _ref(*segments: str) -> NodeRef:
    return NodeRef("repo", ("ver", *segments))


def _folder_node(*segments: str) -> Node:
    return Node(ref=_ref(*segments), name=segments[-1] if segments else "ver", is_leaf=False)


def _file_node(*segments: str, name: str | None = None) -> Node:
    return Node(ref=_ref(*segments), name=name if name is not None else segments[-1], is_leaf=True, size=4)


@faithful_to(ContentSource)
class _Content:
    """Writes fixed bytes and sets ``started``, then blocks until cancelled (``block``) or raises ``fail``."""

    def __init__(self, data: bytes = b"data", *, block: bool = False, fail: Exception | None = None) -> None:
        self.size = len(data)
        self._data = data
        self._block = block
        self._fail = fail
        self.started = asyncio.Event()

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
        progress: Any = None,
        tuning: object = None,
    ) -> ExportResult:
        await sink.write_at(0, self._data)
        if progress is not None:
            await progress(len(self._data))
        self.started.set()
        if self._block:
            await asyncio.Event().wait()
        if self._fail is not None:
            raise self._fail
        return ExportResult(bytes_written=len(self._data), logical_size=len(self._data), holes=0, zeros=0)


@faithful_to(UnitProvider)
class _Provider:
    def __init__(self, children: dict[NodeRef, list[Node]], contents: dict[NodeRef, _Content]) -> None:
        self._children = children
        self._contents = contents

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children.get(node.ref, [])

    async def unit(self, node: Node) -> RestorableUnit:
        if node.ref not in self._contents:
            raise NotRestorableError(f"{node.name!r} has no content")
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, content=self._contents[node.ref])


@faithful_to(Catalog)
class _Catalog:
    def __init__(self, provider: _Provider) -> None:
        self._provider = provider
        self._provider_calls: list[bool] = []

    async def provider(self, version: Version, *, raw: object = None) -> _Provider:
        self._provider_calls.append(raw is not None)
        return self._provider


@faithful_to(Repository)
class _Repo:
    def __init__(self) -> None:
        self.released: list[object] = []

    async def release_provider(self, provider: UnitProvider) -> None:
        self.released.append(provider)


def _folder_export(
    children: dict[NodeRef, list[Node]], contents: dict[NodeRef, _Content], *, name: str = "ver"
) -> tuple[FolderExport, _Catalog, _Repo]:
    catalog, repo = _Catalog(_Provider(children, contents)), _Repo()
    target = FolderExport(cast(Any, repo), cast(Any, catalog), cast(Any, object()), True, _folder_node(), name)
    return target, catalog, repo


def _two_files() -> tuple[dict[NodeRef, list[Node]], dict[NodeRef, _Content]]:
    children = {_ref(): [_folder_node("d"), _file_node("top.txt")], _ref("d"): [_file_node("d", "f.txt")]}
    return children, {_ref("d", "f.txt"): _Content(b"FFFF"), _ref("top.txt"): _Content(b"TTTT")}


# -- AppEffects ---------------------------------------------------------------


async def test_a_folder_export_holds_the_load_gate_so_an_invalidation_waits_for_it(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        gate = LoadGate()
        store = _make_store(app, gate)
        blocking = _Content(b"BBBB", block=True)
        target, _, _ = _folder_export({_ref(): [_file_node("b.txt")]}, {_ref("b.txt"): blocking})

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: blocking.started.is_set(), timeout=SDK_TIMEOUT, interval=0.02)

        invalidated = asyncio.Event()

        async def invalidate() -> None:
            async with gate.exclusive():
                invalidated.set()

        task = asyncio.create_task(invalidate())
        # Parked on the gate behind the export's shared hold: from here only a release can let it run.
        await wait_until(pilot, lambda: gate._exclusive_pending == 1 and gate._waiters, timeout=SDK_TIMEOUT)
        assert gate._shared == 1
        assert not invalidated.is_set()

        (job_id,) = store.model.jobs
        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, invalidated.is_set, timeout=SDK_TIMEOUT, interval=0.02)  # released on cancel
        await task


async def test_cancelling_while_the_provider_is_released_does_not_report_a_finished_file_as_partial(
    tmp_path: Path,
) -> None:
    @faithful_to(Repository)
    class _SlowReleaseRepo(_Repo):
        async def release_provider(self, provider: UnitProvider) -> None:
            self.released.append(provider)
            await asyncio.Event().wait()  # only a cancel ends this

    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        repo = _SlowReleaseRepo()
        catalog = _Catalog(_Provider({_ref(): [_file_node("a.txt")]}, {_ref("a.txt"): _Content(b"AAAA")}))
        target = FolderExport(cast(Any, repo), cast(Any, catalog), cast(Any, object()), False, _folder_node(), "ver")

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: bool(repo.released), timeout=SDK_TIMEOUT, interval=0.02)
        (job_id,) = store.model.jobs
        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert store.model.recent[0].outcome.notify_message == "ver: cancelled — 1 of 1 file had been exported"
        assert (tmp_path / "out" / "a.txt").read_bytes() == b"AAAA"


async def test_a_folder_export_writes_each_item_and_releases_its_own_provider(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        target, catalog, repo = _folder_export(*_two_files())

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "information"
        assert outcome.notify_message == f"ver: exported 2 files to {tmp_path / 'out'}"
        assert "[green]done[/green] — 2 files" in outcome.status_text
        assert (tmp_path / "out" / "d" / "f.txt").read_bytes() == b"FFFF"
        assert (tmp_path / "out" / "top.txt").read_bytes() == b"TTTT"
        assert catalog._provider_calls == [True]  # opened raw: _folder_export sets force_raw
        assert repo.released == [catalog._provider]


async def test_an_existing_destination_file_is_refused_before_anything_is_written(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        target, _, repo = _folder_export(*_two_files())
        (tmp_path / "out").mkdir()
        (tmp_path / "out" / "top.txt").write_bytes(b"mine")

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert "1 of 2 destination files already exists" in outcome.notify_message
        assert (tmp_path / "out" / "top.txt").read_bytes() == b"mine"
        assert not (tmp_path / "out" / "d").exists()
        assert len(repo.released) == 1  # the provider is released on this path too


async def test_a_destination_that_is_a_file_is_refused(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        target, catalog, _ = _folder_export(*_two_files())
        (tmp_path / "out").write_bytes(b"a file")

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert "is a file — exporting a folder needs a directory" in store.model.recent[0].outcome.notify_message
        assert catalog._provider_calls == []  # nothing was opened for it


async def test_skipped_items_are_reported_as_a_warning(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        children = {_ref(): [_file_node("ok.txt"), _file_node("empty.txt")]}
        target, _, _ = _folder_export(children, {_ref("ok.txt"): _Content(b"OK")})

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert outcome.notify_message.endswith("exported 1 file to " + str(tmp_path / "out") + ", 1 skipped")
        assert "skipped empty.txt: it has no content to export" in outcome.status_text


async def test_incomplete_and_skipped_items_are_both_reported(
    tmp_path: Path,
) -> None:
    class _Degraded(_Provider):
        async def unit(self, node: Node) -> RestorableUnit:
            unit = await super().unit(node)
            return dataclasses.replace(unit, degraded="1 of 2 parts are missing") if node.name == "disk.img" else unit

    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        children = {_ref(): [_file_node("disk.img"), _file_node("empty.txt")]}
        target, catalog, _ = _folder_export(children, {})
        catalog._provider = _Degraded(children, {_ref("disk.img"): _Content(b"DISK")})

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert outcome.notify_message.endswith(", 1 skipped, 1 incomplete")
        assert outcome.status_text.splitlines()[1:] == [
            "incomplete disk.img: 1 of 2 parts are missing",
            "skipped empty.txt: it has no content to export",
        ]


async def test_an_incomplete_single_item_finishes_as_a_warning(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = RestorableUnit(
            ref=_ref("disk.img"), name="disk.img", is_leaf=True, degraded="1 of 2 parts are missing", content=_Content()
        )

        store.dispatch(StartExport(target=unit, dst_text=str(tmp_path / "disk.img"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert outcome.notify_message.endswith("— incomplete: 1 of 2 parts are missing")
        assert outcome.status_text.splitlines()[1] == "1 of 2 parts are missing"
        assert (tmp_path / "disk.img").read_bytes() == b"data"


async def test_a_failing_item_names_itself_and_stops_the_run(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        children = {_ref(): [_file_node("a.txt"), _file_node("b.txt"), _file_node("c.txt")]}
        contents = {
            _ref("a.txt"): _Content(b"AAAA"),
            _ref("b.txt"): _Content(b"BBBB", fail=ValueError("boom")),
            _ref("c.txt"): _Content(b"CCCC"),
        }
        target, _, repo = _folder_export(children, contents)

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "error"
        assert outcome.notify_message == "ver: export failed — b.txt: boom"
        assert (tmp_path / "out" / "a.txt").exists()  # finished files stay
        assert not (tmp_path / "out" / "b.txt").exists()
        assert not (tmp_path / "out" / "b.txt.part").exists()
        assert not (tmp_path / "out" / "c.txt").exists()
        assert len(repo.released) == 1


async def test_cancelling_mid_folder_reports_how_far_it_got(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        blocking = _Content(b"BBBB", block=True)
        children = {_ref(): [_file_node("a.txt"), _file_node("b.txt")]}
        target, _, repo = _folder_export(children, {_ref("a.txt"): _Content(b"AAAA"), _ref("b.txt"): blocking})

        store.dispatch(StartExport(target=target, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: blocking.started.is_set(), timeout=SDK_TIMEOUT, interval=0.02)
        (job_id,) = store.model.jobs
        assert store.model.jobs[job_id].file_text == "file 2 of 2 — b.txt"
        store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        outcome = store.model.recent[0].outcome
        assert outcome.notify_severity == "warning"
        assert "cancelled" in outcome.notify_message
        assert "1 of 2 files had been exported" in outcome.notify_message
        assert (tmp_path / "out" / "a.txt").exists()
        assert not (tmp_path / "out" / "b.txt").exists()
        assert not (tmp_path / "out" / "b.txt.part").exists()
        assert len(repo.released) == 1


async def test_a_single_item_onto_a_directory_says_so(
    tmp_path: Path,
) -> None:
    app = App[None]()
    async with app.run_test() as pilot:
        store = _make_store(app)
        unit = RestorableUnit(ref=_ref("x.bin"), name="x.bin", is_leaf=True, content=_Content())
        (tmp_path / "out").mkdir()

        store.dispatch(StartExport(target=unit, dst_text=str(tmp_path / "out"), sparse=True))
        await wait_until(pilot, lambda: not store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)

        assert store.model.recent[0].outcome.notify_message == (
            f"x.bin: export failed — {tmp_path / 'out'} is a directory — a single item exports to a file path"
        )


# -- core update --------------------------------------------------------------


def test_a_folder_export_queues_behind_a_running_one_and_is_promoted_with_its_target() -> None:
    first, _, _ = _folder_export({}, {}, name="first")
    second, _, _ = _folder_export({}, {}, name="second")

    model, cmds = update(AppModel(), StartExport(target=first, dst_text="./a", sparse=True))
    model, cmds_queued = update(model, StartExport(target=second, dst_text="./b", sparse=True))

    assert [type(cmd).__name__ for cmd in cmds_queued] == ["Notify"]
    assert [job.status for job in model.jobs.values()] == [JobStatus.RUNNING, JobStatus.QUEUED]
    (run,) = cmds
    assert isinstance(run, RunExport) and run.target is first

    from synology_apm_repo.browser.core.app.model import JobOutcome
    from synology_apm_repo.browser.core.app.msg import ExportFinished

    outcome = JobOutcome(notify_message="done", notify_severity="information", status_text="done")
    _, promoted = update(model, ExportFinished(job_id=run.job_id, outcome=outcome))

    runs = [cmd for cmd in promoted if isinstance(cmd, RunExport)]
    assert len(runs) == 1 and runs[0].target is second and runs[0].dst == Path("./b")


# -- the dialog ---------------------------------------------------------------


async def test_the_dialog_in_folder_mode_titles_the_folder_and_suggests_its_name() -> None:
    target, _, _ = _folder_export({}, {}, name="Documents")
    app = ApmRepoBrowserApp()
    async with app.run_test(size=RUN_TEST_SIZE) as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.push_screen(ExportScreen(target))
        await wait_until(
            pilot,
            lambda: isinstance(app.screen, ExportScreen) and bool(app.screen.query("#export-dst")),
            timeout=UI_TIMEOUT,
            interval=0.02,
        )

        assert app.screen.query_one("#export-dst", Input).value == "./Documents"
        rendered = " ".join(str(static.render()) for static in app.screen.query(Static))
        assert "Export folder: Documents" in rendered
        assert "Destination folder" in rendered


async def test_the_dialog_shows_which_file_is_being_exported_and_the_scanning_phase(
    tmp_path: Path,
) -> None:
    from textual.widgets import Button

    from synology_apm_repo.browser.strings import EXPORT_SCANNING_TEXT

    @faithful_to(Catalog)
    class _GatedCatalog(_Catalog):
        """Holds the export in its scanning phase until ``release`` is set, so the phase is observable."""

        def __init__(self, provider: _Provider) -> None:
            super().__init__(provider)
            self.release = asyncio.Event()

        async def provider(self, version: Version, *, raw: object = None) -> _Provider:
            await self.release.wait()
            return await super().provider(version, raw=raw)

    catalog = _GatedCatalog(_Provider({_ref(): [_file_node("b.txt")]}, {_ref("b.txt"): _Content(b"BBBB", block=True)}))
    target = FolderExport(cast(Any, _Repo()), cast(Any, catalog), cast(Any, object()), True, _folder_node(), "ver")
    app = ApmRepoBrowserApp()
    async with app.run_test(size=RUN_TEST_SIZE) as pilot:
        await wait_for_screen(pilot, ConnectDialog)
        app.push_screen(ExportScreen(target))
        await wait_until(
            pilot,
            lambda: isinstance(app.screen, ExportScreen) and bool(app.screen.query("#export-start")),
            timeout=UI_TIMEOUT,
            interval=0.02,
        )
        dialog = app.screen
        dialog.query_one("#export-dst", Input).value = str(tmp_path / "out")
        dialog.query_one("#export-start", Button).press()
        rate = dialog.query_one("#export-rate", Static)

        await wait_until(pilot, lambda: str(rate.render()) == EXPORT_SCANNING_TEXT, timeout=SDK_TIMEOUT, interval=0.02)

        catalog.release.set()
        await wait_until(pilot, lambda: "file 1 of 1 — b.txt" in str(rate.render()), timeout=SDK_TIMEOUT, interval=0.02)

        (job_id,) = app.store.model.jobs
        app.store.dispatch(CancelJobRequested(job_id=job_id))
        await wait_until(pilot, lambda: not app.store.model.jobs, timeout=SDK_TIMEOUT, interval=0.02)
