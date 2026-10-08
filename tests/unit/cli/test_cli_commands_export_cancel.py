"""Unit tests for ``synology_apm_repo.cli.commands.export``'s
Ctrl-C/``--keep-partial`` handling: the SIGINT handler's first-press branch
and the CLI's behavior when the export raises ``asyncio.CancelledError``. The
second-press ``os._exit()`` branch is not exercised.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.cli.commands.export import _install_sigint_cancel
from synology_apm_repo.sdk.api import NodeFrame, RawView, Repository
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.cli.export_fakes import item_frame
from unit.cli.session_fakes import install_fake_session


@faithful_to(Repository)
class _ItemRepo:
    def __init__(self, unit: RestorableUnit) -> None:
        self._unit = unit

    async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
        return item_frame(self._unit)


def test_first_sigint_cancels_the_task_and_can_be_restored() -> None:
    async def scenario() -> None:
        task = asyncio.ensure_future(asyncio.Event().wait())  # never set: only the cancel ends it
        restore = _install_sigint_cancel(task)
        try:
            handler = signal.getsignal(signal.SIGINT)
            assert callable(handler)
            handler(signal.SIGINT, None)  # simulated first Ctrl-C
            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
        finally:
            restore()

    asyncio.run(scenario())


@faithful_to(ContentSource)
class _CancellingContentSource:
    """A ``ContentSource`` whose ``export_range()`` writes some bytes, then is cancelled."""

    size = 100

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
        await sink.write_at(0, b"partial")  # bytes land before the cancellation
        raise asyncio.CancelledError("cancelled mid-export")


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    ref = NodeRef("", ("item",))
    unit = RestorableUnit(ref=ref, name="item", is_leaf=True, content=_CancellingContentSource())

    install_fake_session(monkeypatch, [_ItemRepo(unit)])


def test_cancelled_during_export_removes_the_partial_file_by_default(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    result = invoke(["export", "somewhere", "-o", str(dst)], exit_code=130)
    assert "cancelled" in result.output
    assert "removed" in result.output
    assert not (dst.parent / (dst.name + ".part")).exists()
    assert not dst.exists()


def test_cancelled_during_export_keeps_the_partial_file_with_keep_partial(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    part = dst.parent / (dst.name + ".part")
    result = invoke(["export", "somewhere", "-o", str(dst), "--keep-partial"], exit_code=130)
    assert "kept" in result.output
    assert part.exists()
    assert part.read_bytes().startswith(b"partial")
    assert not dst.exists()
