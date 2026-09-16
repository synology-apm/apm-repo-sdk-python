"""Unit tests for ``synology_apm_repo.cli.commands.export``'s
Ctrl-C/``--keep-partial`` handling — covers what's safely unit-testable:
``handle_cancelled()``'s pure logic, and the CLI's overall behavior when
an ``asyncio.CancelledError`` is raised — never the literal ``os._exit()``
force-quit branch itself, which would terminate the test runner if
actually invoked; that branch is two lines of trivially-reviewable code,
verified by inspection rather than by execution, a deliberate and
documented limit, not an oversight.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.commands.export import _install_sigint_cancel, handle_cancelled
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.units.base import RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef

runner = CliRunner()


class TestHandleCancelled:
    def test_default_removes_the_partial_file(self, tmp_path: Path) -> None:
        part = tmp_path / "out.bin.part"
        part.write_bytes(b"partial content")
        message = handle_cancelled(part, keep_partial=False)
        assert not part.exists()
        assert "removed" in message

    def test_keep_partial_leaves_the_file_in_place(self, tmp_path: Path) -> None:
        part = tmp_path / "out.bin.part"
        part.write_bytes(b"partial content")
        message = handle_cancelled(part, keep_partial=True)
        assert part.exists()
        assert part.read_bytes() == b"partial content"
        assert "kept" in message
        assert part.name in message

    def test_default_is_safe_even_if_the_partial_file_never_existed(self, tmp_path: Path) -> None:
        part = tmp_path / "never-written.part"
        message = handle_cancelled(part, keep_partial=False)  # must not raise
        assert "removed" in message


def test_first_sigint_cancels_the_task_and_can_be_restored() -> None:
    """The *first*-press branch of ``_install_sigint_cancel``'s handler —
    exercised by fetching the handler ``signal.signal()`` just installed
    and calling it directly with a synthetic ``(signum, frame)``, exactly
    like a real ``SIGINT`` delivery would, but without sending an actual
    signal. The *second*-press branch (``os._exit(130)``) is deliberately
    never invoked here — see this module's own docstring for why."""

    async def scenario() -> None:
        task = asyncio.ensure_future(asyncio.sleep(10))
        restore = _install_sigint_cancel(task)
        try:
            handler = signal.getsignal(signal.SIGINT)
            assert callable(handler)
            handler(signal.SIGINT, None)  # simulated first Ctrl-C, not a real signal
            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
        finally:
            restore()

    asyncio.run(scenario())


class _CancellingContentSource:
    """Matches the ``ContentSource`` protocol shape: ``size`` is a plain
    synchronous attribute; ``read``/``export_to`` are ``async def``;
    ``stream`` is an async generator."""

    size = 100
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        raise asyncio.CancelledError("cancelled mid-export")


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    ref = NodeRef("", ("item",))
    unit = RestorableUnit(ref=ref, name="item", is_leaf=True, content=_CancellingContentSource())

    class _FakeRepo:
        async def resolve(self, node_ref: object, **kwargs: object) -> RestorableUnit:
            return unit

    class _FakeSession:
        def __init__(self) -> None:
            # Session.__init__ itself stays synchronous.
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.Session", _FakeSession)


def test_cancelled_during_export_removes_the_partial_file_by_default(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    (dst.parent / (dst.name + ".part")).write_bytes(b"leftover")  # simulate export_to() having written some bytes
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 0, result.output
    assert "cancelled" in result.output
    assert "removed" in result.output
    assert not (dst.parent / (dst.name + ".part")).exists()
    assert not dst.exists()


def test_cancelled_during_export_keeps_the_partial_file_with_keep_partial(tmp_path: Path) -> None:
    dst = tmp_path / "out.bin"
    part = dst.parent / (dst.name + ".part")
    part.write_bytes(b"leftover")
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst), "--keep-partial"])
    assert result.exit_code == 0, result.output
    assert "kept" in result.output
    assert part.exists()
    assert not dst.exists()


__all__: list[str] = []
