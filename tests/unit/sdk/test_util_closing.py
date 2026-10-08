"""Unit tests for ``synology_apm_repo.sdk._util.closing``."""

from __future__ import annotations

import asyncio

import pytest

from synology_apm_repo.sdk._util.closing import (
    AsyncClosing,
    close_all,
    close_each,
    close_preserving,
    leaf_exceptions,
    shield_or_undo,
)
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore


class _Resource:
    def __init__(self, name: str, *, fail: bool = False, hang: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.hang = hang
        self.closed = False

    async def close(self) -> None:
        if self.hang:
            await asyncio.Event().wait()
        self.closed = True
        if self.fail:
            raise OSError(f"{self.name} close failed")


class TestCloseEach:
    async def test_every_closer_runs_even_after_a_failure(self) -> None:
        resources = [_Resource("a", fail=True), _Resource("b"), _Resource("c", fail=True)]
        errors = await close_each(r.close for r in resources)
        assert all(r.closed for r in resources)
        assert [str(e) for e in errors] == ["a close failed", "c close failed"]

    async def test_a_hung_closer_times_out_without_blocking_the_rest(self) -> None:
        hung, ok = _Resource("hung", hang=True), _Resource("ok")
        errors = await close_each([hung.close, ok.close], per_close_timeout=0.01)
        assert ok.closed
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)

    async def test_cancellation_propagates(self) -> None:
        async def cancelled() -> None:
            raise asyncio.CancelledError

        later = _Resource("later")
        with pytest.raises(asyncio.CancelledError):
            await close_each([cancelled, later.close])
        assert not later.closed


class TestCloseAll:
    async def test_raises_every_failure_as_one_group(self) -> None:
        resources = [_Resource("a", fail=True), _Resource("b", fail=True)]
        with pytest.raises(ExceptionGroup, match="sweep failed") as exc_info:
            await close_all((r.close for r in resources), "sweep failed")
        assert len(exc_info.value.exceptions) == 2

    async def test_returns_quietly_when_every_close_succeeds(self) -> None:
        resources = [_Resource("a"), _Resource("b")]
        await close_all((r.close for r in resources), "sweep failed")
        assert [r.closed for r in resources] == [True, True]


class TestClosePreserving:
    async def test_close_failures_become_notes_on_the_primary_error(self) -> None:
        primary = ValueError("primary")
        resource = _Resource("a", fail=True)
        await close_preserving(primary, [resource.close])
        assert resource.closed
        assert primary.__notes__ == ["cleanup also failed: OSError('a close failed')"]

    async def test_no_note_when_cleanup_succeeds(self) -> None:
        primary = ValueError("primary")
        await close_preserving(primary, [_Resource("a").close])
        assert not hasattr(primary, "__notes__")


def test_leaf_exceptions_flattens_nested_groups() -> None:
    a, b, c = ValueError("a"), OSError("b"), KeyError("c")
    group = ExceptionGroup("outer", [a, ExceptionGroup("inner", [b, ExceptionGroup("deep", [c])])])
    assert leaf_exceptions(group) == [a, b, c]


class _Releasable(AsyncClosing):
    def __init__(self) -> None:
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1


class TestAsyncClosing:
    async def test_async_with_yields_the_instance_and_closes_it(self) -> None:
        resource = _Releasable()
        async with resource as entered:
            assert entered is resource
            assert resource.closed == 0
        assert resource.closed == 1

    async def test_closes_when_the_body_raises(self) -> None:
        resource = _Releasable()
        with pytest.raises(RuntimeError, match="body failed"):
            async with resource:
                raise RuntimeError("body failed")
        assert resource.closed == 1

    @pytest.mark.parametrize("store_class", [S3Store, AzureStore, SmbStore])
    def test_every_network_store_supports_async_with(self, store_class: type) -> None:
        assert issubclass(store_class, AsyncClosing)


class TestShieldOrUndo:
    async def test_success_returns_the_result_without_undo(self) -> None:
        undone: list[object] = []

        async def work() -> int:
            return 1

        async def undo(result: int | None) -> None:
            undone.append(result)

        assert await shield_or_undo(work(), undo) == 1
        assert undone == []

    async def test_a_cancelled_caller_waits_for_the_work_and_undoes_its_result(self) -> None:
        started, gate = asyncio.Event(), asyncio.Event()
        undone: list[object] = []

        async def work() -> str:
            started.set()
            await gate.wait()
            return "made"

        async def undo(result: str | None) -> None:
            undone.append(result)

        task = asyncio.create_task(shield_or_undo(work(), undo))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert undone == ["made"]

    async def test_a_cancel_during_the_undo_does_not_interrupt_it(self) -> None:
        undo_started, undo_gate = asyncio.Event(), asyncio.Event()
        undone: list[object] = []

        async def work() -> int:
            raise RuntimeError("work failed")

        async def undo(result: int | None) -> None:
            undo_started.set()
            await undo_gate.wait()
            undone.append(result)

        task = asyncio.create_task(shield_or_undo(work(), undo))
        await undo_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        undo_gate.set()
        with pytest.raises(RuntimeError, match="work failed"):
            await task
        assert undone == [None]

    async def test_an_undo_failure_is_noted_on_the_original_error(self) -> None:
        async def work() -> int:
            raise RuntimeError("work failed")

        async def undo(result: int | None) -> None:
            raise OSError("undo failed")

        with pytest.raises(RuntimeError, match="work failed") as info:
            await shield_or_undo(work(), undo)
        assert info.value.__notes__ == ["cleanup also failed: OSError('undo failed')"]
