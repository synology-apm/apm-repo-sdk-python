"""Unit tests for ``synology_apm_repo.browser.runtime.load_gate.LoadGate``."""

from __future__ import annotations

import asyncio

import pytest

from synology_apm_repo.browser.runtime.load_gate import LoadGate


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_shared_holders_run_together() -> None:
    gate = LoadGate()
    inside = 0
    peak = 0

    async def load() -> None:
        nonlocal inside, peak
        async with gate.shared():
            inside += 1
            peak = max(peak, inside)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            inside -= 1

    await asyncio.gather(load(), load(), load())

    assert peak == 3


async def test_exclusive_waits_for_running_shared_holders() -> None:
    gate = LoadGate()
    release = asyncio.Event()
    events: list[str] = []

    async def load() -> None:
        async with gate.shared():
            events.append("load-start")
            await release.wait()
            events.append("load-end")

    async def invalidate() -> None:
        async with gate.exclusive():
            events.append("exclusive")

    loading = asyncio.create_task(load())
    await _settle()
    invalidating = asyncio.create_task(invalidate())
    await _settle()
    assert events == ["load-start"]  # the invalidation is still waiting

    release.set()
    await asyncio.gather(loading, invalidating)

    assert events == ["load-start", "load-end", "exclusive"]


async def test_a_load_starting_while_an_invalidation_waits_queues_behind_it() -> None:
    gate = LoadGate()
    release = asyncio.Event()
    events: list[str] = []

    async def first_load() -> None:
        async with gate.shared():
            await release.wait()

    async def invalidate() -> None:
        async with gate.exclusive():
            events.append("exclusive")

    async def late_load() -> None:
        async with gate.shared():
            events.append("late-load")

    first = asyncio.create_task(first_load())
    await _settle()
    invalidating = asyncio.create_task(invalidate())
    await _settle()
    late = asyncio.create_task(late_load())
    await _settle()
    assert events == []  # neither may run while the first load holds the gate

    release.set()
    await asyncio.gather(first, invalidating, late)

    assert events == ["exclusive", "late-load"]


async def test_exclusive_holders_run_one_at_a_time() -> None:
    gate = LoadGate()
    inside = 0
    peak = 0

    async def invalidate() -> None:
        nonlocal inside, peak
        async with gate.exclusive():
            inside += 1
            peak = max(peak, inside)
            await asyncio.sleep(0)
            inside -= 1

    await asyncio.gather(invalidate(), invalidate(), invalidate())

    assert peak == 1


async def test_a_cancelled_waiting_invalidation_releases_the_loads_it_was_holding_back() -> None:
    gate = LoadGate()
    release = asyncio.Event()
    ran: list[str] = []

    async def first_load() -> None:
        async with gate.shared():
            await release.wait()

    async def invalidate() -> None:
        async with gate.exclusive():
            ran.append("exclusive")

    async def later_load() -> None:
        async with gate.shared():
            ran.append("later-load")

    first = asyncio.create_task(first_load())
    await _settle()
    invalidating = asyncio.create_task(invalidate())
    await _settle()
    later = asyncio.create_task(later_load())
    await _settle()

    invalidating.cancel()
    with pytest.raises(asyncio.CancelledError):
        await invalidating
    await _settle()
    assert ran == ["later-load"]  # no longer held back by the cancelled waiter

    release.set()
    await asyncio.gather(first, later)
    assert ran == ["later-load"]


async def test_an_exception_inside_a_holder_still_releases_the_gate() -> None:
    gate = LoadGate()

    with pytest.raises(RuntimeError, match="boom"):
        async with gate.exclusive():
            raise RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        async with gate.shared():
            raise RuntimeError("boom")

    async with gate.exclusive():  # not stuck behind either failure
        pass


async def test_a_cancelled_shared_holder_releases_the_gate_for_a_later_invalidation() -> None:
    gate = LoadGate()
    inside = asyncio.Event()

    async def load() -> None:
        async with gate.shared():
            inside.set()
            await asyncio.Event().wait()

    loading = asyncio.create_task(load())
    await inside.wait()
    loading.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loading

    async with asyncio.timeout(1):  # would hang forever if the cancelled holder were still counted
        async with gate.exclusive():
            pass


async def test_a_holder_cancelled_while_the_gate_is_contended_releases_it() -> None:
    gate = LoadGate()
    release = asyncio.Event()

    async def hold_exclusive() -> None:
        async with gate.exclusive():
            await release.wait()

    async def load() -> None:
        async with gate.shared():
            pass

    holder = asyncio.create_task(hold_exclusive())
    await _settle()
    waiting = asyncio.create_task(load())
    await _settle()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    release.set()
    await holder
    async with asyncio.timeout(1):
        async with gate.exclusive():
            pass
