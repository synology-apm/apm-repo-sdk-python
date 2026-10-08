"""Unit tests for ``synology_apm_repo.sdk._util.once``."""

from __future__ import annotations

import asyncio

import pytest

from synology_apm_repo.sdk._util.once import AsyncOnce


class _Opener:
    def __init__(self) -> None:
        self.opened: list[int] = []
        self.closed: list[int] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.fail_next = False

    async def open(self) -> int:
        await self.gate.wait()
        if self.fail_next:
            self.fail_next = False
            raise OSError("open failed")
        value = len(self.opened)
        self.opened.append(value)
        return value

    async def close(self, value: int) -> None:
        self.closed.append(value)


async def test_concurrent_first_gets_share_one_open() -> None:
    opener = _Opener()
    opener.gate.clear()
    once = AsyncOnce(opener.open)
    tasks = [asyncio.create_task(once.get()) for _ in range(5)]
    await asyncio.sleep(0)
    opener.gate.set()
    assert await asyncio.gather(*tasks) == [0] * 5
    assert opener.opened == [0]
    assert once.opened


async def test_a_failed_open_is_retried_by_the_next_get() -> None:
    opener = _Opener()
    opener.fail_next = True
    once = AsyncOnce(opener.open)
    with pytest.raises(OSError, match="open failed"):
        await once.get()
    assert await once.get() == 0


async def test_close_waits_for_an_open_in_flight_and_releases_it() -> None:
    opener = _Opener()
    opener.gate.clear()
    once = AsyncOnce(opener.open)
    getter = asyncio.create_task(once.get())
    await asyncio.sleep(0)
    closer = asyncio.create_task(once.close(opener.close))
    await asyncio.sleep(0)
    assert opener.closed == []
    opener.gate.set()
    await asyncio.gather(getter, closer)
    assert opener.closed == [0]
    assert not once.opened


async def test_close_without_any_open_does_nothing() -> None:
    opener = _Opener()
    once = AsyncOnce(opener.open)
    await once.close(opener.close)
    assert opener.opened == []
    assert opener.closed == []


async def test_get_after_close_opens_a_fresh_value() -> None:
    opener = _Opener()
    once = AsyncOnce(opener.open)
    assert await once.get() == 0
    await once.close(opener.close)
    assert await once.get() == 1
    assert opener.closed == [0]
