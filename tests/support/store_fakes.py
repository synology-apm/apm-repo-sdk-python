"""Fake ``ObjectStore``s shared across test files.

``WrappingStore`` forwards every call to a real backing store; a test-local
wrapper subclasses it and overrides only the call it instruments. Its
``close()`` leaves the backing store open, since the test owns that.
"""

from __future__ import annotations

import asyncio

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import StorageBackendError
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore


@faithful_to(ObjectStore)
class WrappingStore:
    """Pass-through ``ObjectStore`` over ``backing``."""

    def __init__(self, backing: ObjectStore) -> None:
        self._backing = backing

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await self._backing.read(path, offset, length)

    async def size(self, path: str) -> int:
        return await self._backing.size(path)

    async def exists(self, path: str) -> bool:
        return await self._backing.exists(path)

    async def listdir(self, path: str) -> list[Entry]:
        return await self._backing.listdir(path)

    async def close(self) -> None:
        pass


class BlockingStore(WrappingStore):
    """Once ``armed``, parks every ``read()`` forever, so a test can cancel
    the surrounding task at a real ``await``; ``blocked`` is set once a read
    has parked."""

    def __init__(self, backing: ObjectStore) -> None:
        super().__init__(backing)
        self.armed = False
        self.blocked = asyncio.Event()
        self._never_released = asyncio.Event()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        if self.armed:
            self.blocked.set()
            await self._never_released.wait()
        return await super().read(path, offset, length)


class CountingStore(WrappingStore):
    """Tallies ``read()`` calls in ``read_count``."""

    def __init__(self, backing: ObjectStore) -> None:
        super().__init__(backing)
        self.read_count = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self.read_count += 1
        return await super().read(path, offset, length)


class LoopCheckingStore(WrappingStore):
    """Binds to the event loop of its first call and raises
    ``RuntimeError("Event loop is closed")`` when a later call runs on a
    different one — the observable failure of a store whose network client is
    lazily built and cached on one loop (``S3Store``'s), without a network."""

    def __init__(self, backing: ObjectStore) -> None:
        super().__init__(backing)
        self._bound_loop: asyncio.AbstractEventLoop | None = None

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._bound_loop is None:
            self._bound_loop = loop
        elif self._bound_loop is not loop:
            raise RuntimeError("Event loop is closed")

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        self._check_loop()
        return await super().read(path, offset, length)

    async def size(self, path: str) -> int:
        self._check_loop()
        return await super().size(path)

    async def exists(self, path: str) -> bool:
        self._check_loop()
        return await super().exists(path)

    async def listdir(self, path: str) -> list[Entry]:
        self._check_loop()
        return await super().listdir(path)


@faithful_to(ObjectStore)
class CloseCountingStore:
    """An empty ``ObjectStore`` that counts ``close()`` calls in ``close_count``."""

    def __init__(self) -> None:
        self.close_count = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def size(self, path: str) -> int:
        return 0

    async def exists(self, path: str) -> bool:
        return False

    async def listdir(self, path: str) -> list[Entry]:
        return []

    async def close(self) -> None:
        self.close_count += 1


@faithful_to(ObjectStore)
class FailingStore:
    """An ``ObjectStore`` whose every call but ``close()`` raises
    ``StorageBackendError("down")``."""

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        raise StorageBackendError("down", ref=path)

    async def size(self, path: str) -> int:
        raise StorageBackendError("down", ref=path)

    async def exists(self, path: str) -> bool:
        raise StorageBackendError("down", ref=path)

    async def listdir(self, path: str) -> list[Entry]:
        raise StorageBackendError("down", ref=path)

    async def close(self) -> None:
        pass
