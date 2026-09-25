"""Conformance test: every ``ObjectStore`` wrapper in ``storage/recording.py``
forwards ``aclose()`` to whatever it wraps.

Guards against a wrapper that implements no ``aclose()`` of its own:
``Session.close()`` decides whether to await a tracked store's ``aclose()``
via ``isinstance(store, AsyncCloseable)`` — a ``@runtime_checkable`` Protocol
that only checks method *presence* — so a wrapper missing its own
``aclose()`` silently fails that check and any real ``aiohttp`` connector it
wraps is never released. A parametrized case here for each
current wrapper means the next one added to this module gets the same
guarantee checked automatically, rather than depending on its author
remembering to wire ``aclose()`` through by hand.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from synology_apm_repo.sdk.storage.base import AsyncCloseable, ObjectStore
from synology_apm_repo.sdk.storage.recording import RecordingStore, TracingStore


class _FakeCloseableStore:
    """A minimal ``AsyncCloseable`` ``ObjectStore`` stand-in — only tracks
    how many times ``aclose()`` was awaited."""

    def __init__(self) -> None:
        self.close_count = 0

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def size(self, path: str) -> int:
        return 0

    async def exists(self, path: str) -> bool:
        return False

    async def listdir(self, path: str) -> list[str]:
        return []

    async def aclose(self) -> None:
        self.close_count += 1


class _FakeNonCloseableStore:
    """An ``ObjectStore`` with no ``aclose()`` at all — ``LocalFsStore``/
    ``SmbStore``'s own shape. A wrapper's ``aclose()`` must tolerate this
    (nothing to forward to), not assume every backing store has one."""

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def size(self, path: str) -> int:
        return 0

    async def exists(self, path: str) -> bool:
        return False

    async def listdir(self, path: str) -> list[str]:
        return []


#: Every ``ObjectStore`` wrapper this module currently defines. Add a new
#: entry here whenever ``storage/recording.py`` gains another one — the two
#: tests below then cover it automatically.
_WRAPPERS: list[tuple[str, Callable[[ObjectStore], ObjectStore]]] = [
    ("RecordingStore", lambda backing: RecordingStore(backing)),
    ("TracingStore", lambda backing: TracingStore(backing, lambda _event: None)),
]
_WRAPPER_IDS = [name for name, _ in _WRAPPERS]


@pytest.mark.parametrize("name, make_wrapper", _WRAPPERS, ids=_WRAPPER_IDS)
async def test_wrapper_forwards_aclose_to_a_closeable_backing_store(
    name: str, make_wrapper: Callable[[ObjectStore], ObjectStore]
) -> None:
    backing = _FakeCloseableStore()
    wrapper = make_wrapper(backing)
    assert isinstance(wrapper, AsyncCloseable), (
        f"{name} must implement aclose() itself, or Session.close()'s isinstance(store, AsyncCloseable) "
        "check never finds it to await in the first place"
    )
    await wrapper.aclose()  # the isinstance check above already proved this exists
    assert backing.close_count == 1


@pytest.mark.parametrize("name, make_wrapper", _WRAPPERS, ids=_WRAPPER_IDS)
async def test_wrapper_aclose_is_a_noop_over_a_non_closeable_backing_store(
    name: str, make_wrapper: Callable[[ObjectStore], ObjectStore]
) -> None:
    wrapper = make_wrapper(_FakeNonCloseableStore())
    await wrapper.aclose()  # type: ignore[attr-defined]  # must not raise
