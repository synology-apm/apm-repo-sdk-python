"""``SequentialReadahead``: few large reads for a reader that asks for one
small block at a time and waits for each (Dissect parsing a disk image
through ``disk_fs``'s bridge), where every read is a storage round trip.

Once a stream of consecutive reads is seen, the read that continues it
fetches a window from its own offset in one read, and later reads inside
that window are served from it. A read no window covers, or whose window
failed or came back short, goes to the source as asked; random reads
fetch nothing extra. Windows are dropped once reads stop for
``IDLE_SECONDS``.
"""

from __future__ import annotations

import asyncio
import collections
from collections.abc import Awaitable, Callable

#: Consecutive reads that make a stream sequential.
TRIGGER = 2

#: The first window of a stream; each window read past doubles the next.
FIRST_WINDOW = 256 * 1024

#: The largest window. A read at least this big is passed through, being
#: one large read already.
MAX_WINDOW = 4 << 20

#: The most bytes held over all streams, windows still loading included.
MAX_BYTES = 16 << 20

#: Seconds with no read in progress after which every window is dropped.
IDLE_SECONDS = 2.0

#: Smaller windows are not worth a read of their own.
MIN_WINDOW = 64 * 1024

#: Sequential streams tracked at once; the least recently used goes.
STREAMS = 4


class _Window:
    """``[start, end)`` of the source, fetched or being fetched; ``data``
    resolves to ``None`` if the fetch failed, came back short or was
    cancelled."""

    __slots__ = ("data", "end", "start")

    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end
        self.data: asyncio.Future[bytes | None] = asyncio.get_running_loop().create_future()

    @property
    def held(self) -> int:
        """Bytes held or loading; a failed window holds none."""
        return 0 if self.data.done() and self.data.result() is None else self.end - self.start


class _Stream:
    __slots__ = ("hits", "last_used", "next", "window", "windows")

    def __init__(self, offset: int) -> None:
        self.next = offset  # where the next sequential read starts
        self.hits = 0  # consecutive reads seen; reads inside a window keep it
        self.window = FIRST_WINDOW
        # Contiguous, in source order: ``covering`` relies on it.
        self.windows: collections.deque[_Window] = collections.deque()
        self.last_used = 0

    def covers(self, offset: int) -> bool:
        return any(w.start <= offset < w.end for w in self.windows)

    def covering(self, start: int, end: int) -> list[_Window] | None:
        """The windows that together cover ``[start, end)``, if they do."""
        parts = [w for w in self.windows if w.end > start and w.start < end]
        if not parts or parts[0].start > start or parts[-1].end < end:
            return None
        return parts


class SequentialReadahead:
    """Reads ahead for sequential readers of ``read``, a source of ``size``
    bytes that never changes. Every call runs on one event loop, which is
    what makes its bookkeeping safe without a lock.

    Args:
        read: The source's positional read.
        size: The source's size; reads past it are passed through.
    """

    def __init__(self, read: Callable[[int, int], Awaitable[bytes | bytearray]], size: int) -> None:
        self._read = read
        self._size = size
        self._streams: list[_Stream] = []
        self._tick = 0
        # Loading windows, counted even once their stream lets go of them.
        self._loading: set[_Window] = set()
        self._active = 0
        self._idle: asyncio.TimerHandle | None = None

    @property
    def held_bytes(self) -> int:
        """Bytes the windows hold now, loading ones included."""
        return sum(w.held for w in {w for s in self._streams for w in s.windows} | self._loading)

    async def read(self, offset: int, length: int) -> bytes | bytearray:
        """``length`` bytes at ``offset``, from windows when they cover it,
        else from the source (the source's errors propagate unchanged)."""
        self._active += 1
        if self._idle is not None:
            self._idle.cancel()
            self._idle = None
        try:
            return await self._read_through(offset, length)
        finally:
            self._active -= 1
            if not self._active:
                self._idle = asyncio.get_running_loop().call_later(IDLE_SECONDS, self._streams.clear)

    async def _read_through(self, offset: int, length: int) -> bytes | bytearray:
        end = offset + length
        if length == 0 or end > self._size:
            return await self._read(offset, length)
        if length >= MAX_WINDOW:
            # One large read already, but the stream it continues goes on past it.
            for stream in self._streams:
                if stream.next == offset:
                    stream.next = end
            return await self._read(offset, length)
        parts, fetch = self._plan(offset, end)
        if fetch is not None:
            await self._fetch(*fetch)
        if parts is None:
            return await self._read(offset, length)
        pieces = []
        for w in parts:
            data = await asyncio.shield(w.data)
            if data is None:
                return await self._read(offset, length)
            pieces.append(data[max(offset, w.start) - w.start : min(end, w.end) - w.start])
        return pieces[0] if len(pieces) == 1 else b"".join(pieces)

    def _plan(self, offset: int, end: int) -> tuple[list[_Window] | None, tuple[_Stream, _Window] | None]:
        """Updates the stream ``[offset, end)`` belongs to and returns the
        windows covering it (``None``: read directly), plus a new window (and
        its stream) this read must fetch first. Synchronous, so atomic on the
        event loop."""
        self._tick += 1
        stream = next((s for s in self._streams if s.next == offset), None) or next(
            (s for s in self._streams if s.covers(offset)), None
        )
        if stream is None:
            stream = _Stream(offset)
            if len(self._streams) >= STREAMS:
                self._streams.remove(min(self._streams, key=lambda s: s.last_used))
            self._streams.append(stream)
        stream.last_used = self._tick
        if stream.next == offset:
            stream.hits += 1
        stream.next = max(stream.next, end)
        stream.window = max(stream.window, min(4 * (end - offset), MAX_WINDOW))
        while stream.windows and stream.windows[0].end <= offset:
            stream.windows.popleft()
            stream.window = min(stream.window * 2, MAX_WINDOW)

        parts = stream.covering(offset, end)
        if parts is not None or stream.hits < TRIGGER:
            return parts, None
        # Continue from where the windows covering offset leave off.
        frontier = stream.windows[-1].end if stream.windows else offset
        room = MAX_BYTES - self.held_bytes
        size = min(max(stream.window, end - frontier), room, self._size - frontier)
        if size < MIN_WINDOW or frontier + size < end:
            return None, None
        window = _Window(frontier, frontier + size)
        stream.windows.append(window)
        self._loading.add(window)
        return stream.covering(offset, end), (stream, window)

    async def _fetch(self, stream: _Stream, window: _Window) -> None:
        """Fetch ``window`` for whoever awaits it. A failed or short fetch
        resolves it to ``None`` (each reader then reads directly and sees the
        error itself) and stays until read past, so it is not fetched again
        for every read it covers. A cancelled one is dropped, with any window
        appended after it, to be fetched again. Held as ``bytes``, so a read
        served from it is one slice."""
        try:
            data = await self._read(window.start, window.end - window.start)
        except asyncio.CancelledError:
            window.data.set_result(None)
            if window in stream.windows:  # not already retired or evicted
                while stream.windows.pop() is not window:
                    pass
            raise
        except Exception:  # noqa: BLE001 - the direct read that follows surfaces it
            window.data.set_result(None)
        else:
            window.data.set_result(bytes(data) if len(data) == window.end - window.start else None)
        finally:
            self._loading.discard(window)
