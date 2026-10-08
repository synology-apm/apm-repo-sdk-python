"""``SmbStore`` — an ``ObjectStore`` implementation for a repository reached
over SMB2/3 rather than a locally-mounted share.

``smbclient`` is imported lazily and has no async API, so the four read methods
run its synchronous calls on a thread pool of the store's own, bounded
(``_THREAD_POOL_SIZE``): a timed-out call's thread cannot be stopped, and on
the loop's default executor a stalled server would starve every other
``to_thread()`` user (``LocalFsStore``, chunk decode). Each slot keeps its own
``connection_cache`` instead of ``smbclient``'s process-wide session table,
so closing one ``SmbStore`` never tears down a sibling's connection.

Requests are routed through a small pool of independent sessions
(``_DEFAULT_CONNECTION_POOL_SIZE``) because an SMB2 connection's credit
window starts at 1, and two in-flight requests on one connection would race
for it.

Unlike ``S3Store``/``AzureStore``, a share has real directories, so
``listdir`` on a missing path raises ``NotFoundError`` (as ``LocalFsStore``
does). ``smbprotocol`` has no timeout or retry after the initial connect, so
this module adds both (``NETWORK_READ_TIMEOUT``/``_DEFAULT_MAX_ATTEMPTS``);
its connect timeout is capped to ``NETWORK_CONNECT_TIMEOUT``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import errno
import functools
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, override

from .._util.closing import AsyncClosing
from ..errors import ApmRepoError, NotFoundError, PermissionDeniedError, StorageBackendError
from .base import NETWORK_CONNECT_TIMEOUT, NETWORK_READ_TIMEOUT, Entry, join_path

if TYPE_CHECKING:
    import smbclient as _smbclient_module


DEFAULT_SMB_PORT = 445
"""The SMB server port used unless one is given."""

# Attempts per connect and per operation; a timed-out operation drops its
# slot's session (see _drop_slot_session) so the retry reconnects.
_DEFAULT_MAX_ATTEMPTS = 2

# Each slot is a separately authenticated, costly SMB session. Worker
# processes each rebuild a pool of this size, so the worst case is
# `concurrency.default_worker_count() * this` sessions against one server.
_DEFAULT_CONNECTION_POOL_SIZE = 2

# Room for every slot's call plus each of its retries while timed-out
# threads are still stuck in a stalled server.
_THREAD_POOL_SIZE = _DEFAULT_CONNECTION_POOL_SIZE * _DEFAULT_MAX_ATTEMPTS

# Pause before retrying a credit-exhaustion race (see _is_credit_exhaustion).
_CREDIT_RETRY_DELAY = 0.1

#: ``OSError.errno`` values ``smbclient`` raises for a missing path (``EISDIR``:
#: opening a directory for ``read()``), mapped to ``NotFoundError`` as in
#: ``LocalFsStore``.
_NOT_FOUND_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.EISDIR})

#: ``OSError.errno`` mapped to ``PermissionDeniedError``. Not ``errno.EPERM``:
#: ``smbprotocol`` uses it only for ``STATUS_SHARING_VIOLATION`` (a file locked
#: by another client, not an access-control failure).
_PERMISSION_DENIED_ERRNOS = frozenset({errno.EACCES})

#: ``smbprotocol``'s ``STATUS_ACCESS_DENIED`` (0xC0000022). ``SMBOSError`` maps
#: it to ``errno=0``, so a permission failure must be recognized by this code.
_STATUS_ACCESS_DENIED = 0xC0000022


def _is_permission_denied(exc: OSError) -> bool:
    """Whether ``exc`` is permission-denied: a mapped errno, or the raw
    ``_STATUS_ACCESS_DENIED`` (``.ntstatus`` is ``SMBOSError``-specific)."""
    return exc.errno in _PERMISSION_DENIED_ERRNOS or getattr(exc, "ntstatus", None) == _STATUS_ACCESS_DENIED


def _is_credit_exhaustion(exc: Exception) -> bool:
    """Whether ``exc`` is smbprotocol's transient SMB2 credit-exhaustion signal.
    ``SMBException`` has no subtype for it, so this matches the message text
    ``Connection._send()`` raises."""
    from smbprotocol.exceptions import SMBException

    return isinstance(exc, SMBException) and "credits are available" in str(exc)


def _import_smbclient() -> Any:
    """Lazy ``import smbclient``."""
    import smbclient

    return smbclient


def _smb_entry_size(entry: Any) -> int | None:
    """A file entry's size in bytes from its ``scandir`` stat; ``None`` for a
    directory or when the stat is unavailable."""
    try:
        return None if entry.is_dir() else int(entry.stat().st_size)
    except OSError:
        return None


def _mapped_os_error(exc: OSError, path: str) -> ApmRepoError:
    """The ``ObjectStore`` error an ``smbclient`` ``OSError`` on ``path`` means."""
    if exc.errno in _NOT_FOUND_ERRNOS:
        return NotFoundError("no such path", ref=path)
    if _is_permission_denied(exc):
        return PermissionDeniedError("permission denied", ref=path)
    return StorageBackendError(f"SMB request failed: {exc}", ref=path)


def _mapped_transport_error(exc: Exception, server: str) -> ApmRepoError:
    """The ``ObjectStore`` error a failed connect or request to ``server``
    means: a rejected logon is ``PermissionDeniedError``."""
    from smbprotocol.exceptions import LogonFailure, SMBAuthenticationError

    if isinstance(exc, LogonFailure | SMBAuthenticationError):
        return PermissionDeniedError(f"SMB logon rejected: {exc}", ref=server)
    return StorageBackendError(f"SMB request failed: {exc!r}", ref=server)


class _ConnectionSlot:
    """One pooled SMB session: ``connection_cache`` (passed to every
    ``smbclient`` call) and ``ready`` (``register_session`` has succeeded).
    Lock-free: the pool's semaphore and free-stack check a slot out to one
    caller at a time."""

    __slots__ = ("connection_cache", "ready")

    def __init__(self) -> None:
        self.connection_cache: dict[Any, Any] = {}
        self.ready = False


@dataclasses.dataclass(frozen=True, slots=True)
class SmbStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``SmbStore``."""

    share: str
    server: str
    port: int
    username: str | None
    password: str | None

    def build(self) -> SmbStore:
        return SmbStore(self.share, server=self.server, port=self.port, username=self.username, password=self.password)


class SmbStore(AsyncClosing):
    """One SMB share, addressed by ``"/"``-separated paths relative to the
    share root (the equivalent of ``S3Store``'s bucket).

    ``username`` accepts ``DOMAIN\\username`` or ``user@domain``; leaving
    ``username``/``password`` unset attempts an anonymous/guest session.

    Backed by a pool of SMB sessions created lazily (see module docstring).
    A call checks out a free slot, preferring an already connected one (LIFO),
    and holds it until the call, including retries, finishes.
    """

    def __init__(
        self,
        share: str,
        *,
        server: str,
        port: int = DEFAULT_SMB_PORT,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._share = share
        self._server = server
        self._port = port
        self._username = username
        self._password = password
        self._slots = [_ConnectionSlot() for _ in range(_DEFAULT_CONNECTION_POOL_SIZE)]
        self._free_slots: list[_ConnectionSlot] = list(self._slots)
        self._pool_semaphore = asyncio.Semaphore(_DEFAULT_CONNECTION_POOL_SIZE)
        self._executor: ThreadPoolExecutor | None = None

    def descriptor(self) -> SmbStoreDescriptor:
        """How a worker process rebuilds this store."""
        return SmbStoreDescriptor(self._share, self._server, self._port, self._username, self._password)

    @override
    def __repr__(self) -> str:
        return f"SmbStore(server={self._server!r}, share={self._share!r})"

    def _unc(self, path: str) -> str:
        combined = join_path(path).replace("/", "\\")
        base = f"\\\\{self._server}\\{self._share}"
        return f"{base}\\{combined}" if combined else base

    async def _in_thread[T](self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """``fn(*args, **kwargs)`` on this store's own thread pool, created on
        first use."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=_THREAD_POOL_SIZE, thread_name_prefix="SmbStore")
        return await asyncio.get_running_loop().run_in_executor(self._executor, functools.partial(fn, *args, **kwargs))

    async def _acquire_slot(self) -> _ConnectionSlot:
        """Wait for a free slot and pop it; ``_call_with_retry`` (the only
        caller) releases it in a ``finally``."""
        await self._pool_semaphore.acquire()
        return self._free_slots.pop()

    def _release_slot(self, slot: _ConnectionSlot) -> None:
        self._free_slots.append(slot)
        self._pool_semaphore.release()

    async def _ensure_slot_session(self, slot: _ConnectionSlot) -> Any:
        """The ``smbclient`` module with ``slot``'s session registered,
        connecting on first use. Connecting is attempted up to
        ``_DEFAULT_MAX_ATTEMPTS`` times on ``ValueError``/``OSError``
        (``smbprotocol`` can't tell transient failures apart)."""
        from smbprotocol.exceptions import SMBException

        smbclient = _import_smbclient()
        if not slot.ready:
            last_exc: Exception | None = None
            for _attempt in range(_DEFAULT_MAX_ATTEMPTS):
                try:
                    await self._in_thread(
                        smbclient.register_session,
                        self._server,
                        username=self._username,
                        password=self._password,
                        port=self._port,
                        connection_timeout=NETWORK_CONNECT_TIMEOUT,
                        connection_cache=slot.connection_cache,
                    )
                    slot.ready = True
                    break
                except (OSError, ValueError) as exc:
                    last_exc = exc
                except SMBException as exc:
                    raise _mapped_transport_error(exc, self._server) from exc
            else:
                assert last_exc is not None
                raise _mapped_transport_error(last_exc, self._server) from last_exc
        return smbclient

    def _drop_slot_session(self, slot: _ConnectionSlot) -> None:
        """Discard ``slot``'s session and ``connection_cache`` without a
        graceful close; the next attempt reconnects."""
        slot.ready = False
        slot.connection_cache = {}

    async def _call_with_retry[T](self, fn: Callable[[dict[Any, Any]], T]) -> T:
        """Check out a slot and run ``fn`` on this store's thread pool, bounded by
        ``NETWORK_READ_TIMEOUT``. Attempted up to ``_DEFAULT_MAX_ATTEMPTS``
        times, retrying on a timeout (session dropped first) or a credit-exhaustion race
        (session kept). The slot is always released.
        """
        from smbprotocol.exceptions import SMBException

        slot = await self._acquire_slot()
        try:
            last_exc: Exception | None = None
            for _attempt in range(_DEFAULT_MAX_ATTEMPTS):
                await self._ensure_slot_session(slot)
                cache = slot.connection_cache
                try:
                    return await asyncio.wait_for(self._in_thread(fn, cache), timeout=NETWORK_READ_TIMEOUT)
                except TimeoutError as exc:
                    last_exc = exc
                    self._drop_slot_session(slot)
                except SMBException as exc:
                    if not _is_credit_exhaustion(exc):
                        raise _mapped_transport_error(exc, self._server) from exc
                    last_exc = exc
                    await asyncio.sleep(_CREDIT_RETRY_DELAY)
            assert last_exc is not None
            raise _mapped_transport_error(last_exc, self._server) from last_exc
        finally:
            self._release_slot(slot)

    @override
    async def close(self) -> None:
        """Tear down every pooled SMB session of this instance (not a sibling's)
        once in-flight calls finish; the store stays usable afterward. Safe to
        call more than once."""
        for _ in self._slots:
            await self._pool_semaphore.acquire()
        try:
            smbclient: Any | None = None
            for slot in self._slots:
                if not slot.ready:
                    continue
                if smbclient is None:
                    smbclient = _import_smbclient()
                slot.ready = False
                await self._in_thread(
                    smbclient.delete_session,
                    self._server,
                    port=self._port,
                    connection_cache=slot.connection_cache,
                )
        finally:
            if self._executor is not None:
                # No wait: a thread stuck in a stalled server must not block close.
                self._executor.shutdown(wait=False, cancel_futures=True)
                self._executor = None
            for _ in self._slots:
                self._pool_semaphore.release()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return await self._call_with_retry(
            lambda cache: self._read_sync(_import_smbclient(), path, offset, length, cache)
        )

    async def size(self, path: str) -> int:
        return await self._call_with_retry(lambda cache: self._size_sync(_import_smbclient(), path, cache))

    async def exists(self, path: str) -> bool:
        return await self._call_with_retry(lambda cache: self._exists_sync(_import_smbclient(), path, cache))

    async def listdir(self, path: str) -> list[Entry]:
        return await self._call_with_retry(lambda cache: self._listdir_sync(_import_smbclient(), path, cache))

    def _read_sync(
        self,
        smbclient: _smbclient_module,
        path: str,
        offset: int,
        length: int | None,
        connection_cache: dict[Any, Any],
    ) -> bytes:
        unc = self._unc(path)
        try:
            # share_access="rwd": the "rb" default implies FILE_SHARE_READ only, which
            # would raise a spurious STATUS_SHARING_VIOLATION on a file open for writing elsewhere.
            with smbclient.open_file(unc, mode="rb", share_access="rwd", connection_cache=connection_cache) as f:
                if offset:
                    f.seek(offset)
                return f.read() if length is None else f.read(length)  # type: ignore[no-any-return]
        except OSError as exc:
            raise _mapped_os_error(exc, path) from exc

    def _size_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> int:
        unc = self._unc(path)
        try:
            return int(smbclient.stat(unc, connection_cache=connection_cache).st_size)
        except OSError as exc:
            raise _mapped_os_error(exc, path) from exc

    def _exists_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> bool:
        try:
            return bool(smbclient.path.exists(self._unc(path), connection_cache=connection_cache))
        except OSError as exc:
            # Denied reads as absent so a candidate search isn't aborted (as in LocalFsStore).
            if _is_permission_denied(exc):
                return False
            raise _mapped_os_error(exc, path) from exc

    def _listdir_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> list[Entry]:
        unc = self._unc(path)
        try:
            entries = [
                Entry(entry.name, _smb_entry_size(entry))
                for entry in smbclient.scandir(unc, connection_cache=connection_cache)
            ]
        except OSError as exc:
            raise _mapped_os_error(exc, path) from exc
        return sorted(entries, key=lambda entry: entry.name)
