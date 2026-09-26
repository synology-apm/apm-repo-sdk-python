"""``SmbStore`` — an ``ObjectStore`` implementation for a repository reached
over SMB2/3 rather than a locally-mounted share.

``smbclient`` is imported lazily (each method calls ``_import_smbclient``)
and has no native-async surface, so this store's four methods are thin
``asyncio.to_thread()`` wrappers around its synchronous API. Each instance
keeps its own ``connection_cache`` rather than sharing ``smbclient``'s
process-wide, server-keyed session bookkeeping, so closing one ``SmbStore``
never tears down a connection a sibling instance is still using.

Requests are routed through a small pool of independent sessions
(``_DEFAULT_CONNECTION_POOL_SIZE``) rather than one shared session, because
an SMB2 connection's flow-control credit window starts at exactly 1 and
only grows as the server replies — two requests in flight at once on one
connection would race for that single credit.

Unlike ``S3Store``/``AzureStore``, an SMB share has real directory entities,
so ``listdir`` on a missing path raises ``NotFoundError`` (matching
``LocalFsStore``). ``smbprotocol`` also gives no timeout/retry past the
initial connect, so this module adds both itself
(``_DEFAULT_OPERATION_TIMEOUT``/``_DEFAULT_MAX_ATTEMPTS`` below).
"""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from ..errors import NotFoundError, PermissionDeniedError
from .base import join_path

if TYPE_CHECKING:
    import smbclient as _smbclient_module

_T = TypeVar("_T")

_DEFAULT_PORT = 445

# smbprotocol's own connect timeout (60s) is tuned for a batch job, not an
# interactive reachability probe — capped here the same way S3Store/AzureStore
# cap theirs, to a value the TUI's connect dialog can wait through.
_DEFAULT_CONNECTION_TIMEOUT = 5

# smbprotocol has no timeout after a successful connect (its socket becomes
# blocking with no timeout), so every method routes through _call_with_retry,
# which wraps the blocking call in asyncio.wait_for(timeout=this).
_DEFAULT_OPERATION_TIMEOUT = 15

# smbprotocol has no retry of its own; a retried operation drops its slot's
# session first (see _drop_slot_session) so the retry reconnects rather than
# reusing a connection already known to be stuck.
_DEFAULT_MAX_ATTEMPTS = 2

# Each slot is a real, separately-authenticated SMB session, costly to
# establish — far smaller than S3Store's _DEFAULT_MAX_POOL_CONNECTIONS (32),
# and deliberately kept below units/verify_reachable.py's own
# _MAX_CONCURRENT_BUCKET_CHECKS (8): FULL verify's multiprocess dispatch runs
# up to concurrency.default_worker_count() worker processes, each rebuilding
# its own pool of this size, so the real worst case is `workers * this
# constant` concurrent sessions against one server, not this constant alone.
_DEFAULT_CONNECTION_POOL_SIZE = 2

# Short pause before retrying a credit-exhaustion race (see
# _is_credit_exhaustion) — the connection itself is healthy, this only waits
# out a transient flow-control condition.
_CREDIT_RETRY_DELAY = 0.1

#: ``OSError.errno`` values ``smbclient`` raises for a missing path:
#: ``ENOENT``, ``ENOTDIR``, and ``EISDIR`` (opening a directory for a
#: byte-oriented ``read()``) — mapped to ``NotFoundError``, the same set
#: ``LocalFsStore`` maps from the equivalent builtin exceptions.
_NOT_FOUND_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.EISDIR})

#: ``OSError.errno`` for "exists but access denied" -> ``PermissionDeniedError``.
#: Deliberately not ``errno.EPERM``: ``smbclient``'s error-mapping table
#: (``smbprotocol.exceptions.SMBOSError.__init__``) uses ``EPERM``
#: exclusively for ``STATUS_SHARING_VIOLATION`` (a file locked by another
#: client — a transient conflict, not an access-control failure).
_PERMISSION_DENIED_ERRNOS = frozenset({errno.EACCES})

#: ``smbprotocol``'s ``NtStatus.STATUS_ACCESS_DENIED`` (0xC0000022), hardcoded
#: rather than imported: ``SMBOSError``'s own mapping table has no entry for
#: it, so it falls through to ``errno=0`` — a permission failure over real
#: SMB has to be recognized by this raw status code, not by ``errno`` alone.
_STATUS_ACCESS_DENIED = 0xC0000022


def _is_permission_denied(exc: OSError) -> bool:
    """Whether ``exc`` is a permission-denied condition: an errno
    ``smbclient`` maps to one, or the raw NT status it maps to no errno at
    all (``_STATUS_ACCESS_DENIED``). ``.ntstatus`` is ``SMBOSError``-specific,
    hence ``getattr`` with a default."""
    return exc.errno in _PERMISSION_DENIED_ERRNOS or getattr(exc, "ntstatus", None) == _STATUS_ACCESS_DENIED


def _is_credit_exhaustion(exc: Exception) -> bool:
    """Whether ``exc`` is smbprotocol's own SMB2 credit-window-exhaustion
    signal — a transient, connection-level condition, not real corruption.
    ``SMBException`` has no distinct subtype for this case, so this matches
    on the fixed message text ``Connection._send()`` always raises with."""
    from smbprotocol.exceptions import SMBException

    return isinstance(exc, SMBException) and "credits are available" in str(exc)


def _import_smbclient() -> Any:
    """Lazy ``import smbclient`` — shared by every method here so the
    choice of what to import lives in one place."""
    import smbclient

    return smbclient


def _raise_if_not_found(exc: OSError, path: str) -> None:
    """Raise ``NotFoundError`` when ``exc`` is one of the not-found-shaped
    errnos; otherwise return normally, leaving the caller's own bare
    ``raise`` to re-raise ``exc`` unchanged."""
    if exc.errno in _NOT_FOUND_ERRNOS:
        raise NotFoundError("no such path", ref=path) from exc


def _raise_if_permission_denied(exc: OSError, path: str) -> None:
    """Raise ``PermissionDeniedError`` when ``exc`` is a permission-denied
    condition (see ``_is_permission_denied``); otherwise return normally,
    leaving the caller's own bare ``raise`` to re-raise ``exc`` unchanged."""
    if _is_permission_denied(exc):
        raise PermissionDeniedError("permission denied", ref=path) from exc


class _ConnectionSlot:
    """One pooled SMB session's bookkeeping: ``connection_cache`` (passed to
    every ``smbclient`` call) and ``ready`` (whether ``register_session`` has
    succeeded for it yet). Needs no lock: the pool's semaphore + free-stack
    guarantee a slot is only ever checked out to one caller at a time."""

    __slots__ = ("connection_cache", "ready")

    def __init__(self) -> None:
        self.connection_cache: dict[Any, Any] = {}
        self.ready = False


class SmbStore:
    """One SMB share, addressed by ``"/"``-separated paths relative to the
    share root — the SMB equivalent of ``S3Store``'s bucket/``AzureStore``'s
    container.

    ``username`` accepts the Windows-native ``DOMAIN\\username`` (or
    ``user@domain`` UPN) form directly; ``smbprotocol``'s own NTLM/SPNEGO
    layer splits the domain back out. Leaving ``username``/``password``
    unset attempts an anonymous/guest session.

    Backed by a pool of independent SMB sessions (see module docstring),
    each created lazily on first checkout. A call checks out whichever slot
    is free, preferring one already connected via a LIFO free-stack so
    sequential calls reuse the same session, and holds it exclusively until
    that call (including its own retries) finishes.
    """

    def __init__(
        self,
        share: str,
        *,
        server: str,
        port: int = _DEFAULT_PORT,
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

    def __repr__(self) -> str:
        return f"SmbStore(server={self._server!r}, share={self._share!r})"

    def _unc(self, path: str) -> str:
        combined = join_path(path).replace("/", "\\")
        base = f"\\\\{self._server}\\{self._share}"
        return f"{base}\\{combined}" if combined else base

    async def _acquire_slot(self) -> _ConnectionSlot:
        """Waits for a free slot and pops one off the LIFO free-stack.
        Always paired with ``_release_slot`` in a ``finally`` by
        ``_call_with_retry``, the only caller."""
        await self._pool_semaphore.acquire()
        return self._free_slots.pop()

    def _release_slot(self, slot: _ConnectionSlot) -> None:
        self._free_slots.append(slot)
        self._pool_semaphore.release()

    async def _ensure_slot_session(self, slot: _ConnectionSlot) -> Any:
        """The registered SMB session for ``slot``, created on first use.
        Needs no lock: only ever called while ``slot`` is checked out
        exclusively to the current caller.

        The connect attempt is retried up to ``_DEFAULT_MAX_ATTEMPTS`` times
        on either ``ValueError``/``OSError`` (``smbprotocol`` gives no way to
        distinguish "worth retrying" from "never will be")."""
        smbclient = _import_smbclient()
        if not slot.ready:
            last_exc: Exception | None = None
            for _attempt in range(_DEFAULT_MAX_ATTEMPTS):
                try:
                    await asyncio.to_thread(
                        smbclient.register_session,
                        self._server,
                        username=self._username,
                        password=self._password,
                        port=self._port,
                        connection_timeout=_DEFAULT_CONNECTION_TIMEOUT,
                        connection_cache=slot.connection_cache,
                    )
                    slot.ready = True
                    break
                except (OSError, ValueError) as exc:
                    last_exc = exc
            else:
                assert last_exc is not None  # the loop above only exits without `break` via this path
                raise last_exc
        return smbclient

    def _drop_slot_session(self, slot: _ConnectionSlot) -> None:
        """Discard ``slot``'s session/``connection_cache`` without a
        graceful close — the next attempt re-establishes a fresh session via
        ``_ensure_slot_session()`` instead of reusing a connection already
        known to be stuck."""
        slot.ready = False
        slot.connection_cache = {}

    async def _call_with_retry(self, fn: Callable[[dict[Any, Any]], _T]) -> _T:
        """Checks out one pooled slot and runs ``fn`` (each public method
        below passes its own ``_*_sync`` call, bound to its own
        path/offset/length arguments) in a worker thread, bounded by
        ``_DEFAULT_OPERATION_TIMEOUT``. Retried up to ``_DEFAULT_MAX_ATTEMPTS``
        times on a timeout (drops the slot's session first) or a credit-
        exhaustion race (session kept, see ``_is_credit_exhaustion``). The
        slot is always released back to the pool in a ``finally``.
        """
        slot = await self._acquire_slot()
        try:
            last_exc: Exception | None = None
            for _attempt in range(_DEFAULT_MAX_ATTEMPTS):
                await self._ensure_slot_session(slot)
                cache = slot.connection_cache
                try:
                    return await asyncio.wait_for(asyncio.to_thread(fn, cache), timeout=_DEFAULT_OPERATION_TIMEOUT)
                except TimeoutError as exc:
                    last_exc = exc
                    self._drop_slot_session(slot)
                except Exception as exc:
                    if not _is_credit_exhaustion(exc):
                        raise
                    last_exc = exc
                    await asyncio.sleep(_CREDIT_RETRY_DELAY)
            assert last_exc is not None  # the loop above only exits without returning via this path
            raise last_exc
        finally:
            self._release_slot(slot)

    async def aclose(self) -> None:
        """Tear down every pooled slot's SMB session — safe to call more
        than once, and scoped to this instance's own slots (never disturbs
        a sibling ``SmbStore`` on the same server).

        Acquires every slot's checkout permit first, so an in-flight
        ``_call_with_retry`` caller can't have its slot torn down mid-call;
        releases every permit afterward, so the store is still usable (a
        later call simply re-establishes whichever sessions it needs)."""
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
                await asyncio.to_thread(
                    smbclient.delete_session,
                    self._server,
                    port=self._port,
                    connection_cache=slot.connection_cache,
                )
        finally:
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

    async def listdir(self, path: str) -> list[str]:
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
            # share_access="rwd": mode="rb"'s own default only implies
            # FILE_SHARE_READ, which would raise a spurious
            # STATUS_SHARING_VIOLATION against a file open for writing
            # elsewhere (this is an offline reader, never the only client).
            with smbclient.open_file(unc, mode="rb", share_access="rwd", connection_cache=connection_cache) as f:
                if offset:
                    f.seek(offset)
                return f.read() if length is None else f.read(length)  # type: ignore[no-any-return]
        except OSError as exc:
            _raise_if_not_found(exc, path)
            _raise_if_permission_denied(exc, path)
            raise

    def _size_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> int:
        unc = self._unc(path)
        try:
            return int(smbclient.stat(unc, connection_cache=connection_cache).st_size)
        except OSError as exc:
            _raise_if_not_found(exc, path)
            _raise_if_permission_denied(exc, path)
            raise

    def _exists_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> bool:
        try:
            return bool(smbclient.path.exists(self._unc(path), connection_cache=connection_cache))
        except OSError as exc:
            # A probe that can raise on a denied sibling would abort a
            # caller's whole candidate search, so this reads as "not found"
            # instead — matching LocalFsStore's own _exists_sync.
            if _is_permission_denied(exc):
                return False
            raise

    def _listdir_sync(self, smbclient: _smbclient_module, path: str, connection_cache: dict[Any, Any]) -> list[str]:
        unc = self._unc(path)
        try:
            return sorted(smbclient.listdir(unc, connection_cache=connection_cache))
        except OSError as exc:
            _raise_if_not_found(exc, path)
            _raise_if_permission_denied(exc, path)
            raise
