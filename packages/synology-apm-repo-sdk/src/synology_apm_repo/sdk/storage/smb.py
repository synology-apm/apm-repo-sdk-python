"""``SmbStore`` — a real, executable ``ObjectStore`` implementation for a
repository reached over the MS-SMB protocol (SMB2/3) rather than a
locally-mounted share.

``smbprotocol`` is imported lazily, inside ``__init__`` and each method,
rather than at module scope — the same rationale ``storage/s3.py``'s module
docstring gives for ``aioboto3``: it's always installed (a required
dependency), but this module is imported unconditionally by
``storage/__init__.py``, so a module-level import would make every caller
of ``storage`` pay for it even when it never touches SMB.

Unlike ``S3Store``/``AzureStore``, ``smbprotocol`` has no native-async
surface (no ``aiohttp`` foundation to sit on) — its ``smbclient`` submodule
is a synchronous, ``os``-shaped API (``open_file``/``stat``/``listdir``,
file objects with ``seek``/``read``). This store's four methods are thin
``asyncio.to_thread()`` wrappers around that synchronous body, the same
shape ``LocalFsStore`` uses for ``os.pread``/``os.fstat``/``iterdir`` —
not the native-async pattern the other two network backends use.

``smbclient``'s session/connection bookkeeping is process-wide by default
(keyed by server/port), which would let ``aclose()`` on one ``SmbStore``
tear down a connection a sibling instance sharing the same server is still
using. Every call here passes its own ``connection_cache`` dict instead,
scoping each underlying SMB session to this instance alone — the same
per-instance isolation ``S3Store``/``AzureStore`` get for free from owning
their own client object.

This store keeps a small *pool* of independent sessions
(``_DEFAULT_CONNECTION_POOL_SIZE``), not one shared session — see
``_ConnectionSlot`` and ``_call_with_retry`` for why: an SMB2 connection's
own client-side flow-control window (``smbprotocol``'s "credits") starts at
exactly 1 and only grows as the server grants more back on replies, so any
two requests genuinely in flight at once on *one* connection race for that
single credit and one of them gets a raised ``smbprotocol.exceptions.
SMBException``. Rather than track that live, per-connection credit count
(which would mean reaching into ``smbprotocol`` internals this module
otherwise stays well clear of), every operation is instead routed through
one of a small number of independent connections, each never handling more
than one in-flight request at a time — real, safe parallelism up to the
pool size, without ever needing to know how large any one connection's
window currently is.

One contract difference from ``LocalFsStore``, in the other direction from
``S3Store``/``AzureStore``: an SMB share has real directory entities (like
a local filesystem, unlike an object store's prefixes), so ``listdir`` on
a missing path raises ``NotFoundError``, matching ``LocalFsStore`` rather than
the S3/Azure prefix-probing behavior.

Unlike ``S3Store``/``AzureStore``, ``smbprotocol`` gives this module no
timeout/retry knobs of its own to configure past the initial connect —
a stalled share would otherwise block a call forever, and neither a
connect nor a later operation failure carries any signal distinguishing
"worth retrying" from "never will be" — so this module adds both itself,
via ``_DEFAULT_OPERATION_TIMEOUT``/``_DEFAULT_MAX_ATTEMPTS`` below.
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

# smbprotocol's own default connect timeout (60s, smbclient.register_session's
# own connection_timeout parameter) is tuned for a long-running batch job, not
# an interactive "is this share even reachable" probe — S3Store/AzureStore cap
# their own long batch-job default timeouts the same way, down to a value an
# interactive caller (the TUI's connect dialog, in particular) can actually
# wait through when an endpoint is unreachable.
_DEFAULT_CONNECTION_TIMEOUT = 5

# smbprotocol has no timeout of its own for anything *after* a successful
# connect: its underlying socket is switched to blocking mode with no
# timeout the moment connect() succeeds
# (smbprotocol.transport.Tcp.connect()'s own self._sock.settimeout(None)),
# so a share that stops responding mid-operation would otherwise block a
# real open_file()/read()/stat()/listdir() call forever, unlike
# S3Store/AzureStore (whose own read_timeout already covers this). Every
# one of this store's four methods routes through _call_with_retry, which
# wraps the blocking call in asyncio.wait_for(timeout=this) instead — the
# calling coroutine gets its own bounded wait even though the underlying
# blocked worker thread itself can't be cancelled (the same trade-off any
# asyncio.to_thread() call around a library with no native cancellation
# has) and is simply abandoned until its own call eventually returns or
# the process exits.
_DEFAULT_OPERATION_TIMEOUT = 15

# smbprotocol has no retry mechanism of its own for either the initial
# connect or a later operation — both go through this store's own retry
# loop instead, matching storage/s3.py's own _DEFAULT_MAX_ATTEMPTS value.
# A retried operation always drops its own slot's session first (see
# _drop_slot_session) so the retry reconnects rather than reusing a
# connection already known to be stuck.
_DEFAULT_MAX_ATTEMPTS = 2

# Each slot is a real, separately-authenticated SMB session — costly to
# establish, unlike a pooled HTTP socket — so this stays far smaller than
# S3Store's _DEFAULT_MAX_POOL_CONNECTIONS (32). It's also deliberately kept
# below units/verify_reachable.py's own _MAX_CONCURRENT_BUCKET_CHECKS (8):
# FULL verify's multiprocess dispatch already runs up to
# concurrency.default_worker_count() (commonly 8) separate worker
# processes for a describable store like this one, each independently
# rebuilding its *own* pool of this size, so the real worst case for one
# repository's FULL verify is `workers * this constant` concurrent
# sessions against the same physical server, not this constant alone.
_DEFAULT_CONNECTION_POOL_SIZE = 2

# A short pause before retrying a credit-exhaustion race (see
# _is_credit_exhaustion) rather than immediately re-attempting — gives
# whatever raced this store's own request for the connection's sole
# starting credit a moment to return it. Deliberately much shorter than a
# real reconnect: the connection itself is healthy here, this is only ever
# waiting out a transient flow-control condition.
_CREDIT_RETRY_DELAY = 0.1

#: ``OSError.errno`` values ``smbclient`` raises for "this path doesn't
#: exist"-shaped failures — ``ENOENT`` (missing path), ``ENOTDIR`` (a path
#: component, or the ``listdir`` target itself, isn't a directory), and
#: ``EISDIR`` (opening a directory for a byte-oriented ``read()``). All three
#: are this contract's own ``NotFoundError``, the same set ``LocalFsStore``
#: maps from the equivalent builtin exception types.
_NOT_FOUND_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.EISDIR})

#: ``OSError.errno`` values for "the path exists but access was denied"
#: (share/file permissions, or a credential without list/read rights) —
#: this contract's own ``PermissionDeniedError``, the same distinction
#: ``LocalFsStore`` draws from the equivalent builtin ``PermissionError``.
#: Deliberately not ``errno.EPERM``: ``smbclient``'s own error-mapping table
#: (``smbprotocol.exceptions.SMBOSError.__init__``) uses ``EPERM``
#: exclusively for ``STATUS_SHARING_VIOLATION`` (a file locked by another
#: client, a transient conflict, not an access-control failure) — folding
#: it in here would mislabel that case.
_PERMISSION_DENIED_ERRNOS = frozenset({errno.EACCES})

#: ``smbprotocol``'s own ``NtStatus.STATUS_ACCESS_DENIED`` (0xC0000022) —
#: hardcoded rather than importing ``smbprotocol.header.NtStatus`` at
#: module scope, the same lazy-import policy the module docstring above
#: gives for ``smbprotocol`` itself (and the same trade-off
#: ``storage/local.py``'s
#: ``_DARWIN_F_RDADVISE`` constant already makes). This is the actual NT
#: status an SMB server sends for a plain "access denied" (an unlistable
#: directory's real-world response) — yet ``SMBOSError``'s own mapping
#: table above has no entry for it at all, so it falls through to that
#: table's default and comes back as ``errno=0``. A permission failure
#: over real SMB therefore has to be recognized by this raw status code,
#: not by ``errno`` alone.
_STATUS_ACCESS_DENIED = 0xC0000022


def _is_permission_denied(exc: OSError) -> bool:
    """Whether ``exc`` is a permission-denied condition: either an errno
    ``smbclient`` does map to one (``EACCES``, from
    ``STATUS_PRIVILEGE_NOT_HELD``), or the raw NT status it currently maps
    to no errno at all (``STATUS_ACCESS_DENIED`` — see
    ``_STATUS_ACCESS_DENIED``'s comment above for why that one has no
    errno). ``.ntstatus`` is ``SMBOSError``-specific, not a plain
    ``OSError`` attribute — absent on any other ``OSError`` this might be,
    hence ``getattr`` with a default rather than a plain attribute
    access."""
    return exc.errno in _PERMISSION_DENIED_ERRNOS or getattr(exc, "ntstatus", None) == _STATUS_ACCESS_DENIED


def _is_credit_exhaustion(exc: Exception) -> bool:
    """Whether ``exc`` is smbprotocol's own SMB2 credit-window-exhaustion
    signal (``Connection._send()``'s own flow-control check) — a transient,
    connection-level condition, not real corruption. ``SMBException`` (a
    plain ``Exception`` subclass, not an ``OSError``) has no distinct
    subtype or attribute for this specific case, so — like
    ``_is_permission_denied`` above matching on a raw NT status where
    ``smbprotocol`` gives no dedicated errno — this matches on the fixed
    message text ``_send()`` always raises with. Each pooled slot normally
    handles one request at a time — the pool exists specifically so no two
    requests ever race the same connection's single starting credit — so
    this should be rare in practice; it's a defense-in-depth catch for whatever
    the pool doesn't cover (a fresh connection's own background
    keepalive/echo traffic racing this store's first real request on it,
    say), not the primary fix."""
    from smbprotocol.exceptions import SMBException

    return isinstance(exc, SMBException) and "credits are available" in str(exc)


def _import_smbclient() -> Any:
    """Lazy ``import smbclient``, same rationale as the module docstring
    above. Shared by every method here so the choice of what to import
    lives in one place."""
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
    """One pooled SMB session's own bookkeeping: ``connection_cache`` (the
    per-session dict passed to every ``smbclient`` call) and ``ready``
    (whether ``register_session`` has succeeded for it yet).

    Needs no lock of its own to guard this state: ``SmbStore._acquire_slot``/
    ``._release_slot`` (via the pool's semaphore + free-stack) guarantee a
    slot is only ever checked out to one caller at a time, so no two callers
    can ever race on the same slot's ``ready``/``connection_cache``
    concurrently."""

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
    layer splits the domain back out of that single string, so this class
    has no separate ``domain`` field to keep in sync with it. Leaving
    ``username``/``password`` unset attempts an anonymous/guest session,
    same as leaving S3/Azure's credential fields unset falls back to their
    own ambient credential chains.

    Backed by a small pool of ``_DEFAULT_CONNECTION_POOL_SIZE`` independent
    SMB sessions, not one shared session — an SMB2 connection's own
    client-side flow-control window starts at exactly 1, so two requests
    in flight at once on one connection would race for that single
    credit — each created lazily on first checkout rather than in
    ``__init__`` (which does no I/O). A call checks out whichever slot is
    currently free — preferring one already connected, via a LIFO
    free-stack, so sequential, non-overlapping calls keep reusing the same
    session instead of needlessly spreading across the whole pool — and
    holds it exclusively until that call (including any of its own
    retries) finishes.
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
        """Waits for a free slot (bounded by ``_pool_semaphore``) and pops
        one off the LIFO free-stack — see the class docstring for why LIFO,
        not round-robin. Always paired with ``_release_slot`` in a
        ``finally`` by ``_call_with_retry``, the only caller."""
        await self._pool_semaphore.acquire()
        return self._free_slots.pop()

    def _release_slot(self, slot: _ConnectionSlot) -> None:
        self._free_slots.append(slot)
        self._pool_semaphore.release()

    async def _ensure_slot_session(self, slot: _ConnectionSlot) -> Any:
        """The registered SMB session for ``slot``, created on first use.
        Needs no lock: a slot is only ever passed in here by
        ``_call_with_retry`` while that slot is checked out exclusively to
        the current caller, so no other caller can be concurrently
        touching the same slot's ``ready``/``connection_cache``.

        The connect attempt itself is retried up to ``_DEFAULT_MAX_ATTEMPTS``
        times — ``smbprotocol`` raises a plain ``ValueError``/``OSError``
        for a connect timeout/failure (``smbprotocol.transport.Tcp.connect``),
        with nothing further downstream distinguishing "worth retrying"
        from "never will be," so this retries either kind alike, the same
        blanket policy ``storage/s3.py``'s own ``max_attempts`` applies."""
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
        """Discard ``slot``'s own session/``connection_cache`` without
        attempting a graceful close — used by ``_call_with_retry`` after a
        timed-out operation, whose underlying connection may itself be
        unable to complete a clean ``smbclient.delete_session()``
        round-trip (the same reason the operation timed out in the first
        place). The next attempt on this slot re-establishes a fresh
        session via ``_ensure_slot_session()`` instead of reusing a
        connection already known to be stuck."""
        slot.ready = False
        slot.connection_cache = {}

    async def _call_with_retry(self, fn: Callable[[dict[Any, Any]], _T]) -> _T:
        """Checks out one pooled slot (see ``_acquire_slot``) and runs
        ``fn`` — each public method below passes its own ``_*_sync`` call,
        already bound to its own path/offset/length arguments, as a closure
        taking just the connection cache to use — in a worker thread,
        bounded by ``_DEFAULT_OPERATION_TIMEOUT``. Retried up to
        ``_DEFAULT_MAX_ATTEMPTS`` times on either a timeout (which also
        drops the slot's session, so the retry reconnects rather than
        reusing a connection already known to be stuck) or a credit-
        exhaustion race (see ``_is_credit_exhaustion`` — the session itself
        is fine there, so it's kept, just after a short pause). The slot is
        always released back to the pool in a ``finally``, whether this
        call ultimately succeeds or exhausts every attempt.
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
        """Tear down every pooled slot's own SMB session that was ever
        used — safe to call more than once, and scoped to this instance's
        own slots alone (each with its own private ``connection_cache``,
        rather than sharing ``smbclient``'s process-wide, server-keyed
        session bookkeeping), so it never disturbs another
        ``SmbStore`` sharing the same server.

        Acquires every slot's own checkout permit first (the same
        semaphore ``_acquire_slot`` uses, drained down to zero free slots)
        before touching any slot's ``ready``/``connection_cache`` — a slot
        still checked out to an in-flight ``_call_with_retry`` caller (mid
        ``_ensure_slot_session``, or mid read/stat/listdir on an already
        -established one) can't have its own permit acquired here until
        that caller releases it, so this never tears a slot down out from
        under one. Releases every permit back afterward, so this store is
        still usable (a later call simply re-establishes whichever
        sessions it needs) rather than left permanently unusable."""
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
            # share_access="rwd": smbclient.open_file()'s own default for
            # mode="rb" only implies FILE_SHARE_READ, unlike its stat()/
            # listdir() (which already request full "rwd" sharing) - a
            # live repository can have a file open for writing elsewhere
            # (this is an offline *reader*, never the only client touching
            # it), and a narrower share mode here would raise a spurious
            # STATUS_SHARING_VIOLATION purely from this store's own
            # unnecessarily strict open request, not a real conflict.
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
