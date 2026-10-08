"""Unit tests for ``synology_apm_repo.sdk.storage.smb``: what is specific to
``SmbStore`` (lazy-import guard, connection-pool lifecycle, per-instance
``connection_cache`` isolation, connect/operation retries, error-code
mapping, UNC paths). The generic ``ObjectStore``
contract is in ``test_storage_object_store_contract.py``. Backed by a fake
``smbclient`` module, no network."""

from __future__ import annotations

import asyncio
import errno
import sys
import threading
from typing import Any

import pytest
import smbclient
from smbprotocol.exceptions import SMBAuthenticationError, SMBException

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError, StorageBackendError
from synology_apm_repo.sdk.storage import smb as smb_module
from synology_apm_repo.sdk.storage.base import NETWORK_CONNECT_TIMEOUT, Entry
from synology_apm_repo.sdk.storage.smb import SmbStore


class TestLazyImportPropagatesImportError:
    def test_construction_does_not_import_smbclient(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Building a ``SmbStore`` does no I/O and imports nothing; only a method
        that touches the share does."""
        monkeypatch.setitem(sys.modules, "smbclient", None)
        store = SmbStore("share", server="host", username="user", password="pw")
        assert store._share == "share"

    async def test_raises_importerror_when_smbclient_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``sys.modules["smbclient"] = None`` makes ``import smbclient`` raise
        ``ImportError``, simulating a missing install."""
        monkeypatch.setitem(sys.modules, "smbclient", None)
        store = SmbStore("share", server="host")
        with pytest.raises(ImportError, match="import of smbclient halted"):
            await store.size("f.txt")


@unchecked_fake("the smbclient module")
class _FakeSmbClientModule:
    """The subset of the real ``smbclient`` module's functions ``SmbStore`` calls,
    as plain methods (they are synchronous)."""

    def __init__(
        self,
        files: dict[str, bytes] | None = None,
        *,
        fail_with: OSError | None = None,
        register_fail_count: int = 0,
        register_fail_exc: Exception | None = None,
        stuck: bool = False,
        stuck_only_on_first_connection: bool = False,
        credit_exhaustion_fail_count: int = 0,
        rendezvous: int = 0,
    ) -> None:
        self._files = files or {}
        self._fail_with = fail_with
        self.path = self
        self.register_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        # -- connect-retry knobs (TestConnectRetry) --
        self._register_fail_count = register_fail_count
        self._register_fail_exc = register_fail_exc or OSError("simulated connect failure")
        self._register_attempts = 0
        # -- operation-timeout/retry knobs (TestOperationTimeoutAndRetry) --
        self._stuck = stuck
        self._stuck_only_on_first_connection = stuck_only_on_first_connection
        #: Releases every stuck call; a test sets it before it ends.
        self.unstick = threading.Event()
        self._first_connection_cache: object | None = None
        # -- concurrency-tracking knob (TestConnectionPool) --
        self._current_concurrent = 0
        self.max_concurrent_seen = 0
        self._rendezvous = threading.Barrier(rendezvous) if rendezvous else None
        self._rendezvous_left = rendezvous
        self._rendezvous_lock = threading.Lock()
        # -- credit-exhaustion knob (TestCreditExhaustionRetry) --
        self._credit_exhaustion_fail_remaining = credit_exhaustion_fail_count

    def register_session(self, server: str, **kwargs: Any) -> None:
        self.register_calls.append({"server": server, **kwargs})
        self._register_attempts += 1
        if self._register_attempts <= self._register_fail_count:
            raise self._register_fail_exc
        if self._first_connection_cache is None:
            self._first_connection_cache = kwargs.get("connection_cache")

    def _maybe_block(self, connection_cache: object) -> None:
        """With ``stuck``, parks the calling (worker) thread until ``unstick``
        is set: every call, or with ``stuck_only_on_first_connection`` only
        calls still on the first registered connection (so a retry on a fresh
        session succeeds at once).

        With ``rendezvous=N``, the first ``N`` calls each wait until all ``N``
        are in flight at once; ``max_concurrent_seen`` is the high-water mark
        of calls in flight. If the store serializes them instead, the barrier
        times out and the call raises."""
        if self._stuck and not (
            self._stuck_only_on_first_connection and connection_cache is not self._first_connection_cache
        ):
            self.unstick.wait(30)
        if self._rendezvous is None:
            return
        with self._rendezvous_lock:
            self._current_concurrent += 1
            self.max_concurrent_seen = max(self.max_concurrent_seen, self._current_concurrent)
            meet = self._rendezvous_left > 0
            self._rendezvous_left -= 1
        if meet:
            self._rendezvous.wait(timeout=10)
        with self._rendezvous_lock:
            self._current_concurrent -= 1

    def delete_session(self, server: str, **kwargs: Any) -> None:
        self.delete_calls.append({"server": server, **kwargs})

    def _content_or_raise(self, unc: str) -> bytes:
        if self._fail_with is not None:
            raise self._fail_with
        name = unc.rsplit("\\", 1)[-1]
        content = self._files.get(name)
        if content is None:
            raise OSError(errno.ENOENT, "no such file", unc)
        return content

    def open_file(self, unc: str, mode: str = "rb", **kwargs: Any) -> Any:
        content = self._content_or_raise(unc)

        class _File:
            def __enter__(self_inner) -> Any:
                return self_inner

            def __exit__(self_inner, *exc_info: object) -> None:
                return None

            def read(self_inner, length: int | None = None) -> bytes:
                return content if length is None else content[:length]

            def seek(self_inner, offset: int) -> None:
                return None

        return _File()

    def stat(self, unc: str, **kwargs: Any) -> Any:
        if self._credit_exhaustion_fail_remaining > 0:
            self._credit_exhaustion_fail_remaining -= 1
            raise SMBException("Request requires 1 credits but only 0 credits are available")
        self._maybe_block(kwargs.get("connection_cache"))
        content = self._content_or_raise(unc)

        class _Stat:
            st_size = len(content)

        return _Stat()

    def scandir(self, unc: str, **kwargs: Any) -> list[Any]:
        if self._fail_with is not None:
            raise self._fail_with
        files = self._files

        class _Entry:
            def __init__(self_inner, name: str) -> None:
                self_inner.name = name

            def is_dir(self_inner) -> bool:
                return self_inner.name.endswith("/")

            def stat(self_inner) -> Any:
                class _Stat:
                    st_size = len(files[self_inner.name])

                return _Stat()

        return [_Entry(name) for name in files]

    def exists(self, unc: str, **kwargs: Any) -> bool:
        # Like smbclient.path.exists(): False for a missing path, any other OSError propagates.
        if self._fail_with is None:
            return True
        if self._fail_with.errno == errno.ENOENT:
            return False
        raise self._fail_with


def _patch_smbclient(monkeypatch: pytest.MonkeyPatch, fake: _FakeSmbClientModule) -> None:
    monkeypatch.setattr(smbclient, "register_session", fake.register_session)
    monkeypatch.setattr(smbclient, "delete_session", fake.delete_session)
    monkeypatch.setattr(smbclient, "open_file", fake.open_file)
    monkeypatch.setattr(smbclient, "stat", fake.stat)
    monkeypatch.setattr(smbclient, "scandir", fake.scandir)
    monkeypatch.setattr(smbclient.path, "exists", fake.exists)


class TestSessionLifecycle:
    async def test_session_is_registered_once_and_reused_across_calls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host", username="user", password="pw")

        await store.size("f.txt")
        await store.size("f.txt")

        assert len(fake.register_calls) == 1
        assert fake.register_calls[0]["server"] == "host"
        assert fake.register_calls[0]["username"] == "user"
        assert fake.register_calls[0]["password"] == "pw"

    async def test_close_is_idempotent_and_a_no_op_before_any_use(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await store.close()  # never used - must not register/delete anything
        assert fake.register_calls == []
        assert fake.delete_calls == []

        await store.size("f.txt")
        await store.close()
        await store.close()  # idempotent
        assert len(fake.delete_calls) == 1

    async def test_each_store_uses_its_own_private_connection_cache(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``smbclient``'s session cache is process-wide by default, so every call
        passes this store's own ``connection_cache``."""
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        a = SmbStore("share", server="host")
        b = SmbStore("share", server="host")

        await a.size("f.txt")
        await b.size("f.txt")

        cache_a = fake.register_calls[0]["connection_cache"]
        cache_b = fake.register_calls[1]["connection_cache"]
        assert cache_a is a._slots[-1].connection_cache
        assert cache_b is b._slots[-1].connection_cache
        assert cache_a is not cache_b


class TestConnectRetry:
    """``_ensure_slot_session()``'s own retry loop around
    ``smbclient.register_session`` — smbprotocol has no retry mechanism of
    its own for a connect failure."""

    async def test_connection_timeout_is_passed_to_register_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await store.size("f.txt")

        assert fake.register_calls[0]["connection_timeout"] == NETWORK_CONNECT_TIMEOUT

    async def test_a_transient_connect_failure_is_retried_and_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, register_fail_count=1)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        assert await store.size("f.txt") == 5
        assert len(fake.register_calls) == 2  # one failed attempt, one that succeeded

    async def test_connect_failure_raises_after_exhausting_all_attempts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, register_fail_count=smb_module._DEFAULT_MAX_ATTEMPTS)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(StorageBackendError, match="simulated connect failure") as excinfo:
            await store.size("f.txt")
        assert isinstance(excinfo.value.__cause__, OSError)
        assert len(fake.register_calls) == smb_module._DEFAULT_MAX_ATTEMPTS

    async def test_a_rejected_logon_is_a_permission_denied_error_without_a_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rejected = SMBAuthenticationError("bad credentials")
        fake = _FakeSmbClientModule(
            {"f.txt": b"hello"}, register_fail_count=smb_module._DEFAULT_MAX_ATTEMPTS, register_fail_exc=rejected
        )
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(PermissionDeniedError, match="bad credentials") as excinfo:
            await store.size("f.txt")
        assert excinfo.value.__cause__ is rejected
        assert len(fake.register_calls) == 1


_STUCK_TIMEOUT = 0.5
"""The operation timeout a retry test patches in when a later attempt
succeeds: long enough that a fast fake operation (a thread round-trip)
beats it on a heavily loaded machine."""

_ALWAYS_STUCK_TIMEOUT = 0.01
"""The operation timeout when every attempt is stuck: no fast call has to
beat it."""


class TestOperationTimeoutAndRetry:
    """``_call_with_retry()``: every operation past the initial connect is
    bounded by ``NETWORK_READ_TIMEOUT`` and retried up to ``_DEFAULT_MAX_ATTEMPTS``
    times, since ``smbprotocol`` can't bound or retry a single call itself."""

    async def test_a_stuck_operation_times_out_reconnects_and_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "NETWORK_READ_TIMEOUT", _STUCK_TIMEOUT)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, stuck=True, stuck_only_on_first_connection=True)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        try:
            assert await store.size("f.txt") == 5  # 1st attempt times out; 2nd (fresh connection) succeeds fast
        finally:
            fake.unstick.set()
        assert len(fake.register_calls) == 2

    async def test_a_stuck_operation_drops_the_old_connection_cache_before_retrying(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "NETWORK_READ_TIMEOUT", _STUCK_TIMEOUT)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, stuck=True, stuck_only_on_first_connection=True)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        used_slot = store._slots[-1]  # the only slot a sequential, non-concurrent call ever checks out
        original_cache = used_slot.connection_cache

        try:
            await store.size("f.txt")
        finally:
            fake.unstick.set()

        assert used_slot.connection_cache is not original_cache

    async def test_a_persistently_stuck_operation_raises_timeout_after_exhausting_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "NETWORK_READ_TIMEOUT", _ALWAYS_STUCK_TIMEOUT)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, stuck=True)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        try:
            with pytest.raises(StorageBackendError, match="SMB request failed") as excinfo:
                await store.size("f.txt")
        finally:
            fake.unstick.set()
        assert isinstance(excinfo.value.__cause__, TimeoutError)
        assert len(fake.register_calls) == smb_module._DEFAULT_MAX_ATTEMPTS

    async def test_a_stuck_operation_holds_a_thread_of_the_stores_own_pool_not_the_default_executor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A timed-out thread keeps running; on the loop's default executor it
        would starve every other ``to_thread()`` caller."""
        monkeypatch.setattr(smb_module, "NETWORK_READ_TIMEOUT", _ALWAYS_STUCK_TIMEOUT)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, stuck=True)
        _patch_smbclient(monkeypatch, fake)
        thread_names: list[str] = []
        real_stat = fake.stat

        def recording_stat(unc: str, **kwargs: Any) -> Any:
            thread_names.append(threading.current_thread().name)
            return real_stat(unc, **kwargs)

        monkeypatch.setattr(smbclient, "stat", recording_stat)
        store = SmbStore("share", server="host")

        try:
            with pytest.raises(StorageBackendError, match="SMB request failed"):
                await store.size("f.txt")
            await store.close()
        finally:
            fake.unstick.set()

        assert len(thread_names) == smb_module._DEFAULT_MAX_ATTEMPTS  # one stuck stat() per attempt
        assert all(name.startswith("SmbStore") for name in thread_names)
        assert store._executor is None


class TestConnectionPool:
    """The connection pool (``_DEFAULT_CONNECTION_POOL_SIZE`` independent
    ``_ConnectionSlot`` instances): an SMB2 connection's credit window starts at 1, so
    each slot serves one caller at a time, with up to the pool size concurrently."""

    async def test_concurrent_calls_use_independent_slots_and_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Two overlapping calls each get their own slot, ``connection_cache`` and
        ``register_session`` call."""
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, rendezvous=2)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await asyncio.gather(store.size("f.txt"), store.size("f.txt"))

        assert len(fake.register_calls) == 2
        cache_a = fake.register_calls[0]["connection_cache"]
        cache_b = fake.register_calls[1]["connection_cache"]
        assert cache_a is not cache_b
        assert fake.max_concurrent_seen == 2

    async def test_pool_bounds_real_concurrency_and_queues_the_rest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """More concurrent callers than slots all complete, never more than the
        pool size at once."""
        monkeypatch.setattr(smb_module, "_DEFAULT_CONNECTION_POOL_SIZE", 2)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, rendezvous=2)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        results = await asyncio.gather(store.size("f.txt"), store.size("f.txt"), store.size("f.txt"))

        assert list(results) == [5, 5, 5]
        assert fake.max_concurrent_seen == 2
        assert len(fake.register_calls) == 2  # only 2 slots ever get created, the 3rd call reuses one


class TestCreditExhaustionRetry:
    """``_call_with_retry()``'s second retryable condition: an ``SMBException``
    matching the SMB2 credit-exhaustion message (``_is_credit_exhaustion``).
    Unlike a timeout it does not reconnect: the connection is healthy."""

    async def test_a_credit_exhaustion_error_is_retried_and_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_CREDIT_RETRY_DELAY", 0.0)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, credit_exhaustion_fail_count=1)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        assert await store.size("f.txt") == 5
        assert len(fake.register_calls) == 1

    async def test_a_persistent_credit_exhaustion_error_raises_after_exhausting_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_CREDIT_RETRY_DELAY", 0.0)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, credit_exhaustion_fail_count=smb_module._DEFAULT_MAX_ATTEMPTS)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(StorageBackendError, match="credits are available") as excinfo:
            await store.size("f.txt")
        assert isinstance(excinfo.value.__cause__, SMBException)
        assert len(fake.register_calls) == 1

    async def test_an_unrelated_exception_is_not_treated_as_credit_exhaustion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unrelated ``SMBException`` (or any other exception) fails immediately."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EBUSY, "device or resource busy"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(StorageBackendError, match="device or resource busy"):
            await store.size("f.txt")


class TestCloseTearsDownEverySlot:
    async def test_close_tears_down_every_slot_actually_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smb_module, "_DEFAULT_CONNECTION_POOL_SIZE", 2)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, rendezvous=2)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await asyncio.gather(store.size("f.txt"), store.size("f.txt"))
        assert len(fake.register_calls) == 2  # both slots now in use

        await store.close()

        assert len(fake.delete_calls) == 2
        assert all(not slot.ready for slot in store._slots)

    async def test_close_waits_for_an_in_flight_call_before_tearing_down_its_slot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``size()`` already holding its slot finishes before ``close()`` tears
        that slot's session down."""
        events: list[str] = []
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        in_stat, release = threading.Event(), threading.Event()
        real_stat = fake.stat

        def gated_stat(unc: str, **kwargs: Any) -> Any:
            in_stat.set()
            release.wait(10)
            return real_stat(unc, **kwargs)

        monkeypatch.setattr(smbclient, "stat", gated_stat)
        store = SmbStore("share", server="host")

        async def slow_call() -> None:
            await store.size("f.txt")
            events.append("call_done")

        async def close_call() -> None:
            await store.close()
            events.append("close_done")

        try:
            calling = asyncio.create_task(slow_call())
            assert await asyncio.to_thread(in_stat.wait, 10)  # slow_call holds its slot
            closing = asyncio.create_task(close_call())
            await asyncio.sleep(0)  # close takes every free slot without suspending, then waits on the held one
            assert not closing.done()
            assert fake.delete_calls == []
        finally:
            release.set()
        await asyncio.wait_for(asyncio.gather(calling, closing), 10)

        assert events == ["call_done", "close_done"]
        assert len(fake.delete_calls) == 1  # the one slot slow_call actually used


class TestErrorMapping:
    @pytest.mark.parametrize("code", [errno.ENOENT, errno.ENOTDIR, errno.EISDIR])
    async def test_not_found_shaped_errnos_map_to_not_found(self, monkeypatch: pytest.MonkeyPatch, code: int) -> None:
        fake = _FakeSmbClientModule(fail_with=OSError(code, "not found"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(NotFoundError, match="no such path"):
            await store.read("f.txt")
        with pytest.raises(NotFoundError, match="no such path"):
            await store.size("f.txt")
        with pytest.raises(NotFoundError, match="no such path"):
            await store.listdir("f.txt")

    async def test_eacces_maps_to_permission_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``EACCES`` is the one permission-denied errno smbclient's mapping table
        produces (from ``STATUS_PRIVILEGE_NOT_HELD``)."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EACCES, "permission denied"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.read("f.txt")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.size("f.txt")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.listdir("f.txt")

    async def test_status_access_denied_with_no_errno_mapping_maps_to_permission_denied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """smbclient has no errno mapping for ``STATUS_ACCESS_DENIED``, so it
        arrives as ``errno=0`` with the NT status on ``.ntstatus``
        (``_is_permission_denied`` checks both)."""
        exc = OSError(0, "Unknown NtStatus error returned 'STATUS_ACCESS_DENIED'")
        exc.ntstatus = 0xC0000022  # type: ignore[attr-defined]
        fake = _FakeSmbClientModule(fail_with=exc)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.read("f.txt")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.size("f.txt")
        with pytest.raises(PermissionDeniedError, match="permission denied"):
            await store.listdir("f.txt")
        assert await store.exists("f.txt") is False

    async def test_eperm_sharing_violation_is_not_reinterpreted_as_permission_denied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``EPERM`` is smbclient's mapping for ``STATUS_SHARING_VIOLATION`` (a file
        locked by another client): a transient conflict, not an access failure."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EPERM, "sharing violation"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(StorageBackendError, match="sharing violation"):
            await store.read("f.txt")

    async def test_an_unmapped_oserror_is_a_storage_backend_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Checked against every method with its own ``except OSError`` block."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EBUSY, "device or resource busy"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        for method in ("read", "size", "listdir", "exists"):
            with pytest.raises(StorageBackendError, match="device or resource busy") as excinfo:
                await getattr(store, method)("f.txt")
            assert isinstance(excinfo.value.__cause__, OSError)

    async def test_exists_is_false_for_a_missing_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule(fail_with=OSError(errno.ENOENT, "not found"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        assert await store.exists("nope.txt") is False

    async def test_exists_returns_false_on_permission_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``exists()`` reports access denied as ``False``: callers probe many
        candidate paths per real hit, and raising on it would abort that
        search."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EACCES, "permission denied"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        assert await store.exists("nope.txt") is False


class TestUncPathBuilding:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            pytest.param("", "\\\\host\\share", id="root_path_has_no_trailing_separator"),
            pytest.param("sub/dir/file.txt", "\\\\host\\share\\sub\\dir\\file.txt", id="nested_path_uses_backslashes"),
            pytest.param(
                "/sub/file.txt/", "\\\\host\\share\\sub\\file.txt", id="stray_leading_and_trailing_slashes_are_stripped"
            ),
        ],
    )
    def test_unc(self, path: str, expected: str) -> None:
        store = SmbStore("share", server="host")
        assert store._unc(path) == expected


class TestRepr:
    def test_repr_does_not_crash(self) -> None:
        store = SmbStore("share", server="host")
        assert "SmbStore" in repr(store)
        assert "host" in repr(store)
        assert "share" in repr(store)


class TestListdirSizes:
    async def test_files_carry_their_size_and_a_directory_none_sorted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"b.bin": b"12345", "a.bin": b"", "sub/": b""})
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        assert await store.listdir("d") == [Entry("a.bin", 0), Entry("b.bin", 5), Entry("sub/", None)]

    async def test_an_entry_whose_stat_fails_reports_no_size(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Entry:
            name = "gone.bin"

            def is_dir(self) -> bool:
                return False

            def stat(self) -> object:
                raise OSError(errno.ENOENT, "vanished")

        fake = _FakeSmbClientModule({"f.txt": b"x"})
        _patch_smbclient(monkeypatch, fake)
        monkeypatch.setattr(smbclient, "scandir", lambda unc, **kw: [_Entry()])

        assert await SmbStore("share", server="host").listdir("d") == [Entry("gone.bin", None)]
