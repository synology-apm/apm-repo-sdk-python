"""Unit tests for ``synology_apm_repo.sdk.storage.smb`` — the pieces
specific to ``SmbStore`` itself (lazy-import guard, connection-pool
lifecycle, per-instance ``connection_cache`` isolation, error-code mapping)
rather than the generic ``ObjectStore`` contract, which lives in
``test_storage_object_store_contract.py`` alongside ``LocalFsStore``/
``S3Store``/``AzureStore``. Backed by a fake ``smbclient`` module — no
real network access.
"""

from __future__ import annotations

import asyncio
import errno
import sys
import time
from typing import Any

import pytest
import smbclient
from smbprotocol.exceptions import SMBException

from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.storage import smb as smb_module
from synology_apm_repo.sdk.storage.smb import SmbStore


class TestLazyImportPropagatesImportError:
    def test_construction_does_not_import_smbclient(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Like ``LocalFsStore``, building a ``SmbStore`` does no I/O and
        needs no import at all — only a method that actually touches the
        share does."""
        monkeypatch.setitem(sys.modules, "smbclient", None)
        store = SmbStore("share", server="host", username="user", password="pw")
        assert store._share == "share"

    async def test_raises_importerror_when_smbclient_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``sys.modules["smbclient"] = None`` makes a subsequent ``import
        smbclient`` raise ``ImportError`` (a real, documented CPython
        import system behavior — not a mock), simulating a broken/partial
        install without needing to actually uninstall smbprotocol from
        this dev environment."""
        monkeypatch.setitem(sys.modules, "smbclient", None)
        store = SmbStore("share", server="host")
        with pytest.raises(ImportError):
            await store.size("f.txt")


class _FakeSmbClientModule:
    """Minimal fake mirroring the subset of the real ``smbclient`` module's
    functions ``SmbStore`` calls — see
    ``test_storage_object_store_contract.py``'s own, more complete fake of
    the same shape for why a real class rather than ``MagicMock``: these
    functions are synchronous, so plain methods already match."""

    def __init__(
        self,
        files: dict[str, bytes] | None = None,
        *,
        fail_with: OSError | None = None,
        register_fail_count: int = 0,
        register_fail_exc: Exception | None = None,
        slow_seconds: float = 0.0,
        slow_only_on_first_connection: bool = False,
        credit_exhaustion_fail_count: int = 0,
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
        self._slow_seconds = slow_seconds
        self._slow_only_on_first_connection = slow_only_on_first_connection
        self._first_connection_cache: object | None = None
        # -- concurrency-tracking knob (TestConnectionPool) --
        self._current_concurrent = 0
        self.max_concurrent_seen = 0
        # -- credit-exhaustion knob (TestCreditExhaustionRetry) --
        self._credit_exhaustion_fail_remaining = credit_exhaustion_fail_count

    def register_session(self, server: str, **kwargs: Any) -> None:
        self.register_calls.append({"server": server, **kwargs})
        self._register_attempts += 1
        if self._register_attempts <= self._register_fail_count:
            raise self._register_fail_exc
        if self._first_connection_cache is None:
            self._first_connection_cache = kwargs.get("connection_cache")

    def _maybe_sleep(self, connection_cache: object) -> None:
        """Blocks the calling (worker) thread for ``_slow_seconds`` — every
        call when ``slow_only_on_first_connection`` is false, or only calls
        still using the *first* registered connection when it's true (so a
        test can prove a retry against a freshly reconnected session
        succeeds fast instead of hanging again). Also tracks how many calls
        are ever sleeping at once (``max_concurrent_seen``), for
        ``TestConnectionPool``'s own assertions — a plain, unsynchronized
        counter is good enough here since CPython's GIL serializes each
        increment/decrement, and the test only needs a high-water mark, not
        perfect concurrent bookkeeping."""
        if self._slow_seconds <= 0:
            return
        if self._slow_only_on_first_connection and connection_cache is not self._first_connection_cache:
            return
        self._current_concurrent += 1
        self.max_concurrent_seen = max(self.max_concurrent_seen, self._current_concurrent)
        time.sleep(self._slow_seconds)
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
        self._maybe_sleep(kwargs.get("connection_cache"))
        content = self._content_or_raise(unc)

        class _Stat:
            st_size = len(content)

        return _Stat()

    def listdir(self, unc: str, **kwargs: Any) -> list[str]:
        if self._fail_with is not None:
            raise self._fail_with
        return sorted(self._files)

    def exists(self, unc: str, **kwargs: Any) -> bool:
        # Mirrors real smbclient.path.exists()'s os.path.exists()-like
        # tolerance for a missing path (never raises for that); any other
        # OSError (e.g. permission denied) does propagate, exercising
        # SmbStore._exists_sync's own handling of that case.
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
    monkeypatch.setattr(smbclient, "listdir", fake.listdir)
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

    async def test_aclose_is_idempotent_and_a_no_op_before_any_use(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule({"f.txt": b"hello"})
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await store.aclose()  # never used - must not register/delete anything
        assert fake.register_calls == []
        assert fake.delete_calls == []

        await store.size("f.txt")
        await store.aclose()
        await store.aclose()  # idempotent
        assert len(fake.delete_calls) == 1

    async def test_each_store_uses_its_own_private_connection_cache(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """See the module docstring: ``smbclient``'s own session cache is
        process-wide by default, so every call must pass this store's own
        ``connection_cache`` dict rather than relying on the global one -
        proven here by two stores' caches never being the same object."""
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

        assert fake.register_calls[0]["connection_timeout"] == smb_module._DEFAULT_CONNECTION_TIMEOUT

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

        with pytest.raises(OSError, match="simulated connect failure"):
            await store.size("f.txt")
        assert len(fake.register_calls) == smb_module._DEFAULT_MAX_ATTEMPTS


class TestOperationTimeoutAndRetry:
    """``_call_with_retry()`` — every operation past the initial connect is
    bounded by ``_DEFAULT_OPERATION_TIMEOUT`` and retried up to
    ``_DEFAULT_MAX_ATTEMPTS`` times, since ``smbprotocol`` gives a caller no
    way to bound (or retry) a single ``open_file``/``stat``/``listdir``
    call once a session is established."""

    async def test_a_stuck_operation_times_out_reconnects_and_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_DEFAULT_OPERATION_TIMEOUT", 0.05)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.2, slow_only_on_first_connection=True)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        assert await store.size("f.txt") == 5  # 1st attempt times out; 2nd (fresh connection) succeeds fast
        assert len(fake.register_calls) == 2

    async def test_a_stuck_operation_drops_the_old_connection_cache_before_retrying(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_DEFAULT_OPERATION_TIMEOUT", 0.05)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.2, slow_only_on_first_connection=True)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        used_slot = store._slots[-1]  # the only slot a sequential, non-concurrent call ever checks out
        original_cache = used_slot.connection_cache

        await store.size("f.txt")

        assert used_slot.connection_cache is not original_cache

    async def test_a_persistently_stuck_operation_raises_timeout_after_exhausting_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_DEFAULT_OPERATION_TIMEOUT", 0.05)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.2)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(TimeoutError):
            await store.size("f.txt")
        assert len(fake.register_calls) == smb_module._DEFAULT_MAX_ATTEMPTS


class TestConnectionPool:
    """The connection *pool* (``_DEFAULT_CONNECTION_POOL_SIZE`` independent
    ``_ConnectionSlot``s) that replaced the single shared session — see the
    module docstring for why: SMB2's own client-side credit window starts
    at 1, so any two requests genuinely in flight at once on *one*
    connection would race for it. Bounding each slot to one caller at a
    time, while allowing up to the pool size real concurrent slots, is
    what makes both guarantees below true at once."""

    async def test_concurrent_calls_use_independent_slots_and_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Two genuinely overlapping calls (forced via ``slow_seconds``)
        must not be serialized onto one shared session — each gets its own
        slot, with its own ``connection_cache``, and its own
        ``register_session`` call."""
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.05)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await asyncio.gather(store.size("f.txt"), store.size("f.txt"))

        assert len(fake.register_calls) == 2
        cache_a = fake.register_calls[0]["connection_cache"]
        cache_b = fake.register_calls[1]["connection_cache"]
        assert cache_a is not cache_b
        assert fake.max_concurrent_seen == 2

    async def test_pool_bounds_real_concurrency_and_queues_the_rest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """More concurrent callers than the pool has slots must still all
        complete — the extra ones simply wait for a slot to free up,
        never exceeding the pool size's worth of real concurrent
        operations against the shared server."""
        monkeypatch.setattr(smb_module, "_DEFAULT_CONNECTION_POOL_SIZE", 2)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.05)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        results = await asyncio.gather(store.size("f.txt"), store.size("f.txt"), store.size("f.txt"))

        assert list(results) == [5, 5, 5]
        assert fake.max_concurrent_seen == 2
        assert len(fake.register_calls) == 2  # only 2 slots ever get created, the 3rd call reuses one


class TestCreditExhaustionRetry:
    """``_call_with_retry()``'s second retryable condition, alongside a
    timeout — a raised ``smbprotocol.exceptions.SMBException`` matching the
    SMB2 credit-window-exhaustion message (see ``_is_credit_exhaustion``).
    Unlike a timeout, this must *not* drop/reconnect the session: the
    connection itself is healthy, just momentarily out of credit."""

    async def test_a_credit_exhaustion_error_is_retried_and_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_CREDIT_RETRY_DELAY", 0.0)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, credit_exhaustion_fail_count=1)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        assert await store.size("f.txt") == 5
        # No reconnect for this exception -- only ever the one session.
        assert len(fake.register_calls) == 1

    async def test_a_persistent_credit_exhaustion_error_raises_after_exhausting_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(smb_module, "_CREDIT_RETRY_DELAY", 0.0)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, credit_exhaustion_fail_count=smb_module._DEFAULT_MAX_ATTEMPTS)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(SMBException, match="credits are available"):
            await store.size("f.txt")
        assert len(fake.register_calls) == 1

    async def test_an_unrelated_exception_is_not_treated_as_credit_exhaustion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A plain ``SMBException`` with an unrelated message (or any
        other exception) must still propagate immediately, not get
        swallowed into a retry loop meant for one specific condition."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EBUSY, "device or resource busy"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        with pytest.raises(OSError, match="device or resource busy"):
            await store.size("f.txt")


class TestAcloseTearsDownEverySlot:
    async def test_aclose_tears_down_every_slot_actually_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smb_module, "_DEFAULT_CONNECTION_POOL_SIZE", 2)
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.05)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        await asyncio.gather(store.size("f.txt"), store.size("f.txt"))
        assert len(fake.register_calls) == 2  # both slots now in use

        await store.aclose()

        assert len(fake.delete_calls) == 2
        assert all(not slot.ready for slot in store._slots)

    async def test_aclose_waits_for_an_in_flight_call_before_tearing_down_its_slot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``aclose()`` must never tear a slot's session down while a
        caller is still mid ``_ensure_slot_session``/read on it — proven
        here by a slow, already-in-flight ``size()`` call finishing
        (and being recorded) strictly before ``aclose()`` does, even
        though ``aclose()`` is invoked while that call still holds its
        slot checked out."""
        events: list[str] = []
        fake = _FakeSmbClientModule({"f.txt": b"hello"}, slow_seconds=0.1)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")

        async def slow_call() -> None:
            await store.size("f.txt")
            events.append("call_done")

        async def close_call() -> None:
            await asyncio.sleep(0.01)  # let slow_call acquire its slot and start sleeping first
            await store.aclose()
            events.append("aclose_done")

        await asyncio.gather(slow_call(), close_call())

        assert events.index("call_done") < events.index("aclose_done")
        assert len(fake.delete_calls) == 1  # the one slot slow_call actually used


class TestErrorMapping:
    @pytest.mark.parametrize("code", [errno.ENOENT, errno.ENOTDIR, errno.EISDIR])
    async def test_not_found_shaped_errnos_map_to_not_found(self, monkeypatch: pytest.MonkeyPatch, code: int) -> None:
        fake = _FakeSmbClientModule(fail_with=OSError(code, "not found"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(NotFoundError):
            await store.read("f.txt")
        with pytest.raises(NotFoundError):
            await store.size("f.txt")
        with pytest.raises(NotFoundError):
            await store.listdir("f.txt")

    async def test_eacces_maps_to_permission_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``EACCES`` is the one permission-denied errno smbclient's own
        mapping table actually produces (from ``STATUS_PRIVILEGE_NOT_HELD``)."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EACCES, "permission denied"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(PermissionDeniedError):
            await store.read("f.txt")
        with pytest.raises(PermissionDeniedError):
            await store.size("f.txt")
        with pytest.raises(PermissionDeniedError):
            await store.listdir("f.txt")

    async def test_status_access_denied_with_no_errno_mapping_maps_to_permission_denied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``smbclient``'s own ``SMBOSError`` has no errno mapping at all
        for ``STATUS_ACCESS_DENIED`` (the actual NT status a real SMB
        server sends for a plain access-denied failure), so it comes back
        as plain ``errno=0`` with the raw NT status on ``.ntstatus``
        instead — recognized here via ``.ntstatus``, not ``errno`` alone
        (see ``_is_permission_denied``)."""
        exc = OSError(0, "Unknown NtStatus error returned 'STATUS_ACCESS_DENIED'")
        exc.ntstatus = 0xC0000022  # type: ignore[attr-defined]
        fake = _FakeSmbClientModule(fail_with=exc)
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(PermissionDeniedError):
            await store.read("f.txt")
        with pytest.raises(PermissionDeniedError):
            await store.size("f.txt")
        with pytest.raises(PermissionDeniedError):
            await store.listdir("f.txt")
        assert await store.exists("f.txt") is False

    async def test_eperm_sharing_violation_is_not_reinterpreted_as_permission_denied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``EPERM`` is smbclient's own mapping for ``STATUS_SHARING_VIOLATION``
        (a file locked open by another client) — a transient conflict, not
        an access-control failure, and must not be folded into
        ``PermissionDeniedError``."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EPERM, "sharing violation"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(OSError, match="sharing violation") as excinfo:
            await store.read("f.txt")
        assert not isinstance(excinfo.value, PermissionDeniedError)

    async def test_an_unmapped_oserror_propagates_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A share-full-of-open-handles conflict, say, must never be
        silently swallowed or reinterpreted as a missing path or a
        permission failure — checked against every method with its own
        ``except OSError`` block."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EBUSY, "device or resource busy"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        with pytest.raises(OSError, match="device or resource busy"):
            await store.read("f.txt")
        with pytest.raises(OSError, match="device or resource busy"):
            await store.size("f.txt")
        with pytest.raises(OSError, match="device or resource busy"):
            await store.listdir("f.txt")
        with pytest.raises(OSError, match="device or resource busy"):
            await store.exists("f.txt")

    async def test_exists_never_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeSmbClientModule(fail_with=OSError(errno.ENOENT, "not found"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        assert await store.exists("nope.txt") is False

    async def test_exists_returns_false_on_permission_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unlike ``read``/``size``/``listdir``, ``exists()`` never raises:
        callers probe many candidate paths per real hit, and raising on
        access-denied would abort that search over one bad sibling."""
        fake = _FakeSmbClientModule(fail_with=OSError(errno.EACCES, "permission denied"))
        _patch_smbclient(monkeypatch, fake)
        store = SmbStore("share", server="host")
        assert await store.exists("nope.txt") is False


class TestUncPathBuilding:
    def test_root_path_has_no_trailing_separator(self) -> None:
        store = SmbStore("share", server="host")
        assert store._unc("") == "\\\\host\\share"

    def test_nested_path_uses_backslashes(self) -> None:
        store = SmbStore("share", server="host")
        assert store._unc("sub/dir/file.txt") == "\\\\host\\share\\sub\\dir\\file.txt"

    def test_stray_leading_and_trailing_slashes_are_stripped(self) -> None:
        store = SmbStore("share", server="host")
        assert store._unc("/sub/file.txt/") == "\\\\host\\share\\sub\\file.txt"


class TestRepr:
    def test_repr_does_not_crash(self) -> None:
        store = SmbStore("share", server="host")
        assert "SmbStore" in repr(store)
        assert "host" in repr(store)
        assert "share" in repr(store)
