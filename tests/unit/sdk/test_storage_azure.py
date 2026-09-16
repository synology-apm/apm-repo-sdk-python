"""Unit tests for ``synology_apm_repo.sdk.storage.azure`` — the pieces
specific to ``AzureStore`` itself (the lazy-import guard, status-code
mapping, the ``walk_blobs`` shape) rather than the generic ``ObjectStore``
contract, which lives in ``test_storage_object_store_contract.py``
alongside ``LocalFsStore``/``S3Store``. Backed by a mocked
``BlobServiceClient`` rather than a live Azurite instance.

The mock now models ``azure.storage.blob.aio``'s shapes rather than the
synchronous SDK's: ``download_blob``/``get_blob_properties``
are coroutines, the downloader's ``readall()`` is a coroutine, and
``walk_blobs()`` is a *synchronous* call returning an **async iterator**
(the real ``AsyncItemPaged``), which is what ``AzureStore`` consumes with
``async for``.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.azure import (
    AzureStore,
    _account_name_from_url,
    _force_close_transport,
    _resolve_shared_key_credential,
    _with_default_timeouts,
    list_containers,
)


def _not_found() -> ResourceNotFoundError:
    err = ResourceNotFoundError(message="not found")
    err.status_code = 404
    return err


def _range_not_satisfiable() -> HttpResponseError:
    err = HttpResponseError(message="range not satisfiable")
    err.status_code = 416
    return err


async def _as_async_iter(items: list[MagicMock]) -> AsyncIterator[MagicMock]:
    """Stand-in for ``AsyncItemPaged`` — ``walk_blobs()`` itself is not a
    coroutine in the real async SDK either; it returns something you
    ``async for`` over."""
    for item in items:
        yield item


def _service_client_for(files: dict[str, bytes]) -> MagicMock:
    """A mocked ``BlobServiceClient`` -> ``ContainerClient`` ->
    ``BlobClient`` chain, real enough (method names, which of them are
    coroutines, exception types and where they're raised,
    ``walk_blobs``'s async-iterator-of-``BlobPrefix``/``BlobProperties``
    name-attribute shape) to drive ``AzureStore`` exactly like the real
    async SDK would, without needing a live Azurite."""
    service_client = MagicMock()
    container_client = MagicMock()
    service_client.get_container_client.return_value = container_client

    def get_blob_client(name: str) -> MagicMock:
        blob_client = MagicMock()
        content = files.get(name)

        def download_blob(*, offset: int = 0, length: int | None = None) -> MagicMock:
            if content is None:
                raise _not_found()
            if offset > len(content):
                raise _range_not_satisfiable()
            end = len(content) if length is None else offset + length
            downloader = MagicMock()
            downloader.readall = AsyncMock(return_value=content[offset:end])
            return downloader

        def get_blob_properties() -> MagicMock:
            if content is None:
                raise _not_found()
            props = MagicMock()
            props.size = len(content)
            return props

        # AsyncMock: awaiting the call yields the sync side_effect's return
        # value, and a side_effect that raises propagates out of the await —
        # exactly the real ``await blob_client.download_blob(...)`` shape.
        blob_client.download_blob = AsyncMock(side_effect=download_blob)
        blob_client.get_blob_properties = AsyncMock(side_effect=get_blob_properties)
        return blob_client

    container_client.get_blob_client.side_effect = get_blob_client

    def walk_blobs(*, name_starts_with: str = "", delimiter: str = "/") -> AsyncIterator[MagicMock]:
        seen: set[str] = set()
        items: list[MagicMock] = []
        for name in files:
            if not name.startswith(name_starts_with):
                continue
            rest = name[len(name_starts_with) :]
            if delimiter in rest:
                prefix = name_starts_with + rest.split(delimiter, 1)[0] + delimiter
                if prefix in seen:
                    continue
                seen.add(prefix)
                item = MagicMock()
                item.name = prefix
            else:
                item = MagicMock()
                item.name = name
            items.append(item)
        return _as_async_iter(items)

    container_client.walk_blobs.side_effect = walk_blobs
    container_client.container_name = "test-container"
    return service_client


class TestLazyImportPropagatesImportError:
    def test_raises_importerror_when_the_sdk_is_missing_and_no_client_given(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sys.modules["azure.storage.blob.aio"] = None`` makes a
        subsequent ``from azure.storage.blob.aio import BlobServiceClient``
        raise ``ImportError`` — a real, documented CPython import system
        behavior, simulating a broken/partial install without uninstalling
        it from this dev environment. The ``.aio`` subpackage is named
        explicitly (as well as its parent) because ``AzureStore`` now
        imports from there, and a parent already cached in
        ``sys.modules`` would otherwise not stop the child import from
        resolving."""
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        with pytest.raises(ImportError):
            AzureStore("some-container")

    def test_does_not_import_the_sdk_when_a_client_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        service_client = _service_client_for({})
        store = AzureStore("some-container", client=service_client)
        # Reaching this line at all already proves no ImportError fired;
        # the real claim this test makes is that the injected client is
        # used as-is (not rewrapped) and marked as not owned by the store.
        assert store._service_client is service_client
        assert store._owns_client is False


class TestErrorMapping:
    async def test_read_past_eof_returns_empty_bytes_not_an_error(self) -> None:
        store = AzureStore("c", client=_service_client_for({"f.txt": b"hello"}))
        assert await store.read("f.txt", offset=100, length=10) == b""

    async def test_read_missing_blob_raises_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        with pytest.raises(NotFoundError):
            await store.read("nope.txt")

    async def test_size_missing_blob_raises_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        with pytest.raises(NotFoundError):
            await store.size("nope.txt")

    async def test_exists_false_for_missing_blob_true_for_present(self) -> None:
        store = AzureStore("c", client=_service_client_for({"f.txt": b"hello"}))
        assert await store.exists("f.txt") is True
        assert await store.exists("nope.txt") is False

    async def test_a_non_404_non_416_error_propagates_unchanged(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.get_blob_properties = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(HttpResponseError):
            await store.exists("f.txt")

    async def test_a_non_404_non_416_error_propagates_unchanged_from_read(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.download_blob = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(HttpResponseError):
            await store.read("f.txt")

    async def test_a_non_404_error_propagates_unchanged_from_size(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.get_blob_properties = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(HttpResponseError):
            await store.size("f.txt")

    async def test_exists_true_for_a_directory_prefix_with_no_blob_of_its_own(self) -> None:
        # "dir" itself is never an object (get_blob_properties() 404s for
        # it), only a prefix with a real blob underneath -- exists()
        # must still report True via the walk_blobs() fallback, same
        # contract test_storage_s3.py's exists() pins down.
        store = AzureStore("c", client=_service_client_for({"dir/file.txt": b"x"}))
        assert await store.exists("dir") is True

    async def test_exists_false_for_a_similarly_named_sibling_blob_not_a_real_directory(self) -> None:
        # "dirfile.txt" shares "dir" as a literal string prefix but isn't
        # inside a "dir/" directory -- proves prefix_listing.as_list_prefix's
        # trailing-slash disambiguation (name_starts_with="dir/", not "dir")
        # is what actually keeps the walk_blobs() fallback from
        # false-matching an unrelated sibling blob.
        store = AzureStore("c", client=_service_client_for({"dirfile.txt": b"x"}))
        assert await store.exists("dir") is False


class TestAcloseOwnedClient:
    async def test_aclose_closes_a_client_this_store_built_itself(self) -> None:
        # Every other test in this file injects a client=... (as
        # AzureStore's constructor optionally accepts), so aclose()'s
        # real body -- closing a client this store built and owns --
        # never runs. account_url alone is enough to construct a real
        # BlobServiceClient with no network I/O.
        store = AzureStore("some-container", account_url="https://fakeaccount.blob.core.windows.net")
        assert store._owns_client is True
        await store.aclose()
        await store.aclose()  # idempotent - must not raise

    async def test_aclose_leaves_an_injected_client_untouched(self) -> None:
        # aclose()'s own docstring: "only for a client this class built
        # itself -- an injected client belongs to whoever created it."
        service_client = _service_client_for({})
        store = AzureStore("some-container", client=service_client)
        assert store._owns_client is False
        await store.aclose()
        service_client.close.assert_not_called()


class TestCancellation:
    """Mirrors ``test_storage_s3.py``'s own ``TestCancellation`` for the
    identical hazard on the Azure side — but the fix looks different
    here: there is no per-connection handle reachable from
    ``downloader``/``blob_client`` at all (unlike S3's response body),
    only the shared top-level transport (``AzureStore.read()``'s own
    comment has the full explanation). ``self._container``/``blob_client``'s own
    ``._pipeline._transport`` is a deliberate no-op wrapper — this test
    pins down that the fix reaches past it to
    ``self._service_client._pipeline._transport`` specifically, not
    just "some close() got called somewhere"."""

    async def test_a_cancelled_read_closes_the_top_level_transport(self) -> None:
        service_client = _service_client_for({})
        service_client._pipeline._transport.close = AsyncMock()
        container_client = service_client.get_container_client.return_value
        started = asyncio.Event()

        def get_blob_client(name: str) -> MagicMock:
            blob_client = MagicMock()

            async def download_blob(*, offset: int = 0, length: int | None = None) -> MagicMock:
                started.set()
                await asyncio.Event().wait()  # park forever -- only a cancellation ends this
                raise AssertionError("unreachable")  # pragma: no cover

            blob_client.download_blob = download_blob
            return blob_client

        container_client.get_blob_client.side_effect = get_blob_client
        store = AzureStore("c", client=service_client)

        task = asyncio.create_task(store.read("f.txt"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        service_client._pipeline._transport.close.assert_awaited_once()


class TestForceCloseTransport:
    """Direct tests of the helper ``TestCancellation`` above exercises
    indirectly through a real cancelled ``read()`` — this covers its own
    graceful-degradation fallback, which a real ``BlobServiceClient``
    mock never exercises (its ``_pipeline._transport`` chain is always
    present)."""

    async def test_closes_the_pipeline_transport_when_present(self) -> None:
        service_client = MagicMock()
        service_client._pipeline._transport.close = AsyncMock()
        await _force_close_transport(service_client)
        service_client._pipeline._transport.close.assert_awaited_once()
        service_client.close.assert_not_called()

    async def test_falls_back_to_the_public_close_when_pipeline_is_missing(self) -> None:
        service_client = MagicMock(spec=["close"])
        service_client.close = AsyncMock()
        await _force_close_transport(service_client)
        service_client.close.assert_awaited_once()

    async def test_falls_back_to_the_public_close_when_transport_is_missing(self) -> None:
        service_client = MagicMock(spec=["_pipeline", "close"])
        service_client._pipeline = MagicMock(spec=[])
        service_client.close = AsyncMock()
        await _force_close_transport(service_client)
        service_client.close.assert_awaited_once()


class TestListdir:
    async def test_listdir_merges_blobs_and_virtual_directories(self) -> None:
        files = {f"dir/file{i}.txt": str(i).encode() for i in range(5)}
        files["dir/sub/nested.txt"] = b"x"
        store = AzureStore("c", client=_service_client_for(files))
        entries = await store.listdir("dir")
        assert entries == sorted([f"file{i}.txt" for i in range(5)] + ["sub"])

    async def test_listdir_on_missing_prefix_returns_empty_not_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        assert await store.listdir("nope") == []


class TestListContainers:
    async def test_list_containers_returns_every_container_visible_to_these_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed = False

        class _FakeContainerProperties:
            def __init__(self, name: str) -> None:
                self.name = name

        async def _containers() -> AsyncIterator[_FakeContainerProperties]:
            for name in ["container-b", "container-a"]:
                yield _FakeContainerProperties(name)

        class _FakeServiceClient:
            def __init__(self, **kwargs: object) -> None:
                pass

            def list_containers(self) -> AsyncIterator[_FakeContainerProperties]:
                # Real BlobServiceClient.aio.list_containers() is not a
                # coroutine either — it returns something ``async for``'d
                # over (the real ``AsyncItemPaged``), same shape as
                # AzureStore's own ``walk_blobs()``.
                return _containers()

            async def close(self) -> None:
                nonlocal closed
                closed = True

        monkeypatch.setattr("azure.storage.blob.aio.BlobServiceClient", _FakeServiceClient)
        containers = await list_containers(account_url="https://example.blob.core.windows.net")
        assert containers == ["container-a", "container-b"]
        assert closed, "the transient client must be closed before returning"

    def test_raises_importerror_when_the_sdk_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        with pytest.raises(ImportError):
            asyncio.run(list_containers())


class TestRepr:
    def test_repr_does_not_crash(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        assert "AzureStore" in repr(store)


class TestSharedKeyCredentialResolution:
    """``_resolve_shared_key_credential`` exists because azure-storage-blob's
    own account-name sniffing (``StorageAccountHostsMixin.__init__`` in
    ``azure.storage.blob._shared.base_client``) only recognizes a path-style
    account URL's account name when the host is literally ``"localhost"``/
    ``"127.0.0.1"`` — any other Azurite-style endpoint (a real hostname, a
    remote IP, a docker service name) leaves it unable to determine the
    account name at all, raising ``ValueError: Unable to determine account
    name for shared key credential`` (see that function's own docstring)."""

    def test_account_name_recovered_from_production_subdomain_url(self) -> None:
        assert _account_name_from_url("https://myaccount.blob.core.windows.net") == "myaccount"

    def test_account_name_recovered_from_path_style_url_on_a_non_localhost_host(self) -> None:
        assert _account_name_from_url("http://192.0.2.10:10000/devstoreaccount1") == "devstoreaccount1"

    def test_account_name_is_none_when_neither_form_matches(self) -> None:
        assert _account_name_from_url("http://192.0.2.10:10000") is None

    def test_resolves_a_plain_string_credential_into_an_explicit_account_name_dict(self) -> None:
        resolved = _resolve_shared_key_credential(
            {"account_url": "http://192.0.2.10:10000/devstoreaccount1", "credential": "some-key"}
        )
        assert resolved["credential"] == {"account_name": "devstoreaccount1", "account_key": "some-key"}

    def test_leaves_a_non_string_credential_untouched(self) -> None:
        original = {
            "account_url": "http://192.0.2.10:10000/devstoreaccount1",
            "credential": {"account_name": "x", "account_key": "y"},
        }
        assert _resolve_shared_key_credential(original) is original

    def test_leaves_client_kwargs_untouched_when_account_name_cannot_be_recovered(self) -> None:
        original = {"account_url": "http://192.0.2.10:10000", "credential": "some-key"}
        assert _resolve_shared_key_credential(original) is original

    def test_azure_store_construction_passes_the_resolved_credential_to_the_real_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end version of the two tests above: constructing
        ``AzureStore`` against a path-style, non-localhost Azurite endpoint
        must not raise, and the client actually receives the resolved
        ``{"account_name": ..., "account_key": ...}`` dict rather than the
        raw key string."""
        captured: dict[str, object] = {}

        class _FakeServiceClient:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            def get_container_client(self, name: str) -> MagicMock:
                return MagicMock()

        monkeypatch.setattr("azure.storage.blob.aio.BlobServiceClient", _FakeServiceClient)
        AzureStore("c", account_url="http://192.0.2.10:10000/devstoreaccount1", credential="some-key")
        assert captured["credential"] == {"account_name": "devstoreaccount1", "account_key": "some-key"}


class TestDefaultTimeouts:
    """Interactive callers (the TUI's connect dialog, in particular)
    building a client against an unreachable account URL must not inherit
    azure-core's own 300s connect/300s read defaults — see the module
    docstring."""

    def test_fills_in_short_connect_and_read_timeouts_when_caller_passes_none(self) -> None:
        merged = _with_default_timeouts({"account_url": "https://example.invalid"})
        assert merged["connection_timeout"] < 300
        assert merged["read_timeout"] < 300
        assert merged["account_url"] == "https://example.invalid"

    def test_a_caller_supplied_timeout_is_not_overridden(self) -> None:
        merged = _with_default_timeouts({"connection_timeout": 1})
        assert merged["connection_timeout"] == 1
        # read_timeout was never set by the caller, so it still falls back
        # to this module's own default rather than azure-core's 300s one.
        assert merged["read_timeout"] < 300
