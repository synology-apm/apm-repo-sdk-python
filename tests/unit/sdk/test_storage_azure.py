"""Unit tests for ``synology_apm_repo.sdk.storage.azure`` — what is
specific to ``AzureStore`` and its helpers (the lazy-import guard,
status-code mapping, the ``walk_blobs`` shape, client ownership and
cancellation, ``list_containers``, shared-key credential resolution, default
timeouts) rather than the generic ``ObjectStore`` contract, which
``test_storage_object_store_contract.py`` runs against every backend.
Backed by a mocked ``BlobServiceClient`` rather than a live Azurite instance.

The mock models ``azure.storage.blob.aio``'s shapes, not the
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
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError, StorageBackendError
from synology_apm_repo.sdk.storage.azure import (
    AzureStore,
    _account_name_from_url,
    _force_close_transport,
    _resolve_shared_key_credential,
    _with_default_timeouts,
    list_containers,
)
from synology_apm_repo.sdk.storage.base import Entry


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
                item = MagicMock(spec=["name"])  # a BlobPrefix has no size
                item.name = prefix
            else:
                item = MagicMock()
                item.name = name
                item.size = len(files[name])
            items.append(item)
        return _as_async_iter(items)

    container_client.walk_blobs.side_effect = walk_blobs
    container_client.container_name = "test-container"
    return service_client


class TestLazyImportPropagatesImportError:
    def test_raises_importerror_when_the_sdk_is_missing_and_no_client_given(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``None`` entry in ``sys.modules`` makes the import raise
        ``ImportError``, simulating a missing install. The ``.aio``
        subpackage ``AzureStore`` imports from is blocked too, since an
        already-cached one would still resolve with only its parent
        blocked."""
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        with pytest.raises(ImportError, match=r"import of azure\.storage\.blob\.aio halted"):
            AzureStore("some-container")

    def test_does_not_import_the_sdk_when_a_client_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        service_client = _service_client_for({})
        store = AzureStore("some-container", client=service_client)
        assert store._service_client is service_client
        assert store._owns_client is False


class TestErrorMapping:
    async def test_read_past_eof_returns_empty_bytes_not_an_error(self) -> None:
        store = AzureStore("c", client=_service_client_for({"f.txt": b"hello"}))
        assert await store.read("f.txt", offset=100, length=10) == b""

    async def test_read_missing_blob_raises_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        with pytest.raises(NotFoundError, match="no such blob"):
            await store.read("nope.txt")

    async def test_size_missing_blob_raises_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        with pytest.raises(NotFoundError, match="no such blob"):
            await store.size("nope.txt")

    async def test_exists_false_for_missing_blob_true_for_present(self) -> None:
        store = AzureStore("c", client=_service_client_for({"f.txt": b"hello"}))
        assert await store.exists("f.txt") is True
        assert await store.exists("nope.txt") is False

    async def test_a_403_is_a_permission_denied_error_from_exists(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.get_blob_properties = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(PermissionDeniedError, match=r"access denied \(HTTP") as excinfo:
            await store.exists("f.txt")
        assert excinfo.value.__cause__ is forbidden

    async def test_a_403_is_a_permission_denied_error_from_read(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.download_blob = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(PermissionDeniedError, match=r"access denied \(HTTP"):
            await store.read("f.txt")

    async def test_a_403_is_a_permission_denied_error_from_size(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        forbidden = HttpResponseError(message="forbidden")
        forbidden.status_code = 403
        blob_client.get_blob_properties = AsyncMock(side_effect=forbidden)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(PermissionDeniedError, match=r"access denied \(HTTP"):
            await store.size("f.txt")

    async def test_a_service_error_is_a_storage_backend_error(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        unavailable = HttpResponseError(message="server busy")
        unavailable.status_code = 503
        blob_client.get_blob_properties = AsyncMock(side_effect=unavailable)
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(StorageBackendError, match="503"):
            await store.size("f.txt")

    async def test_a_connection_failure_is_a_storage_backend_error(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        blob_client.download_blob = AsyncMock(side_effect=ServiceRequestError("connection refused"))
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        with pytest.raises(StorageBackendError, match="connection refused"):
            await store.read("f.txt")

    async def test_a_zero_length_read_downloads_nothing_and_returns_empty_bytes(self) -> None:
        service_client = _service_client_for({})
        container = service_client.get_container_client.return_value
        blob_client = MagicMock()
        properties = MagicMock()
        properties.size = 5
        blob_client.get_blob_properties = AsyncMock(return_value=properties)
        blob_client.download_blob = AsyncMock(side_effect=AssertionError("unexpected download_blob"))
        container.get_blob_client.side_effect = lambda name: blob_client
        store = AzureStore("c", client=service_client)
        assert await store.read("f.txt", offset=2, length=0) == b""

    async def test_exists_true_for_a_directory_prefix_with_no_blob_of_its_own(self) -> None:
        # "dir" is only a prefix (get_blob_properties() 404s for it);
        # exists() reports True via the walk_blobs() fallback.
        store = AzureStore("c", client=_service_client_for({"dir/file.txt": b"x"}))
        assert await store.exists("dir") is True

    async def test_exists_false_for_a_similarly_named_sibling_blob_not_a_real_directory(self) -> None:
        # "dirfile.txt" shares "dir" as a string prefix but isn't under
        # "dir/": the fallback lists name_starts_with="dir/"
        # (as_list_prefix), so it must not false-match the sibling blob.
        store = AzureStore("c", client=_service_client_for({"dirfile.txt": b"x"}))
        assert await store.exists("dir") is False


class TestCloseOwnedClient:
    async def test_close_closes_a_client_this_store_built_itself(self) -> None:
        # account_url alone constructs a real BlobServiceClient with no
        # network I/O.
        store = AzureStore("some-container", account_url="https://fakeaccount.blob.core.windows.net")
        assert store._owns_client is True
        await store.close()
        await store.close()  # idempotent - must not raise

    async def test_close_leaves_an_injected_client_untouched(self) -> None:
        service_client = _service_client_for({})
        store = AzureStore("some-container", client=service_client)
        assert store._owns_client is False
        await store.close()
        service_client.close.assert_not_called()


class TestCancellation:
    """A cancelled read must not return a half-read connection to the
    pool. No per-connection handle is reachable from
    ``downloader``/``blob_client``, and a child client's transport is a
    no-op wrapper, so the read closes
    ``self._service_client._pipeline._transport`` itself."""

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
    """``_force_close_transport``, including its fallback to the public
    ``close()`` when the private ``_pipeline._transport`` chain is gone."""

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
        assert [entry.name for entry in entries] == sorted([f"file{i}.txt" for i in range(5)] + ["sub"])

    async def test_listdir_on_missing_prefix_returns_empty_not_not_found(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        assert await store.listdir("nope") == []


class TestListdirSizes:
    async def test_blobs_carry_their_size_and_a_virtual_directory_has_none(self) -> None:
        files = {"dir/a.bin": b"12345", "dir/b.bin": b"", "dir/sub/nested.bin": b"x"}
        store = AzureStore("c", client=_service_client_for(files))

        assert await store.listdir("dir") == [Entry("a.bin", 5), Entry("b.bin", 0), Entry("sub", None)]


class TestListContainers:
    async def test_list_containers_returns_every_container_visible_to_these_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        close_calls = 0

        class _FakeContainerProperties:
            def __init__(self, name: str) -> None:
                self.name = name

        async def _containers() -> AsyncIterator[_FakeContainerProperties]:
            for name in ["container-b", "container-a"]:
                yield _FakeContainerProperties(name)

        @unchecked_fake("azure.storage.blob's async BlobServiceClient")
        class _FakeServiceClient:
            def __init__(self, **kwargs: object) -> None:
                pass

            def list_containers(self) -> AsyncIterator[_FakeContainerProperties]:
                # Like walk_blobs(), a plain call returning an
                # AsyncItemPaged-shaped async iterator.
                return _containers()

            async def close(self) -> None:
                nonlocal close_calls
                close_calls += 1

        monkeypatch.setattr("azure.storage.blob.aio.BlobServiceClient", _FakeServiceClient)
        containers = await list_containers(account_url="https://example.blob.core.windows.net")
        assert containers == ["container-a", "container-b"]
        assert close_calls == 1, "the transient client must be closed before returning"

    async def test_list_containers_maps_a_403_to_permission_denied_and_still_closes_the_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        close_calls = 0

        async def _denied() -> AsyncIterator[object]:
            err = HttpResponseError(message="denied")
            err.status_code = 403
            raise err
            yield  # pragma: no cover - makes this an async generator

        @unchecked_fake("azure.storage.blob's async BlobServiceClient")
        class _FakeServiceClient:
            def __init__(self, **kwargs: object) -> None:
                pass

            def list_containers(self) -> AsyncIterator[object]:
                return _denied()

            async def close(self) -> None:
                nonlocal close_calls
                close_calls += 1

        monkeypatch.setattr("azure.storage.blob.aio.BlobServiceClient", _FakeServiceClient)
        with pytest.raises(PermissionDeniedError, match=r"access denied \(HTTP"):
            await list_containers(account_url="https://example.blob.core.windows.net")
        assert close_calls == 1

    def test_raises_importerror_when_the_sdk_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "azure.storage.blob", None)
        monkeypatch.setitem(sys.modules, "azure.storage.blob.aio", None)
        with pytest.raises(ImportError, match=r"import of azure\.storage\.blob\.aio halted"):
            asyncio.run(list_containers())


class TestRepr:
    def test_repr_does_not_crash(self) -> None:
        store = AzureStore("c", client=_service_client_for({}))
        assert "AzureStore" in repr(store)


class TestSharedKeyCredentialResolution:
    """azure-storage-blob recognizes a path-style account URL's account
    name only when the host is ``localhost``/``127.0.0.1``; any other
    Azurite-style endpoint raises ``ValueError: Unable to determine account
    name for shared key credential`` unless
    ``_resolve_shared_key_credential`` passes the name explicitly."""

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            pytest.param("https://myaccount.blob.core.windows.net", "myaccount", id="production_subdomain_url"),
            pytest.param(
                "http://192.0.2.10:10000/devstoreaccount1",
                "devstoreaccount1",
                id="path_style_url_on_a_non_localhost_host",
            ),
        ],
    )
    def test_account_name_recovered_from(self, url: str, expected: str) -> None:
        assert _account_name_from_url(url) == expected

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
        """``AzureStore`` against a path-style, non-localhost endpoint hands
        the client the resolved dict, not the raw key string."""
        captured: dict[str, object] = {}

        @unchecked_fake("azure.storage.blob's async BlobServiceClient")
        class _FakeServiceClient:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            def get_container_client(self, name: str) -> MagicMock:
                return MagicMock()

        monkeypatch.setattr("azure.storage.blob.aio.BlobServiceClient", _FakeServiceClient)
        AzureStore("c", account_url="http://192.0.2.10:10000/devstoreaccount1", credential="some-key")
        assert captured["credential"] == {"account_name": "devstoreaccount1", "account_key": "some-key"}


class TestDefaultTimeouts:
    """A client built against an unreachable account URL must not inherit
    azure-core's 300s connect/300s read defaults."""

    def test_fills_in_short_connect_and_read_timeouts_when_caller_passes_none(self) -> None:
        merged = _with_default_timeouts({"account_url": "https://example.invalid"})
        assert merged["connection_timeout"] < 300
        assert merged["read_timeout"] < 300
        assert merged["account_url"] == "https://example.invalid"

    def test_a_caller_supplied_timeout_is_not_overridden(self) -> None:
        merged = _with_default_timeouts({"connection_timeout": 1})
        assert merged["connection_timeout"] == 1
        assert merged["read_timeout"] < 300
