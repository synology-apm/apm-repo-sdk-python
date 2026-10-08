"""Unit tests for ``synology_apm_repo.sdk.storage.s3`` — what is specific
to ``S3Store`` and its helpers (pagination, the lazy-import guard, error code
mapping, client ownership and cancellation, ``list_buckets``, the default
client config) rather than the generic ``ObjectStore`` contract, which
``test_storage_object_store_contract.py`` runs against every backend. Backed
by an in-memory fake client (``FakeS3Client``) — no real or emulated network
server, no real AWS credentials.
"""

from __future__ import annotations

import asyncio
import sys

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError, StorageBackendError
from synology_apm_repo.sdk.storage.base import Entry
from synology_apm_repo.sdk.storage.s3 import S3Store, _with_default_timeouts, list_buckets
from unit.sdk.storage_fakes import FakeS3Client


@pytest.fixture
async def client() -> FakeS3Client:
    c = FakeS3Client()
    await c.create_bucket(Bucket="test-bucket")
    return c


class TestLazyImportPropagatesImportError:
    def test_raises_importerror_when_aioboto3_is_missing_and_no_client_given(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sys.modules["aioboto3"] = None`` makes ``import aioboto3``
        raise ``ImportError``, simulating a missing install."""
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        with pytest.raises(ImportError, match="import of aioboto3 halted"):
            S3Store("some-bucket")

    def test_does_not_import_aioboto3_when_a_client_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A caller-supplied client means aioboto3 need not be importable
        at all."""
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        client = object()
        store = S3Store("some-bucket", client=client)
        assert store._client is client
        assert store._owns_client is False


class TestErrorMapping:
    async def test_read_past_eof_returns_empty_bytes_not_an_error(self, client: FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.read("f.txt", offset=100, length=10) == b""

    async def test_a_zero_length_read_sends_no_ranged_get_and_returns_empty_bytes(
        self, client: FakeS3Client, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No valid ``Range`` exists for zero bytes; S3 ignores an invalid
        one and returns the whole object, so no ``get_object`` may be sent."""
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")

        async def _no_get(**kwargs: object) -> dict[str, object]:
            raise AssertionError(f"unexpected get_object({kwargs})")

        monkeypatch.setattr(client, "get_object", _no_get)
        store = S3Store("test-bucket", client=client)
        assert await store.read("f.txt", offset=2, length=0) == b""

    async def test_a_zero_length_read_of_a_missing_key_raises_not_found(self, client: FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        with pytest.raises(NotFoundError, match="no such object"):
            await store.read("nope.txt", length=0)

    async def test_read_missing_key_raises_not_found(self, client: FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        with pytest.raises(NotFoundError, match="no such object"):
            await store.read("nope.txt")

    async def test_size_missing_key_raises_not_found(self, client: FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        with pytest.raises(NotFoundError, match="no such object"):
            await store.size("nope.txt")

    async def test_size_returns_content_length_for_an_existing_object(self, client: FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.size("f.txt") == 5

    async def test_exists_false_for_missing_key_true_for_present(self, client: FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("f.txt") is True
        assert await store.exists("nope.txt") is False

    async def test_exists_true_for_a_directory_prefix_with_no_key_of_its_own(self, client: FakeS3Client) -> None:
        # "dir" is only a prefix (head_object() 404s for it); exists()
        # reports True via the list_objects_v2() fallback.
        await client.put_object(Bucket="test-bucket", Key="dir/file.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("dir") is True

    async def test_exists_false_for_a_similarly_named_sibling_key_not_a_real_directory(
        self, client: FakeS3Client
    ) -> None:
        # "dirfile.txt" shares "dir" as a string prefix but isn't under
        # "dir/": the fallback lists Prefix="dir/" (as_list_prefix), so it
        # must not false-match the sibling key.
        await client.put_object(Bucket="test-bucket", Key="dirfile.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("dir") is False


class TestCancellation:
    """A cancellation landing mid-body-read must not let the partially-read
    response return its connection to the pool, where a later request
    would get it in a corrupted state."""

    async def test_a_cancelled_read_closes_the_response_body_rather_than_letting_it_be_pooled(self) -> None:
        @unchecked_fake("aiobotocore's S3 client surface")
        class _FakeBody:
            def __init__(self) -> None:
                self.closed = False
                self.started = asyncio.Event()

            async def read(self) -> bytes:
                self.started.set()
                await asyncio.Event().wait()  # park forever -- only a cancellation ends this
                raise AssertionError("unreachable")  # pragma: no cover

            def close(self) -> None:
                self.closed = True

        @unchecked_fake("aiobotocore's S3 client surface")
        class _FakeClient:
            def __init__(self, body: _FakeBody) -> None:
                self._body = body

            async def get_object(self, **kwargs: object) -> dict[str, object]:
                return {"Body": self._body}

        body = _FakeBody()
        store = S3Store("test-bucket", client=_FakeClient(body))

        task = asyncio.create_task(store.read("f.txt"))
        await body.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert body.closed is True


class TestLazyClientLifecycle:
    """``S3Store`` building and owning a real ``aioboto3`` client
    (``_get_client()``'s laziness, caching and close). ``endpoint_url`` is
    a never-dialed TEST-NET-1 address (RFC 5737): entering/exiting the
    client does no network I/O, and no operation is issued."""

    async def test_get_client_lazily_builds_and_reuses_one_real_client(self) -> None:
        store = S3Store(
            "test-bucket",
            endpoint_url="http://192.0.2.1:1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        try:
            client_one = await store._get_client()
            client_two = await store._get_client()
            assert client_one is client_two  # built once, reused thereafter
        finally:
            await store.close()
            await store.close()  # idempotent - must not raise


async def test_close_leaves_an_injected_client_untouched(client: FakeS3Client) -> None:
    store = S3Store("test-bucket", client=client)
    assert store._owns_client is False
    await store.close()
    assert client.closed is False


class TestMappedClientErrors:
    """A failure other than the ones each method special-cases
    (``InvalidRange``/not-found) surfaces as ``ObjectStore``'s own error,
    the original kept as ``__cause__`` — a permissions error must never be
    swallowed, reinterpreted as absent, or leak as a raw ``ClientError``."""

    @unchecked_fake("aiobotocore's S3 client surface")
    class _FakeClient:
        def __init__(self, exc: Exception) -> None:
            self._exc = exc

        async def get_object(self, **kwargs: object) -> dict[str, object]:
            raise self._exc

        async def head_object(self, **kwargs: object) -> dict[str, object]:
            raise self._exc

    @staticmethod
    def _client_error(code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "nope"}}, "GetObject")

    @pytest.mark.parametrize("method", ["read", "size", "exists"])
    async def test_access_denied_is_a_permission_denied_error(self, method: str) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(self._client_error("AccessDenied")))
        with pytest.raises(PermissionDeniedError, match="AccessDenied") as excinfo:
            await getattr(store, method)("f.txt")
        assert isinstance(excinfo.value.__cause__, ClientError)

    @pytest.mark.parametrize("method", ["read", "size", "exists"])
    async def test_any_other_service_error_is_a_storage_backend_error(self, method: str) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(self._client_error("InternalError")))
        with pytest.raises(StorageBackendError, match="InternalError"):
            await getattr(store, method)("f.txt")

    @pytest.mark.parametrize("method", ["read", "size", "exists"])
    async def test_a_connection_failure_is_a_storage_backend_error(self, method: str) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(EndpointConnectionError(endpoint_url="https://s3")))
        with pytest.raises(StorageBackendError, match="S3 request failed") as excinfo:
            await getattr(store, method)("f.txt")
        assert isinstance(excinfo.value.__cause__, EndpointConnectionError)


class TestListBuckets:
    async def test_list_buckets_returns_every_bucket_visible_to_these_credentials(
        self, monkeypatch: pytest.MonkeyPatch, client: FakeS3Client
    ) -> None:
        """``list_buckets()`` builds and closes its own transient client
        (no injected one), so ``aioboto3.Session`` itself is patched to
        hand back the fake."""

        contexts: list[_FakeClientContext] = []

        class _FakeClientContext:
            def __init__(self, fake_client: FakeS3Client) -> None:
                self._fake_client = fake_client
                self.closed = False

            async def __aenter__(self) -> FakeS3Client:
                return self._fake_client

            async def __aexit__(self, *exc_info: object) -> None:
                self.closed = True

        @unchecked_fake("aiobotocore's S3 client surface")
        class _FakeSession:
            def __init__(self, **kwargs: object) -> None:
                pass

            def client(self, service_name: str, **kwargs: object) -> _FakeClientContext:
                assert service_name == "s3"
                contexts.append(_FakeClientContext(client))
                return contexts[-1]

        monkeypatch.setattr("aioboto3.Session", _FakeSession)
        buckets = await list_buckets(
            endpoint_url="http://192.0.2.1:1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        assert buckets == ["test-bucket"]
        assert [c.closed for c in contexts] == [True]

    async def test_list_buckets_maps_access_denied_to_permission_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        @unchecked_fake("aiobotocore's S3 client surface")
        class _DeniedClient:
            closed = False

            async def __aenter__(self) -> _DeniedClient:
                return self

            async def __aexit__(self, *exc_info: object) -> None:
                self.closed = True

            async def list_buckets(self) -> dict[str, object]:
                raise ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListBuckets")

        @unchecked_fake("aiobotocore's S3 client surface")
        class _FakeSession:
            def client(self, service_name: str, **kwargs: object) -> _DeniedClient:
                return denied

        denied = _DeniedClient()
        monkeypatch.setattr("aioboto3.Session", _FakeSession)
        with pytest.raises(PermissionDeniedError, match="access denied"):
            await list_buckets(endpoint_url="http://192.0.2.1:1")
        assert denied.closed  # closed on the error path too

    def test_raises_importerror_when_aioboto3_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        with pytest.raises(ImportError, match="import of aioboto3 halted"):
            asyncio.run(list_buckets())


class TestListdirPagination:
    async def test_listdir_merges_common_prefixes_and_contents_in_one_page(self, client: FakeS3Client) -> None:
        for i in range(5):
            await client.put_object(Bucket="test-bucket", Key=f"dir/file{i}.txt", Body=str(i).encode())
        await client.put_object(Bucket="test-bucket", Key="dir/sub/nested.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        entries = await store.listdir("dir")
        assert [entry.name for entry in entries] == sorted([f"file{i}.txt" for i in range(5)] + ["sub"])

    async def test_listdir_follows_continuation_token_past_the_real_1000_key_page_limit(
        self, client: FakeS3Client
    ) -> None:
        """1001 keys force a second 1000-key page; a Pool directory can
        exceed 1000 entries."""
        n = 1001
        await asyncio.gather(
            *(client.put_object(Bucket="test-bucket", Key=f"dir/f{i:04d}.txt", Body=b"x") for i in range(n))
        )
        store = S3Store("test-bucket", client=client)
        entries = await store.listdir("dir")
        assert [entry.name for entry in entries] == sorted(f"f{i:04d}.txt" for i in range(n))


class TestListdirSizes:
    async def test_objects_carry_their_size_and_a_common_prefix_has_none(self, client: FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="dir/a.bin", Body=b"12345")
        await client.put_object(Bucket="test-bucket", Key="dir/b.bin", Body=b"")
        await client.put_object(Bucket="test-bucket", Key="dir/sub/nested.bin", Body=b"x")
        store = S3Store("test-bucket", client=client)

        assert await store.listdir("dir") == [Entry("a.bin", 5), Entry("b.bin", 0), Entry("sub", None)]

    async def test_every_page_of_a_paginated_listing_carries_sizes(self, client: FakeS3Client) -> None:
        n = 1001
        await asyncio.gather(
            *(client.put_object(Bucket="test-bucket", Key=f"dir/f{i:04d}.bin", Body=b"ab") for i in range(n))
        )
        store = S3Store("test-bucket", client=client)

        entries = await store.listdir("dir")

        assert len(entries) == n
        assert {entry.size for entry in entries} == {2}

    async def test_an_absent_prefix_is_empty_not_an_error(self, client: FakeS3Client) -> None:
        assert await S3Store("test-bucket", client=client).listdir("nope") == []


class TestRepr:
    async def test_repr_does_not_crash(self, client: FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        assert "S3Store" in repr(store)
        assert "test-bucket" in repr(store)


class TestDefaultTimeouts:
    """A client built against an unreachable endpoint must not inherit
    botocore's 60s connect/60s read defaults."""

    def test_fills_in_short_connect_and_read_timeouts_when_caller_passes_no_config(self) -> None:
        merged = _with_default_timeouts({"endpoint_url": "http://example.invalid"})
        config = merged["config"]
        assert config.connect_timeout < 60
        assert config.read_timeout < 60
        assert merged["endpoint_url"] == "http://example.invalid"

    def test_a_caller_supplied_config_field_overrides_the_default_for_that_field_only(self) -> None:
        merged = _with_default_timeouts({"config": Config(connect_timeout=1)})
        config = merged["config"]
        assert config.connect_timeout == 1
        assert config.read_timeout < 60

    def test_raises_the_default_connection_pool_size_above_botocores_own_default_of_10(self) -> None:
        """One client serves a whole session's callers (e.g.
        ``ExportTuning``'s concurrent reads and opens); botocore's pool of
        10 would queue them inside the connector."""
        merged = _with_default_timeouts({"endpoint_url": "http://example.invalid"})
        config = merged["config"]
        assert config.max_pool_connections > 10

    def test_a_caller_supplied_max_pool_connections_overrides_the_default(self) -> None:
        merged = _with_default_timeouts({"config": Config(max_pool_connections=5)})
        config = merged["config"]
        assert config.max_pool_connections == 5
