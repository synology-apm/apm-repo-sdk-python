"""Unit tests for ``synology_apm_repo.sdk.storage.s3`` — the pieces
specific to ``S3Store`` itself (pagination, the lazy-import guard, error
code mapping) rather than the generic ``ObjectStore`` contract, which
lives in ``test_storage_object_store_contract.py`` alongside
``LocalFsStore``/``AzureStore``. Backed by an in-memory fake client
(``_FakeS3Client`` below), the same house style
``test_storage_azure.py``'s mocked ``BlobServiceClient`` already uses —
no real or emulated network server, no real AWS credentials.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.s3 import S3Store, _with_default_timeouts, list_buckets


def _not_found(operation: str, code: str = "NoSuchKey") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "not found"}}, operation)


class _FakeStreamingBody:
    """Stands in for ``get_object``'s ``Body`` — a real response body is
    an async-``read``-once stream with a synchronous ``close()`` (see
    ``S3Store.read``'s own comment on why a cancelled read closes rather
    than releases it)."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.closed = False

    async def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.closed = True


class _FakePaginator:
    """Stands in for ``client.get_paginator("list_objects_v2")`` — real
    aiobotocore's own paginator is likewise a plain (non-coroutine)
    ``.paginate(...)`` call returning something ``async for``'d over,
    driving the same ``ContinuationToken`` loop a real one would."""

    def __init__(self, client: _FakeS3Client) -> None:
        self._client = client

    async def paginate(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        token = None
        while True:
            page = await self._client.list_objects_v2(ContinuationToken=token, **kwargs)
            yield page
            if not page.get("IsTruncated"):
                return
            token = page["NextContinuationToken"]


class _FakeS3Client:
    """A minimal, real-enough-to-drive-``S3Store`` in-memory S3: real
    ``Range``-header parsing, real ``NoSuchKey``/``404``/``InvalidRange``
    error codes at the call sites ``S3Store`` actually maps, and a real
    1000-key-per-page ``list_objects_v2``/``ContinuationToken`` loop
    (AWS's own hard page limit) so
    ``test_listdir_follows_continuation_token_past_the_real_1000_key_page_limit``
    below still exercises ``S3Store.listdir``'s own paginator-driven loop
    for real, not just against a single-page fixture."""

    _PAGE_SIZE = 1000

    def __init__(self) -> None:
        self._buckets: set[str] = set()
        self._objects: dict[tuple[str, str], bytes] = {}
        self.closed = False

    async def create_bucket(self, *, Bucket: str) -> None:
        self._buckets.add(Bucket)

    async def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self._objects[(Bucket, Key)] = Body

    async def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, Any]:
        content = self._objects.get((Bucket, Key))
        if content is None:
            raise _not_found("GetObject", "NoSuchKey")
        start, end = _parse_range(Range, len(content))
        if start >= len(content) and len(content) > 0:
            raise _not_found("GetObject", "InvalidRange")
        return {"Body": _FakeStreamingBody(content[start:end])}

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        content = self._objects.get((Bucket, Key))
        if content is None:
            raise _not_found("HeadObject", "404")
        return {"ContentLength": len(content)}

    async def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str = "",
        MaxKeys: int = _PAGE_SIZE,
        Delimiter: str | None = None,
        ContinuationToken: str | None = None,
    ) -> dict[str, Any]:
        keys = sorted(key for bucket, key in self._objects if bucket == Bucket and key.startswith(Prefix))
        start_index = keys.index(ContinuationToken) + 1 if ContinuationToken is not None else 0
        page_size = min(MaxKeys, self._PAGE_SIZE)
        page_keys = keys[start_index : start_index + page_size]

        contents: list[str] = []
        common_prefixes: list[str] = []
        seen_prefixes: set[str] = set()
        for key in page_keys:
            rest = key[len(Prefix) :]
            if Delimiter and Delimiter in rest:
                prefix = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
                    common_prefixes.append(prefix)
            else:
                contents.append(key)

        is_truncated = start_index + len(page_keys) < len(keys)
        result: dict[str, Any] = {
            "Contents": [{"Key": key} for key in contents],
            "CommonPrefixes": [{"Prefix": prefix} for prefix in common_prefixes],
            "IsTruncated": is_truncated,
        }
        if is_truncated:
            result["NextContinuationToken"] = page_keys[-1]
        return result

    def get_paginator(self, operation_name: str) -> _FakePaginator:
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self)

    async def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": [{"Name": name} for name in sorted(self._buckets)]}

    async def close(self) -> None:
        self.closed = True


def _parse_range(range_header: str, size: int) -> tuple[int, int]:
    """``"bytes=X-Y"``/``"bytes=X-"`` -> ``(start, end)``, ``end`` already
    clamped to ``size`` — the same short-read-at-EOF the real service
    gives for a valid start past a requested (but not actual) end."""
    start_s, _, end_s = range_header.removeprefix("bytes=").partition("-")
    start = int(start_s)
    end = int(end_s) + 1 if end_s else size
    return start, min(end, size)


@pytest.fixture
async def client() -> _FakeS3Client:
    c = _FakeS3Client()
    await c.create_bucket(Bucket="test-bucket")
    return c


class TestLazyImportPropagatesImportError:
    def test_raises_importerror_when_aioboto3_is_missing_and_no_client_given(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sys.modules["aioboto3"] = None`` makes a subsequent ``import
        aioboto3`` raise ``ImportError`` (a real, documented CPython import
        system behavior — not a mock), simulating a broken/partial install
        without needing to actually uninstall aioboto3 from this dev
        environment."""
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        with pytest.raises(ImportError):
            S3Store("some-bucket")

    def test_does_not_import_aioboto3_when_a_client_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A caller supplying their own client (as every test in this
        file does, and as any real caller wiring in credentials centrally
        would) never needs aioboto3 importable at construction time at
        all — only methods that actually call the client do, and those
        already have one."""
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        client = object()
        store = S3Store("some-bucket", client=client)
        # Reaching this line at all already proves no ImportError fired;
        # the real claim this test makes is that the injected client is
        # used as-is (not rewrapped) and marked as not owned by the store.
        assert store._client is client
        assert store._owns_client is False


class TestErrorMapping:
    async def test_read_past_eof_returns_empty_bytes_not_an_error(self, client: _FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.read("f.txt", offset=100, length=10) == b""

    async def test_read_missing_key_raises_not_found(self, client: _FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        with pytest.raises(NotFoundError):
            await store.read("nope.txt")

    async def test_size_missing_key_raises_not_found(self, client: _FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        with pytest.raises(NotFoundError):
            await store.size("nope.txt")

    async def test_size_returns_content_length_for_an_existing_object(self, client: _FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.size("f.txt") == 5

    async def test_exists_false_for_missing_key_true_for_present(self, client: _FakeS3Client) -> None:
        await client.put_object(Bucket="test-bucket", Key="f.txt", Body=b"hello")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("f.txt") is True
        assert await store.exists("nope.txt") is False

    async def test_exists_true_for_a_directory_prefix_with_no_key_of_its_own(self, client: _FakeS3Client) -> None:
        # "dir" itself is never an object (head_object() 404s for it),
        # only a prefix with a real object underneath -- exists() must
        # still report True via the list_objects_v2() fallback, same
        # contract test_storage_azure.py's exists() pins down.
        await client.put_object(Bucket="test-bucket", Key="dir/file.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("dir") is True

    async def test_exists_false_for_a_similarly_named_sibling_key_not_a_real_directory(
        self, client: _FakeS3Client
    ) -> None:
        # "dirfile.txt" shares "dir" as a literal string prefix but isn't
        # inside a "dir/" directory -- proves prefix_listing.as_list_prefix's
        # trailing-slash disambiguation (Prefix="dir/", not "dir") is what
        # actually keeps the list_objects_v2() fallback from false-matching
        # an unrelated sibling key, not just an untested implementation detail.
        await client.put_object(Bucket="test-bucket", Key="dirfile.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        assert await store.exists("dir") is False


class TestCancellation:
    """A cancellation landing mid-body-read (Textual's ``Worker.cancel()``
    is fire-and-forget, never awaiting the underlying Task's actual
    unwind — a fresh request issued right after "cancelling..." can race
    a still-in-flight one on the same keep-alive connection pool) must
    not let the partially-read response quietly return its connection to
    the pool, which can otherwise hand a later request a broken
    connection (``ClientError("SlowDownRead", ...)``). Uses a fake client
    with deterministic control over exactly when the cancellation lands,
    not real, inherently racy network timing."""

    async def test_a_cancelled_read_closes_the_response_body_rather_than_letting_it_be_pooled(self) -> None:
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
    """Every other test in this file injects an already-built ``client``
    (as ``S3Store``'s constructor optionally accepts) to skip aioboto3's
    own connection setup — this test instead lets ``S3Store`` build and
    own a real ``aioboto3`` client itself, the path a real caller
    actually takes. ``endpoint_url`` is a real but never-dialed address
    (a reserved TEST-NET-1 host, RFC 5737): entering/exiting an
    ``aioboto3`` client's own async context manager does no real network
    I/O by itself, only connector/session setup — only an actual
    operation call like ``get_object`` would need a reachable endpoint,
    and this test issues
    none, so no fake server is needed to prove ``_get_client()``'s own
    laziness/caching/close behavior."""

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
            await store.aclose()
            await store.aclose()  # idempotent - must not raise


async def test_aclose_leaves_an_injected_client_untouched(client: _FakeS3Client) -> None:
    # An S3Store built with client=... never owns it (_owns_client is
    # False) -- aclose() must not close it, unlike the TestLazyClientLifecycle
    # case above where the store built the client itself.
    store = S3Store("test-bucket", client=client)
    assert store._owns_client is False
    await store.aclose()
    assert client.closed is False


class TestUnhandledClientErrors:
    """Any error code other than the ones each method special-cases
    (``InvalidRange``/not-found) must propagate unchanged — a permissions
    error, say, must never be silently swallowed or reinterpreted."""

    class _FakeClient:
        def __init__(self, exc: ClientError) -> None:
            self._exc = exc

        async def get_object(self, **kwargs: object) -> dict[str, object]:
            raise self._exc

        async def head_object(self, **kwargs: object) -> dict[str, object]:
            raise self._exc

    @staticmethod
    def _access_denied() -> ClientError:
        return ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "GetObject")

    async def test_read_reraises_an_unmapped_client_error(self) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(self._access_denied()))
        with pytest.raises(ClientError, match="AccessDenied"):
            await store.read("f.txt")

    async def test_size_reraises_an_unmapped_client_error(self) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(self._access_denied()))
        with pytest.raises(ClientError, match="AccessDenied"):
            await store.size("f.txt")

    async def test_exists_reraises_a_non_not_found_client_error_from_head_object(self) -> None:
        store = S3Store("test-bucket", client=self._FakeClient(self._access_denied()))
        with pytest.raises(ClientError, match="AccessDenied"):
            await store.exists("f.txt")


class TestListBuckets:
    async def test_list_buckets_returns_every_bucket_visible_to_these_credentials(
        self, monkeypatch: pytest.MonkeyPatch, client: _FakeS3Client
    ) -> None:
        """``list_buckets()`` builds its own transient client rather than
        accepting an injected one (see its own docstring) — monkeypatching
        ``aioboto3.Session`` itself to hand back this fixture's fake
        client is the same shape ``test_storage_azure.py``'s own
        ``TestListContainers`` test uses for ``AzureStore``'s equivalent
        free function."""

        class _FakeClientContext:
            def __init__(self, fake_client: _FakeS3Client) -> None:
                self._fake_client = fake_client

            async def __aenter__(self) -> _FakeS3Client:
                return self._fake_client

            async def __aexit__(self, *exc_info: object) -> None:
                return None

        class _FakeSession:
            def __init__(self, **kwargs: object) -> None:
                pass

            def client(self, service_name: str, **kwargs: object) -> _FakeClientContext:
                assert service_name == "s3"
                return _FakeClientContext(client)

        monkeypatch.setattr("aioboto3.Session", _FakeSession)
        buckets = await list_buckets(
            endpoint_url="http://192.0.2.1:1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        assert buckets == ["test-bucket"]

    def test_raises_importerror_when_aioboto3_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "aioboto3", None)
        with pytest.raises(ImportError):
            asyncio.run(list_buckets())


class TestListdirPagination:
    async def test_listdir_merges_common_prefixes_and_contents_in_one_page(self, client: _FakeS3Client) -> None:
        for i in range(5):
            await client.put_object(Bucket="test-bucket", Key=f"dir/file{i}.txt", Body=str(i).encode())
        await client.put_object(Bucket="test-bucket", Key="dir/sub/nested.txt", Body=b"x")
        store = S3Store("test-bucket", client=client)
        entries = await store.listdir("dir")
        assert entries == sorted([f"file{i}.txt" for i in range(5)] + ["sub"])

    async def test_listdir_follows_continuation_token_past_the_real_1000_key_page_limit(
        self, client: _FakeS3Client
    ) -> None:
        """``list_objects_v2`` pages at up to 1000 keys per call — AWS's
        own hard default, mirrored exactly by ``_FakeS3Client`` above — so
        1001 puts here genuinely forces a second page, exercising
        ``listdir``'s own paginator-driven loop for real rather than only
        against a single-page fixture. A real bucket's Pool layer
        directory can easily exceed 1000 entries the same way."""
        n = 1001
        await asyncio.gather(
            *(client.put_object(Bucket="test-bucket", Key=f"dir/f{i:04d}.txt", Body=b"x") for i in range(n))
        )
        store = S3Store("test-bucket", client=client)
        entries = await store.listdir("dir")
        assert entries == sorted(f"f{i:04d}.txt" for i in range(n))


class TestRepr:
    async def test_repr_does_not_crash(self, client: _FakeS3Client) -> None:
        store = S3Store("test-bucket", client=client)
        assert "S3Store" in repr(store)
        assert "test-bucket" in repr(store)


class TestDefaultTimeouts:
    """Interactive callers (the TUI's connect dialog, in particular)
    building a client against an unreachable endpoint must not inherit
    botocore's own 60s connect/60s read defaults — see the module
    docstring."""

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
        # read_timeout was never set by the caller, so it still falls back
        # to this module's own default rather than botocore's 60s one.
        assert config.read_timeout < 60

    def test_raises_the_default_connection_pool_size_above_botocores_own_default_of_10(self) -> None:
        """chunk_walk.py's max_concurrent_reads and
        max_concurrent_opens both draw from one
        S3Store's shared client — botocore's own default of 10
        (aiobotocore hands this straight to aiohttp.TCPConnector(limit=...))
        would silently queue connections inside the pool itself once
        either knob (or both together) pushes concurrency past it, on
        top of whatever the network/server is already doing."""
        merged = _with_default_timeouts({"endpoint_url": "http://example.invalid"})
        config = merged["config"]
        assert config.max_pool_connections > 10

    def test_a_caller_supplied_max_pool_connections_overrides_the_default(self) -> None:
        merged = _with_default_timeouts({"config": Config(max_pool_connections=5)})
        config = merged["config"]
        assert config.max_pool_connections == 5
