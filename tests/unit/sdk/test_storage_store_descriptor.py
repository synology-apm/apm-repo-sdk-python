"""Unit tests for ``synology_apm_repo.sdk.storage.store_descriptor`` —
``describe_store()``/``rebuild_store()``'s own round-trip contract per
backend, and the "can't reconstruct" cases that must come back ``None``
rather than raise. No real network access anywhere here: every real
backend's constructor is synchronous and does no I/O — it only stores
configuration (or, for ``AzureStore``, builds a plain client object) and
defers any real connection to first use — so a plain, un-entered
``S3Store``/``AzureStore``/``SmbStore`` (no injected ``client=``) is safe
to construct and describe entirely offline.
"""

from __future__ import annotations

import asyncio

from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore
from synology_apm_repo.sdk.storage.store_descriptor import (
    AzureStoreDescriptor,
    LocalFsStoreDescriptor,
    S3StoreDescriptor,
    SmbStoreDescriptor,
    describe_store,
    rebuild_store,
)


class _UnrecognizedStore:
    """Implements the ``ObjectStore`` protocol structurally but isn't any
    of the four backends ``describe_store()`` knows by name — stands in
    for both a ``TracingStore``/``RecordingStore`` wrapper and any future
    backend this module hasn't been taught about yet."""

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def size(self, path: str) -> int:
        return 0

    async def exists(self, path: str) -> bool:
        return True

    async def listdir(self, path: str) -> list[str]:
        return []


def test_describe_store_returns_none_for_an_unrecognized_store() -> None:
    assert describe_store(_UnrecognizedStore()) is None


def test_local_fs_store_round_trips(tmp_path: str) -> None:
    store = LocalFsStore(tmp_path)
    descriptor = describe_store(store)
    assert descriptor == LocalFsStoreDescriptor(root=str(store.root))
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, LocalFsStore)
    assert rebuilt.root == store.root


def test_s3_store_round_trips_when_it_owns_its_own_client() -> None:
    # No client= override -- S3Store's constructor only stores
    # client_kwargs and builds the real client lazily on first use, so
    # this is safe entirely offline.
    store = S3Store("my-bucket", endpoint_url="https://example.invalid:9000", aws_access_key_id="ak")
    descriptor = describe_store(store)
    assert descriptor == S3StoreDescriptor(
        bucket="my-bucket", client_kwargs={"endpoint_url": "https://example.invalid:9000", "aws_access_key_id": "ak"}
    )
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, S3Store)


def test_s3_store_with_an_injected_client_is_not_describable() -> None:
    # A live, already-entered client has no picklable recipe -- describe_store()
    # must return None (a graceful "use the fallback path"), never raise.
    store = S3Store("my-bucket", client=object())
    assert describe_store(store) is None


def test_azure_store_round_trips_when_it_owns_its_own_client() -> None:
    store = AzureStore("my-container", account_url="http://example.invalid:10000/devstoreaccount1", credential="k")
    descriptor = describe_store(store)
    assert descriptor == AzureStoreDescriptor(
        container="my-container",
        client_kwargs={"account_url": "http://example.invalid:10000/devstoreaccount1", "credential": "k"},
    )
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, AzureStore)


def test_azure_store_with_an_injected_client_is_not_describable() -> None:
    class _FakeServiceClient:
        def get_container_client(self, container: str) -> object:
            class _Client:
                container_name = container

            return _Client()

    store = AzureStore("my-container", client=_FakeServiceClient())
    assert describe_store(store) is None


def test_smb_store_round_trips() -> None:
    # SmbStore's own session is always lazy (never built in __init__, no
    # injected-client concept at all) -- always describable.
    store = SmbStore("my-share", server="10.0.0.1", username="admin", password="secret")
    descriptor = describe_store(store)
    assert descriptor == SmbStoreDescriptor(
        share="my-share", server="10.0.0.1", port=445, username="admin", password="secret"
    )
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, SmbStore)


def test_rebuilt_local_fs_store_reads_the_same_bytes_as_the_original(tmp_path: str) -> None:
    """The one round-trip test that actually exercises real I/O (safe and
    fast for ``LocalFsStore`` — no real network involved) rather than just
    checking types/fields, closing the loop on "an equivalent store" for
    at least one backend."""
    from pathlib import Path

    root = Path(tmp_path)
    (root / "hello.txt").write_bytes(b"world")
    original = LocalFsStore(root)
    descriptor = describe_store(original)
    assert descriptor is not None
    rebuilt = rebuild_store(descriptor)

    async def _read_both() -> tuple[bytes, bytes]:
        return await original.read("hello.txt"), await rebuilt.read("hello.txt")

    original_bytes, rebuilt_bytes = asyncio.run(_read_both())
    assert original_bytes == rebuilt_bytes == b"world"
