"""Unit tests for ``synology_apm_repo.sdk.storage.store_descriptor`` —
``describe_store()``/``rebuild_store()``'s round trip per backend, and the
"can't reconstruct" cases that come back ``None`` rather than raise. Every
backend's constructor is synchronous and does no network I/O, so an
un-entered ``S3Store``/``AzureStore``/``SmbStore`` is safe to build offline.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from support.fakes import faithful_to, unchecked_fake
from synology_apm_repo.sdk.storage.azure import AzureStore, AzureStoreDescriptor
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore
from synology_apm_repo.sdk.storage.local import LocalFsStore, LocalFsStoreDescriptor
from synology_apm_repo.sdk.storage.s3 import S3Store, S3StoreDescriptor
from synology_apm_repo.sdk.storage.smb import SmbStore, SmbStoreDescriptor
from synology_apm_repo.sdk.storage.store_descriptor import describe_store, rebuild_store


@faithful_to(ObjectStore)
class _UnrecognizedStore:
    """An ``ObjectStore`` without ``descriptor()``, as a ``TracingStore``
    wrapper is."""

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        return b""

    async def size(self, path: str) -> int:
        return 0

    async def exists(self, path: str) -> bool:
        return True

    async def close(self) -> None:
        pass

    async def listdir(self, path: str) -> list[Entry]:
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
    store = S3Store("my-bucket", endpoint_url="https://example.invalid:9000", aws_access_key_id="ak")
    descriptor = describe_store(store)
    assert descriptor == S3StoreDescriptor(
        bucket="my-bucket", client_kwargs={"endpoint_url": "https://example.invalid:9000", "aws_access_key_id": "ak"}
    )
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, S3Store)


def test_s3_store_with_an_injected_client_is_not_describable() -> None:
    # An injected client has no picklable recipe.
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
    @unchecked_fake("azure.storage.blob's async BlobServiceClient")
    class _FakeServiceClient:
        def get_container_client(self, container: str) -> object:
            class _Client:
                container_name = container

            return _Client()

    store = AzureStore("my-container", client=cast(Any, _FakeServiceClient()))
    assert describe_store(store) is None


def test_smb_store_round_trips() -> None:
    # SmbStore takes no injected client, so it is always describable.
    store = SmbStore("my-share", server="10.0.0.1", username="admin", password="secret")
    descriptor = describe_store(store)
    assert descriptor == SmbStoreDescriptor(
        share="my-share", server="10.0.0.1", port=445, username="admin", password="secret"
    )
    rebuilt = rebuild_store(descriptor)
    assert isinstance(rebuilt, SmbStore)


def test_rebuilt_local_fs_store_reads_the_same_bytes_as_the_original(tmp_path: str) -> None:
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
