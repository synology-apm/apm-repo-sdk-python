"""Cross-backend ``ObjectStore`` contract tests: the same test bodies, run
against ``LocalFsStore``, ``S3Store`` (backed by an in-memory fake client,
``_FakeS3Client`` below — no real or emulated network server, the same
house style ``test_storage_s3.py``'s own copy of this class uses),
``AzureStore`` (backed by a
mocked async ``BlobServiceClient`` — deliberately chosen over
requiring a live Azurite instance), and ``SmbStore`` (backed by a mocked
``smbclient`` module — deliberately chosen over spinning up a real SMB
server, the same trade-off ``AzureStore``'s own fake already makes here).
Four backends behaving differently
here would be a bug, not an expected difference — **with one disclosed
exception**: ``listdir`` on an absent "directory". Object storage has
no directory entities to check for (a prefix with zero matches and one
that was "never created" are the same observable state), so
``S3Store``/``AzureStore`` return ``[]`` there while ``LocalFsStore``/
``SmbStore`` (real filesystems, which *do* have directory entities) raise
``NotFoundError`` — every one of those modules' own
docstrings document this as deliberate, and it is tested explicitly as a
*difference*, not folded into the shared parametrized cases below.

Backend-specific internals that aren't part of the generic ``ObjectStore``
contract at all (``LocalFsStore``'s fd cache and ``..``-escape
prevention; ``S3Store``'s pagination loop and lazy-import guard;
``AzureStore``'s ``walk_blobs`` shape; ``SmbStore``'s per-instance
``connection_cache`` isolation) stay in their own dedicated test
files (``test_storage_local.py``, ``test_storage_s3.py``,
``test_storage_azure.py``, ``test_storage_smb.py``) rather than being
force-fit into this one.
"""

from __future__ import annotations

import asyncio
import errno
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import smbclient

from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.base import ObjectStore, join_path
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore

# One fixed content tree, uploaded/written identically to every backend:
_FILES = {
    "a.txt": b"0123456789",
    "sub/b.txt": b"hello world",
}


def _make_local(tmp_path: Path) -> ObjectStore:
    for rel, content in _FILES.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
    return LocalFsStore(tmp_path)


def _s3_parse_range(range_header: str, size: int) -> tuple[int, int]:
    """``"bytes=X-Y"``/``"bytes=X-"`` -> ``(start, end)``, ``end`` already
    clamped to ``size`` — see ``test_storage_s3.py``'s own copy of this
    same helper for the full rationale (duplicated per this project's
    "no test module imports from another" convention)."""
    start_s, _, end_s = range_header.removeprefix("bytes=").partition("-")
    start = int(start_s)
    end = int(end_s) + 1 if end_s else size
    return start, min(end, size)


class _FakeS3Paginator:
    """See ``test_storage_s3.py``'s own, fuller copy of this class for
    the real-1000-key-page-limit rationale — this file's own fixed
    two-file tree never needs a second page, but ``S3Store.listdir()``
    always drives this same paginator regardless of dataset size."""

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
    """A minimal in-memory S3 -- see ``test_storage_s3.py``'s own, fuller
    copy of this class for the full rationale behind each error code."""

    _PAGE_SIZE = 1000

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, content: bytes) -> None:
        self._objects[key] = content

    async def get_object(self, *, Bucket: str, Key: str, Range: str) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        content = self._objects.get(Key)
        if content is None:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject")
        start, end = _s3_parse_range(Range, len(content))
        if start >= len(content):
            raise ClientError({"Error": {"Code": "InvalidRange", "Message": "range not satisfiable"}}, "GetObject")
        chunk = content[start:end]

        class _Body:
            async def read(self) -> bytes:
                return chunk

        return {"Body": _Body()}

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        content = self._objects.get(Key)
        if content is None:
            raise ClientError({"Error": {"Code": "404", "Message": "not found"}}, "HeadObject")
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
        keys = sorted(key for key in self._objects if key.startswith(Prefix))
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

    def get_paginator(self, operation_name: str) -> _FakeS3Paginator:
        assert operation_name == "list_objects_v2"
        return _FakeS3Paginator(self)


def _make_s3() -> ObjectStore:
    client = _FakeS3Client()
    for rel, content in _FILES.items():
        client.put(rel, content)
    return S3Store("test-bucket", client=client)


class _FakeAzureBlob:
    """A real class with ``async def`` methods, unlike
    ``test_storage_azure.py``'s ``MagicMock``/``AsyncMock``-based fake for
    the same ``BlobClient`` shape — see that module's docstring for why
    the shape (coroutine ``download_blob``/``get_blob_properties``) looks
    like this.
    """

    def __init__(self, content: bytes | None) -> None:
        self._content = content

    def _require_exists(self) -> bytes:
        from azure.core.exceptions import ResourceNotFoundError

        if self._content is None:
            # ResourceNotFoundError doesn't set .status_code to 404 on its
            # own unless constructed with a real ``response`` object (which
            # this fake has no reason to build) - set it explicitly so
            # AzureStore's own ``exc.status_code == 404`` check, which is
            # exactly what it does against the real SDK's exception too,
            # has something to match against.
            err = ResourceNotFoundError(message="blob not found")
            err.status_code = 404
            raise err
        return self._content

    async def download_blob(self, *, offset: int = 0, length: int | None = None) -> MagicMock:
        from azure.core.exceptions import HttpResponseError

        content = self._require_exists()
        if offset > len(content):
            err = HttpResponseError(message="range not satisfiable")
            err.status_code = 416
            raise err
        end = len(content) if length is None else offset + length
        chunk = content[offset:end]
        downloader = MagicMock()
        downloader.readall = AsyncMock(return_value=chunk)
        return downloader

    async def get_blob_properties(self) -> MagicMock:
        content = self._require_exists()
        props = MagicMock()
        props.size = len(content)
        return props


class _FakeAzureContainer:
    """Container counterpart to ``_FakeAzureBlob`` over the same
    ``_FILES`` tree — see ``test_storage_azure.py``'s module docstring
    for why ``walk_blobs`` is a plain method returning an async iterator
    rather than ``async def``."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.container_name = "test-container"  # AzureStore.__repr__ reads this, like the real ContainerClient

    def get_blob_client(self, name: str) -> _FakeAzureBlob:
        return _FakeAzureBlob(self._files.get(name))

    async def walk_blobs(self, *, name_starts_with: str = "", delimiter: str = "/") -> AsyncIterator[Any]:
        seen_prefixes: set[str] = set()
        for name in self._files:
            if not name.startswith(name_starts_with):
                continue
            rest = name[len(name_starts_with) :]
            if delimiter in rest:
                sub_prefix = name_starts_with + rest.split(delimiter, 1)[0] + delimiter
                if sub_prefix not in seen_prefixes:
                    seen_prefixes.add(sub_prefix)
                    prefix_item = MagicMock()
                    prefix_item.name = sub_prefix
                    yield prefix_item
            else:
                blob_item = MagicMock()
                blob_item.name = name
                yield blob_item


def _make_azure() -> ObjectStore:
    service_client = MagicMock()
    service_client.get_container_client.return_value = _FakeAzureContainer(_FILES)
    return AzureStore("test-container", client=service_client)


_FAKE_SMB_SERVER = "fakeserver"
_FAKE_SMB_SHARE = "test-share"


class _FakeSmbFile:
    """Stands in for the file object ``smbclient.open_file()`` returns —
    only the ``seek``/``read`` subset ``SmbStore`` actually calls."""

    def __init__(self, content: bytes) -> None:
        self._content = content
        self._pos = 0

    def __enter__(self) -> _FakeSmbFile:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def seek(self, offset: int) -> None:
        self._pos = offset

    def read(self, length: int | None = None) -> bytes:
        end = len(self._content) if length is None else self._pos + length
        chunk = self._content[self._pos : end]
        self._pos += len(chunk)
        return chunk


class _FakeSmbStat:
    def __init__(self, size: int) -> None:
        self.st_size = size


class _FakeSmbClientModule:
    """A real class with plain methods, standing in for the ``smbclient``
    module's own functions — unlike ``_FakeAzureBlob``/
    ``_FakeAzureContainer``'s ``async def`` methods, ``smbclient``'s real
    functions are synchronous, so this fake's methods are too, matching
    its shape exactly. Every method accepts and ignores
    ``connection_cache``/other keyword args ``SmbStore`` passes through,
    the same way the real ``smbclient`` functions do."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.path = self  # smbclient.path.exists -> self.exists, same object
        self.registered: list[str] = []
        self.deleted: list[str] = []

    def _rel(self, unc: str) -> str:
        prefix = f"\\\\{_FAKE_SMB_SERVER}\\{_FAKE_SMB_SHARE}"
        assert unc == prefix or unc.startswith(prefix + "\\"), unc
        return unc[len(prefix) :].lstrip("\\").replace("\\", "/")

    def register_session(self, server: str, **kwargs: Any) -> None:
        self.registered.append(server)

    def delete_session(self, server: str, **kwargs: Any) -> None:
        self.deleted.append(server)

    def open_file(self, unc: str, mode: str = "rb", **kwargs: Any) -> _FakeSmbFile:
        content = self._files.get(self._rel(unc))
        if content is None:
            raise OSError(errno.ENOENT, "no such file", unc)
        return _FakeSmbFile(content)

    def stat(self, unc: str, **kwargs: Any) -> _FakeSmbStat:
        content = self._files.get(self._rel(unc))
        if content is None:
            raise OSError(errno.ENOENT, "no such file", unc)
        return _FakeSmbStat(len(content))

    def exists(self, unc: str, **kwargs: Any) -> bool:
        rel = self._rel(unc)
        if rel == "" or rel in self._files:
            return True
        return any(name.startswith(rel + "/") for name in self._files)

    def listdir(self, unc: str, **kwargs: Any) -> list[str]:
        rel = self._rel(unc)
        prefix = "" if rel == "" else rel + "/"
        names: set[str] = set()
        for name in self._files:
            if not name.startswith(prefix):
                continue
            names.add(name[len(prefix) :].split("/", 1)[0])
        if not names and rel != "" and rel not in self._files:
            # Every real directory in _FILES has >=1 entry, so a
            # zero-match prefix here always means "never existed" -
            # SmbStore's own contract-difference test relies on this.
            raise OSError(errno.ENOENT, "no such directory", unc)
        return sorted(names)


def _make_smb(monkeypatch: pytest.MonkeyPatch) -> ObjectStore:
    fake = _FakeSmbClientModule(_FILES)
    monkeypatch.setattr(smbclient, "register_session", fake.register_session)
    monkeypatch.setattr(smbclient, "delete_session", fake.delete_session)
    monkeypatch.setattr(smbclient, "open_file", fake.open_file)
    monkeypatch.setattr(smbclient, "stat", fake.stat)
    monkeypatch.setattr(smbclient, "listdir", fake.listdir)
    monkeypatch.setattr(smbclient.path, "exists", fake.exists)
    return SmbStore(_FAKE_SMB_SHARE, server=_FAKE_SMB_SERVER, username="admin", password="secret")


@pytest.fixture(params=["local", "s3", "azure", "smb"])
async def store(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ObjectStore]:
    if request.param == "local":
        yield _make_local(tmp_path)
    elif request.param == "s3":
        yield _make_s3()
    elif request.param == "smb":
        yield _make_smb(monkeypatch)
    else:
        yield _make_azure()


async def test_satisfies_the_object_store_protocol(store: ObjectStore) -> None:
    # ObjectStore is @runtime_checkable — this isinstance check is the
    # actual proof the four-method shape matches, not just a docstring
    # claim (carried over from the pre-real-implementation skeleton era's
    # own test of this, which otherwise would have had no successor).
    assert isinstance(store, ObjectStore)


async def test_read_whole_file(store: ObjectStore) -> None:
    assert await store.read("a.txt") == b"0123456789"


async def test_read_with_offset_and_length(store: ObjectStore) -> None:
    assert await store.read("a.txt", offset=3, length=4) == b"3456"


async def test_read_offset_only_reads_to_eof(store: ObjectStore) -> None:
    assert await store.read("a.txt", offset=8) == b"89"


async def test_read_short_at_eof_does_not_raise(store: ObjectStore) -> None:
    """Every backend must treat an over-long read at/past EOF as a short
    read, never an error — this is the exact contract line S3/Azure need
    their own status-code translation (``InvalidRange``/416) for."""
    assert await store.read("a.txt", offset=8, length=100) == b"89"


async def test_read_nested_path(store: ObjectStore) -> None:
    assert await store.read("sub/b.txt") == b"hello world"


async def test_read_missing_raises_not_found(store: ObjectStore) -> None:
    with pytest.raises(NotFoundError):
        await store.read("does/not/exist.txt")


async def test_size(store: ObjectStore) -> None:
    assert await store.size("a.txt") == 10
    assert await store.size("sub/b.txt") == 11


async def test_size_missing_raises(store: ObjectStore) -> None:
    with pytest.raises(NotFoundError):
        await store.size("nope")


async def test_exists(store: ObjectStore) -> None:
    assert await store.exists("a.txt") is True
    assert await store.exists("sub") is True  # a directory-ish prefix, not just a plain file
    assert await store.exists("nope") is False


async def test_listdir_sorted(store: ObjectStore) -> None:
    assert await store.listdir("") == ["a.txt", "sub"]
    assert await store.listdir("sub") == ["b.txt"]


# -- the one disclosed, deliberate cross-backend difference --------------


async def test_listdir_on_missing_directory_local_raises(tmp_path: Path) -> None:
    store = _make_local(tmp_path)
    with pytest.raises(NotFoundError):
        await store.listdir("nope")


async def test_listdir_on_missing_directory_smb_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _make_smb(monkeypatch)
    with pytest.raises(NotFoundError):
        await store.listdir("nope")


async def test_listdir_on_missing_prefix_s3_returns_empty() -> None:
    assert await _make_s3().listdir("nope") == []


async def test_listdir_on_missing_prefix_azure_returns_empty() -> None:
    store = _make_azure()
    assert await store.listdir("nope") == []


# A second disclosed, deliberate cross-backend difference: only the two
# real-filesystem backends (local, SMB) have an OS-level permission concept
# to translate at all — S3/Azure leave every non-not-found ClientError/
# HttpResponseError untranslated by their own existing design (see each
# module's own docstring), so there's no equivalent case to test there.


async def test_listdir_on_permission_denied_directory_local_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _make_local(tmp_path)

    def raising_iterdir(self: Path) -> Any:
        raise PermissionError()

    monkeypatch.setattr(Path, "iterdir", raising_iterdir)
    with pytest.raises(PermissionDeniedError):
        await store.listdir("sub")


async def test_listdir_on_permission_denied_directory_smb_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _make_smb(monkeypatch)

    def raising_listdir(unc: str, **kwargs: Any) -> list[str]:
        raise OSError(errno.EACCES, "permission denied", unc)

    monkeypatch.setattr(smbclient, "listdir", raising_listdir)
    with pytest.raises(PermissionDeniedError):
        await store.listdir("sub")


# -- join_path -------------------------------------------------------------


class TestJoinPath:
    def test_joins_plain_segments_with_a_slash(self) -> None:
        assert join_path("repo", "sub", "file.txt") == "repo/sub/file.txt"

    def test_a_single_segment_passes_through(self) -> None:
        assert join_path("only") == "only"

    def test_no_segments_yields_an_empty_string(self) -> None:
        assert join_path() == ""

    def test_an_empty_leading_segment_is_dropped(self) -> None:
        # repo_root is often "" for an object-store bucket with no
        # per-repository prefix.
        assert join_path("", "sub", "file.txt") == "sub/file.txt"

    def test_every_segment_empty_yields_an_empty_string(self) -> None:
        assert join_path("", "") == ""

    def test_stray_leading_and_trailing_slashes_are_stripped_per_segment(self) -> None:
        assert join_path("repo/", "/sub/", "/file.txt") == "repo/sub/file.txt"

    def test_a_segment_that_is_only_slashes_strips_to_empty_but_still_joins(self) -> None:
        # the emptiness check happens before stripping, so a
        # slashes-only segment is truthy going in and leaves a blank
        # joined element behind (an empty, not an absent, segment).
        assert join_path("repo", "/", "file.txt") == "repo//file.txt"


# -- concurrency (ObjectStore's "safe under concurrent Tasks/threads"
# invariant, base.py's ObjectStore docstring) --------------------------


async def test_local_fs_store_concurrent_calls_return_correct_results(tmp_path: Path) -> None:
    """``LocalFsStore`` caches fds shared across ``asyncio.to_thread()``
    executor threads behind a real lock — this drives many concurrent
    ``read``/``exists``/``listdir`` calls against one store instance and
    checks every result is still correct, not just that nothing raises."""
    store = _make_local(tmp_path)

    async def read_a() -> bytes:
        return await store.read("a.txt")

    async def read_b() -> bytes:
        return await store.read("sub/b.txt")

    async def check_exists() -> bool:
        return await store.exists("a.txt")

    async def check_missing() -> bool:
        return await store.exists("does/not/exist")

    async def list_root() -> list[str]:
        return await store.listdir("")

    calls = [read_a(), read_b(), check_exists(), check_missing(), list_root()] * 20
    results = await asyncio.gather(*calls)

    for i in range(0, len(results), 5):
        assert results[i] == b"0123456789"
        assert results[i + 1] == b"hello world"
        assert results[i + 2] is True
        assert results[i + 3] is False
        assert results[i + 4] == ["a.txt", "sub"]
