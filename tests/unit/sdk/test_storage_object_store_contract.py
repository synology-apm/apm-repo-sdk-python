"""Cross-backend ``ObjectStore`` contract tests: the same test bodies run
against ``LocalFsStore``, ``S3Store`` (``FakeS3Client``), ``AzureStore``
(``_FakeAzureContainer``) and ``SmbStore`` (``_FakeSmbClientModule``), each
over an in-memory fake. The backends must behave identically, except where a
test below says otherwise: ``listdir`` on an absent directory returns ``[]``
on object storage (a prefix with no matches is indistinguishable from one
never created) but raises ``NotFoundError`` on the filesystem backends.
``storage/base.py``'s ``join_path`` and ``list_names`` are tested here too.

Backend-specific internals stay in ``test_storage_local.py``,
``test_storage_s3.py``, ``test_storage_azure.py`` and ``test_storage_smb.py``.
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

from support.fakes import unchecked_fake
from synology_apm_repo.sdk.errors import NotFoundError, PermissionDeniedError
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore, join_path, list_names
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore
from unit.sdk.storage_fakes import FakeS3Client

# The content tree every backend serves.
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


def _make_s3() -> ObjectStore:
    client = FakeS3Client()
    for rel, content in _FILES.items():
        client.seed("test-bucket", rel, content)
    return S3Store("test-bucket", client=client)


@unchecked_fake("azure.storage.blob's async clients")
class _FakeAzureBlob:
    """A ``BlobClient`` stand-in: in ``azure.storage.blob.aio``,
    ``download_blob``/``get_blob_properties`` are coroutines."""

    def __init__(self, content: bytes | None) -> None:
        self._content = content

    def _require_exists(self) -> bytes:
        from azure.core.exceptions import ResourceNotFoundError

        if self._content is None:
            # Without a ``response`` object ResourceNotFoundError has no
            # status_code; AzureStore checks ``exc.status_code == 404``.
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


@unchecked_fake("azure.storage.blob's async clients")
class _FakeAzureContainer:
    """Container counterpart to ``_FakeAzureBlob``. ``walk_blobs`` is a
    plain method returning an async iterator, as the real one returns an
    ``AsyncItemPaged``."""

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
                    prefix_item = MagicMock(spec=["name"])  # a BlobPrefix has no size
                    prefix_item.name = sub_prefix
                    yield prefix_item
            else:
                blob_item = MagicMock()
                blob_item.name = name
                blob_item.size = len(self._files[name])
                yield blob_item


def _make_azure() -> ObjectStore:
    service_client = MagicMock()
    service_client.get_container_client.return_value = _FakeAzureContainer(_FILES)
    return AzureStore("test-container", client=service_client)


_FAKE_SMB_SERVER = "fakeserver"
_FAKE_SMB_SHARE = "test-share"


@unchecked_fake("the smbclient module")
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


@unchecked_fake("the smbclient module")
class _FakeSmbClientModule:
    """Stands in for the ``smbclient`` module, whose functions are
    synchronous. Every method ignores ``connection_cache`` and the other
    keyword args ``SmbStore`` passes."""

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

    def _child_names(self, unc: str) -> list[str]:
        rel = self._rel(unc)
        prefix = "" if rel == "" else rel + "/"
        names: set[str] = set()
        for name in self._files:
            if not name.startswith(prefix):
                continue
            names.add(name[len(prefix) :].split("/", 1)[0])
        if not names and rel != "" and rel not in self._files:
            # Every directory in _FILES has an entry, so no match means
            # the directory doesn't exist.
            raise OSError(errno.ENOENT, "no such directory", unc)
        return sorted(names)

    def scandir(self, unc: str, **kwargs: Any) -> list[Any]:
        rel = self._rel(unc)
        prefix = "" if rel == "" else rel + "/"

        class _Entry:
            def __init__(self_inner, name: str, size: int | None) -> None:
                self_inner.name = name
                self_inner._size = size

            def is_dir(self_inner) -> bool:
                return self_inner._size is None

            def stat(self_inner) -> _FakeSmbStat:
                assert self_inner._size is not None
                return _FakeSmbStat(self_inner._size)

        return [
            _Entry(name, len(self._files[prefix + name]) if prefix + name in self._files else None)
            for name in self._child_names(unc)
        ]


def _make_smb(monkeypatch: pytest.MonkeyPatch) -> ObjectStore:
    fake = _FakeSmbClientModule(_FILES)
    monkeypatch.setattr(smbclient, "register_session", fake.register_session)
    monkeypatch.setattr(smbclient, "delete_session", fake.delete_session)
    monkeypatch.setattr(smbclient, "open_file", fake.open_file)
    monkeypatch.setattr(smbclient, "stat", fake.stat)
    monkeypatch.setattr(smbclient, "scandir", fake.scandir)
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
    # ObjectStore is @runtime_checkable: this checks the five methods exist.
    assert isinstance(store, ObjectStore)


@pytest.mark.parametrize(
    ("path", "kwargs", "expected"),
    [
        pytest.param("a.txt", {}, b"0123456789", id="whole_file"),
        pytest.param("a.txt", {"offset": 3, "length": 4}, b"3456", id="with_offset_and_length"),
        pytest.param("a.txt", {"offset": 8}, b"89", id="offset_only_reads_to_eof"),
        # A read past EOF is short, never an error (S3/Azure translate
        # InvalidRange/416 for it).
        pytest.param("a.txt", {"offset": 8, "length": 100}, b"89", id="short_at_eof_does_not_raise"),
        pytest.param("sub/b.txt", {}, b"hello world", id="nested_path"),
    ],
)
async def test_read(store: ObjectStore, path: str, kwargs: dict[str, int], expected: bytes) -> None:
    assert await store.read(path, **kwargs) == expected


async def test_read_missing_raises_not_found(store: ObjectStore) -> None:
    with pytest.raises(NotFoundError, match=r"no such (blob|file|object|path)"):
        await store.read("does/not/exist.txt")


async def test_size(store: ObjectStore) -> None:
    assert await store.size("a.txt") == 10
    assert await store.size("sub/b.txt") == 11


async def test_size_missing_raises(store: ObjectStore) -> None:
    with pytest.raises(NotFoundError, match=r"no such (blob|object|path)"):
        await store.size("nope")


async def test_exists(store: ObjectStore) -> None:
    assert await store.exists("a.txt") is True
    assert await store.exists("sub") is True  # a directory-ish prefix, not just a plain file
    assert await store.exists("nope") is False


async def test_listdir_is_sorted_entries_with_each_file_sized_and_directories_unsized(store: ObjectStore) -> None:
    root = await store.listdir("")
    assert root == [Entry("a.txt", 10), Entry("sub", None)]
    assert all(isinstance(entry, Entry) for entry in root)
    assert await store.listdir("sub") == [Entry("b.txt", 11)]


async def test_list_names_is_listdir_names_alone(store: ObjectStore) -> None:
    assert await list_names(store, "") == ["a.txt", "sub"]
    assert await list_names(store, "sub") == ["b.txt"]


# -- listdir on an absent directory: object storage vs filesystems -------


async def test_listdir_on_missing_directory_local_raises(tmp_path: Path) -> None:
    store = _make_local(tmp_path)
    with pytest.raises(NotFoundError, match="no such directory"):
        await store.listdir("nope")


async def test_listdir_on_missing_directory_smb_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _make_smb(monkeypatch)
    with pytest.raises(NotFoundError, match="no such path"):
        await store.listdir("nope")


async def test_listdir_on_missing_prefix_s3_returns_empty() -> None:
    assert await _make_s3().listdir("nope") == []


async def test_listdir_on_missing_prefix_azure_returns_empty() -> None:
    store = _make_azure()
    assert await store.listdir("nope") == []


# OS-level permission errors exist only on the filesystem backends; S3's
# and Azure's access-denied responses map to PermissionDeniedError too,
# tested in test_storage_s3.py/test_storage_azure.py.


async def test_listdir_on_permission_denied_directory_local_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _make_local(tmp_path)

    def raising_scandir(path: object) -> Any:
        raise PermissionError()

    monkeypatch.setattr("os.scandir", raising_scandir)
    with pytest.raises(PermissionDeniedError, match="permission denied"):
        await store.listdir("sub")


async def test_listdir_on_permission_denied_directory_smb_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _make_smb(monkeypatch)

    def raising_scandir(unc: str, **kwargs: Any) -> list[Any]:
        raise OSError(errno.EACCES, "permission denied", unc)

    monkeypatch.setattr(smbclient, "scandir", raising_scandir)
    with pytest.raises(PermissionDeniedError, match="permission denied"):
        await store.listdir("sub")


# -- join_path -------------------------------------------------------------


class TestJoinPath:
    @pytest.mark.parametrize(
        ("segments", "expected"),
        [
            pytest.param(("repo", "sub", "file.txt"), "repo/sub/file.txt", id="joins_plain_segments_with_a_slash"),
            pytest.param(("only",), "only", id="a_single_segment_passes_through"),
            pytest.param((), "", id="no_segments_yields_an_empty_string"),
            # repo_root is often "" for an object-store bucket with no
            # per-repository prefix.
            pytest.param(("", "sub", "file.txt"), "sub/file.txt", id="an_empty_leading_segment_is_dropped"),
            pytest.param(("", ""), "", id="every_segment_empty_yields_an_empty_string"),
            pytest.param(
                ("repo/", "/sub/", "/file.txt"),
                "repo/sub/file.txt",
                id="stray_leading_and_trailing_slashes_are_stripped_per_segment",
            ),
            # The emptiness check happens before stripping, so a
            # slashes-only segment leaves an empty (not absent) element.
            pytest.param(
                ("repo", "/", "file.txt"),
                "repo//file.txt",
                id="a_segment_that_is_only_slashes_strips_to_empty_but_still_joins",
            ),
            # "." can't escape the root, so join_path leaves it alone.
            pytest.param(("repo", ".", "file.txt"), "repo/./file.txt", id="a_lone_dot_segment_is_not_rejected"),
        ],
    )
    def test_join_path(self, segments: tuple[str, ...], expected: str) -> None:
        assert join_path(*segments) == expected

    @pytest.mark.parametrize(
        "segments",
        [
            pytest.param(("repo", "..", "secret"), id="a_bare_dotdot_segment"),
            # One catalog-derived string (a target_meta_path basename, a
            # saas_stream_uuid) can smuggle in several segments at once.
            pytest.param(("repo", "copy_meta_file/../../secret"), id="a_dotdot_embedded_in_one_catalog_derived_part"),
            pytest.param(("repo", "sub", ".."), id="a_trailing_dotdot_segment"),
            # Not a separator here, but pathlib on Windows reads it as one.
            pytest.param(("repo", "..\\..\\secret"), id="a_segment_containing_a_backslash"),
        ],
    )
    def test_an_escaping_segment_is_rejected(self, segments: tuple[str, ...]) -> None:
        with pytest.raises(NotFoundError, match=r"path segment .* (escapes the store root|contains a backslash)"):
            join_path(*segments)


# -- concurrency (ObjectStore's "safe under concurrent Tasks/threads"
# invariant, base.py's ObjectStore docstring) --------------------------


async def test_local_fs_store_concurrent_calls_return_correct_results(tmp_path: Path) -> None:
    """Each ``LocalFsStore`` call runs in its own ``asyncio.to_thread()``;
    many concurrent calls on one instance all return correct results."""
    store = _make_local(tmp_path)

    async def read_a() -> bytes:
        return await store.read("a.txt")

    async def read_b() -> bytes:
        return await store.read("sub/b.txt")

    async def check_exists() -> bool:
        return await store.exists("a.txt")

    async def check_missing() -> bool:
        return await store.exists("does/not/exist")

    async def list_root() -> list[Entry]:
        return await store.listdir("")

    calls = [read_a(), read_b(), check_exists(), check_missing(), list_root()] * 20
    results = await asyncio.gather(*calls)

    for i in range(0, len(results), 5):
        assert results[i] == b"0123456789"
        assert results[i + 1] == b"hello world"
        assert results[i + 2] is True
        assert results[i + 3] is False
        assert results[i + 4] == [Entry("a.txt", 10), Entry("sub", None)]
