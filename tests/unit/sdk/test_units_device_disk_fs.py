"""Unit tests for ``synology_apm_repo.sdk.units.device_disk_fs``'s
``DiskFsSibling``: the "(filesystem)" axis next to a disk-image leaf, over
the committed ``tests/fixtures/tiny_ext4.raw.tar.gz`` image (holding
``hello.txt`` and ``subdir/nested.txt``; see ``test_units_content_disk_fs.py``)
served through a fake owning provider."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import cast

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.units.base import ContentSource, FileState, Node, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.content.disk_fs import DiskFilesystem, DiskFilesystemUnavailableError
from synology_apm_repo.sdk.units.content.disk_fs._base import _DirEntry
from synology_apm_repo.sdk.units.device import DeviceProvider
from synology_apm_repo.sdk.units.device_disk_fs import DiskFsSibling
from synology_apm_repo.sdk.units.device_handles import DiskFsDiagnostic, DiskFsEntry, DiskFsRoot, DiskKey
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.sdk.disk_fs_fakes import FileBackedContent, MemoryContent, extracted_image

_HELLO = b"hello from a real ext4 test fixture\n"
_NESTED = b"nested file\n"
_DISK_KEY: DiskKey = ("object", 1)
_SOURCE_NODE = Node(ref=NodeRef("repo", ("disk",)), name="disk0", is_leaf=True)


@faithful_to(DeviceProvider)
class _FakeDeviceProvider:
    """``unit()``, the one ``DeviceProvider`` call ``DiskFsSibling`` makes:
    opens the disk image as ``content``; ``opened`` counts the calls."""

    def __init__(self, content: ContentSource) -> None:
        self._content = content
        self.opened = 0

    async def unit(self, node: Node) -> RestorableUnit:
        self.opened += 1
        return RestorableUnit.of(node, self._content)


def _ext4_content() -> ContentSource:
    return FileBackedContent(*extracted_image("tiny_ext4.raw.tar.gz"))


def _sibling(content: ContentSource | None = None) -> tuple[DiskFsSibling, _FakeDeviceProvider]:
    provider = _FakeDeviceProvider(content if content is not None else _ext4_content())
    return DiskFsSibling(cast(DeviceProvider, provider)), provider


def _root(sibling: DiskFsSibling) -> Node:
    return sibling.root_node(disk_key=_DISK_KEY, source_node=_SOURCE_NODE, name="disk0")


def _entry(node: Node) -> DiskFsEntry:
    assert isinstance(node.handle, DiskFsEntry)
    return node.handle


async def _partition(sibling: DiskFsSibling) -> Node:
    (partition,) = await sibling.children(_root(sibling))
    return partition


async def _child(sibling: DiskFsSibling, parent: Node, name: str) -> Node:
    return next(node for node in await sibling.children(parent) if node.name == name)


def _raise_on_open(monkeypatch: pytest.MonkeyPatch, exc: BaseException) -> None:
    async def raising_open(cls: object, content: object) -> None:
        raise exc

    monkeypatch.setattr(DiskFilesystem, "open", classmethod(raising_open))


class TestRootNode:
    def test_is_a_container_sibling_of_the_disk_image_and_does_no_io(self) -> None:
        sibling, provider = _sibling()
        node = _root(sibling)
        assert (node.ref, node.name, node.is_leaf, node.kind) == (
            _SOURCE_NODE.ref.child("fs"),
            "disk0 (filesystem)",
            False,
            UnitKind.DISK_FILESYSTEM,
        )
        assert node.handle == DiskFsRoot(_DISK_KEY, _SOURCE_NODE)
        assert provider.opened == 0


class TestChildren:
    async def test_the_root_lists_each_partition_at_its_root_path(self) -> None:
        sibling, _provider = _sibling()
        root = _root(sibling)
        partition = await _partition(sibling)
        addr = _entry(partition).partition_addr
        assert (partition.ref, partition.name, partition.is_leaf, partition.kind, partition.details) == (
            root.ref.child(f"p{addr}"),
            "TinyExt4Test (ext2/3/4)",
            False,
            UnitKind.DISK_FILESYSTEM,
            {"path": "/"},
        )
        assert partition.handle == DiskFsEntry(_DISK_KEY, _SOURCE_NODE, addr, "/")

    async def test_the_root_pages_its_partitions(self) -> None:
        sibling, _provider = _sibling()
        assert await sibling.children(_root(sibling), offset=1) == []
        assert len(await sibling.children(_root(sibling), offset=0, limit=1)) == 1

    async def test_a_partition_lists_its_root_directory(self) -> None:
        sibling, _provider = _sibling()
        partition = await _partition(sibling)
        hello = await _child(sibling, partition, "hello.txt")
        subdir = await _child(sibling, partition, "subdir")

        assert (hello.ref, hello.is_leaf, hello.kind, hello.size, hello.details) == (
            partition.ref.child("hello.txt"),
            True,
            UnitKind.DISK_FILE,
            len(_HELLO),
            {"path": "/hello.txt"},
        )
        assert hello.handle == DiskFsEntry(_DISK_KEY, _SOURCE_NODE, _entry(partition).partition_addr, "/hello.txt")
        assert isinstance(hello.mtime, datetime)
        assert (subdir.is_leaf, subdir.kind, subdir.details) == (False, UnitKind.DISK_FILESYSTEM, {"path": "/subdir"})

    async def test_a_subdirectorys_entries_carry_its_full_path(self) -> None:
        sibling, _provider = _sibling()
        subdir = await _child(sibling, await _partition(sibling), "subdir")
        (nested,) = await sibling.children(subdir)
        assert (nested.name, nested.ref, nested.details) == (
            "nested.txt",
            subdir.ref.child("nested.txt"),
            {"path": "/subdir/nested.txt"},
        )
        assert _entry(nested).path == "/subdir/nested.txt"

    async def test_a_directory_pages_its_entries(self) -> None:
        sibling, _provider = _sibling()
        partition = await _partition(sibling)
        everything = [node.name for node in await sibling.children(partition)]
        assert len(everything) >= 3
        page = await sibling.children(partition, offset=1, limit=2)
        assert [node.name for node in page] == everything[1:3]

    async def test_a_file_state_and_mtime_flow_into_the_entry_node(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mtime = datetime(2024, 5, 6, 7, 8, 9, tzinfo=UTC)

        @faithful_to(DiskFilesystem)
        class _FakeDiskFs:
            def partitions(self) -> list[tuple[int, str]]:
                return [(0, "fake partition")]

            async def list_dir(self, partition_addr: int, path: str) -> list[_DirEntry]:
                return [
                    _DirEntry(name="cloud.txt", is_dir=False, size=10, file_state=FileState.CLOUD_ONLY, mtime=mtime)
                ]

            async def open_file(self, partition_addr: int, path: str) -> ContentSource:
                return MemoryContent(b"0123456789")

        async def fake_open(cls: object, content: object) -> _FakeDiskFs:
            return _FakeDiskFs()

        monkeypatch.setattr(DiskFilesystem, "open", classmethod(fake_open))
        sibling, _provider = _sibling()
        partition = await _partition(sibling)
        assert (partition.file_state, partition.mtime) == (FileState.NORMAL, None)

        (file_node,) = await sibling.children(partition)
        assert (file_node.file_state, file_node.mtime) == (FileState.CLOUD_ONLY, mtime)
        assert (await sibling.open_entry(file_node, _entry(file_node))).file_state is FileState.CLOUD_ONLY


class TestOpenEntry:
    async def test_opens_a_file_as_a_disk_file_unit(self) -> None:
        sibling, _provider = _sibling()
        hello = await _child(sibling, await _partition(sibling), "hello.txt")
        unit = await sibling.open_entry(hello, _entry(hello))
        assert (unit.ref, unit.kind, unit.size, unit.is_leaf) == (hello.ref, UnitKind.DISK_FILE, len(_HELLO), True)
        assert await unit.content.read(0, len(_HELLO)) == _HELLO

    async def test_a_fresh_sibling_resolves_the_disk_itself(self) -> None:
        """An entry handle from a pasted canonical ref reaches ``open_entry``
        before any ``children()`` call on this sibling."""
        listed, _provider = _sibling()
        subdir = await _child(listed, await _partition(listed), "subdir")
        (nested,) = await listed.children(subdir)

        fresh, fresh_provider = _sibling()
        unit = await fresh.open_entry(nested, _entry(nested))
        assert await unit.content.read(0, len(_NESTED)) == _NESTED
        assert fresh_provider.opened == 1

    async def test_raises_not_found_when_no_filesystem_is_recognized(self) -> None:
        sibling, _provider = _sibling(MemoryContent(bytes(1 << 20)))
        node = Node(
            ref=_root(sibling).ref.child("p0"),
            name="file.txt",
            is_leaf=True,
            handle=DiskFsEntry(_DISK_KEY, _SOURCE_NODE, 0, "/file.txt"),
        )
        with pytest.raises(NotFoundError, match="no filesystem recognized") as exc_info:
            await sibling.open_entry(node, _entry(node))
        assert exc_info.value.ref == str(node.ref)


class TestResolve:
    async def test_the_disk_is_parsed_once_across_listings_and_opens(self) -> None:
        sibling, provider = _sibling()
        partition = await _partition(sibling)
        hello = await _child(sibling, partition, "hello.txt")
        await sibling.open_entry(hello, _entry(hello))
        assert provider.opened == 1

    async def test_concurrent_first_resolves_parse_the_disk_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        opens: list[object] = []

        async def counting_open(cls: object, content: object) -> None:
            opens.append(content)
            await asyncio.sleep(0)

        monkeypatch.setattr(DiskFilesystem, "open", classmethod(counting_open))
        sibling, _provider = _sibling()
        handle = DiskFsRoot(_DISK_KEY, _SOURCE_NODE)
        assert list(await asyncio.gather(sibling.resolve(handle), sibling.resolve(handle))) == [None, None]
        assert len(opens) == 1

    async def test_an_unrecognized_disk_resolves_to_none_once(self) -> None:
        sibling, provider = _sibling(MemoryContent(bytes(1 << 20)))
        handle = DiskFsRoot(_DISK_KEY, _SOURCE_NODE)
        assert await sibling.resolve(handle) is None
        assert await sibling.resolve(handle) is None
        assert provider.opened == 1

    async def test_each_disk_key_is_parsed_separately(self) -> None:
        sibling, provider = _sibling()
        await sibling.resolve(DiskFsRoot(("object", 1), _SOURCE_NODE))
        await sibling.resolve(DiskFsRoot(("pcps", "disk-uuid", "0"), _SOURCE_NODE))
        assert provider.opened == 2

    async def test_another_parse_failure_propagates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _raise_on_open(monkeypatch, DataCorruptError("bad partition table"))
        sibling, _provider = _sibling()
        with pytest.raises(DataCorruptError, match="bad partition table"):
            await sibling.resolve(DiskFsRoot(_DISK_KEY, _SOURCE_NODE))


class TestDiagnostic:
    async def test_an_unrecognized_disk_lists_one_diagnostic_leaf_on_the_first_page_only(self) -> None:
        sibling, _provider = _sibling(MemoryContent(bytes(1 << 20)))
        root = _root(sibling)
        (diagnostic,) = await sibling.children(root)
        assert (diagnostic.ref, diagnostic.name, diagnostic.is_leaf) == (
            root.ref.child("diagnostic"),
            "(no filesystem recognized on this disk)",
            True,
        )
        assert diagnostic.diagnostic is not None and "BitLocker" in diagnostic.diagnostic
        assert diagnostic.handle == DiskFsDiagnostic(None)
        assert await sibling.children(root, offset=1) == []

    async def test_an_entry_node_of_an_unrecognized_disk_also_gets_the_diagnostic(self) -> None:
        sibling, _provider = _sibling(MemoryContent(bytes(1 << 20)))
        entry = Node(
            ref=_root(sibling).ref.child("p0"),
            name="p0",
            is_leaf=False,
            handle=DiskFsEntry(_DISK_KEY, _SOURCE_NODE, 0, "/"),
        )
        assert [node.ref for node in await sibling.children(entry)] == [entry.ref.child("diagnostic")]

    async def test_no_dissect_package_gives_a_diagnostic_without_a_reason(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _raise_on_open(monkeypatch, DiskFilesystemUnavailableError("dissect not installed"))
        sibling, _provider = _sibling()
        (diagnostic,) = await sibling.children(_root(sibling))
        assert diagnostic.handle == DiskFsDiagnostic(None)

    async def test_absent_disk_data_keeps_its_reason_in_the_handle_not_the_text(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        exc = NotFoundError("no such composition chunk", ref="@data/Composition/5/9.com/c0")
        _raise_on_open(monkeypatch, exc)
        sibling, _provider = _sibling()
        (diagnostic,) = await sibling.children(_root(sibling))
        assert diagnostic.handle == DiskFsDiagnostic(str(exc))
        assert diagnostic.diagnostic is not None and "@data" not in diagnostic.diagnostic
