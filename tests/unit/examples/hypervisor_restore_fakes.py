"""Fakes of the repository side the hypervisor restore scripts share (apm_restore_common): a node tree, its
contents (read, or exported sparse), a Repository and a Session over them, and a RestorePlan of given disk
sizes."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk import Frame, Node, NodeFrame, NodeRef, RestorableUnit, RootFrame, UnitKind
from synology_apm_repo.sdk.api import Repository, Session
from synology_apm_repo.sdk.export import ExportResult
from synology_apm_repo.sdk.units.base import ContentSource, UnitProvider

MIB = 1 << 20
GIB = 1 << 30


def node(name: str, *, leaf: bool, kind: UnitKind | None = None, size: int | None = None) -> Node:
    return Node(ref=NodeRef("repo", (name,)), name=name, is_leaf=leaf, kind=kind, size=size)


@faithful_to(ContentSource)
class FakeContent:
    """A ``ContentSource`` over bytes that records its reads."""

    def __init__(self, data: bytes, size: int | None = None) -> None:
        self._data = data
        self.size = size if size is not None else len(data)
        self.reads: list[tuple[int, int]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        n = length if length is not None else self.size - offset
        self.reads.append((offset, n))
        return self._data[offset : offset + n].ljust(n, b"\0")

    async def stream(self, block: int = MIB) -> AsyncIterator[tuple[int, bytes]]:
        return
        yield  # unreachable: makes this an (empty) async generator

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self, sink: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        raise NotImplementedError


@faithful_to(ContentSource)
class ExtentContent:
    """A ``ContentSource`` of ``size`` bytes, zero but for ``extents`` (offset -> bytes), whose export writes only
    the extents, as a sparse export leaves holes unwritten; it fails instead at ``fail_at``, if given."""

    def __init__(self, size: int, extents: dict[int, bytes], *, fail_at: int | None = None) -> None:
        self.size = size
        self._extents = extents
        self._fail_at = fail_at

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> Any:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self, writer: Any, start: int, end: int, *, sparse: bool = True, progress: Any = None, tuning: object = None
    ) -> ExportResult:
        if self._fail_at is not None and start <= self._fail_at < end:
            raise RuntimeError(f"synthetic failure at {self._fail_at}")
        for offset, data in self._extents.items():
            lo, hi = max(offset, start), min(offset + len(data), end)
            if lo < hi:
                await writer.write_at(lo - start, data[lo - offset : hi - offset])
        return ExportResult(bytes_written=end - start, logical_size=end - start, holes=0, zeros=0)


@faithful_to(UnitProvider)
class FakeProvider:
    def __init__(self, children: dict[str, list[Node]]) -> None:
        self._children = children
        self.visited: list[str] = []

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        self.visited.append(node.name)
        return self._children.get(node.name, [])

    async def unit(self, node: Node) -> RestorableUnit:
        return RestorableUnit(
            ref=node.ref,
            name=node.name,
            is_leaf=True,
            kind=node.kind,
            size=node.size,
            content=FakeContent(b"", node.size),
        )


def vm_tree() -> tuple[FakeProvider, Node]:
    root = node("Devices", leaf=False)
    device = node("host-1", leaf=False)
    tree = {
        "Devices": [device],
        "host-1": [
            node("disk.img", leaf=True, kind=UnitKind.DISK_IMAGE, size=30 * GIB),
            node("(filesystem)", leaf=False, kind=UnitKind.DISK_FILESYSTEM),
            node("notes.txt", leaf=True, kind=UnitKind.FILE, size=10),
        ],
        "(filesystem)": [node("must-not-be-listed", leaf=True, kind=UnitKind.DISK_IMAGE, size=1)],
    }
    return FakeProvider(tree), root


@faithful_to(Repository)
class FakeRepo:
    """The ``Repository`` calls the example makes."""

    def __init__(self, provider: Any = None, node: Node | None = None, level: str = "node") -> None:
        self.provider, self.node, self.level = provider, node, level
        self.is_encrypted = False
        self.keys: list[str] = []
        self.key_ok = True

    async def locate(self, ref: NodeRef, *, raw: object = None) -> Frame:
        if self.level == "node":
            assert self.node is not None
            return NodeFrame(self.provider, self.node)
        return RootFrame()

    async def set_key(self, key_string: str) -> SimpleNamespace:
        self.keys.append(key_string)
        return SimpleNamespace(verification=SimpleNamespace(ok=self.key_ok))


@faithful_to(Session)
class FakeSession:
    def __init__(self, repos: list[Any]) -> None:
        self.repos = repos
        self.calls: list[tuple[Any, ...]] = []

    async def open(
        self, source: Any, key: str | None = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[Any]:
        if isinstance(source, str):
            self.calls.append(("open", source))
        else:
            self.calls.append(("open_store", source, root))
        return self.repos


class SessionContext:
    """What ``Session()`` returns: an async context manager yielding the fake session."""

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> FakeSession:
        return self._session

    async def __aexit__(self, *exc: object) -> None:
        return None


def patch_session(ex: ModuleType, monkeypatch: pytest.MonkeyPatch, repo: FakeRepo) -> FakeSession:
    session = FakeSession([repo])
    monkeypatch.setattr(ex, "Session", lambda: SessionContext(session))
    return session


def make_plan(ex: ModuleType, *sizes: int, firmware: str = "uefi") -> Any:
    """A ``RestorePlan`` of ``ex`` (apm_restore_common or a script importing it) with disks of ``sizes``."""
    common = sys.modules[ex.RestorePlan.__module__]
    disks = [common.SourceDisk(f"disk-{i}", SimpleNamespace(), size) for i, size in enumerate(sizes)]
    return ex.RestorePlan(disks, firmware)
