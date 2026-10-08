"""``BlockingContentSource``, a ``ContentSource`` whose export runs until
cancelled, for tests that inspect a job's or a screen's mid-export state;
and ``TreeProvider``, a fixed tree of nodes over given contents."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping

from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import NotRestorableError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef


@faithful_to(ContentSource)
class BlockingContentSource:
    """``export_range`` records that it ``started`` and the ``sparse`` it
    received, then parks until cancelled, setting ``cancelled``. ``read`` and
    ``stream`` fail the test."""

    size = 4096

    def __init__(self) -> None:
        self.started = False
        self.cancelled = False
        self.received_sparse: bool | None = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("BlockingContentSource is never read")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("BlockingContentSource is never streamed")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        self.started = True
        self.received_sparse = sparse
        try:
            await asyncio.Event().wait()  # never set: only cancellation ends this
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("unreachable: this export ends only by cancellation")


@faithful_to(UnitProvider)
class TreeProvider:
    """A tree of ``Node``s keyed by parent ref; ``contents`` maps a leaf's
    ref to its content, and ``unit`` of any other node raises
    ``NotRestorableError``."""

    def __init__(
        self, root: Node, children: dict[NodeRef, list[Node]], contents: Mapping[NodeRef, ContentSource]
    ) -> None:
        self._root = root
        self._children = children
        self._contents = contents

    def root(self) -> Node:
        return self._root

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        return self._children.get(node.ref, [])

    async def unit(self, node: Node) -> RestorableUnit:
        if node.ref not in self._contents:
            raise NotRestorableError(f"{node.name!r} has no content")
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, content=self._contents[node.ref])
