"""Unit tests for ``synology_apm_repo.browser.runtime.preview``, a leaf's
inline preview loading, driven by a fake provider."""

from __future__ import annotations

import asyncio

import pytest

from support.fakes import faithful_to
from synology_apm_repo.browser.runtime.preview import PreviewError, PreviewNote, PreviewText, load_preview
from synology_apm_repo.sdk.errors import ContentUnavailableError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitKind, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef

_HTML = b"<html><body><p>hello</p></body></html>"


@faithful_to(ContentSource)
class _Content:
    """A content source with canned bytes; ``read`` records every call."""

    def __init__(self, data: bytes | BaseException, *, size: int | None = None) -> None:
        self._data = data
        self.size = size
        self.reads: list[tuple[int, int | None]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        self.reads.append((offset, length))
        if isinstance(self._data, BaseException):
            raise self._data
        return self._data

    def stream(self, block: int = 0) -> object:
        raise AssertionError("not used in this test")

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
        raise AssertionError("not used in this test")


@faithful_to(UnitProvider)
class _Provider:
    def __init__(self, content: _Content | None = None, *, unit_error: BaseException | None = None) -> None:
        self._content = content
        self._unit_error = unit_error

    async def unit(self, node: Node) -> RestorableUnit:
        if self._unit_error is not None:
            raise self._unit_error
        return RestorableUnit(ref=node.ref, name=node.name, is_leaf=True, content=self._content)  # type: ignore[arg-type]


def _leaf(kind: UnitKind | None = None) -> Node:
    return Node(ref=NodeRef("repo", ("f",)), name="f", is_leaf=True, kind=kind)


async def test_a_renderable_leaf_yields_its_text_read_from_the_start() -> None:
    content = _Content(_HTML)

    result = await load_preview(_Provider(content), _leaf(), read_limit=100)  # type: ignore[arg-type]

    assert isinstance(result, PreviewText)
    assert "hello" in result.text
    assert content.reads == [(0, 100)]


async def test_content_the_renderer_has_nothing_for_yields_none() -> None:
    result = await load_preview(_Provider(_Content(b"\x00\x01binary")), _leaf(), read_limit=100)  # type: ignore[arg-type]

    assert result is None


@pytest.mark.parametrize(
    ("size", "expected_reads"),
    [
        # A zero-length read first builds the lazy content, then the tail is read.
        pytest.param(1000, [(0, 0), (900, 100)], id="over_the_limit_is_read_from_its_tail"),
        pytest.param(50, [(0, 0), (0, 100)], id="within_the_limit_is_read_from_the_start"),
    ],
)
async def test_a_teams_chat_message_preview_read(size: int, expected_reads: list[tuple[int, int]]) -> None:
    content = _Content(_HTML, size=size)

    await load_preview(_Provider(content), _leaf(UnitKind.TEAMS_CHAT_MESSAGE), read_limit=100)  # type: ignore[arg-type]

    assert content.reads == expected_reads


async def test_content_unavailable_is_a_note_not_an_error() -> None:
    provider = _Provider(_Content(ContentUnavailableError("placeholder only")))

    result = await load_preview(provider, _leaf(), read_limit=100)  # type: ignore[arg-type]

    assert result == PreviewNote("placeholder only")


@pytest.mark.parametrize("where", ["unit", "read"])
async def test_any_other_failure_is_an_error_result(where: str) -> None:
    boom = ValueError("parser exploded")
    provider = _Provider(unit_error=boom) if where == "unit" else _Provider(_Content(boom))

    result = await load_preview(provider, _leaf(), read_limit=100)  # type: ignore[arg-type]

    assert result == PreviewError("parser exploded")


async def test_cancellation_propagates() -> None:
    provider = _Provider(_Content(asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await load_preview(provider, _leaf(), read_limit=100)  # type: ignore[arg-type]
