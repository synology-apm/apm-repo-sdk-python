"""Unit tests for ``synology_apm_repo.cli.commands.cat``.

The negative ``--offset``/``--length`` tests need no fake repository: Typer
rejects the value before one is opened. Every other test fakes
``opened_repo``/``resolve_restorable`` around a hand-built ``ContentSource``.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import pytest

import synology_apm_repo.cli.commands.cat as cat_module
from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef


@pytest.mark.parametrize("option", ["--offset", "--length"])
def test_negative_offset_or_length_is_rejected_before_opening_a_repo(option: str) -> None:
    result = invoke(["cat", option, "-1", "/some/path#item"], exit_code=2)
    assert result.stdout == ""
    assert f"Invalid value for '{option}': -1 is not in the range x>=0." in result.stderr


def _serve_unit(
    monkeypatch: pytest.MonkeyPatch, unit: RestorableUnit, *, received_kwargs: dict[str, object] | None = None
) -> None:
    """Make ``cat`` resolve every ref to ``unit`` without opening a repository;
    ``received_kwargs`` collects ``resolve_restorable``'s keyword arguments."""

    @contextlib.asynccontextmanager
    async def fake_opened_repo(*args: object, **kwargs: object) -> AsyncIterator[object]:
        yield object()

    async def fake_resolve_restorable(*args: object, **kwargs: object) -> RestorableUnit:
        if received_kwargs is not None:
            received_kwargs.update(kwargs)
        return unit

    monkeypatch.setattr(cat_module, "opened_repo", fake_opened_repo)
    monkeypatch.setattr(cat_module, "resolve_restorable", fake_resolve_restorable)


@faithful_to(ContentSource)
class _ReadOnlyContent:
    """A ``ContentSource`` over ``data`` whose ``read()`` clamps rather than
    raises at EOF and records each ``(offset, length)`` it was called with;
    ``stream()`` raises, so only ``read()`` can pass."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.size = len(data)
        self.reads: list[tuple[int, int | None]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset/length must be non-negative")
        self.reads.append((offset, length))
        return self._data[offset:] if length is None else self._data[offset : offset + length]

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError  # pragma: no cover - an explicit range must use read()

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
        raise NotImplementedError  # pragma: no cover - not exercised by this test


def test_cat_on_a_genuinely_empty_file_writes_empty_output_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=0,
        content=_ReadOnlyContent(b""),
    )

    _serve_unit(monkeypatch, unit)

    result = invoke(["cat", "--length", "64", "/some/path#item"])
    assert result.stdout_bytes == b""


@faithful_to(ContentSource)
class _StreamOnlyContent:
    """A ``ContentSource`` whose ``read()`` raises, so only ``stream()`` can pass."""

    size = 11

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("the default (no --offset/--length) dump must not call read()")

    def stream(self, block: int = 4) -> AsyncIterator[tuple[int, bytes]]:
        async def _gen() -> AsyncIterator[tuple[int, bytes]]:
            data = b"hello world"
            pos = 0
            while pos < len(data):
                chunk = data[pos : pos + block]
                yield pos, chunk
                pos += len(chunk)

        return _gen()

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
        raise NotImplementedError  # pragma: no cover - not exercised by this test


def test_cat_with_no_offset_or_length_streams_instead_of_bulk_reading(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=11,
        content=_StreamOnlyContent(),
    )

    _serve_unit(monkeypatch, unit)

    result = invoke(["cat", "/some/path#item"])
    assert result.stdout_bytes == b"hello world"


def test_cat_with_a_small_explicit_offset_and_length_reads_the_requested_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = _ReadOnlyContent(b"hello world")
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=content.size,
        content=content,
    )

    _serve_unit(monkeypatch, unit)

    result = invoke(["cat", "--offset", "3", "--length", "5", "/some/path#item"])
    assert result.stdout_bytes == b"lo wo"
    assert content.reads == [(3, 5)]


@faithful_to(ContentSource)
class _BoundedRangeStreamContent:
    """A ``ContentSource`` whose ``read()`` rejects any call not sized to exactly
    ``cat_module._OFFSET_READ_BLOCK``, so a bounded-range request can only pass
    by looping over block-sized reads (one bulk read would exceed
    ``DedupFile.read()``'s ``MAX_SINGLE_READ_SIZE``)."""

    size = 11

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if length != cat_module._OFFSET_READ_BLOCK:
            raise AssertionError(f"expected an _OFFSET_READ_BLOCK-sized read, got length={length}")
        return b"hello world"[offset : offset + length]

    def stream(self, block: int = 4) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError  # pragma: no cover - not exercised by a bounded-range request

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
        raise NotImplementedError  # pragma: no cover - not exercised by this test


def test_cat_with_offset_but_no_length_streams_in_bounded_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=11,
        content=_BoundedRangeStreamContent(),
    )

    _serve_unit(monkeypatch, unit)

    result = invoke(["cat", "--offset", "3", "/some/path#item"])
    assert result.stdout_bytes == b"lo world"


def test_cat_with_a_large_explicit_length_streams_in_bounded_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=11,
        content=_BoundedRangeStreamContent(),
    )

    _serve_unit(monkeypatch, unit)

    huge_length = cat_module._OFFSET_READ_BLOCK * 3
    result = invoke(["cat", "--length", str(huge_length), "/some/path#item"])
    assert result.stdout_bytes == b"hello world"


def test_object_db_id_forwards_to_resolve_restorable(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=0,
        content=_ReadOnlyContent(b""),
    )
    received: dict[str, object] = {}

    _serve_unit(monkeypatch, unit, received_kwargs=received)

    invoke(["cat", "--object-db-id", "obj-42", "--length", "0", "/some/path#item"])
    assert received["object_db_id"] == "obj-42"
