"""Unit tests for ``synology_apm_repo.cli.commands.cat``.

The ``--offset``/``--length`` argument validation tests need no fake
session/repository at all — Typer's ``min=0`` rejects a negative value
before any repository is ever opened. The 0-byte-content test below does
need a fake repository/unit: a genuinely empty file must produce clean,
empty output, never an internal-error traceback.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.commands.cat as cat_module
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.units.base import RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef

runner = CliRunner()


def test_negative_offset_is_rejected_before_opening_a_repo() -> None:
    result = runner.invoke(app, ["cat", "--offset", "-1", "/some/path#item"])
    assert result.exit_code != 0
    assert "--offset" in result.output


def test_negative_length_is_rejected_before_opening_a_repo() -> None:
    result = runner.invoke(app, ["cat", "--length", "-1", "/some/path#item"])
    assert result.exit_code != 0
    assert "--length" in result.output


class _EmptyContent:
    """A real (not mocked) ``ContentSource`` over zero bytes — exercises
    the actual clamp-not-raise EOF contract rather than assuming it."""

    size = 0
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset/length must be non-negative")
        return b""

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise NotImplementedError  # pragma: no cover - not exercised by this test

    async def export_to(self, dst: object, **kwargs: object) -> object:
        raise NotImplementedError  # pragma: no cover - not exercised by this test


def test_cat_on_a_genuinely_empty_file_writes_empty_output_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=0,
        content=_EmptyContent(),
    )

    @contextlib.asynccontextmanager
    async def fake_opened_repo(*args: object, **kwargs: object) -> AsyncIterator[object]:
        yield object()

    async def fake_resolve_restorable(*args: object, **kwargs: object) -> RestorableUnit:
        return unit

    monkeypatch.setattr(cat_module, "opened_repo", fake_opened_repo)
    monkeypatch.setattr(cat_module, "resolve_restorable", fake_resolve_restorable)

    result = runner.invoke(app, ["cat", "--length", "64", "/some/path#item"])

    assert result.exit_code == 0
    assert result.stdout_bytes == b""


class _StreamOnlyContent:
    """A ``ContentSource`` whose ``read()`` always raises -- proves the
    default (no ``--offset``/``--length``) dump goes through ``stream()``
    exclusively, in chunks, rather than buffering everything via one bulk
    ``read()`` call. Regression test for a real restorable unit that can be
    a multi-GB VM/PC/PS disk image or a large SaaS attachment: content
    must not be fully materialized in memory before a single byte reaches
    stdout."""

    size = 11
    supports_concurrent_export = False

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

    async def export_to(self, dst: object, **kwargs: object) -> object:
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

    @contextlib.asynccontextmanager
    async def fake_opened_repo(*args: object, **kwargs: object) -> AsyncIterator[object]:
        yield object()

    async def fake_resolve_restorable(*args: object, **kwargs: object) -> RestorableUnit:
        return unit

    monkeypatch.setattr(cat_module, "opened_repo", fake_opened_repo)
    monkeypatch.setattr(cat_module, "resolve_restorable", fake_resolve_restorable)

    result = runner.invoke(app, ["cat", "/some/path#item"])

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"hello world"


def test_cat_with_an_explicit_offset_still_uses_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit ``--offset``/``--length`` is already bounded by the
    caller's own request, so it keeps going through ``read()`` --
    ``stream()`` has no offset/length parameters of its own to satisfy
    this with."""
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=0,
        content=_EmptyContent(),
    )

    @contextlib.asynccontextmanager
    async def fake_opened_repo(*args: object, **kwargs: object) -> AsyncIterator[object]:
        yield object()

    async def fake_resolve_restorable(*args: object, **kwargs: object) -> RestorableUnit:
        return unit

    monkeypatch.setattr(cat_module, "opened_repo", fake_opened_repo)
    monkeypatch.setattr(cat_module, "resolve_restorable", fake_resolve_restorable)

    result = runner.invoke(app, ["cat", "--offset", "3", "/some/path#item"])

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b""


def test_object_db_id_forwards_to_resolve_restorable(monkeypatch: pytest.MonkeyPatch) -> None:
    unit = RestorableUnit(
        ref=NodeRef("/some/path", ("item",)),
        name="item",
        is_leaf=True,
        kind=UnitKind.FILE,
        size=0,
        content=_EmptyContent(),
    )
    received: dict[str, object] = {}

    @contextlib.asynccontextmanager
    async def fake_opened_repo(*args: object, **kwargs: object) -> AsyncIterator[object]:
        yield object()

    async def fake_resolve_restorable(*args: object, **kwargs: object) -> RestorableUnit:
        received.update(kwargs)
        return unit

    monkeypatch.setattr(cat_module, "opened_repo", fake_opened_repo)
    monkeypatch.setattr(cat_module, "resolve_restorable", fake_resolve_restorable)

    result = runner.invoke(app, ["cat", "--object-db-id", "obj-42", "--length", "0", "/some/path#item"])

    assert result.exit_code == 0, result.output
    assert received["object_db_id"] == "obj-42"


__all__: list[str] = []
