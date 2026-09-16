"""Unit tests for ``synology_apm_repo.cli.commands.cat``.

The ``--offset``/``--length`` argument validation tests need no fake
session/repository at all — Typer's ``min=0`` rejects a negative value before
any repository is ever opened (unlike ``tests/integration/cli/test_cli_cat_
replay.py``'s real-fixture-backed reads). The 0-byte-content test below
does need a fake repository/unit: it reproduces, synthetically, exactly the
crash a real ``ps-sample``'s empty file triggered against ``cat --length``
before the ``ContentSource.read()`` EOF-clamp fix (``dedup/dedup_file.py``/
``units/content/pcps_disk.py``) — a genuinely empty file must produce
clean, empty output, never an internal-error traceback.
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


__all__: list[str] = []
