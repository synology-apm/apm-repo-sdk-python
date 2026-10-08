"""Unit tests for ``synology-apm-repo-cli export`` of a single item: destination
handling, ``--sparse``/``--force``/``--quiet``/``--key``/``--profile``/
``--object-db-id``, the success summary, and failure reporting (unavailable
content, out of space)."""

from __future__ import annotations

import errno
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.sdk.api import NodeFrame, RawView, Repository
from synology_apm_repo.sdk.errors import ContentUnavailableError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.cli.export_fakes import item_frame
from unit.cli.session_fakes import FakeSession, install_fake_session


@faithful_to(ContentSource)
class _RecordingBucketBackedSource:
    """A ``ContentSource`` whose ``export_range()`` writes one byte and
    records the ``sparse`` it received."""

    size = 1

    def __init__(self) -> None:
        self.received_sparse: bool | None = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
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
        self.received_sparse = sparse
        await sink.write_at(0, b"x")
        return ExportResult(bytes_written=1, logical_size=1, holes=0, zeros=0)


@faithful_to(ContentSource)
class _FixedResultSource(_RecordingBucketBackedSource):
    """Writes one byte and reports a fixed result with distinct written,
    logical, hole and zero-fill counts."""

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
        await sink.write_at(0, b"x")
        return ExportResult(bytes_written=100, logical_size=400, holes=250, zeros=50)


@faithful_to(Repository)
class _ItemRepo:
    """Resolves every ref to ``unit``, recording the ``raw`` each call asked for."""

    def __init__(self, unit: RestorableUnit) -> None:
        self._unit = unit
        self.raw_requests: list[RawView | None] = []

    async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
        self.raw_requests.append(raw)
        return item_frame(self._unit)


def _item_unit(content: object, *, degraded: str | None = None) -> RestorableUnit:
    return RestorableUnit(ref=NodeRef("", ("item",)), name="item", is_leaf=True, degraded=degraded, content=content)  # type: ignore[arg-type]


def _fake_session_returning(
    monkeypatch: pytest.MonkeyPatch, content: object, *, degraded: str | None = None
) -> type[FakeSession]:
    return install_fake_session(monkeypatch, [_ItemRepo(_item_unit(content, degraded=degraded))])


@faithful_to(ContentSource)
class _ContentUnavailableSource:
    """A disk-fs content source whose ``export_range()`` raises
    ``ContentUnavailableError`` (a cloud-sync placeholder with no local data)."""

    size = 4096

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
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
        raise ContentUnavailableError("cloud-sync placeholder — no local data at backup time")


def test_exporting_a_content_unavailable_placeholder_fails_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _ContentUnavailableSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = invoke(["export", "somewhere", "-o", str(dst)], exit_code=1)
    assert "cloud-sync placeholder" in result.output
    assert not dst.exists()


def test_existing_destination_is_refused_without_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"original content")
    result = invoke(["export", "somewhere", "-o", str(dst)], exit_code=1)
    assert result.stdout == ""
    message = " ".join(result.stderr.split())  # Rich wraps the long temp path
    assert message.startswith("error: ") and message.endswith(
        "already exists — pass --force to overwrite it (only for a single item; a folder REF needs a directory)"
    )
    assert dst.read_bytes() == b"original content"


def test_existing_destination_is_overwritten_with_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"original content")
    invoke(["export", "somewhere", "-o", str(dst), "--force"])
    assert dst.read_bytes() == b"x"


def test_no_sparse_flag_forwards_sparse_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    invoke(["export", "somewhere", "-o", str(dst), "--no-sparse"])
    assert content.received_sparse is False


def test_success_summary_reports_real_bytes_written_holes_and_zeros(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _FixedResultSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = invoke(["export", "somewhere", "-o", str(dst)])
    # Rich wraps at the terminal width, so compare whitespace-collapsed output.
    collapsed = " ".join(result.output.split())
    assert "exported" in collapsed
    assert dst.name in collapsed
    assert "100 B written" in collapsed
    assert "400 B logical" in collapsed
    assert "250 B holes" in collapsed
    assert "50 B zero-fill" in collapsed


@pytest.mark.parametrize("flag", ["-q", "--quiet"])
def test_quiet_suppresses_the_success_summary_but_still_writes_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    content = _FixedResultSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = invoke([flag, "export", "somewhere", "-o", str(dst)])
    assert result.stdout == ""
    assert dst.read_bytes() == b"x"


@pytest.mark.parametrize("flag", ["-q", "--quiet"])
def test_quiet_does_not_hide_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"original content")

    result = invoke([flag, "export", "somewhere", "-o", str(dst)], exit_code=1)
    assert result.stdout == ""
    assert "already exists" in result.stderr
    assert dst.read_bytes() == b"original content"


@pytest.mark.parametrize("args", [[], ["-q"]])
def test_a_degraded_item_is_exported_and_reported_incomplete_on_stderr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, args: list[str]
) -> None:
    _fake_session_returning(monkeypatch, _RecordingBucketBackedSource(), degraded="2 of 3 parts are missing")
    dst = tmp_path / "out.bin"
    result = invoke([*args, "export", "somewhere", "-o", str(dst)])
    assert "incomplete: 2 of 3 parts are missing" in result.stderr  # shown even under -q
    assert dst.read_bytes() == b"x"


def test_object_db_id_asks_resolve_for_that_raw_view(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _ItemRepo(_item_unit(_RecordingBucketBackedSource()))
    install_fake_session(monkeypatch, [repo])
    dst = tmp_path / "out.bin"
    invoke(["export", "somewhere", "-o", str(dst), "--object-db-id", "obj-42"])
    assert repo.raw_requests == [RawView("obj-42")]


def test_missing_output_parent_directory_is_created(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "does" / "not" / "exist" / "out.bin"
    assert not dst.parent.exists()
    invoke(["export", "somewhere", "-o", str(dst)])
    assert dst.read_bytes() == b"x"


def test_key_reaches_session_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    session = _fake_session_returning(monkeypatch, _RecordingBucketBackedSource())

    invoke(["export", "somewhere", "-o", str(tmp_path / "out.bin"), "--key", "id@secret"])
    assert [call["key"] for call in session.open_calls] == ["id@secret"]


def test_profile_flag_opens_the_profile_store_not_a_local_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    session = _fake_session_returning(monkeypatch, _RecordingBucketBackedSource())
    fake_store = object()

    async def fake_store_from_profile(profile: str | None) -> object:
        assert profile == "myprofile"
        return fake_store

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.store_from_profile", fake_store_from_profile)
    dst = tmp_path / "out.bin"
    invoke(["export", "somewhere", "-o", str(dst), "--profile", "myprofile"])
    # The ref's path becomes the store-relative root.
    assert [(call["source"], call["root"]) for call in session.open_calls] == [(fake_store, "somewhere")]


@faithful_to(ContentSource)
class _FailingWriteSource(_RecordingBucketBackedSource):
    """Writes some bytes (or none), then fails with the given ``OSError``."""

    size = 4096

    def __init__(self, code: int, *, write_first: bool = True) -> None:
        super().__init__()
        self._code = code
        self._write_first = write_first

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
        if self._write_first:
            await sink.write_at(0, b"partial")
        raise OSError(self._code, "No space left on device" if self._code == errno.ENOSPC else "synthetic failure")


def _squash(text: str) -> str:
    """``text`` without whitespace, since Rich wraps CLI messages mid-path."""
    return "".join(text.split())


class TestOutOfSpace:
    """ENOSPC/EDQUOT get an actionable message, not the generic "internal error"."""

    def _export(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: object, *extra: str) -> tuple[Any, Path]:
        _fake_session_returning(monkeypatch, source)
        dst = tmp_path / "out.bin"
        return invoke(["export", "somewhere", "-o", str(dst), *extra], exit_code=None), dst

    @pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
    def test_running_out_of_room_mid_export_is_reported_helpfully(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
    ) -> None:
        result, dst = self._export(tmp_path, monkeypatch, _FailingWriteSource(code))
        assert result.exit_code == 1
        assert result.stdout == ""
        reason = "No space left on device" if code == errno.ENOSPC else "synthetic failure"
        assert _squash(result.stderr) == _squash(
            f"error: not enough free space to write {dst} ({reason}) — "
            "partial file removed (use --keep-partial to keep it)"
        )
        assert not dst.exists()
        assert not (tmp_path / "out.bin.part").exists()

    def test_keep_partial_reports_the_file_that_was_kept(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        result, dst = self._export(tmp_path, monkeypatch, _FailingWriteSource(errno.ENOSPC), "--keep-partial")
        assert result.exit_code == 1
        assert _squash(result.stderr) == _squash(
            f"error: not enough free space to write {dst} (No space left on device) — partial file kept as out.bin.part"
        )
        assert (tmp_path / "out.bin.part").exists()

    def test_failing_before_the_first_byte_says_nothing_was_written(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result, dst = self._export(tmp_path, monkeypatch, _FailingWriteSource(errno.ENOSPC, write_first=False))
        assert result.exit_code == 1
        assert _squash(result.stderr) == _squash(
            f"error: not enough free space to write {dst} (No space left on device) — no output file was ever written"
        )
        assert not dst.exists()
        assert not (tmp_path / "out.bin.part").exists()

    def test_any_other_oserror_is_still_an_unexpected_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result, _ = self._export(tmp_path, monkeypatch, _FailingWriteSource(errno.EIO))
        assert result.exit_code == 1
        assert result.stdout == ""
        assert "internalerror" in _squash(result.stderr)
        assert "notenoughfreespace" not in _squash(result.stderr)
