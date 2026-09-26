"""Unit tests for the ``synology-apm-repo-cli export`` command's own
behavior — pre-existing-destination handling, ``--sparse``/``--force``,
the success summary, and ``--quiet``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import ExportResult
from synology_apm_repo.sdk.errors import ContentUnavailableError
from synology_apm_repo.sdk.units.base import Node, RestorableUnit
from synology_apm_repo.sdk.units.node_ref import NodeRef

runner = CliRunner()


class _RecordingBucketBackedSource:
    """Matches ``ContentSource`` and declares
    ``supports_concurrent_export = True`` — the ``DedupFile``/
    ``ByteRangeView`` shape a real bucket-backed export uses. Records
    ``sparse`` only — ``export_to()``'s own parallelism (an unconditional
    multiprocess dispatch whenever the repository's store supports it)
    isn't a CLI-facing knob, so there's no concurrency kwarg to record."""

    size = 4096
    supports_concurrent_export = True

    def __init__(self) -> None:
        self.received_sparse: bool | None = None

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        self.received_sparse = sparse
        dst.write_bytes(b"x")
        return ExportResult(bytes_written=1, logical_size=1, holes=0, zeros=0)


def _fake_session_returning(monkeypatch: pytest.MonkeyPatch, content: object) -> None:
    ref = NodeRef("", ("item",))
    unit = RestorableUnit(ref=ref, name="item", is_leaf=True, content=content)  # type: ignore[arg-type]

    class _FakeRepo:
        async def resolve(self, node_ref: object, **kwargs: object) -> RestorableUnit:
            return unit

    class _FakeSession:
        def __init__(self) -> None:
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.Session", _FakeSession)


def test_exporting_a_folder_ref_fails_cleanly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    node = Node(ref=NodeRef("", ("folder",)), name="folder", is_leaf=False)

    class _FakeRepo:
        async def resolve(self, node_ref: object, **kwargs: object) -> Node:
            return node

    class _FakeSession:
        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.Session", _FakeSession)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere#folder", "-o", str(dst)])
    assert result.exit_code == 1
    assert "names a folder, not a single item" in result.output
    assert not dst.exists()


class _ContentUnavailableSource:
    """Stands in for a disk-fs content source that raises
    ``ContentUnavailableError`` (a cloud-sync placeholder with no local
    data). ``ContentUnavailableError`` is an ``ApmRepoError`` subclass, so
    it's already caught by ``export.py``'s existing ``except ApmRepoError``
    branch (which calls ``fail_from_apm_error``) — this closes a coverage
    gap for that existing branch, not a new code path there."""

    size = 4096
    supports_concurrent_export = False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
        raise ContentUnavailableError("cloud-sync placeholder — no local data at backup time")


def test_exporting_a_content_unavailable_placeholder_fails_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _ContentUnavailableSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 1
    assert "cloud-sync placeholder" in result.output
    assert not dst.exists()


def test_existing_destination_is_refused_without_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"original content")
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 1
    assert "already exists" in result.output
    assert "--force" in result.output
    # Refused before anything opened a session/started the export Task —
    # the pre-existing file must come back untouched.
    assert dst.read_bytes() == b"original content"


def test_existing_destination_is_overwritten_with_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    dst.write_bytes(b"original content")
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst), "--force"])
    assert result.exit_code == 0, result.output
    assert dst.read_bytes() == b"x"


def test_no_sparse_flag_forwards_sparse_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The only test in this file that passes --no-sparse -- every other
    # test relies on the default (sparse=True), so this is what proves
    # sparse=False actually reaches export_to().
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst), "--no-sparse"])
    assert result.exit_code == 0, result.output
    assert content.received_sparse is False


def test_success_summary_reports_real_bytes_written_holes_and_zeros(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FixedResultSource(_RecordingBucketBackedSource):
        async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
            dst.write_bytes(b"x")
            return ExportResult(bytes_written=100, logical_size=400, holes=250, zeros=50)

    content = _FixedResultSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 0, result.output
    # The real success-summary line -- no test in this file asserts on
    # its content, only exit code and written bytes. Rich wraps long
    # lines at the terminal width, so compare against the
    # whitespace-collapsed output rather than the raw string (which can
    # have a real newline inserted mid-path/mid-sentence).
    collapsed = " ".join(result.output.split())
    assert "exported" in collapsed
    assert dst.name in collapsed
    assert "100 B written" in collapsed
    assert "400 B logical" in collapsed
    assert "250 B holes" in collapsed
    assert "50 B zero-fill" in collapsed


def test_quiet_suppresses_the_success_summary_but_still_writes_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FixedResultSource(_RecordingBucketBackedSource):
        async def export_to(self, dst: Path, *, sparse: bool = True, progress: object = None) -> object:
            dst.write_bytes(b"x")
            return ExportResult(bytes_written=100, logical_size=400, holes=250, zeros=50)

    content = _FixedResultSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["--quiet", "export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 0, result.output
    assert "exported" not in result.output
    assert dst.read_bytes() == b"x"


def test_object_db_id_forwards_to_resolve_restorable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    ref = NodeRef("", ("item",))
    unit = RestorableUnit(ref=ref, name="item", is_leaf=True, content=content)
    received: dict[str, object] = {}

    class _FakeRepo:
        async def resolve(self, node_ref: object, **kwargs: object) -> RestorableUnit:
            received.update(kwargs)
            return unit

    class _FakeSession:
        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.Session", _FakeSession)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst), "--object-db-id", "obj-42"])
    assert result.exit_code == 0, result.output
    assert received["object_db_id"] == "obj-42"


def test_missing_output_parent_directory_is_created(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    content = _RecordingBucketBackedSource()
    _fake_session_returning(monkeypatch, content)
    dst = tmp_path / "does" / "not" / "exist" / "out.bin"
    assert not dst.parent.exists()
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst)])
    assert result.exit_code == 0, result.output
    assert dst.read_bytes() == b"x"


def test_profile_flag_uses_open_remote_instead_of_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``export.py``'s own ``--profile`` branch (``store =
    await resolve_profile_store(profile) if profile is not None else
    None``) is otherwise only exercised via the fixture-replay
    integration test."""
    content = _RecordingBucketBackedSource()
    ref = NodeRef("", ("item",))
    unit = RestorableUnit(ref=ref, name="item", is_leaf=True, content=content)
    open_remote_calls: list[tuple[object, object]] = []

    class _FakeRepo:
        async def resolve(self, node_ref: object, **kwargs: object) -> RestorableUnit:
            return unit

    class _FakeSession:
        async def open_remote(self, store: object, key: object, *, root: object, **kwargs: object) -> list[object]:
            open_remote_calls.append((store, root))
            return [_FakeRepo()]

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            raise AssertionError("--profile must resolve via open_remote, not open")

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.Session", _FakeSession)
    fake_store = object()

    async def fake_resolve_profile_store(profile: str | None) -> object:
        assert profile == "myprofile"
        return fake_store

    monkeypatch.setattr("synology_apm_repo.cli.commands.export.resolve_profile_store", fake_resolve_profile_store)
    dst = tmp_path / "out.bin"
    result = runner.invoke(app, ["export", "somewhere", "-o", str(dst), "--profile", "myprofile"])
    assert result.exit_code == 0, result.output
    assert open_remote_calls == [(fake_store, "somewhere")]


__all__: list[str] = []
