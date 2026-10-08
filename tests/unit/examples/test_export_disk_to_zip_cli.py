"""Tests for the command line and output helpers of examples/export_disk_to_zip.py: ``parse_args``,
``ZipSettings``, ``entry_names``, ``parse_ref`` and ``main``."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from synology_apm_repo.sdk import ApmRepoError, NodeRef, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.content.saas_artifact import LazyArtifact


async def _unread_content() -> bytes:
    raise AssertionError("this test never reads a unit's content")


_MIB = 1 << 20


def _disk(ex: ModuleType, label: str) -> Any:
    unit = RestorableUnit(
        ref=NodeRef("repo", (label,)),
        name=label,
        is_leaf=True,
        kind=UnitKind.DISK_IMAGE,
        size=1,
        content=LazyArtifact(_unread_content),
    )
    return ex.SourceDisk(label, unit, 1)


# -- parse_args ------------------------------------------------------------------


def test_parse_args_defaults(ex: ModuleType, tmp_path: Path) -> None:
    args = ex.parse_args(["repo#a/b/c", str(tmp_path / "out.zip")])

    assert (args.segment_size_mib, args.buffered_segments, args.level) == (256, 2, 6)
    assert args.profile is None and args.key is None
    assert args.force is False and args.dry_run is False


def test_parse_args_takes_the_overrides(ex: ModuleType, tmp_path: Path) -> None:
    args = ex.parse_args(
        [
            "repo#a/b/c",
            str(tmp_path / "out.zip"),
            "--segment-size-mib",
            "64",
            "--buffered-segments",
            "3",
            "--level",
            "1",
            "--profile",
            "p",
            "--key",
            "id@k",
            "--force",
            "--dry-run",
        ]
    )

    assert (args.segment_size_mib, args.buffered_segments, args.level) == (64, 3, 1)
    assert (args.profile, args.key) == ("p", "id@k")
    assert args.force and args.dry_run


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--segment-size-mib", "0", "--segment-size-mib must be at least 1"),
        ("--buffered-segments", "0", "--buffered-segments must be at least 1"),
        ("--level", "0", "invalid choice"),
        ("--level", "10", "invalid choice"),
    ],
)
def test_parse_args_rejects_a_bad_number(
    ex: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], option: str, value: str, message: str
) -> None:
    with pytest.raises(SystemExit) as caught:
        ex.parse_args(["repo#a/b/c", str(tmp_path / "out.zip"), option, value])

    assert caught.value.code == 2
    assert message in capsys.readouterr().err


def test_parse_args_refuses_to_replace_an_existing_output_without_force(
    ex: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "out.zip"
    output.write_bytes(b"old")

    with pytest.raises(SystemExit) as caught:
        ex.parse_args(["repo#a/b/c", str(output)])

    assert caught.value.code == 2
    assert "pass --force" in capsys.readouterr().err
    assert ex.parse_args(["repo#a/b/c", str(output), "--force"]).force
    assert ex.parse_args(["repo#a/b/c", str(output), "--dry-run"]).dry_run  # a dry run writes nothing


def test_parse_args_refuses_a_directory_as_output(
    ex: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit):
        ex.parse_args(["repo#a/b/c", str(tmp_path)])

    assert "is a directory" in capsys.readouterr().err


def test_zip_settings_convert_mib_to_bytes(ex: ModuleType, tmp_path: Path) -> None:
    args = ex.parse_args(["repo#a/b/c", str(tmp_path / "o.zip"), "--segment-size-mib", "8", "--level", "2"])

    settings = ex.ZipSettings.from_args(args)

    assert settings.output == tmp_path / "o.zip"
    assert (settings.segment_size, settings.buffered_segments, settings.level) == (8 * _MIB, 2, 2)


# -- helpers ---------------------------------------------------------------------


def test_a_single_disk_is_named_after_the_output(ex: ModuleType) -> None:
    assert ex.entry_names([_disk(ex, "device/disk0")], Path("/x/web-01.zip")) == ["web-01.img"]


def test_several_disks_are_numbered_and_made_path_safe(ex: ModuleType) -> None:
    disks = [_disk(ex, "host-1/disk 0"), _disk(ex, "host-1/disk 1"), _disk(ex, "../escape")]

    names = ex.entry_names(disks, Path("out.zip"))

    assert names == ["01-host-1_disk_0.img", "02-host-1_disk_1.img", "03-.._escape.img"]
    assert all("/" not in name for name in names)
    assert len(set(names)) == len(names)


def test_a_bare_path_is_a_human_ref_with_no_segments(ex: ModuleType) -> None:
    assert ex.parse_ref("/some/repo") == NodeRef.human("/some/repo")
    assert ex.parse_ref("/some/repo#a/b") == NodeRef.parse("/some/repo#a/b")


# -- main ------------------------------------------------------------------------


def test_main_reports_an_sdk_error_without_a_traceback(
    ex: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def failing(args: Any) -> None:
        raise ApmRepoError("the repository is unreadable")

    monkeypatch.setattr(ex, "run", failing)

    with pytest.raises(SystemExit, match="error: the repository is unreadable") as caught:
        ex.main(["repo#a/b/c", str(tmp_path / "out.zip")])

    assert caught.value.code == "error: the repository is unreadable"


def test_main_prints_the_total_time_on_success(
    ex: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:

    async def fine(args: Any) -> None:
        return None

    monkeypatch.setattr(ex, "run", fine)

    ex.main(["repo#a/b/c", str(tmp_path / "out.zip")])

    assert "done in" in capsys.readouterr().err
