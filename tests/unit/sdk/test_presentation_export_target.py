"""Unit tests for ``sdk/presentation/export_target.py``."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from synology_apm_repo.sdk.presentation.export_target import (
    destination_state,
    part_path_for,
)


def test_part_path_for_appends_part_to_the_whole_name(tmp_path: Path) -> None:
    assert part_path_for(tmp_path / "disk.img") == tmp_path / "disk.img.part"
    assert part_path_for(tmp_path / "noext") == tmp_path / "noext.part"


def _dangling_symlink(path: Path) -> None:
    try:
        os.symlink(path.parent / "nowhere", path)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available here")


def test_a_dangling_symlink_is_something_that_is_there(tmp_path: Path) -> None:
    _dangling_symlink(tmp_path / "link")

    assert destination_state(tmp_path / "link") == "file"


def test_a_symlink_to_a_directory_is_a_directory(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    try:
        os.symlink(tmp_path / "real", tmp_path / "link", target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available here")

    assert destination_state(tmp_path / "link") == "directory"


def test_destination_state_tells_nothing_a_file_and_a_directory_apart(tmp_path: Path) -> None:
    (tmp_path / "f").write_bytes(b"x")
    (tmp_path / "d").mkdir()

    assert destination_state(tmp_path / "nope") == "missing"
    assert destination_state(tmp_path / "f") == "file"
    assert destination_state(tmp_path / "d") == "directory"


def test_single_destination_problem(tmp_path: Path) -> None:
    from synology_apm_repo.sdk.presentation.export_target import single_destination_problem

    (tmp_path / "file").write_bytes(b"x")
    (tmp_path / "dir").mkdir()

    assert single_destination_problem(tmp_path / "missing", force=False) is None
    assert single_destination_problem(tmp_path / "file", force=False) == "exists"
    assert single_destination_problem(tmp_path / "file", force=True) is None
    assert single_destination_problem(tmp_path / "dir", force=False) == "directory"
    assert single_destination_problem(tmp_path / "dir", force=True) == "directory"  # force never replaces a directory


def test_a_dangling_symlink_is_an_existing_destination_for_a_single_item(tmp_path: Path) -> None:
    from synology_apm_repo.sdk.presentation.export_target import single_destination_problem

    try:
        os.symlink(tmp_path / "nowhere", tmp_path / "link")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available here")

    assert single_destination_problem(tmp_path / "link", force=False) == "exists"


def test_destination_problems_are_worded_for_the_user(tmp_path: Path) -> None:
    from synology_apm_repo.sdk.presentation.export_target import (
        folder_destination_problem,
        single_destination_message,
    )

    (tmp_path / "file").write_bytes(b"x")

    assert single_destination_message(tmp_path, "directory") == (
        f"{tmp_path} is a directory — a single item exports to a file path"
    )
    assert single_destination_message(tmp_path / "file", "exists") == f"{tmp_path / 'file'} already exists"
    assert folder_destination_problem(tmp_path / "file") == (
        f"{tmp_path / 'file'} is a file — exporting a folder needs a directory"
    )
    assert folder_destination_problem(tmp_path) is None
    assert folder_destination_problem(tmp_path / "missing") is None
