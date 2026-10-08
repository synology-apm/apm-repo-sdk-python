"""``doctor`` command behaviour that needs no repository."""

from __future__ import annotations

from pathlib import Path

from support.cli import invoke


def test_doctor_on_a_directory_holding_no_repository_fails_cleanly(tmp_path: Path) -> None:
    empty = tmp_path / "not_a_repo"
    empty.mkdir()
    invoke(["doctor", str(empty)], exit_code=1)
