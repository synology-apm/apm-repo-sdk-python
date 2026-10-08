"""On-disk export destinations: the ``.part`` staging name
``LocalFileSink`` writes to, what is at a destination now, and whether a
single item or a folder may be exported there, with the user-facing
wording. Every check is synchronous: a ``stat()`` or two, not bulk I/O."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal


def part_path_for(dst: Path) -> Path:
    """The staging path an export writes to before renaming to ``dst`` on
    success: ``<dst>.part``."""
    return dst.with_name(dst.name + ".part")


def destination_state(dst: Path) -> Literal["missing", "file", "directory"]:
    """What is at ``dst`` now: a directory (or a symlink to one), nothing, or ``"file"`` for
    anything else, a dangling symlink included."""
    if dst.is_dir():
        return "directory"
    return "file" if os.path.lexists(dst) else "missing"


def single_destination_problem(dst: Path, *, force: bool) -> Literal["exists", "directory"] | None:
    """Why one item cannot be exported to ``dst``: ``"directory"`` when a directory is there (never replaceable),
    ``"exists"`` when anything else is there and ``force`` is not set, else ``None``. ``single_destination_message``
    words it."""
    state = destination_state(dst)
    if state == "directory":
        return "directory"
    return "exists" if state == "file" and not force else None


def single_destination_message(dst: Path, problem: Literal["exists", "directory"]) -> str:
    """The user-facing wording of a ``single_destination_problem`` result."""
    if problem == "directory":
        return f"{dst} is a directory — a single item exports to a file path"
    return f"{dst} already exists"


def folder_destination_problem(dst: Path) -> str | None:
    """Why a folder cannot be exported into ``dst`` (a file is there), or ``None``."""
    if destination_state(dst) == "file":
        return f"{dst} is a file — exporting a folder needs a directory"
    return None
