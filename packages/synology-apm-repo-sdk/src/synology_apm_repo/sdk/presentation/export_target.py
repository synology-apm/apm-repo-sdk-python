"""The shared ``<dst>.part`` staging-file safety contract for a real,
on-disk export destination — used identically by the CLI's ``export``
command and the Browser's own export worker, so the two surfaces can't
silently disagree on when an existing destination is refused or what
happens to a partial file once a cancelled export unwinds. Both callers
already share ``presentation.progress``'s ``ProgressMeter`` for the same
underlying ``ContentSource.export_to()`` call; this module covers the
file-lifecycle half of that same shared concern.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path


def part_path_for(dst: Path) -> Path:
    """The staging path an export writes to before renaming to ``dst`` on
    success — ``<dst>.part``, the one naming convention both callers
    share."""
    return dst.with_name(dst.name + ".part")


def destination_available(dst: Path, *, force: bool) -> bool:
    """Whether it's safe to start writing toward ``dst`` — ``False`` only
    when ``dst`` already exists and the caller didn't ask to overwrite
    it. Checked once, up front, before any real work starts: a
    truncated-but-plausible-looking output file is a safety-level
    problem for a restore tool, not just a UX nicety.

    A plain synchronous call even from an async caller — one cheap
    ``stat()``, the same tier as ``finalize_export()``'s ``replace()``
    and ``resolve_cancelled_partial()``'s ``unlink()``: single filesystem
    metadata operations, not the bulk data path the SDK offloads with
    ``asyncio.to_thread()``."""
    return force or not dst.exists()


def finalize_export(part_path: Path, dst: Path) -> None:
    """Renames a successfully-completed ``part_path`` to its final
    ``dst``."""
    part_path.replace(dst)


@dataclasses.dataclass(frozen=True)
class CancelledPartialOutcome:
    """What happened to ``part_path`` once a cancelled export unwound —
    each caller renders its own message from this, since the CLI and TUI
    surfaces phrase it differently."""

    #: Whether ``part_path`` was left on disk (``keep_partial=True``, or
    #: there was nothing to remove because it was never written).
    kept: bool
    #: ``False`` only when ``export_to()`` never got past deferring its
    #: own first-block-succeeds file creation (e.g. a cloud-sync
    #: placeholder cancelled early) — there was never anything to keep
    #: or remove either way.
    ever_written: bool


def resolve_cancelled_partial(part_path: Path, *, keep_partial: bool) -> CancelledPartialOutcome:
    """Decides what happens to ``part_path`` once a cancelled export
    unwinds, and performs it (deletes it unless ``keep_partial``) — the
    one place this decision is made, rather than the CLI and TUI each
    hand-rolling their own.

    ``keep_partial=False`` always reports (and performs) removal, even
    when ``part_path`` never existed to begin with — "nothing left
    behind" holds either way, so this branch never needs to check
    existence at all. Only ``keep_partial=True`` needs to distinguish
    "kept" from "never written," since claiming a kept file that isn't
    there would be actively misleading."""
    if keep_partial:
        if part_path.exists():
            return CancelledPartialOutcome(kept=True, ever_written=True)
        return CancelledPartialOutcome(kept=False, ever_written=False)
    part_path.unlink(missing_ok=True)
    return CancelledPartialOutcome(kept=False, ever_written=True)
