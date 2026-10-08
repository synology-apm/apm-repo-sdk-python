"""How an export is described to the user, shared by the CLI's ``export``
command and the Browser's export worker: its byte counts, what a failed sink
left on disk, and which failures mean "no room left"."""

from __future__ import annotations

import dataclasses
import errno
from pathlib import Path
from typing import Protocol, TypeGuard

from .format import format_bytes, pluralize

#: ``errno`` values that mean the destination has no room left, from a
#: full filesystem or the user's quota on it.
NO_SPACE_ERRNOS = frozenset({errno.ENOSPC, errno.EDQUOT})


class LeftoverOutcome(Protocol):
    """What ``ExportSink.abort`` reports (``AbortOutcome`` satisfies this)."""

    @property
    def kept(self) -> bool: ...

    @property
    def ever_written(self) -> bool: ...


class AbortableSink(Protocol):
    """A sink an unfinished export can abort (``LocalFileSink`` satisfies this)."""

    @property
    def path(self) -> Path: ...

    async def abort(self) -> LeftoverOutcome: ...


class ExportSizes(Protocol):
    """An export's byte counts (``ExportResult`` satisfies this)."""

    @property
    def bytes_written(self) -> int: ...

    @property
    def logical_size(self) -> int: ...

    @property
    def holes(self) -> int: ...

    @property
    def zeros(self) -> int: ...


def size_summary(sizes: ExportSizes) -> str:
    """``"<written> written, <logical> logical, <holes> holes, <zeros> zero-fill"``."""
    return (
        f"{format_bytes(sizes.bytes_written)} written, {format_bytes(sizes.logical_size)} logical, "
        f"{format_bytes(sizes.holes)} holes, {format_bytes(sizes.zeros)} zero-fill"
    )


def is_out_of_space(exc: BaseException) -> TypeGuard[OSError]:
    """Whether ``exc`` is an ``OSError`` for a full filesystem or an exceeded quota."""
    return isinstance(exc, OSError) and exc.errno in NO_SPACE_ERRNOS


def leftover_note(outcome: LeftoverOutcome, part_name: str, *, keep_partial_hint: bool = False) -> str:
    """What an unfinished export left on disk, phrased from the sink's ``abort()``
    outcome. ``keep_partial_hint`` adds the ``--keep-partial`` advice where the
    surface has that option."""
    if not outcome.ever_written:
        return "no output file was ever written"
    if outcome.kept:
        return f"partial file kept as {part_name}"
    return "partial file removed (use --keep-partial to keep it)" if keep_partial_hint else "partial file removed"


def out_of_space_message(
    output: Path, exc: OSError, outcome: LeftoverOutcome, part_name: str, *, keep_partial_hint: bool = False
) -> str:
    """The message for an export that ran out of room writing ``output``, with
    what it left behind. ``exc`` satisfies ``is_out_of_space``."""
    reason = exc.strerror or "no space left on device"
    note = leftover_note(outcome, part_name, keep_partial_hint=keep_partial_hint)
    return f"not enough free space to write {output} ({reason}) — {note}"


@dataclasses.dataclass
class ExportTracker:
    """What an export in flight is writing — the current item's sink,
    destination and label, and for a folder how many files are done — so a
    cancel or a full disk reports on the right file. Updated as the export
    runs, hence not frozen.

    Attributes:
        destination: Where the current item is written.
        sink: The current item's sink; ``None`` before it starts and once it
            is committed, when a cancel has nothing of it to report.
        label: The current item's name within a folder export (``""`` for one item).
        done: Folder files exported so far.
        total: The folder's file count; ``None`` for a single item.
    """

    destination: Path
    sink: AbortableSink | None = None
    label: str = ""
    done: int = 0
    total: int | None = None

    def item_started(self, sink: AbortableSink, destination: Path, label: str) -> None:
        """A folder export moved on to the item ``label``, written through ``sink`` to ``destination``."""
        self.sink, self.destination, self.label = sink, destination, label

    def item_finished(self) -> None:
        """The current folder item was committed."""
        self.done += 1
        self.sink = None

    def files_note(self) -> str | None:
        """``"<done> of <total> files had been exported"`` for a folder export, else ``None``."""
        if self.total is None:
            return None
        return f"{self.done} of {self.total} {pluralize(self.total, 'file')} had been exported"

    async def leftover(self, *, keep_partial_hint: bool = False) -> str | None:
        """Aborts the current item's sink (idempotent) and says what it left on
        disk (``leftover_note``); ``None`` when no item is in flight."""
        if self.sink is None:
            return None
        return leftover_note(await self.sink.abort(), self.sink.path.name, keep_partial_hint=keep_partial_hint)

    async def out_of_space(self, exc: OSError, *, keep_partial_hint: bool = False) -> str:
        """``out_of_space_message`` for the current item, aborting its sink; without
        an item in flight, just the reason. ``exc`` satisfies ``is_out_of_space``."""
        if self.sink is None:
            return f"not enough free space to write {self.destination} ({exc.strerror or 'no space left on device'})"
        outcome = await self.sink.abort()
        return out_of_space_message(
            self.destination, exc, outcome, self.sink.path.name, keep_partial_hint=keep_partial_hint
        )
