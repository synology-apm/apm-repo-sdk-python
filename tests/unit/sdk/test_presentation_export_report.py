"""Unit tests for ``sdk/presentation/export_report.py``."""

from __future__ import annotations

import errno
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from support.fakes import faithful_to
from synology_apm_repo.sdk.dedup.export_sink import AbortOutcome, ExportSink
from synology_apm_repo.sdk.presentation.export_report import (
    ExportTracker,
    is_out_of_space,
    leftover_note,
    out_of_space_message,
)


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_a_full_filesystem_or_quota_is_out_of_space(code: int) -> None:
    assert is_out_of_space(OSError(code, "no room"))


@pytest.mark.parametrize("exc", [OSError(errno.EIO, "io"), OSError("no errno"), ValueError("x"), RuntimeError()])
def test_anything_else_is_not_out_of_space(exc: BaseException) -> None:
    assert not is_out_of_space(exc)


@pytest.mark.parametrize(
    ("outcome", "hint", "expected"),
    [
        (AbortOutcome(kept=False, ever_written=False), False, "no output file was ever written"),
        (AbortOutcome(kept=False, ever_written=False), True, "no output file was ever written"),
        (AbortOutcome(kept=False, ever_written=True), False, "partial file removed"),
        (AbortOutcome(kept=False, ever_written=True), True, "partial file removed (use --keep-partial to keep it)"),
        (AbortOutcome(kept=True, ever_written=True), False, "partial file kept as out.bin.part"),
        (AbortOutcome(kept=True, ever_written=True), True, "partial file kept as out.bin.part"),
    ],
)
def test_leftover_note_states_what_was_left_behind(outcome: AbortOutcome, hint: bool, expected: str) -> None:
    assert leftover_note(outcome, "out.bin.part", keep_partial_hint=hint) == expected


def test_out_of_space_message_names_the_destination_the_reason_and_the_leftover() -> None:
    output = Path("/data/out.bin")  # rendered with the platform's own separator

    message = out_of_space_message(
        output,
        OSError(errno.ENOSPC, "No space left on device"),
        AbortOutcome(kept=True, ever_written=True),
        "out.bin.part",
    )

    assert message == (
        f"not enough free space to write {output} (No space left on device) — partial file kept as out.bin.part"
    )


def test_out_of_space_message_without_an_os_reason_still_reads_sensibly() -> None:
    message = out_of_space_message(
        Path("out.bin"), OSError(errno.ENOSPC), AbortOutcome(kept=False, ever_written=False), "out.bin.part"
    )

    assert message == snapshot(
        "not enough free space to write out.bin (no space left on device) — no output file was ever written"
    )


@faithful_to(ExportSink)
class _RecordingSink:
    def __init__(self, outcome: AbortOutcome) -> None:
        self.path = Path("out.bin.part")
        self.aborts = 0
        self._outcome = outcome

    async def abort(self) -> AbortOutcome:
        self.aborts += 1
        return self._outcome


async def test_tracker_without_an_item_in_flight_has_no_leftover_and_aborts_nothing() -> None:
    tracker = ExportTracker(Path("out.bin"))

    assert await tracker.leftover() is None
    assert tracker.files_note() is None
    assert await tracker.out_of_space(OSError(errno.ENOSPC, "full")) == snapshot(
        "not enough free space to write out.bin (full)"
    )


async def test_tracker_aborts_the_item_in_flight_and_reports_what_it_left() -> None:
    sink = _RecordingSink(AbortOutcome(kept=True, ever_written=True))
    tracker = ExportTracker(Path("dst"), total=3)
    tracker.item_started(sink, Path("dst/a.bin"), "a.bin")

    assert await tracker.leftover() == "partial file kept as out.bin.part"
    assert sink.aborts == 1
    assert (tracker.destination, tracker.label) == (Path("dst/a.bin"), "a.bin")
    assert tracker.files_note() == "0 of 3 files had been exported"


async def test_a_finished_item_leaves_nothing_to_abort() -> None:
    sink = _RecordingSink(AbortOutcome(kept=False, ever_written=True))
    tracker = ExportTracker(Path("dst"), total=2)
    tracker.item_started(sink, Path("dst/a.bin"), "a.bin")
    tracker.item_finished()

    assert await tracker.leftover() is None
    assert sink.aborts == 0
    assert tracker.files_note() == "1 of 2 files had been exported"
