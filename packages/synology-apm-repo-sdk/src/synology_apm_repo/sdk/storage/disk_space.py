"""The free-space check before every temporary SQLite materialization
(``sqlite.open_sqlite``'s private copy, ``sqlite_source.SqliteSource``'s
temp files), so a write never leaves the temp filesystem full.

A write proceeds only if ``free - in_flight >= needed + reserve``; one of
unknown size is checked the same way piece by piece as it writes:

- ``free``: ``shutil.disk_usage(dir).free``, the bytes an unprivileged
  process may still write.
- ``in_flight``: bytes reserved by this process's other writes to the same
  filesystem that haven't finished yet.
- ``reserve``: ``total / 20``, at least the smaller of 1 GiB and
  ``total / 10``, at most 10 GiB -- left free for the rest of the machine.

Other processes' writes are not seen until they land, so the check stays
best-effort.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import threading
from collections.abc import Iterator
from pathlib import Path

from ..errors import ResourceLimitExceededError
from ..presentation.format import format_bytes

#: The reserve's floor on a filesystem of 10 GiB or more: room for swap
#: growth, logs and a package update, where 5% would be a few hundred MiB.
_MIN_RESERVE = 1 << 30  # 1 GiB
#: Below 10 GiB the floor is this fraction of the filesystem instead, so a
#: small tmpfs still takes a small copy.
_SMALL_FS_FLOOR_DIVISOR = 10  # 10%
#: The reserve's ceiling, reached at 200 GiB, so a large filesystem with
#: plenty free still takes a small copy.
_MAX_RESERVE = 10 << 30  # 10 GiB
#: The reserve as a fraction of the filesystem's total size, between the two bounds.
_RESERVE_DIVISOR = 20  # 5%

_lock = threading.Lock()
#: Bytes reserved by unfinished writes, per filesystem (``st_dev``).
_in_flight: dict[int, int] = {}


def _free_space_reserve(total_bytes: int) -> int:
    """The bytes kept free on a filesystem of ``total_bytes``."""
    floor = min(_MIN_RESERVE, total_bytes // _SMALL_FS_FLOOR_DIVISOR)
    return max(floor, min(total_bytes // _RESERVE_DIVISOR, _MAX_RESERVE))


class DiskReservation:
    """A write's hold on free space, from ``reserve_disk_space``.

    A write of known size holds all of it from the start; one of unknown
    size holds what it has written so far, growing through ``account()``,
    so concurrent writes see each other's bytes either way.

    Attributes:
        dir_path: The directory checked.
        needed: The size asked for, ``None`` when unknown.
        free: The filesystem's free bytes at the check.
        in_flight: Bytes other unfinished writes held at the check.
        reserve: The bytes kept free for the rest of the machine.
    """

    __slots__ = ("_device", "_held", "_written", "dir_path", "free", "in_flight", "needed", "reserve")

    def __init__(
        self, dir_path: Path, device: int, needed: int | None, free: int, in_flight: int, reserve: int
    ) -> None:
        self.dir_path = dir_path
        self.needed = needed
        self.free = free
        self.in_flight = in_flight
        self.reserve = reserve
        self._device = device
        self._held = needed or 0
        self._written = 0

    def account(self, size: int) -> None:
        """Count ``size`` more bytes about to be written.

        Raises:
            ResourceLimitExceededError: They would take a write past its
                declared ``needed``, or an unknown-size write into the
                reserve or other writes' holds.
        """
        written = self._written + size
        if self.needed is not None:
            if written > self.needed:
                raise self.shortfall_error(self.needed, more_than=True)
            self._written = written
            return
        with _lock:
            others = _in_flight.get(self._device, 0) - self._held
            room = self.free - others - self.reserve
            if room < written:
                raise self.shortfall_error(max(room, 0), more_than=True)
            _in_flight[self._device] = others + written
            self._written = self._held = written

    def shortfall_error(self, needed_bytes: int | None, *, more_than: bool = False) -> ResourceLimitExceededError:
        """The error for a write of ``needed_bytes`` (``None``: undeclared)
        that doesn't fit; ``more_than`` for one found mid-write to outgrow
        it."""
        if needed_bytes is None:
            needed = "an undeclared size"
        else:
            needed = f"more than {format_bytes(needed_bytes)}" if more_than else format_bytes(needed_bytes)
        held = (
            f", {format_bytes(self.in_flight)} of it held by other temporary copies in progress"
            if self.in_flight
            else ""
        )
        short = self.reserve + self.in_flight + (needed_bytes if needed_bytes is not None else 1) - self.free
        shortfall = "" if more_than else f" ({format_bytes(short)} short)"
        return ResourceLimitExceededError(
            f"not enough free space under {self.dir_path} for a temporary copy: it needs {needed} "
            f"plus {format_bytes(self.reserve)} kept free for the rest of the system, but only "
            f"{format_bytes(self.free)} is free{held}{shortfall} — free up space there, or set TMPDIR "
            "(TEMP on Windows) to a directory on a larger filesystem",
            ref=str(self.dir_path),
        )


@contextlib.contextmanager
def reserve_disk_space(dir_path: Path, needed_bytes: int | None) -> Iterator[DiskReservation]:
    """Check that ``needed_bytes`` fit under ``dir_path`` with the reserve
    left over, and hold them against this process's other writes to that
    filesystem until the block exits, however it exits.

    Blocking; call it from the worker thread that does the write.

    Args:
        dir_path: An existing directory on the filesystem written to.
        needed_bytes: The most the write can produce. ``None`` (unknown)
            holds nothing up front; the caller passes each piece to
            ``DiskReservation.account()`` before writing it.

    Raises:
        ResourceLimitExceededError: The write doesn't fit with the reserve
            left free (for ``None``, nothing is free above it).
    """
    device = os.stat(dir_path).st_dev
    usage = shutil.disk_usage(dir_path)
    reserve = _free_space_reserve(usage.total)
    with _lock:
        in_flight = _in_flight.get(device, 0)
        reservation = DiskReservation(dir_path, device, needed_bytes, usage.free, in_flight, reserve)
        if usage.free - in_flight - reserve < (needed_bytes if needed_bytes is not None else 1):
            raise reservation.shortfall_error(needed_bytes)
        _in_flight[device] = in_flight + (needed_bytes or 0)
    try:
        yield reservation
    finally:
        with _lock:
            remaining = _in_flight.get(device, 0) - reservation._held  # noqa: SLF001 -- released by its owner
            if remaining:
                _in_flight[device] = remaining
            else:
                _in_flight.pop(device, None)
