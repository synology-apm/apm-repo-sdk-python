"""The plain value types a ``DedupFile`` hands out: its extents and an export's
result. A leaf module (no imports from the rest of ``dedup/``), so ``chunk_walk``
and ``export_scheduler`` can use them without importing ``dedup_file``, which
imports both."""

from __future__ import annotations

import dataclasses
import enum
from typing import Literal

from ..format.addressing import ChunkAddress


class ExtentKind(enum.Enum):
    """The kind of a ``DedupFile`` extent. ``ZERO`` (an explicit
    ``ChunkMapKind.ZERO`` chunk-map record) and ``HOLE`` (a gap between
    records) both read back as zero bytes but are counted separately: a
    ``HOLE`` is a sparse gap, a ``ZERO`` is recorded zero data.
    """

    DATA = 1
    ZERO = 2
    HOLE = 3


@dataclasses.dataclass(frozen=True, slots=True)
class DataExtent:
    """A ``DATA`` span: ``addr`` is the template's starting address and the
    span's chunks are drawn from its ``1 + repeat`` repetitions of
    ``map_num`` chunks (``ChunkAddress.advance``'s carry semantics)."""

    offset: int
    length: int
    addr: ChunkAddress
    map_num: int
    repeat: int = 0
    kind: Literal[ExtentKind.DATA] = dataclasses.field(default=ExtentKind.DATA, init=False)

    @property
    def end(self) -> int:
        return self.offset + self.length


@dataclasses.dataclass(frozen=True, slots=True)
class GapExtent:
    """A span that reads back as zero bytes: a ``HOLE`` or a ``ZERO``."""

    offset: int
    length: int
    kind: Literal[ExtentKind.ZERO, ExtentKind.HOLE]

    @property
    def end(self) -> int:
        return self.offset + self.length


Extent = DataExtent | GapExtent
"""One contiguous span of a ``DedupFile``; ``kind`` tells the two apart."""


@dataclasses.dataclass(frozen=True, slots=True)
class ExportResult:
    """The outcome of one ``export_range()`` call.

    Attributes:
        bytes_written: Real ``DATA`` bytes written, excluding any zero-fill.
        logical_size: The exported file's full logical size.
        holes: Total bytes of ``HOLE`` regions in the range.
        zeros: Total bytes of ``ZERO`` regions in the range.
        sink_wait_seconds: Seconds a segmented export waited for its sink to
            accept the next segment; ``0.0`` for any other sink.
    """

    bytes_written: int
    logical_size: int
    holes: int
    zeros: int
    sink_wait_seconds: float = 0.0
