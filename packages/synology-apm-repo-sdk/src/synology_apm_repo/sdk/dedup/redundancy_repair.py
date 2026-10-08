"""Self-repair of a CRC-mismatched region from its on-disk Redundancy
trailer (FORMAT-SPEC.md: ChunkCrcStore & Redundancy): fetch the trailer,
then run ``format.redundancy.attempt_repair`` off the event loop."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ..errors import FormatError, NotFoundError
from ..format.redundancy import attempt_repair


async def repair_via_trailer(
    data: bytes,
    *,
    coverage: int,
    expected_crc: int,
    fetch_trailer: Callable[[], Awaitable[bytes]],
) -> bytes | None:
    """Fetch the trailer via ``fetch_trailer`` (already bound to the record's
    or bucket's trailer offset) and hand it to ``attempt_repair``, which runs
    on a worker thread so a large array doesn't stall the event loop.

    Returns:
        ``attempt_repair``'s result, or ``None`` if ``fetch_trailer`` raises
        ``NotFoundError``/``FormatError``.
    """
    try:
        redundancy_raw = await fetch_trailer()
    except (NotFoundError, FormatError):
        return None
    return await asyncio.to_thread(attempt_repair, data, redundancy_raw, coverage=coverage, expected_crc=expected_crc)
