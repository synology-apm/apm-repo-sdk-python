"""Redundancy blob sizing and self-repair (FORMAT-SPEC.md: ChunkCrcStore).

Every Composition record and ``.buk`` bucket carries a trailing Redundancy
blob: a ``2*coverage``-byte ring-buffer XOR parity (even-indexed windows
into ``parity[0:coverage]``, odd-indexed into ``parity[coverage:2*coverage]``)
plus a *rolling* CRC32 checkpoint after each window (``StepCrc``). Because
the checkpoint is rolling, not independent per window, this mechanism can
locate and repair at most one corrupted region, bounded to two consecutive
coverage-windows — a "single-disk-failure RAID" limit, not N-way parity.

``redundancy_size`` computes the blob's on-disk length, needed to size a
bucket/record independently of its own declared length (see
``expected_bucket_size``, ``composition.py``'s ``record_total_length``).
``parse_redundancy_blob``/``attempt_repair`` do the actual repair — see
``attempt_repair`` for how it confirms one before returning it —
and ``repair_via_trailer`` wraps the "fetch the trailer, then repair" shape
every call site otherwise duplicated.
"""

from __future__ import annotations

import asyncio
import dataclasses
import struct
import zlib
from collections.abc import Awaitable, Callable

from ..errors import FormatError, NotFoundError
from .headers import compute_crc32

REDUNDANCY_MAGIC = b"RD"
_HEAD_LENGTH = 16
_VERSION = 0

_SPEC = "FORMAT-SPEC.md: ChunkCrcStore"


def redundancy_size(data_size: int, coverage: int) -> int:
    """Total byte length of a Redundancy blob covering ``data_size`` bytes
    of underlying data, checkpointed every ``coverage`` bytes:

    ``16 (header) + 4 * ceil(data_size / coverage) (StepCrc array) +
    min(data_size, 2 * coverage) (parity)`` (FORMAT-SPEC.md: ChunkCrcStore).

    ``coverage`` is 256 for bucket trailers, 8192 for composition record
    trailers (FORMAT-SPEC.md: ChunkCrcStore).
    """
    step_crc_len, parity_len = _blob_component_lengths(data_size, coverage)
    return _HEAD_LENGTH + step_crc_len + parity_len


def _blob_component_lengths(data_size: int, coverage: int) -> tuple[int, int]:
    """``(step_crc_len, parity_len)`` — the two variable-length components
    ``redundancy_size`` sums and ``parse_redundancy_blob`` slices out
    individually."""
    step_crc_len = 4 * ((data_size + coverage - 1) // coverage)
    parity_len = min(data_size, 2 * coverage)
    return step_crc_len, parity_len


@dataclasses.dataclass(frozen=True)
class RedundancyBlob:
    """One parsed Redundancy trailer: the rolling per-window CRC32
    checkpoints (``step_crc``, one per ``coverage``-byte window of the
    protected data, the last window truncated if ``data_size`` isn't an
    exact multiple of ``coverage``) plus the ``2*coverage``-byte XOR parity
    ring buffer (``parity``) — the even/odd-window-into-each-half layout
    described above, that ``attempt_repair`` reconstructs from."""

    coverage: int
    data_size: int
    step_crc: tuple[int, ...]
    parity: bytes


def parse_redundancy_blob(data: bytes, *, data_size: int, coverage: int) -> RedundancyBlob:
    """Parse a Redundancy trailer of exactly ``redundancy_size(data_size,
    coverage)`` bytes (FORMAT-SPEC.md: ChunkCrcStore) — the *raw* magic/
    version/coverage/data-size header fields are cross-checked against the
    caller-supplied ``data_size``/``coverage`` (the values already known
    independently, from the record's own ``RecordHead``/bucket header),
    not trusted blindly from the blob itself.

    Raises:
        FormatError: ``data`` is shorter than expected, or its magic/
            version/coverage/data_size header fields don't match what was
            expected — either way, this Redundancy blob cannot be used for
            repair (``attempt_repair`` treats this as "no repair possible",
            not a hard failure).
    """
    expected_len = redundancy_size(data_size, coverage)
    if len(data) < expected_len:
        raise FormatError(f"Redundancy blob too short: {len(data)} bytes < {expected_len}", spec=_SPEC)
    if data[0:2] != REDUNDANCY_MAGIC:
        raise FormatError(f"bad Redundancy magic {data[0:2]!r}, expected {REDUNDANCY_MAGIC!r}", spec=_SPEC)
    (version,) = struct.unpack(">H", data[2:4])
    if version != _VERSION:
        raise FormatError(f"unsupported Redundancy blob version {version}", spec=_SPEC)
    stored_coverage, stored_data_size = struct.unpack(">IQ", data[4:16])
    if stored_coverage != coverage or stored_data_size != data_size:
        raise FormatError(
            f"Redundancy blob header (coverage={stored_coverage}, data_size={stored_data_size}) doesn't match "
            f"expected (coverage={coverage}, data_size={data_size})",
            spec=_SPEC,
        )

    step_crc_len, parity_len = _blob_component_lengths(data_size, coverage)
    step_crc_raw = data[_HEAD_LENGTH : _HEAD_LENGTH + step_crc_len]
    parity = data[_HEAD_LENGTH + step_crc_len : _HEAD_LENGTH + step_crc_len + parity_len]
    num_steps = step_crc_len // 4
    step_crc = struct.unpack(f">{num_steps}I", step_crc_raw) if num_steps else ()
    return RedundancyBlob(coverage=coverage, data_size=data_size, step_crc=step_crc, parity=bytes(parity))


def _locate_first_bad_window(blob: RedundancyBlob, data: bytes) -> int | None:
    """Rolling-CRC scan: recompute the cumulative CRC32 after each window
    and compare against ``blob.step_crc[i]``; return the index of the
    *first* window whose checkpoint diverges from the stored value, or
    ``None`` if every checkpoint matches (nothing to repair — a caller
    should only reach this after ``data``'s own CRC check already failed,
    so ``None`` here means the corruption isn't something this rolling
    scan can localize, not that ``data`` is actually fine)."""
    crc = 0
    for i, expected in enumerate(blob.step_crc):
        start, end = _window_span(i, blob.coverage, blob.data_size)
        crc = zlib.crc32(data[start:end], crc) & 0xFFFFFFFF
        if crc != expected:
            return i
    return None


def _window_span(idx: int, coverage: int, data_size: int) -> tuple[int, int]:
    start = idx * coverage
    end = min(start + coverage, data_size)
    return start, end


def _reconstruct_window(data: bytes, blob: RedundancyBlob, idx: int, num_windows: int) -> bytes:
    """Reconstruct window ``idx``'s true bytes from its own parity-ring
    half and every *other* window sharing that half (even/odd), taken from
    ``data`` as-is — correct as long as at most one window per half is
    actually corrupted, which is exactly the bound this whole mechanism
    relies on.

    XORs whole windows as big-endian integers rather than byte-by-byte —
    only the final window (when ``data_size`` isn't an exact multiple of
    ``coverage``) is shorter than ``coverage``, folded in shifted up to
    the same leading bytes a per-byte XOR would touch. ``half_len`` (a
    small ``data_size`` truncates ``blob.parity`` below ``2*coverage``,
    per ``redundancy_size``'s own formula) is this half's *actual* byte
    width — every window folded into it is at most this long, never
    ``blob.coverage`` itself when the two differ.
    """
    half = idx % 2
    half_bytes = blob.parity[half * blob.coverage : (half + 1) * blob.coverage]
    half_len = len(half_bytes)
    acc = int.from_bytes(half_bytes, "big")
    for j in range(half, num_windows, 2):
        if j == idx:
            continue
        start, end = _window_span(j, blob.coverage, blob.data_size)
        window = data[start:end]
        acc ^= int.from_bytes(window, "big") << (8 * (half_len - len(window)))
    start, end = _window_span(idx, blob.coverage, blob.data_size)
    target_len = end - start
    return (acc >> (8 * (half_len - target_len))).to_bytes(target_len, "big")


def attempt_repair(data: bytes, redundancy_raw: bytes, *, coverage: int, expected_crc: int) -> bytes | None:
    """Best-effort, in-memory self-repair of ``data`` (whose CRC32 is
    already known not to match ``expected_crc``) using its trailing
    Redundancy blob (FORMAT-SPEC.md: ChunkCrcStore).

    Reconstructs the pair of consecutive windows ``[bad_idx, bad_idx+1]``
    around the first divergent rolling checkpoint (one window from each
    parity half, since the rolling checkpoint alone can't distinguish
    "only ``bad_idx`` is corrupted" from "``bad_idx+1`` might be too") and
    re-validates the *whole* candidate reconstruction's own CRC32 against
    ``expected_crc`` before ever returning it — so a non-``None`` result is
    byte-for-byte confirmed correct, never a guess.

    Returns:
        The fully repaired ``data`` (same length) on a confirmed-correct
        reconstruction; ``None`` if the blob itself doesn't parse, the
        rolling scan can't localize a starting point, or the candidate
        reconstruction still doesn't validate (more than one region
        corrupted, or the Redundancy blob itself is also damaged). Never
        raises — every failure mode collapses to ``None``, since a
        caller's own fallback (propagate the original ``DataCorruptError``
        unchanged) is identical either way.
    """
    try:
        blob = parse_redundancy_blob(redundancy_raw, data_size=len(data), coverage=coverage)
    except FormatError:
        return None
    if not blob.step_crc:
        return None
    bad_idx = _locate_first_bad_window(blob, data)
    if bad_idx is None:
        return None

    num_windows = len(blob.step_crc)
    candidate_windows = [bad_idx] if bad_idx + 1 >= num_windows else [bad_idx, bad_idx + 1]
    patched = bytearray(data)
    for idx in candidate_windows:
        start, end = _window_span(idx, blob.coverage, blob.data_size)
        patched[start:end] = _reconstruct_window(data, blob, idx, num_windows)

    candidate = bytes(patched)
    if compute_crc32(candidate) != expected_crc:
        return None
    return candidate


async def repair_via_trailer(
    data: bytes,
    *,
    coverage: int,
    expected_crc: int,
    fetch_trailer: Callable[[], Awaitable[bytes]],
) -> bytes | None:
    """Shared orchestration behind every Redundancy-blob self-repair call
    site: fetch the trailer via ``fetch_trailer`` — already resolved to
    that record's/bucket's own trailer offset — and hand it to
    ``attempt_repair``.

    ``attempt_repair`` runs on a real OS thread (``asyncio.to_thread``): the
    map-CRC repair path can see an array up to several MB, and its rolling
    CRC32 scan plus reconstruction must not stall other concurrent Tasks.

    Returns:
        ``attempt_repair``'s own result, or ``None`` if ``fetch_trailer``
        itself raises ``NotFoundError``/``FormatError`` — the "trailer
        can't be fetched" case every call site otherwise handled with its
        own identical ``try``/``except``.
    """
    try:
        redundancy_raw = await fetch_trailer()
    except (NotFoundError, FormatError):
        return None
    return await asyncio.to_thread(attempt_repair, data, redundancy_raw, coverage=coverage, expected_crc=expected_crc)
