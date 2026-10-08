"""Redundancy blob sizing and self-repair (FORMAT-SPEC.md: ChunkCrcStore & Redundancy).

Every Composition record and ``.buk`` bucket carries a trailing Redundancy
blob: a ``2*coverage``-byte ring-buffer XOR parity (even-indexed windows
into ``parity[0:coverage]``, odd-indexed into ``parity[coverage:2*coverage]``)
plus a *rolling* CRC32 checkpoint after each window (``StepCrc``). Because
the checkpoint is rolling, not independent per window, this mechanism can
locate and repair at most one corrupted region, bounded to two consecutive
coverage-windows — a "single-disk-failure RAID" limit, not N-way parity.

``redundancy_size`` computes the blob's on-disk length (see
``expected_bucket_size``, ``composition.py``'s ``record_total_length``).
``parse_redundancy_blob``/``attempt_repair`` do the repair
(``dedup.redundancy_repair`` fetches the trailer and runs it).
"""

from __future__ import annotations

import dataclasses
import struct
import zlib

from ..errors import FormatError
from .headers import compute_crc32

REDUNDANCY_MAGIC = b"RD"
_HEAD_LENGTH = 16
_VERSION = 0

_SPEC = "FORMAT-SPEC.md: ChunkCrcStore & Redundancy"


def redundancy_size(data_size: int, coverage: int) -> int:
    """Total byte length of a Redundancy blob covering ``data_size`` bytes
    of underlying data, checkpointed every ``coverage`` bytes:

    ``16 (header) + 4 * ceil(data_size / coverage) (StepCrc array) +
    min(data_size, 2 * coverage) (parity)`` (FORMAT-SPEC.md: ChunkCrcStore &
    Redundancy).

    ``coverage`` is 256 for bucket trailers (``REDUNDANCY_COVERAGE_BUCKET``),
    8192 for composition record trailers (``REDUNDANCY_COVERAGE_COMPOSITION``).
    """
    step_crc_len, parity_len = _blob_component_lengths(data_size, coverage)
    return _HEAD_LENGTH + step_crc_len + parity_len


def _blob_component_lengths(data_size: int, coverage: int) -> tuple[int, int]:
    """``(step_crc_len, parity_len)``, the variable-length components of the
    blob."""
    step_crc_len = 4 * ((data_size + coverage - 1) // coverage)
    parity_len = min(data_size, 2 * coverage)
    return step_crc_len, parity_len


@dataclasses.dataclass(frozen=True, slots=True)
class RedundancyBlob:
    """One parsed Redundancy trailer: the rolling per-window CRC32
    checkpoints (``step_crc``, one per ``coverage``-byte window of the
    protected data, the last possibly shorter) plus the XOR parity ring
    buffer (``parity``, at most ``2*coverage`` bytes) that
    ``attempt_repair`` reconstructs from."""

    coverage: int
    data_size: int
    step_crc: tuple[int, ...]
    parity: bytes


def parse_redundancy_blob(data: bytes, *, data_size: int, coverage: int) -> RedundancyBlob:
    """Parse a Redundancy trailer of ``redundancy_size(data_size, coverage)``
    bytes (FORMAT-SPEC.md: ChunkCrcStore & Redundancy). The header's
    coverage and data size are checked against the caller's values (known
    independently from the record or bucket header), not trusted.

    Raises:
        FormatError: ``data`` is too short, or its magic, version, coverage
            or data_size doesn't match; the blob is unusable for repair.
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
    """Index of the first window whose rolling CRC32 checkpoint diverges from
    ``blob.step_crc``, or ``None`` if all match. Callers reach this only after
    ``data``'s own CRC failed, so ``None`` means the corruption can't be
    localized."""
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
    """Reconstruct window ``idx`` from its parity half and every other window
    sharing that half (even/odd) in ``data``; correct while at most one
    window per half is corrupted.

    Windows are XORed as big-endian integers. A short final window is shifted
    to the leading bytes a per-byte XOR would touch, and ``half_len`` (the
    half's actual width, below ``coverage`` for a small ``data_size``)
    replaces ``blob.coverage`` in that shift.
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
    """Best-effort, in-memory self-repair of ``data`` (known not to match
    ``expected_crc``) using its trailing Redundancy blob (FORMAT-SPEC.md:
    ChunkCrcStore & Redundancy).

    Reconstructs windows ``[bad_idx, bad_idx+1]`` around the first divergent
    rolling checkpoint (the checkpoint alone can't tell whether
    ``bad_idx+1`` is also corrupt), then checks the whole candidate against
    ``expected_crc``; a non-``None`` result is confirmed correct.

    Returns:
        The repaired ``data`` (same length), or ``None`` if the blob doesn't
        parse, the scan can't localize the damage, or the candidate still
        fails the CRC. Never raises.
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
