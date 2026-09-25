"""Dedup/bucket/composition constants.

Centralized here specifically because these are *cross-module* constants
(used by ``addressing.py``, ``bucket.py``, ``composition.py``,
``dedup/pool/_bucket_reader.py``, ``dedup/dedup_file.py``). Per-format
*header field offsets*
(e.g. ``bucket.py``'s ``_OFF_MODE``) are local to their own module instead —
those aren't shared, so centralizing them would just add a layer of
indirection with no reuse benefit.
"""

from __future__ import annotations

# --- ChunkAddress bit layout (FORMAT-SPEC.md: ChunkAddress) -------------------------

BUCKET_ID_BIT_NUM = 40
"""Width of the ``bucketID`` field in a packed ``ChunkAddress``."""

CHUNK_BIT_NUM = 16
"""Width of the ``chunkIdx`` field in a packed ``ChunkAddress`` — the address
is ``(streamID << BUCKET_ID_BIT_NUM | bucketID) << CHUNK_BIT_NUM | chunkIdx``."""

MAX_BUCKET_NUM = 1 << BUCKET_ID_BIT_NUM

# --- Bucket capacity / Pool path layering (FORMAT-SPEC.md: ChunkAddress, pool-path-layering) ------

BUCKET_ID_LAYER_SHIFT = 10
"""Both the Pool path 10-bit directory layering shift *and* the Composition
sub-path (sessionID/subID) layering shift: FORMAT-SPEC.md's pool-path-layering
and composition-splitting sections give both the same shift value. Kept as
one named constant since they are, bit-for-bit, the same algorithm applied
to different id domains."""

GROUP_BUCKET_NUM = 1 << BUCKET_ID_LAYER_SHIFT
"""Buckets per ``.inf``/``.fgp``/``.ref`` group (1024)."""

BUCKET_CHUNK_BIT_NUM = 13
BUCKET_MAX_CHUNK_NUM = 1 << BUCKET_CHUNK_BIT_NUM
"""Chunks per bucket (8192), the carry threshold for
``ChunkAddress.advance``. Overflow carries into ``bucketID`` at 8192, *not*
at the packed field's 16-bit width (65536)."""

FIXED_CHUNK_BIT_NUM = 12
FIXED_CHUNK_LENGTH = 1 << FIXED_CHUNK_BIT_NUM  # 4096
"""The dedup engine's fixed chunk granularity — every chunk-map offset is a
multiple of this."""

# --- Bucket file physical layout (FORMAT-SPEC.md: bucket-physical-layout) ----------------------

RESERVED_LENG = 4096
"""Uncompressed bucket layout's data start offset."""

COMPRESS_RESERVED_LENG = 16384
"""Compressed bucket layout's data start offset (header[0,64) + SizeStore
blob padded to this length) — the layout every ABP-built ``.buk`` uses,
since ABP always enables COMPRESS (FORMAT-SPEC.md: bucket-header)."""

# --- SizeStore (FORMAT-SPEC.md: SizeStore) ----------------------------------------

SIZE_STORE_REC_BIT_NUM = 15
"""Bits per chunk in the SizeStore bitstream — 3-bit CompressType + 12-bit size."""

# --- ChunkCrcStore (FORMAT-SPEC.md: ChunkCrcStore) ------------------------------------

CHUNK_CRC_SIZE = 4
"""Bytes per non-empty chunk's entry in the (verify-only) ChunkCrcStore trailer."""

# --- Redundancy coverage (FORMAT-SPEC.md: ChunkCrcStore) ------------------------------

REDUNDANCY_COVERAGE_BUCKET = 256
REDUNDANCY_COVERAGE_COMPOSITION = 8192

# --- Composition (FORMAT-SPEC.md: composition-splitting, RecordHead) --------------------------------

SUB_FILE_SIZE_SHIFT = 24
SUB_FILE_SIZE = 1 << SUB_FILE_SIZE_SHIFT  # 16 MiB
"""Composition sub-file size and the shift used to split a global offset
into ``(sub_id, offset_within_subfile)``."""

RECORD_HEAD_LENGTH = 32
"""``RecordHead`` byte length (FORMAT-SPEC.md: RecordHead)."""

CHUNK_MAP_RECORD_LENGTH = 20
"""``ChunkMapRecord`` byte length (FORMAT-SPEC.md: ChunkMapRecord)."""
