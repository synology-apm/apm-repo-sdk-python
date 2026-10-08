"""Dedup/bucket/composition constants.

Cross-module constants only; per-format header field offsets stay local to
their module.
"""

from __future__ import annotations

# --- ChunkAddress bit layout (FORMAT-SPEC.md: ChunkAddress) -------------------------

BUCKET_ID_BIT_NUM = 40
"""Width of the ``bucketID`` field in a packed ``ChunkAddress``."""

CHUNK_BIT_NUM = 16
"""Width of the ``chunkIdx`` field in a packed ``ChunkAddress`` — the address
is ``(streamID << BUCKET_ID_BIT_NUM | bucketID) << CHUNK_BIT_NUM | chunkIdx``."""

MAX_BUCKET_NUM = 1 << BUCKET_ID_BIT_NUM

# --- Bucket capacity / Pool path layering (FORMAT-SPEC.md: ChunkAddress; Pool path layering) ------

BUCKET_ID_LAYER_SHIFT = 10
"""The 10-bit layering shift of both Pool paths and Composition sub-paths,
the same algorithm applied to different id domains (see FORMAT-SPEC.md, Pool
path layering and Composition file splitting)."""

GROUP_BUCKET_NUM = 1 << BUCKET_ID_LAYER_SHIFT
"""Buckets per ``.inf``/``.fgp``/``.ref`` group (1024)."""

BUCKET_CHUNK_BIT_NUM = 13
BUCKET_MAX_CHUNK_NUM = 1 << BUCKET_CHUNK_BIT_NUM
"""Chunks per bucket (8192), the carry threshold for
``ChunkAddress.advance``. Overflow carries into ``bucketID`` at 8192, *not*
at the packed field's 16-bit width (65536)."""

FIXED_CHUNK_BIT_NUM = 12
FIXED_CHUNK_LENGTH = 1 << FIXED_CHUNK_BIT_NUM  # 4096
"""The fixed chunk granularity; every chunk-map offset is a multiple of it."""

# --- Bucket file physical layout (FORMAT-SPEC.md: Physical layout) ----------------------

RESERVED_LENG = 4096
"""Uncompressed bucket layout's data start offset."""

COMPRESS_RESERVED_LENG = 16384
"""Compressed bucket layout's data start offset (64-byte header + SizeStore
blob padded to this length), the layout current writers always use
(FORMAT-SPEC.md: Header & mode bits)."""

# --- SizeStore (FORMAT-SPEC.md: SizeStore) ----------------------------------------

SIZE_STORE_REC_BIT_NUM = 15
"""Bits per chunk in the SizeStore bitstream — 3-bit CompressType + 12-bit size."""

# --- ChunkCrcStore (FORMAT-SPEC.md: ChunkCrcStore & Redundancy) ------------------------------------

CHUNK_CRC_SIZE = 4
"""Bytes per non-empty chunk's entry in the (verify-only) ChunkCrcStore trailer."""

# --- Redundancy coverage (FORMAT-SPEC.md: ChunkCrcStore & Redundancy) ------------------------------

REDUNDANCY_COVERAGE_BUCKET = 256
REDUNDANCY_COVERAGE_COMPOSITION = 8192

# --- Composition (FORMAT-SPEC.md: Composition file splitting; RecordHead) --------------------------------

SUB_FILE_SIZE_SHIFT = 24
SUB_FILE_SIZE = 1 << SUB_FILE_SIZE_SHIFT  # 16 MiB
"""Composition sub-file size and the shift used to split a global offset
into ``(sub_id, offset_within_subfile)``."""

RECORD_HEAD_LENGTH = 32
"""The byte length of a ``RecordHead``, as FORMAT-SPEC.md's RecordHead
section defines it."""

CHUNK_MAP_RECORD_LENGTH = 20
"""The byte length of a ``ChunkMapRecord``, as FORMAT-SPEC.md's
ChunkMapRecord section defines it."""
