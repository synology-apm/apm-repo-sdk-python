"""The dedup core: ``(stream, session, comp_offset) -> bytes``.
This is the layer that actually knows about chunks, buckets, and
encryption; everything above it only ever deals in file-shaped reads.
"""

from __future__ import annotations
