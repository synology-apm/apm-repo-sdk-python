"""``BucketReaderCache``: a private ``BucketReader`` cache for one bulk
sweep through many buckets — unbounded by default, unlike ``Pool``'s own
session-wide bounded cache, since export's own repeat-visit pattern
benefits from unbounded growth; a caller whose own access pattern doesn't
benefit passes its own ``maxsize``.
"""

from __future__ import annotations

import dataclasses

from ...asynccache import AsyncKeyedCache
from ...identifiers import BucketId, StreamId
from ._bucket_reader import BucketReader


# Not frozen: a stateful cache object, not a value model — buckets is
# grown in place as new BucketReaders are opened.
@dataclasses.dataclass
class BucketReaderCache:
    """Export's bucket-major path and ``verify``'s Bucket-and-key stage
    each build one instead of going through ``Pool``'s own bounded
    ``_buckets``, so a sweep touching every bucket once doesn't evict
    genuinely-hot interactive entries from that shared cache. Reads/writes
    never cross over with ``Pool``'s own cache — the two stay fully
    independent.

    ``maxsize`` (``None``, the default: unbounded) is bounded by however
    many distinct buckets one sweep actually touches unless the caller
    passes a smaller cap — see each construction site's own reasoning for
    why it picked the value it did. Built on ``AsyncKeyedCache`` like every
    other cache in this SDK, with ``fetch`` supplied *per call* (typically
    ``Pool.open_bucket_uncached``) since this class is constructed at
    layers with no ``Pool`` reference of their own.

    One instance is shared across every fragment of a
    ``VirtualDiskContentSource`` export, so a bucket one fragment opens
    stays open for the next; a standalone ``export_to`` call or a
    ``verify`` run instead each get their own separate instance.

    Note:
        Deliberately does not also cache decoded chunk plaintext across
        calls — same-call repeats are already deduplicated by
        ``exec_chunks`` itself, and unconditionally remembering every
        decoded chunk would cost memory roughly equal to the whole
        export's unique DATA content for little cross-call reuse against
        real VM-image exports. Chunk decode stays scoped to one
        bucket-group call, which is also what lets ``decompress_many``
        hand back zero-copy ``memoryview`` values instead of an
        independent ``bytes`` copy per chunk.
    """

    maxsize: int | None = None
    buckets: AsyncKeyedCache[tuple[StreamId, BucketId], BucketReader] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.buckets = AsyncKeyedCache(maxsize=self.maxsize)
