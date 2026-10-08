"""``BucketReaderCache``: a private ``BucketReader`` cache for one bulk
sweep through many buckets."""

from __future__ import annotations

from ...asynccache import AsyncKeyedCache
from ...cachemanager import DEFAULT_LIMITS
from ...identifiers import BucketId, StreamId
from ._bucket_reader import BucketReader


class BucketReaderCache(AsyncKeyedCache[tuple[StreamId, BucketId], BucketReader]):
    """Bucket-major export and ``verify``'s bucket checks each use one instead
    of ``Pool``'s bounded cache, so a sweep touching every bucket once doesn't
    evict hot interactive entries. Read through ``Pool.bucket(..., cache=)``;
    never shared with ``Pool``'s own cache. One instance spans every fragment
    of a ``VirtualDiskContentSource`` export; a standalone ``export_range``
    or a ``verify`` run gets its own.

    Decoded chunk plaintext is not cached here: it stays scoped to one
    bucket-group call, which lets ``decompress_many`` return zero-copy
    ``memoryview`` values.
    """

    def __init__(self, maxsize: int = DEFAULT_LIMITS.bucket_readers) -> None:
        super().__init__(maxsize=maxsize)

    @classmethod
    def for_verify(cls) -> BucketReaderCache:
        """One verify run's cache, sized ``DEFAULT_LIMITS.verify_bucket_readers``."""
        return cls(maxsize=DEFAULT_LIMITS.verify_bucket_readers)
