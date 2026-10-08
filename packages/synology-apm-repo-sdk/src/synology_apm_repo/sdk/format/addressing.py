"""Chunk addressing and the two path-layering schemes that key off it
(FORMAT-SPEC.md: Pool path layering; Composition file splitting).

Directory-name constants (``Pool``, ``Composition``, ...) live in the Dedup
Layer's ``repository.py`` (FORMAT-SPEC.md: directory layout). This module
computes only the id-layering fragment relative to a caller-supplied root.
"""

from __future__ import annotations

from typing import NamedTuple

from ..identifiers import BucketId, ChunkIdx, SessionId, StreamId
from .const import (
    BUCKET_ID_BIT_NUM,
    BUCKET_ID_LAYER_SHIFT,
    BUCKET_MAX_CHUNK_NUM,
    CHUNK_BIT_NUM,
    GROUP_BUCKET_NUM,
    MAX_BUCKET_NUM,
    SUB_FILE_SIZE,
    SUB_FILE_SIZE_SHIFT,
)

_CHUNK_IDX_MASK = (1 << CHUNK_BIT_NUM) - 1  # 0xFFFF
_BUCKET_ID_MASK = MAX_BUCKET_NUM - 1  # 2**40 - 1


class ChunkAddress(NamedTuple):
    """A single ``uint64`` chunk address: ``streamID(8b) | bucketID(40b) |
    chunkIdx(16b)`` (FORMAT-SPEC.md: ChunkAddress).

    Fields are not range-checked on construction or by ``from_int``; an
    out-of-range field surfaces downstream (``IndexError`` for an
    over-capacity ``chunk_idx``, ``NotFoundError`` for a bogus ``bucket_id``)
    or as a ``verify()`` finding.

    A ``NamedTuple``, not a frozen dataclass: built per chunk, where it is cheaper.
    """

    stream_id: StreamId
    bucket_id: BucketId
    chunk_idx: ChunkIdx

    @classmethod
    def from_int(cls, data: int) -> ChunkAddress:
        """Unpack a raw ``uint64`` chunk address (not range-checked)."""
        chunk_idx = data & _CHUNK_IDX_MASK
        addr_id = data >> CHUNK_BIT_NUM
        bucket_id = addr_id & _BUCKET_ID_MASK
        stream_id = addr_id >> BUCKET_ID_BIT_NUM
        return cls(
            stream_id=StreamId(stream_id),
            bucket_id=BucketId(bucket_id),
            chunk_idx=ChunkIdx(chunk_idx),
        )

    def to_int(self) -> int:
        """Pack into the raw ``uint64`` form."""
        addr_id = (self.stream_id << BUCKET_ID_BIT_NUM) | self.bucket_id
        return (addr_id << CHUNK_BIT_NUM) | self.chunk_idx

    def advance(self, k: int) -> ChunkAddress:
        """Advance by ``k`` chunks, carrying into ``bucket_id`` when
        ``chunk_idx`` reaches ``BUCKET_MAX_CHUNK_NUM`` (8192), not at the packed
        field's 16-bit boundary.

        Raises:
            ValueError: ``k`` is negative.
        """
        if k < 0:
            raise ValueError(f"advance() does not support negative k ({k})")
        total = self.chunk_idx + k
        carry, new_chunk_idx = divmod(total, BUCKET_MAX_CHUNK_NUM)
        return ChunkAddress(
            stream_id=self.stream_id,
            bucket_id=BucketId(self.bucket_id + carry),
            chunk_idx=ChunkIdx(new_chunk_idx),
        )


def _id_layer_ancestors(id_: int) -> list[str]:
    """Ancestor directory names of the 10-bit id-layering scheme, outermost
    first, excluding the leaf: ``id >> 10``, ``>> 20``, ... until the quotient
    is 0 (FORMAT-SPEC.md: Pool path layering). Empty for ``id_ < 1024``.
    """
    ancestors: list[str] = []
    shifted = id_ >> BUCKET_ID_LAYER_SHIFT
    while shifted != 0:
        ancestors.insert(0, str(shifted))
        shifted >>= BUCKET_ID_LAYER_SHIFT
    return ancestors


def pool_layer_path(stream_id: StreamId, bucket_id: BucketId) -> str:
    """Relative path (no ``Pool/`` prefix, no ``.buk``/``.inf``/... suffix)
    for ``bucket_id`` within ``stream_id``'s pool:
    ``<streamID>/[ancestor layers.../]<bucketID>`` (FORMAT-SPEC.md: Pool path
    layering).

    For a ``.inf``/``.fgp``/``.ref`` group path, pass the group's starting
    bucket id (``group_start_bucket_id``), not an individual bucket's.
    """
    return "/".join([str(stream_id), *_id_layer_ancestors(bucket_id), str(bucket_id)])


def composition_session_dir(stream_id: StreamId, session_id: SessionId) -> str:
    """Relative path (no ``Composition/`` prefix) to a session's directory:
    ``<streamID>/[ancestor layers.../]<sessionID>.com`` (FORMAT-SPEC.md:
    Composition file splitting). Unlike the Pool leaf, the leaf carries a
    ``.com`` suffix.
    """
    return "/".join([str(stream_id), *_id_layer_ancestors(session_id), f"{session_id}.com"])


def composition_path(stream_id: StreamId, session_id: SessionId, sub_id: int) -> str:
    """Full relative path (no ``Composition/`` prefix, no sequence-id
    suffix) to a composition sub-file: ``<sessionDir>/[ancestor
    layers.../]c<subID>`` (FORMAT-SPEC.md: Composition file splitting).
    """
    session_dir = composition_session_dir(stream_id, session_id)
    return "/".join([session_dir, *_id_layer_ancestors(sub_id), f"c{sub_id}"])


def split_layer_leaf(layer_path: str) -> tuple[str, str]:
    """Split a ``pool_layer_path()``/``composition_path()`` result into
    ``(dir_part, leaf)``. ``dir_part`` is ``""`` when there are no ancestor
    layers; pass it to ``join_path``, which tolerates that."""
    dir_part, _, leaf = layer_path.rpartition("/")
    return dir_part, leaf


def split_composition_offset(global_offset: int) -> tuple[int, int]:
    """Split a composition-record global offset into ``(sub_id,
    offset_within_subfile)``: ``off >> 24`` and ``off & (16 MiB - 1)``
    (FORMAT-SPEC.md: Composition file splitting)."""
    return global_offset >> SUB_FILE_SIZE_SHIFT, global_offset & (SUB_FILE_SIZE - 1)


def group_start_bucket_id(bucket_id: BucketId) -> BucketId:
    """The starting bucket id of the 1024-bucket group ``bucket_id``
    belongs to: ``bucket_id & ~(GROUP_BUCKET_NUM - 1)``.
    """
    return BucketId(bucket_id & ~(GROUP_BUCKET_NUM - 1))
