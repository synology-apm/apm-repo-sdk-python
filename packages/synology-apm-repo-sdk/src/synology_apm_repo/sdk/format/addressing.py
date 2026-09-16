"""Chunk addressing and the two path-layering schemes that key off it
(FORMAT-SPEC.md §3, its composition-splitting section).

Directory-name constants (``Pool``, ``Composition``, ...) are *not* decided
here — those are repository-root layout constants that belong to the Dedup
Layer's ``repository.py`` (FORMAT-SPEC.md: repo-root-layout's repository-root layout).
This module only computes the id-layering *fragment* relative to whichever
root the caller prepends.
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

    Trusts its fields — neither construction nor ``from_int``
    range-checks them. An out-of-range field either surfaces naturally
    downstream (an over-capacity ``chunk_idx`` raises ``IndexError``; a
    bogus ``bucket_id`` resolves to a missing path and raises
    ``NotFoundError``) or is the job of ``units/verify_reachable.py``'s
    dedicated checks (chunk-map ``mapCrc``, per-chunk fingerprint), which
    are already verify-only and never run on the ordinary read/export
    path. This matters at scale: ``advance`` runs once per real chunk in
    an export — millions of times for a large disk — reconstructing a
    value that is, by construction, already correct.

    Same deliberate ``NamedTuple``-not-``@dataclass(frozen=True)`` exception
    as ``SizeStoreEntry`` — see there for the reasoning.
    """

    stream_id: StreamId
    bucket_id: BucketId
    chunk_idx: ChunkIdx

    @classmethod
    def from_int(cls, data: int) -> ChunkAddress:
        """Unpack a raw ``uint64`` chunk address. See the class's own
        docstring for why this doesn't range-check the result."""
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
        addr_id = (self.stream_id << BUCKET_ID_BIT_NUM) | self.bucket_id
        return (addr_id << CHUNK_BIT_NUM) | self.chunk_idx

    def advance(self, k: int) -> ChunkAddress:
        """Advance by ``k`` chunks, carrying into ``bucket_id`` when
        ``chunk_idx`` would reach ``BUCKET_MAX_CHUNK_NUM`` (8192).
        This is the semantics needed to expand a ``Type::Mapping``
        chunk-map entry's ``map_num * (1 + repeat)`` run of chunks starting
        from this address; it must never be approximated as "add k to the
        raw 64-bit integer", which would misplace the carry at the packed
        field's 16-bit boundary instead of the bucket's real 8192-chunk
        capacity.
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
    """Ancestor directory names for the 10-bit id-layering scheme, from
    outermost to innermost, *excluding* the leaf itself —
    FORMAT-SPEC.md: pool-path-layering: ``id >> 10``, ``>> 20``, ``>> 30``, ... until the quotient is 0.
    Empty for any ``id_ < 1024`` (the common case in modest-sized repositories).
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
    ``<streamID>/[ancestor layers.../]<bucketID>`` (FORMAT-SPEC.md: pool-path-layering).

    For a ``.inf``/``.fgp``/``.ref`` group path, pass the *group's starting*
    bucket id (``bucket_id & ~(GROUP_BUCKET_NUM - 1)``),
    not an individual bucket's own id.
    """
    return "/".join([str(stream_id), *_id_layer_ancestors(bucket_id), str(bucket_id)])


def composition_session_dir(stream_id: StreamId, session_id: SessionId) -> str:
    """Relative path (no ``Composition/`` prefix) to a session's directory:
    ``<streamID>/[ancestor layers.../]<sessionID>.com`` (FORMAT-SPEC.md:
    composition-splitting) — the leaf component carries the ``.com`` suffix rather than being
    a bare decimal number, unlike the Pool leaf.
    """
    return "/".join([str(stream_id), *_id_layer_ancestors(session_id), f"{session_id}.com"])


def composition_path(stream_id: StreamId, session_id: SessionId, sub_id: int) -> str:
    """Full relative path (no ``Composition/`` prefix, no sequence-id
    suffix) to a composition sub-file: ``<sessionDir>/[ancestor
    layers.../]c<subID>`` (FORMAT-SPEC.md: composition-splitting).
    """
    session_dir = composition_session_dir(stream_id, session_id)
    return "/".join([session_dir, *_id_layer_ancestors(sub_id), f"c{sub_id}"])


def split_layer_leaf(layer_path: str) -> tuple[str, str]:
    """Split a ``pool_layer_path()``/``composition_path()`` result into
    ``(dir_part, leaf)`` — the ancestor-layer directory and the final path
    component, which every caller resolving a physical file under it
    needs separately (a leaf-only suffix to append, a directory to
    generation-resolve within). ``dir_part`` is ``""`` when ``layer_path``
    has no ancestor layers (id fits in one layer); pass it to
    ``join_path`` rather than concatenating with ``"/"`` directly, since
    that tolerates the empty case."""
    dir_part, _, leaf = layer_path.rpartition("/")
    return dir_part, leaf


def split_composition_offset(global_offset: int) -> tuple[int, int]:
    """Split a composition-record global offset into ``(sub_id,
    offset_within_subfile)`` — ``sub_id = off >> 24``,
    ``sub_off = off & (16MiB - 1)`` (FORMAT-SPEC.md: composition-splitting)."""
    return global_offset >> SUB_FILE_SIZE_SHIFT, global_offset & (SUB_FILE_SIZE - 1)


def group_start_bucket_id(bucket_id: BucketId) -> BucketId:
    """The starting bucket id of the 1024-bucket group ``bucket_id``
    belongs to: ``bucket_id & ~(GROUP_BUCKET_NUM - 1)``.
    """
    return BucketId(bucket_id & ~(GROUP_BUCKET_NUM - 1))
