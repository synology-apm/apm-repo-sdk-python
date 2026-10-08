"""Synthetic Pool fixtures shared by the dedup and format tests: ``PLAINTEXTS``
written as stream 5's bucket 0 under ``<root>/Pool``, ``Pool`` constructors
over it, and ``.fgp``/redundancy-bearing bucket writers."""

from __future__ import annotations

import os
import zlib
from pathlib import Path

from support.format_builders import (
    bucket_header_bytes,
    chunk_crc_store_bytes,
    encode_size_store,
    redundancy_blob_bytes,
    sizestore_region_pad,
)
from support.repo_builders import write_bucket
from synology_apm_repo.sdk.dedup.pool import NO_VERIFY, Pool, VerifyPolicy
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.dircache import DirCache
from synology_apm_repo.sdk.storage.local import LocalFsStore

PLAINTEXTS = [((b"chunk-%d-content" % i) * 300)[:4096] for i in range(3)]
assert all(len(p) == 4096 for p in PLAINTEXTS)  # every real chunk is exactly FIXED_CHUNK_LENGTH


CIPHERTEXT_CRC = VerifyPolicy(ciphertext_crc=True)


def plaintext_pool_at(root: Path) -> Pool:
    """An unencrypted ``Pool`` over a freshly written ``PLAINTEXTS`` bucket."""
    write_bucket(root / "Pool" / "5" / "0.buk", PLAINTEXTS, stream_id=5, bucket_id=0, chunk_crc_store=False)
    store = LocalFsStore(root)
    return Pool(store, "Pool", DirCache(store))


def encrypted_pool_at(root: Path) -> tuple[Pool, bytes]:
    """A vault-encrypted ``PLAINTEXTS`` bucket and a ``Pool`` holding its key, with that key."""
    vault_key = os.urandom(32)
    write_bucket(
        root / "Pool" / "5" / "0.buk",
        PLAINTEXTS,
        stream_id=5,
        bucket_id=0,
        vault_key=vault_key,
        chunk_crc_store=False,
    )
    store = LocalFsStore(root)
    pool = Pool(store, "Pool", DirCache(store), vault_key=vault_key)
    return pool, vault_key


def pool_at(root: Path, *, verify: VerifyPolicy = NO_VERIFY) -> Pool:
    """A fresh unencrypted ``Pool`` over whatever buckets ``root`` already holds."""
    store = LocalFsStore(root)
    return Pool(store, "Pool", DirCache(store), verify=verify)


def chunk_address(stream_id: int, bucket_id: int, chunk_idx: int) -> ChunkAddress:
    """A ``ChunkAddress`` from plain ints."""
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx))


def pool_chunk_addr(chunk_idx: int) -> ChunkAddress:
    """Chunk ``chunk_idx`` of the pool fixtures' bucket 0 at stream 5."""
    return chunk_address(5, 0, chunk_idx)


def write_fgp(path: Path, data: bytes) -> None:
    """A fingerprint-group ``.fgp`` file holding ``data`` verbatim."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def write_bucket_with_real_size_store_redundancy(
    path: Path, entries: list[tuple[int, int]], *, corrupt_byte_idx: int | None = None
) -> None:
    """A bucket of random chunk payloads sized by ``entries`` (``(compress
    type, stored size)``) with a valid SizeStore Redundancy blob
    (coverage=256) and a self-consistent ChunkCrcStore trailer, unlike
    ``write_bucket``'s random filler. ``corrupt_byte_idx`` flips one
    SizeStore byte after ``chunk_size_crc`` and the blob were computed: a
    single-window-recoverable corruption."""
    tight = encode_size_store(entries)
    chunk_data = [os.urandom(size) for _type, size in entries]
    chunk_crc_store = chunk_crc_store_bytes([zlib.crc32(chunk) for chunk in chunk_data])
    header = bucket_header_bytes(
        mode=MODE_COMPRESS | MODE_CHUNK_CRC,
        chunk_num=len(entries),
        chunk_size_crc=zlib.crc32(tight),
        crc_of_chunk_crc=zlib.crc32(chunk_crc_store),
    )
    on_disk_tight = bytearray(tight)
    if corrupt_byte_idx is not None:
        on_disk_tight[corrupt_byte_idx] ^= 0xFF
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        header
        + sizestore_region_pad(bytes(on_disk_tight))
        + b"".join(chunk_data)
        + chunk_crc_store
        + redundancy_blob_bytes(tight, coverage=256)
    )
