"""What a committed fixture would disclose: every byte a recorded read holds,
decoded the way the SDK decodes it, searched for identifying text.

A fixture's ``reads`` are searched as stored, and also as decoded: a whole
zstd frame (``version.db.zst``) decompressed, an aHlT file decrypted, and
every pool bucket chunk a ``.buk`` read covers decrypted and decompressed
(``iter_stored_chunks``, ``chunk_plaintext``). Text is searched both as
bytes and as UTF-16LE. Two things count as identifying
(``find_identifying``):

- an email address under a real-looking top-level domain, unless it is under
  a domain reserved for documentation (``example.com``/``.net``/``.org``, the
  ``.example``/``.invalid``/``.test``/``.localhost`` TLDs);
- a Windows domain SID (``S-1-5-21-<a>-<b>-<c>``) other than
  ``SYNTHETIC_DOMAIN_SID``.

``tar_findings`` and ``gzip_findings`` check the disk-image fixtures'
archive metadata: a tar member's owner name, a gzip header's file name and
timestamp.
"""

from __future__ import annotations

import base64
import contextlib
import json
import re
import struct
import tarfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import zstandard

from support.recording.fixture_store import ReplayStore, parse_read_key
from support.recording.sample_constants import (
    OBJSTORE_ENCRYPTED_KEY_STRING,
    OBJSTORE_M365_ENCRYPTED_KEY_STRING,
    PCPS_ENCRYPTED_KEY_STRING,
    VAULT_ENCRYPTED_KEY_STRING,
)
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import BucketIndex, parse_bucket_header, parse_size_store
from synology_apm_repo.sdk.format.compression import (
    ZSTD_FRAME_MAGIC,
    CompressType,
    decompress,
    decompress_zstd_stream,
)
from synology_apm_repo.sdk.format.const import COMPRESS_RESERVED_LENG
from synology_apm_repo.sdk.format.crypto import ahlt_decrypt, decrypt_chunk, is_ahlt
from synology_apm_repo.sdk.format.headers import HEADER_LEN
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId
from synology_apm_repo.sdk.storage.layout import iter_repository_layouts, key_probe_layout

#: The domain part of the one Windows domain SID a fixture may carry; a real
#: one is rewritten to it, its RID kept. Every sub-authority keeps the digit
#: count of the 9-10-10 form, so a same-length rewrite is possible.
SYNTHETIC_DOMAIN_SID = "S-1-5-21-111111111-2222222222-3333333333"

#: A domain SID's domain part (the RID that may follow is not matched).
DOMAIN_SID_RE = re.compile(r"S-1-5-21-\d+-\d+-\d+")

#: An email address; its domain is judged by ``_is_identifying_domain``.
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]{1,64}@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24})")

#: Generic top-level domains an address is plausibly under; with every
#: two-letter (country code) TLD they are the real-looking ones. Anything
#: else after an ``@`` (``user@1000.service``, ``icon@2x.png``) is not an
#: address.
_GENERIC_TLDS = frozenset(
    [
        "com",
        "net",
        "org",
        "edu",
        "gov",
        "mil",
        "int",
        "info",
        "biz",
        "name",
        "pro",
        "mobi",
        "asia",
        "io",
        "ai",
        "app",
        "dev",
        "cloud",
        "online",
        "site",
        "tech",
        "xyz",
    ]
)

#: Domains (and their subdomains) reserved for documentation by RFC 2606/6761.
_RESERVED_DOMAINS = ("example.com", "example.net", "example.org")
_RESERVED_TLDS = frozenset({"example", "invalid", "test", "localhost"})

#: Every recorded sample's key string: the vault keys they unwrap decrypt
#: the encrypted samples' chunks and aHlT files.
KNOWN_KEY_STRINGS = (
    VAULT_ENCRYPTED_KEY_STRING,
    PCPS_ENCRYPTED_KEY_STRING,
    OBJSTORE_ENCRYPTED_KEY_STRING,
    OBJSTORE_M365_ENCRYPTED_KEY_STRING,
)

#: ``.../Pool/<stream>/[<layer>/...]<bucket>.buk[.<seq>]``.
_BUCKET_PATH_RE = re.compile(r"(?:^|/)Pool/(\d+)/(?:\d+/)*(\d+)\.buk(?:\.\d+)?$")

#: An upper bound on a decoded whole file, against a decompression bomb.
_MAX_DECODED_FILE = 1 << 28


def _is_real_looking_domain(domain: str) -> bool:
    tld = domain.rsplit(".", 1)[-1]
    if tld in _RESERVED_TLDS or any(domain == d or domain.endswith(f".{d}") for d in _RESERVED_DOMAINS):
        return False
    return len(tld) == 2 or tld in _GENERIC_TLDS


def _is_identifying_domain(domain: str) -> bool:
    """Whether ``domain``, or a prefix of it ending at a dot, is real-looking:
    the match runs on into whatever follows an address (``a@b.com.pst``)."""
    labels = domain.lower().split(".")
    return any(_is_real_looking_domain(".".join(labels[:end])) for end in range(2, len(labels) + 1))


def _texts(data: bytes) -> Iterator[str]:
    """``data`` as Latin-1 text, then with its UTF-16LE runs of printable
    ASCII (at either byte alignment) turned into single bytes."""
    yield data.decode("latin-1")
    yield re.sub(rb"([\x20-\x7e])\x00", rb"\1", data).decode("latin-1")


def find_identifying(data: bytes) -> set[str]:
    """Every identifying string in ``data`` (see the module docstring)."""
    found: set[str] = set()
    for text in _texts(data):
        found.update(m.group(0) for m in _EMAIL_RE.finditer(text) if _is_identifying_domain(m.group(1)))
        found.update(sid for sid in DOMAIN_SID_RE.findall(text) if sid != SYNTHETIC_DOMAIN_SID)
    return found


@dataclass(frozen=True, slots=True)
class StoredChunk:
    """One whole pool chunk inside a recorded ``.buk`` read.

    Attributes:
        read_key: The ``reads`` key holding it.
        start: Its offset within that read's bytes.
        stored: Its stored (compressed, maybe encrypted) bytes.
        compress_type: Its SizeStore compression type.
        address: Its chunk address, which an encrypted chunk's IV derives from.
        encrypted: Whether its bucket is vault-encrypted.
    """

    read_key: str
    start: int
    stored: bytes
    compress_type: CompressType
    address: ChunkAddress
    encrypted: bool


def iter_stored_chunks(reads: dict[str, bytes]) -> Iterator[StoredChunk]:
    """Every whole, non-empty chunk any ``.buk`` read in ``reads`` covers,
    located through that bucket's recorded header read (a bucket read
    without one has no chunk layout to locate by and is skipped)."""
    by_bucket: dict[str, list[tuple[str, int, bytes]]] = {}
    for key, data in reads.items():
        path, offset, _length = parse_read_key(key)
        if _BUCKET_PATH_RE.search(path):
            by_bucket.setdefault(path, []).append((key, offset, data))
    for path, bucket_reads in by_bucket.items():
        match = _BUCKET_PATH_RE.search(path)
        assert match is not None
        head = next((data for _key, offset, data in bucket_reads if offset == 0 and len(data) >= HEADER_LEN), None)
        if head is None:
            continue
        header = parse_bucket_header(head)
        if header.is_compressed:
            if len(head) < COMPRESS_RESERVED_LENG:
                continue
            index = parse_size_store(head[HEADER_LEN:COMPRESS_RESERVED_LENG], header.chunk_num)
        else:
            index = BucketIndex.uncompressed(header.chunk_num)
        stream_id, bucket_id = int(match.group(1)), int(match.group(2))
        for key, offset, data in bucket_reads:
            end = offset + len(data)
            for chunk_idx in range(len(index)):
                chunk_offset, length = index.locator(chunk_idx)
                if length == 0 or chunk_offset < offset or chunk_offset + length > end:
                    continue
                start = chunk_offset - offset
                yield StoredChunk(
                    read_key=key,
                    start=start,
                    stored=data[start : start + length],
                    compress_type=index[chunk_idx].compress_type,
                    address=ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx)),
                    encrypted=header.is_vault_encrypted,
                )


def chunk_plaintext(chunk: StoredChunk, vault_keys: Iterable[bytes]) -> tuple[bytes, bytes | None] | None:
    """``chunk``'s 4096 plaintext bytes and the vault key that decrypted it
    (``None`` for an unencrypted bucket), or ``None`` when it does not
    decode -- for an encrypted chunk, under none of ``vault_keys``."""
    if not chunk.encrypted:
        try:
            return decompress(chunk.compress_type, chunk.stored), None
        except ApmRepoError:
            return None
    for vault_key in vault_keys:
        try:
            return decompress(chunk.compress_type, decrypt_chunk(vault_key, chunk.address, chunk.stored)), vault_key
        except ApmRepoError:
            continue
    return None


def decoded_file_views(data: bytes, vault_keys: Iterable[bytes]) -> list[bytes]:
    """``data``, a whole file, as it decodes: decrypted when it is aHlT (under the
    first of ``vault_keys`` that yields a SQLite file or a zstd frame), then
    decompressed when it is one zstd frame. Empty for anything else."""
    views: list[bytes] = []
    if is_ahlt(data):
        for vault_key in vault_keys:
            plain = ahlt_decrypt(data, vault_key)
            if plain.startswith((b"SQLite format 3\x00", ZSTD_FRAME_MAGIC)):
                views.append(plain)
                data = plain
                break
    if data.startswith(ZSTD_FRAME_MAGIC):
        with contextlib.suppress(ApmRepoError, zstandard.ZstdError):
            views.append(decompress_zstd_stream(data, max_output_size=_MAX_DECODED_FILE))
    return views


async def harvest_vault_keys(payload: dict[str, Any]) -> list[bytes]:
    """Every vault key a ``KNOWN_KEY_STRINGS`` key unwraps from the key
    records ``payload`` (a decoded fixture) recorded."""
    store = ReplayStore(json.dumps(payload), strict=False)
    layouts = []
    try:
        async for layout in iter_repository_layouts(store):
            layouts.append(layout)  # noqa: PERF401 -- keeps what was found before an early end below
    except ApmRepoError:
        pass  # a partial recording ends the walk early
    keys: list[bytes] = []
    for layout in layouts:
        for key_string in KNOWN_KEY_STRINGS:
            try:
                vault_key = await KeyMaterial.from_key_string(key_string).resolve_vault_key(
                    store, key_probe_layout(layout)
                )
            except ApmRepoError:
                continue  # another sample's key record, or none recorded
            if vault_key is not None and vault_key not in keys:
                keys.append(vault_key)
    return keys


def fixture_findings(payload: dict[str, Any], vault_keys: Iterable[bytes]) -> list[str]:
    """Every identifying string ``payload`` (a decoded fixture) discloses,
    and every chunk it holds that does not decode under ``vault_keys``, as
    ``"<read path>: <finding>"`` lines."""
    vault_keys = list(vault_keys)
    reads = {key: base64.b64decode(b64) for key, b64 in payload["reads"].items()}
    findings: set[str] = set()
    for key, data in reads.items():
        path, offset, length = parse_read_key(key)
        whole_file = offset == 0 and length is None
        for view in [data, *(decoded_file_views(data, vault_keys) if whole_file else [])]:
            findings.update(f"{path}: {hit}" for hit in find_identifying(view))
    for chunk in iter_stored_chunks(reads):
        path = parse_read_key(chunk.read_key)[0]
        decoded = chunk_plaintext(chunk, vault_keys)
        if decoded is None:
            findings.add(f"{path}: chunk {chunk.address.chunk_idx} does not decode")
            continue
        findings.update(f"{path}: {hit}" for hit in find_identifying(decoded[0]))
    for text in (*payload["sizes"], *payload["exists"], *payload["listdirs"]):
        findings.update(f"{text}: {hit}" for hit in find_identifying(text.encode("utf-8")))
    for entries in payload["listdirs"].values():
        for entry in entries:
            findings.update(f"{entry}: {hit}" for hit in find_identifying(entry.encode("utf-8")))
    return sorted(findings)


def tar_findings(path: Path) -> list[str]:
    """Every member of the tar archive at ``path`` owned by a user or group
    name other than ``root`` (or none)."""
    with tarfile.open(path) as archive:
        return [
            f"{member.name}: owned by {member.uname or member.uid}:{member.gname or member.gid}"
            for member in archive.getmembers()
            if member.uname not in ("", "root") or member.gname not in ("", "root")
        ]


_GZIP_FNAME = 0x08


def gzip_findings(path: Path) -> list[str]:
    """What the gzip header of ``path`` records about where and when it was
    made: a file name, or a modification time."""
    header = path.read_bytes()[:10]
    if header[:2] != b"\x1f\x8b":
        return [f"{path.name}: not a gzip file"]
    (mtime,) = struct.unpack("<I", header[4:8])
    findings = []
    if header[3] & _GZIP_FNAME:
        findings.append(f"{path.name}: gzip header names a file")
    if mtime:
        findings.append(f"{path.name}: gzip header carries a timestamp")
    return findings
