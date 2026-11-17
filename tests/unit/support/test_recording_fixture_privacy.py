"""What a committed fixture may hold: ``tests/support/recording/fixture_privacy.py``
checked on synthetic data, then run over every committed fixture.

Every ``tests/fixtures/*.json.gz`` is decoded the way the SDK decodes it (a
whole zstd or aHlT file, every pool chunk a bucket read covers, decrypted
under the vault keys the samples' key strings unwrap) and searched for an
email address outside the reserved domains or a Windows domain SID other
than the synthetic one; a chunk that does not decode fails too, since it
could not be searched. Every ``.gz`` fixture's gzip header carries no file
name or timestamp, and every tar member is owned by root.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import io
import json
import tarfile
from functools import cache
from pathlib import Path
from typing import Any

import pytest
import zstandard

from support.recording.fixture_privacy import (
    SYNTHETIC_DOMAIN_SID,
    find_identifying,
    fixture_findings,
    gzip_findings,
    harvest_vault_keys,
    tar_findings,
)
from support.recording.fixture_store import load_fixture_text
from support.repo_builders import write_bucket
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import FIXED_CHUNK_LENGTH

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
_JSON_FIXTURES = sorted(_FIXTURES.glob("*.json.gz"))
_GZIP_FIXTURES = sorted(_FIXTURES.glob("*.gz"))
_TAR_FIXTURES = sorted(_FIXTURES.glob("*.tar.gz"))

#: A made-up address under a real-looking domain.
_ADDRESS = "jane.doe@corp-mail.com"
_FOREIGN_SID = "S-1-5-21-123456789-1234567890-1234567890"
_VAULT_KEY = bytes(range(32))


def _payload(reads: dict[str, bytes] | None = None, *, listdirs: dict[str, list[str]] | None = None) -> dict[str, Any]:
    return {
        "format": 2,
        "reads": {key: base64.b64encode(data).decode("ascii") for key, data in (reads or {}).items()},
        "sizes": {},
        "exists": {},
        "listdirs": listdirs or {},
        "missing": {},
    }


def _whole_read(path: str) -> str:
    return f"{path}\x000\x00"


def _chunk(text: str) -> bytes:
    return text.encode("utf-8").ljust(FIXED_CHUNK_LENGTH, b"\x00")


def _bucket_bytes(tmp_path: Path, plaintexts: list[bytes], **kwargs: Any) -> bytes:
    path = tmp_path / "bucket.buk"
    write_bucket(path, plaintexts, stream_id=3, bucket_id=7, **kwargs)
    return path.read_bytes()


# --- find_identifying -------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        _ADDRESS,
        "someone@company.de",
        "first.last@sub.company.co",
    ],
)
def test_an_address_under_a_real_looking_domain_is_identifying(text: str) -> None:
    assert find_identifying(f"From: {text}\n".encode()) == {text}


def test_an_address_in_utf16_text_is_identifying() -> None:
    assert _ADDRESS in find_identifying(f"<{_ADDRESS}>".encode("utf-16-le"))


@pytest.mark.parametrize(
    "text",
    [
        "alice@example.com",
        "bob@mail.example.org",
        "carol@example.net",
        "dave@host.example",
        "erin@lab.test",
        "frank@nowhere.invalid",
        "user@1000.service",
        "logo@2x.png",
    ],
)
def test_reserved_domains_and_non_addresses_are_not_identifying(text: str) -> None:
    assert find_identifying(text.encode()) == set()


def test_a_domain_sid_other_than_the_synthetic_one_is_identifying() -> None:
    data = f"owner {_FOREIGN_SID}-1001, group {SYNTHETIC_DOMAIN_SID}-513".encode()
    assert find_identifying(data) == {_FOREIGN_SID}
    assert find_identifying(f"{_FOREIGN_SID}-500".encode("utf-16-le")) == {_FOREIGN_SID}


def test_well_known_sids_are_not_identifying() -> None:
    assert find_identifying(b"S-1-5-18 S-1-5-32-544 S-1-1-0") == set()


# --- fixture_findings -------------------------------------------------------


@pytest.mark.parametrize("compress_type", [CompressType.NONE, CompressType.LZ4, CompressType.ZSTD])
@pytest.mark.parametrize("vault_key", [None, _VAULT_KEY], ids=["plain", "encrypted"])
def test_an_address_inside_a_bucket_chunk_is_found(
    tmp_path: Path, compress_type: CompressType, vault_key: bytes | None
) -> None:
    plaintexts = [_chunk("nothing to see"), _chunk(f"To: {_ADDRESS}")]
    data = _bucket_bytes(tmp_path, plaintexts, vault_key=vault_key, compress_types=[compress_type] * 2)
    payload = _payload({_whole_read("vault/Pool/3/7.buk"): data})

    assert fixture_findings(payload, [_VAULT_KEY]) == [f"vault/Pool/3/7.buk: {_ADDRESS}"]


def test_a_sid_inside_a_layered_bucket_chunk_is_found(tmp_path: Path) -> None:
    data = _bucket_bytes(tmp_path, [_chunk(f"{_FOREIGN_SID}-1104")])
    payload = _payload({_whole_read("Pool/3/0/1/7.buk.2"): data})

    assert fixture_findings(payload, []) == [f"Pool/3/0/1/7.buk.2: {_FOREIGN_SID}"]


def test_a_chunk_no_vault_key_decrypts_is_a_finding(tmp_path: Path) -> None:
    data = _bucket_bytes(tmp_path, [_chunk("secret")], vault_key=_VAULT_KEY)
    payload = _payload({_whole_read("Pool/3/7.buk"): data})

    assert fixture_findings(payload, [bytes(32)]) == ["Pool/3/7.buk: chunk 0 does not decode"]


def test_clean_chunks_have_no_findings(tmp_path: Path) -> None:
    plaintexts = [_chunk(f"alice@example.com {SYNTHETIC_DOMAIN_SID}-1001"), _chunk("")]
    data = _bucket_bytes(tmp_path, plaintexts, vault_key=_VAULT_KEY)
    payload = _payload({_whole_read("Pool/3/7.buk"): data})

    assert fixture_findings(payload, [_VAULT_KEY]) == []


def test_an_address_inside_a_whole_zstd_file_is_found() -> None:
    plain = b"row\n" * 1000 + _ADDRESS.encode("utf-16-le")
    frame = zstandard.ZstdCompressor().compress(plain)
    payload = _payload({_whole_read("db/version.db.zst"): frame})

    assert fixture_findings(payload, []) == [f"db/version.db.zst: {_ADDRESS}"]


def test_an_address_in_a_listed_name_is_found() -> None:
    payload = _payload(listdirs={"share": [f"{_ADDRESS}.pst", "ok.txt"]})

    assert fixture_findings(payload, []) == [f"{_ADDRESS}.pst: {_ADDRESS}.pst"]


# --- archive metadata -------------------------------------------------------


def _tar_gz(path: Path, *, uname: str, gname: str) -> Path:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        member = tarfile.TarInfo("image.raw")
        member.size = 4
        member.uname, member.gname = uname, gname
        archive.addfile(member, io.BytesIO(b"data"))
    path.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))
    return path


def test_a_tar_member_owned_by_a_named_user_is_a_finding(tmp_path: Path) -> None:
    archive = _tar_gz(tmp_path / "a.tar.gz", uname="someone", gname="staff")
    assert tar_findings(archive) == ["image.raw: owned by someone:staff"]


@pytest.mark.parametrize(("uname", "gname"), [("", ""), ("root", "root")])
def test_a_tar_member_owned_by_root_is_clean(tmp_path: Path, uname: str, gname: str) -> None:
    assert tar_findings(_tar_gz(tmp_path / "a.tar.gz", uname=uname, gname=gname)) == []


def test_a_gzip_header_with_a_name_and_timestamp_is_a_finding(tmp_path: Path) -> None:
    path = tmp_path / "image.raw.gz"
    with open(path, "wb") as f, gzip.GzipFile(filename="image.raw", mode="wb", fileobj=f, mtime=1700000000) as gz:
        gz.write(b"data")

    assert gzip_findings(path) == [
        "image.raw.gz: gzip header names a file",
        "image.raw.gz: gzip header carries a timestamp",
    ]


def test_a_bare_gzip_header_is_clean(tmp_path: Path) -> None:
    path = tmp_path / "image.raw.gz"
    path.write_bytes(gzip.compress(b"data", mtime=0))
    assert gzip_findings(path) == []


def test_a_file_that_is_not_gzip_is_a_finding(tmp_path: Path) -> None:
    path = tmp_path / "image.raw.gz"
    path.write_bytes(b"plain")
    assert gzip_findings(path) == ["image.raw.gz: not a gzip file"]


# --- the committed fixtures -------------------------------------------------


@cache
def _decoded(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(load_fixture_text(path))
    return payload


@cache
def _vault_keys() -> tuple[bytes, ...]:
    """Every vault key any committed fixture's key records unwrap: an
    encrypted fixture need not record its own."""
    keys: list[bytes] = []
    for path in _JSON_FIXTURES:
        keys.extend(k for k in asyncio.run(harvest_vault_keys(_decoded(path))) if k not in keys)
    return tuple(keys)


def test_the_committed_fixtures_exist() -> None:
    assert _JSON_FIXTURES and _TAR_FIXTURES


@pytest.mark.parametrize("path", _JSON_FIXTURES, ids=lambda p: p.name)
def test_a_committed_fixture_discloses_nothing(path: Path) -> None:
    assert fixture_findings(_decoded(path), _vault_keys()) == []


@pytest.mark.parametrize("path", _GZIP_FIXTURES, ids=lambda p: p.name)
def test_a_committed_gzip_header_records_no_name_or_time(path: Path) -> None:
    assert gzip_findings(path) == []


@pytest.mark.parametrize("path", _TAR_FIXTURES, ids=lambda p: p.name)
def test_a_committed_tar_is_owned_by_root(path: Path) -> None:
    assert tar_findings(path) == []
