"""Unit tests for ``synology_apm_repo.cli.commands.dump`` against
synthetic ``.buk``/composition files under ``tmp_path``;
``TestDumpProfile`` covers how the store is resolved: ``--profile`` against
an in-memory fake ``ObjectStore``, and a local path or profile that fails.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from support.cli import invoke
from support.fakes import faithful_to
from support.format_builders import (
    chunk_addr_int,
    composition_header_bytes,
)
from support.repo_builders import (
    composition_record_bytes,
    filler_bucket_bytes,
    uncompressed_bucket_bytes,
)
from synology_apm_repo.cli.commands import dump as dump_module
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import RECORD_HEAD_LENGTH
from synology_apm_repo.sdk.profiles.errors import ProfileNotFoundError
from synology_apm_repo.sdk.storage.base import Entry, ObjectStore


def _mapping_entry(*, file_chunk_idx: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    tail = (map_num << 16) | repeat
    return bytes([0x00]) + file_chunk_idx.to_bytes(7, "big") + addr_int.to_bytes(8, "big") + tail.to_bytes(4, "big")


def _zero_entry(*, file_chunk_idx: int, zero_num: int) -> bytes:
    return bytes([0x01]) + file_chunk_idx.to_bytes(7, "big") + (0).to_bytes(8, "big") + zero_num.to_bytes(4, "big")


@pytest.fixture
def bucket_path(tmp_path: Path) -> Path:
    entries = [(CompressType.NONE, 0), (CompressType.ZSTD, 100), (CompressType.LZ4, 50)]
    path = tmp_path / "0.buk"
    path.write_bytes(filler_bucket_bytes(entries))
    return path


def _build_map_array() -> bytes:
    return _mapping_entry(file_chunk_idx=0, addr_int=chunk_addr_int(5, 3, 0), map_num=1) + _zero_entry(
        file_chunk_idx=1, zero_num=1
    )


@pytest.fixture
def composition_path(tmp_path: Path) -> Path:
    record = composition_record_bytes(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes() + record)
    return path


@pytest.fixture
def composition_header_only_path(tmp_path: Path) -> Path:
    """Header, no records at all."""
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes())
    return path


@pytest.fixture
def composition_two_records_path(tmp_path: Path) -> Path:
    record = composition_record_bytes(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes() + record + record)
    return path


@pytest.fixture
def composition_large_map_path(tmp_path: Path) -> Path:
    """One record whose chunk-map array (13108 20-byte entries = 262160
    bytes) is past ``should_thread_chunk_map_crc``'s 256 KiB threshold."""
    map_array = _zero_entry(file_chunk_idx=0, zero_num=1) * 13108
    record = composition_record_bytes(map_array=map_array)
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes() + record)
    return path


@pytest.fixture
def composition_trailing_garbage_path(tmp_path: Path) -> Path:
    """One valid record, then bytes that fail ``parse_record_head`` (wrong
    magic) instead of a clean end-of-file."""
    record = composition_record_bytes(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes() + record + b"\x00" * RECORD_HEAD_LENGTH)
    return path


@pytest.fixture
def composition_garbage_from_the_start_path(tmp_path: Path) -> Path:
    """No composition-header magic, and the very first record head also
    fails to parse — nothing at all was walked successfully."""
    path = tmp_path / "c0"
    path.write_bytes(b"\x00" * RECORD_HEAD_LENGTH)
    return path


@pytest.fixture
def composition_no_header_but_valid_record_path(tmp_path: Path) -> Path:
    """No composition-header magic (a real, if rare, non-``subID=0``
    file), but the bytes at offset 0 parse as a record."""
    record = composition_record_bytes(map_array=_build_map_array())
    path = tmp_path / "1.com"
    path.write_bytes(record)
    return path


@pytest.fixture
def composition_bad_header_crc_path(tmp_path: Path) -> Path:
    header = bytearray(composition_header_bytes())
    header[8] ^= 0xFF  # corrupt SUB_FILE_SIZE without recomputing the trailing CRC
    path = tmp_path / "c0"
    path.write_bytes(bytes(header) + composition_record_bytes(map_array=_build_map_array()))
    return path


@pytest.fixture
def composition_bad_map_crc_path(tmp_path: Path) -> Path:
    """A record whose map array no longer matches its head's ``map_crc``,
    while the head (and its own CRC) still parses."""
    record = bytearray(composition_record_bytes(map_array=_build_map_array()))
    # +5 lands in the first entry's file_chunk_idx, not its kind-tag byte,
    # so the entry still decodes as MAPPING.
    map_off = RECORD_HEAD_LENGTH + 5
    record[map_off] ^= 0xFF
    path = tmp_path / "c0"
    path.write_bytes(composition_header_bytes() + bytes(record))
    return path


@pytest.fixture
def in_tmp_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Run from ``tmp_path``, so a test passes its file by bare name and the
    rendered ``file`` line holds no temporary directory."""
    monkeypatch.chdir(tmp_path)


class TestDumpBucket:
    def test_human_output_shows_expected_fields(self, bucket_path: Path, in_tmp_path: None) -> None:
        assert invoke(["dump", "bucket", bucket_path.name]).stdout == snapshot("""\
file           : 0.buk
major/minor    : 3/0
mode           : 0x03 (compress, chunk_crc)
chunk_num      : 3
chunk_size_crc : 0xcba3f243
sizestore      : LZ4=1 NONE=1 ZSTD=1
expected_size  : 20668
actual_size    : 20668  (OK)
""")

    def test_trace_logs_the_reads_it_makes_on_stderr(self, bucket_path: Path) -> None:
        result = invoke(["--trace", "dump", "bucket", str(bucket_path)])
        trace_lines = [line for line in result.stderr.splitlines() if line.startswith("[trace]")]
        assert trace_lines
        assert all("0.buk" in line for line in trace_lines)
        assert "[trace]" not in result.stdout

    def test_an_uncompressed_bucket_shows_no_size_check(self, tmp_path: Path, in_tmp_path: None) -> None:
        (tmp_path / "0.buk").write_bytes(uncompressed_bucket_bytes(4))
        assert invoke(["dump", "bucket", "0.buk"]).stdout == snapshot("""\
file           : 0.buk
major/minor    : 3/0
mode           : 0x00 (none)
chunk_num      : 4
chunk_size_crc : 0x00000000
sizestore      : NONE=4
expected_size  : n/a (uncompressed layout)
actual_size    : 16448
""")

    def test_json_output_matches_expected_shape(self, bucket_path: Path) -> None:
        result = invoke(["--json", "dump", "bucket", str(bucket_path)])
        report = json.loads(result.stdout)
        assert report["chunk_num"] == 3
        assert report["size_check_ok"] is True
        assert report["mode_flags"] == ["compress", "chunk_crc"]

    def test_chunk_option_human_output_shows_locator_line(self, bucket_path: Path, in_tmp_path: None) -> None:
        result = invoke(["dump", "bucket", bucket_path.name, "--chunk", "1"])
        assert result.stdout == snapshot("""\
file           : 0.buk
major/minor    : 3/0
mode           : 0x03 (compress, chunk_crc)
chunk_num      : 3
chunk_size_crc : 0xcba3f243
sizestore      : LZ4=1 NONE=1 ZSTD=1
expected_size  : 20668
actual_size    : 20668  (OK)
chunk[1]       : compress=ZSTD stored_len=100 effective_len=100 offset=20480 \n\
length=100
""")

    def test_chunk_option_shows_one_locator(self, bucket_path: Path) -> None:
        result = invoke(["--json", "dump", "bucket", str(bucket_path), "--chunk", "1"])
        report = json.loads(result.stdout)
        assert report["chunk"]["index"] == 1
        assert report["chunk"]["compress_type"] == "ZSTD"
        assert report["chunk"]["stored_len"] == 100

    def test_chunk_out_of_range_fails(self, bucket_path: Path) -> None:
        result = invoke(["dump", "bucket", str(bucket_path), "--chunk", "99"], exit_code=1)
        assert result.stdout == ""
        assert result.stderr == snapshot("error: chunk 99 out of range [0, 3)\n")


class TestDumpComposition:
    def test_walks_the_header_and_one_record(self, composition_path: Path, in_tmp_path: None) -> None:
        assert invoke(["dump", "composition", composition_path.name]).stdout == snapshot("""\
file : c0
header : major=1 minor=1
head_off=64         status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0
""")

    def test_verify_map_reports_ok(self, composition_path: Path, in_tmp_path: None) -> None:
        assert invoke(["dump", "composition", composition_path.name, "--verify-map"]).stdout == snapshot("""\
file : c0
header : major=1 minor=1
head_off=64         status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0  \n\
map_crc=OK
""")

    def test_json_shape(self, composition_path: Path) -> None:
        result = invoke(["--json", "dump", "composition", str(composition_path)])
        report = json.loads(result.stdout)
        assert report["header"] == {"major": 1, "minor": 1}
        assert len(report["records"]) == 1
        assert report["records"][0]["map_num"] == 2

    def test_explicit_offset_overrides_the_computed_start(self, composition_path: Path) -> None:
        # The same value the header-derived default picks.
        result = invoke(["dump", "composition", str(composition_path), "--offset", "64"])
        assert result.stdout == invoke(["dump", "composition", str(composition_path)]).stdout

    def test_verify_map_reports_fail_on_corrupted_map_bytes(
        self, composition_bad_map_crc_path: Path, in_tmp_path: None
    ) -> None:
        result = invoke(["dump", "composition", composition_bad_map_crc_path.name, "--verify-map"])
        assert result.stdout == snapshot("""\
file : c0
header : major=1 minor=1
head_off=64         status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0  \n\
map_crc=FAIL
""")

    def test_bad_header_crc_fails(self, composition_bad_header_crc_path: Path) -> None:
        result = invoke(["dump", "composition", str(composition_bad_header_crc_path)], exit_code=1)
        assert result.stdout == ""
        assert result.stderr == snapshot("error: header CRC mismatch: computed 0x97309e15 != stored 0xf661649f\n")

    def test_no_records_prints_hint(self, composition_header_only_path: Path, in_tmp_path: None) -> None:
        assert invoke(["dump", "composition", composition_header_only_path.name]).stdout == snapshot("""\
file : c0
header : major=1 minor=1
(no records)
""")

    def test_limit_reached_with_more_records_prints_hint(
        self, composition_two_records_path: Path, in_tmp_path: None
    ) -> None:
        result = invoke(["dump", "composition", composition_two_records_path.name, "--limit", "1"])
        assert result.stdout == snapshot("""\
file : c0
header : major=1 minor=1
head_off=64         status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0
(stopped at --limit=1; more records follow at offset 196)
""")

    def test_a_failed_record_after_a_good_one_stops_but_keeps_it(
        self, composition_trailing_garbage_path: Path, in_tmp_path: None
    ) -> None:
        result = invoke(["dump", "composition", composition_trailing_garbage_path.name])
        assert result.stdout == snapshot("""\
(stopped walking at offset 196: bad magic b'\\x00\\x00', expected b'Mu' \n\
[spec=FORMAT-SPEC.md: RecordHead])
file : c0
header : major=1 minor=1
head_off=64         status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0
""")

    def test_nothing_parses_at_all_fails(self, composition_garbage_from_the_start_path: Path) -> None:
        result = invoke(["dump", "composition", str(composition_garbage_from_the_start_path)], exit_code=1)
        assert result.stdout == ""
        assert result.stderr == "error: bad magic b'\\x00\\x00', expected b'Mu'\n"

    def test_no_header_magic_but_a_real_record_at_offset_0_still_parses(
        self, composition_no_header_but_valid_record_path: Path, in_tmp_path: None
    ) -> None:
        result = invoke(["dump", "composition", composition_no_header_but_valid_record_path.name])
        # No header line: none was found.
        assert result.stdout == snapshot("""\
file : 1.com
head_off=0          status=COMPLETE   map_num=2        mode=0x0001 attr_leng=0
""")


class TestDumpChunkmap:
    def test_entries_decode_correctly(self, composition_path: Path) -> None:
        result = invoke(["--json", "dump", "chunkmap", str(composition_path), "--offset", "64", "--verify-map"])
        report = json.loads(result.stdout)
        assert report["map_num"] == 2
        assert report["map_crc_ok"] is True
        assert report["entries"][0]["kind"] == "MAPPING"
        assert report["entries"][0]["addr"] == {"stream_id": 5, "bucket_id": 3, "chunk_idx": 0}
        assert report["entries"][1]["kind"] == "ZERO"
        assert report["entries"][1].get("addr") is None  # ZERO entries have no addr

    def test_human_output_shows_addr(self, composition_path: Path, in_tmp_path: None) -> None:
        assert invoke(["dump", "chunkmap", composition_path.name, "--offset", "64"]).stdout == snapshot("""\
file     : c0
head_off : 64
map_num  : 2
  [   0] MAPPING  file_offset=0          end=4096       inherit=False \n\
addr=(stream=5,bucket=3,chunk=0)  map_num=1 repeat=0
  [   1] ZERO     file_offset=4096       end=8192       inherit=False addr=-    \n\
map_num=1 repeat=0
""")

    def test_offset_is_required(self, composition_path: Path) -> None:
        result = invoke(["dump", "chunkmap", str(composition_path)], exit_code=2)
        assert result.stdout == ""
        assert "Missing option '--offset'." in result.stderr

    def test_human_output_with_verify_shows_the_map_crc_line(self, composition_path: Path, in_tmp_path: None) -> None:
        result = invoke(["dump", "chunkmap", composition_path.name, "--offset", "64", "--verify-map"])
        assert result.stdout == snapshot("""\
file     : c0
head_off : 64
map_num  : 2
map_crc  : OK
  [   0] MAPPING  file_offset=0          end=4096       inherit=False \n\
addr=(stream=5,bucket=3,chunk=0)  map_num=1 repeat=0
  [   1] ZERO     file_offset=4096       end=8192       inherit=False addr=-    \n\
map_num=1 repeat=0
""")

    def test_verify_reports_fail_on_corrupted_map(self, composition_bad_map_crc_path: Path) -> None:
        result = invoke(
            [
                "--json",
                "dump",
                "chunkmap",
                str(composition_bad_map_crc_path),
                "--offset",
                "64",
                "--verify-map",
            ]
        )
        report = json.loads(result.stdout)
        assert report["map_crc_ok"] is False

    def test_limit_smaller_than_map_num_shows_hint(self, composition_path: Path, in_tmp_path: None) -> None:
        result = invoke(["dump", "chunkmap", composition_path.name, "--offset", "64", "--limit", "1"])
        assert result.stdout == snapshot("""\
file     : c0
head_off : 64
map_num  : 2
  [   0] MAPPING  file_offset=0          end=4096       inherit=False \n\
addr=(stream=5,bucket=3,chunk=0)  map_num=1 repeat=0
(showing 1 of 2 entries — pass --limit to see more)
""")

    def test_verify_on_a_large_map_array_still_reports_ok(
        self, composition_large_map_path: Path, in_tmp_path: None
    ) -> None:
        # Past should_thread_chunk_map_crc's threshold: the threaded CRC branch.
        args = ["dump", "chunkmap", composition_large_map_path.name, "--offset", "64", "--verify-map", "--limit", "1"]
        assert invoke(args).stdout == snapshot("""\
file     : c0
head_off : 64
map_num  : 13108
map_crc  : OK
  [   0] ZERO     file_offset=0          end=4096       inherit=False addr=-    \n\
map_num=1 repeat=0
(showing 1 of 13108 entries — pass --limit to see more)
""")

    def test_offset_pointing_at_garbage_fails_cleanly_not_a_traceback(
        self, composition_garbage_from_the_start_path: Path
    ) -> None:
        # inspect_chunk_map raises on a bad record head (no
        # stopped_with_error, unlike the composition walk); _resolved_store's
        # handler turns it into a clean exit-1 error.
        result = invoke(
            ["dump", "chunkmap", str(composition_garbage_from_the_start_path), "--offset", "0"], exit_code=1
        )
        assert result.stdout == ""
        assert result.stderr == snapshot("error: bad magic b'\\x00\\x00', expected b'Mu'\n")
        assert result.exception is None or isinstance(result.exception, SystemExit)


@faithful_to(ObjectStore)
class _FakeObjectStore:
    """An in-memory ``ObjectStore`` for ``--profile``; ``closed`` records
    whether ``close()`` ran."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.closed = False

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        data = self._files[path]
        end = len(data) if length is None else offset + length
        return data[offset:end]

    async def size(self, path: str) -> int:
        return len(self._files[path])

    async def exists(self, path: str) -> bool:
        return path in self._files

    async def listdir(self, path: str) -> list[Entry]:
        return [Entry(name, len(data)) for name, data in sorted(self._files.items())]

    async def close(self) -> None:
        self.closed = True


class TestDumpProfile:
    def test_bucket_via_profile_returns_expected_shape(
        self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _FakeObjectStore({"0.buk": bucket_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            assert name == "demo"
            return store

        monkeypatch.setattr(dump_module, "store_from_profile", _fake_resolve)
        result = invoke(["--json", "dump", "bucket", "--profile", "demo", "0.buk"])
        report = json.loads(result.stdout)
        assert report["path"] == "0.buk"  # the argument as typed, not the store-internal rel
        assert report["chunk_num"] == 3
        assert report["size_check_ok"] is True

    def test_composition_via_profile_returns_expected_shape(
        self, composition_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _FakeObjectStore({"c0": composition_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            return store

        monkeypatch.setattr(dump_module, "store_from_profile", _fake_resolve)
        result = invoke(["--json", "dump", "composition", "--profile", "demo", "c0"])
        report = json.loads(result.stdout)
        assert report["path"] == "c0"
        assert report["header"] == {"major": 1, "minor": 1}
        assert len(report["records"]) == 1

    def test_chunkmap_via_profile_returns_expected_shape(
        self, composition_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _FakeObjectStore({"c0": composition_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            return store

        monkeypatch.setattr(dump_module, "store_from_profile", _fake_resolve)
        result = invoke(["--json", "dump", "chunkmap", "--profile", "demo", "c0", "--offset", "64", "--verify-map"])
        report = json.loads(result.stdout)
        assert report["path"] == "c0"
        assert report["map_num"] == 2
        assert report["map_crc_ok"] is True

    def test_without_profile_never_calls_store_from_profile(
        self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _boom(name: str) -> _FakeObjectStore:
            raise AssertionError("store_from_profile must not be called without --profile")

        monkeypatch.setattr(dump_module, "store_from_profile", _boom)
        invoke(["dump", "bucket", str(bucket_path)])

    def test_profile_store_is_closed_after_use(self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        store = _FakeObjectStore({"0.buk": bucket_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            return store

        monkeypatch.setattr(dump_module, "store_from_profile", _fake_resolve)
        invoke(["dump", "bucket", "--profile", "demo", "0.buk"])
        assert store.closed is True

    def test_missing_local_directory_fails_cleanly_not_a_traceback(self, tmp_path: Path) -> None:
        # LocalFsStore's __init__ raises NotFoundError before
        # _resolved_store yields; its handler must still turn it into a
        # clean exit-1 error.
        missing = tmp_path / "no-such-dir" / "0.buk"
        result = invoke(["dump", "bucket", str(missing)], exit_code=1)
        assert result.stdout == ""
        assert result.stderr == "error: store root is not a directory\n"
        assert result.exception is None or isinstance(result.exception, SystemExit)

    def test_unknown_profile_fails_cleanly_not_a_traceback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolve(name: str) -> _FakeObjectStore:
            raise ProfileNotFoundError(f"no such profile: {name!r}")

        monkeypatch.setattr(dump_module, "store_from_profile", _fake_resolve)
        result = invoke(["dump", "bucket", "--profile", "nope", "0.buk"], exit_code=1)
        assert result.stdout == ""
        assert result.stderr == "error: no such profile: 'nope'\n"
        assert result.exception is None or isinstance(result.exception, SystemExit)
