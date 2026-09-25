"""Unit tests for ``synology_apm_repo.cli.commands.dump`` — synthetic
``.buk``/composition fixtures (same byte-construction approach as
``tests/unit/sdk/test_format_bucket.py``/``test_format_composition.py``/
``test_format_chunkmap.py``, written to real ``tmp_path`` files for the
local (non-``--profile``) case, since that's what ``dump`` operates on
without a store of its own; ``TestDumpProfile`` below covers the
``--profile`` case against an in-memory fake ``ObjectStore`` instead).
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.commands import dump as dump_module
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.errors import ProfileNotFoundError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import RECORD_HEAD_LENGTH, SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import BucketId, ChunkIdx, StreamId

runner = CliRunner()


def _encode_size_store(entries: list[tuple[int, int]]) -> bytes:
    n = len(entries)
    tight_len = (n * 15 + 7) >> 3
    buf = bytearray(tight_len + 4)
    for idx, (type_value, size) in enumerate(entries):
        bit_off = idx * 15
        byte_off = bit_off >> 3
        bit_shift = 17 - (bit_off & 7)
        blob = (type_value << 12) | size
        window = int.from_bytes(buf[byte_off : byte_off + 4], "big")
        window |= (blob << bit_shift) & 0xFFFFFFFF
        buf[byte_off : byte_off + 4] = window.to_bytes(4, "big")
    return bytes(buf[:tight_len])


def _build_bucket(entries: list[tuple[CompressType, int]]) -> bytes:
    chunk_num = len(entries)
    tight = _encode_size_store([(e[0].value, e[1]) for e in entries])
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF

    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", chunk_num)
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    chunk_data = b""
    for ctype, stored_len in entries:
        eff = 4096 if ctype is CompressType.NONE else stored_len
        chunk_data += os.urandom(eff)
    trailer_len = 4 * chunk_num + redundancy_size((chunk_num * 15 + 7) >> 3, 256)
    trailer = os.urandom(trailer_len)
    return bytes(header) + sizestore_region + chunk_data + trailer


def _build_composition_header() -> bytes:
    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", SUB_FILE_SIZE)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    return bytes(header)


def _addr_int(stream_id: int, bucket_id: int, chunk_idx: int) -> int:
    return ChunkAddress(StreamId(stream_id), BucketId(bucket_id), ChunkIdx(chunk_idx)).to_int()


def _mapping_entry(*, file_chunk_idx: int, addr_int: int, map_num: int, repeat: int = 0) -> bytes:
    tail = (map_num << 16) | repeat
    return bytes([0x00]) + file_chunk_idx.to_bytes(7, "big") + addr_int.to_bytes(8, "big") + tail.to_bytes(4, "big")


def _zero_entry(*, file_chunk_idx: int, zero_num: int) -> bytes:
    return bytes([0x01]) + file_chunk_idx.to_bytes(7, "big") + (0).to_bytes(8, "big") + zero_num.to_bytes(4, "big")


def _build_record(*, map_array: bytes, attr: bytes = b"") -> bytes:
    map_num = len(map_array) // 20
    map_crc = zlib.crc32(map_array) & 0xFFFFFFFF
    attr_crc = zlib.crc32(attr) & 0xFFFFFFFF
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = map_num.to_bytes(8, "big")
    head[14:18] = map_crc.to_bytes(4, "big")
    head[18:20] = (0x0001).to_bytes(2, "big")  # Redundancy bit set
    head[20:24] = len(attr).to_bytes(4, "big")
    head[24:28] = attr_crc.to_bytes(4, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    trailer_len = redundancy_size(len(map_array), 8192)
    trailer = os.urandom(trailer_len)
    return bytes(head) + map_array + attr + trailer


@pytest.fixture
def bucket_path(tmp_path: Path) -> Path:
    entries = [(CompressType.NONE, 0), (CompressType.ZSTD, 100), (CompressType.LZ4, 50)]
    path = tmp_path / "0.buk"
    path.write_bytes(_build_bucket(entries))
    return path


def _build_map_array() -> bytes:
    return _mapping_entry(file_chunk_idx=0, addr_int=_addr_int(5, 3, 0), map_num=1) + _zero_entry(
        file_chunk_idx=1, zero_num=1
    )


@pytest.fixture
def composition_path(tmp_path: Path) -> Path:
    record = _build_record(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header() + record)
    return path


@pytest.fixture
def composition_header_only_path(tmp_path: Path) -> Path:
    """Header, no records at all — the walk's ``pos < file_size`` loop
    condition is false on its very first check."""
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header())
    return path


@pytest.fixture
def composition_two_records_path(tmp_path: Path) -> Path:
    record = _build_record(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header() + record + record)
    return path


@pytest.fixture
def composition_large_map_path(tmp_path: Path) -> Path:
    """One record whose chunk-map array is past
    ``should_thread_chunk_map_crc``'s 256 KiB threshold (13108 20-byte
    entries = 262160 bytes) — the one entry actually rendered is
    ``--limit``-bounded regardless of ``map_num``, so this stays cheap to
    parse; only the CRC pass itself needs the full array."""
    map_array = _zero_entry(file_chunk_idx=0, zero_num=1) * 13108
    record = _build_record(map_array=map_array)
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header() + record)
    return path


@pytest.fixture
def composition_trailing_garbage_path(tmp_path: Path) -> Path:
    """One valid record, then bytes that fail ``parse_record_head`` (wrong
    magic) instead of a clean end-of-file — the walk must stop there, not
    raise past a record it already parsed."""
    record = _build_record(map_array=_build_map_array())
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header() + record + b"\x00" * RECORD_HEAD_LENGTH)
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
    file), but the bytes starting at offset 0 still parse as a real
    record — distinct from ``composition_garbage_from_the_start_path``,
    where the record parse also fails."""
    record = _build_record(map_array=_build_map_array())
    path = tmp_path / "1.com"
    path.write_bytes(record)
    return path


@pytest.fixture
def composition_bad_header_crc_path(tmp_path: Path) -> Path:
    header = bytearray(_build_composition_header())
    header[8] ^= 0xFF  # corrupt SUB_FILE_SIZE without recomputing the trailing CRC
    path = tmp_path / "c0"
    path.write_bytes(bytes(header) + _build_record(map_array=_build_map_array()))
    return path


@pytest.fixture
def composition_bad_map_crc_path(tmp_path: Path) -> Path:
    """A record whose on-disk map array no longer matches its head's
    ``map_crc`` — same on-disk shape ``--verify-map``/``chunkmap --verify``
    check against, corrupted after the fact so the record head itself
    (and its own CRC) still parses fine."""
    record = bytearray(_build_record(map_array=_build_map_array()))
    # chunk_map_array_offset(head_off) - head_off, within one record's own
    # bytes; +5 lands inside the first entry's file_chunk_idx field, not
    # its leading kind-tag byte, so it still parses as a MAPPING entry —
    # this must corrupt the CRC without also breaking entry decoding.
    map_off = RECORD_HEAD_LENGTH + 5
    record[map_off] ^= 0xFF
    path = tmp_path / "c0"
    path.write_bytes(_build_composition_header() + bytes(record))
    return path


class TestDumpBucket:
    def test_human_output_shows_expected_fields(self, bucket_path: Path) -> None:
        result = runner.invoke(app, ["dump", "bucket", str(bucket_path)])
        assert result.exit_code == 0, result.output
        assert "major/minor    : 3/0" in result.output
        assert "chunk_num      : 3" in result.output
        assert "NONE=1" in result.output
        assert "ZSTD=1" in result.output
        assert "LZ4=1" in result.output
        assert "OK" in result.output

    def test_json_output_matches_expected_shape(self, bucket_path: Path) -> None:
        result = runner.invoke(app, ["--json", "dump", "bucket", str(bucket_path)])
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["chunk_num"] == 3
        assert report["size_check_ok"] is True
        assert report["mode_flags"] == ["compress", "chunk_crc"]

    def test_chunk_option_human_output_shows_locator_line(self, bucket_path: Path) -> None:
        result = runner.invoke(app, ["dump", "bucket", str(bucket_path), "--chunk", "1"])
        assert result.exit_code == 0, result.output
        assert "chunk[1]" in result.output
        assert "compress=ZSTD" in result.output

    def test_chunk_option_shows_one_locator(self, bucket_path: Path) -> None:
        result = runner.invoke(app, ["--json", "dump", "bucket", str(bucket_path), "--chunk", "1"])
        report = json.loads(result.stdout)
        assert report["chunk"]["index"] == 1
        assert report["chunk"]["compress_type"] == "ZSTD"
        assert report["chunk"]["stored_len"] == 100

    def test_chunk_out_of_range_fails(self, bucket_path: Path) -> None:
        result = runner.invoke(app, ["dump", "bucket", str(bucket_path), "--chunk", "99"])
        assert result.exit_code == 1
        assert "out of range" in result.output


class TestDumpComposition:
    def test_walks_the_header_and_one_record(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_path)])
        assert result.exit_code == 0, result.output
        assert "major=1 minor=1" in result.output
        assert "head_off=64" in result.output
        assert "map_num=2" in result.output

    def test_verify_map_reports_ok(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_path), "--verify-map"])
        assert result.exit_code == 0, result.output
        assert "OK" in result.output

    def test_json_shape(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["--json", "dump", "composition", str(composition_path)])
        report = json.loads(result.stdout)
        assert report["header"] == {"major": 1, "minor": 1}
        assert len(report["records"]) == 1
        assert report["records"][0]["map_num"] == 2

    def test_explicit_offset_overrides_the_computed_start(self, composition_path: Path) -> None:
        # Same effective value the header-derived default would pick, but
        # given explicitly: exercises the override assignment itself.
        result = runner.invoke(app, ["dump", "composition", str(composition_path), "--offset", "64"])
        assert result.exit_code == 0, result.output
        assert "head_off=64" in result.output

    def test_verify_map_reports_fail_on_corrupted_map_bytes(self, composition_bad_map_crc_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_bad_map_crc_path), "--verify-map"])
        assert result.exit_code == 0, result.output
        assert "FAIL" in result.output

    def test_bad_header_crc_fails(self, composition_bad_header_crc_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_bad_header_crc_path)])
        assert result.exit_code == 1

    def test_no_records_prints_hint(self, composition_header_only_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_header_only_path)])
        assert result.exit_code == 0, result.output
        assert "(no records)" in result.output

    def test_limit_reached_with_more_records_prints_hint(self, composition_two_records_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_two_records_path), "--limit", "1"])
        assert result.exit_code == 0, result.output
        assert "more records follow" in result.output

    def test_a_failed_record_after_a_good_one_stops_but_keeps_it(self, composition_trailing_garbage_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_trailing_garbage_path)])
        assert result.exit_code == 0, result.output
        assert "head_off=64" in result.output  # the one good record still printed
        assert "stopped walking" in result.output

    def test_nothing_parses_at_all_fails(self, composition_garbage_from_the_start_path: Path) -> None:
        result = runner.invoke(app, ["dump", "composition", str(composition_garbage_from_the_start_path)])
        assert result.exit_code == 1

    def test_no_header_magic_but_a_real_record_at_offset_0_still_parses(
        self, composition_no_header_but_valid_record_path: Path
    ) -> None:
        # The other branch of ``if head_probe[:4] == MAGIC["composition"]:``
        # -- every other fixture in this file either has the real header
        # magic or fails to parse at all; none cover "no magic, but the
        # file still parses fine from byte 0" (a real, if rare,
        # non-subID=0 file).
        result = runner.invoke(app, ["dump", "composition", str(composition_no_header_but_valid_record_path)])
        assert result.exit_code == 0, result.output
        assert "map_num=2" in result.output
        assert "header :" not in result.output  # no header line -- none was found


class TestDumpChunkmap:
    def test_entries_decode_correctly(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["--json", "dump", "chunkmap", str(composition_path), "--offset", "64", "--verify"])
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["map_num"] == 2
        assert report["map_crc_ok"] is True
        assert report["entries"][0]["kind"] == "MAPPING"
        assert report["entries"][0]["addr"] == {"stream_id": 5, "bucket_id": 3, "chunk_idx": 0}
        assert report["entries"][1]["kind"] == "ZERO"
        assert report["entries"][1].get("addr") is None  # ZERO entries have no addr

    def test_human_output_shows_addr(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_path), "--offset", "64"])
        assert result.exit_code == 0, result.output
        assert "stream=5,bucket=3,chunk=0" in result.output

    def test_offset_is_required(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_path)])
        assert result.exit_code != 0

    def test_human_output_with_verify_shows_the_map_crc_line(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_path), "--offset", "64", "--verify"])
        assert result.exit_code == 0, result.output
        assert "map_crc  : " in result.output
        assert "OK" in result.output

    def test_verify_reports_fail_on_corrupted_map(self, composition_bad_map_crc_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "--json",
                "dump",
                "chunkmap",
                str(composition_bad_map_crc_path),
                "--offset",
                "64",
                "--verify",
            ],
        )
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["map_crc_ok"] is False

    def test_limit_smaller_than_map_num_shows_hint(self, composition_path: Path) -> None:
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_path), "--offset", "64", "--limit", "1"])
        assert result.exit_code == 0, result.output
        assert "showing 1 of 2 entries" in result.output

    def test_verify_on_a_large_map_array_still_reports_ok(self, composition_large_map_path: Path) -> None:
        # Past should_thread_chunk_map_crc's threshold -- exercises the
        # asyncio.to_thread() branch of diagnostics.py's _verify_map_crc,
        # not just the direct-call branch every other --verify test here
        # stays under.
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_large_map_path), "--offset", "64", "--verify"])
        assert result.exit_code == 0, result.output
        assert "map_num  : 13108" in result.output
        assert "OK" in result.output

    def test_offset_pointing_at_garbage_fails_cleanly_not_a_traceback(
        self, composition_garbage_from_the_start_path: Path
    ) -> None:
        # Unlike dump composition (whose own walk catches a bad record and
        # reports it via stopped_with_error instead of raising),
        # inspect_chunk_map has no such catch around its own
        # parse_record_head call -- this must still surface as unwrap()'s
        # ordinary clean exit-1 error, not an uncaught exception.
        result = runner.invoke(app, ["dump", "chunkmap", str(composition_garbage_from_the_start_path), "--offset", "0"])
        assert result.exit_code == 1
        assert "error:" in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)


class _FakeObjectStore:
    """A minimal in-memory ``ObjectStore`` for exercising ``--profile``.

    Unlike every other ``--profile`` unit test in this suite (which fakes
    out ``Repository``/``Session`` one layer up and never touches the
    store itself), ``dump`` reads straight through whatever store it's
    given — so this fake must actually answer ``read``/``size``/``exists``/
    ``listdir`` correctly, not stay an inert sentinel. ``closed`` records
    whether ``aclose()`` ran, for the cleanup test below.
    """

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

    async def listdir(self, path: str) -> list[str]:
        return sorted(self._files)

    async def aclose(self) -> None:
        self.closed = True


class TestDumpProfile:
    def test_bucket_via_profile_returns_expected_shape(
        self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _FakeObjectStore({"0.buk": bucket_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            assert name == "demo"
            return store

        monkeypatch.setattr(dump_module, "resolve_profile_store", _fake_resolve)
        result = runner.invoke(app, ["--json", "dump", "bucket", "--profile", "demo", "0.buk"])
        assert result.exit_code == 0, result.stdout
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

        monkeypatch.setattr(dump_module, "resolve_profile_store", _fake_resolve)
        result = runner.invoke(app, ["--json", "dump", "composition", "--profile", "demo", "c0"])
        assert result.exit_code == 0, result.stdout
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

        monkeypatch.setattr(dump_module, "resolve_profile_store", _fake_resolve)
        result = runner.invoke(
            app, ["--json", "dump", "chunkmap", "--profile", "demo", "c0", "--offset", "64", "--verify"]
        )
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["path"] == "c0"
        assert report["map_num"] == 2
        assert report["map_crc_ok"] is True

    def test_without_profile_never_calls_resolve_profile_store(
        self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _boom(name: str) -> _FakeObjectStore:
            raise AssertionError("resolve_profile_store must not be called without --profile")

        monkeypatch.setattr(dump_module, "resolve_profile_store", _boom)
        result = runner.invoke(app, ["dump", "bucket", str(bucket_path)])
        assert result.exit_code == 0, result.output

    def test_profile_store_is_closed_after_use(self, bucket_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        store = _FakeObjectStore({"0.buk": bucket_path.read_bytes()})

        async def _fake_resolve(name: str) -> _FakeObjectStore:
            return store

        monkeypatch.setattr(dump_module, "resolve_profile_store", _fake_resolve)
        result = runner.invoke(app, ["dump", "bucket", "--profile", "demo", "0.buk"])
        assert result.exit_code == 0, result.output
        assert store.closed is True

    def test_missing_local_directory_fails_cleanly_not_a_traceback(self, tmp_path: Path) -> None:
        # LocalFsStore's own __init__ raises NotFoundError eagerly, before
        # _resolved_store ever yields -- outside anything unwrap() sees,
        # so this needs _resolved_store's own try/except to surface as a
        # clean exit-1 error instead of an uncaught exception.
        missing = tmp_path / "no-such-dir" / "0.buk"
        result = runner.invoke(app, ["dump", "bucket", str(missing)])
        assert result.exit_code == 1
        assert "error:" in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)

    def test_unknown_profile_fails_cleanly_not_a_traceback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolve(name: str) -> _FakeObjectStore:
            raise ProfileNotFoundError(f"no such profile: {name!r}")

        monkeypatch.setattr(dump_module, "resolve_profile_store", _fake_resolve)
        result = runner.invoke(app, ["dump", "bucket", "--profile", "nope", "0.buk"])
        assert result.exit_code == 1
        assert "error:" in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)


__all__: list[str] = []
