"""Unit tests for ``synology_apm_repo.sdk.units.saas.calendar`` — a
full synthetic repository root (same building blocks as
``test_units_saas_drive.py``), with two real ZSTD-compressed service
DBs (``calendar_table`` + ``calendar_event_table``) embedded in its
``saas_obj`` content."""

from __future__ import annotations

import json
import os
import sqlite3
import struct
import tempfile
import zlib
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

import icalendar
import pytest
import zstandard

from synology_apm_repo.sdk.catalog.version import Version
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.errors import DataCorruptError, UnsupportedDataFormatError
from synology_apm_repo.sdk.format.addressing import ChunkAddress
from synology_apm_repo.sdk.format.bucket import MODE_CHUNK_CRC, MODE_COMPRESS
from synology_apm_repo.sdk.format.chunkmap import ChunkMapKind
from synology_apm_repo.sdk.format.compression import CompressType
from synology_apm_repo.sdk.format.const import SUB_FILE_SIZE
from synology_apm_repo.sdk.format.redundancy import redundancy_size
from synology_apm_repo.sdk.identifiers import (
    BucketId,
    ChunkIdx,
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamId,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore
from synology_apm_repo.sdk.units.base import Node, UnitKind, node_leaf_kind
from synology_apm_repo.sdk.units.content.saas_calendar import build_ics
from synology_apm_repo.sdk.units.saas.calendar import CalendarProvider, _event_display_name, _recurrence_label
from synology_apm_repo.sdk.units.saas.objectdb import ObjectDb
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache

_STREAM_ID = 13
_CCID = 1
_CONNECTION_ID = "conn-1"
_STREAM_UUID = "calendar-stream-uuid"


def _write_repo_info(path: Path) -> None:
    payload = json.dumps({"repo_type": 2}).encode("utf-8")
    header = bytearray(64)
    header[0:4] = b"RpiF"
    header[8:12] = (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "big")
    header[12:20] = len(payload).to_bytes(8, "big")
    header[20:36] = b"a" * 16
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + payload)


def _write_vault_encryption_key_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE vault_encryption_key(user_key_uuid TEXT UNIQUE, encrypted_data_key TEXT)")
    conn.execute("INSERT INTO vault_encryption_key VALUES ('NoEncryption', '')")
    conn.commit()
    conn.close()


def _write_file_map(path: Path, rows: list[tuple[str, int, int, int, int, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE file_map(path TEXT PRIMARY KEY, crtime DATETIME, mtime DATETIME, "
        "stream_id INTEGER, session_id INTEGER, comp_offset INTEGER, block INTEGER, status INTEGER)"
    )
    conn.executemany(
        "INSERT INTO file_map(path, stream_id, session_id, comp_offset, block, status) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _write_connection_config(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE connection_config(connection_config_id INTEGER PRIMARY KEY, connection_id TEXT)")
    conn.executemany("INSERT INTO connection_config VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_workload_config(path: Path, rows: list[tuple[int, str]]) -> None:
    """``rows``: (workload_id, workload_spec JSON)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE workload_config(workload_id INTEGER PRIMARY KEY, workload_spec TEXT)")
    conn.executemany("INSERT INTO workload_config VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_saas_snapshot_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE snapshot_info(snapshot_id INTEGER PRIMARY KEY, snapshot_uuid TEXT, "
        "first_version_id INTEGER, stream_version INTEGER)"
    )
    conn.execute("INSERT INTO snapshot_info VALUES (1, 'snap-uuid', 3, 1)")
    conn.execute(
        "CREATE TABLE snapshot_distribution(offset INTEGER, length INTEGER, snapshot_id INTEGER, version_id INTEGER)"
    )
    conn.commit()
    conn.close()


def _write_saas_version_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_info(snapshot_id INTEGER, version_id INTEGER, stream_version INTEGER, deleted INTEGER)"
    )
    conn.execute("INSERT INTO version_info VALUES (1, 3, 1, 0)")
    conn.execute("CREATE TABLE stream_info(target_type TEXT)")
    conn.execute("INSERT INTO stream_info VALUES ('GW')")
    conn.commit()
    conn.close()


def _write_copy_target_version_db(
    path: Path, *, version_uid: str, object_db_id: str, db_objects: list[tuple[str, str]]
) -> None:
    """The connector's own index bookkeeping
    (``synology_apm_repo.sdk.units.saas.object_name_index``) — every
    ``SaasWorkloadProvider``/``TeamsChatProvider`` construction resolves
    its service DB(s) *only* through this table, with no scan-based
    fallback, so a fixture repository that wants a table found must
    record it here rather than merely embedding the bytes somewhere in
    ``saas_obj``. Plain, unencrypted JSON — these fixture repositories
    never configure a vault_key, matching every other db this file writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE copy_target_version(version_uid TEXT PRIMARY KEY, version_spec TEXT)")
    additional_meta = json.dumps(
        {
            "object_db_id": object_db_id,
            "db_object_ids": {"db_objects": [{"name": name, "object_id": object_id} for name, object_id in db_objects]},
        }
    )
    version_spec = json.dumps({"status": {"additional_meta": additional_meta}})
    conn.execute("INSERT INTO copy_target_version VALUES (?, ?)", (version_uid, version_spec))
    conn.commit()
    conn.close()


def _build_object_db(rows: list[tuple[str, int, int]]) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE object_table(object_id TEXT PRIMARY KEY, offset INTEGER, length INTEGER)")
        conn.executemany("INSERT INTO object_table VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        return path.read_bytes()


def _build_calendar_list_db(calendars: list[tuple[str, str]], *, overrides: dict[str, str] | None = None) -> bytes:
    """``calendars``: (calendar_id, calendar_name). ``overrides``, when
    given: calendar_id -> its own real ``calendar_name_override`` --
    every other calendar's own column value is ``''``, matching the real
    schema's "no override set" convention (never ``NULL``)."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "cal.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE calendar_table(calendar_id TEXT PRIMARY KEY, calendar_name TEXT, timezone TEXT, "
            "calendar_name_override TEXT)"
        )
        rows = [(calendar_id, name, "UTC", (overrides or {}).get(calendar_id, "")) for calendar_id, name in calendars]
        conn.executemany("INSERT INTO calendar_table VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _build_event_db(
    events: list[tuple[str, str, str, str]], *, times: dict[str, tuple[int, int]] | None = None
) -> bytes:
    """``events``: (event_id, calendar_id, summary, meta_object_id).
    ``times`` (event_id -> (event_start_time, event_end_time)), when
    given, populates those two real, optional columns -- omitted, both
    columns stay null."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "event.db"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")
        conn.execute(
            "CREATE TABLE calendar_event_table(event_id TEXT PRIMARY KEY, calendar_id TEXT, summary TEXT, "
            "meta_object_id TEXT, event_start_time INTEGER, event_end_time INTEGER)"
        )
        conn.executemany(
            "INSERT INTO calendar_event_table VALUES (?, ?, ?, ?, ?, ?)",
            [(*row, *(times or {}).get(row[0], (None, None))) for row in events],
        )
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


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


def _write_bucket(path: Path, plaintexts: list[bytes]) -> None:
    compressor = zstandard.ZstdCompressor()
    payloads = [compressor.compress(p) for p in plaintexts]
    entries = [(CompressType.ZSTD.value, len(p)) for p in payloads]
    tight = _encode_size_store(entries)
    chunk_size_crc = zlib.crc32(tight) & 0xFFFFFFFF
    header = bytearray(64)
    header[0:4] = b"bFiL"
    header[4:6] = (3).to_bytes(2, "big")
    header[8:12] = struct.pack(">I", MODE_COMPRESS | MODE_CHUNK_CRC)
    header[12:16] = struct.pack(">I", len(plaintexts))
    header[16:20] = struct.pack(">I", chunk_size_crc)
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")
    sizestore_region = tight + b"\x00" * (16320 - len(tight))
    trailer = os.urandom(4 * len(plaintexts) + redundancy_size((len(plaintexts) * 15 + 7) >> 3, 256))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + sizestore_region + b"".join(payloads) + trailer)


def _chunk_map_record_bytes(*, kind_value: int, file_chunk_idx: int, addr_int: int, tail_u32: int) -> bytes:
    type_byte = kind_value & 0x0F
    return (
        bytes([type_byte])
        + file_chunk_idx.to_bytes(7, "big")
        + addr_int.to_bytes(8, "big")
        + tail_u32.to_bytes(4, "big")
    )


def _write_composition(root: Path, *, stream_id: int, session_id: int, num_chunks: int) -> None:
    addr_int = ChunkAddress(StreamId(stream_id), BucketId(0), ChunkIdx(0)).to_int()
    entry = _chunk_map_record_bytes(
        kind_value=ChunkMapKind.MAPPING.value, file_chunk_idx=0, addr_int=addr_int, tail_u32=num_chunks << 16
    )
    head = bytearray(32)
    head[0:2] = b"Mu"
    head[6:14] = (1).to_bytes(8, "big")
    head[18:20] = (1).to_bytes(2, "big")
    head[28:32] = (zlib.crc32(bytes(head[:28])) & 0xFFFFFFFF).to_bytes(4, "big")
    record_bytes = bytes(head) + entry

    header = bytearray(64)
    header[0:4] = b"cMpS"
    header[4:6] = (1).to_bytes(2, "big")
    header[6:8] = (1).to_bytes(2, "big")
    header[8:12] = SUB_FILE_SIZE.to_bytes(4, "big")
    header[60:64] = (zlib.crc32(bytes(header[:60])) & 0xFFFFFFFF).to_bytes(4, "big")

    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + record_bytes)


def _chunk_it(buf: bytes) -> list[bytes]:
    padded = buf + b"\x00" * (-len(buf) % 4096)
    return [padded[i : i + 4096] for i in range(0, len(padded), 4096)]


_META_EVENT_1 = json.dumps(
    {
        "attachment_list": [],
        "client_metadata": {
            "id": "event-1",
            "iCalUID": "event-1@example.com",
            "summary": "Standup",
            "location": "Room 1",
            "start": {"dateTime": "2026-01-01T09:00:00+00:00"},
            "end": {"dateTime": "2026-01-01T09:30:00+00:00"},
            "organizer": {"email": "boss@example.com"},
            "recurrence": ["RRULE:FREQ=WEEKLY"],
        },
        "version": "1.0",
    }
).encode()

_META_EVENT_EWS = json.dumps(
    {"attachment_list": [], "client_metadata": {"RawXML": "<xml/>", "RecurringMasterId": "abc"}, "version": "1.0"}
).encode()


def _build_calendar_repo(
    tmp_path: Path,
    *,
    session_id: int = 8,
    event_meta: bytes = _META_EVENT_1,
    event_times: dict[str, tuple[int, int]] | None = None,
    calendars: list[tuple[str, str]] | None = None,
    calendar_name_overrides: dict[str, str] | None = None,
) -> None:
    _write_repo_info(tmp_path / "repo_info")
    _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
    _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    _write_saas_version_db(stream_db_dir / "saas_version")

    calendar_list_bytes = _build_calendar_list_db(
        calendars or [("cal-1", "Primary Calendar")], overrides=calendar_name_overrides
    )
    event_db_bytes = _build_event_db([("event-1", "cal-1", "Standup", "meta_1")], times=event_times)

    payloads = [("cal_svc", calendar_list_bytes), ("event_svc", event_db_bytes), ("meta_1", event_meta)]
    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(_build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = _build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = _chunk_it(saas_obj_content)

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    _write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)
    _write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=_version().version_uid,
        object_db_id=f"{_STREAM_UUID}_0_{object_db_len}",
        db_objects=[("calendar_db", "cal_svc"), ("calendar_event_db", "event_svc")],
    )


def _version() -> Version:
    return Version(
        version_id=VersionId(61),
        version_uid=VersionUid("vuid-calendar"),
        workload_id=WorkloadId(1),
        connection_config_id=ConnectionConfigId(_CCID),
        target_type="GW",
        target_id=TargetId(_STREAM_UUID),
        saas_stream_uuid=StreamUuid(_STREAM_UUID),
        saas_snapshot_uuid=SnapshotUuid("snap-uuid"),
        saas_version_id=SaasVersionId(3),
        deleted=False,
        display_name="2026-01-01 00:00",
        meta=None,
    )


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[SaasWorkloadProvider]:
    _build_calendar_repo(tmp_path)
    store = LocalFsStore(tmp_path)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        p = await CalendarProvider(repo, _version(), saas_streams)
        try:
            yield p
        finally:
            await p.close()


async def _calendars_of(provider: SaasWorkloadProvider) -> list[Node]:
    """Every real calendar across the My/Other Calendars synthetic level
    ``children(root())`` inserts — every calendar this file's own
    fixtures build has no ``calendar_type`` column at all (schema-drift
    tolerance backfills ``None``), which ``_is_other_calendar`` treats as
    "my own", so this is normally just that one category's own children,
    written generically over however many categories are actually
    present."""
    calendars: list[Node] = []
    for category in await provider.children(provider.root()):
        calendars.extend(await provider.children(category))
    return calendars


class TestTree:
    async def test_root_lists_a_single_my_calendars_category(self, provider: SaasWorkloadProvider) -> None:
        categories = await provider.children(provider.root())
        assert [c.name for c in categories] == ["My Calendars"]
        assert categories[0].is_leaf is False

    async def test_root_lists_the_calendar(self, provider: SaasWorkloadProvider) -> None:
        calendars = await _calendars_of(provider)
        assert len(calendars) == 1
        assert calendars[0].name == "Primary Calendar"
        assert calendars[0].is_leaf is False

    async def test_root_and_category_nodes_override_leaf_kind_to_category_group(
        self, provider: SaasWorkloadProvider
    ) -> None:
        """The root's own children are categories, and a category's own
        children are individual calendars -- both containers, never
        leaves -- so overriding leaf_kind to CATEGORY_GROUP at both
        levels (rather than inheriting CALENDAR_EVENT's
        ``leaves_only=True`` column spec) is what keeps the browser's
        file table from filtering every one of them out. An individual
        calendar's own leaf_kind stays CALENDAR_EVENT, unaffected by
        this override."""
        assert node_leaf_kind(provider.root()) is UnitKind.CATEGORY_GROUP
        [category] = await provider.children(provider.root())
        assert node_leaf_kind(category) is UnitKind.CATEGORY_GROUP
        [calendar] = await provider.children(category)
        assert node_leaf_kind(calendar) is UnitKind.CALENDAR_EVENT

    async def test_calendar_lists_its_events(self, provider: SaasWorkloadProvider) -> None:
        [calendar] = await _calendars_of(provider)
        events = await provider.children(calendar)
        assert len(events) == 1
        assert events[0].name == "Standup"
        assert events[0].is_leaf is True
        assert events[0].kind is UnitKind.CALENDAR_EVENT

    async def test_children_of_an_event_node_is_empty(self, provider: SaasWorkloadProvider) -> None:
        """A leaf event node has no children."""
        [calendar] = await _calendars_of(provider)
        [event] = await provider.children(calendar)
        assert await provider.children(event) == []

    async def test_repeated_calls_return_the_same_events(self, provider: SaasWorkloadProvider) -> None:
        # No one-shot in-memory index built once and cached: the leaf
        # (event) level runs a real WHERE/ORDER BY/LIMIT/OFFSET query on
        # every call -- this just confirms repeated calls stay consistent.
        [calendar] = await _calendars_of(provider)
        first = await provider.children(calendar)
        second = await provider.children(calendar)
        assert [n.ref for n in first] == [n.ref for n in second]

    async def test_calendar_lists_its_events_issues_exactly_one_query(
        self, provider: SaasWorkloadProvider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``children_of()`` for one calendar's events issues exactly
        one ``WHERE calendar_id = ?`` query."""
        from synology_apm_repo.sdk.storage.table import Table

        calls: list[tuple[str, Sequence[object]]] = []
        original_select = Table.select

        def counting_select(
            self: Table,
            where: str = "",
            params: Sequence[object] = (),
            *,
            order_by: str | None = None,
            limit: int | None = None,
            offset: int = 0,
        ) -> AsyncIterator[dict[str, object | None]]:
            calls.append((where, params))
            return original_select(self, where, params, order_by=order_by, limit=limit, offset=offset)

        monkeypatch.setattr(Table, "select", counting_select)

        [calendar] = await _calendars_of(provider)
        calls.clear()
        await provider.children(calendar)
        assert calls == [("calendar_id = ?", ("cal-1",))]

    async def test_event_start_and_end_are_exposed_as_attrs(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, event_times={"event-1": (0, 3600)})
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await CalendarProvider(repo, _version(), saas_streams) as provider,
        ):
            [calendar] = await _calendars_of(provider)
            [event] = await provider.children(calendar)
            assert event.attrs.get("event_start") == datetime.fromtimestamp(0, UTC)
            assert event.attrs.get("event_end") == datetime.fromtimestamp(3600, UTC)


class TestPrimaryCalendarDisplayName:
    """Google's own API convention: an unrenamed primary calendar's
    ``calendar_id`` *and* ``calendar_name`` both default to the bare
    account email -- ``_group_name_override`` is what keeps "My
    Calendars" from showing that raw email instead of the account's
    real name."""

    _EMAIL = "user.test025@gwsdemo.example.com"

    async def test_primary_calendar_shows_the_owning_accounts_real_name(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, calendars=[(self._EMAIL, self._EMAIL)])
        spec = json.dumps(
            {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        )
        _write_workload_config(tmp_path / "db" / "workload_config", [(int(_version().workload_id), spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await CalendarProvider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "User Test025"

    async def test_a_calendar_that_is_not_the_owning_account_keeps_its_own_name(self, tmp_path: Path) -> None:
        """A shared/secondary calendar's own ``calendar_id`` never
        matches the backed-up account's own email, so this override
        never fires for it -- it keeps its real ``calendar_name``."""
        _build_calendar_repo(tmp_path, calendars=[("cal-1", "Team Holidays")])
        spec = json.dumps(
            {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        )
        _write_workload_config(tmp_path / "db" / "workload_config", [(int(_version().workload_id), spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await CalendarProvider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "Team Holidays"

    async def test_a_users_own_calendar_name_override_wins_over_the_account_name(self, tmp_path: Path) -> None:
        """``calendar_name_override`` is a real, separate Google API
        field (a user's own personal relabeling) -- it must win even
        over the account-name substitution above, since it's the more
        specific, more recently-expressed real user intent."""
        _build_calendar_repo(
            tmp_path,
            calendars=[(self._EMAIL, self._EMAIL)],
            calendar_name_overrides={self._EMAIL: "Work"},
        )
        spec = json.dumps(
            {"status": {"entity_meta": {"spec": {"user_info": {"email": self._EMAIL, "name": "User Test025"}}}}}
        )
        _write_workload_config(tmp_path / "db" / "workload_config", [(int(_version().workload_id), spec)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await CalendarProvider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == "Work"

    async def test_no_workload_config_at_all_falls_back_to_the_plain_calendar_name(self, tmp_path: Path) -> None:
        """No resolvable owning identity means no override candidate at
        all -- this is the one test in the class that pairs a missing
        ``workload_config`` with an email-shaped ``calendar_id``, the
        specific shape that would otherwise trigger it."""
        _build_calendar_repo(tmp_path, calendars=[(self._EMAIL, self._EMAIL)])
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with (
            await DedupRepo.open(store, layout) as repo,
            SaasStreamCache(repo) as saas_streams,
            await CalendarProvider(repo, _version(), saas_streams) as provider,
        ):
            [category] = await provider.children(provider.root())
            [calendar] = await provider.children(category)
            assert calendar.name == self._EMAIL


class TestUnit:
    async def test_builds_a_valid_reparseable_ics(self, provider: SaasWorkloadProvider) -> None:
        [calendar] = await _calendars_of(provider)
        [event] = await provider.children(calendar)
        content = (await provider.unit(event)).open()
        # LazyArtifact.size is None until assembled (see TestSize in
        # test_units_saas_artifact.py) — read the whole artifact instead.
        data = await content.read()
        reparsed = icalendar.Calendar.from_ical(data)
        [vevent] = list(reparsed.walk("VEVENT"))
        assert str(vevent.get("summary")) == "Standup"
        assert str(vevent.get("location")) == "Room 1"
        assert str(vevent.get("organizer")) == "mailto:boss@example.com"

    async def test_unit_on_a_calendar_node_raises(self, provider: SaasWorkloadProvider) -> None:
        [calendar] = await _calendars_of(provider)
        with pytest.raises(ValueError, match="not a restorable unit"):
            await provider.unit(calendar)

    async def test_ews_envelope_client_metadata_raises_on_first_access_not_construction(self, tmp_path: Path) -> None:
        _build_calendar_repo(tmp_path, event_meta=_META_EVENT_EWS)
        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            p = await CalendarProvider(repo, _version(), saas_streams)
            try:
                [calendar] = await _calendars_of(p)
                [event] = await p.children(calendar)
                unit = await p.unit(event)  # must not raise here
                with pytest.raises(UnsupportedDataFormatError):
                    await unit.open().read()
            finally:
                await p.close()


class TestBuildIcs:
    def test_unrecognized_start_shape_raises_unsupported_data_format(self) -> None:
        # _parse_when raises UnsupportedDataFormatError, matching build_ics's own
        # sibling raise (the EWS-envelope check two lines above it) for the
        # same class of "recognized but unsupported shape" failure — this
        # shape is currently unreachable from real sample data (start/end
        # always has date or dateTime).
        meta = json.dumps({"client_metadata": {"summary": "weird", "start": {"nonsense": "x"}}}).encode()
        with pytest.raises(UnsupportedDataFormatError, match="unrecognized calendar event start/end shape"):
            build_ics(meta, "event-weird")

    def test_minimal_event_without_optional_fields(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "bare event"}}).encode()
        ics = build_ics(meta, "event-x")
        assert b"SUMMARY:bare event" in ics
        assert b"UID:event-x" in ics  # falls back to the caller-supplied event_id

    def test_all_day_event_uses_date_not_datetime(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "All day", "start": {"date": "2026-03-01"}, "end": {"date": "2026-03-02"}}}
        ).encode()
        ics = build_ics(meta, "event-y")
        assert b"DTSTART;VALUE=DATE:20260301" in ics

    def test_m365_naive_datetime_with_utc_timezone_is_anchored_not_floating(self) -> None:
        """M365 Graph's ``dateTime`` has no offset at all when its sibling
        ``timeZone`` is ``"UTC"``. A naive ``dateTime`` alone (GWS's own
        shape always carries a real offset instead) must anchor to UTC,
        not serialize as a floating time a receiving calendar app would
        reinterpret in its own local zone."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "UTC"},
                    "end": {"dateTime": "2026-01-01T09:30:00.0000000", "timeZone": "UTC"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-utc")
        assert b"DTSTART:20260101T090000Z" in ics
        assert b"DTEND:20260101T093000Z" in ics

    def test_m365_naive_datetime_with_named_timezone_resolves_via_zoneinfo(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "Asia/Taipei"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-named-zone")
        assert b"DTSTART;TZID=Asia/Taipei:20260101T090000" in ics

    def test_m365_naive_datetime_with_unresolvable_timezone_stays_floating_not_crash(self) -> None:
        # A legacy Windows zone name (Graph's own default when a client
        # never requests "Prefer: outlook.timezone" in IANA form) has no
        # confirmed real sample data behind it -- refusing to guess means
        # falling back to the pre-existing naive/floating serialization
        # rather than raising, since this SDK's job is best-effort backup
        # recovery, not failing every event in an otherwise-recoverable
        # calendar over one unresolvable zone name.
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "Pacific Standard Time"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-unresolvable-zone")
        assert b"DTSTART:20260101T090000" in ics
        assert b"DTSTART;TZID" not in ics

    def test_m365_naive_datetime_with_malformed_timezone_stays_floating_not_crash(self) -> None:
        """``ZoneInfo(str(x))`` raises ``ValueError`` (not
        ``ZoneInfoNotFoundError``) for a malformed key such as an
        absolute-path-shaped string — a corrupted ``timeZone`` field
        must degrade to naive/floating time the same as a genuinely
        unresolvable zone name above, not crash this event's export."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "M365 meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00.0000000", "timeZone": "/etc/passwd"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-m365-malformed-zone")
        assert b"DTSTART:20260101T090000" in ics
        assert b"DTSTART;TZID" not in ics

    def test_meta_bytes_not_valid_json_raises_data_corrupt(self) -> None:
        with pytest.raises(DataCorruptError):
            build_ics(b"not json at all", "event-corrupt")

    def test_m365_naive_datetime_with_no_timezone_key_defaults_to_utc(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "M365 meeting", "start": {"dateTime": "2026-01-01T09:00:00.0000000"}}}
        ).encode()
        ics = build_ics(meta, "event-m365-no-zone")
        assert b"DTSTART:20260101T090000Z" in ics

    def test_gws_offset_bearing_datetime_is_used_as_is_ignoring_any_timezone_key(self) -> None:
        # GWS's own shape already carries a real UTC offset in "dateTime"
        # itself -- a "timeZone" key, if present at all, must never
        # override or double-apply on top of it.
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "GWS meeting",
                    "start": {"dateTime": "2026-01-01T09:00:00-08:00", "timeZone": "America/Los_Angeles"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-gws")
        assert b'DTSTART;TZID="UTC-08:00":20260101T090000' in ics

    def test_ews_envelope_raises_unsupported_data_format(self) -> None:
        with pytest.raises(UnsupportedDataFormatError):
            build_ics(_META_EVENT_EWS, "event-ews")

    def test_recurrence_rule_is_written_as_a_real_rrule(self) -> None:
        # _META_EVENT_1 already carries a real "recurrence" list
        # (RRULE:FREQ=WEEKLY), but no test anywhere in this file asserts
        # on the RRULE line the ics output actually gets from it.
        ics = build_ics(_META_EVENT_1, "event-1")
        assert b"RRULE:FREQ=WEEKLY" in ics

    def test_m365_absolute_yearly_recurrence_produces_a_real_rrule(self) -> None:
        # A real dict-shaped M365 Exchange recurrence -- iterating it
        # must not fall through to yielding its own dict keys as bogus
        # RRULE lines.
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Labor Day",
                    "recurrence": {
                        "pattern": {"type": "absoluteYearly", "dayOfMonth": 1, "month": 5},
                        "range": {"type": "noEnd"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-yearly")
        assert b"RRULE:FREQ=YEARLY;BYMONTHDAY=1;BYMONTH=5" in ics

    def test_m365_weekly_recurrence_with_interval_and_days(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Sprint sync",
                    "recurrence": {
                        "pattern": {
                            "type": "weekly",
                            "interval": 2,
                            "daysOfWeek": ["monday", "wednesday"],
                        },
                        "range": {"type": "numbered", "numberOfOccurrences": 10},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-weekly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=WEEKLY" in rrule_line
        assert "INTERVAL=2" in rrule_line
        assert "BYDAY=MO,WE" in rrule_line
        assert "COUNT=10" in rrule_line

    def test_m365_relative_monthly_recurrence_with_end_date(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Last Friday review",
                    "recurrence": {
                        "pattern": {"type": "relativeMonthly", "index": "last", "daysOfWeek": ["friday"]},
                        "range": {"type": "endDate", "endDate": "2026-12-31"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-relative-monthly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=MONTHLY" in rrule_line
        assert "BYDAY=-1FR" in rrule_line
        assert "UNTIL=20261231" in rrule_line

    def test_m365_recurrence_with_a_timed_dtstart_widens_until_to_a_datetime(self) -> None:
        """RFC 5545 requires UNTIL's own value type to match DTSTART's --
        a timed event (``start`` has a ``dateTime``) must not pair a
        bare-date UNTIL with its DATE-TIME DTSTART, even though Graph's
        own ``range.endDate`` is always date-only."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Weekly standup",
                    "start": {"dateTime": "2026-01-05T09:00:00", "timeZone": "UTC"},
                    "end": {"dateTime": "2026-01-05T09:30:00", "timeZone": "UTC"},
                    "recurrence": {
                        "pattern": {"type": "weekly", "daysOfWeek": ["monday"]},
                        "range": {"type": "endDate", "endDate": "2026-12-31"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-timed-until")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "UNTIL=20261231T235959Z" in rrule_line

    def test_m365_recurrence_with_non_dict_pattern_omits_rrule(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "x", "recurrence": {"pattern": "not-a-dict"}}}).encode()
        ics = build_ics(meta, "event-bad-pattern")
        assert b"RRULE" not in ics

    def test_m365_relative_yearly_recurrence_includes_bymonth(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "Thanksgiving-style holiday",
                    "recurrence": {
                        "pattern": {
                            "type": "relativeYearly",
                            "index": "fourth",
                            "daysOfWeek": ["thursday"],
                            "month": 11,
                        },
                        "range": {"type": "noEnd"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-relative-yearly")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "FREQ=YEARLY" in rrule_line
        assert "BYDAY=4TH" in rrule_line
        assert "BYMONTH=11" in rrule_line

    def test_m365_recurrence_with_unrecognized_pattern_type_omits_rrule(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "x", "recurrence": {"pattern": {"type": "bogus"}, "range": {}}}}
        ).encode()
        ics = build_ics(meta, "event-unrecognized")
        assert b"RRULE" not in ics

    def test_m365_recurrence_with_a_datetime_shaped_end_date_still_resolves_until(self) -> None:
        """Graph's own spec shape for ``endDate`` is a bare "YYYY-MM-DD",
        but some real payloads carry a full dateTime string instead --
        this must still resolve to a real ``UNTIL``, not silently drop
        it the way a genuinely malformed value does (the test below)."""
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "x",
                    "recurrence": {
                        "pattern": {"type": "daily"},
                        "range": {"type": "endDate", "endDate": "2026-12-31T00:00:00Z"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-datetime-until")
        rrule_line = next(line for line in ics.decode().splitlines() if line.startswith("RRULE:"))
        assert "UNTIL=20261231" in rrule_line

    def test_m365_recurrence_with_malformed_end_date_omits_until(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "summary": "x",
                    "recurrence": {
                        "pattern": {"type": "daily"},
                        "range": {"type": "endDate", "endDate": "not-a-date"},
                    },
                }
            }
        ).encode()
        ics = build_ics(meta, "event-bad-until")
        assert b"RRULE:FREQ=DAILY" in ics
        assert b"UNTIL" not in ics

    def test_uid_falls_back_to_gws_ical_uid_casing_when_m365_casing_is_absent(self) -> None:
        # Distinct from both the M365 "iCalUId" case and the
        # internal-graph-id fallback below it -- the middle rung of the
        # documented ``iCalUId or iCalUID or id or event_id`` chain.
        meta = json.dumps({"client_metadata": {"iCalUID": "gws-uid@example.com"}}).encode()
        ics = build_ics(meta, "event-fallback")
        assert b"UID:gws-uid@example.com" in ics

    def test_recurring_instance_override_adds_recurrence_id(self) -> None:
        # originalStart carries the pre-modification scheduled time, so a
        # calendar app treats this VEVENT as an override of that specific
        # recurring instance rather than a separate event sharing the UID.
        meta = json.dumps(
            {"client_metadata": {"summary": "Moved instance", "originalStart": "2026-03-01T09:00:00"}}
        ).encode()
        ics = build_ics(meta, "event-recurring")
        assert b"RECURRENCE-ID:20260301T090000" in ics

    @pytest.mark.parametrize(
        "top_level",
        [{}, {"client_metadata": None}, {"client_metadata": ""}],
        ids=["missing_key", "null_value", "empty_string"],
    )
    def test_client_metadata_missing_null_or_empty_defaults_to_empty(self, top_level: dict[str, object]) -> None:
        """``meta.get("client_metadata")`` — absent entirely, JSON
        ``null``, and an empty string are three distinct raw shapes that
        must all fall back to the same empty default."""
        meta = json.dumps(top_level).encode()
        ics = build_ics(meta, "event-id")
        assert b"UID:event-id" in ics
        assert b"SUMMARY" not in ics

    def test_uid_prefers_m365_ical_uid_casing_over_the_internal_graph_id(self) -> None:
        meta = json.dumps({"client_metadata": {"iCalUId": "m365-uid@example.com", "id": "graph-internal-id"}}).encode()
        ics = build_ics(meta, "event-fallback")
        assert b"UID:m365-uid@example.com" in ics

    def test_uid_falls_back_to_the_internal_graph_id_when_no_ical_uid_is_present(self) -> None:
        meta = json.dumps({"client_metadata": {"id": "graph-internal-id"}}).encode()
        ics = build_ics(meta, "event-fallback")
        assert b"UID:graph-internal-id" in ics

    def test_title_falls_back_to_the_m365_subject_key(self) -> None:
        meta = json.dumps({"client_metadata": {"subject": "M365 Meeting"}}).encode()
        ics = build_ics(meta, "event-subject")
        assert b"SUMMARY:M365 Meeting" in ics

    def test_organizer_resolves_the_nested_m365_email_address_shape(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"summary": "x", "organizer": {"emailAddress": {"address": "boss@example.com"}}}}
        ).encode()
        ics = build_ics(meta, "event-organizer")
        assert b"mailto:boss@example.com" in ics

    def test_location_resolves_the_m365_display_name_dict_shape(self) -> None:
        meta = json.dumps({"client_metadata": {"summary": "x", "location": {"displayName": "Room 42"}}}).encode()
        ics = build_ics(meta, "event-location")
        assert b"LOCATION:Room 42" in ics

    def test_location_dict_without_a_display_name_omits_location_entirely(self) -> None:
        # A real Graph API ``location`` object can lack ``displayName`` —
        # a plain "in tester's office" case is a real, observed example.
        meta = json.dumps({"client_metadata": {"summary": "x", "location": {"address": {}}}}).encode()
        ics = build_ics(meta, "event-location-2")
        assert b"LOCATION" not in ics


class TestDegradation:
    async def test_raises_unsupported_data_format_when_no_calendar_tables_exist(self, tmp_path: Path) -> None:
        _write_repo_info(tmp_path / "repo_info")
        _write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
        _write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID)])
        stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
        _write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
        _write_saas_version_db(stream_db_dir / "saas_version")

        content = b"\x00" * 4096
        saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
        _write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, 8, 64, 1, 2)])
        _write_composition(tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=8, num_chunks=1)
        _write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", [content])

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            with pytest.raises(UnsupportedDataFormatError):
                await CalendarProvider(repo, _version(), saas_streams)


class TestSharedObjectDbCaching:
    """``SaasWorkloadProvider`` base-class-only behavior
    (``units/saas/provider.py``, which has no dedicated test file of its
    own — every concrete provider's own test file exercises it only
    indirectly). Calendar is a real two-required-table config
    (``calendar_table``/``calendar_event_table``) whose two tables live
    in the *same* embedded ``saas_obj`` ObjectDB, making it the one
    already-available fixture that exercises two base-class behaviors no
    test anywhere else asserts on directly: the second table reuses the
    first's already-loaded ObjectDB instead of re-materializing it, and
    ``close()`` only closes that shared instance once."""

    async def test_two_required_tables_sharing_one_object_load_it_only_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_calendar_repo(tmp_path)
        original_load = ObjectDb.load
        load_calls: list[tuple[int, int]] = []

        async def counting_load(dedup_file: object, offset: int, length: int) -> ObjectDb:
            load_calls.append((offset, length))
            return await original_load(dedup_file, offset, length)  # type: ignore[arg-type]

        monkeypatch.setattr(ObjectDb, "load", counting_load)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await CalendarProvider(repo, _version(), saas_streams)
            try:
                # Both calendar_table and calendar_event_table resolved
                # successfully (create() wouldn't have returned otherwise),
                # yet the embedded ObjectDB backing both was only ever
                # loaded once.
                assert len(load_calls) == 1
            finally:
                await provider.close()

    async def test_close_only_closes_the_shared_object_db_once_despite_two_table_entries(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build_calendar_repo(tmp_path)
        original_load = ObjectDb.load
        close_calls: list[ObjectDb] = []

        async def spying_load(dedup_file: object, offset: int, length: int) -> ObjectDb:
            object_db = await original_load(dedup_file, offset, length)  # type: ignore[arg-type]
            original_close = object_db.close

            async def spy_close() -> None:
                close_calls.append(object_db)
                await original_close()

            object_db.close = spy_close  # type: ignore[method-assign]
            return object_db

        monkeypatch.setattr(ObjectDb, "load", spying_load)

        store = LocalFsStore(tmp_path)
        layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
        async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
            provider = await CalendarProvider(repo, _version(), saas_streams)
            await provider.close()
            # Not two -- close() iterates self._object_db_cache (keyed by
            # (offset, length)), not self._object_dbs (one entry per
            # table_name, two of which point at the same instance here).
            assert len(close_calls) == 1


class TestEventDisplayName:
    """Direct unit tests for ``_event_display_name`` -- no test anywhere
    in this file builds an event row with an empty or null summary, so
    ``_NO_TITLE_LABEL`` was never actually produced."""

    def test_empty_string_summary_gets_the_no_title_label(self) -> None:
        assert _event_display_name({"summary": ""}) == "(no title)"

    def test_null_summary_gets_the_no_title_label(self) -> None:
        assert _event_display_name({"summary": None}) == "(no title)"

    def test_real_summary_is_used_as_is(self) -> None:
        assert _event_display_name({"summary": "Standup"}) == "Standup"


class TestRecurrenceLabel:
    """Direct unit tests for ``_recurrence_label``."""

    def test_no_recurrence_rule_is_blank(self) -> None:
        assert _recurrence_label({"recurrence_rule": None}) == ""
        assert _recurrence_label({"recurrence_rule": ""}) == ""

    def test_malformed_json_is_blank_not_a_raise(self) -> None:
        assert _recurrence_label({"recurrence_rule": "{not json"}) == ""

    def test_no_pattern_key_is_blank(self) -> None:
        assert _recurrence_label({"recurrence_rule": json.dumps({"type": "seriesMaster"})}) == ""

    def test_known_pattern_types_get_their_own_label(self) -> None:
        for pattern_type, label in (
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("absoluteMonthly", "Monthly"),
            ("relativeMonthly", "Monthly"),
            ("absoluteYearly", "Yearly"),
            ("relativeYearly", "Yearly"),
        ):
            row: dict[str, object | None] = {"recurrence_rule": json.dumps({"pattern": {"type": pattern_type}})}
            assert _recurrence_label(row) == label

    def test_unrecognized_pattern_type_still_says_recurring(self) -> None:
        row: dict[str, object | None] = {"recurrence_rule": json.dumps({"pattern": {"type": "somethingNew"}})}
        assert _recurrence_label(row) == "Recurring"
