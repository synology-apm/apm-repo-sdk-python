"""A synthetic one-stream SaaS repository and the service DBs its ``saas_obj``
carries, shared by the ``test_units_saas_*`` files and
``test_units_dispatch_saas.py``. Each ``*_db`` builder returns a
zstd-compressed sqlite file, the form a service DB takes inside the ObjectDB;
``FakeDedupFile`` serves such bytes from memory."""

from __future__ import annotations

import dataclasses
import json
import sqlite3
import tempfile
from collections.abc import Callable
from pathlib import Path

import zstandard

from support.fakes import faithful_to
from support.format_builders import pack_object_db
from support.repo_builders import (
    chunk_it,
    write_bare_connection_config,
    write_bucket,
    write_composition,
    write_copy_target_version_db,
    write_file_map,
    write_repo_info,
    write_saas_snapshot_db,
    write_saas_version_db,
    write_vault_encryption_key_db,
)
from synology_apm_repo.sdk.dedup.dedup_file import DedupFile


@dataclasses.dataclass(frozen=True, slots=True)
class SaasStreamIds:
    """The identifiers of the one stream a synthetic SaaS repository holds."""

    stream_id: int
    stream_uuid: str
    ccid: int = 1
    connection_id: str = "conn-1"


def write_saas_stream_dbs(tmp_path: Path, ids: SaasStreamIds, *, target_type: str = "M365") -> None:
    """``repo_info``, the vault key and connection tables, and the stream's
    ``saas_snapshot``/``saas_version`` dbs: everything but its ``saas_obj``."""
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    write_bare_connection_config(tmp_path / "db" / "connection_config", [(ids.ccid, ids.connection_id)])
    stream_db_dir = tmp_path / "saas" / str(ids.ccid) / ids.stream_uuid / "db"
    write_saas_snapshot_db(stream_db_dir / "saas_snapshot")
    write_saas_version_db(stream_db_dir / "saas_version", target_type=target_type)


def write_saas_obj(tmp_path: Path, ids: SaasStreamIds, *, session_id: int, content: bytes) -> None:
    """``content`` as the version's one ``saas_obj``: its ``file_map`` row,
    composition and Pool bucket."""
    plaintexts = chunk_it(content)
    saas_obj_path = f"{ids.stream_uuid}/{ids.connection_id}/1/saas_obj"
    write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, ids.stream_id, session_id, 64, len(plaintexts), 2)])
    write_composition(
        tmp_path / "@data" / "Composition", stream_id=ids.stream_id, session_id=session_id, num_chunks=len(plaintexts)
    )
    write_bucket(tmp_path / "@data" / "Pool" / str(ids.stream_id) / "0.buk", plaintexts)


def write_indexed_saas_obj(
    tmp_path: Path,
    ids: SaasStreamIds,
    *,
    session_id: int,
    version_uid: str,
    payloads: list[tuple[str, bytes]],
    db_objects: list[tuple[str, str]],
) -> None:
    """A ``saas_obj`` holding ``payloads`` behind one embedded ObjectDB at
    offset 0, and the object-name index naming ``db_objects`` (``(name,
    object_id)``) for ``version_uid``."""
    content, object_db_len = pack_object_db(payloads)
    write_saas_obj(tmp_path, ids, session_id=session_id, content=content)
    write_copy_target_version_db(
        tmp_path / "db" / "copy_target_version",
        version_uid=version_uid,
        object_db_id=f"{ids.stream_uuid}_0_{object_db_len}",
        db_objects=db_objects,
    )


def write_saas_object_repo(
    tmp_path: Path,
    ids: SaasStreamIds,
    *,
    session_id: int,
    version_uid: str,
    payloads: list[tuple[str, bytes]],
    db_objects: list[tuple[str, str]],
    target_type: str = "M365",
) -> None:
    """A whole one-version SaaS repository: ``write_saas_stream_dbs`` plus
    ``write_indexed_saas_obj``."""
    write_saas_stream_dbs(tmp_path, ids, target_type=target_type)
    write_indexed_saas_obj(
        tmp_path, ids, session_id=session_id, version_uid=version_uid, payloads=payloads, db_objects=db_objects
    )


def write_empty_saas_repo(tmp_path: Path, ids: SaasStreamIds, *, session_id: int, target_type: str = "M365") -> None:
    """A ``saas_obj`` of zeros with no embedded ObjectDB and no object-name
    index."""
    write_saas_stream_dbs(tmp_path, ids, target_type=target_type)
    write_saas_obj(tmp_path, ids, session_id=session_id, content=b"\x00" * 4096)


def zstd_sqlite(build: Callable[[sqlite3.Connection], None]) -> bytes:
    """A sqlite file ``build`` fills, zstd-compressed."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "service.db"
        conn = sqlite3.connect(path)
        build(conn)
        conn.commit()
        conn.close()
        raw = path.read_bytes()
    return zstandard.ZstdCompressor().compress(raw)


def _config_table(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE config_table(key TEXT, value TEXT)")


def calendar_list_db(calendars: list[tuple[str, str]], *, overrides: dict[str, str] | None = None) -> bytes:
    """``calendars``: (calendar_id, calendar_name). ``overrides``:
    calendar_id -> ``calendar_name_override``; every other calendar's is
    ``''``, the real schema's "no override set" (never ``NULL``)."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE calendar_table(calendar_id TEXT PRIMARY KEY, calendar_name TEXT, timezone TEXT, "
            "calendar_name_override TEXT)"
        )
        rows = [(calendar_id, name, "UTC", (overrides or {}).get(calendar_id, "")) for calendar_id, name in calendars]
        conn.executemany("INSERT INTO calendar_table VALUES (?, ?, ?, ?)", rows)

    return zstd_sqlite(build)


def calendar_event_db(
    events: list[tuple[str, str, str, str]], *, times: dict[str, tuple[int, int]] | None = None
) -> bytes:
    """``events``: (event_id, calendar_id, summary, meta_object_id).
    ``times``: event_id -> (event_start_time, event_end_time), two
    optional columns that stay null when omitted."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE calendar_event_table(event_id TEXT PRIMARY KEY, calendar_id TEXT, summary TEXT, "
            "meta_object_id TEXT, event_start_time INTEGER, event_end_time INTEGER)"
        )
        conn.executemany(
            "INSERT INTO calendar_event_table VALUES (?, ?, ?, ?, ?, ?)",
            [(*row, *(times or {}).get(row[0], (None, None))) for row in events],
        )

    return zstd_sqlite(build)


def mail_db(mails: list[tuple[str, str, str, str]]) -> bytes:
    """``mails``: (mail_id, subject, parent_folder_id, meta_object_id)."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute("CREATE TABLE mail_table(mail_id TEXT, subject TEXT, parent_folder_id TEXT, meta_object_id TEXT)")
        conn.executemany("INSERT INTO mail_table VALUES (?, ?, ?, ?)", mails)

    return zstd_sqlite(build)


def gws_contact_db(
    contacts: list[tuple[str, str, str, str]], group_memberships: tuple[tuple[str, str], ...] = ()
) -> bytes:
    """``contacts``: (contact_id, first_name, last_name, meta_object_id) — no
    folder column. ``group_memberships`` (contact_id, group_id) go in this
    object's ``contact_group_table``; group definitions live in the separate
    ``contact_group_db`` object."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE contact_table(contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, "
            "meta_object_id TEXT)"
        )
        conn.executemany("INSERT INTO contact_table VALUES (?, ?, ?, ?)", contacts)
        if group_memberships:
            conn.execute("CREATE TABLE contact_group_table(contact_id TEXT, group_id TEXT)")
            conn.executemany("INSERT INTO contact_group_table VALUES (?, ?)", group_memberships)

    return zstd_sqlite(build)


def item_service_db(
    *,
    root_folder_id: str | None,
    items: list[tuple[str, str, str, int, int, str, str]],
    missing_hash_column: bool = False,
) -> bytes:
    """Drive's ``config_table``/``item_table`` pair. ``items``: (item_id,
    name, parent_folder_id, type, size, content_object_id, hash).
    ``root_folder_id=None`` omits the config_table row entirely."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        if root_folder_id is not None:
            conn.execute("INSERT INTO config_table VALUES ('root_folder_id', ?)", (root_folder_id,))
        hash_col = "" if missing_hash_column else ", hash TEXT"
        conn.execute(
            f"CREATE TABLE item_table(item_id TEXT PRIMARY KEY, name TEXT, parent_folder_id TEXT, "
            f"type INTEGER, size INTEGER, mtime INTEGER, meta_object_id TEXT, content_object_id TEXT{hash_col})"
        )
        for item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash in items:
            if missing_hash_column:
                conn.execute(
                    "INSERT INTO item_table(item_id, name, parent_folder_id, type, size, mtime, "
                    "meta_object_id, content_object_id) VALUES (?, ?, ?, ?, ?, 0, '', ?)",
                    (item_id, name, parent_folder_id, item_type, size, content_object_id),
                )
            else:
                conn.execute(
                    "INSERT INTO item_table(item_id, name, parent_folder_id, type, size, mtime, "
                    "meta_object_id, content_object_id, hash) VALUES (?, ?, ?, ?, ?, 0, '', ?, ?)",
                    (item_id, name, parent_folder_id, item_type, size, content_object_id, item_hash),
                )

    return zstd_sqlite(build)


def site_list_db(lists: list[tuple[str, str, str, int, str, int]]) -> bytes:
    """``lists``: (list_id, list_title, meta_object_id, list_type,
    root_folder_id, create_time). ``list_type`` 1 is a document library, 0
    a plain list; ``root_folder_id`` is the ``parent_folder_id`` of the
    list's top-level items (non-empty for a document library, ``""`` for a
    plain list)."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE list_version_table(list_id TEXT PRIMARY KEY, list_title TEXT, "
            "meta_object_id TEXT, list_type INTEGER, root_folder_id TEXT, create_time INTEGER)"
        )
        conn.executemany("INSERT INTO list_version_table VALUES (?, ?, ?, ?, ?, ?)", lists)

    return zstd_sqlite(build)


def site_item_db(items: list[tuple[str, str, str, str, str, str, str | None, str, str]]) -> bytes:
    """``items``: (item_id, list_id, file_id, parent_folder_id, title,
    item_type, meta_object_id, url_path, value1). A document-library item's
    display name comes from ``url_path``; ``value1`` is its byte size only
    when ``file_id`` is non-empty."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE item_version_table(item_id TEXT, list_id TEXT, file_id TEXT, parent_folder_id TEXT, "
            "title TEXT, item_type TEXT, meta_object_id TEXT, url_path TEXT, value1 TEXT)"
        )
        conn.executemany("INSERT INTO item_version_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", items)

    return zstd_sqlite(build)


def channel_list_db(channels: list[tuple[str, str]]) -> bytes:
    """Teams' ``channel_info_table``; ``channels``: (channel_id, name)."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(
            "CREATE TABLE channel_info_table(row_id INTEGER, channel_id TEXT PRIMARY KEY, name TEXT, "
            "description TEXT, metadata TEXT, channel_type TEXT, create_time INTEGER)"
        )
        conn.executemany(
            "INSERT INTO channel_info_table(row_id, channel_id, name) VALUES (?, ?, ?)",
            [(i + 1, cid, name) for i, (cid, name) in enumerate(channels)],
        )

    return zstd_sqlite(build)


def chat_list_db(chats: list[tuple[str, str]], *, id_col: str = "chat_id", label_col: str = "topic") -> bytes:
    """Chat's ``chat_info_table``; ``chats``: (id, label) under the column
    names ``id_col``/``label_col``."""

    def build(conn: sqlite3.Connection) -> None:
        _config_table(conn)
        conn.execute(f"CREATE TABLE chat_info_table({id_col} TEXT PRIMARY KEY, {label_col} TEXT)")
        conn.executemany(f"INSERT INTO chat_info_table({id_col}, {label_col}) VALUES (?, ?)", chats)

    return zstd_sqlite(build)


def index_json(entries: list[tuple[str, str]]) -> bytes:
    """A Teams/Chat index object; ``entries``: (name, object_id)."""
    return json.dumps({"version": 1, "db_objects": [{"name": n, "object_id": o} for n, o in entries]}).encode()


@faithful_to(DedupFile)
class FakeDedupFile:
    """An in-memory ``DedupFile``: ``read()`` over ``buf``, recording each
    ``(offset, length)`` in ``read_calls``."""

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self.size = len(buf)
        self.read_calls: list[tuple[int, int]] = []

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        if length is None:
            length = self.size - offset
        self.read_calls.append((offset, length))
        return self._buf[offset : offset + length]
