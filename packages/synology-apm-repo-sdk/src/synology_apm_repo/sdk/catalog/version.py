"""``Version``: one ``db/copy_target_version`` row (+ ``_meta`` when
present), the queries that list or look one up, its meta-directory
resolution, and ``open_target_db()`` for the Device and FS providers.

``version_spec`` is AES-256-CTR ciphertext whenever the repository has a
vault key (FORMAT-SPEC.md: ``copy_target_version.version_spec``
encryption); ``parse_version_spec`` decrypts and
parses it.
"""

from __future__ import annotations

import binascii
import dataclasses
import json
from datetime import UTC, datetime
from typing import Any

from .._util.jsonparse import json_int, json_object, parse_json_array, try_parse_json_object
from ..dedup.repository import DedupRepo
from ..errors import KeyMaterialError, NotFoundError
from ..format.crypto import decrypt_version_spec
from ..identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from ..presentation.format import format_timestamp
from ..storage.base import join_path
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource
from ..storage.table import Column, Table, as_int, as_str, sql_placeholders
from .workload import SAAS_TARGET_TYPES, Workload


@dataclasses.dataclass(frozen=True, slots=True)
class VersionMeta:
    """A version's ``_meta`` sidecar, when present: its directory, the
    filenames it holds, and its own status code."""

    target_meta_path: str
    meta_filenames: tuple[str, ...]
    status: int


@dataclasses.dataclass(frozen=True, slots=True)
class Version:
    """One browsable ``db/copy_target_version`` row (+ ``_meta`` when
    present).

    Attributes:
        target_type: The raw column value, normally a ``TargetType`` value.
        deleted: The row's ``deleted`` flag.
        display_name: The backup time in local time
            (``"YYYY-MM-DD HH:MM:SS"``), or ``version_uid`` when it can't be
            read.
        meta: The ``copy_target_version_meta`` row, or ``None`` when none
            landed.
    """

    version_id: VersionId
    version_uid: VersionUid
    workload_id: WorkloadId
    connection_config_id: ConnectionConfigId
    target_type: str
    target_id: TargetId
    saas_stream_uuid: StreamUuid
    saas_snapshot_uuid: SnapshotUuid
    saas_version_id: SaasVersionId
    deleted: bool
    display_name: str
    meta: VersionMeta | None

    @property
    def is_saas(self) -> bool:
        """Whether this is a SaaS (GWS/M365) version."""
        return self.target_type in SAAS_TARGET_TYPES


_VERSION_COLUMNS = [
    Column("version_id"),
    Column("version_uid"),
    Column("workload_id"),
    Column("connection_config_id"),
    Column("target_type"),
    Column("target_id"),
    Column("saas_stream_uuid"),
    Column("saas_snapshot_uuid"),
    Column("saas_version_id"),
    Column("deleted"),
    Column("version_spec"),
]


#: ``version_spec.status.status`` values of a version with landed data, for
#: every workload type. ``CANCELED`` is included: a canceled job can land
#: data before it stops. Any other value, or an unreadable status, hides
#: the version.
_BROWSABLE_VERSION_STATUSES = frozenset({"COMPLETED", "PARTIAL", "CANCELED"})


def resolve_meta_filename(meta: VersionMeta, name: str, *, ref: str | None = None) -> str:
    """Return ``name`` (e.g. ``"target.db"``) after confirming it is in
    ``meta.meta_filenames``, the meta directory's authoritative file list
    (FORMAT-SPEC.md: dedup data mapping chain).

    Raises:
        NotFoundError: ``name`` was never registered for this version.
    """
    if name not in meta.meta_filenames:
        raise NotFoundError(
            f"{name!r} is not registered in this version's meta_filenames",
            ref=ref,
            spec="FORMAT-SPEC.md: dedup data mapping chain",
        )
    return name


def resolve_copy_meta_dir(version: Version, repo_root: str) -> str:
    """The ``copy_meta_file/<dir>`` under ``repo_root`` holding a device or
    FS ``version``'s meta files (``target.db``, ...), named by the last
    component of ``copy_target_version_meta.target_meta_path``.

    Raises:
        NotFoundError: ``version`` has no meta row, or its
            ``target_meta_path`` is malformed.
    """
    meta = version.meta
    if meta is None or not meta.target_meta_path:
        raise NotFoundError(
            f"version {version.version_uid} has no copy_target_version_meta row (no meta directory ever landed for it)",
            ref=version.version_uid,
        )
    dirname = meta.target_meta_path.rstrip("/").rsplit("/", 1)[-1]
    # Checked here for an error naming the catalog row: join_path() would
    # reject ".."/backslash less clearly, and would silently drop an empty
    # part, resolving to copy_meta_file itself.
    if not dirname or dirname == ".." or "\\" in dirname:
        raise NotFoundError(
            f"version {version.version_uid} has a malformed copy_target_version_meta.target_meta_path: "
            f"{meta.target_meta_path!r}",
            ref=version.version_uid,
        )
    return join_path(repo_root, "copy_meta_file", dirname)


async def open_target_db(repo: DedupRepo, version: Version, meta_dir: str) -> SqliteSource:
    """Open ``<meta_dir>/target.db`` (``aHlT``-enveloped or not, with any
    ``-wal``/``-shm`` sidecar; FORMAT-SPEC.md: Landing directory layout), checked against
    ``meta_filenames``. ``version.meta`` must be set, as
    ``resolve_copy_meta_dir`` already required.

    Raises:
        NotFoundError: ``target.db`` is not in ``meta_filenames``.
        DataCorruptError: The file or a sidecar claims an ``aHlT``/zstd
            frame that doesn't decode (or not to its declared size), or is
            ``aHlT``-enveloped and the repository has no vault key.
        ResourceLimitExceededError: Its temporary copy, or a sidecar's,
            doesn't fit in free disk space with the reserve left free.
    """
    meta = version.meta
    assert meta is not None
    physical_name = resolve_meta_filename(meta, "target.db", ref=f"{meta_dir}/target.db")
    return await SqliteSource.from_enveloped_store(
        repo.store, f"{meta_dir}/{physical_name}", vault_key=repo.vault_key, what="target.db"
    )


async def target_version_id(target_db: SqliteSource, meta_dir: str) -> int:
    """``target.db``'s own ``version_table.version_id``.

    Raises:
        NotFoundError: ``version_table`` has no row.
    """
    cursor = await target_db.connection.execute("SELECT version_id FROM version_table")
    row = await cursor.fetchone()
    if row is None:
        raise NotFoundError("target.db has no version_table row", ref=meta_dir)
    return as_int(row[0])


async def versions(repo: DedupRepo, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
    """``workload``'s browsable versions, newest backup first (the time
    ``display_name`` shows); one without a readable time sorts last, ties
    by ``version_id`` descending.

    Raises:
        DataCorruptError: A ``copy_target_version_meta`` row's
            ``meta_filenames`` is not a JSON array.
    """
    # Without the hint a materialized copy (remote, or local with a live
    # -wal) full-scans a table of version_spec blobs: the schema indexes
    # only version_uid.
    where = "workload_id = ?" if include_deleted else "workload_id = ? AND deleted = 0"
    result = await _browsable_versions(repo, where, (workload.workload_id,), index_hints=[["workload_id", "deleted"]])
    result.sort(key=lambda item: (item[0] is not None, item[0] or 0, item[1]), reverse=True)
    return [version for _epoch, _version_id, version in result]


async def version_by_uid(repo: DedupRepo, version_uid: VersionUid) -> Version | None:
    """The version with ``version_uid``, deleted or not, decrypting only
    its row. ``None`` if it doesn't exist or isn't browsable.

    Raises:
        DataCorruptError: As ``versions``.
    """
    found = await _browsable_versions(repo, "version_uid = ?", (version_uid,), index_hints=[["version_uid"]])
    return found[0][2] if found else None


async def _browsable_versions(
    repo: DedupRepo, where: str, params: tuple[object, ...], *, index_hints: list[list[str]]
) -> list[tuple[int | None, int, Version]]:
    """Every ``copy_target_version`` row matching ``where`` whose status is
    browsable, as ``(epoch, version_id, Version)`` for the caller to sort."""
    table = await Table.create(
        await repo.db("copy_target_version"), "copy_target_version", _VERSION_COLUMNS, index_hints=index_hints
    )
    rows: list[tuple[dict[str, object], str, ParsedVersionStatus | None]] = []
    async for row in table.select(where, params):
        version_uid = as_str(row["version_uid"])
        version_spec_raw = as_str(row["version_spec"])
        status = _parse_version_status(version_spec_raw, version_uid, repo.vault_key)
        status_value = status.status if status is not None else None
        if not isinstance(status_value, str) or status_value not in _BROWSABLE_VERSION_STATUSES:
            continue
        rows.append((row, version_uid, status))
    metas = await _version_metas_for(repo, [version_uid for _row, version_uid, _status in rows])
    result: list[tuple[int | None, int, Version]] = []
    for row, version_uid, status in rows:
        version_id = as_int(row["version_id"])
        result.append(
            (
                _version_epoch(status),
                version_id,
                Version(
                    version_id=VersionId(version_id),
                    version_uid=VersionUid(version_uid),
                    workload_id=WorkloadId(as_int(row["workload_id"])),
                    connection_config_id=ConnectionConfigId(as_int(row["connection_config_id"])),
                    target_type=as_str(row["target_type"]),
                    target_id=TargetId(as_str(row["target_id"])),
                    saas_stream_uuid=StreamUuid(as_str(row["saas_stream_uuid"])),
                    saas_snapshot_uuid=SnapshotUuid(as_str(row["saas_snapshot_uuid"])),
                    saas_version_id=SaasVersionId(as_int(row["saas_version_id"])),
                    deleted=bool(row["deleted"]),
                    display_name=_version_display_name(status, version_uid),
                    meta=metas.get(version_uid),
                ),
            )
        )
    return result


async def version_file_ids(repo: DedupRepo, version: Version) -> list[int]:
    """Every ``fid`` this version registered in ``copy_target_file`` (a
    PC/PS version's disk fragments); empty when the repository has no
    ``copy_target_version`` database."""
    try:
        conn = await repo.db("copy_target_version")
    except NotFoundError:
        return []
    # copy_target_file lives in the same physical sqlite file as copy_target_version.
    await apply_index_hint(conn, "copy_target_file", ["version_id"])
    cursor = await conn.execute("SELECT fid FROM copy_target_file WHERE version_id = ?", (version.version_id,))
    return [row[0] for row in await cursor.fetchall()]


async def version_additional_meta(repo: DedupRepo, version: Version) -> dict[str, Any] | None:
    """``version_spec.status.additional_meta`` (SaaS connector bookkeeping,
    e.g. its object-name index), parsed; ``None`` whenever it can't be
    read, including when a level of it is not of its expected JSON type."""
    try:
        db = await repo.db("copy_target_version")
    except NotFoundError:
        return None
    table = await Table.create(db, "copy_target_version", [Column("version_uid"), Column("version_spec")])
    row = await table.select_one("version_uid = ?", (version.version_uid,))
    if row is None:
        return None
    spec = parse_version_spec(str(row["version_spec"]), version.version_uid, repo.vault_key)
    return try_parse_json_object(json_object(json_object(spec).get("status")).get("additional_meta"))


def _as_epoch_seconds(value: object) -> int | None:
    """A ``start_time``/``end_time`` value (a protobuf-JSON int64, usually a
    string) as epoch seconds; ``None`` when unparseable or ``0``, protobuf's
    "not set"."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value or None
    if isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError:
            return None
        return parsed or None
    return None


@dataclasses.dataclass(frozen=True, slots=True)
class ParsedVersionStatus:
    """The ``version_spec.status`` fields this module reads, named as the
    JSON keys."""

    status: object = None
    start_time: object = None
    end_time: object = None


def parse_version_spec(version_spec_raw: str, version_uid: str, vault_key: bytes | None) -> object | None:
    """One ``copy_target_version.version_spec`` value, decrypted whenever
    ``vault_key`` is given (the column has no marker to probe) and
    JSON-parsed; ``None`` on any failure."""
    try:
        raw = (
            decrypt_version_spec(version_spec_raw, version_uid, vault_key)
            if vault_key is not None
            else version_spec_raw
        )
        parsed: object = json.loads(raw)
    except (ValueError, UnicodeDecodeError, binascii.Error, KeyMaterialError, RecursionError):
        # RecursionError: JSON nested deeper than the decoder's recursion limit.
        return None
    return parsed


def _parse_version_status(
    version_spec_raw: str, version_uid: str, vault_key: bytes | None
) -> ParsedVersionStatus | None:
    """``parse_version_spec``'s ``status`` object, or ``None``."""
    spec = parse_version_spec(version_spec_raw, version_uid, vault_key)
    raw_status = spec.get("status") if isinstance(spec, dict) else None
    if not isinstance(raw_status, dict):
        return None
    return ParsedVersionStatus(
        status=raw_status.get("status"),
        start_time=raw_status.get("start_time"),
        end_time=raw_status.get("end_time"),
    )


def _version_epoch(status: ParsedVersionStatus | None) -> int | None:
    """The backup time: ``start_time``, else ``end_time``, else ``None``."""
    if status is None:
        return None
    epoch = _as_epoch_seconds(status.start_time)
    if epoch is None:
        epoch = _as_epoch_seconds(status.end_time)
    return epoch


def _version_display_name(status: ParsedVersionStatus | None, version_uid: str) -> str:
    """``_version_epoch`` formatted in local time, else ``version_uid``."""
    epoch = _version_epoch(status)
    if epoch is None:
        return version_uid
    try:
        return format_timestamp(datetime.fromtimestamp(epoch, UTC))
    except (OverflowError, OSError, ValueError):
        return version_uid


async def _version_metas_for(repo: DedupRepo, version_uids: list[str]) -> dict[str, VersionMeta]:
    """The ``copy_target_version_meta`` rows for ``version_uids`` in one
    query; a uid without one, or whose ``target_meta_path``/``status`` has
    the wrong type, is absent."""
    if not version_uids:
        return {}
    try:
        conn = await repo.db("copy_target_version_meta")
    except NotFoundError:
        return {}
    placeholders = sql_placeholders(len(version_uids))
    cursor = await conn.execute(
        "SELECT version_uid, target_meta_path, meta_filenames, status FROM copy_target_version_meta "
        f"WHERE version_uid IN ({placeholders})",
        version_uids,
    )
    metas: dict[str, VersionMeta] = {}
    for version_uid, target_meta_path, meta_filenames_json, status in await cursor.fetchall():
        status_code = json_int(status)
        if not isinstance(target_meta_path, str) or status_code is None:
            continue
        # meta_filenames is NULL while a row is still being written
        # (status 0): no files have landed yet.
        filenames = parse_json_array(meta_filenames_json or "[]", "meta_filenames", ref=version_uid)
        metas[version_uid] = VersionMeta(
            target_meta_path=target_meta_path,
            meta_filenames=tuple(name for name in filenames if isinstance(name, str)),
            status=status_code,
        )
    return metas
